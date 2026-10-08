use crate::app::App;
use anyhow::{Result, ensure};
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

#[derive(Deserialize)]
struct StorageMigration {
    source_bucket: Uuid,
    source_key: String,
    source_version: Uuid,
    target_bucket: Uuid,
    target_key: String,
    target_version: Option<Uuid>,
    action: String,
    public_read: bool,
}

impl App {
    pub(crate) async fn storage_migration_batch(
        &self,
        id: Uuid,
        lease_token: Uuid,
        detail: Value,
    ) -> Result<bool> {
        let job: StorageMigration =
            serde_json::from_value(detail.get("job").cloned().unwrap_or(detail))?;
        ensure!(
            matches!(job.action.as_str(), "copy" | "move"),
            "invalid storage migration action"
        );
        let (source, _source_pin) = self.current(job.source_bucket, &job.source_key).await?;
        ensure!(
            source.id == job.source_version,
            "source object changed before storage migration"
        );
        let target_storage: Uuid = sqlx::query_scalar("SELECT storage_id FROM buckets WHERE id=$1")
            .bind(job.target_bucket)
            .fetch_one(&self.db)
            .await?;
        let _storage_permit = self
            .storage
            .profile_permit(target_storage, "migration")
            .await?;
        let profile =
            crate::storage::profile::StorageProfile::load(&self.db, target_storage).await?;
        if let Some(limit) = profile
            .migration_byte_budget
            .as_deref()
            .map(crate::config::bytes)
            .transpose()?
            .filter(|value| *value > 0)
        {
            ensure!(
                source.size as u64 <= limit,
                "storage migration byte budget exceeded; retry with a larger profile budget"
            );
        }
        let authority = crate::authorization::Permit::system();
        let (destination, _destination_pin) = self
            .new_stream(
                &authority,
                job.target_bucket,
                &job.target_key,
                "object",
                source.metadata.clone(),
                job.public_read,
                false,
                None,
                Some((id, lease_token)),
            )
            .await?;
        let result: Result<()> = async {
            self.copy_extents(destination, &source).await?;
            self.finish_stream(
                destination,
                source.size,
                &source.etag,
                source.checksums.clone(),
            )
            .await?;
            #[cfg(feature = "fault-injection")]
            crate::faults::point("migration-before-publish").await;
            let _coord = self.coord.lock().await;
            self.writable()?;
            let mut tx = self.db.begin().await?;
            let owned: Option<Uuid> = sqlx::query_scalar("SELECT id FROM tasks WHERE id=$1 AND state='running' AND lease_token=$2 AND lease_expires_at>clock_timestamp() FOR UPDATE")
                .bind(id)
                .bind(lease_token)
                .fetch_optional(&mut *tx)
                .await?;
            ensure!(owned.is_some(), "storage migration lease lost before publish");
            let source_current = crate::control::actions::selected(
                &mut tx,
                job.source_bucket,
                &job.source_key,
                Some(job.source_version),
            )
            .await?;
            let old = crate::control::actions::selected(
                &mut tx,
                job.target_bucket,
                &job.target_key,
                job.target_version,
            )
            .await?;
            ensure!(
                source_current == Some(job.source_version),
                "source object changed during storage migration"
            );
            let published = sqlx::query("UPDATE streams SET state='ready',owner_task_id=NULL,owner_lease_token=NULL,touched_at=now() WHERE id=$1 AND state='writing' AND owner_task_id=$2 AND owner_lease_token=$3")
                .bind(destination)
                .bind(id)
                .bind(lease_token)
                .execute(&mut *tx)
                .await?;
            ensure!(published.rows_affected() == 1, "storage migration stream was reclaimed");
            sqlx::query("INSERT INTO objects(bucket_id,object_key,stream_id,write_token) VALUES($1,$2,$3,$3) ON CONFLICT(bucket_id,object_key) DO UPDATE SET stream_id=excluded.stream_id,write_token=excluded.write_token")
                .bind(job.target_bucket)
                .bind(&job.target_key)
                .bind(destination)
                .execute(&mut *tx)
                .await?;
            if job.action == "move" {
                sqlx::query("UPDATE objects SET stream_id=NULL,write_token=$3 WHERE bucket_id=$1 AND object_key=$2")
                    .bind(job.source_bucket)
                    .bind(&job.source_key)
                    .bind(Uuid::new_v4())
                    .execute(&mut *tx)
                    .await?;
                sqlx::query("UPDATE streams SET state='retired',touched_at=now() WHERE id=$1")
                    .bind(job.source_version)
                    .execute(&mut *tx)
                    .await?;
            }
            if let Some(old) = old {
                sqlx::query("UPDATE streams SET state='retired',touched_at=now() WHERE id=$1")
                    .bind(old)
                    .execute(&mut *tx)
                    .await?;
            }
            let completed = sqlx::query("UPDATE tasks SET state='completed',processed=processed+1,lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$2 AND lease_expires_at>clock_timestamp()")
                .bind(id)
                .bind(lease_token)
                .execute(&mut *tx)
                .await?;
            ensure!(
                completed.rows_affected() == 1,
                "storage migration lease lost before publish"
            );
            tx.commit().await?;
            Ok(())
        }
        .await;
        if result.is_err() {
            sqlx::query("UPDATE streams SET state='abandoned',owner_task_id=NULL,owner_lease_token=NULL,touched_at=now() WHERE id=$1 AND state='writing'")
                .bind(destination)
                .execute(&self.db)
                .await?;
        }
        result?;
        self.wake_gc.notify_one();
        Ok(true)
    }
}
