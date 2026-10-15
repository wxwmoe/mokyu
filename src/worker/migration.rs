use crate::app::App;
use crate::model::object::StorageMigration;
use anyhow::{Result, ensure};
use serde_json::Value;
use uuid::Uuid;

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
        let target_storage = self
            .objects
            .storage(job.target_bucket)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
        let _storage_permit = self
            .storage
            .profile_permit(target_storage, "migration")
            .await?;
        let profile = self.storage_profiles.load(target_storage).await?;
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
            self.objects
                .complete_migration(id, lease_token, destination, &job)
                .await?;
            Ok(())
        }
        .await;
        if result.is_err() {
            self.objects.abandon_migration(destination).await?;
        }
        result?;
        self.wake_gc.notify_one();
        Ok(true)
    }
}
