use crate::{app::App, config};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use uuid::Uuid;

use super::insert;

impl App {
    pub async fn purge_preview(&self, name: &str) -> Result<Value> {
        let b = self.bucket(name, false).await?;
        let (objects,uploads):(i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM objects WHERE bucket_id=$1 AND stream_id IS NOT NULL),(SELECT count(*) FROM uploads WHERE bucket_id=$1 AND state IN ('active','completing'))").bind(b.id).fetch_one(&self.db).await?;
        Ok(
            json!({"bucket_id":b.id,"name":b.name,"state":b.state,"objects":objects,"active_uploads":uploads,"effect":"seal bucket, remove every object and upload, then delete bucket; shared chunks use normal GC grace"}),
        )
    }
    pub async fn purge_start(
        &self,
        principal: &crate::authorization::Principal,
        name: &str,
        id: Uuid,
        confirmation: Option<&str>,
    ) -> Result<Value> {
        self.writable()?;
        let _coord = self.coord.lock().await;
        self.writable()?;
        let mut tx = crate::control::users::admin_transaction(self, principal).await?;
        let found: Option<Uuid> =
            sqlx::query_scalar("SELECT id FROM buckets WHERE name=$1 AND id=$2 FOR UPDATE")
                .bind(name)
                .bind(id)
                .fetch_optional(&mut *tx)
                .await?;
        if found.is_none() {
            return Err(s3s::s3_error!(PreconditionFailed).into());
        }
        if let Some(task) = sqlx::query_scalar::<_, Uuid>(
            "SELECT id FROM tasks WHERE task_type='purge' AND bucket_id=$1 AND state<>'completed'",
        )
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?
        {
            return Ok(json!({"task_id":task,"existing":true}));
        }
        if let Some(expected) = confirmation {
            let preview = crate::control::buckets::impact(&mut tx, id).await?;
            if crate::control::buckets::fingerprint(&preview, None)? != expected {
                return Err(s3s::s3_error!(PreconditionFailed).into());
            }
        }
        sqlx::query("UPDATE buckets SET state='purging' WHERE id=$1")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        let task = insert(
            &mut tx,
            "purge",
            Some(id),
            json!({"bucket_id":id,"name":name}),
        )
        .await?;
        crate::control::audit::checkpoint(
            &mut tx,
            "bucket.purge",
            name,
            json!({"bucket_id":id,"task_id":task}),
        )
        .await?;
        tx.commit().await?;
        self.wake_tasks.notify_one();
        Ok(json!({"task_id":task}))
    }
    pub async fn sweep_start(
        &self,
        execute: bool,
        preview: Option<Uuid>,
        confirm_prefix: Option<&str>,
        older_than: &str,
        storage_id: Option<Uuid>,
    ) -> Result<Value> {
        let _coord = self.coord.lock().await;
        let age = config::seconds(older_than)?;
        ensure!(age <= i64::MAX as u64 / 1000, "age too large");
        let storage_id = self.storage.select_store(storage_id)?;
        let profile = crate::storage::profile::StorageProfile::load(&self.db, storage_id).await?;
        let minimum = profile.min_storage_seconds()?;
        let prefix = self.storage.namespace_for(storage_id)?;
        if execute {
            let indexed: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM chunks)")
                .fetch_one(&self.db)
                .await?;
            ensure!(
                indexed,
                "empty chunk index; restore the correct database before destructive sweep"
            );
            ensure!(
                confirm_prefix == Some(prefix.as_str()),
                "confirmed prefix must exactly match the preview backend prefix"
            );
            let previous: Value = sqlx::query_scalar(
                "SELECT detail FROM tasks WHERE id=$1 AND task_type='sweep' AND state='completed'",
            )
            .bind(preview.context("--preview needs a completed dry-run task ID")?)
            .fetch_optional(&self.db)
            .await?
            .context("completed preview task not found")?;
            ensure!(
                previous["dry_run"] == true && previous["prefix"] == prefix,
                "preview scope mismatch"
            );
            let previous_target: Uuid = previous["storage_id"]
                .as_str()
                .context("preview store is missing")?
                .parse()?;
            ensure!(previous_target == storage_id, "preview store mismatch");
            ensure!(
                previous["older_than_seconds"].as_u64() == Some(age),
                "use the same --age as the preview"
            );
            ensure!(
                previous["min_storage_duration_seconds"]
                    .as_u64()
                    .unwrap_or(0)
                    == minimum,
                "backend.min_storage_duration changed; preview again"
            );
            ensure!(
                self.maintenance.load(std::sync::atomic::Ordering::Acquire),
                "enable maintenance before destructive backend sweep"
            );
            ensure!(
                self.active.lock().unwrap().is_empty(),
                "wait for active uploads, completions and reads to drain"
            );
        }
        let cutoff = chrono::Utc::now()
            .checked_sub_signed(chrono::TimeDelta::seconds(age.max(minimum) as i64))
            .context("age too large")?;
        let detail = json!({"dry_run":!execute,"storage_id":storage_id,"profile_revision":profile.revision,"batch_size":profile.maintenance_concurrency,"byte_budget":profile.maintenance_byte_budget,"prefix":prefix,"older_than_seconds":age,"min_storage_duration_seconds":minimum,"cutoff":cutoff.to_rfc3339(),"candidates":0,"bytes":0,"unrecognized":0,"samples":[]});
        let id = insert(&mut *self.db.acquire().await?, "sweep", None, detail).await?;
        self.wake_tasks.notify_one();
        Ok(json!({"task_id":id,"dry_run":!execute}))
    }
    pub(crate) async fn purge_batch(
        &self,
        id: Uuid,
        bucket: Uuid,
        cursor: Option<&str>,
    ) -> Result<bool> {
        #[cfg(feature = "fault-injection")]
        crate::faults::point("purge-before-batch").await;
        let coord = self.coord.lock().await;
        if self.maintenance.load(std::sync::atomic::Ordering::Acquire) {
            sqlx::query("UPDATE tasks SET state='paused',updated_at=now() WHERE id=$1 AND state IN ('queued','running')")
                .bind(id).execute(&self.db).await?;
            return Ok(false);
        }
        let running: bool = sqlx::query_scalar("SELECT state='running' FROM tasks WHERE id=$1")
            .bind(id)
            .fetch_one(&self.db)
            .await?;
        if !running {
            return Ok(false);
        }
        let batch = sqlx::query_scalar::<_, Option<i32>>("SELECT maintenance_concurrency FROM storage_profiles WHERE storage_id=(SELECT storage_id FROM buckets WHERE id=$1)")
            .bind(bucket)
            .fetch_one(&self.db)
            .await?
            .unwrap_or(self.config.maintenance.batch_size as i32)
            .max(1) as i64;
        let uploads:Vec<Uuid>=sqlx::query_scalar("SELECT id FROM uploads WHERE bucket_id=$1 AND state IN ('active','completing') ORDER BY id LIMIT $2").bind(bucket).bind(batch).fetch_all(&self.db).await?;
        if !uploads.is_empty() {
            for upload in uploads {
                let lock = self.operation_lock(upload);
                let Ok(_guard) = lock.try_lock() else {
                    continue;
                };
                self.abort_upload_locked(upload, &coord, &crate::authorization::Permit::system())
                    .await?;
            }
            return Ok(false);
        }
        let mut tx = self.db.begin().await?;
        let keys:Vec<(String,Option<Uuid>)>=sqlx::query_as("SELECT object_key,stream_id FROM objects WHERE bucket_id=$1 AND ($2::text IS NULL OR object_key>$2) ORDER BY object_key LIMIT $3 FOR UPDATE").bind(bucket).bind(cursor).bind(batch).fetch_all(&mut *tx).await?;
        if !keys.is_empty() {
            let last = &keys.last().unwrap().0;
            for (key, stream) in &keys {
                if let Some(stream) = stream {
                    sqlx::query("UPDATE streams SET state='retired',touched_at=now() WHERE id=$1")
                        .bind(stream)
                        .execute(&mut *tx)
                        .await?;
                }
                sqlx::query("UPDATE objects SET stream_id=NULL,write_token=$3 WHERE bucket_id=$1 AND object_key=$2").bind(bucket).bind(key).bind(Uuid::new_v4()).execute(&mut *tx).await?;
            }
            sqlx::query(
                "UPDATE tasks SET cursor=$2,processed=processed+$3,updated_at=now() WHERE id=$1",
            )
            .bind(id)
            .bind(last)
            .bind(keys.len() as i64)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            self.wake_gc.notify_one();
            return Ok(false);
        }
        let active: Vec<Uuid> = self.active.lock().unwrap().keys().copied().collect();
        sqlx::query("DELETE FROM uploads WHERE id IN (SELECT id FROM uploads WHERE bucket_id=$1 AND state IN ('completed','aborted') AND NOT (id=ANY($2)) LIMIT $3)").bind(bucket).bind(active).bind(batch).execute(&mut *tx).await?;
        let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM streams WHERE bucket_id=$1) OR EXISTS(SELECT 1 FROM uploads WHERE bucket_id=$1)").bind(bucket).fetch_one(&mut *tx).await?;
        if waiting {
            tx.commit().await?;
            self.wake_gc.notify_one();
            return Ok(false);
        }
        sqlx::query("DELETE FROM objects WHERE bucket_id=$1")
            .bind(bucket)
            .execute(&mut *tx)
            .await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(crate::control::account::USER_LOCK)
            .execute(&mut *tx)
            .await?;
        crate::control::buckets::lock_dependents(&mut tx, bucket).await?;
        sqlx::query("DELETE FROM buckets WHERE id=$1 AND state='purging'")
            .bind(bucket)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE tasks SET state='completed',updated_at=now() WHERE id=$1")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(true)
    }
    pub(crate) async fn sweep_batch(
        &self,
        id: Uuid,
        cursor: Option<&str>,
        mut detail: Value,
    ) -> Result<bool> {
        let destructive = detail["dry_run"] == false;
        if destructive {
            ensure!(
                self.maintenance.load(std::sync::atomic::Ordering::Acquire),
                "maintenance was disabled; sweep stopped"
            );
            ensure!(
                self.active.lock().unwrap().is_empty(),
                "active data operations have not drained"
            );
            ensure!(
                !self.gc_running.load(std::sync::atomic::Ordering::Acquire),
                "wait for current GC batch"
            );
        }
        let storage_id: Uuid = detail["storage_id"]
            .as_str()
            .context("missing sweep store")?
            .parse()?;
        let prefix = self.storage.namespace_for(storage_id)?;
        ensure!(detail["prefix"] == prefix, "backend namespace changed");
        let cutoff = chrono::DateTime::parse_from_rfc3339(
            detail["cutoff"].as_str().context("missing sweep cutoff")?,
        )?
        .with_timezone(&chrono::Utc);
        // A resumed task may have been created under a shorter retention policy.
        let profile = crate::storage::profile::StorageProfile::load(&self.db, storage_id).await?;
        let minimum = profile.min_storage_seconds()?;
        let cutoff = cutoff.min(
            chrono::Utc::now()
                .checked_sub_signed(chrono::TimeDelta::seconds(minimum as i64))
                .context("age too large")?,
        );
        let mut rows = self
            .storage
            .list_physical_for(
                storage_id,
                cursor,
                detail["batch_size"]
                    .as_u64()
                    .unwrap_or(
                        profile
                            .maintenance_concurrency
                            .unwrap_or(self.config.maintenance.batch_size as i32)
                            as u64,
                    )
                    .max(1) as usize,
            )
            .await?;
        if let Some(limit) = profile
            .maintenance_byte_budget
            .as_deref()
            .map(config::bytes)
            .transpose()?
            .filter(|value| *value > 0)
        {
            let mut bytes = 0u64;
            let keep = rows
                .iter()
                .enumerate()
                .take_while(|(_, row)| {
                    if bytes > 0 && bytes.saturating_add(row.size) > limit {
                        return false;
                    }
                    bytes = bytes.saturating_add(row.size);
                    true
                })
                .count();
            rows.truncate(keep.max(1).min(rows.len()));
        }
        let Some(last) = rows.last().map(|r| r.location.to_string()) else {
            sqlx::query("UPDATE tasks SET state='completed',updated_at=now() WHERE id=$1 AND state='running'").bind(id).execute(&self.db).await?;
            return Ok(true);
        };
        for row in &rows {
            if row.last_modified > cutoff {
                continue;
            }
            let name = row.location.as_ref();
            let Some(storage) = self.storage.parse_physical_path_for(storage_id, name)? else {
                detail["unrecognized"] = json!(detail["unrecognized"].as_u64().unwrap_or(0) + 1);
                continue;
            };
            // Every indexed state protects a physical key, including preparing and deleting.
            let indexed: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM chunk_encodings WHERE location_id=$1) OR EXISTS(SELECT 1 FROM packs WHERE location_id=$1)")
                    .bind(storage)
                    .fetch_one(&self.db)
                    .await?;
            if indexed {
                continue;
            }
            if destructive {
                let _coord = self.coord.lock().await;
                let running: bool =
                    sqlx::query_scalar("SELECT state='running' FROM tasks WHERE id=$1")
                        .bind(id)
                        .fetch_one(&self.db)
                        .await?;
                if !running {
                    return Ok(false);
                }
                ensure!(
                    self.maintenance.load(std::sync::atomic::Ordering::Acquire),
                    "maintenance was disabled; sweep stopped"
                );
                self.storage
                    .delete_path_for(storage_id, &row.location)
                    .await?;
            }
            detail["candidates"] = json!(detail["candidates"].as_u64().unwrap_or(0) + 1);
            detail["bytes"] = json!(detail["bytes"].as_u64().unwrap_or(0) + row.size);
            let samples = detail["samples"]
                .as_array_mut()
                .context("invalid task samples")?;
            if samples.len() < 20 {
                samples.push(json!(name));
            }
        }
        sqlx::query("UPDATE tasks SET cursor=$2,processed=processed+$3,detail=$4,updated_at=now() WHERE id=$1").bind(id).bind(last).bind(rows.len()as i64).bind(detail).execute(&self.db).await?;
        Ok(false)
    }
}
