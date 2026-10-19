use crate::model::task::Execution;
use crate::{app::App, config};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use uuid::Uuid;

impl App {
    pub async fn purge_preview(&self, name: &str) -> Result<Value> {
        let b = self.bucket(name, false).await?;
        let (objects, uploads): (i64, i64) = self.buckets.purge_counts(b.id).await?;
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
        let result = self
            .buckets
            .start_purge(principal, name, id, confirmation)
            .await?;
        if result["existing"] != true {
            self.wake_tasks.notify_one();
        }
        Ok(result)
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
        self.ensure_mode()?;
        let age = config::seconds(older_than)?;
        ensure!(age <= i64::MAX as u64 / 1000, "age too large");
        let storage_id = self.storage.select_store(storage_id)?;
        let profile = self.storage_profiles.load(storage_id).await?;
        let minimum = profile.min_storage_seconds()?;
        let prefix = self.storage.namespace_for(storage_id)?;
        if execute {
            let indexed: bool = self.physical.has_chunks().await?;
            ensure!(
                indexed,
                "empty chunk index; restore the correct database before destructive sweep"
            );
            ensure!(
                confirm_prefix == Some(prefix.as_str()),
                "confirmed prefix must exactly match the preview backend prefix"
            );
            let previous: Value = self
                .task_control
                .sweep_preview(preview.context("--preview needs a completed dry-run task ID")?)
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
                "storage min_storage_duration changed; preview again"
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
        let id = self.task_control.enqueue("sweep", None, detail).await?;
        self.wake_tasks.notify_one();
        Ok(json!({"task_id":id,"dry_run":!execute}))
    }
    pub(crate) async fn purge_batch(
        &self,
        id: Execution,
        bucket: Uuid,
        cursor: Option<&str>,
    ) -> Result<bool> {
        #[cfg(feature = "fault-injection")]
        crate::faults::point("purge-before-batch").await;
        let coord = self.coord.lock().await;
        if self.maintenance.load(std::sync::atomic::Ordering::Acquire) {
            self.task_control.pause(id).await?;
            return Ok(false);
        }
        let running: bool = self.task_control.running(id).await?;
        if !running {
            return Ok(false);
        }
        let (batch, uploads) = self
            .buckets
            .purge_uploads(bucket, self.config.maintenance.batch_size as i32)
            .await?;
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
        let active: Vec<Uuid> = self.active.lock().unwrap().keys().copied().collect();
        let completed = self
            .buckets
            .purge_batch(id, bucket, cursor, batch, &active)
            .await?;
        if !completed {
            self.wake_gc.notify_one();
        }
        Ok(completed)
    }
    pub(crate) async fn sweep_batch(
        &self,
        id: Execution,
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
        let profile = self.storage_profiles.load(storage_id).await?;
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
                self.config.maintenance.batch_size as usize,
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
            self.task_control.complete(id).await?;
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
            let indexed: bool = self.physical.indexed(storage).await?;
            if indexed {
                continue;
            }
            if destructive {
                let _coord = self.coord.lock().await;
                let running: bool = self.task_control.running(id).await?;
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
        self.task_control
            .progress(id, &last, rows.len() as i64, detail)
            .await?;
        Ok(false)
    }
}
