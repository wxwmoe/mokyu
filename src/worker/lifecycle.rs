use crate::model::Id;
use crate::{app::App, config};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex, atomic::Ordering},
    time::{Duration, Instant},
};
use uuid::Uuid;

#[derive(Default)]
pub struct Cleanup {
    pub(crate) running: tokio::sync::Mutex<()>,
    last_run: Mutex<Value>,
}

impl App {
    pub(crate) async fn cleanup_scoped(
        &self,
        storage_id: Uuid,
        batch: usize,
        retention_seconds: u64,
    ) -> Result<usize> {
        self.cleanup_inner(Some(storage_id), batch, Some(retention_seconds))
            .await
    }
    /// Reclaim only chunks owned by one storage and consume one bounded slice.
    pub(crate) async fn reclaim_scoped(
        &self,
        storage_id: Uuid,
        batch: usize,
        grace_seconds: u64,
        byte_budget: Option<u64>,
        min_storage_seconds: u64,
    ) -> Result<(usize, usize)> {
        if self.gc_running.swap(true, Ordering::AcqRel) {
            return Ok((0, 0));
        }
        let _running = Running(&self.gc_running);
        let ids: Vec<(Id, i32)> = self
            .maintenance_store
            .chunk_candidates(storage_id, grace_seconds as f64, batch.max(1) as i64)
            .await?;
        let mut count = 0;
        let mut consumed = 0u64;
        for (id, raw_size) in ids {
            let bytes = raw_size.max(0) as u64;
            if let Some(limit) = byte_budget {
                if consumed > 0 && consumed.saturating_add(bytes) > limit {
                    break;
                }
                consumed = consumed.saturating_add(bytes);
            }
            if self
                .reclaim_chunk(id, storage_id, grace_seconds as f64)
                .await?
            {
                count += 1;
            }
        }
        let physical = self
            .reclaim_physical(grace_seconds as f64, storage_id, batch, min_storage_seconds)
            .await?;
        Ok((count, physical))
    }

    pub fn cleanup_status(&self) -> Value {
        let c = &self.config.maintenance;
        json!({"running":self.cleanup_state.running.try_lock().is_err(),
            "last_run":self.cleanup_state.last_run.lock().unwrap().clone(),
            "interval":c.interval,"batch_size":c.batch_size,"max_duration":c.max_duration,
            "task_retention":c.task_retention,"audit_retention":c.audit_retention})
    }

    pub async fn cleanup_history(&self) -> Result<Value> {
        self.ensure_mode()?;
        anyhow::ensure!(
            !self.maintenance_paused("cleanup").await?,
            "history cleanup is paused"
        );
        let Ok(guard) = self.cleanup_state.running.try_lock() else {
            return Ok(self.cleanup_status());
        };
        let c = &self.config.maintenance;
        let started_at = chrono::Utc::now();
        let start = Instant::now();
        let duration = Duration::from_secs(config::seconds(&c.max_duration)?);
        let batch = c.batch_size as i64;
        let mut deleted = json!({"chunks":0,"uploads":0,"tasks":0,"sessions":0,"refresh_tokens":0,"integrity_issues":0,"packs":0,"chunk_encodings":0,"audit_events":0,"media_operations":0,"maintenance_previews":0,"key_replacements":0});
        let mut batches = 0;
        let sections = [
            ("key_replacements", 0.0),
            ("maintenance_previews", 0.0),
            (
                "media_operations",
                config::seconds(&c.task_retention)? as f64,
            ),
            ("audit_events", config::seconds(&c.audit_retention)? as f64),
            ("chunks", 0.0),
            ("packs", 0.0),
            ("chunk_encodings", 0.0),
            ("uploads", 0.0),
            (
                "integrity_issues",
                config::seconds(&c.task_retention)? as f64,
            ),
            ("tasks", config::seconds(&c.task_retention)? as f64),
            ("sessions", 0.0),
            ("refresh_tokens", config::seconds(&c.task_retention)? as f64),
        ];
        // Only SQL is canceled at the deadline: an unfinished transaction rolls back.
        let result = tokio::time::timeout(duration, async {
            loop {
                let mut full = false;
                for (section, age) in sections {
                    if self.maintenance_paused("cleanup").await? {
                        return Ok(());
                    }
                    let active = if section == "uploads" {
                        self.active_uploads
                            .lock()
                            .unwrap()
                            .keys()
                            .copied()
                            .collect::<Vec<_>>()
                    } else {
                        Vec::new()
                    };
                    let count = self
                        .maintenance_store
                        .cleanup_history(
                            section,
                            age,
                            batch,
                            &active,
                            duration.saturating_sub(start.elapsed()).as_millis(),
                        )
                        .await?;
                    deleted[section] = json!(deleted[section].as_u64().unwrap() + count);
                    batches += 1;
                    full |= count == batch as u64;
                }
                if !full {
                    break;
                }
                tokio::task::yield_now().await;
            }
            Ok::<(), anyhow::Error>(())
        })
        .await;
        let (budget_exhausted, error) = match result {
            Ok(Ok(())) => (false, None),
            Err(_) => (true, None),
            Ok(Err(e)) => {
                let timeout = crate::model::error(&e) == Some(crate::model::Error::QueryCancelled);
                (timeout, if timeout { None } else { Some(e) })
            }
        };
        *self.cleanup_state.last_run.lock().unwrap() = json!({
            "started_at":started_at,"finished_at":chrono::Utc::now(),
            "duration_ms":start.elapsed().as_millis() as u64,"deleted":deleted,"batches":batches,
            "budget_exhausted":budget_exhausted,"last_error":error.as_ref().map(|_| "cleanup_failed")});
        drop(guard);
        if let Some(e) = error {
            return Err(e);
        }
        Ok(self.cleanup_status())
    }

    pub async fn recover(&self) -> Result<()> {
        let mut after = crate::model::Id::default();
        while self.sync_raw_batch(None, true, &mut after).await? {}
        let mut after = Id::default();
        while self.sync_chunk_batch(None, true, &mut after).await? {}
        self.maintenance_store
            .recover(self.maintenance.load(Ordering::Acquire))
            .await?;
        // Only startup scans unindexed local files: no receiver can be between fsync and SQL commit yet.
        if self.role == "single" {
            for area in ["fragment", "candidate"] {
                let mut files = tokio::fs::read_dir(
                    self.storage.disk.root.join("pending/multipart").join(area),
                )
                .await?;
                while let Some(file) = files.next_entry().await? {
                    if !file.file_type().await?.is_file() {
                        continue;
                    }
                    let Ok(id) = Uuid::parse_str(&file.file_name().to_string_lossy()) else {
                        continue;
                    };
                    if !self.maintenance_store.fragment_exists(id).await? {
                        if area == "fragment" {
                            self.storage.disk.remove_fragment(id).await?;
                        } else {
                            self.storage.disk.remove_candidate(id).await?;
                        }
                    }
                }
            }
        }
        let mut cursor = Id::default();
        loop {
            let rows = self.maintenance_store.recovery_fragments(cursor).await?;
            if rows.is_empty() {
                break;
            }
            for row in rows {
                let id = row.id;
                let source = row.source;
                let data = self
                    .storage
                    .read_fragment(source.location_id, &source)
                    .await;
                if !data.is_ok_and(|bytes| {
                    bytes.len() == source.size as usize
                        && blake3::hash(&bytes).as_bytes() == source.hash.as_slice()
                }) {
                    let uploads: Vec<Uuid> = self.maintenance_store.fragment_uploads(id).await?;
                    for upload in uploads {
                        self.abort_upload(upload).await?;
                        tracing::error!(upload_id=%upload,fragment_id=%id,"multipart source missing or corrupt; upload invalidated");
                    }
                }
                cursor = id;
            }
        }
        Ok(())
    }
    pub async fn cleanup(&self) -> Result<usize> {
        self.cleanup_inner(None, self.config.maintenance.batch_size as usize, None)
            .await
    }

    async fn cleanup_inner(
        &self,
        storage_id: Option<Uuid>,
        batch_size: usize,
        retention_seconds: Option<u64>,
    ) -> Result<usize> {
        let Ok(_running) = self.local_cleanup.try_lock() else {
            return Ok(0);
        };
        self.ensure_mode()?;
        if self.role != "single" {
            self.cluster()?.expire_stream_leases().await?;
        }
        self.reconcile_upload_pins(storage_id).await?;
        self.storage.reconcile_staging(storage_id).await?;
        let deadline = Instant::now()
            + Duration::from_secs(config::seconds(&self.config.maintenance.max_duration)?);
        let mut removed = 0;
        let mut compacted = 0;
        let batch = batch_size.max(1) as i64;
        let expired: Vec<Uuid> = self
            .maintenance_store
            .expired_uploads(batch, storage_id)
            .await?;
        for id in expired {
            if Instant::now() >= deadline {
                break;
            }
            let lock = self.operation_lock(id);
            let Ok(_guard) = lock.try_lock() else {
                continue;
            };
            if !self.is_upload_active(id) {
                let _coord = self.coord.lock().await;
                if self.multipart.expire(id).await? {
                    self.wake_gc.notify_one();
                    removed += 1;
                }
            }
        }
        let active: Vec<Id> = self.active.lock().unwrap().keys().copied().collect();
        let candidates: Vec<Id> = self
            .maintenance_store
            .stale_streams(
                &active,
                batch,
                storage_id,
                retention_seconds.map(|n| n as f64),
            )
            .await?;
        for id in candidates {
            if Instant::now() >= deadline {
                break;
            }
            let Ok(_coord) = self.coord.try_lock() else {
                break;
            };
            if self.is_active(id) {
                continue;
            }
            if !self
                .maintenance_store
                .claim_stream(id, storage_id, retention_seconds.map(|n| n as f64))
                .await?
            {
                continue;
            }
            // Claim under coord; this orphan can no longer be pinned through an object or part.
            drop(_coord);
            #[cfg(feature = "fault-injection")]
            crate::faults::point("cleanup-stream-claimed").await;
            // Keep fragment ownership until its physical object is removed below.
            removed += self.maintenance_store.cleanup_stream(id, batch).await?;
        }
        let active: Vec<Id> = self.active.lock().unwrap().keys().copied().collect();
        let fragments: Vec<Id> = self
            .maintenance_store
            .stale_fragments(batch, &active, storage_id)
            .await?;
        for id in fragments {
            if Instant::now() >= deadline {
                break;
            }
            let Ok(coord) = self.coord.try_lock() else {
                break;
            };
            let Some(fragment) = self.maintenance_store.claim_fragment(id).await? else {
                continue;
            };
            if !fragment.sealed && fragment.owner.is_some_and(|id| self.is_active(id)) {
                continue;
            }
            if self.storage.disk.pending(fragment.location_id) {
                continue;
            }
            // Keep the row locked against new references while releasing coord for I/O.
            drop(coord);
            if fragment.file.is_none() {
                self.storage
                    .remove_fragment(
                        fragment.location_id,
                        fragment.storage,
                        fragment.key.as_deref(),
                    )
                    .await?;
            }
            fragment.deletion.finish().await?;
            removed += 1;
        }
        let (files, ranges) = self
            .reclaim_multipart_files(storage_id, batch, deadline)
            .await?;
        removed += files;
        compacted += ranges;
        if Instant::now() < deadline {
            // Epoch tombstones must be checked while writers cannot create a new stream.
            if let Ok(_coord) = self.coord.try_lock() {
                let count = self
                    .maintenance_store
                    .cleanup_tombstones(storage_id, batch)
                    .await?;
                removed += count as usize;
            }
        }
        if removed > 0 || compacted > 0 {
            self.wake_gc.notify_one();
        }
        Ok(removed)
    }
    pub(crate) async fn reclaim_multipart_files(
        &self,
        storage_id: Option<Uuid>,
        batch: i64,
        deadline: Instant,
    ) -> Result<(usize, usize)> {
        let mut removed = 0;
        let mut compacted = 0;
        {
            let owner = self.node.get().map(|node| node.node_id);
            for file in self
                .multipart
                .unused_files(storage_id, owner, batch)
                .await?
            {
                if Instant::now() >= deadline {
                    break;
                }
                let upload = file.upload_id;
                let lock = self.operation_lock(upload.unwrap_or(file.id));
                let Ok(_guard) = lock.try_lock() else {
                    continue;
                };
                if upload.is_some_and(|id| self.is_upload_active(id))
                    || self.storage.disk.pending(file.id)
                {
                    continue;
                }
                let _gate = self.storage.source_gate.write().await;
                if self.multipart.claim_file(file.id).await? {
                    self.storage.remove_multipart_file(&file).await?;
                    self.multipart.remove_file(file.id).await?;
                    removed += 1;
                }
            }
            for file in self
                .multipart
                .pending_files(storage_id, owner, batch)
                .await?
            {
                if Instant::now() >= deadline {
                    break;
                }
                let lock = self.operation_lock(file.upload_id.unwrap_or(file.id));
                let Ok(_guard) = lock.try_lock() else {
                    continue;
                };
                if file.upload_id.is_some_and(|id| self.is_upload_active(id)) {
                    continue;
                }
                #[cfg(feature = "fault-injection")]
                crate::faults::point("multipart-reclaim-pending").await;
                let _io = self.storage.disk.io.clone().acquire_owned().await?;
                let _gate = self.storage.source_gate.write().await;
                crate::storage::assembly::compact(&self.storage, self.multipart.as_ref(), file.id)
                    .await?;
                compacted += 1;
            }
        }
        Ok((removed, compacted))
    }
    pub async fn reclaim(&self) -> Result<usize> {
        crate::storage::resource_gate::PRIORITY
            .scope(
                crate::storage::resource_gate::MAINTENANCE,
                self.reclaim_inner(),
            )
            .await
    }
    async fn reclaim_inner(&self) -> Result<usize> {
        let paused: bool = self.maintenance_store.gc_paused().await?;
        if paused {
            return Ok(0);
        }
        let storages: Vec<Uuid> = self.maintenance_store.storages().await?;
        let mut count = 0;
        for storage_id in storages {
            let profile = self.storage_profiles.load(storage_id).await?;
            let (chunks, _) = self
                .reclaim_scoped(
                    storage_id,
                    self.config.maintenance.batch_size as usize,
                    config::seconds(&profile.unreferenced_grace)
                        .context("storage unreferenced grace")?,
                    profile
                        .maintenance_byte_budget
                        .as_deref()
                        .map(config::bytes)
                        .transpose()?,
                    profile.min_storage_seconds()?,
                )
                .await?;
            count += chunks;
        }
        Ok(count)
    }
    async fn reclaim_chunk(&self, id: Id, storage_id: Uuid, grace: f64) -> Result<bool> {
        let coord = self.coord.lock().await;
        if self.maintenance.load(Ordering::Acquire) {
            return Ok(false);
        }
        let active = self
            .active
            .lock()
            .unwrap()
            .keys()
            .copied()
            .collect::<Vec<_>>();
        let Some(locations) = self
            .maintenance_store
            .retire_chunk(id, storage_id, grace, &active)
            .await?
        else {
            return Ok(false);
        };
        drop(coord);
        #[cfg(feature = "fault-injection")]
        crate::faults::point("chunk-delete-claimed").await;
        for location_id in locations {
            if let Err(error) = self.storage.invalidate(location_id).await {
                self.statistics.gc_failures.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(chunk_id=%id, %location_id, %error, "chunk cache invalidation will be retried");
                return Ok(false);
            }
        }
        // Physical encodings remain journaled until their retention and deletion finish.
        #[cfg(feature = "fault-injection")]
        crate::faults::point("chunk-deleted").await;
        self.maintenance_store.delete_chunk(id, storage_id).await?;
        self.statistics.gc_deleted.fetch_add(1, Ordering::Relaxed);
        Ok(true)
    }
    async fn reclaim_physical(
        &self,
        grace: f64,
        storage_id: Uuid,
        batch: usize,
        min_storage_seconds: u64,
    ) -> Result<usize> {
        let minimum = min_storage_seconds as f64;
        let retention = config::seconds(
            &self
                .storage_profiles
                .load(storage_id)
                .await?
                .deleted_chunk_retention,
        )? as f64;
        self.maintenance_store.retire_packs(storage_id).await?;
        let mut count = 0;
        for packed in [false, true] {
            let rows = self
                .maintenance_store
                .physical_candidates(packed, grace, batch.max(1) as i64, minimum, storage_id)
                .await?;
            for (id, location_id, unknown_age) in rows {
                let path = if packed {
                    self.storage.pack_path_for(storage_id, location_id)?
                } else {
                    self.storage.path_for(storage_id, location_id)?
                };
                if minimum > 0.0 && unknown_age {
                    match self.storage.head_path_for(storage_id, &path).await {
                        Ok(meta) => {
                            self.maintenance_store
                                .physical_age(packed, id, meta.last_modified)
                                .await?;
                        }
                        Err(e)
                            if matches!(
                                e.downcast_ref::<object_store::Error>(),
                                Some(object_store::Error::NotFound { .. })
                            ) => {}
                        Err(e) => {
                            self.statistics.gc_failures.fetch_add(1, Ordering::Relaxed);
                            tracing::warn!(error=%e,packed,%id,"cannot determine remote age; physical deletion deferred");
                            continue;
                        }
                    }
                }
                let coord = self.coord.lock().await;
                if self.maintenance.load(Ordering::Acquire) {
                    return Ok(count);
                }
                let sources = self.storage.source_gate.write().await;
                if self.storage.physical_active(location_id) {
                    continue;
                }
                let changed = self
                    .maintenance_store
                    .claim_physical(packed, id, grace, minimum)
                    .await?;
                drop(sources);
                drop(coord);
                if !changed {
                    continue;
                }
                match self.storage.delete_path_for(storage_id, &path).await {
                    Ok(()) => {
                        self.maintenance_store
                            .delete_physical(packed, id, retention)
                            .await?;
                        count += 1;
                    }
                    Err(e) => {
                        tracing::warn!(error=%e,packed,%id,"physical deletion will be retried");
                        self.statistics.gc_failures.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
        for source in self
            .raw
            .candidates(
                storage_id,
                grace as i64,
                min_storage_seconds as i64,
                batch.clamp(1, 128) as i64,
            )
            .await?
        {
            let coord = self.coord.lock().await;
            if self.maintenance.load(Ordering::Acquire) {
                return Ok(count);
            }
            let guard = self.storage.source_gate.write().await;
            if self.storage.physical_active(source.location_id) {
                continue;
            }
            let claimed = self
                .raw
                .claim(
                    source.id,
                    source.revision,
                    grace as i64,
                    min_storage_seconds as i64,
                )
                .await?;
            drop(guard);
            drop(coord);
            if !claimed {
                continue;
            }
            match self.storage.remove_raw(&source).await {
                Ok(()) => {
                    self.raw.removed(source.id).await?;
                    count += 1;
                }
                Err(error) => {
                    self.statistics.gc_failures.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(source=%source.id,%error,"raw deletion will be retried");
                }
            }
        }
        Ok(count)
    }
    pub async fn gc_status(&self) -> Result<Value> {
        let (counts, paused, maintenance) = self.maintenance_store.gc_status().await?;
        Ok(
            json!({"paused":paused,"maintenance":maintenance,"running":self.gc_running.load(Ordering::Acquire),"chunks":counts,"interval":self.config.maintenance.interval,"profiles":self.storage_profiles.enabled_profiles().await?}),
        )
    }
    pub async fn mark_unreferenced(&self) -> Result<usize> {
        ensure!(
            self.maintenance.load(Ordering::Acquire),
            "enable maintenance before marking unreferenced chunks"
        );
        ensure!(
            self.active_operations() == 0,
            "active data operations have not drained"
        );
        let _coord = self.coord.lock().await;
        self.maintenance_store.mark_orphans().await
    }
}

struct Running<'a>(&'a std::sync::atomic::AtomicBool);
impl Drop for Running<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
pub async fn run(app: Arc<App>) -> Result<()> {
    let mut local = tokio::time::interval(Duration::from_secs(config::seconds(
        &app.config.uploads.sweep_interval,
    )?));
    local.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {_=app.wake_gc.notified()=>{},_=local.tick()=>{}}
        #[cfg(feature = "fault-injection")]
        crate::faults::point("local-cleanup").await;
        if let Err(e) = app.cleanup().await {
            app.statistics.gc_failures.fetch_add(1, Ordering::Relaxed);
            tracing::error!(error=%e,"local cleanup failed; retaining recoverable state");
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
