use crate::model::Id;
use crate::model::task::Execution;
use crate::{
    app::App,
    config,
    content::codec::Chunk,
    model::cache::{CacheBatch, CacheStage, EncodingPolicy},
    storage::ReadContext,
};
use anyhow::{Context, Result};
use bytes::Bytes;
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    sync::atomic::{AtomicU64, Ordering},
};
use uuid::Uuid;

tokio::task_local! { pub static FLUSH: bool; }
pub static FALLBACKS: AtomicU64 = AtomicU64::new(0);
pub static REJECTIONS: AtomicU64 = AtomicU64::new(0);

pub(crate) fn overflow(reject: bool) -> Result<()> {
    if reject {
        REJECTIONS.fetch_add(1, Ordering::Relaxed);
        anyhow::bail!(s3s::s3_error!(
            SlowDown,
            "upload cache is full; retry later"
        ));
    }
    FALLBACKS.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

impl App {
    pub fn storage_writable(&self) -> Result<()> {
        if FLUSH.try_with(|f| *f).unwrap_or(false) {
            Ok(())
        } else {
            self.writable()
        }
    }
    pub async fn stage_chunk(
        &self,
        stream: Id,
        offset: i64,
        c: &Chunk,
        data: Bytes,
        compressed: bool,
        new: bool,
        profile: &crate::storage::profile::StorageProfile,
    ) -> Result<bool> {
        if !profile.upload_cache {
            return Ok(false);
        }
        if self.upload_cache.bypass(stream).await? {
            return Ok(false);
        }
        let buffered =
            new && self.role == "single" && profile.write_durability.as_deref() == Some("buffered");
        let staged = self
            .storage
            .stage_upload(c, data, compressed, buffered)
            .await;
        let pin = match staged {
            Ok(Some(pin)) => pin,
            other => {
                if let Err(e) = other {
                    tracing::warn!(error=%e,"upload cache staging failed");
                }
                overflow(profile.upload_cache_overflow == "reject")?;
                self.upload_cache.disable(stream).await?;
                return Ok(false);
            }
        };
        let _coord = self.coord.lock().await;
        self.writable()?;
        let storage = pin
            .staging_storage_id
            .or(c.storage_id)
            .context("chunk has no staging storage")?;
        let key = if pin.staging_object_key.is_empty() {
            format!(
                "{}.{}",
                c.location_id,
                if pin.compressed { "zst" } else { "raw" }
            )
        } else {
            pin.staging_object_key.clone()
        };
        let size = pin.size as i64;
        let timeout = config::seconds(&profile.pack_upload_flush_timeout)?;
        let encoding = new
            .then(|| {
                serde_json::to_value(EncodingPolicy {
                    compression: profile
                        .compression_config(&self.config.instance.compression_idle_timeout),
                    pack_enabled: profile.pack_enabled,
                    pack_max_size: profile.pack_max_size.clone(),
                    pack_flush_timeout: profile.pack_upload_flush_timeout.clone(),
                    algorithm: c.algorithm.clone(),
                    block_key_id: c.block_key_id,
                })
            })
            .transpose()?;
        let local = !pin.remote;
        let local_id = if local {
            Some(
                crate::storage::parse_upload_cache_name(&key)
                    .context("invalid upload cache key")?,
            )
        } else {
            None
        };
        // Protect before starting the transaction; cancellation may retain an extra pin until reconciliation.
        if let Some(id) = local_id {
            self.storage.hold_upload(id, pin);
        }
        if !self
            .upload_cache
            .stage(CacheStage {
                chunk: c,
                stream,
                offset,
                new,
                buffered,
                size,
                storage,
                key: &key,
                timeout,
                encoding: encoding.as_ref(),
                node: self
                    .node
                    .get()
                    .map(|node| (node, self.role.as_str(), &self.secrets.credential_key)),
            })
            .await?
        {
            if let Some(id) = local_id {
                self.storage.release_upload(id);
            }
            return Ok(false);
        }
        drop(_coord);
        if buffered {
            self.storage.buffered_writes.store(true, Ordering::Release);
            self.storage.wake_sync.notify_one();
        }
        if self.role == "single" {
            self.wake_tasks.notify_one();
        }
        self.seal_uploads(stream, false).await?;
        Ok(true)
    }
    pub async fn seal_uploads(&self, stream: Id, eof: bool) -> Result<()> {
        if self
            .upload_cache
            .seal(stream, eof || !self.pack_creation_allowed())
            .await?
        {
            self.wake_tasks.notify_waiters();
        }
        Ok(())
    }
    pub async fn upload_cache_status(&self) -> Result<Value> {
        let (total, limit, used) = self.storage.disk.upload_capacity()?;
        let (pending, pins) = self.upload_cache.status().await?;
        Ok(
            json!({"enabled":self.storage_profiles.enabled_profiles().await?.iter().any(|profile| profile.upload_cache),"configured_size":self.config.instance.upload_cache_size,"effective_cache_bytes":total,"effective_upload_bytes":limit,"reserved_bytes":used,"fallbacks":FALLBACKS.load(Ordering::Relaxed),"rejections":REJECTIONS.load(Ordering::Relaxed),"pending":pending,"pins":pins,"pins_limit":100,"raw_processing":self.raw.processing_status().await?,"buffered_files":self.raw.buffered_status().await?,"buffered_chunks":self.physical.buffered_status().await?,"raw_uploads":self.raw.upload_status().await?}),
        )
    }
    pub async fn cache_flush_start(&self) -> Result<Value> {
        let _coord = self.coord.lock().await;
        self.ensure_mode()?;
        let id = self.upload_cache.flush(None).await?;
        self.wake_tasks.notify_one();
        Ok(json!({"task_id":id}))
    }
    pub async fn enqueue_upload(&self) -> Result<bool> {
        if self.maintenance.load(Ordering::Acquire) {
            return Ok(false);
        }
        let _coord = self.coord.lock().await;
        if self.ensure_mode().is_err() || self.maintenance.load(Ordering::Acquire) {
            return Ok(false);
        }
        self.upload_cache.enqueue().await
    }
    pub async fn upload_batch(&self, task: Execution, detail: Value, flush: bool) -> Result<bool> {
        if !flush && self.maintenance.load(Ordering::Acquire) {
            self.upload_cache.pause(task).await?;
            return Ok(false);
        }
        #[cfg(feature = "fault-injection")]
        crate::faults::point("upload-before-prepare").await;
        let selection = self.coord.lock().await;
        self.storage_writable()?;
        let batch = self.upload_cache.batch(task, &detail, flush).await?;
        match batch {
            CacheBatch::Wait => {
                drop(selection);
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                return Ok(true);
            }
            CacheBatch::Finished => {
                drop(selection);
                if flush {
                    let before = serde_json::from_value(detail["upper_time"].clone())?;
                    if self.role == "single"
                        && self
                            .sync_raw_batch(Some(before), false, &mut crate::model::Id::default())
                            .await?
                    {
                        return Ok(true);
                    }
                    if self.role == "single"
                        && self
                            .sync_chunk_batch(Some(before), false, &mut crate::model::Id::default())
                            .await?
                    {
                        return Ok(true);
                    }
                    let owner = if detail["coordinator"] == true {
                        None
                    } else {
                        self.node.get().map(|n| n.node_id)
                    };
                    if self.raw.uploads_before(before, owner).await? {
                        if detail["coordinator"] != true {
                            crate::worker::raw_upload::tick(self, Some(before)).await?;
                            self.transfer_remote_part(None).await?;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        return Ok(true);
                    }
                }
                return self.finish_upload_task(task).await;
            }
            CacheBatch::Release(first) => {
                drop(selection);
                self.release_pending(&[first], false).await?;
                return if flush {
                    Ok(true)
                } else {
                    self.finish_upload_task(task).await
                };
            }
            CacheBatch::Rewrite { stream, pack, ids } => {
                let _pin = stream.map(|id| self.pin(id));
                drop(selection);
                #[cfg(feature = "fault-injection")]
                crate::faults::point("upload-before-work").await;
                self.rewrite_pack(task, pack, false).await?;
                self.release_pending(&ids, true).await?;
            }
            CacheBatch::Encode {
                stream,
                expired,
                rows,
                encoding,
            } => {
                let _pin = stream.map(|id| self.pin(id));
                drop(selection);
                #[cfg(feature = "fault-injection")]
                crate::faults::point("upload-before-work").await;
                let metadata = if let Some(stream) = stream {
                    self.upload_cache.metadata(stream).await?
                } else {
                    None
                };
                let content_type = metadata.as_ref().and_then(|m| m.0["content_type"].as_str());
                let key = metadata.as_ref().map_or("", |m| m.1.as_str());
                let outputs = self
                    .pack_output(
                        task,
                        &rows,
                        &ReadContext::default(),
                        content_type,
                        key,
                        self.pack_creation_allowed() && !flush && !expired,
                        encoding.as_ref(),
                    )
                    .await?;
                self.commit_outputs(task, &rows, &outputs, false).await?;
                #[cfg(feature = "fault-injection")]
                crate::faults::point("upload-before-release").await;
                let ids: Vec<Id> = rows.iter().map(|r| r.chunk.id).collect();
                self.release_pending(&ids, false).await?;
            }
        }
        self.upload_cache.processed(task).await?;
        if flush {
            Ok(true)
        } else {
            self.finish_upload_task(task).await
        }
    }
    async fn finish_upload_task(&self, id: Execution) -> Result<bool> {
        self.upload_cache.finish(id).await?;
        Ok(false)
    }
    pub async fn release_pending(&self, ids: &[Id], existing_pack: bool) -> Result<()> {
        let _coord = self.coord.lock().await;
        let released = self.upload_cache.release(ids, existing_pack).await?;
        for key in released
            .keys
            .iter()
            .filter_map(|name| crate::storage::parse_upload_cache_name(name))
        {
            self.storage.release_upload(key);
        }
        for (storage_id, object_key) in released.staged {
            if let Err(error) = self.storage.delete_staged(storage_id, &object_key).await {
                tracing::warn!(%storage_id, %object_key, error=%error, "shared staging object cleanup failed");
            }
        }
        Ok(())
    }

    pub async fn reconcile_upload_pins(&self, storage_id: Option<Uuid>) -> Result<()> {
        for keys in self.storage.upload_pin_ids().chunks(128) {
            let _coord = self.coord.lock().await;
            let names: Vec<String> = keys
                .iter()
                .flat_map(|key| [format!("{key}.raw"), format!("{key}.zst")])
                .collect();
            let owned: HashSet<String> = self
                .upload_cache
                .pins(&names, storage_id)
                .await?
                .into_iter()
                .collect();
            let keep: HashSet<Uuid> = owned
                .iter()
                .filter_map(|name| crate::storage::parse_upload_cache_name(name))
                .collect();
            for key in keys {
                if !keep.contains(key)
                    && (storage_id.is_none()
                        || owned.contains(&format!("{key}.raw"))
                        || owned.contains(&format!("{key}.zst")))
                {
                    self.storage.release_upload(*key);
                }
            }
        }
        let dead = self.upload_cache.unused(storage_id).await?;
        self.release_pending(&dead, false).await?;
        Ok(())
    }
}
