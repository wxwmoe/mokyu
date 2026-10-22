use super::{CachePin, Storage, chunk_name, raw::sync_directory, read_bounded};
use crate::content::codec::{Chunk, IntegrityError, MAX};
use anyhow::{Context, Result, ensure};
use std::sync::atomic::Ordering;

impl Storage {
    pub(crate) async fn sync_chunk(
        self: &std::sync::Arc<Self>,
        chunk: &Chunk,
        verify: bool,
    ) -> Result<bool> {
        let target = self.store_backend(chunk.storage_id.context("chunk has no storage")?)?;
        if let Some(data) = self.chunk_memory(chunk.location_id) {
            let mut policy = self.local_cache(chunk.storage_id)?;
            policy.upload_capacity = None;
            let storage = self.clone();
            let storage_id = chunk.storage_id.unwrap();
            let location = chunk.location_id;
            // Publication and pin transfer must finish even if the stronger request is cancelled.
            let persisted = tokio::spawn(async move {
                let Some(pin) = storage
                    .stage_upload_owned(storage_id, location, data, false, policy, false)
                    .await?
                else {
                    return Ok::<_, anyhow::Error>(false);
                };
                storage.hold_upload(location, pin);
                Ok(true)
            })
            .await??;
            if !persisted {
                return Ok(false);
            }
        }
        let pending = self.physical.pending_source(chunk.id).await?;
        let mut cache_pin = None;
        let (root, path, cached) = if let Some((_, key)) = pending {
            let id = super::parse_upload_cache_name(&key).context("invalid pending chunk key")?;
            let fifo = self.fifo.lock().await;
            if let Some(entry) = fifo.entries.get(&id) {
                ensure!(
                    entry.pins.load(Ordering::Relaxed) != usize::MAX,
                    "cache removal in progress"
                );
                entry.pins.fetch_add(1, Ordering::Relaxed);
                cache_pin = Some(CachePin(entry.pins.clone()));
            }
            (
                self.disk.root.join("chunks"),
                self.cache_path(id, key.ends_with(".zst")),
                true,
            )
        } else if !target.is_local {
            // Final publication can replace the cached source while sync is waiting.
            self.physical.source(chunk.id, false).await?;
            return Ok(true);
        } else {
            let root = target
                .raw_root
                .as_ref()
                .context("buffered chunk has no local root")?
                .parent()
                .context("local root is missing")?
                .to_path_buf();
            let path = root.join(chunk_name(chunk.location_id));
            (root, path, false)
        };
        let _permit = target.writes.acquire().await?;
        let file = tokio::fs::File::open(&path).await?;
        let length = file.metadata().await?.len();
        ensure!(
            length
                == if cached {
                    chunk.raw_size as u64
                } else {
                    chunk.stored_size.context("encoded size missing")? as u64
                },
            IntegrityError::Length
        );
        if verify {
            let bytes = read_bounded(&path, MAX + 16).await?;
            if cached {
                ensure!(
                    blake3::hash(&bytes).as_bytes() == chunk.hash.as_slice(),
                    IntegrityError::Hash
                );
            } else {
                self.ensure_key_loaded(chunk.block_key_id).await?;
                self.decode(chunk, bytes).await?;
            }
        }
        #[cfg(feature = "fault-injection")]
        crate::faults::point("buffered-chunk-before-sync").await;
        file.sync_all().await?;
        sync_directory(path.parent().unwrap()).await?;
        sync_directory(&root).await?;
        sync_directory(root.parent().context("local root has no parent")?).await?;
        drop(cache_pin);
        Ok(true)
    }
}
