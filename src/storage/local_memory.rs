use super::{Storage, local::Space, memory, raw};
use crate::model::{Id, raw::RawSource};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use std::sync::{Arc, atomic::Ordering};
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

pub(crate) struct Writer {
    pub buffer: memory::Buffer,
    pub space: Space,
    pub external: bool,
    _global: super::resource_gate::Permit,
    _permit: super::StorePermit,
}

pub(super) struct Body {
    source: Id,
    storage: Uuid,
    bytes: Bytes,
    _space: Space,
}

impl Storage {
    pub(crate) fn local_write_gate(&self, storage: Uuid) -> Option<Arc<tokio::sync::RwLock<()>>> {
        let target = self.store_backend(storage).ok()?;
        (target.is_local && target.profile.write_durability.as_deref() == Some("volatile"))
            .then(|| target.local_write_policy.clone())
    }

    pub(crate) fn needs_local_drain(&self) -> bool {
        !self.local_memory.lock().unwrap().is_empty()
            || self.targets.read().unwrap().values().any(|target| {
                target.is_local && target.profile.write_durability.as_deref() == Some("volatile")
            })
    }

    pub(crate) async fn reserve_local_body(
        &self,
        storage: Uuid,
        declared: Option<i64>,
    ) -> Result<Option<Writer>> {
        let target = self.store_backend(storage)?;
        let profile = &target.profile;
        if self.cluster_mode
            || !target.is_local
            || target.cdc_enabled
            || target.deduplication_enabled
            || profile.write_durability.as_deref() != Some("volatile")
        {
            return Ok(None);
        }
        let Some(size) = declared
            .and_then(|n| usize::try_from(n).ok())
            .filter(|n| *n <= 8 * 1024 * 1024 && self.write_memory.can_fit(*n))
        else {
            crate::upload::cache::overflow(false)?;
            return Ok(None);
        };
        let buffer = if self.write_overdue(storage, profile) {
            None
        } else {
            self.write_memory.buffer(size)
        };
        let Some(buffer) = buffer else {
            self.expedite_writes(storage);
            crate::upload::cache::overflow(false)?;
            return Ok(None);
        };
        self.verify_raw_target(&target).await?;
        let global = self.writes.acquire().await?;
        let permit = target.writes.acquire().await?;
        let parent = target
            .raw_root
            .as_ref()
            .and_then(|path| path.parent())
            .context("raw root is missing")?;
        let space = Space::claim(
            parent,
            crate::config::bytes(
                profile
                    .min_free_space
                    .as_deref()
                    .context("local storage floor is missing")?,
            )?,
            size as u64,
            &self.disk.space_for(parent)?,
        )
        .map_err(|_| {
            crate::upload::cache::REJECTIONS.fetch_add(1, Ordering::Relaxed);
            s3s::s3_error!(SlowDown, "local storage has insufficient free space")
        })?;
        self.buffered_writes.store(true, Ordering::Release);
        Ok(Some(Writer {
            buffer,
            space,
            external: false,
            _global: global,
            _permit: permit,
        }))
    }

    pub(crate) fn remember_local_body(
        &self,
        location: Uuid,
        source: Id,
        storage: Uuid,
        bytes: Bytes,
        space: Space,
    ) {
        self.local_memory.lock().unwrap().insert(
            location,
            Body {
                source,
                storage,
                bytes,
                _space: space,
            },
        );
    }

    pub(crate) fn forget_local_body(&self, location: Uuid) {
        self.local_memory.lock().unwrap().remove(&location);
    }

    pub(crate) fn local_sources(&self, storage: Uuid) -> Vec<(Uuid, Id)> {
        self.local_memory
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, b)| b.storage == storage)
            .map(|(id, b)| (*id, b.source))
            .collect()
    }

    pub(super) fn local_bytes(&self, location: Uuid) -> Option<Bytes> {
        self.local_memory
            .lock()
            .unwrap()
            .get(&location)
            .map(|b| b.bytes.clone())
    }

    pub(super) async fn persist_local_body(&self, source: &RawSource, data: Bytes) -> Result<()> {
        let target = self.store_backend(source.storage_id)?;
        self.verify_raw_target(&target).await?;
        let _global = self.writes.acquire().await?;
        let _permit = target.writes.acquire().await?;
        let root = target
            .raw_root
            .as_ref()
            .context("local raw root is missing")?;
        let dest = root.join(super::chunk_name(source.location_id));
        let parent = dest.parent().unwrap();
        raw::directory(root).await?;
        raw::directory(parent).await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("local-memory-before-file").await;
        if tokio::fs::try_exists(&dest).await? {
            ensure!(
                tokio::fs::symlink_metadata(&dest).await?.is_file(),
                "invalid raw file"
            );
            let file = tokio::fs::File::open(&dest).await?;
            file.sync_all().await?;
            raw::verify_file(&mut raw::RawFile::Local(file), source, None).await?;
        } else {
            let temp = dest.with_extension("part");
            // The immutable source lock serializes retries after a cancelled flush.
            match tokio::fs::remove_file(&temp).await {
                Ok(()) => (),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
                Err(e) => return Err(e.into()),
            }
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp)
                .await?;
            file.write_all(&data).await?;
            file.flush().await?;
            file.sync_all().await?;
            tokio::fs::hard_link(&temp, &dest).await?;
            tokio::fs::remove_file(temp).await?;
            self.backend_puts.fetch_add(1, Ordering::Relaxed);
            self.backend_write_bytes
                .fetch_add(data.len() as u64, Ordering::Relaxed);
        }
        raw::sync_directory(parent).await?;
        raw::sync_directory(root).await?;
        raw::sync_directory(root.parent().context("raw root has no parent")?).await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("local-memory-persisted").await;
        Ok(())
    }
}
