use super::{
    Area, CacheFile, CachePin, CacheWork, Reservation, Storage, StoreBackend,
    raw::{RawFile, verify_file},
};
use crate::model::raw::RawSource;
use anyhow::{Result, ensure};
use std::{
    path::PathBuf,
    sync::{Arc, atomic::Ordering},
};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use uuid::Uuid;

pub(super) struct Write {
    pub file: Option<tokio::fs::File>,
    path: PathBuf,
    ticket: Option<Reservation>,
    work: Option<CacheWork>,
}

impl Drop for Write {
    fn drop(&mut self) {
        let Some(ticket) = self.ticket.take() else {
            return;
        };
        let path = self.path.clone();
        let file = self.file.take();
        let work = self.work.take();
        // Cancellation must retain the reservation until the temporary file is gone.
        tokio::spawn(async move {
            drop(file);
            if let Err(error) = tokio::fs::remove_file(&path).await
                && error.kind() != std::io::ErrorKind::NotFound
            {
                ticket.commit();
                tracing::warn!(error=%error, "raw cache cleanup failed; retaining disk charge");
            }
            drop(work);
        });
    }
}

impl Storage {
    pub(super) fn raw_cache_lock(&self, id: Uuid) -> Arc<tokio::sync::Mutex<()>> {
        self.raw_misses.get(id)
    }

    pub(super) async fn cached_raw(&self, source: &RawSource) -> Option<RawFile> {
        let id = source.location_id;
        let pin = {
            let mut fifo = self.fifo.lock().await;
            let entry = fifo.entries.get_mut(&id)?;
            if entry.file != CacheFile::Raw
                || !entry.valid
                || entry.pins.load(Ordering::Relaxed) == usize::MAX
            {
                return None;
            }
            entry.pins.fetch_add(1, Ordering::Relaxed);
            entry.frequency = entry.frequency.saturating_add(1).min(3);
            CachePin(entry.pins.clone())
        };
        let result = async {
            let path = CacheFile::Raw.path(&self.disk.root, id);
            ensure!(
                tokio::fs::symlink_metadata(&path).await?.is_file(),
                "raw cache was replaced"
            );
            let mut file = RawFile::Local(tokio::fs::File::open(path).await?);
            let _cpu = self.cpu.acquire().await?;
            verify_file(&mut file, source, None).await?;
            file.rewind().await?;
            let RawFile::Local(file) = file else {
                unreachable!()
            };
            Ok::<_, anyhow::Error>(file)
        }
        .await;
        match result {
            Ok(file) => {
                #[cfg(feature = "fault-injection")]
                crate::faults::point("raw-cache-hit").await;
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
                Some(RawFile::Cached { file, _pin: pin })
            }
            Err(error) => {
                let missing = error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound);
                let counter = if missing {
                    &self.diagnostics.cache_missing
                } else {
                    &self.diagnostics.cache_invalid
                };
                counter.fetch_add(1, Ordering::Relaxed);
                self.diagnostics
                    .cache_fallback
                    .fetch_add(1, Ordering::Relaxed);
                let mut fifo = self.fifo.lock().await;
                if let Some(entry) = fifo.entries.get_mut(&id)
                    && Arc::ptr_eq(&entry.pins, &pin.0)
                {
                    entry.valid = false;
                    if missing && entry.pins.load(Ordering::Relaxed) == 1 {
                        let entry = fifo.forget(id).unwrap();
                        self.disk.release(Area::Cache, entry.size);
                    }
                }
                None
            }
        }
    }

    pub(super) async fn begin_raw_cache(&self, source: &RawSource) -> Result<Option<Write>> {
        let id = source.location_id;
        let policy = self.local_cache(Some(source.storage_id))?;
        let size = source.size as u64;
        if !policy.enabled || policy.capacity.is_some_and(|capacity| size > capacity) {
            return Ok(None);
        }
        let Some(work) = self.cache_work(id, 1) else {
            return Ok(None);
        };
        let invalid = self
            .fifo
            .lock()
            .await
            .entries
            .get(&id)
            .filter(|entry| !entry.valid)
            .map(|entry| entry.pins.clone());
        if let Some(pins) = invalid {
            self.invalidate_entry(id, Some(&pins)).await?;
        }
        let _write = self.cache_writes.lock().await;
        if self.fifo.lock().await.entries.contains_key(&id) {
            return Ok(None);
        }
        let Some(ticket) = self.reserve_cache(size, policy).await? else {
            return Ok(None);
        };
        let path = CacheFile::Raw
            .path(&self.disk.root, id)
            .with_extension("tmp");
        let mut write = Write {
            file: None,
            path,
            ticket: Some(ticket),
            work: Some(work),
        };
        tokio::fs::create_dir_all(write.path.parent().unwrap()).await?;
        write.file = Some(
            tokio::fs::OpenOptions::new()
                .write(true)
                .read(true)
                .create_new(true)
                .mode(0o644)
                .open(&write.path)
                .await?,
        );
        Ok(Some(write))
    }

    pub(super) async fn finish_raw_cache(
        self: &Arc<Self>,
        mut write: Write,
        source: &RawSource,
        target: &Arc<StoreBackend>,
    ) -> Result<Option<RawFile>> {
        let storage = self.clone();
        let source = source.clone();
        let target = target.clone();
        // Publication finishes even if the requesting client disconnects.
        tokio::spawn(async move {
            let id = source.location_id;
            let _write = storage.cache_writes.lock().await;
            if !Arc::ptr_eq(&storage.store_backend(source.storage_id)?, &target) {
                return Ok(None);
            }
            let file = write.file.as_mut().unwrap();
            file.flush().await?;
            file.rewind().await?;
            #[cfg(feature = "fault-injection")]
            crate::faults::point("raw-cache-written").await;
            let path = CacheFile::Raw.path(&storage.disk.root, id);
            tokio::fs::rename(&write.path, &path).await?;
            write.path = path;
            let mut fifo = storage.fifo.lock().await;
            fifo.insert(id, source.size as u64, CacheFile::Raw);
            let pin = fifo.entries[&id].pins.clone();
            pin.fetch_add(1, Ordering::Relaxed);
            write.ticket.take().unwrap().commit();
            Ok::<_, anyhow::Error>(Some(RawFile::Cached {
                file: write.file.take().unwrap(),
                _pin: CachePin(pin),
            }))
        })
        .await?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::local::{Disk, Usage};
    use std::{collections::HashSet, sync::Mutex};
    use tokio::sync::Semaphore;

    #[tokio::test]
    async fn cancelled_raw_fill_releases_space_only_after_removing_the_file() -> Result<()> {
        let root = std::env::temp_dir().join(format!("mokyu-raw-cache-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&root).await?;
        let disk = Arc::new(Disk {
            root: root.clone(),
            limits: [None, Some(1024 * 1024), None],
            floor: 0,
            spaces: Mutex::default(),
            upload_limit: None,
            usage: Mutex::new(Usage {
                used: [0; 3],
                upload: 0,
            }),
            io: Arc::new(Semaphore::new(1)),
            pending: Mutex::new(HashSet::new()),
        });
        let ticket = disk.reserve(Area::Cache, 65536)?;
        let path = root.join("cancelled.tmp");
        let mut file = tokio::fs::File::create(&path).await?;
        file.write_all(&[3; 4096]).await?;
        let write = Write {
            file: Some(file),
            path: path.clone(),
            ticket: Some(ticket),
            work: None,
        };
        assert_eq!(disk.used()[1], 65536);
        drop(write);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while disk.used()[1] != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert!(!path.exists());
        tokio::fs::remove_dir(root).await?;
        Ok(())
    }
}
