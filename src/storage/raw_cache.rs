use super::{
    Area, CacheFile, CachePin, CachePolicy, CacheWork, Reservation, Storage, StoreBackend,
    file_cache::Files,
    raw::{RawFile, verify_file},
};
use crate::model::raw::RawSource;
use anyhow::{Result, ensure};
use std::{
    path::PathBuf,
    sync::{Arc, atomic::Ordering},
};
use tokio::io::{AsyncSeekExt, AsyncWrite, AsyncWriteExt};
use uuid::Uuid;

pub(crate) struct Write {
    pub file: Option<tokio::fs::File>,
    path: PathBuf,
    ticket: Option<Reservation>,
    work: Option<CacheWork>,
    cold: bool,
    pub(crate) epoch: u64,
}

impl AsyncWrite for Write {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        data: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(self.get_mut().file.as_mut().unwrap()).poll_write(cx, data)
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(self.get_mut().file.as_mut().unwrap()).poll_flush(cx)
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(self.get_mut().file.as_mut().unwrap()).poll_shutdown(cx)
    }
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

pub(crate) async fn append(write: &mut Option<Write>, data: &[u8]) {
    if let Some(writer) = write {
        let result = tokio::time::timeout(std::time::Duration::from_millis(100), async {
            #[cfg(feature = "fault-injection")]
            crate::faults::point("raw-cache-stream").await;
            for part in data.chunks(256 * 1024) {
                writer.write_all(part).await?;
            }
            Ok::<_, std::io::Error>(())
        })
        .await;
        if !matches!(result, Ok(Ok(()))) {
            *write = None;
        }
    }
}

impl Storage {
    pub(crate) fn prepare_raw_file(
        self: &Arc<Self>,
        mut file: super::raw_request::File,
        id: Uuid,
        target: Arc<StoreBackend>,
    ) -> Option<tokio::sync::oneshot::Sender<()>> {
        let policy = self.local_cache(Some(target.profile.storage_id)).ok()?;
        if !policy.enabled {
            return None;
        }
        let work = self.cache_work(id, 1)?;
        let storage = self.clone();
        let (ready, published) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let _work = work;
            if !matches!(
                tokio::time::timeout(std::time::Duration::from_secs(5), published).await,
                Ok(Ok(()))
            ) {
                return;
            }
            let result = file
                .cache_in(&storage.file_cache(), id, policy, || {
                    ensure!(
                        Arc::ptr_eq(&storage.store_backend(target.profile.storage_id)?, &target),
                        "cache storage changed"
                    );
                    Ok(())
                })
                .await;
            if let Err(error) = result {
                tracing::debug!(%error, "raw request cache publication skipped");
            }
        });
        Some(ready)
    }

    pub(crate) fn prepare_raw_fill(
        self: &Arc<Self>,
        write: Write,
        id: Uuid,
        size: u64,
        target: Arc<StoreBackend>,
    ) -> tokio::sync::oneshot::Sender<()> {
        let storage = self.clone();
        let (ready, published) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            if !matches!(
                tokio::time::timeout(std::time::Duration::from_secs(5), published).await,
                Ok(Ok(()))
            ) {
                return;
            }
            let result = async {
                let _write = storage.cache_writes.lock().await;
                storage
                    .file_cache()
                    .publish_raw(write, id, size, || {
                        ensure!(
                            Arc::ptr_eq(
                                &storage.store_backend(target.profile.storage_id)?,
                                &target
                            ),
                            "cache storage changed"
                        );
                        Ok(())
                    })
                    .await
            }
            .await;
            if let Err(error) = result {
                tracing::debug!(%error, "processed raw cache publication skipped");
            }
        });
        ready
    }

    pub(super) fn raw_cache_lock(&self, id: Uuid) -> Arc<tokio::sync::Mutex<()>> {
        self.raw_misses.get(id)
    }

    pub(super) async fn cached_raw(&self, source: &RawSource, verify: bool) -> Option<RawFile> {
        let id = source.location_id;
        let (pin, file) = self.file_cache().pin(id).await?;
        if !matches!(file, CacheFile::Raw | CacheFile::PendingRaw) {
            return None;
        }
        let result = async {
            let path = file.path(&self.disk.root, id);
            ensure!(
                tokio::fs::symlink_metadata(&path).await?.is_file(),
                "raw cache was replaced"
            );
            let mut file = RawFile::Local(tokio::fs::File::open(path).await?);
            if let RawFile::Local(file) = &file {
                ensure!(
                    file.metadata().await?.len() == source.size as u64,
                    crate::content::codec::IntegrityError::Length
                );
            }
            if verify {
                let _cpu = self.cpu.acquire().await?;
                verify_file(&mut file, source, None).await?;
            }
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

    pub(crate) async fn begin_raw_cache(&self, source: &RawSource) -> Result<Option<Write>> {
        self.begin_raw_output(source.storage_id, source.location_id, source.size as u64)
            .await
    }

    pub(crate) async fn begin_raw_output(
        &self,
        storage: Uuid,
        id: Uuid,
        size: u64,
    ) -> Result<Option<Write>> {
        let policy = self.local_cache(Some(storage))?;
        if !policy.enabled || policy.capacity.is_some_and(|capacity| size > capacity) {
            return Ok(None);
        }
        let Some(work) = self.cache_work(id, 1) else {
            return Ok(None);
        };
        self.file_cache().begin_raw(id, size, policy, work).await
    }

    pub(crate) async fn finish_raw_cache(
        self: &Arc<Self>,
        write: Write,
        source: &RawSource,
        target: &Arc<StoreBackend>,
    ) -> Result<Option<RawFile>> {
        let storage = self.clone();
        let source = source.clone();
        let target = target.clone();
        // Publication finishes even if the requesting client disconnects.
        tokio::spawn(async move {
            let _write = storage.cache_writes.lock().await;
            if !Arc::ptr_eq(&storage.store_backend(source.storage_id)?, &target) {
                return Ok(None);
            }
            storage
                .file_cache()
                .publish_raw(write, source.location_id, source.size as u64, || {
                    ensure!(
                        Arc::ptr_eq(&storage.store_backend(source.storage_id)?, &target),
                        "cache storage changed"
                    );
                    Ok(())
                })
                .await
                .map(Some)
        })
        .await?
    }
}

impl Files<'_> {
    pub(crate) async fn begin_raw(
        &self,
        id: Uuid,
        size: u64,
        policy: CachePolicy,
        work: CacheWork,
    ) -> Result<Option<Write>> {
        let invalid = self
            .fifo
            .lock()
            .await
            .entries
            .get(&id)
            .filter(|entry| !entry.valid)
            .map(|entry| entry.pins.clone());
        if let Some(pins) = invalid {
            self.invalidate(id, Some(&pins)).await?;
        }
        let _write = self.cache_writes.lock().await;
        if self.fifo.lock().await.entries.contains_key(&id) {
            return Ok(None);
        }
        let path = CacheFile::Raw.path(self.root, id).with_extension("tmp");
        let Some(ticket) = self.reserve(size, policy, false, &path).await? else {
            return Ok(None);
        };
        let mut write = Write {
            file: None,
            path,
            ticket: Some(ticket),
            work: Some(work),
            cold: policy.cold,
            epoch: 0,
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

    // The owner holds cache_writes through its authority check and this publication.
    pub(crate) async fn publish_raw(
        &self,
        mut write: Write,
        id: Uuid,
        size: u64,
        check: impl Fn() -> Result<()> + Send + Sync,
    ) -> Result<RawFile> {
        let file = write.file.as_mut().unwrap();
        tokio::time::timeout(std::time::Duration::from_millis(100), async {
            file.flush().await?;
            file.rewind().await?;
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("raw-cache-written").await;
        check()?;
        ensure!(
            file.metadata().await?.len() == size,
            "raw cache length changed"
        );
        let path = CacheFile::Raw.path(self.root, id);
        tokio::fs::rename(&write.path, &path).await?;
        write.path = path;
        let mut fifo = self.fifo.lock().await;
        if write.cold {
            fifo.ghost.remove(&id);
        }
        fifo.insert(id, size, CacheFile::Raw);
        let pin = fifo.entries[&id].pins.clone();
        pin.fetch_add(1, Ordering::Relaxed);
        write.ticket.take().unwrap().commit();
        Ok(RawFile::Cached {
            file: write.file.take().unwrap(),
            _pin: CachePin(pin),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::local::{Disk, Usage};
    use std::{
        collections::{HashMap, HashSet},
        sync::Mutex,
    };
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
                upload_by_storage: HashMap::new(),
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
            cold: false,
            epoch: 0,
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
