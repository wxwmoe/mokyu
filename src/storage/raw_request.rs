use super::{Area, CacheFile, CachePolicy, Disk, Reservation, Storage};
use anyhow::{Result, ensure};
use std::{path::PathBuf, sync::Arc};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use uuid::Uuid;

pub(crate) struct File {
    pub file: Option<tokio::fs::File>,
    path: PathBuf,
    disk: Arc<Disk>,
    policy: CachePolicy,
    bytes: u64,
    pending: Option<Reservation>,
    published: bool,
    retained: bool,
    linked: Option<PathBuf>,
}

impl File {
    pub async fn create(storage: &Storage, id: Uuid, policy: CachePolicy) -> Result<Self> {
        Self::create_in(&storage.file_cache(), id, policy).await
    }

    pub(crate) async fn create_in(
        files: &super::file_cache::Files<'_>,
        id: Uuid,
        mut policy: CachePolicy,
    ) -> Result<Self> {
        // Required request storage may reclaim disposable caches even for maintenance work.
        policy.cold = false;
        let path = CacheFile::PendingRaw
            .path(files.root, id)
            .with_extension("partial");
        tokio::fs::create_dir_all(path.parent().unwrap())
            .await
            .map_err(receive_error)?;
        let mut options = tokio::fs::OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o644);
        let file = options.open(&path).await.map_err(receive_error)?;
        Ok(Self {
            file: Some(file),
            path,
            disk: files.disk.clone(),
            policy,
            bytes: 0,
            pending: None,
            published: false,
            retained: false,
            linked: None,
        })
    }

    pub async fn assemble(
        mut self,
        storage: Arc<Storage>,
        ranges: Vec<super::assembly::Range>,
        model: Arc<dyn crate::model::multipart::MultipartStore>,
        upload: Uuid,
        source: crate::model::Id,
        location: Uuid,
    ) -> Result<(Self, super::assembly::Prepared)> {
        // Complete checkpoints and ownership accounting even if the request disappears.
        tokio::spawn(async move {
            let size = ranges
                .iter()
                .try_fold(0u64, |total, range| total.checked_add(range.length as u64))
                .ok_or_else(|| anyhow::anyhow!("assembly size overflow"))?;
            ensure!(
                self.bytes == 0 && self.policy.capacity.is_none_or(|limit| size <= limit),
                s3s::s3_error!(SlowDown, "raw request workspace is full")
            );
            let reservation = {
                let _write = storage.cache_writes.lock().await;
                storage
                    .file_cache()
                    .reserve(size, self.policy, true, &self.path)
                    .await?
                    .ok_or_else(|| s3s::s3_error!(SlowDown, "raw request workspace is full"))?
            };
            reservation.commit();
            self.bytes = size;
            let result = super::assembly::prepare(
                &storage,
                &self.path,
                ranges,
                model.as_ref(),
                upload,
                source,
                storage.disk.floor,
            )
            .await;
            self.retained = match &result {
                Ok((_, _, retained)) => *retained,
                Err(_) => storage
                    .raw_store
                    .cached_checkpoint(location)
                    .await
                    .map_or(true, |source| source.is_some()),
            };
            if let Ok(metadata) = tokio::fs::metadata(&self.path).await {
                let actual = metadata.len();
                storage
                    .disk
                    .release(Area::Cache, size.saturating_sub(actual));
                self.bytes = actual;
            }
            let (file, prepared, _) = result?;
            self.file = Some(file);
            Ok::<_, anyhow::Error>((self, prepared))
        })
        .await?
    }

    pub async fn append(&mut self, storage: &Storage, data: &[u8]) -> Result<()> {
        self.append_in(&storage.file_cache(), data).await
    }

    pub(crate) async fn append_in(
        &mut self,
        files: &super::file_cache::Files<'_>,
        data: &[u8],
    ) -> Result<()> {
        let size = data.len() as u64;
        ensure!(
            self.policy
                .capacity
                .is_none_or(|limit| self.bytes.saturating_add(size) <= limit),
            s3s::s3_error!(SlowDown, "raw request workspace is full")
        );
        {
            let _write = files.cache_writes.lock().await;
            self.pending = Some(
                files
                    .reserve(size, self.policy, false, &self.path)
                    .await?
                    .ok_or_else(|| s3s::s3_error!(SlowDown, "raw request workspace is full"))?,
            );
        }
        let file = self.file.as_mut().unwrap();
        file.write_all(data).await.map_err(receive_error)?;
        file.flush().await.map_err(receive_error)?;
        self.pending.take().unwrap().commit();
        self.bytes += size;
        Ok(())
    }

    pub(crate) fn replay(&self) -> futures_util::stream::BoxStream<'_, Result<bytes::Bytes>> {
        use tokio::io::AsyncReadExt;
        Box::pin(async_stream::try_stream! {
            let mut file = tokio::fs::File::open(&self.path).await?;
            loop {
                let mut bytes = vec![0; 256 * 1024];
                let count = file.read(&mut bytes).await?;
                if count == 0 { break; }
                bytes.truncate(count);
                yield bytes::Bytes::from(bytes);
            }
        })
    }

    pub async fn publish(
        mut self,
        storage: Arc<Storage>,
        source: crate::model::Id,
        id: Uuid,
        charge: super::UploadCharge,
        buffered: bool,
    ) -> Result<(Self, bool)> {
        // Finish publication and pin registration even when the request is cancelled.
        tokio::spawn(async move {
            let published =
                if let Some(pin) = self.publish_inner(&storage, id, charge, buffered).await? {
                    storage
                        .raw_upload_pins
                        .lock()
                        .unwrap()
                        .insert(id, (source, pin));
                    true
                } else {
                    false
                };
            Ok::<_, anyhow::Error>((self, published))
        })
        .await?
    }

    async fn publish_inner(
        &mut self,
        storage: &Storage,
        id: Uuid,
        charge: super::UploadCharge,
        buffered: bool,
    ) -> Result<Option<super::UploadPin>> {
        let _write = storage.cache_writes.lock().await;
        let Some(reservation) = storage
            .file_cache()
            .reserve(0, self.policy, true, &self.path)
            .await?
        else {
            return Ok(None);
        };
        drop(reservation);
        self.publish_in(&storage.disk.root, id, buffered).await?;
        let mut fifo = storage.fifo.lock().await;
        fifo.insert(id, self.bytes, CacheFile::PendingRaw);
        let pins = fifo.entries[&id].pins.clone();
        pins.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.published = true;
        Ok(Some(super::UploadPin {
            memory: None,
            _pin: Some(super::CachePin(pins)),
            _charge: Some(charge),
            size: self.bytes,
            compressed: false,
            remote: false,
            staging_storage_id: None,
            staging_object_key: id.to_string(),
        }))
    }

    pub(crate) async fn publish_in(
        &mut self,
        root: &std::path::Path,
        id: Uuid,
        buffered: bool,
    ) -> Result<()> {
        let destination = CacheFile::PendingRaw.path(root, id);
        tokio::fs::create_dir_all(destination.parent().unwrap()).await?;
        let file = self.file.as_mut().unwrap();
        file.flush().await?;
        if !buffered {
            file.sync_all().await?;
            #[cfg(feature = "fault-injection")]
            crate::faults::point("raw-file-durable").await;
        } else {
            #[cfg(feature = "fault-injection")]
            crate::faults::point("raw-file-buffered").await;
        }
        tokio::fs::hard_link(&self.path, &destination).await?;
        self.linked = Some(destination.clone());
        if !self.retained {
            tokio::fs::remove_file(&self.path).await?;
        }
        self.path = destination;
        self.linked = None;
        if !buffered {
            tokio::fs::File::open(self.path.parent().unwrap())
                .await?
                .sync_all()
                .await?;
            tokio::fs::File::open(root.join("pending/raw"))
                .await?
                .sync_all()
                .await?;
            tokio::fs::File::open(root).await?.sync_all().await?;
        }
        #[cfg(feature = "fault-injection")]
        crate::faults::point("raw-file-published").await;
        self.published = true;
        Ok(())
    }

    pub async fn rewind(&mut self) -> Result<()> {
        self.file.as_mut().unwrap().rewind().await?;
        Ok(())
    }

    pub(crate) async fn cache_in(
        &mut self,
        files: &super::file_cache::Files<'_>,
        id: Uuid,
        policy: CachePolicy,
        check: impl Fn() -> Result<()> + Send + Sync,
    ) -> Result<()> {
        if self.published
            || self.retained
            || self.pending.is_some()
            || !policy.enabled
            || policy.capacity.is_some_and(|limit| self.bytes > limit)
        {
            return Ok(());
        }
        let _write = files.cache_writes.lock().await;
        check()?;
        {
            let fifo = files.fifo.lock().await;
            if fifo.entries.contains_key(&id)
                || fifo.entries.len() >= policy.max_entries
                || policy
                    .capacity
                    .is_some_and(|limit| files.disk.used()[1] > limit)
            {
                return Ok(());
            }
        }
        let destination = CacheFile::Raw.path(files.root, id);
        if !super::file_cache::link(&self.path, &destination, self.bytes).await? {
            return Ok(());
        }
        self.linked = Some(destination.clone());
        check()?;
        tokio::fs::remove_file(&self.path).await?;
        self.path = destination;
        self.linked = None;
        let mut fifo = files.fifo.lock().await;
        if policy.cold {
            fifo.ghost.remove(&id);
        }
        fifo.insert(id, self.bytes, CacheFile::Raw);
        self.published = true;
        Ok(())
    }
}

fn receive_error(error: std::io::Error) -> anyhow::Error {
    if matches!(
        error.kind(),
        std::io::ErrorKind::StorageFull | std::io::ErrorKind::QuotaExceeded
    ) {
        anyhow::Error::new(s3s::s3_error!(SlowDown, "raw request workspace is full")).context(error)
    } else {
        error.into()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn resource_exhaustion_is_retryable_but_io_failure_is_not_hidden() {
        use std::io::{Error, ErrorKind};
        for value in [ErrorKind::StorageFull, ErrorKind::QuotaExceeded] {
            let error = super::receive_error(Error::from(value));
            assert_eq!(
                error.downcast_ref::<s3s::S3Error>().unwrap().code(),
                &s3s::S3ErrorCode::SlowDown
            );
        }
        let error = super::receive_error(Error::from(ErrorKind::PermissionDenied));
        assert!(error.downcast_ref::<s3s::S3Error>().is_none());
        assert_eq!(
            error.downcast_ref::<Error>().unwrap().kind(),
            ErrorKind::PermissionDenied
        );
    }
}

impl Drop for File {
    fn drop(&mut self) {
        if self.published || self.retained {
            return;
        }
        let file = self.file.take();
        let pending = self.pending.take();
        let paths = std::iter::once(self.path.clone())
            .chain(self.linked.take())
            .collect::<Vec<_>>();
        let disk = self.disk.clone();
        let bytes = self.bytes;
        // Keep every written or in-flight byte charged until cancellation cleanup finishes.
        tokio::spawn(async move {
            drop(file);
            let mut failed = false;
            for path in paths {
                match tokio::fs::remove_file(path).await {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => {
                        failed = true;
                        tracing::warn!(%error,"raw request cleanup failed; retaining disk charge");
                    }
                }
            }
            if failed {
                if let Some(pending) = pending {
                    pending.commit();
                }
            } else {
                disk.release(Area::Cache, bytes);
            }
        });
    }
}
