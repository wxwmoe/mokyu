use super::{Area, CacheFile, CachePolicy, Disk, Reservation, Storage};
use anyhow::{Result, ensure};
use std::{path::PathBuf, sync::Arc};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use uuid::Uuid;

pub(super) struct File {
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
        let path = CacheFile::Raw
            .path(&storage.disk.root, id)
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
            disk: storage.disk.clone(),
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
                    .reserve_cache_space(size, self.policy, true)
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
        let size = data.len() as u64;
        ensure!(
            self.policy
                .capacity
                .is_none_or(|limit| self.bytes.saturating_add(size) <= limit),
            s3s::s3_error!(SlowDown, "raw request workspace is full")
        );
        {
            let _write = storage.cache_writes.lock().await;
            self.pending = Some(
                storage
                    .reserve_cache(size, self.policy)
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

    pub async fn publish(
        mut self,
        storage: Arc<Storage>,
        source: crate::model::Id,
        id: Uuid,
        charge: super::UploadCharge,
    ) -> Result<(Self, bool)> {
        // Finish publication and pin registration even when the request is cancelled.
        tokio::spawn(async move {
            let published = if let Some(pin) = self.publish_inner(&storage, id, charge).await? {
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
    ) -> Result<Option<super::UploadPin>> {
        let _write = storage.cache_writes.lock().await;
        let Some(reservation) = storage.reserve_cache(0, self.policy).await? else {
            return Ok(None);
        };
        drop(reservation);
        let file = self.file.as_mut().unwrap();
        file.flush().await?;
        file.sync_all().await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("raw-file-durable").await;
        let destination = CacheFile::Raw.path(&storage.disk.root, id);
        tokio::fs::hard_link(&self.path, &destination).await?;
        self.linked = Some(destination.clone());
        if !self.retained {
            tokio::fs::remove_file(&self.path).await?;
        }
        self.path = destination;
        self.linked = None;
        tokio::fs::File::open(self.path.parent().unwrap())
            .await?
            .sync_all()
            .await?;
        tokio::fs::File::open(storage.disk.root.join("raw"))
            .await?
            .sync_all()
            .await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("raw-file-published").await;
        let mut fifo = storage.fifo.lock().await;
        fifo.insert(id, self.bytes, CacheFile::Raw);
        let pins = fifo.entries[&id].pins.clone();
        pins.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.published = true;
        Ok(Some(super::UploadPin {
            _pin: Some(super::CachePin(pins)),
            _charge: Some(charge),
            size: self.bytes,
            compressed: false,
            remote: false,
            staging_storage_id: None,
            staging_object_key: id.to_string(),
        }))
    }

    pub async fn rewind(&mut self) -> Result<()> {
        self.file.as_mut().unwrap().rewind().await?;
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
