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
        })
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
        let file = self.file.take();
        let pending = self.pending.take();
        let path = self.path.clone();
        let disk = self.disk.clone();
        let bytes = self.bytes;
        // Keep every written or in-flight byte charged until cancellation cleanup finishes.
        tokio::spawn(async move {
            drop(file);
            match tokio::fs::remove_file(path).await {
                Ok(()) => disk.release(Area::Cache, bytes),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    disk.release(Area::Cache, bytes)
                }
                Err(error) => {
                    if let Some(pending) = pending {
                        pending.commit();
                    }
                    tracing::warn!(%error, "raw request cleanup failed; retaining disk charge");
                }
            }
        });
    }
}
