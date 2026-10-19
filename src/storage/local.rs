use super::cache::UploadCharge;
use crate::content::codec::MAX;
use anyhow::{Context, Result, ensure};
use std::{
    collections::HashSet,
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokio::{io::AsyncReadExt, sync::Semaphore};
use uuid::Uuid;

#[derive(Clone, Copy)]
pub(crate) enum Area {
    Multipart = 0,
    Cache = 1,
    Thumbnails = 2,
}
pub async fn read_bounded(path: impl AsRef<std::path::Path>, max: usize) -> Result<Vec<u8>> {
    let f = tokio::fs::File::open(path).await?;
    let size = f.metadata().await?.len();
    ensure!(size <= max as u64, "local file exceeds bound");
    let mut bytes = Vec::with_capacity(size as usize);
    f.take(max as u64 + 1).read_to_end(&mut bytes).await?;
    ensure!(bytes.len() <= max, "local file exceeds bound");
    Ok(bytes)
}
pub(crate) struct Usage {
    pub(crate) used: [u64; 3],
    pub(crate) reserved: u64,
    pub(crate) upload: u64,
}
pub(crate) struct Disk {
    pub root: PathBuf,
    pub(crate) limits: [Option<u64>; 3],
    pub(crate) floor: u64,
    pub(crate) upload_limit: Option<u64>,
    pub(crate) usage: Mutex<Usage>,
    pub(crate) io: Arc<Semaphore>,
    pub(crate) pending: Mutex<HashSet<Uuid>>,
}
pub(crate) struct Fragment {
    pub id: Uuid,
    pub(crate) disk: Arc<Disk>,
}
impl Drop for Fragment {
    fn drop(&mut self) {
        self.disk.pending.lock().unwrap().remove(&self.id);
    }
}
pub(crate) struct Reservation {
    pub(crate) disk: Arc<Disk>,
    pub(crate) area: Area,
    pub(crate) bytes: u64,
    pub(crate) committed: bool,
}
impl Reservation {
    pub(crate) fn commit(mut self) {
        self.disk.usage.lock().unwrap().reserved -= self.bytes;
        self.committed = true;
    }
    fn commit_written(mut self, bytes: u64) {
        let mut usage = self.disk.usage.lock().unwrap();
        usage.used[self.area as usize] -= self.bytes.saturating_sub(bytes);
        usage.reserved -= self.bytes;
        self.committed = true;
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.committed {
            let mut u = self.disk.usage.lock().unwrap();
            u.used[self.area as usize] -= self.bytes;
            u.reserved -= self.bytes;
        }
    }
}
impl Disk {
    fn cache_capacity(&self, usage: &Usage) -> Result<u64> {
        let capacity = usage.used[1].saturating_add(
            fs2::available_space(&self.root)?
                .saturating_sub(self.floor)
                .saturating_sub(usage.reserved),
        );
        Ok(self.limits[1].map_or(capacity, |limit| limit.min(capacity)))
    }
    pub(crate) fn upload_capacity(&self) -> Result<(u64, u64, u64)> {
        let usage = self.usage.lock().unwrap();
        let total = self.cache_capacity(&usage)?;
        Ok((
            total,
            self.upload_limit.unwrap_or(total / 5).min(total),
            usage.upload,
        ))
    }
    pub(crate) fn claim_upload_with_limit(
        self: &Arc<Self>,
        size: u64,
        configured_limit: Option<u64>,
    ) -> Result<UploadCharge> {
        let mut usage = self.usage.lock().unwrap();
        let total = self.cache_capacity(&usage)?;
        let limit = configured_limit
            .or(self.upload_limit)
            .unwrap_or(total / 5)
            .min(total);
        ensure!(
            usage.upload.saturating_add(size) <= limit,
            "upload cache quota exhausted"
        );
        usage.upload += size;
        Ok(UploadCharge {
            disk: self.clone(),
            size,
        })
    }
    pub(crate) fn reserve(self: &Arc<Self>, area: Area, bytes: u64) -> Result<Reservation> {
        let mut usage = self.usage.lock().unwrap();
        let next = usage.used[area as usize]
            .checked_add(bytes)
            .context("disk accounting overflow")?;
        ensure!(
            self.limits[area as usize].is_none_or(|n| next <= n),
            "local storage quota exhausted"
        );
        let free = fs2::available_space(&self.root)?;
        ensure!(
            free.saturating_sub(usage.reserved.saturating_add(bytes)) >= self.floor,
            "filesystem free-space floor reached"
        );
        usage.used[area as usize] = next;
        usage.reserved += bytes;
        Ok(Reservation {
            disk: self.clone(),
            area,
            bytes,
            committed: false,
        })
    }
    pub(crate) fn used(&self) -> [u64; 3] {
        self.usage.lock().unwrap().used
    }
    pub(crate) fn account_existing(&self, area: Area, bytes: u64) {
        self.usage.lock().unwrap().used[area as usize] += bytes;
    }
    pub(crate) fn release(&self, area: Area, bytes: u64) {
        let mut u = self.usage.lock().unwrap();
        u.used[area as usize] = u.used[area as usize].saturating_sub(bytes);
    }
    pub(crate) fn fragment_path(&self, id: Uuid) -> PathBuf {
        self.root.join("multipart").join(id.simple().to_string())
    }
    pub(crate) fn pending(&self, id: Uuid) -> bool {
        self.pending.lock().unwrap().contains(&id)
    }
    pub(crate) async fn write_fragment(
        self: &Arc<Self>,
        id: Uuid,
        data: &[u8],
    ) -> Result<Fragment> {
        ensure!(
            !data.is_empty() && data.len() <= MAX,
            "invalid fragment length"
        );
        let permit = self.io.clone().acquire_owned().await?;
        let ticket = self.reserve(Area::Multipart, data.len() as u64)?;
        let path = self.fragment_path(id);
        self.pending.lock().unwrap().insert(id);
        let lease = Fragment {
            id,
            disk: self.clone(),
        };
        let data = data.to_vec();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            use std::io::Write;
            let result = (|| {
                let mut f = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&path)?;
                f.write_all(&data)?;
                f.sync_all()?;
                std::fs::File::open(path.parent().unwrap())?.sync_all()?;
                Ok::<_, std::io::Error>(())
            })();
            if let Err(e) = result {
                if std::fs::remove_file(&path).is_err() && path.try_exists().unwrap_or(true) {
                    ticket.commit();
                }
                return Err(e.into());
            }
            ticket.commit();
            Ok(lease)
        })
        .await?
    }
    pub(crate) async fn remove_fragment(&self, id: Uuid) -> Result<()> {
        let path = self.fragment_path(id);
        match tokio::fs::metadata(&path).await {
            Ok(m) => {
                tokio::fs::remove_file(path).await?;
                self.release(Area::Multipart, m.len());
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        Ok(())
    }

    pub(crate) async fn append_fragment(
        self: &Arc<Self>,
        id: Uuid,
        data: &[u8],
        guard: tokio::sync::OwnedMutexGuard<()>,
    ) -> Result<(i64, Fragment, tokio::sync::OwnedMutexGuard<()>)> {
        ensure!(
            !data.is_empty() && data.len() <= MAX,
            "invalid fragment length"
        );
        let permit = self.io.clone().acquire_owned().await?;
        let ticket = self.reserve(Area::Multipart, data.len() as u64)?;
        let path = self.fragment_path(id);
        self.pending.lock().unwrap().insert(id);
        let lease = Fragment {
            id,
            disk: self.clone(),
        };
        let data = data.to_vec();
        tokio::task::spawn_blocking(move || {
            // Cancellation must not release the upload lock while the file is still changing.
            let guard = guard;
            let _permit = permit;
            use std::io::Write;
            let created = match std::fs::symlink_metadata(&path) {
                Ok(meta) => {
                    ensure!(meta.is_file(), "multipart file was replaced");
                    false
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
                Err(error) => return Err(error.into()),
            };
            let mut options = std::fs::OpenOptions::new();
            options.append(true).create_new(created);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o644);
            }
            let mut file = options.open(&path)?;
            let offset = file.metadata()?.len();
            ensure!(
                offset <= i64::MAX as u64 - data.len() as u64,
                "multipart file is too large"
            );
            let result = (|| {
                file.write_all(&data)?;
                file.sync_all()?;
                if created {
                    std::fs::File::open(path.parent().unwrap())?.sync_all()?;
                }
                Ok::<_, std::io::Error>(())
            })();
            ticket.commit_written(file.metadata()?.len().saturating_sub(offset));
            result?;
            Ok((offset as i64, lease, guard))
        })
        .await?
    }
}
