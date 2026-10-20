use super::cache::UploadCharge;
use crate::content::codec::MAX;
use anyhow::{Context, Result, ensure};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{Arc, Mutex, Weak},
};
use tokio::{io::AsyncReadExt, sync::Semaphore};
use uuid::Uuid;

#[derive(Clone, Copy)]
pub(crate) enum Area {
    Multipart = 0,
    Cache = 1,
    Thumbnails = 2,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_namespaces_cannot_alias_disposable_cache_files() -> Result<()> {
        let root = std::env::temp_dir().join(format!("mokyu-paths-{}", Uuid::new_v4()));
        std::fs::create_dir_all(root.join("raw"))?;
        for name in ["raw", "chunks", "multipart", "thumbnails"] {
            assert!(validate_storage_path(&root, &root.join(name)).is_err());
            assert!(validate_storage_path(&root, &root.join(name).join("backend")).is_err());
        }
        assert!(validate_storage_path(&root, &root).is_err());
        assert!(validate_storage_path(&root, root.parent().unwrap()).is_err());
        validate_storage_path(&root, &root.join("storage"))?;
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(root.join("raw"), root.join("alias"))?;
            assert!(validate_storage_path(&root, &root.join("alias/new")).is_err());
        }
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn multipart_accounts_allocated_blocks_and_shares_filesystem_reservations() -> Result<()>
    {
        let root = std::env::temp_dir().join(format!("mokyu-space-{}", Uuid::new_v4()));
        std::fs::create_dir_all(root.join("multipart"))?;
        std::fs::create_dir(root.join("storage"))?;
        let disk = Arc::new(Disk {
            root: root.clone(),
            limits: [Some(1024 * 1024), None, None],
            floor: 0,
            spaces: Mutex::default(),
            upload_limit: None,
            usage: Mutex::new(Usage {
                used: [0; 3],
                upload: 0,
            }),
            io: Arc::new(Semaphore::new(1)),
            pending: Mutex::default(),
        });
        let pending = disk.space_for(&root)?;
        assert!(Arc::ptr_eq(
            &pending,
            &disk.space_for(&root.join("storage"))?
        ));
        let space = Space::claim(&root, 0, 1, &pending)?;
        assert!(*pending.lock().unwrap() >= 1);
        drop(space);
        assert_eq!(*pending.lock().unwrap(), 0);
        let granularity = fs2::allocation_granularity(&root)?.max(1);
        assert!(Space::claim(&root, 0, u64::MAX / granularity * granularity, &pending).is_err());
        assert_eq!(*pending.lock().unwrap(), 0);

        let id = Uuid::new_v4();
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        let (_, lease, guard) = disk
            .append_fragment(id, &[7], lock.clone().lock_owned().await)
            .await?;
        let (_, second, guard) = disk.append_fragment(id, &[8], guard).await?;
        let metadata = std::fs::metadata(disk.fragment_path(id))?;
        assert_eq!(disk.used()[0], allocated(&metadata));
        assert_eq!(std::fs::read(disk.fragment_path(id))?, [7, 8]);
        assert_eq!(*pending.lock().unwrap(), 0);
        drop((guard, lease, second));
        disk.remove_fragment(id).await?;
        assert_eq!(disk.used()[0], 0);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
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
    pub(crate) upload: u64,
}
pub(crate) struct Disk {
    pub root: PathBuf,
    pub(crate) limits: [Option<u64>; 3],
    pub(crate) floor: u64,
    pub(crate) spaces: Mutex<HashMap<String, Weak<Mutex<u64>>>>,
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
    _space: Space,
}
impl Reservation {
    pub(crate) fn commit(mut self) {
        self.committed = true;
    }
    fn commit_written(mut self, bytes: u64) {
        let mut usage = self.disk.usage.lock().unwrap();
        usage.used[self.area as usize] = usage.used[self.area as usize]
            .saturating_sub(self.bytes)
            .saturating_add(bytes);
        self.committed = true;
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.committed {
            let mut u = self.disk.usage.lock().unwrap();
            u.used[self.area as usize] -= self.bytes;
        }
    }
}
pub(crate) fn allocated(metadata: &std::fs::Metadata) -> u64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        metadata.blocks().saturating_mul(512)
    }
    #[cfg(not(unix))]
    {
        metadata.len()
    }
}

pub(crate) fn validate_storage_path(
    data: &std::path::Path,
    storage: &std::path::Path,
) -> Result<()> {
    fn resolve(path: &std::path::Path) -> Result<PathBuf> {
        let mut path = std::path::absolute(path)?;
        let mut missing = Vec::new();
        loop {
            match std::fs::canonicalize(&path) {
                Ok(root) => {
                    path = root;
                    break;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    missing.push(
                        path.file_name()
                            .context("storage path has no parent")?
                            .to_owned(),
                    );
                    ensure!(path.pop(), "storage path has no parent");
                }
                Err(error) => return Err(error.into()),
            }
        }
        for component in missing.into_iter().rev() {
            path.push(component);
        }
        Ok(path)
    }
    let storage = resolve(storage)?;
    for name in ["chunks", "raw", "multipart", "thumbnails"] {
        let reserved = resolve(&data.join(name))?;
        ensure!(
            !storage.starts_with(&reserved) && !reserved.starts_with(&storage),
            "local storage overlaps the instance cache or temporary namespace"
        );
    }
    Ok(())
}

pub(crate) struct Space {
    pending: Arc<Mutex<u64>>,
    bytes: u64,
}
impl Space {
    pub(crate) fn claim(
        path: &std::path::Path,
        floor: u64,
        bytes: u64,
        pending: &Arc<Mutex<u64>>,
    ) -> Result<Self> {
        let mut reserved = pending.lock().unwrap();
        let stats = fs2::statvfs(path)?;
        let granularity = stats.allocation_granularity().max(1);
        let bytes = bytes
            .div_ceil(granularity)
            .checked_mul(granularity)
            .context("disk reservation overflow")?;
        let next = reserved
            .checked_add(bytes)
            .context("disk reservation overflow")?;
        ensure!(
            stats.available_space()
                >= next
                    .checked_add(floor)
                    .context("disk reservation overflow")?,
            "filesystem free-space floor reached"
        );
        *reserved = next;
        Ok(Self {
            pending: pending.clone(),
            bytes,
        })
    }
}
impl Drop for Space {
    fn drop(&mut self) {
        *self.pending.lock().unwrap() -= self.bytes;
    }
}

impl Disk {
    pub(crate) fn space_for(&self, path: &std::path::Path) -> Result<Arc<Mutex<u64>>> {
        #[cfg(unix)]
        let key = {
            use std::os::unix::fs::MetadataExt;
            std::fs::metadata(path)?.dev().to_string()
        };
        #[cfg(not(unix))]
        let key = std::fs::canonicalize(path)?
            .components()
            .next()
            .context("filesystem root is missing")?
            .as_os_str()
            .to_string_lossy()
            .into_owned();
        let mut spaces = self.spaces.lock().unwrap();
        spaces.retain(|_, space| space.strong_count() > 0);
        if let Some(space) = spaces.get(&key).and_then(Weak::upgrade) {
            return Ok(space);
        }
        let space = Arc::new(Mutex::new(0));
        spaces.insert(key, Arc::downgrade(&space));
        Ok(space)
    }

    fn cache_capacity(&self, usage: &Usage) -> Result<u64> {
        let capacity = usage.used[1].saturating_add(
            fs2::available_space(&self.root)?
                .saturating_sub(self.floor)
                .saturating_sub(*self.space_for(&self.root)?.lock().unwrap()),
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
        let bytes = if matches!(area, Area::Multipart) {
            let granularity = fs2::allocation_granularity(&self.root)?.max(1);
            bytes
                .div_ceil(granularity)
                .checked_mul(granularity)
                .context("disk accounting overflow")?
        } else {
            bytes
        };
        let next = usage.used[area as usize]
            .checked_add(bytes)
            .context("disk accounting overflow")?;
        ensure!(
            self.limits[area as usize].is_none_or(|n| next <= n),
            "local storage quota exhausted"
        );
        let space = Space::claim(&self.root, self.floor, bytes, &self.space_for(&self.root)?)?;
        usage.used[area as usize] = next;
        Ok(Reservation {
            disk: self.clone(),
            area,
            bytes,
            committed: false,
            _space: space,
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
                    ticket.commit_written(
                        std::fs::metadata(&path)
                            .map(|meta| allocated(&meta))
                            .unwrap_or(data.len() as u64),
                    );
                }
                return Err(e.into());
            }
            ticket.commit_written(allocated(&std::fs::metadata(&path)?));
            Ok(lease)
        })
        .await?
    }
    pub(crate) async fn remove_fragment(&self, id: Uuid) -> Result<()> {
        let path = self.fragment_path(id);
        match tokio::fs::metadata(&path).await {
            Ok(m) => {
                tokio::fs::remove_file(path).await?;
                self.release(Area::Multipart, allocated(&m));
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
            let metadata = file.metadata()?;
            let offset = metadata.len();
            let before = allocated(&metadata);
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
            ticket.commit_written(allocated(&file.metadata()?).saturating_sub(before));
            result?;
            Ok((offset as i64, lease, guard))
        })
        .await?
    }
}
