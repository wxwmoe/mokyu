use anyhow::{Context, Result, ensure};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct Usage {
    ready: bool,
    used: u64,
    reserved: u64,
    revision: u64,
    active: u64,
}

pub(crate) struct Budget {
    root: PathBuf,
    limit: u64,
    usage: Mutex<Usage>,
}

pub(crate) struct Reservation {
    budget: Arc<Budget>,
    bytes: u64,
    written: u64,
    released: u64,
    active: bool,
}

impl Budget {
    pub(crate) fn open(root: &Path, limit: u64) -> Result<Arc<Self>> {
        let root = root.canonicalize()?;
        let used = allocated_tree(&root)?;
        Ok(Arc::new(Self {
            root,
            limit,
            usage: Mutex::new(Usage {
                ready: true,
                used,
                ..Usage::default()
            }),
        }))
    }

    pub(crate) fn deferred(root: &Path, limit: u64) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            root: root.canonicalize()?,
            limit,
            usage: Mutex::default(),
        }))
    }

    pub(crate) fn ready(&self) -> bool {
        self.usage.lock().unwrap().ready
    }

    pub(crate) fn measurement(&self) -> Option<(u64, u64)> {
        let usage = self.usage.lock().unwrap();
        usage.ready.then_some((usage.used, usage.reserved))
    }

    pub(crate) fn contains(&self, path: &Path) -> Result<bool> {
        Ok(path.canonicalize()?.starts_with(&self.root))
    }

    pub(crate) fn reserve(self: &Arc<Self>, bytes: u64) -> Result<Reservation> {
        let mut usage = self.usage.lock().unwrap();
        ensure!(
            usage.ready,
            s3s::s3_error!(SlowDown, "data directory usage is still being measured")
        );
        let reserved = usage
            .reserved
            .checked_add(bytes)
            .context("data directory reservation overflow")?;
        ensure!(
            bytes == 0
                || usage
                    .used
                    .checked_add(reserved)
                    .is_some_and(|n| n <= self.limit),
            s3s::s3_error!(SlowDown, "data directory quota exhausted")
        );
        usage.reserved = reserved;
        usage.active += 1;
        usage.revision = usage.revision.wrapping_add(1);
        Ok(Reservation {
            budget: self.clone(),
            bytes,
            written: 0,
            released: 0,
            active: true,
        })
    }

    pub(crate) fn snapshot(&self) -> (u64, u64, u64) {
        let usage = self.usage.lock().unwrap();
        (self.limit, usage.used, usage.reserved)
    }

    pub(crate) fn reconcile(&self) -> Result<()> {
        let (started, active) = {
            let usage = self.usage.lock().unwrap();
            (usage.revision, usage.active)
        };
        if active != 0 {
            return Ok(());
        }
        let observed = allocated_tree(&self.root)?;
        let mut usage = self.usage.lock().unwrap();
        // A rename can move a file behind the walker; only reclaim from a stable walk.
        if usage.active == 0 && usage.revision == started {
            usage.used = observed;
            usage.ready = true;
        }
        Ok(())
    }

    pub(crate) fn start(self: &Arc<Self>) {
        let budget = Arc::downgrade(self);
        let immediate = !self.ready();
        tokio::spawn(async move {
            if immediate {
                if let Some(budget) = budget.upgrade() {
                    if !matches!(
                        tokio::task::spawn_blocking(move || budget.reconcile()).await,
                        Ok(Ok(()))
                    ) {
                        tracing::warn!("initial data directory usage measurement failed");
                    }
                }
            }
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                let Some(budget) = budget.upgrade() else {
                    break;
                };
                let result = tokio::task::spawn_blocking(move || budget.reconcile()).await;
                if !matches!(result, Ok(Ok(()))) {
                    tracing::warn!("data directory usage refresh failed");
                }
            }
        });
    }
}

impl Reservation {
    pub(crate) fn written(&mut self, bytes: u64) {
        self.written = bytes;
    }

    pub(crate) fn released(&mut self, bytes: u64) {
        self.released = bytes;
    }

    pub(crate) fn active(&mut self, active: bool) {
        if self.active != active {
            let mut usage = self.budget.usage.lock().unwrap();
            if active {
                usage.active += 1;
            } else {
                usage.active -= 1;
            }
            usage.revision = usage.revision.wrapping_add(1);
            self.active = active;
        }
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let mut usage = self.budget.usage.lock().unwrap();
        usage.reserved -= self.bytes;
        usage.used = usage
            .used
            .saturating_add(self.written)
            .saturating_sub(self.released);
        if self.active {
            usage.active -= 1;
        }
        usage.revision = usage.revision.wrapping_add(1);
    }
}

pub(super) fn allocated_files(paths: &[PathBuf]) -> Result<u64> {
    let mut used = 0u64;
    #[cfg(unix)]
    let mut links = std::collections::HashSet::new();
    for path in paths {
        let metadata = match std::fs::symlink_metadata(path) {
            Ok(value) => value,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if !links.insert((metadata.dev(), metadata.ino())) {
                continue;
            }
        }
        used = used
            .checked_add(super::local::allocated(&metadata))
            .context("data directory usage overflow")?;
    }
    Ok(used)
}

fn allocated_tree(root: &Path) -> Result<u64> {
    let mut used = super::local::allocated(&std::fs::symlink_metadata(root)?);
    let mut directories = vec![std::fs::read_dir(root)?];
    #[cfg(unix)]
    let mut links = std::collections::HashSet::new();
    while let Some(directory) = directories.last_mut() {
        let Some(entry) = directory.next() else {
            directories.pop();
            continue;
        };
        let entry = entry?;
        let metadata = match std::fs::symlink_metadata(entry.path()) {
            Ok(value) => value,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if metadata.is_file()
                && metadata.nlink() > 1
                && !links.insert((metadata.dev(), metadata.ino()))
            {
                continue;
            }
        }
        used = used
            .checked_add(super::local::allocated(&metadata))
            .context("data directory usage overflow")?;
        if metadata.is_dir() {
            match std::fs::read_dir(entry.path()) {
                Ok(next) => directories.push(next),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
                Err(error) => return Err(error.into()),
            }
        }
    }
    Ok(used)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deferred_accounting_never_treats_unknown_space_as_empty() -> Result<()> {
        let directory = tempfile::tempdir()?;
        std::fs::write(directory.path().join("existing"), [5; 8192])?;
        let initial = allocated_tree(directory.path())?;
        let budget = Budget::deferred(directory.path(), initial)?;
        assert!(!budget.ready());
        assert!(budget.reserve(1).is_err());
        assert!(budget.reserve(0).is_err());
        assert_eq!(
            std::fs::read(directory.path().join("existing"))?.len(),
            8192
        );
        budget.reconcile()?;
        assert!(budget.ready());
        assert_eq!(budget.snapshot().1, initial);
        assert!(budget.reserve(1).is_err());
        assert!(budget.reserve(0).is_ok());
        Ok(())
    }

    #[test]
    fn refresh_does_not_count_inflight_writes_twice() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let budget = Budget::open(dir.path(), u64::MAX)?;
        let initial = budget.snapshot().1;
        let mut pending = budget.reserve(4096)?;
        std::fs::write(dir.path().join("body"), [7; 4096])?;
        budget.reconcile()?;
        assert_eq!(budget.snapshot().1, initial);
        pending.written(4096);
        drop(pending);
        budget.reconcile()?;
        assert_eq!(budget.snapshot().1, allocated_tree(dir.path())?);
        let mut queued = budget.reserve(4096)?;
        queued.active(false);
        std::fs::write(dir.path().join("database-wal"), [8; 4096])?;
        budget.reconcile()?;
        assert_eq!(budget.snapshot().1, allocated_tree(dir.path())?);
        assert_eq!(budget.snapshot().2, 4096);
        Ok(())
    }

    async fn limited_disk(root: &Path, bytes: u64) -> Result<Arc<super::super::local::Disk>> {
        use super::super::local::{Disk, Usage};
        let mut disk = Arc::new(Disk {
            root: root.to_owned(),
            data: None,
            limits: [None; 3],
            floor: 0,
            spaces: Mutex::default(),
            upload_limit: None,
            usage: Mutex::new(Usage {
                used: [0; 3],
                upload: 0,
                upload_by_storage: Default::default(),
            }),
            io: Arc::new(tokio::sync::Semaphore::new(8)),
            pending: Mutex::default(),
        });
        disk.initialize().await?;
        let initial = allocated_tree(root)?;
        Arc::get_mut(&mut disk).unwrap().data = Some(Budget::open(root, initial + bytes)?);
        Ok(disk)
    }

    #[tokio::test]
    async fn multipart_writers_share_the_limit_and_deletion_restores_capacity() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let disk = limited_disk(dir.path(), 4096).await?;
        let ids = [uuid::Uuid::now_v7(), uuid::Uuid::now_v7()];
        let bytes = [5; 4096];
        let (first, second) = tokio::join!(
            disk.write_fragment(ids[0], &bytes),
            disk.write_fragment(ids[1], &bytes)
        );
        assert_ne!(first.is_ok(), second.is_ok());
        let saved = if first.is_ok() { ids[0] } else { ids[1] };
        drop((first, second));
        assert!(
            disk.write_fragment(uuid::Uuid::now_v7(), &bytes)
                .await
                .is_err()
        );
        disk.remove_fragment(saved).await?;
        let retry = disk.write_fragment(uuid::Uuid::now_v7(), &bytes).await?;
        let restarted = Budget::open(dir.path(), disk.data.as_ref().unwrap().snapshot().0)?;
        assert_eq!(disk.data.as_ref().unwrap().snapshot(), restarted.snapshot());
        assert_eq!(std::fs::read(disk.fragment_path(retry.id))?, bytes);
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unlinking_one_hard_link_keeps_the_file_charged() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let disk = limited_disk(dir.path(), 4096).await?;
        let fragment = disk
            .write_fragment(uuid::Uuid::now_v7(), &[3; 4096])
            .await?;
        let alias = dir.path().join("alias");
        std::fs::hard_link(disk.fragment_path(fragment.id), &alias)?;
        disk.remove_fragment(fragment.id).await?;
        assert!(
            disk.write_fragment(uuid::Uuid::now_v7(), &[3; 4096])
                .await
                .is_err()
        );
        disk.remove_committed(alias).await?;
        disk.write_fragment(uuid::Uuid::now_v7(), &[3; 4096])
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn anonymous_manifest_files_keep_capacity_until_the_reader_closes() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let disk = limited_disk(dir.path(), 8192).await?;
        let budget = disk.data.as_ref().unwrap();
        let mut spool = super::super::raw_manifest::Spool::new(dir.path(), Some(disk.clone()));
        let block = vec![1; crate::content::raw_pages::PAGE];
        for _ in 0..crate::content::raw_pages::GROUP {
            spool.feed(&block).await?;
        }
        let mut reader = spool.finish().await?;
        budget.reconcile()?;
        assert_eq!(budget.snapshot().2, 8192);
        assert!(budget.reserve(1).is_err());
        assert!(reader.next().await?.is_some());
        assert!(reader.next().await?.is_none());
        drop(reader);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while budget.snapshot().2 != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert!(budget.reserve(8192).is_ok());
        Ok(())
    }

    #[test]
    fn writes_share_capacity_and_recovery_counts_existing_files() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let budget = Budget::open(dir.path(), u64::MAX)?;
        let initial = budget.snapshot().1;
        let budget = Budget::open(dir.path(), initial + 8192)?;
        let mut first = budget.reserve(4096)?;
        let second = budget.reserve(4096)?;
        assert!(budget.reserve(1).is_err());
        drop(second);
        let replacement = budget.reserve(4096)?;
        std::fs::write(dir.path().join("body"), [7; 4096])?;
        first.written(4096);
        drop(first);
        assert!(budget.reserve(1).is_err());
        drop(replacement);
        budget.reconcile()?;
        let restarted = Budget::open(dir.path(), initial + 8192)?;
        assert_eq!(budget.snapshot(), restarted.snapshot());
        std::fs::remove_file(dir.path().join("body"))?;
        budget.reconcile()?;
        assert_eq!(budget.snapshot().1, initial);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn inventory_counts_hard_links_once_and_ignores_sparse_holes() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let external = tempfile::tempdir()?;
        std::fs::write(dir.path().join("original"), [3; 4096])?;
        std::fs::hard_link(dir.path().join("original"), dir.path().join("linked"))?;
        let sparse = std::fs::File::create(dir.path().join("sparse"))?;
        sparse.set_len(1024 * 1024 * 1024)?;
        std::fs::write(external.path().join("outside"), [4; 8192])?;
        std::os::unix::fs::symlink(external.path(), dir.path().join("alias"))?;
        let budget = Budget::open(dir.path(), 1024 * 1024)?;
        assert!(budget.snapshot().1 < 32 * 1024);
        assert!(!budget.contains(&dir.path().join("alias"))?);
        let before = budget.snapshot().1;
        std::fs::remove_file(dir.path().join("linked"))?;
        budget.reconcile()?;
        assert_eq!(budget.snapshot().1, before);
        Ok(())
    }
}
