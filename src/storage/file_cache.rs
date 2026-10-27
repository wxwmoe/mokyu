use super::{Area, CacheFile, CachePin, CachePolicy, CacheRemoval, Disk, Fifo, Reservation};
use anyhow::{Result, ensure};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::{io::AsyncWriteExt, sync::Mutex};
use uuid::Uuid;

pub(crate) struct Files<'a> {
    pub(crate) disk: &'a Arc<Disk>,
    pub(crate) root: &'a Path,
    pub(crate) fifo: &'a Mutex<Fifo>,
    pub(crate) cache_writes: &'a Mutex<()>,
}

impl Files<'_> {
    pub(crate) async fn pin(&self, id: Uuid) -> Option<(CachePin, CacheFile)> {
        let mut fifo = self.fifo.lock().await;
        let entry = fifo.entries.get_mut(&id)?;
        if !entry.valid || entry.pins.load(Ordering::Relaxed) == usize::MAX {
            return None;
        }
        entry.pins.fetch_add(1, Ordering::Relaxed);
        if super::resource_gate::PRIORITY
            .try_with(|priority| *priority)
            .unwrap_or(super::resource_gate::FOREGROUND)
            == super::resource_gate::FOREGROUND
        {
            entry.frequency = entry.frequency.saturating_add(1).min(3);
        }
        Some((CachePin(entry.pins.clone()), entry.file))
    }

    pub(crate) async fn reject(&self, id: Uuid) {
        if let Some(entry) = self.fifo.lock().await.entries.get_mut(&id) {
            entry.valid = false;
        }
        if let Err(error) = self.invalidate(id, None).await {
            tracing::debug!(%error, "invalid read cache awaits cleanup");
        }
    }
    pub(crate) async fn invalidate(
        &self,
        id: Uuid,
        expected: Option<&Arc<AtomicUsize>>,
    ) -> Result<()> {
        let _write = self.cache_writes.lock().await;
        let mut fifo = self.fifo.lock().await;
        let Some(entry) = fifo.entries.get(&id) else {
            return Ok(());
        };
        if entry.file.is_pending()
            || entry.pins.load(Ordering::Relaxed) > 0
            || expected.is_some_and(|pins| !Arc::ptr_eq(pins, &entry.pins))
        {
            return Ok(());
        }
        let removal = CacheRemoval::new(&entry.pins);
        let path = entry.file.path(self.root, id);
        drop(fifo);
        #[cfg(feature = "fault-injection")]
        crate::faults::point("cache-delete").await;
        let result = tokio::fs::remove_file(path).await;
        fifo = self.fifo.lock().await;
        match result {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        if let Some(e) = fifo.forget(id) {
            self.disk.release(Area::Cache, e.size);
        }
        drop(removal);
        Ok(())
    }
    pub(crate) async fn reserve(
        &self,
        size: u64,
        policy: CachePolicy,
        existing: bool,
        path: &Path,
    ) -> Result<Option<Reservation>> {
        let directory = path.parent().unwrap();
        tokio::fs::create_dir_all(directory).await?;
        let mut fifo = self.fifo.lock().await;
        loop {
            if policy.cold
                && policy
                    .capacity
                    .is_some_and(|limit| self.disk.used()[1].saturating_add(size) > limit)
            {
                return Ok(None);
            }
            if fifo.entries.len() < policy.max_entries
                && let Ok(ticket) = self.disk.reserve_at(Area::Cache, size, existing, directory)
            {
                return Ok(Some(ticket));
            }
            if policy.cold {
                return Ok(None);
            }
            let capacity = policy.capacity.unwrap_or(self.disk.used()[1].max(size));
            let Some(victim) = fifo.victim(capacity) else {
                return Ok(None);
            };
            let entry = &fifo.entries[&victim];
            let removal = CacheRemoval::new(&entry.pins);
            let path = entry.file.path(self.root, victim);
            drop(fifo);
            #[cfg(feature = "fault-injection")]
            crate::faults::point("cache-delete").await;
            let result = tokio::fs::remove_file(path).await;
            fifo = self.fifo.lock().await;
            match result {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    fifo.restore_victim(victim);
                    return Err(e.into());
                }
            }
            if let Some(e) = fifo.evict(victim, policy.max_entries) {
                self.disk.release(Area::Cache, e.size);
            }
            drop(removal);
        }
    }
    pub(crate) async fn fill(
        &self,
        id: Uuid,
        data: &[u8],
        compressed: bool,
        policy: CachePolicy,
        check: impl Fn() -> Result<()> + Send + Sync,
    ) -> Result<()> {
        let size = data.len() as u64;
        if policy.capacity.is_some_and(|capacity| size > capacity) {
            return Ok(());
        }
        let invalid = self
            .fifo
            .lock()
            .await
            .entries
            .get(&id)
            .filter(|e| !e.valid)
            .map(|e| e.pins.clone());
        if let Some(expected) = invalid {
            self.invalidate(id, Some(&expected)).await?;
        }
        // Serialize publication/invalidation while cache hits only need the FIFO index lock.
        let _write = self.cache_writes.lock().await;
        if self.fifo.lock().await.entries.contains_key(&id) {
            return Ok(());
        }
        check()?;
        let path = CacheFile::Chunk(compressed).path(self.root, id);
        let Some(ticket) = self.reserve(size, policy, false, &path).await? else {
            return Ok(());
        };
        #[cfg(feature = "fault-injection")]
        crate::faults::point("cache-reserved").await;
        let tmp = path.with_extension(if compressed { "zst.tmp" } else { "raw.tmp" });
        tokio::fs::create_dir_all(path.parent().unwrap()).await?;
        let result = async {
            tokio::time::timeout(std::time::Duration::from_millis(100), async {
                let mut f = tokio::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .mode(0o644)
                    .open(&tmp)
                    .await?;
                f.write_all(data).await?;
                f.flush().await?;
                Ok::<_, anyhow::Error>(())
            })
            .await??;
            #[cfg(feature = "fault-injection")]
            crate::faults::point("cache-written").await;
            check()?;
            tokio::fs::rename(&tmp, &path).await?;
            #[cfg(feature = "fault-injection")]
            crate::faults::point("cache-published").await;
            Ok::<_, anyhow::Error>(())
        }
        .await;
        if let Err(e) = result {
            if tokio::fs::remove_file(&tmp).await.is_err()
                && tokio::fs::try_exists(&tmp).await.unwrap_or(true)
            {
                ticket.commit();
            }
            return Err(e);
        }
        ticket.commit();
        let mut fifo = self.fifo.lock().await;
        if policy.cold {
            fifo.ghost.remove(&id);
        }
        fifo.insert(id, size, compressed);
        Ok(())
    }

    // Call from a cancellation-safe owner after its metadata references have been released.
    pub(crate) async fn promote(
        &self,
        id: Uuid,
        destination: Option<Uuid>,
        owner: Option<&Arc<AtomicUsize>>,
        policy: CachePolicy,
    ) -> Result<bool> {
        let _write = self.cache_writes.lock().await;
        let mut fifo = self.fifo.lock().await;
        let Some(entry) = fifo.entries.get(&id) else {
            return Ok(true);
        };
        if !entry.file.is_pending() {
            ensure!(
                owner.is_some_and(|pins| !Arc::ptr_eq(pins, &entry.pins)),
                "pending file identity changed"
            );
            return Ok(true);
        }
        if entry.pins.load(Ordering::Relaxed) != usize::from(owner.is_some())
            || owner.is_some_and(|pins| !Arc::ptr_eq(pins, &entry.pins))
        {
            return Ok(false);
        }
        let removal = if owner.is_some() {
            CacheRemoval::owned(&entry.pins)
        } else {
            CacheRemoval::new(&entry.pins)
        };
        let source = entry.file.path(self.root, id);
        let format = entry.file.cached();
        let size = entry.size;
        let destination = destination.filter(|_| {
            entry.valid
                && policy.enabled
                && fifo.entries.len() <= policy.max_entries
                && policy.capacity.is_none_or(|limit| size <= limit)
        });
        let other = destination
            .filter(|target| *target != id)
            .and_then(|target| fifo.entries.get(&target).map(|entry| (target, entry)));
        if other.is_some_and(|(_, entry)| {
            entry.file.is_pending() || entry.pins.load(Ordering::Relaxed) != 0
        }) {
            return Ok(false);
        }
        let other_removal = other.map(|(_, entry)| CacheRemoval::new(&entry.pins));
        let other_path = other.map(|(target, entry)| entry.file.path(self.root, target));
        drop(fifo);
        let cache_path = destination.map(|target| format.path(self.root, target));
        let cached = if let Some(target) = destination {
            link(&source, &format.path(self.root, target), size).await?
        } else {
            false
        };
        if cached && let Some(path) = other_path {
            if Some(&path) != cache_path.as_ref() {
                remove(&path).await?;
            }
            if let Some(entry) = self.fifo.lock().await.forget(destination.unwrap()) {
                self.disk.release(Area::Cache, entry.size);
            }
        }
        #[cfg(feature = "fault-injection")]
        crate::faults::point("pending-cache-linked").await;
        remove(&source).await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("pending-cache-unlinked").await;
        fifo = self.fifo.lock().await;
        fifo.forget(id);
        if cached {
            fifo.insert(destination.unwrap(), size, format);
        } else {
            self.disk.release(Area::Cache, size);
        }
        drop((removal, other_removal));
        Ok(true)
    }
}

async fn remove(path: &Path) -> Result<()> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub(super) async fn link(source: &Path, destination: &Path, size: u64) -> Result<bool> {
    let metadata = tokio::fs::symlink_metadata(source).await?;
    ensure!(
        metadata.is_file() && metadata.len() == size,
        "pending file was replaced"
    );
    tokio::fs::create_dir_all(destination.parent().unwrap()).await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.dev()
            != tokio::fs::metadata(destination.parent().unwrap())
                .await?
                .dev()
        {
            return Ok(false);
        }
    }
    match tokio::fs::symlink_metadata(destination).await {
        Ok(existing) => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                if existing.is_file()
                    && metadata.dev() == existing.dev()
                    && metadata.ino() == existing.ino()
                {
                    return Ok(true);
                }
            }
            // The pending file is authoritative; an unrelated cache entry is replaceable.
            tokio::fs::remove_file(destination).await?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    match tokio::fs::hard_link(source, destination).await {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::CrossesDevices => Ok(false),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::local::Usage;
    use std::collections::{HashMap, HashSet};
    use tokio::sync::Semaphore;

    fn disk(root: &Path) -> Arc<Disk> {
        Arc::new(Disk {
            root: root.to_owned(),
            limits: [None, Some(8192), None],
            floor: 0,
            spaces: Default::default(),
            upload_limit: None,
            usage: std::sync::Mutex::new(Usage {
                used: [0; 3],
                upload: 0,
                upload_by_storage: HashMap::new(),
            }),
            io: Arc::new(Semaphore::new(1)),
            pending: std::sync::Mutex::new(HashSet::new()),
        })
    }

    #[tokio::test]
    async fn promotion_preserves_inode_waits_for_readers_and_retries_without_double_charging()
    -> Result<()> {
        let root = std::env::temp_dir().join(format!("mokyu-promotion-{}", Uuid::new_v4()));
        let disk = disk(&root);
        disk.initialize().await?;
        let fifo = Mutex::new(Fifo::default());
        let writes = Mutex::new(());
        let cache = Files {
            disk: &disk,
            root: &root,
            fifo: &fifo,
            cache_writes: &writes,
        };
        let policy = CachePolicy {
            enabled: true,
            cold: false,
            capacity: Some(8192),
            max_entries: 2,
            upload_cache: true,
            upload_capacity: None,
        };
        let source = Uuid::new_v4();
        let target = Uuid::new_v4();
        let pending = CacheFile::PendingChunk(true).path(&root, source);
        tokio::fs::create_dir_all(pending.parent().unwrap()).await?;
        tokio::fs::write(&pending, [7; 4096]).await?;
        disk.account_existing(Area::Cache, 4096);
        fifo.lock()
            .await
            .insert(source, 4096, CacheFile::PendingChunk(true));
        let owner = fifo.lock().await.entries[&source].pins.clone();
        owner.fetch_add(1, Ordering::Relaxed);
        let upload = CachePin(owner.clone());
        let (reader, _) = cache.pin(source).await.unwrap();
        assert!(
            !cache
                .promote(source, Some(target), Some(&owner), policy)
                .await?
        );
        cache.invalidate(source, None).await?;
        assert!(pending.exists());
        drop(reader);
        let destination = CacheFile::Chunk(true).path(&root, target);
        tokio::fs::create_dir_all(&destination).await?;
        assert!(
            cache
                .promote(source, Some(target), Some(&owner), policy)
                .await
                .is_err()
        );
        assert_eq!(disk.used()[1], 4096);
        assert_eq!(owner.load(Ordering::Relaxed), 1);
        tokio::fs::remove_dir(&destination).await?;
        tokio::fs::write(&destination, [3; 4096]).await?;
        let before = tokio::fs::metadata(&pending).await?;
        assert!(
            cache
                .promote(source, Some(target), Some(&owner), policy)
                .await?
        );
        drop(upload);
        assert!(!pending.exists());
        assert_eq!(tokio::fs::read(&destination).await?, [7; 4096]);
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let after = tokio::fs::metadata(&destination).await?;
            assert_eq!((before.dev(), before.ino()), (after.dev(), after.ino()));
        }
        assert_eq!(disk.used()[1], 4096);
        cache.invalidate(target, None).await?;

        // A crash can leave both names for one inode before the index handoff.
        let id = Uuid::new_v4();
        let pending = CacheFile::PendingRaw.path(&root, id);
        let destination = CacheFile::Raw.path(&root, id);
        for path in [&pending, &destination] {
            tokio::fs::create_dir_all(path.parent().unwrap()).await?;
        }
        tokio::fs::write(&pending, [9; 4096]).await?;
        tokio::fs::hard_link(&pending, &destination).await?;
        disk.account_existing(Area::Cache, 4096);
        fifo.lock().await.insert(id, 4096, CacheFile::PendingRaw);
        assert!(fifo.lock().await.victim(8192).is_none());
        assert!(cache.promote(id, Some(id), None, policy).await?);
        assert_eq!(disk.used()[1], 4096);
        assert_eq!(tokio::fs::read(&destination).await?, [9; 4096]);
        cache.invalidate(id, None).await?;
        assert_eq!(disk.used()[1], 0);
        tokio::fs::remove_dir_all(root).await?;
        Ok(())
    }

    #[tokio::test]
    async fn maintenance_fills_preserve_hot_entries_and_recheck_publication() -> Result<()> {
        let root = std::env::temp_dir().join(format!("mokyu-cold-cache-{}", Uuid::new_v4()));
        let disk = disk(&root);
        let fifo = Mutex::new(Fifo::default());
        let writes = Mutex::new(());
        let files = Files {
            disk: &disk,
            root: &root,
            fifo: &fifo,
            cache_writes: &writes,
        };
        let policy = CachePolicy {
            enabled: true,
            cold: false,
            capacity: Some(8192),
            max_entries: 2,
            upload_cache: false,
            upload_capacity: None,
        };
        let hot = Uuid::new_v4();
        files
            .fill(hot, &[1; 4096], false, policy, || Ok(()))
            .await?;
        drop(files.pin(hot).await);
        let frequency = fifo.lock().await.entries[&hot].frequency;
        super::super::resource_gate::PRIORITY
            .scope(super::super::resource_gate::MAINTENANCE, async {
                for _ in 0..8 {
                    drop(files.pin(hot).await);
                }
            })
            .await;
        assert_eq!(fifo.lock().await.entries[&hot].frequency, frequency);
        let cold = Uuid::new_v4();
        fifo.lock().await.ghost.insert(cold);
        let cold_policy = CachePolicy {
            cold: true,
            ..policy
        };
        files
            .fill(cold, &[2; 4096], false, cold_policy, || Ok(()))
            .await?;
        assert!(matches!(
            fifo.lock().await.entries[&cold].queue,
            super::super::cache::Queue::Small
        ));
        for _ in 0..8 {
            files
                .fill(Uuid::new_v4(), &[3; 4096], false, cold_policy, || Ok(()))
                .await?;
        }
        assert_eq!(disk.used()[1], 8192);
        assert!(files.pin(hot).await.is_some());
        files.invalidate(cold, None).await?;
        let rejected = Uuid::new_v4();
        let checks = AtomicUsize::new(0);
        assert!(
            files
                .fill(rejected, &[4; 4096], false, cold_policy, || {
                    ensure!(
                        checks.fetch_add(1, Ordering::Relaxed) == 0,
                        "authority expired"
                    );
                    Ok(())
                })
                .await
                .is_err()
        );
        assert_eq!(checks.load(Ordering::Relaxed), 2);
        assert_eq!(disk.used()[1], 4096);
        assert!(!fifo.lock().await.entries.contains_key(&rejected));
        assert!(!CacheFile::Chunk(false).path(&root, rejected).exists());
        tokio::fs::remove_dir_all(root).await?;
        Ok(())
    }

    #[tokio::test]
    async fn confirmed_request_handoff_reuses_inode_and_one_disk_charge() -> Result<()> {
        let root = std::env::temp_dir().join(format!("mokyu-request-cache-{}", Uuid::new_v4()));
        let disk = disk(&root);
        let fifo = Mutex::new(Fifo::default());
        let writes = Mutex::new(());
        let files = Files {
            disk: &disk,
            root: &root,
            fifo: &fifo,
            cache_writes: &writes,
        };
        let policy = CachePolicy {
            enabled: true,
            cold: true,
            capacity: Some(8192),
            max_entries: 2,
            upload_cache: false,
            upload_capacity: None,
        };
        let id = Uuid::new_v4();
        let mut request = super::super::raw_request::File::create_in(&files, id, policy).await?;
        request.append_in(&files, &[7; 4096]).await?;
        let source = CacheFile::PendingRaw
            .path(&root, id)
            .with_extension("partial");
        let metadata = tokio::fs::metadata(&source).await?;
        let target = Uuid::new_v4();
        request.cache_in(&files, target, policy, || Ok(())).await?;
        drop(request);
        assert!(!source.exists());
        let path = CacheFile::Raw.path(&root, target);
        assert_eq!(tokio::fs::read(&path).await?, [7; 4096]);
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(metadata.ino(), tokio::fs::metadata(&path).await?.ino());
        }
        assert_eq!(disk.used()[1], 4096);
        files.invalidate(target, None).await?;
        assert_eq!(disk.used()[1], 0);
        tokio::fs::remove_dir_all(root).await?;
        Ok(())
    }

    #[tokio::test]
    async fn pinned_files_and_failed_deletion_keep_disk_accounting() -> Result<()> {
        let root = std::env::temp_dir().join(format!("mokyu-file-cache-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&root).await?;
        let disk = Arc::new(Disk {
            root: root.clone(),
            limits: [None, Some(8192), None],
            floor: 0,
            spaces: Default::default(),
            upload_limit: None,
            usage: std::sync::Mutex::new(Usage {
                used: [0; 3],
                upload: 0,
                upload_by_storage: HashMap::new(),
            }),
            io: Arc::new(Semaphore::new(1)),
            pending: std::sync::Mutex::new(HashSet::new()),
        });
        let fifo = Mutex::new(Fifo::default());
        let writes = Mutex::new(());
        let cache = Files {
            disk: &disk,
            root: &root,
            fifo: &fifo,
            cache_writes: &writes,
        };
        let policy = CachePolicy {
            enabled: true,
            cold: false,
            capacity: Some(8192),
            max_entries: 1,
            upload_cache: false,
            upload_capacity: None,
        };
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        cache
            .fill(first, &[1; 4096], false, policy, || Ok(()))
            .await?;
        let (pin, _) = cache.pin(first).await.unwrap();
        cache
            .fill(second, &[2; 4096], false, policy, || Ok(()))
            .await?;
        assert_eq!(fifo.lock().await.entries.len(), 1);
        assert!(fifo.lock().await.entries.contains_key(&first));
        drop(pin);
        let path = CacheFile::Chunk(false).path(&root, first);
        tokio::fs::remove_file(&path).await?;
        tokio::fs::create_dir(&path).await?;
        tokio::fs::write(path.join("blocked"), b"blocked").await?;
        assert!(
            cache
                .fill(second, &[2; 4096], false, policy, || Ok(()))
                .await
                .is_err()
        );
        assert_eq!(disk.used()[1], 4096);
        assert!(cache.pin(first).await.is_some());
        tokio::fs::remove_file(path.join("blocked")).await?;
        tokio::fs::remove_dir(&path).await?;
        tokio::fs::write(&path, [1; 4096]).await?;
        cache
            .fill(second, &[2; 4096], false, policy, || Ok(()))
            .await?;
        assert!(!path.exists());
        assert_eq!(disk.used()[1], 4096);
        assert_eq!(
            tokio::fs::read(CacheFile::Chunk(false).path(&root, second)).await?,
            [2; 4096]
        );
        tokio::fs::remove_dir_all(root).await?;
        Ok(())
    }
}
