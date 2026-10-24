use super::{Area, CacheFile, CachePin, CachePolicy, CacheRemoval, Disk, Fifo, Reservation};
use anyhow::Result;
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
        entry.frequency = entry.frequency.saturating_add(1).min(3);
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
        if entry.pins.load(Ordering::Relaxed) > 0
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
    ) -> Result<Option<Reservation>> {
        let mut fifo = self.fifo.lock().await;
        loop {
            if fifo.entries.len() < policy.max_entries
                && let Ok(ticket) = if existing {
                    self.disk.reserve_existing(Area::Cache, size)
                } else {
                    self.disk.reserve(Area::Cache, size)
                }
            {
                return Ok(Some(ticket));
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
        let Some(ticket) = self.reserve(size, policy, false).await? else {
            return Ok(());
        };
        let path = CacheFile::Chunk(compressed).path(self.root, id);
        #[cfg(feature = "fault-injection")]
        crate::faults::point("cache-reserved").await;
        let tmp = path.with_extension(if compressed { "zst.tmp" } else { "raw.tmp" });
        tokio::fs::create_dir_all(path.parent().unwrap()).await?;
        let result = async {
            let mut f = tokio::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o644)
                .open(&tmp)
                .await?;
            f.write_all(data).await?;
            f.flush().await?;
            drop(f);
            #[cfg(feature = "fault-injection")]
            crate::faults::point("cache-written").await;
            tokio::fs::rename(&tmp, &path).await?;
            #[cfg(feature = "fault-injection")]
            crate::faults::point("cache-published").await;
            Ok::<_, std::io::Error>(())
        }
        .await;
        if let Err(e) = result {
            if tokio::fs::remove_file(&tmp).await.is_err()
                && tokio::fs::try_exists(&tmp).await.unwrap_or(true)
            {
                ticket.commit();
            }
            return Err(e.into());
        }
        ticket.commit();
        self.fifo.lock().await.insert(id, size, compressed);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::local::Usage;
    use std::collections::{HashMap, HashSet};
    use tokio::sync::Semaphore;

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
            capacity: Some(8192),
            max_entries: 1,
            upload_cache: false,
            upload_capacity: None,
        };
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        cache.fill(first, &[1; 4096], false, policy).await?;
        let (pin, _) = cache.pin(first).await.unwrap();
        cache.fill(second, &[2; 4096], false, policy).await?;
        assert_eq!(fifo.lock().await.entries.len(), 1);
        assert!(fifo.lock().await.entries.contains_key(&first));
        drop(pin);
        let path = CacheFile::Chunk(false).path(&root, first);
        tokio::fs::remove_file(&path).await?;
        tokio::fs::create_dir(&path).await?;
        tokio::fs::write(path.join("blocked"), b"blocked").await?;
        assert!(cache.fill(second, &[2; 4096], false, policy).await.is_err());
        assert_eq!(disk.used()[1], 4096);
        assert!(cache.pin(first).await.is_some());
        tokio::fs::remove_file(path.join("blocked")).await?;
        tokio::fs::remove_dir(&path).await?;
        tokio::fs::write(&path, [1; 4096]).await?;
        cache.fill(second, &[2; 4096], false, policy).await?;
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
