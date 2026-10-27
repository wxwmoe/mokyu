use super::local::Disk;
use crate::content::codec::Chunk;
use crate::model::Id;
use bytes::Bytes;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::atomic::{AtomicUsize, Ordering},
    sync::{Arc, Mutex},
};
use uuid::Uuid;

pub(super) fn cache_id(chunk: &Chunk) -> Uuid {
    if !chunk.location_id.is_nil() {
        return chunk.location_id;
    }
    chunk.uuid
}

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum CacheFile {
    Chunk(bool),
    Raw,
    PendingChunk(bool),
    PendingRaw,
}
impl CacheFile {
    pub(crate) fn is_pending(self) -> bool {
        matches!(self, Self::PendingChunk(_) | Self::PendingRaw)
    }
    pub(crate) fn pending(self) -> Self {
        match self {
            Self::Chunk(compressed) | Self::PendingChunk(compressed) => {
                Self::PendingChunk(compressed)
            }
            Self::Raw | Self::PendingRaw => Self::PendingRaw,
        }
    }
    pub(crate) fn cached(self) -> Self {
        match self {
            Self::Chunk(compressed) | Self::PendingChunk(compressed) => Self::Chunk(compressed),
            Self::Raw | Self::PendingRaw => Self::Raw,
        }
    }
    pub(crate) fn path(self, root: &std::path::Path, id: Uuid) -> std::path::PathBuf {
        let root = root.join(if self.is_pending() {
            "pending"
        } else {
            "cache"
        });
        match self {
            Self::Chunk(compressed) | Self::PendingChunk(compressed) => root
                .join("chunks")
                .join(super::chunk_name(id))
                .with_extension(if compressed { "zst" } else { "raw" }),
            Self::Raw | Self::PendingRaw => root.join("raw").join(super::chunk_name(id)),
        }
    }
}
impl From<bool> for CacheFile {
    fn from(compressed: bool) -> Self {
        Self::Chunk(compressed)
    }
}

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum Queue {
    Small,
    Main,
}
pub(crate) struct Entry {
    pub(crate) size: u64,
    pub(crate) file: CacheFile,
    pub(crate) frequency: u8,
    pub(crate) pins: Arc<AtomicUsize>,
    pub(crate) queue: Queue,
    pub(crate) valid: bool,
}
pub(crate) struct CachePin(pub(crate) Arc<AtomicUsize>);
impl Drop for CachePin {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}
pub(crate) struct CacheRemoval(Arc<AtomicUsize>, usize);
impl CacheRemoval {
    pub(crate) fn new(pins: &Arc<AtomicUsize>) -> Self {
        // Reserve deletion while unrelated cache hits continue through the FIFO.
        debug_assert_eq!(pins.load(Ordering::Relaxed), 0);
        pins.store(usize::MAX, Ordering::Relaxed);
        Self(pins.clone(), 0)
    }
}
impl CacheRemoval {
    pub(crate) fn owned(pins: &Arc<AtomicUsize>) -> Self {
        debug_assert_eq!(pins.load(Ordering::Relaxed), 1);
        pins.store(usize::MAX, Ordering::Relaxed);
        Self(pins.clone(), 1)
    }
}
impl Drop for CacheRemoval {
    fn drop(&mut self) {
        self.0.store(self.1, Ordering::Relaxed);
    }
}
pub(crate) struct CacheWork {
    pub(crate) id: Uuid,
    pub(crate) pending: Arc<Mutex<HashSet<Uuid>>>,
    pub(crate) _slot: tokio::sync::OwnedSemaphorePermit,
    pub(crate) _memory: tokio::sync::OwnedSemaphorePermit,
}
impl Drop for CacheWork {
    fn drop(&mut self) {
        self.pending.lock().unwrap().remove(&self.id);
    }
}
pub(crate) struct UploadCharge {
    pub(crate) disk: Arc<Disk>,
    pub(crate) storage: Uuid,
    pub(crate) size: u64,
}
impl Drop for UploadCharge {
    fn drop(&mut self) {
        let mut usage = self.disk.usage.lock().unwrap();
        usage.upload -= self.size;
        if let Some(used) = usage.upload_by_storage.get_mut(&self.storage) {
            *used -= self.size;
            if *used == 0 {
                usage.upload_by_storage.remove(&self.storage);
            }
        }
    }
}
pub(crate) struct UploadPin {
    pub(crate) memory: Option<Bytes>,
    pub(crate) _pin: Option<CachePin>,
    pub(crate) _charge: Option<UploadCharge>,
    pub size: u64,
    pub compressed: bool,
    pub remote: bool,
    pub staging_storage_id: Option<Uuid>,
    pub staging_object_key: String,
}
pub(crate) struct LoadedPack {
    pub pack: crate::content::pack::Pack,
    pub data: crate::content::pack::Decoded,
    pub(crate) verified: bool,
    pub(crate) _memory: tokio::sync::OwnedSemaphorePermit,
    pub(crate) used: Mutex<DownloadUsage>,
    pub(crate) access: Arc<crate::status::access::Access>,
}
#[derive(Default)]
pub(crate) struct DownloadUsage {
    pub(crate) members: HashMap<Id, (usize, usize)>,
    pub(crate) observed: bool,
    pub(crate) range: bool,
    pub(crate) full: bool,
}
impl LoadedPack {
    pub(crate) fn touch(&self, c: &Chunk, observe: bool, range: bool) {
        let mut used = self.used.lock().unwrap();
        used.observed |= observe;
        used.range |= observe && range;
        used.full |= !observe || !range;
        used.members.insert(
            c.id,
            (
                c.raw_size as usize,
                c.stored_size.unwrap_or(c.raw_size + 16) as usize,
            ),
        );
    }
}
impl Drop for LoadedPack {
    fn drop(&mut self) {
        let used = self.used.lock().unwrap();
        if used.observed {
            let raw: usize = used.members.values().map(|(n, _)| n).sum();
            let bytes = self.pack.stored_size.unwrap_or(0) as usize;
            let useful: usize = used.members.values().map(|(_, n)| n).sum();
            self.access.pack(
                self.pack.id,
                bytes,
                used.range && !used.full && raw < self.pack.raw_size as usize,
                useful.min(bytes),
                used.members.keys().copied(),
            );
        }
    }
}
#[derive(Default)]
pub(crate) struct ReadContext {
    pub(crate) pack: tokio::sync::Mutex<Option<Arc<LoadedPack>>>,
    pub(crate) observe: bool,
    pub(crate) range: bool,
    pub(crate) checks: Option<Arc<crate::app::checks::Checks>>,
    pub(crate) capture: Option<Arc<Mutex<crate::app::responses::Mapping>>>,
}
impl ReadContext {
    pub fn user(
        range: bool,
        checks: Arc<crate::app::checks::Checks>,
        capture: Option<Arc<Mutex<crate::app::responses::Mapping>>>,
    ) -> Self {
        Self {
            observe: true,
            range,
            checks: Some(checks),
            capture,
            ..Default::default()
        }
    }
    pub(crate) async fn touch(&self, c: &Chunk) {
        if let Some(p) = self.pack.lock().await.as_ref()
            && p.data.members.iter().any(|m| m.id == c.uuid)
        {
            p.touch(c, self.observe, self.range);
        }
    }
}
pub(crate) enum Source {
    Chunk(Chunk, CachePin),
    Pack(crate::content::pack::Pack, CachePin),
}
#[derive(Default)]
pub(crate) struct Fifo {
    pub(crate) entries: HashMap<Uuid, Entry>,
    pub(crate) small: VecDeque<Uuid>,
    pub(crate) main: VecDeque<Uuid>,
    pub(crate) ghost: HashSet<Uuid>,
    pub(crate) ghosts: VecDeque<Uuid>,
    pub(crate) small_bytes: u64,
}
impl Fifo {
    pub(crate) fn insert(&mut self, id: Uuid, size: u64, file: impl Into<CacheFile>) {
        if self.entries.contains_key(&id) {
            return;
        }
        let queue = if self.ghost.remove(&id) {
            Queue::Main
        } else {
            Queue::Small
        };
        if queue == Queue::Small {
            self.small.push_back(id);
            self.small_bytes += size;
        } else {
            self.main.push_back(id);
        }
        self.entries.insert(
            id,
            Entry {
                size,
                file: file.into(),
                frequency: 0,
                pins: Arc::new(AtomicUsize::new(0)),
                queue,
                valid: true,
            },
        );
    }
    pub(crate) fn remove(&mut self, id: Uuid) -> Option<Entry> {
        let e = self.entries.remove(&id)?;
        if e.queue == Queue::Small {
            self.small_bytes -= e.size;
        }
        Some(e)
    }
    pub(crate) fn forget(&mut self, id: Uuid) -> Option<Entry> {
        let e = self.remove(id)?;
        self.small.retain(|v| *v != id);
        self.main.retain(|v| *v != id);
        Some(e)
    }
    pub(crate) fn restore_victim(&mut self, id: Uuid) {
        match self.entries[&id].queue {
            Queue::Small => self.small.push_front(id),
            Queue::Main => self.main.push_front(id),
        }
    }
    // Commit eviction only after the cache file is gone.
    pub(crate) fn evict(&mut self, id: Uuid, max_ghosts: usize) -> Option<Entry> {
        let e = self.remove(id)?;
        if e.queue == Queue::Small {
            self.ghost.insert(id);
            self.ghosts.push_back(id);
            while self.ghosts.len() > max_ghosts {
                if let Some(old) = self.ghosts.pop_front() {
                    self.ghost.remove(&old);
                }
            }
        }
        Some(e)
    }
    pub(crate) fn victim(&mut self, capacity: u64) -> Option<Uuid> {
        // A bounded scan also terminates when every candidate is pinned.
        for _ in 0..self.entries.len().saturating_mul(5).max(1) {
            let small = (self.small_bytes > capacity / 10 || self.main.is_empty())
                && !self.small.is_empty();
            let id = if small {
                self.small.pop_front()
            } else {
                self.main.pop_front()
            }?;
            let Some(e) = self.entries.get_mut(&id) else {
                continue;
            };
            if e.file.is_pending() || e.pins.load(Ordering::Relaxed) > 0 {
                if small {
                    self.small.push_back(id);
                } else {
                    self.main.push_back(id);
                }
                continue;
            }
            if small && e.frequency > 1 {
                e.queue = Queue::Main;
                e.frequency = 0;
                self.small_bytes -= e.size;
                self.main.push_back(id);
                continue;
            }
            if !small && e.frequency > 0 {
                e.frequency -= 1;
                self.main.push_back(id);
                continue;
            }
            return Some(id);
        }
        None
    }
}

pub(crate) type FetchResult =
    tokio::sync::OnceCell<Result<(Bytes, Option<Arc<LoadedPack>>, usize), Arc<anyhow::Error>>>;
pub(crate) struct PackFetch {
    pub(crate) result: tokio::sync::OnceCell<Result<Arc<LoadedPack>, Arc<anyhow::Error>>>,
    pub(crate) priority: Arc<AtomicUsize>,
}
