use crate::model::Id;
use crate::{
    app::StoredStream,
    model::{CachedExtent, ReadExtent},
};
use bytes::Bytes;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
    },
};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

impl super::App {
    pub(crate) fn cache_received(
        self: &Arc<Self>,
        id: Id,
        bucket: Uuid,
        path: String,
        etag: String,
        received: Option<Received>,
    ) {
        let Some(received) = received else { return };
        let app = self.clone();
        tokio::spawn(async move {
            let work = async {
                #[cfg(feature = "fault-injection")]
                crate::faults::point("response-upload-before-admit").await;
                let stream = app.stream_store.current(bucket, &path).await?;
                if stream.id != id
                    || stream.etag != etag
                    || stream.size != received.bytes.len() as i64
                {
                    return Ok::<_, anyhow::Error>(());
                }
                let metadata: super::Metadata = serde_json::from_value(stream.metadata.clone())?;
                let headers = crate::http::object::metadata_headers(&metadata)?;
                if !crate::http::object::cacheable(&headers) {
                    return Ok(());
                }
                if let Some(mapping) = app.response_mapping(&stream).await? {
                    received.publish(&app.response_cache, &stream, mapping.0, mapping.1);
                }
                Ok(())
            };
            // Warming is optional; it must not prolong the accepted write or retain candidates indefinitely.
            let _ = tokio::time::timeout(std::time::Duration::from_secs(2), work).await;
        });
    }

    pub(crate) async fn response_mapping(
        &self,
        stream: &StoredStream,
    ) -> anyhow::Result<Option<([u8; 32], Vec<Uuid>)>> {
        let mut mapping = Mapping::new(stream.size);
        if stream.payload_type == "raw" {
            let rows = if let Some(row) = super::reads::raw(stream.id, stream.payload_revision) {
                vec![row]
            } else {
                self.raw.page(stream.id, 0, stream.size).await?
            };
            for row in &rows {
                self.storage.profile(row.source.storage_id)?;
                mapping.record_raw(row, stream.payload_revision);
            }
        } else {
            for row in self.chunks.read_extents(stream.id, 0, stream.size).await? {
                if let Some(storage) = row.chunk.storage_id {
                    self.storage.profile(storage)?;
                }
                mapping.record(&row);
            }
        }
        Ok(mapping
            .signature()
            .map(|signature| (signature, mapping.storages)))
    }
}

const UNIT: usize = 4096;
pub(super) const MEMORY_UNIT: usize = 256;
const METADATA: usize = 1024 * 1024;
// Covers entry/index overhead and two bounded admission-history keys per entry.
const ENTRY_METADATA: usize = 2048;

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub(crate) struct Key {
    pub bucket: Uuid,
    pub stream: Id,
    pub encoding: &'static str,
    pub policy: i32,
}

#[derive(Default)]
struct Counters {
    hits: AtomicU64,
    misses: AtomicU64,
    rejected: AtomicU64,
    abandoned: AtomicU64,
    retired: AtomicU64,
    validated: AtomicU64,
}

pub(crate) struct Entry {
    pub bytes: Bytes,
    pub raw_size: i64,
    pub etag: String,
    pub mapping: [u8; 32],
    storages: Vec<Uuid>,
    path: [u8; 32],
    ownership: Arc<Ownership>,
    checks: Vec<Arc<super::checks::Evidence>>,
    read_epoch: u64,
    referenced: AtomicBool,
}

impl Entry {
    pub(crate) fn usable(&self, epoch: u64) -> bool {
        self.checks.iter().all(|check| check.cacheable())
            && (self.checks.iter().all(|check| check.verified()) || self.read_epoch == epoch)
    }
}

struct Ownership {
    counters: Arc<Counters>,
    retired: AtomicBool,
    charge: u64,
}
impl Ownership {
    fn retire(&self) {
        if !self.retired.swap(true, Relaxed) {
            self.counters.retired.fetch_add(self.charge, Relaxed);
        }
    }
}
struct Allocation {
    bytes: Vec<u8>,
    _memory: OwnedSemaphorePermit,
    ownership: Arc<Ownership>,
}
impl AsRef<[u8]> for Allocation {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl Drop for Allocation {
    fn drop(&mut self) {
        if self.ownership.retired.load(Relaxed) {
            self.ownership
                .counters
                .retired
                .fetch_sub(self.ownership.charge, Relaxed);
        }
    }
}

#[derive(Default)]
struct Index {
    slots: usize,
    _memory: Vec<OwnedSemaphorePermit>,
    ready: HashMap<Key, Arc<Entry>>,
    blocked: HashSet<Uuid>,
    generation: u64,
    order: VecDeque<Key>,
    seen: HashSet<Key>,
    history: VecDeque<Key>,
    builders: HashMap<Key, Arc<Notify>>,
    paths: HashMap<[u8; 32], usize>,
}

impl Index {
    fn remove(&mut self, key: Key) {
        if let Some(entry) = self.ready.remove(&key) {
            entry.ownership.retire();
            if let Some(count) = self.paths.get_mut(&entry.path) {
                *count -= 1;
                if *count == 0 {
                    self.paths.remove(&entry.path);
                }
            }
        }
    }
}

pub(super) fn path_key(bucket: Uuid, key: &str) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(bucket.as_bytes());
    hash.update(key.as_bytes());
    *hash.finalize().as_bytes()
}

pub(crate) struct Cache {
    limit: usize,
    pub(super) public_bytes: usize,
    entries: usize,
    entry_limit: usize,
    pub(super) memory: Arc<Semaphore>,
    pub(super) public_memory: Arc<Semaphore>,
    building: Arc<Semaphore>,
    waiters: Arc<Semaphore>,
    uploads: Arc<Semaphore>,
    index: Mutex<Index>,
    counters: Arc<Counters>,
}

impl Cache {
    pub fn new(bytes: u64) -> Arc<Self> {
        let limit = bytes as usize;
        let public_bytes = super::public_reads::extra_budget(bytes);
        let metadata = if limit == 0 {
            0
        } else {
            ((limit - public_bytes) / 8).max(1024 * 1024).div_ceil(UNIT) * UNIT
        };
        Arc::new(Self {
            limit,
            public_bytes,
            entries: metadata / ENTRY_METADATA,
            entry_limit: (limit / 16).min(8 * 1024 * 1024),
            memory: Arc::new(Semaphore::new(limit.saturating_sub(METADATA) / MEMORY_UNIT)),
            public_memory: Arc::new(Semaphore::new(public_bytes / MEMORY_UNIT)),
            building: Arc::new(Semaphore::new(limit / 4 / UNIT)),
            waiters: Arc::new(Semaphore::new(32)),
            uploads: Arc::new(Semaphore::new(16)),
            index: Mutex::new(Index {
                slots: if limit == 0 {
                    0
                } else {
                    METADATA / ENTRY_METADATA
                },
                ..Index::default()
            }),
            counters: Arc::new(Counters::default()),
        })
    }

    pub(crate) fn storage_policy(&self, storage: Uuid, blocked: bool) {
        let mut index = self.index.lock().unwrap();
        let changed = if blocked {
            index.blocked.insert(storage)
        } else {
            index.blocked.remove(&storage)
        };
        if !changed {
            return;
        }
        index.generation = index.generation.wrapping_add(1);
        let retired: Vec<_> = index
            .ready
            .iter()
            .filter(|(_, entry)| entry.storages.contains(&storage))
            .map(|(key, _)| *key)
            .collect();
        for key in retired {
            index.remove(key);
        }
        let ready: HashSet<_> = index.ready.keys().copied().collect();
        index.order.retain(|key| ready.contains(key));
    }

    pub fn eligible(&self, stream: &StoredStream) -> bool {
        self.eligible_size(stream.size)
    }

    fn eligible_size(&self, size: i64) -> bool {
        self.limit >= 4 * 1024 * 1024 && size > 0 && size as u64 <= self.entry_limit as u64
    }

    pub(crate) fn receive(&self, bucket: Uuid, path: &str, size: Option<i64>) -> Option<Upload> {
        let size = size?;
        // Preserve hot-path admission: write-only objects must not displace useful read data.
        if !self.eligible_size(size) || !self.candidate(bucket, path) {
            return None;
        }
        let slot = self.uploads.clone().try_acquire_owned().ok()?;
        let (memory, building) = self.reserve(size as usize)?;
        Some(Upload {
            generation: self.index.lock().unwrap().generation,
            bytes: Vec::with_capacity(size as usize),
            size: size as usize,
            memory,
            building,
            slot,
        })
    }

    pub fn get(&self, key: Key) -> Option<Arc<Entry>> {
        let index = self.index.lock().unwrap();
        let entry = index.ready.get(&key)?;
        entry.referenced.store(true, Relaxed);
        Some(entry.clone())
    }

    pub fn candidate(&self, bucket: Uuid, key: &str) -> bool {
        self.limit > 0
            && self
                .index
                .lock()
                .unwrap()
                .paths
                .contains_key(&path_key(bucket, key))
    }

    pub fn validated(&self) {
        self.counters.validated.fetch_add(1, Relaxed);
    }

    pub async fn lookup(&self, key: Key) -> Option<Arc<Entry>> {
        if let Some(entry) = self.get(key) {
            return Some(entry);
        }
        let signal = self.index.lock().unwrap().builders.get(&key).cloned();
        if let Some(signal) = signal
            && let Ok(_waiter) = self.waiters.clone().try_acquire_owned()
        {
            let notified = signal.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(entry) = self.get(key) {
                return Some(entry);
            }
            let _ = tokio::time::timeout(std::time::Duration::from_millis(25), notified).await;
            if let Some(entry) = self.get(key) {
                return Some(entry);
            }
        }
        self.counters.misses.fetch_add(1, Relaxed);
        None
    }

    pub fn hit(&self) {
        self.counters.hits.fetch_add(1, Relaxed);
    }

    pub fn remove(&self, key: Key, expected: &Arc<Entry>) {
        let mut index = self.index.lock().unwrap();
        if index
            .ready
            .get(&key)
            .is_some_and(|entry| Arc::ptr_eq(entry, expected))
        {
            index.remove(key);
            index.order.retain(|old| *old != key);
        }
    }

    fn evict(index: &mut Index) -> bool {
        // Cache admission must not scan the whole hot set while holding the read index.
        for _ in 0..64 {
            let Some(key) = index.order.pop_front() else {
                return false;
            };
            if index
                .ready
                .get(&key)
                .is_some_and(|entry| entry.referenced.swap(false, Relaxed))
            {
                index.order.push_back(key);
                continue;
            }
            index.remove(key);
            return true;
        }
        false
    }

    fn grow_index(&self, index: &mut Index, needed: usize) -> bool {
        if needed <= index.slots {
            return true;
        }
        let slots = needed.div_ceil(256).saturating_mul(256).min(self.entries);
        if slots <= index.slots {
            return false;
        }
        let bytes = (slots - index.slots) * ENTRY_METADATA;
        let Ok(memory) = self
            .memory
            .clone()
            .try_acquire_many_owned((bytes / MEMORY_UNIT) as u32)
        else {
            return false;
        };
        // Hash tables retain capacity after eviction, so keep their high-water charge too.
        index._memory.push(memory);
        index.slots = slots;
        true
    }

    fn reserve(&self, capacity: usize) -> Option<(OwnedSemaphorePermit, OwnedSemaphorePermit)> {
        let units = capacity.div_ceil(UNIT) as u32;
        let building = self.building.clone().try_acquire_many_owned(units).ok()?;
        let mut index = self.index.lock().unwrap();
        let mut stalled = 0;
        loop {
            if let Ok(memory) = self
                .memory
                .clone()
                .try_acquire_many_owned(units * (UNIT / MEMORY_UNIT) as u32)
            {
                return Some((memory, building));
            }
            let available = self.memory.available_permits();
            if stalled == 4 || !Self::evict(&mut index) {
                return None;
            }
            // Bytes slices can retain allocations after their entries have been evicted.
            stalled = if self.memory.available_permits() <= available {
                stalled + 1
            } else {
                0
            };
        }
    }

    pub fn begin(self: &Arc<Self>, key: Key, stream: &StoredStream, path: &str) -> Option<Builder> {
        if !self.eligible(stream) {
            return None;
        }
        let mut index = self.index.lock().unwrap();
        if index.ready.contains_key(&key) || index.builders.contains_key(&key) {
            return None;
        }
        if !index.seen.contains(&key) {
            let needed = (index.history.len() + 1).div_ceil(2);
            if !self.grow_index(&mut index, needed)
                && let Some(old) = index.history.pop_front()
            {
                index.seen.remove(&old);
            }
            index.seen.insert(key);
            index.history.push_back(key);
            return None;
        }
        if index.builders.len() == 16 {
            self.counters.rejected.fetch_add(1, Relaxed);
            return None;
        }
        let signal = Arc::new(Notify::new());
        index.builders.insert(key, signal.clone());
        Some(Builder {
            generation: index.generation,
            cache: Arc::downgrade(self),
            key,
            path: path_key(key.bucket, path),
            raw_size: stream.size,
            etag: stream.etag.clone(),
            capture: Arc::new(Mutex::new(Mapping::new(stream.size))),
            signal,
            buffer: None,
            published: false,
        })
    }

    pub fn snapshot(&self) -> serde_json::Value {
        let index = self.index.lock().unwrap();
        let metadata = index.slots * ENTRY_METADATA;
        let public = self.public_bytes - self.public_memory.available_permits() * MEMORY_UNIT;
        let live = self
            .limit
            .saturating_sub(metadata + public + self.memory.available_permits() * MEMORY_UNIT);
        serde_json::json!({"limit_bytes":self.limit,"live_bytes":live,"metadata_bytes":metadata,"public_bytes":public,"public_limit_bytes":self.public_bytes,"entry_limit":self.entries,"candidate_bytes":self.limit/4-self.building.available_permits()*UNIT,"retired_bytes":self.counters.retired.load(Relaxed),"entries":index.ready.len(),"paths":index.paths.len(),"building":index.builders.len(),"hits":self.counters.hits.load(Relaxed),"validated_hits":self.counters.validated.load(Relaxed),"misses":self.counters.misses.load(Relaxed),"rejected":self.counters.rejected.load(Relaxed),"abandoned":self.counters.abandoned.load(Relaxed)})
    }
}

struct Buffer {
    bytes: Vec<u8>,
    memory: OwnedSemaphorePermit,
    _building: OwnedSemaphorePermit,
}

pub(crate) struct Upload {
    generation: u64,
    bytes: Vec<u8>,
    size: usize,
    memory: OwnedSemaphorePermit,
    building: OwnedSemaphorePermit,
    slot: OwnedSemaphorePermit,
}

pub(crate) struct Received {
    generation: u64,
    pub bytes: Bytes,
    ownership: Arc<Ownership>,
    _building: OwnedSemaphorePermit,
    _slot: OwnedSemaphorePermit,
}

impl Upload {
    pub fn append(&mut self, data: &[u8]) -> anyhow::Result<()> {
        anyhow::ensure!(
            data.len() <= self.size - self.bytes.len(),
            "response receive length exceeded"
        );
        self.bytes.extend_from_slice(data);
        Ok(())
    }

    pub fn finish(self, cache: &Cache) -> anyhow::Result<Received> {
        anyhow::ensure!(
            self.bytes.len() == self.size,
            "response receive length mismatch"
        );
        let ownership = Arc::new(Ownership {
            counters: cache.counters.clone(),
            retired: false.into(),
            charge: self.memory.num_permits() as u64 * MEMORY_UNIT as u64,
        });
        Ok(Received {
            generation: self.generation,
            bytes: Bytes::from_owner(Allocation {
                bytes: self.bytes,
                _memory: self.memory,
                ownership: ownership.clone(),
            }),
            ownership,
            _building: self.building,
            _slot: self.slot,
        })
    }
}

impl Received {
    fn publish(self, cache: &Cache, stream: &StoredStream, mapping: [u8; 32], storages: Vec<Uuid>) {
        let key = Key {
            bucket: stream.bucket_id,
            stream: stream.id,
            encoding: "identity",
            policy: crate::http::object::POLICY,
        };
        let path = path_key(stream.bucket_id, &stream.object_key);
        let mut index = cache.index.lock().unwrap();
        if index.generation != self.generation
            || storages.iter().any(|id| index.blocked.contains(id))
            || index.ready.contains_key(&key)
            || index.builders.contains_key(&key)
        {
            return;
        }
        let needed = index.ready.len() + 1;
        cache.grow_index(&mut index, needed);
        while index.ready.len() >= index.slots {
            if !Cache::evict(&mut index) {
                return;
            }
        }
        *index.paths.entry(path).or_default() += 1;
        index.ready.insert(
            key,
            Arc::new(Entry {
                raw_size: stream.size,
                bytes: self.bytes,
                etag: stream.etag.clone(),
                mapping,
                storages,
                path,
                ownership: self.ownership,
                checks: Vec::new(),
                read_epoch: 0,
                referenced: false.into(),
            }),
        );
        index.order.push_back(key);
    }
}

pub(crate) struct Builder {
    generation: u64,
    cache: Weak<Cache>,
    key: Key,
    path: [u8; 32],
    raw_size: i64,
    etag: String,
    pub capture: Arc<Mutex<Mapping>>,
    signal: Arc<Notify>,
    buffer: Option<Buffer>,
    published: bool,
}

impl Builder {
    pub fn len(&self) -> usize {
        self.buffer.as_ref().map_or(0, |buffer| buffer.bytes.len())
    }
    pub fn source_complete(&self) -> bool {
        self.capture.lock().unwrap().finished
    }

    pub fn append(&mut self, bytes: &[u8]) -> bool {
        let Some(cache) = self.cache.upgrade() else {
            return false;
        };
        {
            let capture = self.capture.lock().unwrap();
            let index = cache.index.lock().unwrap();
            if !capture.valid
                || index.generation != self.generation
                || capture.storages.iter().any(|id| index.blocked.contains(id))
            {
                return false;
            }
        }
        let size = self.len().saturating_add(bytes.len());
        if size > cache.entry_limit {
            return false;
        }
        let capacity = self
            .buffer
            .as_ref()
            .map_or(0, |buffer| buffer.bytes.capacity());
        if size > capacity {
            let next = size
                .max(capacity.saturating_mul(2))
                .max(UNIT)
                .div_ceil(UNIT)
                * UNIT;
            let next = next.min(cache.entry_limit);
            // Reserve both allocations during growth; cancellation must not hide the copying peak.
            let Some((memory, building)) = cache.reserve(next) else {
                cache.counters.rejected.fetch_add(1, Relaxed);
                return false;
            };
            let mut next = Buffer {
                bytes: Vec::with_capacity(next),
                memory,
                _building: building,
            };
            if let Some(old) = self.buffer.take() {
                next.bytes.extend_from_slice(&old.bytes);
            }
            self.buffer = Some(next);
        }
        if let Some(buffer) = &mut self.buffer {
            buffer.bytes.extend_from_slice(bytes);
        }
        true
    }

    pub fn finish(mut self) {
        if !self.source_complete() {
            return;
        }
        let Some(cache) = self.cache.upgrade() else {
            return;
        };
        let (mapping, checks, read_epoch, storages) = {
            let capture = self.capture.lock().unwrap();
            let Some(mapping) = capture.signature() else {
                return;
            };
            if !capture.checks.iter().all(|check| check.cacheable()) {
                return;
            }
            (
                mapping,
                capture
                    .checks
                    .iter()
                    .filter(|check| !check.verified())
                    .cloned()
                    .collect(),
                capture.read_epoch,
                capture.storages.clone(),
            )
        };
        let Some(buffer) = self.buffer.take() else {
            return;
        };
        if self.key.encoding == "identity" && buffer.bytes.len() as i64 != self.raw_size {
            return;
        }
        let ownership = Arc::new(Ownership {
            counters: cache.counters.clone(),
            retired: false.into(),
            charge: buffer.memory.num_permits() as u64 * MEMORY_UNIT as u64,
        });
        let bytes = Bytes::from_owner(Allocation {
            bytes: buffer.bytes,
            _memory: buffer.memory,
            ownership: ownership.clone(),
        });
        drop(buffer._building);
        let mut index = cache.index.lock().unwrap();
        if index.generation != self.generation
            || storages.iter().any(|id| index.blocked.contains(id))
        {
            return;
        }
        let entry = Arc::new(Entry {
            bytes,
            raw_size: self.raw_size,
            etag: std::mem::take(&mut self.etag),
            mapping,
            storages,
            checks,
            read_epoch,
            path: self.path,
            ownership,
            referenced: false.into(),
        });
        let needed = index.ready.len() + 1;
        cache.grow_index(&mut index, needed);
        while index.ready.len() >= index.slots {
            if !Cache::evict(&mut index) {
                return;
            }
        }
        if index.ready.contains_key(&self.key) {
            index.remove(self.key);
            index.order.retain(|key| *key != self.key);
        }
        *index.paths.entry(self.path).or_default() += 1;
        index.ready.insert(self.key, entry);
        index.order.push_back(self.key);
        self.published = true;
    }
}
impl Drop for Builder {
    fn drop(&mut self) {
        if let Some(cache) = self.cache.upgrade() {
            cache.index.lock().unwrap().builders.remove(&self.key);
            if !self.published {
                cache.counters.abandoned.fetch_add(1, Relaxed);
            }
        }
        self.signal.notify_waiters();
    }
}

pub(crate) struct Mapping {
    hash: blake3::Hasher,
    storages: Vec<Uuid>,
    size: i64,
    position: i64,
    count: usize,
    valid: bool,
    finished: bool,
    checks: Vec<Arc<super::checks::Evidence>>,
    read_epoch: u64,
}
impl Mapping {
    pub fn new(size: i64) -> Self {
        Self {
            hash: blake3::Hasher::new(),
            storages: Vec::new(),
            size,
            position: 0,
            count: 0,
            valid: true,
            finished: false,
            checks: Vec::new(),
            read_epoch: 0,
        }
    }
    pub fn start_read(&mut self, epoch: u64) {
        self.read_epoch = epoch;
    }
    pub fn record_check(&mut self, check: Arc<super::checks::Evidence>) {
        if self
            .checks
            .iter()
            .any(|current| Arc::ptr_eq(current, &check))
        {
            return;
        }
        if self.checks.len() == 64 {
            self.valid = false;
            return;
        }
        self.checks.push(check);
    }
    pub fn finish_source(&mut self) {
        self.finished = true;
    }
    pub fn record_raw(&mut self, row: &crate::model::raw::RawRange, revision: i64) {
        // Only complete immutable files can be verified against their full-source digest.
        self.valid &= self.count == 0
            && row.validate().is_ok()
            && row.offset_bytes == 0
            && row.source_offset == 0
            && row.length == self.size
            && row.source.size == self.size;
        if !self.valid {
            return;
        }
        self.storages.push(row.source.storage_id);
        self.hash.update(b"raw");
        self.hash.update(row.source.location_id.as_bytes());
        self.hash.update(row.source.storage_id.as_bytes());
        for value in [revision, row.source.revision, row.source.size] {
            self.hash.update(&value.to_le_bytes());
        }
        self.hash.update(row.source.digest.as_ref().unwrap());
        self.position = self.size;
        self.count = 1;
    }
    pub fn record(&mut self, row: &ReadExtent) {
        if let Some(id) = row.chunk.storage_id
            && !self.storages.contains(&id)
        {
            if self.storages.len() == 8 {
                self.valid = false;
                return;
            }
            self.storages.push(id);
        }
        if row.extent.chunk_id != Some(row.chunk.id) || row.chunk.state != "ready" {
            self.valid = false;
        }
        self.record_values(
            row.extent.offset_bytes,
            row.extent.length,
            row.extent.source_offset,
            row.chunk.id,
            row.chunk.raw_size,
            &row.chunk.hash,
        );
    }
    pub fn record_cached(&mut self, row: &CachedExtent) {
        match (
            row.cache_offset,
            row.cache_length,
            row.cache_source,
            row.cache_chunk,
            row.cache_ready,
            row.cache_size,
            row.cache_hash.as_deref(),
        ) {
            (
                Some(offset),
                Some(length),
                Some(source),
                Some(chunk),
                Some(ready),
                Some(size),
                Some(hash),
            ) if chunk == ready => self.record_values(offset, length, source, chunk, size, hash),
            _ => self.valid = false,
        }
    }
    fn record_values(
        &mut self,
        offset: i64,
        length: i32,
        source: i32,
        chunk: Id,
        size: i32,
        hash: &[u8],
    ) {
        if !self.valid {
            return;
        }
        self.count += 1;
        let end = self.position.checked_add(i64::from(length));
        self.valid &= self.count <= 64
            && offset == self.position
            && length > 0
            && source >= 0
            && i64::from(source) + i64::from(length) <= i64::from(size)
            && end.is_some_and(|end| end <= self.size)
            && hash.len() == 32;
        if !self.valid {
            return;
        }
        for value in [
            offset,
            i64::from(length),
            i64::from(source),
            i64::from(size),
        ] {
            self.hash.update(&value.to_le_bytes());
        }
        match chunk {
            Id::Integer(id) => self.hash.update(&id.to_le_bytes()),
            Id::Uuid(id) => self.hash.update(id.as_bytes()),
        };
        self.hash.update(hash);
        self.position = end.unwrap();
    }
    pub fn signature(&self) -> Option<[u8; 32]> {
        (self.valid && self.position == self.size && self.count > 0)
            .then(|| *self.hash.finalize().as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{app::Extent, content::codec::Chunk};
    use serde_json::json;

    #[test]
    fn pinned_allocations_do_not_flush_the_cache_for_an_unfunded_candidate() {
        let cache = Cache::new(4 * 1024 * 1024);
        let mut retained = Vec::new();
        for _ in 0..24 {
            let (stream, key, row) = fixture(128 * 1024);
            assert!(cache.begin(key, &stream, &stream.object_key).is_none());
            let mut builder = cache.begin(key, &stream, &stream.object_key).unwrap();
            builder.capture.lock().unwrap().record(&row);
            builder.capture.lock().unwrap().finish_source();
            assert!(builder.append(&vec![7; 128 * 1024]));
            builder.finish();
            // Retain only a Bytes slice, not the Entry Arc.
            retained.push(cache.get(key).unwrap().bytes.slice(0..1));
        }
        assert_eq!(cache.snapshot()["live_bytes"], 3 * 1024 * 1024);
        let (stream, key, row) = fixture(4096);
        assert!(cache.begin(key, &stream, &stream.object_key).is_none());
        let mut builder = cache.begin(key, &stream, &stream.object_key).unwrap();
        assert!(!builder.append(&[1; 4096]));
        drop(builder);
        let status = cache.snapshot();
        assert!(status["entries"].as_u64().unwrap() >= 20, "{status}");
        assert_eq!(status["live_bytes"], 3 * 1024 * 1024);
        assert_eq!(status["retired_bytes"], 4 * 128 * 1024);
        assert_eq!(status["candidate_bytes"], 0);
        assert!(retained.iter().all(|bytes| bytes.as_ref() == [7]));
        drop(retained);
        assert_eq!(cache.snapshot()["retired_bytes"], 0);
        let mut builder = cache.begin(key, &stream, &stream.object_key).unwrap();
        builder.capture.lock().unwrap().record(&row);
        builder.capture.lock().unwrap().finish_source();
        assert!(builder.append(&[1; 4096]));
        builder.finish();
        assert_eq!(cache.get(key).unwrap().bytes.as_ref(), &[1; 4096]);
    }

    #[test]
    fn hot_index_pressure_can_skip_admission_and_retry_without_losing_entries() {
        let mut cache = Cache::new(4 * 1024 * 1024);
        let limits = Arc::get_mut(&mut cache).unwrap();
        limits.entries = 80;
        limits.index.get_mut().unwrap().slots = 80;
        let fill = |stream: &StoredStream, key, row: &ReadExtent| {
            let mut builder = cache.begin(key, stream, &stream.object_key).unwrap();
            builder.capture.lock().unwrap().record(row);
            builder.capture.lock().unwrap().finish_source();
            assert!(builder.append(&[7; 4096]));
            builder.finish();
        };
        for _ in 0..80 {
            let (stream, key, row) = fixture(4096);
            assert!(cache.begin(key, &stream, &stream.object_key).is_none());
            fill(&stream, key, &row);
            assert!(cache.get(key).is_some());
        }
        let (stream, key, row) = fixture(4096);
        assert!(cache.begin(key, &stream, &stream.object_key).is_none());
        fill(&stream, key, &row);
        assert!(cache.get(key).is_none());
        assert_eq!(cache.snapshot()["entries"], 80);
        assert_eq!(cache.snapshot()["candidate_bytes"], 0);
        fill(&stream, key, &row);
        assert!(cache.get(key).is_some());
        assert_eq!(cache.snapshot()["entries"], 80);
    }

    #[test]
    fn eviction_gives_used_bodies_a_second_chance_without_keeping_cold_entries_forever() {
        let mut cache = Cache::new(4 * 1024 * 1024);
        let limits = Arc::get_mut(&mut cache).unwrap();
        limits.entries = 3;
        limits.index.get_mut().unwrap().slots = 3;
        let insert = || {
            let (stream, key, row) = fixture(8192);
            assert!(cache.begin(key, &stream, &stream.object_key).is_none());
            let mut builder = cache.begin(key, &stream, &stream.object_key).unwrap();
            builder.capture.lock().unwrap().record(&row);
            builder.capture.lock().unwrap().finish_source();
            assert!(builder.append(&[7; 8192]));
            builder.finish();
            key
        };
        let hot = insert();
        let cold = insert();
        let other = insert();
        assert!(cache.get(hot).is_some());
        let scan = insert();
        {
            let index = cache.index.lock().unwrap();
            assert!(index.ready.contains_key(&hot));
            assert!(!index.ready.contains_key(&cold));
            assert!(index.ready.contains_key(&other));
            assert!(index.ready.contains_key(&scan));
        }
        insert();
        insert();
        let index = cache.index.lock().unwrap();
        assert!(!index.ready.contains_key(&hot));
        assert_eq!(index.ready.len(), 3);
        assert_eq!(index.order.len(), 3);
    }

    #[test]
    fn response_budget_grows_small_object_capacity_without_extra_memory() {
        for bytes in [0, 4, 8, 64, 256].map(|mib| mib * 1024 * 1024) {
            let cache = Cache::new(bytes);
            assert_eq!(
                cache.memory.available_permits() * MEMORY_UNIT
                    + cache.index.lock().unwrap().slots * ENTRY_METADATA,
                bytes as usize
            );
            assert_eq!(
                cache.entry_limit,
                (bytes as usize / 16).min(8 * 1024 * 1024)
            );
        }
        let cache = Cache::new(64 * 1024 * 1024);
        let mut keys = Vec::new();
        for _ in 0..2048 {
            let (stream, key, row) = fixture(4096);
            assert!(cache.begin(key, &stream, &stream.object_key).is_none());
            let mut builder = cache.begin(key, &stream, &stream.object_key).unwrap();
            builder.capture.lock().unwrap().record(&row);
            builder.capture.lock().unwrap().finish_source();
            assert!(builder.append(&[7; 4096]));
            builder.finish();
            keys.push(key);
        }
        assert!(keys.iter().all(|key| cache.get(*key).is_some()));
        assert_eq!(cache.snapshot()["live_bytes"], 2048 * 4096);
        assert!(cache.index.lock().unwrap().history.len() <= cache.entries * 2);
    }

    #[test]
    fn unused_metadata_capacity_does_not_displace_large_bodies() {
        let cache = Cache::new(64 * 1024 * 1024);
        let mut keys = Vec::new();
        for _ in 0..15 {
            let (stream, key, row) = fixture(4 * 1024 * 1024);
            assert!(cache.begin(key, &stream, &stream.object_key).is_none());
            let mut builder = cache.begin(key, &stream, &stream.object_key).unwrap();
            builder.capture.lock().unwrap().record(&row);
            builder.capture.lock().unwrap().finish_source();
            assert!(builder.append(&vec![7; 2 * 1024 * 1024]));
            assert!(builder.append(&vec![7; 2 * 1024 * 1024]));
            builder.finish();
            keys.push(key);
        }
        assert!(keys.iter().all(|key| cache.get(*key).is_some()));
        assert_eq!(cache.snapshot()["live_bytes"], 60 * 1024 * 1024);
        assert_eq!(cache.snapshot()["metadata_bytes"], METADATA);
        assert_eq!(cache.snapshot()["public_bytes"], 0);
    }

    #[test]
    fn proxy_delivery_retires_only_its_storage_and_fences_late_admission() {
        let cache = Cache::new(4 * 1024 * 1024);
        let storage = Uuid::new_v4();
        let (mut stream, key, mut row) = fixture(8192);
        row.chunk.storage_id = Some(storage);
        assert!(cache.begin(key, &stream, &stream.object_key).is_none());
        let mut builder = cache.begin(key, &stream, &stream.object_key).unwrap();
        builder.capture.lock().unwrap().record(&row);
        builder.capture.lock().unwrap().finish_source();
        assert!(builder.append(&vec![1; 8192]));
        builder.finish();
        let mut upload = cache
            .receive(key.bucket, &stream.object_key, Some(8192))
            .unwrap();
        upload.append(&vec![2; 8192]).unwrap();
        let received = upload.finish(&cache).unwrap();
        let (other, other_key, other_row) = fixture(8192);
        assert!(cache.begin(other_key, &other, &other.object_key).is_none());
        let mut other_builder = cache.begin(other_key, &other, &other.object_key).unwrap();
        other_builder.capture.lock().unwrap().record(&other_row);
        other_builder.capture.lock().unwrap().finish_source();
        assert!(other_builder.append(&vec![3; 8192]));
        other_builder.finish();
        let (late, late_key, _) = fixture(8192);
        assert!(cache.begin(late_key, &late, &late.object_key).is_none());
        let mut late_builder = cache.begin(late_key, &late, &late.object_key).unwrap();
        late_builder.capture.lock().unwrap().record(&row);
        cache.storage_policy(storage, true);
        assert!(cache.get(key).is_none());
        assert!(cache.get(other_key).is_some());
        assert!(!late_builder.append(&vec![4; 8192]));
        cache.storage_policy(storage, false);
        stream.id = Uuid::new_v4().into();
        received.publish(&cache, &stream, [1; 32], vec![storage]);
        assert!(
            cache
                .get(Key {
                    stream: stream.id,
                    ..key
                })
                .is_none()
        );
    }

    #[test]
    fn upload_admission_reuses_the_received_allocation_and_releases_failed_candidates() {
        let cache = Cache::new(4 * 1024 * 1024);
        let (mut stream, key, row) = fixture(8192);
        assert!(
            cache
                .receive(key.bucket, &stream.object_key, Some(8192))
                .is_none()
        );
        assert!(cache.begin(key, &stream, &stream.object_key).is_none());
        let mut builder = cache.begin(key, &stream, &stream.object_key).unwrap();
        builder.capture.lock().unwrap().record(&row);
        builder.capture.lock().unwrap().finish_source();
        assert!(builder.append(&vec![1; 8192]));
        builder.finish();
        assert!(
            cache
                .receive(key.bucket, &stream.object_key, None)
                .is_none()
        );
        assert!(
            cache
                .receive(key.bucket, &stream.object_key, Some(0))
                .is_none()
        );
        assert!(
            cache
                .receive(key.bucket, &stream.object_key, Some(262145))
                .is_none()
        );
        let baseline = cache.snapshot()["live_bytes"].clone();
        let mut failed = cache
            .receive(key.bucket, &stream.object_key, Some(8192))
            .unwrap();
        assert!(failed.append(&vec![2; 8193]).is_err());
        assert!(failed.finish(&cache).is_err());
        assert_eq!(cache.snapshot()["live_bytes"], baseline);
        let mut upload = cache
            .receive(key.bucket, &stream.object_key, Some(8192))
            .unwrap();
        upload.append(&vec![3; 8192]).unwrap();
        let pointer = upload.bytes.as_ptr();
        let received = upload.finish(&cache).unwrap();
        assert_eq!(received.bytes.as_ptr(), pointer);
        stream.id = Uuid::new_v4().into();
        received.publish(&cache, &stream, [7; 32], Vec::new());
        let next = Key {
            stream: stream.id,
            ..key
        };
        let entry = cache.get(next).unwrap();
        assert_eq!(entry.bytes.as_ptr(), pointer);
        assert_eq!(entry.mapping, [7; 32]);
        assert!(entry.usable(u64::MAX));
        let reader = entry.bytes.slice(4096..);
        cache.remove(next, &entry);
        drop(entry);
        assert_eq!(cache.snapshot()["retired_bytes"], 8192);
        drop(reader);
        assert_eq!(cache.snapshot()["retired_bytes"], 0);
        assert_eq!(cache.snapshot()["live_bytes"], baseline);
        assert_eq!(cache.snapshot()["candidate_bytes"], 0);
    }

    #[test]
    fn raw_mapping_requires_a_complete_healthy_source_and_current_representation() {
        let row = crate::model::raw::RawRange {
            source: crate::model::raw::RawSource {
                id: 1.into(),
                location_id: Uuid::now_v7(),
                storage_id: Uuid::new_v4(),
                size: 8192,
                digest: Some(vec![1; 16]),
                state: "ready".into(),
                medium: "local".into(),
                pending_upload: false,
                revision: 1,
                quarantine_code: None,
            },
            offset_bytes: 0,
            length: 8192,
            source_offset: 0,
        };
        let signature = |row: &crate::model::raw::RawRange, revision| {
            let mut mapping = Mapping::new(8192);
            mapping.record_raw(row, revision);
            mapping.signature()
        };
        let original = signature(&row, 1);
        assert!(original.is_some());
        assert_ne!(original, signature(&row, 2));
        for mutate in [
            |r: &mut crate::model::raw::RawRange| r.source.revision += 1,
            |r: &mut crate::model::raw::RawRange| r.source.location_id = Uuid::new_v4(),
            |r: &mut crate::model::raw::RawRange| r.source.storage_id = Uuid::new_v4(),
            |r: &mut crate::model::raw::RawRange| r.source.digest = Some(vec![2; 16]),
            |r: &mut crate::model::raw::RawRange| {
                r.source.quarantine_code = Some("hash_mismatch".into())
            },
            |r: &mut crate::model::raw::RawRange| r.source.state = "deleting".into(),
            |r: &mut crate::model::raw::RawRange| r.source_offset = 1,
            |r: &mut crate::model::raw::RawRange| r.length -= 1,
        ] {
            let mut changed = row.clone();
            mutate(&mut changed);
            assert_ne!(original, signature(&changed, 1));
        }
        let mut repeated = Mapping::new(8192);
        repeated.record_raw(&row, 1);
        repeated.record_raw(&row, 1);
        assert!(repeated.signature().is_none());
        let (_, _, chunk) = fixture(8192);
        let mut chunks = Mapping::new(8192);
        chunks.record(&chunk);
        assert_ne!(original, chunks.signature());
    }

    fn fixture(size: i32) -> (StoredStream, Key, ReadExtent) {
        let now = chrono::Utc::now();
        let stream = StoredStream {
            id: Uuid::new_v4().into(),
            bucket_id: Uuid::new_v4(),
            object_key: "object".into(),
            stream_type: "object".into(),
            payload_type: "chunks".into(),
            payload_revision: 1,
            state: "ready".into(),
            size: i64::from(size),
            etag: "etag".into(),
            metadata: json!({}),
            public_read: true,
            checksums: json!({}),
            http_compression: json!({}),
            created_at: now,
            touched_at: now,
        };
        let key = Key {
            bucket: stream.bucket_id,
            stream: stream.id,
            encoding: "identity",
            policy: 1,
        };
        let row = ReadExtent {
            extent: Extent {
                stream_id: stream.id,
                offset_bytes: 0,
                length: size,
                chunk_id: Some(1.into()),
                fragment_id: None,
                source_offset: 0,
            },
            chunk: Chunk {
                uuid: Uuid::now_v7(),
                id: 1.into(),
                encoding_id: 1.into(),
                location_id: Uuid::new_v4(),
                storage_id: None,
                hash: vec![1; 32],
                raw_size: size,
                stored_size: Some(size),
                algorithm: "none".into(),
                block_key_id: None,
                compressed: false,
                nonce: None,
                format: 2,
                state: "ready".into(),
                created_at: now,
            },
        };
        (stream, key, row)
    }

    #[test]
    fn eviction_and_slices_keep_live_memory_charged() {
        let cache = Cache::new(4 * 1024 * 1024);
        let (stream, key, row) = fixture(8192);
        assert!(cache.begin(key, &stream, &stream.object_key).is_none());
        let mut builder = cache.begin(key, &stream, &stream.object_key).unwrap();
        builder.capture.lock().unwrap().record(&row);
        builder.capture.lock().unwrap().finish_source();
        assert!(builder.append(&vec![7; 8192]));
        builder.finish();
        assert!(cache.candidate(key.bucket, &stream.object_key));
        assert!(!cache.candidate(Uuid::new_v4(), &stream.object_key));
        assert!(!cache.candidate(key.bucket, "other"));
        let entry = cache.get(key).unwrap();
        let retained = entry.bytes.slice(0..1);
        cache.remove(key, &entry);
        assert!(!cache.candidate(key.bucket, &stream.object_key));
        drop(entry);
        assert_eq!(cache.snapshot()["live_bytes"], 8192);
        assert_eq!(cache.snapshot()["retired_bytes"], 8192);
        assert_eq!(retained.as_ref(), &[7]);
        drop(retained);
        assert_eq!(cache.snapshot()["live_bytes"], 0);
        assert_eq!(cache.snapshot()["retired_bytes"], 0);
    }

    #[tokio::test]
    async fn builders_coalesce_and_cancel_without_leaking_memory() {
        let cache = Cache::new(4 * 1024 * 1024);
        let (stream, key, row) = fixture(8192);
        assert!(cache.begin(key, &stream, &stream.object_key).is_none());
        let mut builder = cache.begin(key, &stream, &stream.object_key).unwrap();
        assert!(cache.begin(key, &stream, &stream.object_key).is_none());
        builder.capture.lock().unwrap().record(&row);
        builder.capture.lock().unwrap().finish_source();
        assert!(builder.append(&vec![7; 8192]));
        let (entry, ()) = tokio::join!(cache.lookup(key), async {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            builder.finish();
        });
        assert!(entry.is_some());
        assert_eq!(cache.snapshot()["candidate_bytes"], 0);
        cache.remove(key, &entry.unwrap());
        let mut canceled = cache.begin(key, &stream, &stream.object_key).unwrap();
        assert!(canceled.append(&vec![0; 8192]));
        assert!(!canceled.append(&vec![0; cache.entry_limit]));
        drop(canceled);
        assert_eq!(cache.snapshot()["building"], 0);
        assert_eq!(cache.snapshot()["candidate_bytes"], 0);
        assert_eq!(cache.snapshot()["live_bytes"], 0);
    }

    #[test]
    fn mapping_identity_ignores_physical_rewrites_and_rejects_gaps() {
        let (_, _, mut row) = fixture(8192);
        let mut original = Mapping::new(8192);
        original.record(&row);
        row.chunk.encoding_id = 2.into();
        row.chunk.location_id = Uuid::new_v4();
        row.chunk.compressed = true;
        let mut rewritten = Mapping::new(8192);
        rewritten.record(&row);
        assert_eq!(original.signature(), rewritten.signature());
        let mut cached = CachedExtent {
            cache_offset: Some(0),
            cache_length: Some(8192),
            cache_source: Some(0),
            cache_chunk: Some(1.into()),
            cache_ready: Some(1.into()),
            cache_size: Some(8192),
            cache_hash: Some(vec![1; 32]),
        };
        let mut snapshot = Mapping::new(8192);
        snapshot.record_cached(&cached);
        assert_eq!(original.signature(), snapshot.signature());
        cached.cache_ready = None;
        let mut missing = Mapping::new(8192);
        missing.record_cached(&cached);
        assert!(missing.signature().is_none());
        row.extent.source_offset = 1;
        let mut invalid = Mapping::new(8192);
        invalid.record(&row);
        assert!(invalid.signature().is_none());
        row.extent.source_offset = 0;
        row.chunk.hash[0] ^= 1;
        let mut changed = Mapping::new(8192);
        changed.record(&row);
        assert_ne!(original.signature(), changed.signature());
    }

    #[test]
    fn candidate_peaks_and_history_have_fixed_bounds() {
        let cache = Cache::new(4 * 1024 * 1024);
        let mut builders = Vec::new();
        for _ in 0..16 {
            let (stream, key, _) = fixture(256 * 1024);
            assert!(cache.begin(key, &stream, &stream.object_key).is_none());
            builders.push(cache.begin(key, &stream, &stream.object_key).unwrap());
        }
        let accepted = builders
            .iter_mut()
            .map(|builder| builder.append(&vec![0; 256 * 1024]))
            .filter(|accepted| *accepted)
            .count();
        assert_eq!(accepted, 4);
        assert_eq!(cache.snapshot()["candidate_bytes"], 1024 * 1024);
        drop(builders);
        assert_eq!(cache.snapshot()["live_bytes"], 0);
        assert_eq!(cache.snapshot()["building"], 0);
        for _ in 0..10_000 {
            let (stream, key, _) = fixture(1024);
            assert!(cache.begin(key, &stream, &stream.object_key).is_none());
        }
        let index = cache.index.lock().unwrap();
        assert_eq!(index.history.len(), cache.entries * 2);
        assert_eq!(index.seen.len(), cache.entries * 2);
    }
}
