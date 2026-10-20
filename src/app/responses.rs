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

const UNIT: usize = 4096;
const METADATA: usize = 1024 * 1024;
const ENTRIES: usize = 512;
const HISTORY: usize = 1024;

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
    path: [u8; 32],
    ownership: Arc<Ownership>,
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
    ready: HashMap<Key, Arc<Entry>>,
    fifo: VecDeque<Key>,
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

fn path_key(bucket: Uuid, key: &str) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(bucket.as_bytes());
    hash.update(key.as_bytes());
    *hash.finalize().as_bytes()
}

pub(crate) struct Cache {
    limit: usize,
    entry_limit: usize,
    memory: Arc<Semaphore>,
    building: Arc<Semaphore>,
    waiters: Arc<Semaphore>,
    index: Mutex<Index>,
    counters: Arc<Counters>,
}

impl Cache {
    pub fn new(bytes: u64) -> Arc<Self> {
        let limit = bytes as usize;
        Arc::new(Self {
            limit,
            entry_limit: (limit / 16).min(8 * 1024 * 1024),
            memory: Arc::new(Semaphore::new(limit.saturating_sub(METADATA) / UNIT)),
            building: Arc::new(Semaphore::new(limit / 4 / UNIT)),
            waiters: Arc::new(Semaphore::new(32)),
            index: Mutex::new(Index::default()),
            counters: Arc::new(Counters::default()),
        })
    }

    pub fn eligible(&self, stream: &StoredStream) -> bool {
        self.limit >= 4 * 1024 * 1024
            && stream.size > 0
            && stream.size as u64 <= self.entry_limit as u64
    }

    pub fn get(&self, key: Key) -> Option<Arc<Entry>> {
        self.index.lock().unwrap().ready.get(&key).cloned()
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
            index.fifo.retain(|old| *old != key);
        }
    }

    fn evict(index: &mut Index) -> bool {
        let Some(key) = index.fifo.pop_front() else {
            return false;
        };
        index.remove(key);
        true
    }

    fn reserve(&self, capacity: usize) -> Option<(OwnedSemaphorePermit, OwnedSemaphorePermit)> {
        let units = capacity.div_ceil(UNIT) as u32;
        let building = self.building.clone().try_acquire_many_owned(units).ok()?;
        let mut index = self.index.lock().unwrap();
        loop {
            if let Ok(memory) = self.memory.clone().try_acquire_many_owned(units) {
                return Some((memory, building));
            }
            if !Self::evict(&mut index) {
                return None;
            }
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
            if index.history.len() == HISTORY
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
        serde_json::json!({"limit_bytes":self.limit,"live_bytes":self.limit.saturating_sub(METADATA).saturating_sub(self.memory.available_permits()*UNIT),"metadata_bytes":if self.limit>0 {METADATA} else {0},"candidate_bytes":self.limit/4-self.building.available_permits()*UNIT,"retired_bytes":self.counters.retired.load(Relaxed),"entries":index.ready.len(),"paths":index.paths.len(),"building":index.builders.len(),"hits":self.counters.hits.load(Relaxed),"validated_hits":self.counters.validated.load(Relaxed),"misses":self.counters.misses.load(Relaxed),"rejected":self.counters.rejected.load(Relaxed),"abandoned":self.counters.abandoned.load(Relaxed)})
    }
}

struct Buffer {
    bytes: Vec<u8>,
    memory: OwnedSemaphorePermit,
    _building: OwnedSemaphorePermit,
}

pub(crate) struct Builder {
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
        if !self.capture.lock().unwrap().valid {
            return false;
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
        let Some(mapping) = self.capture.lock().unwrap().signature() else {
            return;
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
            charge: buffer.memory.num_permits() as u64 * UNIT as u64,
        });
        let bytes = Bytes::from_owner(Allocation {
            bytes: buffer.bytes,
            _memory: buffer.memory,
            ownership: ownership.clone(),
        });
        drop(buffer._building);
        let entry = Arc::new(Entry {
            bytes,
            raw_size: self.raw_size,
            etag: std::mem::take(&mut self.etag),
            mapping,
            path: self.path,
            ownership,
        });
        let mut index = cache.index.lock().unwrap();
        while index.ready.len() >= ENTRIES {
            Cache::evict(&mut index);
        }
        if index.ready.contains_key(&self.key) {
            index.remove(self.key);
            index.fifo.retain(|key| *key != self.key);
        }
        *index.paths.entry(self.path).or_default() += 1;
        index.ready.insert(self.key, entry);
        index.fifo.push_back(self.key);
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
    size: i64,
    position: i64,
    count: usize,
    valid: bool,
    finished: bool,
}
impl Mapping {
    pub fn new(size: i64) -> Self {
        Self {
            hash: blake3::Hasher::new(),
            size,
            position: 0,
            count: 0,
            valid: true,
            finished: false,
        }
    }
    pub fn finish_source(&mut self) {
        self.finished = true;
    }
    pub fn record(&mut self, row: &ReadExtent) {
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
        assert_eq!(index.history.len(), HISTORY);
        assert_eq!(index.seen.len(), HISTORY);
    }
}
