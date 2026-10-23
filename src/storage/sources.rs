use super::{Source, Storage};
use crate::{content::codec::Chunk, model::Id};
use anyhow::{Context, Result};
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

const UNIT: usize = 4096;
const LIMIT: usize = 1024 * 1024;
const INDEX: usize = 128 * 1024;
const ENTRIES: usize = (LIMIT - INDEX) / UNIT;
type Key = (Id, Id, Uuid);

struct Entry {
    request: Chunk,
    source: Source,
    profile: (Uuid, i64),
    verified: Instant,
    age: Duration,
    attempted: Mutex<Instant>,
    _memory: OwnedSemaphorePermit,
}

struct Index {
    entries: HashMap<Key, Arc<Entry>>,
    order: VecDeque<Key>,
    generation: u64,
    pruned: Instant,
}

pub(crate) struct Cache {
    index: Mutex<Index>,
    memory: Arc<Semaphore>,
    refresh: Arc<Semaphore>,
    hits: AtomicU64,
    refreshed: AtomicU64,
    failed: AtomicU64,
    skipped: AtomicU64,
}

fn key(c: &Chunk) -> Key {
    (c.id, c.encoding_id, c.location_id)
}
fn allocated(c: &Chunk) -> usize {
    c.hash.capacity()
        + c.algorithm.capacity()
        + c.nonce.as_ref().map_or(0, Vec::capacity)
        + c.state.capacity()
}
fn prune(index: &mut Index) {
    index
        .entries
        .retain(|_, entry| entry.verified.elapsed() < entry.age);
    index.order.retain(|key| index.entries.contains_key(key));
    index.pruned = Instant::now();
}

impl Cache {
    pub fn new() -> Self {
        Self {
            index: Mutex::new(Index {
                entries: HashMap::new(),
                order: VecDeque::new(),
                generation: 0,
                pruned: Instant::now(),
            }),
            memory: Arc::new(Semaphore::new(ENTRIES)),
            refresh: Arc::new(Semaphore::new(1)),
            hits: 0.into(),
            refreshed: 0.into(),
            failed: 0.into(),
            skipped: 0.into(),
        }
    }
    pub fn clear(&self) {
        let mut index = self.index.lock().unwrap();
        index.generation = index.generation.wrapping_add(1);
        index.entries.clear();
        index.order.clear();
    }
    pub fn prune(&self) {
        prune(&mut self.index.lock().unwrap());
    }
    fn generation(&self) -> u64 {
        self.index.lock().unwrap().generation
    }
    fn get(&self, c: &Chunk) -> Option<Arc<Entry>> {
        let mut index = self.index.lock().unwrap();
        if index.pruned.elapsed() >= Duration::from_secs(1) {
            prune(&mut index);
        }
        index
            .entries
            .get(&key(c))
            .filter(|entry| {
                entry.verified.elapsed() < entry.age
                    && entry.request.uuid == c.uuid
                    && entry.request.raw_size == c.raw_size
                    && entry.request.hash == c.hash
            })
            .cloned()
    }
    fn remove(&self, entry: &Arc<Entry>) {
        let mut index = self.index.lock().unwrap();
        let key = key(&entry.request);
        if index
            .entries
            .get(&key)
            .is_some_and(|current| Arc::ptr_eq(current, entry))
        {
            index.entries.remove(&key);
            index.order.retain(|old| *old != key);
        }
    }
    fn put(&self, c: &Chunk, source: Source, profile: (Uuid, i64, u64), stamp: (Instant, u64)) {
        let bytes = std::mem::size_of::<Entry>()
            + allocated(c)
            + match &source {
                Source::Chunk(c, _) => allocated(c),
                Source::Pack(p, _) => {
                    p.algorithm.capacity()
                        + p.state.capacity()
                        + p.digest.as_ref().map_or(0, Vec::capacity)
                        + p.nonce.as_ref().map_or(0, Vec::capacity)
                }
            };
        if bytes + 512 > UNIT {
            return;
        }
        let mut index = self.index.lock().unwrap();
        if index.generation != stamp.1 {
            return;
        }
        prune(&mut index);
        let key = key(c);
        index.entries.remove(&key);
        index.order.retain(|old| *old != key);
        loop {
            if let Ok(memory) = self.memory.clone().try_acquire_owned() {
                index.entries.insert(
                    key,
                    Arc::new(Entry {
                        request: c.clone(),
                        source,
                        profile: (profile.0, profile.1),
                        verified: stamp.0,
                        age: Duration::from_secs(profile.2),
                        attempted: Mutex::new(stamp.0),
                        _memory: memory,
                    }),
                );
                index.order.push_back(key);
                return;
            }
            let Some(old) = index.order.pop_front() else {
                return;
            };
            index.entries.remove(&old);
        }
    }
    pub fn snapshot(&self) -> serde_json::Value {
        self.prune();
        serde_json::json!({"limit_bytes":LIMIT,"used_bytes":LIMIT-self.memory.available_permits()*UNIT,
            "entries":self.index.lock().unwrap().entries.len(),"hits":self.hits.load(Relaxed),
            "refreshed":self.refreshed.load(Relaxed),"failed":self.failed.load(Relaxed),"skipped":self.skipped.load(Relaxed)})
    }
}

impl Storage {
    fn clone_source(&self, source: &Source) -> Source {
        match source {
            Source::Chunk(c, _) => Source::Chunk(c.clone(), self.physical_pin(c.location_id)),
            Source::Pack(p, _) => Source::Pack(p.clone(), self.physical_pin(p.location_id)),
        }
    }
    fn remember_source(&self, c: &Chunk, source: &Source, stamp: (Instant, u64)) -> Result<()> {
        let id = match source {
            Source::Chunk(c, _) => c.storage_id,
            Source::Pack(p, _) => p.storage_id,
        }
        .context("source has no storage")?;
        let profile = self.profile(id)?;
        if profile.read_source_validation != "background" {
            return Ok(());
        }
        // A cluster cache needs the current encoding identity supplied by the protected extent page.
        if self.cluster_mode
            && !matches!(source, Source::Chunk(physical, _) if !c.location_id.is_nil() && c.encoding_id == physical.encoding_id && c.location_id == physical.location_id)
        {
            return Ok(());
        }
        self.source_reads.put(
            c,
            self.clone_source(source),
            (id, profile.revision, profile.source_validation_seconds()?),
            stamp,
        );
        Ok(())
    }
    pub(super) async fn read_source(self: &Arc<Self>, c: &Chunk, user: bool) -> Result<Source> {
        if !user
            || !c.storage_id.is_some_and(|id| {
                self.profile(id)
                    .is_ok_and(|profile| profile.read_source_validation == "background")
            })
        {
            return self.source(c.id, false).await;
        }
        let gate = self.source_gate.read().await;
        if let Some(entry) = self.source_reads.get(c) {
            if self.profile(entry.profile.0).is_ok_and(|profile| {
                profile.revision == entry.profile.1
                    && profile.read_source_validation == "background"
            }) {
                self.source_reads.hits.fetch_add(1, Relaxed);
                let source = self.clone_source(&entry.source);
                drop(gate);
                self.refresh_source(entry);
                return Ok(source);
            }
            self.source_reads.remove(&entry);
        }
        drop(gate);
        let stamp = (Instant::now(), self.source_reads.generation());
        let source = self.source(c.id, false).await?;
        self.remember_source(c, &source, stamp)?;
        Ok(source)
    }
    fn refresh_source(self: &Arc<Self>, entry: Arc<Entry>) {
        let mut attempted = entry.attempted.lock().unwrap();
        if attempted.elapsed() < (entry.age / 2).max(Duration::from_secs(1)) {
            return;
        }
        *attempted = Instant::now();
        let Ok(permit) = self.source_reads.refresh.clone().try_acquire_owned() else {
            self.source_reads.skipped.fetch_add(1, Relaxed);
            return;
        };
        drop(attempted);
        let storage = self.clone();
        tokio::spawn(super::resource_gate::PRIORITY.scope(
            super::resource_gate::MAINTENANCE,
            async move {
                let _permit = permit;
                let stamp = (Instant::now(), storage.source_reads.generation());
                let result = tokio::time::timeout(Duration::from_secs(2), async {
                    #[cfg(feature = "fault-injection")]
                    crate::faults::point("source-cache-refresh").await;
                    let source = storage.source(entry.request.id, false).await?;
                    storage.remember_source(&entry.request, &source, stamp)
                })
                .await;
                match result {
                    Ok(Ok(())) => {
                        storage.source_reads.refreshed.fetch_add(1, Relaxed);
                    }
                    _ => {
                        storage.source_reads.remove(&entry);
                        storage.source_reads.failed.fetch_add(1, Relaxed);
                    }
                }
            },
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(id: i64) -> Chunk {
        Chunk {
            id: Id::Integer(id),
            uuid: Uuid::now_v7(),
            encoding_id: Id::Integer(id),
            location_id: Uuid::now_v7(),
            storage_id: Some(Uuid::now_v7()),
            hash: vec![0; 32],
            raw_size: 4,
            stored_size: Some(4),
            algorithm: "none".into(),
            block_key_id: None,
            compressed: false,
            nonce: None,
            format: 2,
            state: "ready".into(),
            created_at: chrono::Utc::now(),
        }
    }
    fn put(cache: &Cache, c: &Chunk, stamp: (Instant, u64)) -> Arc<std::sync::atomic::AtomicUsize> {
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(1));
        cache.put(
            c,
            Source::Chunk(c.clone(), super::super::CachePin(count.clone())),
            (c.storage_id.unwrap(), 1, 1),
            stamp,
        );
        count
    }
    #[test]
    fn bounded_pins_expire_and_late_builders_cannot_restore_them() {
        let cache = Cache::new();
        let c = chunk(1);
        let stamp = (Instant::now(), cache.generation());
        let pin = put(&cache, &c, stamp);
        let reader = cache.get(&c).unwrap();
        cache.clear();
        assert_eq!(pin.load(Relaxed), 1);
        assert!(cache.get(&c).is_none());
        let late = put(&cache, &c, stamp);
        assert_eq!(late.load(Relaxed), 0);
        assert!(cache.get(&c).is_none());
        drop(reader);
        assert_eq!(pin.load(Relaxed), 0);
        let expired = put(
            &cache,
            &c,
            (Instant::now() - Duration::from_secs(2), cache.generation()),
        );
        assert!(cache.get(&c).is_none());
        cache.prune();
        assert_eq!(expired.load(Relaxed), 0);
        for id in 2..ENTRIES as i64 + 20 {
            put(&cache, &chunk(id), (Instant::now(), cache.generation()));
        }
        assert!(cache.index.lock().unwrap().entries.len() <= ENTRIES);
        assert!(cache.snapshot()["used_bytes"].as_u64().unwrap() <= LIMIT as u64);
    }
}
