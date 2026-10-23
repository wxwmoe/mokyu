use super::{App, responses::Mapping};
use crate::model::{ReadRoute, ReadSnapshot};
use anyhow::Result;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use uuid::Uuid;

const UNIT: usize = 256;
const LIMIT: usize = 1024 * 1024;
const INDEX: usize = 128 * 1024;
const ENTRY_BYTES: usize = 256 * 1024;
const ENTRIES: usize = 256;
type Key = [u8; 32];

pub(crate) struct Entry {
    pub snapshot: Arc<ReadSnapshot>,
    pub mapping: Option<Key>,
    route: ReadRoute,
    profiles: Vec<(Uuid, i64)>,
    verified: Instant,
    refresh: Duration,
    age: Duration,
    attempted: Mutex<Instant>,
    _memory: OwnedSemaphorePermit,
}

#[derive(Default)]
struct Index {
    entries: HashMap<Key, Arc<Entry>>,
    order: VecDeque<Key>,
    buckets: HashMap<Key, Weak<Entry>>,
    pending: HashSet<Key>,
    generation: u64,
    changing: usize,
}

pub(crate) struct Cache {
    index: Mutex<Index>,
    memory: Arc<Semaphore>,
    sender: mpsc::Sender<Work>,
    cors: Arc<crate::http::cors::Cache>,
    hits: AtomicU64,
    refreshed: AtomicU64,
    failed: AtomicU64,
    skipped: AtomicU64,
}

pub(crate) enum Work {
    Refresh(Arc<Entry>),
    Fill {
        route: ReadRoute,
        snapshot: Arc<ReadSnapshot>,
        started: Instant,
        generation: u64,
        _memory: OwnedSemaphorePermit,
    },
}

pub(crate) struct Change<'a> {
    cache: &'a Cache,
    object: Option<(Uuid, &'a str)>,
}
impl Drop for Change<'_> {
    fn drop(&mut self) {
        let mut index = self.cache.index.lock().unwrap();
        invalidate(&mut index, self.object);
        if self.object.is_none() {
            index.changing -= 1;
            self.cache.cors.change(false);
        }
    }
}
fn invalidate(index: &mut Index, object: Option<(Uuid, &str)>) {
    index.generation = index.generation.wrapping_add(1);
    if let Some((bucket, key)) = object {
        index.entries.retain(|_, entry| {
            entry.snapshot.bucket.id != bucket
                || entry
                    .snapshot
                    .stream
                    .as_ref()
                    .is_none_or(|stream| stream.object_key != key)
        });
        index.order.retain(|key| index.entries.contains_key(key));
        index.buckets.retain(|_, entry| entry.strong_count() > 0);
    } else {
        index.entries.clear();
        index.order.clear();
        index.buckets.clear();
    }
}

fn current(index: &Index, entry: &Arc<Entry>) -> bool {
    index.changing == 0
        && index
            .entries
            .get(&key(&entry.route))
            .is_some_and(|value| Arc::ptr_eq(value, entry))
}

fn key(route: &ReadRoute) -> Key {
    route_key(route, false)
}
fn route_key(route: &ReadRoute, bucket: bool) -> Key {
    let mut hash = blake3::Hasher::new();
    let mut value = |value: &str| {
        hash.update(&(value.len() as u64).to_le_bytes());
        hash.update(value.as_bytes());
    };
    match route {
        ReadRoute::Host { host, key } => {
            value("host");
            value(host);
            value(if bucket { "" } else { key });
        }
        ReadRoute::Bucket { name, key, alias } => {
            value("bucket");
            value(name);
            value(if bucket { "" } else { key });
            if let Some((name, key)) = alias {
                value(name);
                value(if bucket { "" } else { key });
            }
        }
    }
    *hash.finalize().as_bytes()
}

fn bucket_key(route: &ReadRoute) -> Key {
    route_key(route, true)
}

fn json_bytes(value: &serde_json::Value) -> usize {
    use serde_json::Value;
    std::mem::size_of::<Value>()
        + match value {
            Value::String(value) => value.capacity(),
            Value::Array(values) => {
                values.capacity() * std::mem::size_of::<Value>()
                    + values.iter().map(json_bytes).sum::<usize>()
            }
            Value::Object(values) => values
                .iter()
                .map(|(key, value)| 256 + key.capacity() + json_bytes(value))
                .sum(),
            _ => 0,
        }
}

impl Cache {
    pub fn new(cors: Arc<crate::http::cors::Cache>) -> (Arc<Self>, mpsc::Receiver<Work>) {
        let (sender, receiver) = mpsc::channel(32);
        (
            Arc::new(Self {
                index: Mutex::new(Index::default()),
                memory: Arc::new(Semaphore::new((LIMIT - INDEX) / UNIT)),
                sender,
                cors,
                hits: 0.into(),
                refreshed: 0.into(),
                failed: 0.into(),
                skipped: 0.into(),
            }),
            receiver,
        )
    }

    pub fn changing(&self) -> Change<'_> {
        let mut index = self.index.lock().unwrap();
        invalidate(&mut index, None);
        index.changing += 1;
        self.cors.change(true);
        Change {
            cache: self,
            object: None,
        }
    }

    pub fn changing_object<'a>(&'a self, bucket: Uuid, key: &'a str) -> Change<'a> {
        // Publishers and cache workers hold coord across both invalidation boundaries.
        let object = Some((bucket, key));
        invalidate(&mut self.index.lock().unwrap(), object);
        Change {
            cache: self,
            object,
        }
    }

    pub fn generation(&self) -> u64 {
        self.index.lock().unwrap().generation
    }

    pub fn raw_current(&self, entry: &Arc<Entry>, storage: &crate::storage::Storage) -> bool {
        let index = self.index.lock().unwrap();
        if !current(&index, entry)
            || entry.mapping.is_none()
            || entry.verified.elapsed() >= entry.age
        {
            return false;
        }
        let Some(raw) = &entry.snapshot.raw else {
            return false;
        };
        entry.profiles.iter().all(|(id, revision)| {
            storage.profile(*id).is_ok_and(|profile| {
                profile.revision == *revision
                    && profile.public_cache_validation == "background"
                    && profile.read_source_validation == "background"
                    && profile.source_validation_seconds().is_ok_and(|seconds| {
                        entry.verified.elapsed() < Duration::from_secs(seconds)
                    })
            })
        }) && raw.source.validate().is_ok()
    }

    pub fn get(&self, route: &ReadRoute, storage: &crate::storage::Storage) -> Option<Arc<Entry>> {
        let index = self.index.lock().unwrap();
        if index.changing != 0 {
            return None;
        }
        let entry = index.entries.get(&key(route))?.clone();
        drop(index);
        if entry.verified.elapsed() >= entry.age
            || entry.profiles.iter().any(|(id, revision)| {
                storage.profile(*id).map_or(true, |profile| {
                    profile.revision != *revision || profile.public_cache_validation != "background"
                })
            })
        {
            return None;
        }
        self.hits.fetch_add(1, Relaxed);
        let mut attempted = entry.attempted.lock().unwrap();
        if attempted.elapsed() >= entry.refresh
            && self.index.lock().unwrap().pending.insert(key(route))
        {
            *attempted = Instant::now();
            if self.sender.try_send(Work::Refresh(entry.clone())).is_err() {
                self.index.lock().unwrap().pending.remove(&key(route));
                self.skipped.fetch_add(1, Relaxed);
            }
        }
        drop(attempted);
        Some(entry)
    }

    pub fn bucket(
        &self,
        route: &ReadRoute,
        storage: &crate::storage::Storage,
    ) -> Option<super::Bucket> {
        let index = self.index.lock().unwrap();
        if index.changing != 0 {
            return None;
        }
        let entry = index.buckets.get(&bucket_key(route))?.upgrade()?;
        drop(index);
        if entry.verified.elapsed() >= entry.age
            || entry.profiles.iter().any(|(id, revision)| {
                storage.profile(*id).map_or(true, |profile| {
                    profile.revision != *revision || profile.public_cache_validation != "background"
                })
            })
        {
            return None;
        }
        self.hits.fetch_add(1, Relaxed);
        Some(entry.snapshot.bucket.clone())
    }

    pub(crate) fn queue(
        &self,
        route: &ReadRoute,
        snapshot: Arc<ReadSnapshot>,
        started: Instant,
        generation: u64,
    ) {
        let route_bytes = match route {
            ReadRoute::Host { host, key } => host.len() + key.len(),
            ReadRoute::Bucket { name, key, alias } => {
                name.len() + key.len() + alias.as_ref().map_or(0, |(a, b)| a.len() + b.len())
            }
        };
        if route_bytes > 4096 {
            return;
        }
        let bytes = 8192
            + route_bytes * 2
            + snapshot.bucket.cors.bytes()
            + snapshot.stream.as_ref().map_or(0, |s| {
                json_bytes(&s.metadata)
                    + json_bytes(&s.checksums)
                    + json_bytes(&s.http_compression)
                    + s.object_key.capacity()
                    + s.etag.capacity()
            });
        if bytes > LIMIT / 2 {
            return;
        }
        let Ok(mut index) = self.index.try_lock() else {
            return;
        };
        let key = key(route);
        if index.changing != 0 || index.generation != generation || index.pending.contains(&key) {
            return;
        }
        let Ok(memory) = self
            .memory
            .clone()
            .try_acquire_many_owned(bytes.div_ceil(UNIT) as u32)
        else {
            return;
        };
        index.pending.insert(key);
        if self
            .sender
            .try_send(Work::Fill {
                route: route.clone(),
                snapshot,
                started,
                generation,
                _memory: memory,
            })
            .is_err()
        {
            index.pending.remove(&key);
            self.skipped.fetch_add(1, Relaxed);
        }
    }

    fn put(
        &self,
        route: &ReadRoute,
        snapshot: Arc<ReadSnapshot>,
        mapping: Option<Key>,
        profiles: Vec<(Uuid, i64)>,
        times: (u64, u64),
        (verified, generation): (Instant, u64),
    ) {
        let Some(stream) = &snapshot.stream else {
            return;
        };
        let route_bytes = match route {
            ReadRoute::Host { host, key } => host.len() + key.len(),
            ReadRoute::Bucket { name, key, alias } => {
                name.len() + key.len() + alias.as_ref().map_or(0, |(a, b)| a.len() + b.len())
            }
        };
        if route_bytes > 4096 {
            return;
        }
        let bucket = &snapshot.bucket;
        let bytes = 4096
            + route_bytes * 2
            + profiles.capacity() * 32
            + [
                &bucket.name,
                &bucket.state,
                &bucket.public_base_url,
                &bucket.index_document,
                &bucket.error_document,
                &stream.object_key,
                &stream.stream_type,
                &stream.payload_type,
                &stream.state,
                &stream.etag,
            ]
            .into_iter()
            .map(|s| s.capacity())
            .sum::<usize>()
            + [
                &stream.metadata,
                &stream.checksums,
                &stream.http_compression,
            ]
            .into_iter()
            .map(json_bytes)
            .sum::<usize>()
            + if bucket.cors.charged() {
                0
            } else {
                bucket.cors.bytes()
            };
        if bytes > ENTRY_BYTES {
            return;
        }
        let mut index = self.index.lock().unwrap();
        if index.changing != 0 || index.generation != generation {
            return;
        }
        let key = key(route);
        index.entries.remove(&key);
        index.order.retain(|current| *current != key);
        loop {
            if index.entries.len() < ENTRIES
                && let Ok(memory) = self
                    .memory
                    .clone()
                    .try_acquire_many_owned(bytes.div_ceil(UNIT) as u32)
            {
                let entry = Arc::new(Entry {
                    snapshot,
                    mapping,
                    route: route.clone(),
                    profiles,
                    verified,
                    refresh: Duration::from_secs(times.0),
                    age: Duration::from_secs(times.1),
                    attempted: Mutex::new(verified),
                    _memory: memory,
                });
                index.buckets.retain(|_, entry| entry.strong_count() > 0);
                if index.buckets.len() < ENTRIES {
                    index
                        .buckets
                        .insert(bucket_key(route), Arc::downgrade(&entry));
                }
                index.entries.insert(key, entry);
                index.order.push_back(key);
                return;
            }
            let Some(oldest) = index.order.pop_front() else {
                return;
            };
            index.entries.remove(&oldest);
        }
    }

    fn remove(&self, entry: &Arc<Entry>) {
        let mut index = self.index.lock().unwrap();
        let key = key(&entry.route);
        if index
            .entries
            .get(&key)
            .is_some_and(|current| Arc::ptr_eq(current, entry))
        {
            index.entries.remove(&key);
            index.order.retain(|current| *current != key);
        }
    }

    pub fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({"limit_bytes":LIMIT,"used_bytes":LIMIT-self.memory.available_permits()*UNIT,"entries":self.index.lock().unwrap().entries.len(),"hits":self.hits.load(Relaxed),"refreshed":self.refreshed.load(Relaxed),"failed":self.failed.load(Relaxed),"skipped":self.skipped.load(Relaxed)})
    }
}

impl App {
    pub(crate) async fn cache_public_read(
        &self,
        route: &ReadRoute,
        mut snapshot: Arc<ReadSnapshot>,
        started: Instant,
        generation: u64,
    ) -> Result<()> {
        // Some global changes do not hold coord; capture CORS before checking the snapshot fence.
        let cors_generation = self.cors_cache.generation();
        if self.public_reads.generation() != generation {
            return Ok(());
        }
        let Some(stream) = snapshot
            .stream
            .as_ref()
            .filter(|s| s.public_read && s.state == "ready")
        else {
            return Ok(());
        };
        if snapshot.bucket.state != "active"
            || snapshot.secret.is_some()
            || !stream
                .metadata
                .get("cache_control")
                .and_then(|v| v.as_str())
                .is_none_or(crate::http::object::cache_control)
        {
            return Ok(());
        }
        let Some(storage) = snapshot.bucket.storage_id else {
            return Ok(());
        };
        let profile = self.storage.profile(storage)?;
        if profile.public_cache_validation != "background" {
            return Ok(());
        }
        let mut times = profile.public_cache_times()?;
        let mut profiles = vec![(storage, profile.revision)];
        let mut mapping = Mapping::new(stream.size);
        if stream.payload_type == "raw" {
            if let Some(row) = &snapshot.raw {
                let profile = self.storage.profile(row.source.storage_id)?;
                if profile.public_cache_validation != "background" {
                    return Ok(());
                }
                let other = profile.public_cache_times()?;
                times = (times.0.min(other.0), times.1.min(other.1));
                profiles.push((profile.storage_id, profile.revision));
                mapping.record_raw(row, stream.payload_revision);
            }
        } else {
            for row in self.chunks.read_extents(stream.id, 0, stream.size).await? {
                let Some(storage) = row.chunk.storage_id else {
                    return Ok(());
                };
                let profile = self.storage.profile(storage)?;
                if profile.public_cache_validation != "background" {
                    return Ok(());
                }
                let other = profile.public_cache_times()?;
                times = (times.0.min(other.0), times.1.min(other.1));
                profiles.push((profile.storage_id, profile.revision));
                mapping.record(&row);
            }
        }
        profiles.sort_unstable();
        profiles.dedup();
        #[cfg(feature = "fault-injection")]
        crate::faults::point("public-cache-validated").await;
        let validation = crate::http::cors::Validation::for_bucket(self, &snapshot.bucket);
        self.cors_cache.intern(
            &mut Arc::make_mut(&mut snapshot).bucket,
            validation,
            started,
            cors_generation,
        );
        self.public_reads.put(
            route,
            snapshot,
            mapping.signature(),
            profiles,
            times,
            (started, generation),
        );
        Ok(())
    }
}

pub(crate) async fn run(app: Weak<App>, mut receiver: mpsc::Receiver<Work>) {
    while let Some(work) = receiver.recv().await {
        if let Work::Fill {
            route,
            snapshot,
            started,
            generation,
            _memory,
        } = work
        {
            let Some(app) = app.upgrade() else {
                return;
            };
            let result = tokio::time::timeout(Duration::from_secs(2), async {
                #[cfg(feature = "fault-injection")]
                crate::faults::point("public-cache-fill").await;
                let _coord = app.coord.lock().await;
                let _sources = app.storage.source_gate.read().await;
                if app.public_reads.generation() == generation {
                    app.cache_public_read(&route, snapshot, started, generation)
                        .await?;
                }
                Ok::<_, anyhow::Error>(())
            })
            .await;
            app.public_reads
                .index
                .lock()
                .unwrap()
                .pending
                .remove(&key(&route));
            if !matches!(result, Ok(Ok(()))) {
                app.public_reads.failed.fetch_add(1, Relaxed);
            }
            continue;
        }
        let Work::Refresh(entry) = work else {
            unreachable!()
        };
        let Some(app) = app.upgrade() else {
            return;
        };
        let result = tokio::time::timeout(Duration::from_secs(2), async {
            #[cfg(feature = "fault-injection")]
            crate::faults::point("public-cache-refresh").await;
            let _coord = app.coord.lock().await;
            let _sources = app.storage.source_gate.read().await;
            app.ensure_mode()?;
            let generation = {
                let index = app.public_reads.index.lock().unwrap();
                if !current(&index, &entry) {
                    return Ok(false);
                }
                index.generation
            };
            let started = Instant::now();
            let snapshot = app.stream_store.snapshot(&entry.route, None).await?;
            app.public_reads.remove(&entry);
            if let Some(snapshot) = snapshot {
                app.cache_public_read(&entry.route, Arc::new(snapshot), started, generation)
                    .await?;
            }
            Ok::<_, anyhow::Error>(true)
        })
        .await;
        app.public_reads
            .index
            .lock()
            .unwrap()
            .pending
            .remove(&key(&entry.route));
        match result {
            Ok(Ok(true)) => {
                app.public_reads.refreshed.fetch_add(1, Relaxed);
            }
            Ok(Ok(false)) => {
                app.public_reads.skipped.fetch_add(1, Relaxed);
            }
            _ => {
                app.public_reads.failed.fetch_add(1, Relaxed);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::{Bucket, StoredStream};
    use serde_json::json;

    fn fixture() -> (ReadRoute, Arc<ReadSnapshot>) {
        let id = Uuid::new_v4();
        let now = chrono::Utc::now();
        (
            ReadRoute::Host {
                host: "media.test".into(),
                key: "object".into(),
            },
            Arc::new(ReadSnapshot {
                bucket: Bucket {
                    id,
                    project_id: Uuid::new_v4(),
                    name: "media".into(),
                    storage_id: Some(Uuid::new_v4()),
                    state: "active".into(),
                    settings_revision: 1,
                    uploads_paused: false,
                    public_base_url: String::new(),
                    cors: json!([]).into(),
                    website_enabled: false,
                    index_document: "index.html".into(),
                    error_document: String::new(),
                    created_at: now,
                },
                stream: Some(StoredStream {
                    id: 1.into(),
                    bucket_id: id,
                    object_key: "object".into(),
                    stream_type: "object".into(),
                    payload_type: "raw".into(),
                    payload_revision: 1,
                    state: "ready".into(),
                    size: 1024,
                    etag: "etag".into(),
                    metadata: json!({}),
                    public_read: true,
                    checksums: json!({}),
                    http_compression: json!({}),
                    created_at: now,
                    touched_at: now,
                }),
                raw: None,
                secret: None,
                allowed: false,
            }),
        )
    }

    #[test]
    fn metadata_invalidation_fences_old_builders_and_retained_entries_keep_their_charge() {
        let (cors, _cors_receiver) = crate::http::cors::Cache::new(1024 * 1024, 256);
        let (cache, _receiver) = Cache::new(cors);
        let (route, snapshot) = fixture();
        let generation = cache.generation();
        let available = cache.memory.available_permits();
        cache.put(
            &route,
            snapshot.clone(),
            None,
            vec![],
            (1, 5),
            (Instant::now(), generation),
        );
        let entry = cache
            .index
            .lock()
            .unwrap()
            .entries
            .get(&key(&route))
            .unwrap()
            .clone();
        let change = cache.changing();
        assert!(cache.index.lock().unwrap().entries.is_empty());
        assert!(cache.memory.available_permits() < available);
        cache.put(
            &route,
            snapshot.clone(),
            None,
            vec![],
            (1, 5),
            (Instant::now(), cache.generation()),
        );
        assert!(cache.index.lock().unwrap().entries.is_empty());
        drop(change);
        cache.put(
            &route,
            snapshot,
            None,
            vec![],
            (1, 5),
            (Instant::now(), generation),
        );
        assert!(cache.index.lock().unwrap().entries.is_empty());
        drop(entry);
        assert_eq!(cache.memory.available_permits(), available);
    }

    #[test]
    fn object_changes_preserve_other_snapshots_and_cors_but_fence_late_builders() {
        let (cors, _cors_receiver) = crate::http::cors::Cache::new(1024 * 1024, 256);
        let (cache, _receiver) = Cache::new(cors.clone());
        let (route, snapshot) = fixture();
        let alias = ReadRoute::Bucket {
            name: snapshot.bucket.name.clone(),
            key: "object".into(),
            alias: None,
        };
        let other_route = ReadRoute::Host {
            host: "media.test".into(),
            key: "other".into(),
        };
        let mut other = (*snapshot).clone();
        other.stream.as_mut().unwrap().object_key = "other".into();
        let (foreign_route, foreign) = fixture();
        let foreign_route = match foreign_route {
            ReadRoute::Host { key, .. } => ReadRoute::Host {
                host: "other.test".into(),
                key,
            },
            _ => unreachable!(),
        };
        let generation = cache.generation();
        for (route, snapshot) in [
            (&route, snapshot.clone()),
            (&alias, snapshot.clone()),
            (&other_route, Arc::new(other)),
            (&foreign_route, foreign),
        ] {
            cache.put(
                route,
                snapshot,
                None,
                vec![],
                (1, 5),
                (Instant::now(), generation),
            );
        }
        let (affected, unaffected) = {
            let index = cache.index.lock().unwrap();
            (
                index.entries[&key(&route)].clone(),
                index.entries[&key(&other_route)].clone(),
            )
        };
        let cors_generation = cors.generation();
        let change = cache.changing_object(snapshot.bucket.id, "object");
        let during = cache.generation();
        {
            let index = cache.index.lock().unwrap();
            assert!(!current(&index, &affected));
            assert!(!index.entries.contains_key(&key(&alias)));
            assert!(current(&index, &unaffected));
            assert!(index.entries.contains_key(&key(&foreign_route)));
        }
        drop(change);
        assert_eq!(cors.generation(), cors_generation);
        for generation in [generation, during] {
            cache.put(
                &route,
                snapshot.clone(),
                None,
                vec![],
                (1, 5),
                (Instant::now(), generation),
            );
            assert!(
                !cache
                    .index
                    .lock()
                    .unwrap()
                    .entries
                    .contains_key(&key(&route))
            );
        }
        assert!(current(&cache.index.lock().unwrap(), &unaffected));
        drop(cache.changing());
        assert!(!current(&cache.index.lock().unwrap(), &unaffected));
        assert!(cors.generation() > cors_generation);
    }

    #[test]
    fn large_cors_snapshots_remain_bounded_and_oversized_metadata_is_not_cached() {
        let (cors, _cors_receiver) = crate::http::cors::Cache::new(1024 * 1024, 256);
        let (cache, _receiver) = Cache::new(cors);
        let (route, mut snapshot) = fixture();
        Arc::make_mut(&mut snapshot).bucket.cors = json!(
            (0..100)
                .map(|n| json!({
                    "origins": [format!("https://site{n}.test")],
                    "methods": ["GET", "HEAD"],
                    "headers": ["range"],
                    "expose": ["ETag"],
                    "max_age": 60
                }))
                .collect::<Vec<_>>()
        )
        .into();
        let generation = cache.generation();
        for n in 0..20 {
            let route = ReadRoute::Host {
                host: "media.test".into(),
                key: n.to_string(),
            };
            cache.put(
                &route,
                snapshot.clone(),
                None,
                vec![],
                (1, 5),
                (Instant::now(), generation),
            );
            let state = cache.snapshot();
            assert!(state["entries"].as_u64().unwrap() > 0);
            assert!(state["used_bytes"].as_u64().unwrap() <= LIMIT as u64);
        }
        assert!(cache.snapshot()["entries"].as_u64().unwrap() < 20);
        let snapshot = Arc::make_mut(&mut snapshot);
        snapshot.stream.as_mut().unwrap().metadata = json!({"value": "x".repeat(ENTRY_BYTES)});
        cache.put(
            &route,
            Arc::new(snapshot.clone()),
            None,
            vec![],
            (1, 5),
            (Instant::now(), generation),
        );
        assert!(
            !cache
                .index
                .lock()
                .unwrap()
                .entries
                .contains_key(&key(&route))
        );
    }

    #[test]
    fn metadata_capacity_and_route_boundaries_are_independent_of_body_cache() {
        let (cors, _cors_receiver) = crate::http::cors::Cache::new(1024 * 1024, 256);
        let (cache, _receiver) = Cache::new(cors);
        let (route, snapshot) = fixture();
        let generation = cache.generation();
        for n in 0..1024 {
            let route = ReadRoute::Host {
                host: "media.test".into(),
                key: n.to_string(),
            };
            cache.put(
                &route,
                snapshot.clone(),
                None,
                vec![],
                (1, 5),
                (Instant::now(), generation),
            );
        }
        assert!(cache.index.lock().unwrap().entries.len() <= ENTRIES);
        assert!(cache.snapshot()["used_bytes"].as_u64().unwrap() <= LIMIT as u64);
        assert_ne!(
            key(&route),
            key(&ReadRoute::Bucket {
                name: "media.test".into(),
                key: "object".into(),
                alias: None
            })
        );
        assert_ne!(
            key(&ReadRoute::Host {
                host: "a".into(),
                key: "bc".into()
            }),
            key(&ReadRoute::Host {
                host: "ab".into(),
                key: "c".into()
            })
        );
    }
}
