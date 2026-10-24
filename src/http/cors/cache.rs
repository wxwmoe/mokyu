use super::{Rules, charged_header};
use crate::app::{App, Bucket};
use anyhow::Result;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Deref,
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{
            AtomicU64, AtomicUsize,
            Ordering::{Acquire, Relaxed, Release},
        },
    },
    time::{Duration, Instant},
};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc};
use uuid::Uuid;

const UNIT: usize = 64;
const QUEUE: usize = 32;
const CONTROL_BYTES: usize = 16 * 1024;
const ENTRY_BYTES: usize = 512 * 1024;
type Key = [u8; 32];
type Memory = Arc<OnceLock<Arc<OwnedSemaphorePermit>>>;

#[derive(Clone)]
pub struct Policy(Arc<Document>);
struct Document {
    value: Value,
    rules: OnceLock<std::result::Result<Arc<Rules>, String>>,
    memory: Memory,
}
impl From<Value> for Policy {
    fn from(value: Value) -> Self {
        Self(Arc::new(Document {
            value,
            rules: OnceLock::new(),
            memory: Arc::new(OnceLock::new()),
        }))
    }
}
impl Deref for Policy {
    type Target = Value;
    fn deref(&self) -> &Value {
        &self.0.value
    }
}
impl serde::Serialize for Policy {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        self.0.value.serialize(serializer)
    }
}
impl<'de> serde::Deserialize<'de> for Policy {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        Ok(Self::from(Value::deserialize(deserializer)?))
    }
}
impl Policy {
    pub(crate) fn bytes(&self) -> usize {
        // Include JSON, parsed strings, header owners and tree/Arc allocations before admission.
        fn size(value: &Value) -> usize {
            128usize.saturating_add(match value {
                Value::String(s) => s.capacity().saturating_mul(4),
                Value::Array(a) => a
                    .iter()
                    .fold(a.capacity() * 64, |n, v| n.saturating_add(size(v))),
                Value::Object(o) => o.iter().fold(0usize, |n, (k, v)| {
                    n.saturating_add(k.capacity() * 4 + size(v))
                }),
                _ => 0,
            })
        }
        1024usize.saturating_add(size(self))
    }
    pub(crate) fn charged(&self) -> bool {
        self.0.memory.get().is_some()
    }
    pub(super) fn rules(&self, cache: &Cache) -> Result<Arc<Rules>> {
        self.0
            .rules
            .get_or_init(|| {
                cache.parses.fetch_add(1, Relaxed);
                let mut rules =
                    Rules::parse(self, self.0.memory.clone()).map_err(|e| e.to_string())?;
                for rule in &mut rules.rules {
                    rule.methods =
                        charged_header(&rule.methods, &self.0.memory).map_err(|e| e.to_string())?;
                    rule.age =
                        charged_header(&rule.age, &self.0.memory).map_err(|e| e.to_string())?;
                    rule.expose = rule
                        .expose
                        .as_ref()
                        .map(|v| charged_header(v, &self.0.memory))
                        .transpose()
                        .map_err(|e| e.to_string())?;
                }
                Ok(Arc::new(rules))
            })
            .clone()
            .map_err(anyhow::Error::msg)
    }
    fn key(&self) -> Key {
        struct Writer(blake3::Hasher);
        impl std::io::Write for Writer {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.update(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut writer = Writer(blake3::Hasher::new());
        serde_json::to_writer(&mut writer, &self.0.value).expect("JSON serialization");
        *writer.0.finalize().as_bytes()
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Validation {
    storage: Uuid,
    revision: i64,
    refresh: Duration,
    age: Duration,
}
impl Validation {
    pub(crate) fn for_bucket(app: &App, bucket: &Bucket) -> Option<Self> {
        let profile = app.storage.profile(bucket.storage_id?).ok()?;
        if !profile.cors_cache_enabled && profile.public_cache_validation != "background" {
            return None;
        }
        let (refresh, age) = profile.public_cache_times().ok()?;
        Some(Self {
            storage: profile.storage_id,
            revision: profile.revision,
            refresh: Duration::from_secs(refresh),
            age: Duration::from_secs(age),
        })
    }
    fn current(&self, other: &Self) -> bool {
        self.storage == other.storage && self.revision == other.revision
    }
}

struct Shared {
    policy: Policy,
    used: AtomicU64,
}
pub(crate) struct Binding {
    id: Uuid,
    name: String,
    revision: i64,
    policy: Policy,
    key: Key,
    validation: Option<Validation>,
    verified: Instant,
    attempted: Mutex<Instant>,
    used: AtomicU64,
    generation: u64,
    _memory: OwnedSemaphorePermit,
}
pub(crate) struct Fill {
    id: Uuid,
    name: String,
    revision: i64,
    policy: Policy,
    validation: Option<Validation>,
    verified: Instant,
    generation: u64,
    memory: OwnedSemaphorePermit,
}
pub(crate) enum Work {
    Fill(Fill),
    Refresh(Arc<Binding>),
}
#[derive(Default)]
struct Index {
    policies: BTreeMap<Key, Shared>,
    buckets: BTreeMap<Uuid, Arc<Binding>>,
    pending: BTreeSet<(Uuid, u64)>,
    generation: u64,
    changing: usize,
}
pub(crate) struct Cache {
    limit: usize,
    entries: usize,
    memory: Arc<Semaphore>,
    index: Mutex<Index>,
    sender: mpsc::Sender<Work>,
    pressure: AtomicUsize,
    generation: AtomicU64,
    wake: Arc<Notify>,
    clock: AtomicU64,
    hits: AtomicU64,
    parses: AtomicU64,
    evicted: AtomicU64,
    bucket_evicted: AtomicU64,
    invalidated: AtomicU64,
    bypassed: AtomicU64,
    refreshed: AtomicU64,
    failed: AtomicU64,
}
impl Cache {
    pub(crate) fn new(bytes: usize, entries: usize) -> (Arc<Self>, mpsc::Receiver<Work>) {
        let (sender, receiver) = mpsc::channel(QUEUE);
        (
            Arc::new(Self {
                limit: bytes / UNIT * UNIT,
                entries,
                memory: Arc::new(Semaphore::new(bytes.saturating_sub(CONTROL_BYTES) / UNIT)),
                index: Mutex::new(Index::default()),
                sender,
                pressure: 0.into(),
                generation: 0.into(),
                wake: Arc::new(Notify::new()),
                clock: 0.into(),
                hits: 0.into(),
                parses: 0.into(),
                evicted: 0.into(),
                bucket_evicted: 0.into(),
                invalidated: 0.into(),
                bypassed: 0.into(),
                refreshed: 0.into(),
                failed: 0.into(),
            }),
            receiver,
        )
    }
    fn charge(&self, bytes: usize) -> Option<OwnedSemaphorePermit> {
        self.memory
            .clone()
            .try_acquire_many_owned(bytes.div_ceil(UNIT).try_into().ok()?)
            .ok()
    }
    pub(crate) fn change(&self, begin: bool) {
        let mut index = self.index.lock().unwrap();
        index.generation = index.generation.wrapping_add(1);
        self.generation.store(index.generation, Release);
        if begin {
            index.changing += 1;
        } else {
            index.changing -= 1;
        }
        index.buckets.clear();
        index.pending.clear();
        // Immutable policies remain reusable; bucket authority never survives a change.
        self.invalidated.fetch_add(1, Relaxed);
    }
    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Acquire)
    }
    pub(crate) fn resolve(
        &self,
        bucket: &Bucket,
        validation: Option<Validation>,
        generation: u64,
    ) -> Policy {
        if validation.is_none() || self.entries == 0 || self.limit == 0 {
            return bucket.cors.clone();
        }
        let Ok(mut index) = self.index.try_lock() else {
            return bucket.cors.clone();
        };
        if index.changing != 0 || index.generation != generation {
            return bucket.cors.clone();
        }
        if let Some(entry) = index.buckets.get(&bucket.id).cloned() {
            let background = validation
                .zip(entry.validation)
                .is_some_and(|(a, b)| a.current(&b));
            if background && entry.verified.elapsed() < entry.validation.unwrap().age {
                let used = self.clock.fetch_add(1, Relaxed);
                entry.used.store(used, Relaxed);
                if let Some(policy) = index.policies.get(&entry.key) {
                    policy.used.store(used, Relaxed);
                }
                self.hits.fetch_add(1, Relaxed);
                if entry.attempted.lock().unwrap().elapsed() >= entry.validation.unwrap().refresh
                    && index.pending.insert((bucket.id, generation))
                {
                    *entry.attempted.lock().unwrap() = Instant::now();
                    if self.sender.try_send(Work::Refresh(entry.clone())).is_err() {
                        let generation = index.generation;
                        index.pending.remove(&(bucket.id, generation));
                    }
                }
                return entry.policy.clone();
            }
            if entry.revision > bucket.settings_revision && !background {
                return bucket.cors.clone();
            }
        }
        let generation = index.generation;
        drop(index);
        self.offer(bucket, validation, Instant::now(), generation);
        bucket.cors.clone()
    }
    pub(crate) fn offer(
        &self,
        bucket: &Bucket,
        validation: Option<Validation>,
        verified: Instant,
        generation: u64,
    ) {
        if self.entries == 0 || self.limit == 0 {
            return;
        }
        let bytes = bucket.cors.bytes();
        let association = 512 + bucket.name.capacity();
        if bytes > ENTRY_BYTES || bytes + association > self.limit {
            self.bypassed.fetch_add(1, Relaxed);
            return;
        }
        let Ok(mut index) = self.index.try_lock() else {
            return;
        };
        if index.changing != 0
            || index.generation != generation
            || index.pending.contains(&(bucket.id, generation))
        {
            return;
        }
        let memory = self.charge(association);
        let policy_memory = bucket
            .cors
            .charged()
            .then_some(None)
            .or_else(|| self.charge(bytes).map(|p| Some(Arc::new(p))));
        let (Some(memory), Some(policy_memory)) = (memory, policy_memory) else {
            self.pressure.fetch_max(bytes + association, Relaxed);
            self.wake.notify_one();
            self.bypassed.fetch_add(1, Relaxed);
            return;
        };
        if let Some(memory) = policy_memory {
            let _ = bucket.cors.0.memory.set(memory);
        }
        index.pending.insert((bucket.id, generation));
        let work = Fill {
            id: bucket.id,
            name: bucket.name.clone(),
            revision: bucket.settings_revision,
            policy: bucket.cors.clone(),
            validation,
            verified,
            generation,
            memory,
        };
        if self.sender.try_send(Work::Fill(work)).is_err() {
            index.pending.remove(&(bucket.id, generation));
            self.bypassed.fetch_add(1, Relaxed);
        }
    }
    fn evict(&self, index: &mut Index, protect: Option<Key>) -> bool {
        if index.policies.len() > 1 {
            if let Some(key) = index
                .policies
                .iter()
                .filter(|(key, _)| Some(**key) != protect)
                .min_by_key(|(_, v)| v.used.load(Relaxed))
                .map(|(k, _)| *k)
            {
                index.policies.remove(&key);
                index.buckets.retain(|_, v| v.key != key);
                self.evicted.fetch_add(1, Relaxed);
                return true;
            }
        }
        if let Some(id) = index
            .buckets
            .values()
            .min_by_key(|v| v.used.load(Relaxed))
            .map(|v| v.id)
        {
            index.buckets.remove(&id);
            self.bucket_evicted.fetch_add(1, Relaxed);
            return true;
        }
        false
    }
    fn fill(&self, fill: Fill) {
        let key = fill.policy.key();
        let mut index = self.index.lock().unwrap();
        if index.generation != fill.generation
            || index.changing != 0
            || index
                .buckets
                .get(&fill.id)
                .is_some_and(|v| v.revision > fill.revision)
        {
            return;
        }
        let used = self.clock.fetch_add(1, Relaxed);
        let policy = if let Some(shared) = index.policies.get(&key) {
            if *shared.policy != *fill.policy {
                return;
            }
            shared.used.store(used, Relaxed);
            shared.policy.clone()
        } else {
            while index.policies.len() >= self.entries {
                // Admission of a different policy can replace the last policy at a count limit.
                if index.policies.len() == 1 {
                    index.policies.clear();
                    index.buckets.clear();
                    self.evicted.fetch_add(1, Relaxed);
                } else if !self.evict(&mut index, None) {
                    return;
                }
            }
            index.policies.insert(
                key,
                Shared {
                    policy: fill.policy.clone(),
                    used: used.into(),
                },
            );
            fill.policy
        };
        index.buckets.insert(
            fill.id,
            Arc::new(Binding {
                id: fill.id,
                name: fill.name,
                revision: fill.revision,
                policy,
                key,
                validation: fill.validation,
                verified: fill.verified,
                attempted: Mutex::new(fill.verified),
                used: used.into(),
                generation: fill.generation,
                _memory: fill.memory,
            }),
        );
    }
    pub(crate) fn intern(
        &self,
        bucket: &mut Bucket,
        validation: Option<Validation>,
        verified: Instant,
        generation: u64,
    ) {
        if self.generation() != generation {
            return;
        }
        if validation.is_none() || self.limit == 0 || self.entries == 0 {
            return;
        }
        if let Some(policy) = self.shared(bucket) {
            bucket.cors = policy;
        }
        let bytes = bucket.cors.bytes();
        if bytes > ENTRY_BYTES {
            return;
        }
        let Some(memory) = self.charge(512 + bucket.name.capacity()) else {
            return;
        };
        if !bucket.cors.charged() {
            let Some(memory) = self.charge(bytes) else {
                return;
            };
            let _ = bucket.cors.0.memory.set(Arc::new(memory));
        }
        self.fill(Fill {
            id: bucket.id,
            name: bucket.name.clone(),
            revision: bucket.settings_revision,
            policy: bucket.cors.clone(),
            validation,
            verified,
            generation,
            memory,
        });
        if let Some(policy) = self.shared(bucket) {
            bucket.cors = policy;
        }
    }
    pub(crate) fn shared(&self, bucket: &Bucket) -> Option<Policy> {
        let index = self.index.try_lock().ok()?;
        let entry = index.buckets.get(&bucket.id)?;
        (index.changing == 0 && entry.revision == bucket.settings_revision)
            .then(|| entry.policy.clone())
    }
    pub(crate) fn current(&self, bucket: &mut Bucket) {
        let Ok(index) = self.index.try_lock() else {
            return;
        };
        if index.changing != 0 {
            return;
        }
        if let Some(entry) = index.buckets.get(&bucket.id)
            && entry.revision == bucket.settings_revision
        {
            let used = self.clock.fetch_add(1, Relaxed);
            entry.used.store(used, Relaxed);
            if let Some(policy) = index.policies.get(&entry.key) {
                policy.used.store(used, Relaxed);
            }
            self.hits.fetch_add(1, Relaxed);
            bucket.cors = entry.policy.clone();
            return;
        }
        let generation = index.generation;
        drop(index);
        // The RPC confirmed this revision; only policy parsing and storage are reused.
        self.offer(bucket, None, Instant::now(), generation);
    }
    pub(crate) fn snapshot(&self) -> Value {
        let index = self.index.lock().unwrap();
        serde_json::json!({"limit_bytes":self.limit,"index_limit_bytes":CONTROL_BYTES.min(self.limit),
            "live_bytes":self.limit.saturating_sub(CONTROL_BYTES)-self.memory.available_permits()*UNIT,"entry_limit":self.entries,
            "entries":index.policies.len(),"buckets":index.buckets.len(),"pending":index.pending.len(),
            "hits":self.hits.load(Relaxed),"parses":self.parses.load(Relaxed),"evicted":self.evicted.load(Relaxed),
            "bucket_evicted":self.bucket_evicted.load(Relaxed),"invalidated":self.invalidated.load(Relaxed),
            "bypassed":self.bypassed.load(Relaxed),"refreshed":self.refreshed.load(Relaxed),"failed":self.failed.load(Relaxed)})
    }
}

pub(crate) async fn run_current(cache: Weak<Cache>, mut receiver: mpsc::Receiver<Work>) {
    let Some(wake) = cache.upgrade().map(|cache| cache.wake.clone()) else {
        return;
    };
    loop {
        let work = tokio::select! {
            work = receiver.recv() => match work { Some(work) => Some(work), None => return },
            _ = wake.notified() => None,
        };
        let Some(cache) = cache.upgrade() else { return };
        match work {
            Some(Work::Fill(fill)) => {
                let key = (fill.id, fill.generation);
                #[cfg(feature = "fault-injection")]
                crate::faults::point("cors-cache-work").await;
                cache.fill(fill);
                cache.index.lock().unwrap().pending.remove(&key);
            }
            Some(Work::Refresh(entry)) => {
                cache
                    .index
                    .lock()
                    .unwrap()
                    .pending
                    .remove(&(entry.id, entry.generation));
            }
            None => {
                let bytes = cache.pressure.swap(0, Relaxed);
                let mut index = cache.index.lock().unwrap();
                while cache.memory.available_permits() * UNIT < bytes
                    && cache.evict(&mut index, None)
                {}
            }
        }
    }
}

pub(crate) async fn run(app: Weak<App>, mut receiver: mpsc::Receiver<Work>) {
    let Some(cache) = app.upgrade().map(|a| a.cors_cache.clone()) else {
        return;
    };
    loop {
        let work = tokio::select! {
            work = receiver.recv() => match work { Some(work) => work, None => return },
            _ = cache.wake.notified() => {
                let bytes = cache.pressure.swap(0, Relaxed);
                let mut index = cache.index.lock().unwrap();
                while cache.memory.available_permits()*UNIT < bytes && cache.evict(&mut index, None) {}
                continue;
            }
        };
        let (id, generation) = match &work {
            Work::Fill(v) => (v.id, v.generation),
            Work::Refresh(v) => (v.id, v.generation),
        };
        #[cfg(feature = "fault-injection")]
        crate::faults::point("cors-cache-work").await;
        match work {
            Work::Fill(fill) => cache.fill(fill),
            Work::Refresh(entry) => {
                let Some(app) = app.upgrade() else {
                    return;
                };
                let started = Instant::now();
                match tokio::time::timeout(
                    Duration::from_secs(2),
                    app.buckets.by_name(&entry.name, false),
                )
                .await
                {
                    Ok(Ok(Some(bucket))) if bucket.id == id => {
                        cache
                            .index
                            .lock()
                            .unwrap()
                            .pending
                            .remove(&(id, generation));
                        cache.offer(
                            &bucket,
                            Validation::for_bucket(&app, &bucket),
                            started,
                            generation,
                        );
                        cache.refreshed.fetch_add(1, Relaxed);
                        continue;
                    }
                    Ok(Ok(_)) => {
                        let mut index = cache.index.lock().unwrap();
                        if index
                            .buckets
                            .get(&id)
                            .is_some_and(|v| Arc::ptr_eq(v, &entry))
                        {
                            index.buckets.remove(&id);
                        }
                    }
                    _ => {
                        cache.failed.fetch_add(1, Relaxed);
                    }
                }
            }
        }
        cache
            .index
            .lock()
            .unwrap()
            .pending
            .remove(&(id, generation));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderMap;
    use serde_json::json;

    fn bucket() -> Bucket {
        Bucket {
            id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            name: "bucket".into(),
            storage_id: Some(Uuid::new_v4()),
            state: "active".into(),
            settings_revision: 1,
            uploads_paused: false,
            public_base_url: String::new(),
            cors:
                json!([{"origins":["https://one.test","https://two.test"],"methods":["GET","HEAD"],
                "headers":["range"],"expose":["ETag"],"max_age":60}])
                .into(),
            website_enabled: false,
            index_document: "index.html".into(),
            error_document: String::new(),
            created_at: chrono::Utc::now(),
        }
    }
    fn validation(bucket: &Bucket) -> Option<Validation> {
        Some(Validation {
            storage: bucket.storage_id.unwrap(),
            revision: 1,
            refresh: Duration::from_secs(1),
            age: Duration::from_secs(5),
        })
    }
    fn drain(cache: &Cache, receiver: &mut mpsc::Receiver<Work>) {
        while let Ok(work) = receiver.try_recv() {
            if let Work::Fill(fill) = work {
                let key = (fill.id, fill.generation);
                cache.fill(fill);
                cache.index.lock().unwrap().pending.remove(&key);
            }
        }
    }
    fn headers(cache: &Cache, bucket: &Bucket, origin: &str) -> HeaderMap {
        let mut request = HeaderMap::new();
        request.insert("origin", origin.parse().unwrap());
        super::super::cors_headers(
            cache,
            bucket,
            &request,
            "GET",
            validation(bucket),
            cache.generation(),
        )
        .unwrap()
    }
    #[tokio::test]
    async fn authoritative_bindings_share_rules_and_ignore_late_versions() {
        let (cache, mut receiver) = Cache::new(1024 * 1024, 256);
        let original = bucket();
        let mut first = original.clone();
        cache.current(&mut first);
        drain(&cache, &mut receiver);
        cache.current(&mut first);
        let shared = first.cors.clone();
        for _ in 0..100 {
            let mut other = bucket();
            cache.current(&mut other);
            drain(&cache, &mut receiver);
            cache.current(&mut other);
            assert!(Arc::ptr_eq(&shared.0, &other.cors.0));
        }
        assert_eq!(cache.snapshot()["entries"], 1);
        let mut changed = original.clone();
        changed.settings_revision += 1;
        changed.cors = json!([]).into();
        cache.current(&mut changed);
        drain(&cache, &mut receiver);
        let mut late = original;
        cache.current(&mut late);
        drain(&cache, &mut receiver);
        assert_eq!(*cache.shared(&changed).unwrap(), json!([]));
        assert!(cache.shared(&late).is_none());
        let worker = tokio::spawn(run_current(Arc::downgrade(&cache), receiver));
        drop(cache);
        tokio::time::timeout(Duration::from_secs(1), worker)
            .await
            .unwrap()
            .unwrap();
    }
    #[test]
    fn shared_json_rules_and_headers_survive_bucket_eviction() {
        let (cache, mut receiver) = Cache::new(1024 * 1024, 256);
        assert_eq!(cache.snapshot()["live_bytes"], 0);
        let first = bucket();
        assert_eq!(
            headers(&cache, &first, "https://one.test")["access-control-allow-origin"],
            "https://one.test"
        );
        drain(&cache, &mut receiver);
        let policy = cache.shared(&first).unwrap();
        for _ in 0..100 {
            let mut other = bucket();
            cache.intern(
                &mut other,
                validation(&first),
                Instant::now(),
                cache.generation(),
            );
            assert!(Arc::ptr_eq(&policy.0, &other.cors.0));
        }
        assert_eq!(cache.snapshot()["entries"], 1);
        assert_eq!(cache.snapshot()["buckets"], 101);
        let response = headers(&cache, &first, "https://two.test");
        assert_eq!(response["access-control-allow-origin"], "https://two.test");
        assert_eq!(cache.snapshot()["parses"], 1);
        let mut index = cache.index.lock().unwrap();
        assert!(cache.evict(&mut index, None));
        assert_eq!(index.policies.len(), 1);
        assert_eq!(index.buckets.len(), 100);
        index.buckets.clear();
        index.policies.clear();
        drop(index);
        drop(first);
        drop(policy);
        assert!(cache.snapshot()["live_bytes"].as_u64().unwrap() > 0);
        drop(response);
        assert_eq!(cache.snapshot()["live_bytes"], 0);
    }
    #[test]
    fn policy_recency_precedes_bucket_eviction() {
        let (cache, mut receiver) = Cache::new(1024 * 1024, 256);
        let first = bucket();
        headers(&cache, &first, "https://one.test");
        drain(&cache, &mut receiver);
        let mut second = bucket();
        second.cors = json!([{"origins":["*"],"methods":["GET"]}]).into();
        headers(&cache, &second, "https://one.test");
        drain(&cache, &mut receiver);
        headers(&cache, &first, "https://one.test");
        let mut index = cache.index.lock().unwrap();
        assert!(cache.evict(&mut index, None));
        assert!(index.buckets.contains_key(&first.id));
        assert!(!index.buckets.contains_key(&second.id));
        assert_eq!(cache.bucket_evicted.load(Relaxed), 0);
    }
    #[test]
    fn stale_fills_are_fenced_and_full_or_locked_queue_never_blocks_headers() {
        let (cache, mut receiver) = Cache::new(1024 * 1024, 256);
        let first = bucket();
        headers(&cache, &first, "https://one.test");
        let old_generation = cache.generation();
        cache.change(true);
        cache.change(false);
        drain(&cache, &mut receiver);
        assert_eq!(cache.snapshot()["buckets"], 0);
        cache.resolve(&first, validation(&first), old_generation);
        assert!(receiver.try_recv().is_err());
        let index = cache.index.lock().unwrap();
        assert!(cache.generation() > old_generation);
        let result = cache.resolve(&first, validation(&first), old_generation);
        assert_eq!(*result, *first.cors);
        drop(index);
        for _ in 0..100 {
            headers(&cache, &bucket(), "https://one.test");
        }
        assert!(cache.snapshot()["pending"].as_u64().unwrap() <= QUEUE as u64);
        assert!(cache.snapshot()["live_bytes"].as_u64().unwrap() <= cache.limit as u64);
        drain(&cache, &mut receiver);
    }
    #[test]
    fn disabled_cache_and_expired_confirmation_use_current_sql_policy() {
        let (cache, mut receiver) = Cache::new(1024 * 1024, 256);
        let mut first = bucket();
        cache.resolve(&first, None, cache.generation());
        assert_eq!(cache.snapshot()["pending"], 0);
        headers(&cache, &first, "https://one.test");
        drain(&cache, &mut receiver);
        first.cors = json!([]).into();
        first.settings_revision += 1;
        assert!(
            headers(&cache, &first, "https://one.test").contains_key("access-control-allow-origin")
        );
        {
            let mut index = cache.index.lock().unwrap();
            let entry = Arc::get_mut(index.buckets.get_mut(&first.id).unwrap()).unwrap();
            entry.verified = Instant::now() - Duration::from_secs(6);
        }
        assert!(
            !headers(&cache, &first, "https://one.test")
                .contains_key("access-control-allow-origin")
        );
        drain(&cache, &mut receiver);
        assert_eq!(cache.shared(&first).unwrap().as_array().unwrap().len(), 0);
    }
    #[test]
    fn matching_keeps_request_origin_headers_order_and_invalid_documents() {
        let (cache, mut receiver) = Cache::new(1024 * 1024, 256);
        let mut bucket = bucket();
        let empty = super::super::cors_headers(
            &cache,
            &bucket,
            &HeaderMap::new(),
            "GET",
            validation(&bucket),
            0,
        )
        .unwrap();
        assert_eq!(empty["vary"], "Origin");
        assert_eq!(cache.parses.load(Relaxed), 0);
        drain(&cache, &mut receiver);
        let mut request = HeaderMap::new();
        request.insert("origin", "https://two.test".parse().unwrap());
        request.insert("access-control-request-method", "HEAD".parse().unwrap());
        request.insert("access-control-request-headers", "Range".parse().unwrap());
        let out = super::super::cors_headers(
            &cache,
            &bucket,
            &request,
            "OPTIONS",
            validation(&bucket),
            0,
        )
        .unwrap();
        assert_eq!(out["access-control-allow-headers"], "Range");
        request.insert(
            "access-control-request-headers",
            "Authorization".parse().unwrap(),
        );
        assert!(
            !super::super::cors_headers(
                &cache,
                &bucket,
                &request,
                "OPTIONS",
                validation(&bucket),
                0
            )
            .unwrap()
            .contains_key("access-control-allow-origin")
        );
        bucket.cors = json!([{"invalid":true}]).into();
        assert!(super::super::cors_headers(&cache, &bucket, &request, "OPTIONS", None, 0).is_err());
    }
    #[test]
    fn hundred_rule_policy_and_retained_jobs_fit_the_budget() {
        let (cache, mut receiver) = Cache::new(1024 * 1024, 256);
        let mut first = bucket();
        first.cors = json!(
            (0..100)
                .map(|n| json!({"origins":[format!("https://site{n}.test")],
            "methods":["GET","HEAD"],"headers":["range"],"expose":["ETag"],"max_age":60}))
                .collect::<Vec<_>>()
        )
        .into();
        assert!(first.cors.bytes() <= ENTRY_BYTES, "{}", first.cors.bytes());
        headers(&cache, &first, "https://site99.test");
        drain(&cache, &mut receiver);
        let shared = cache.shared(&first).unwrap();
        for _ in 0..100 {
            let mut next = first.clone();
            next.id = Uuid::new_v4();
            next.cors = (*first.cors).clone().into();
            cache.intern(
                &mut next,
                validation(&first),
                Instant::now(),
                cache.generation(),
            );
            assert!(Arc::ptr_eq(&next.cors.0, &shared.0));
        }
        assert_eq!(cache.snapshot()["entries"], 1);
        assert_eq!(cache.snapshot()["buckets"], 101);
        assert!(cache.snapshot()["live_bytes"].as_u64().unwrap() <= cache.limit as u64);
    }
}
