use crate::{
    codec::{self, Chunk, MAX},
    config::{self, Budget, Config, Secrets},
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt};
use object_store::{
    ClientOptions, ObjectStore, ObjectStoreExt, PutMode, PutOptions, RetryConfig,
    aws::AmazonS3Builder, path::Path,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Semaphore,
};
use uuid::Uuid;

#[derive(serde::Serialize, serde::Deserialize)]
struct BackendMeta {
    format_version: u32,
    deployment_id: Uuid,
    chunk_layout: String,
    created_at: chrono::DateTime<chrono::Utc>,
    created_by: String,
}

fn chunk_name(id: Uuid) -> String {
    let id = id.simple().to_string();
    format!("{}/{id}", &id[..2])
}

fn parse_chunk_name(name: &str) -> Option<Uuid> {
    let (shard, id) = name.split_once('/')?;
    if shard.len() != 2
        || id.len() != 32
        || !id.starts_with(shard)
        || !id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return None;
    }
    Uuid::parse_str(id).ok()
}
fn parse_cache_name(name: &str) -> Option<(Uuid, bool)> {
    let (name, extension) = name.rsplit_once('.')?;
    let compressed = match extension {
        "raw" => false,
        "zst" => true,
        _ => return None,
    };
    Some((parse_chunk_name(name)?, compressed))
}

#[derive(Clone, Copy)]
pub enum Area {
    Multipart = 0,
    Cache = 1,
}
pub async fn read_bounded(path: impl AsRef<std::path::Path>, max: usize) -> Result<Vec<u8>> {
    let f = tokio::fs::File::open(path).await?;
    ensure!(
        f.metadata().await?.len() <= max as u64,
        "local file exceeds bound"
    );
    let mut bytes = Vec::new();
    f.take(max as u64 + 1).read_to_end(&mut bytes).await?;
    ensure!(bytes.len() <= max, "local file exceeds bound");
    Ok(bytes)
}
struct Usage {
    used: [u64; 2],
    reserved: u64,
}
pub struct Disk {
    pub root: PathBuf,
    limits: [Option<u64>; 2],
    floor: u64,
    usage: Mutex<Usage>,
    io: Arc<Semaphore>,
    pending: Mutex<HashSet<Uuid>>,
}
pub struct Fragment {
    pub id: Uuid,
    disk: Arc<Disk>,
}
impl Drop for Fragment {
    fn drop(&mut self) {
        self.disk.pending.lock().unwrap().remove(&self.id);
    }
}
struct Reservation {
    disk: Arc<Disk>,
    area: Area,
    bytes: u64,
    committed: bool,
}
impl Reservation {
    fn commit(mut self) {
        self.disk.usage.lock().unwrap().reserved -= self.bytes;
        self.committed = true;
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.committed {
            let mut u = self.disk.usage.lock().unwrap();
            u.used[self.area as usize] -= self.bytes;
            u.reserved -= self.bytes;
        }
    }
}
impl Disk {
    fn reserve(self: &Arc<Self>, area: Area, bytes: u64) -> Result<Reservation> {
        let mut usage = self.usage.lock().unwrap();
        let next = usage.used[area as usize]
            .checked_add(bytes)
            .context("disk accounting overflow")?;
        ensure!(
            self.limits[area as usize].is_none_or(|n| next <= n),
            "local storage quota exhausted"
        );
        let free = fs2::available_space(&self.root)?;
        ensure!(
            free.saturating_sub(usage.reserved.saturating_add(bytes)) >= self.floor,
            "filesystem free-space floor reached"
        );
        usage.used[area as usize] = next;
        usage.reserved += bytes;
        Ok(Reservation {
            disk: self.clone(),
            area,
            bytes,
            committed: false,
        })
    }
    pub fn used(&self) -> [u64; 2] {
        self.usage.lock().unwrap().used
    }
    fn account_existing(&self, area: Area, bytes: u64) {
        self.usage.lock().unwrap().used[area as usize] += bytes;
    }
    fn release(&self, area: Area, bytes: u64) {
        let mut u = self.usage.lock().unwrap();
        u.used[area as usize] = u.used[area as usize].saturating_sub(bytes);
    }
    pub fn fragment_path(&self, id: Uuid) -> PathBuf {
        self.root.join("multipart").join(id.simple().to_string())
    }
    pub fn pending(&self, id: Uuid) -> bool {
        self.pending.lock().unwrap().contains(&id)
    }
    pub async fn write_fragment(self: &Arc<Self>, id: Uuid, data: &[u8]) -> Result<Fragment> {
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
                    ticket.commit();
                }
                return Err(e.into());
            }
            ticket.commit();
            Ok(lease)
        })
        .await?
    }
    pub async fn remove_fragment(&self, id: Uuid) -> Result<()> {
        let path = self.fragment_path(id);
        match tokio::fs::metadata(&path).await {
            Ok(m) => {
                tokio::fs::remove_file(path).await?;
                self.release(Area::Multipart, m.len());
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Queue {
    Small,
    Main,
}
struct Entry {
    size: u64,
    compressed: bool,
    frequency: u8,
    pins: Arc<AtomicUsize>,
    queue: Queue,
}
struct CachePin(Arc<AtomicUsize>);
impl Drop for CachePin {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}
#[derive(Default)]
struct Fifo {
    entries: HashMap<Uuid, Entry>,
    small: VecDeque<Uuid>,
    main: VecDeque<Uuid>,
    ghost: HashSet<Uuid>,
    ghosts: VecDeque<Uuid>,
    small_bytes: u64,
}
impl Fifo {
    fn insert(&mut self, id: Uuid, size: u64, compressed: bool) {
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
                compressed,
                frequency: 0,
                pins: Arc::new(AtomicUsize::new(0)),
                queue,
            },
        );
    }
    fn remove(&mut self, id: Uuid) -> Option<Entry> {
        let e = self.entries.remove(&id)?;
        if e.queue == Queue::Small {
            self.small_bytes -= e.size;
        }
        Some(e)
    }
    fn forget(&mut self, id: Uuid) -> Option<Entry> {
        let e = self.remove(id)?;
        self.small.retain(|v| *v != id);
        self.main.retain(|v| *v != id);
        Some(e)
    }
    fn victim(&mut self, capacity: u64, max_ghosts: usize) -> Option<Uuid> {
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
            if e.pins.load(Ordering::Relaxed) > 0 {
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
            if small {
                self.ghost.insert(id);
                self.ghosts.push_back(id);
                while self.ghosts.len() > max_ghosts {
                    if let Some(old) = self.ghosts.pop_front() {
                        self.ghost.remove(&old);
                    }
                }
            }
            return Some(id);
        }
        None
    }
}

type FetchResult = tokio::sync::OnceCell<Result<Bytes, Arc<anyhow::Error>>>;

pub struct Storage {
    pub backend: Arc<dyn ObjectStore>,
    prefix: String,
    pub disk: Arc<Disk>,
    pub secrets: Arc<Secrets>,
    cpu: Arc<Semaphore>,
    requests: Arc<Semaphore>,
    fifo: tokio::sync::Mutex<Fifo>,
    cache_writes: tokio::sync::Mutex<()>,
    fills: Arc<Semaphore>,
    max_entries: usize,
    capacity: Option<u64>,
    min_compression_savings_percent: u8,
    misses: Mutex<HashMap<Uuid, std::sync::Weak<FetchResult>>>,
    pub backend_gets: std::sync::atomic::AtomicU64,
    pub cache_hits: std::sync::atomic::AtomicU64,
    pub backend_puts: std::sync::atomic::AtomicU64,
    pub backend_deletes: std::sync::atomic::AtomicU64,
    pub backend_read_bytes: std::sync::atomic::AtomicU64,
    pub backend_write_bytes: std::sync::atomic::AtomicU64,
    pub cache_hit_bytes: std::sync::atomic::AtomicU64,
    cache_lookups: std::sync::atomic::AtomicU64,
    operations: [Arc<crate::stats::Counters>; 3],
}
impl Storage {
    pub async fn new(c: &Config, secrets: Arc<Secrets>, budget: &Budget) -> Result<Self> {
        for name in ["multipart", "chunks"] {
            tokio::fs::create_dir_all(c.storage.data.join(name)).await?;
        }
        let disk = Arc::new(Disk {
            root: c.storage.data.clone(),
            limits: [
                c.multipart
                    .local_limit
                    .as_deref()
                    .map(config::bytes)
                    .transpose()?,
                c.cache.max_size.as_deref().map(config::bytes).transpose()?,
            ],
            floor: config::bytes(&c.storage.free_space_floor)?,
            usage: Mutex::new(Usage {
                used: [0; 2],
                reserved: 0,
            }),
            io: Arc::new(Semaphore::new(budget.cpu_jobs)),
            pending: Mutex::new(HashSet::new()),
        });
        let backend = AmazonS3Builder::new()
            .with_endpoint(&c.backend.endpoint)
            .with_region(&c.backend.region)
            .with_bucket_name(&c.backend.bucket)
            .with_access_key_id(&secrets.backend_access)
            .with_secret_access_key(&secrets.backend_secret)
            .with_allow_http(c.backend.allow_http)
            .with_virtual_hosted_style_request(false)
            .with_client_options(
                ClientOptions::new()
                    .with_allow_http(c.backend.allow_http)
                    .with_timeout(Duration::from_secs(30)),
            )
            .with_retry(RetryConfig {
                max_retries: 3,
                retry_timeout: Duration::from_secs(120),
                ..Default::default()
            })
            .build()?;
        let s = Self {
            backend: Arc::new(backend),
            prefix: c.backend.prefix.trim_end_matches('/').into(),
            disk,
            secrets,
            cpu: Arc::new(Semaphore::new(budget.cpu_jobs)),
            requests: Arc::new(Semaphore::new(budget.backend_concurrency)),
            fifo: tokio::sync::Mutex::new(Fifo::default()),
            cache_writes: tokio::sync::Mutex::new(()),
            fills: Arc::new(Semaphore::new(budget.cpu_jobs)),
            max_entries: budget.cache_entries,
            capacity: c.cache.max_size.as_deref().map(config::bytes).transpose()?,
            min_compression_savings_percent: c.cache.min_compression_savings_percent,
            misses: Mutex::new(HashMap::new()),
            backend_gets: 0.into(),
            cache_hits: 0.into(),
            backend_puts: 0.into(),
            backend_deletes: 0.into(),
            backend_read_bytes: 0.into(),
            backend_write_bytes: 0.into(),
            cache_hit_bytes: 0.into(),
            cache_lookups: 0.into(),
            operations: std::array::from_fn(|_| Arc::default()),
        };
        s.scan().await?;
        Ok(s)
    }
    pub fn statistics(&self) -> serde_json::Value {
        let lookups = self.cache_lookups.load(Ordering::Relaxed);
        serde_json::json!({"backend":{"get":self.operations[0].snapshot(),"put":self.operations[1].snapshot(),"delete":self.operations[2].snapshot()},
            "cache_lookups":lookups,"cache_hit_rate":crate::stats::ratio(self.cache_hits.load(Ordering::Relaxed),lookups).map(|v| v.min(1.0)),
            "cpu_slots_available":self.cpu.available_permits(),"backend_slots_available":self.requests.available_permits(),
            "cache_limit_bytes":self.capacity,"multipart_limit_bytes":self.disk.limits[0]})
    }
    pub fn path(&self, id: Uuid) -> Path {
        Path::from(format!("{}{}", self.prefix(), chunk_name(id)))
    }
    pub fn prefix(&self) -> String {
        format!("{}chunks/", self.namespace())
    }
    fn namespace(&self) -> String {
        if self.prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", self.prefix)
        }
    }
    pub fn parse_path(&self, path: &str) -> Option<Uuid> {
        let prefix = Path::from(self.prefix());
        parse_chunk_name(path.strip_prefix(&format!("{prefix}/"))?)
    }
    pub async fn check_identity(&self, db: &sqlx::PgPool) -> Result<()> {
        let (deployment_id, created_at, initialized): (Uuid, chrono::DateTime<chrono::Utc>, bool) =
            sqlx::query_as("SELECT deployment_id,created_at,backend_initialized FROM gateway_meta")
                .fetch_one(db)
                .await?;
        let path = Path::from(format!("{}meta.json", self.namespace()));
        let saved = match self.backend.get(&path).await {
            Ok(result) => {
                ensure!(
                    result.meta.size <= 16 * 1024,
                    "backend meta.json exceeds limit"
                );
                let mut stream = result.into_stream();
                let mut data = Vec::new();
                while let Some(bytes) = stream.try_next().await? {
                    ensure!(
                        data.len() + bytes.len() <= 16 * 1024,
                        "backend meta.json exceeds limit"
                    );
                    data.extend_from_slice(&bytes);
                }
                Some(
                    serde_json::from_slice::<BackendMeta>(&data)
                        .context("invalid backend meta.json")?,
                )
            }
            Err(object_store::Error::NotFound { .. }) => None,
            Err(e) => return Err(e.into()),
        };
        if let Some(meta) = saved {
            ensure!(
                meta.deployment_id == deployment_id,
                "backend deployment ID differs from database"
            );
            ensure!(
                meta.format_version == 1 && meta.chunk_layout == "uuid-prefix2",
                "unsupported backend storage format"
            );
        } else {
            ensure!(
                !initialized,
                "backend meta.json is missing; restore the matching marker before startup"
            );
            let used: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM chunks) OR EXISTS(SELECT 1 FROM objects)",
            )
            .fetch_one(db)
            .await?;
            ensure!(
                !used,
                "cannot initialize backend marker for an existing database"
            );
            let prefix = Path::from(self.prefix.clone());
            let namespace = if self.prefix.is_empty() {
                String::new()
            } else {
                format!("{prefix}/")
            };
            let mut objects = self.backend.list(if self.prefix.is_empty() {
                None
            } else {
                Some(&prefix)
            });
            while let Some(object) = objects.try_next().await? {
                ensure!(
                    !object.location.as_ref().starts_with(&namespace),
                    "backend namespace is not empty; refusing initialization"
                );
            }
            let meta = BackendMeta {
                format_version: 1,
                deployment_id,
                chunk_layout: "uuid-prefix2".into(),
                created_at,
                created_by: format!("{}/{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION")),
            };
            // Create-only protects an existing marker if another database races initialization.
            self.backend
                .put_opts(
                    &path,
                    Bytes::from(serde_json::to_vec_pretty(&meta)?).into(),
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
                .await
                .context("cannot create backend meta.json without overwriting")?;
        }
        // A crash after the S3 write is recoverable using this database's committed deployment ID.
        sqlx::query(
            "UPDATE gateway_meta SET backend_initialized=true WHERE NOT backend_initialized",
        )
        .execute(db)
        .await?;
        Ok(())
    }
    fn cache_path(&self, id: Uuid, compressed: bool) -> PathBuf {
        self.disk
            .root
            .join("chunks")
            .join(chunk_name(id))
            .with_extension(if compressed { "zst" } else { "raw" })
    }
    async fn scan(&self) -> Result<()> {
        let mut parts = tokio::fs::read_dir(self.disk.root.join("multipart")).await?;
        while let Some(e) = parts.next_entry().await? {
            if e.file_type().await?.is_file() {
                self.disk
                    .account_existing(Area::Multipart, e.metadata().await?.len());
            }
        }
        let mut dirs = tokio::fs::read_dir(self.disk.root.join("chunks")).await?;
        while let Some(dir) = dirs.next_entry().await? {
            if !dir.file_type().await?.is_dir() {
                continue;
            }
            let mut files = tokio::fs::read_dir(dir.path()).await?;
            while let Some(file) = files.next_entry().await? {
                if !file.file_type().await?.is_file() {
                    continue;
                }
                let len = file.metadata().await?.len();
                let cached = parse_cache_name(&format!(
                    "{}/{}",
                    dir.file_name().to_string_lossy(),
                    file.file_name().to_string_lossy()
                ));
                let mut fifo = self.fifo.lock().await;
                if let Some((id, compressed)) = cached.filter(|(id, _)| {
                    (1..=MAX as u64).contains(&len)
                        && !fifo.entries.contains_key(id)
                        && fifo.entries.len() < self.max_entries
                        && self
                            .capacity
                            .is_none_or(|c| self.disk.used()[1].saturating_add(len) <= c)
                }) {
                    fifo.insert(id, len, compressed);
                    self.disk.account_existing(Area::Cache, len);
                } else {
                    tokio::fs::remove_file(file.path()).await?;
                }
            }
        }
        Ok(())
    }
    pub async fn encode(&self, c: Chunk, raw: Vec<u8>) -> Result<(Chunk, Vec<u8>, Bytes)> {
        let permit = self.cpu.clone().acquire_owned().await?;
        let secrets = self.secrets.clone();
        let threshold = self.min_compression_savings_percent;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            codec::encode(c, raw, &secrets, threshold)
        })
        .await?
    }
    async fn decode(&self, c: &Chunk, encoded: Vec<u8>) -> Result<(Bytes, Bytes)> {
        let permit = self.cpu.clone().acquire_owned().await?;
        let c = c.clone();
        let secrets = self.secrets.clone();
        let threshold = self.min_compression_savings_percent;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            codec::decode(&c, encoded, &secrets, threshold)
        })
        .await?
    }
    pub async fn put(self: &Arc<Self>, c: &Chunk, encoded: Vec<u8>, cache: Bytes) -> Result<()> {
        ensure!(
            Some(encoded.len() as i32) == c.stored_size,
            "encoded length mismatch"
        );
        let permit = self.requests.acquire().await?;
        let mut operation = self.operations[1].begin();
        self.backend_puts.fetch_add(1, Ordering::Relaxed);
        let size = encoded.len();
        let result = self
            .backend
            .put(&self.path(c.storage_id), Bytes::from(encoded).into())
            .await;
        operation.finish(result.is_err());
        result?;
        drop(permit);
        self.operations[1].bytes(size as u64);
        self.backend_write_bytes
            .fetch_add(size as u64, Ordering::Relaxed);
        if let Err(e) = self.fill(c, cache).await {
            tracing::warn!(error=%e,"cache fill failed; backend upload is durable");
        }
        Ok(())
    }
    async fn cached(&self, c: &Chunk) -> Result<Option<Bytes>> {
        let (pin, compressed) = {
            let mut fifo = self.fifo.lock().await;
            let Some(e) = fifo.entries.get_mut(&c.storage_id) else {
                return Ok(None);
            };
            e.pins.fetch_add(1, Ordering::Relaxed);
            e.frequency = e.frequency.saturating_add(1).min(3);
            (CachePin(e.pins.clone()), e.compressed)
        };
        let data = read_bounded(self.cache_path(c.storage_id, compressed), MAX).await;
        drop(pin);
        if let Ok(data) = data {
            let size = data.len() as u64;
            let permit = self.cpu.clone().acquire_owned().await?;
            let chunk = c.clone();
            let decoded = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                codec::decode_cache(&chunk, Bytes::from(data), compressed)
            })
            .await?;
            if let Ok(raw) = decoded {
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
                self.cache_hit_bytes.fetch_add(size, Ordering::Relaxed);
                return Ok(Some(raw));
            }
        }
        self.invalidate(c.storage_id).await?;
        Ok(None)
    }
    async fn invalidate(&self, id: Uuid) -> Result<()> {
        let _write = self.cache_writes.lock().await;
        let mut fifo = self.fifo.lock().await;
        let Some(entry) = fifo.entries.get(&id) else {
            return Ok(());
        };
        if entry.pins.load(Ordering::Relaxed) > 0 {
            return Ok(());
        }
        match tokio::fs::remove_file(self.cache_path(id, entry.compressed)).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        if let Some(e) = fifo.forget(id) {
            self.disk.release(Area::Cache, e.size);
        }
        Ok(())
    }
    async fn fill(self: &Arc<Self>, c: &Chunk, data: Bytes) -> Result<()> {
        let Ok(permit) = self.fills.clone().try_acquire_owned() else {
            return Ok(());
        };
        let storage = self.clone();
        let id = c.storage_id;
        let compressed = codec::cache_compressed(c, self.min_compression_savings_percent);
        tokio::spawn(async move {
            let _permit = permit;
            storage.fill_owned(id, &data, compressed).await
        })
        .await?
    }
    async fn fill_owned(&self, id: Uuid, data: &[u8], compressed: bool) -> Result<()> {
        let size = data.len() as u64;
        if self.capacity.is_some_and(|capacity| size > capacity) {
            return Ok(());
        }
        // Serialize publication/invalidation while cache hits only need the FIFO index lock.
        let _write = self.cache_writes.lock().await;
        let mut fifo = self.fifo.lock().await;
        let ticket = {
            if fifo.entries.contains_key(&id) {
                return Ok(());
            }
            loop {
                if fifo.entries.len() < self.max_entries
                    && let Ok(ticket) = self.disk.reserve(Area::Cache, size)
                {
                    break ticket;
                }
                let capacity = self.capacity.unwrap_or(self.disk.used()[1].max(size));
                let Some(victim) = fifo.victim(capacity, self.max_entries) else {
                    return Ok(());
                };
                let entry = &fifo.entries[&victim];
                match tokio::fs::remove_file(self.cache_path(victim, entry.compressed)).await {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
                if let Some(e) = fifo.remove(victim) {
                    self.disk.release(Area::Cache, e.size);
                }
            }
        };
        drop(fifo);
        let path = self.cache_path(id, compressed);
        #[cfg(feature = "fault-injection")]
        crate::faults::point("cache-reserved").await;
        let tmp = path.with_extension(if compressed { "zst.tmp" } else { "raw.tmp" });
        tokio::fs::create_dir_all(path.parent().unwrap()).await?;
        let result = async {
            let mut f = tokio::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
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
    pub async fn get(self: &Arc<Self>, c: &Chunk) -> Result<Bytes> {
        self.cache_lookups.fetch_add(1, Ordering::Relaxed);
        if let Some(raw) = self.cached(c).await? {
            return Ok(raw);
        }
        let lock = {
            let mut map = self.misses.lock().unwrap();
            map.retain(|_, v| v.strong_count() > 0);
            if let Some(lock) = map.get(&c.storage_id).and_then(|v| v.upgrade()) {
                lock
            } else {
                let lock = Arc::new(tokio::sync::OnceCell::new());
                map.insert(c.storage_id, Arc::downgrade(&lock));
                lock
            }
        };
        match lock
            .get_or_init(|| async { self.fetch(c).await.map_err(Arc::new) })
            .await
        {
            Ok(raw) => Ok(raw.clone()),
            Err(error) => Err(anyhow::anyhow!("chunk read failed: {error:#}")),
        }
    }
    async fn fetch(self: &Arc<Self>, c: &Chunk) -> Result<Bytes> {
        if let Some(raw) = self.cached(c).await? {
            return Ok(raw);
        }
        let encoded = {
            let _permit = self.requests.acquire().await?;
            let mut operation = self.operations[0].begin();
            self.backend_gets
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let result: Result<Vec<u8>> = async {
                let result = self.backend.get(&self.path(c.storage_id)).await?;
                ensure!(
                    result.meta.size <= (MAX + 16) as u64,
                    "backend chunk exceeds bound"
                );
                let mut stream = result.into_stream();
                let mut data = Vec::new();
                while let Some(bytes) = stream.next().await {
                    let bytes = bytes?;
                    ensure!(
                        data.len() + bytes.len() <= MAX + 16,
                        "backend chunk exceeds bound"
                    );
                    data.extend_from_slice(&bytes);
                }
                Ok(data)
            }
            .await;
            operation.finish(result.is_err());
            let data = result?;
            self.operations[0].bytes(data.len() as u64);
            data
        };
        self.backend_read_bytes
            .fetch_add(encoded.len() as u64, Ordering::Relaxed);
        let (raw, cache) = self.decode(c, encoded).await?;
        if let Err(e) = self.fill(c, cache).await {
            tracing::warn!(error=%e,"cache fill failed; serving verified backend data");
        }
        Ok(raw)
    }
    pub async fn delete(&self, id: Uuid) -> Result<()> {
        let _permit = self.requests.acquire().await?;
        let mut operation = self.operations[2].begin();
        self.backend_deletes.fetch_add(1, Ordering::Relaxed);
        let result = self.backend.delete(&self.path(id)).await;
        operation.finish(result.is_err());
        result?;
        self.invalidate(id).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sweep_accepts_only_canonical_sharded_keys() {
        let id = Uuid::parse_str("084f2ff912ff4c6daef1b416fee7b800").unwrap();
        assert_eq!(parse_chunk_name(&chunk_name(id)), Some(id));
        assert_eq!(
            parse_cache_name(&format!("{}.raw", chunk_name(id))),
            Some((id, false))
        );
        assert_eq!(
            parse_cache_name(&format!("{}.zst", chunk_name(id))),
            Some((id, true))
        );
        for suffix in ["", ".tmp", ".raw.tmp", ".zst.tmp", ".unknown"] {
            assert!(parse_cache_name(&format!("{}{suffix}", chunk_name(id))).is_none());
        }
        for name in [
            "084f2ff912ff4c6daef1b416fee7b800",
            "09/084f2ff912ff4c6daef1b416fee7b800",
            "08/084F2FF912FF4C6DAEF1B416FEE7B800",
            "08/084f2ff912ff4c6daef1b416fee7b800/other",
            "08/../084f2ff912ff4c6daef1b416fee7b800",
            "meta.json",
        ] {
            assert!(parse_chunk_name(name).is_none());
            assert!(parse_cache_name(&format!("{name}.raw")).is_none());
        }
    }
    #[test]
    #[ignore = "synthetic hit-ratio replay; run with --ignored --nocapture"]
    fn fifo_lru_trace_comparison() {
        for pattern in ["uniform", "hot_with_scan"] {
            let mut fifo = Fifo::default();
            let mut lru = VecDeque::new();
            let (mut fifo_hits, mut lru_hits) = (0, 0);
            let mut random = 20260922u64;
            for i in 0..50_000u64 {
                random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                let key = if pattern == "uniform" {
                    (random >> 32) % 1000
                } else if i % 100 < 80 {
                    (random >> 32) % 50
                } else {
                    1000 + i
                };
                let id = Uuid::from_u128(key as u128);
                if let Some(e) = fifo.entries.get_mut(&id) {
                    e.frequency = (e.frequency + 1).min(3);
                    fifo_hits += 1;
                } else {
                    if fifo.entries.len() == 100 {
                        let victim = fifo.victim(100, 100).unwrap();
                        fifo.remove(victim);
                    }
                    fifo.insert(id, 1, false);
                }
                if let Some(position) = lru.iter().position(|v| *v == id) {
                    lru.remove(position);
                    lru_hits += 1;
                } else if lru.len() == 100 {
                    lru.pop_front();
                }
                lru.push_back(id);
                assert!(fifo.entries.len() <= 100 && fifo.ghosts.len() <= 100);
            }
            println!(
                "trace={pattern} requests=50000 entries=100 bytes=100 fifo_hits={fifo_hits} lru_hits={lru_hits}"
            );
        }
    }
    #[test]
    fn fifo_promotes_reused_items_and_bounds_ghosts() {
        let mut q = Fifo::default();
        let a = Uuid::from_u128(1);
        let b = Uuid::from_u128(2);
        q.insert(a, 100, false);
        q.insert(b, 100, true);
        q.entries.get_mut(&a).unwrap().frequency = 2;
        assert_eq!(q.victim(200, 1), Some(b));
        q.remove(b);
        assert!(q.entries.get(&a).is_some_and(|e| e.queue == Queue::Main));
        q.insert(b, 100, true);
        assert!(q.entries.get(&b).is_some_and(|e| e.queue == Queue::Main));
        for e in q.entries.values_mut() {
            e.pins.store(1, Ordering::Relaxed);
        }
        assert_eq!(q.victim(200, 1), None);
        assert!(q.ghosts.len() <= 1);
    }
}
