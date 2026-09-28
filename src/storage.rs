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
    #[serde(default)]
    pack_layout: Option<String>,
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
    let size = f.metadata().await?.len();
    ensure!(size <= max as u64, "local file exceeds bound");
    let mut bytes = Vec::with_capacity(size as usize);
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
pub struct LoadedPack {
    pub pack: crate::pack::Pack,
    pub data: crate::pack::Decoded,
    _memory: tokio::sync::OwnedSemaphorePermit,
    observe: bool,
    range: bool,
    used: Mutex<HashMap<i64, (usize, usize)>>,
    access: Arc<crate::access::Access>,
}
impl LoadedPack {
    fn touch(&self, c: &Chunk) {
        if self
            .used
            .lock()
            .unwrap()
            .insert(
                c.id,
                (
                    c.raw_size as usize,
                    c.stored_size.unwrap_or(c.raw_size + 16) as usize,
                ),
            )
            .is_none()
            && self.observe
            && self.range
        {
            self.access.origin(c.id);
        }
    }
}
impl Drop for LoadedPack {
    fn drop(&mut self) {
        if self.observe {
            let used = self.used.lock().unwrap();
            let raw: usize = used.values().map(|(n, _)| n).sum();
            let bytes = self.pack.stored_size.unwrap_or(0) as usize;
            let useful: usize = used.values().map(|(_, n)| n).sum();
            self.access.pack(
                self.pack.id,
                bytes,
                self.range && raw < self.pack.raw_size as usize,
                useful.min(bytes),
            );
        }
    }
}
#[derive(Default)]
pub struct ReadContext {
    pack: tokio::sync::Mutex<Option<Arc<LoadedPack>>>,
    observe: bool,
    range: bool,
}
impl ReadContext {
    pub fn user(range: bool) -> Self {
        Self {
            observe: true,
            range,
            ..Default::default()
        }
    }
    async fn touch(&self, c: &Chunk) {
        if self.observe
            && let Some(p) = self.pack.lock().await.as_ref()
            && p.data.members.iter().any(|m| m.id == c.id)
        {
            p.touch(c);
        }
    }
}
enum Source {
    Chunk(Chunk, CachePin),
    Pack(crate::pack::Pack, CachePin),
}
#[derive(sqlx::FromRow)]
struct InspectionMember {
    #[sqlx(flatten)]
    chunk: Chunk,
    offset_bytes: i64,
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
    fn restore_victim(&mut self, id: Uuid) {
        match self.entries[&id].queue {
            Queue::Small => self.small.push_front(id),
            Queue::Main => self.main.push_front(id),
        }
    }
    // Commit eviction only after the cache file is gone.
    fn evict(&mut self, id: Uuid, max_ghosts: usize) -> Option<Entry> {
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
    fn victim(&mut self, capacity: u64) -> Option<Uuid> {
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
            return Some(id);
        }
        None
    }
}

type FetchResult =
    tokio::sync::OnceCell<Result<(Bytes, Option<Arc<LoadedPack>>), Arc<anyhow::Error>>>;
struct PackFetch {
    result: tokio::sync::OnceCell<Result<Arc<LoadedPack>, Arc<anyhow::Error>>>,
    priority: Arc<AtomicUsize>,
}

pub struct Storage {
    pub access: Arc<crate::access::Access>,
    maintenance: Arc<std::sync::atomic::AtomicBool>,
    pub db: sqlx::PgPool,
    pub source_gate: tokio::sync::RwLock<()>,
    physical_pins: Mutex<HashMap<Uuid, std::sync::Weak<AtomicUsize>>>,
    pack_misses: Mutex<HashMap<i64, std::sync::Weak<PackFetch>>>,
    pack_live: Mutex<HashMap<i64, std::sync::Weak<LoadedPack>>>,
    pack_memory: Arc<Semaphore>,
    pack_memory_units: u32,
    pack_work: Arc<Semaphore>,
    inspection: tokio::sync::Mutex<()>,
    backend: Arc<dyn ObjectStore>,
    prefix: String,
    pub disk: Arc<Disk>,
    pub secrets: Arc<Secrets>,
    cpu: Arc<Semaphore>,
    pub compression: Arc<crate::compression::Pool>,
    pub reads: Arc<crate::backend::Gate>,
    pub writes: Arc<crate::backend::Gate>,
    pub controls: Arc<crate::backend::Gate>,
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
    operations: [Arc<crate::stats::Counters>; 4],
}
impl Storage {
    pub async fn new(
        c: &Config,
        secrets: Arc<Secrets>,
        budget: &Budget,
        db: sqlx::PgPool,
        maintenance: Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<Self> {
        let pack_units = budget.pack_memory_units;
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
                c.cache
                    .max_size
                    .as_deref()
                    .map(config::cache_bytes)
                    .transpose()?,
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
                    .with_connect_timeout(Duration::from_secs(config::seconds(
                        &c.backend.connect_timeout,
                    )?))
                    .with_timeout(Duration::from_secs(config::seconds(
                        &c.backend.request_timeout,
                    )?)),
            )
            .with_retry(RetryConfig {
                max_retries: c.backend.max_retries,
                retry_timeout: Duration::from_secs(config::seconds(&c.backend.retry_timeout)?),
                ..Default::default()
            })
            .build()?;
        let s = Self {
            maintenance,
            access: Arc::new(crate::access::Access::new(
                budget.cache_entries.clamp(128, 8192),
            )),
            db,
            source_gate: tokio::sync::RwLock::new(()),
            physical_pins: Mutex::new(HashMap::new()),
            pack_misses: Mutex::new(HashMap::new()),
            pack_live: Mutex::new(HashMap::new()),
            pack_memory: Arc::new(Semaphore::new(pack_units as usize)),
            pack_memory_units: pack_units,
            pack_work: Arc::new(Semaphore::new(pack_units as usize)),
            inspection: tokio::sync::Mutex::new(()),
            backend: Arc::new(backend),
            prefix: c.backend.prefix.trim_end_matches('/').into(),
            disk,
            secrets,
            cpu: Arc::new(Semaphore::new(budget.cpu_jobs)),
            compression: Arc::new(crate::compression::Pool::new(
                c.compression.clone(),
                budget.cpu_jobs,
            )?),
            reads: crate::backend::Gate::new(
                c.backend
                    .read_concurrency
                    .unwrap_or(budget.backend_concurrency),
                Duration::from_secs(config::seconds(&c.backend.priority_aging)?),
            ),
            writes: crate::backend::Gate::new(
                c.backend
                    .upload_concurrency
                    .unwrap_or(budget.backend_concurrency),
                Duration::from_secs(config::seconds(&c.backend.priority_aging)?),
            ),
            controls: crate::backend::Gate::new(
                c.backend
                    .control_concurrency
                    .unwrap_or(budget.available_cpus.clamp(1, 8)),
                Duration::from_secs(config::seconds(&c.backend.priority_aging)?),
            ),
            fifo: tokio::sync::Mutex::new(Fifo::default()),
            cache_writes: tokio::sync::Mutex::new(()),
            fills: Arc::new(Semaphore::new(budget.cpu_jobs)),
            max_entries: budget.cache_entries,
            capacity: c
                .cache
                .max_size
                .as_deref()
                .map(config::cache_bytes)
                .transpose()?,
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
        serde_json::json!({"backend":{"get":self.operations[0].snapshot(),"put":self.operations[1].snapshot(),"delete":self.operations[2].snapshot(),"head":self.operations[3].snapshot()},
            "cache_lookups":lookups,"cache_hit_rate":crate::stats::ratio(self.cache_hits.load(Ordering::Relaxed),lookups).map(|v| v.min(1.0)),
            "cpu_slots_available":self.cpu.available_permits(),"backend_queues":{"read":self.reads.snapshot(),"upload":self.writes.snapshot(),"control":self.controls.snapshot()},
            "cache_limit_bytes":self.capacity,"multipart_limit_bytes":self.disk.limits[0]})
    }
    pub fn path(&self, id: Uuid) -> Path {
        Path::from(format!("{}{}", self.prefix(), chunk_name(id)))
    }
    pub fn prefix(&self) -> String {
        format!("{}chunks/", self.namespace())
    }
    pub fn namespace(&self) -> String {
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
    pub fn parse_physical_path(&self, path: &str) -> Option<Uuid> {
        self.parse_path(path).or_else(|| {
            parse_chunk_name(path.strip_prefix(&format!("{}packs/", self.namespace()))?)
        })
    }
    pub async fn list_physical(
        &self,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<object_store::ObjectMeta>> {
        let _permit = crate::backend::PRIORITY
            .scope(crate::backend::MAINTENANCE, self.controls.acquire())
            .await?;
        let prefix = Path::from(self.namespace());
        let offset = Path::parse(cursor.unwrap_or(""))?;
        Ok(self
            .backend
            .list_with_offset(Some(&prefix), &offset)
            .filter(|r| {
                std::future::ready(
                    r.as_ref()
                        .map_or(true, |m| m.location.as_ref().starts_with(&self.namespace())),
                )
            })
            .take(limit)
            .try_collect()
            .await?)
    }
    pub async fn check_identity(&self, db: &sqlx::PgPool, maintenance: bool) -> Result<()> {
        let (deployment_id, created_at, initialized): (Uuid, chrono::DateTime<chrono::Utc>, bool) =
            sqlx::query_as("SELECT deployment_id,created_at,backend_initialized FROM gateway_meta")
                .fetch_one(db)
                .await?;
        let path = Path::from(format!("{}meta.json", self.namespace()));
        let read_permit = self.reads.acquire().await?;
        let saved = match self.backend.get(&path).await {
            Ok(result) => {
                ensure!(
                    result.meta.size <= 16 * 1024,
                    "backend meta.json exceeds limit"
                );
                let version = object_store::UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                };
                let mut stream = result.into_stream();
                let mut data = Vec::new();
                while let Some(bytes) = stream.try_next().await? {
                    ensure!(
                        data.len() + bytes.len() <= 16 * 1024,
                        "backend meta.json exceeds limit"
                    );
                    data.extend_from_slice(&bytes);
                }
                Some((
                    serde_json::from_slice::<BackendMeta>(&data)
                        .context("invalid backend meta.json")?,
                    version,
                ))
            }
            Err(object_store::Error::NotFound { .. }) => None,
            Err(e) => return Err(e.into()),
        };
        drop(read_permit);
        if let Some((mut meta, version)) = saved {
            ensure!(
                meta.deployment_id == deployment_id,
                "backend deployment ID differs from database"
            );
            ensure!(
                (meta.format_version == 1
                    || (meta.format_version == 2
                        && meta.pack_layout.as_deref() == Some("uuid-prefix2-v1")))
                    && meta.chunk_layout == "uuid-prefix2",
                "unsupported backend storage format"
            );
            if meta.format_version == 1 && !maintenance {
                meta.format_version = 2;
                meta.pack_layout = Some("uuid-prefix2-v1".into());
                meta.created_by =
                    format!("{}/{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
                let _permit = self.writes.acquire().await?;
                self.backend
                    .put_opts(
                        &path,
                        Bytes::from(serde_json::to_vec_pretty(&meta)?).into(),
                        PutOptions {
                            mode: PutMode::Update(version),
                            ..Default::default()
                        },
                    )
                    .await
                    .context("cannot upgrade backend marker conditionally")?;
            }
        } else {
            ensure!(
                !maintenance,
                "backend marker is missing; initialize outside maintenance mode"
            );
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
            let control_permit = self.controls.acquire().await?;
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
            drop(objects);
            drop(control_permit);
            let meta = BackendMeta {
                format_version: 2,
                deployment_id,
                chunk_layout: "uuid-prefix2".into(),
                pack_layout: Some("uuid-prefix2-v1".into()),
                created_at,
                created_by: format!("{}/{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION")),
            };
            // Create-only protects an existing marker if another database races initialization.
            let _write_permit = self.writes.acquire().await?;
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
    pub async fn encode(
        &self,
        c: Chunk,
        raw: Vec<u8>,
        should_compress: bool,
    ) -> Result<(Chunk, Vec<u8>, Bytes)> {
        let permit = self.cpu.clone().acquire_owned().await?;
        let secrets = self.secrets.clone();
        let threshold = self.min_compression_savings_percent;
        let compression = self.compression.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            codec::encode(c, raw, &secrets, threshold, &compression, should_compress)
        })
        .await?
    }
    pub(crate) async fn decode(&self, c: &Chunk, encoded: Vec<u8>) -> Result<(Bytes, Bytes)> {
        let permit = self.cpu.clone().acquire_owned().await?;
        let c = c.clone();
        let secrets = self.secrets.clone();
        let threshold = self.min_compression_savings_percent;
        let compression = self.compression.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            codec::decode(&c, encoded, &secrets, threshold, &compression)
        })
        .await?
    }
    pub async fn put(self: &Arc<Self>, c: &Chunk, encoded: Vec<u8>, cache: Bytes) -> Result<()> {
        ensure!(
            Some(encoded.len() as i32) == c.stored_size,
            "encoded length mismatch"
        );
        let permit = self.writes.acquire().await?;
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
            let compression = self.compression.clone();
            let decoded = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                codec::decode_cache(&chunk, Bytes::from(data), compressed, &compression)
            })
            .await?;
            if let Ok(raw) = decoded {
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
                self.cache_hit_bytes.fetch_add(size, Ordering::Relaxed);
                return Ok(Some(raw));
            }
        }
        if let Err(e) = self.invalidate(c.storage_id).await {
            tracing::warn!(storage_id=%c.storage_id,error=%e,"cache invalidation failed; falling back to backend");
        }
        Ok(None)
    }
    pub(crate) async fn invalidate(&self, id: Uuid) -> Result<()> {
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
                let Some(victim) = fifo.victim(capacity) else {
                    return Ok(());
                };
                let entry = &fifo.entries[&victim];
                match tokio::fs::remove_file(self.cache_path(victim, entry.compressed)).await {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => {
                        fifo.restore_victim(victim);
                        return Err(e.into());
                    }
                }
                if let Some(e) = fifo.evict(victim, self.max_entries) {
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
    pub async fn get_with(
        self: &Arc<Self>,
        c: &Chunk,
        context: Option<&ReadContext>,
    ) -> Result<Bytes> {
        self.cache_lookups.fetch_add(1, Ordering::Relaxed);
        if let Some(raw) = self.cached(c).await? {
            if let Some(context) = context {
                context.touch(c).await;
            }
            return Ok(raw);
        }
        if let Some(context) = context {
            let current = context.pack.lock().await;
            if let Some(loaded) = current
                .as_ref()
                .filter(|p| p.data.members.iter().any(|m| m.id == c.id))
            {
                if context.observe {
                    loaded.touch(c);
                }
                return loaded.data.chunk(c);
            }
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
            .get_or_init(|| async { self.fetch(c, context).await.map_err(Arc::new) })
            .await
        {
            Ok((raw, loaded)) => {
                if let (Some(context), Some(loaded)) = (context, loaded) {
                    if context.observe {
                        loaded.touch(c);
                    }
                    // Prefetch may be advancing to the next pack while waiting for this one's
                    // memory. Never retain the previous pack while waiting for that advance.
                    if let Ok(mut current) = context.pack.try_lock() {
                        *current = Some(loaded.clone());
                    }
                }
                Ok(raw.clone())
            }
            Err(error) => Err(anyhow::anyhow!("chunk read failed: {error:#}")),
        }
    }
    async fn fetch(
        self: &Arc<Self>,
        c: &Chunk,
        context: Option<&ReadContext>,
    ) -> Result<(Bytes, Option<Arc<LoadedPack>>)> {
        if let Some(raw) = self.cached(c).await? {
            if let Some(context) = context {
                context.touch(c).await;
            }
            return Ok((raw, None));
        }
        match self.source(c.id).await? {
            Source::Chunk(physical, _pin) => {
                let encoded = self.read_backend(&physical).await?;
                let (raw, cache) = self.decode(&physical, encoded).await?;
                let fill = if physical.compressed == c.compressed
                    && physical.stored_size == c.stored_size
                {
                    self.fill(c, cache).await
                } else {
                    self.fill_raw(c, raw.clone()).await
                };
                if let Err(e) = fill {
                    tracing::warn!(error=%e,"cache fill failed; serving verified backend data");
                }
                Ok((raw, None))
            }
            Source::Pack(pack, _pin) => {
                if let Some(context) = context {
                    let mut current = context.pack.lock().await;
                    if let Some(loaded) = current.as_ref().filter(|p| p.pack.id == pack.id) {
                        if context.observe {
                            loaded.touch(c);
                        }
                        return Ok((loaded.data.chunk(c)?, Some(loaded.clone())));
                    }
                    *current = None;
                    let loaded = self.load_pack(pack, context.observe, context.range).await?;
                    if context.observe {
                        loaded.touch(c);
                    }
                    let raw = loaded.data.chunk(c)?;
                    *current = Some(loaded.clone());
                    Ok((raw, Some(loaded)))
                } else {
                    let loaded = self.load_pack(pack, false, false).await?;
                    Ok((loaded.data.chunk(c)?, Some(loaded)))
                }
            }
        }
    }
    pub async fn register_location(&self, c: &Chunk) -> Result<()> {
        sqlx::query("INSERT INTO chunk_locations(id,chunk_id,storage_id,stored_size,compressed,nonce,state,created_at,unreferenced_at) VALUES($1,$2,$3,$4,$5,$6,'uploading',$7,now())")
            .bind(c.encoding_id).bind(c.id).bind(c.storage_id).bind(c.stored_size).bind(c.compressed).bind(&c.nonce).bind(c.created_at).execute(&self.db).await?;
        Ok(())
    }
    fn physical_pin(&self, id: Uuid) -> CachePin {
        let mut pins = self.physical_pins.lock().unwrap();
        pins.retain(|_, p| p.strong_count() > 0);
        let counter = pins
            .get(&id)
            .and_then(std::sync::Weak::upgrade)
            .unwrap_or_else(|| {
                let counter = Arc::new(AtomicUsize::new(0));
                pins.insert(id, Arc::downgrade(&counter));
                counter
            });
        counter.fetch_add(1, Ordering::Relaxed);
        CachePin(counter)
    }
    pub fn physical_active(&self, id: Uuid) -> bool {
        self.physical_pins
            .lock()
            .unwrap()
            .get(&id)
            .and_then(std::sync::Weak::upgrade)
            .is_some_and(|p| p.load(Ordering::Relaxed) > 0)
    }
    async fn source(&self, id: i64) -> Result<Source> {
        let _gate = self.source_gate.read().await;
        let physical:Option<Chunk>=sqlx::query_as("SELECT c.id,l.id encoding_id,l.storage_id,c.hash,c.raw_size,l.stored_size,c.algorithm,c.key_id,l.compressed,l.nonce,c.format,c.state,l.created_at FROM chunk_locations l JOIN chunks c ON c.id=l.chunk_id WHERE c.id=$1 AND l.state='ready' AND c.state='ready'")
            .bind(id).fetch_optional(&self.db).await?;
        if let Some(c) = physical {
            let pin = self.physical_pin(c.storage_id);
            return Ok(Source::Chunk(c, pin));
        }
        let p:crate::pack::Pack=sqlx::query_as("SELECT p.* FROM chunks c JOIN packs p ON p.id=c.pack_id WHERE c.id=$1 AND c.state='ready' AND p.state='ready'")
            .bind(id).fetch_optional(&self.db).await?.context(codec::IntegrityError::Metadata)?;
        let pin = self.physical_pin(p.storage_id);
        Ok(Source::Pack(p, pin))
    }
    pub fn pack_path(&self, id: Uuid) -> Path {
        Path::from(format!("{}packs/{}", self.namespace(), chunk_name(id)))
    }
    pub async fn pack_memory(&self, size: usize) -> Result<tokio::sync::OwnedSemaphorePermit> {
        let workspace = self.compression.workspace_bytes()?;
        let units = (size as u64 * 4 + workspace).div_ceil(1024 * 1024);
        ensure!(
            units <= u64::from(self.pack_memory_units),
            "pack exceeds available memory budget"
        );
        Ok(self
            .pack_memory
            .clone()
            .acquire_many_owned(units as u32)
            .await?)
    }
    pub async fn pack_work(&self, size: usize) -> Result<tokio::sync::OwnedSemaphorePermit> {
        let units = (size as u64 * 4 + self.compression.workspace_bytes()?).div_ceil(1024 * 1024);
        ensure!(
            units <= u64::from(self.pack_memory_units),
            "pack rewrite exceeds available memory budget"
        );
        Ok(self
            .pack_work
            .clone()
            .acquire_many_owned(units as u32)
            .await?)
    }
    pub async fn encode_pack(
        &self,
        p: crate::pack::Pack,
        members: Vec<(Chunk, Bytes)>,
        strategy: crate::compression::Strategy,
        should_try: bool,
    ) -> Result<(crate::pack::Pack, Vec<u8>)> {
        let permit = self.cpu.clone().acquire_owned().await?;
        let secrets = self.secrets.clone();
        let pool = self.compression.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            crate::pack::encode(p, &members, &secrets, &pool, strategy, should_try)
        })
        .await?
    }
    pub async fn put_path(&self, path: &Path, data: Vec<u8>) -> Result<()> {
        let _permit = self.writes.acquire().await?;
        self.background_writable()?;
        let mut operation = self.operations[1].begin();
        self.backend_puts.fetch_add(1, Ordering::Relaxed);
        let size = data.len() as u64;
        let result = self.backend.put(path, Bytes::from(data).into()).await;
        operation.finish(result.is_err());
        result?;
        self.operations[1].bytes(size);
        self.backend_write_bytes.fetch_add(size, Ordering::Relaxed);
        Ok(())
    }
    pub async fn delete_path(&self, path: &Path) -> Result<()> {
        let _permit = self.controls.acquire().await?;
        self.background_writable()?;
        let mut operation = self.operations[2].begin();
        self.backend_deletes.fetch_add(1, Ordering::Relaxed);
        let result = self.backend.delete(path).await;
        operation.finish(result.is_err());
        result?;
        Ok(())
    }
    fn background_writable(&self) -> Result<()> {
        ensure!(
            !self.maintenance.load(Ordering::Acquire)
                || crate::backend::PRIORITY
                    .try_with(|p| *p)
                    .unwrap_or(crate::backend::FOREGROUND)
                    == crate::backend::FOREGROUND,
            "background writes are paused for maintenance"
        );
        Ok(())
    }
    async fn load_pack(
        self: &Arc<Self>,
        p: crate::pack::Pack,
        observe: bool,
        range: bool,
    ) -> Result<Arc<LoadedPack>> {
        if let Some(live) = self
            .pack_live
            .lock()
            .unwrap()
            .get(&p.id)
            .and_then(std::sync::Weak::upgrade)
        {
            return Ok(live);
        }
        let lock = {
            let mut map = self.pack_misses.lock().unwrap();
            map.retain(|_, v| v.strong_count() > 0);
            map.get(&p.id)
                .and_then(std::sync::Weak::upgrade)
                .unwrap_or_else(|| {
                    let cell = Arc::new(PackFetch {
                        result: tokio::sync::OnceCell::new(),
                        priority: Arc::new(AtomicUsize::new(crate::backend::MAINTENANCE)),
                    });
                    map.insert(p.id, Arc::downgrade(&cell));
                    cell
                })
        };
        lock.priority.fetch_min(
            crate::backend::PRIORITY
                .try_with(|p| *p)
                .unwrap_or(crate::backend::FOREGROUND),
            Ordering::Relaxed,
        );
        self.reads.refresh();
        let result = lock
            .result
            .get_or_init(|| async {
                let result: Result<Arc<LoadedPack>> = async {
                    let memory = self.pack_memory(p.payload_size()?).await?;
                    let read_permit = self
                        .reads
                        .acquire_shared(Some(lock.priority.clone()))
                        .await?;
                    #[cfg(feature = "fault-injection")]
                    crate::faults::point("pack-download").await;
                    let encoded = self
                        .read_path_permitted(&self.pack_path(p.storage_id), p.payload_size()? + 16)
                        .await?;
                    drop(read_permit);
                    let permit = self.cpu.clone().acquire_owned().await?;
                    let secrets = self.secrets.clone();
                    let pool = self.compression.clone();
                    let pack = p.clone();
                    let data = tokio::task::spawn_blocking(move || {
                        let _permit = permit;
                        crate::pack::decode(&pack, encoded, &secrets, &pool)
                    })
                    .await??;
                    let loaded = Arc::new(LoadedPack {
                        pack: p.clone(),
                        data,
                        _memory: memory,
                        observe,
                        range,
                        used: Mutex::new(HashMap::new()),
                        access: self.access.clone(),
                    });
                    {
                        let mut live = self.pack_live.lock().unwrap();
                        live.retain(|_, v| v.strong_count() > 0);
                        live.insert(p.id, Arc::downgrade(&loaded));
                    }
                    if let Ok(permit) = self.fills.clone().try_acquire_owned() {
                        let storage = self.clone();
                        let loaded = loaded.clone();
                        tokio::spawn(async move {
                            let _permit = permit;
                            if let Err(e) = storage.fill_pack(&loaded).await {
                                tracing::warn!(error=%e,"pack cache fill failed");
                            }
                        });
                    }
                    Ok(loaded)
                }
                .await;
                result.map_err(Arc::new)
            })
            .await;
        result
            .as_ref()
            .map(Arc::clone)
            .map_err(|e| anyhow::anyhow!("pack read failed: {e:#}"))
    }
    async fn fill_pack(self: &Arc<Self>, loaded: &LoadedPack) -> Result<()> {
        let ids: Vec<i64> = loaded.data.members.iter().map(|m| m.id).collect();
        let chunks: Vec<Chunk> =
            sqlx::query_as("SELECT * FROM chunks WHERE id=ANY($1) AND state='ready'")
                .bind(ids)
                .fetch_all(&self.db)
                .await?;
        for c in chunks {
            self.fill_raw(&c, loaded.data.chunk(&c)?).await?;
        }
        Ok(())
    }
    pub async fn fill_raw(&self, c: &Chunk, raw: Bytes) -> Result<()> {
        if self.capacity == Some(0) {
            return Ok(());
        }
        if codec::cache_compressed(c, self.min_compression_savings_percent) {
            let permit = self.cpu.clone().acquire_owned().await?;
            let pool = self.compression.clone();
            let input = raw.clone();
            let compressed = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                pool.compress_with(&input, true, crate::compression::Strategy::Always)
            })
            .await??;
            if let Some(data) = compressed {
                return self.fill_owned(c.storage_id, &data, true).await;
            }
        }
        self.fill_owned(c.storage_id, &raw, false).await
    }
    pub(crate) async fn head(&self, c: &Chunk) -> Result<u64> {
        let _permit = self.controls.acquire().await?;
        let mut operation = self.operations[3].begin();
        let result = self.backend.head(&self.path(c.storage_id)).await;
        operation.finish(result.is_err());
        Ok(result?.size)
    }
    pub async fn inspect(
        &self,
        task: Uuid,
        c: &Chunk,
        mode: crate::integrity::Mode,
        physical_detail: &mut serde_json::Value,
    ) -> Result<u64> {
        match self.source(c.id).await? {
            Source::Chunk(physical, _pin) => {
                *physical_detail = serde_json::json!({"source":"chunk","encoding_id":physical.encoding_id.to_string(),"storage_id":physical.storage_id});
                codec::validate_metadata(&physical, &self.secrets)?;
                match mode {
                    crate::integrity::Mode::Metadata => Ok(0),
                    crate::integrity::Mode::Head => {
                        let size = self.head(&physical).await?;
                        ensure!(
                            Some(size as i32) == physical.stored_size,
                            codec::IntegrityError::Length
                        );
                        Ok(0)
                    }
                    crate::integrity::Mode::Full => {
                        let data = self.read_backend(&physical).await?;
                        let size = data.len() as u64;
                        self.decode(&physical, data).await?;
                        Ok(size)
                    }
                }
            }
            Source::Pack(p, _pin) => {
                *physical_detail = serde_json::json!({"source":"pack","pack_id":p.id.to_string(),"storage_id":p.storage_id});
                p.validate(&self.secrets)?;
                let capacity = p.payload_size().context(codec::IntegrityError::Metadata)?;
                ensure!(
                    sqlx::query_scalar::<_, bool>(
                        "SELECT EXISTS(SELECT 1 FROM pack_members WHERE pack_id=$1 AND chunk_id=$2)"
                    )
                    .bind(p.id)
                    .bind(c.id)
                    .fetch_one(&self.db)
                    .await?,
                    codec::IntegrityError::Metadata
                );
                // Persistent task results avoid downloading one physical pack per logical chunk.
                let _inspection = self.inspection.lock().await;
                if let Some((code,)) = sqlx::query_as::<_, (Option<String>,)>(
                    "SELECT error_code FROM integrity_packs WHERE task_id=$1 AND pack_id=$2",
                )
                .bind(task)
                .bind(p.id)
                .fetch_optional(&self.db)
                .await?
                {
                    if let Some(code) = code {
                        return Err(codec::IntegrityError::from_code(&code)
                            .context("invalid inspection result")?
                            .into());
                    }
                    return Ok(0);
                }
                let result:Result<u64>=async {
                    let members:Vec<(Chunk,i64)>=sqlx::query_as::<_,InspectionMember>("SELECT c.*,m.offset_bytes FROM pack_members m JOIN chunks c ON c.id=m.chunk_id WHERE m.pack_id=$1 ORDER BY m.ordinal LIMIT 4097")
                        .bind(p.id).fetch_all(&self.db).await?.into_iter().map(|m|(m.chunk,m.offset_bytes)).collect();
                    let mut offset=0i64;
                    ensure!(members.len()==p.member_count as usize,codec::IntegrityError::Metadata);
                    for (c,start) in &members {ensure!(*start==offset && c.raw_size>0,codec::IntegrityError::Metadata);offset+=i64::from(c.raw_size);}
                    ensure!(offset==p.raw_size,codec::IntegrityError::Metadata);
                    if matches!(mode,crate::integrity::Mode::Metadata) {return Ok(0);}
                    if matches!(mode,crate::integrity::Mode::Head) {
                        let _permit=self.controls.acquire().await?;
                        let mut operation=self.operations[3].begin();
                        let result=self.backend.head(&self.pack_path(p.storage_id)).await;operation.finish(result.is_err());
                        ensure!(Some(result?.size as i64)==p.stored_size,codec::IntegrityError::Length);return Ok(0);
                    }
                    let _memory=self.pack_memory(capacity).await?;
                    let data=self.read_path(&self.pack_path(p.storage_id),capacity+16).await?;
                    let size=data.len() as u64;
                    let permit=self.cpu.clone().acquire_owned().await?;let pool=self.compression.clone();let secrets=self.secrets.clone();let pack=p.clone();
                    tokio::task::spawn_blocking(move||{
                        let _permit=permit;let decoded=crate::pack::decode(&pack,data,&secrets,&pool)?;
                        for ((c,offset),member) in members.iter().zip(&decoded.members) {
                            ensure!(member.id==c.id && member.hash.as_slice()==c.hash && member.len==c.raw_size as usize && member.start==12+44*members.len()+*offset as usize,codec::IntegrityError::Metadata);
                        }
                        Ok::<_,anyhow::Error>(())
                    }).await??;
                    Ok(size)
                }.await;
                let code = result.as_ref().err().and_then(|e| {
                    e.downcast_ref::<codec::IntegrityError>()
                        .map(|e| e.code())
                        .or_else(|| {
                            matches!(
                                e.downcast_ref::<object_store::Error>(),
                                Some(object_store::Error::NotFound { .. })
                            )
                            .then_some("remote_missing")
                        })
                });
                if result.is_ok() || code.is_some() {
                    sqlx::query("INSERT INTO integrity_packs(task_id,pack_id,error_code) VALUES($1,$2,$3) ON CONFLICT DO NOTHING").bind(task).bind(p.id).bind(code).execute(&self.db).await?;
                }
                result
            }
        }
    }
    // Shared bounded remote read. Inspection must not use or populate the cache.
    pub(crate) async fn read_backend(&self, c: &Chunk) -> Result<Vec<u8>> {
        self.read_path(&self.path(c.storage_id), MAX + 16).await
    }
    pub async fn read_path(&self, path: &Path, bound: usize) -> Result<Vec<u8>> {
        let _permit = self.reads.acquire().await?;
        self.read_path_permitted(path, bound).await
    }
    async fn read_path_permitted(&self, path: &Path, bound: usize) -> Result<Vec<u8>> {
        let mut operation = self.operations[0].begin();
        self.backend_gets
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let result: Result<Vec<u8>> = async {
            let result = self.backend.get(path).await?;
            ensure!(
                result.meta.size <= bound as u64,
                codec::IntegrityError::Length
            );
            let mut data = Vec::with_capacity(result.meta.size as usize);
            let mut stream = result.into_stream();
            while let Some(bytes) = stream.next().await {
                let bytes = bytes?;
                ensure!(
                    data.len() + bytes.len() <= bound,
                    codec::IntegrityError::Length
                );
                data.extend_from_slice(&bytes);
            }
            Ok(data)
        }
        .await;
        operation.finish(result.is_err());
        let data = result?;
        self.operations[0].bytes(data.len() as u64);
        self.backend_read_bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        Ok(data)
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
                        let victim = fifo.victim(100).unwrap();
                        fifo.evict(victim, 100);
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
        assert_eq!(q.victim(200), Some(b));
        q.evict(b, 1);
        assert!(q.entries.get(&a).is_some_and(|e| e.queue == Queue::Main));
        q.insert(b, 100, true);
        assert!(q.entries.get(&b).is_some_and(|e| e.queue == Queue::Main));
        for e in q.entries.values_mut() {
            e.pins.store(1, Ordering::Relaxed);
        }
        assert_eq!(q.victim(200), None);
        assert!(q.ghosts.len() <= 1);
    }
    #[test]
    fn fifo_failed_evictions_preserve_candidates_and_ghosts() {
        for frequency in [0, 2] {
            let mut q = Fifo::default();
            let old = Uuid::from_u128(1);
            let id = Uuid::from_u128(2);
            q.insert(old, 100, false);
            assert_eq!(q.victim(100), Some(old));
            q.evict(old, 1);
            q.insert(id, 100, true);
            q.entries.get_mut(&id).unwrap().frequency = frequency;
            let queue = if frequency == 0 {
                Queue::Small
            } else {
                Queue::Main
            };
            for _ in 0..3 {
                assert_eq!(q.victim(100), Some(id));
                assert_eq!(q.ghost, HashSet::from([old]));
                assert_eq!(q.ghosts, VecDeque::from([old]));
                q.restore_victim(id);
                assert_eq!(q.entries.len(), 1);
                let e = &q.entries[&id];
                assert!(e.queue == queue && e.size == 100 && e.compressed);
                assert_eq!(e.frequency, 0);
                assert_eq!(q.small_bytes, if queue == Queue::Small { 100 } else { 0 });
                assert_eq!(q.small.len() + q.main.len(), 1);
                assert_eq!(q.small.front() == Some(&id), queue == Queue::Small);
                assert_eq!(q.main.front() == Some(&id), queue == Queue::Main);
            }
            assert_eq!(q.victim(100), Some(id));
            assert_eq!(q.evict(id, 1).unwrap().size, 100);
            assert!(q.entries.is_empty() && q.small.is_empty() && q.main.is_empty());
            assert_eq!(q.small_bytes, 0);
            let ghost = if queue == Queue::Small { id } else { old };
            assert_eq!(q.ghost, HashSet::from([ghost]));
            assert_eq!(q.ghosts, VecDeque::from([ghost]));
        }
    }
}
