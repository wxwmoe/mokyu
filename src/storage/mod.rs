use crate::{
    config::{self, Budget, Config, Secrets},
    content::codec::{self, Chunk, MAX},
};
pub(crate) mod cache;
mod diagnostics;
pub(crate) mod local;
pub(crate) mod location;
pub(crate) mod model;
pub(crate) mod permits;
pub(crate) mod profile;
mod rate;
pub(crate) mod resource_gate;
pub(crate) mod target;

#[cfg(all(test, feature = "fault-injection"))]
#[path = "../../tests/recovery/pack_memory.rs"]
mod memory_tests;

#[cfg(all(test, feature = "fault-injection"))]
#[path = "../../tests/storage/diagnostics.rs"]
mod diagnostic_tests;

use self::model::{Record as StoreRecord, Store as VerifiedStore};
use self::profile::StorageProfile;
pub(crate) use self::{
    cache::*,
    local::*,
    location::{chunk_name, parse_cache_name, parse_chunk_name, parse_upload_cache_name},
    permits::*,
    target::*,
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::PathBuf,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};
use tokio::{io::AsyncWriteExt, sync::Semaphore};
use uuid::Uuid;

pub(crate) const STAGING_PREFIX: &str = "__mokyu/multipart/";
const DEFAULT_STAGING_LIMIT: u64 = 1024 * 1024 * 1024;

pub struct Storage {
    pub access: Arc<crate::status::access::Access>,
    maintenance: Arc<std::sync::atomic::AtomicBool>,
    physical: Arc<dyn crate::model::PhysicalStore>,
    cluster: Option<Arc<dyn crate::model::cluster::ClusterStore>>,
    profile_store: Arc<dyn crate::model::StorageProfileStore>,
    managed_keys: Arc<dyn crate::model::ManagedKeyStore>,
    record_store: Arc<dyn crate::model::StorageRecordStore>,
    pub source_gate: tokio::sync::RwLock<()>,
    physical_pins: Mutex<HashMap<Uuid, std::sync::Weak<AtomicUsize>>>,
    pack_misses: Mutex<HashMap<i64, std::sync::Weak<PackFetch>>>,
    pack_live: Mutex<HashMap<i64, std::sync::Weak<LoadedPack>>>,
    pack_memory: Arc<Semaphore>,
    pack_memory_units: u32,
    pack_work: Arc<Semaphore>,
    inspection: tokio::sync::Mutex<()>,
    targets: RwLock<HashMap<Uuid, Arc<StoreBackend>>>,
    store_defaults: (usize, usize, usize),
    store_gates: Mutex<GateRegistry>,
    integrity_rates: Mutex<HashMap<Uuid, rate::IntegrityRate>>,
    refresh: tokio::sync::Mutex<()>,
    permit_holder: Uuid,
    pub storage_id: Option<Uuid>,
    pub disk: Arc<Disk>,
    staging_storage_id: Option<Uuid>,
    staging_bytes_limit: Option<u64>,
    cluster_mode: bool,
    pub secrets: Arc<Secrets>,
    pub(crate) cpu: Arc<Semaphore>,
    pub compression: Arc<crate::content::compression::Pool>,
    compression_enabled: bool,
    base_storage: config::BackendDefaults,
    base_compression: crate::content::compression::Config,
    cpu_jobs: usize,
    pub reads: Arc<crate::storage::resource_gate::Gate>,
    pub writes: Arc<crate::storage::resource_gate::Gate>,
    pub controls: Arc<crate::storage::resource_gate::Gate>,
    fifo: tokio::sync::Mutex<Fifo>,
    cache_writes: tokio::sync::Mutex<()>,
    upload_pins: Mutex<HashMap<Uuid, UploadPin>>,
    fills: Arc<Semaphore>,
    cache_fills: Arc<Mutex<HashSet<Uuid>>>,
    max_entries: usize,
    capacity: Option<u64>,
    min_compression_savings_percent: u8,
    misses: Mutex<HashMap<(i64, Uuid), std::sync::Weak<FetchResult>>>,
    pub backend_gets: std::sync::atomic::AtomicU64,
    pub cache_hits: std::sync::atomic::AtomicU64,
    pub backend_puts: std::sync::atomic::AtomicU64,
    pub backend_deletes: std::sync::atomic::AtomicU64,
    pub backend_read_bytes: std::sync::atomic::AtomicU64,
    pub backend_write_bytes: std::sync::atomic::AtomicU64,
    pub cache_hit_bytes: std::sync::atomic::AtomicU64,
    cache_lookups: std::sync::atomic::AtomicU64,
    operations: [Arc<crate::status::stats::Counters>; 4],
    diagnostics: Arc<diagnostics::Diagnostics>,
}

async fn build_store(
    options: StoreOptions,
    secrets: &Secrets,
    cluster: &Option<Arc<dyn crate::model::cluster::ClusterStore>>,
    row: crate::model::StorageRecord,
    profile: &StorageProfile,
    base_compression: &crate::content::compression::Config,
    cpu_jobs: usize,
    default_cache_entries: usize,
    defaults: (usize, usize, usize),
    permit_holder: Uuid,
) -> Result<StoreBackend> {
    let crate::model::StorageRecord {
        id,
        store_type,
        path,
        endpoint,
        bucket,
        region,
        prefix,
        access,
        secret,
        identity_marker,
        revision,
    } = row;
    let is_local = store_type == "local";
    let record = StoreRecord {
        id,
        name: String::new(),
        r#type: store_type,
        path,
        endpoint_url: endpoint,
        remote_bucket_name: bucket,
        region,
        prefix: prefix.clone(),
        access_key_encrypted: access,
        secret_key_encrypted: secret,
        identity_marker: identity_marker.clone(),
        enabled: true,
    };
    let target = VerifiedStore::build(&record, options, secrets)?;
    let compression = Arc::new(crate::content::compression::Pool::new(
        profile.compression_config(base_compression),
        cpu_jobs,
    )?);
    let aging = Duration::from_secs(config::seconds(&profile.priority_aging)?);
    Ok(StoreBackend {
        backend: target.backend,
        is_local,
        prefix: prefix.trim_matches('/').into(),
        revision,
        identity_marker,
        identity_ready: false.into(),
        identity_check: tokio::sync::Mutex::new(()),
        reads: StoreGate::new(
            cluster,
            id,
            "read",
            profile.read_concurrency.map_or(defaults.0, |n| n as usize),
            profile.read_concurrency,
            aging,
            permit_holder,
        ),
        writes: StoreGate::new(
            cluster,
            id,
            "upload",
            profile
                .upload_concurrency
                .map_or(defaults.1, |n| n as usize),
            profile.upload_concurrency,
            aging,
            permit_holder,
        ),
        controls: StoreGate::new(
            cluster,
            id,
            "control",
            profile
                .control_concurrency
                .map_or(defaults.2, |n| n as usize),
            profile.control_concurrency,
            aging,
            permit_holder,
        ),
        pack: StoreGate::new(
            cluster,
            id,
            "pack",
            profile.pack_concurrency.max(1) as usize,
            Some(profile.pack_concurrency.max(1)),
            aging,
            permit_holder,
        ),
        integrity: StoreGate::new(
            cluster,
            id,
            "integrity",
            profile.integrity_concurrency.max(1) as usize,
            Some(profile.integrity_concurrency.max(1)),
            aging,
            permit_holder,
        ),
        maintenance: StoreGate::new(
            cluster,
            id,
            "maintenance",
            profile
                .maintenance_concurrency
                .unwrap_or(defaults.2 as i32)
                .max(1) as usize,
            profile.maintenance_concurrency,
            aging,
            permit_holder,
        ),
        migration: StoreGate::new(
            cluster,
            id,
            "migration",
            profile
                .migration_concurrency
                .unwrap_or(defaults.1 as i32)
                .max(1) as usize,
            profile.migration_concurrency,
            aging,
            permit_holder,
        ),
        compression,
        compression_enabled: profile.compression_enabled,
        min_compression_savings_percent: profile.compression_min_savings_percent as u8,
        cache_enabled: profile.cache_enabled,
        cache_capacity: profile
            .cache_soft_limit
            .as_deref()
            .map(config::cache_bytes)
            .transpose()?,
        cache_entries: profile
            .cache_entries
            .map(|value| value.max(1) as usize)
            .unwrap_or(default_cache_entries),
        upload_cache: profile.upload_cache,
        upload_cache_size: profile
            .upload_cache_size
            .as_deref()
            .map(config::cache_bytes)
            .transpose()?,
        pack_config: Arc::new(profile.pack_config()),
    })
}

impl Storage {
    pub async fn new(
        c: &Config,
        secrets: Arc<Secrets>,
        budget: &Budget,
        physical: Arc<dyn crate::model::PhysicalStore>,
        cluster: Option<Arc<dyn crate::model::cluster::ClusterStore>>,
        profile_store: Arc<dyn crate::model::StorageProfileStore>,
        managed_keys: Arc<dyn crate::model::ManagedKeyStore>,
        record_store: Arc<dyn crate::model::StorageRecordStore>,
        maintenance: Arc<std::sync::atomic::AtomicBool>,
        cluster_mode: bool,
        use_target: bool,
    ) -> Result<Self> {
        let cluster = if cluster_mode { cluster } else { None };
        let pack_units = budget.pack_memory_units;
        let mut compression = c.local_compression.clone();
        compression.context_idle_timeout = c.instance.compression_idle_timeout.clone();
        let rows: Vec<crate::model::StorageRecord> = if use_target {
            record_store.enabled().await?
        } else {
            Vec::new()
        };
        for name in ["multipart", "chunks"] {
            tokio::fs::create_dir_all(c.instance.data_path.join(name)).await?;
        }
        let disk = Arc::new(Disk {
            root: c.instance.data_path.clone(),
            limits: [
                c.instance
                    .multipart_limit
                    .as_deref()
                    .map(config::bytes)
                    .transpose()?,
                c.instance
                    .cache_size
                    .as_deref()
                    .map(config::cache_bytes)
                    .transpose()?,
                Some(config::cache_bytes(&c.instance.thumbnail_cache_size)?),
            ],
            floor: config::bytes(&c.instance.min_free_space)?,
            upload_limit: c
                .instance
                .upload_cache_size
                .as_deref()
                .map(config::cache_bytes)
                .transpose()?,
            usage: Mutex::new(Usage {
                used: [0; 3],
                reserved: 0,
                upload: 0,
            }),
            io: Arc::new(Semaphore::new(budget.cpu_jobs)),
            pending: Mutex::new(HashSet::new()),
        });
        let aging = Duration::from_secs(config::seconds(&c.backend_defaults.priority_aging)?);
        let defaults = (
            budget.storage_read_slots,
            budget.storage_upload_slots,
            budget.storage_control_slots,
        );
        let storage_id = rows.first().map(|row| row.id);
        let permit_holder = Uuid::new_v4();
        let mut targets = HashMap::with_capacity(rows.len());
        let mut store_gates = GateRegistry::new();
        for row in rows {
            let id = row.id;
            let profile = profile_store
                .load(id)
                .await
                .with_context(|| format!("storage profile is missing for {id}"))?;
            let mut backend = build_store(
                StoreOptions::from_profile(&profile)?,
                &secrets,
                &cluster,
                row,
                &profile,
                &compression,
                budget.cpu_jobs,
                c.instance.cache_entries.unwrap_or(budget.cache_entries),
                defaults,
                permit_holder,
            )
            .await?;
            backend.reuse_gates(&mut store_gates);
            targets.insert(id, Arc::new(backend));
        }
        let s = Self {
            maintenance,
            access: Arc::new(crate::status::access::Access::new(
                budget.cache_entries.clamp(128, 8192),
            )),
            physical,
            cluster,
            profile_store,
            managed_keys,
            record_store,
            source_gate: tokio::sync::RwLock::new(()),
            physical_pins: Mutex::new(HashMap::new()),
            pack_misses: Mutex::new(HashMap::new()),
            pack_live: Mutex::new(HashMap::new()),
            pack_memory: Arc::new(Semaphore::new(pack_units as usize)),
            pack_memory_units: pack_units,
            pack_work: Arc::new(Semaphore::new(pack_units as usize)),
            inspection: tokio::sync::Mutex::new(()),
            targets: RwLock::new(targets),
            store_defaults: defaults,
            store_gates: Mutex::new(store_gates),
            integrity_rates: Mutex::new(HashMap::new()),
            refresh: tokio::sync::Mutex::new(()),
            permit_holder,
            storage_id,
            disk,
            staging_storage_id: c.uploads.staging_storage_id,
            staging_bytes_limit: c
                .uploads
                .staging_bytes_limit
                .as_deref()
                .map(config::cache_bytes)
                .transpose()?,
            cluster_mode,
            secrets,
            cpu: Arc::new(Semaphore::new(budget.cpu_jobs)),
            compression: Arc::new(crate::content::compression::Pool::new(
                compression.clone(),
                budget.cpu_jobs,
            )?),
            compression_enabled: c.local_compression.enabled,
            base_storage: c.backend_defaults.clone(),
            base_compression: compression,
            cpu_jobs: budget.cpu_jobs,
            reads: crate::storage::resource_gate::Gate::new(budget.storage_read_slots, aging),
            writes: crate::storage::resource_gate::Gate::new(budget.storage_upload_slots, aging),
            controls: crate::storage::resource_gate::Gate::new(budget.storage_control_slots, aging),
            fifo: tokio::sync::Mutex::new(Fifo::default()),
            cache_writes: tokio::sync::Mutex::new(()),
            upload_pins: Mutex::new(HashMap::new()),
            fills: Arc::new(Semaphore::new(budget.cpu_jobs)),
            cache_fills: Arc::new(Mutex::new(HashSet::new())),
            max_entries: c.instance.cache_entries.unwrap_or(budget.cache_entries),
            capacity: c
                .instance
                .cache_size
                .as_deref()
                .map(config::cache_bytes)
                .transpose()?,
            min_compression_savings_percent: c.local_cache.min_compression_savings_percent,
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
            diagnostics: Arc::default(),
        };
        s.restore_upload_pins().await?;
        s.scan().await?;
        Ok(s)
    }
    pub fn statistics(&self) -> serde_json::Value {
        let lookups = self.cache_lookups.load(Ordering::Relaxed);
        let mut diagnostics = self.diagnostics.snapshot();
        let gates = self.store_gates.lock().unwrap();
        for direction in [
            "read",
            "upload",
            "control",
            "pack",
            "integrity",
            "maintenance",
            "migration",
        ] {
            let canceled: u64 = gates
                .iter()
                .filter(|((_, name), _)| *name == direction)
                .filter_map(|(_, gate)| gate.upgrade())
                .map(|gate| {
                    gate.local.snapshot()["waiting"]["canceled"]
                        .as_u64()
                        .unwrap_or(0)
                })
                .sum();
            diagnostics["waiting_canceled"][direction] = serde_json::json!(canceled);
        }
        serde_json::json!({"backend":{"get":self.operations[0].snapshot(),"put":self.operations[1].snapshot(),"delete":self.operations[2].snapshot(),"head":self.operations[3].snapshot()},"diagnostics":diagnostics,
            "cache_lookups":lookups,"cache_hit_rate":crate::status::stats::ratio(self.cache_hits.load(Ordering::Relaxed),lookups).map(|v| v.min(1.0)),
            "cpu_slots_available":self.cpu.available_permits(),"backend_queues":{"read":self.reads.snapshot(),"upload":self.writes.snapshot(),"control":self.controls.snapshot()},
            "cache_limit_bytes":self.capacity,"multipart_limit_bytes":self.disk.limits[0]})
    }
    pub fn enabled(&self) -> bool {
        !self.targets.read().unwrap().is_empty()
    }
    pub fn storage_ids(&self) -> Vec<Uuid> {
        self.targets.read().unwrap().keys().copied().collect()
    }
    fn store_backend(&self, storage_id: Uuid) -> Result<Arc<StoreBackend>> {
        self.targets
            .read()
            .unwrap()
            .get(&storage_id)
            .cloned()
            .with_context(|| format!("store {storage_id} is not enabled"))
    }
    fn local_cache(&self, storage_id: Option<Uuid>) -> CachePolicy {
        storage_id
            .and_then(|id| self.targets.read().unwrap().get(&id).cloned())
            .map(|target| CachePolicy {
                enabled: target.cache_enabled,
                capacity: target.cache_capacity.or(self.capacity),
                max_entries: target.cache_entries,
                upload_cache: target.upload_cache,
                upload_capacity: target.upload_cache_size,
            })
            .unwrap_or(CachePolicy {
                enabled: true,
                capacity: self.capacity,
                max_entries: self.max_entries,
                upload_cache: true,
                upload_capacity: None,
            })
    }
    fn compression_for(
        &self,
        storage_id: Option<Uuid>,
    ) -> (Arc<crate::content::compression::Pool>, bool, u8) {
        storage_id
            .and_then(|id| self.targets.read().unwrap().get(&id).cloned())
            .map(|target| {
                (
                    target.compression.clone(),
                    target.compression_enabled,
                    target.min_compression_savings_percent,
                )
            })
            .unwrap_or((
                self.compression.clone(),
                self.compression_enabled,
                self.min_compression_savings_percent,
            ))
    }
    pub(crate) fn pack_config(&self, storage: Uuid) -> Result<Arc<crate::content::pack::Config>> {
        Ok(self.store_backend(storage)?.pack_config.clone())
    }
    pub(crate) fn acquire_integrity_rate(
        &self,
        storage: Uuid,
        request_rate: Option<f64>,
        byte_rate: Option<f64>,
        requests: f64,
        bytes: f64,
    ) -> Result<bool> {
        let targets = self.targets.read().unwrap();
        ensure!(
            targets.contains_key(&storage),
            "integrity storage is not enabled"
        );
        Ok(self
            .integrity_rates
            .lock()
            .unwrap()
            .entry(storage)
            .or_default()
            .acquire(
                request_rate,
                byte_rate,
                requests,
                bytes,
                std::time::Instant::now(),
            ))
    }
    pub(crate) async fn pack_permit(
        &self,
        storage_id: Uuid,
    ) -> Result<crate::storage::permits::StorePermit> {
        self.store_backend(storage_id)?.pack.acquire().await
    }
    pub(crate) async fn profile_permit(
        &self,
        storage_id: Uuid,
        direction: &str,
    ) -> Result<crate::storage::permits::StorePermit> {
        let target = self.store_backend(storage_id)?;
        match direction {
            "integrity" => target.integrity.acquire().await,
            "maintenance" => target.maintenance.acquire().await,
            "migration" => target.migration.acquire().await,
            _ => anyhow::bail!("unknown storage permit direction: {direction}"),
        }
    }
    pub async fn refresh_store(&self, storage_id: Uuid) -> Result<()> {
        let _refresh = self.refresh.lock().await;
        let row = self.record_store.enabled_by_id(storage_id).await?;
        let backend = if let Some(row) = row {
            let profile = self
                .profile_store
                .load(storage_id)
                .await
                .with_context(|| format!("storage profile is missing for {storage_id}"))?;
            Some(
                build_store(
                    StoreOptions::from_profile(&profile)?,
                    &self.secrets,
                    &self.cluster,
                    row,
                    &profile,
                    &self.base_compression,
                    self.cpu_jobs,
                    self.max_entries,
                    self.store_defaults,
                    self.permit_holder,
                )
                .await?,
            )
        } else {
            None
        };
        let mut targets = self.targets.write().unwrap();
        if let Some(mut backend) = backend {
            backend.reuse_gates(&mut self.store_gates.lock().unwrap());
            targets.insert(storage_id, Arc::new(backend));
        } else {
            targets.remove(&storage_id);
            self.integrity_rates.lock().unwrap().remove(&storage_id);
        }
        Ok(())
    }
    pub async fn refresh_storages(&self) -> Result<()> {
        let rows = self.record_store.enabled().await?;
        let revisions: HashMap<Uuid, i64> = rows.iter().map(|row| (row.id, row.revision)).collect();
        let known: HashMap<Uuid, i64> = self
            .targets
            .read()
            .unwrap()
            .iter()
            .map(|(id, target)| (*id, target.revision))
            .collect();
        for (id, revision) in &revisions {
            if known.get(id) != Some(revision) {
                self.refresh_store(*id).await?;
            }
        }
        for id in known.keys().filter(|id| !revisions.contains_key(id)) {
            self.refresh_store(*id).await?;
        }
        Ok(())
    }
    fn default_store(&self) -> Result<Uuid> {
        self.storage_id
            .filter(|id| self.targets.read().unwrap().contains_key(id))
            .or_else(|| self.targets.read().unwrap().keys().next().copied())
            .context("no store is configured")
    }
    pub(crate) fn select_store(&self, storage_id: Option<Uuid>) -> Result<Uuid> {
        match storage_id {
            Some(id) => {
                let _ = self.store_backend(id)?;
                Ok(id)
            }
            None => self.default_store(),
        }
    }
    pub fn namespace_for(&self, storage_id: Uuid) -> Result<String> {
        let prefix = self.store_backend(storage_id)?.prefix.clone();
        Ok(if prefix.is_empty() {
            String::new()
        } else {
            format!("{prefix}/")
        })
    }
    pub fn path_for(&self, storage_id: Uuid, id: Uuid) -> Result<Path> {
        let _ = self.store_backend(storage_id)?;
        Ok(Path::from(chunk_name(id)))
    }
    pub fn pack_path_for(&self, storage_id: Uuid, id: Uuid) -> Result<Path> {
        let _ = self.store_backend(storage_id)?;
        Ok(Path::from(format!("packs/{}", chunk_name(id))))
    }
    fn parse_path(path: &str) -> Option<Uuid> {
        parse_chunk_name(path).or_else(|| parse_chunk_name(path.strip_prefix("chunks/")?))
    }
    fn parse_physical_path(path: &str) -> Option<Uuid> {
        Self::parse_path(path).or_else(|| parse_chunk_name(path.strip_prefix("packs/")?))
    }
    pub fn parse_physical_path_for(&self, storage_id: Uuid, path: &str) -> Result<Option<Uuid>> {
        let _ = self.store_backend(storage_id)?;
        Ok(Self::parse_physical_path(path))
    }
    pub async fn list_physical_for(
        &self,
        storage_id: Uuid,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<object_store::ObjectMeta>> {
        let target = self.store_backend(storage_id)?;
        let _permit = crate::storage::resource_gate::PRIORITY
            .scope(
                crate::storage::resource_gate::MAINTENANCE,
                target.controls.acquire(),
            )
            .await?;
        let offset = Path::parse(cursor.unwrap_or(""))?;
        let mut rows = target.backend.list_with_offset(None, &offset);
        if !target.is_local {
            return Ok(rows.take(limit).try_collect().await?);
        }
        // Local listings are unordered; keep the first bounded page after the cursor.
        let mut page = std::collections::BTreeMap::new();
        while let Some(row) = rows.try_next().await? {
            page.insert(row.location.clone(), row);
            if page.len() > limit {
                page.pop_last();
            }
        }
        Ok(page.into_values().collect())
    }
    async fn ensure_storage_verified(&self, storage_id: Uuid) -> Result<()> {
        let target = self.store_backend(storage_id)?;
        if target.identity_ready.load(Ordering::Acquire) {
            return Ok(());
        }
        let _check = target.identity_check.lock().await;
        if !target.identity_ready.load(Ordering::Acquire) {
            self.check_storage_inner(&target).await?;
        }
        Ok(())
    }
    pub async fn check_identity(&self) -> Result<()> {
        let ids = self.storage_ids();
        ensure!(!ids.is_empty(), "no store is configured");
        for id in ids {
            self.check_storage(id).await?;
        }
        Ok(())
    }
    pub async fn check_storage(&self, storage_id: Uuid) -> Result<()> {
        let target = self.store_backend(storage_id)?;
        let _check = target.identity_check.lock().await;
        target.identity_ready.store(false, Ordering::Release);
        self.check_storage_inner(&target).await?;
        Ok(())
    }
    pub(crate) async fn initialize_store(&self, record: StoreRecord) -> Result<()> {
        self.initialize_store_with_profile(record, None).await
    }

    pub(crate) async fn initialize_store_with_profile(
        &self,
        record: StoreRecord,
        profile: Option<&StorageProfile>,
    ) -> Result<()> {
        if record.r#type == "local" {
            tokio::fs::create_dir_all(
                record
                    .path
                    .as_deref()
                    .context("local store path is missing")?,
            )
            .await?;
        }
        let profile_exists: bool = self.profile_store.exists(record.id).await?;
        let options = if let Some(profile) = profile {
            StoreOptions::from_profile(profile)?
        } else if profile_exists {
            StoreOptions::from_profile(&self.profile_store.load(record.id).await?)?
        } else {
            StoreOptions::new(&self.base_storage)?
        };
        let target = VerifiedStore::build(&record, options, &self.secrets)?;
        let _permit = crate::storage::resource_gate::PRIORITY
            .scope(
                crate::storage::resource_gate::MAINTENANCE,
                self.controls.acquire(),
            )
            .await?;
        target.initialize().await
    }
    pub(crate) async fn verify_store_record(&self, record: StoreRecord) -> Result<()> {
        let profile = self
            .profile_store
            .load(record.id)
            .await
            .with_context(|| format!("storage profile is missing for {}", record.id))?;
        let target = VerifiedStore::build(
            &record,
            StoreOptions::from_profile(&profile)?,
            &self.secrets,
        )?;
        let _permit = crate::storage::resource_gate::PRIORITY
            .scope(
                crate::storage::resource_gate::MAINTENANCE,
                self.controls.acquire(),
            )
            .await?;
        target.verify().await
    }
    async fn check_storage_inner(&self, target: &StoreBackend) -> Result<()> {
        let _permit = target.reads.acquire().await?;
        let result = target
            .backend
            .get(&Path::from("meta.json"))
            .await
            .context("storage meta.json is required; restore it before using this store")?;
        ensure!(
            result.meta.size <= 16 * 1024,
            "storage meta.json exceeds limit"
        );
        let mut data = Vec::new();
        let mut stream = result.into_stream();
        while let Some(bytes) = stream.try_next().await? {
            ensure!(
                data.len() + bytes.len() <= 16 * 1024,
                "storage meta.json exceeds limit"
            );
            data.extend_from_slice(&bytes);
        }
        let actual: serde_json::Value =
            serde_json::from_slice(&data).context("invalid storage meta.json")?;
        ensure!(
            actual == target.identity_marker,
            "storage identity marker does not match this store"
        );
        target.identity_ready.store(true, Ordering::Release);
        Ok(())
    }
    fn cache_path(&self, id: Uuid, compressed: bool) -> PathBuf {
        self.disk
            .root
            .join("chunks")
            .join(chunk_name(id))
            .with_extension(if compressed { "zst" } else { "raw" })
    }
    async fn restore_upload_pins(&self) -> Result<()> {
        let mut after = 0i64;
        loop {
            let rows: Vec<(i64, Option<Uuid>, Option<String>, i64)> =
                self.physical.upload_pins(after).await?;
            if rows.is_empty() {
                break;
            }
            for (id, staging_storage_id, object_key, expected) in rows {
                after = id;
                let Some(object_key) = object_key else {
                    continue;
                };
                if object_key.starts_with(STAGING_PREFIX) {
                    continue;
                }
                let compressed = object_key.ends_with(".zst");
                let key = parse_upload_cache_name(&object_key);
                let Some(key) = key else {
                    continue;
                };
                let size = match tokio::fs::metadata(self.cache_path(key, compressed)).await {
                    Ok(m) => m.len(),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
                    Err(e) => return Err(e.into()),
                };
                let mut fifo = self.fifo.lock().await;
                fifo.insert(key, size, compressed);
                let pin = fifo.entries[&key].pins.clone();
                pin.fetch_add(1, Ordering::Relaxed);
                self.disk.account_existing(Area::Cache, size);
                self.disk.usage.lock().unwrap().upload += expected as u64;
                self.upload_pins.lock().unwrap().insert(
                    key,
                    UploadPin {
                        _pin: Some(CachePin(pin)),
                        _charge: Some(UploadCharge {
                            disk: self.disk.clone(),
                            size: expected as u64,
                        }),
                        size: expected as u64,
                        compressed,
                        remote: false,
                        staging_storage_id,
                        staging_object_key: format!(
                            "{key}.{}",
                            if compressed { "zst" } else { "raw" }
                        ),
                    },
                );
            }
        }
        Ok(())
    }
    pub fn hold_upload(&self, id: Uuid, pin: UploadPin) {
        self.upload_pins.lock().unwrap().insert(id, pin);
    }
    pub fn release_upload(&self, id: Uuid) {
        self.upload_pins.lock().unwrap().remove(&id);
    }
    pub fn upload_pin_ids(&self) -> Vec<Uuid> {
        self.upload_pins.lock().unwrap().keys().copied().collect()
    }
    fn shared_staging_id(&self) -> Result<Uuid> {
        ensure!(
            self.cluster_mode,
            "shared staging is only available in cluster mode"
        );
        let id = self
            .staging_storage_id
            .context("cluster mode requires uploads.staging_storage_id")?;
        let target = self.store_backend(id)?;
        ensure!(
            !target.is_local,
            "cluster staging storage must be S3-compatible"
        );
        Ok(id)
    }
    async fn claim_staging(&self, storage_id: Uuid, key: &str, bytes: u64) -> Result<()> {
        self.cluster
            .as_ref()
            .context("shared staging requires cluster mode")?
            .claim_staging(
                storage_id,
                key,
                bytes,
                self.staging_bytes_limit.unwrap_or(DEFAULT_STAGING_LIMIT),
            )
            .await
    }
    async fn release_staging(&self, storage_id: Uuid, key: &str) -> Result<()> {
        self.cluster
            .as_ref()
            .context("shared staging requires cluster mode")?
            .release_staging(storage_id, key)
            .await?;
        Ok(())
    }
    async fn put_staged(&self, storage_id: Uuid, key: &str, data: Bytes) -> Result<()> {
        self.ensure_storage_verified(storage_id).await?;
        self.claim_staging(storage_id, key, data.len() as u64)
            .await?;
        let target = self.store_backend(storage_id)?;
        let result = target
            .backend
            .put(&Path::from(key), data.into())
            .await
            .inspect_err(|e| self.diagnostics.backend(e));
        if let Err(error) = result {
            let _ = self.release_staging(storage_id, key).await;
            return Err(error.into());
        }
        Ok(())
    }
    async fn read_staged(&self, storage_id: Uuid, key: &str, bound: usize) -> Result<Vec<u8>> {
        ensure!(
            key.starts_with(STAGING_PREFIX),
            "invalid staging object key"
        );
        self.read_path_for(storage_id, &Path::from(key), bound)
            .await
    }
    pub(crate) async fn delete_staged(&self, storage_id: Uuid, key: &str) -> Result<()> {
        if !key.starts_with(STAGING_PREFIX) {
            return Ok(());
        }
        let target = self.store_backend(storage_id)?;
        self.ensure_storage_verified(storage_id).await?;
        let _permit = target.controls.acquire().await?;
        match target
            .backend
            .delete(&Path::from(key))
            .await
            .inspect_err(|e| self.diagnostics.backend(e))
        {
            Ok(()) => {}
            Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(error.into()),
        }
        self.release_staging(storage_id, key).await
    }
    pub(crate) async fn shared_staging_ready(&self) -> Result<()> {
        let _ = self.shared_staging_id()?;
        ensure!(
            self.staging_bytes_limit.is_none_or(|n| n > 0),
            "uploads.staging_bytes_limit must be positive"
        );
        Ok(())
    }
    pub(crate) async fn write_fragment(
        &self,
        id: Uuid,
        data: &[u8],
    ) -> Result<(Option<Uuid>, Option<String>, Option<Fragment>)> {
        if !self.cluster_mode {
            return Ok((None, None, Some(self.disk.write_fragment(id, data).await?)));
        }
        let storage_id = self.shared_staging_id()?;
        let key = format!("{STAGING_PREFIX}fragment/{}", id.simple());
        self.put_staged(storage_id, &key, Bytes::copy_from_slice(data))
            .await?;
        Ok((Some(storage_id), Some(key), None))
    }
    pub(crate) async fn read_fragment(
        &self,
        id: Uuid,
        storage_id: Option<Uuid>,
        key: Option<&str>,
    ) -> Result<Vec<u8>> {
        if let (Some(storage_id), Some(key)) = (storage_id, key) {
            return self.read_staged(storage_id, key, MAX).await;
        }
        read_bounded(self.disk.fragment_path(id), MAX).await
    }
    pub(crate) async fn remove_fragment(
        &self,
        id: Uuid,
        storage_id: Option<Uuid>,
        key: Option<&str>,
    ) -> Result<()> {
        if let (Some(storage_id), Some(key)) = (storage_id, key) {
            return self.delete_staged(storage_id, key).await;
        }
        self.disk.remove_fragment(id).await
    }
    pub(crate) async fn reconcile_staging(&self, storage_id: Option<Uuid>) -> Result<()> {
        let Some(cluster) = &self.cluster else {
            return Ok(());
        };
        let rows: Vec<(Uuid, String)> = cluster.stale_staging(storage_id).await?;
        for (storage_id, key) in rows {
            if let Err(error) = self.delete_staged(storage_id, &key).await {
                tracing::warn!(%storage_id, %key, error=%error, "orphan staging object cleanup failed");
            }
        }
        Ok(())
    }
    async fn decode_staged_cache(&self, c: &Chunk, data: Bytes, compressed: bool) -> Result<Bytes> {
        if !compressed {
            return Ok(data);
        }
        let permit = self.cpu.clone().acquire_owned().await?;
        let c = c.clone();
        let (compression, _, _) = self.compression_for(c.storage_id);
        Ok(tokio::task::spawn_blocking(move || {
            let _permit = permit;
            crate::content::codec::decode_cache(&c, data, true, &compression)
        })
        .await??)
    }
    pub async fn stage_upload(
        self: &Arc<Self>,
        c: &Chunk,
        data: Bytes,
        compressed: bool,
    ) -> Result<Option<UploadPin>> {
        let policy = self.local_cache(c.storage_id);
        let id = cache::cache_id(c);
        if !self.cluster_mode && (!policy.enabled || !policy.upload_cache) {
            return Ok(None);
        }
        if self.cluster_mode {
            let storage_id = self.shared_staging_id()?;
            let raw = self.decode_staged_cache(c, data, compressed).await?;
            let key = format!("{STAGING_PREFIX}pending/{}", id.simple());
            self.put_staged(storage_id, &key, raw.clone()).await?;
            return Ok(Some(UploadPin {
                _pin: None,
                _charge: None,
                size: raw.len() as u64,
                compressed: false,
                remote: true,
                staging_storage_id: Some(storage_id),
                staging_object_key: key,
            }));
        }
        let permit = self.fills.clone().acquire_owned().await?;
        let storage = self.clone();
        tokio::spawn(async move {
            let _permit = permit;
            storage
                .stage_upload_owned(id, data, compressed, policy)
                .await
        })
        .await?
    }
    async fn stage_upload_owned(
        &self,
        id: Uuid,
        data: Bytes,
        compressed: bool,
        policy: CachePolicy,
    ) -> Result<Option<UploadPin>> {
        let size = data.len() as u64;
        let _write = self.cache_writes.lock().await;
        let mut fifo = self.fifo.lock().await;
        if fifo
            .entries
            .get(&id)
            .is_some_and(|e| e.pins.load(Ordering::Relaxed) > 0)
        {
            return Ok(None);
        }
        let Ok(charge) = self
            .disk
            .claim_upload_with_limit(size, policy.upload_capacity)
        else {
            return Ok(None);
        };
        if let Some(e) = fifo.entries.get(&id) {
            let removal = CacheRemoval::new(&e.pins);
            let path = self.cache_path(id, e.compressed);
            drop(fifo);
            let result = tokio::fs::remove_file(path).await;
            fifo = self.fifo.lock().await;
            match result {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
            let e = fifo.forget(id).unwrap();
            self.disk.release(Area::Cache, e.size);
            drop(removal);
        }
        let ticket = loop {
            if fifo.entries.len() < policy.max_entries
                && let Ok(ticket) = self.disk.reserve(Area::Cache, size)
            {
                break ticket;
            }
            let capacity = policy.capacity.unwrap_or(self.disk.used()[1].max(size));
            let Some(victim) = fifo.victim(capacity) else {
                return Ok(None);
            };
            let entry = &fifo.entries[&victim];
            let removal = CacheRemoval::new(&entry.pins);
            let path = self.cache_path(victim, entry.compressed);
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
        };
        drop(fifo);
        let path = self.cache_path(id, compressed);
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
            f.write_all(&data).await?;
            #[cfg(feature = "fault-injection")]
            crate::faults::point("upload-cache-written").await;
            f.sync_all().await?;
            drop(f);
            tokio::fs::rename(&tmp, &path).await?;
            tokio::fs::File::open(path.parent().unwrap())
                .await?
                .sync_all()
                .await?;
            tokio::fs::File::open(self.disk.root.join("chunks"))
                .await?
                .sync_all()
                .await?;
            Ok::<_, std::io::Error>(())
        }
        .await;
        if let Err(e) = result {
            let mut retained = false;
            for file in [&tmp, &path] {
                retained |= tokio::fs::remove_file(file).await.is_err()
                    && tokio::fs::try_exists(file).await.unwrap_or(true);
            }
            if retained {
                ticket.commit();
            }
            return Err(e.into());
        }
        ticket.commit();
        let mut fifo = self.fifo.lock().await;
        fifo.insert(id, size, compressed);
        let pin = fifo.entries[&id].pins.clone();
        pin.fetch_add(1, Ordering::Relaxed);
        drop(fifo);
        let pin = UploadPin {
            _pin: Some(CachePin(pin)),
            _charge: Some(charge),
            size,
            compressed,
            remote: false,
            staging_storage_id: None,
            staging_object_key: format!("{}.{}", id, if compressed { "zst" } else { "raw" }),
        };
        #[cfg(feature = "fault-injection")]
        crate::faults::point("upload-cache-durable").await;
        Ok(Some(pin))
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
                if cached.is_some_and(|(id, compressed)| {
                    self.upload_pins
                        .lock()
                        .unwrap()
                        .get(&id)
                        .is_some_and(|p| p.compressed == compressed)
                }) {
                    // Restored durable entries are protected and accounted before any eviction.
                    continue;
                }
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
        memory: Option<&Arc<tokio::sync::OwnedSemaphorePermit>>,
    ) -> Result<(Chunk, Vec<u8>, Bytes)> {
        let memory = memory.cloned();
        let permit = self.cpu.clone().acquire_owned().await?;
        let secrets = self.secrets.clone();
        let (compression, compression_enabled, threshold) = self.compression_for(c.storage_id);
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let _memory = memory;
            #[cfg(feature = "fault-injection")]
            if _memory.is_some() {
                crate::faults::blocking("pack-chunk-encode");
            }
            codec::encode(
                c,
                raw,
                &secrets,
                threshold,
                &compression,
                should_compress && compression_enabled,
            )
        })
        .await?
    }
    pub(crate) async fn decode(&self, c: &Chunk, encoded: Vec<u8>) -> Result<(Bytes, Bytes)> {
        let permit = self.cpu.clone().acquire_owned().await?;
        let c = c.clone();
        let secrets = self.secrets.clone();
        let (compression, _, threshold) = self.compression_for(c.storage_id);
        let diagnostics = self.diagnostics.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            codec::decode(&c, encoded, &secrets, threshold, &compression)
                .inspect_err(|e| diagnostics.content(e))
        })
        .await?
    }
    pub async fn put(self: &Arc<Self>, c: &Chunk, encoded: Vec<u8>, cache: Bytes) -> Result<()> {
        let storage_id = c.storage_id.context("chunk has no store")?;
        self.ensure_storage_verified(storage_id).await?;
        let target = self.store_backend(storage_id)?;
        ensure!(
            Some(encoded.len() as i32) == c.stored_size,
            "encoded length mismatch"
        );
        let permit = target.writes.acquire().await?;
        let mut operation = self.operations[1].begin();
        self.backend_puts.fetch_add(1, Ordering::Relaxed);
        let size = encoded.len();
        #[cfg(feature = "fault-injection")]
        crate::faults::point("storage-put-permitted").await;
        let result = target
            .backend
            .put(
                &self.path_for(storage_id, c.location_id)?,
                Bytes::from(encoded).into(),
            )
            .await
            .inspect_err(|e| self.diagnostics.backend(e));
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
    async fn cached_file(&self, c: &Chunk, id: Uuid) -> Result<Option<Bytes>> {
        if !self.local_cache(c.storage_id).enabled {
            return Ok(None);
        }
        let cached = {
            let mut fifo = self.fifo.lock().await;
            fifo.entries.get_mut(&id).and_then(|e| {
                if !e.valid || e.pins.load(Ordering::Relaxed) == usize::MAX {
                    return None;
                }
                e.pins.fetch_add(1, Ordering::Relaxed);
                e.frequency = e.frequency.saturating_add(1).min(3);
                Some((CachePin(e.pins.clone()), e.compressed))
            })
        };
        if let Some((pin, compressed)) = cached {
            let file = read_bounded(self.cache_path(id, compressed), MAX).await;
            let missing = file.as_ref().err().is_some_and(|e| {
                e.downcast_ref::<std::io::Error>()
                    .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound)
            });
            if let Ok(data) = file {
                let size = data.len() as u64;
                let permit = self.cpu.clone().acquire_owned().await?;
                let chunk = c.clone();
                let (compression, _, _) = self.compression_for(c.storage_id);
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
            if missing {
                self.diagnostics
                    .cache_missing
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                self.diagnostics
                    .cache_invalid
                    .fetch_add(1, Ordering::Relaxed);
            }
            self.diagnostics
                .cache_fallback
                .fetch_add(1, Ordering::Relaxed);
            let mut fifo = self.fifo.lock().await;
            if let Some(entry) = fifo.entries.get_mut(&id)
                && Arc::ptr_eq(&entry.pins, &pin.0)
            {
                entry.valid = false;
                // A missing unshared file needs no cleanup job or publication lock.
                if missing && entry.pins.load(Ordering::Relaxed) == 1 {
                    let entry = fifo.forget(id).unwrap();
                    self.disk.release(Area::Cache, entry.size);
                }
            }
        }
        Ok(None)
    }
    async fn cached(&self, c: &Chunk) -> Result<Option<Bytes>> {
        let mut cache_id = cache::cache_id(c);
        if let Some(raw) = self.cached_file(c, cache_id).await? {
            return Ok(Some(raw));
        }
        let staged: Option<(Uuid, String)> = self.physical.pending_source(c.id).await?;
        if c.location_id.is_nil()
            && let Some(id) = staged
                .as_ref()
                .and_then(|(_, key)| parse_upload_cache_name(key))
            && id != cache_id
        {
            cache_id = id;
            if let Some(raw) = self.cached_file(c, id).await? {
                return Ok(Some(raw));
            }
        }
        if let Some((storage_id, key)) = staged.filter(|(_, key)| key.starts_with(STAGING_PREFIX)) {
            #[cfg(feature = "fault-injection")]
            crate::faults::point("read-pending-source").await;
            let raw = match self
                .read_staged(storage_id, &key, c.raw_size as usize)
                .await
            {
                Ok(raw) => raw,
                // Publication may remove staging after lookup; fetch reselects a protected source.
                Err(error)
                    if matches!(
                        error.downcast_ref::<object_store::Error>(),
                        Some(object_store::Error::NotFound { .. })
                    ) =>
                {
                    return Ok(None);
                }
                Err(error) => return Err(error),
            };
            let validation: Result<()> = (|| {
                ensure!(
                    raw.len() == c.raw_size as usize,
                    codec::IntegrityError::Length
                );
                ensure!(
                    blake3::hash(&raw).as_bytes() == c.hash.as_slice(),
                    codec::IntegrityError::Hash
                );
                Ok(())
            })();
            validation.inspect_err(|e| self.diagnostics.content(e))?;
            return Ok(Some(Bytes::from(raw)));
        }
        let pending = self
            .upload_pins
            .lock()
            .unwrap()
            .keys()
            .any(|id| *id == cache_id);
        if pending && self.physical.pending_only(c.id).await? {
            self.diagnostics
                .pending_unavailable
                .fetch_add(1, Ordering::Relaxed);
            return Err(anyhow::anyhow!("pending upload cache is missing or corrupt for chunk {}; preserve data/chunks and restore its durable source", c.id).context(codec::IntegrityError::PendingUnavailable));
        }
        Ok(None)
    }
    pub(crate) async fn invalidate(&self, id: Uuid) -> Result<()> {
        self.invalidate_entry(id, None).await
    }
    async fn invalidate_entry(&self, id: Uuid, expected: Option<&Arc<AtomicUsize>>) -> Result<()> {
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
        let path = self.cache_path(id, entry.compressed);
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
    pub fn cache_jobs_active(&self) -> usize {
        self.cpu_jobs - self.fills.available_permits()
    }
    fn cache_work(&self, id: Uuid, units: u32) -> Option<CacheWork> {
        let slot = self.fills.clone().try_acquire_owned().ok()?;
        let memory = self.pack_work.clone().try_acquire_many_owned(units).ok()?;
        if !self.cache_fills.lock().unwrap().insert(id) {
            return None;
        }
        Some(CacheWork {
            id,
            pending: self.cache_fills.clone(),
            _slot: slot,
            _memory: memory,
        })
    }
    fn fill_read(self: &Arc<Self>, c: &Chunk, data: Bytes, compressed: Option<bool>) {
        let policy = self.local_cache(c.storage_id);
        if !policy.enabled {
            return;
        }
        let (pool, _, _) = self.compression_for(c.storage_id);
        let Ok(workspace) = pool.workspace_bytes() else {
            return;
        };
        // Detached fills retain their buffers after the request returns its data slot.
        let units =
            (data.len().max(c.raw_size as usize) as u64 * 4 + workspace).div_ceil(1024 * 1024);
        let Ok(units) = u32::try_from(units) else {
            return;
        };
        let Some(work) = self.cache_work(cache::cache_id(c), units) else {
            return;
        };
        let storage = self.clone();
        let chunk = c.clone();
        tokio::spawn(
            resource_gate::PRIORITY.scope(resource_gate::MAINTENANCE, async move {
                let _work = work;
                let result = if let Some(compressed) = compressed {
                    storage
                        .fill_owned(cache::cache_id(&chunk), &data, compressed, policy)
                        .await
                } else {
                    storage.fill_raw(&chunk, data).await
                };
                if let Err(e) = result {
                    tracing::warn!(error=%e,"cache fill failed; serving verified backend data");
                }
            }),
        );
    }
    async fn fill(self: &Arc<Self>, c: &Chunk, data: Bytes) -> Result<()> {
        let policy = self.local_cache(c.storage_id);
        if !policy.enabled {
            return Ok(());
        }
        let Ok(permit) = self.fills.clone().try_acquire_owned() else {
            return Ok(());
        };
        let storage = self.clone();
        let id = cache::cache_id(c);
        let (_, _, threshold) = self.compression_for(c.storage_id);
        let compressed = codec::cache_compressed(c, threshold);
        tokio::spawn(async move {
            let _permit = permit;
            storage.fill_owned(id, &data, compressed, policy).await
        })
        .await?
    }
    async fn fill_owned(
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
            self.invalidate_entry(id, Some(&expected)).await?;
        }
        // Serialize publication/invalidation while cache hits only need the FIFO index lock.
        let _write = self.cache_writes.lock().await;
        let mut fifo = self.fifo.lock().await;
        let ticket = {
            if fifo.entries.contains_key(&id) {
                return Ok(());
            }
            loop {
                if fifo.entries.len() < policy.max_entries
                    && let Ok(ticket) = self.disk.reserve(Area::Cache, size)
                {
                    break ticket;
                }
                let capacity = policy.capacity.unwrap_or(self.disk.used()[1].max(size));
                let Some(victim) = fifo.victim(capacity) else {
                    return Ok(());
                };
                let entry = &fifo.entries[&victim];
                let removal = CacheRemoval::new(&entry.pins);
                let path = self.cache_path(victim, entry.compressed);
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
        if let Some(context) = context {
            let current = context.pack.lock().await;
            if let Some(loaded) = current
                .as_ref()
                .filter(|p| p.data.members.iter().any(|m| m.id == c.id))
            {
                loaded.touch(c, context.observe, context.range);
                return loaded.data.chunk(c);
            }
        }
        if let Some(raw) = self.cached(c).await? {
            if let Some(context) = context {
                context.touch(c).await;
            }
            return Ok(raw);
        }
        let lock = {
            let mut map = self.misses.lock().unwrap();
            map.retain(|_, v| v.strong_count() > 0);
            let key = (c.id, c.location_id);
            if let Some(lock) = map.get(&key).and_then(|v| v.upgrade()) {
                lock
            } else {
                let lock = Arc::new(tokio::sync::OnceCell::new());
                map.insert(key, Arc::downgrade(&lock));
                lock
            }
        };
        match lock
            .get_or_init(|| async { self.fetch(c, context).await.map_err(Arc::new) })
            .await
        {
            Ok((raw, loaded)) => {
                if let (Some(context), Some(loaded)) = (context, loaded) {
                    loaded.touch(c, context.observe, context.range);
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
        match self.source(c.id, false).await? {
            Source::Chunk(physical, _pin) => {
                self.ensure_key_loaded(physical.block_key_id).await?;
                let encoded = self.read_backend(&physical).await?;
                let (raw, cache) = self.decode(&physical, encoded).await?;
                if physical.compressed == c.compressed && physical.stored_size == c.stored_size {
                    let (_, _, threshold) = self.compression_for(c.storage_id);
                    self.fill_read(c, cache, Some(codec::cache_compressed(c, threshold)));
                } else {
                    self.fill_read(c, raw.clone(), None);
                }
                Ok((raw, None))
            }
            Source::Pack(pack, _pin) => {
                if let Some(context) = context {
                    let mut current = context.pack.lock().await;
                    if let Some(loaded) = current.as_ref().filter(|p| p.pack.id == pack.id) {
                        loaded.touch(c, context.observe, context.range);
                        return Ok((loaded.data.chunk(c)?, Some(loaded.clone())));
                    }
                    *current = None;
                    let loaded = self.load_pack(pack).await?;
                    loaded.touch(c, context.observe, context.range);
                    let raw = loaded.data.chunk(c)?;
                    *current = Some(loaded.clone());
                    Ok((raw, Some(loaded)))
                } else {
                    let loaded = self.load_pack(pack).await?;
                    loaded.touch(c, false, false);
                    Ok((loaded.data.chunk(c)?, Some(loaded)))
                }
            }
        }
    }
    pub async fn register_location(&self, c: &Chunk) -> Result<()> {
        self.physical.register_encoding(c).await?;
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
    async fn source(&self, id: i64, inspection: bool) -> Result<Source> {
        let _gate = self.source_gate.read().await;
        match self.physical.source(id, inspection).await? {
            crate::model::PhysicalSource::Chunk(chunk) => {
                let pin = self.physical_pin(chunk.location_id);
                Ok(Source::Chunk(chunk, pin))
            }
            crate::model::PhysicalSource::Pack(pack) => {
                let pin = self.physical_pin(pack.location_id);
                Ok(Source::Pack(pack, pin))
            }
        }
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
    pub async fn pack_work(&self, size: usize) -> Result<Arc<tokio::sync::OwnedSemaphorePermit>> {
        let units = (size as u64 * 4 + self.compression.workspace_bytes()?).div_ceil(1024 * 1024);
        ensure!(
            units <= u64::from(self.pack_memory_units),
            "pack rewrite exceeds available memory budget"
        );
        Ok(Arc::new(
            self.pack_work
                .clone()
                .acquire_many_owned(units as u32)
                .await?,
        ))
    }
    pub(crate) fn http_work(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        self.pack_work.clone().try_acquire_many_owned(32).ok()
    }
    pub async fn encode_pack(
        &self,
        p: crate::content::pack::Pack,
        members: Vec<(Chunk, Bytes)>,
        strategy: crate::content::compression::Strategy,
        should_try: bool,
        memory: &Arc<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<(crate::content::pack::Pack, Vec<u8>)> {
        let memory = memory.clone();
        let permit = self.cpu.clone().acquire_owned().await?;
        let secrets = self.secrets.clone();
        let (pool, compression_enabled, _) = self.compression_for(p.storage_id);
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let _memory = memory;
            #[cfg(feature = "fault-injection")]
            crate::faults::blocking("pack-encode");
            crate::content::pack::encode(
                p,
                &members,
                &secrets,
                &pool,
                strategy,
                should_try && compression_enabled,
            )
        })
        .await?
    }
    pub async fn put_path_for(&self, storage_id: Uuid, path: &Path, data: Vec<u8>) -> Result<()> {
        self.ensure_storage_verified(storage_id).await?;
        let target = self.store_backend(storage_id)?;
        let _permit = target.writes.acquire().await?;
        self.background_writable()?;
        let mut operation = self.operations[1].begin();
        self.backend_puts.fetch_add(1, Ordering::Relaxed);
        let size = data.len() as u64;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("storage-put-permitted").await;
        let result = target
            .backend
            .put(path, Bytes::from(data).into())
            .await
            .inspect_err(|e| self.diagnostics.backend(e));
        operation.finish(result.is_err());
        result?;
        self.operations[1].bytes(size);
        self.backend_write_bytes.fetch_add(size, Ordering::Relaxed);
        Ok(())
    }
    pub async fn delete_path_for(&self, storage_id: Uuid, path: &Path) -> Result<()> {
        self.ensure_storage_verified(storage_id).await?;
        let target = self.store_backend(storage_id)?;
        let _permit = target.controls.acquire().await?;
        self.background_writable()?;
        let mut operation = self.operations[2].begin();
        self.backend_deletes.fetch_add(1, Ordering::Relaxed);
        let result = match target.backend.delete(path).await {
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            result => result,
        }
        .inspect_err(|e| self.diagnostics.backend(e));
        operation.finish(result.is_err());
        result?;
        Ok(())
    }
    fn background_writable(&self) -> Result<()> {
        ensure!(
            !self.maintenance.load(Ordering::Acquire)
                || crate::upload::cache::FLUSH
                    .try_with(|f| *f)
                    .unwrap_or(false)
                || crate::storage::resource_gate::PRIORITY
                    .try_with(|p| *p)
                    .unwrap_or(crate::storage::resource_gate::FOREGROUND)
                    == crate::storage::resource_gate::FOREGROUND,
            "background writes are paused for maintenance"
        );
        Ok(())
    }
    pub(crate) async fn ensure_key_loaded(&self, id: Option<Uuid>) -> Result<()> {
        let Some(name) = self.secrets.key_name(id) else {
            return Ok(());
        };
        if self.secrets.key(&name).is_some() {
            return Ok(());
        }
        self.managed_keys.ensure_loaded(&self.secrets, &name).await
    }
    pub(crate) async fn ensure_key_loaded_name(&self, name: &str) -> Result<()> {
        if name.is_empty() || self.secrets.key(name).is_some() {
            return Ok(());
        }
        self.managed_keys.ensure_loaded(&self.secrets, name).await
    }

    pub(crate) async fn load_pack(
        self: &Arc<Self>,
        p: crate::content::pack::Pack,
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
                        priority: Arc::new(AtomicUsize::new(
                            crate::storage::resource_gate::MAINTENANCE,
                        )),
                    });
                    map.insert(p.id, Arc::downgrade(&cell));
                    cell
                })
        };
        lock.priority.fetch_min(
            crate::storage::resource_gate::PRIORITY
                .try_with(|p| *p)
                .unwrap_or(crate::storage::resource_gate::FOREGROUND),
            Ordering::Relaxed,
        );
        let result = lock
            .result
            .get_or_init(|| async {
                let result: Result<Arc<LoadedPack>> = async {
                    let storage_id = p.storage_id.context("pack has no store")?;
                    let target = self.store_backend(storage_id)?;
                    target.reads.local.refresh();
                    self.ensure_storage_verified(storage_id).await?;
                    self.ensure_key_loaded(p.block_key_id).await?;
                    let memory = self.pack_memory(p.payload_size()?).await?;
                    let read_permit = target
                        .reads
                        .acquire_with_priority(Some(lock.priority.clone()))
                        .await?;
                    #[cfg(feature = "fault-injection")]
                    crate::faults::point("pack-download").await;
                    let encoded = self
                        .read_path_permitted(
                            storage_id,
                            &self.pack_path_for(storage_id, p.location_id)?,
                            p.payload_size()? + 16,
                        )
                        .await?;
                    drop(read_permit);
                    let permit = self.cpu.clone().acquire_owned().await?;
                    let secrets = self.secrets.clone();
                    let (pool, _, _) = self.compression_for(p.storage_id);
                    let pack = p.clone();
                    let diagnostics = self.diagnostics.clone();
                    let (data, memory) = tokio::task::spawn_blocking(move || {
                        let _permit = permit;
                        #[cfg(feature = "fault-injection")]
                        crate::faults::blocking("pack-decode");
                        let data = crate::content::pack::decode(&pack, encoded, &secrets, &pool)
                            .inspect_err(|e| diagnostics.content(e))?;
                        Ok::<_, anyhow::Error>((data, memory))
                    })
                    .await??;
                    let loaded = Arc::new(LoadedPack {
                        pack: p.clone(),
                        data,
                        _memory: memory,
                        used: Mutex::new(DownloadUsage::default()),
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
        let chunks: Vec<Chunk> = self.physical.ready_chunks(&ids).await?;
        for c in chunks {
            self.fill_raw(&c, loaded.data.chunk(&c)?).await?;
        }
        Ok(())
    }
    pub async fn fill_raw(&self, c: &Chunk, raw: Bytes) -> Result<()> {
        if !self.local_cache(c.storage_id).enabled {
            return Ok(());
        }
        let (data, compressed) = self.cache_payload(c, raw).await?;
        let policy = self.local_cache(c.storage_id);
        self.fill_owned(cache::cache_id(c), &data, compressed, policy)
            .await
    }
    pub async fn cache_payload(&self, c: &Chunk, raw: Bytes) -> Result<(Bytes, bool)> {
        let policy = self.local_cache(c.storage_id);
        if !policy.enabled {
            return Ok((raw, false));
        }
        let (pool, enabled, threshold) = self.compression_for(c.storage_id);
        if enabled && codec::cache_compressed(c, threshold) {
            let permit = self.cpu.clone().acquire_owned().await?;
            let input = raw.clone();
            let compressed = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                pool.compress_with(&input, true, crate::content::compression::Strategy::Always)
            })
            .await??;
            if let Some(data) = compressed {
                return Ok((Bytes::from(data), true));
            }
        }
        Ok((raw, false))
    }
    pub(crate) async fn head(&self, c: &Chunk) -> Result<u64> {
        let storage_id = c.storage_id.context("chunk has no store")?;
        Ok(self
            .head_path_for(storage_id, &self.path_for(storage_id, c.location_id)?)
            .await?
            .size)
    }
    pub(crate) async fn verify_chunk(&self, chunk: &Chunk) -> Result<()> {
        self.ensure_key_loaded(chunk.block_key_id).await?;
        let encoded = self.read_backend(chunk).await?;
        self.decode(chunk, encoded).await?;
        Ok(())
    }
    pub(crate) async fn head_path_for(
        &self,
        storage_id: Uuid,
        path: &Path,
    ) -> Result<object_store::ObjectMeta> {
        self.ensure_storage_verified(storage_id).await?;
        let target = self.store_backend(storage_id)?;
        let _permit = target.controls.acquire().await?;
        let mut operation = self.operations[3].begin();
        let result = target
            .backend
            .head(path)
            .await
            .inspect_err(|e| self.diagnostics.backend(e));
        operation.finish(result.is_err());
        Ok(result?)
    }
    pub async fn inspect(
        &self,
        task: Uuid,
        c: &Chunk,
        mode: crate::worker::integrity::Mode,
        physical_detail: &mut serde_json::Value,
    ) -> Result<u64> {
        let pending: bool = self.physical.pending_only(c.id).await?;
        if pending {
            *physical_detail = serde_json::json!({"source":"pending","storage_id":c.storage_id});
            ensure!(
                self.cached(c).await?.is_some(),
                "pending upload source is missing"
            );
            return Ok(0);
        }
        self.inspect_source(
            Some(task),
            self.source(c.id, true).await?,
            c,
            mode,
            physical_detail,
        )
        .await
    }
    pub(crate) async fn verify_source_namespace(
        &self,
        snapshot: &crate::model::source::SourceSnapshot,
    ) -> Result<bool> {
        let target = self.store_backend(snapshot.scope.storage_id)?;
        ensure!(
            target.revision == snapshot.storage_revision
                && target.identity_marker == snapshot.identity_marker,
            crate::model::Error::OperationAborted
        );
        self.check_storage_inner(&target).await?;
        target.identity_ready.store(true, Ordering::Release);
        Ok(target.is_local)
    }
    pub(crate) async fn inspect_exact(
        &self,
        snapshot: &crate::model::source::SourceSnapshot,
    ) -> Result<u64> {
        let (source, chunk) = {
            let _gate = self.source_gate.read().await;
            let pin = self.physical_pin(snapshot.location());
            match &snapshot.physical {
                crate::model::PhysicalSource::Chunk(chunk) => {
                    (Source::Chunk(chunk.clone(), pin), chunk.clone())
                }
                crate::model::PhysicalSource::Pack(pack) => {
                    let members = self.physical.inspection_members(pack.id).await?;
                    let chunk = members
                        .first()
                        .context(codec::IntegrityError::Metadata)?
                        .0
                        .clone();
                    (Source::Pack(pack.clone(), pin), chunk)
                }
            }
        };
        self.inspect_source(
            None,
            source,
            &chunk,
            crate::worker::integrity::Mode::Full,
            &mut serde_json::Value::Null,
        )
        .await
    }
    async fn inspect_source(
        &self,
        task: Option<Uuid>,
        source: Source,
        c: &Chunk,
        mode: crate::worker::integrity::Mode,
        physical_detail: &mut serde_json::Value,
    ) -> Result<u64> {
        match source {
            Source::Chunk(physical, _pin) => {
                self.ensure_key_loaded(physical.block_key_id).await?;
                *physical_detail = serde_json::json!({"source":"chunk","encoding_id":physical.encoding_id.to_string(),"location_id":physical.location_id});
                codec::validate_metadata(&physical, &self.secrets)
                    .inspect_err(|e| self.diagnostics.content(e))?;
                match mode {
                    crate::worker::integrity::Mode::Metadata => Ok(0),
                    crate::worker::integrity::Mode::Head => {
                        let size = self.head(&physical).await?;
                        ensure!(
                            Some(size as i32) == physical.stored_size,
                            codec::IntegrityError::Length
                        );
                        Ok(0)
                    }
                    crate::worker::integrity::Mode::Full => {
                        let data = self.read_backend(&physical).await?;
                        let size = data.len() as u64;
                        self.decode(&physical, data).await?;
                        Ok(size)
                    }
                }
            }
            Source::Pack(p, _pin) => {
                self.ensure_key_loaded(p.block_key_id).await?;
                *physical_detail = serde_json::json!({"source":"pack","pack_id":p.id.to_string(),"location_id":p.location_id});
                p.validate(&self.secrets)
                    .inspect_err(|e| self.diagnostics.content(e))?;
                let capacity = p.payload_size().context(codec::IntegrityError::Metadata)?;
                ensure!(
                    self.physical.pack_contains(p.id, c.id).await?,
                    codec::IntegrityError::Metadata
                );
                // Persistent task results avoid downloading one physical pack per logical chunk.
                let _inspection = self.inspection.lock().await;
                if let Some(task) = task
                    && let Some(code) = self.physical.inspection(task, p.id).await?
                {
                    if let Some(code) = code {
                        return Err(codec::IntegrityError::from_code(&code)
                            .context("invalid inspection result")?
                            .into());
                    }
                    return Ok(0);
                }
                let result: Result<u64> = async {
                    let members: Vec<(Chunk, i64)> = self.physical.inspection_members(p.id).await?;
                    let mut offset = 0i64;
                    ensure!(
                        members.len() == p.member_count as usize,
                        codec::IntegrityError::Metadata
                    );
                    for (c, start) in &members {
                        ensure!(
                            *start == offset && c.raw_size > 0,
                            codec::IntegrityError::Metadata
                        );
                        offset += i64::from(c.raw_size);
                    }
                    ensure!(offset == p.raw_size, codec::IntegrityError::Metadata);
                    if matches!(mode, crate::worker::integrity::Mode::Metadata) {
                        return Ok(0);
                    }
                    if matches!(mode, crate::worker::integrity::Mode::Head) {
                        let storage_id = p.storage_id.context(codec::IntegrityError::Metadata)?;
                        let meta = self
                            .head_path_for(
                                storage_id,
                                &self.pack_path_for(storage_id, p.location_id)?,
                            )
                            .await?;
                        ensure!(
                            Some(meta.size as i64) == p.stored_size,
                            codec::IntegrityError::Length
                        );
                        return Ok(0);
                    }
                    let memory = self.pack_memory(capacity).await?;
                    let storage_id = p.storage_id.context(codec::IntegrityError::Metadata)?;
                    let data = self
                        .read_path_for(
                            storage_id,
                            &self.pack_path_for(storage_id, p.location_id)?,
                            capacity + 16,
                        )
                        .await?;
                    let size = data.len() as u64;
                    let permit = self.cpu.clone().acquire_owned().await?;
                    let pool = self.compression.clone();
                    let secrets = self.secrets.clone();
                    let pack = p.clone();
                    let diagnostics = self.diagnostics.clone();
                    tokio::task::spawn_blocking(move || {
                        let _permit = permit;
                        let _memory = memory;
                        #[cfg(feature = "fault-injection")]
                        crate::faults::blocking("pack-inspect");
                        let result = (|| {
                            let decoded =
                                crate::content::pack::decode(&pack, data, &secrets, &pool)?;
                            for ((c, offset), member) in members.iter().zip(&decoded.members) {
                                ensure!(
                                    member.id == c.id
                                        && member.hash.as_slice() == c.hash
                                        && member.len == c.raw_size as usize
                                        && member.start
                                            == 12 + 44 * members.len() + *offset as usize,
                                    codec::IntegrityError::Metadata
                                );
                            }
                            Ok::<_, anyhow::Error>(())
                        })();
                        result.inspect_err(|e| diagnostics.content(e))
                    })
                    .await??;
                    Ok(size)
                }
                .await;
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
                if let Some(task) = task
                    && (result.is_ok() || code.is_some())
                {
                    self.physical.record_inspection(task, p.id, code).await?;
                }
                result
            }
        }
    }
    // Shared bounded remote read. Inspection must not use or populate the cache.
    pub(crate) async fn read_backend(&self, c: &Chunk) -> Result<Vec<u8>> {
        let storage_id = c.storage_id.context("chunk has no store")?;
        self.read_path_for(
            storage_id,
            &self.path_for(storage_id, c.location_id)?,
            MAX + 16,
        )
        .await
    }
    pub async fn read_path_for(
        &self,
        storage_id: Uuid,
        path: &Path,
        bound: usize,
    ) -> Result<Vec<u8>> {
        self.ensure_storage_verified(storage_id).await?;
        let target = self.store_backend(storage_id)?;
        let _permit = target.reads.acquire().await?;
        self.read_path_permitted(storage_id, path, bound).await
    }
    async fn read_path_permitted(
        &self,
        storage_id: Uuid,
        path: &Path,
        bound: usize,
    ) -> Result<Vec<u8>> {
        let target = self.store_backend(storage_id)?;
        let mut operation = self.operations[0].begin();
        self.backend_gets
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let result: Result<Vec<u8>> = async {
            #[cfg(feature = "fault-injection")]
            crate::faults::point("storage-read-permitted").await;
            let result = target.backend.get(path).await?;
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
        let content_error = result
            .as_ref()
            .err()
            .is_some_and(|e| e.downcast_ref::<codec::IntegrityError>().is_some());
        if let Err(error) = &result {
            if content_error {
                self.diagnostics.content(error);
            } else if let Some(error) = error.downcast_ref::<object_store::Error>() {
                self.diagnostics.backend(error);
            }
        }
        operation.finish(result.is_err() && !content_error);
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
