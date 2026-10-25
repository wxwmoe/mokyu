use crate::model::Id;
use crate::{
    config::{self, Budget, Config, Secrets},
    content::codec::{self, Chunk, MAX},
};
pub(crate) mod assembly;
mod backlog;
pub(crate) mod cache;
pub(crate) mod diagnostics;
pub(crate) mod file_cache;
pub(crate) mod local;
pub(crate) mod location;
mod memory;
pub(crate) mod model;
mod multipart;
pub(crate) mod permits;
pub(crate) mod profile;
mod rate;
pub(crate) mod raw;
pub(crate) mod raw_cache;
pub(crate) mod raw_request;
pub(crate) mod raw_upload;
pub(crate) mod resource_gate;
mod s3;
mod sources;
mod sync;
pub(crate) mod target;

#[cfg(all(test, feature = "fault-injection"))]
#[path = "../../tests/recovery/pack_memory.rs"]
mod memory_tests;

#[cfg(all(test, feature = "fault-injection"))]
#[path = "../../tests/storage/diagnostics.rs"]
mod diagnostic_tests;

#[cfg(all(test, feature = "fault-injection"))]
#[path = "../../tests/recovery/raw_permits.rs"]
mod raw_permit_tests;

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
pub(crate) const DEFAULT_STAGING_LIMIT: u64 = 1024 * 1024 * 1024;

pub struct Storage {
    write_backlog: Mutex<backlog::Backlog>,
    pub(crate) write_memory: memory::Pool,
    pub(crate) boot_id: Uuid,
    pub(crate) buffered_writes: std::sync::atomic::AtomicBool,
    pub(crate) wake_sync: tokio::sync::Notify,
    pub access: Arc<crate::status::access::Access>,
    maintenance: Arc<std::sync::atomic::AtomicBool>,
    physical: Arc<dyn crate::model::PhysicalStore>,
    cluster: Option<Arc<dyn crate::model::cluster::ClusterStore>>,
    profile_store: Arc<dyn crate::model::StorageProfileStore>,
    raw_store: Arc<dyn crate::model::raw::RawStore>,
    reflink: Arc<Mutex<HashMap<(u64, u64), bool>>>,
    managed_keys: Arc<dyn crate::model::ManagedKeyStore>,
    nonces: crate::content::nonce::Allocator,
    record_store: Arc<dyn crate::model::StorageRecordStore>,
    pub source_gate: tokio::sync::RwLock<()>,
    pub(crate) source_reads: sources::Cache,
    read_epoch: std::sync::atomic::AtomicU64,
    pub(crate) response_cache: std::sync::OnceLock<std::sync::Weak<crate::app::responses::Cache>>,
    physical_pins: Mutex<HashMap<Uuid, std::sync::Weak<AtomicUsize>>>,
    pack_misses: Mutex<HashMap<(Id, bool), std::sync::Weak<PackFetch>>>,
    pack_live: Mutex<HashMap<(Id, bool), std::sync::Weak<LoadedPack>>>,
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
    node_id: std::sync::OnceLock<Uuid>,
    pub secrets: Arc<Secrets>,
    pub(crate) cpu: Arc<Semaphore>,
    pub compression: Arc<crate::content::compression::Pool>,
    compression_idle_timeout: String,
    compression_workspace: u64,
    restart_required: Arc<std::sync::atomic::AtomicBool>,
    cpu_jobs: usize,
    pub reads: Arc<crate::storage::resource_gate::Gate>,
    pub writes: Arc<crate::storage::resource_gate::Gate>,
    pub controls: Arc<crate::storage::resource_gate::Gate>,
    fifo: tokio::sync::Mutex<Fifo>,
    cache_writes: tokio::sync::Mutex<()>,
    upload_pins: Mutex<HashMap<Uuid, UploadPin>>,
    raw_upload_pins: Mutex<HashMap<Uuid, (crate::model::Id, UploadPin)>>,
    remote_charges: Mutex<HashMap<Id, UploadCharge>>,
    fills: Arc<Semaphore>,
    cache_fills: Arc<Mutex<HashSet<Uuid>>>,
    max_entries: usize,
    capacity: Option<u64>,
    misses: Mutex<HashMap<(Id, Uuid, bool, bool), std::sync::Weak<FetchResult>>>,
    raw_misses: crate::runtime::locks::Locks<Uuid>,
    pub backend_gets: Arc<std::sync::atomic::AtomicU64>,
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
    compression_idle_timeout: &str,
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
    let raw_root = record.path.as_ref().map(|path| {
        PathBuf::from(path)
            .join(prefix.trim_matches('/'))
            .join("raw")
    });
    let compression = Arc::new(crate::content::compression::Pool::new(
        profile.compression_config(compression_idle_timeout),
        cpu_jobs,
    )?);
    let aging = Duration::from_secs(config::seconds(&profile.priority_aging)?);
    let buffered_backend = if is_local {
        Some(Arc::new(object_store::prefix::PrefixStore::new(
            object_store::local::LocalFileSystem::new_with_prefix(
                record.path.as_deref().context("local path missing")?,
            )?,
            Path::parse(&prefix)?,
        )) as Arc<dyn ObjectStore>)
    } else {
        None
    };
    let backend = StoreBackend {
        buffered_backend,
        profile: Arc::new(profile.clone()),
        backend: target.backend,
        multipart: target.multipart,
        streaming: target.streaming,
        raw_root,
        cdc_enabled: profile.cdc_enabled,
        deduplication_enabled: profile.deduplication_enabled,
        profile_revision: profile.revision,
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
    };
    for gate in [&backend.reads, &backend.writes, &backend.controls] {
        let (limit, aging, _) = gate.local.settings();
        gate.local.configure(
            limit,
            aging.min(Duration::from_secs(profile.write_backlog_seconds()?)),
            profile.background_scheduling == "foreground",
        );
    }
    Ok(backend)
}

impl Storage {
    pub async fn new(
        c: &Config,
        secrets: Arc<Secrets>,
        budget: &Budget,
        physical: Arc<dyn crate::model::PhysicalStore>,
        cluster: Option<Arc<dyn crate::model::cluster::ClusterStore>>,
        profile_store: Arc<dyn crate::model::StorageProfileStore>,
        raw_store: Arc<dyn crate::model::raw::RawStore>,
        managed_keys: Arc<dyn crate::model::ManagedKeyStore>,
        record_store: Arc<dyn crate::model::StorageRecordStore>,
        maintenance: Arc<std::sync::atomic::AtomicBool>,
        restart_required: Arc<std::sync::atomic::AtomicBool>,
        cluster_mode: bool,
        use_target: bool,
    ) -> Result<Self> {
        let cluster = if cluster_mode { cluster } else { None };
        let pack_units = budget.pack_memory_units;
        let mut compression = crate::content::compression::Config::default();
        compression.context_idle_timeout = c.instance.compression_idle_timeout.clone();
        let rows: Vec<crate::model::StorageRecord> = if use_target {
            record_store.enabled().await?
        } else {
            Vec::new()
        };
        for row in &rows {
            if let Some(path) = &row.path {
                local::validate_storage_path(
                    &c.instance.data_path,
                    &PathBuf::from(path).join(row.prefix.trim_matches('/')),
                )?;
            }
        }
        for name in ["multipart", "chunks", "raw"] {
            tokio::fs::create_dir_all(c.instance.data_path.join(name)).await?;
        }
        // The parent entries must survive before a cache file can be the only acknowledged copy.
        tokio::fs::File::open(&c.instance.data_path)
            .await?
            .sync_all()
            .await?;
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
            spaces: Mutex::default(),
            upload_limit: c
                .instance
                .upload_cache_size
                .as_deref()
                .map(config::cache_bytes)
                .transpose()?,
            usage: Mutex::new(Usage {
                used: [0; 3],
                upload: 0,
                upload_by_storage: HashMap::new(),
            }),
            io: Arc::new(Semaphore::new(budget.cpu_jobs)),
            pending: Mutex::new(HashSet::new()),
        });
        let aging = Duration::from_secs(30);
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
                &c.instance.compression_idle_timeout,
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
            write_backlog: Mutex::new(backlog::Backlog::new(physical.write_backlog().await?)),
            maintenance,
            access: Arc::new(crate::status::access::Access::new(
                budget.cache_entries.clamp(128, 8192),
            )),
            physical: physical.clone(),
            cluster,
            profile_store,
            wake_sync: Default::default(),
            write_memory: memory::Pool::new(budget.write_memory_bytes),
            boot_id: Uuid::new_v4(),
            buffered_writes: (!raw_store
                .buffered(Id::default(), None, None, false, &[])
                .await?
                .is_empty()
                || !physical
                    .buffered_chunks(Id::default(), None, None, None, false, &[])
                    .await?
                    .is_empty())
            .into(),
            raw_store,
            reflink: Arc::new(Mutex::new(HashMap::new())),
            managed_keys,
            nonces: crate::content::nonce::Allocator::default(),
            record_store,
            source_gate: tokio::sync::RwLock::new(()),
            source_reads: sources::Cache::new(),
            read_epoch: 0.into(),
            response_cache: std::sync::OnceLock::new(),
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
            node_id: std::sync::OnceLock::new(),
            secrets,
            cpu: Arc::new(Semaphore::new(budget.cpu_jobs)),
            compression: Arc::new(crate::content::compression::Pool::new(
                compression.clone(),
                budget.cpu_jobs,
            )?),
            compression_idle_timeout: c.instance.compression_idle_timeout.clone(),
            compression_workspace: budget.compression_workspace,
            restart_required,
            cpu_jobs: budget.cpu_jobs,
            reads: crate::storage::resource_gate::Gate::new(budget.storage_read_slots, aging),
            writes: crate::storage::resource_gate::Gate::new(budget.storage_upload_slots, aging),
            controls: crate::storage::resource_gate::Gate::new(budget.storage_control_slots, aging),
            fifo: tokio::sync::Mutex::new(Fifo::default()),
            cache_writes: tokio::sync::Mutex::new(()),
            upload_pins: Mutex::new(HashMap::new()),
            raw_upload_pins: Mutex::new(HashMap::new()),
            remote_charges: Mutex::new(HashMap::new()),
            fills: Arc::new(Semaphore::new(budget.cpu_jobs)),
            cache_fills: Arc::new(Mutex::new(HashSet::new())),
            max_entries: c.instance.cache_entries.unwrap_or(budget.cache_entries),
            capacity: c
                .instance
                .cache_size
                .as_deref()
                .map(config::cache_bytes)
                .transpose()?,
            misses: Mutex::new(HashMap::new()),
            raw_misses: Default::default(),
            backend_gets: Arc::new(0.into()),
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
        if !cluster_mode {
            s.raw_store.recover_memory(s.boot_id).await?;
            s.physical.recover_memory(s.boot_id).await?;
            s.restore_cache().await?;
        }
        assembly::probe(&s, s.disk.root.join("raw"));
        for target in s.targets.read().unwrap().values() {
            if let Some(root) = &target.raw_root {
                assembly::probe(&s, root.clone());
            }
        }
        Ok(s)
    }
    pub(crate) async fn initialize_cache(&self, node: Uuid) -> Result<()> {
        ensure!(
            self.node_id.set(node).is_ok(),
            "cache owner was already initialized"
        );
        self.restore_cache().await
    }

    async fn restore_cache(&self) -> Result<()> {
        self.reconcile_remote_charges().await?;
        self.restore_upload_pins().await?;
        self.restore_raw_uploads().await?;
        self.restore_raw_checkpoints().await?;
        self.scan().await
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
        serde_json::json!({"backend":{"get":self.operations[0].snapshot(),"put":self.operations[1].snapshot(),"delete":self.operations[2].snapshot(),"head":self.operations[3].snapshot()},"diagnostics":diagnostics,"source_cache":self.source_reads.snapshot(),
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
    fn local_space(&self, target: &StoreBackend, bytes: u64) -> Result<Option<local::Space>> {
        if !target.is_local {
            return Ok(None);
        }
        let root = target
            .raw_root
            .as_ref()
            .and_then(|path| path.parent())
            .context("local storage path is missing")?;
        let floor = config::bytes(
            target
                .profile
                .min_free_space
                .as_deref()
                .context("local storage floor is missing")?,
        )?;
        Ok(Some(local::Space::claim(
            root,
            floor,
            bytes,
            &self.disk.space_for(root)?,
        )?))
    }
    fn local_cache(&self, storage_id: Option<Uuid>) -> Result<CachePolicy> {
        let target = self.store_backend(storage_id.context("source has no storage")?)?;
        Ok(CachePolicy {
            enabled: target.cache_enabled,
            capacity: target.cache_capacity.or(self.capacity),
            max_entries: target.cache_entries,
            upload_cache: target.upload_cache,
            upload_capacity: target.upload_cache_size,
        })
    }
    fn compression_for(
        &self,
        storage_id: Option<Uuid>,
    ) -> Result<(Arc<crate::content::compression::Pool>, bool, u8)> {
        let target = self.store_backend(storage_id.context("source has no storage")?)?;
        Ok((
            target.compression.clone(),
            target.compression_enabled,
            target.min_compression_savings_percent,
        ))
    }
    pub(crate) fn profile(&self, storage: Uuid) -> Result<Arc<StorageProfile>> {
        Ok(self.store_backend(storage)?.profile.clone())
    }
    pub(crate) fn public_cache_enabled(&self) -> bool {
        self.targets
            .read()
            .unwrap()
            .values()
            .any(|target| target.profile.public_cache_validation == "background")
    }
    fn encoding_compression(
        &self,
        storage: Option<Uuid>,
        accepted: Option<&crate::content::compression::Config>,
    ) -> Result<(Arc<crate::content::compression::Pool>, bool, u8)> {
        let Some(config) = accepted else {
            return self.compression_for(storage);
        };
        ensure!(
            config.workspace_bytes()? <= self.compression_workspace,
            "accepted compression policy exceeds node memory budget"
        );
        let target = self.store_backend(storage.context("source has no storage")?)?;
        let pool = if target
            .profile
            .compression_config(&self.compression_idle_timeout)
            == *config
        {
            target.compression.clone()
        } else {
            Arc::new(crate::content::compression::Pool::new(config.clone(), 1)?)
        };
        Ok((pool, config.enabled, config.min_savings_percent))
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
        let result = self.refresh_store_inner(storage_id).await;
        if result.is_err() {
            let mut targets = self.targets.write().unwrap();
            if targets
                .get(&storage_id)
                .is_some_and(|target| target.profile.file_delivery != "application")
            {
                // A failed refresh must not keep exporting the old proxy path.
                targets.remove(&storage_id);
                self.invalidate_reads();
            }
        }
        result
    }

    async fn refresh_store_inner(&self, storage_id: Uuid) -> Result<()> {
        let row = self.record_store.enabled_by_id(storage_id).await?;
        let backend = if let Some(row) = row {
            if let Some(path) = &row.path {
                local::validate_storage_path(
                    &self.disk.root,
                    &PathBuf::from(path).join(row.prefix.trim_matches('/')),
                )?;
            }
            let profile = self
                .profile_store
                .load(storage_id)
                .await
                .with_context(|| format!("storage profile is missing for {storage_id}"))?;
            let workspace = profile
                .compression_config(&self.compression_idle_timeout)
                .workspace_bytes()?;
            let maximum = config::bytes(&profile.pack_max_size)?;
            let payload = maximum
                + 12
                + crate::content::pack::MEMBER_SIZE as u64 * (maximum / codec::MIN as u64 + 1);
            if workspace > self.compression_workspace
                || (profile.pack_enabled
                    && (payload * 4 + workspace).div_ceil(1024 * 1024)
                        > u64::from(self.pack_memory_units))
            {
                self.restart_required.store(true, Ordering::Release);
                tracing::warn!(%storage_id, "storage profile saved; restart required to resize node memory budgets");
                return Ok(());
            }
            Some(
                build_store(
                    StoreOptions::from_profile(&profile)?,
                    &self.secrets,
                    &self.cluster,
                    row,
                    &profile,
                    &self.compression_idle_timeout,
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
        if let Some(cache) = self.response_cache.get().and_then(std::sync::Weak::upgrade) {
            cache.storage_policy(
                storage_id,
                backend
                    .as_ref()
                    .is_none_or(|backend| backend.profile.file_delivery != "application"),
            );
        }
        if let Some(mut backend) = backend {
            backend.reuse_gates(&mut self.store_gates.lock().unwrap());
            if let Some(root) = &backend.raw_root {
                assembly::probe(self, root.clone());
            }
            targets.insert(storage_id, Arc::new(backend));
        } else {
            targets.remove(&storage_id);
            self.integrity_rates.lock().unwrap().remove(&storage_id);
        }
        self.invalidate_reads();
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
    pub(crate) fn parse_physical_path(path: &str) -> Option<Uuid> {
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
        let profile = self.profile_store.load(record.id).await?;
        self.initialize_store_with_profile(record, &profile).await
    }

    pub(crate) async fn initialize_store_with_profile(
        &self,
        record: StoreRecord,
        profile: &StorageProfile,
    ) -> Result<()> {
        if record.r#type == "local" {
            local::validate_storage_path(
                &self.disk.root,
                &PathBuf::from(
                    record
                        .path
                        .as_ref()
                        .context("local store path is missing")?,
                )
                .join(record.prefix.trim_matches('/')),
            )?;
            tokio::fs::create_dir_all(
                record
                    .path
                    .as_deref()
                    .context("local store path is missing")?,
            )
            .await?;
        }
        let options = StoreOptions::from_profile(profile)?;
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
        if let Some(path) = &record.path {
            local::validate_storage_path(
                &self.disk.root,
                &PathBuf::from(path).join(record.prefix.trim_matches('/')),
            )?;
        }
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
        CacheFile::Chunk(compressed).path(&self.disk.root, id)
    }
    async fn restore_upload_pins(&self) -> Result<()> {
        let mut after = Id::default();
        loop {
            let rows = self
                .physical
                .upload_pins(after, self.node_id.get().copied())
                .await?;
            if rows.is_empty() {
                break;
            }
            for (id, staging_storage_id, object_key, expected, storage_id) in rows {
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
                self.upload_pins.lock().unwrap().insert(
                    key,
                    UploadPin {
                        memory: None,
                        _pin: Some(CachePin(pin)),
                        _charge: Some(self.disk.restore_upload(storage_id, expected as u64)),
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
        source: &crate::model::FragmentSource,
    ) -> Result<Vec<u8>> {
        if let (Some(storage_id), Some(key)) = (
            source.staging_storage_id,
            source.staging_object_key.as_deref(),
        ) {
            return self.read_staged(storage_id, key, MAX).await;
        }
        if let Some(file) = source.file_id {
            use tokio::io::{AsyncReadExt, AsyncSeekExt};
            let offset = source
                .file_offset
                .context("multipart file offset missing")?;
            ensure!(
                offset >= 0 && source.size > 0 && source.size as usize <= MAX,
                "invalid multipart range"
            );
            let mut file = tokio::fs::File::open(self.multipart_path(
                file,
                source.file_storage,
                source.raw_source,
            )?)
            .await?;
            let length = file.metadata().await?.len();
            ensure!(
                (offset as u64)
                    .checked_add(source.size as u64)
                    .is_some_and(|end| end <= length),
                "multipart range exceeds file"
            );
            file.seek(std::io::SeekFrom::Start(offset as u64)).await?;
            let mut bytes = vec![0; source.size as usize];
            file.read_exact(&mut bytes).await?;
            return Ok(bytes);
        }
        read_bounded(self.disk.fragment_path(id), MAX).await
    }
    pub(crate) fn multipart_path(
        &self,
        file: Uuid,
        storage: Option<Uuid>,
        source: Option<Uuid>,
    ) -> Result<std::path::PathBuf> {
        if let Some(source) = source {
            let backend = self.store_backend(storage.context("multipart storage missing")?)?;
            return Ok(if let Some(root) = &backend.raw_root {
                root.join(chunk_name(source)).with_extension("partial")
            } else {
                CacheFile::Raw
                    .path(&self.disk.root, source)
                    .with_extension("partial")
            });
        }
        Ok(self.disk.fragment_path(file))
    }
    pub(crate) async fn remove_multipart_file(
        &self,
        file: &crate::model::multipart::MultipartFile,
    ) -> Result<()> {
        if file.source_id.is_none() {
            return self.disk.remove_fragment(file.id).await;
        }
        if !self.is_local(file.storage_id)? {
            return self.remove_raw_checkpoint(file.id).await;
        }
        let path = self.multipart_path(
            file.id,
            Some(file.storage_id),
            file.source_id.map(|_| file.id),
        )?;
        match tokio::fs::remove_file(path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
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
    pub(crate) fn stage_memory(&self, data: Bytes) -> Option<UploadPin> {
        let size = data.len() as u64;
        let data = self.write_memory.retain(data)?;
        Some(UploadPin {
            memory: Some(data),
            _pin: None,
            _charge: None,
            size,
            compressed: false,
            remote: false,
            staging_storage_id: None,
            staging_object_key: String::new(),
        })
    }
    pub(crate) fn chunk_memory(&self, id: Uuid) -> Option<Bytes> {
        self.upload_pins.lock().unwrap().get(&id)?.memory.clone()
    }
    pub async fn stage_upload(
        self: &Arc<Self>,
        c: &Chunk,
        data: Bytes,
        compressed: bool,
        buffered: bool,
    ) -> Result<Option<UploadPin>> {
        let policy = self.local_cache(c.storage_id)?;
        let id = cache::cache_id(c);
        let storage_id = c.storage_id.context("cached chunk has no storage")?;
        if !policy.upload_cache {
            return Ok(None);
        }
        let permit = self.fills.clone().acquire_owned().await?;
        let storage = self.clone();
        tokio::spawn(async move {
            let _permit = permit;
            storage
                .stage_upload_owned(storage_id, id, data, compressed, policy, buffered)
                .await
        })
        .await?
    }
    pub(super) async fn stage_upload_owned(
        &self,
        storage_id: Uuid,
        id: Uuid,
        data: Bytes,
        compressed: bool,
        policy: CachePolicy,
        buffered: bool,
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
        let Ok(charge) =
            self.disk
                .claim_upload_with_limit(size, storage_id, policy.upload_capacity)
        else {
            return Ok(None);
        };
        if let Some(e) = fifo.entries.get(&id) {
            let removal = CacheRemoval::new(&e.pins);
            let path = e.file.path(&self.disk.root, id);
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
            let path = entry.file.path(&self.disk.root, victim);
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
                .mode(0o644)
                .open(&tmp)
                .await?;
            f.write_all(&data).await?;
            #[cfg(feature = "fault-injection")]
            crate::faults::point("upload-cache-written").await;
            if buffered {
                f.flush().await?;
            } else {
                f.sync_all().await?;
            }
            drop(f);
            tokio::fs::rename(&tmp, &path).await?;
            if !buffered {
                raw::sync_directory(path.parent().unwrap()).await?;
                raw::sync_directory(&self.disk.root.join("chunks")).await?;
            }
            Ok::<_, anyhow::Error>(())
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
            return Err(e);
        }
        ticket.commit();
        let mut fifo = self.fifo.lock().await;
        fifo.insert(id, size, compressed);
        let pin = fifo.entries[&id].pins.clone();
        pin.fetch_add(1, Ordering::Relaxed);
        drop(fifo);
        let pin = UploadPin {
            memory: None,
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
                    .account_existing(Area::Multipart, local::allocated(&e.metadata().await?));
            }
        }
        for directory in ["chunks", "raw"] {
            let mut dirs = tokio::fs::read_dir(self.disk.root.join(directory)).await?;
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
                    let name = format!(
                        "{}/{}",
                        dir.file_name().to_string_lossy(),
                        file.file_name().to_string_lossy()
                    );
                    let cached = if directory == "raw" {
                        parse_chunk_name(&name).map(|id| (id, CacheFile::Raw))
                    } else {
                        parse_cache_name(&name)
                            .map(|(id, compressed)| (id, CacheFile::Chunk(compressed)))
                    };
                    if directory == "raw"
                        && let Some(id) = name.strip_suffix(".partial").and_then(parse_chunk_name)
                        && self.raw_store.cached_checkpoint(id).await?.is_some()
                    {
                        continue;
                    }
                    let mut fifo = self.fifo.lock().await;
                    if cached.is_some_and(|(id, format)| {
                        format == CacheFile::Raw
                            && self.raw_upload_pins.lock().unwrap().contains_key(&id)
                    }) {
                        continue;
                    }
                    if cached.is_some_and(|(id, format)| {
                        self.upload_pins
                            .lock()
                            .unwrap()
                            .get(&id)
                            .is_some_and(|p| format == CacheFile::Chunk(p.compressed))
                    }) {
                        continue;
                    }
                    if let Some((id, format)) = cached.filter(|(id, format)| {
                        (matches!(format, CacheFile::Raw) || (1..=MAX as u64).contains(&len))
                            && !fifo.entries.contains_key(id)
                            && fifo.entries.len() < self.max_entries
                            && self
                                .capacity
                                .is_none_or(|c| self.disk.used()[1].saturating_add(len) <= c)
                    }) {
                        fifo.insert(id, len, format);
                        self.disk.account_existing(Area::Cache, len);
                    } else {
                        tokio::fs::remove_file(file.path()).await?;
                    }
                }
            }
        }
        Ok(())
    }
    pub async fn encode(
        &self,
        mut c: Chunk,
        raw: Vec<u8>,
        should_compress: bool,
        memory: Option<&Arc<tokio::sync::OwnedSemaphorePermit>>,
        accepted: Option<&crate::content::compression::Config>,
    ) -> Result<(Chunk, Vec<u8>, Bytes)> {
        let (compression, compression_enabled, threshold) =
            self.encoding_compression(c.storage_id, accepted)?;
        c.nonce = self
            .encoding_nonce(
                &c.algorithm,
                c.block_key_id,
                c.created_at,
                crate::content::nonce::CHUNK,
            )
            .await?;
        let memory = memory.cloned();
        let permit = self.cpu.clone().acquire_owned().await?;
        let secrets = self.secrets.clone();
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
        self.decode_read(c, encoded, true).await
    }
    async fn decode_read(
        &self,
        c: &Chunk,
        encoded: Vec<u8>,
        verify: bool,
    ) -> Result<(Bytes, Bytes)> {
        let permit = self.cpu.clone().acquire_owned().await?;
        let c = c.clone();
        let secrets = self.secrets.clone();
        let (compression, _, threshold) = self.compression_for(c.storage_id)?;
        let diagnostics = self.diagnostics.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            codec::decode_read(&c, encoded, &secrets, threshold, &compression, verify)
                .inspect_err(|e| diagnostics.content(e))
        })
        .await?
    }
    pub async fn put(self: &Arc<Self>, c: &Chunk, encoded: Vec<u8>, cache: Bytes) -> Result<()> {
        self.put_received(c, encoded, cache, false).await
    }
    pub(crate) async fn put_received(
        self: &Arc<Self>,
        c: &Chunk,
        encoded: Vec<u8>,
        cache: Bytes,
        buffered: bool,
    ) -> Result<()> {
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
        let space = self.local_space(&target, size as u64)?;
        let backend = if buffered {
            self.physical.buffer_chunk(c, None).await?;
            self.buffered_writes.store(true, Ordering::Release);
            target
                .buffered_backend
                .as_ref()
                .context("buffered chunks require local storage")?
        } else {
            &target.backend
        };
        let result = backend
            .put(
                &self.path_for(storage_id, c.location_id)?,
                Bytes::from(encoded).into(),
            )
            .await
            .inspect_err(|e| self.diagnostics.backend(e));
        operation.finish(result.is_err());
        result?;
        drop(space);
        drop(permit);
        self.operations[1].bytes(size as u64);
        self.backend_write_bytes
            .fetch_add(size as u64, Ordering::Relaxed);
        if let Ok((_, _, threshold)) = self.compression_for(c.storage_id) {
            self.fill_read(c, cache, Some(codec::cache_compressed(c, threshold)));
        }
        Ok(())
    }
    async fn cached_file(
        &self,
        c: &Chunk,
        id: Uuid,
        verify: bool,
    ) -> Result<Option<(Bytes, usize)>> {
        if let Some(data) = self.chunk_memory(id) {
            ensure!(
                data.len() == c.raw_size as usize,
                crate::content::codec::IntegrityError::Length
            );
            // RAM retains the verified immutable upload bytes, like a decoded pack cache.
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            self.cache_hit_bytes
                .fetch_add(data.len() as u64, Ordering::Relaxed);
            return Ok(Some((data, 0)));
        }
        if !self.local_cache(c.storage_id)?.enabled
            && !self.upload_pins.lock().unwrap().contains_key(&id)
        {
            return Ok(None);
        }
        let cached = self
            .file_cache()
            .pin(id)
            .await
            .and_then(|(pin, file)| match file {
                CacheFile::Chunk(compressed) => Some((pin, compressed)),
                CacheFile::Raw => None,
            });
        if let Some((pin, compressed)) = cached {
            let file = read_bounded(self.cache_path(id, compressed), MAX).await;
            let missing = file.as_ref().err().is_some_and(|e| {
                e.downcast_ref::<std::io::Error>()
                    .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound)
            });
            if let Ok(data) = file {
                let size = data.len() as u64;
                let retained = if compressed {
                    c.raw_size as usize
                } else {
                    data.capacity()
                };
                let permit = self.cpu.clone().acquire_owned().await?;
                let chunk = c.clone();
                let (compression, _, _) = self.compression_for(c.storage_id)?;
                let decoded = tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    codec::decode_cache_read(
                        &chunk,
                        Bytes::from(data),
                        compressed,
                        &compression,
                        verify,
                    )
                })
                .await?;
                if let Ok(raw) = decoded {
                    self.cache_hits.fetch_add(1, Ordering::Relaxed);
                    self.cache_hit_bytes.fetch_add(size, Ordering::Relaxed);
                    return Ok(Some((raw, retained)));
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
        Ok(self.cached_read(c, true).await?.map(|(bytes, _)| bytes))
    }
    async fn cached_read(&self, c: &Chunk, verify: bool) -> Result<Option<(Bytes, usize)>> {
        let mut cache_id = cache::cache_id(c);
        if let Some(raw) = self.cached_file(c, cache_id, verify).await? {
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
            if let Some(raw) = self.cached_file(c, id, verify).await? {
                return Ok(Some(raw));
            }
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
        if self.cluster_mode && self.physical.pending_only(c.id).await? {
            return Err(crate::app::readiness::pending());
        }
        Ok(None)
    }
    pub(crate) fn read_epoch(&self) -> u64 {
        self.read_epoch.load(Ordering::Acquire)
    }
    pub(crate) fn invalidate_reads(&self) {
        self.read_epoch.fetch_add(1, Ordering::AcqRel);
        self.source_reads.clear();
    }
    pub(crate) async fn reject_chunk_cache(&self, chunk: &Chunk) {
        self.reject_cache(cache::cache_id(chunk)).await;
        if chunk.location_id.is_nil()
            && let Ok(Some((_, path))) = self.physical.pending_source(chunk.id).await
            && let Some(id) = parse_upload_cache_name(&path)
        {
            self.reject_cache(id).await;
        }
    }
    pub(crate) async fn reject_cache(&self, id: Uuid) {
        self.file_cache().reject(id).await;
    }
    pub(crate) async fn invalidate(&self, id: Uuid) -> Result<()> {
        self.invalidate_entry(id, None).await
    }
    fn file_cache(&self) -> file_cache::Files<'_> {
        file_cache::Files {
            disk: &self.disk,
            root: &self.disk.root,
            fifo: &self.fifo,
            cache_writes: &self.cache_writes,
        }
    }
    async fn invalidate_entry(&self, id: Uuid, expected: Option<&Arc<AtomicUsize>>) -> Result<()> {
        self.file_cache().invalidate(id, expected).await
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
    pub(crate) fn fill_read(self: &Arc<Self>, c: &Chunk, data: Bytes, compressed: Option<bool>) {
        if let Ok(fifo) = self.fifo.try_lock()
            && fifo
                .entries
                .get(&cache::cache_id(c))
                .is_some_and(|entry| entry.valid)
        {
            return;
        }
        let Ok(policy) = self.local_cache(c.storage_id) else {
            return;
        };
        if !policy.enabled {
            return;
        }
        let Ok((pool, _, _)) = self.compression_for(c.storage_id) else {
            return;
        };
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
    async fn reserve_cache(&self, size: u64, policy: CachePolicy) -> Result<Option<Reservation>> {
        self.reserve_cache_space(size, policy, false).await
    }
    async fn reserve_cache_space(
        &self,
        size: u64,
        policy: CachePolicy,
        existing: bool,
    ) -> Result<Option<Reservation>> {
        self.file_cache().reserve(size, policy, existing).await
    }
    async fn fill_owned(
        &self,
        id: Uuid,
        data: &[u8],
        compressed: bool,
        policy: CachePolicy,
    ) -> Result<()> {
        self.file_cache().fill(id, data, compressed, policy).await
    }
    pub async fn get_with(
        self: &Arc<Self>,
        c: &Chunk,
        context: Option<&ReadContext>,
    ) -> Result<Bytes> {
        let profile = if context.is_some_and(|context| context.observe) {
            Some(self.profile(c.storage_id.context("chunk has no store")?)?)
        } else {
            None
        };
        let mode = profile
            .as_ref()
            .map_or("inline", |profile| profile.read_integrity.as_str());
        let (raw, retained) = self.read_chunk(c, context, mode == "inline").await?;
        if let Some(context) = context
            && let Some(checks) = &context.checks
        {
            let evidence = match mode {
                "background" => Some(checks.chunk(c, raw.clone(), retained)),
                "off" => Some(checks.unchecked()),
                _ => None,
            };
            if let Some(capture) = &context.capture
                && let Some(evidence) = evidence
            {
                capture.lock().unwrap().record_check(evidence);
            }
        }
        Ok(raw)
    }
    async fn read_chunk(
        self: &Arc<Self>,
        c: &Chunk,
        context: Option<&ReadContext>,
        verify: bool,
    ) -> Result<(Bytes, usize)> {
        self.cache_lookups.fetch_add(1, Ordering::Relaxed);
        if let Some(context) = context {
            let current = context.pack.lock().await;
            if let Some(loaded) = current.as_ref().filter(|p| {
                (!verify || p.verified) && p.data.members.iter().any(|m| m.id == c.uuid)
            }) {
                loaded.touch(c, context.observe, context.range);
                return Ok((loaded.data.chunk(c)?, c.raw_size as usize));
            }
        }
        if let Some(raw) = self.cached_read(c, verify).await? {
            if let Some(context) = context {
                context.touch(c).await;
            }
            return Ok(raw);
        }
        let lock = {
            let mut map = self.misses.lock().unwrap();
            map.retain(|_, v| v.strong_count() > 0);
            let key = (
                c.id,
                c.location_id,
                context.is_some_and(|context| context.observe),
                verify,
            );
            if let Some(lock) = map.get(&key).and_then(|v| v.upgrade()) {
                lock
            } else {
                let lock = Arc::new(tokio::sync::OnceCell::new());
                map.insert(key, Arc::downgrade(&lock));
                lock
            }
        };
        match lock
            .get_or_init(|| async { self.fetch(c, context, verify).await.map_err(Arc::new) })
            .await
        {
            Ok((raw, loaded, retained)) => {
                if let (Some(context), Some(loaded)) = (context, loaded) {
                    loaded.touch(c, context.observe, context.range);
                    // Prefetch may be advancing to the next pack while waiting for this one's
                    // memory. Never retain the previous pack while waiting for that advance.
                    if let Ok(mut current) = context.pack.try_lock() {
                        *current = Some(loaded.clone());
                    }
                }
                Ok((raw.clone(), *retained))
            }
            Err(error) => Err(anyhow::anyhow!("chunk read failed: {error:#}")),
        }
    }
    async fn fetch(
        self: &Arc<Self>,
        c: &Chunk,
        context: Option<&ReadContext>,
        verify: bool,
    ) -> Result<(Bytes, Option<Arc<LoadedPack>>, usize)> {
        if let Some((raw, retained)) = self.cached_read(c, verify).await? {
            if let Some(context) = context {
                context.touch(c).await;
            }
            return Ok((raw, None, retained));
        }
        match self
            .read_source(c, context.is_some_and(|context| context.observe))
            .await?
        {
            Source::Chunk(physical, _pin) => {
                self.ensure_key_loaded(physical.block_key_id).await?;
                let encoded = self.read_backend(&physical).await?;
                let retained = if physical.compressed {
                    physical.raw_size as usize
                } else {
                    encoded.capacity()
                };
                let (raw, cache) = self.decode_read(&physical, encoded, verify).await?;
                if verify {
                    if physical.compressed == c.compressed && physical.stored_size == c.stored_size
                    {
                        let (_, _, threshold) = self.compression_for(c.storage_id)?;
                        self.fill_read(c, cache, Some(codec::cache_compressed(c, threshold)));
                    } else {
                        self.fill_read(c, raw.clone(), None);
                    }
                }
                Ok((raw, None, retained))
            }
            Source::Pack(pack, _pin) => {
                if let Some(context) = context {
                    let mut current = context.pack.lock().await;
                    if let Some(loaded) = current
                        .as_ref()
                        .filter(|p| p.pack.id == pack.id && (!verify || p.verified))
                    {
                        loaded.touch(c, context.observe, context.range);
                        return Ok((
                            loaded.data.chunk(c)?,
                            Some(loaded.clone()),
                            c.raw_size as usize,
                        ));
                    }
                    *current = None;
                    let loaded = self.load_pack_read(pack, verify).await?;
                    loaded.touch(c, context.observe, context.range);
                    let raw = loaded.data.chunk(c)?;
                    *current = Some(loaded.clone());
                    Ok((raw, Some(loaded), c.raw_size as usize))
                } else {
                    let loaded = self.load_pack_read(pack, verify).await?;
                    loaded.touch(c, false, false);
                    Ok((loaded.data.chunk(c)?, Some(loaded), c.raw_size as usize))
                }
            }
        }
    }
    pub(crate) fn physical_pin(&self, id: Uuid) -> CachePin {
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
        self.source_reads.prune();
        self.physical_pins
            .lock()
            .unwrap()
            .get(&id)
            .and_then(std::sync::Weak::upgrade)
            .is_some_and(|p| p.load(Ordering::Relaxed) > 0)
    }
    async fn source(&self, id: Id, inspection: bool) -> Result<Source> {
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
            crate::model::PhysicalSource::Raw(_) => anyhow::bail!("chunk resolved to a raw source"),
        }
    }
    pub async fn pack_memory(&self, size: usize) -> Result<tokio::sync::OwnedSemaphorePermit> {
        let workspace = self.compression_workspace;
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
        let units = (size as u64 * 4 + self.compression_workspace).div_ceil(1024 * 1024);
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
        mut p: crate::content::pack::Pack,
        members: Vec<(Chunk, Bytes)>,
        strategy: crate::content::compression::Strategy,
        should_try: bool,
        memory: &Arc<tokio::sync::OwnedSemaphorePermit>,
        accepted: Option<&crate::content::compression::Config>,
    ) -> Result<(crate::content::pack::Pack, Vec<u8>)> {
        let (pool, compression_enabled, _) = self.encoding_compression(p.storage_id, accepted)?;
        p.nonce = self
            .encoding_nonce(
                &p.algorithm,
                p.block_key_id,
                p.created_at,
                crate::content::nonce::PACK,
            )
            .await?;
        let memory = memory.clone();
        let permit = self.cpu.clone().acquire_owned().await?;
        let secrets = self.secrets.clone();
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
                None,
            )
        })
        .await?
    }
    pub(crate) async fn encoding_nonce(
        &self,
        algorithm: &str,
        key: Option<Uuid>,
        created_at: chrono::DateTime<chrono::Utc>,
        type_tag: u8,
    ) -> Result<Option<Vec<u8>>> {
        ensure!(
            (algorithm != "none") == key.is_some(),
            "invalid encryption key metadata"
        );
        let Some(key) = key else { return Ok(None) };
        let id = self
            .nonces
            .next(key, type_tag, |units| {
                self.managed_keys.reserve_nonce(key, type_tag, units)
            })
            .await?;
        Ok(Some(codec::typed_nonce(created_at, id, type_tag)?.to_vec()))
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
        let _space = self.local_space(&target, size)?;
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
            !self.restart_required.load(Ordering::Acquire),
            "restart required before storage writes resume"
        );
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
        let Some(id) = id else {
            return Ok(());
        };
        if self.secrets.key_for_id(Some(id)).is_some() {
            return Ok(());
        }
        let name = match self.secrets.key_name(Some(id)) {
            Some(name) => name,
            None => {
                self.managed_keys
                    .active_block(id)
                    .await?
                    .context("block key was not found")?
                    .0
            }
        };
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
        self.load_pack_read(p, true).await
    }
    async fn load_pack_read(
        self: &Arc<Self>,
        p: crate::content::pack::Pack,
        verify: bool,
    ) -> Result<Arc<LoadedPack>> {
        if let Some(live) = self
            .pack_live
            .lock()
            .unwrap()
            .get(&(p.id, verify))
            .and_then(std::sync::Weak::upgrade)
        {
            return Ok(live);
        }
        let lock = {
            let mut map = self.pack_misses.lock().unwrap();
            map.retain(|_, v| v.strong_count() > 0);
            map.get(&(p.id, verify))
                .and_then(std::sync::Weak::upgrade)
                .unwrap_or_else(|| {
                    let cell = Arc::new(PackFetch {
                        result: tokio::sync::OnceCell::new(),
                        priority: Arc::new(AtomicUsize::new(
                            crate::storage::resource_gate::MAINTENANCE,
                        )),
                    });
                    map.insert((p.id, verify), Arc::downgrade(&cell));
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
                    let (pool, _, _) = self.compression_for(p.storage_id)?;
                    let pack = p.clone();
                    let diagnostics = self.diagnostics.clone();
                    let (data, memory) = tokio::task::spawn_blocking(move || {
                        let _permit = permit;
                        #[cfg(feature = "fault-injection")]
                        crate::faults::blocking("pack-decode");
                        let data = crate::content::pack::decode_read(
                            &pack, encoded, &secrets, &pool, verify,
                        )
                        .inspect_err(|e| diagnostics.content(e))?;
                        Ok::<_, anyhow::Error>((data, memory))
                    })
                    .await??;
                    let loaded = Arc::new(LoadedPack {
                        pack: p.clone(),
                        verified: verify,
                        data,
                        _memory: memory,
                        used: Mutex::new(DownloadUsage::default()),
                        access: self.access.clone(),
                    });
                    {
                        let mut live = self.pack_live.lock().unwrap();
                        live.retain(|_, v| v.strong_count() > 0);
                        live.insert((p.id, verify), Arc::downgrade(&loaded));
                    }
                    if verify && let Ok(permit) = self.fills.clone().try_acquire_owned() {
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
        let chunks: Vec<Chunk> = self.physical.pack_chunks(loaded.pack.id).await?;
        for c in chunks {
            self.fill_raw(&c, loaded.data.chunk(&c)?).await?;
        }
        Ok(())
    }
    pub async fn fill_raw(&self, c: &Chunk, raw: Bytes) -> Result<()> {
        if !self.local_cache(c.storage_id)?.enabled {
            return Ok(());
        }
        let (data, compressed) = self.cache_payload(c, raw).await?;
        let policy = self.local_cache(c.storage_id)?;
        self.fill_owned(cache::cache_id(c), &data, compressed, policy)
            .await
    }
    pub async fn cache_payload(&self, c: &Chunk, raw: Bytes) -> Result<(Bytes, bool)> {
        let policy = self.local_cache(c.storage_id)?;
        if !policy.enabled {
            return Ok((raw, false));
        }
        let (pool, enabled, threshold) = self.compression_for(c.storage_id)?;
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
            if mode == crate::worker::integrity::Mode::Metadata {
                return Ok(0);
            }
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
        if let crate::model::PhysicalSource::Raw(raw) = &snapshot.physical {
            let mut raw = raw.clone();
            raw.quarantine_code = None;
            let _reader = self.raw_reader_uncached(&raw, true).await?;
            return Ok(raw.size as u64);
        }
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
                crate::model::PhysicalSource::Raw(_) => unreachable!(),
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
                                    member.id == c.uuid
                                        && member.hash.as_slice() == c.hash
                                        && member.len == c.raw_size as usize
                                        && member.start
                                            == 12
                                                + crate::content::pack::MEMBER_SIZE * members.len()
                                                + *offset as usize,
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
                assert!(e.queue == queue && e.size == 100 && e.file == CacheFile::Chunk(true));
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
