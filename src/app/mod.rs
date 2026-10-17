use crate::{
    authorization::{Action, Permit, Principal},
    config::{self, Budget, Config, Secrets},
    content::codec::{Chunk, MAX},
    storage::Storage,
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use s3s::{dto::StreamingBlob, s3_error};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock, Weak},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

pub(crate) mod buckets;
pub(crate) mod chunks;
pub(crate) mod compression;
pub(crate) mod extents;
pub(crate) mod listing;
pub(crate) mod publish;
pub(crate) mod quota;
pub(crate) mod responses;
pub(crate) mod streams;

#[derive(Clone, Serialize)]
pub struct Bucket {
    pub id: Uuid,
    pub project_id: Uuid,
    pub name: String,
    pub storage_id: Option<Uuid>,
    pub state: String,
    pub settings_revision: i64,
    pub uploads_paused: bool,
    pub public_base_url: String,
    pub cors: Value,
    pub website_enabled: bool,
    pub index_document: String,
    pub error_document: String,
    pub created_at: DateTime<Utc>,
}
#[derive(Clone, Serialize)]
pub struct StoredStream {
    pub id: Uuid,
    pub bucket_id: Uuid,
    pub object_key: String,
    pub stream_type: String,
    pub state: String,
    pub size: i64,
    pub etag: String,
    pub metadata: Value,
    pub public_read: bool,
    pub checksums: Value,
    #[serde(skip)]
    pub http_compression: Value,
    pub created_at: DateTime<Utc>,
    pub touched_at: DateTime<Utc>,
}
#[derive(Clone, Serialize)]
pub struct Extent {
    pub stream_id: Uuid,
    pub offset_bytes: i64,
    pub length: i32,
    pub chunk_id: Option<i64>,
    pub fragment_id: Option<Uuid>,
    pub source_offset: i32,
}
impl Extent {
    fn slice(&self, data: Bytes) -> Result<Bytes> {
        let start = self.source_offset as usize;
        let end = start
            .checked_add(self.length as usize)
            .context("extent overflow")?;
        ensure!(end <= data.len(), "extent exceeds source");
        Ok(data.slice(start..end))
    }
}
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Metadata {
    pub content_type: Option<String>,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,
    pub content_language: Option<String>,
    pub expires: Option<String>,
    pub user: Option<s3s::dto::Metadata>,
}
pub struct Active {
    id: Uuid,
    registry: Arc<Mutex<HashMap<Uuid, usize>>>,
    wake: Arc<tokio::sync::Notify>,
    read_lease: Option<ReadLease>,
}
struct ReadLease {
    id: Uuid,
    store: Arc<dyn crate::model::cluster::ClusterStore>,
    ttl_seconds: i64,
    renewed_at: tokio::sync::Mutex<tokio::time::Instant>,
}
impl ReadLease {
    async fn renew(&self) -> Result<()> {
        let mut renewed_at = self.renewed_at.lock().await;
        let started = tokio::time::Instant::now();
        if started.duration_since(*renewed_at).as_secs() < (self.ttl_seconds / 3) as u64 {
            return Ok(());
        }
        self.store.renew_read(self.id, self.ttl_seconds).await?;
        // Count database latency against the next renewal interval.
        *renewed_at = started;
        Ok(())
    }
}
impl Drop for ReadLease {
    fn drop(&mut self) {
        let id = self.id;
        let store = self.store.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let _ = store.release_read(id).await;
            });
        }
    }
}
impl Drop for Active {
    fn drop(&mut self) {
        let mut registry = self.registry.lock().unwrap();
        if let Some(n) = registry.get_mut(&self.id) {
            *n -= 1;
            if *n == 0 {
                registry.remove(&self.id);
                self.wake.notify_one();
            }
        }
    }
}
impl Active {
    pub(crate) async fn renew_read_lease(&self) -> Result<()> {
        if let Some(lease) = &self.read_lease {
            lease.renew().await?;
        }
        Ok(())
    }
}
pub struct App {
    cluster: Option<Arc<dyn crate::model::cluster::ClusterStore>>,
    pub(crate) metrics: Arc<dyn crate::model::metrics::MetricsStore>,
    pub(crate) integrity_store: Arc<dyn crate::model::integrity::IntegrityStore>,
    pub(crate) maintenance_store: Arc<dyn crate::model::maintenance::MaintenanceStore>,
    pub(crate) physical: Arc<dyn crate::model::PhysicalStore>,
    pub(crate) packing: Arc<dyn crate::model::PackingStore>,
    pub role: String,
    pub recovery_required: std::sync::atomic::AtomicBool,
    pub config: Config,
    pub runtime_defaults: HashMap<String, Value>,
    pub runtime_revisions: RwLock<HashMap<String, i64>>,
    pub runtime_settings: Arc<dyn crate::model::RuntimeSettingsStore>,
    pub managed_keys: Arc<dyn crate::model::ManagedKeyStore>,
    pub storage_records: Arc<dyn crate::model::StorageRecordStore>,
    pub chunks: Arc<dyn crate::model::ChunkStore>,
    pub budget: Budget,
    pub secrets: Arc<Secrets>,
    pub runtime: Arc<dyn crate::model::runtime::RuntimeStore>,
    pub quota: Arc<dyn crate::model::QuotaStore>,
    pub objects: Arc<dyn crate::model::ObjectStore>,
    pub publish: Arc<dyn crate::model::PublishStore>,
    pub task_control: Arc<dyn crate::model::TaskControlStore>,
    pub authorization: Arc<dyn crate::model::AuthorizationStore>,
    pub audit: Arc<dyn crate::model::AuditStore>,
    pub administration: Arc<dyn crate::model::AdministrationStore>,
    pub upload_cache: Arc<dyn crate::model::UploadCacheStore>,
    pub multipart: Arc<dyn crate::model::MultipartStore>,
    pub buckets: Arc<dyn crate::model::BucketStore>,
    pub credentials: Arc<dyn crate::model::CredentialStore>,
    pub accounts: Arc<dyn crate::model::AccountStore>,
    pub task_execution: Arc<dyn crate::model::TaskExecutionStore>,
    pub storage_profiles: Arc<dyn crate::model::StorageProfileStore>,
    pub stream_store: Arc<dyn crate::model::StreamStore>,
    pub node: std::sync::OnceLock<crate::cluster::NodeState>,
    pub storage: Arc<Storage>,
    pub thumbnails: Arc<crate::control::media::Thumbnails>,
    pub statistics: crate::status::stats::Statistics,
    pub compression_observations: Arc<compression::Observations>,
    pub response_cache: Arc<responses::Cache>,
    pub cleanup_state: crate::worker::lifecycle::Cleanup,
    pub integrity_pacing: std::sync::Mutex<std::time::Instant>,
    pub local_cleanup: tokio::sync::Mutex<()>,
    pub coord: tokio::sync::Mutex<()>,
    pub active: Arc<Mutex<HashMap<Uuid, usize>>>,
    pub uploads: Arc<Semaphore>,
    pub reads: Arc<Semaphore>,
    pub slots: Arc<Semaphore>,
    pub maintenance: Arc<std::sync::atomic::AtomicBool>,
    pub pack_creation_paused: std::sync::atomic::AtomicBool,
    pub gc_running: std::sync::atomic::AtomicBool,
    started_mode: String,
    restart_required: std::sync::atomic::AtomicBool,
    pub wake_gc: Arc<tokio::sync::Notify>,
    pub wake_tasks: Arc<tokio::sync::Notify>,
    hashes: Mutex<HashMap<(Uuid, [u8; 32]), Weak<tokio::sync::Mutex<()>>>>,
    pub operation_locks: Mutex<HashMap<Uuid, Weak<tokio::sync::Mutex<()>>>>,
    _file_lock: std::fs::File,
}
impl App {
    pub async fn new(
        mut config: Config,
        mut secrets: Secrets,
        budget: Budget,
        start_maintenance: bool,
        use_storage: bool,
        role: &str,
    ) -> Result<(Arc<Self>, Box<dyn crate::model::runtime::Owner>)> {
        tokio::fs::create_dir_all(&config.instance.data_path).await?;
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(config.instance.data_path.join("gateway.lock"))?;
        fs2::FileExt::try_lock_exclusive(&lock)
            .context("another gateway owns this data directory")?;
        let (stores, owner) = crate::model::connect(&config, &mut secrets, &budget, role).await?;
        let defaults = crate::model::runtime_defaults(&config);
        stores.runtime_settings.ensure(&defaults).await?;
        let runtime_defaults = defaults
            .into_iter()
            .map(|(section, value)| (section.to_owned(), value))
            .collect();
        let runtime_revisions =
            crate::model::load_runtime_settings(stores.runtime_settings.as_ref(), &mut config)
                .await?;
        let mut budget = config.budget()?;
        if !matches!(role, "single" | "data") {
            budget.pack_memory_units = budget
                .pack_memory_units
                .saturating_add((budget.response_cache_bytes / 2 / (1024 * 1024)) as u32);
            budget.response_cache_bytes = 0;
        }
        let state = stores.runtime.initialize(start_maintenance).await?;
        let maintenance = Arc::new(std::sync::atomic::AtomicBool::new(state.maintenance));
        let pack_creation_paused = state.pack_creation_paused;
        let secrets = Arc::new(secrets);
        let storage = Arc::new(
            Storage::new(
                &config,
                secrets.clone(),
                &budget,
                stores.physical.clone(),
                stores.cluster.clone(),
                stores.storage_profiles.clone(),
                stores.managed_keys.clone(),
                stores.storage_records.clone(),
                maintenance.clone(),
                role != "single",
                use_storage,
            )
            .await?,
        );
        if storage.enabled() {
            if let Err(error) = storage.check_identity().await {
                let pending = stores.runtime.pending_uploads().await?;
                if !pending || error.downcast_ref::<object_store::Error>().is_none() {
                    return Err(error);
                }
                tracing::warn!(error=%error,"backend identity unavailable; serving durable local uploads while remote operations wait for verification");
            }
        } else {
            tracing::info!("no store configured; configure a store before accepting objects");
        }
        let thumbnails =
            Arc::new(crate::control::media::Thumbnails::new(&config, storage.disk.clone()).await?);
        let (compression_observations, compression_receiver) = compression::Observations::new();
        let response_cache = responses::Cache::new(budget.response_cache_bytes);
        let app = Arc::new(Self {
            cluster: stores.cluster,
            role: role.to_owned(),
            recovery_required: std::sync::atomic::AtomicBool::new(false),
            uploads: Arc::new(Semaphore::new(budget.upload_concurrency)),
            reads: Arc::new(Semaphore::new(budget.read_concurrency)),
            slots: Arc::new(Semaphore::new(budget.data_slots)),
            config,
            runtime_defaults,
            runtime_revisions: RwLock::new(runtime_revisions),
            runtime_settings: stores.runtime_settings,
            metrics: stores.metrics,
            managed_keys: stores.managed_keys,
            storage_records: stores.storage_records,
            chunks: stores.chunks,
            budget,
            secrets,
            runtime: stores.runtime,
            quota: stores.quota,
            objects: stores.objects,
            publish: stores.publish,
            task_control: stores.task_control,
            authorization: stores.authorization,
            audit: stores.audit,
            accounts: stores.accounts,
            physical: stores.physical,
            packing: stores.packing,
            maintenance_store: stores.maintenance,
            integrity_store: stores.integrity,
            upload_cache: stores.upload_cache,
            multipart: stores.multipart,
            buckets: stores.buckets,
            credentials: stores.credentials,
            administration: stores.administration,
            task_execution: stores.task_execution,
            storage_profiles: stores.storage_profiles,
            stream_store: stores.streams,
            node: std::sync::OnceLock::new(),
            storage,
            thumbnails,
            statistics: crate::status::stats::Statistics::default(),
            compression_observations,
            response_cache,
            cleanup_state: crate::worker::lifecycle::Cleanup::default(),
            integrity_pacing: std::sync::Mutex::new(std::time::Instant::now()),
            local_cleanup: tokio::sync::Mutex::new(()),
            coord: tokio::sync::Mutex::new(()),
            active: Arc::new(Mutex::new(HashMap::new())),
            maintenance,
            pack_creation_paused: pack_creation_paused.into(),
            gc_running: false.into(),
            started_mode: state.mode,
            restart_required: false.into(),
            wake_gc: Arc::new(tokio::sync::Notify::new()),
            wake_tasks: Arc::new(tokio::sync::Notify::new()),
            hashes: Mutex::new(HashMap::new()),
            operation_locks: Mutex::new(HashMap::new()),
            _file_lock: lock,
        });
        compression::Observations::start(Arc::downgrade(&app), compression_receiver);
        if app.role != "single" {
            let cluster = app
                .cluster
                .clone()
                .context("cluster backend is unavailable")?;
            let profile_app = app.clone();
            tokio::spawn(async move {
                loop {
                    let result: Result<()> = async {
                        let mut listener = cluster.profile_changes().await?;
                        profile_app.storage.refresh_storages().await?;
                        loop {
                            let storage_id = listener.next().await?;
                            profile_app.storage.refresh_store(storage_id).await?;
                        }
                    }
                    .await;
                    if let Err(error) = result {
                        tracing::warn!(%error, "storage profile listener will retry");
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                }
            });
        }
        Ok((app, owner))
    }
    pub fn pin(&self, id: Uuid) -> Active {
        self.pin_with_lease(id, None)
    }
    fn pin_with_lease(&self, id: Uuid, read_lease: Option<ReadLease>) -> Active {
        *self.active.lock().unwrap().entry(id).or_default() += 1;
        Active {
            id,
            registry: self.active.clone(),
            wake: self.wake_gc.clone(),
            read_lease,
        }
    }
    pub fn is_active(&self, id: Uuid) -> bool {
        self.active.lock().unwrap().contains_key(&id)
    }
    pub fn writable(&self) -> Result<()> {
        self.ensure_mode()?;
        if self.maintenance.load(std::sync::atomic::Ordering::Acquire) {
            return Err(s3_error!(ServiceUnavailable, "gateway is in maintenance mode").into());
        }
        Ok(())
    }
    pub(crate) fn ensure_mode(&self) -> Result<()> {
        let expected = if self.role == "single" {
            "single"
        } else {
            "cluster"
        };
        if self.started_mode != expected
            || self
                .restart_required
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(s3_error!(
                ServiceUnavailable,
                "restart in the configured deployment mode"
            )
            .into());
        }
        Ok(())
    }
}

impl App {
    pub(crate) fn cluster(&self) -> Result<&dyn crate::model::cluster::ClusterStore> {
        self.cluster.as_deref().ok_or_else(|| {
            s3_error!(NotImplemented, "cluster operations require PostgreSQL").into()
        })
    }
    pub(crate) async fn change_mode(&self, principal: &Principal, mode: &str) -> Result<()> {
        use std::sync::atomic::Ordering;
        ensure!(
            matches!(mode, "single" | "cluster"),
            "mode must be single or cluster"
        );
        let _coord = self.coord.lock().await;
        let cluster = self.cluster()?;
        let current = cluster.mode().await?;
        if current == mode {
            return Ok(());
        }
        ensure!(
            !self.restart_required.load(Ordering::Acquire),
            "restart before changing deployment mode again"
        );
        ensure!(
            self.maintenance.load(Ordering::Acquire),
            "enable maintenance before changing deployment mode"
        );
        ensure!(
            self.active.lock().unwrap().is_empty(),
            "wait for active reads and writes to drain"
        );
        ensure!(
            !self.gc_running.load(Ordering::Acquire),
            "wait for the current GC batch"
        );
        let _cleanup = self
            .cleanup_state
            .running
            .try_lock()
            .context("wait for history cleanup")?;
        let _local = self
            .local_cleanup
            .try_lock()
            .context("wait for local cleanup")?;
        // Cancellation or an uncertain commit must leave this process restricted.
        self.restart_required.store(true, Ordering::Release);
        let result = cluster.set_mode(principal, mode).await;
        #[cfg(feature = "fault-injection")]
        if result.is_ok() {
            crate::faults::point("mode-committed").await;
        }
        if result.is_err() && matches!(cluster.mode().await, Ok(observed) if observed == current) {
            self.restart_required.store(false, Ordering::Release);
        }
        result
    }
    pub async fn status(&self) -> Result<Value> {
        let state = self.runtime.state().await?;
        let (pool_size, pool_idle) = self.runtime.pool();
        let expected = if self.role == "single" {
            "single"
        } else {
            "cluster"
        };
        let mismatch = state.mode != expected
            || self
                .restart_required
                .load(std::sync::atomic::Ordering::Acquire);
        let mut status = json!({"version":env!("CARGO_PKG_VERSION"),"role":self.role,"deployment_mode":state.mode,"deployment_mode_revision":state.mode_revision,"mode_mismatch":mismatch,"node_registered":self.node.get().is_some(),"resources":self.budget,"local_bytes":self.storage.disk.used(),"gc_paused":state.gc_paused,"maintenance":state.maintenance,"backend_gets":self.storage.backend_gets.load(std::sync::atomic::Ordering::Relaxed),"cache_hits":self.storage.cache_hits.load(std::sync::atomic::Ordering::Relaxed),"backend_puts":self.storage.backend_puts.load(std::sync::atomic::Ordering::Relaxed),"backend_deletes":self.storage.backend_deletes.load(std::sync::atomic::Ordering::Relaxed),"backend_read_bytes":self.storage.backend_read_bytes.load(std::sync::atomic::Ordering::Relaxed),"backend_write_bytes":self.storage.backend_write_bytes.load(std::sync::atomic::Ordering::Relaxed),"cache_hit_bytes":self.storage.cache_hit_bytes.load(std::sync::atomic::Ordering::Relaxed),"db_pool_size":pool_size,"db_pool_idle":pool_idle,"data_slots_available":self.slots.available_permits(),"active_streams":self.active.lock().unwrap().len()});
        status["runtime"] = self.statistics.runtime();
        status["cache_jobs_active"] = json!(self.storage.cache_jobs_active());
        status["http_compression"] = self.compression_observations.snapshot();
        status["response_cache"] = self.response_cache.snapshot();
        status["cleanup"] = self.cleanup_status();
        status["storage"] = self.statistics.inventory(crate::config::seconds(
            &self.config.metrics.refresh_interval,
        )?);
        status["io"] = self.storage.statistics();
        status["upload_cache"] = self.upload_cache_status().await?;
        status["process_memory"] = crate::status::stats::process_memory().await;
        status["gc_running"] = json!(self.gc_running.load(std::sync::atomic::Ordering::Relaxed));
        status["upload_slots_available"] = json!(self.uploads.available_permits());
        status["read_slots_available"] = json!(self.reads.available_permits());
        Ok(status)
    }

    pub async fn mode_mismatch(&self) -> Result<bool> {
        Ok(self.ensure_mode().is_err())
    }

    pub fn recovery_required(&self) -> bool {
        self.recovery_required
            .load(std::sync::atomic::Ordering::Acquire)
    }
}
pub fn internal(error: anyhow::Error) -> s3s::S3Error {
    if let Some(error) = crate::app::quota::database_error(&error) {
        return error;
    }
    match error.downcast::<s3s::S3Error>() {
        Ok(e) => e,
        Err(e) => {
            tracing::error!(error=%e,"request failed");
            s3_error!(InternalError, "request could not be completed")
        }
    }
}
