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
use sqlx::{PgPool, postgres::PgListener};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock, Weak},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

pub(crate) mod buckets;
pub(crate) mod chunks;
pub(crate) mod extents;
pub(crate) mod listing;
pub(crate) mod publish;
pub(crate) mod quota;
pub(crate) mod streams;

#[derive(Clone, sqlx::FromRow, Serialize)]
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
#[derive(Clone, sqlx::FromRow, Serialize)]
pub struct StoredStream {
    pub id: Uuid,
    pub bucket_id: Uuid,
    pub object_key: String,
    #[sqlx(rename = "stream_type")]
    pub stream_type: String,
    pub state: String,
    pub size: i64,
    pub etag: String,
    pub metadata: Value,
    pub public_read: bool,
    pub checksums: Value,
    pub created_at: DateTime<Utc>,
    pub touched_at: DateTime<Utc>,
}
#[derive(Clone, sqlx::FromRow, Serialize)]
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
#[derive(sqlx::FromRow)]
struct ReadExtent {
    #[sqlx(flatten)]
    extent: Extent,
    #[sqlx(flatten)]
    chunk: Chunk,
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
    db: PgPool,
    ttl_seconds: i64,
}
impl ReadLease {
    async fn renew(&self, db: &PgPool) -> Result<()> {
        let changed = sqlx::query(
            "UPDATE stream_leases SET expires_at=now()+$2*interval '1 second' WHERE id=$1 AND lease_type='read' AND expires_at>now()",
        )
        .bind(self.id)
        .bind(self.ttl_seconds)
        .execute(db)
        .await?
        .rows_affected();
        ensure!(changed == 1, "read lease expired");
        Ok(())
    }
}
impl Drop for ReadLease {
    fn drop(&mut self) {
        let id = self.id;
        let db = self.db.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let _ = sqlx::query("DELETE FROM stream_leases WHERE id=$1 AND lease_type='read'")
                    .bind(id)
                    .execute(&db)
                    .await;
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
    pub(crate) async fn renew_read_lease(&self, db: &PgPool) -> Result<()> {
        if let Some(lease) = &self.read_lease {
            lease.renew(db).await?;
        }
        Ok(())
    }
}
pub struct App {
    pub role: String,
    pub recovery_required: std::sync::atomic::AtomicBool,
    pub config: Config,
    pub runtime_defaults: HashMap<String, Value>,
    pub runtime_revisions: RwLock<HashMap<String, i64>>,
    pub budget: Budget,
    pub secrets: Arc<Secrets>,
    pub db: PgPool,
    pub node: std::sync::OnceLock<crate::cluster::NodeState>,
    pub storage: Arc<Storage>,
    pub thumbnails: Arc<crate::control::media::Thumbnails>,
    pub statistics: crate::status::stats::Statistics,
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
    ) -> Result<(Arc<Self>, sqlx::PgConnection)> {
        tokio::fs::create_dir_all(&config.instance.data_path).await?;
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(config.instance.data_path.join("gateway.lock"))?;
        fs2::FileExt::try_lock_exclusive(&lock)
            .context("another gateway owns this data directory")?;
        let (db, owner) = crate::database::connect(&config, &mut secrets, &budget, role).await?;
        let runtime_defaults = crate::database::runtime_defaults(&config)
            .into_iter()
            .map(|(section, value)| (section.to_owned(), value))
            .collect();
        crate::database::ensure_runtime_settings(&db, &config).await?;
        let runtime_revisions = crate::database::load_runtime_settings(&db, &mut config).await?;
        let budget = config.budget()?;
        if start_maintenance {
            sqlx::query("UPDATE mokyu_meta SET maintenance=true")
                .execute(&db)
                .await?;
        }
        let maintenance: bool = sqlx::query_scalar("SELECT maintenance FROM mokyu_meta")
            .fetch_one(&db)
            .await?;
        let maintenance = Arc::new(std::sync::atomic::AtomicBool::new(maintenance));
        let pack_creation_paused: bool =
            sqlx::query_scalar("SELECT pack_creation_paused FROM mokyu_meta")
                .fetch_one(&db)
                .await?;
        let secrets = Arc::new(secrets);
        let storage = Arc::new(
            Storage::new(
                &config,
                secrets.clone(),
                &budget,
                db.clone(),
                maintenance.clone(),
                role != "single",
                use_storage,
            )
            .await?,
        );
        if storage.enabled() {
            if let Err(error) = storage
                .check_identity(&db, maintenance.load(std::sync::atomic::Ordering::Acquire))
                .await
            {
                let pending: bool =
                    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_uploads)")
                        .fetch_one(&db)
                        .await?;
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
        let app = Arc::new(Self {
            role: role.to_owned(),
            recovery_required: std::sync::atomic::AtomicBool::new(false),
            uploads: Arc::new(Semaphore::new(budget.upload_concurrency)),
            reads: Arc::new(Semaphore::new(budget.read_concurrency)),
            slots: Arc::new(Semaphore::new(budget.data_slots)),
            config,
            runtime_defaults,
            runtime_revisions: RwLock::new(runtime_revisions),
            budget,
            secrets,
            db,
            node: std::sync::OnceLock::new(),
            storage,
            thumbnails,
            statistics: crate::status::stats::Statistics::default(),
            cleanup_state: crate::worker::lifecycle::Cleanup::default(),
            integrity_pacing: std::sync::Mutex::new(std::time::Instant::now()),
            local_cleanup: tokio::sync::Mutex::new(()),
            coord: tokio::sync::Mutex::new(()),
            active: Arc::new(Mutex::new(HashMap::new())),
            maintenance,
            pack_creation_paused: pack_creation_paused.into(),
            gc_running: false.into(),
            wake_gc: Arc::new(tokio::sync::Notify::new()),
            wake_tasks: Arc::new(tokio::sync::Notify::new()),
            hashes: Mutex::new(HashMap::new()),
            operation_locks: Mutex::new(HashMap::new()),
            _file_lock: lock,
        });
        let profile_app = app.clone();
        tokio::spawn(async move {
            let mut listener = match PgListener::connect_with(&profile_app.db).await {
                Ok(listener) => listener,
                Err(error) => {
                    tracing::warn!(%error, "storage profile listener unavailable");
                    return;
                }
            };
            if let Err(error) = listener.listen("mokyu_storage_profiles").await {
                tracing::warn!(%error, "storage profile subscription failed");
                return;
            }
            loop {
                match listener.recv().await {
                    Ok(notification) => {
                        let Ok(storage_id) = notification.payload().parse::<Uuid>() else {
                            tracing::warn!(
                                payload = notification.payload(),
                                "invalid storage profile notification"
                            );
                            continue;
                        };
                        if let Err(error) = profile_app.storage.refresh_store(storage_id).await {
                            tracing::warn!(%error, %storage_id, "storage profile refresh failed");
                        }
                    }
                    Err(error) => {
                        tracing::warn!(%error, "storage profile listener stopped");
                        break;
                    }
                }
            }
        });
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
        if self.maintenance.load(std::sync::atomic::Ordering::Acquire) {
            return Err(s3_error!(ServiceUnavailable, "gateway is in maintenance mode").into());
        }
        Ok(())
    }
}

impl App {
    pub async fn status(&self) -> Result<Value> {
        let (paused, maintenance, mode, mode_revision): (bool, bool, String, i64) = sqlx::query_as(
            "SELECT gc_paused,maintenance,deployment_mode,deployment_mode_revision FROM mokyu_meta",
        )
        .fetch_one(&self.db)
        .await?;
        let expected = if self.role == "single" {
            "single"
        } else {
            "cluster"
        };
        let mismatch = mode != expected;
        let mut status = json!({"version":env!("CARGO_PKG_VERSION"),"role":self.role,"deployment_mode":mode,"deployment_mode_revision":mode_revision,"mode_mismatch":mismatch,"node_registered":self.node.get().is_some(),"resources":self.budget,"local_bytes":self.storage.disk.used(),"gc_paused":paused,"maintenance":maintenance,"backend_gets":self.storage.backend_gets.load(std::sync::atomic::Ordering::Relaxed),"cache_hits":self.storage.cache_hits.load(std::sync::atomic::Ordering::Relaxed),"backend_puts":self.storage.backend_puts.load(std::sync::atomic::Ordering::Relaxed),"backend_deletes":self.storage.backend_deletes.load(std::sync::atomic::Ordering::Relaxed),"backend_read_bytes":self.storage.backend_read_bytes.load(std::sync::atomic::Ordering::Relaxed),"backend_write_bytes":self.storage.backend_write_bytes.load(std::sync::atomic::Ordering::Relaxed),"cache_hit_bytes":self.storage.cache_hit_bytes.load(std::sync::atomic::Ordering::Relaxed),"db_pool_size":self.db.size(),"db_pool_idle":self.db.num_idle(),"data_slots_available":self.slots.available_permits(),"active_streams":self.active.lock().unwrap().len()});
        status["runtime"] = self.statistics.runtime();
        status["cleanup"] = self.cleanup_status();
        status["catalog"] = serde_json::to_value(crate::control::catalog::progress(self).await?)?;
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
        let mode: String =
            sqlx::query_scalar("SELECT deployment_mode FROM mokyu_meta WHERE singleton")
                .fetch_one(&self.db)
                .await?;
        Ok(mode
            != if self.role == "single" {
                "single"
            } else {
                "cluster"
            })
    }

    pub fn recovery_required(&self) -> bool {
        self.recovery_required
            .load(std::sync::atomic::Ordering::Acquire)
    }
}
pub(super) fn check_write_conditions(
    current: Option<&str>,
    if_match: Option<&s3s::dto::ETagCondition>,
    if_none: Option<&s3s::dto::ETagCondition>,
) -> Result<()> {
    use s3s::dto::{ETag, ETagCondition};
    let matches = |c: &ETagCondition| match c {
        ETagCondition::Any => current.is_some(),
        ETagCondition::ETag(e) => current.is_some_and(|v| ETag::Strong(v.into()).strong_cmp(e)),
    };
    if if_match.is_some_and(|c| !matches(c)) || if_none.is_some_and(matches) {
        return Err(s3_error!(PreconditionFailed).into());
    }
    Ok(())
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
