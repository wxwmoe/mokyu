pub(crate) mod account;
pub(crate) mod administration;
pub(crate) mod audit;
mod authorization;
mod backend;
pub(crate) mod bucket;
pub(crate) mod cache;
mod chunk;
pub(crate) mod cluster;
pub(crate) mod credential;
mod error;
pub(crate) mod integrity;
pub(crate) mod key;
pub(crate) mod maintenance;
pub(crate) mod metrics;
pub(crate) mod multipart;
pub(crate) mod node;
pub(crate) mod object;
pub(crate) mod packing;
mod physical;
mod profile;
mod publish;
pub(crate) mod quota;
pub(crate) mod raw;
pub(crate) mod recovery;
pub(crate) mod runtime;
pub(crate) mod settings;
pub(crate) mod source;
pub(crate) mod storage;
mod stream;
pub(crate) mod task;

use backend::postgres;
use std::sync::Arc;

pub(crate) use account::AccountStore;
pub(crate) use administration::AdministrationStore;
pub(crate) use audit::AuditStore;
pub(crate) use authorization::{AdminGuard, AuthorizationStore};

pub(crate) use bucket::BucketStore;
pub(crate) use cache::UploadCacheStore;
pub(crate) use chunk::Mapping;
pub(crate) use chunk::{ChunkCreate, ChunkStore, FragmentSource, ReadExtent};
pub(crate) use credential::CredentialStore;
pub(crate) use error::Error;
pub(crate) use multipart::MultipartStore;
pub(crate) use object::{ObjectChange, ObjectStore};
pub(crate) use packing::PackingStore;
pub(crate) use publish::{PublishStore, check_write_conditions};

pub(crate) fn error(source: &anyhow::Error) -> Option<Error> {
    source
        .downcast_ref::<Error>()
        .copied()
        .or_else(|| postgres::error(source))
        .or_else(|| backend::mysql::error(source))
        .or_else(|| backend::sqlite::error(source))
}
pub(crate) use key::{ManagedKeyRecord, ManagedKeyStore};
pub(crate) use physical::{PhysicalSource, PhysicalStore};
pub(crate) use profile::StorageProfileStore;
pub(crate) use quota::QuotaStore;
pub(crate) use settings::{
    RuntimeSettingsStore, apply_runtime_section, load_runtime_settings, runtime_defaults,
};
pub(crate) use storage::{StorageRecord, StorageRecordStore};
pub(crate) use stream::{
    CachedExtent, CachedStream, CompressionObservation, StreamCreate, StreamStore, StreamTail,
};
pub(crate) use task::{TaskClaim, TaskControlStore, TaskExecutionStore, TaskFinish, TaskRecord};

pub(crate) struct Stores {
    pub(crate) runtime: Arc<dyn runtime::RuntimeStore>,
    pub(crate) cluster: Option<Arc<dyn cluster::ClusterStore>>,
    pub(crate) metrics: Arc<dyn metrics::MetricsStore>,
    pub(crate) integrity: Arc<dyn integrity::IntegrityStore>,
    pub(crate) maintenance: Arc<dyn maintenance::MaintenanceStore>,
    pub(crate) packing: Arc<dyn PackingStore>,
    pub(crate) physical: Arc<dyn PhysicalStore>,
    pub(crate) upload_cache: Arc<dyn UploadCacheStore>,
    pub(crate) multipart: Arc<dyn MultipartStore>,
    pub(crate) buckets: Arc<dyn BucketStore>,
    pub(crate) credentials: Arc<dyn CredentialStore>,
    pub(crate) administration: Arc<dyn AdministrationStore>,
    pub(crate) accounts: Arc<dyn AccountStore>,
    pub(crate) authorization: Arc<dyn AuthorizationStore>,
    pub(crate) audit: Arc<dyn AuditStore>,
    pub(crate) objects: Arc<dyn ObjectStore>,
    pub(crate) publish: Arc<dyn PublishStore>,
    pub(crate) quota: Arc<dyn QuotaStore>,
    pub(crate) streams: Arc<dyn StreamStore>,
    pub(crate) raw: Arc<dyn raw::RawStore>,
    pub(crate) task_control: Arc<dyn TaskControlStore>,
    pub(crate) task_execution: Arc<dyn TaskExecutionStore>,
    pub(crate) storage_profiles: Arc<dyn StorageProfileStore>,
    pub(crate) runtime_settings: Arc<dyn RuntimeSettingsStore>,
    pub(crate) managed_keys: Arc<dyn ManagedKeyStore>,
    pub(crate) storage_records: Arc<dyn StorageRecordStore>,
    pub(crate) chunks: Arc<dyn ChunkStore>,
}

pub(crate) async fn connect(
    config: &crate::config::Config,
    secrets: &mut crate::config::Secrets,
    budget: &crate::config::Budget,
    role: &str,
) -> anyhow::Result<(Stores, Box<dyn runtime::Owner>)> {
    config.validate_role(role)?;
    match config.database.backend {
        crate::config::DatabaseType::Postgres => {
            postgres::connect(config, secrets, budget, role).await
        }
        crate::config::DatabaseType::Mysql => {
            backend::mysql::connect(config, secrets, budget, role).await
        }
        crate::config::DatabaseType::Sqlite => {
            backend::sqlite::connect(config, secrets, budget, role).await
        }
    }
}

pub(crate) use task::REQUEST_ID;
