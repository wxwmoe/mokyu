use super::{model::Options as StoreOptionsV2, permits::StoreGate};
use object_store::ObjectStore;
use std::sync::Arc;

pub(crate) struct StoreBackend {
    pub profile: Arc<crate::storage::profile::StorageProfile>,
    pub(crate) backend: Arc<dyn ObjectStore>,
    pub(crate) multipart: Option<Arc<dyn object_store::multipart::MultipartStore>>,
    pub(crate) streaming: Option<super::s3::Streaming>,
    pub(crate) raw_root: Option<std::path::PathBuf>,
    pub(crate) cdc_enabled: bool,
    pub(crate) deduplication_enabled: bool,
    pub(crate) profile_revision: i64,
    pub(crate) is_local: bool,
    pub(crate) prefix: String,
    pub(crate) revision: i64,
    pub(crate) identity_marker: serde_json::Value,
    pub(crate) identity_ready: std::sync::atomic::AtomicBool,
    pub(crate) identity_check: tokio::sync::Mutex<()>,
    pub(crate) reads: Arc<StoreGate>,
    pub(crate) writes: Arc<StoreGate>,
    pub(crate) controls: Arc<StoreGate>,
    pub(crate) pack: Arc<StoreGate>,
    pub(crate) integrity: Arc<StoreGate>,
    pub(crate) maintenance: Arc<StoreGate>,
    pub(crate) migration: Arc<StoreGate>,
    pub(crate) compression: Arc<crate::content::compression::Pool>,
    pub(crate) compression_enabled: bool,
    pub(crate) min_compression_savings_percent: u8,
    pub(crate) cache_enabled: bool,
    pub(crate) cache_capacity: Option<u64>,
    pub(crate) cache_entries: usize,
    pub(crate) upload_cache: bool,
    pub(crate) upload_cache_size: Option<u64>,
    pub(crate) pack_config: Arc<crate::content::pack::Config>,
}

impl StoreBackend {
    pub(crate) fn reuse_gates(&mut self, registry: &mut super::permits::GateRegistry) {
        registry.retain(|_, gate| gate.strong_count() > 0);
        for gate in [
            &mut self.reads,
            &mut self.writes,
            &mut self.controls,
            &mut self.pack,
            &mut self.integrity,
            &mut self.maintenance,
            &mut self.migration,
        ] {
            *gate = gate.reuse(registry);
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct CachePolicy {
    pub(crate) enabled: bool,
    pub(crate) capacity: Option<u64>,
    pub(crate) max_entries: usize,
    pub(crate) upload_cache: bool,
    pub(crate) upload_capacity: Option<u64>,
}

pub(crate) type StoreOptions = StoreOptionsV2;
