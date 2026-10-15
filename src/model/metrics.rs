use crate::model::task::Execution;
use crate::{
    authorization::Principal,
    config::{Maintenance, MetricsSettings},
};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

#[derive(Default, Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScopeQuery {
    pub bucket: Option<Uuid>,
    pub project: Option<Uuid>,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub(crate) struct PhysicalUsage {
    pub(crate) indexed_bytes: String,
    pub(crate) selected_bytes: String,
    pub(crate) retained_bytes: String,
    pub(crate) gc_bytes: String,
    pub(crate) unconfirmed_bytes: String,
    pub(crate) chunk_objects: String,
    pub(crate) pack_objects: String,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub(crate) struct StorageUsage {
    pub(crate) buckets: String,
    pub(crate) objects: String,
    pub(crate) logical_bytes: String,
    pub(crate) unique_bytes: String,
    pub(crate) attributed_raw_bytes: String,
    pub(crate) encoded_bytes: Option<String>,
    pub(crate) encoded_known_bytes: String,
    pub(crate) local_savings: String,
    pub(crate) shared_savings: String,
    pub(crate) encoding_savings: Option<String>,
    pub(crate) chunks: String,
    pub(crate) packs: String,
    pub(crate) packed_bytes: String,
    pub(crate) pending_bytes: String,
    pub(crate) physical: Option<PhysicalUsage>,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub(crate) struct RuntimePoint {
    pub(crate) at: DateTime<Utc>,
    pub(crate) epoch: DateTime<Utc>,
    pub(crate) requests: String,
    pub(crate) failed: String,
    pub(crate) backend_read_bytes: String,
    pub(crate) backend_write_bytes: String,
    pub(crate) cache_hits: String,
    pub(crate) cache_lookups: String,
    pub(crate) cache_bytes: String,
    pub(crate) cache_limit: String,
    pub(crate) upload_bytes: String,
    pub(crate) upload_limit: String,
    pub(crate) multipart_bytes: String,
    pub(crate) thumbnail_bytes: String,
    pub(crate) rss_bytes: Option<String>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct CapacityPoint {
    pub(crate) at: DateTime<Utc>,
    pub(crate) usage: StorageUsage,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct WorkSummary {
    pub(crate) id: Uuid,
    pub(crate) task_type: String,
    #[serde(rename = "status")]
    pub(crate) state: String,
    pub(crate) processed: String,
    pub(crate) updated_at: DateTime<Utc>,
}

#[derive(Default)]
pub(crate) struct ChunkCounts {
    pub(crate) reads: i64,
    pub(crate) ranges: i64,
    pub(crate) bytes: i64,
    pub(crate) origins: i64,
}

#[derive(Default)]
pub(crate) struct PackCounts {
    pub(crate) downloads: i64,
    pub(crate) bytes: i64,
    pub(crate) partial: i64,
    pub(crate) partial_bytes: i64,
    pub(crate) useful: i64,
}

#[derive(Default)]
pub(crate) struct AccessBatch {
    pub(crate) chunks: HashMap<(i64, DateTime<Utc>), ChunkCounts>,
    pub(crate) packs: HashMap<(i64, DateTime<Utc>), PackCounts>,
    pub(crate) members: HashMap<(i64, i64, DateTime<Utc>), i64>,
    pub(crate) dropped: bool,
}

pub(crate) struct UsageSnapshot {
    pub(crate) global: bool,
    pub(crate) bucket_count: usize,
    pub(crate) as_of: Option<DateTime<Utc>>,
    pub(crate) usage: Option<StorageUsage>,
    pub(crate) history: Vec<CapacityPoint>,
    pub(crate) created: bool,
}
pub(crate) struct RuntimeHistory {
    pub(crate) history: Vec<RuntimePoint>,
    pub(crate) work: Vec<WorkSummary>,
    pub(crate) pending_entries: String,
    pub(crate) pending_bytes: String,
    pub(crate) oldest_pending: Option<DateTime<Utc>>,
    pub(crate) failed_uploads: String,
}
#[async_trait]
pub(crate) trait MetricsStore: Send + Sync {
    async fn inventory(
        &self,
        metrics: &MetricsSettings,
        maintenance: &Maintenance,
    ) -> Result<Value>;
    async fn storage_inventory(&self, storage: Uuid) -> Result<Value>;
    async fn start_usage(&self, node: Option<Uuid>) -> Result<Value>;
    async fn complete_usage(&self, task: Execution) -> Result<bool>;
    async fn usage(
        &self,
        principal: &Principal,
        scope: &ScopeQuery,
        limit: usize,
    ) -> Result<UsageSnapshot>;
    async fn runtime(&self, principal: &Principal) -> Result<RuntimeHistory>;
    async fn sample_runtime(&self, point: RuntimePoint, retention: f64) -> Result<()>;
    async fn flush_access(&self, pending: AccessBatch, stale: f64) -> Result<()>;
    async fn expire_access(&self, retention: f64, batch: i64) -> Result<()>;
}
