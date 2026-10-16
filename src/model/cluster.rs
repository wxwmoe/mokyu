use crate::{authorization::Principal, cluster::NodeState, content::codec::Key};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Serialize;
use utoipa::ToSchema;
use uuid::Uuid;

#[async_trait]
pub(crate) trait ClusterStore: Send + Sync {
    async fn claim_staging(&self, storage: Uuid, key: &str, bytes: u64, limit: u64) -> Result<()>;
    async fn release_staging(&self, storage: Uuid, key: &str) -> Result<()>;
    async fn stale_staging(&self, storage: Option<Uuid>) -> Result<Vec<(Uuid, String)>>;

    async fn claim_task(
        &self,
        input: super::TaskClaim<'_>,
        node: &NodeState,
        role: &str,
        key: &Key,
    ) -> Result<Option<Uuid>>;
    async fn recover_tasks(&self) -> Result<u64>;
    async fn renew_task(&self, id: Uuid, token: Uuid) -> Result<bool>;
    async fn start_usage(&self, node: Option<Uuid>) -> Result<serde_json::Value>;
    async fn complete_usage(&self, task: super::task::Execution) -> Result<bool>;
    async fn acquire_integrity_rate(
        &self,
        storage: Uuid,
        rates: &[(&str, f64, f64)],
    ) -> Result<bool>;
    async fn acquire_storage_permit(
        &self,
        storage: Uuid,
        direction: &str,
        limit: i32,
        holder: Uuid,
    ) -> Result<Option<Uuid>>;
    async fn renew_storage_permit(&self, token: Uuid) -> Result<bool>;
    async fn release_storage_permit(&self, token: Uuid) -> Result<()>;
    async fn current_stream(
        &self,
        bucket: Uuid,
        key: &str,
        ttl_seconds: i64,
    ) -> Result<(crate::app::StoredStream, Uuid)>;
    async fn create_stream(&self, input: super::StreamCreate, ttl_seconds: i64) -> Result<Uuid>;
    async fn renew_read(&self, id: Uuid, ttl_seconds: i64) -> Result<()>;
    async fn release_read(&self, id: Uuid) -> Result<()>;
    async fn renew_write(&self, stream: Uuid, ttl_seconds: i64) -> Result<()>;
    async fn expire_stream_leases(&self) -> Result<()>;
    async fn claim_chunk(&self, storage: Uuid, hash: &[u8; 32]) -> Result<Box<dyn Send>>;
    async fn profile_changes(&self) -> Result<Box<dyn ProfileChanges>>;
    async fn listen_tasks(&self, wake: std::sync::Arc<tokio::sync::Notify>) -> Result<()>;
    async fn mode(&self) -> Result<String>;
    async fn status(&self) -> Result<ClusterStatus>;
    async fn nodes(&self) -> Result<Vec<NodeView>>;
    async fn set_mode(&self, principal: &Principal, mode: &str) -> Result<()>;
    async fn release(&self, name: &str) -> Result<()>;
    async fn recover(&self, name: &str, role: &str) -> Result<()>;
    async fn credential(&self, id: Uuid) -> Result<Option<(String, Vec<u8>)>>;
    async fn register(
        &self,
        name: &str,
        role: &str,
        credential: String,
        key: &Key,
    ) -> Result<Option<NodeState>>;
    async fn heartbeat(&self, state: &NodeState, role: &str, key: &Key) -> Result<()>;
    async fn leader(&self, key: i64) -> Result<Option<Box<dyn Leader>>>;
}
#[async_trait]
pub(crate) trait ProfileChanges: Send {
    async fn next(&mut self) -> Result<Uuid>;
}
#[async_trait]
pub(crate) trait Leader: Send {
    async fn finish(self: Box<Self>) -> Result<()>;
}
#[derive(Serialize, ToSchema)]
pub(crate) struct ClusterStatus {
    pub(crate) mode: String,
    pub(crate) enabled_nodes: i64,
    pub(crate) local_storages: i64,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct NodeView {
    pub(crate) id: Uuid,
    pub(crate) name: String,
    pub(crate) role: String,
    pub(crate) enabled: bool,
    pub(crate) last_seen_at: Option<DateTime<Utc>>,
    pub(crate) created_at: DateTime<Utc>,
}
