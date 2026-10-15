use crate::{authorization::Principal, cluster::NodeState, content::codec::Key};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Serialize;
use utoipa::ToSchema;
use uuid::Uuid;

#[async_trait]
pub(crate) trait ClusterStore: Send + Sync {
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
