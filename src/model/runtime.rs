use anyhow::Result;
use async_trait::async_trait;

#[derive(Clone, serde::Serialize)]
pub(crate) struct DatabaseInfo {
    pub program: &'static str,
    pub version: String,
    pub schema_version: i64,
    pub observed_at: chrono::DateTime<chrono::Utc>,
}

pub(crate) struct RuntimeState {
    pub(crate) gc_paused: bool,
    pub(crate) maintenance: bool,
    pub(crate) pack_creation_paused: bool,
    pub(crate) mode: String,
    pub(crate) mode_revision: i64,
}

#[async_trait]
pub(crate) trait RuntimeStore: Send + Sync {
    fn database_info(&self) -> Option<&DatabaseInfo>;
    async fn initialize(&self, maintenance: bool) -> Result<RuntimeState>;
    async fn state(&self) -> Result<RuntimeState>;
    async fn pending_uploads(&self) -> Result<bool>;
    async fn health(&self) -> Result<()>;
    fn pool(&self) -> (u32, usize);
    fn read_replica(&self) -> Option<serde_json::Value> {
        None
    }
}

#[async_trait]
pub(crate) trait Owner: Send {
    async fn check(&mut self) -> Result<()>;
}
