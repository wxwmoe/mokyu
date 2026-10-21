use anyhow::Result;
use async_trait::async_trait;

#[derive(Clone, serde::Serialize)]
pub(crate) struct DatabaseInfo {
    pub program: &'static str,
    pub version: String,
    pub schema_version: i64,
    pub observed_at: chrono::DateTime<chrono::Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub journal_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub synchronous: Option<String>,
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

#[derive(Clone, serde::Deserialize, serde::Serialize, utoipa::ToSchema)]
pub(crate) struct ConfigField {
    pub(crate) key: String,
    pub(crate) value: String,
    pub(crate) origin: String,
    pub(crate) value_mode: String,
    pub(crate) restart: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) value_state: Option<String>,
}
