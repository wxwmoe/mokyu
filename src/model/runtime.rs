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

pub(crate) fn accepts_role(mode: &str, role: &str) -> bool {
    match mode {
        "single" | "single_control" => role == "single",
        "cluster" => role == "control",
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::accepts_role;

    #[test]
    fn database_runtimes_match_the_persisted_mode() {
        for mode in ["single", "single_control", "cluster", "unknown"] {
            for role in ["single", "control", "data", "worker", "node"] {
                assert_eq!(
                    accepts_role(mode, role),
                    matches!(
                        (mode, role),
                        ("single" | "single_control", "single") | ("cluster", "control")
                    )
                );
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ModeConflict(pub(crate) &'static str);

impl std::fmt::Display for ModeConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for ModeConflict {}

pub(crate) fn check_drain(
    maintenance: bool,
    tasks: bool,
    uploads: bool,
    sources: bool,
) -> Result<()> {
    anyhow::ensure!(
        maintenance,
        ModeConflict("enable maintenance before changing deployment mode")
    );
    anyhow::ensure!(
        !tasks,
        ModeConflict("finish queued, running and paused tasks before changing deployment mode")
    );
    anyhow::ensure!(
        !uploads,
        ModeConflict(
            "finish or abort uploads and wait for reads to drain before changing deployment mode"
        )
    );
    anyhow::ensure!(
        !sources,
        ModeConflict(
            "flush pending uploads and referenced fragments before changing deployment mode"
        )
    );
    Ok(())
}

#[async_trait]
pub(crate) trait RuntimeStore: Send + Sync {
    fn database_info(&self) -> Option<&DatabaseInfo>;
    async fn initialize(&self, maintenance: bool) -> Result<RuntimeState>;
    async fn state(&self) -> Result<RuntimeState>;
    async fn set_mode(&self, principal: &crate::authorization::Principal, mode: &str)
    -> Result<()>;
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
