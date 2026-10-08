use crate::{cluster::NodeState, config::FileConfig};

pub(super) struct CliCredentials {
    pub(super) state: Option<NodeState>,
    pub(super) deployment_key: Option<String>,
}

pub(super) fn credentials(config: &FileConfig) -> CliCredentials {
    let state_path = config.instance.data_path.join("node.json");
    let state = std::fs::read(state_path)
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok());
    CliCredentials {
        state,
        deployment_key: config.instance.deployment_key.clone(),
    }
}
