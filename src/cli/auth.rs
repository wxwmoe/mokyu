use crate::{cluster::NodeState, config::Instance};

pub(super) struct CliCredentials {
    pub(super) state: Option<NodeState>,
    pub(super) deployment_key: Option<String>,
}

pub(super) fn credentials(config: &Instance) -> CliCredentials {
    let state_path = config.data_path.join("node.json");
    let state = std::fs::read(state_path)
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok());
    CliCredentials {
        state,
        deployment_key: config.deployment_key.clone(),
    }
}
