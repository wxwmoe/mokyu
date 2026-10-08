use super::Command;
use super::auth::credentials;
use anyhow::{Context, Result, ensure};
use serde_json::Value;
use std::path::Path;

pub(super) struct CliTarget {
    base: String,
    state: Option<crate::cluster::NodeState>,
    deployment_key: Option<String>,
}

pub(super) async fn rpc(target: &CliTarget, command: &Command) -> Result<Value> {
    let mut request =
        reqwest::Client::new().post(format!("{}/api/v1/cli", target.base.trim_end_matches('/')));
    if let Some(state) = &target.state {
        request = request
            .header("x-mokyu-node-id", state.node_id.to_string())
            .header("x-mokyu-node-credential", &state.credential);
    }
    if let Some(key) = &target.deployment_key {
        request = request.header("x-mokyu-deployment-key", key);
    }
    let response = request
        .json(command)
        .send()
        .await
        .context("cannot reach local control HTTP")?;
    let status = response.status();
    let reply: Value = response.json().await?;
    ensure!(
        status.is_success() && reply["ok"] == true,
        "{}",
        reply["error"].as_str().unwrap_or("CLI request failed")
    );
    Ok(reply["result"].clone())
}

pub(super) fn cli_target(path: &Path) -> Result<CliTarget> {
    let text = std::fs::read_to_string(path).context("cannot read configuration")?;
    let config: crate::config::FileConfig =
        toml::from_str(&text).map_err(|_| anyhow::anyhow!("invalid TOML configuration"))?;
    let control: std::net::SocketAddr = config
        .instance
        .listen_control
        .parse()
        .context("invalid control listener")?;
    let auth = credentials(&config);
    Ok(CliTarget {
        base: format!("http://127.0.0.1:{}", control.port()),
        state: auth.state,
        deployment_key: auth.deployment_key,
    })
}
