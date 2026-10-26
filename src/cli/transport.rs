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
    #[derive(serde::Deserialize)]
    struct Local {
        #[serde(default)]
        instance: crate::config::Instance,
    }
    let local: Local =
        toml::from_str(&text).map_err(|_| anyhow::anyhow!("invalid TOML configuration"))?;
    let instance = if local.instance.control_url.is_some() {
        crate::config::node::NodeConfig::parse(&text, "data")?.instance
    } else {
        toml::from_str::<crate::config::FileConfig>(&text)
            .map_err(|_| anyhow::anyhow!("invalid TOML configuration"))?
            .instance
    };
    let control: std::net::SocketAddr = instance
        .listen_control
        .parse()
        .context("invalid control listener")?;
    let auth = credentials(&instance);
    Ok(CliTarget {
        base: format!("http://127.0.0.1:{}", control.port()),
        state: auth.state,
        deployment_key: auth.deployment_key,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_cli_uses_the_local_listener_without_database_settings() -> Result<()> {
        let directory = std::env::temp_dir().join(format!("mokyu-cli-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory)?;
        let path = directory.join("config.toml");
        let value = format!(
            "[instance]\ncontrol_url='https://control.example'\nlisten_control='0.0.0.0:19002'\ndata_path='{}'\n",
            directory.display()
        );
        std::fs::write(&path, &value)?;
        let target = cli_target(&path)?;
        assert_eq!(target.base, "http://127.0.0.1:19002");
        assert!(target.deployment_key.is_none());
        std::fs::write(&path, format!("{value}[database]\ntype='postgres'\n"))?;
        assert!(cli_target(&path).is_err());
        std::fs::remove_dir_all(directory)?;
        Ok(())
    }
}
