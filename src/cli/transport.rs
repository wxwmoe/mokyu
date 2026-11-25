use super::Command;
use super::auth::credentials;
use anyhow::{Context, Result, ensure};
use serde_json::Value;
use std::path::Path;

const DEFAULT_CONFIG: &str = "/config/config.toml";

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
        reply["detail"]
            .as_str()
            .or_else(|| reply["error"].as_str())
            .unwrap_or("CLI request failed")
    );
    Ok(reply["result"].clone())
}

pub(super) fn cli_target(explicit: Option<&Path>) -> Result<CliTarget> {
    let path = explicit.unwrap_or_else(|| Path::new(DEFAULT_CONFIG));
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if explicit.is_none() && error.kind() == std::io::ErrorKind::NotFound => {
            String::new()
        }
        Err(error) => return Err(error).context("cannot read configuration"),
    };
    #[derive(serde::Deserialize)]
    struct Local {
        #[serde(default)]
        instance: crate::config::Instance,
    }
    let local: Local =
        toml::from_str(&text).map_err(|_| anyhow::anyhow!("invalid TOML configuration"))?;
    let instance = if local.instance.control_url.is_some() {
        let role = if local.instance.storage_url.is_some() || local.instance.storage_group.is_some()
        {
            "storage"
        } else {
            "data"
        };
        crate::config::node::NodeConfig::parse(&text, role)?.instance
    } else if toml::from_str::<toml::Table>(&text)?.contains_key("database") {
        toml::from_str::<crate::config::FileConfig>(&text)
            .map_err(|_| anyhow::anyhow!("invalid TOML configuration"))?
            .instance
    } else {
        crate::runtime::testing::cli_instance(&text)?
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

pub(super) async fn recovery_status(path: Option<&Path>) -> Result<Option<Value>> {
    let Some(credential) = std::env::var_os(crate::node::storage::readonly::CREDENTIAL_ENV) else {
        return Ok(None);
    };
    let path = path.unwrap_or_else(|| Path::new(DEFAULT_CONFIG));
    let Ok(text) = std::fs::read_to_string(path) else {
        return Ok(None);
    };
    let Ok(config) = crate::config::node::NodeConfig::parse(&text, "storage-recovery") else {
        return Ok(None);
    };
    let error = match crate::storage::native::recovery::probe::status(
        &config.instance,
        Path::new(&credential),
    )
    .await
    {
        Ok(status) => return Ok(Some(status)),
        Err(error) => error,
    };
    if error
        .downcast_ref::<tonic::Status>()
        .is_some_and(|status| status.code() != tonic::Code::Unimplemented)
    {
        return Err(error);
    }
    // A registered storage may still have the recovery credential mounted.
    if config.instance.control_url.is_some()
        && let Ok(target) = cli_target(Some(path))
        && let Some(node) = &target.state
        && let Ok(status) = rpc(&target, &Command::Status { ready: false }).await
        && status["role"] == "storage"
        && status["node_id"] == node.node_id.to_string()
    {
        return Ok(None);
    }
    Err(error)
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
        let target = cli_target(Some(&path))?;
        assert_eq!(target.base, "http://127.0.0.1:19002");
        assert!(target.deployment_key.is_none());
        std::fs::write(&path, format!("{value}[database]\ntype='postgres'\n"))?;
        assert!(cli_target(Some(&path)).is_err());
        std::fs::remove_dir_all(directory)?;
        Ok(())
    }
}
