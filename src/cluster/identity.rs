use crate::app::App;
use anyhow::{Context, Result, ensure};
use base64::Engine;
use std::path::Path;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct NodeState {
    pub(crate) node_id: Uuid,
    pub(crate) credential: String,
}

pub(crate) fn credential_binding(node_id: Uuid) -> String {
    format!("node-credential-{node_id}")
}

pub(crate) fn verify_deployment_key(app: &crate::app::App, value: &str) -> Result<()> {
    let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(value.as_bytes())
        .map_err(|_| anyhow::anyhow!("invalid deployment key"))?;
    ensure!(raw.len() == 48, "invalid deployment key");
    ensure!(
        aws_lc_rs::constant_time::verify_slices_are_equal(
            blake3::hash(&raw).as_bytes(),
            &app.secrets.credential_key.material,
        )
        .is_ok(),
        "invalid deployment key"
    );
    Ok(())
}

pub(super) fn node_state_path(app: &App) -> std::path::PathBuf {
    app.config.instance.data_path.join("node.json")
}

pub(crate) async fn read_state(path: &Path) -> Result<Option<NodeState>> {
    if !tokio::fs::try_exists(path).await? {
        return Ok(None);
    }
    let bytes = tokio::fs::read(path).await?;
    match serde_json::from_slice(&bytes) {
        Ok(state) => Ok(Some(state)),
        Err(error) => {
            tracing::warn!(path=%path.display(), %error, "node identity file is invalid; checking database registration");
            Ok(None)
        }
    }
}

pub(super) async fn write_state(path: &Path, state: &NodeState) -> Result<()> {
    let temporary = path.with_extension("json.tmp");
    let _ = tokio::fs::remove_file(&temporary).await;
    let data = serde_json::to_vec(state)?;
    let mut file = tokio::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&temporary)
        .await?;
    file.write_all(&data).await?;
    file.sync_all().await?;
    drop(file);
    tokio::fs::rename(&temporary, path).await?;
    Ok(())
}

pub(super) fn role_name(app: &App) -> Result<String> {
    app.config
        .instance
        .name
        .clone()
        .context("instance.name is required for a split service role")
}

pub(super) fn credential() -> Result<String> {
    let mut bytes = [0u8; 48];
    aws_lc_rs::rand::fill(&mut bytes).map_err(|_| anyhow::anyhow!("randomness unavailable"))?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}
