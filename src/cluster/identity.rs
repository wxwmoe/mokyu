use crate::app::App;
use anyhow::{Context, Result, ensure};
use base64::Engine;
use std::path::Path;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

#[derive(Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct NodeState {
    pub(crate) format: u32,
    pub(crate) deployment_id: Uuid,
    pub(crate) node_id: Uuid,
    pub(crate) role: String,
    pub(crate) credential_generation: i64,
    pub(crate) credential: String,
}

impl NodeState {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(self.format == 1, "unsupported node identity format");
        ensure!(
            !self.deployment_id.is_nil()
                && !self.node_id.is_nil()
                && self.credential_generation > 0
                && matches!(self.role.as_str(), "control" | "data" | "worker" | "node"),
            "invalid node identity"
        );
        ensure!(
            self.credential.len() == 64
                && base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(&self.credential)
                    .is_ok_and(|bytes| bytes.len() == 48),
            "invalid node credential"
        );
        Ok(())
    }
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
    ensure!(
        tokio::fs::metadata(path).await?.len() <= 4096,
        "node identity file is too large"
    );
    let bytes = tokio::fs::read(path).await?;
    let state: NodeState = serde_json::from_slice(&bytes).map_err(|_| {
        anyhow::anyhow!("invalid node identity file; administrator recovery is required")
    })?;
    state.validate()?;
    Ok(Some(state))
}

pub(crate) async fn write_state(path: &Path, state: &NodeState) -> Result<()> {
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

pub(crate) fn credential() -> Result<String> {
    let mut bytes = [0u8; 48];
    aws_lc_rs::rand::fill(&mut bytes).map_err(|_| anyhow::anyhow!("randomness unavailable"))?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[tokio::test]
    async fn node_credentials_remain_private_after_replacement() -> Result<()> {
        let root = std::env::temp_dir().join(format!("mokyu-identity-{}", Uuid::new_v4()));
        std::fs::create_dir(&root)?;
        let path = root.join("node.json");
        let state = NodeState {
            format: 1,
            deployment_id: Uuid::new_v4(),
            node_id: Uuid::new_v4(),
            role: "data".into(),
            credential_generation: 1,
            credential: credential()?,
        };
        for _ in 0..2 {
            write_state(&path, &state).await?;
            assert_eq!(
                std::fs::metadata(&path)?.permissions().mode() & 0o777,
                0o600
            );
            assert!(!path.with_extension("json.tmp").exists());
            assert_eq!(
                read_state(&path).await?.unwrap().credential,
                state.credential
            );
        }
        tokio::fs::write(
            &path,
            br#"{"node_id":"00000000-0000-0000-0000-000000000001","credential":"old"}"#,
        )
        .await?;
        assert!(read_state(&path).await.is_err());
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}
