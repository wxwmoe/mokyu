use super::identity::{credential, node_state_path, read_state, role_name, write_state};
use crate::app::App;
use anyhow::{Result, ensure};

pub async fn initialize(app: &App, role: &str) -> Result<()> {
    if role == "single" {
        return Ok(());
    }
    let mode = app.cluster()?.mode().await?;
    if mode != "cluster" {
        tracing::warn!(role, "service role is waiting for cluster deployment mode");
        return Ok(());
    }
    if matches!(role, "data" | "worker") {
        app.storage.shared_staging_ready().await?;
    }
    let path = node_state_path(app);
    let state = if let Some(state) = read_state(&path).await? {
        state
    } else {
        let name = role_name(app)?;
        let Some(state) = app
            .cluster()?
            .register(&name, role, credential()?, &app.secrets.credential_key)
            .await?
        else {
            app.recovery_required
                .store(true, std::sync::atomic::Ordering::Release);
            return Ok(());
        };
        write_state(&path, &state).await?;
        state
    };
    app.cluster()?
        .heartbeat(&state, role, &app.secrets.credential_key)
        .await?;
    ensure!(
        app.node.set(state).is_ok(),
        "node identity was already initialized"
    );
    Ok(())
}

pub async fn recover(app: &App) -> Result<()> {
    ensure!(
        app.role == "control",
        "only a local control node can recover"
    );
    let name = role_name(app)?;
    let role = "control";
    app.cluster()?.recover(&name, role).await?;
    let path = node_state_path(app);
    let _ = tokio::fs::remove_file(path).await;
    app.recovery_required
        .store(false, std::sync::atomic::Ordering::Release);
    initialize(app, &app.role).await
}
