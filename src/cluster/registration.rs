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
    let boot = uuid::Uuid::new_v4();
    app.cluster()?
        .begin_report(
            &state,
            role,
            &app.secrets.credential_key,
            boot,
            serde_json::from_value(app.statistics.runtime()["started_at"].clone())?,
        )
        .await?;
    ensure!(
        app.node_boot.set(boot).is_ok(),
        "node boot was already initialized"
    );
    ensure!(
        app.node.set(state).is_ok(),
        "node identity was already initialized"
    );
    app.storage
        .initialize_cache(app.node.get().unwrap().node_id)
        .await?;
    Ok(())
}

pub async fn recover(app: &App) -> Result<()> {
    let cluster = app.cluster()?;
    ensure!(
        app.role == "control",
        "only a local control node can recover"
    );
    let name = role_name(app)?;
    let role = "control";
    cluster.recover(&name, role).await?;
    let path = node_state_path(app);
    let _ = tokio::fs::remove_file(path).await;
    app.recovery_required
        .store(false, std::sync::atomic::Ordering::Release);
    initialize(app, &app.role).await
}
