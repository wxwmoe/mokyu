use crate::app::App;
use anyhow::Result;
use std::{sync::Arc, time::Duration};

pub async fn control_loop(app: Arc<App>) -> Result<()> {
    let Some(state) = app.node.get() else {
        return Ok(());
    };
    loop {
        let result: Result<()> = async {
            app.cluster()?
                .heartbeat(state, "control", &app.secrets.credential_key)
                .await?;
            app.storage.refresh_storages().await?;
            Ok(())
        }
        .await;
        if let Err(error) = result {
            tracing::warn!(%error, "control node heartbeat failed; retrying");
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}

pub async fn node_loop(app: Arc<App>, role: &'static str) -> Result<()> {
    let Some(state) = app.node.get() else {
        return Ok(());
    };
    loop {
        let result: Result<()> = async {
            app.cluster()?
                .heartbeat(state, role, &app.secrets.credential_key)
                .await?;
            app.storage.refresh_storages().await?;
            Ok(())
        }
        .await;
        if let Err(error) = result {
            tracing::warn!(%error, "node heartbeat failed; retrying");
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}
