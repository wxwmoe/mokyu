use crate::app::App;
use anyhow::Result;
use std::{sync::Arc, time::Duration};

pub async fn control_loop(app: Arc<App>) -> Result<()> {
    node_loop(app, "control").await
}

pub async fn node_loop(app: Arc<App>, role: &'static str) -> Result<()> {
    run(&app, role, true).await
}

pub(crate) async fn restricted_loop(app: Arc<App>) -> Result<()> {
    run(&app, &app.role, false).await
}

async fn run(app: &App, role: &str, refresh: bool) -> Result<()> {
    let Some(state) = app.node.get() else {
        return std::future::pending().await;
    };
    loop {
        let result: Result<()> = async {
            app.refresh_maintenance().await?;
            app.cluster()?
                .heartbeat(state, role, &app.secrets.credential_key)
                .await?;
            if refresh {
                app.storage.refresh_storages().await?;
            }
            Ok(())
        }
        .await;
        if let Err(error) = result {
            tracing::warn!(%error, "node heartbeat failed; retrying");
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}
