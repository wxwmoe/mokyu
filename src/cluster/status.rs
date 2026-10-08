use super::registration::validate_node;
use crate::app::App;
use anyhow::Result;
use std::{sync::Arc, time::Duration};

pub async fn control_loop(app: Arc<App>) -> Result<()> {
    let Some(state) = app.node.get() else {
        return Ok(());
    };
    loop {
        let result: Result<()> = async {
            let mut tx = app.db.begin().await?;
            validate_node(&app, &mut *tx, state, "control").await?;
            sqlx::query("UPDATE nodes SET last_seen_at=now(),updated_at=now() WHERE id=$1")
                .bind(state.node_id)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
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
            let mut tx = app.db.begin().await?;
            validate_node(&app, &mut *tx, state, role).await?;
            sqlx::query("UPDATE nodes SET last_seen_at=now(),updated_at=now() WHERE id=$1")
                .bind(state.node_id)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
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
