use super::{NodeContext, Phase};
use crate::http::errors::HttpError;
use axum::{Json, Router, extract::State};
use serde_json::{Value, json};
use std::sync::Arc;

impl NodeContext {
    pub(crate) fn ready(&self) -> bool {
        self.serving.load(std::sync::atomic::Ordering::Acquire)
            && self.current.borrow().phase == Phase::Ready
            && self.snapshot().is_ok()
    }

    pub(crate) async fn status(&self) -> Value {
        let state = self.current.borrow().clone();
        let runtime = self.statistics.runtime();
        json!({
            "version": env!("CARGO_PKG_VERSION"), "role": self.client.identity.role,
            "deployment_mode": "cluster", "ready": self.ready(), "phase": state.phase,
            "maintenance": state.maintenance, "draining": state.draining,
            "node_id": self.client.identity.node_id, "boot_id": self.boot,
            "runtime": runtime, "resources": self.budget.get(),
            "process_memory": crate::status::stats::process_memory().await,
            "db_pool_size": 0, "db_pool_idle": 0, "active_grants": self.resources.active(),
            "pending_uploads": state.pending_uploads,
            "configuration": state.snapshot.as_ref().map(|snapshot| snapshot.settings.view()),
            "control": {"url": self.config.instance.control_url, "peer": state.peer,
                "id": state.control_id, "epoch": state.epoch, "last_error": state.last_error}
        })
    }
}

pub(crate) fn router(node: Arc<NodeContext>) -> Router {
    Router::new()
        .route("/status", axum::routing::get(status))
        .route("/api/v1/cli", axum::routing::post(command))
        .with_state(node)
        .layer(axum::middleware::from_fn(
            crate::http::transport::router_request,
        ))
}

async fn status(State(node): State<Arc<NodeContext>>) -> Json<Value> {
    Json(node.status().await)
}

async fn command(
    State(node): State<Arc<NodeContext>>,
    Json(command): Json<crate::cli::Command>,
) -> Result<Json<Value>, HttpError> {
    let crate::cli::Command::Status { ready } = command else {
        return Err(anyhow::anyhow!(
            "only status is available on a node; send management commands to control"
        )
        .into());
    };
    if ready && !node.ready() {
        return Err(anyhow::anyhow!("node is not ready").into());
    }
    Ok(Json(json!({"ok": true, "result": node.status().await})))
}
