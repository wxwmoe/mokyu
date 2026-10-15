use super::Identity;
use crate::{app::App, http::HttpError};
use anyhow::Result;
use axum::{
    Json,
    extract::{Extension, Path, State},
    http::HeaderMap,
};
use serde::Deserialize;
use std::sync::Arc;
use utoipa::ToSchema;

pub(crate) use crate::model::cluster::{ClusterStatus, NodeView};

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct ModeInput {
    #[schema(example = "cluster")]
    mode: String,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReleaseInput {
    #[serde(default)]
    confirm: bool,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecoverInput {
    #[serde(default)]
    confirm: bool,
}

async fn admin(actor: &Identity, app: &App) -> Result<(), HttpError> {
    actor.recent(app).await?;
    app.authorization
        .admin(&actor.principal)
        .await?
        .finish()
        .await?;
    Ok(())
}

#[utoipa::path(get, operation_id = "cluster_status", path = "/api/v1/cluster/status", responses((status = 200, body = ClusterStatus)))]
pub(crate) async fn status(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<ClusterStatus>, HttpError> {
    admin(&actor, &app).await?;
    Ok(Json(app.cluster()?.status().await?))
}

#[utoipa::path(post, operation_id = "cluster_mode", path = "/api/v1/cluster/mode", request_body = ModeInput, responses((status = 200, body = ClusterStatus)))]
pub(crate) async fn mode(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<ModeInput>,
) -> Result<Json<ClusterStatus>, HttpError> {
    admin(&actor, &app).await?;
    if !matches!(input.mode.as_str(), "single" | "cluster") {
        return Err(anyhow::anyhow!("mode must be single or cluster").into());
    }
    app.cluster()?
        .set_mode(&actor.principal, &input.mode)
        .await?;
    status(State(app), Extension(actor)).await
}

#[utoipa::path(get, operation_id = "cluster_nodes", path = "/api/v1/cluster/nodes", responses((status = 200, body = Vec<NodeView>)))]
pub(crate) async fn nodes(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<Vec<NodeView>>, HttpError> {
    admin(&actor, &app).await?;
    Ok(Json(app.cluster()?.nodes().await?))
}

#[utoipa::path(post, operation_id = "cluster_release", path = "/api/v1/cluster/nodes/{name}/release", params(("name" = String, Path)), request_body = ReleaseInput, responses((status = 200)))]
pub(crate) async fn release(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(name): Path<String>,
    Json(input): Json<ReleaseInput>,
) -> Result<Json<serde_json::Value>, HttpError> {
    admin(&actor, &app).await?;
    if !input.confirm {
        return Err(anyhow::anyhow!("release requires confirm=true").into());
    }
    app.cluster()?.release(&name).await?;
    Ok(Json(serde_json::json!({"released": name})))
}

#[utoipa::path(
    post,
    operation_id = "cluster_recover",
    path = "/api/v1/cluster/recover",
    request_body = RecoverInput,
    responses((status = 200, body = ClusterStatus), (status = 403, body = crate::control::contract::ErrorBody))
)]
pub(crate) async fn recover(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(input): Json<RecoverInput>,
) -> Result<Json<ClusterStatus>, HttpError> {
    if !input.confirm {
        return Err(anyhow::anyhow!("recovery requires confirm=true").into());
    }
    let key = headers
        .get("x-mokyu-deployment-key")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| crate::http::unauthorized())?;
    crate::cluster::verify_deployment_key(&app, key).map_err(HttpError::from)?;
    crate::cluster::recover(&app).await?;
    Ok(Json(app.cluster()?.status().await?))
}
