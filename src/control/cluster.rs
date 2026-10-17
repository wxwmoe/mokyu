use super::Identity;
use crate::{app::App, http::HttpError};
use anyhow::Result;
use axum::{
    Json,
    extract::{Extension, Path, Query, State},
    http::HeaderMap,
};
use serde::Deserialize;
use std::sync::Arc;
use utoipa::ToSchema;

pub(crate) use crate::model::cluster::ClusterStatus;
use crate::model::node::{
    ClusterOverview, NodeConfig, NodeFilter, NodePage, NodeStatus, SettingCount,
};
use serde_json::{Value, json};
use uuid::Uuid;

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
    read_admin(actor, app).await
}

async fn read_admin(actor: &Identity, app: &App) -> Result<(), HttpError> {
    app.authorization
        .admin(&actor.principal)
        .await?
        .finish()
        .await?;
    Ok(())
}

pub(crate) async fn overview(app: &App) -> Result<ClusterOverview> {
    let desired = crate::model::node::desired(app).await?;
    let cluster = app.cluster()?;
    Ok(ClusterOverview {
        deployment: cluster.status().await?,
        nodes: cluster.node_counts(&desired).await?,
        settings: cluster.setting_counts(&desired).await?,
        observed_at: chrono::Utc::now(),
    })
}

#[utoipa::path(get, operation_id = "cluster_status", path = "/api/v1/cluster/status", responses((status = 200, body = ClusterOverview), (status = 501, body = crate::control::contract::ErrorBody)))]
pub(crate) async fn status(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<ClusterOverview>, HttpError> {
    read_admin(&actor, &app).await?;
    Ok(Json(overview(&app).await?))
}

#[utoipa::path(post, operation_id = "cluster_mode", path = "/api/v1/cluster/mode", request_body = ModeInput, responses((status = 200, body = ClusterOverview), (status = 501, body = crate::control::contract::ErrorBody)))]
pub(crate) async fn mode(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<ModeInput>,
) -> Result<Json<ClusterOverview>, HttpError> {
    admin(&actor, &app).await?;
    app.change_mode(&actor.principal, &input.mode).await?;
    status(State(app), Extension(actor)).await
}

#[utoipa::path(get, operation_id = "cluster_nodes", path = "/api/v1/cluster/nodes", params(NodeFilter), responses((status = 200, body = NodePage), (status = 501, body = crate::control::contract::ErrorBody)))]
pub(crate) async fn nodes(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Query(filter): Query<NodeFilter>,
) -> Result<Json<NodePage>, HttpError> {
    read_admin(&actor, &app).await?;
    filter
        .cursor()
        .map_err(|_| s3s::s3_error!(InvalidArgument))?;
    let desired = crate::model::node::desired(&app).await?;
    Ok(Json(app.cluster()?.nodes(&filter, &desired).await?))
}

#[derive(serde::Serialize, ToSchema)]
pub(crate) struct ClusterSettings {
    sections: Vec<SettingCount>,
    nodes: NodePage,
}
#[utoipa::path(get, operation_id = "cluster_settings", path = "/api/v1/cluster/settings", params(NodeFilter), responses((status=200,body=ClusterSettings)))]
pub(crate) async fn settings(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Query(filter): Query<NodeFilter>,
) -> Result<Json<ClusterSettings>, HttpError> {
    read_admin(&actor, &app).await?;
    filter
        .cursor()
        .map_err(|_| s3s::s3_error!(InvalidArgument))?;
    let desired = crate::model::node::desired(&app).await?;
    Ok(Json(ClusterSettings {
        sections: app.cluster()?.setting_counts(&desired).await?,
        nodes: app.cluster()?.nodes(&filter, &desired).await?,
    }))
}
#[utoipa::path(get, operation_id = "cluster_node_status", path = "/api/v1/cluster/nodes/{id}/status", params(("id"=Uuid,Path)), responses((status=200,body=NodeStatus)))]
pub(crate) async fn node_status(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<Json<NodeStatus>, HttpError> {
    read_admin(&actor, &app).await?;
    let desired = crate::model::node::desired(&app).await?;
    let cluster = app.cluster()?;
    let node = cluster
        .node(id, &desired)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    let report = cluster.node_report(id).await?;
    let mut tasks = cluster.node_tasks(id).await?;
    let more_tasks = tasks.len() > 20;
    tasks.truncate(20);
    Ok(Json(NodeStatus {
        node,
        boot_id: report.as_ref().map(|r| r.boot_id),
        started_at: report.as_ref().map(|r| r.started_at),
        status: report.map(|r| r.status),
        tasks,
        more_tasks,
    }))
}
#[utoipa::path(get, operation_id = "cluster_node_config", path = "/api/v1/cluster/nodes/{id}/config", params(("id"=Uuid,Path)), responses((status=200,body=NodeConfig)))]
pub(crate) async fn node_config(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<Json<NodeConfig>, HttpError> {
    read_admin(&actor, &app).await?;
    let desired = crate::model::node::desired(&app).await?;
    let cluster = app.cluster()?;
    let node = cluster
        .node(id, &desired)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    let metadata = cluster
        .node_report(id)
        .await?
        .map(|r| r.metadata)
        .unwrap_or(Value::Null);
    let fields = if metadata["format"] == crate::model::node::REPORT_FORMAT {
        serde_json::from_value(metadata["fields"].clone())?
    } else {
        Vec::new()
    };
    Ok(Json(NodeConfig {
        node,
        fields,
        revisions: metadata.get("revisions").cloned().unwrap_or(json!({})),
    }))
}

#[utoipa::path(post, operation_id = "cluster_release", path = "/api/v1/cluster/nodes/{name}/release", params(("name" = String, Path)), request_body = ReleaseInput, responses((status = 200), (status = 501, body = crate::control::contract::ErrorBody)))]
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
    responses((status = 200, body = ClusterStatus), (status = 403, body = crate::control::contract::ErrorBody), (status = 501, body = crate::control::contract::ErrorBody))
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
