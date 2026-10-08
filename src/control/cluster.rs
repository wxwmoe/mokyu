use super::Identity;
use crate::{app::App, http::HttpError};
use anyhow::Result;
use axum::{
    Json,
    extract::{Extension, Path, State},
    http::HeaderMap,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Serialize, ToSchema)]
pub(crate) struct ClusterStatus {
    mode: String,
    enabled_nodes: i64,
    local_storages: i64,
}

#[derive(Serialize, sqlx::FromRow, ToSchema)]
pub(crate) struct NodeView {
    id: Uuid,
    name: String,
    role: String,
    enabled: bool,
    last_seen_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
}

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
    let mut tx = app.db.begin().await?;
    actor.principal.lock_admin(&mut tx).await?;
    tx.commit().await?;
    Ok(())
}

#[utoipa::path(get, operation_id = "cluster_status", path = "/api/v1/cluster/status", responses((status = 200, body = ClusterStatus)))]
pub(crate) async fn status(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<ClusterStatus>, HttpError> {
    admin(&actor, &app).await?;
    Ok(Json(ClusterStatus {
        mode: sqlx::query_scalar("SELECT deployment_mode FROM mokyu_meta WHERE singleton")
            .fetch_one(&app.db)
            .await?,
        enabled_nodes: sqlx::query_scalar("SELECT count(*) FROM nodes WHERE enabled")
            .fetch_one(&app.db)
            .await?,
        local_storages: sqlx::query_scalar("SELECT count(*) FROM storages WHERE type='local'")
            .fetch_one(&app.db)
            .await?,
    }))
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
    let mut tx = app.db.begin().await?;
    let current: String =
        sqlx::query_scalar("SELECT deployment_mode FROM mokyu_meta WHERE singleton FOR UPDATE")
            .fetch_one(&mut *tx)
            .await?;
    if current != input.mode {
        if input.mode == "cluster" {
            let local: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM storages WHERE type='local')")
                    .fetch_one(&mut *tx)
                    .await?;
            if local {
                return Err(anyhow::anyhow!(
                    "local storages exist; migrate them to S3 before enabling cluster mode"
                )
                .into());
            }
            let staging: Option<Uuid> = sqlx::query_scalar(
                "SELECT NULLIF(value->>'staging_storage_id','')::uuid FROM runtime_settings WHERE section='uploads'",
            )
            .fetch_one(&mut *tx)
            .await?;
            let valid_staging: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM storages WHERE id=$1 AND enabled AND type='s3')",
            )
            .bind(staging)
            .fetch_one(&mut *tx)
            .await?;
            if !valid_staging {
                return Err(anyhow::anyhow!(
                    "cluster mode requires an enabled S3 uploads.staging_storage_id"
                )
                .into());
            }
        } else {
            let local_node = match &actor.principal {
                crate::authorization::Principal::Node { id, role } if role == "control" => {
                    Some(*id)
                }
                _ => None,
            };
            let nodes: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM nodes WHERE enabled AND ($1::uuid IS NULL OR id<>$1))",
            )
            .bind(local_node)
            .fetch_one(&mut *tx)
            .await?;
            if nodes {
                return Err(anyhow::anyhow!(
                    "release all registered nodes before returning to single mode"
                )
                .into());
            }
            let pending: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pending_uploads) OR EXISTS(SELECT 1 FROM fragments WHERE staging_storage_id IS NOT NULL)",
            )
            .fetch_one(&mut *tx)
            .await?;
            if pending {
                return Err(anyhow::anyhow!(
                    "complete or cancel active uploads before returning to single mode"
                )
                .into());
            }
        }
        if input.mode == "single"
            && let crate::authorization::Principal::Node { id, role } = &actor.principal
            && role == "control"
        {
            sqlx::query("UPDATE nodes SET enabled=false,credential_encrypted=NULL,last_seen_at=NULL,status='released',recovery_required=false,updated_at=now() WHERE id=$1")
                .bind(id)
                .execute(&mut *tx)
                .await?;
            sqlx::query("UPDATE tasks SET state=CASE WHEN execution_scope='node' THEN 'paused' ELSE 'queued' END,lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE lease_node_id=$1 AND state='running'")
                .bind(id)
                .execute(&mut *tx)
                .await?;
            sqlx::query("DELETE FROM storage_permits WHERE holder_id=$1")
                .bind(id)
                .execute(&mut *tx)
                .await?;
        }
        sqlx::query("UPDATE mokyu_meta SET deployment_mode=$1,deployment_mode_revision=deployment_mode_revision+1 WHERE singleton").bind(&input.mode).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    status(State(app), Extension(actor)).await
}

#[utoipa::path(get, operation_id = "cluster_nodes", path = "/api/v1/cluster/nodes", responses((status = 200, body = Vec<NodeView>)))]
pub(crate) async fn nodes(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<Vec<NodeView>>, HttpError> {
    admin(&actor, &app).await?;
    Ok(Json(
        sqlx::query_as(
            "SELECT id,name,role,enabled,last_seen_at,created_at FROM nodes ORDER BY name",
        )
        .fetch_all(&app.db)
        .await?,
    ))
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
    let mut tx = app.db.begin().await?;
    let id: Uuid = sqlx::query_scalar("UPDATE nodes SET enabled=false,credential_encrypted=NULL,last_seen_at=NULL,status='released',recovery_required=false,updated_at=now() WHERE name=$1 AND enabled RETURNING id")
        .bind(&name).fetch_optional(&mut *tx).await?.ok_or_else(|| anyhow::anyhow!("node not found or already released"))?;
    sqlx::query("UPDATE tasks SET state=CASE WHEN execution_scope='node' THEN 'paused' ELSE 'queued' END,lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE lease_node_id=$1 AND state='running'").bind(id).execute(&mut *tx).await?;
    sqlx::query("DELETE FROM storage_permits WHERE holder_id=$1")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
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
    Ok(Json(ClusterStatus {
        mode: sqlx::query_scalar("SELECT deployment_mode FROM mokyu_meta WHERE singleton")
            .fetch_one(&app.db)
            .await?,
        enabled_nodes: sqlx::query_scalar("SELECT count(*) FROM nodes WHERE enabled")
            .fetch_one(&app.db)
            .await?,
        local_storages: sqlx::query_scalar("SELECT count(*) FROM storages WHERE type='local'")
            .fetch_one(&app.db)
            .await?,
    }))
}
