use super::Identity;
use crate::{app::App, cluster, config, http::HttpError};
use anyhow::{Context, ensure};
use axum::{
    Json,
    extract::{Extension, Path, State},
    http::StatusCode,
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
    local_stores: i64,
}

#[derive(Serialize, sqlx::FromRow, ToSchema)]
pub(crate) struct NodeView {
    id: Uuid,
    name: String,
    kind: String,
    advertise_url: Option<String>,
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
pub(crate) struct TokenInput {
    name: String,
    #[schema(example = "data")]
    kind: String,
    advertise_url: Option<String>,
    #[serde(default = "default_expiry")]
    expires_in: String,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct TokenView {
    node_id: Uuid,
    name: String,
    kind: String,
    token: String,
    expires_at: DateTime<Utc>,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct JoinInput {
    token: String,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct JoinView {
    node_id: Uuid,
    kind: String,
    credential: String,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct HeartbeatInput {
    node_id: Uuid,
    credential: String,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct HeartbeatView {
    kind: String,
    enabled: bool,
}

fn default_expiry() -> String {
    "1h".into()
}

fn validate_name(name: &str) -> anyhow::Result<()> {
    ensure!(
        (1..=128).contains(&name.trim().len())
            && !name.chars().any(char::is_control)
            && !name.contains('/'),
        "node name must contain 1..128 safe bytes"
    );
    Ok(())
}

fn validate_url(value: &str) -> anyhow::Result<()> {
    let uri: hyper::Uri = value.parse().context("invalid advertise URL")?;
    ensure!(
        uri.scheme_str().is_some() && uri.authority().is_some(),
        "advertise URL must be absolute"
    );
    Ok(())
}

async fn admin(actor: &Identity, app: &App) -> Result<(), HttpError> {
    actor.recent(app).await?;
    let mut tx = app.db.begin().await?;
    actor.principal.lock_admin(&mut tx).await?;
    tx.commit().await?;
    Ok(())
}

#[utoipa::path(get, operation_id = "cluster_status", path = "/api/cluster", responses((status = 200, body = ClusterStatus)))]
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
        local_stores: sqlx::query_scalar(
            "SELECT count(*) FROM stores WHERE type='local'",
        )
        .fetch_one(&app.db)
        .await?,
    }))
}

#[utoipa::path(post, operation_id = "cluster_mode", path = "/api/cluster/mode", request_body = ModeInput, responses((status = 200, body = ClusterStatus)))]
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
            let local: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM stores WHERE type='local')",
            )
            .fetch_one(&mut *tx)
            .await?;
            if local {
                return Err(anyhow::anyhow!(
                    "local stores exist; migrate them to S3 before enabling cluster mode"
                )
                .into());
            }
        } else {
            let nodes: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM nodes WHERE enabled)")
                    .fetch_one(&mut *tx)
                    .await?;
            if nodes {
                return Err(anyhow::anyhow!(
                    "disable all registered cluster nodes before returning to single mode"
                )
                .into());
            }
        }
        sqlx::query("UPDATE mokyu_meta SET deployment_mode=$1 WHERE singleton")
            .bind(&input.mode)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    status(State(app), Extension(actor)).await
}

#[utoipa::path(get, operation_id = "cluster_nodes", path = "/api/cluster/nodes", responses((status = 200, body = Vec<NodeView>)))]
pub(crate) async fn nodes(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<Vec<NodeView>>, HttpError> {
    admin(&actor, &app).await?;
    let rows: Vec<NodeView> = sqlx::query_as(
        "SELECT id,name,kind,advertise_url,enabled,last_seen_at,created_at FROM nodes ORDER BY name",
    )
    .fetch_all(&app.db)
    .await?;
    Ok(Json(rows))
}

#[utoipa::path(post, operation_id = "cluster_token", path = "/api/cluster/tokens", request_body = TokenInput, responses((status = 201, body = TokenView)))]
pub(crate) async fn token(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<TokenInput>,
) -> Result<(StatusCode, Json<TokenView>), HttpError> {
    admin(&actor, &app).await?;
    validate_name(&input.name)?;
    if !matches!(input.kind.as_str(), "control" | "worker" | "data") {
        return Err(anyhow::anyhow!("node kind is control, worker or data").into());
    }
    if input.kind == "control" && input.advertise_url.is_none() {
        return Err(anyhow::anyhow!("control nodes require advertise_url").into());
    }
    if let Some(url) = &input.advertise_url {
        validate_url(url)?;
    }
    let seconds = config::seconds(&input.expires_in)?;
    if !(60..=30 * 86400).contains(&seconds) {
        return Err(anyhow::anyhow!("token lifetime must be 1m..30d").into());
    }
    let token = cluster::join_token()?;
    let hash = cluster::join_token_hash(&token)?;
    let node_id = Uuid::new_v4();
    let expires_at = Utc::now()
        + chrono::Duration::seconds(i64::try_from(seconds).context("token lifetime overflow")?);
    let mut tx = app.db.begin().await?;
    sqlx::query("INSERT INTO nodes(id,name,kind,advertise_url) VALUES($1,$2,$3,$4)")
        .bind(node_id)
        .bind(input.name.trim())
        .bind(&input.kind)
        .bind(input.advertise_url)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "INSERT INTO node_join_tokens(id,node_id,token_hash,expires_at) VALUES($1,$2,$3,$4)",
    )
    .bind(Uuid::new_v4())
    .bind(node_id)
    .bind(hash.as_slice())
    .bind(expires_at)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok((
        StatusCode::CREATED,
        Json(TokenView {
            node_id,
            name: input.name,
            kind: input.kind,
            token,
            expires_at,
        }),
    ))
}

#[utoipa::path(delete, operation_id = "cluster_node_revoke", path = "/api/cluster/nodes/{id}", params(("id" = Uuid, Path)), responses((status = 200)))]
pub(crate) async fn revoke(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, HttpError> {
    admin(&actor, &app).await?;
    let changed =
        sqlx::query("UPDATE nodes SET enabled=false,updated_at=now() WHERE id=$1 AND enabled")
            .bind(id)
            .execute(&app.db)
            .await?
            .rows_affected();
    if changed != 1 {
        return Err(anyhow::anyhow!("node not found or already disabled").into());
    }
    sqlx::query("UPDATE node_join_tokens SET consumed_at=coalesce(consumed_at,now()) WHERE node_id=$1 AND consumed_at IS NULL")
        .bind(id)
        .execute(&app.db)
        .await?;
    Ok(Json(serde_json::json!({"revoked": id})))
}

#[utoipa::path(post, operation_id = "cluster_join", path = "/api/cluster/join", request_body = JoinInput, responses((status = 200, body = JoinView)))]
pub(crate) async fn join(
    State(app): State<Arc<App>>,
    Json(input): Json<JoinInput>,
) -> Result<Json<JoinView>, HttpError> {
    let hash = cluster::join_token_hash(&input.token).map_err(|_| crate::http::unauthorized())?;
    let mut tx = app.db.begin().await?;
    let mode: String = sqlx::query_scalar("SELECT deployment_mode FROM mokyu_meta WHERE singleton")
        .fetch_one(&mut *tx)
        .await?;
    if mode != "cluster" {
        return Err(crate::http::problem(
            StatusCode::CONFLICT,
            "ClusterDisabled",
        ));
    }
    let record: Option<(Uuid, Uuid, String)> = sqlx::query_as(
        "SELECT t.id,n.id,n.kind FROM node_join_tokens t JOIN nodes n ON n.id=t.node_id WHERE t.token_hash=$1 AND t.consumed_at IS NULL AND t.expires_at>now() AND n.enabled FOR UPDATE OF t,n",
    )
    .bind(hash.as_slice())
    .fetch_optional(&mut *tx)
    .await?;
    let (token_id, node_id, kind) = record.ok_or_else(crate::http::unauthorized)?;
    let credential = cluster::join_token()?;
    let protected = crate::codec::protect(
        credential.as_bytes(),
        &app.secrets.credential_key,
        cluster::credential_binding(node_id).as_bytes(),
    )?;
    sqlx::query("UPDATE node_join_tokens SET consumed_at=now() WHERE id=$1")
        .bind(token_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "UPDATE nodes SET credential_encrypted=$2,last_seen_at=now(),updated_at=now() WHERE id=$1",
    )
    .bind(node_id)
    .bind(protected)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Json(JoinView {
        node_id,
        kind,
        credential,
    }))
}

#[utoipa::path(post, operation_id = "cluster_heartbeat", path = "/api/cluster/heartbeat", request_body = HeartbeatInput, responses((status = 200, body = HeartbeatView)))]
pub(crate) async fn heartbeat(
    State(app): State<Arc<App>>,
    Json(input): Json<HeartbeatInput>,
) -> Result<Json<HeartbeatView>, HttpError> {
    let row: Option<(String, bool, Option<Vec<u8>>)> =
        sqlx::query_as("SELECT kind,enabled,credential_encrypted FROM nodes WHERE id=$1")
            .bind(input.node_id)
            .fetch_optional(&app.db)
            .await?;
    let (kind, enabled, encrypted) = row.ok_or_else(crate::http::unauthorized)?;
    let encrypted = encrypted.ok_or_else(crate::http::unauthorized)?;
    let expected = crate::codec::unprotect(
        &encrypted,
        &app.secrets.credential_key,
        cluster::credential_binding(input.node_id).as_bytes(),
    )?;
    use subtle::ConstantTimeEq;
    if !bool::from(expected.as_bytes().ct_eq(input.credential.as_bytes())) {
        return Err(crate::http::unauthorized());
    }
    if enabled {
        sqlx::query("UPDATE nodes SET last_seen_at=now(),updated_at=now() WHERE id=$1 AND enabled")
            .bind(input.node_id)
            .execute(&app.db)
            .await?;
    }
    Ok(Json(HeartbeatView { kind, enabled }))
}
