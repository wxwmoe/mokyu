use crate::{app::App, codec};
use anyhow::{Context, Result, bail, ensure};
use axum::{
    body::{Body, to_bytes},
    extract::State,
    http::{HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use base64::Engine;
use serde::{Deserialize, Serialize};
use std::{os::unix::fs::PermissionsExt, path::Path, sync::Arc, time::Duration};
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct NodeState {
    pub(crate) node_id: Uuid,
    pub(crate) credential: String,
}

#[derive(Serialize)]
pub(crate) struct JoinRequest<'a> {
    pub token: &'a str,
}

#[derive(Deserialize)]
struct JoinReply {
    node_id: Uuid,
    kind: String,
    credential: String,
}

#[derive(Serialize)]
struct HeartbeatRequest {
    node_id: Uuid,
    credential: String,
}

#[derive(Deserialize)]
struct HeartbeatReply {
    kind: String,
    enabled: bool,
}

pub fn join_token() -> Result<String> {
    let mut bytes = [0; 48];
    aws_lc_rs::rand::fill(&mut bytes).map_err(|_| anyhow::anyhow!("randomness unavailable"))?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

pub fn join_token_hash(token: &str) -> Result<[u8; 32]> {
    let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(token.as_bytes())
        .context("join token must be URL-safe base64")?;
    ensure!(raw.len() == 48, "join token must encode 48 bytes");
    Ok(*blake3::hash(token.as_bytes()).as_bytes())
}

pub(crate) fn credential_binding(node_id: Uuid) -> String {
    format!("node-credential-{node_id}")
}

fn node_state_path(app: &App) -> std::path::PathBuf {
    app.config.storage.data.join("node.json")
}

pub(crate) async fn read_state(path: &Path) -> Result<Option<NodeState>> {
    if !tokio::fs::try_exists(path).await? {
        return Ok(None);
    }
    let state =
        serde_json::from_slice(&tokio::fs::read(path).await?).context("invalid node state file")?;
    Ok(Some(state))
}

pub(crate) async fn local_node_id(app: &App) -> Result<Option<Uuid>> {
    Ok(read_state(&node_state_path(app))
        .await?
        .map(|state| state.node_id))
}

pub(crate) async fn try_leader(
    app: &App,
    key: i64,
) -> Result<Option<sqlx::Transaction<'static, sqlx::Postgres>>> {
    let mut transaction = app.db.begin().await?;
    let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)")
        .bind(key)
        .fetch_one(&mut *transaction)
        .await?;
    if locked {
        Ok(Some(transaction))
    } else {
        transaction.rollback().await?;
        Ok(None)
    }
}

async fn write_state(path: &Path, state: &NodeState) -> Result<()> {
    let temporary = path.with_extension("json.tmp");
    tokio::fs::write(&temporary, serde_json::to_vec(state)?).await?;
    tokio::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600)).await?;
    tokio::fs::rename(temporary, path).await?;
    Ok(())
}

fn endpoint(base: &str, path: &str) -> String {
    format!(
        "{}/{}",
        base.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

pub(crate) async fn post_json<T: Serialize, R: for<'de> Deserialize<'de>>(
    base: &str,
    path: &str,
    value: &T,
) -> Result<R> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let response = client
        .post(endpoint(base, path))
        .json(value)
        .send()
        .await
        .context("cannot reach Mokyu control")?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        let detail: String = body.chars().take(256).collect();
        bail!("control request failed ({status}): {detail}");
    }
    Ok(response.json().await?)
}

async fn join_remote(app: &App, role: &str) -> Result<()> {
    let control = app
        .config
        .cluster
        .control_url
        .as_deref()
        .context("cluster.control_url is required for a joining node")?;
    let path = node_state_path(app);
    if let Some(state) = read_state(&path).await? {
        let reply: HeartbeatReply = post_json(
            control,
            "/api/cluster/heartbeat",
            &HeartbeatRequest {
                node_id: state.node_id,
                credential: state.credential,
            },
        )
        .await?;
        ensure!(reply.enabled, "this node is disabled by the control plane");
        ensure!(
            reply.kind == role,
            "registered node role does not match this process"
        );
        return Ok(());
    }
    let token = app
        .config
        .cluster
        .join_token
        .as_deref()
        .context("cluster.join_token is required for a new node")?;
    let reply: JoinReply = post_json(control, "/api/cluster/join", &JoinRequest { token }).await?;
    ensure!(
        reply.kind == role,
        "join token role does not match this process"
    );
    ensure!(
        !reply.credential.is_empty(),
        "control returned an empty node credential"
    );
    write_state(
        &path,
        &NodeState {
            node_id: reply.node_id,
            credential: reply.credential,
        },
    )
    .await?;
    Ok(())
}

async fn bootstrap_control(app: &App) -> Result<()> {
    let mode: String = sqlx::query_scalar("SELECT deployment_mode FROM mokyu_meta WHERE singleton")
        .fetch_one(&app.db)
        .await?;
    ensure!(
        mode == "cluster",
        "set cluster mode before starting a control node"
    );
    let name = app
        .config
        .cluster
        .node_name
        .as_deref()
        .unwrap_or("control-1");
    let advertise_url = app
        .config
        .cluster
        .advertise_url
        .as_deref()
        .context("cluster.advertise_url is required for the first control node")?;
    let path = node_state_path(app);
    let mut tx = app.db.begin().await?;
    let existing: Option<(Uuid, String, bool, Option<Vec<u8>>)> = sqlx::query_as(
        "SELECT id,kind,enabled,credential_encrypted FROM nodes WHERE name=$1 FOR UPDATE",
    )
    .bind(name)
    .fetch_optional(&mut *tx)
    .await?;
    let (node_id, credential) = match existing {
        Some((id, kind, enabled, encrypted)) => {
            ensure!(
                kind == "control",
                "configured control node name belongs to another role"
            );
            ensure!(enabled, "configured control node is disabled");
            let credential = encrypted
                .as_deref()
                .map(|data| {
                    codec::unprotect(
                        data,
                        &app.secrets.credential_key,
                        credential_binding(id).as_bytes(),
                    )
                })
                .transpose()?
                .context("control node credential is missing")?;
            sqlx::query(
                "UPDATE nodes SET advertise_url=$2,last_seen_at=now(),updated_at=now() WHERE id=$1",
            )
            .bind(id)
            .bind(advertise_url)
            .execute(&mut *tx)
            .await?;
            (id, credential)
        }
        None => {
            let other: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM nodes WHERE kind='control' AND enabled)",
            )
            .fetch_one(&mut *tx)
            .await?;
            ensure!(
                !other,
                "an active control node already exists; use a join token"
            );
            let id = Uuid::new_v4();
            let credential = join_token()?;
            let encrypted = codec::protect(
                credential.as_bytes(),
                &app.secrets.credential_key,
                credential_binding(id).as_bytes(),
            )?;
            sqlx::query("INSERT INTO nodes(id,name,kind,advertise_url,credential_encrypted,last_seen_at) VALUES($1,$2,'control',$3,$4,now())")
                .bind(id)
                .bind(name)
                .bind(advertise_url)
                .bind(encrypted)
                .execute(&mut *tx)
                .await?;
            (id, credential)
        }
    };
    let lease: Option<Uuid> = sqlx::query_scalar(
        "UPDATE control_leases SET node_id=$1,lease_token=gen_random_uuid(),expires_at=now()+interval '30 seconds',updated_at=now() WHERE singleton AND (node_id=$1 OR node_id IS NULL OR expires_at<now()) RETURNING lease_token",
    )
    .bind(node_id)
    .fetch_optional(&mut *tx)
    .await?;
    ensure!(
        lease.is_some(),
        "another control node currently owns the control lease"
    );
    tx.commit().await?;
    write_state(
        &path,
        &NodeState {
            node_id,
            credential,
        },
    )
    .await?;
    Ok(())
}

pub async fn initialize(app: &App, role: &str) -> Result<()> {
    let mode: String = sqlx::query_scalar("SELECT deployment_mode FROM mokyu_meta WHERE singleton")
        .fetch_one(&app.db)
        .await?;
    if role == "single" {
        ensure!(
            mode == "single",
            "single mode cannot run while deployment_mode=cluster"
        );
        return Ok(());
    }
    ensure!(
        mode == "cluster",
        "set cluster mode before starting a split service role"
    );
    if let Some(state) = read_state(&node_state_path(app)).await? {
        validate_node(app, &state, role).await?;
        if role == "control" {
            if let Some(url) = app.config.cluster.advertise_url.as_deref() {
                sqlx::query("UPDATE nodes SET advertise_url=$2 WHERE id=$1")
                    .bind(state.node_id)
                    .bind(url)
                    .execute(&app.db)
                    .await?;
            }
        }
        return Ok(());
    }
    if role == "control" && app.config.cluster.join_token.is_none() {
        bootstrap_control(app).await
    } else {
        ensure!(
            matches!(role, "control" | "worker" | "data"),
            "unknown service role"
        );
        join_remote(app, role).await
    }
}

async fn validate_node(app: &App, state: &NodeState, role: &str) -> Result<()> {
    let record: Option<(String, bool, Option<Vec<u8>>)> =
        sqlx::query_as("SELECT kind,enabled,credential_encrypted FROM nodes WHERE id=$1")
            .bind(state.node_id)
            .fetch_optional(&app.db)
            .await?;
    let (kind, enabled, encrypted) = record.context("registered node no longer exists")?;
    ensure!(
        enabled && kind == role,
        "registered node is disabled or belongs to another role"
    );
    let expected = codec::unprotect(
        encrypted.as_deref().context("node has not joined")?,
        &app.secrets.credential_key,
        credential_binding(state.node_id).as_bytes(),
    )?;
    ensure!(
        aws_lc_rs::constant_time::verify_slices_are_equal(
            expected.as_bytes(),
            state.credential.as_bytes()
        )
        .is_ok(),
        "invalid node credential"
    );
    Ok(())
}

pub async fn control_loop(app: Arc<App>) -> Result<()> {
    let state = read_state(&node_state_path(&app))
        .await?
        .context("control node state is missing")?;
    loop {
        let renewal: Result<()> = async {
            let updated = sqlx::query("UPDATE nodes SET last_seen_at=now(),updated_at=now() WHERE id=$1 AND kind='control' AND enabled")
            .bind(state.node_id)
            .execute(&app.db)
            .await?;
            ensure!(updated.rows_affected() == 1, "control node is disabled");
            sqlx::query("UPDATE control_leases SET node_id=$1,lease_token=CASE WHEN node_id=$1 AND expires_at>now() THEN lease_token ELSE gen_random_uuid() END,expires_at=now()+interval '30 seconds',updated_at=now() WHERE singleton AND (node_id=$1 OR node_id IS NULL OR expires_at<now()) AND EXISTS(SELECT 1 FROM nodes WHERE id=$1 AND enabled)")
                .bind(state.node_id)
                .execute(&app.db)
                .await?;
            Ok(())
        }.await;
        if let Err(error) = renewal {
            if error.downcast_ref::<sqlx::Error>().is_none() {
                return Err(error);
            }
            tracing::warn!(%error, "control lease renewal failed; retrying");
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}

pub async fn node_loop(app: Arc<App>, role: &'static str) -> Result<()> {
    let control = app
        .config
        .cluster
        .control_url
        .as_deref()
        .context("cluster.control_url is required for node heartbeat")?;
    let state = read_state(&node_state_path(&app))
        .await?
        .context("node state is missing")?;
    loop {
        if let Err(error) = sqlx::query(
            "UPDATE nodes SET last_seen_at=now(),updated_at=now() WHERE id=$1 AND enabled",
        )
        .bind(state.node_id)
        .execute(&app.db)
        .await
        {
            tracing::warn!(%error, "node database heartbeat failed; retrying");
        }
        if let Err(error) = validate_node(&app, &state, role).await {
            tracing::warn!(%error, "node validation failed; retrying");
            tokio::time::sleep(Duration::from_secs(10)).await;
            continue;
        }
        let reply: Result<HeartbeatReply> = post_json(
            control,
            "/api/cluster/heartbeat",
            &HeartbeatRequest {
                node_id: state.node_id,
                credential: state.credential.clone(),
            },
        )
        .await;
        match reply {
            Ok(reply) if reply.kind == role && reply.enabled => {}
            Ok(_) => tracing::warn!("control rejected node heartbeat; retrying"),
            Err(error) => tracing::warn!(%error, "control heartbeat failed; retrying"),
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}

fn hop_by_hop(name: &str) -> bool {
    matches!(
        name,
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "host"
            | "content-length"
            | "x-mokyu-forwarded"
    )
}

async fn forward_request(target: &str, request: axum::extract::Request) -> Response {
    let (parts, body) = request.into_parts();
    let bytes = match to_bytes(body, 32 * 1024 * 1024).await {
        Ok(bytes) => bytes,
        Err(error) => {
            tracing::warn!(error=%error, "standby request body exceeded forwarding limit");
            return crate::http::problem(StatusCode::PAYLOAD_TOO_LARGE, "RequestTooLarge")
                .into_response();
        }
    };
    let path = parts
        .uri
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or("/");
    let url = format!("{}{}", target.trim_end_matches('/'), path);
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .redirect(reqwest::redirect::Policy::none())
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            tracing::error!(error=%error, "cannot create control forwarding client");
            return crate::http::problem(StatusCode::BAD_GATEWAY, "ControlForwardFailed")
                .into_response();
        }
    };
    let mut builder = client.request(parts.method, url);
    for (name, value) in &parts.headers {
        if !hop_by_hop(name.as_str()) {
            builder = builder.header(name.as_str(), value);
        }
    }
    builder = builder.header("x-mokyu-forwarded", HeaderValue::from_static("1"));
    let response = match builder.body(bytes).send().await {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!(error=%error, "control forwarding request failed");
            return crate::http::problem(StatusCode::BAD_GATEWAY, "ControlForwardFailed")
                .into_response();
        }
    };
    let status = response.status();
    let headers = response.headers().clone();
    let body = match response.bytes().await {
        Ok(body) => body,
        Err(error) => {
            tracing::warn!(error=%error, "control forwarding response failed");
            return crate::http::problem(StatusCode::BAD_GATEWAY, "ControlForwardFailed")
                .into_response();
        }
    };
    let mut output = Response::builder().status(status.as_u16());
    for (name, value) in &headers {
        if !hop_by_hop(name.as_str()) {
            output = output.header(name, value);
        }
    }
    output.body(Body::from(body)).unwrap_or_else(|_| {
        crate::http::problem(StatusCode::BAD_GATEWAY, "ControlForwardFailed").into_response()
    })
}

pub async fn forward_if_standby(
    State(app): State<Arc<App>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let path = request.uri().path();
    if !path.starts_with("/api/") {
        return next.run(request).await;
    }
    let mode: Result<String> =
        sqlx::query_scalar("SELECT deployment_mode FROM mokyu_meta WHERE singleton")
            .fetch_one(&app.db)
            .await
            .map_err(Into::into);
    if matches!(mode.as_deref(), Ok("single")) {
        return next.run(request).await;
    }
    if mode.is_err() {
        return crate::http::problem(StatusCode::SERVICE_UNAVAILABLE, "ControlUnavailable")
            .into_response();
    }
    let Some(state) = (match read_state(&node_state_path(&app)).await {
        Ok(state) => state,
        Err(error) => {
            tracing::warn!(error=%error, "cannot read control node state");
            return crate::http::problem(StatusCode::SERVICE_UNAVAILABLE, "ControlUnavailable")
                .into_response();
        }
    }) else {
        return crate::http::problem(StatusCode::SERVICE_UNAVAILABLE, "ControlUnavailable")
            .into_response();
    };
    let lease: Option<(Option<Uuid>, bool, Option<String>)> = match sqlx::query_as(
        "SELECT l.node_id,l.expires_at>now() AND COALESCE(n.enabled,false),n.advertise_url FROM control_leases l LEFT JOIN nodes n ON n.id=l.node_id WHERE l.singleton",
    )
    .fetch_optional(&app.db)
    .await
    {
        Ok(lease) => lease,
        Err(error) => {
            tracing::warn!(error=%error, "cannot read control lease");
            return crate::http::problem(StatusCode::SERVICE_UNAVAILABLE, "ControlUnavailable")
                .into_response();
        }
    };
    let Some((node_id, valid, target)) = lease else {
        return crate::http::problem(StatusCode::SERVICE_UNAVAILABLE, "ControlUnavailable")
            .into_response();
    };
    if !valid {
        return crate::http::problem(StatusCode::SERVICE_UNAVAILABLE, "ControlUnavailable")
            .into_response();
    }
    if node_id == Some(state.node_id) {
        return next.run(request).await;
    }
    if request.headers().get("x-mokyu-forwarded").is_some() {
        return crate::http::problem(StatusCode::SERVICE_UNAVAILABLE, "ControlForwardLoop")
            .into_response();
    }
    let Some(target) = target else {
        return crate::http::problem(StatusCode::SERVICE_UNAVAILABLE, "ControlUnavailable")
            .into_response();
    };
    forward_request(&target, request).await
}
