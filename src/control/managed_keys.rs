use super::{Identity, users};
use crate::{app::App, authorization::Principal, codec, http::HttpError};
use anyhow::Result;
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
pub(crate) struct ManagedKeyView {
    pub id: Uuid,
    pub name: String,
    pub purpose: String,
    pub algorithm: String,
    pub in_use: bool,
    pub fingerprint: String,
    pub created_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManagedKeyInput {
    pub name: String,
    pub purpose: String,
    #[serde(default)]
    pub algorithm: Option<String>,
}

fn validate_name(name: &str) -> Result<()> {
    anyhow::ensure!(
        (1..=128).contains(&name.trim().len())
            && !name.chars().any(char::is_control)
            && !name.contains('/')
            && name.trim() != "__credentials",
        "managed key name must contain 1..128 safe bytes"
    );
    Ok(())
}

fn validate_purpose(purpose: &str) -> Result<()> {
    anyhow::ensure!(
        matches!(purpose, "access" | "signing" | "block"),
        "managed key purpose is access, signing, or block"
    );
    Ok(())
}

fn view(
    (id, name, purpose, algorithm, active, fingerprint, created_at, revoked_at): (
        Uuid,
        String,
        String,
        String,
        bool,
        Vec<u8>,
        DateTime<Utc>,
        Option<DateTime<Utc>>,
    ),
) -> ManagedKeyView {
    ManagedKeyView {
        id,
        name,
        purpose,
        algorithm,
        in_use: active,
        fingerprint: hex::encode(fingerprint),
        created_at,
        revoked_at,
    }
}

pub(crate) async fn list_rows(db: &sqlx::PgPool) -> Result<Vec<ManagedKeyView>> {
    let rows: Vec<(
        Uuid,
        String,
        String,
        String,
        bool,
        Vec<u8>,
        DateTime<Utc>,
        Option<DateTime<Utc>>,
    )> = sqlx::query_as(
        "SELECT k.id,k.name,k.purpose,k.algorithm,EXISTS(SELECT 1 FROM stores s WHERE s.current_key_id=k.id),k.fingerprint,k.created_at,k.revoked_at FROM managed_keys k ORDER BY k.name",
    )
    .fetch_all(db)
    .await?;
    Ok(rows.into_iter().map(view).collect())
}

pub(crate) async fn create_record(
    app: &App,
    principal: &Principal,
    input: ManagedKeyInput,
) -> Result<ManagedKeyView> {
    validate_name(&input.name)?;
    validate_purpose(&input.purpose)?;
    let id = Uuid::new_v4();
    let mut secret = [0u8; 32];
    aws_lc_rs::rand::fill(&mut secret).map_err(|_| anyhow::anyhow!("randomness unavailable"))?;
    let fingerprint = blake3::hash(&secret);
    let protected = codec::protect(
        &secret,
        &app.secrets.credential_key,
        format!("managed-key-{id}").as_bytes(),
    )?;
    let algorithm = if input.purpose == "block" {
        match input.algorithm.as_deref().unwrap_or("aes-256-gcm") {
            "aes-256-gcm" => "aes-256-gcm",
            "chacha20-poly1305" => "chacha20-poly1305",
            _ => return Err(anyhow::anyhow!("block key algorithm must be aes-256-gcm or chacha20-poly1305")),
        }
    } else {
        anyhow::ensure!(input.algorithm.is_none(), "access and signing keys do not accept an algorithm");
        "raw"
    };
    let runtime_key = (input.purpose == "block")
        .then(|| codec::Key::new(algorithm, secret))
        .transpose()?;
    let mut tx = users::admin_transaction(app, principal).await?;
    let row: (
        Uuid,
        String,
        String,
        String,
        bool,
        Vec<u8>,
        DateTime<Utc>,
        Option<DateTime<Utc>>,
    ) = sqlx::query_as(
        "INSERT INTO managed_keys(id,name,purpose,algorithm,secret_encrypted,fingerprint) VALUES($1,$2,$3,$4,$5,$6) RETURNING id,name,purpose,algorithm,false,fingerprint,created_at,revoked_at",
    )
    .bind(id)
    .bind(input.name.trim())
    .bind(&input.purpose)
    .bind(algorithm)
    .bind(protected)
    .bind(fingerprint.as_bytes().as_slice())
    .fetch_one(&mut *tx)
    .await?;
    let result = view(row);
    super::audit::checkpoint(
        &mut tx,
        "managed_key.create",
        &id.to_string(),
        serde_json::json!({"name":result.name,"purpose":result.purpose,"algorithm":result.algorithm,"fingerprint":result.fingerprint}),
    )
    .await?;
    tx.commit().await?;
    if let Some(key) = runtime_key {
        app.secrets.publish_key(result.name.clone(), key)?;
    }
    Ok(result)
}

pub(crate) async fn revoke_record(app: &App, principal: &Principal, id: Uuid) -> Result<()> {
    let mut tx = users::admin_transaction(app, principal).await?;
    let name: Option<String> = sqlx::query_scalar("SELECT name FROM managed_keys WHERE id=$1 FOR UPDATE")
        .bind(id).fetch_optional(&mut *tx).await?;
    let name = name.ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    let used: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM stores WHERE current_key_id=$1) OR EXISTS(SELECT 1 FROM chunk_locations WHERE key_id=$2 AND state<>'deleted') OR EXISTS(SELECT 1 FROM packs WHERE key_id=$2 AND state<>'deleted') OR EXISTS(SELECT 1 FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE c.key_id=$2) OR EXISTS(SELECT 1 FROM tasks WHERE detail->>'target_key'=$2 AND state IN ('queued','running','paused','failed'))")
        .bind(id).bind(&name).fetch_one(&mut *tx).await?;
    if used {
        return Err(s3s::s3_error!(InvalidRequest, "key is still used by a store, physical encoding, pending upload or task").into());
    }
    let changed = sqlx::query(
        "UPDATE managed_keys SET revoked_at=coalesce(revoked_at,now()),secret_encrypted=NULL WHERE id=$1",
    )
    .bind(id)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if changed == 0 {
        return Err(s3s::s3_error!(NoSuchKey).into());
    }
    super::audit::checkpoint(
        &mut tx,
        "managed_key.revoke",
        &id.to_string(),
        serde_json::json!({}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

#[utoipa::path(get, operation_id = "managed_keys", path = "/api/keys", responses((status = 200, body = Vec<ManagedKeyView>)))]
pub(crate) async fn list(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<Vec<ManagedKeyView>>, HttpError> {
    let tx = users::admin_transaction(&app, &actor.principal).await?;
    tx.commit().await?;
    let rows = list_rows(&app.db).await?;
    Ok(Json(rows))
}

#[utoipa::path(post, operation_id = "managed_key_create", path = "/api/keys", request_body = ManagedKeyInput, responses((status = 201, body = ManagedKeyView)))]
pub(crate) async fn create(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<ManagedKeyInput>,
) -> Result<(StatusCode, Json<ManagedKeyView>), HttpError> {
    actor.recent(&app).await?;
    Ok((
        StatusCode::CREATED,
        Json(create_record(&app, &actor.principal, input).await?),
    ))
}

#[utoipa::path(delete, operation_id = "managed_key_revoke", path = "/api/keys/{id}", params(("id" = Uuid, Path)), responses((status = 204)))]
pub(crate) async fn revoke(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, HttpError> {
    actor.recent(&app).await?;
    revoke_record(&app, &actor.principal, id).await?;
    Ok(StatusCode::NO_CONTENT)
}
