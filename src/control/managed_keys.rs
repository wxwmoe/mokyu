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
    pub active: bool,
    pub fingerprint: String,
    pub created_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManagedKeyInput {
    pub name: String,
    pub purpose: String,
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
        matches!(purpose, "access" | "signing" | "encryption"),
        "managed key purpose is access, signing, or encryption"
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
        active,
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
        "SELECT id,name,purpose,algorithm,active,fingerprint,created_at,revoked_at FROM managed_keys ORDER BY name",
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
    let algorithm = if input.purpose == "encryption" {
        match app.config.encryption.algorithm.as_str() {
            "chacha20-poly1305" => "chacha20-poly1305",
            _ => "aes-256-gcm",
        }
    } else {
        "raw"
    };
    let mut tx = users::admin_transaction(app, principal).await?;
    let active = input.purpose == "encryption"
        && !sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM managed_keys WHERE purpose='encryption' AND active AND revoked_at IS NULL)",
        )
        .fetch_one(&mut *tx)
        .await?;
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
        "INSERT INTO managed_keys(id,name,purpose,algorithm,active,secret_encrypted,fingerprint) VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING id,name,purpose,algorithm,active,fingerprint,created_at,revoked_at",
    )
    .bind(id)
    .bind(input.name.trim())
    .bind(&input.purpose)
    .bind(algorithm)
    .bind(active)
    .bind(protected)
    .bind(fingerprint.as_bytes().as_slice())
    .fetch_one(&mut *tx)
    .await?;
    let result = view(row);
    super::audit::checkpoint(
        &mut tx,
        "managed_key.create",
        &id.to_string(),
        serde_json::json!({"name":result.name,"purpose":result.purpose,"algorithm":result.algorithm,"active":result.active,"fingerprint":result.fingerprint}),
    )
    .await?;
    tx.commit().await?;
    Ok(result)
}

pub(crate) async fn revoke_record(app: &App, principal: &Principal, id: Uuid) -> Result<()> {
    let mut tx = users::admin_transaction(app, principal).await?;
    let changed = sqlx::query(
        "UPDATE managed_keys SET active=false,revoked_at=coalesce(revoked_at,now()) WHERE id=$1",
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

pub(crate) async fn activate_record(app: &App, principal: &Principal, id: Uuid) -> Result<()> {
    let mut tx = users::admin_transaction(app, principal).await?;
    sqlx::query("UPDATE managed_keys SET active=false WHERE purpose='encryption'")
        .execute(&mut *tx)
        .await?;
    let changed = sqlx::query("UPDATE managed_keys SET active=true WHERE id=$1 AND purpose='encryption' AND revoked_at IS NULL")
        .bind(id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    if changed == 0 {
        return Err(s3s::s3_error!(NoSuchKey).into());
    }
    super::audit::checkpoint(
        &mut tx,
        "managed_key.activate",
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

#[utoipa::path(post, operation_id = "managed_key_activate", path = "/api/keys/{id}/activate", params(("id" = Uuid, Path)), responses((status = 204)))]
pub(crate) async fn activate(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, HttpError> {
    actor.recent(&app).await?;
    activate_record(&app, &actor.principal, id).await?;
    Ok(StatusCode::NO_CONTENT)
}
