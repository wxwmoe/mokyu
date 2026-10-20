use super::Identity;
use crate::{app::App, authorization::Principal, content::codec, http::HttpError};
use anyhow::Result;
use axum::{
    Json,
    extract::{Extension, Path, State},
    http::StatusCode,
};
use serde::Deserialize;
use std::sync::Arc;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::model::key::ManagedKeyCreate;
pub(crate) use crate::model::key::ManagedKeyView;

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

pub(crate) async fn list_rows(
    store: &dyn crate::model::ManagedKeyStore,
    principal: &Principal,
) -> Result<Vec<ManagedKeyView>> {
    let mut rows = store.list(principal).await?;
    let capacity = match store.nonce_status().await {
        Ok(rows) => rows,
        Err(error) => {
            tracing::warn!(%error, "key capacity is unavailable");
            Vec::new()
        }
    };
    let mut capacity: std::collections::HashMap<_, _> =
        capacity.into_iter().map(|v| (v.key_id, v)).collect();
    for row in &mut rows {
        row.nonce_capacity = capacity.remove(&row.id);
    }
    Ok(rows)
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
            _ => {
                return Err(anyhow::anyhow!(
                    "block key algorithm must be aes-256-gcm or chacha20-poly1305"
                ));
            }
        }
    } else {
        anyhow::ensure!(
            input.algorithm.is_none(),
            "access and signing keys do not accept an algorithm"
        );
        "raw"
    };
    let runtime_key = (input.purpose == "block")
        .then(|| codec::Key::new(algorithm, secret))
        .transpose()?;
    let result = app
        .managed_keys
        .create(
            principal,
            ManagedKeyCreate {
                id,
                name: input.name,
                purpose: input.purpose,
                algorithm: algorithm.to_owned(),
                protected,
                fingerprint: *fingerprint.as_bytes(),
            },
        )
        .await?;
    if let Some(key) = runtime_key {
        app.secrets
            .publish_key_with_id(Some(id), result.name.clone(), key)?;
    }
    Ok(result)
}

pub(crate) async fn revoke_record(app: &App, principal: &Principal, id: Uuid) -> Result<()> {
    app.managed_keys.revoke(principal, id).await
}

#[utoipa::path(get, operation_id = "managed_keys", path = "/api/v1/keys", responses((status = 200, body = Vec<ManagedKeyView>)))]
pub(crate) async fn list(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<Vec<ManagedKeyView>>, HttpError> {
    let rows = list_rows(app.managed_keys.as_ref(), &actor.principal).await?;
    Ok(Json(rows))
}

#[utoipa::path(post, operation_id = "managed_key_create", path = "/api/v1/keys", request_body = ManagedKeyInput, responses((status = 201, body = ManagedKeyView)))]
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

#[utoipa::path(delete, operation_id = "managed_key_revoke", path = "/api/v1/keys/{id}", params(("id" = Uuid, Path)), responses((status = 204)))]
pub(crate) async fn revoke(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, HttpError> {
    actor.recent(&app).await?;
    revoke_record(&app, &actor.principal, id).await?;
    Ok(StatusCode::NO_CONTENT)
}
