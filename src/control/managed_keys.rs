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

pub(crate) use crate::model::key::ManagedKeyView;
use crate::model::key::{ManagedKeyCreate, validate_name};

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManagedKeyInput {
    pub name: String,
    pub purpose: String,
    #[serde(default)]
    pub algorithm: Option<String>,
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

fn prepare(
    app: &App,
    input: ManagedKeyInput,
    id: Uuid,
) -> Result<(ManagedKeyCreate, Option<codec::Key>)> {
    validate_name(&input.name)?;
    validate_purpose(&input.purpose)?;
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
    Ok((
        ManagedKeyCreate {
            id,
            name: input.name.trim().to_owned(),
            purpose: input.purpose,
            algorithm: algorithm.into(),
            protected,
            fingerprint: *fingerprint.as_bytes(),
        },
        runtime_key,
    ))
}

pub(crate) async fn create_record(
    app: &App,
    principal: &Principal,
    input: ManagedKeyInput,
) -> Result<ManagedKeyView> {
    let (input, runtime_key) = prepare(app, input, Uuid::new_v4())?;
    let id = input.id;
    let result = app.managed_keys.create(principal, input).await?;
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

use crate::model::key_replacement::{
    KeyReplacementInput, KeyReplacementPreview, KeyReplacementResult,
};

#[derive(serde::Serialize, ToSchema)]
pub(crate) struct KeyReplacementStatus {
    pub preview: KeyReplacementPreview,
    pub result: Option<KeyReplacementResult>,
}
#[derive(serde::Serialize, ToSchema)]
pub(crate) struct KeyReplacementApplied {
    pub result: KeyReplacementResult,
    pub pending: Vec<Uuid>,
    pub cluster: bool,
}

pub(crate) async fn preview_replacement(
    app: &App,
    principal: &Principal,
    source: Uuid,
    input: KeyReplacementInput,
) -> Result<KeyReplacementPreview> {
    app.managed_keys
        .replacement_preview(principal, source, input)
        .await
}
pub(crate) async fn replacement_status(
    app: &App,
    principal: &Principal,
    id: Uuid,
) -> Result<KeyReplacementStatus> {
    let (preview, result) = app.managed_keys.replacement(principal, id).await?;
    Ok(KeyReplacementStatus {
        preview: preview.0,
        result: result.map(|v| v.0),
    })
}
pub(crate) async fn replace_associated(
    app: &App,
    principal: &Principal,
    id: Uuid,
) -> Result<KeyReplacementApplied> {
    let status = replacement_status(app, principal, id).await?;
    let prepared = if status.preview.create && status.result.is_none() {
        Some(
            prepare(
                app,
                ManagedKeyInput {
                    name: status.preview.target_name.clone(),
                    purpose: "block".into(),
                    algorithm: Some(status.preview.algorithm.clone()),
                },
                status.preview.target,
            )?
            .0,
        )
    } else {
        None
    };
    let result = app.managed_keys.replace(principal, id, prepared).await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("key-replacement-before-refresh").await;
    let mut pending = Vec::new();
    for storage in &result.preview.storages {
        let refresh = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            app.storage.refresh_store(storage.id),
        )
        .await;
        if !matches!(refresh, Ok(Ok(())))
            || (storage.enabled
                && app
                    .storage
                    .profile(storage.id)
                    .map_or(true, |p| p.revision <= storage.revision))
        {
            tracing::warn!(storage_id=%storage.id, "key replacement saved; storage refresh remains pending");
            pending.push(storage.id);
        }
    }
    Ok(KeyReplacementApplied {
        result,
        pending,
        cluster: app.role != "single",
    })
}

#[utoipa::path(post, operation_id="key_replacement_preview", path="/api/v1/keys/{id}/replace-preview", params(("id"=Uuid,Path)), request_body=KeyReplacementInput, responses((status=200,body=KeyReplacementPreview)))]
pub(crate) async fn replacement_preview(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
    Json(input): Json<KeyReplacementInput>,
) -> Result<Json<KeyReplacementPreview>, HttpError> {
    actor.recent(&app).await?;
    Ok(Json(
        preview_replacement(&app, &actor.principal, id, input).await?,
    ))
}
#[utoipa::path(get, operation_id="key_replacement_status", path="/api/v1/key-replacements/{id}", params(("id"=Uuid,Path)), responses((status=200,body=KeyReplacementStatus)))]
pub(crate) async fn replacement_get(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<Json<KeyReplacementStatus>, HttpError> {
    Ok(Json(replacement_status(&app, &actor.principal, id).await?))
}
#[utoipa::path(post, operation_id="key_replacement_apply", path="/api/v1/key-replacements/{id}", params(("id"=Uuid,Path)), responses((status=200,body=KeyReplacementApplied)))]
pub(crate) async fn replacement_apply(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<Json<KeyReplacementApplied>, HttpError> {
    actor.recent(&app).await?;
    Ok(Json(replace_associated(&app, &actor.principal, id).await?))
}
