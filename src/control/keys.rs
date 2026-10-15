use super::{Identity, users::BucketGrant};
use crate::model::credential::CredentialMaterial;
pub(crate) use crate::model::credential::{
    Credential, CredentialInput, CredentialRotation, CredentialSecret, CredentialSettings,
    RotateCredential,
};
use crate::{app::App, authorization::Principal, content::codec, http::HttpError};
use anyhow::Result;
use axum::{
    Json,
    extract::{Extension, Path, Query, State},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

pub(crate) async fn get_credential(
    store: &dyn crate::model::CredentialStore,
    access: &str,
) -> Result<Credential> {
    store.credential(access).await
}

pub(crate) async fn create_credential(
    app: &App,
    principal: &Principal,
    input: CredentialInput,
) -> Result<CredentialSecret> {
    let (material, secret_key) = credential_material(&app.secrets.credential_key)?;
    let access_key = material.access.clone();
    let credential = app
        .credentials
        .create_credential(principal, input, material)
        .await?;
    Ok(CredentialSecret {
        access_key,
        secret_key,
        credential,
    })
}

pub(crate) async fn update_credential(
    app: &App,
    principal: &Principal,
    access: &str,
    input: CredentialSettings,
) -> Result<Credential> {
    app.credentials
        .update_credential(principal, access, input)
        .await
}
pub(crate) async fn set_grants(
    app: &App,
    principal: &Principal,
    access: &str,
    grants: Vec<BucketGrant>,
) -> Result<Credential> {
    app.credentials.set_grants(principal, access, grants).await
}
pub(crate) async fn revoke_credential(
    app: &App,
    principal: &Principal,
    access: &str,
) -> Result<()> {
    app.credentials.revoke_credential(principal, access).await
}
pub(crate) async fn enable_credential(
    app: &App,
    principal: &Principal,
    access: &str,
    enabled: bool,
) -> Result<()> {
    app.credentials
        .enable_credential(principal, access, enabled)
        .await
}
pub(crate) async fn change_grant(
    app: &App,
    principal: &Principal,
    access: &str,
    bucket: Uuid,
    actions: Vec<crate::authorization::Action>,
) -> Result<()> {
    app.credentials
        .change_grant(principal, access, bucket, actions)
        .await
}

pub(crate) async fn rotate_credential(
    app: &App,
    principal: &Principal,
    access: &str,
    input: RotateCredential,
) -> Result<CredentialRotation> {
    let (material, secret_key) = credential_material(&app.secrets.credential_key)?;
    let access_key = material.access.clone();
    let (credential, old_expires_at) = app
        .credentials
        .rotate_credential(principal, access, input, material)
        .await?;
    Ok(CredentialRotation {
        replacement: CredentialSecret {
            access_key,
            secret_key,
            credential,
        },
        old_expires_at,
    })
}
#[derive(Deserialize, IntoParams)]
pub(super) struct CredentialQuery {
    project: Option<Uuid>,
    after: Option<String>,
    limit: Option<i64>,
}
#[derive(Serialize, ToSchema)]
pub(super) struct CredentialPage {
    #[serde(rename = "items")]
    credentials: Vec<Credential>,
    #[serde(rename = "next_cursor")]
    next: Option<String>,
}
pub(crate) async fn list_credentials(
    store: &dyn crate::model::CredentialStore,
    project: Option<Uuid>,
    after: &str,
    limit: i64,
) -> Result<Vec<Credential>> {
    store.credentials(project, after, limit).await
}
#[utoipa::path(get,path="/api/v1/s3-credentials",operation_id="credentials_list",params(CredentialQuery),responses((status=200,body=CredentialPage)))]
pub(super) async fn list(
    State(app): State<Arc<App>>,
    Query(q): Query<CredentialQuery>,
) -> Result<Json<CredentialPage>, HttpError> {
    let limit = q.limit.unwrap_or(50);
    if !(1..=100).contains(&limit) || q.after.as_ref().is_some_and(|s| s.len() > 128) {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let mut credentials = list_credentials(
        app.credentials.as_ref(),
        q.project,
        &q.after.unwrap_or_default(),
        limit + 1,
    )
    .await?;
    let more = credentials.len() > limit as usize;
    credentials.truncate(limit as usize);
    let next = more.then(|| credentials.last().unwrap().access_key.clone());
    Ok(Json(CredentialPage { credentials, next }))
}
#[utoipa::path(post,path="/api/v1/s3-credentials",operation_id="credentials_create",request_body=CredentialInput,responses((status=201,body=CredentialSecret)))]
pub(super) async fn create(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<CredentialInput>,
) -> Result<(StatusCode, Json<CredentialSecret>), HttpError> {
    actor.recent(&app).await?;
    Ok((
        StatusCode::CREATED,
        Json(create_credential(&app, &actor.principal, input).await?),
    ))
}
#[utoipa::path(put,path="/api/v1/s3-credentials/{key}",operation_id="credentials_update",params(("key"=String,Path)),request_body=CredentialSettings,responses((status=200,body=Credential)))]
pub(super) async fn update(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(key): Path<String>,
    Json(input): Json<CredentialSettings>,
) -> Result<Json<Credential>, HttpError> {
    actor.recent(&app).await?;
    Ok(Json(
        update_credential(&app, &actor.principal, &key, input).await?,
    ))
}
#[utoipa::path(delete,path="/api/v1/s3-credentials/{key}",operation_id="credentials_revoke",params(("key"=String,Path)),responses((status=204)))]
pub(super) async fn revoke(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(key): Path<String>,
) -> Result<StatusCode, HttpError> {
    actor.recent(&app).await?;
    revoke_credential(&app, &actor.principal, &key).await?;
    Ok(StatusCode::NO_CONTENT)
}
#[utoipa::path(put,path="/api/v1/s3-credentials/{key}/grants",operation_id="credentials_grants",params(("key"=String,Path)),request_body=Vec<BucketGrant>,responses((status=200,body=Credential)))]
pub(super) async fn grants(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(key): Path<String>,
    Json(input): Json<Vec<BucketGrant>>,
) -> Result<Json<Credential>, HttpError> {
    actor.recent(&app).await?;
    Ok(Json(set_grants(&app, &actor.principal, &key, input).await?))
}
#[utoipa::path(post,path="/api/v1/s3-credentials/{key}/rotate",operation_id="credentials_rotate",params(("key"=String,Path)),request_body=RotateCredential,responses((status=201,body=CredentialRotation)))]
pub(super) async fn rotate(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(key): Path<String>,
    Json(input): Json<RotateCredential>,
) -> Result<(StatusCode, Json<CredentialRotation>), HttpError> {
    actor.recent(&app).await?;
    Ok((
        StatusCode::CREATED,
        Json(rotate_credential(&app, &actor.principal, &key, input).await?),
    ))
}

fn credential_material(key: &codec::Key) -> Result<(CredentialMaterial, String)> {
    let access = format!("MOKYU{}", &crate::cli::random_secret()?[..24]);
    let secret = crate::cli::random_secret()?;
    let encrypted = codec::protect(secret.as_bytes(), key, access.as_bytes())?;
    Ok((CredentialMaterial { access, encrypted }, secret))
}
