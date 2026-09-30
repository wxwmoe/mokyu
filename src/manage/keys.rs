use super::{
    Identity,
    users::{self, BucketGrant},
};
use crate::{app::App, authorization::Principal, codec, http::HttpError};
use anyhow::Result;
use axum::{
    Json,
    extract::{Extension, Path, Query, State},
    http::StatusCode,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, Postgres, Transaction};
use std::{collections::HashSet, sync::Arc};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

pub(super) fn label(value: &str) -> Result<()> {
    if value.trim().is_empty() || value.len() > 128 || value.chars().any(char::is_control) {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    Ok(())
}
pub(super) fn expiration(value: Option<&str>) -> Result<Option<DateTime<Utc>>> {
    value
        .map(|value| {
            let seconds =
                crate::config::seconds(value).map_err(|_| s3s::s3_error!(InvalidArgument))?;
            if seconds == 0 || seconds > 3650 * 86400 {
                return Err(s3s::s3_error!(InvalidArgument).into());
            }
            Ok(Utc::now() + chrono::Duration::seconds(seconds as i64))
        })
        .transpose()
}
pub(super) fn validate_grants(grants: &[BucketGrant]) -> Result<()> {
    let mut ids = HashSet::new();
    if grants.len() > 1000
        || grants.iter().any(|g| {
            !ids.insert(g.bucket_id)
                || g.actions.is_empty()
                || g.actions.len() > 7
                || g.actions
                    .iter()
                    .enumerate()
                    .any(|(i, a)| g.actions[..i].contains(a))
        })
    {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    Ok(())
}
#[derive(Serialize, sqlx::FromRow, ToSchema)]
pub(crate) struct Credential {
    pub access_key: String,
    pub project_id: Uuid,
    pub label: String,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub created_by: Option<Uuid>,
    #[schema(value_type=Vec<BucketGrant>)]
    pub grants: sqlx::types::Json<Vec<BucketGrant>>,
}
pub(crate) async fn get_credential(
    connection: &mut PgConnection,
    access: &str,
) -> Result<Credential> {
    Ok(sqlx::query_as("SELECT c.*,COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket_id',g.bucket_id,'actions',g.actions) ORDER BY g.bucket_id) FROM grants g WHERE g.access_key=c.access_key),'[]'::jsonb) AS grants FROM credentials c WHERE c.access_key=$1")
        .bind(access).fetch_optional(connection).await?.ok_or_else(||s3s::s3_error!(NoSuchKey))?)
}
#[derive(Serialize, ToSchema)]
pub(crate) struct CredentialSecret {
    pub access_key: String,
    pub secret_key: String,
    pub credential: Credential,
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct CredentialInput {
    pub project_id: Uuid,
    pub label: String,
    pub expires_in: Option<String>,
    pub grants: Vec<BucketGrant>,
}
async fn replace_grants(
    tx: &mut Transaction<'_, Postgres>,
    access: &str,
    project: Uuid,
    grants: Vec<BucketGrant>,
) -> Result<()> {
    validate_grants(&grants)?;
    let mut ids: Vec<_> = grants.iter().map(|g| g.bucket_id).collect();
    ids.sort();
    let found: Vec<Uuid> = sqlx::query_scalar(
        "SELECT id FROM buckets WHERE id=ANY($1) AND project_id=$2 ORDER BY id FOR SHARE",
    )
    .bind(&ids)
    .bind(project)
    .fetch_all(&mut **tx)
    .await?;
    if found != ids {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    sqlx::query("DELETE FROM grants WHERE access_key=$1")
        .bind(access)
        .execute(&mut **tx)
        .await?;
    for grant in grants {
        sqlx::query("INSERT INTO grants(access_key,bucket_id,actions) VALUES($1,$2,$3)")
            .bind(access)
            .bind(grant.bucket_id)
            .bind(grant.actions.iter().map(|a| a.name()).collect::<Vec<_>>())
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}
async fn insert(
    app: &App,
    tx: &mut Transaction<'_, Postgres>,
    principal: &Principal,
    input: CredentialInput,
) -> Result<CredentialSecret> {
    label(&input.label)?;
    validate_grants(&input.grants)?;
    let expires = expiration(input.expires_in.as_deref())?;
    let project: Option<Uuid> = sqlx::query_scalar("SELECT id FROM projects WHERE id=$1 FOR SHARE")
        .bind(input.project_id)
        .fetch_optional(&mut **tx)
        .await?;
    if project.is_none() {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let access = format!("MOKYU{}", &crate::admin::random_secret()?[..24]);
    let secret = crate::admin::random_secret()?;
    let protected = codec::protect(
        secret.as_bytes(),
        &app.secrets.credential_key,
        access.as_bytes(),
    )?;
    sqlx::query("INSERT INTO credentials(access_key,secret_encrypted,project_id,label,expires_at,created_by) VALUES($1,$2,$3,$4,$5,$6)").bind(&access).bind(protected).bind(input.project_id).bind(input.label.trim()).bind(expires).bind(principal.user_id()).execute(&mut **tx).await?;
    replace_grants(tx, &access, input.project_id, input.grants).await?;
    let credential = get_credential(tx, &access).await?;
    Ok(CredentialSecret {
        access_key: access,
        secret_key: secret,
        credential,
    })
}
pub(crate) async fn create_credential(
    app: &App,
    principal: &Principal,
    input: CredentialInput,
) -> Result<CredentialSecret> {
    let mut tx = users::admin_transaction(app, principal).await?;
    let row = insert(app, &mut tx, principal, input).await?;
    super::audit::checkpoint(&mut tx,"credential.create",&row.access_key,serde_json::json!({"project_id":row.credential.project_id,"label":row.credential.label,"grants":row.credential.grants,"expires_at":row.credential.expires_at})).await?;
    tx.commit().await?;
    Ok(row)
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct CredentialSettings {
    pub label: String,
    pub enabled: bool,
    pub expires_in: Option<String>,
    #[serde(default)]
    pub keep_expiry: bool,
    pub grants: Option<Vec<BucketGrant>>,
}
pub(crate) async fn update_credential(
    app: &App,
    principal: &Principal,
    access: &str,
    input: CredentialSettings,
) -> Result<Credential> {
    label(&input.label)?;
    let expires = expiration(input.expires_in.as_deref())?;
    if input.keep_expiry && expires.is_some() {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let mut tx = users::admin_transaction(app, principal).await?;
    let changed=sqlx::query("UPDATE credentials SET label=$2,enabled=$3,expires_at=CASE WHEN $5 THEN expires_at ELSE $4 END WHERE access_key=$1").bind(access).bind(input.label.trim()).bind(input.enabled).bind(expires).bind(input.keep_expiry).execute(&mut *tx).await?.rows_affected();
    if changed == 0 {
        return Err(s3s::s3_error!(NoSuchKey).into());
    }
    if let Some(grants) = input.grants {
        let row = get_credential(&mut tx, access).await?;
        replace_grants(&mut tx, access, row.project_id, grants).await?;
    }
    let row = get_credential(&mut tx, access).await?;
    super::audit::checkpoint(&mut tx,"credential.update",access,serde_json::json!({"project_id":row.project_id,"label":row.label,"enabled":row.enabled,"grants":row.grants,"expires_at":row.expires_at})).await?;
    tx.commit().await?;
    Ok(row)
}
pub(crate) async fn set_grants(
    app: &App,
    principal: &Principal,
    access: &str,
    grants: Vec<BucketGrant>,
) -> Result<Credential> {
    let mut tx = users::admin_transaction(app, principal).await?;
    let project =
        sqlx::query_scalar("SELECT project_id FROM credentials WHERE access_key=$1 FOR UPDATE")
            .bind(access)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    replace_grants(&mut tx, access, project, grants).await?;
    let row = get_credential(&mut tx, access).await?;
    super::audit::checkpoint(
        &mut tx,
        "credential.permissions",
        access,
        serde_json::json!({"project_id":row.project_id,"grants":row.grants}),
    )
    .await?;
    tx.commit().await?;
    Ok(row)
}
pub(crate) async fn revoke_credential(
    app: &App,
    principal: &Principal,
    access: &str,
) -> Result<()> {
    let mut tx = users::admin_transaction(app, principal).await?;
    sqlx::query("DELETE FROM credentials WHERE access_key=$1")
        .bind(access)
        .execute(&mut *tx)
        .await?;
    super::audit::checkpoint(&mut tx, "credential.delete", access, serde_json::json!({})).await?;
    tx.commit().await?;
    Ok(())
}
pub(crate) async fn enable_credential(
    app: &App,
    principal: &Principal,
    access: &str,
    enabled: bool,
) -> Result<()> {
    let mut tx = users::admin_transaction(app, principal).await?;
    let n = sqlx::query("UPDATE credentials SET enabled=$2 WHERE access_key=$1")
        .bind(access)
        .bind(enabled)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    if n == 0 {
        return Err(s3s::s3_error!(NoSuchKey).into());
    }
    super::audit::checkpoint(
        &mut tx,
        "credential.enabled",
        access,
        serde_json::json!({"enabled":enabled}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}
pub(crate) async fn change_grant(
    app: &App,
    principal: &Principal,
    access: &str,
    bucket: Uuid,
    actions: Vec<crate::authorization::Action>,
) -> Result<()> {
    let mut tx = users::admin_transaction(app, principal).await?;
    sqlx::query("SELECT access_key FROM credentials WHERE access_key=$1 FOR UPDATE")
        .bind(access)
        .execute(&mut *tx)
        .await?;
    let current = get_credential(&mut tx, access).await?;
    let mut grants = current.grants.0;
    grants.retain(|g| g.bucket_id != bucket);
    if !actions.is_empty() {
        grants.push(BucketGrant {
            bucket_id: bucket,
            actions,
        });
    }
    replace_grants(&mut tx, access, current.project_id, grants).await?;
    let row = get_credential(&mut tx, access).await?;
    super::audit::checkpoint(
        &mut tx,
        "credential.permissions",
        access,
        serde_json::json!({"project_id":row.project_id,"grants":row.grants}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct RotateCredential {
    pub overlap: String,
    pub expires_in: Option<String>,
}
#[derive(Serialize, ToSchema)]
pub(crate) struct CredentialRotation {
    #[serde(flatten)]
    pub replacement: CredentialSecret,
    pub old_expires_at: DateTime<Utc>,
}
pub(crate) async fn rotate_credential(
    app: &App,
    principal: &Principal,
    access: &str,
    input: RotateCredential,
) -> Result<CredentialRotation> {
    let seconds =
        crate::config::seconds(&input.overlap).map_err(|_| s3s::s3_error!(InvalidArgument))?;
    if seconds > 30 * 86400 {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let mut tx = users::admin_transaction(app, principal).await?;
    sqlx::query("SELECT access_key FROM credentials WHERE access_key=$1 FOR UPDATE")
        .bind(access)
        .execute(&mut *tx)
        .await?;
    let current = get_credential(&mut tx, access).await?;
    if !current.enabled {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let old_expires_at = current
        .expires_at
        .unwrap_or(Utc::now() + chrono::Duration::days(30))
        .min(Utc::now() + chrono::Duration::seconds(seconds as i64));
    let replacement = insert(
        app,
        &mut tx,
        principal,
        CredentialInput {
            project_id: current.project_id,
            label: current.label,
            expires_in: input.expires_in,
            grants: current.grants.0,
        },
    )
    .await?;
    sqlx::query("UPDATE credentials SET expires_at=$2 WHERE access_key=$1")
        .bind(access)
        .bind(old_expires_at)
        .execute(&mut *tx)
        .await?;
    super::audit::checkpoint(&mut tx,"credential.rotate",access,serde_json::json!({"project_id":replacement.credential.project_id,"replacement_key":replacement.access_key,"old_expires_at":old_expires_at})).await?;
    tx.commit().await?;
    Ok(CredentialRotation {
        replacement,
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
    credentials: Vec<Credential>,
    next: Option<String>,
}
pub(crate) async fn list_credentials(
    db: &sqlx::PgPool,
    project: Option<Uuid>,
    after: &str,
    limit: i64,
) -> Result<Vec<Credential>> {
    Ok(sqlx::query_as("SELECT c.*,COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket_id',g.bucket_id,'actions',g.actions) ORDER BY g.bucket_id) FROM grants g WHERE g.access_key=c.access_key),'[]'::jsonb) AS grants FROM credentials c WHERE ($1::uuid IS NULL OR c.project_id=$1) AND c.access_key>$2 ORDER BY c.access_key LIMIT $3")
        .bind(project).bind(after).bind(limit).fetch_all(db).await?)
}
#[utoipa::path(get,path="/api/credentials",operation_id="credentials_list",params(CredentialQuery),responses((status=200,body=CredentialPage)))]
pub(super) async fn list(
    State(app): State<Arc<App>>,
    Query(q): Query<CredentialQuery>,
) -> Result<Json<CredentialPage>, HttpError> {
    let limit = q.limit.unwrap_or(50);
    if !(1..=100).contains(&limit) || q.after.as_ref().is_some_and(|s| s.len() > 128) {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let mut credentials =
        list_credentials(&app.db, q.project, &q.after.unwrap_or_default(), limit + 1).await?;
    let more = credentials.len() > limit as usize;
    credentials.truncate(limit as usize);
    let next = more.then(|| credentials.last().unwrap().access_key.clone());
    Ok(Json(CredentialPage { credentials, next }))
}
#[utoipa::path(post,path="/api/credentials",operation_id="credentials_create",request_body=CredentialInput,responses((status=201,body=CredentialSecret)))]
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
#[utoipa::path(put,path="/api/credentials/{key}",operation_id="credentials_update",params(("key"=String,Path)),request_body=CredentialSettings,responses((status=200,body=Credential)))]
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
#[utoipa::path(delete,path="/api/credentials/{key}",operation_id="credentials_revoke",params(("key"=String,Path)),responses((status=204)))]
pub(super) async fn revoke(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(key): Path<String>,
) -> Result<StatusCode, HttpError> {
    actor.recent(&app).await?;
    revoke_credential(&app, &actor.principal, &key).await?;
    Ok(StatusCode::NO_CONTENT)
}
#[utoipa::path(put,path="/api/credentials/{key}/grants",operation_id="credentials_grants",params(("key"=String,Path)),request_body=Vec<BucketGrant>,responses((status=200,body=Credential)))]
pub(super) async fn grants(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(key): Path<String>,
    Json(input): Json<Vec<BucketGrant>>,
) -> Result<Json<Credential>, HttpError> {
    actor.recent(&app).await?;
    Ok(Json(set_grants(&app, &actor.principal, &key, input).await?))
}
#[utoipa::path(post,path="/api/credentials/{key}/rotate",operation_id="credentials_rotate",params(("key"=String,Path)),request_body=RotateCredential,responses((status=201,body=CredentialRotation)))]
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
