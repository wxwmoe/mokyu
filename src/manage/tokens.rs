use super::{Identity, keys, users::BucketGrant};
use crate::{app::App, authorization::Principal, http::HttpError};
use anyhow::Result;
use axum::{
    Json,
    extract::{Extension, Path, Query, State},
    http::StatusCode,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, Postgres, Transaction};
use std::sync::Arc;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

#[derive(Serialize, sqlx::FromRow, ToSchema)]
pub(crate) struct Token {
    pub id: Uuid,
    pub user_id: Uuid,
    pub label: String,
    pub prefix: String,
    pub system: bool,
    pub active: bool,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
    #[schema(value_type=Vec<BucketGrant>)]
    pub grants: sqlx::types::Json<Vec<BucketGrant>>,
}
pub(crate) async fn get_token(db: &mut PgConnection, id: Uuid) -> Result<Token> {
    Ok(sqlx::query_as("SELECT t.id,t.user_id,t.label,t.prefix,t.system,t.created_at,t.expires_at,t.last_used_at,t.revoked_at,u.enabled AND NOT u.must_change_password AND t.auth_revision=u.auth_revision AND t.revoked_at IS NULL AND (t.expires_at IS NULL OR t.expires_at>now()) AS active,COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket_id',g.bucket_id,'actions',g.actions) ORDER BY g.bucket_id) FROM token_grants g WHERE g.token_id=t.id),'[]'::jsonb) AS grants FROM api_tokens t JOIN web_users u ON u.id=t.user_id WHERE t.id=$1")
        .bind(id).fetch_optional(db).await?.ok_or_else(||s3s::s3_error!(NoSuchKey))?)
}
fn default_expiry() -> Option<String> {
    Some("90d".into())
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct TokenInput {
    pub label: String,
    #[serde(default)]
    pub system: bool,
    #[serde(default = "default_expiry")]
    pub expires_in: Option<String>,
    #[serde(default)]
    pub keep_expiry: bool,
    #[serde(default)]
    pub grants: Vec<BucketGrant>,
}
#[derive(Serialize, ToSchema)]
pub(crate) struct TokenSecret {
    pub token: Token,
    pub secret: String,
}
async fn owner<'a>(
    app: &'a App,
    principal: &Principal,
    user: Uuid,
    other: bool,
) -> Result<Transaction<'a, Postgres>> {
    let mut tx = app.db.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(super::account::USER_LOCK)
        .execute(&mut *tx)
        .await?;
    let admin = principal.lock_identity(&mut tx).await?;
    if !matches!(principal, Principal::Local)
        && principal.user_id() != Some(user)
        && !(other && admin)
    {
        return Err(s3s::s3_error!(AccessDenied).into());
    }
    sqlx::query("SELECT id FROM web_users WHERE id=$1 FOR SHARE")
        .bind(user)
        .execute(&mut *tx)
        .await?;
    Ok(tx)
}
async fn scope(tx: &mut Transaction<'_, Postgres>, user: Uuid, input: &TokenInput) -> Result<i64> {
    keys::label(&input.label)?;
    keys::validate_grants(&input.grants)?;
    let (role,revision): (String,i64)=sqlx::query_as("SELECT role,auth_revision FROM web_users WHERE id=$1 AND enabled AND NOT must_change_password")
        .bind(user).fetch_optional(&mut **tx).await?.ok_or_else(||s3s::s3_error!(AccessDenied))?;
    if input.system && (role != "admin" || !input.grants.is_empty()) {
        return Err(s3s::s3_error!(AccessDenied).into());
    }
    let mut grants: Vec<_> = input.grants.iter().collect();
    grants.sort_by_key(|g| g.bucket_id);
    for grant in grants {
        let exists: Option<Uuid> =
            sqlx::query_scalar("SELECT id FROM buckets WHERE id=$1 FOR SHARE")
                .bind(grant.bucket_id)
                .fetch_optional(&mut **tx)
                .await?;
        if exists.is_none() {
            return Err(s3s::s3_error!(AccessDenied).into());
        }
        if role != "admin" {
            let actions: Option<Vec<String>> = sqlx::query_scalar(
                "SELECT actions FROM user_bucket_access WHERE user_id=$1 AND bucket_id=$2",
            )
            .bind(user)
            .bind(grant.bucket_id)
            .fetch_optional(&mut **tx)
            .await?;
            if grant.actions.iter().any(|a| {
                !actions
                    .as_ref()
                    .is_some_and(|list| list.iter().any(|v| v == a.name()))
            }) {
                return Err(s3s::s3_error!(AccessDenied).into());
            }
        }
    }
    Ok(revision)
}
async fn grants(
    tx: &mut Transaction<'_, Postgres>,
    id: Uuid,
    grants: Vec<BucketGrant>,
) -> Result<()> {
    sqlx::query("DELETE FROM token_grants WHERE token_id=$1")
        .bind(id)
        .execute(&mut **tx)
        .await?;
    for grant in grants {
        sqlx::query("INSERT INTO token_grants(token_id,bucket_id,actions) VALUES($1,$2,$3)")
            .bind(id)
            .bind(grant.bucket_id)
            .bind(grant.actions.iter().map(|a| a.name()).collect::<Vec<_>>())
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}
pub(crate) async fn create_token(
    app: &App,
    principal: &Principal,
    user: Uuid,
    input: TokenInput,
) -> Result<TokenSecret> {
    if input.keep_expiry {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let expires = keys::expiration(input.expires_in.as_deref())?;
    let mut tx = owner(app, principal, user, false).await?;
    let revision = scope(&mut tx, user, &input).await?;
    let id = Uuid::new_v4();
    let secret = format!("mky_{}", crate::admin::random_secret()?);
    let hash = blake3::hash(secret.as_bytes());
    sqlx::query("INSERT INTO api_tokens(id,user_id,label,prefix,token_hash,system,auth_revision,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8)")
        .bind(id).bind(user).bind(input.label.trim()).bind(&secret[..12]).bind(hash.as_bytes().as_slice()).bind(input.system).bind(revision).bind(expires).execute(&mut *tx).await?;
    grants(&mut tx, id, input.grants).await?;
    let token = get_token(&mut tx, id).await?;
    tx.commit().await?;
    Ok(TokenSecret { token, secret })
}
pub(crate) async fn update_token(
    app: &App,
    principal: &Principal,
    user: Uuid,
    id: Uuid,
    input: TokenInput,
) -> Result<Token> {
    let expires = keys::expiration(input.expires_in.as_deref())?;
    if input.keep_expiry && expires.is_some() {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let mut tx = owner(app, principal, user, false).await?;
    sqlx::query("SELECT id FROM api_tokens WHERE id=$1 FOR UPDATE")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    let current = get_token(&mut tx, id).await?;
    if current.user_id != user || current.revoked_at.is_some() {
        return Err(s3s::s3_error!(AccessDenied).into());
    }
    scope(&mut tx, user, &input).await?;
    sqlx::query("UPDATE api_tokens SET label=$2,system=$3,expires_at=CASE WHEN $5 THEN expires_at ELSE $4 END WHERE id=$1").bind(id).bind(input.label.trim()).bind(input.system).bind(expires).bind(input.keep_expiry).execute(&mut *tx).await?;
    grants(&mut tx, id, input.grants).await?;
    let result = get_token(&mut tx, id).await?;
    tx.commit().await?;
    Ok(result)
}
pub(crate) async fn revoke_token(app: &App, principal: &Principal, id: Uuid) -> Result<()> {
    let user: Uuid = sqlx::query_scalar("SELECT user_id FROM api_tokens WHERE id=$1")
        .bind(id)
        .fetch_optional(&app.db)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    let mut tx = owner(app, principal, user, true).await?;
    sqlx::query("UPDATE api_tokens SET revoked_at=COALESCE(revoked_at,now()) WHERE id=$1")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}
#[derive(Deserialize, IntoParams)]
pub(super) struct TokenQuery {
    user: Option<Uuid>,
    after: Option<Uuid>,
    limit: Option<i64>,
}
#[derive(Serialize, ToSchema)]
pub(super) struct TokenPage {
    tokens: Vec<Token>,
    next: Option<Uuid>,
}
pub(crate) async fn list_tokens(
    db: &sqlx::PgPool,
    user: Uuid,
    after: Option<Uuid>,
    limit: i64,
) -> Result<Vec<Token>> {
    let rows: Vec<Token>=sqlx::query_as("SELECT t.id,t.user_id,t.label,t.prefix,t.system,t.created_at,t.expires_at,t.last_used_at,t.revoked_at,u.enabled AND NOT u.must_change_password AND t.auth_revision=u.auth_revision AND t.revoked_at IS NULL AND (t.expires_at IS NULL OR t.expires_at>now()) AS active,COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket_id',g.bucket_id,'actions',g.actions) ORDER BY g.bucket_id) FROM token_grants g WHERE g.token_id=t.id),'[]'::jsonb) AS grants FROM api_tokens t JOIN web_users u ON u.id=t.user_id WHERE t.user_id=$1 AND ($2::uuid IS NULL OR t.id>$2) ORDER BY t.id LIMIT $3")
        .bind(user).bind(after).bind(limit).fetch_all(db).await?;
    Ok(rows)
}
#[utoipa::path(get,path="/api/tokens",operation_id="tokens_list",params(TokenQuery),responses((status=200,body=TokenPage)))]
pub(super) async fn list(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Query(q): Query<TokenQuery>,
) -> Result<Json<TokenPage>, HttpError> {
    let user = q.user.unwrap_or(actor.id);
    let limit = q.limit.unwrap_or(50);
    if user != actor.id && !actor.admin {
        return Err(s3s::s3_error!(AccessDenied).into());
    }
    if !(1..=100).contains(&limit) {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let mut tokens = list_tokens(&app.db, user, q.after, limit + 1).await?;
    let more = tokens.len() > limit as usize;
    tokens.truncate(limit as usize);
    let next = more.then(|| tokens.last().unwrap().id);
    Ok(Json(TokenPage { tokens, next }))
}
#[utoipa::path(post,path="/api/tokens",operation_id="tokens_create",request_body=TokenInput,responses((status=201,body=TokenSecret)))]
pub(super) async fn create(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<TokenInput>,
) -> Result<(StatusCode, Json<TokenSecret>), HttpError> {
    actor.recent(&app).await?;
    Ok((
        StatusCode::CREATED,
        Json(create_token(&app, &actor.principal, actor.id, input).await?),
    ))
}
#[utoipa::path(put,path="/api/tokens/{id}",operation_id="tokens_update",params(("id"=Uuid,Path)),request_body=TokenInput,responses((status=200,body=Token)))]
pub(super) async fn update(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
    Json(input): Json<TokenInput>,
) -> Result<Json<Token>, HttpError> {
    actor.recent(&app).await?;
    Ok(Json(
        update_token(&app, &actor.principal, actor.id, id, input).await?,
    ))
}
#[utoipa::path(delete,path="/api/tokens/{id}",operation_id="tokens_revoke",params(("id"=Uuid,Path)),responses((status=204)))]
pub(super) async fn revoke(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, HttpError> {
    actor.recent(&app).await?;
    revoke_token(&app, &actor.principal, id).await?;
    Ok(StatusCode::NO_CONTENT)
}
