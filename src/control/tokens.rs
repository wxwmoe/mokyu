use super::Identity;
use crate::model::credential::TokenMaterial;
pub(crate) use crate::model::credential::{Token, TokenInput, TokenSecret};
use crate::{app::App, authorization::Principal, http::HttpError};
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

pub(crate) async fn create_token(
    app: &App,
    principal: &Principal,
    user: Uuid,
    input: TokenInput,
) -> Result<TokenSecret> {
    let secret = format!("mky_{}", crate::cli::random_secret()?);
    let material = TokenMaterial {
        prefix: secret[..12].to_owned(),
        hash: *blake3::hash(secret.as_bytes()).as_bytes(),
    };
    let token = app
        .credentials
        .create_token(principal, user, input, material)
        .await?;
    Ok(TokenSecret { token, secret })
}
pub(crate) async fn update_token(
    app: &App,
    principal: &Principal,
    user: Uuid,
    id: Uuid,
    input: TokenInput,
) -> Result<Token> {
    app.credentials
        .update_token(principal, user, id, input)
        .await
}
pub(crate) async fn revoke_token(app: &App, principal: &Principal, id: Uuid) -> Result<()> {
    app.credentials.revoke_token(principal, id).await
}
#[derive(Deserialize, IntoParams)]
pub(super) struct TokenQuery {
    user: Option<Uuid>,
    after: Option<Uuid>,
    limit: Option<i64>,
}
#[derive(Serialize, ToSchema)]
pub(super) struct TokenPage {
    #[serde(rename = "items")]
    tokens: Vec<Token>,
    #[serde(rename = "next_cursor")]
    next: Option<Uuid>,
}
pub(crate) async fn list_tokens(
    store: &dyn crate::model::CredentialStore,
    user: Uuid,
    after: Option<Uuid>,
    limit: i64,
) -> Result<Vec<Token>> {
    store.tokens(user, after, limit).await
}
#[utoipa::path(get,path="/api/v1/api-tokens",operation_id="tokens_list",params(TokenQuery),responses((status=200,body=TokenPage)))]
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
    let mut tokens = list_tokens(app.credentials.as_ref(), user, q.after, limit + 1).await?;
    let more = tokens.len() > limit as usize;
    tokens.truncate(limit as usize);
    let next = more.then(|| tokens.last().unwrap().id);
    Ok(Json(TokenPage { tokens, next }))
}
#[utoipa::path(post,path="/api/v1/api-tokens",operation_id="tokens_create",request_body=TokenInput,responses((status=201,body=TokenSecret)))]
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
#[utoipa::path(put,path="/api/v1/api-tokens/{id}",operation_id="tokens_update",params(("id"=Uuid,Path)),request_body=TokenInput,responses((status=200,body=Token)))]
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
#[utoipa::path(delete,path="/api/v1/api-tokens/{id}",operation_id="tokens_revoke",params(("id"=Uuid,Path)),responses((status=204)))]
pub(super) async fn revoke(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, HttpError> {
    actor.recent(&app).await?;
    revoke_token(&app, &actor.principal, id).await?;
    Ok(StatusCode::NO_CONTENT)
}
