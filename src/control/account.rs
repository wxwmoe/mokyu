use super::{Identity, operations, origin, token};
use crate::{
    app::App,
    http::{HttpError, unauthorized},
};
use anyhow::Result;
use axum::{
    Json,
    extract::{Extension, Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use utoipa::ToSchema;
use uuid::Uuid;

pub(crate) use crate::model::account::{Preferences, Profile, Session};

pub(super) async fn profile(app: &App, id: Uuid) -> Result<Profile, HttpError> {
    let mut row = app.accounts.profile(id).await?.ok_or_else(unauthorized)?;
    if row.avatar_enabled && !row.avatar_email.is_empty() {
        let hash = hex::encode(Sha256::digest(
            row.avatar_email.trim().to_lowercase().as_bytes(),
        ));
        row.avatar_url = Some(format!(
            "{}/{hash}?s=128&r=g&d=404",
            app.config.web.gravatar_base_url.trim_end_matches('/')
        ));
    }
    Ok(row)
}

impl Preferences {
    fn validate(&self) -> Result<(), HttpError> {
        let email = self.avatar_email.trim();
        if self.display_name.len() > 240
            || self.display_name.chars().any(char::is_control)
            || !matches!(self.locale.as_deref(), None | Some("en" | "zh-CN" | "ja"))
            || !matches!(
                self.theme.as_deref(),
                None | Some("auto" | "light" | "dark")
            )
            || email.len() > 320
            || email.chars().any(|c| c.is_whitespace() || c.is_control())
            || (!email.is_empty()
                && (email.matches('@').count() != 1
                    || email.starts_with('@')
                    || email.ends_with('@')))
            || (self.avatar_enabled && email.is_empty())
        {
            return Err(s3s::s3_error!(InvalidArgument).into());
        }
        Ok(())
    }
}

#[utoipa::path(get, path="/api/v1/account", responses((status=200, body=Profile)))]
pub(super) async fn me(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<Profile>, HttpError> {
    let id = actor.id;
    Ok(Json(profile(&app, id).await?))
}
#[utoipa::path(put, path="/api/v1/account", request_body=Preferences, responses((status=200, body=Profile)))]
pub(super) async fn save(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    headers: HeaderMap,
    Json(input): Json<Preferences>,
) -> Result<Json<Profile>, HttpError> {
    let id = actor.id;
    input.validate()?;
    app.accounts
        .preferences(
            id,
            blake3::hash(token(&headers)?.as_bytes()).as_bytes(),
            input,
        )
        .await?;
    Ok(Json(profile(&app, id).await?))
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct Password {
    current_password: String,
    new_password: String,
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct Reauthenticate {
    password: String,
}

async fn verify(app: &App, id: Uuid, password: String) -> Result<(Uuid, String, i64), HttpError> {
    if password.len() > 1024 {
        return Err(unauthorized());
    }
    let (hash, revision) = app.accounts.password(id).await?.ok_or_else(unauthorized)?;
    if !operations::password_matches(hash.clone(), password).await? {
        return Err(unauthorized());
    }
    Ok((id, hash, revision))
}
#[utoipa::path(post, path="/api/v1/account/password", request_body=Password, responses((status=204)))]
pub(super) async fn password(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    headers: HeaderMap,
    Json(input): Json<Password>,
) -> Result<axum::response::Response, HttpError> {
    if !(12..=1024).contains(&input.new_password.len()) {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let verified = verify(&app, actor.id, input.current_password).await?;
    let hash = operations::password_hash(input.new_password).await?;
    let refresh = crate::cli::random_secret()?;
    app.accounts
        .change_password(
            &verified,
            blake3::hash(token(&headers)?.as_bytes()).as_bytes(),
            &hash,
            blake3::hash(refresh.as_bytes()).as_bytes(),
        )
        .await?;
    let mut response = StatusCode::NO_CONTENT.into_response();
    super::auth::set_refresh_header(&mut response, &refresh)?;
    Ok(response)
}

#[utoipa::path(post, path="/api/v1/account/reauth", request_body=Reauthenticate, responses((status=204)))]
pub(super) async fn reauthenticate(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    headers: HeaderMap,
    Json(input): Json<Reauthenticate>,
) -> Result<axum::response::Response, HttpError> {
    let verified = verify(&app, actor.id, input.password).await?;
    let refresh = crate::cli::random_secret()?;
    app.accounts
        .reauthenticate(
            &verified,
            blake3::hash(token(&headers)?.as_bytes()).as_bytes(),
            blake3::hash(refresh.as_bytes()).as_bytes(),
        )
        .await?;
    let mut response = StatusCode::NO_CONTENT.into_response();
    super::auth::set_refresh_header(&mut response, &refresh)?;
    Ok(response)
}

#[derive(Serialize, ToSchema)]
pub(super) struct SessionList {
    sessions: Vec<Session>,
    more: bool,
}

#[utoipa::path(get, path="/api/v1/account/sessions", responses((status=200, body=SessionList)))]
pub(super) async fn sessions(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    headers: HeaderMap,
) -> Result<Json<SessionList>, HttpError> {
    let id = actor.id;
    let mut rows = app
        .accounts
        .sessions(id, blake3::hash(token(&headers)?.as_bytes()).as_bytes())
        .await?;
    let more = rows.len() > 100;
    rows.truncate(100);
    Ok(Json(SessionList {
        sessions: rows,
        more,
    }))
}
#[utoipa::path(delete, path="/api/v1/account/sessions/{id}", params(("id"=Uuid, Path)), responses((status=204)))]
pub(super) async fn revoke_session(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<axum::response::Response, HttpError> {
    app.accounts
        .revoke_sessions(
            actor.id,
            blake3::hash(token(&headers)?.as_bytes()).as_bytes(),
            Some(id),
        )
        .await?;
    let mut response = StatusCode::NO_CONTENT.into_response();
    if matches!(actor.principal, crate::authorization::Principal::User { session, .. } if session == id)
    {
        super::auth::clear_auth_cookies(&mut response)?;
    }
    Ok(response)
}
#[utoipa::path(delete, path="/api/v1/account/sessions", responses((status=204)))]
pub(super) async fn revoke_others(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    headers: HeaderMap,
) -> Result<StatusCode, HttpError> {
    app.accounts
        .revoke_sessions(
            actor.id,
            blake3::hash(token(&headers)?.as_bytes()).as_bytes(),
            None,
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn initialize(app: &App) -> Result<()> {
    app.accounts.initialize().await
}

pub(crate) async fn setup_token(app: &App) -> Result<String> {
    let token = crate::cli::random_secret()?;
    app.accounts
        .setup_token(blake3::hash(token.as_bytes()).as_bytes())
        .await?;
    Ok(token)
}

pub(crate) async fn initialize_admin(
    app: &App,
    username: String,
    password: String,
    require_change: bool,
) -> Result<Uuid> {
    anyhow::ensure!(
        !username.is_empty() && username.len() <= 64 && username.chars().all(|c| !c.is_control()),
        "username must be 1..64 characters without control characters"
    );
    let hash = operations::password_hash(password).await?;
    app.accounts
        .initialize_admin(&username, &hash, require_change)
        .await
}

#[derive(Serialize, ToSchema)]
pub(super) struct Bootstrap {
    setup_required: bool,
}
#[utoipa::path(get, path="/api/v1/auth/bootstrap", responses((status=200, body=Bootstrap)))]
pub(super) async fn bootstrap(State(app): State<Arc<App>>) -> Result<Json<Bootstrap>, HttpError> {
    let setup_required = app.accounts.setup_required().await?;
    Ok(Json(Bootstrap { setup_required }))
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct Setup {
    token: String,
    username: String,
    password: String,
}
#[utoipa::path(post, path="/api/v1/auth/setup", request_body=Setup, responses((status=201)))]
pub(super) async fn setup(
    State(app): State<Arc<App>>,
    Extension(context): Extension<crate::status::stats::RequestContext>,
    headers: HeaderMap,
    Json(input): Json<Setup>,
) -> Result<StatusCode, HttpError> {
    origin(&app, &headers)?;
    if input.token.len() != 64
        || input.username.is_empty()
        || input.username.len() > 64
        || input.username.chars().any(char::is_control)
        || !(12..=1024).contains(&input.password.len())
    {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let expected = app.accounts.setup_hash().await?;
    let supplied = blake3::hash(input.token.as_bytes());
    use subtle::ConstantTimeEq;
    if !expected.is_some_and(|expected| bool::from(expected.ct_eq(supplied.as_bytes()))) {
        return Err(unauthorized());
    }
    let hash = operations::password_hash(input.password).await?;
    app.accounts
        .setup(&input.username, &hash, supplied.as_bytes(), &context.id)
        .await?;
    Ok(StatusCode::CREATED)
}
