use super::{
    account, audit,
    contract::{self, LoginReply, SessionView},
    operations,
};
use crate::{
    app::App,
    authorization::Principal,
    config,
    http::{HttpError, unauthorized},
};
use anyhow::Result;
use axum::{
    Json,
    extract::{Extension, MatchedPath, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

use crate::model::account::{AuthSession, SessionLogin, SessionRefresh, csrf_token, derived_token};

fn refresh_after(expires: DateTime<Utc>, recent: DateTime<Utc>) -> u64 {
    let now = Utc::now();
    ((expires - now).num_seconds() * 3 / 4)
        .min((recent + chrono::Duration::minutes(3) - now).num_seconds())
        .clamp(0, 180) as u64
}

fn cookie<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get("cookie")
        .and_then(|v| v.to_str().ok())
        .and_then(|cookies| {
            cookies.split(';').map(str::trim).find_map(|value| {
                let (key, value) = value.split_once('=')?;
                (key == name).then_some(value)
            })
        })
}

pub(crate) fn origin(app: &App, headers: &HeaderMap) -> Result<(), HttpError> {
    if headers.get("origin").and_then(|v| v.to_str().ok()) != Some(&app.config.web.origin) {
        return Err(unauthorized());
    }
    Ok(())
}
pub(crate) fn token(headers: &HeaderMap) -> Result<&str, HttpError> {
    cookie(headers, "mokyu_session")
        .filter(|s| s.len() == 64)
        .ok_or_else(unauthorized)
}
fn secure_cookie(app: &App) -> &'static str {
    if app.config.web.secure_cookie {
        "; Secure"
    } else {
        ""
    }
}

fn append_cookie(response: &mut Response, value: String) -> Result<(), HttpError> {
    response
        .headers_mut()
        .append("set-cookie", HeaderValue::from_str(&value)?);
    Ok(())
}

fn set_session_cookie(
    response: &mut Response,
    app: &App,
    token: &str,
    seconds: u64,
) -> Result<(), HttpError> {
    append_cookie(
        response,
        format!(
            "mokyu_session={token}; Path=/api/v1; HttpOnly; SameSite=Strict; Max-Age={seconds}{}",
            secure_cookie(app)
        ),
    )
}

pub(crate) fn set_refresh_header(response: &mut Response, token: &str) -> Result<(), HttpError> {
    response
        .headers_mut()
        .insert("x-mokyu-refresh-token", HeaderValue::from_str(token)?);
    Ok(())
}

pub(crate) fn clear_auth_cookies(response: &mut Response) -> Result<(), HttpError> {
    append_cookie(
        response,
        "mokyu_session=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0".into(),
    )?;
    append_cookie(
        response,
        "mokyu_session=; Path=/api/v1; HttpOnly; SameSite=Strict; Max-Age=0".into(),
    )?;
    append_cookie(
        response,
        "mokyu_refresh=; Path=/api/v1/auth/refresh; HttpOnly; SameSite=Strict; Max-Age=0".into(),
    )
}

#[derive(Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct RefreshInput {
    refresh_token: String,
}

#[derive(Clone)]
pub(crate) struct Identity {
    pub(super) id: Uuid,
    pub(super) admin: bool,
    pub(super) password_change: bool,
    pub(super) principal: Principal,
}
impl Identity {
    pub(crate) async fn recent(&self, app: &App) -> Result<(), HttpError> {
        if self.principal.token_id().is_some() && self.admin
            || matches!(self.principal, Principal::Local | Principal::Node { .. })
        {
            return Ok(());
        }
        let Principal::User { session, .. } = self.principal else {
            return Err(unauthorized());
        };
        let valid = app.accounts.recent(session, self.id).await?;
        if !valid {
            return Err(crate::http::problem(
                StatusCode::FORBIDDEN,
                "ReauthenticationRequired",
            ));
        }
        Ok(())
    }
}
fn member_route(path: &str, method: &Method) -> bool {
    let method = if method == Method::HEAD {
        &Method::GET
    } else {
        method
    };
    matches!(
        (method.as_str(), path),
        (
            "GET",
            "/api/v1/auth/session"
                | "/api/v1/account"
                | "/api/v1/account/sessions"
                | "/api/v1/buckets"
                | "/api/v1/usage/summary"
                | "/api/v1/usage/runtime"
                | "/api/v1/packs"
                | "/api/v1/packs/{id}"
                | "/api/v1/storage/packs"
                | "/api/v1/storage/packs/{id}"
                | "/api/v1/storage/packs/{id}/objects"
                | "/api/v1/bucket-projects"
                | "/api/v1/buckets/{bucket}/object/chunks"
                | "/api/v1/projects"
                | "/api/v1/quotas/{scope_type}/{scope_id}"
                | "/api/v1/buckets/{bucket}/objects"
                | "/api/v1/buckets/{bucket}/catalog"
                | "/api/v1/buckets/{bucket}/object"
                | "/api/v1/buckets/{bucket}/object/content"
                | "/api/v1/buckets/{bucket}/object/thumbnail"
                | "/api/v1/buckets/{bucket}/object/text"
                | "/api/v1/uploads"
                | "/api/v1/uploads/{id}"
                | "/api/v1/uploads/{id}/parts"
                | "/api/v1/api-tokens"
                | "/api/v1/audit"
                | "/api/v1/audit/export"
                | "/api/v1/audit/{id}"
        ) | (
            "POST",
            "/api/v1/auth/logout"
                | "/api/v1/account/password"
                | "/api/v1/account/reauth"
                | "/api/v1/media/actions"
                | "/api/v1/buckets"
                | "/api/v1/api-tokens"
                | "/api/v1/buckets/{bucket}/uploads"
                | "/api/v1/uploads/{id}/complete"
        ) | ("PUT", "/api/v1/account")
            | ("PUT", "/api/v1/uploads/{id}/parts/{number}")
            | ("DELETE", "/api/v1/uploads/{id}")
            | ("PUT" | "DELETE", "/api/v1/api-tokens/{id}")
            | (
                "DELETE",
                "/api/v1/account/sessions" | "/api/v1/account/sessions/{id}"
            )
            | (
                "GET" | "PUT",
                "/api/v1/buckets/{bucket}/cors"
                    | "/api/v1/buckets/{bucket}/website"
                    | "/api/v1/buckets/{bucket}/settings"
            )
    )
}
pub(super) async fn access_gate(
    State(app): State<Arc<App>>,
    mut request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let Some(path) = request
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str().to_owned())
    else {
        return next.run(request).await;
    };
    if !path.starts_with("/api/v1/")
        || matches!(
            path.as_str(),
            "/api/v1/auth/login"
                | "/api/v1/auth/refresh"
                | "/api/v1/info"
                | "/api/v1/openapi.json"
                | "/api/v1/auth/bootstrap"
                | "/api/v1/auth/setup"
                | "/api/v1/cluster/recover"
                | "/api/v1/cli"
        )
    {
        return next.run(request).await;
    }
    let write = !matches!(
        *request.method(),
        Method::GET | Method::HEAD | Method::OPTIONS
    );
    match authenticate(&app, request.headers(), write).await {
        Ok(identity)
            if identity.principal.token_id().is_some()
                && (matches!(
                    path.as_str(),
                    "/api/v1/auth/session" | "/api/v1/auth/logout" | "/api/v1/account"
                ) || path.starts_with("/api/v1/account/")
                    || path.starts_with("/api/v1/api-tokens")
                    || (path.starts_with("/api/v1/audit") && !identity.admin)) =>
        {
            unauthorized().into_response()
        }
        Ok(identity)
            if identity.password_change
                && !matches!(
                    path.as_str(),
                    "/api/v1/auth/session"
                        | "/api/v1/account"
                        | "/api/v1/account/password"
                        | "/api/v1/account/sessions"
                        | "/api/v1/account/sessions/{id}"
                        | "/api/v1/auth/logout"
                ) =>
        {
            crate::http::problem(StatusCode::FORBIDDEN, "PasswordChangeRequired").into_response()
        }
        Ok(identity) if identity.admin || member_route(&path, request.method()) => {
            let event = if write {
                let request_id = request
                    .extensions()
                    .get::<crate::status::stats::RequestContext>()
                    .map(|c| c.id.as_str());
                match audit::begin(
                    app.audit.as_ref(),
                    &identity.principal,
                    &format!("{} {}", request.method(), path),
                    request_id,
                )
                .await
                {
                    Ok(id) => Some(id),
                    Err(error) => return HttpError(error).into_response(),
                }
            } else {
                None
            };
            let context = request
                .extensions()
                .get::<crate::status::stats::RequestContext>()
                .cloned();
            request.extensions_mut().insert(identity);
            if let Some(id) = event {
                let response = audit::scope(id, next.run(request)).await;
                audit::finish(
                    app.audit.as_ref(),
                    id,
                    response.status().as_u16(),
                    context.is_some_and(|c| c.failed.load(std::sync::atomic::Ordering::Relaxed)),
                )
                .await;
                response
            } else {
                next.run(request).await
            }
        }
        Ok(identity) => {
            if write
                && let Ok(id) = audit::begin(
                    app.audit.as_ref(),
                    &identity.principal,
                    &format!("{} {}", request.method(), path),
                    request
                        .extensions()
                        .get::<crate::status::stats::RequestContext>()
                        .map(|c| c.id.as_str()),
                )
                .await
            {
                audit::finish(app.audit.as_ref(), id, 403, false).await;
            }
            unauthorized().into_response()
        }
        Err(error) => error.into_response(),
    }
}
async fn authenticate(app: &App, headers: &HeaderMap, csrf: bool) -> Result<Identity, HttpError> {
    if headers.contains_key("authorization") {
        if headers.contains_key("cookie") {
            return Err(unauthorized());
        }
        let secret = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .filter(|v| {
                v.len() == 68
                    && v.starts_with("mky_")
                    && v[4..].bytes().all(|b| b.is_ascii_hexdigit())
            })
            .ok_or_else(unauthorized)?;
        let hash = blake3::hash(secret.as_bytes());
        let (id, user, role, system, revision, user_revision) = app
            .accounts
            .authenticate_token(hash.as_bytes())
            .await?
            .ok_or_else(unauthorized)?;
        return Ok(Identity {
            id: user,
            admin: role == "admin" && system,
            password_change: false,
            principal: Principal::Token {
                id,
                user,
                revision,
                user_revision,
            },
        });
    }
    let token = token(headers)?;
    let hash = blake3::hash(token.as_bytes());
    let row = app.accounts.authenticate_session(hash.as_bytes()).await?;
    let AuthSession {
        user_id: id,
        session_id: session,
        role,
        must_change_password,
        authorization_revision: revision,
        csrf_hash: expected,
        last_seen_at: _,
    } = row.ok_or_else(unauthorized)?;
    if csrf {
        origin(app, headers)?;
        let supplied = headers
            .get("x-csrf-token")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| crate::http::problem(StatusCode::FORBIDDEN, "CsrfTokenMismatch"))?;
        let hash = blake3::hash(supplied.as_bytes());
        use subtle::ConstantTimeEq;
        if !bool::from(expected.as_slice().ct_eq(hash.as_bytes())) {
            return Err(crate::http::problem(
                StatusCode::FORBIDDEN,
                "CsrfTokenMismatch",
            ));
        }
    }
    Ok(Identity {
        id,
        admin: role == "admin",
        password_change: must_change_password,
        principal: Principal::User {
            id,
            session,
            revision,
        },
    })
}
#[derive(Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct Login {
    username: String,
    password: String,
}

#[utoipa::path(post, path = "/api/v1/auth/login", request_body = Login, responses((status = 200, body = LoginReply), (status = 403, body = contract::ErrorBody)))]
pub(super) async fn login(
    State(app): State<Arc<App>>,
    Extension(context): Extension<crate::status::stats::RequestContext>,
    headers: HeaderMap,
    Json(input): Json<Login>,
) -> Result<Response, HttpError> {
    origin(&app, &headers)?;
    if input.username.len() > 64 || input.password.len() > 1024 {
        return Err(unauthorized());
    }
    let row = app.accounts.login_user(&input.username).await?;
    let attempted = row.as_ref().map(|row| row.0);
    let started = tokio::time::Instant::now();
    let verified = if let Some((id, hash, revision)) = row {
        operations::password_matches(hash.clone(), input.password)
            .await?
            .then_some((id, hash, revision))
    } else {
        None
    };
    tokio::time::sleep_until(started + Duration::from_millis(300)).await;
    let Some((user, verified_hash, revision)) = verified else {
        if let Some(user) = attempted {
            app.accounts.failed_login(user, &context.id).await?;
        }
        return Err(unauthorized());
    };
    #[cfg(feature = "fault-injection")]
    crate::faults::point("login-verified").await;
    let refresh = crate::cli::random_secret()?;
    let token = derived_token(
        &app.secrets.credential_key.material,
        "mokyu-session-v1",
        &refresh,
    );
    let csrf = csrf_token(&token);
    let seconds = config::seconds(&app.config.web.session_lifetime)?;
    let user_agent = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .chars()
        .filter(|c| !c.is_control())
        .take(128)
        .collect::<String>();
    app.accounts
        .login(SessionLogin {
            verified: &(user, verified_hash, revision),
            access_hash: blake3::hash(token.as_bytes()).as_bytes(),
            csrf_hash: blake3::hash(csrf.as_bytes()).as_bytes(),
            refresh_hash: blake3::hash(refresh.as_bytes()).as_bytes(),
            seconds,
            user_agent: &user_agent,
            request_id: &context.id,
        })
        .await?;
    let mut response = Json(LoginReply {
        csrf_token: csrf,
        refresh_token: refresh.clone(),
    })
    .into_response();
    clear_auth_cookies(&mut response)?;
    set_session_cookie(&mut response, &app, &token, seconds)?;
    response
        .headers_mut()
        .insert("cache-control", HeaderValue::from_static("no-store"));
    Ok(response)
}
#[utoipa::path(post, path = "/api/v1/auth/logout", responses((status = 204), (status = 403, body = contract::ErrorBody)))]
pub(super) async fn logout(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    headers: HeaderMap,
) -> Result<Response, HttpError> {
    app.accounts
        .logout(
            actor.id,
            blake3::hash(token(&headers)?.as_bytes()).as_bytes(),
        )
        .await?;
    let mut r = StatusCode::NO_CONTENT.into_response();
    clear_auth_cookies(&mut r)?;
    Ok(r)
}

#[utoipa::path(post, path = "/api/v1/auth/refresh", request_body = RefreshInput, responses((status = 200, body = SessionView), (status = 403, body = contract::ErrorBody)))]
pub(super) async fn refresh(
    State(app): State<Arc<App>>,
    Extension(context): Extension<crate::status::stats::RequestContext>,
    headers: HeaderMap,
    Json(input): Json<RefreshInput>,
) -> Result<Response, HttpError> {
    origin(&app, &headers)?;
    let supplied = input.refresh_token.as_str();
    if supplied.len() != 64 || !supplied.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(unauthorized());
    }
    let seconds = config::seconds(&app.config.web.session_lifetime)?;
    let refreshed = app
        .accounts
        .refresh(SessionRefresh {
            supplied,
            access: token(&headers).ok(),
            key: &app.secrets.credential_key.material,
            seconds,
            request_id: &context.id,
        })
        .await?;
    let mut response = Json(SessionView {
        account: account::profile(&app, refreshed.user).await?,
        csrf_token: refreshed.csrf,
        session_id: refreshed.session,
        refresh_after: refresh_after(refreshed.expires, refreshed.refreshed_at),
        refresh_token: Some(refreshed.refresh),
    })
    .into_response();
    if refreshed.issue_access {
        set_session_cookie(&mut response, &app, &refreshed.access, seconds)?;
    }
    response
        .headers_mut()
        .insert("cache-control", HeaderValue::from_static("no-store"));
    Ok(response)
}
#[utoipa::path(get, path = "/api/v1/auth/session", responses((status = 200, body = SessionView), (status = 403, body = contract::ErrorBody)))]
pub(super) async fn session(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    headers: HeaderMap,
) -> Result<Json<SessionView>, HttpError> {
    let id = actor.id;
    let (session_id, expires, recent) = app
        .accounts
        .session(blake3::hash(token(&headers)?.as_bytes()).as_bytes())
        .await?
        .ok_or_else(unauthorized)?;
    Ok(Json(SessionView {
        account: account::profile(&app, id).await?,
        csrf_token: csrf_token(token(&headers)?),
        session_id,
        refresh_after: refresh_after(expires, recent),
        refresh_token: None,
    }))
}
