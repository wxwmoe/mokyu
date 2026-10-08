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
    extract::{Extension, MatchedPath, Path, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
};
use base64::Engine;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

fn csrf_token(token: &str) -> String {
    let mut hash = blake3::Hasher::new();
    hash.update(b"mokyu-web-csrf-v1\0");
    hash.update(token.as_bytes());
    hash.finalize().to_hex().to_string()
}
pub(crate) fn origin(app: &App, headers: &HeaderMap) -> Result<(), HttpError> {
    if headers.get("origin").and_then(|v| v.to_str().ok()) != Some(&app.config.web.origin) {
        return Err(unauthorized());
    }
    Ok(())
}
pub(crate) fn token(headers: &HeaderMap) -> Result<&str, HttpError> {
    headers
        .get("cookie")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| {
            s.split(';')
                .map(str::trim)
                .find_map(|s| s.strip_prefix("mokyu_session="))
        })
        .filter(|s| s.len() == 64)
        .ok_or_else(unauthorized)
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
            || matches!(self.principal, Principal::Node { .. })
        {
            return Ok(());
        }
        let Principal::User { session, .. } = self.principal else {
            return Err(unauthorized());
        };
        let valid: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM sessions WHERE id=$1 AND user_id=$2 AND reauthenticated_at>now()-interval '5 minutes')").bind(session).bind(self.id).fetch_one(&app.db).await?;
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
                    .get::<crate::stats::RequestContext>()
                    .map(|c| c.id.as_str());
                match audit::begin(
                    &app.db,
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
                .get::<crate::stats::RequestContext>()
                .cloned();
            request.extensions_mut().insert(identity);
            if let Some(id) = event {
                let response = audit::scope(id, next.run(request)).await;
                audit::finish(
                    &app.db,
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
                    &app.db,
                    &identity.principal,
                    &format!("{} {}", request.method(), path),
                    request
                        .extensions()
                        .get::<crate::stats::RequestContext>()
                        .map(|c| c.id.as_str()),
                )
                .await
            {
                audit::finish(&app.db, id, 403, false).await;
            }
            unauthorized().into_response()
        }
        Err(error) => error.into_response(),
    }
}
#[derive(sqlx::FromRow)]
struct AuthSession {
    user_id: Uuid,
    session_id: Uuid,
    role: String,
    must_change_password: bool,
    authorization_revision: i64,
    csrf_hash: Vec<u8>,
    last_seen_at: DateTime<Utc>,
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
        let row: Option<(Uuid,Uuid,String,bool,i64,i64)>=sqlx::query_as("SELECT t.id,t.user_id,u.role,t.system,t.authorization_revision,u.authorization_revision FROM api_tokens t JOIN web_users u ON u.id=t.user_id WHERE t.token_hash=$1 AND t.revoked_at IS NULL AND (t.expires_at IS NULL OR t.expires_at>now()) AND t.auth_revision=u.auth_revision AND u.enabled AND NOT u.must_change_password")
            .bind(hash.as_bytes().as_slice()).fetch_optional(&app.db).await?;
        let (id, user, role, system, revision, user_revision) = row.ok_or_else(unauthorized)?;
        sqlx::query("UPDATE api_tokens SET last_used_at=now() WHERE id=$1 AND (last_used_at IS NULL OR last_used_at<now()-interval '5 minutes')").bind(id).execute(&app.db).await?;
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
    let row: Option<AuthSession> = sqlx::query_as("SELECT u.id AS user_id,s.id AS session_id,u.role,u.must_change_password,u.authorization_revision,s.csrf_hash,s.last_seen_at FROM sessions s JOIN web_users u ON u.id=s.user_id WHERE s.token_hash=$1 AND s.expires_at>now() AND u.enabled AND s.auth_revision=u.auth_revision").bind(hash.as_bytes().as_slice()).fetch_optional(&app.db).await?;
    let AuthSession {
        user_id: id,
        session_id: session,
        role,
        must_change_password,
        authorization_revision: revision,
        csrf_hash: expected,
        last_seen_at: last_seen,
    } = row.ok_or_else(unauthorized)?;
    if (Utc::now() - last_seen).num_seconds() >= 300 {
        sqlx::query("UPDATE sessions SET last_seen_at=now() WHERE token_hash=$1 AND last_seen_at<now()-interval '5 minutes'").bind(hash.as_bytes().as_slice()).execute(&app.db).await?;
    }
    if csrf {
        origin(app, headers)?;
        let supplied = headers
            .get("x-csrf-token")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(unauthorized)?;
        let hash = blake3::hash(supplied.as_bytes());
        use subtle::ConstantTimeEq;
        if !bool::from(expected.as_slice().ct_eq(hash.as_bytes())) {
            return Err(unauthorized());
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
    Extension(context): Extension<crate::stats::RequestContext>,
    headers: HeaderMap,
    Json(input): Json<Login>,
) -> Result<Response, HttpError> {
    origin(&app, &headers)?;
    if input.username.len() > 64 || input.password.len() > 1024 {
        return Err(unauthorized());
    }
    let row: Option<(Uuid, String, i64)> = sqlx::query_as(
        "SELECT id,password_hash,auth_revision FROM web_users WHERE username=$1 AND enabled",
    )
    .bind(input.username)
    .fetch_optional(&app.db)
    .await?;
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
            let mut tx = app.db.begin().await?;
            audit::security(&mut tx, user, "session.login", false, &context.id).await?;
            tx.commit().await?;
        }
        return Err(unauthorized());
    };
    #[cfg(feature = "fault-injection")]
    crate::faults::point("login-verified").await;
    let token = crate::cli::random_secret()?;
    let csrf = csrf_token(&token);
    let seconds = config::seconds(&app.config.web.session_lifetime)?;
    let mut tx = app.db.begin().await?;
    // Password reset and disable update this same row before revoking sessions.
    let current: Option<(String, bool, i64)> = sqlx::query_as(
        "SELECT password_hash,enabled,auth_revision FROM web_users WHERE id=$1 FOR UPDATE",
    )
    .bind(user)
    .fetch_optional(&mut *tx)
    .await?;
    if !current.is_some_and(|(hash, enabled, current_revision)| {
        enabled && hash == verified_hash && current_revision == revision
    }) {
        return Err(unauthorized());
    }
    #[cfg(feature = "fault-injection")]
    crate::faults::point("login-before-session").await;
    sqlx::query("INSERT INTO sessions(token_hash,user_id,csrf_hash,expires_at,auth_revision,user_agent) VALUES($1,$2,$3,now()+$4*interval '1 second',$5,$6)").bind(blake3::hash(token.as_bytes()).as_bytes().as_slice()).bind(user).bind(blake3::hash(csrf.as_bytes()).as_bytes().as_slice()).bind(seconds as f64).bind(revision).bind(headers.get("user-agent").and_then(|v| v.to_str().ok()).unwrap_or("").chars().filter(|c| !c.is_control()).take(128).collect::<String>()).execute(&mut *tx).await?;
    audit::security(&mut tx, user, "session.login", true, &context.id).await?;
    tx.commit().await?;
    let mut response = Json(LoginReply { csrf_token: csrf }).into_response();
    let secure = if app.config.web.secure_cookie {
        "; Secure"
    } else {
        ""
    };
    response.headers_mut().insert(
        "set-cookie",
        HeaderValue::from_str(&format!(
            "mokyu_session={token}; Path=/; HttpOnly; SameSite=Strict; Max-Age={seconds}{secure}"
        ))?,
    );
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
    let id = actor.id;
    let mut tx = app.db.begin().await?;
    account::lock_session(&mut tx, &headers, id).await?;
    sqlx::query("DELETE FROM sessions WHERE token_hash=$1")
        .bind(
            blake3::hash(token(&headers)?.as_bytes())
                .as_bytes()
                .as_slice(),
        )
        .execute(&mut *tx)
        .await?;
    audit::checkpoint(&mut tx, "session.logout", &id.to_string(), json!({})).await?;
    tx.commit().await?;
    let mut r = StatusCode::NO_CONTENT.into_response();
    r.headers_mut().insert(
        "set-cookie",
        HeaderValue::from_static("mokyu_session=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0"),
    );
    Ok(r)
}
#[utoipa::path(get, path = "/api/v1/auth/session", responses((status = 200, body = SessionView), (status = 403, body = contract::ErrorBody)))]
pub(super) async fn session(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    headers: HeaderMap,
) -> Result<Json<SessionView>, HttpError> {
    let id = actor.id;
    Ok(Json(SessionView {
        account: account::profile(&app, id).await?,
        csrf_token: csrf_token(token(&headers)?),
    }))
}
