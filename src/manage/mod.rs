pub(crate) mod account;
mod assets;
pub(crate) mod audit;
pub(crate) mod catalog;
mod contract;
pub(crate) mod keys;
pub(crate) mod operations;
pub(crate) mod projects;
pub(crate) mod quotas;
pub(crate) mod tokens;
mod uploads;
pub(crate) mod users;

use crate::{
    app::App,
    authorization::{Action, Permit, Principal},
    config,
    http::{HttpError, observe, respond, unauthorized},
};
use anyhow::Result;
use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Extension, MatchedPath, Path, Query, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use contract::{BucketView, LoginReply, SessionView};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

pub fn schema() -> utoipa::openapi::OpenApi {
    contract::routes().1
}

pub fn router(app: Arc<App>) -> Router {
    let avatar_origin = app
        .config
        .manage
        .gravatar_origin()
        .expect("validated Gravatar origin");
    let web_policy: HeaderValue = assets::CSP
        .replace(
            "img-src 'self' blob: data:;",
            &format!("img-src 'self' blob: data: {avatar_origin};"),
        )
        .parse()
        .expect("validated content security policy");
    let (api, schema) = contract::routes();
    let schema = bytes::Bytes::from(serde_json::to_vec(&schema).expect("serializable API schema"));
    Router::new()
        .merge(api)
        .route(
            "/api/openapi.json",
            get(move || {
                let schema = schema.clone();
                async move { ([("content-type", "application/json")], schema) }
            }),
        )
        .fallback(assets::serve)
        .route("/api/buckets/{bucket}/cors", get(cors).put(save_cors))
        .route("/api/objects", get(objects))
        .route(
            "/api/objects/actions",
            post(object_actions)
                .layer(DefaultBodyLimit::max(2 * 1024 * 1024))
                .layer(axum::middleware::from_fn_with_state(
                    app.clone(),
                    limit_object_actions,
                )),
        )
        .route("/api/object", get(object))
        .route("/api/object/chunks", get(object_chunks))
        .route("/api/download", get(download))
        .route("/api/tasks", get(tasks))
        .route("/api/tasks/{id}", get(task))
        .route("/api/tasks/{id}/actions", post(task_action))
        .route("/api/integrity", post(start_integrity))
        .route("/api/packs", get(packs))
        .route("/api/packs/{id}", get(pack_detail))
        .route("/api/packs/run", post(pack_run))
        .route("/api/cache/flush", post(cache_flush))
        .route("/api/packs/unpack", post(pack_unpack))
        .route("/api/tasks/{id}/issues", get(integrity_issues))
        .route(
            "/api/tasks/{id}/issues/{issue}/objects",
            get(integrity_objects),
        )
        .route("/api/tasks/{id}/report", get(integrity_report))
        .layer(DefaultBodyLimit::max(16 * 1024))
        .layer(axum::middleware::from_fn_with_state(
            app.clone(),
            access_gate,
        ))
        .layer(axum::middleware::from_fn_with_state(
            web_policy,
            web_headers,
        ))
        .layer(axum::middleware::from_fn(contract::errors))
        .with_state(app.clone())
        .layer(axum::middleware::from_fn_with_state((app, 2usize), observe))
}
async fn web_headers(
    State(policy): State<HeaderValue>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let mut response = next.run(request).await;
    if response.headers().contains_key("content-security-policy") {
        response
            .headers_mut()
            .insert("content-security-policy", policy);
    }
    if !response.headers().contains_key("cache-control") {
        response.headers_mut().insert(
            "cache-control",
            HeaderValue::from_static("private, no-store"),
        );
    }
    for (name, value) in [
        ("x-content-type-options", "nosniff"),
        ("referrer-policy", "no-referrer"),
        ("x-frame-options", "DENY"),
    ] {
        response
            .headers_mut()
            .insert(name, HeaderValue::from_static(value));
    }
    response
}
fn csrf_token(token: &str) -> String {
    let mut hash = blake3::Hasher::new();
    hash.update(b"mokyu-web-csrf-v1\0");
    hash.update(token.as_bytes());
    hash.finalize().to_hex().to_string()
}
fn origin(app: &App, headers: &HeaderMap) -> Result<(), HttpError> {
    if headers.get("origin").and_then(|v| v.to_str().ok()) != Some(&app.config.manage.origin) {
        return Err(unauthorized());
    }
    Ok(())
}
fn token(headers: &HeaderMap) -> Result<&str, HttpError> {
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
pub(super) struct Identity {
    id: Uuid,
    admin: bool,
    password_change: bool,
    principal: Principal,
}
impl Identity {
    async fn recent(&self, app: &App) -> Result<(), HttpError> {
        if self.principal.token_id().is_some() && self.admin {
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
            "/api/session"
                | "/api/me"
                | "/api/me/sessions"
                | "/api/buckets"
                | "/api/objects"
                | "/api/object"
                | "/api/object/chunks"
                | "/api/download"
                | "/api/projects"
                | "/api/quotas/{kind}/{id}"
                | "/api/buckets/{bucket}/objects"
                | "/api/buckets/{bucket}/catalog"
                | "/api/uploads"
                | "/api/uploads/{id}"
                | "/api/uploads/{id}/parts"
                | "/api/tokens"
                | "/api/audit"
                | "/api/audit/export"
                | "/api/audit/{id}"
        ) | (
            "POST",
            "/api/logout"
                | "/api/me/password"
                | "/api/me/reauth"
                | "/api/objects/actions"
                | "/api/tokens"
                | "/api/buckets/{bucket}/uploads"
                | "/api/uploads/{id}/complete"
        ) | ("PUT", "/api/me")
            | ("PUT", "/api/uploads/{id}/parts/{number}")
            | ("DELETE", "/api/uploads/{id}")
            | ("PUT" | "DELETE", "/api/tokens/{id}")
            | ("DELETE", "/api/me/sessions" | "/api/me/sessions/{id}")
            | (
                "GET" | "PUT",
                "/api/buckets/{bucket}/cors" | "/api/buckets/{bucket}/website"
            )
    )
}
async fn access_gate(
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
    if !path.starts_with("/api/")
        || matches!(
            path.as_str(),
            "/api/login" | "/api/info" | "/api/openapi.json" | "/api/bootstrap" | "/api/setup"
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
                && (matches!(path.as_str(), "/api/session" | "/api/logout" | "/api/me")
                    || path.starts_with("/api/me/")
                    || path.starts_with("/api/tokens")
                    || (path.starts_with("/api/audit") && !identity.admin)) =>
        {
            unauthorized().into_response()
        }
        Ok(identity)
            if identity.password_change
                && !matches!(
                    path.as_str(),
                    "/api/session"
                        | "/api/me"
                        | "/api/me/password"
                        | "/api/me/sessions"
                        | "/api/me/sessions/{id}"
                        | "/api/logout"
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
struct Login {
    username: String,
    password: String,
}

#[utoipa::path(post, path = "/api/login", request_body = Login, responses((status = 200, body = LoginReply), (status = 403, body = contract::ErrorBody)))]
async fn login(
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
    let token = crate::admin::random_secret()?;
    let csrf = csrf_token(&token);
    let seconds = config::seconds(&app.config.manage.session_lifetime)?;
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
    let secure = if app.config.manage.secure_cookie {
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
#[utoipa::path(post, path = "/api/logout", responses((status = 204), (status = 403, body = contract::ErrorBody)))]
async fn logout(
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
#[utoipa::path(get, path = "/api/session", responses((status = 200, body = SessionView), (status = 403, body = contract::ErrorBody)))]
async fn session(
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
#[utoipa::path(get, path = "/api/status", responses((status = 200, body = Value), (status = 403, body = contract::ErrorBody)))]
async fn status(State(app): State<Arc<App>>) -> Result<Json<Value>, HttpError> {
    Ok(Json(app.status().await?))
}
#[utoipa::path(get, path = "/api/buckets", responses((status = 200, body = Vec<BucketView>), (status = 403, body = contract::ErrorBody)))]
async fn buckets(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<Vec<BucketView>>, HttpError> {
    let rows: Vec<BucketView> = sqlx::query_as("SELECT b.*,CASE WHEN $2 THEN ARRAY['bucket.list','object.read','object.write','object.delete','object.acl','bucket.settings','storage.inspect'] WHEN $3::uuid IS NOT NULL THEN t.actions ELSE a.actions END AS actions FROM buckets b LEFT JOIN user_bucket_access a ON a.bucket_id=b.id AND a.user_id=$1 LEFT JOIN token_bucket_access t ON t.bucket_id=b.id AND t.token_id=$3 WHERE $2 OR 'bucket.list'=ANY(CASE WHEN $3::uuid IS NOT NULL THEN t.actions ELSE a.actions END) ORDER BY b.name LIMIT 1000")
        .bind(actor.id).bind(actor.admin).bind(actor.principal.token_id())
        .fetch_all(&app.db)
        .await?;
    Ok(Json(rows))
}
#[derive(Deserialize, Serialize, sqlx::FromRow, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
struct Website {
    website_enabled: bool,
    index_document: String,
    error_document: String,
}
async fn cors(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
) -> Result<Json<Value>, HttpError> {
    actor
        .principal
        .require(&app.db, bucket, Action::Settings)
        .await?;
    let rules = sqlx::query_scalar("SELECT cors FROM buckets WHERE id=$1")
        .bind(bucket)
        .fetch_optional(&app.db)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    Ok(Json(rules))
}
async fn save_cors(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Json(rules): Json<Value>,
) -> Result<Json<Value>, HttpError> {
    Ok(Json(app.set_cors(&actor.principal, bucket, rules).await?))
}
impl Website {
    fn validate(&self) -> Result<(), HttpError> {
        let valid_key = |key: &str| {
            !key.starts_with('/')
                && !key.contains('\\')
                && !key.chars().any(char::is_control)
                && !key.split('/').any(|part| part == "." || part == "..")
        };
        if self.index_document.is_empty()
            || self.index_document.len() > 255
            || self.index_document.contains('/')
            || !valid_key(&self.index_document)
            || self.error_document.len() > 1024
            || !valid_key(&self.error_document)
        {
            return Err(s3s::s3_error!(InvalidArgument, "invalid website document key").into());
        }
        Ok(())
    }
}
#[utoipa::path(get, path = "/api/buckets/{bucket}/website", params(("bucket" = Uuid, Path)), responses((status = 200, body = Website), (status = 403, body = contract::ErrorBody)))]
async fn website(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
) -> Result<Json<Website>, HttpError> {
    actor
        .principal
        .require(&app.db, bucket, Action::Settings)
        .await?;
    let row = sqlx::query_as(
        "SELECT website_enabled,index_document,error_document FROM buckets WHERE id=$1",
    )
    .bind(bucket)
    .fetch_optional(&app.db)
    .await?
    .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    Ok(Json(row))
}
#[utoipa::path(put, path = "/api/buckets/{bucket}/website", params(("bucket" = Uuid, Path)), request_body = Website, responses((status = 200, body = Website), (status = 400, body = contract::ErrorBody)))]
async fn save_website(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Json(input): Json<Website>,
) -> Result<Json<Website>, HttpError> {
    app.writable()?;
    input.validate()?;
    let mut tx = app.db.begin().await?;
    Permit::for_action(actor.principal, bucket, Action::Settings)
        .lock(&mut tx)
        .await?;
    let row = sqlx::query_as("UPDATE buckets SET website_enabled=$2,index_document=$3,error_document=$4 WHERE id=$1 AND state='active' RETURNING website_enabled,index_document,error_document")
        .bind(bucket).bind(input.website_enabled).bind(&input.index_document).bind(&input.error_document)
        .fetch_optional(&mut *tx).await?.ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    audit::checkpoint(&mut tx,"bucket.website",&bucket.to_string(),json!({"bucket_id":bucket,"website_enabled":input.website_enabled,"index_document":input.index_document,"error_document":input.error_document})).await?;
    tx.commit().await?;
    Ok(Json(row))
}
#[derive(Deserialize)]
struct Browse {
    bucket: Uuid,
    #[serde(default)]
    prefix: String,
    token: Option<String>,
    limit: Option<usize>,
    #[serde(default)]
    recursive: bool,
}
async fn objects(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Query(q): Query<Browse>,
) -> Result<Json<Value>, HttpError> {
    actor
        .principal
        .require(&app.db, q.bucket, Action::List)
        .await?;
    let page = app
        .list(
            q.bucket,
            &q.prefix,
            if q.recursive { "" } else { "/" },
            None,
            q.token.as_deref(),
            q.limit.unwrap_or(100),
        )
        .await?;
    Ok(Json(
        json!({"objects":page.objects,"prefixes":page.prefixes,"next_token":page.next}),
    ))
}
#[derive(Deserialize)]
struct ObjectQuery {
    bucket: Uuid,
    key: String,
    #[serde(default)]
    preview: bool,
}
async fn object(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Query(q): Query<ObjectQuery>,
) -> Result<Json<Value>, HttpError> {
    actor
        .principal
        .require(&app.db, q.bucket, Action::Read)
        .await?;
    let (s, _pin) = app.current(q.bucket, &q.key).await?;
    let grants: Vec<(String, bool)> = sqlx::query_as(
        "SELECT access_key,'object.write'=ANY(actions) AS writable FROM grants WHERE bucket_id=$1 AND $2 ORDER BY access_key",
    )
    .bind(q.bucket)
    .bind(actor.admin)
    .fetch_all(&app.db)
    .await?;
    Ok(Json(
        json!({"object":s,"bucket_grants":grants.into_iter().map(|(access_key,writable)|json!({"access_key":access_key,"writable":writable})).collect::<Vec<_>>()}),
    ))
}
static OBJECT_ACTIONS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(2);
async fn limit_object_actions(
    State(app): State<Arc<App>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let _ = app;
    if request.extensions().get::<Identity>().is_none() {
        return unauthorized().into_response();
    }
    let Ok(_permit) = OBJECT_ACTIONS.try_acquire() else {
        return HttpError(s3s::s3_error!(SlowDown).into()).into_response();
    };
    match tokio::time::timeout(Duration::from_secs(60), next.run(request)).await {
        Ok(response) => response,
        Err(_) => (
            StatusCode::REQUEST_TIMEOUT,
            Json(json!({"error":"Request Timeout"})),
        )
            .into_response(),
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SelectedObject {
    key: String,
    version: Uuid,
}
#[derive(Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
enum ObjectAction {
    Delete,
    Private,
    PublicRead,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ObjectActions {
    bucket: Uuid,
    action: ObjectAction,
    objects: Vec<SelectedObject>,
}
async fn object_actions(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    axum::extract::Extension(context): axum::extract::Extension<crate::stats::RequestContext>,
    Json(input): Json<ObjectActions>,
) -> Result<Json<Value>, HttpError> {
    let user_id = actor.id;
    let mut keys = std::collections::HashSet::new();
    if input.objects.is_empty()
        || input.objects.len() > 1000
        || input
            .objects
            .iter()
            .any(|o| o.key.is_empty() || o.key.len() > 1024 || !keys.insert(&o.key))
    {
        return Err(s3s::s3_error!(InvalidArgument, "select 1 to 1000 distinct objects").into());
    }
    let public = match input.action {
        ObjectAction::Delete => None,
        ObjectAction::Private => Some(false),
        ObjectAction::PublicRead => Some(true),
    };
    let mut results = Vec::with_capacity(input.objects.len());
    #[cfg(feature = "fault-injection")]
    crate::faults::point("management-before-change").await;
    for object in &input.objects {
        let status = match app
            .change_object(
                &actor.principal,
                input.bucket,
                &object.key,
                Some(object.version),
                public,
            )
            .await
        {
            Ok(()) => 200,
            Err(e) => HttpError(e).into_response().status().as_u16(),
        };
        if status != 200 {
            context
                .failed
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        tracing::info!(%user_id, bucket_id=%input.bucket, action=?input.action, object_key=%object.key, version=%object.version, status, "management object action");
        results.push(json!({"key":object.key,"version":object.version,"status":status}));
    }
    let mut tx = app.db.begin().await?;
    audit::checkpoint(&mut tx,"object.batch",&input.bucket.to_string(),json!({"bucket_id":input.bucket,"operation":format!("{:?}",input.action),"count":results.len(),"failed":results.iter().filter(|r|r["status"]!=200).count(),"sample":results.iter().take(20).collect::<Vec<_>>()})).await?;
    if context.failed.load(std::sync::atomic::Ordering::Relaxed) {
        audit::partial(&mut tx).await?;
    }
    tx.commit().await?;
    Ok(Json(json!({"results":results})))
}
#[derive(Deserialize)]
struct ChunkQuery {
    bucket: Uuid,
    key: String,
    version: Uuid,
    after: Option<i64>,
    limit: Option<i64>,
}
async fn object_chunks(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Query(q): Query<ChunkQuery>,
) -> Result<Json<Value>, HttpError> {
    actor
        .principal
        .require(&app.db, q.bucket, Action::Inspect)
        .await?;
    let limit = q.limit.unwrap_or(100);
    if !(1..=200).contains(&limit) || q.after.is_some_and(|a| a < 0) {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let (object, _pin) = app.current(q.bucket, &q.key).await?;
    if object.id != q.version {
        return Err(s3s::s3_error!(PreconditionFailed).into());
    }
    let mut rows: Vec<Value> = sqlx::query_scalar("SELECT jsonb_build_object('id',c.id::text,'offset_bytes',e.offset_bytes::text,'length',e.length,'source_offset',e.source_offset,'raw_size',c.raw_size,'stored_size',l.stored_size,'independent_size_hint',c.stored_size,'payload_size',c.stored_size-CASE WHEN c.algorithm='none' THEN 0 ELSE 16 END,'compression',CASE WHEN c.compressed THEN 'zstd' ELSE 'none' END,'algorithm',c.algorithm,'key_id',c.key_id,'pack_id',c.pack_id::text,'source',CASE WHEN l.id IS NOT NULL THEN 'chunk' WHEN c.pack_id IS NOT NULL THEN 'pack' ELSE 'pending' END,'reads',COALESCE(a.reads,0),'range_reads',COALESCE(a.range_reads,0)) FROM extents e JOIN chunks c ON c.id=e.chunk_id LEFT JOIN chunk_locations l ON l.chunk_id=c.id AND l.state='ready' LEFT JOIN chunk_access_stats a ON a.chunk_id=c.id WHERE e.stream_id=$1 AND e.offset_bytes>$2 ORDER BY e.offset_bytes LIMIT $3")
        .bind(object.id).bind(q.after.unwrap_or(-1)).bind(limit + 1).fetch_all(&app.db).await?;
    let more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    if !actor.admin {
        for row in &mut rows {
            if let Some(row) = row.as_object_mut() {
                for key in ["reads", "range_reads", "key_id"] {
                    row.remove(key);
                }
            }
        }
    }
    let next = more.then(|| rows.last().unwrap()["offset_bytes"].clone());
    Ok(Json(json!({"chunks":rows,"next_offset":next})))
}
#[derive(Deserialize)]
struct TasksQuery {
    state: Option<String>,
    token: Option<String>,
    limit: Option<i64>,
}
#[derive(Deserialize, Serialize)]
struct TaskCursor {
    state: Option<String>,
    created_at: DateTime<Utc>,
    id: Uuid,
}
async fn tasks(
    State(app): State<Arc<App>>,
    Extension(_actor): Extension<Identity>,
    Query(q): Query<TasksQuery>,
) -> Result<Json<Value>, HttpError> {
    let limit = q.limit.unwrap_or(100);
    if !(1..=200).contains(&limit)
        || q.state
            .as_deref()
            .is_some_and(|s| !["queued", "running", "paused", "completed", "failed"].contains(&s))
    {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let cursor = q
        .token
        .as_ref()
        .map(|token| -> Result<TaskCursor> {
            let bytes = URL_SAFE_NO_PAD.decode(token)?;
            Ok(serde_json::from_slice(&bytes)?)
        })
        .transpose()
        .map_err(|_| s3s::s3_error!(InvalidArgument))?;
    if cursor.as_ref().is_some_and(|c| c.state != q.state) {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let mut query =
        sqlx::QueryBuilder::<sqlx::Postgres>::new("SELECT to_jsonb(t) FROM tasks t WHERE true");
    if let Some(state) = &q.state {
        query.push(" AND state=").push_bind(state);
    }
    if let Some(cursor) = cursor {
        query
            .push(" AND (created_at,id)<(")
            .push_bind(cursor.created_at)
            .push(",")
            .push_bind(cursor.id)
            .push(")");
    }
    query
        .push(" ORDER BY created_at DESC,id DESC LIMIT ")
        .push_bind(limit + 1);
    let mut rows: Vec<Value> = query.build_query_scalar().fetch_all(&app.db).await?;
    let more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    let next = if more {
        let last = rows.last().unwrap();
        Some(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&TaskCursor {
            state: q.state,
            created_at: serde_json::from_value(last["created_at"].clone())?,
            id: serde_json::from_value(last["id"].clone())?,
        })?))
    } else {
        None
    };
    Ok(Json(json!({"tasks":rows,"next_token":next})))
}
async fn task(
    State(app): State<Arc<App>>,
    Extension(_actor): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, HttpError> {
    let task = sqlx::query_scalar("SELECT to_jsonb(t) FROM tasks t WHERE id=$1")
        .bind(id)
        .fetch_optional(&app.db)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    Ok(Json(task))
}
#[derive(Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
enum TaskAction {
    Pause,
    Resume,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskChange {
    action: TaskAction,
}
async fn task_action(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
    Json(input): Json<TaskChange>,
) -> Result<Json<Value>, HttpError> {
    let user_id = actor.id;
    let result = app
        .task_change(id, matches!(input.action, TaskAction::Resume))
        .await;
    tracing::info!(%user_id, task_id=%id, action=?input.action, success=result.is_ok(), "management task action");
    Ok(Json(result?))
}
async fn download(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    headers: HeaderMap,
    method: Method,
    Query(q): Query<ObjectQuery>,
) -> Result<Response, HttpError> {
    actor
        .principal
        .require(&app.db, q.bucket, Action::Read)
        .await?;
    respond(
        app,
        q.bucket,
        &q.key,
        headers,
        method,
        false,
        Some(q.preview),
    )
    .await
}
async fn start_integrity(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<crate::integrity::Request>,
) -> Result<Json<Value>, HttpError> {
    let user_id = actor.id;
    let result = app.integrity_start(input).await;
    tracing::info!(%user_id,success=result.is_ok(),task_id=?result.as_ref().ok().and_then(|r|r.get("task_id")),"management integrity check");
    Ok(Json(result?))
}
async fn packs(
    State(app): State<Arc<App>>,
    Extension(_actor): Extension<Identity>,
    Query(q): Query<IssueQuery>,
) -> Result<Json<Value>, HttpError> {
    let limit = q.limit.unwrap_or(100);
    if !(1..=200).contains(&limit) || q.after.is_some_and(|id| id < 0) {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let rows:Vec<Value>=sqlx::query_scalar("SELECT to_jsonb(p)||jsonb_build_object('id',p.id::text) FROM packs p WHERE id>$1 ORDER BY id LIMIT $2")
        .bind(q.after.unwrap_or(0)).bind(limit+1).fetch_all(&app.db).await?;
    let next = if rows.len() > limit as usize {
        rows[limit as usize - 1]["id"].clone()
    } else {
        Value::Null
    };
    Ok(Json(
        json!({"status":app.pack_status().await?,"packs":rows.into_iter().take(limit as usize).collect::<Vec<_>>(),"next_after":next}),
    ))
}
async fn pack_detail(
    State(app): State<Arc<App>>,
    Extension(_actor): Extension<Identity>,
    Path(id): Path<i64>,
) -> Result<Json<Value>, HttpError> {
    let pack: Value = sqlx::query_scalar(
        "SELECT to_jsonb(p)||jsonb_build_object('id',p.id::text) FROM packs p WHERE id=$1",
    )
    .bind(id)
    .fetch_optional(&app.db)
    .await?
    .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    let members:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('ordinal',m.ordinal,'chunk_id',m.chunk_id::text,'offset',m.offset_bytes,'raw_size',c.raw_size,'current',c.pack_id=m.pack_id) FROM pack_members m JOIN chunks c ON c.id=m.chunk_id WHERE m.pack_id=$1 ORDER BY ordinal LIMIT 4096").bind(id).fetch_all(&app.db).await?;
    Ok(Json(json!({"pack":pack,"members":members})))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PackRun {
    kind: String,
}
async fn cache_flush(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<Value>, HttpError> {
    let user_id = actor.id;
    let result = app.cache_flush_start().await;
    tracing::info!(%user_id,success=result.is_ok(),"management upload cache flush");
    Ok(Json(result?))
}
async fn pack_run(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<PackRun>,
) -> Result<Json<Value>, HttpError> {
    let user_id = actor.id;
    let result = app.pack_start(&input.kind).await;
    tracing::info!(%user_id,kind=%input.kind,success=result.is_ok(),"management pack maintenance");
    Ok(Json(result?))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PackUnpack {
    pack_id: Option<String>,
    #[serde(default)]
    all: bool,
    #[serde(default)]
    execute: bool,
}
async fn pack_unpack(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<PackUnpack>,
) -> Result<Json<Value>, HttpError> {
    let user_id = actor.id;
    let id = input
        .pack_id
        .as_deref()
        .map(str::parse::<i64>)
        .transpose()
        .map_err(|_| s3s::s3_error!(InvalidArgument))?;
    let result = app.unpack_start(id, input.all, input.execute).await;
    tracing::info!(%user_id,success=result.is_ok(),"management pack unpack");
    Ok(Json(result?))
}
#[derive(Deserialize)]
struct IssueQuery {
    after: Option<i64>,
    limit: Option<i64>,
}
async fn integrity_issues(
    State(app): State<Arc<App>>,
    Extension(_actor): Extension<Identity>,
    Path(id): Path<Uuid>,
    Query(q): Query<IssueQuery>,
) -> Result<Json<Value>, HttpError> {
    Ok(Json(
        app.integrity_issues(id, q.after.unwrap_or(0), q.limit.unwrap_or(100))
            .await?,
    ))
}
#[derive(Deserialize)]
struct IssueObjectsQuery {
    after: Option<String>,
}
async fn integrity_objects(
    State(app): State<Arc<App>>,
    Extension(_actor): Extension<Identity>,
    Path((id, issue)): Path<(Uuid, i64)>,
    Query(q): Query<IssueObjectsQuery>,
) -> Result<Json<Value>, HttpError> {
    let after = q
        .after
        .map(|s| serde_json::from_str::<(Uuid, String)>(&s))
        .transpose()
        .map_err(|_| s3s::s3_error!(InvalidArgument))?;
    Ok(Json(app.integrity_objects(id, issue, after).await?))
}
async fn integrity_report(
    State(app): State<Arc<App>>,
    Extension(_actor): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<Response, HttpError> {
    let task: Value =
        sqlx::query_scalar("SELECT to_jsonb(t) FROM tasks t WHERE id=$1 AND kind='integrity'")
            .bind(id)
            .fetch_optional(&app.db)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    if task["state"] != "completed" {
        return Err(s3s::s3_error!(
            OperationAborted,
            "finish the inspection before exporting its report"
        )
        .into());
    }
    // Fix the boundary so report size stays finite; pages do not retain a database connection.
    let upper: i64 =
        sqlx::query_scalar("SELECT COALESCE(max(id),0) FROM integrity_issues WHERE task_id=$1")
            .bind(id)
            .fetch_one(&app.db)
            .await?;
    let body = async_stream::try_stream! {
        yield bytes::Bytes::from(format!("{}\n",json!({"type":"task","task":task})));
        let mut after = 0;
        let mut count = 0u64;
        loop {
            let rows: Vec<Value> = sqlx::query_scalar("SELECT to_jsonb(i)||jsonb_build_object('id',i.id::text,'chunk_id',i.chunk_id::text) FROM integrity_issues i WHERE task_id=$1 AND id>$2 AND id<=$3 ORDER BY id LIMIT 100").bind(id).bind(after).bind(upper).fetch_all(&app.db).await.map_err(std::io::Error::other)?;
            if rows.is_empty() { break; }
            for row in rows {
                after = row["id"].as_str().and_then(|v|v.parse().ok()).ok_or_else(||std::io::Error::other("invalid report ID"))?;
                count += 1;
                yield bytes::Bytes::from(format!("{}\n",json!({"type":"issue","issue":row})));
            }
        }
        if count != task["detail"]["issues"].as_u64().unwrap_or(0) { Err(std::io::Error::other("report expired during export"))?; }
        yield bytes::Bytes::from(format!("{}\n",json!({"type":"end","issues":count})));
    };
    let body: std::pin::Pin<
        Box<dyn futures_util::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send>,
    > = Box::pin(body);
    let mut response = Body::from_stream(body).into_response();
    response.headers_mut().insert(
        "content-type",
        HeaderValue::from_static("application/x-ndjson"),
    );
    response.headers_mut().insert(
        "content-disposition",
        HeaderValue::from_str(&format!("attachment; filename=\"integrity-{id}.jsonl\""))?,
    );
    Ok(response)
}
