use crate::{
    app::{App, Bucket, Metadata},
    config, s3,
};
use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use s3s::dto::{ETagCondition, Range, Timestamp, TimestampFormat};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

#[derive(Debug)]
struct HttpError(anyhow::Error);
impl<E: Into<anyhow::Error>> From<E> for HttpError {
    fn from(e: E) -> Self {
        Self(e.into())
    }
}
impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let status = if let Some(e) = self.0.downcast_ref::<s3s::S3Error>() {
            e.code()
                .status_code()
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
        } else {
            StatusCode::INTERNAL_SERVER_ERROR
        };
        if status.is_server_error() {
            tracing::error!(error=%self.0,"HTTP request failed");
        }
        if status == StatusCode::NOT_MODIFIED {
            return status.into_response();
        }
        let mut response = (
            status,
            Json(json!({"error":status.canonical_reason().unwrap_or("request failed")})),
        )
            .into_response();
        if let Some(headers) = self
            .0
            .downcast_ref::<s3s::S3Error>()
            .and_then(|e| e.headers())
        {
            response.headers_mut().extend(headers.clone());
        }
        response
    }
}
fn unauthorized() -> HttpError {
    s3s::s3_error!(AccessDenied).into()
}
impl HttpError {
    fn has_code(&self, code: s3s::S3ErrorCode) -> bool {
        self.0
            .downcast_ref::<s3s::S3Error>()
            .is_some_and(|e| e.code() == &code)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CorsRule {
    pub origins: Vec<String>,
    pub methods: Vec<String>,
    #[serde(default)]
    pub headers: Vec<String>,
    #[serde(default)]
    pub expose: Vec<String>,
    #[serde(default)]
    pub max_age: u32,
}
pub fn validate_cors(value: &Value) -> Result<()> {
    let rules: Vec<CorsRule> =
        serde_json::from_value(value.clone()).context("invalid CORS document")?;
    ensure!(rules.len() <= 100, "too many CORS rules");
    for r in rules {
        ensure!(
            !r.origins.is_empty() && !r.methods.is_empty(),
            "CORS rule requires origins and methods"
        );
        for m in r.methods {
            ensure!(
                ["GET", "HEAD", "PUT", "POST", "DELETE", "OPTIONS"].contains(&m.as_str()),
                "invalid CORS method"
            );
        }
        for h in r.headers.into_iter().chain(r.expose) {
            ensure!(
                h == "*" || hyper::header::HeaderName::from_bytes(h.as_bytes()).is_ok(),
                "invalid CORS header"
            );
        }
        for o in r.origins {
            ensure!(
                o == "*"
                    || ((o.starts_with("https://") || o.starts_with("http://"))
                        && !o.contains(['\r', '\n'])),
                "invalid CORS origin"
            );
        }
    }
    Ok(())
}
pub fn cors_headers(bucket: &Bucket, headers: &HeaderMap, method: &str) -> Result<HeaderMap> {
    let mut out = HeaderMap::new();
    let rules: Vec<CorsRule> = serde_json::from_value(bucket.cors.clone())?;
    if rules.is_empty() {
        return Ok(out);
    }
    out.insert(
        "vary",
        HeaderValue::from_static(if method == "OPTIONS" {
            "Origin, Access-Control-Request-Method, Access-Control-Request-Headers"
        } else {
            "Origin"
        }),
    );
    let Some(origin) = headers.get("origin").and_then(|s| s.to_str().ok()) else {
        return Ok(out);
    };
    let (requested, names) = if method == "OPTIONS" {
        let Some(requested) = headers
            .get("access-control-request-method")
            .and_then(|s| s.to_str().ok())
        else {
            return Ok(out);
        };
        (
            requested,
            headers
                .get("access-control-request-headers")
                .and_then(|s| s.to_str().ok())
                .unwrap_or(""),
        )
    } else {
        (method, "")
    };
    for r in rules {
        if !r.origins.iter().any(|o| o == "*" || o == origin)
            || !r.methods.iter().any(|m| m == requested)
        {
            continue;
        }
        if !names
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .all(|n| {
                r.headers
                    .iter()
                    .any(|a| a == "*" || a.eq_ignore_ascii_case(n))
            })
        {
            continue;
        }
        out.insert(
            "access-control-allow-origin",
            HeaderValue::from_str(if r.origins.iter().any(|o| o == "*") {
                "*"
            } else {
                origin
            })?,
        );
        out.insert(
            "access-control-allow-methods",
            HeaderValue::from_str(&r.methods.join(", "))?,
        );
        if !names.is_empty() {
            out.insert(
                "access-control-allow-headers",
                HeaderValue::from_str(names)?,
            );
        }
        if !r.expose.is_empty() {
            out.insert(
                "access-control-expose-headers",
                HeaderValue::from_str(&r.expose.join(", "))?,
            );
        }
        out.insert(
            "access-control-max-age",
            HeaderValue::from_str(&r.max_age.to_string())?,
        );
        break;
    }
    Ok(out)
}
pub fn public_router(app: Arc<App>) -> Router {
    Router::new()
        .fallback(public)
        .with_state(app.clone())
        .layer(axum::middleware::from_fn_with_state((app, 1usize), observe))
}
pub fn manage_router(app: Arc<App>) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/app.js", get(js))
        .route("/i18n.js", get(i18n))
        .route("/app.css", get(css))
        .route("/api/login", post(login))
        .route("/api/logout", post(logout))
        .route("/api/session", get(session))
        .route("/api/status", get(status))
        .route("/api/buckets", get(buckets))
        .route(
            "/api/buckets/{bucket}/website",
            get(website).put(save_website),
        )
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
        .route("/api/tasks/{id}/issues", get(integrity_issues))
        .route(
            "/api/tasks/{id}/issues/{issue}/objects",
            get(integrity_objects),
        )
        .route("/api/tasks/{id}/report", get(integrity_report))
        .layer(DefaultBodyLimit::max(16 * 1024))
        .layer(axum::middleware::from_fn(web_headers))
        .with_state(app.clone())
        .layer(axum::middleware::from_fn_with_state((app, 2usize), observe))
}
async fn observe(
    State((app, index)): State<(Arc<App>, usize)>,
    mut request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    use tracing::Instrument;
    let observation = crate::stats::Request::new(
        &app.statistics.http[index],
        if index == 1 { "web" } else { "manage" },
        request.method().clone(),
    );
    request.extensions_mut().insert(observation.context.clone());
    let response = next.run(request).instrument(observation.span.clone()).await;
    observation.response(response, false).map(Body::new)
}
async fn web_headers(request: axum::extract::Request, next: axum::middleware::Next) -> Response {
    let mut response = next.run(request).await;
    for (name, value) in [
        ("cache-control", "private, no-store"),
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
    hash.update(b"media-gateway-web-csrf-v1\0");
    hash.update(token.as_bytes());
    hash.finalize().to_hex().to_string()
}
pub async fn s3_http(
    app: Arc<App>,
    service: s3s::service::S3Service,
    request: hyper::Request<hyper::body::Incoming>,
    controls: Arc<tokio::sync::Semaphore>,
) -> s3s::HttpResponse {
    let aws_chunked = request
        .headers()
        .get("x-amz-content-sha256")
        .is_some_and(|v| v.as_bytes().starts_with(b"STREAMING-"));
    let mut request = request.map(|body| {
        let body = s3s::Body::from(body);
        if aws_chunked {
            crate::upload::guard_aws_end(body)
        } else {
            body
        }
    });
    let mut cors = HeaderMap::new();
    let mut public_alias = false;
    let result: Result<s3s::HttpResponse> = async {
        let query: Vec<String> = request
            .uri()
            .query()
            .unwrap_or("")
            .split('&')
            .map(|p| decode_path(p.split('=').next().unwrap_or("")))
            .collect::<Result<_>>()?;
        let host = request
            .headers()
            .get("host")
            .and_then(|h| h.to_str().ok())
            .and_then(|h| h.parse::<axum::http::uri::Authority>().ok())
            .map(|h| h.host().to_ascii_lowercase())
            .unwrap_or_default();
        let domain = app
            .config
            .listen
            .s3_domain
            .as_deref()
            .and_then(|h| h.parse::<axum::http::uri::Authority>().ok())
            .map(|h| h.host().to_ascii_lowercase());
        let virtual_bucket = domain
            .as_ref()
            .and_then(|d| host.strip_suffix(&format!(".{d}")));
        let signed = request.headers().contains_key("authorization")
            || query.iter().any(|q| {
                let q = q.to_ascii_lowercase();
                q.starts_with("x-amz-")
                    || matches!(q.as_str(), "awsaccesskeyid" | "signature" | "expires")
            });
        let mut bucket: Option<Bucket> = None;
        if !signed
            && matches!(
                *request.method(),
                Method::GET | Method::HEAD | Method::OPTIONS
            )
            && virtual_bucket.is_none()
            && domain.as_ref() != Some(&host)
            && s3s::path::check_bucket_name(&host)
        {
            bucket = sqlx::query_as("SELECT * FROM buckets WHERE name=$1")
                .bind(&host)
                .fetch_optional(&app.db)
                .await?;
            if let Some(b) = &bucket {
                public_alias = true;
                if b.state != "active" {
                    return Err(s3s::s3_error!(AccessDenied).into());
                }
                // Only unsigned public reads are rewritten; signed URIs remain byte-for-byte intact.
                let mut uri = request.uri().clone().into_parts();
                uri.path_and_query = Some(
                    format!(
                        "/{}{}",
                        b.name,
                        request.uri().path_and_query().map_or("/", |p| p.as_str())
                    )
                    .parse()?,
                );
                *request.uri_mut() = axum::http::Uri::from_parts(uri)?;
            }
        }
        if bucket.is_none() {
            let name = if let Some(name) = virtual_bucket {
                name.to_owned()
            } else {
                decode_path(
                    request
                        .uri()
                        .path()
                        .trim_start_matches('/')
                        .split('/')
                        .next()
                        .unwrap_or(""),
                )?
            };
            bucket = sqlx::query_as("SELECT * FROM buckets WHERE name=$1 AND state='active'")
                .bind(name)
                .fetch_optional(&app.db)
                .await?;
        }
        if let Some(bucket) = bucket {
            cors = cors_headers(&bucket, request.headers(), request.method().as_str())?;
        }
        if request.method() == Method::OPTIONS {
            let mut response = hyper::Response::new(s3s::Body::empty());
            let alias_method_allowed = !public_alias
                || request
                    .headers()
                    .get("access-control-request-method")
                    .is_some_and(|m| m == "GET" || m == "HEAD");
            *response.status_mut() =
                if cors.contains_key("access-control-allow-origin") && alias_method_allowed {
                    StatusCode::NO_CONTENT
                } else {
                    cors.remove("access-control-allow-origin");
                    StatusCode::FORBIDDEN
                };
            return Ok(response);
        }
        if request.method() == Method::POST
            && request
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.starts_with("multipart/form-data"))
        {
            return Err(s3s::s3_error!(NotImplemented, "use PutObject or multipart API").into());
        }
        let delete = request.method() == Method::POST && query.iter().any(|n| n == "delete");
        let complete = request.method() == Method::POST && query.iter().any(|n| n == "uploadId");
        let _control = if delete || complete {
            Some(
                controls
                    .try_acquire()
                    .map_err(|_| s3s::s3_error!(SlowDown))?,
            )
        } else {
            None
        };
        if delete || complete {
            if request
                .headers()
                .get("content-encoding")
                .is_some_and(|v| v == "aws-chunked")
            {
                return Err(
                    s3s::s3_error!(NotImplemented, "aws-chunked XML is unsupported").into(),
                );
            }
            let bytes = tokio::time::timeout(
                Duration::from_secs(config::seconds(&app.config.multipart.client_idle_timeout)?),
                request.body_mut().store_all_limited(2 * 1024 * 1024),
            )
            .await
            .map_err(|_| s3s::s3_error!(RequestTimeout))?
            .map_err(|_| s3s::s3_error!(MaxMessageLengthExceeded))?;
            let mut headers = request.headers().clone();
            if complete {
                let names: Vec<_> = headers
                    .keys()
                    .filter(|n| n.as_str().starts_with("x-amz-checksum-"))
                    .cloned()
                    .collect();
                for name in names {
                    headers.remove(name);
                }
            }
            if delete
                && !headers.contains_key("content-md5")
                && !headers
                    .keys()
                    .any(|n| n.as_str().starts_with("x-amz-checksum-"))
            {
                return Err(s3s::s3_error!(
                    InvalidRequest,
                    "multi-delete requires a body checksum"
                )
                .into());
            }
            let mut integrity = crate::upload::Integrity::new(&headers, None)?;
            integrity.update(&bytes);
            integrity.finish(None)?;
            if let Some(n) = headers.get("content-length")
                && n.to_str()?.parse::<usize>()? != bytes.len()
            {
                return Err(s3s::s3_error!(IncompleteBody).into());
            }
        }
        let response = service
            .call(request)
            .await
            .map_err(|e| anyhow::anyhow!("S3 HTTP failure: {e:?}"))?;
        Ok(response)
    }
    .await;
    let mut response = match result {
        Ok(response) => response,
        Err(error) => crate::app::internal(error)
            .to_http_response()
            .unwrap_or_else(|_| {
                let mut response = hyper::Response::new(s3s::Body::empty());
                *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                response
            }),
    };
    if public_alias && response.status() == StatusCode::NOT_FOUND {
        response = s3s::s3_error!(AccessDenied)
            .to_http_response()
            .expect("static S3 error");
    }
    response.headers_mut().extend(cors);
    response
}
fn asset(content: &'static str, mime: &'static str) -> Response {
    let mut r = content.into_response();
    r.headers_mut()
        .insert("content-type", HeaderValue::from_static(mime));
    r.headers_mut().insert("content-security-policy",HeaderValue::from_static("default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' blob:; media-src 'self'; object-src 'none'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"));
    r.headers_mut().insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    r.headers_mut()
        .insert("cache-control", HeaderValue::from_static("no-store"));
    r
}
async fn index() -> Response {
    asset(
        include_str!("../web/index.html"),
        "text/html; charset=utf-8",
    )
}
async fn js() -> Response {
    asset(
        include_str!("../web/app.js"),
        "text/javascript; charset=utf-8",
    )
}
async fn css() -> Response {
    asset(include_str!("../web/app.css"), "text/css; charset=utf-8")
}
async fn i18n() -> Response {
    asset(
        include_str!("../web/i18n.js"),
        "text/javascript; charset=utf-8",
    )
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
                .find_map(|s| s.strip_prefix("mgw_session="))
        })
        .filter(|s| s.len() == 64)
        .ok_or_else(unauthorized)
}
async fn authenticate(
    app: &App,
    headers: &HeaderMap,
    csrf: bool,
) -> Result<(Uuid, String), HttpError> {
    let token = token(headers)?;
    let hash = blake3::hash(token.as_bytes());
    let row:Option<(Uuid,String,Vec<u8>)>=sqlx::query_as("SELECT u.id,u.username,s.csrf_hash FROM sessions s JOIN web_users u ON u.id=s.user_id WHERE s.token_hash=$1 AND s.expires_at>now() AND u.enabled").bind(hash.as_bytes().as_slice()).fetch_optional(&app.db).await?;
    let (id, name, expected) = row.ok_or_else(unauthorized)?;
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
    Ok((id, name))
}
#[derive(Deserialize)]
struct Login {
    username: String,
    password: String,
}
static LOGIN_JOBS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(2);
async fn login(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(input): Json<Login>,
) -> Result<Response, HttpError> {
    origin(&app, &headers)?;
    if input.username.len() > 64 || input.password.len() > 1024 {
        return Err(unauthorized());
    }
    let job = LOGIN_JOBS
        .try_acquire()
        .map_err(|_| HttpError(s3s::s3_error!(SlowDown).into()))?;
    let row: Option<(Uuid, String)> =
        sqlx::query_as("SELECT id,password_hash FROM web_users WHERE username=$1 AND enabled")
            .bind(input.username)
            .fetch_optional(&app.db)
            .await?;
    let started = tokio::time::Instant::now();
    let verified = if let Some((id, hash)) = row {
        tokio::task::spawn_blocking(move || {
            let _job = job;
            use argon2::{Argon2, PasswordHash, PasswordVerifier};
            let valid = PasswordHash::new(&hash).ok().is_some_and(|hash| {
                Argon2::default()
                    .verify_password(input.password.as_bytes(), &hash)
                    .is_ok()
            });
            valid.then_some((id, hash))
        })
        .await?
    } else {
        None
    };
    tokio::time::sleep_until(started + Duration::from_millis(300)).await;
    let (user, verified_hash) = verified.ok_or_else(unauthorized)?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("login-verified").await;
    let token = crate::admin::random_secret()?;
    let csrf = csrf_token(&token);
    let seconds = config::seconds(&app.config.manage.session_lifetime)?;
    let mut tx = app.db.begin().await?;
    // Password reset and disable update this same row before revoking sessions.
    let current: Option<(String, bool)> =
        sqlx::query_as("SELECT password_hash,enabled FROM web_users WHERE id=$1 FOR UPDATE")
            .bind(user)
            .fetch_optional(&mut *tx)
            .await?;
    if !current.is_some_and(|(hash, enabled)| enabled && hash == verified_hash) {
        return Err(unauthorized());
    }
    #[cfg(feature = "fault-injection")]
    crate::faults::point("login-before-session").await;
    sqlx::query("INSERT INTO sessions(token_hash,user_id,csrf_hash,expires_at) VALUES($1,$2,$3,now()+$4*interval '1 second')").bind(blake3::hash(token.as_bytes()).as_bytes().as_slice()).bind(user).bind(blake3::hash(csrf.as_bytes()).as_bytes().as_slice()).bind(seconds as f64).execute(&mut *tx).await?;
    tx.commit().await?;
    let mut response = Json(json!({"csrf_token":csrf})).into_response();
    let secure = if app.config.manage.secure_cookie {
        "; Secure"
    } else {
        ""
    };
    response.headers_mut().insert(
        "set-cookie",
        HeaderValue::from_str(&format!(
            "mgw_session={token}; Path=/; HttpOnly; SameSite=Strict; Max-Age={seconds}{secure}"
        ))?,
    );
    response
        .headers_mut()
        .insert("cache-control", HeaderValue::from_static("no-store"));
    Ok(response)
}
async fn logout(State(app): State<Arc<App>>, headers: HeaderMap) -> Result<Response, HttpError> {
    authenticate(&app, &headers, true).await?;
    sqlx::query("DELETE FROM sessions WHERE token_hash=$1")
        .bind(
            blake3::hash(token(&headers)?.as_bytes())
                .as_bytes()
                .as_slice(),
        )
        .execute(&app.db)
        .await?;
    let mut r = StatusCode::NO_CONTENT.into_response();
    r.headers_mut().insert(
        "set-cookie",
        HeaderValue::from_static("mgw_session=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0"),
    );
    Ok(r)
}
async fn session(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
) -> Result<Json<Value>, HttpError> {
    let (id, username) = authenticate(&app, &headers, false).await?;
    Ok(Json(
        json!({"id":id,"username":username,"csrf_token":csrf_token(token(&headers)?)}),
    ))
}
async fn status(State(app): State<Arc<App>>, headers: HeaderMap) -> Result<Json<Value>, HttpError> {
    authenticate(&app, &headers, false).await?;
    Ok(Json(app.status().await?))
}
async fn buckets(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
) -> Result<Json<Value>, HttpError> {
    authenticate(&app, &headers, false).await?;
    let rows: Vec<Bucket> = sqlx::query_as("SELECT * FROM buckets ORDER BY name LIMIT 1000")
        .fetch_all(&app.db)
        .await?;
    Ok(Json(serde_json::to_value(rows)?))
}
#[derive(Deserialize, Serialize, sqlx::FromRow)]
#[serde(deny_unknown_fields)]
struct Website {
    website_enabled: bool,
    index_document: String,
    error_document: String,
}
async fn cors(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path(bucket): Path<Uuid>,
) -> Result<Json<Value>, HttpError> {
    authenticate(&app, &headers, false).await?;
    let rules = sqlx::query_scalar("SELECT cors FROM buckets WHERE id=$1")
        .bind(bucket)
        .fetch_optional(&app.db)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    Ok(Json(rules))
}
async fn save_cors(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path(bucket): Path<Uuid>,
    Json(rules): Json<Value>,
) -> Result<Json<Value>, HttpError> {
    authenticate(&app, &headers, true).await?;
    Ok(Json(app.set_cors(bucket, rules).await?))
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
async fn website(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path(bucket): Path<Uuid>,
) -> Result<Json<Website>, HttpError> {
    authenticate(&app, &headers, false).await?;
    let row = sqlx::query_as(
        "SELECT website_enabled,index_document,error_document FROM buckets WHERE id=$1",
    )
    .bind(bucket)
    .fetch_optional(&app.db)
    .await?
    .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    Ok(Json(row))
}
async fn save_website(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path(bucket): Path<Uuid>,
    Json(input): Json<Website>,
) -> Result<Json<Website>, HttpError> {
    authenticate(&app, &headers, true).await?;
    app.writable()?;
    input.validate()?;
    let row = sqlx::query_as("UPDATE buckets SET website_enabled=$2,index_document=$3,error_document=$4 WHERE id=$1 AND state='active' RETURNING website_enabled,index_document,error_document")
        .bind(bucket).bind(input.website_enabled).bind(input.index_document).bind(input.error_document)
        .fetch_optional(&app.db).await?.ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
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
    headers: HeaderMap,
    Query(q): Query<Browse>,
) -> Result<Json<Value>, HttpError> {
    authenticate(&app, &headers, false).await?;
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
    headers: HeaderMap,
    Query(q): Query<ObjectQuery>,
) -> Result<Json<Value>, HttpError> {
    authenticate(&app, &headers, false).await?;
    let (s, _pin) = app.current(q.bucket, &q.key).await?;
    let grants: Vec<(String, bool)> = sqlx::query_as(
        "SELECT access_key,writable FROM grants WHERE bucket_id=$1 ORDER BY access_key",
    )
    .bind(q.bucket)
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
    if let Err(error) = authenticate(&app, request.headers(), true).await {
        return error.into_response();
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
    headers: HeaderMap,
    axum::extract::Extension(context): axum::extract::Extension<crate::stats::RequestContext>,
    Json(input): Json<ObjectActions>,
) -> Result<Json<Value>, HttpError> {
    let (user_id, _) = authenticate(&app, &headers, true).await?;
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
    for object in &input.objects {
        let status = match app
            .change_object(input.bucket, &object.key, Some(object.version), public)
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
    headers: HeaderMap,
    Query(q): Query<ChunkQuery>,
) -> Result<Json<Value>, HttpError> {
    authenticate(&app, &headers, false).await?;
    let limit = q.limit.unwrap_or(100);
    if !(1..=200).contains(&limit) || q.after.is_some_and(|a| a < 0) {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let (object, _pin) = app.current(q.bucket, &q.key).await?;
    if object.id != q.version {
        return Err(s3s::s3_error!(PreconditionFailed).into());
    }
    let mut rows: Vec<Value> = sqlx::query_scalar("SELECT jsonb_build_object('id',c.id::text,'offset_bytes',e.offset_bytes::text,'length',e.length,'source_offset',e.source_offset,'raw_size',c.raw_size,'stored_size',c.stored_size,'payload_size',c.stored_size-CASE WHEN c.algorithm='none' THEN 0 ELSE 16 END,'compression',CASE WHEN c.compressed THEN 'zstd' ELSE 'none' END,'algorithm',c.algorithm,'key_id',c.key_id) FROM extents e JOIN chunks c ON c.id=e.chunk_id WHERE e.stream_id=$1 AND e.offset_bytes>$2 ORDER BY e.offset_bytes LIMIT $3")
        .bind(object.id).bind(q.after.unwrap_or(-1)).bind(limit + 1).fetch_all(&app.db).await?;
    let more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
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
    headers: HeaderMap,
    Query(q): Query<TasksQuery>,
) -> Result<Json<Value>, HttpError> {
    authenticate(&app, &headers, false).await?;
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
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, HttpError> {
    authenticate(&app, &headers, false).await?;
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
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(input): Json<TaskChange>,
) -> Result<Json<Value>, HttpError> {
    let (user_id, _) = authenticate(&app, &headers, true).await?;
    let result = app
        .task_change(id, matches!(input.action, TaskAction::Resume))
        .await;
    tracing::info!(%user_id, task_id=%id, action=?input.action, success=result.is_ok(), "management task action");
    Ok(Json(result?))
}
async fn download(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    method: Method,
    Query(q): Query<ObjectQuery>,
) -> Result<Response, HttpError> {
    authenticate(&app, &headers, false).await?;
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
    headers: HeaderMap,
    Json(input): Json<crate::integrity::Request>,
) -> Result<Json<Value>, HttpError> {
    let (user_id, _) = authenticate(&app, &headers, true).await?;
    let result = app.integrity_start(input).await;
    tracing::info!(%user_id,success=result.is_ok(),task_id=?result.as_ref().ok().and_then(|r|r.get("task_id")),"management integrity check");
    Ok(Json(result?))
}
#[derive(Deserialize)]
struct IssueQuery {
    after: Option<i64>,
    limit: Option<i64>,
}
async fn integrity_issues(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Query(q): Query<IssueQuery>,
) -> Result<Json<Value>, HttpError> {
    authenticate(&app, &headers, false).await?;
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
    headers: HeaderMap,
    Path((id, issue)): Path<(Uuid, i64)>,
    Query(q): Query<IssueObjectsQuery>,
) -> Result<Json<Value>, HttpError> {
    authenticate(&app, &headers, false).await?;
    let after = q
        .after
        .map(|s| serde_json::from_str::<(Uuid, String)>(&s))
        .transpose()
        .map_err(|_| s3s::s3_error!(InvalidArgument))?;
    Ok(Json(app.integrity_objects(id, issue, after).await?))
}
async fn integrity_report(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Response, HttpError> {
    authenticate(&app, &headers, false).await?;
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
pub fn decode_path(path: &str) -> Result<String> {
    let bytes = path.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            ensure!(i + 2 < bytes.len(), "bad path escape");
            out.push(u8::from_str_radix(
                std::str::from_utf8(&bytes[i + 1..i + 3])?,
                16,
            )?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    Ok(String::from_utf8(out)?)
}
async fn public(
    State(app): State<Arc<App>>,
    request: axum::extract::Request,
) -> Result<Response, HttpError> {
    if !matches!(
        *request.method(),
        Method::GET | Method::HEAD | Method::OPTIONS
    ) {
        return Ok(StatusCode::METHOD_NOT_ALLOWED.into_response());
    }
    if request.headers().contains_key("authorization")
        || request
            .uri()
            .query()
            .is_some_and(|q| q.to_ascii_lowercase().contains("x-amz-"))
    {
        return Err(unauthorized());
    }
    let host = request
        .headers()
        .get("host")
        .and_then(|s| s.to_str().ok())
        .ok_or_else(unauthorized)?
        .to_ascii_lowercase();
    let bucket: Option<Bucket> = sqlx::query_as(
        "SELECT b.* FROM buckets b JOIN domains d ON d.bucket_id=b.id WHERE d.host=$1",
    )
    .bind(host)
    .fetch_optional(&app.db)
    .await?;
    let bucket = bucket.ok_or_else(unauthorized)?;
    let cors = cors_headers(&bucket, request.headers(), request.method().as_str())?;
    if request.method() == Method::OPTIONS {
        let mut r = if cors.contains_key("access-control-allow-origin")
            && request
                .headers()
                .get("access-control-request-method")
                .is_some_and(|m| m == "GET" || m == "HEAD")
        {
            StatusCode::NO_CONTENT
        } else {
            StatusCode::FORBIDDEN
        }
        .into_response();
        *r.headers_mut() = cors;
        if r.status() == StatusCode::FORBIDDEN {
            r.headers_mut().remove("access-control-allow-origin");
        }
        return Ok(r);
    }
    let key = decode_path(
        request
            .uri()
            .path()
            .strip_prefix('/')
            .unwrap_or(request.uri().path()),
    )
    .map_err(|_| unauthorized())?;
    let result = respond(
        app.clone(),
        bucket.id,
        &key,
        request.headers().clone(),
        request.method().clone(),
        true,
        None,
    )
    .await;
    let result = match result {
        Err(e) if bucket.website_enabled && e.has_code(s3s::S3ErrorCode::NoSuchKey) => {
            website_response(app, &bucket, &key, request.into_parts().0).await
        }
        other => other,
    };
    let mut response = result.unwrap_or_else(IntoResponse::into_response);
    response.headers_mut().extend(cors);
    Ok(response)
}
async fn website_response(
    app: Arc<App>,
    bucket: &Bucket,
    key: &str,
    request: axum::http::request::Parts,
) -> Result<Response, HttpError> {
    let directory = key.is_empty() || key.ends_with('/');
    let index = format!(
        "{key}{}{}",
        if directory { "" } else { "/" },
        bucket.index_document
    );
    // Only a missing original object reaches this resolver; private objects never fall back.
    let result = respond(
        app.clone(),
        bucket.id,
        &index,
        if directory {
            request.headers.clone()
        } else {
            HeaderMap::new()
        },
        if directory {
            request.method.clone()
        } else {
            Method::HEAD
        },
        true,
        None,
    )
    .await;
    match result {
        Ok(response) if directory => return Ok(response),
        Ok(_) => {
            let mut path = request.uri.path().replace('\\', "%5C");
            // A Location beginning // would redirect to another origin in a browser.
            if path.starts_with("//") {
                path.replace_range(1..2, "%2F");
            }
            path.push('/');
            if let Some(query) = request.uri.query() {
                path.push('?');
                path.push_str(query);
            }
            return Ok(Response::builder()
                .status(StatusCode::PERMANENT_REDIRECT)
                .header("location", path)
                .header("cache-control", "no-cache")
                .body(Body::empty())?);
        }
        Err(e) if e.has_code(s3s::S3ErrorCode::NoSuchKey) => {}
        Err(e) => return Err(e),
    }
    if !bucket.error_document.is_empty() {
        // The missing resource's Range/If-* headers do not apply to the error document.
        match respond(
            app,
            bucket.id,
            &bucket.error_document,
            HeaderMap::new(),
            request.method.clone(),
            true,
            None,
        )
        .await
        {
            Ok(mut response) => {
                *response.status_mut() = StatusCode::NOT_FOUND;
                for name in ["etag", "last-modified", "accept-ranges"] {
                    response.headers_mut().remove(name);
                }
                response
                    .headers_mut()
                    .insert("cache-control", HeaderValue::from_static("no-store"));
                return Ok(response);
            }
            Err(e)
                if e.has_code(s3s::S3ErrorCode::NoSuchKey)
                    || e.has_code(s3s::S3ErrorCode::AccessDenied) => {}
            Err(e) => return Err(e),
        }
    }
    let body = "<!doctype html><html lang=\"en\"><meta charset=\"utf-8\"><title>404 Not Found</title><h1>404 Not Found</h1></html>";
    Ok(Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header("content-type", "text/html; charset=utf-8")
        .header("content-length", body.len())
        .header("cache-control", "no-store")
        .header("x-content-type-options", "nosniff")
        .body(if request.method == Method::HEAD {
            Body::empty()
        } else {
            Body::from(body)
        })?)
}
async fn respond(
    app: Arc<App>,
    bucket: Uuid,
    key: &str,
    headers: HeaderMap,
    method: Method,
    public: bool,
    preview: Option<bool>,
) -> Result<Response, HttpError> {
    let permits = app.admit(false).await?;
    let (s, pin) = app.current(bucket, key).await?;
    if public && !s.public_read {
        return Err(unauthorized());
    }
    let etag = |name: &str| {
        headers
            .get(name)
            .map(|s| ETagCondition::parse_http_header(s.as_bytes()))
            .transpose()
    };
    let date = |name: &str| {
        headers
            .get(name)
            .and_then(|s| s.to_str().ok())
            .and_then(|s| Timestamp::parse(TimestampFormat::HttpDate, s).ok())
    };
    let condition = s3::conditions(
        &s,
        etag("if-match")?.as_ref(),
        etag("if-none-match")?.as_ref(),
        date("if-modified-since").as_ref(),
        date("if-unmodified-since").as_ref(),
        true,
    );
    if let Err(e) = condition {
        if e.downcast_ref::<s3s::S3Error>()
            .is_some_and(|e| e.code() == &s3s::S3ErrorCode::NotModified)
        {
            return Ok(Response::builder()
                .status(StatusCode::NOT_MODIFIED)
                .header("etag", format!("\"{}\"", s.etag))
                .header("last-modified", s3::http_date(s3::stamp(s.touched_at))?)
                .body(Body::empty())?);
        }
        return Err(e.into());
    }
    let requested = headers
        .get("range")
        .map(|h| Range::parse(h.to_str().unwrap_or("")))
        .transpose()
        .map_err(|_| HttpError(s3::invalid_range(s.size).into()))?;
    let requested = if s3::if_range_matches(&s, &headers) {
        requested
    } else {
        None
    };
    let range = requested
        .map(|r| {
            r.check(s.size as u64)
                .map_err(|_| HttpError(s3::invalid_range(s.size).into()))
        })
        .transpose()?;
    let span = range.clone().unwrap_or(0..s.size as u64);
    let metadata: Metadata = serde_json::from_value(s.metadata.clone())?;
    let mut response = Response::builder().status(if range.is_some() {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    });
    response = response
        .header("content-length", (span.end - span.start).to_string())
        .header("etag", format!("\"{}\"", s.etag))
        .header("last-modified", s3::http_date(s3::stamp(s.touched_at))?)
        .header("accept-ranges", "bytes")
        .header("x-content-type-options", "nosniff");
    if range.is_some() {
        response = response.header(
            "content-range",
            format!("bytes {}-{}/{}", span.start, span.end - 1, s.size),
        );
    }
    let mime = metadata
        .content_type
        .as_deref()
        .unwrap_or("application/octet-stream");
    if let Some(preview) = preview {
        let safe = matches!(
            mime.split(';').next().unwrap_or(""),
            "image/jpeg"
                | "image/png"
                | "image/gif"
                | "image/webp"
                | "image/avif"
                | "video/mp4"
                | "video/webm"
                | "audio/mpeg"
                | "audio/ogg"
                | "audio/mp4"
        );
        response = response
            .header("cache-control", "private, no-store")
            .header("vary", "Cookie")
            .header("content-security-policy", "sandbox; default-src 'none'")
            .header(
                "content-type",
                if preview && safe {
                    mime
                } else {
                    "application/octet-stream"
                },
            )
            .header(
                "content-disposition",
                if preview && safe {
                    "inline"
                } else {
                    "attachment"
                },
            );
    } else {
        response = response.header("content-type", mime);
        for (name, value) in [
            ("cache-control", metadata.cache_control),
            ("content-disposition", metadata.content_disposition),
            ("content-encoding", metadata.content_encoding),
            ("content-language", metadata.content_language),
            ("expires", metadata.expires),
        ] {
            if let Some(value) = value {
                response = response.header(name, value);
            }
        }
    }
    let body = if method == Method::HEAD {
        Body::empty()
    } else {
        Body::new(s3s::Body::from(app.body(
            s,
            pin,
            span.start as i64,
            span.end as i64,
            permits,
        )))
    };
    Ok(response.body(body)?)
}
