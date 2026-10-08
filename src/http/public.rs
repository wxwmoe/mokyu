use super::errors::{ErrorCode, HttpError, unauthorized};
use super::s3::{ReadOptions, decode_path, respond};
use crate::app::{App, Bucket};
use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    body::Body,
    extract::State,
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};

#[derive(Clone, serde::Serialize, Deserialize, utoipa::ToSchema)]
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

pub fn status_router(app: Arc<App>) -> Router {
    Router::new()
        .route("/status", axum::routing::get(status))
        .route("/api/v1/cli", axum::routing::post(status_cli))
        .with_state(app)
}

async fn status(State(app): State<Arc<App>>) -> Result<Json<Value>, HttpError> {
    Ok(Json(app.status().await?))
}

async fn status_cli(
    State(app): State<Arc<App>>,
    Json(command): Json<crate::cli::Command>,
) -> Result<Json<Value>, HttpError> {
    let crate::cli::Command::Status { ready } = command else {
        return Err(
            anyhow::anyhow!("only status is available while the service is restricted").into(),
        );
    };
    let result = app.status().await?;
    if ready {
        if app.mode_mismatch().await? {
            return Err(anyhow::anyhow!("service is in mode_mismatch").into());
        }
        if app.recovery_required() {
            return Err(anyhow::anyhow!("node recovery is required").into());
        }
        if app.role != "single" && app.node.get().is_none() {
            return Err(
                anyhow::anyhow!("node is not registered for the current deployment mode").into(),
            );
        }
    }
    Ok(Json(serde_json::json!({"ok": true, "result": result})))
}
pub(crate) async fn observe(
    State((app, index)): State<(Arc<App>, usize)>,
    mut request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    use tracing::Instrument;
    let observation = crate::stats::Request::new(
        &app.statistics.http[index],
        if index == 1 { "web" } else { "control" },
        request.method().clone(),
    );
    request.extensions_mut().insert(observation.context.clone());
    let response = next.run(request).instrument(observation.span.clone()).await;
    observation.response(response, false).map(Body::new)
}

async fn public(
    State(app): State<Arc<App>>,
    request: axum::extract::Request,
) -> Result<Response, HttpError> {
    if app.mode_mismatch().await? {
        return Err(s3s::s3_error!(ServiceUnavailable).into());
    }
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
        "SELECT b.* FROM buckets b JOIN bucket_domains d ON d.bucket_id=b.id WHERE d.host=$1",
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
        ReadOptions::public(),
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
        ReadOptions::public(),
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
            ReadOptions::public(),
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
