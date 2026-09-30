use crate::{
    app::{App, Bucket, Metadata},
    config, s3,
};
use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    body::Body,
    extract::State,
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
};
use s3s::dto::{ETagCondition, Range, Timestamp, TimestampFormat};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

#[derive(Debug)]
pub(crate) struct HttpError(pub(crate) anyhow::Error);
#[derive(Clone)]
pub(crate) struct ErrorCode(pub String);
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
        response.extensions_mut().insert(ErrorCode(
            self.0
                .downcast_ref::<s3s::S3Error>()
                .map(|error| error.code().as_str().to_owned())
                .unwrap_or_else(|| "InternalError".into()),
        ));
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
pub(crate) fn unauthorized() -> HttpError {
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
pub(crate) async fn observe(
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
pub(crate) async fn respond(
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
            range.is_some(),
        )))
    };
    Ok(response.body(body)?)
}
