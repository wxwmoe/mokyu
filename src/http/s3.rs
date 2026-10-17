use super::errors::{HttpError, unauthorized};
use super::public::cors_headers;
use crate::{
    app::{App, Bucket, Metadata},
    config,
    http::gateway,
};
use anyhow::{Context, Result, ensure};
use axum::{
    body::Body,
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
};
use s3s::dto::{ETagCondition, Range, Timestamp, TimestampFormat};
use serde_json::Value;
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

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
        if app.mode_mismatch().await? {
            return Err(s3s::s3_error!(ServiceUnavailable).into());
        }
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
            .requests
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
            bucket = app.buckets.by_name(&host, false).await?;
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
            bucket = app.buckets.by_name(&name, true).await?;
        }
        if let Some(bucket) = bucket {
            cors = cors_headers(&bucket, request.headers(), request.method().as_str())?;
            request.extensions_mut().insert(bucket);
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
                Duration::from_secs(config::seconds(&app.config.uploads.client_idle_timeout)?),
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
    super::compression::merge(response.headers_mut(), cors);
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

pub(crate) struct ReadOptions {
    pub public: bool,
    pub preview: Option<bool>,
    pub version: Option<uuid::Uuid>,
}
impl ReadOptions {
    pub(super) fn public() -> Self {
        Self {
            public: true,
            preview: None,
            version: None,
        }
    }
    pub fn managed(preview: bool, version: Option<uuid::Uuid>) -> Self {
        Self {
            public: false,
            preview: Some(preview),
            version,
        }
    }
}
pub(crate) async fn respond(
    app: Arc<App>,
    bucket: Uuid,
    key: &str,
    headers: HeaderMap,
    method: Method,
    options: ReadOptions,
) -> Result<Response, HttpError> {
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
    let im = etag("if-match")?;
    let inm = etag("if-none-match")?;
    let ims = date("if-modified-since");
    let iums = date("if-unmodified-since");
    let light =
        method == Method::HEAD || im.is_some() || inm.is_some() || ims.is_some() || iums.is_some();
    let (mut s, mut pin) = if light {
        (app.metadata(bucket, key).await?, None)
    } else {
        let (s, pin) = app.current(bucket, key).await?;
        (s, Some(pin))
    };
    loop {
        if options.public && !s.public_read {
            return Err(unauthorized());
        }
        if options.version.is_some_and(|id| id != s.id) {
            return Err(s3s::s3_error!(PreconditionFailed).into());
        }
        let condition = gateway::conditions(
            &s,
            im.as_ref(),
            inm.as_ref(),
            ims.as_ref(),
            iums.as_ref(),
            true,
        );
        if let Err(e) = condition {
            if e.downcast_ref::<s3s::S3Error>()
                .is_some_and(|e| e.code() == &s3s::S3ErrorCode::NotModified)
            {
                return Ok(Response::builder()
                    .status(StatusCode::NOT_MODIFIED)
                    .header("etag", format!("\"{}\"", s.etag))
                    .header(
                        "last-modified",
                        gateway::http_date(gateway::stamp(s.touched_at))?,
                    )
                    .body(Body::empty())?);
            }
            return Err(e.into());
        }
        if method == Method::HEAD || pin.is_some() {
            break;
        }
        // Recheck permissions and conditions on the atomically protected version.
        let current = app.current(bucket, key).await?;
        s = current.0;
        pin = Some(current.1);
    }
    let requested = headers
        .get("range")
        .map(|h| Range::parse(h.to_str().unwrap_or("")))
        .transpose()
        .map_err(|_| HttpError(gateway::invalid_range(s.size).into()))?;
    let requested = if gateway::if_range_matches(&s, &headers) {
        requested
    } else {
        None
    };
    let range = requested
        .map(|r| {
            r.check(s.size as u64)
                .map_err(|_| HttpError(gateway::invalid_range(s.size).into()))
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
        .header(
            "last-modified",
            gateway::http_date(gateway::stamp(s.touched_at))?,
        )
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
    if let Some(preview) = options.preview {
        let safe = metadata
            .content_encoding
            .as_deref()
            .is_none_or(|v| v.is_empty() || v == "identity")
            && matches!(
                mime.split(';')
                    .next()
                    .unwrap_or("")
                    .trim()
                    .to_ascii_lowercase()
                    .as_str(),
                "image/jpeg"
                    | "image/png"
                    | "image/gif"
                    | "image/webp"
                    | "image/avif"
                    | "video/mp4"
                    | "video/webm"
                    | "video/ogg"
                    | "audio/mpeg"
                    | "audio/ogg"
                    | "audio/mp4"
                    | "audio/webm"
                    | "audio/wav"
                    | "audio/flac"
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
        let permits = app.admit(false).await?;
        Body::new(s3s::Body::from(app.body(
            s,
            pin.expect("body read is protected"),
            span.start as i64,
            span.end as i64,
            permits,
            range.is_some(),
        )))
    };
    Ok(response.body(body)?)
}
