use super::cors_headers;
use super::errors::HttpError;
use crate::model::Id;
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
use hyper::body::Body as _;
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
    let close_on_error = matches!(
        request.version(),
        hyper::Version::HTTP_10 | hyper::Version::HTTP_11
    ) && !request.body().is_end_stream();
    let head = request.method() == Method::HEAD;
    let read = head || request.method() == Method::GET;
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
        let alias = (!signed
            && matches!(
                *request.method(),
                Method::GET | Method::HEAD | Method::OPTIONS
            )
            && virtual_bucket.is_none()
            && domain.as_ref() != Some(&host)
            && s3s::path::check_bucket_name(&host))
        .then_some(host.as_str());
        // Conditional GETs keep the second authority check after metadata evaluation.
        let route = (read
            && (head || !super::object::conditional(request.headers()))
            && query.iter().all(|name| {
                let lower = name.to_ascii_lowercase();
                lower.starts_with("x-amz-")
                    || lower.starts_with("response-")
                    || matches!(name.as_str(), "" | "x-id" | "versionId" | "partNumber")
            }))
        .then(|| read_route(request.uri().path(), virtual_bucket, alias))
        .flatten();
        let access = access_key(request.headers(), request.uri().query());
        let prepared = if let Some(route) = route {
            app.prepare_read(
                &route,
                access.as_deref(),
                !signed && super::object::reuse_public(request.headers()),
            )
            .await?
        } else {
            None
        };
        let mut bucket: Option<Bucket> = prepared.as_ref().map(|read| read.snapshot.bucket.clone());
        if request.method() == Method::OPTIONS
            && !signed
            && super::object::reuse_public(request.headers())
            && let Some(route) = read_route(request.uri().path(), virtual_bucket, alias)
        {
            bucket = app.public_reads.bucket(&route, &app.storage);
        }
        if let Some(alias) = alias {
            if bucket.is_none() {
                bucket = app.buckets.by_name(alias, false).await?;
            }
            if let Some(b) = &bucket
                && b.name == alias
            {
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
            cors = cors_headers(
                &app.cors_cache,
                &bucket,
                request.headers(),
                request.method().as_str(),
            )?;
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
                    .clone()
                    .try_acquire_owned()
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
        let mut response = crate::app::reads::scope(prepared, service.call(request))
            .await
            .map_err(|e| anyhow::anyhow!("S3 HTTP failure: {e:?}"))?;
        if complete {
            let body = std::mem::replace(response.body_mut(), s3s::Body::empty());
            *response.body_mut() = s3s::Body::http_body_unsync(CompletionBody {
                inner: body,
                last: tokio::time::Instant::now(),
                _permit: _control,
            });
            response.headers_mut().insert(
                hyper::header::CACHE_CONTROL,
                HeaderValue::from_static("no-store"),
            );
            response
                .headers_mut()
                .insert("x-accel-buffering", HeaderValue::from_static("no"));
        }
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
    // Rejected uploads may leave unread request bytes on an HTTP/1 connection.
    if close_on_error
        && (response.status().is_client_error() || response.status().is_server_error())
    {
        response
            .headers_mut()
            .insert(hyper::header::CONNECTION, HeaderValue::from_static("close"));
    }
    super::compression::merge(response.headers_mut(), cors);
    if read {
        super::compression::vary(
            response.headers_mut(),
            "Accept-Encoding, Authorization, X-Amz-Checksum-Mode",
        );
    }
    if head
        && response.headers().contains_key("content-encoding")
        && !response.headers().contains_key("content-length")
    {
        *response.body_mut() =
            s3s::Body::from(super::object::blob(Box::pin(futures_util::stream::empty())));
    }
    response
}

fn read_route(
    path: &str,
    virtual_bucket: Option<&str>,
    alias: Option<&str>,
) -> Option<crate::model::ReadRoute> {
    let decoded = decode_path(path).ok()?;
    let parsed = s3s::path::parse_virtual_hosted_style(virtual_bucket, &decoded).ok();
    let (name, key) = match parsed.as_ref().and_then(s3s::path::S3Path::as_object) {
        Some((name, key)) => (name.to_owned(), key.to_owned()),
        None if alias.is_some() => (String::new(), String::new()),
        None => return None,
    };
    Some(crate::model::ReadRoute::Bucket {
        name,
        key,
        alias: alias.map(|name| {
            (
                name.to_owned(),
                decoded.strip_prefix('/').unwrap_or(&decoded).to_owned(),
            )
        }),
    })
}

fn access_key(headers: &HeaderMap, query: Option<&str>) -> Option<String> {
    // This is a query hint only; s3s still validates the complete signature and requested key.
    let credential = if let Some(header) = headers.get(hyper::header::AUTHORIZATION) {
        let header = header.to_str().ok()?.strip_prefix("AWS4-HMAC-SHA256 ")?;
        let mut values = header
            .split(',')
            .filter_map(|part| part.trim().strip_prefix("Credential="));
        let credential = values.next()?;
        if values.next().is_some() {
            return None;
        }
        credential.to_owned()
    } else {
        let uri = format!("/?{}", query?).parse::<hyper::Uri>().ok()?;
        let axum::extract::Query(values) =
            axum::extract::Query::<Vec<(String, String)>>::try_from_uri(&uri).ok()?;
        let mut values = values
            .into_iter()
            .filter(|(name, _)| name == "X-Amz-Credential");
        let (_, credential) = values.next()?;
        if values.next().is_some() {
            return None;
        }
        credential
    };
    let (key, _) = credential.split_once('/')?;
    (!key.is_empty() && key.len() <= 128).then(|| key.to_owned())
}

struct CompletionBody {
    inner: s3s::Body,
    last: tokio::time::Instant,
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl hyper::body::Body for CompletionBody {
    type Data = bytes::Bytes;
    type Error = <s3s::Body as hyper::body::Body>::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<std::result::Result<hyper::body::Frame<Self::Data>, Self::Error>>>
    {
        loop {
            let result = std::task::ready!(std::pin::Pin::new(&mut self.inner).poll_frame(cx));
            if let Some(Ok(frame)) = &result {
                // s3s emits whitespace every 100 ms; preserve XML and trailers without delaying them.
                if frame
                    .data_ref()
                    .is_some_and(|data| data.iter().all(u8::is_ascii_whitespace))
                    && self.last.elapsed() < Duration::from_secs(5)
                {
                    continue;
                }
                self.last = tokio::time::Instant::now();
            }
            return std::task::Poll::Ready(result);
        }
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.inner.size_hint()
    }
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
}

#[cfg(test)]
mod completion_tests {
    use super::*;
    use hyper::body::{Body as _, Frame};

    #[test]
    fn read_hints_preserve_encoded_keys_and_signature_boundaries() {
        use crate::model::ReadRoute;
        let Some(ReadRoute::Bucket {
            name,
            key,
            alias: None,
        }) = read_route("/bucket/a%2Fb%2B%252F", None, None)
        else {
            panic!("object route")
        };
        assert_eq!(name, "bucket");
        assert_eq!(key, "a/b+%2F");
        let Some(ReadRoute::Bucket { name, key, alias }) =
            read_route("/other/key", None, Some("public"))
        else {
            panic!("alias route")
        };
        assert_eq!((name.as_str(), key.as_str()), ("other", "key"));
        assert_eq!(alias, Some(("public".into(), "other/key".into())));
        assert!(read_route("/bucket", None, None).is_none());
        assert!(read_route("/bucket/%xx", None, None).is_none());
        let Some(ReadRoute::Bucket { name, key, .. }) = read_route("/a%2Fb", Some("virtual"), None)
        else {
            panic!("virtual route")
        };
        assert_eq!((name.as_str(), key.as_str()), ("virtual", "a/b"));
        let mut headers = HeaderMap::new();
        let query = "X-Amz-Credential=access%2F20261023%2Fregion%2Fs3%2Faws4_request";
        assert_eq!(access_key(&headers, Some(query)).as_deref(), Some("access"));
        assert!(access_key(&headers, Some(&format!("{query}&{query}"))).is_none());
        headers.insert("authorization", HeaderValue::from_static("invalid"));
        assert!(access_key(&headers, Some(query)).is_none());
        headers.insert("authorization", format!("AWS4-HMAC-SHA256 Credential=header/20261023/region/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature={}", "0".repeat(64)).parse().unwrap());
        assert_eq!(access_key(&headers, Some(query)).as_deref(), Some("header"));
    }

    struct Frames(std::collections::VecDeque<Frame<bytes::Bytes>>);
    impl hyper::body::Body for Frames {
        type Data = bytes::Bytes;
        type Error = std::io::Error;
        fn poll_frame(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<std::result::Result<Frame<Self::Data>, Self::Error>>> {
            std::task::Poll::Ready(self.0.pop_front().map(Ok))
        }
    }

    #[tokio::test]
    async fn heartbeat_filter_keeps_xml_trailers_and_waiter_admission() {
        let slots = Arc::new(tokio::sync::Semaphore::new(1));
        let mut trailers = HeaderMap::new();
        trailers.insert("x-test-result", HeaderValue::from_static("complete"));
        let source = Frames(
            [
                Frame::data(bytes::Bytes::from_static(b"<?xml version=\"1.0\"?>")),
                Frame::data(bytes::Bytes::from_static(b" \n")),
                Frame::data(bytes::Bytes::from_static(b"<Result/>")),
                Frame::trailers(trailers),
            ]
            .into(),
        );
        let mut body = CompletionBody {
            inner: s3s::Body::http_body_unsync(source),
            last: tokio::time::Instant::now(),
            _permit: Some(slots.clone().try_acquire_owned().unwrap()),
        };
        assert!(slots.try_acquire().is_err());
        let mut data = Vec::new();
        let mut trailers = false;
        while let Some(frame) =
            futures_util::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx)).await
        {
            let frame = frame.unwrap();
            if let Some(bytes) = frame.data_ref() {
                data.extend_from_slice(bytes);
            }
            if let Some(headers) = frame.trailers_ref() {
                assert_eq!(headers["x-test-result"], "complete");
                trailers = true;
            }
        }
        assert!(trailers);
        assert_eq!(data, b"<?xml version=\"1.0\"?><Result/>");
        drop(body);
        assert!(slots.try_acquire().is_ok());
    }
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
    pub public: Option<Bucket>,
    pub preview: Option<bool>,
    pub version: Option<Id>,
}
impl ReadOptions {
    pub(super) fn public(bucket: &Bucket) -> Self {
        Self {
            public: Some(bucket.clone()),
            preview: None,
            version: None,
        }
    }
    pub fn managed(preview: bool, version: Option<Id>) -> Self {
        Self {
            public: None,
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
    if let Some(public_bucket) = &options.public {
        let read = super::object::read(
            &app,
            public_bucket,
            key,
            &headers,
            method == Method::HEAD,
            &HeaderMap::new(),
            false,
        )
        .await?;
        let mut response = Response::new(match read.body {
            Some(body) => Body::new(s3s::Body::from(body)),
            None => {
                Body::from_stream(futures_util::stream::empty::<std::io::Result<bytes::Bytes>>())
            }
        });
        *response.status_mut() = read.status;
        *response.headers_mut() = read.headers;
        return Ok(response);
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
        .map(|r| gateway::checked_range(r, s.size).map_err(|e| HttpError(e.into())))
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
        Body::new(s3s::Body::from(
            app.body(
                s,
                pin.expect("body read is protected"),
                span.start as i64..span.end as i64,
                permits,
                range.is_some(),
                None,
            )
            .await?,
        ))
    };
    Ok(response.body(body)?)
}
