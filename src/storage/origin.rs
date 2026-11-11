use crate::model::{
    external::Snapshot,
    mirroring::{Config, Source},
};
use anyhow::{Context, Result, ensure};
use axum::{
    body::Body,
    http::{HeaderMap, Method, Request, Response, Uri, header},
};
use bytes::Bytes;
use futures_util::{StreamExt, stream::BoxStream};
use hyper_util::{
    client::legacy::{
        Client,
        connect::{HttpConnector, dns::Name},
    },
    rt::TokioExecutor,
};
use std::{
    collections::HashMap,
    future::Future,
    io,
    net::{IpAddr, SocketAddr},
    pin::Pin,
    sync::{Arc, Mutex, OnceLock},
    task::{Context as TaskContext, Poll},
    time::Duration,
};
use tower::Service;

#[path = "origin_range.rs"]
mod ranges;
pub(crate) use ranges::{changed, observed, ranged, requested};

pub(crate) fn retry_backup(error: &anyhow::Error) -> bool {
    error.downcast_ref::<s3s::S3Error>().is_some_and(|error| {
        matches!(
            error.code(),
            s3s::S3ErrorCode::NoSuchKey
                | s3s::S3ErrorCode::ServiceUnavailable
                | s3s::S3ErrorCode::SlowDown
                | s3s::S3ErrorCode::RequestTimeout
        )
    })
}

type HttpClient = Client<hyper_rustls::HttpsConnector<HttpConnector<Resolver>>, Body>;
type HttpClients = HashMap<(bool, u32), HttpClient>;
type S3Clients = HashMap<(uuid::Uuid, i64, u32, u32), Arc<super::external::Client>>;

#[derive(Clone)]
struct Resolver {
    private: bool,
}

fn allowed(ip: IpAddr, private: bool) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            !ip.is_loopback()
                && !ip.is_link_local()
                && !ip.is_unspecified()
                && !ip.is_broadcast()
                && !ip.is_multicast()
                && ip.octets()[0] != 0
                && ip.octets()[0] < 224
                && (private
                    || !ip.is_private()
                        && !(ip.octets()[0] == 100 && (64..128).contains(&ip.octets()[1])))
                && ip.octets() != [100, 100, 100, 200]
        }
        IpAddr::V6(ip) => {
            if let Some(ip) = ip.to_ipv4_mapped() {
                return allowed(ip.into(), private);
            }
            !ip.is_loopback()
                && !ip.is_unspecified()
                && !ip.is_multicast()
                && ip.segments() != [0xfd00, 0x0ec2, 0, 0, 0, 0, 0, 0x0254]
                && (ip.segments()[0] & 0xffc0) != 0xfe80
                && ((ip.segments()[0] & 0xe000) == 0x2000
                    || private && (ip.segments()[0] & 0xfe00) == 0xfc00)
        }
    }
}

impl Service<Name> for Resolver {
    type Response = std::vec::IntoIter<SocketAddr>;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = io::Result<Self::Response>> + Send>>;

    fn poll_ready(&mut self, _: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, name: Name) -> Self::Future {
        let private = self.private;
        Box::pin(async move {
            let addresses: Vec<_> = tokio::net::lookup_host((name.as_str(), 0)).await?.collect();
            if addresses.is_empty()
                || addresses
                    .iter()
                    .any(|address| !allowed(address.ip(), private))
            {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "origin address is not allowed",
                ));
            }
            Ok(addresses.into_iter())
        })
    }
}

fn http(private: bool, timeout: u32) -> Result<HttpClient> {
    static CLIENTS: OnceLock<Mutex<HttpClients>> = OnceLock::new();
    let mut clients = CLIENTS.get_or_init(Mutex::default).lock().unwrap();
    if let Some(client) = clients.get(&(private, timeout)) {
        return Ok(client.clone());
    }
    let mut connector = HttpConnector::new_with_resolver(Resolver { private });
    connector.enforce_http(false);
    connector.set_connect_timeout(Some(Duration::from_secs(timeout.into())));
    let connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_provider_and_native_roots(rustls::crypto::aws_lc_rs::default_provider())?
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .wrap_connector(connector);
    let client = Client::builder(TokioExecutor::new())
        .pool_idle_timeout(Duration::from_secs(60))
        .pool_max_idle_per_host(4)
        .build(connector);
    if clients.len() >= 64 {
        clients.clear();
    }
    clients.insert((private, timeout), client.clone());
    Ok(client)
}

fn s3(snapshot: &Snapshot, config: &Config) -> Result<Arc<super::external::Client>> {
    static CLIENTS: OnceLock<Mutex<S3Clients>> = OnceLock::new();
    let mut clients = CLIENTS.get_or_init(Mutex::default).lock().unwrap();
    let key = (
        snapshot.external_id,
        snapshot.revision,
        config.header_timeout_seconds,
        config.connect_timeout_seconds,
    );
    if let Some(client) = clients.get(&key) {
        return Ok(client.clone());
    }
    snapshot.validate()?;
    let client = Arc::new(snapshot.connection.connect(
        &snapshot.access_key,
        &snapshot.secret_key,
        super::model::Options {
            connect_timeout: Duration::from_secs(config.connect_timeout_seconds.into()),
            request_timeout: Duration::from_secs(config.header_timeout_seconds.into()),
            retry_timeout: Duration::ZERO,
            max_retries: 0,
        },
    )?);
    if clients.len() >= 64 {
        clients.clear();
    }
    clients.insert(key, client.clone());
    Ok(client)
}

pub(crate) struct Open {
    pub status: axum::http::StatusCode,
    pub headers: HeaderMap,
    pub length: Option<u64>,
    pub body: BoxStream<'static, Result<Bytes>>,
    pub bytes: Arc<std::sync::atomic::AtomicU64>,
}

pub(crate) async fn open(
    source: &Source,
    config: &Config,
    key: &str,
    snapshot: Option<&Snapshot>,
    method: Method,
    extra: HeaderMap,
) -> Result<Open> {
    versioned(source, config, key, snapshot, method, extra, None).await
}

async fn versioned(
    source: &Source,
    config: &Config,
    key: &str,
    snapshot: Option<&Snapshot>,
    method: Method,
    mut extra: HeaderMap,
    version: Option<&str>,
) -> Result<Open> {
    source.validate()?;
    extra.insert("x-mokyu-origin-hop", "1".parse()?);
    let response = match source {
        Source::Http {
            allow_private,
            follow_redirects,
            headers,
            ..
        } => {
            let mut uri: Uri = source.address(key)?.parse()?;
            let client = http(*allow_private, config.connect_timeout_seconds)?;
            let mut remaining = if *follow_redirects { 3 } else { 0 };
            loop {
                if let Ok(ip) = uri
                    .host()
                    .context("origin hostname missing")?
                    .trim_matches(['[', ']'])
                    .parse::<IpAddr>()
                {
                    ensure!(
                        allowed(ip, *allow_private),
                        s3s::s3_error!(InvalidArgument, "origin address is not allowed")
                    );
                }
                let mut request = Request::builder()
                    .method(method.clone())
                    .uri(uri.clone())
                    .body(Body::empty())?;
                *request.headers_mut() = extra.clone();
                request
                    .headers_mut()
                    .insert(header::ACCEPT_ENCODING, "identity".parse()?);
                request
                    .headers_mut()
                    .insert("x-mokyu-origin-hop", "1".parse()?);
                if let Some(headers) = headers {
                    for (name, value) in headers {
                        let mut value = value.parse::<axum::http::HeaderValue>()?;
                        value.set_sensitive(true);
                        request
                            .headers_mut()
                            .insert(name.parse::<axum::http::HeaderName>()?, value);
                    }
                }
                let response = tokio::time::timeout(
                    Duration::from_secs(config.header_timeout_seconds.into()),
                    client.request(request),
                )
                .await
                .map_err(|_| s3s::s3_error!(RequestTimeout, "origin response timed out"))?
                .map_err(|_| s3s::s3_error!(ServiceUnavailable, "origin connection failed"))?;
                if !response.status().is_redirection() {
                    break response.map(Body::new);
                }
                ensure!(
                    remaining > 0,
                    s3s::s3_error!(ServiceUnavailable, "origin redirect is not allowed")
                );
                let location = response
                    .headers()
                    .get(header::LOCATION)
                    .context("origin redirect location missing")?
                    .to_str()?;
                let target = reqwest::Url::parse(&uri.to_string())?.join(location)?;
                ensure!(
                    target.username().is_empty()
                        && target.password().is_none()
                        && target.fragment().is_none(),
                    s3s::s3_error!(ServiceUnavailable, "invalid origin redirect")
                );
                let target: Uri = target.as_str().parse()?;
                // Fixed credentials never leave the configured origin, including on redirects.
                ensure!(
                    target.scheme() == uri.scheme() && target.authority() == uri.authority(),
                    s3s::s3_error!(ServiceUnavailable, "cross-origin redirect is not allowed")
                );
                uri = target;
                remaining -= 1;
            }
        }
        Source::S3 { external_id, .. } => {
            let snapshot = snapshot.context("external origin connection missing")?;
            ensure!(
                snapshot.external_id == *external_id,
                "external origin identity differs"
            );
            let query = version
                .map(|value| vec![("versionId", value)])
                .unwrap_or_default();
            let key = source.key(key)?;
            s3(snapshot, config)?
                .bytes(method.clone(), Some(&key), &query, extra, Bytes::new())
                .await
                .map_err(|_| s3s::s3_error!(ServiceUnavailable, "external origin request failed"))?
        }
    };
    opened(response, config, method == Method::HEAD)
}

fn opened(response: Response<Body>, config: &Config, head: bool) -> Result<Open> {
    match response.status().as_u16() {
        200 | 206 => {}
        404 => return Err(s3s::s3_error!(NoSuchKey).into()),
        401 | 403 => return Err(s3s::s3_error!(AccessDenied, "origin authorization failed").into()),
        412 => return Err(s3s::s3_error!(PreconditionFailed, "origin version changed").into()),
        416 => return Err(s3s::s3_error!(InvalidRange).into()),
        429 | 503 => return Err(s3s::s3_error!(SlowDown, "origin is busy").into()),
        _ => return Err(s3s::s3_error!(ServiceUnavailable, "origin request was rejected").into()),
    }
    let status = response.status();
    let headers = response.headers().clone();
    let length = headers
        .get(header::CONTENT_LENGTH)
        .map(|value| value.to_str()?.parse::<u64>().map_err(anyhow::Error::from))
        .transpose()?;
    let limit = config.max_object_size.unwrap_or(5 * 1024_u64.pow(4));
    ensure!(
        length.is_none_or(|length| length <= limit),
        s3s::s3_error!(EntityTooLarge)
    );
    let idle = Duration::from_secs(config.idle_timeout_seconds.into());
    let bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let counts = bytes.clone();
    let mut source = response.into_body().into_data_stream();
    let body = async_stream::try_stream! {
        let mut received = 0_u64;
        loop {
            let next = tokio::time::timeout(idle, source.next()).await.map_err(|_| s3s::s3_error!(RequestTimeout, "origin body timed out"))?;
            let Some(bytes) = next else { break; };
            let bytes = bytes.map_err(|_| s3s::s3_error!(IncompleteBody, "origin body was interrupted"))?;
            received = received.checked_add(bytes.len() as u64).context("origin length overflow")?;
            counts.store(received, std::sync::atomic::Ordering::Relaxed);
            if received > limit || length.is_some_and(|length| received > length) { Err(s3s::s3_error!(EntityTooLarge))?; }
            yield bytes;
        }
        if !head && length.is_some_and(|length| length != received) { Err(s3s::s3_error!(IncompleteBody))?; }
    }.boxed();
    Ok(Open {
        status,
        headers,
        length,
        body,
        bytes,
    })
}

pub(crate) fn integrity(headers: &HeaderMap) -> HeaderMap {
    let full = headers
        .get("x-amz-checksum-type")
        .is_none_or(|value| value == "FULL_OBJECT");
    headers
        .iter()
        .filter(|(name, _)| {
            name.as_str() == "content-md5"
                || full
                    && matches!(
                        name.as_str(),
                        "x-amz-checksum-crc32"
                            | "x-amz-checksum-crc32c"
                            | "x-amz-checksum-crc64nvme"
                            | "x-amz-checksum-sha1"
                            | "x-amz-checksum-sha256"
                    )
        })
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

pub(crate) fn metadata(headers: &HeaderMap) -> Result<crate::app::Metadata> {
    let field = |name| {
        headers
            .get(name)
            .map(|value| value.to_str().map(str::to_owned))
            .transpose()
    };
    Ok(crate::app::Metadata {
        content_type: field(header::CONTENT_TYPE)?,
        content_encoding: field(header::CONTENT_ENCODING)?,
        content_disposition: field(header::CONTENT_DISPOSITION)?,
        content_language: field(header::CONTENT_LANGUAGE)?,
        cache_control: field(header::CACHE_CONTROL)?,
        expires: field(header::EXPIRES)?,
        user: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origin_checksums_require_full_object_semantics() {
        let mut headers = HeaderMap::new();
        headers.insert("content-md5", "1B2M2Y8AsgTpgAmY7PhCfg==".parse().unwrap());
        let mut checksum = crate::upload::Integrity::new(&integrity(&headers), None).unwrap();
        checksum.update(b"not empty");
        assert!(checksum.finish(None).is_err());
        headers.insert("x-amz-checksum-type", "COMPOSITE".parse().unwrap());
        headers.insert("x-amz-checksum-sha256", "composite-2".parse().unwrap());
        assert!(!integrity(&headers).contains_key("x-amz-checksum-sha256"));
    }

    #[test]
    fn origin_addresses_require_explicit_private_access() {
        for ip in [
            "127.0.0.1",
            "169.254.169.254",
            "0.0.0.0",
            "::1",
            "::ffff:127.0.0.1",
            "fe80::1",
            "100.100.100.200",
            "fd00:ec2::254",
        ] {
            assert!(!allowed(ip.parse().unwrap(), true), "{ip}");
        }
        assert!(!allowed("192.168.1.1".parse().unwrap(), false));
        assert!(allowed("192.168.1.1".parse().unwrap(), true));
        assert!(allowed("1.1.1.1".parse().unwrap(), false));
    }

    #[tokio::test]
    async fn a_truncated_origin_body_never_completes() {
        let response = Response::builder()
            .header("content-length", "8")
            .body(Body::from("short"))
            .unwrap();
        let mut result = opened(response, &Config::default(), false).unwrap().body;
        assert_eq!(result.next().await.unwrap().unwrap(), "short");
        assert!(result.next().await.unwrap().is_err());
    }
}
