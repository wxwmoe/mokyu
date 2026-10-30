use super::model::Options;
use crate::app::listing::encode_key;
use anyhow::{Context, Result, ensure};
use axum::{
    body::Body,
    http::{HeaderMap, Method, Request, Response, Uri, header},
};
use bytes::Bytes;
use chrono::Utc;
use futures_util::StreamExt;
use hyper_util::{
    client::legacy::{Client as HttpClient, connect::HttpConnector},
    rt::TokioExecutor,
};
use object_store::path::Path;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::Duration;
use utoipa::ToSchema;

#[path = "external_objects.rs"]
pub(crate) mod objects;

#[path = "external_transfer.rs"]
pub(crate) mod transfer;

pub(crate) fn credentials(access: &str, secret: &str) -> Result<()> {
    ensure!(
        (1..=256).contains(&access.len())
            && access
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b)),
        "invalid external access key"
    );
    ensure!(
        (1..=4096).contains(&secret.len()) && !secret.contains('\0'),
        "invalid external secret key"
    );
    Ok(())
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = ExternalConnection)]
pub(crate) struct Connection {
    pub endpoint_url: String,
    pub region: String,
    pub bucket: String,
    #[serde(default)]
    pub prefix: String,
    #[serde(default)]
    pub virtual_hosted: bool,
    #[serde(default)]
    pub allow_http: bool,
}

impl Connection {
    pub(crate) fn object_address(&self, key: &str) -> Result<String> {
        let mut connection = self.clone();
        connection.virtual_hosted = false;
        Ok(connection.uri(Some(key), &[])?.to_string())
    }

    pub(crate) fn object_prefix(&self, prefix: &str) -> Result<String> {
        if prefix.is_empty() {
            Ok(self.object_address("_")?.trim_end_matches('_').to_owned())
        } else {
            self.object_address(prefix)
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            self.endpoint_url.len() <= 2048,
            "external endpoint is too long"
        );
        let url = reqwest::Url::parse(&self.endpoint_url)?;
        ensure!(
            (url.scheme() == "https" || self.allow_http && url.scheme() == "http")
                && url.host_str().is_some()
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none(),
            "external endpoint must be an HTTP or HTTPS URL without credentials, query or fragment"
        );
        for (value, maximum, label) in
            [(&self.region, 128, "region"), (&self.bucket, 255, "bucket")]
        {
            ensure!(
                !value.is_empty() && value.len() <= maximum && !value.chars().any(char::is_control),
                "invalid external {label}"
            );
        }
        ensure!(
            !self.bucket.contains('/') && !self.bucket.contains('\\'),
            "external bucket must not contain a path"
        );
        ensure!(
            self.prefix.len() <= 512
                && !self.prefix.starts_with('/')
                && !self.prefix.ends_with('/')
                && !self.prefix.chars().any(char::is_control),
            "invalid external prefix"
        );
        ensure!(
            Path::parse(&self.prefix)?.as_ref() == self.prefix,
            "external prefix must be canonical"
        );
        Ok(())
    }

    pub(crate) fn connect(&self, access: &str, secret: &str, options: Options) -> Result<Client> {
        self.validate()?;
        credentials(access, secret)?;
        let mut connector = HttpConnector::new();
        connector.enforce_http(false);
        connector.set_connect_timeout(Some(options.connect_timeout));
        let connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_provider_and_native_roots(rustls::crypto::aws_lc_rs::default_provider())?
            .https_or_http()
            .enable_http1()
            .wrap_connector(connector);
        let client = HttpClient::builder(TokioExecutor::new())
            .pool_idle_timeout(Duration::from_secs(60))
            .pool_max_idle_per_host(4)
            .build(connector);
        Ok(Client {
            connection: self.clone(),
            access: access.to_owned(),
            secret: secret.to_owned(),
            client,
            timeout: options.request_timeout,
            memory: None,
        })
    }

    fn uri(&self, key: Option<&str>, query: &[(&str, &str)]) -> Result<Uri> {
        let mut endpoint = reqwest::Url::parse(&self.endpoint_url)?;
        if self.virtual_hosted {
            let host = endpoint
                .host_str()
                .context("external endpoint has no host")?;
            endpoint.set_host(Some(&format!("{}.{host}", self.bucket)))?;
        }
        let origin = endpoint.origin().ascii_serialization();
        let mut path = endpoint.path().trim_end_matches('/').to_owned();
        if !self.virtual_hosted {
            path.push('/');
            path.push_str(&encode_key(&self.bucket));
        }
        path.push('/');
        if let Some(key) = key {
            ensure!(
                !key.is_empty() && !key.contains('\0'),
                "invalid external object key"
            );
            let scoped = if self.prefix.is_empty() {
                key.to_owned()
            } else {
                format!("{}/{key}", self.prefix)
            };
            ensure!(
                scoped.len() <= 1024,
                "external object key exceeds 1024 bytes"
            );
            path.push_str(&encode_key(&scoped));
        }
        if !query.is_empty() {
            path.push('?');
            let fields: Vec<_> = query
                .iter()
                .map(|(name, value)| {
                    format!(
                        "{}={}",
                        encode_key(name).replace('/', "%2F"),
                        encode_key(value).replace('/', "%2F")
                    )
                })
                .collect();
            path.push_str(&fields.join("&"));
        }
        // Object keys are opaque: URL/Path normalization would rename dot or empty segments.
        Ok(format!("{origin}{path}").parse()?)
    }
}

pub(crate) fn public_read(bytes: &[u8]) -> Result<bool> {
    use s3s::{
        dto::{AccessControlPolicy, Permission, Type},
        xml::Deserialize,
    };
    let mut xml = s3s::xml::Deserializer::new(bytes);
    let policy = AccessControlPolicy::deserialize(&mut xml)?;
    xml.expect_eof()?;
    let grants = policy.grants.context("external ACL grants missing")?;
    ensure!(
        !grants.is_empty() && grants.len() <= 100,
        "invalid external ACL grants"
    );
    let mut public = false;
    for grant in grants {
        let permission = grant
            .permission
            .context("external ACL permission missing")?;
        ensure!(
            matches!(
                permission.as_str(),
                "READ" | "WRITE" | "READ_ACP" | "WRITE_ACP" | "FULL_CONTROL"
            ),
            "unknown external ACL permission"
        );
        let grantee = grant.grantee.context("external ACL grantee missing")?;
        match grantee.type_.as_str() {
            Type::GROUP => {
                let uri = grantee.uri.context("external ACL group missing")?;
                ensure!(
                    matches!(
                        uri.as_str(),
                        "http://acs.amazonaws.com/groups/global/AllUsers"
                            | "http://acs.amazonaws.com/groups/global/AuthenticatedUsers"
                            | "http://acs.amazonaws.com/groups/s3/LogDelivery"
                    ),
                    "unknown external ACL group"
                );
                public |= uri == "http://acs.amazonaws.com/groups/global/AllUsers"
                    && matches!(
                        permission.as_str(),
                        Permission::READ | Permission::FULL_CONTROL
                    );
            }
            Type::CANONICAL_USER => ensure!(
                grantee.id.is_some_and(|id| !id.is_empty()),
                "external ACL identity missing"
            ),
            Type::AMAZON_CUSTOMER_BY_EMAIL => ensure!(
                grantee.email_address.is_some_and(|email| !email.is_empty()),
                "external ACL identity missing"
            ),
            _ => anyhow::bail!("unknown external ACL grantee"),
        }
    }
    Ok(public)
}

pub(crate) struct Client {
    connection: Connection,
    access: String,
    secret: String,
    client: HttpClient<hyper_rustls::HttpsConnector<HttpConnector>, Body>,
    timeout: Duration,
    memory: Option<std::sync::Arc<tokio::sync::OwnedSemaphorePermit>>,
}

struct Buffered {
    bytes: Vec<u8>,
    _memory: std::sync::Arc<tokio::sync::OwnedSemaphorePermit>,
}

impl AsRef<[u8]> for Buffered {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Client {
    pub(crate) async fn bytes(
        &self,
        method: Method,
        key: Option<&str>,
        query: &[(&str, &str)],
        mut headers: HeaderMap,
        body: Bytes,
    ) -> Result<Response<Body>> {
        headers.insert(header::CONTENT_LENGTH, body.len().to_string().parse()?);
        let digest = hex::encode(Sha256::digest(&body));
        self.request(method, key, query, headers, Body::from(body), &digest)
            .await
    }

    pub(crate) async fn request(
        &self,
        method: Method,
        key: Option<&str>,
        query: &[(&str, &str)],
        headers: HeaderMap,
        body: Body,
        digest: &str,
    ) -> Result<Response<Body>> {
        let request = self.signed(method, key, query, headers, body, digest)?;
        let reply = tokio::time::timeout(self.timeout, self.client.request(request)).await??;
        Ok(reply.map(Body::new))
    }

    fn signed(
        &self,
        method: Method,
        key: Option<&str>,
        query: &[(&str, &str)],
        mut headers: HeaderMap,
        body: Body,
        digest: &str,
    ) -> Result<Request<Body>> {
        ensure!(
            digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid external payload digest"
        );
        let uri = self.connection.uri(key, query)?;
        let date = s3s_sigv4::AmzDate::parse(&Utc::now().format("%Y%m%dT%H%M%SZ").to_string())?;
        headers.remove(header::AUTHORIZATION);
        headers.insert(
            header::HOST,
            uri.authority()
                .context("external authority is missing")?
                .as_str()
                .parse()?,
        );
        headers.insert("x-amz-date", date.fmt_iso8601().as_str().parse()?);
        headers.insert("x-amz-content-sha256", digest.parse()?);
        let mut signed = headers
            .iter()
            .map(|(name, value)| Ok((name.as_str(), value.to_str()?)))
            .collect::<Result<Vec<_>>>()?;
        signed.sort_by_key(|item| item.0);
        let canonical = s3s_sigv4::create_canonical_request_with_raw_uri_path(
            method.as_str(),
            uri.path(),
            query,
            &signed,
            s3s_sigv4::Payload::SingleChunk(digest),
        );
        let value =
            s3s_sigv4::create_string_to_sign(&canonical, &date, &self.connection.region, "s3");
        let signature = s3s_sigv4::calculate_signature(
            &value,
            self.secret.as_bytes(),
            &date,
            &self.connection.region,
            "s3",
        );
        let names = signed
            .iter()
            .map(|item| item.0)
            .collect::<Vec<_>>()
            .join(";");
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}/{}/s3/aws4_request, SignedHeaders={names}, Signature={signature}",
            self.access,
            date.fmt_date(),
            self.connection.region
        );
        headers.insert(header::AUTHORIZATION, authorization.parse()?);
        let mut request = Request::builder().method(method).uri(uri).body(body)?;
        *request.headers_mut() = headers;
        Ok(request)
    }

    pub(crate) async fn stream<'a>(
        &self,
        key: &str,
        query: &[(&str, &str)],
        length: u64,
        digest: &'a [u8; 32],
        source: futures_util::stream::BoxStream<'a, Result<Bytes>>,
    ) -> Result<Response<Body>> {
        let (body, feed, mut activity) = super::s3::upload_body::<Sha256>(source, length, digest);
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_LENGTH, length.to_string().parse()?);
        let request = self.signed(
            Method::PUT,
            Some(key),
            query,
            headers,
            Body::from_stream(body),
            &hex::encode(digest),
        )?;
        let send = async {
            let request = self.client.request(request);
            tokio::pin!(request);
            let mut ended = false;
            loop {
                let deadline = *activity.borrow() + self.timeout;
                tokio::select! {
                    reply = &mut request => return reply.map_err(anyhow::Error::from),
                    changed = activity.changed(), if !ended => { ended = changed.is_err(); },
                    _ = tokio::time::sleep_until(deadline) => return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "external upload made no progress").into()),
                }
            }
        };
        let (complete, response) = {
            tokio::pin!(feed, send);
            tokio::select! {
                fed = &mut feed => (fed?, send.await),
                response = &mut send => {
                    let complete = if response.as_ref().is_ok_and(|reply| reply.status().is_success()) {
                        tokio::time::timeout(self.timeout, feed).await??
                    } else {
                        false
                    };
                    (complete, response)
                }
            }
        };
        let reply = response?;
        ensure!(
            !reply.status().is_success() || complete,
            "external accepted an incomplete upload request"
        );
        Ok(reply.map(Body::new))
    }
    pub(crate) async fn anonymous_read(
        &self,
        key: &str,
        query: &[(&str, &str)],
    ) -> Result<Response<Body>> {
        let request = Request::builder()
            .method(Method::GET)
            .uri(self.connection.uri(Some(key), query)?)
            .header(header::CACHE_CONTROL, "no-cache")
            .body(Body::empty())?;
        let reply = tokio::time::timeout(self.timeout, self.client.request(request)).await??;
        Ok(reply.map(Body::new))
    }

    pub(crate) async fn bounded(&self, body: Body, maximum: usize) -> Result<Bytes> {
        let mut stream = body.into_data_stream();
        let mut output = Vec::new();
        while let Some(bytes) = tokio::time::timeout(self.timeout, stream.next()).await? {
            let bytes = bytes?;
            ensure!(
                bytes.len() <= maximum.saturating_sub(output.len()),
                "external response exceeds limit"
            );
            output.extend_from_slice(&bytes);
        }
        Ok(if let Some(memory) = &self.memory {
            Bytes::from_owner(Buffered {
                bytes: output,
                _memory: memory.clone(),
            })
        } else {
            output.into()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acl_visibility_requires_a_complete_known_policy() {
        let owner = r#"<Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>owner</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant>"#;
        let group = r#"<Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group"><URI>http://acs.amazonaws.com/groups/global/AllUsers</URI></Grantee><Permission>READ</Permission></Grant>"#;
        let policy = |grants: &str| {
            format!(
                "<AccessControlPolicy><AccessControlList>{grants}</AccessControlList></AccessControlPolicy>"
            )
        };
        assert!(!public_read(policy(owner).as_bytes()).unwrap());
        let value = policy(&format!("{owner}{group}"));
        assert!(public_read(value.as_bytes()).unwrap());
        assert!(!public_read(value.replace("AllUsers", "AuthenticatedUsers").as_bytes()).unwrap());
        for invalid in [
            policy(""),
            value.replace("AllUsers", "UnknownUsers"),
            value.replace("<Permission>READ</Permission>", ""),
            value.replace(
                "<Permission>READ</Permission>",
                "<Permission>OTHER</Permission>",
            ),
            value.replace("<ID>owner</ID>", ""),
            format!("{value}<AccessControlPolicy/>"),
            "<AccessControlPolicy/>".into(),
            "<Error><Code>AccessDenied</Code></Error>".into(),
        ] {
            assert!(
                public_read(invalid.as_bytes()).is_err(),
                "accepted {invalid}"
            );
        }
    }

    fn connection() -> Connection {
        Connection {
            endpoint_url: "https://storage.example.com".into(),
            region: "region-1".into(),
            bucket: "photos".into(),
            prefix: "imports".into(),
            virtual_hosted: false,
            allow_http: false,
        }
    }

    #[test]
    fn connection_validation_preserves_scope_and_requires_explicit_http() {
        connection().validate().unwrap();
        for endpoint in [
            "file:///etc/passwd",
            "http://storage.example.com",
            "https://user:secret@storage.example.com",
            "https://storage.example.com?secret=value",
            "https://storage.example.com/#private",
        ] {
            let mut value = connection();
            value.endpoint_url = endpoint.into();
            assert!(value.validate().is_err());
        }
        let mut value = connection();
        value.allow_http = true;
        value.endpoint_url = "http://backend:8333".into();
        value.validate().unwrap();
        for prefix in [
            "/imports",
            "imports/",
            "imports//nested",
            "imports/../private",
            "a\0b",
        ] {
            value.prefix = prefix.into();
            assert!(value.validate().is_err(), "{prefix}");
        }
        value.prefix.clear();
        value.validate().unwrap();
    }

    #[test]
    fn opaque_keys_keep_dot_empty_and_encoded_segments() {
        let value = connection();
        let uri = value
            .uri(Some("/odd/.././a//+%2F #.png/"), &[("uploadId", "a+/=&")])
            .unwrap();
        assert_eq!(
            uri.path(),
            "/photos/imports//odd/.././a//%2B%252F%20%23.png/"
        );
        assert_eq!(uri.query(), Some("uploadId=a%2B%2F%3D%26"));
        assert_eq!(value.uri(None, &[]).unwrap().path(), "/photos/");
        let mut virtual_host = value;
        virtual_host.virtual_hosted = true;
        let uri = virtual_host.uri(Some("file"), &[]).unwrap();
        assert_eq!(uri.host(), Some("photos.storage.example.com"));
        assert_eq!(uri.path(), "/imports/file");
    }

    #[tokio::test]
    async fn transport_keeps_paths_and_bounds_untrusted_responses() -> Result<()> {
        use axum::{Router, routing::any};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let router = Router::new().fallback(any(|request: Request<Body>| async move {
            let path = request.uri().path_and_query().unwrap().as_str().to_owned();
            assert!(request.headers().contains_key("authorization"));
            assert!(request.headers().contains_key("x-amz-content-sha256"));
            Response::builder()
                .status(307)
                .header(header::LOCATION, "http://127.0.0.1:1/forbidden")
                .body(Body::from(path))
                .unwrap()
        }));
        let server = tokio::spawn(async move { axum::serve(listener, router).await });
        let mut value = connection();
        value.endpoint_url = format!("http://{address}");
        value.allow_http = true;
        let options = Options {
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(1),
            retry_timeout: Duration::from_secs(1),
            max_retries: 0,
        };
        let client = value.connect("test-access", "test-secret", options)?;
        let reply = client
            .bytes(
                Method::GET,
                Some("/a/.././/%2F"),
                &[("part", "+/")],
                HeaderMap::new(),
                Bytes::new(),
            )
            .await?;
        assert_eq!(reply.status(), 307);
        assert_eq!(
            client.bounded(reply.into_body(), 128).await?,
            "/photos/imports//a/.././/%252F?part=%2B%2F"
        );
        assert!(client.bounded(Body::from("oversize"), 4).await.is_err());
        server.abort();
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires an isolated S3 signature fixture"]
    async fn remote_opaque_keys() -> Result<()> {
        #[derive(Deserialize)]
        struct Fixture {
            connection: Connection,
            access: String,
            secret: String,
        }
        let path = std::env::var("MOKYU_TEST_EXTERNAL_CONFIG")?;
        let fixture: Fixture = serde_json::from_slice(&std::fs::read(path)?)?;
        let options = Options {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(10),
            retry_timeout: Duration::from_secs(10),
            max_retries: 0,
        };
        let client = fixture
            .connection
            .connect(&fixture.access, &fixture.secret, options)?;
        for suffix in [
            "normal",
            "/leading//trailing/",
            ".././dot",
            "%2F +#?&",
            "\u{4e2d}/\u{6587}",
        ] {
            let key = format!("test-{}/{suffix}", uuid::Uuid::now_v7());
            let body = Bytes::from_static(b"external object contents");
            let put = client
                .bytes(Method::PUT, Some(&key), &[], HeaderMap::new(), body.clone())
                .await?;
            ensure!(
                put.status().is_success(),
                "external PUT failed: {}",
                put.status()
            );
            client.bounded(put.into_body(), 4096).await?;
            let get = client
                .bytes(Method::GET, Some(&key), &[], HeaderMap::new(), Bytes::new())
                .await?;
            ensure!(
                get.status().is_success(),
                "external GET failed: {}",
                get.status()
            );
            let actual = client.bounded(get.into_body(), 4096).await?;
            let delete = client
                .bytes(
                    Method::DELETE,
                    Some(&key),
                    &[],
                    HeaderMap::new(),
                    Bytes::new(),
                )
                .await?;
            ensure!(
                delete.status().is_success(),
                "external DELETE failed: {}",
                delete.status()
            );
            client.bounded(delete.into_body(), 4096).await?;
            assert_eq!(actual, body);
        }
        Ok(())
    }
}
