use super::{compression, cors, gateway, object, s3};
use crate::{
    config::RequestSettings,
    model::{ReadRoute, node_read as read},
    node::{
        delivery,
        reads::{Prepared, Reads},
    },
    rpc::wire,
};
use crate::{node::writes, upload::checksum};
use anyhow::{Context, Result, ensure};
use axum::{
    body::Body,
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
};
use hyper::body::Body as _;
use s3s::{
    S3, S3Request, S3Response, S3Result,
    access::{S3Access, S3AccessContext},
    auth::{S3Auth, SecretKey},
    dto::*,
};
use std::{cell::RefCell, sync::Arc};
use tracing::Instrument;

mod catalog;
mod copy;
mod multipart;
mod objects;

enum PreparedRequest {
    Read(RequestRead),
    Write(writes::Request),
    Control(crate::node::request::Request),
}

impl PreparedRequest {
    fn check(&self) -> Result<()> {
        match self {
            Self::Read(read) => read.prepared.check(),
            Self::Write(write) | Self::Control(write) => write.check(),
        }
    }
    fn access(&self) -> &str {
        match self {
            Self::Read(read) => &read.input.access_key,
            Self::Write(write) | Self::Control(write) => &write.access,
        }
    }
    fn credential(&self) -> Option<&wire::Cached> {
        match self {
            Self::Read(read) => read.prepared.reply.credential.as_ref(),
            Self::Write(write) | Self::Control(write) => write.reply.credential.as_ref(),
        }
    }
    fn material<T: serde::de::DeserializeOwned>(&self, version: &wire::Cached) -> Result<T> {
        match self {
            Self::Read(read) => read.prepared.material(version),
            Self::Write(write) | Self::Control(write) => write.material(version),
        }
    }
    fn bucket(&self) -> Result<Option<crate::app::Bucket>> {
        match self {
            Self::Read(read) => delivery::bucket(&read.prepared),
            Self::Write(write) | Self::Control(write) => write
                .reply
                .bucket
                .as_ref()
                .map(|version| write.material(version))
                .transpose(),
        }
    }
}

struct RequestRead {
    input: wire::ReadRequest,
    prepared: Prepared,
    deadline: tokio::time::Instant,
    compression: Option<tokio::sync::OwnedSemaphorePermit>,
}

enum ReadAccess {
    Anonymous,
    Signed(Option<String>),
}

tokio::task_local! {
    static CURRENT: RefCell<Option<PreparedRequest>>;
}

#[derive(Clone)]
pub(crate) struct Gateway {
    reads: Arc<Reads>,
    writes: Arc<writes::Writes>,
    cors: Arc<cors::Cache>,
    settings: RequestSettings,
    controls: Arc<tokio::sync::Semaphore>,
}

impl Gateway {
    pub(crate) fn new(reads: Arc<Reads>) -> Result<Self> {
        let settings = reads.node.background_snapshot()?.runtime().requests.clone();
        reads.report_foreground();
        let cors = reads.cors.clone();
        reads.start_public();
        let controls = Arc::new(tokio::sync::Semaphore::new(
            reads
                .node
                .budget
                .get()
                .context("node budget missing")?
                .data_slots,
        ));
        let writes = Arc::new(writes::Writes::new(reads.clone())?);
        writes.start_native();
        Ok(Self {
            writes,
            reads,
            cors,
            settings,
            controls,
        })
    }

    pub(crate) fn service(&self) -> Result<s3s::service::S3Service> {
        let mut builder = s3s::service::S3ServiceBuilder::new(self.clone());
        builder.set_auth(self.clone());
        builder.set_access(self.clone());
        s3::configure(&mut builder, &self.settings)?;
        Ok(builder.build())
    }

    #[cfg(test)]
    pub(crate) fn cors_cache(&self) -> serde_json::Value {
        self.cors.snapshot()
    }

    #[cfg(test)]
    pub(crate) fn write_resources(&self) -> [usize; 3] {
        self.writes.available()
    }

    #[cfg(test)]
    pub(crate) async fn limit_write_io(&self) -> Result<tokio::sync::OwnedSemaphorePermit> {
        self.writes.limit_io().await
    }

    #[cfg(test)]
    pub(crate) fn control_slots(&self) -> Arc<tokio::sync::Semaphore> {
        self.controls.clone()
    }

    async fn prepare(
        &self,
        route: ReadRoute,
        access: ReadAccess,
        method: &Method,
        web: bool,
        headers: &HeaderMap,
        overrides: &HeaderMap,
    ) -> Result<RequestRead> {
        let (access, anonymous) = match access {
            ReadAccess::Anonymous => (None, true),
            ReadAccess::Signed(key) => (key, false),
        };
        let head = method != Method::GET;
        let reuse = anonymous
            && access.is_none()
            && matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS)
            && object::reuse_public(headers);
        let accept = compression::AcceptEncoding::new(headers);
        let wants_compression = !head
            && access.is_none()
            && accept
                .select(|_| true)
                .is_some_and(|encoding| encoding != compression::Encoding::Identity);
        let memory = wants_compression
            .then(|| delivery::compression_work(&self.reads))
            .flatten();
        let mut headers = headers.clone();
        if wants_compression && memory.is_none() && accept.identity() {
            headers.insert(
                hyper::header::ACCEPT_ENCODING,
                HeaderValue::from_static("identity"),
            );
        }
        let route = match route {
            ReadRoute::Bucket { name, key, alias } => {
                wire::read_route::Route::Bucket(wire::BucketRoute {
                    name,
                    key,
                    alias_name: alias
                        .as_ref()
                        .map(|value| value.0.clone())
                        .unwrap_or_default(),
                    alias_key: alias.map(|value| value.1).unwrap_or_default(),
                })
            }
            ReadRoute::Host { host, key } => {
                wire::read_route::Route::Host(wire::HostRoute { host, key })
            }
        };
        let fields = |headers: &HeaderMap, allowed: &[&str]| {
            headers
                .iter()
                .filter(|(name, _)| allowed.contains(&name.as_str()))
                .map(|(name, value)| wire::ReadHeader {
                    name: name.to_string(),
                    value: value.as_bytes().to_vec(),
                })
                .collect()
        };
        let input = wire::ReadRequest {
            route: Some(wire::ReadRoute { route: Some(route) }),
            access_key: access.unwrap_or_default(),
            method: if head {
                wire::ReadMethod::Head
            } else {
                wire::ReadMethod::Get
            } as i32,
            surface: if web {
                wire::ReadSurface::Web
            } else {
                wire::ReadSurface::S3
            } as i32,
            include: if head {
                vec![
                    wire::ReadInclude::Object as i32,
                    wire::ReadInclude::Policy as i32,
                ]
            } else {
                vec![
                    wire::ReadInclude::Object as i32,
                    wire::ReadInclude::Mapping as i32,
                    wire::ReadInclude::Policy as i32,
                ]
            },
            headers: fields(&headers, crate::rpc::read::HEADERS),
            overrides: fields(overrides, crate::rpc::read::OVERRIDES),
            ..Default::default()
        };
        let deadline = crate::app::readiness::deadline();
        let prepared = if reuse {
            self.reads
                .open_public(input.clone(), method == Method::OPTIONS)
                .await?
        } else {
            self.reads.open(input.clone()).await?
        };
        Ok(RequestRead {
            input,
            prepared,
            deadline,
            compression: memory,
        })
    }

    async fn finish(
        &self,
        mut request: RequestRead,
        overrides: &HeaderMap,
    ) -> Result<object::Read> {
        let head = request.input.method == wire::ReadMethod::Head as i32;
        if !head && request.prepared.reply.outcome == wire::ReadOutcome::Pending as i32 {
            let credential = request.prepared.reply.credential.clone();
            let bucket = request.prepared.reply.bucket.clone();
            drop(request.prepared);
            tokio::time::sleep_until(request.deadline).await;
            request.prepared = self.reads.open(request.input).await?;
            if request.prepared.reply.credential != credential
                || request.prepared.reply.bucket != bucket
            {
                return Err(crate::app::readiness::pending());
            }
        }
        delivery::response(
            &self.reads,
            request.prepared,
            head,
            overrides,
            request.compression,
        )
        .await
    }

    fn cors(&self, prepared: &Prepared, headers: &HeaderMap, method: &Method) -> Result<HeaderMap> {
        let Some(mut bucket) = delivery::bucket(prepared)? else {
            return Ok(HeaderMap::new());
        };
        if let Some(version) = &prepared.reply.policy {
            let policy: crate::model::node_read::Policy = prepared.material(version)?;
            anyhow::ensure!(
                Some(policy.storage_id) == bucket.storage_id && policy.revision > 0,
                "read policy does not match the bucket"
            );
            if policy.cors_cache_enabled {
                self.cors.current(&mut bucket);
            }
        }
        super::cors_headers(
            &self.cors,
            &bucket,
            headers,
            method.as_str(),
            None,
            self.cors.generation(),
        )
    }

    async fn public_read(
        &self,
        host: String,
        key: String,
        headers: HeaderMap,
        method: Method,
    ) -> Result<Response, super::HttpError> {
        let prepared = self
            .prepare(
                ReadRoute::Host { host, key },
                ReadAccess::Anonymous,
                &method,
                true,
                &headers,
                &HeaderMap::new(),
            )
            .await
            .map_err(delivery::error)?;
        Ok(self
            .finish(prepared, &HeaderMap::new())
            .await
            .map_err(delivery::error)?
            .response())
    }

    pub(crate) async fn public(&self, mut request: hyper::Request<Body>) -> Response {
        let observation = crate::status::stats::Request::new(
            &self.reads.node.statistics.http[1],
            "web",
            request.method().clone(),
        );
        request.extensions_mut().insert(observation.context.clone());
        let head = request.method() == Method::HEAD;
        let result: Result<Response, super::HttpError> = async {
            super::transport::authority(&mut request)?;
            self.reads
                .node
                .snapshot()
                .map_err(|error| delivery::error(error.into()))?;
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
                    .is_some_and(|query| query.to_ascii_lowercase().contains("x-amz-"))
            {
                return Err(super::unauthorized());
            }
            let host = request
                .headers()
                .get("host")
                .and_then(|value| value.to_str().ok())
                .ok_or_else(super::unauthorized)?
                .to_ascii_lowercase();
            let key = if request.method() == Method::OPTIONS {
                String::new()
            } else {
                s3::decode_path(
                    request
                        .uri()
                        .path()
                        .strip_prefix('/')
                        .unwrap_or(request.uri().path()),
                )
                .map_err(|_| super::unauthorized())?
            };
            let prepared = self
                .prepare(
                    ReadRoute::Host {
                        host: host.clone(),
                        key: key.clone(),
                    },
                    ReadAccess::Anonymous,
                    request.method(),
                    true,
                    request.headers(),
                    &HeaderMap::new(),
                )
                .await
                .map_err(delivery::error)?;
            let bucket = delivery::bucket(&prepared.prepared)?.ok_or_else(super::unauthorized)?;
            let mut cors = self.cors(&prepared.prepared, request.headers(), request.method())?;
            if request.method() == Method::OPTIONS {
                let allowed = cors.contains_key("access-control-allow-origin")
                    && request
                        .headers()
                        .get("access-control-request-method")
                        .is_some_and(|method| method == "GET" || method == "HEAD");
                let mut response = if allowed {
                    StatusCode::NO_CONTENT
                } else {
                    cors.remove("access-control-allow-origin");
                    StatusCode::FORBIDDEN
                }
                .into_response();
                *response.headers_mut() = cors;
                return Ok(response);
            }
            let result = self
                .finish(prepared, &HeaderMap::new())
                .await
                .map(|read| read.response())
                .map_err(|error| super::HttpError(delivery::error(error).into()));
            let result = match result {
                Err(error)
                    if bucket.website_enabled && error.has_code(s3s::S3ErrorCode::NoSuchKey) =>
                {
                    super::public::website_response(
                        &bucket,
                        &key,
                        request.into_parts().0,
                        |key, headers, method| self.public_read(host.clone(), key, headers, method),
                    )
                    .await
                }
                other => other,
            };
            let mut response = result.unwrap_or_else(IntoResponse::into_response);
            compression::merge(response.headers_mut(), cors);
            compression::vary(response.headers_mut(), "Accept-Encoding");
            Ok(response)
        }
        .instrument(observation.span.clone())
        .await;
        let mut response = result.unwrap_or_else(IntoResponse::into_response);
        if head {
            super::transport::finish_head(&mut response);
        }
        observation.response(response, false).map(Body::new)
    }

    pub(crate) async fn s3(
        &self,
        service: &s3s::service::S3Service,
        mut request: hyper::Request<s3s::Body>,
    ) -> hyper::Response<crate::status::stats::ObservedBody<s3s::Body>> {
        let observation = crate::status::stats::Request::new(
            &self.reads.node.statistics.http[0],
            "s3",
            request.method().clone(),
        );
        request.extensions_mut().insert(observation.context.clone());
        let head = request.method() == Method::HEAD;
        let read = head || request.method() == Method::GET;
        let close = matches!(
            request.version(),
            hyper::Version::HTTP_10 | hyper::Version::HTTP_11
        ) && !request.body().is_end_stream();
        let mut cors = HeaderMap::new();
        let mut alias = false;
        let result: Result<s3s::HttpResponse> = async {
            super::transport::authority(&mut request)?;
            self.reads.node.snapshot()?;
            let target = s3::Target::new(&request, &self.settings)?;
            let route = s3::read_route(
                request.uri().path(),
                target.virtual_bucket.as_deref(),
                target.alias.as_deref(),
            )
            .unwrap_or_else(|| ReadRoute::Bucket {
                name: target.virtual_bucket.clone().unwrap_or_else(|| {
                    s3::decode_path(
                        request
                            .uri()
                            .path()
                            .trim_start_matches('/')
                            .split('/')
                            .next()
                            .unwrap_or(""),
                    )
                    .unwrap_or_default()
                }),
                key: String::new(),
                alias: None,
            });
            let overrides = if request.method() == Method::GET {
                overrides(request.uri())?
            } else {
                HeaderMap::new()
            };
            let object_read = read && catalog::object_read(&route, &target.query);
            let prepared = if object_read || request.method() == Method::OPTIONS {
                PreparedRequest::Read(
                    self.prepare(
                        route,
                        if target.signed {
                            ReadAccess::Signed(s3::access_key(
                                request.headers(),
                                request.uri().query(),
                            ))
                        } else {
                            ReadAccess::Anonymous
                        },
                        request.method(),
                        false,
                        request.headers(),
                        &overrides,
                    )
                    .await?,
                )
            } else {
                let (bucket, key) = match route {
                    ReadRoute::Bucket { name, key, .. } => (name, key),
                    _ => (String::new(), String::new()),
                };
                if read
                    || request.method() == Method::DELETE
                    || (request.method() == Method::POST
                        && target.query.iter().any(|value| value == "uploadId"))
                    || target
                        .query
                        .iter()
                        .any(|value| matches!(value.as_str(), "acl" | "delete" | "uploads"))
                {
                    let query = catalog::query(request.method(), request.uri(), &bucket, &key)?;
                    let memory = Arc::new(
                        self.controls
                            .clone()
                            .try_acquire_owned()
                            .map_err(|_| s3s::s3_error!(SlowDown))?,
                    );
                    PreparedRequest::Control(
                        crate::node::request::prepare(
                            &self.reads,
                            memory,
                            s3::access_key(request.headers(), request.uri().query())
                                .unwrap_or_default(),
                            bucket,
                            query,
                        )
                        .await?,
                    )
                } else {
                    PreparedRequest::Write(
                        self.writes
                            .prepare(
                                s3::access_key(request.headers(), request.uri().query())
                                    .unwrap_or_default(),
                                bucket,
                            )
                            .await?,
                    )
                }
            };
            cors = match &prepared {
                PreparedRequest::Read(read) => {
                    self.cors(&read.prepared, request.headers(), request.method())?
                }
                PreparedRequest::Write(write) | PreparedRequest::Control(write) => {
                    if let Some(mut bucket) = prepared.bucket()? {
                        if let Some(version) = &write.reply.policy {
                            let policy: read::Policy = write.material(version)?;
                            ensure!(
                                Some(policy.storage_id) == bucket.storage_id,
                                "write policy does not match bucket"
                            );
                            if policy.cors_cache_enabled {
                                self.cors.current(&mut bucket);
                            }
                        }
                        super::cors_headers(
                            &self.cors,
                            &bucket,
                            request.headers(),
                            request.method().as_str(),
                            None,
                            self.cors.generation(),
                        )?
                    } else {
                        HeaderMap::new()
                    }
                }
            };
            if let Some(bucket) = prepared.bucket()?
                && target.alias.as_deref() == Some(&bucket.name)
            {
                alias = true;
                if bucket.state != "active" {
                    return Err(s3s::s3_error!(AccessDenied).into());
                }
                let mut uri = request.uri().clone().into_parts();
                uri.path_and_query = Some(
                    format!(
                        "/{}{}",
                        bucket.name,
                        request
                            .uri()
                            .path_and_query()
                            .map_or("/", |path| path.as_str())
                    )
                    .parse()?,
                );
                *request.uri_mut() = hyper::Uri::from_parts(uri)?;
            }
            if request.method() == Method::OPTIONS {
                let allowed = cors.contains_key("access-control-allow-origin")
                    && (!alias
                        || request
                            .headers()
                            .get("access-control-request-method")
                            .is_some_and(|method| method == "GET" || method == "HEAD"));
                let mut response = hyper::Response::new(s3s::Body::empty());
                *response.status_mut() = if allowed {
                    StatusCode::NO_CONTENT
                } else {
                    cors.remove("access-control-allow-origin");
                    StatusCode::FORBIDDEN
                };
                return Ok(response);
            }
            let post = request.method() == Method::POST;
            s3::check_control_body(
                &mut request,
                post && target.query.iter().any(|name| name == "delete"),
                post && target.query.iter().any(|name| name == "uploadId"),
                std::time::Duration::from_secs(crate::config::seconds(
                    &self
                        .reads
                        .node
                        .snapshot()?
                        .runtime()
                        .uploads
                        .client_idle_timeout,
                )?),
            )
            .await?;
            if request
                .headers()
                .get("x-amz-content-sha256")
                .is_some_and(|value| value.as_bytes().starts_with(b"STREAMING-"))
            {
                request = request.map(crate::upload::guard_aws_end);
            }
            Ok(CURRENT
                .scope(RefCell::new(Some(prepared)), service.call(request))
                .await
                .map_err(|error| anyhow::anyhow!("S3 HTTP failure: {error:?}"))?)
        }
        .instrument(observation.span.clone())
        .await;
        let mut response = result.unwrap_or_else(|error| {
            delivery::error(error)
                .to_http_response()
                .unwrap_or_else(|_| {
                    let mut response = hyper::Response::new(s3s::Body::empty());
                    *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                    response
                })
        });
        if alias && response.status() == StatusCode::NOT_FOUND {
            response = s3s::s3_error!(AccessDenied)
                .to_http_response()
                .expect("static S3 error");
        }
        if close && (response.status().is_client_error() || response.status().is_server_error()) {
            response
                .headers_mut()
                .insert(hyper::header::CONNECTION, HeaderValue::from_static("close"));
        }
        compression::merge(response.headers_mut(), cors);
        if read {
            compression::vary(
                response.headers_mut(),
                "Accept-Encoding, Authorization, X-Amz-Checksum-Mode",
            );
        }
        if head {
            *response.body_mut() =
                s3s::Body::from(object::blob(Box::pin(futures_util::stream::empty())));
        }
        crate::status::stats::s3_request_id(&mut response, &observation.context.id);
        observation.response(response, true)
    }
}

fn overrides(uri: &hyper::Uri) -> Result<HeaderMap> {
    let axum::extract::Query(values) =
        axum::extract::Query::<Vec<(String, String)>>::try_from_uri(uri)?;
    let mut headers = HeaderMap::new();
    for (name, value) in values {
        if let Some(name) = name
            .strip_prefix("response-")
            .filter(|name| crate::rpc::read::OVERRIDES.contains(name))
        {
            let name = hyper::header::HeaderName::from_bytes(name.as_bytes())?;
            ensure!(!headers.contains_key(&name), s3s::s3_error!(InvalidRequest));
            headers.insert(name, value.parse()?);
        }
    }
    Ok(headers)
}

fn take(bucket: &str, key: &str, access: Option<&str>) -> Result<RequestRead> {
    let read = CURRENT
        .try_with(|value| value.borrow_mut().take())?
        .context("read request missing")?;
    let PreparedRequest::Read(read) = read else {
        anyhow::bail!(s3s::s3_error!(InvalidRequest));
    };
    ensure!(
        access.unwrap_or("") == read.input.access_key,
        s3s::s3_error!(AccessDenied)
    );
    let Some(wire::read_route::Route::Bucket(route)) = read
        .input
        .route
        .as_ref()
        .and_then(|route| route.route.as_ref())
    else {
        anyhow::bail!("S3 read route missing")
    };
    let resolved = delivery::bucket(&read.prepared)?;
    let alias = resolved
        .as_ref()
        .is_some_and(|bucket| bucket.name == route.alias_name);
    ensure!(
        bucket
            == if alias {
                &route.alias_name
            } else {
                &route.name
            }
            && key == if alias { &route.alias_key } else { &route.key },
        s3s::s3_error!(AccessDenied)
    );
    read.prepared.check()?;
    Ok(read)
}

#[async_trait::async_trait]
impl S3Auth for Gateway {
    async fn get_secret_key(&self, key: &str) -> S3Result<SecretKey> {
        (|| -> Result<SecretKey> {
            CURRENT.try_with(|current| -> Result<SecretKey> {
                let current = current.borrow();
                let read = current.as_ref().context("read authentication missing")?;
                read.check()?;
                let version = read
                    .credential()
                    .ok_or_else(|| s3s::s3_error!(InvalidAccessKeyId))?;
                let credential: read::Credential = read.material(version)?;
                ensure!(
                    credential.access == key && key == read.access(),
                    s3s::s3_error!(InvalidAccessKeyId)
                );
                Ok(SecretKey::from(credential.secret))
            })?
        })()
        .map_err(delivery::error)
    }
}

#[async_trait::async_trait]
impl S3Access for Gateway {
    async fn check(&self, context: &mut S3AccessContext<'_>) -> S3Result<()> {
        gateway::check_request(context)?;
        if let Some(credential) = context.credentials() {
            self.reads
                .node
                .observations
                .activity
                .record(&credential.access_key);
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl S3 for Gateway {
    async fn complete_multipart_upload(
        &self,
        request: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        self.complete_multipart(request)
            .await
            .map_err(delivery::error)
    }
    async fn upload_part(
        &self,
        request: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        self.part(request).await.map_err(delivery::error)
    }
    async fn create_multipart_upload(
        &self,
        request: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        self.create_multipart(request)
            .await
            .map_err(delivery::error)
    }
    async fn abort_multipart_upload(
        &self,
        request: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        self.abort_multipart(request).await.map_err(delivery::error)
    }
    async fn list_multipart_uploads(
        &self,
        request: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        multipart::uploads(request).map_err(delivery::error)
    }
    async fn list_parts(
        &self,
        request: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        multipart::parts(request).map_err(delivery::error)
    }
    async fn copy_object(
        &self,
        request: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        self.copy(request).await.map_err(delivery::error)
    }
    async fn delete_object(
        &self,
        request: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        self.delete(request).await.map_err(delivery::error)
    }
    async fn delete_objects(
        &self,
        request: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        self.delete_batch(request).await.map_err(delivery::error)
    }
    async fn get_object_acl(
        &self,
        request: S3Request<GetObjectAclInput>,
    ) -> S3Result<S3Response<GetObjectAclOutput>> {
        catalog::acl(request).map_err(delivery::error)
    }
    async fn put_object_acl(
        &self,
        request: S3Request<PutObjectAclInput>,
    ) -> S3Result<S3Response<PutObjectAclOutput>> {
        self.set_acl(request).await.map_err(delivery::error)
    }
    async fn head_bucket(
        &self,
        request: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        catalog::describe(
            &request.input.bucket,
            request
                .credentials
                .as_ref()
                .map(|value| value.access_key.as_str()),
        )
        .map_err(delivery::error)?;
        Ok(S3Response::new(HeadBucketOutput {
            bucket_region: Some(self.settings.s3_region.clone()),
            ..Default::default()
        }))
    }

    async fn get_bucket_location(
        &self,
        request: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        catalog::describe(
            &request.input.bucket,
            request
                .credentials
                .as_ref()
                .map(|value| value.access_key.as_str()),
        )
        .map_err(delivery::error)?;
        Ok(S3Response::new(GetBucketLocationOutput {
            location_constraint: (self.settings.s3_region != "us-east-1")
                .then(|| BucketLocationConstraint::from(self.settings.s3_region.clone())),
        }))
    }

    async fn list_buckets(
        &self,
        request: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        catalog::buckets(request, &self.settings.s3_region).map_err(delivery::error)
    }

    async fn list_objects_v2(
        &self,
        request: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        catalog::objects(request).map_err(delivery::error)
    }

    async fn put_object(
        &self,
        mut request: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        async {
            gateway::reject_features(&request.headers)?;
            let prepared = CURRENT
                .try_with(|value| value.borrow_mut().take())?
                .context("write request missing")?;
            let PreparedRequest::Write(prepared) = prepared else {
                anyhow::bail!(s3s::s3_error!(InvalidRequest));
            };
            let input = &request.input;
            let access = request
                .credentials
                .as_ref()
                .ok_or_else(|| s3s::s3_error!(AccessDenied))?;
            let length = writes::declared(&request.headers, input.content_length)?;
            let check = crate::upload::Integrity::new(
                &request.headers,
                input
                    .checksum_algorithm
                    .as_ref()
                    .map(|value| value.as_str()),
            )?;
            let begin = wire::BeginWriteRequest {
                access_key: access.access_key.clone(),
                bucket: input.bucket.clone(),
                key: input.key.clone(),
                metadata_json: serde_json::to_vec(&gateway::metadata!(input))?,
                public_read: gateway::canned(input.acl.as_ref())?,
                size: length.map(|value| value as u64),
                ..Default::default()
            };
            let condition = |name| -> Result<Option<String>> {
                request
                    .headers
                    .get(name)
                    .map(|value| Ok(value.to_str()?.to_owned()))
                    .transpose()
            };
            let if_match = condition("if-match")?;
            let if_none = condition("if-none-match")?;
            let body = request
                .input
                .body
                .take()
                .unwrap_or_else(|| StreamingBlob::from_bytes(bytes::Bytes::new()));
            let (etag, sums) = self
                .writes
                .put(
                    prepared,
                    begin,
                    body,
                    check,
                    request.trailing_headers,
                    if_match,
                    if_none,
                )
                .await?;
            let mut output = PutObjectOutput {
                e_tag: Some(ETag::Strong(etag)),
                ..Default::default()
            };
            gateway::checksums!(output, &sums);
            Ok(S3Response::new(output))
        }
        .await
        .map_err(delivery::error)
    }

    async fn get_object(
        &self,
        request: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let prepared = take(
            &request.input.bucket,
            &request.input.key,
            request
                .credentials
                .as_ref()
                .map(|value| value.access_key.as_str()),
        )
        .map_err(delivery::error)?;
        let mut overrides = HeaderMap::new();
        for field in &prepared.input.overrides {
            overrides.insert(
                hyper::header::HeaderName::from_bytes(field.name.as_bytes())
                    .map_err(|_| s3s::s3_error!(InvalidRequest))?,
                HeaderValue::from_bytes(&field.value)
                    .map_err(|_| s3s::s3_error!(InvalidRequest))?,
            );
        }
        gateway::get_read(
            self.finish(prepared, &overrides)
                .await
                .map_err(delivery::error)?,
            request.input.checksum_mode.is_some(),
        )
    }

    async fn head_object(
        &self,
        request: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let prepared = take(
            &request.input.bucket,
            &request.input.key,
            request
                .credentials
                .as_ref()
                .map(|value| value.access_key.as_str()),
        )
        .map_err(delivery::error)?;
        gateway::head_read(
            self.finish(prepared, &HeaderMap::new())
                .await
                .map_err(delivery::error)?,
            request.input.checksum_mode.is_some(),
        )
    }
}
