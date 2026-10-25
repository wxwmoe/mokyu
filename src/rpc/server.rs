use super::{MESSAGE_LIMIT, PROTOCOL, SESSION_LIMIT, wire};
use crate::{
    app::App,
    cluster::NodeState,
    model::{self, session},
};
use std::{pin::Pin, sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use tonic::{Request, Response, Status, Streaming};
use wire::{
    node_control_server::{NodeControl, NodeControlServer},
    node_multipart_server::{NodeMultipart, NodeMultipartServer},
    node_objects_server::{NodeObjects, NodeObjectsServer},
    node_reads_server::{NodeReads, NodeReadsServer},
    node_sessions_server::{NodeSessions, NodeSessionsServer},
    node_writes_server::{NodeWrites, NodeWritesServer},
};

#[derive(Clone)]
struct Service {
    app: Arc<App>,
}

struct AdmittedBody {
    inner: axum::body::Body,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl hyper::body::Body for AdmittedBody {
    type Data = bytes::Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        Pin::new(&mut self.inner).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> hyper::body::SizeHint {
        self.inner.size_hint()
    }
}

pub(crate) fn router(app: Arc<App>) -> axum::Router {
    let unary = Arc::new(Semaphore::new((app.budget.data_slots * 4).clamp(4, 64)));
    let sessions = Arc::new(Semaphore::new(app.budget.request_connections.min(1024)));
    let service = Service { app };
    tonic::service::Routes::new(
        NodeControlServer::new(service.clone())
            .max_decoding_message_size(super::read::REQUEST_LIMIT)
            .max_encoding_message_size(MESSAGE_LIMIT),
    )
    .add_service(
        NodeSessionsServer::new(service.clone())
            .max_decoding_message_size(SESSION_LIMIT)
            .max_encoding_message_size(SESSION_LIMIT),
    )
    .add_service(
        NodeReadsServer::new(service.clone())
            .max_decoding_message_size(super::read::REQUEST_LIMIT)
            .max_encoding_message_size(MESSAGE_LIMIT),
    )
    .add_service(
        NodeObjectsServer::new(service.clone())
            .max_decoding_message_size(model::node_object::REQUEST_LIMIT)
            .max_encoding_message_size(MESSAGE_LIMIT),
    )
    .add_service(
        NodeMultipartServer::new(service.clone())
            .max_decoding_message_size(super::read::REQUEST_LIMIT)
            .max_encoding_message_size(MESSAGE_LIMIT),
    )
    .add_service(
        NodeWritesServer::new(service)
            .max_decoding_message_size(model::node_write::REQUEST_LIMIT)
            .max_encoding_message_size(MESSAGE_LIMIT),
    )
    .into_axum_router()
    .layer(axum::middleware::from_fn(
        move |mut request: axum::extract::Request, next: axum::middleware::Next| {
            let limits = if request.uri().path() == "/mokyu.node.v1.NodeSessions/NodeSession" {
                sessions.clone()
            } else {
                unary.clone()
            };
            async move {
                let mut headers = Request::new(());
                *headers.metadata_mut() =
                    tonic::metadata::MetadataMap::from_headers(request.headers().clone());
                let node = match identity(&headers) {
                    Ok(node) => node,
                    Err(status) => return status.into_http::<axum::body::Body>(),
                };
                request.extensions_mut().insert(node);
                let permit = match limits.try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        return Status::resource_exhausted("node RPC capacity is full")
                            .into_http::<axum::body::Body>();
                    }
                };
                let response =
                    match tokio::time::timeout(Duration::from_secs(15), next.run(request)).await {
                        Ok(response) => response,
                        Err(_) => {
                            return Status::deadline_exceeded("node RPC request timed out")
                                .into_http::<axum::body::Body>();
                        }
                    };
                let (parts, inner) = response.into_parts();
                axum::response::Response::from_parts(
                    parts,
                    axum::body::Body::new(AdmittedBody {
                        inner,
                        _permit: permit,
                    }),
                )
            }
        },
    ))
}

fn identity<T>(request: &Request<T>) -> Result<NodeState, Status> {
    let metadata = request.metadata();
    if metadata
        .iter()
        .map(|entry| match entry {
            tonic::metadata::KeyAndValueRef::Ascii(key, value) => {
                key.as_str().len() + value.as_bytes().len()
            }
            tonic::metadata::KeyAndValueRef::Binary(key, value) => {
                key.as_str().len() + value.as_encoded_bytes().len()
            }
        })
        .sum::<usize>()
        > 4096
    {
        return Err(Status::resource_exhausted("node metadata exceeds 4 KiB"));
    }
    let get = |name: &'static str| -> Result<&str, Status> {
        let values = metadata.get_all(name);
        let mut values = values.iter();
        let value = values
            .next()
            .ok_or_else(|| Status::unauthenticated("node identity is required"))?;
        if values.next().is_some() {
            return Err(Status::unauthenticated("duplicate node metadata"));
        }
        value
            .to_str()
            .map_err(|_| Status::unauthenticated("invalid node metadata"))
    };
    let node = NodeState {
        format: 1,
        deployment_id: super::uuid(get("x-mokyu-deployment-id")?)?,
        node_id: super::uuid(get("x-mokyu-node-id")?)?,
        role: get("x-mokyu-node-role")?.to_owned(),
        credential_generation: get("x-mokyu-credential-generation")?
            .parse()
            .map_err(|_| Status::unauthenticated("invalid node generation"))?,
        credential: get("authorization")?
            .strip_prefix("Bearer ")
            .ok_or_else(|| Status::unauthenticated("node credential is required"))?
            .to_owned(),
    };
    node.validate()
        .map_err(|_| Status::unauthenticated("invalid node identity"))?;
    if !matches!(node.role.as_str(), "data" | "worker") {
        return Err(Status::permission_denied(
            "this protocol requires a data or worker identity",
        ));
    }
    Ok(node)
}

fn failure(error: anyhow::Error) -> Status {
    use model::Error;
    let mut status = match model::error(&error) {
        Some(Error::NodeAuthentication) => {
            Status::unauthenticated("invalid node identity or credential")
        }
        Some(Error::NodeInactive) => Status::permission_denied("node or deployment is inactive"),
        Some(Error::NodeConflict) => Status::aborted("node is owned by another boot or session"),
        Some(Error::NodeExpired) => Status::failed_precondition("node run is expired or retired"),
        Some(Error::SourceUnavailable) => {
            Status::failed_precondition("read source or payload revision is unavailable")
        }
        Some(Error::NodeLimit) => {
            Status::resource_exhausted("node boot history is full; rotate its credential")
        }
        Some(Error::NodeReadLimit) => {
            Status::resource_exhausted("node read exceeds its resource or message budget")
        }
        Some(Error::NodeWriteLimit) => Status::resource_exhausted("node write capacity is full"),
        Some(Error::NodeWriteBusy | Error::ChunkBusy) => {
            let mut status = Status::resource_exhausted("node write capacity is busy");
            status
                .metadata_mut()
                .insert("mokyu-retryable", "write-capacity".parse().unwrap());
            status
        }
        Some(Error::NodeOperationExpired) => {
            Status::failed_precondition("write operation is outside its admission window")
        }
        Some(Error::NodeOperationConflict) => {
            Status::already_exists("write operation parameters changed")
        }
        Some(Error::NodeOperationMissing) => Status::not_found("write operation was not found"),
        Some(Error::QuotaExceeded) => Status::resource_exhausted("storage quota exceeded"),
        Some(Error::OperationAborted) => Status::aborted("operation was aborted"),
        Some(Error::NodeReadBusy) => {
            let mut status = Status::resource_exhausted("node read capacity is busy");
            status
                .metadata_mut()
                .insert("mokyu-retryable", "read-capacity".parse().unwrap());
            status
        }
        _ if error.downcast_ref::<s3s::S3Error>().is_some() => {
            let code = error.downcast_ref::<s3s::S3Error>().unwrap().code();
            match code {
                s3s::S3ErrorCode::AccessDenied => Status::permission_denied("access denied"),
                s3s::S3ErrorCode::NoSuchBucket => Status::not_found("bucket was not found"),
                s3s::S3ErrorCode::NoSuchUpload => Status::not_found("upload was not found"),
                s3s::S3ErrorCode::SlowDown => {
                    Status::resource_exhausted("multipart admission is busy")
                }
                s3s::S3ErrorCode::InvalidBucketState => {
                    Status::failed_precondition("bucket cannot accept writes")
                }
                s3s::S3ErrorCode::OperationAborted => Status::aborted("operation was aborted"),
                s3s::S3ErrorCode::PreconditionFailed => {
                    Status::failed_precondition("write condition failed")
                }
                s3s::S3ErrorCode::IncompleteBody => {
                    Status::failed_precondition("write body is incomplete")
                }
                s3s::S3ErrorCode::InvalidArgument | s3s::S3ErrorCode::InvalidRequest => {
                    Status::invalid_argument("invalid node request")
                }
                _ => Status::unavailable("control operation is unavailable"),
            }
        }
        _ => {
            tracing::warn!("node RPC database operation failed");
            Status::unavailable("control operation is unavailable")
        }
    };
    let business = match model::error(&error) {
        Some(Error::QuotaExceeded) => Some("QuotaExceeded"),
        Some(Error::OperationAborted) => Some("OperationAborted"),
        _ => error
            .downcast_ref::<s3s::S3Error>()
            .and_then(|error| match error.code() {
                s3s::S3ErrorCode::AccessDenied => Some("AccessDenied"),
                s3s::S3ErrorCode::NoSuchBucket => Some("NoSuchBucket"),
                s3s::S3ErrorCode::InvalidBucketState => Some("InvalidBucketState"),
                s3s::S3ErrorCode::OperationAborted => Some("OperationAborted"),
                s3s::S3ErrorCode::PreconditionFailed => Some("PreconditionFailed"),
                s3s::S3ErrorCode::IncompleteBody => Some("IncompleteBody"),
                s3s::S3ErrorCode::NoSuchUpload => Some("NoSuchUpload"),
                s3s::S3ErrorCode::SlowDown => Some("SlowDown"),
                s3s::S3ErrorCode::InvalidArgument => Some("InvalidArgument"),
                s3s::S3ErrorCode::InvalidRequest => Some("InvalidRequest"),
                _ => None,
            }),
    };
    if let Some(code) = business {
        status
            .metadata_mut()
            .insert("mokyu-s3-code", code.parse().unwrap());
    }
    status
}

#[tonic::async_trait]
impl NodeObjects for Service {
    async fn change_objects(
        &self,
        request: Request<wire::ChangeObjectsRequest>,
    ) -> Result<Response<wire::ChangeObjectsReply>, Status> {
        let node = data_identity(&request)?;
        let input = super::object::change(request.into_inner())?;
        let outcome = bounded(self.app.cluster().map_err(failure)?.change_objects(
            &node,
            &self.app.secrets.credential_key,
            input,
        ))
        .await?;
        self.app.wake_gc.notify_one();
        #[cfg(feature = "fault-injection")]
        crate::faults::point("node-objects-changed").await;
        Ok(Response::new(super::object::reply(outcome)))
    }
}

#[tonic::async_trait]
impl NodeWrites for Service {
    async fn prepare_raw(
        &self,
        request: Request<wire::PrepareRawRequest>,
    ) -> Result<Response<wire::PreparedRaw>, Status> {
        let node = data_identity(&request)?;
        let input = super::write::prepare_raw(request.into_inner())?;
        let reply = bounded(self.app.cluster().map_err(failure)?.prepare_raw(
            &node,
            &self.app.secrets.credential_key,
            input,
        ))
        .await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("node-raw-prepared").await;
        Ok(Response::new(super::write::raw(reply)))
    }

    async fn commit_raw(
        &self,
        request: Request<wire::CommitRawRequest>,
    ) -> Result<Response<wire::PreparedRaw>, Status> {
        let node = data_identity(&request)?;
        let input = super::write::commit_raw(request.into_inner())?;
        let reply = bounded(self.app.cluster().map_err(failure)?.commit_raw(
            &node,
            &self.app.secrets.credential_key,
            input,
        ))
        .await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("node-raw-committed").await;
        Ok(Response::new(super::write::raw(reply)))
    }
    async fn prepare_chunks(
        &self,
        request: Request<wire::PrepareChunksRequest>,
    ) -> Result<Response<wire::PreparedChunks>, Status> {
        let node = data_identity(&request)?;
        let input = super::write::prepare(request.into_inner())?;
        let cluster = self.app.cluster().map_err(failure)?;
        let assembly =
            bounded(cluster.prepare_chunks(&node, &self.app.secrets.credential_key, input.clone()))
                .await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("node-chunks-allocated").await;
        let mut nonces = Vec::new();
        if !assembly.ready && !assembly.committed {
            let mut seen = std::collections::HashSet::new();
            for item in assembly.items.iter().filter(|item| !item.reused) {
                let chunk = &item.chunk;
                if chunk.algorithm != "none" && seen.insert(chunk.uuid) {
                    let value = self
                        .app
                        .storage
                        .encoding_nonce(
                            &chunk.algorithm,
                            chunk.block_key_id,
                            chunk.created_at,
                            crate::content::nonce::CHUNK,
                        )
                        .await
                        .map_err(failure)?
                        .ok_or_else(|| Status::internal("missing reserved nonce"))?;
                    nonces.push(model::node_chunk::Nonce {
                        chunk: chunk.uuid,
                        value,
                    });
                }
            }
        }
        let reply = bounded(cluster.activate_chunks(
            &node,
            &self.app.secrets.credential_key,
            input,
            nonces,
        ))
        .await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("node-chunks-prepared").await;
        Ok(Response::new(super::write::prepared(reply)?))
    }

    async fn commit_chunks(
        &self,
        request: Request<wire::CommitChunksRequest>,
    ) -> Result<Response<wire::ChunkCommitReply>, Status> {
        let node = data_identity(&request)?;
        let input = super::write::commit(request.into_inner())?;
        let batch_id = input.batch as u64;
        let end = bounded(self.app.cluster().map_err(failure)?.commit_chunks(
            &node,
            &self.app.secrets.credential_key,
            input,
        ))
        .await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("node-chunks-committed").await;
        Ok(Response::new(wire::ChunkCommitReply {
            batch_id,
            end: end as u64,
        }))
    }

    async fn copy_chunks(
        &self,
        request: Request<wire::CopyChunksRequest>,
    ) -> Result<Response<wire::ChunkCommitReply>, Status> {
        let node = data_identity(&request)?;
        let input = super::write::copy(request.into_inner())?;
        let batch_id = input.batch as u64;
        let end = bounded(self.app.cluster().map_err(failure)?.copy_chunks(
            &node,
            &self.app.secrets.credential_key,
            input,
        ))
        .await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("node-chunks-copied").await;
        Ok(Response::new(wire::ChunkCommitReply {
            batch_id,
            end: end as u64,
        }))
    }

    async fn publish_write(
        &self,
        request: Request<wire::PublishWriteRequest>,
    ) -> Result<Response<wire::WriteReply>, Status> {
        let node = data_identity(&request)?;
        let input = super::write::publish(request.into_inner())?;
        let reply = bounded(self.app.cluster().map_err(failure)?.publish_write(
            &node,
            &self.app.secrets.credential_key,
            input,
        ))
        .await?;
        self.app.wake_gc.notify_one();
        self.app.wake_raw_uploads.notify_one();
        self.app.wake_tasks.notify_one();
        #[cfg(feature = "fault-injection")]
        crate::faults::point("node-write-published").await;
        Ok(Response::new(super::write::reply(reply)?))
    }

    async fn begin_write(
        &self,
        request: Request<wire::BeginWriteRequest>,
    ) -> Result<Response<wire::WriteReply>, Status> {
        let node = data_identity(&request)?;
        let input = super::write::begin(request.into_inner())?;
        let reply = bounded(self.app.cluster().map_err(failure)?.begin_write(
            &node,
            &self.app.secrets.credential_key,
            input,
        ))
        .await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("node-write-opened").await;
        Ok(Response::new(super::write::reply(reply)?))
    }

    async fn write_status(
        &self,
        request: Request<wire::WriteRequest>,
    ) -> Result<Response<wire::WriteReply>, Status> {
        self.write_lookup(request, false).await
    }

    async fn abort_write(
        &self,
        request: Request<wire::WriteRequest>,
    ) -> Result<Response<wire::WriteReply>, Status> {
        self.write_lookup(request, true).await
    }
}

fn data_identity<T>(request: &Request<T>) -> Result<NodeState, Status> {
    let node = request
        .extensions()
        .get::<NodeState>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("node identity is required"))?;
    if node.role != "data" {
        return Err(Status::permission_denied("request requires a data node"));
    }
    Ok(node)
}

impl Service {
    async fn write_lookup(
        &self,
        request: Request<wire::WriteRequest>,
        abort: bool,
    ) -> Result<Response<wire::WriteReply>, Status> {
        let node = data_identity(&request)?;
        let input = super::write::lookup(request.into_inner())?;
        let reply = bounded(self.app.cluster().map_err(failure)?.write_status(
            &node,
            &self.app.secrets.credential_key,
            input,
            abort,
        ))
        .await?;
        Ok(Response::new(super::write::reply(reply)?))
    }
}

#[tonic::async_trait]
impl NodeReads for Service {
    async fn report_read(
        &self,
        request: Request<wire::ReadReportRequest>,
    ) -> Result<Response<wire::ReadReportReply>, Status> {
        let node = request
            .extensions()
            .get::<NodeState>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("node identity is required"))?;
        if node.role != "data" {
            return Err(Status::permission_denied(
                "read reports require a data node",
            ));
        }
        let input = super::read::report(request.into_inner())?;
        let tasks = bounded(self.app.cluster().map_err(failure)?.report_read(
            &node,
            &self.app.secrets.credential_key,
            input,
        ))
        .await?;
        Ok(Response::new(wire::ReadReportReply {
            task_ids: tasks.into_iter().map(|id| id.to_string()).collect(),
        }))
    }
    async fn open_read(
        &self,
        request: Request<wire::ReadRequest>,
    ) -> Result<Response<wire::ReadReply>, Status> {
        let node = request
            .extensions()
            .get::<NodeState>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("node identity is required"))?;
        if node.role != "data" {
            return Err(Status::permission_denied(
                "object reads require a data node",
            ));
        }
        let input = super::read::query(request.into_inner())?;
        let limit = input.max_bytes;
        let reply = bounded(self.app.cluster().map_err(failure)?.open_read(
            &node,
            &self.app.secrets.credential_key,
            input,
        ))
        .await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("node-read-opened").await;
        let reply = super::read::reply(reply);
        if prost::Message::encoded_len(&reply) > limit {
            return Err(Status::resource_exhausted(
                "read reply exceeds message budget",
            ));
        }
        Ok(Response::new(reply))
    }

    async fn read_window(
        &self,
        request: Request<wire::ReadWindowRequest>,
    ) -> Result<Response<wire::ReadReply>, Status> {
        let node = request
            .extensions()
            .get::<NodeState>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("node identity is required"))?;
        if node.role != "data" {
            return Err(Status::permission_denied(
                "object reads require a data node",
            ));
        }
        let input = super::read::window(request.into_inner())?;
        let limit = input.max_bytes;
        let reply = bounded(self.app.cluster().map_err(failure)?.read_window(
            &node,
            &self.app.secrets.credential_key,
            input,
        ))
        .await?;
        let reply = super::read::reply(reply);
        if prost::Message::encoded_len(&reply) > limit {
            return Err(Status::resource_exhausted(
                "read reply exceeds message budget",
            ));
        }
        Ok(Response::new(reply))
    }
}

async fn bounded<T>(
    future: impl std::future::Future<Output = anyhow::Result<T>>,
) -> Result<T, Status> {
    tokio::time::timeout(Duration::from_secs(15), future)
        .await
        .map_err(|_| Status::deadline_exceeded("control operation timed out"))?
        .map_err(failure)
}

fn state(snapshot: &session::Snapshot) -> wire::NodeState {
    wire::NodeState {
        maintenance: snapshot.maintenance,
        draining: snapshot.draining,
        revisions: snapshot
            .settings
            .iter()
            .filter_map(|(name, _, revision)| {
                Some(wire::Revision {
                    section: super::section(name)? as i32,
                    revision: *revision as u64,
                })
            })
            .collect(),
    }
}

#[tonic::async_trait]
impl NodeControl for Service {
    async fn prepare_request(
        &self,
        request: Request<wire::PrepareRequestInput>,
    ) -> Result<Response<wire::RequestReply>, Status> {
        let node = data_identity(&request)?;
        let input =
            super::request::prepare(request.into_inner(), &self.app.config.requests.s3_region)?;
        let reply = bounded(self.app.cluster().map_err(failure)?.prepare_request(
            &node,
            &self.app.secrets.credential_key,
            input,
        ))
        .await?;
        Ok(Response::new(super::request::reply(reply)?))
    }
    async fn report_observations(
        &self,
        request: Request<wire::ObservationRequest>,
    ) -> Result<Response<wire::ObservationReply>, Status> {
        let node = request
            .extensions()
            .get::<NodeState>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("node identity is required"))?;
        if node.role != "data" {
            return Err(Status::permission_denied(
                "read observations require a data node",
            ));
        }
        let input = super::read::observations(request.into_inner())?;
        let compression_written =
            bounded(self.app.cluster().map_err(failure)?.report_observations(
                &node,
                &self.app.secrets.credential_key,
                input,
            ))
            .await?;
        Ok(Response::new(wire::ObservationReply {
            compression_written,
        }))
    }

    async fn sync_node(
        &self,
        request: Request<wire::SyncRequest>,
    ) -> Result<Response<wire::SyncReply>, Status> {
        if prost::Message::encoded_len(request.get_ref()) > 1024 {
            return Err(Status::resource_exhausted(
                "settings request exceeds message budget",
            ));
        }
        let node = request
            .extensions()
            .get::<NodeState>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("node identity is required"))?;
        let input = request.into_inner();
        let run = input
            .run
            .ok_or_else(|| Status::invalid_argument("node run is required"))?;
        let run = session::Run {
            boot: super::uuid(&run.boot_id)?,
            fence: super::uuid(&run.fence)?,
        };
        if input.cached.len() > session::SECTIONS.len() {
            return Err(Status::invalid_argument("too many cached settings"));
        }
        let mut cached = std::collections::HashMap::new();
        for revision in input.cached {
            if wire::Section::try_from(revision.section)
                .ok()
                .is_none_or(|section| section == wire::Section::Unspecified)
                || revision.revision == 0
                || revision.revision > i64::MAX as u64
                || cached.insert(revision.section, revision.revision).is_some()
            {
                return Err(Status::invalid_argument("invalid cached settings"));
            }
        }
        let cluster = self.app.cluster().map_err(failure)?;
        let snapshot =
            bounded(cluster.sync_node(&node, &self.app.secrets.credential_key, run)).await?;
        let settings = snapshot
            .settings
            .iter()
            .filter_map(|(name, value, revision)| {
                let section = super::section(name)? as i32;
                if cached.get(&section) == Some(&(*revision as u64)) {
                    return None;
                }
                Some(serde_json::to_vec(value).map(|json| wire::Settings {
                    version: Some(wire::Revision {
                        section,
                        revision: *revision as u64,
                    }),
                    json,
                }))
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| Status::internal("invalid runtime settings"))?;
        Ok(Response::new(wire::SyncReply {
            valid_for_ms: snapshot.valid_for_ms as u64,
            state: Some(state(&snapshot)),
            settings,
        }))
    }
}

#[tonic::async_trait]
impl NodeSessions for Service {
    type NodeSessionStream =
        Pin<Box<dyn futures_util::Stream<Item = Result<wire::SessionReply, Status>> + Send>>;

    async fn node_session(
        &self,
        request: Request<Streaming<wire::SessionRequest>>,
    ) -> Result<Response<Self::NodeSessionStream>, Status> {
        let node = request
            .extensions()
            .get::<NodeState>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("node identity is required"))?;
        let mut incoming = request.into_inner();
        let first = tokio::time::timeout(Duration::from_secs(5), incoming.message())
            .await
            .map_err(|_| Status::deadline_exceeded("node hello timed out"))??;
        let Some(wire::SessionRequest {
            message: Some(wire::session_request::Message::Hello(hello)),
        }) = first
        else {
            return Err(Status::invalid_argument(
                "first session message must be Hello",
            ));
        };
        if hello.protocol != PROTOCOL || hello.previous_epoch > i64::MAX as u64 {
            return Err(Status::failed_precondition(
                "unsupported node protocol or epoch",
            ));
        }
        let open = session::Open {
            boot: super::uuid(&hello.boot_id)?,
            connection: super::uuid(&hello.connection_id)?,
            previous_epoch: hello.previous_epoch as i64,
        };
        let cluster = self.app.cluster().map_err(failure)?;
        let app = self.app.clone();
        let snapshot =
            bounded(cluster.open_session(&node, &app.secrets.credential_key, open)).await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("node-session-opened").await;
        let stream = async_stream::try_stream! {
            let run = snapshot.run;
            let epoch = snapshot.epoch;
            yield wire::SessionReply { message: Some(wire::session_reply::Message::Welcome(wire::Welcome {
                run: Some(wire::Run { boot_id: run.boot.to_string(), fence: run.fence.to_string() }),
                epoch: epoch as u64, valid_for_ms: snapshot.valid_for_ms as u64, state: Some(state(&snapshot)),
            })) };
            loop {
                let next = tokio::time::timeout(Duration::from_secs(30), incoming.message()).await
                    .map_err(|_| Status::deadline_exceeded("node heartbeat timed out"))??;
                let Some(next) = next else { break; };
                let Some(wire::session_request::Message::Pulse(pulse)) = next.message else {
                    Err(Status::invalid_argument("session expects Pulse"))?;
                    unreachable!();
                };
                if pulse.sequence == 0 || pulse.sequence > i64::MAX as u64 {
                    Err(Status::invalid_argument("invalid heartbeat sequence"))?;
                }
                let resources = super::read::keep_alive(pulse.renew, pulse.release)?;
                let snapshot = bounded(app.cluster().map_err(failure)?.pulse_session(&node, &app.secrets.credential_key, run, epoch, pulse.sequence as i64, pulse.draining, resources)).await?;
                yield wire::SessionReply { message: Some(wire::session_reply::Message::Pulse(wire::PulseReply {
                    sequence: pulse.sequence, valid_for_ms: snapshot.valid_for_ms as u64, state: Some(state(&snapshot)),
                    grants: snapshot.grants.into_iter().map(super::read::grant_result).collect(),
                })) };
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }
}

#[tonic::async_trait]
impl NodeMultipart for Service {
    async fn create_multipart(
        &self,
        request: Request<wire::CreateMultipartRequest>,
    ) -> Result<Response<wire::MultipartReply>, Status> {
        let node = data_identity(&request)?;
        let input = super::multipart::create(request.into_inner())?;
        let reply = bounded(self.app.cluster().map_err(failure)?.multipart_operation(
            &node,
            &self.app.secrets.credential_key,
            input,
            self.app.config.uploads.max_active_uploads,
        ))
        .await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("node-multipart-changed").await;
        Ok(Response::new(super::multipart::reply(reply)))
    }

    async fn abort_multipart(
        &self,
        request: Request<wire::AbortMultipartRequest>,
    ) -> Result<Response<wire::MultipartReply>, Status> {
        let node = data_identity(&request)?;
        let input = super::multipart::abort(request.into_inner())?;
        let reply = bounded(self.app.cluster().map_err(failure)?.multipart_operation(
            &node,
            &self.app.secrets.credential_key,
            input,
            None,
        ))
        .await?;
        self.app.wake_gc.notify_one();
        #[cfg(feature = "fault-injection")]
        crate::faults::point("node-multipart-changed").await;
        Ok(Response::new(super::multipart::reply(reply)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn business_failures_preserve_codes_without_marking_control_unavailable() {
        for (error, code, business) in [
            (
                model::Error::QuotaExceeded.into(),
                tonic::Code::ResourceExhausted,
                "QuotaExceeded",
            ),
            (
                model::Error::OperationAborted.into(),
                tonic::Code::Aborted,
                "OperationAborted",
            ),
            (
                s3s::s3_error!(AccessDenied).into(),
                tonic::Code::PermissionDenied,
                "AccessDenied",
            ),
            (
                s3s::s3_error!(NoSuchBucket).into(),
                tonic::Code::NotFound,
                "NoSuchBucket",
            ),
        ] {
            let status = failure(error);
            assert_eq!(status.code(), code);
            assert_eq!(status.metadata().get("mokyu-s3-code").unwrap(), business);
        }
        let status = failure(model::Error::NodeWriteLimit.into());
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        assert!(status.metadata().get("mokyu-s3-code").is_none());
    }
}
