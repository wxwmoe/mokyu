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
    node_reads_server::{NodeReads, NodeReadsServer},
    node_sessions_server::{NodeSessions, NodeSessionsServer},
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
            .max_decoding_message_size(1024)
            .max_encoding_message_size(MESSAGE_LIMIT),
    )
    .add_service(
        NodeSessionsServer::new(service.clone())
            .max_decoding_message_size(SESSION_LIMIT)
            .max_encoding_message_size(SESSION_LIMIT),
    )
    .add_service(
        NodeReadsServer::new(service)
            .max_decoding_message_size(super::read::REQUEST_LIMIT)
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
    match model::error(&error) {
        Some(Error::NodeAuthentication) => {
            Status::unauthenticated("invalid node identity or credential")
        }
        Some(Error::NodeInactive) => Status::permission_denied("node or deployment is inactive"),
        Some(Error::NodeConflict) => Status::aborted("node is owned by another boot or session"),
        Some(Error::NodeExpired) => Status::failed_precondition("node run is expired or retired"),
        Some(Error::NodeLimit) => {
            Status::resource_exhausted("node boot history is full; rotate its credential")
        }
        Some(Error::NodeReadLimit) => {
            Status::resource_exhausted("node read exceeds its resource or message budget")
        }
        Some(Error::NodeReadBusy) => {
            let mut status = Status::resource_exhausted("node read capacity is busy");
            status
                .metadata_mut()
                .insert("mokyu-retryable", "read-capacity".parse().unwrap());
            status
        }
        _ => {
            tracing::warn!("node RPC database operation failed");
            Status::unavailable("control operation is unavailable")
        }
    }
}

#[tonic::async_trait]
impl NodeReads for Service {
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
    async fn sync_node(
        &self,
        request: Request<wire::SyncRequest>,
    ) -> Result<Response<wire::SyncReply>, Status> {
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
