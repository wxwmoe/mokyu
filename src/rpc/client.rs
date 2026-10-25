use super::{MESSAGE_LIMIT, PROTOCOL, SESSION_LIMIT, wire};
use crate::cluster::NodeState;
use anyhow::{Context, Result, ensure};
use hickory_resolver::{TokioResolver, proto::rr::RData};
use std::{
    collections::HashSet,
    net::Ipv4Addr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Mutex as AsyncMutex, OnceCell, Semaphore, mpsc},
    time::Instant,
};
use tonic::{
    Request, Status,
    transport::{Channel, ClientTlsConfig, Endpoint},
};
use uuid::Uuid;

const PEER_LIMIT: usize = 32;
const DEADLINE: Duration = Duration::from_secs(15);
const ATTEMPT: Duration = Duration::from_secs(5);

#[cfg(test)]
#[path = "../../tests/rpc/transport.rs"]
mod transport_tests;

struct Health {
    failures: u32,
    retry_at: Instant,
}

struct Peer {
    address: Ipv4Addr,
    endpoint: Endpoint,
    channel: OnceCell<Channel>,
    health: Mutex<Health>,
}

impl Peer {
    fn failed(&self) {
        let mut health = self.health.lock().unwrap();
        health.failures = (health.failures + 1).min(6);
        health.retry_at = Instant::now()
            + Duration::from_millis(
                ((500u64 << health.failures) + u64::from(Uuid::new_v4().as_bytes()[0])).min(30_000),
            );
    }

    fn succeeded(&self) {
        let mut health = self.health.lock().unwrap();
        health.failures = 0;
        health.retry_at = Instant::now();
    }
}

struct Addresses {
    peers: Vec<Arc<Peer>>,
    refresh_at: Instant,
    stale_at: Instant,
}

pub(crate) struct Client {
    pub(crate) identity: NodeState,
    origin: hyper::Uri,
    domain: String,
    port: u16,
    resolver: TokioResolver,
    addresses: AsyncMutex<Addresses>,
    cursor: AtomicU64,
    requests: Semaphore,
}

impl Client {
    pub(crate) fn new(url: &str, identity: NodeState) -> Result<Self> {
        identity.validate()?;
        ensure!(
            matches!(identity.role.as_str(), "data" | "worker"),
            "control_url requires a data or worker identity"
        );
        let origin = control_uri(url)?;
        let domain = origin
            .host()
            .context("control_url domain is required")?
            .to_owned();
        let port = origin.port_u16().unwrap_or(443);
        let now = Instant::now();
        Ok(Self {
            identity,
            origin,
            domain,
            port,
            resolver: TokioResolver::builder_tokio()?.build()?,
            addresses: AsyncMutex::new(Addresses {
                peers: Vec::new(),
                refresh_at: now,
                stale_at: now,
            }),
            cursor: AtomicU64::new(u64::from_le_bytes(
                Uuid::new_v4().as_bytes()[..8].try_into().unwrap(),
            )),
            requests: Semaphore::new(64),
        })
    }

    fn endpoint(&self, address: Ipv4Addr) -> Result<Endpoint> {
        Ok(
            Endpoint::from_shared(format!("https://{address}:{}", self.port))?
                .origin(self.origin.clone())
                .tls_config(
                    ClientTlsConfig::new()
                        .domain_name(&self.domain)
                        .with_native_roots(),
                )?
                .connect_timeout(Duration::from_secs(3))
                .timeout(ATTEMPT)
                .concurrency_limit(64)
                .buffer_size(64)
                .initial_stream_window_size(1024 * 1024)
                .initial_connection_window_size(2 * 1024 * 1024)
                .http2_max_header_list_size(16 * 1024)
                .tcp_nodelay(true),
        )
    }

    async fn peers(&self) -> Result<Vec<Arc<Peer>>, Status> {
        let mut current = self.addresses.lock().await;
        let now = Instant::now();
        if now >= current.refresh_at {
            let lookup = tokio::time::timeout(
                Duration::from_secs(3),
                self.resolver.ipv4_lookup(self.domain.as_str()),
            )
            .await;
            let replacement = match lookup {
                Ok(Ok(lookup)) => {
                    let addresses: HashSet<_> = lookup
                        .answers()
                        .iter()
                        .filter_map(|record| match &record.data {
                            RData::A(address) => Some(address.0),
                            _ => None,
                        })
                        .collect();
                    if addresses.is_empty() || addresses.len() > PEER_LIMIT {
                        None
                    } else {
                        Some((addresses, Instant::from_std(lookup.valid_until())))
                    }
                }
                _ => None,
            };
            if let Some((addresses, valid_until)) = replacement {
                let mut peers = Vec::with_capacity(addresses.len());
                for address in addresses {
                    if let Some(peer) = current.peers.iter().find(|peer| peer.address == address) {
                        peers.push(peer.clone());
                    } else {
                        peers.push(Arc::new(Peer {
                            address,
                            endpoint: self
                                .endpoint(address)
                                .map_err(|_| Status::unavailable("cannot configure control TLS"))?,
                            channel: OnceCell::new(),
                            health: Mutex::new(Health {
                                failures: 0,
                                retry_at: now,
                            }),
                        }));
                    }
                }
                peers.sort_by_key(|peer| peer.address);
                current.peers = peers;
                current.refresh_at =
                    valid_until.clamp(now + Duration::from_secs(1), now + Duration::from_secs(300));
                current.stale_at = current.refresh_at + Duration::from_secs(300);
            } else {
                current.refresh_at = now + Duration::from_secs(5);
                if now >= current.stale_at {
                    current.peers.clear();
                }
            }
        }
        if current.peers.is_empty() {
            return Err(Status::unavailable("control DNS has no usable A records"));
        }
        Ok(current.peers.clone())
    }

    async fn select(&self, excluded: &HashSet<Ipv4Addr>) -> Result<Arc<Peer>, Status> {
        let peers = self.peers().await?;
        let cursor = self.cursor.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();
        for offset in 0..peers.len() {
            let peer = &peers[(cursor.wrapping_add(offset as u64) % peers.len() as u64) as usize];
            if !excluded.contains(&peer.address) && peer.health.lock().unwrap().retry_at <= now {
                return Ok(peer.clone());
            }
        }
        Err(Status::unavailable("control addresses are cooling down"))
    }

    fn request<T>(&self, input: T) -> Request<T> {
        let mut request = Request::new(input);
        let metadata = request.metadata_mut();
        for (name, value) in [
            (
                "x-mokyu-deployment-id",
                self.identity.deployment_id.to_string(),
            ),
            ("x-mokyu-node-id", self.identity.node_id.to_string()),
            ("x-mokyu-node-role", self.identity.role.clone()),
            (
                "x-mokyu-credential-generation",
                self.identity.credential_generation.to_string(),
            ),
            (
                "authorization",
                format!("Bearer {}", self.identity.credential),
            ),
        ] {
            metadata.insert(name, value.parse().expect("validated node metadata"));
        }
        request
    }

    pub(crate) async fn sync(
        &self,
        run: wire::Run,
        cached: Vec<wire::Revision>,
    ) -> Result<wire::SyncReply, Status> {
        self.unary(
            wire::SyncRequest {
                run: Some(run),
                cached,
            },
            "/mokyu.node.v1.NodeControl/SyncNode",
        )
        .await
    }

    pub(crate) async fn read(&self, input: wire::ReadRequest) -> Result<wire::ReadReply, Status> {
        self.unary(input, "/mokyu.node.v1.NodeReads/OpenRead").await
    }

    pub(crate) async fn prepare_request(
        &self,
        input: wire::PrepareRequestInput,
    ) -> Result<wire::RequestReply, Status> {
        self.unary(input, "/mokyu.node.v1.NodeControl/PrepareRequest")
            .await
    }

    pub(crate) async fn change_objects(
        &self,
        input: wire::ChangeObjectsRequest,
    ) -> Result<wire::ChangeObjectsReply, Status> {
        self.unary(input, "/mokyu.node.v1.NodeObjects/ChangeObjects")
            .await
    }

    pub(crate) async fn create_multipart(
        &self,
        input: wire::CreateMultipartRequest,
    ) -> Result<wire::MultipartReply, Status> {
        self.unary(input, "/mokyu.node.v1.NodeMultipart/CreateMultipart")
            .await
    }

    pub(crate) async fn prepare_local_part(
        &self,
        input: wire::PrepareLocalPartRequest,
    ) -> Result<wire::WriteReply, Status> {
        self.unary(input, "/mokyu.node.v1.NodeWrites/PrepareLocalPart")
            .await
    }

    pub(crate) async fn exchange_work(
        &self,
        input: wire::WorkRequest,
    ) -> Result<wire::WorkReply, Status> {
        self.unary(input, "/mokyu.node.v1.NodeWork/Exchange").await
    }

    pub(crate) async fn claim_native(
        &self,
        input: wire::NativeClaimRequest,
    ) -> Result<wire::NativeWorkReply, Status> {
        self.unary(input, "/mokyu.node.v1.NodeMultipart/ClaimNative")
            .await
    }

    pub(crate) async fn finish_native(
        &self,
        input: wire::NativeFinishRequest,
    ) -> Result<wire::NativeFinishReply, Status> {
        self.unary(input, "/mokyu.node.v1.NodeMultipart/FinishNative")
            .await
    }

    pub(crate) async fn check_local_files(
        &self,
        input: wire::LocalFilesRequest,
    ) -> Result<wire::LocalFilesReply, Status> {
        self.unary(input, "/mokyu.node.v1.NodeMultipart/CheckLocalFiles")
            .await
    }

    pub(crate) async fn prepare_native_complete(
        &self,
        input: wire::NativeCompleteRequest,
    ) -> Result<wire::NativeCompleteReply, Status> {
        self.unary(input, "/mokyu.node.v1.NodeMultipart/PrepareNativeComplete")
            .await
    }

    pub(crate) async fn adopt_native_complete(
        &self,
        input: wire::AdoptNativeRequest,
    ) -> Result<wire::WriteReply, Status> {
        self.unary(input, "/mokyu.node.v1.NodeMultipart/AdoptNativeComplete")
            .await
    }

    pub(crate) async fn begin_complete(
        &self,
        input: wire::BeginCompleteRequest,
    ) -> Result<wire::WriteReply, Status> {
        self.unary(input, "/mokyu.node.v1.NodeMultipart/BeginComplete")
            .await
    }

    pub(crate) async fn read_complete(
        &self,
        input: wire::ReadCompleteRequest,
    ) -> Result<wire::CompleteSources, Status> {
        self.unary(input, "/mokyu.node.v1.NodeMultipart/ReadComplete")
            .await
    }

    pub(crate) async fn complete_status(
        &self,
        input: wire::CompleteStatusRequest,
    ) -> Result<wire::CompleteState, Status> {
        self.unary(input, "/mokyu.node.v1.NodeMultipart/CompleteStatus")
            .await
    }

    pub(crate) async fn abort_multipart(
        &self,
        input: wire::AbortMultipartRequest,
    ) -> Result<wire::MultipartReply, Status> {
        self.unary(input, "/mokyu.node.v1.NodeMultipart/AbortMultipart")
            .await
    }

    pub(crate) async fn report_read(
        &self,
        input: wire::ReadReportRequest,
    ) -> Result<wire::ReadReportReply, Status> {
        self.unary(input, "/mokyu.node.v1.NodeReads/ReportRead")
            .await
    }

    pub(crate) async fn report_observations(
        &self,
        input: wire::ObservationRequest,
    ) -> Result<wire::ObservationReply, Status> {
        self.unary(input, "/mokyu.node.v1.NodeControl/ReportObservations")
            .await
    }

    pub(crate) async fn read_window(
        &self,
        input: wire::ReadWindowRequest,
    ) -> Result<wire::ReadReply, Status> {
        self.unary(input, "/mokyu.node.v1.NodeReads/ReadWindow")
            .await
    }

    pub(crate) async fn begin_write(
        &self,
        input: wire::BeginWriteRequest,
    ) -> Result<wire::WriteReply, Status> {
        self.unary(input, "/mokyu.node.v1.NodeWrites/BeginWrite")
            .await
    }

    pub(crate) async fn write_status(
        &self,
        input: wire::WriteRequest,
    ) -> Result<wire::WriteReply, Status> {
        self.unary(input, "/mokyu.node.v1.NodeWrites/WriteStatus")
            .await
    }

    pub(crate) async fn abort_write(
        &self,
        input: wire::WriteRequest,
    ) -> Result<wire::WriteReply, Status> {
        self.unary(input, "/mokyu.node.v1.NodeWrites/AbortWrite")
            .await
    }

    pub(crate) async fn prepare_chunks(
        &self,
        input: wire::PrepareChunksRequest,
    ) -> Result<wire::PreparedChunks, Status> {
        self.unary(input, "/mokyu.node.v1.NodeWrites/PrepareChunks")
            .await
    }

    pub(crate) async fn prepare_tail(
        &self,
        input: wire::PrepareTailRequest,
    ) -> Result<wire::PreparedTail, Status> {
        self.unary(input, "/mokyu.node.v1.NodeWrites/PrepareTail")
            .await
    }

    pub(crate) async fn commit_tail(
        &self,
        input: wire::WriteRequest,
    ) -> Result<wire::PreparedTail, Status> {
        self.unary(input, "/mokyu.node.v1.NodeWrites/CommitTail")
            .await
    }

    pub(crate) async fn prepare_raw(
        &self,
        input: wire::PrepareRawRequest,
    ) -> Result<wire::PreparedRaw, Status> {
        self.unary(input, "/mokyu.node.v1.NodeWrites/PrepareRaw")
            .await
    }

    pub(crate) async fn commit_raw(
        &self,
        input: wire::CommitRawRequest,
    ) -> Result<wire::PreparedRaw, Status> {
        self.unary(input, "/mokyu.node.v1.NodeWrites/CommitRaw")
            .await
    }

    pub(crate) async fn commit_chunks(
        &self,
        input: wire::CommitChunksRequest,
    ) -> Result<wire::ChunkCommitReply, Status> {
        self.unary(input, "/mokyu.node.v1.NodeWrites/CommitChunks")
            .await
    }

    pub(crate) async fn copy_chunks(
        &self,
        input: wire::CopyChunksRequest,
    ) -> Result<wire::ChunkCommitReply, Status> {
        self.unary(input, "/mokyu.node.v1.NodeWrites/CopyChunks")
            .await
    }

    pub(crate) async fn publish_write(
        &self,
        input: wire::PublishWriteRequest,
    ) -> Result<wire::WriteReply, Status> {
        self.unary(input, "/mokyu.node.v1.NodeWrites/PublishWrite")
            .await
    }

    async fn unary<I, O>(&self, input: I, path: &'static str) -> Result<O, Status>
    where
        I: prost::Message + Default + Clone + 'static,
        O: prost::Message + Default + 'static,
    {
        let limit = if path == "/mokyu.node.v1.NodeMultipart/BeginComplete" {
            crate::model::node_complete::REQUEST_LIMIT
        } else if path == "/mokyu.node.v1.NodeObjects/ChangeObjects" {
            crate::model::node_object::REQUEST_LIMIT
        } else {
            super::read::REQUEST_LIMIT
        };
        if input.encoded_len() > limit {
            return Err(Status::resource_exhausted(
                "node request exceeds message limit",
            ));
        }
        let _permit = self
            .requests
            .try_acquire()
            .map_err(|_| Status::resource_exhausted("node control requests are busy"))?;
        let deadline = Instant::now() + DEADLINE;
        tokio::time::timeout_at(deadline, async {
            let mut excluded = HashSet::new();
            let mut failures = 0;
            loop {
                let peer = match self.select(&excluded).await {
                    Ok(peer) => peer,
                    Err(error) => return Err(error),
                };
                excluded.insert(peer.address);
                let attempt = deadline.min(Instant::now() + ATTEMPT);
                let result = tokio::time::timeout_at(attempt, async {
                    let channel = peer
                        .channel
                        .get_or_try_init(|| async {
                            peer.endpoint
                                .connect()
                                .await
                                .map_err(|_| Status::unavailable("control connection failed"))
                        })
                        .await?;
                    let transport = tower::ServiceBuilder::new()
                        .map_response(proxy_response)
                        .service(channel.clone());
                    let mut client = tonic::client::Grpc::new(transport)
                        .max_decoding_message_size(MESSAGE_LIMIT)
                        .max_encoding_message_size(MESSAGE_LIMIT);
                    client
                        .ready()
                        .await
                        .map_err(|_| Status::unavailable("control channel is unavailable"))?;
                    let mut request = self.request(input.clone());
                    request.set_timeout(attempt.saturating_duration_since(Instant::now()));
                    client
                        .unary(
                            request,
                            hyper::http::uri::PathAndQuery::from_static(path),
                            tonic_prost::ProstCodec::<I, O>::default(),
                        )
                        .await
                        .map(|response| response.into_inner())
                })
                .await
                .unwrap_or_else(|_| Err(Status::deadline_exceeded("control attempt timed out")));
                match result {
                    Ok(reply) => {
                        peer.succeeded();
                        return Ok(reply);
                    }
                    Err(error) if capacity_busy(&error) => {
                        peer.succeeded();
                        excluded.remove(&peer.address);
                        let jitter = u64::from(Uuid::new_v4().as_bytes()[0]);
                        tokio::time::sleep(Duration::from_millis(100 + jitter)).await;
                    }
                    Err(error) if retryable(&error) => {
                        peer.failed();
                        failures += 1;
                        if failures == 3 {
                            return Err(error);
                        }
                    }
                    Err(error) => return Err(error),
                }
            }
        })
        .await
        .unwrap_or_else(|_| Err(Status::deadline_exceeded("control deadline exceeded")))
    }

    pub(crate) async fn open(
        &self,
        boot: Uuid,
        connection: Uuid,
        previous_epoch: u64,
    ) -> Result<Session, Status> {
        let deadline = Instant::now() + DEADLINE;
        tokio::time::timeout_at(deadline, async {
            let mut excluded = HashSet::new();
            let mut last = Status::unavailable("control is unavailable");
            for _ in 0..3 {
                let peer = self.select(&excluded).await?;
                excluded.insert(peer.address);
                let started = Instant::now();
                let result = tokio::time::timeout_at(deadline.min(started + ATTEMPT), async {
                    // This is a new transport connection, independent of the Unary Channel.
                    let channel = peer.endpoint.connect().await.map_err(|_| Status::unavailable("session connection failed"))?;
                    let transport = tower::ServiceBuilder::new().map_response(proxy_response).service(channel);
                    let mut client = wire::node_sessions_client::NodeSessionsClient::new(transport)
                        .max_decoding_message_size(SESSION_LIMIT).max_encoding_message_size(SESSION_LIMIT);
                    let (sender, mut receiver) = mpsc::channel(1);
                    sender.try_send(wire::SessionRequest { message: Some(wire::session_request::Message::Hello(wire::Hello {
                        protocol: PROTOCOL, boot_id: boot.to_string(), connection_id: connection.to_string(), previous_epoch,
                    })) }).map_err(|_| Status::data_loss("session hello could not be queued"))?;
                    let outgoing = async_stream::stream! { while let Some(message) = receiver.recv().await { yield message; } };
                    let mut incoming = client.node_session(self.request(outgoing)).await?.into_inner();
                    let Some(wire::SessionReply { message: Some(wire::session_reply::Message::Welcome(welcome)) }) = incoming.message().await? else {
                        return Err(Status::data_loss("control did not send Welcome"));
                    };
                    let run = welcome.run.as_ref().ok_or_else(|| Status::data_loss("control omitted node run"))?;
                    if super::uuid(&run.boot_id)? != boot || welcome.epoch == 0 {
                        return Err(Status::data_loss("control returned a different node run"));
                    }
                    super::uuid(&run.fence)?;
                    let valid_until = lease_deadline(started, welcome.valid_for_ms)?;
                    Ok(Session { peer: peer.address, sender, incoming, welcome, valid_until })
                }).await.unwrap_or_else(|_| Err(Status::deadline_exceeded("session connection timed out")));
                match result {
                    Ok(session) => { peer.succeeded(); return Ok(session); }
                    Err(error) if retryable(&error) || error.code() == tonic::Code::Internal => { peer.failed(); last = error; }
                    Err(error) => return Err(error),
                }
            }
            Err(last)
        }).await.unwrap_or_else(|_| Err(Status::deadline_exceeded("session deadline exceeded")))
    }

    pub(crate) async fn contains(&self, address: Ipv4Addr) -> bool {
        self.peers()
            .await
            .is_ok_and(|peers| peers.iter().any(|peer| peer.address == address))
    }
}

pub(crate) struct Session {
    pub(crate) peer: Ipv4Addr,
    sender: mpsc::Sender<wire::SessionRequest>,
    incoming: tonic::Streaming<wire::SessionReply>,
    pub(crate) welcome: wire::Welcome,
    pub(crate) valid_until: Instant,
}

impl Session {
    pub(crate) async fn pulse(
        &mut self,
        sequence: u64,
        draining: bool,
    ) -> Result<wire::NodeState, Status> {
        self.pulse_resources(sequence, draining, Vec::new(), Vec::new())
            .await?
            .state
            .ok_or_else(|| Status::data_loss("control omitted node state"))
    }

    pub(crate) async fn pulse_resources(
        &mut self,
        sequence: u64,
        draining: bool,
        renew: Vec<wire::GrantRef>,
        release: Vec<wire::GrantRef>,
    ) -> Result<wire::PulseReply, Status> {
        super::read::keep_alive(renew.clone(), release.clone())?;
        let expected: Vec<_> = renew.iter().chain(&release).cloned().collect();
        let renew_count = renew.len();
        let started = Instant::now();
        let deadline = self.valid_until.min(started + Duration::from_secs(30));
        let reply = tokio::time::timeout_at(deadline, async {
            self.sender
                .send(wire::SessionRequest {
                    message: Some(wire::session_request::Message::Pulse(wire::Pulse {
                        sequence,
                        draining,
                        renew,
                        release,
                    })),
                })
                .await
                .map_err(|_| Status::unavailable("node session closed"))?;
            self.incoming.message().await
        })
        .await
        .map_err(|_| Status::deadline_exceeded("node heartbeat acknowledgement timed out"))??;
        let Some(wire::SessionReply {
            message: Some(wire::session_reply::Message::Pulse(reply)),
        }) = reply
        else {
            return Err(Status::unavailable(
                "node session did not acknowledge heartbeat",
            ));
        };
        if reply.sequence != sequence {
            return Err(Status::data_loss("node heartbeat sequence does not match"));
        }
        self.valid_until = lease_deadline(started, reply.valid_for_ms)?;
        if reply.state.is_none()
            || reply.grants.len() != expected.len()
            || reply
                .grants
                .iter()
                .zip(&expected)
                .enumerate()
                .any(|(index, (result, expected))| {
                    result.grant.as_ref() != Some(expected)
                        || if index >= renew_count {
                            !result.accepted || result.valid_for_ms != 0
                        } else if result.accepted {
                            result.valid_for_ms == 0 || result.valid_for_ms > 90_000
                        } else {
                            result.valid_for_ms != 0
                        }
                })
        {
            return Err(Status::data_loss("invalid resource acknowledgement"));
        }
        Ok(reply)
    }
}

fn capacity_busy(status: &Status) -> bool {
    status.code() == tonic::Code::ResourceExhausted
        && status
            .metadata()
            .get("mokyu-retryable")
            .is_some_and(|value| value == "read-capacity" || value == "write-capacity")
}

pub(crate) fn retryable(status: &Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
    ) || (status.code() == tonic::Code::Cancelled
        // Tonic reports its own transport deadline as Cancelled, including behind a proxy.
        && std::iter::successors(std::error::Error::source(status), |error| error.source())
            .any(|error| error.is::<tonic::TimeoutExpired>()))
}

fn proxy_response(
    response: hyper::Response<tonic::body::Body>,
) -> hyper::Response<tonic::body::Body> {
    if response.headers().contains_key("grpc-status")
        || (response.status() == hyper::StatusCode::OK
            && response
                .headers()
                .get("content-type")
                .is_some_and(|value| value.as_bytes().starts_with(b"application/grpc")))
    {
        return response;
    }
    let code = match response.status().as_u16() {
        400 => tonic::Code::Internal,
        401 => tonic::Code::Unauthenticated,
        403 => tonic::Code::PermissionDenied,
        404 => tonic::Code::Unimplemented,
        200 | 429 | 502 | 503 | 504 => tonic::Code::Unavailable,
        _ => tonic::Code::Unknown,
    };
    // Discard proxy HTML before tonic can misclassify it as a protobuf decode failure.
    Status::new(code, "control returned an HTTP gateway response").into_http()
}

pub(crate) fn lease_deadline(started: Instant, milliseconds: u64) -> Result<Instant, Status> {
    if !(2001..=90_000).contains(&milliseconds) {
        return Err(Status::failed_precondition(
            "node lease is expired or invalid",
        ));
    }
    let deadline = started + Duration::from_millis(milliseconds - 2000);
    if deadline <= Instant::now() {
        return Err(Status::deadline_exceeded("node lease expired in transit"));
    }
    Ok(deadline)
}

pub(crate) fn control_uri(value: &str) -> Result<hyper::Uri> {
    let uri: hyper::Uri = value.parse().context("invalid control_url")?;
    ensure!(
        uri.scheme_str() == Some("https")
            && uri
                .path_and_query()
                .is_none_or(|path| matches!(path.as_str(), "" | "/"))
            && uri
                .authority()
                .is_some_and(|authority| !authority.as_str().contains('@'))
            && uri.host().is_some_and(
                |host| host.parse::<std::net::IpAddr>().is_err() && !host.contains(':')
            )
            && uri.port_u16() != Some(0),
        "control_url must be an HTTPS domain origin without credentials, path or query"
    );
    Ok(uri)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_explicit_read_capacity_errors_wait_for_resources() {
        let mut busy = Status::resource_exhausted("node read capacity is busy");
        assert!(!capacity_busy(&busy));
        busy.metadata_mut()
            .insert("mokyu-retryable", "read-capacity".parse().unwrap());
        assert!(capacity_busy(&busy));
        busy.metadata_mut()
            .insert("mokyu-retryable", "write-capacity".parse().unwrap());
        assert!(capacity_busy(&busy));
        assert!(!retryable(&busy));
        let mut denied = Status::permission_denied("revoked");
        denied
            .metadata_mut()
            .insert("mokyu-retryable", "read-capacity".parse().unwrap());
        assert!(!capacity_busy(&denied));
    }

    #[test]
    fn transport_deadlines_retry_but_server_cancellation_does_not() {
        let timeout = Status::from_error(Box::new(tonic::TimeoutExpired(())));
        assert_eq!(timeout.code(), tonic::Code::Cancelled);
        assert!(retryable(&timeout));
        assert!(!retryable(&Status::cancelled("Timeout expired")));
        assert!(!retryable(&Status::internal("server failure")));
        assert!(!retryable(&Status::permission_denied("revoked")));
    }

    #[test]
    fn proxy_errors_preserve_auth_failures_and_allow_bounded_failover() {
        for (http, code) in [
            (200, 14),
            (400, 13),
            (401, 16),
            (403, 7),
            (404, 12),
            (429, 14),
            (502, 14),
            (503, 14),
            (504, 14),
        ] {
            let response = hyper::Response::builder()
                .status(http)
                .header("content-type", "text/html")
                .body(tonic::body::Body::empty())
                .unwrap();
            assert_eq!(
                proxy_response(response).headers()["grpc-status"],
                code.to_string()
            );
        }
        let response = Status::permission_denied("revoked").into_http();
        assert_eq!(proxy_response(response).headers()["grpc-status"], "7");
        let response = hyper::Response::builder()
            .header("content-type", "application/grpc+proto")
            .body(tonic::body::Body::empty())
            .unwrap();
        assert!(
            !proxy_response(response)
                .headers()
                .contains_key("grpc-status")
        );
    }

    #[test]
    fn control_identity_and_monotonic_lease_boundaries() {
        assert!(control_uri("https://control.example:443/").is_ok());
        for invalid in [
            "http://control.example",
            "https://127.0.0.1",
            "https://[::1]",
            "https://u:p@control.example",
            "https://control.example/rpc",
            "https://control.example/?x=1",
            "https://control.example:0",
        ] {
            assert!(control_uri(invalid).is_err(), "{invalid}");
        }
        let started = Instant::now() - Duration::from_secs(30);
        assert!(lease_deadline(started, 90_000).unwrap() <= started + Duration::from_secs(88));
        assert!(lease_deadline(started, 10_000).is_err());
        assert!(lease_deadline(started, 90_001).is_err());
    }
}
