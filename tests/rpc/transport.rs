use super::*;
use hickory_resolver::{
    config::{ConnectionConfig, NameServerConfig, ResolverConfig},
    net::runtime::TokioRuntimeProvider,
    proto::{
        op::{Message, ResponseCode},
        rr::{Record, rdata::A},
    },
};
use std::{path::PathBuf, sync::RwLock};
use tonic::Code;

#[path = "reads.rs"]
mod reads;

struct Dns {
    answers: Arc<RwLock<Option<Vec<Ipv4Addr>>>>,
    resolver: TokioResolver,
    task: tokio::task::JoinHandle<()>,
}

impl Dns {
    async fn new() -> Result<Self> {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await?;
        let port = socket.local_addr()?.port();
        let answers = Arc::new(RwLock::new(Some(vec![
            Ipv4Addr::new(127, 0, 0, 2),
            Ipv4Addr::new(127, 0, 0, 3),
        ])));
        let shared = answers.clone();
        let task = tokio::spawn(async move {
            let mut buffer = [0; 4096];
            while let Ok((length, peer)) = socket.recv_from(&mut buffer).await {
                let request = Message::from_vec(&buffer[..length]).unwrap();
                let mut response = Message::response(request.metadata.id, request.metadata.op_code);
                response.queries = request.queries.clone();
                response.metadata.recursion_available = true;
                response.edns = request.edns.clone();
                if let Some(answers) = shared.read().unwrap().as_ref() {
                    for query in &request.queries {
                        for address in answers {
                            response.answers.push(Record::from_rdata(
                                query.name().clone(),
                                1,
                                RData::A(A(*address)),
                            ));
                        }
                    }
                } else {
                    response.metadata.response_code = ResponseCode::ServFail;
                }
                socket
                    .send_to(&response.to_vec().unwrap(), peer)
                    .await
                    .unwrap();
            }
        });
        let mut connection = ConnectionConfig::udp();
        connection.port = port;
        let config = ResolverConfig::from_name_servers(vec![NameServerConfig::new(
            Ipv4Addr::LOCALHOST.into(),
            true,
            vec![connection],
        )]);
        let resolver =
            TokioResolver::builder_with_config(config, TokioRuntimeProvider::default()).build()?;
        Ok(Self {
            answers,
            resolver,
            task,
        })
    }

    fn client(&self, url: &str, identity: NodeState) -> Result<Client> {
        let mut client = Client::new(url, identity)?;
        client.resolver = self.resolver.clone();
        client.cursor.store(0, Ordering::Relaxed);
        Ok(client)
    }
}

impl Drop for Dns {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn node_state(
    states: &mut tokio::sync::watch::Receiver<crate::node::State>,
    predicate: impl Fn(&crate::node::State) -> bool,
) -> Result<crate::node::State> {
    tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            let state = states.borrow_and_update().clone();
            if predicate(&state) {
                return Ok(state);
            }
            states.changed().await?;
        }
    })
    .await
    .with_context(|| {
        let state = states.borrow();
        format!(
            "node runtime did not reach expected state: phase={:?}, peer={:?}, epoch={}",
            state.phase, state.peer, state.epoch
        )
    })?
}

#[tokio::test]
#[ignore = "requires two isolated controls and TLS origins; see tests/README.md"]
async fn node_runtime_recovers_without_database_or_resurrecting_old_work() -> Result<()> {
    use crate::{
        config::node::NodeConfig,
        node::{NodeContext, Phase},
    };
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let path = PathBuf::from(std::env::var("MOKYU_TEST_NODE_STATE")?);
    let identity: NodeState = serde_json::from_slice(&tokio::fs::read(&path).await?)?;
    let url = std::env::var("MOKYU_TEST_CONTROL_URL")?;
    let directory = path.parent().unwrap().join("node-runtime");
    tokio::fs::create_dir(&directory).await?;
    crate::cluster::identity::write_state(&directory.join("node.json"), &identity).await?;
    let configuration = format!(
        "[instance]\ncontrol_url='{url}'\ndata_path='{}'\n",
        directory.display()
    );
    assert!(
        NodeContext::new(NodeConfig::parse(&configuration, "worker")?, "worker")
            .await
            .is_err()
    );
    let mut context = NodeContext::new(NodeConfig::parse(&configuration, "data")?, "data").await?;
    assert!(
        NodeContext::new(NodeConfig::parse(&configuration, "data")?, "data")
            .await
            .is_err()
    );
    let dns = Dns::new().await?;
    Arc::get_mut(&mut context).unwrap().client = dns.client(&url, identity.clone())?;
    let mut states = context.subscribe();
    assert!(context.snapshot().is_err());

    // Lose the first committed Welcome; the driver's retry must retain its connection ID.
    fixture("lose-welcome").await?;
    let driver = tokio::spawn(context.clone().run());
    let initial = node_state(&mut states, |state| state.phase == Phase::Ready).await?;
    assert_eq!(initial.epoch, 2);
    assert_eq!(initial.peer, Some(Ipv4Addr::new(127, 0, 0, 3)));
    let held = context.snapshot()?.lease.clone();
    assert_eq!(context.budget.get().unwrap().database_connections, 0);
    fixture("resume-welcome").await?;

    *dns.answers.write().unwrap() = Some(vec![Ipv4Addr::new(127, 0, 0, 2)]);
    dns.resolver.clear_cache();
    context.client.addresses.lock().await.refresh_at = Instant::now();
    let moved = node_state(&mut states, |state| {
        state.phase == Phase::Ready
            && state.epoch > initial.epoch
            && state.peer == Some(Ipv4Addr::new(127, 0, 0, 2))
    })
    .await?;
    assert!(Arc::ptr_eq(&context.snapshot()?.lease, &held));

    *dns.answers.write().unwrap() = Some(vec![
        Ipv4Addr::new(127, 0, 0, 2),
        Ipv4Addr::new(127, 0, 0, 3),
    ]);
    dns.resolver.clear_cache();
    context.client.addresses.lock().await.refresh_at = Instant::now();
    fixture("stop-first").await?;
    // Unary is still usable while the separate session connection recovers.
    assert_eq!(
        context
            .client
            .sync(held.run.clone(), vec![])
            .await?
            .settings
            .len(),
        5
    );
    node_state(&mut states, |state| {
        state.phase == Phase::Ready
            && state.epoch > moved.epoch
            && state.peer == Some(Ipv4Addr::new(127, 0, 0, 3))
    })
    .await?;
    assert!(Arc::ptr_eq(&context.snapshot()?.lease, &held));

    fixture("change-settings").await?;
    node_state(&mut states, |state| {
        state
            .snapshot
            .as_ref()
            .is_some_and(|snapshot| snapshot.runtime().requests.read_idle_timeout == "31s")
    })
    .await?;
    fixture("enable-maintenance").await?;
    node_state(&mut states, |state| state.maintenance).await?;
    assert!(context.snapshot().is_err());
    held.check()?;
    fixture("disable-maintenance").await?;
    node_state(&mut states, |state| !state.maintenance).await?;
    context.snapshot()?;

    fixture("expire-run").await?;
    let replacement = node_state(&mut states, |state| {
        state.phase == Phase::Ready
            && state
                .snapshot
                .as_ref()
                .is_some_and(|snapshot| snapshot.lease.run.fence != held.run.fence)
    })
    .await?;
    assert!(held.check().is_err());
    let fresh = replacement.snapshot.unwrap().lease.clone();
    assert_eq!(fresh.run.boot_id, held.run.boot_id);
    fresh.check()?;

    context.drain();
    assert!(context.snapshot().is_err());
    let draining = node_state(&mut states, |state| {
        state.phase == Phase::Draining && state.draining
    })
    .await?;
    fresh.check()?;
    fixture("start-first").await?;
    *dns.answers.write().unwrap() = Some(vec![Ipv4Addr::new(127, 0, 0, 2)]);
    dns.resolver.clear_cache();
    context.client.addresses.lock().await.refresh_at = Instant::now();
    node_state(&mut states, |state| {
        state.phase == Phase::Draining
            && state.epoch > draining.epoch
            && state.peer == Some(Ipv4Addr::new(127, 0, 0, 2))
    })
    .await?;
    fresh.check()?;

    fixture("rotate-node").await?;
    node_state(&mut states, |state| state.phase == Phase::Fenced).await?;
    assert!(fresh.check().is_err());
    assert!(driver.await?.is_err());
    assert!(context.snapshot().is_err());
    context.stop();
    drop(context);

    let rotated: NodeState = serde_json::from_slice(&tokio::fs::read(&path).await?)?;
    crate::cluster::identity::write_state(&directory.join("node.json"), &rotated).await?;
    let mut context = NodeContext::new(NodeConfig::parse(&configuration, "data")?, "data").await?;
    Arc::get_mut(&mut context).unwrap().client = dns.client(&url, rotated)?;
    let mut states = context.subscribe();
    let driver = tokio::spawn(context.clone().run());
    node_state(&mut states, |state| state.phase == Phase::Ready).await?;
    let held = context.snapshot()?.lease.clone();
    driver.abort();
    assert!(driver.await.unwrap_err().is_cancelled());
    assert_eq!(states.borrow().phase, Phase::Stopped);
    assert!(held.check().is_err());
    assert!(context.snapshot().is_err());
    println!(
        "PASS automatic Welcome recovery, DNS migration, origin loss, settings, maintenance, expiry fencing, drain recovery, rotation, cancellation and directory ownership without database configuration"
    );
    Ok(())
}

async fn fixture(action: &str) -> Result<()> {
    let directory = PathBuf::from(std::env::var("MOKYU_TEST_RPC_ACTIONS")?);
    let request = directory.join(action);
    let done = request.with_extension("done");
    let _ = tokio::fs::remove_file(&done).await;
    tokio::fs::write(&request, b"requested").await?;
    let timeout = if action == "http-complete" { 120 } else { 30 };
    tokio::time::timeout(Duration::from_secs(timeout), async {
        while !done.exists() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await?;
    ensure!(
        tokio::fs::read(&done).await? == b"ok",
        "fixture action failed"
    );
    Ok(())
}

async fn raw_channel(client: &Client) -> Result<Channel> {
    Ok(client
        .endpoint(Ipv4Addr::new(127, 0, 0, 3))?
        .connect()
        .await?)
}

#[tokio::test]
#[ignore = "requires two isolated controls and TLS origins; see tests/README.md"]
async fn node_rpc_balances_tls_origins_and_recovers_sessions() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let path = PathBuf::from(std::env::var("MOKYU_TEST_NODE_STATE")?);
    let identity: NodeState = serde_json::from_slice(&tokio::fs::read(&path).await?)?;
    let url = std::env::var("MOKYU_TEST_CONTROL_URL")?;
    let dns = Dns::new().await?;
    let client = dns.client(&url, identity.clone())?;
    let boot = Uuid::new_v4();
    let connection = Uuid::new_v4();
    let mut first = client.open(boot, connection, 0).await?;
    assert_eq!(first.peer, Ipv4Addr::new(127, 0, 0, 2));
    let run = first.welcome.run.clone().unwrap();
    let initial_epoch = first.welcome.epoch;
    assert!(!first.pulse(1, false).await?.draining);
    for _ in 0..12 {
        let reply = client.sync(run.clone(), vec![]).await?;
        assert_eq!(reply.settings.len(), 5);
    }
    let full = client.sync(run.clone(), vec![]).await?;
    let revisions = full.state.unwrap().revisions;
    assert!(
        client
            .sync(run.clone(), revisions.clone())
            .await?
            .settings
            .is_empty()
    );
    fixture("change-settings").await?;
    let changed = client.sync(run.clone(), revisions).await?;
    assert_eq!(changed.settings.len(), 1);
    assert_eq!(
        changed.settings[0].version.as_ref().unwrap().section,
        wire::Section::Requests as i32
    );
    let parallel =
        futures_util::future::join_all((0..24).map(|_| client.sync(run.clone(), vec![]))).await;
    for reply in parallel {
        assert_eq!(reply?.settings.len(), 5);
    }
    client.cursor.store(1, Ordering::Relaxed);
    let mut replacement = client.open(boot, connection, 0).await?;
    assert_eq!(replacement.peer, Ipv4Addr::new(127, 0, 0, 3));
    assert_eq!(replacement.welcome.epoch, initial_epoch + 1);
    assert_eq!(replacement.welcome.run.as_ref().unwrap().fence, run.fence);
    assert_eq!(
        first.pulse(2, false).await.unwrap_err().code(),
        Code::Aborted
    );
    replacement.pulse(1, false).await?;
    let competing = client.open(Uuid::new_v4(), Uuid::new_v4(), 0).await;
    assert_eq!(competing.err().unwrap().code(), Code::Aborted);
    for invalid in [
        NodeState {
            credential_generation: identity.credential_generation + 1,
            ..identity.clone()
        },
        NodeState {
            deployment_id: Uuid::new_v4(),
            ..identity.clone()
        },
        NodeState {
            node_id: Uuid::new_v4(),
            ..identity.clone()
        },
        NodeState {
            role: "worker".into(),
            ..identity.clone()
        },
    ] {
        let invalid = dns.client(&url, invalid)?;
        assert_eq!(
            invalid.sync(run.clone(), vec![]).await.unwrap_err().code(),
            Code::Unauthenticated
        );
        assert!(
            invalid
                .peers()
                .await?
                .iter()
                .all(|peer| peer.health.lock().unwrap().failures == 0)
        );
    }
    let raw = raw_channel(&client).await?;
    let mut unary = wire::node_control_client::NodeControlClient::new(raw.clone())
        .max_encoding_message_size(MESSAGE_LIMIT * 2);
    let mut duplicate = client.request(wire::SyncRequest {
        run: Some(run.clone()),
        cached: vec![],
    });
    duplicate
        .metadata_mut()
        .append("x-mokyu-node-id", identity.node_id.to_string().parse()?);
    assert_eq!(
        unary.sync_node(duplicate).await.unwrap_err().code(),
        Code::Unauthenticated
    );
    let mut large_headers = client.request(wire::SyncRequest {
        run: Some(run.clone()),
        cached: vec![],
    });
    large_headers
        .metadata_mut()
        .insert("x-padding", "x".repeat(8192).parse()?);
    assert_eq!(
        unary.sync_node(large_headers).await.unwrap_err().code(),
        Code::ResourceExhausted
    );
    let too_large = wire::SyncRequest {
        run: Some(wire::Run {
            boot_id: "x".repeat(MESSAGE_LIMIT + 1),
            fence: run.fence.clone(),
        }),
        cached: vec![],
    };
    assert_eq!(
        unary
            .sync_node(Request::new(too_large.clone()))
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );
    assert!(matches!(
        unary
            .sync_node(client.request(too_large))
            .await
            .unwrap_err()
            .code(),
        Code::OutOfRange | Code::ResourceExhausted
    ));
    let mut sessions = wire::node_sessions_client::NodeSessionsClient::new(raw.clone());
    let too_large = wire::SessionRequest {
        message: Some(wire::session_request::Message::Hello(wire::Hello {
            protocol: PROTOCOL,
            boot_id: "x".repeat(SESSION_LIMIT + 1),
            connection_id: connection.to_string(),
            previous_epoch: 0,
        })),
    };
    let stream = futures_util::stream::iter([too_large]);
    assert!(matches!(
        sessions
            .node_session(client.request(stream))
            .await
            .unwrap_err()
            .code(),
        Code::OutOfRange | Code::ResourceExhausted
    ));
    let mut grpc = tonic::client::Grpc::new(raw);
    grpc.ready().await?;
    let unknown: Result<tonic::Response<wire::SyncReply>, Status> = grpc
        .unary(
            client.request(wire::SyncRequest {
                run: Some(run.clone()),
                cached: vec![],
            }),
            axum::http::uri::PathAndQuery::from_static("/mokyu.node.v1.Unknown/NoMethod"),
            tonic_prost::ProstCodec::default(),
        )
        .await;
    assert_eq!(unknown.unwrap_err().code(), Code::Unimplemented);
    fixture("stop-first").await?;
    client.cursor.store(0, Ordering::Relaxed);
    let started = Instant::now();
    assert_eq!(client.sync(run.clone(), vec![]).await?.settings.len(), 5);
    assert!(started.elapsed() < DEADLINE);
    replacement.pulse(2, false).await?;
    fixture("start-first").await?;
    *dns.answers.write().unwrap() = Some(vec![Ipv4Addr::new(127, 0, 0, 3)]);
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(!client.contains(Ipv4Addr::new(127, 0, 0, 2)).await);
    assert!(client.contains(Ipv4Addr::new(127, 0, 0, 3)).await);
    *dns.answers.write().unwrap() = Some(vec![
        Ipv4Addr::new(127, 0, 0, 2),
        Ipv4Addr::new(127, 0, 0, 3),
    ]);
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(client.peers().await?.len(), 2);
    *dns.answers.write().unwrap() = None;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(client.peers().await?.len(), 2);
    {
        let mut state = client.addresses.lock().await;
        state.stale_at = Instant::now() - Duration::from_secs(1);
        state.refresh_at = state.stale_at;
    }
    assert!(client.peers().await.is_err());
    *dns.answers.write().unwrap() = Some(vec![Ipv4Addr::new(127, 0, 0, 3)]);
    dns.resolver.clear_cache();
    client.addresses.lock().await.refresh_at = Instant::now();
    assert_eq!(client.sync(run.clone(), vec![]).await?.settings.len(), 5);
    let bad_name = url.replace("control.rpc.test", "wrong.rpc.test");
    let bad = dns.client(&bad_name, identity.clone())?;
    assert!(bad.sync(run.clone(), vec![]).await.is_err());
    *dns.answers.write().unwrap() = Some(vec![Ipv4Addr::new(127, 0, 0, 4)]);
    dns.resolver.clear_cache();
    let bad_certificate = dns.client(&url, identity.clone())?;
    assert!(bad_certificate.sync(run.clone(), vec![]).await.is_err());
    *dns.answers.write().unwrap() = Some(vec![
        Ipv4Addr::new(127, 0, 0, 2),
        Ipv4Addr::new(127, 0, 0, 3),
    ]);
    dns.resolver.clear_cache();
    drop(replacement);
    fixture("lose-welcome").await?;
    let recovery = dns.client(&url, identity.clone())?;
    let recovered = recovery
        .open(boot, Uuid::new_v4(), initial_epoch + 1)
        .await?;
    assert_eq!(recovered.peer, Ipv4Addr::new(127, 0, 0, 3));
    assert_eq!(recovered.welcome.run.as_ref().unwrap().fence, run.fence);
    assert_eq!(recovered.welcome.epoch, initial_epoch + 3);
    fixture("resume-welcome").await?;
    drop(recovered);
    fixture("rotate-node").await?;
    assert_eq!(
        client.sync(run.clone(), vec![]).await.unwrap_err().code(),
        Code::Unauthenticated
    );
    let rotated: NodeState = serde_json::from_slice(&tokio::fs::read(&path).await?)?;
    assert_eq!(rotated.node_id, identity.node_id);
    let rotated = dns.client(&url, rotated)?;
    let session = rotated.open(Uuid::new_v4(), Uuid::new_v4(), 0).await?;
    fixture("disable-node").await?;
    assert_eq!(
        rotated
            .sync(session.welcome.run.clone().unwrap(), vec![])
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    println!(
        "PASS TLS A balancing, authority, bounded messages, cached settings, session fencing, origin loss, DNS changes, failed DNS grace, invalid certificates and credential rotation"
    );
    Ok(())
}
