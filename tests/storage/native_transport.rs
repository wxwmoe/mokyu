use super::*;
use crate::{
    cluster::NodeState,
    config::node::NodeConfig,
    content::{
        codec::{self, Key},
        fragment::UNIT,
    },
    model::{
        session::Run,
        storage_cluster::{
            Announcement, Device, Group, Manifest, Member, Registration, Target, Topology,
        },
        storage_recovery::public_key,
        storage_transfer::{Completion, Destination, FragmentState, Grant, SignedGrant},
    },
    node::storage::{
        Connection,
        fragments::tests::{fixture, header},
    },
    rpc::client::Client,
    storage::native::transport::{Pool, Renewal},
};
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::time::Instant;
use uuid::Uuid;

pub(crate) struct TestNode {
    pub state: NodeState,
    pub run: Run,
    pub url: String,
    node: Arc<Node>,
    reads: Arc<Semaphore>,
    task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    _directory: tempfile::TempDir,
}

impl TestNode {
    pub async fn new(state: NodeState, run: Run) -> Result<Self> {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let (directory, instance, mut budget, files) = fixture("256MiB").await?;
        budget.storage_upload_slots = 1;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("https://{}", listener.local_addr()?);
        let node = Arc::new(Node {
            config: NodeConfig {
                instance,
                configured_fields: Default::default(),
            },
            budget,
            writes: Arc::new(Semaphore::new(1)),
            active_writes: Mutex::new(vec![]),
            backup_reads: Default::default(),
            client: Client::new("https://localhost:1", state.clone())?,
            files,
            boot: run.boot,
            started: chrono::Utc::now(),
            connection: Mutex::new(Connection {
                phase: "registered",
                run: Some(wire::Run {
                    boot_id: run.boot.to_string(),
                    fence: run.fence.to_string(),
                }),
                deadline: Instant::now() + Duration::from_secs(180),
                mode: Some("single_control".into()),
                mode_revision: 1,
                maintenance: false,
                error: None,
                peer: None,
                control_id: None,
                epoch: 1,
            }),
        });
        let incoming = async_stream::stream! { loop { yield listener.accept().await.map(|(socket, _)| socket); } };
        let tls = std::path::PathBuf::from(std::env::var("MOKYU_TEST_STORAGE_TLS")?);
        let identity = Identity::from_pem(
            tokio::fs::read(tls.join("cert.pem")).await?,
            tokio::fs::read(tls.join("key.pem")).await?,
        );
        let reads = Arc::new(Semaphore::new(2));
        let service = Service {
            node: node.clone(),
            requests: Arc::new(Semaphore::new(2)),
            reads: reads.clone(),
            forwarding: Pool::new(Duration::from_secs(60)),
        };
        let task = tokio::spawn(
            Server::builder()
                .tls_config(ServerTlsConfig::new().identity(identity))?
                .add_service(StorageNodeServer::new(service))
                .serve_with_incoming(incoming),
        );
        Ok(Self {
            state,
            run,
            url,
            node,
            reads,
            task,
            _directory: directory,
        })
    }

    pub fn announcement(&self) -> Result<Announcement> {
        Ok(Announcement {
            media: self.node.files.media.clone(),
            topology: Topology {
                url: self.url.clone(),
                region: Some("test".into()),
                storage_group: self.state.node_id.to_string(),
                purpose: "primary".into(),
            },
            capacity: self.node.files.capacity(self.node.boot)?,
            load: Some(self.node.write_load()),
            manifest: self.node.files.manifest.read().unwrap().clone(),
            pending_manifest: self.node.files.pending.read().unwrap().clone(),
            history_after: self
                .node
                .files
                .history
                .lock()
                .unwrap()
                .as_ref()
                .map_or(0, |head| head.revision),
        })
    }

    pub fn available_reads(&self) -> usize {
        self.reads.available_permits()
    }

    pub fn disk(&self) -> Arc<crate::storage::Disk> {
        self.node.files.disk.clone()
    }

    pub async fn apply(&self, reply: Registration) -> Result<()> {
        self.node.files.apply(reply, self.state.deployment_id).await
    }

    pub async fn historical_layout(&self, head: Manifest, history: Vec<Manifest>) -> Result<()> {
        let deployment = head.deployment_id;
        self.node
            .files
            .apply(
                Registration {
                    manifest: Some(head),
                    history,
                },
                deployment,
            )
            .await
    }

    pub async fn layout_file(&self, record: &Manifest, remove: bool) -> Result<()> {
        let path = self
            ._directory
            .path()
            .join("cluster/history")
            .join(record.cluster_id.simple().to_string())
            .join(format!("{}.json", record.record_id.simple()));
        if remove {
            tokio::fs::remove_file(path).await?;
        } else {
            tokio::fs::write(path, serde_json::to_vec(record)?).await?;
        }
        Ok(())
    }

    pub async fn evidence_file(
        &self,
        proof: &crate::model::storage_recovery::Proof,
        remove: bool,
    ) -> Result<()> {
        let path = crate::node::storage::recovery::history(
            self._directory.path(),
            proof.publication.namespace,
            proof.publication.id,
        );
        if remove {
            tokio::fs::remove_file(path).await?;
        } else {
            tokio::fs::write(path, serde_json::to_vec(proof)?).await?;
        }
        Ok(())
    }

    pub async fn recovery_head(
        &self,
        namespace: Uuid,
        head: &crate::model::storage_recovery::Link,
    ) -> Result<()> {
        let path = self
            ._directory
            .path()
            .join("recovery/heads")
            .join(format!("{}.json", namespace.simple()));
        tokio::fs::write(path, serde_json::to_vec(head)?).await?;
        Ok(())
    }

    pub async fn corrupt(
        &self,
        header: &crate::content::fragment::Header,
        offset: u64,
    ) -> Result<()> {
        self.fragment_byte(header, offset, 0).await
    }

    pub async fn remove_fragment(&self, header: &crate::content::fragment::Header) -> Result<()> {
        anyhow::ensure!(
            header.media == self.node.files.media.media_id,
            "wrong test fragment"
        );
        tokio::fs::remove_file(
            self._directory
                .path()
                .join(header.area.path(header.fragment)),
        )
        .await?;
        Ok(())
    }

    pub async fn fragment_byte(
        &self,
        header: &crate::content::fragment::Header,
        offset: u64,
        value: u8,
    ) -> Result<()> {
        use tokio::io::{AsyncSeekExt, AsyncWriteExt};
        anyhow::ensure!(
            header.media == self.node.files.media.media_id && offset < header.shard_bytes,
            "wrong test fragment"
        );
        let path = self
            ._directory
            .path()
            .join(header.area.path(header.fragment));
        let mut file = tokio::fs::OpenOptions::new().write(true).open(path).await?;
        file.seek(std::io::SeekFrom::Start(
            crate::content::fragment::HEADER_SIZE as u64 + offset,
        ))
        .await?;
        file.write_all(&[value]).await?;
        file.sync_all().await?;
        Ok(())
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_STORAGE_TLS and its CA in SSL_CERT_FILE"]
async fn native_layout_discovery_uses_current_registration() -> Result<()> {
    let state = NodeState {
        format: 1,
        node_id: Uuid::now_v7(),
        deployment_id: Uuid::now_v7(),
        role: "storage".into(),
        credential_generation: 1,
        credential: "b".repeat(64),
    };
    let node = TestNode::new(
        state,
        Run {
            boot: Uuid::now_v7(),
            fence: Uuid::now_v7(),
        },
    )
    .await?;
    let initial = Manifest {
        format_version: 1,
        record_id: Uuid::now_v7(),
        cluster_id: Uuid::now_v7(),
        deployment_id: Uuid::now_v7(),
        publication_key: [21; 32],
        takeover_epoch: 1,
        revision: 1,
        previous: None,
        members: vec![Member {
            media_id: node.node.files.media.media_id,
            node_id: Uuid::now_v7(),
            topology: node.announcement()?.topology,
            state: "available".into(),
        }],
        groups: vec![],
    };
    let mut head = initial.clone();
    let mut history = vec![initial];
    for _ in 0..12 {
        let previous = head.digest()?;
        head.record_id = Uuid::now_v7();
        head.revision += 1;
        head.previous = Some(previous);
        history.push(head.clone());
    }
    node.node
        .files
        .apply(
            Registration {
                manifest: Some(head.clone()),
                history: history.clone(),
            },
            head.deployment_id,
        )
        .await?;
    let pool = Pool::new(Duration::from_secs(10));
    assert!(
        pool.layouts(&node.url, "wrong", node.run, &head)
            .await
            .is_err()
    );
    assert!(
        pool.layouts(
            &node.url,
            &node.state.credential,
            Run {
                fence: Uuid::now_v7(),
                ..node.run
            },
            &head
        )
        .await
        .is_err()
    );
    let mut pages = pool
        .layouts(&node.url, &node.state.credential, node.run, &head)
        .await?;
    let mut found = Vec::new();
    while let Some(page) = pages.try_next().await? {
        for entry in page.entries {
            assert!(!entry.pending && entry.error.is_none() && entry.proof.is_none());
            found.push(entry.manifest.context("missing layout")?);
        }
    }
    found.sort_by_key(|manifest| manifest.revision);
    assert_eq!(found, history);
    node.node.connection.lock().unwrap().deadline = Instant::now();
    tokio::time::timeout(Duration::from_secs(2), async {
        while node.node.files.mutations.clone().try_write_owned().is_err() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(
        pool.layouts(&node.url, &node.state.credential, node.run, &head)
            .await
            .is_err()
    );
    Ok(())
}

struct Authority {
    key: Key,
    manifest: Manifest,
    node: Arc<Node>,
    renewals: AtomicUsize,
}

impl Authority {
    fn grant(&self, action: Action) -> Result<SignedGrant> {
        let run = self.node.connection.lock().unwrap().run.clone().unwrap();
        let now = chrono::Utc::now().timestamp();
        SignedGrant::sign(
            Grant {
                id: Uuid::now_v7(),
                deployment: self.manifest.deployment_id,
                node: self.node.client.identity.node_id,
                media: self.node.files.media.media_id,
                boot: Uuid::parse_str(&run.boot_id)?,
                fence: Uuid::parse_str(&run.fence)?,
                cluster: self.manifest.cluster_id,
                takeover_epoch: 1,
                manifest_revision: 1,
                issued: now,
                expires: now + 30,
                action,
            },
            &self.key,
            &self.manifest,
        )
    }

    fn target(&self) -> Result<Target> {
        let state = &self.node.client.identity;
        let run = self.node.connection.lock().unwrap().run.clone().unwrap();
        Ok(Target {
            device: Device {
                node_id: state.node_id,
                media_id: self.node.files.media.media_id,
                cluster_id: Some(self.manifest.cluster_id),
                observed_at: chrono::Utc::now().timestamp_micros(),
                announcement: Announcement {
                    media: self.node.files.media.clone(),
                    topology: self
                        .manifest
                        .members
                        .iter()
                        .find(|member| member.node_id == state.node_id)
                        .unwrap()
                        .topology
                        .clone(),
                    capacity: self.node.files.capacity(self.node.boot)?,
                    load: Some(self.node.write_load()),
                    manifest: Some(self.manifest.clone()),
                    pending_manifest: None,
                    history_after: 1,
                },
            },
            run: Some(Run {
                boot: Uuid::parse_str(&run.boot_id)?,
                fence: Uuid::parse_str(&run.fence)?,
            }),
            expires: chrono::Utc::now().timestamp_micros() + 90_000_000,
            credential: Some(codec::protect(
                state.credential.as_bytes(),
                &self.key,
                crate::cluster::credential_binding(state.node_id).as_bytes(),
            )?),
            generation: 1,
        })
    }
}

#[async_trait::async_trait]
impl Renewal for Authority {
    async fn renew(&self, signed: &SignedGrant) -> Result<SignedGrant> {
        self.renewals.fetch_add(1, Ordering::Relaxed);
        let mut grant = signed.grant.clone();
        grant.issued = chrono::Utc::now().timestamp();
        grant.expires = grant.issued + 30;
        SignedGrant::sign(grant, &self.key, &self.manifest)
    }
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_STORAGE_TLS and its CA in SSL_CERT_FILE"]
async fn native_forwarding_binds_destinations_and_cancels_both_writers() -> Result<()> {
    let deployment = Uuid::now_v7();
    let mut nodes = Vec::new();
    for credential in ['c', 'd'] {
        nodes.push(
            TestNode::new(
                NodeState {
                    format: 1,
                    node_id: Uuid::now_v7(),
                    deployment_id: deployment,
                    role: "storage".into(),
                    credential_generation: 1,
                    credential: credential.to_string().repeat(64),
                },
                Run {
                    boot: Uuid::now_v7(),
                    fence: Uuid::now_v7(),
                },
            )
            .await?,
        );
    }
    let data = Bytes::from(vec![73; UNIT * 3 + 17]);
    let first = header(&nodes[0].node.files, data.len() as u64);
    let key = Key::new("aes-256-gcm", [78; 32])?;
    let manifest = Manifest {
        format_version: 1,
        record_id: Uuid::now_v7(),
        cluster_id: first.cluster,
        deployment_id: deployment,
        publication_key: public_key(&key, first.cluster)?,
        takeover_epoch: 1,
        revision: 1,
        previous: None,
        members: nodes
            .iter()
            .map(|node| {
                Ok(Member {
                    media_id: node.node.files.media.media_id,
                    node_id: node.state.node_id,
                    topology: node.announcement()?.topology,
                    state: "available".into(),
                })
            })
            .collect::<Result<_>>()?,
        groups: vec![Group {
            id: first.group,
            layout: first.layout.clone(),
            members: nodes
                .iter()
                .map(|node| node.node.files.media.media_id)
                .collect(),
        }],
    };
    for node in &nodes {
        node.apply(Registration {
            manifest: Some(manifest.clone()),
            history: vec![manifest.clone()],
        })
        .await?;
    }
    let authorities = nodes
        .iter()
        .map(|node| Authority {
            key: key.clone(),
            manifest: manifest.clone(),
            node: node.node.clone(),
            renewals: AtomicUsize::new(0),
        })
        .collect::<Vec<_>>();
    let targets = |operation| -> Result<Vec<Destination>> {
        nodes
            .iter()
            .zip(&authorities)
            .enumerate()
            .map(|(index, (node, authority))| {
                let mut header = first.clone();
                header.operation = operation;
                header.fragment = Uuid::now_v7();
                header.copy = index as u16;
                header.media = node.node.files.media.media_id;
                Ok(Destination {
                    url: node.url.clone(),
                    authorization: authority.grant(Action::Write { header })?,
                })
            })
            .collect()
    };
    let pool = Arc::new(Pool::new(Duration::from_secs(60)));
    let planned = targets(Uuid::now_v7())?;
    let bytes = data.clone();
    let body = async_stream::try_stream! {
        yield bytes.slice(..UNIT);
        tokio::time::sleep(Duration::from_secs(22)).await;
        yield bytes.slice(UNIT..);
    }
    .boxed();
    let completed = pool.forward(planned.clone(), body, &authorities[0]).await?;
    assert_eq!(completed.len(), 2);
    assert!(authorities[0].renewals.load(Ordering::Relaxed) >= 2);
    for (authority, completion) in authorities.iter().zip(&completed) {
        completion.verify(&key, &manifest, &authority.target()?)?;
        let receipt = &completion.receipt.receipt;
        let read = Destination {
            url: manifest
                .members
                .iter()
                .find(|m| m.media_id == receipt.header.media)
                .unwrap()
                .topology
                .url
                .clone(),
            authorization: authority.grant(Action::Read {
                receipt: receipt.clone(),
                offset: 0,
                length: data.len() as u64,
            })?,
        };
        let authority = Arc::new(Authority {
            key: key.clone(),
            manifest: manifest.clone(),
            node: authority.node.clone(),
            renewals: AtomicUsize::new(0),
        });
        let output = pool
            .read(read, authority)
            .await?
            .try_fold(Vec::new(), |mut output, bytes| async move {
                output.extend_from_slice(&bytes);
                Ok(output)
            })
            .await?;
        assert_eq!(output, data);
    }
    let mut wrong = targets(Uuid::now_v7())?;
    wrong[1].url = "https://127.0.0.1:1".into();
    assert!(
        pool.forward(
            wrong,
            futures_util::stream::iter([Ok(data.clone())]).boxed(),
            &authorities[0]
        )
        .await
        .is_err()
    );
    let mut wrong = targets(Uuid::now_v7())?;
    let Action::Write { mut header } = wrong[1].authorization.grant.action.clone() else {
        unreachable!()
    };
    header.payload = Uuid::now_v7();
    wrong[1].authorization = authorities[1].grant(Action::Write { header })?;
    assert!(
        pool.forward(
            wrong,
            futures_util::stream::iter([Ok(data.clone())]).boxed(),
            &authorities[0]
        )
        .await
        .is_err()
    );
    nodes[0]
        .node
        .files
        .manifest
        .write()
        .unwrap()
        .as_mut()
        .unwrap()
        .members[1]
        .topology
        .region = Some("other".into());
    assert!(
        pool.forward(
            targets(Uuid::now_v7())?,
            futures_util::stream::iter([Ok(data.clone())]).boxed(),
            &authorities[0]
        )
        .await
        .is_err()
    );
    *nodes[0].node.files.manifest.write().unwrap() = Some(manifest.clone());
    let pending = targets(Uuid::now_v7())?;
    let authority = Arc::new(Authority {
        key,
        manifest: manifest.clone(),
        node: nodes[0].node.clone(),
        renewals: AtomicUsize::new(0),
    });
    let request = tokio::spawn({
        let pool = pool.clone();
        async move {
            let body = futures_util::stream::once(async { Ok(data.slice(..UNIT)) })
                .chain(futures_util::stream::pending())
                .boxed();
            pool.forward(pending, body, authority.as_ref()).await
        }
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while nodes
            .iter()
            .any(|node| node.node.active_writes.lock().unwrap().is_empty())
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    request.abort();
    assert!(matches!(request.await, Err(error) if error.is_cancelled()));
    tokio::time::timeout(Duration::from_secs(3), async {
        while nodes
            .iter()
            .any(|node| node.node.writes.available_permits() != 1)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_STORAGE_TLS and its CA in SSL_CERT_FILE"]
async fn native_transport_checks_receipts_streams_and_cancellation() -> Result<()> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let (_directory, instance, budget, files) = fixture("16MiB").await?;
    let mut planned = header(&files, (UNIT * 3 + 17) as u64);
    let key = Key::new("aes-256-gcm", [71; 32])?;
    let state = NodeState {
        format: 1,
        node_id: Uuid::now_v7(),
        deployment_id: Uuid::now_v7(),
        role: "storage".into(),
        credential_generation: 1,
        credential: "a".repeat(64),
    };
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("https://{}", listener.local_addr()?);
    let topology = Topology {
        url: url.clone(),
        region: Some("a".into()),
        storage_group: "one".into(),
        purpose: "primary".into(),
    };
    let second = Uuid::now_v7();
    let manifest = Manifest {
        format_version: 1,
        record_id: Uuid::now_v7(),
        cluster_id: planned.cluster,
        deployment_id: state.deployment_id,
        publication_key: public_key(&key, planned.cluster)?,
        takeover_epoch: 1,
        revision: 1,
        previous: None,
        members: vec![
            Member {
                media_id: planned.media,
                node_id: state.node_id,
                topology: topology.clone(),
                state: "available".into(),
            },
            Member {
                media_id: second,
                node_id: Uuid::now_v7(),
                topology: Topology {
                    storage_group: "two".into(),
                    ..topology
                },
                state: "available".into(),
            },
        ],
        groups: vec![Group {
            id: planned.group,
            layout: planned.layout.clone(),
            members: vec![planned.media, second],
        }],
    };
    files
        .apply(
            Registration {
                manifest: Some(manifest.clone()),
                history: vec![manifest.clone()],
            },
            state.deployment_id,
        )
        .await?;
    let run = wire::Run {
        boot_id: Uuid::now_v7().to_string(),
        fence: Uuid::now_v7().to_string(),
    };
    let node = Arc::new(Node {
        config: NodeConfig {
            instance,
            configured_fields: Default::default(),
        },
        writes: Arc::new(Semaphore::new(1)),
        active_writes: Mutex::new(vec![]),
        backup_reads: Default::default(),
        budget,
        client: Client::new("https://localhost:1", state)?,
        files,
        boot: Uuid::parse_str(&run.boot_id)?,
        started: chrono::Utc::now(),
        connection: Mutex::new(Connection {
            phase: "registered",
            run: Some(run),
            deadline: Instant::now() + Duration::from_secs(180),
            mode: Some("single_control".into()),
            mode_revision: 1,
            maintenance: false,
            error: None,
            peer: None,
            control_id: None,
            epoch: 1,
        }),
    });
    let reads = Arc::new(Semaphore::new(1));
    let service = Service {
        node: node.clone(),
        requests: Arc::new(Semaphore::new(2)),
        reads: reads.clone(),
        forwarding: Pool::new(Duration::from_secs(60)),
    };
    let incoming = async_stream::stream! {
        loop { yield listener.accept().await.map(|(socket, _)| socket); }
    };
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let tls = std::path::PathBuf::from(std::env::var("MOKYU_TEST_STORAGE_TLS")?);
    let identity = Identity::from_pem(
        tokio::fs::read(tls.join("cert.pem")).await?,
        tokio::fs::read(tls.join("key.pem")).await?,
    );
    let task = tokio::spawn(
        Server::builder()
            .tls_config(ServerTlsConfig::new().identity(identity))?
            .add_service(StorageNodeServer::new(service))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stopped.await;
            }),
    );
    let authority = Arc::new(Authority {
        key,
        manifest,
        node: node.clone(),
        renewals: AtomicUsize::new(0),
    });
    let pool = Pool::new(Duration::from_secs(60));
    let bytes = Bytes::from(
        (0..planned.total)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>(),
    );
    let signed = authority.grant(Action::Write {
        header: planned.clone(),
    })?;
    let destination = Destination {
        url: url.clone(),
        authorization: signed.clone(),
    };
    let completion = pool
        .write(
            destination.clone(),
            futures_util::stream::iter([Ok(bytes.clone())]).boxed(),
            authority.as_ref(),
        )
        .await?;
    let receipt = completion.receipt.clone();
    let mut adopted = authority.manifest.clone();
    adopted.takeover_epoch += 1;
    adopted.revision += 1;
    let mut historical = signed.grant.clone();
    historical.takeover_epoch = adopted.takeover_epoch;
    historical.manifest_revision = adopted.revision as u64;
    assert!(SignedGrant::sign(historical.clone(), &authority.key, &adopted).is_err());
    for action in [
        Action::Read {
            receipt: receipt.receipt.clone(),
            offset: 0,
            length: planned.total,
        },
        Action::Stat {
            header: receipt.receipt.header.clone(),
        },
        Action::Remove {
            receipt: receipt.receipt.clone(),
        },
    ] {
        historical.action = action;
        SignedGrant::sign(historical.clone(), &authority.key, &adopted)?.verify(&adopted)?;
    }
    historical.takeover_epoch = 1;
    assert!(SignedGrant::sign(historical, &authority.key, &adopted).is_err());
    let mut target = authority.target()?;
    completion.verify(&authority.key, &authority.manifest, &target)?;
    target.run.as_mut().unwrap().boot = Uuid::now_v7();
    assert!(
        completion
            .verify(&authority.key, &authority.manifest, &target)
            .is_err()
    );
    target = authority.target()?;
    target.run.as_mut().unwrap().fence = Uuid::now_v7();
    assert!(
        completion
            .verify(&authority.key, &authority.manifest, &target)
            .is_err()
    );
    target = authority.target()?;
    target.credential = None;
    assert!(
        completion
            .verify(&authority.key, &authority.manifest, &target)
            .is_err()
    );
    target = authority.target()?;
    target.credential = Some(codec::protect(
        b"another-credential",
        &authority.key,
        crate::cluster::credential_binding(target.device.node_id).as_bytes(),
    )?);
    assert!(
        completion
            .verify(&authority.key, &authority.manifest, &target)
            .is_err()
    );
    let mut changed = completion.clone();
    changed.authorization.grant.issued -= 60;
    changed.authorization.grant.expires -= 60;
    changed.authorization = SignedGrant::sign(
        changed.authorization.grant,
        &authority.key,
        &authority.manifest,
    )?;
    assert!(
        changed
            .verify(&authority.key, &authority.manifest, &authority.target()?)
            .is_err()
    );
    changed = completion.clone();
    if let Action::Write { header } = &mut changed.authorization.grant.action {
        header.fragment = Uuid::now_v7();
    }
    changed.authorization = SignedGrant::sign(
        changed.authorization.grant,
        &authority.key,
        &authority.manifest,
    )?;
    assert!(
        changed
            .verify(&authority.key, &authority.manifest, &authority.target()?)
            .is_err()
    );
    let slow_bytes = bytes.clone();
    let slow = async_stream::stream! {
        yield Ok(slow_bytes.slice(..31));
        tokio::time::sleep(Duration::from_secs(32)).await;
        yield Ok(slow_bytes.slice(31..));
    };
    let renewed = pool
        .write(destination, slow.boxed(), authority.as_ref())
        .await?;
    assert!(renewed.authorization.grant.expires > signed.grant.expires);
    renewed.verify(&authority.key, &authority.manifest, &authority.target()?)?;
    assert_eq!(renewed.receipt.receipt, receipt.receipt);
    let stat = Destination {
        url: url.clone(),
        authorization: authority.grant(Action::Stat {
            header: planned.clone(),
        })?,
    };
    assert_eq!(
        pool.state(&stat).await?,
        FragmentState::Complete(receipt.receipt.clone())
    );
    let read = Destination {
        url: url.clone(),
        authorization: authority.grant(Action::Read {
            receipt: receipt.receipt.clone(),
            offset: 31,
            length: bytes.len() as u64 - 62,
        })?,
    };
    let got = pool
        .read(read, authority.clone())
        .await?
        .try_fold(Vec::new(), |mut result, bytes| async move {
            result.extend_from_slice(&bytes);
            Ok(result)
        })
        .await?;
    assert_eq!(got, bytes[31..bytes.len() - 31]);

    // A stopped upload must release its slot and leave only a resumable candidate.
    planned.fragment = Uuid::now_v7();
    let next = Destination {
        url: url.clone(),
        authorization: authority.grant(Action::Write {
            header: planned.clone(),
        })?,
    };
    {
        let held = bytes.clone();
        let body =
            async_stream::stream! { yield Ok(held); futures_util::future::pending::<()>().await; };
        let writing = pool.write(next.clone(), body.boxed(), authority.as_ref());
        tokio::pin!(writing);
        assert!(
            tokio::time::timeout(Duration::from_secs(21), &mut writing)
                .await
                .is_err()
        );
        assert_eq!(node.writes.available_permits(), 0);
        assert!(authority.renewals.load(Ordering::Relaxed) > 0);
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        while node.writes.available_permits() != 1 || !node.active_writes.lock().unwrap().is_empty()
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    let stat = Destination {
        url: url.clone(),
        authorization: authority.grant(Action::Stat {
            header: planned.clone(),
        })?,
    };
    assert!(matches!(
        pool.state(&stat).await?,
        FragmentState::Pending { complete: None, .. }
    ));
    pool.write(
        next,
        futures_util::stream::iter([Ok(bytes.clone())]).boxed(),
        authority.as_ref(),
    )
    .await?;
    assert!(matches!(
        pool.state(&stat).await?,
        FragmentState::Complete(_)
    ));

    // A new run cannot reuse the old stream authorization, even with the same node credential.
    node.connection.lock().unwrap().run.as_mut().unwrap().fence = Uuid::now_v7().to_string();
    assert!(pool.state(&stat).await.is_err());
    let _ = stop.send(());
    task.abort();
    let _ = task.await;
    Ok(())
}
