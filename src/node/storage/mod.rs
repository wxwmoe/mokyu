mod files;
mod forward;
mod fragments;
mod inventory;
mod recovery;
mod server;
mod transfer;

#[cfg(test)]
pub(crate) mod recovery_tests;

#[cfg(test)]
pub(crate) use server::tests::TestNode;

use crate::{
    config::{Budget, node::NodeConfig},
    model::{
        node::NodeReport,
        storage_cluster::{Announcement, Registration, Topology, WriteLoad},
    },
    rpc::{
        client::{Client, Session},
        wire,
    },
};
use anyhow::{Context, Result, ensure};
use axum::{Json, Router, extract::State};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{Semaphore, watch},
    time::Instant,
};
use tonic::{Code, Status};
use uuid::Uuid;

struct Connection {
    phase: &'static str,
    run: Option<wire::Run>,
    deadline: Instant,
    mode: Option<String>,
    mode_revision: u64,
    maintenance: bool,
    draining: bool,
    handoff_revision: u64,
    storage_closed: bool,
    error: Option<String>,
    peer: Option<std::net::Ipv4Addr>,
    control_id: Option<Uuid>,
    epoch: u64,
}

pub(super) struct Node {
    config: NodeConfig,
    budget: Budget,
    client: Client,
    files: files::Files,
    boot: Uuid,
    started: chrono::DateTime<chrono::Utc>,
    connection: Mutex<Connection>,
    writes: Arc<Semaphore>,
    active_writes: Mutex<Vec<Uuid>>,
    streams: std::sync::atomic::AtomicUsize,
    backup_reads: std::sync::atomic::AtomicU64,
}

impl Node {
    fn write_load(&self) -> WriteLoad {
        let active = self.active_writes.lock().unwrap().clone();
        let slots = self.budget.storage_upload_slots as u32;
        WriteLoad {
            slots,
            admitting: slots
                .saturating_sub(self.writes.available_permits() as u32)
                .saturating_sub(active.len() as u32),
            active,
        }
    }

    fn ready(&self) -> bool {
        let state = self.connection.lock().unwrap();
        state.phase == "registered" && state.deadline > Instant::now()
    }

    fn status(&self) -> Result<Value> {
        let state = self.connection.lock().unwrap();
        let manifest = self.files.manifest.read().unwrap();
        let ready = state.phase == "registered" && state.deadline > Instant::now();
        let pending = self.files.pending.read().unwrap();
        let foreign = manifest
            .iter()
            .chain(pending.iter())
            .any(|head| head.deployment_id != self.client.identity.deployment_id);
        Ok(json!({
            "version": env!("CARGO_PKG_VERSION"), "role": "storage", "ready": ready,
            "phase": if state.deadline <= Instant::now() && state.phase == "registered" { "recovering" } else { state.phase },
            "deployment_mode": state.mode, "deployment_mode_revision": state.mode_revision,
            "node_id": self.client.identity.node_id, "media_id": self.files.media.media_id,
            "boot_id": self.boot, "started_at": self.started, "resources": self.budget,
            "region": self.config.instance.region, "storage_url": self.config.instance.storage_url,
            "storage_group": self.config.instance.storage_group, "storage_purpose": self.config.instance.storage_purpose.as_deref().unwrap_or("primary"),
            "backup_reads": self.backup_reads.load(std::sync::atomic::Ordering::Relaxed),
            "data_directory": self.files.capacity(self.boot)?, "write_load": self.write_load(), "maintenance": state.maintenance, "draining": state.draining,
            "handoff_revision":state.handoff_revision,"handoff_reconciled":true,"storage_admission_closed":state.storage_closed,"storage_streams":self.streams.load(std::sync::atomic::Ordering::Acquire),
            "work": if foreign { "recovery" } else if ready { "normal" } else { "starting" },
            "cluster_id": manifest.as_ref().map(|m| m.cluster_id), "cluster_revision": manifest.as_ref().map(|m| m.revision),
            "recovery_pending": foreign, "control": { "url": self.config.instance.control_url, "peer": state.peer, "id": state.control_id, "epoch": state.epoch, "last_error": state.error },
            "pending_cluster_revision": pending.as_ref().map(|head| head.revision),
            "rpc": self.client.statistics(), "db_pool_size": 0,
        }))
    }

    async fn announce(&self, run: wire::Run) -> Result<(), Status> {
        let instance = &self.config.instance;
        let input = Announcement {
            media: self.files.media.clone(),
            capacity: self
                .files
                .capacity(self.boot)
                .map_err(|_| Status::internal("cannot inspect storage capacity"))?,
            load: Some(self.write_load()),
            topology: Topology {
                url: instance.storage_url.clone().unwrap(),
                region: instance.region.clone(),
                storage_group: instance.storage_group.clone().unwrap(),
                purpose: instance
                    .storage_purpose
                    .clone()
                    .unwrap_or_else(|| "primary".into()),
            },
            manifest: self.files.manifest.read().unwrap().clone(),
            pending_manifest: self.files.pending.read().unwrap().clone(),
            history_after: self
                .files
                .history
                .lock()
                .unwrap()
                .as_ref()
                .map_or(0, |record| record.revision),
        };
        let reply = self
            .client
            .announce_storage(wire::StorageAnnouncement {
                run: Some(run),
                announcement_json: serde_json::to_vec(&input)
                    .map_err(|_| Status::internal("encode storage announcement"))?,
            })
            .await?;
        let reply: Registration = serde_json::from_slice(&reply.registration_json)
            .map_err(|_| Status::data_loss("invalid storage registration"))?;
        self.files
            .apply(reply, self.client.identity.deployment_id)
            .await
            .map_err(|error| {
                tracing::warn!(%error, "storage membership could not be persisted");
                Status::failed_precondition("cannot persist storage membership")
            })
    }

    async fn connected(
        &self,
        session: &mut Session,
        report_sequence: &mut i64,
    ) -> Result<(), Status> {
        // Storage has no logical object cache, but must acknowledge invalidation notices.
        session.on_notice = Some(Arc::new(|_, _| {}));
        let run = session
            .welcome
            .run
            .clone()
            .ok_or_else(|| Status::data_loss("control omitted storage run"))?;
        {
            let mut current = self.connection.lock().unwrap();
            current.run = Some(run.clone());
            current.deadline = session.valid_until;
            current.peer = Some(session.peer);
            current.control_id = Uuid::parse_str(&session.welcome.control_id).ok();
            current.epoch = session.welcome.epoch;
        }
        let mut sequence = 0u64;
        loop {
            sequence = sequence
                .checked_add(1)
                .ok_or_else(|| Status::data_loss("storage heartbeat sequence exhausted"))?;
            let state = session.pulse(sequence, false).await?;
            if !matches!(state.deployment_mode.as_str(), "single_control" | "cluster") {
                return Err(Status::permission_denied(
                    "storage requires a clustered deployment",
                ));
            }
            session.during(self.announce(run.clone())).await?;
            {
                let mut current = self.connection.lock().unwrap();
                current.deadline = session.valid_until;
                current.mode = Some(state.deployment_mode);
                current.mode_revision = state.mode_revision;
                current.maintenance = state.maintenance;
                current.draining = state.draining;
                current.handoff_revision = state.handoff_revision;
                current.storage_closed = state.storage_closed;
                current.phase = "registered";
                current.error = None;
            }
            *report_sequence += 1;
            let report = NodeReport {
                boot_id: self.boot,
                sequence: *report_sequence,
                sampled_at: chrono::Utc::now(),
                metadata: Some(
                    self.metadata()
                        .map_err(|_| Status::internal("cannot report storage configuration"))?,
                ),
                status: self
                    .status()
                    .map_err(|_| Status::internal("cannot report storage status"))?,
            };
            session
                .during(
                    self.client.report(wire::NodeReportRequest {
                        run: Some(run.clone()),
                        report_json: serde_json::to_vec(&report)
                            .map_err(|_| Status::internal("encode storage report"))?,
                        sample_interval_seconds: 0,
                    }),
                )
                .await?;
            session
                .during(async {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    Ok(())
                })
                .await?;
        }
    }

    async fn drive(&self) -> Result<()> {
        let mut connection = Uuid::new_v4();
        let mut epoch = 0;
        let mut report_sequence = 0;
        loop {
            self.connection.lock().unwrap().phase = "connecting";
            let outcome = match self.client.open(self.boot, connection, epoch).await {
                Ok(mut session) => {
                    epoch = session.welcome.epoch;
                    connection = Uuid::new_v4();
                    self.connected(&mut session, &mut report_sequence).await
                }
                Err(error) => Err(error),
            };
            let error = outcome.unwrap_err();
            let permanent = matches!(
                error.code(),
                Code::Unauthenticated
                    | Code::PermissionDenied
                    | Code::InvalidArgument
                    | Code::Unimplemented
                    | Code::DataLoss
            );
            {
                let mut state = self.connection.lock().unwrap();
                state.phase = if permanent { "fenced" } else { "recovering" };
                state.error = Some(format!("{:?}", error.code()));
            }
            if permanent {
                anyhow::bail!("storage registration rejected: {:?}", error.code());
            }
            tracing::warn!(code=?error.code(), reason=error.message(), "storage control connection recovering");
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    fn metadata(&self) -> Result<Value> {
        let mut instance = crate::control::service::instance_config(&self.config.instance)?;
        instance.as_object_mut().unwrap().retain(|name, _| {
            matches!(
                name.as_str(),
                "name"
                    | "region"
                    | "control_url"
                    | "listen_control"
                    | "storage_url"
                    | "storage_group"
                    | "storage_purpose"
                    | "listen_storage"
                    | "storage_transport"
                    | "storage_tls_cert"
                    | "storage_tls_key"
                    | "data_path"
                    | "data_limit"
                    | "min_free_space"
                    | "request_connections"
                    | "cpu_jobs"
                    | "stream_memory_budget"
                    | "storage_read_slots"
                    | "storage_upload_slots"
                    | "storage_control_slots"
            )
        });
        let fields: Vec<_> = instance
            .as_object()
            .unwrap()
            .iter()
            .map(|(name, value)| {
                let key = format!("instance.{name}");
                let configured = self.config.configured_fields.contains(&key);
                crate::model::runtime::ConfigField {
                    key,
                    value: value
                        .as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| value.to_string()),
                    value_state: None,
                    origin: "config".into(),
                    value_mode: if configured { "set" } else { "default" }.into(),
                    restart: true,
                }
            })
            .collect();
        Ok(
            json!({"format": crate::model::node::REPORT_FORMAT, "fields": fields, "loaded": {}, "revisions": {}}),
        )
    }
}

pub(crate) async fn run(config: NodeConfig, budget: Budget) -> Result<()> {
    let lock = Arc::new(crate::runtime::lock_directory(&config.instance.data_path)?);
    let identity =
        crate::cluster::identity::read_state(&config.instance.data_path.join("node.json"))
            .await?
            .context("node.json is required; provision storage on control first")?;
    ensure!(
        identity.role == "storage",
        "node identity does not match storage role"
    );
    let files = files::Files::open(&config.instance, &budget, lock).await?;
    let client = Client::new(config.instance.control_url.as_deref().unwrap(), identity)?
        .with_region(config.instance.region.clone());
    let node = Arc::new(Node {
        config,
        writes: Arc::new(Semaphore::new(budget.storage_upload_slots)),
        active_writes: Mutex::new(Vec::new()),
        backup_reads: Default::default(),
        streams: Default::default(),
        budget,
        client,
        files,
        boot: Uuid::new_v4(),
        started: chrono::Utc::now(),
        connection: Mutex::new(Connection {
            phase: "connecting",
            run: None,
            deadline: Instant::now(),
            mode: None,
            mode_revision: 0,
            maintenance: true,
            draining: false,
            handoff_revision: 0,
            storage_closed: false,
            error: None,
            peer: None,
            control_id: None,
            epoch: 0,
        }),
    });
    let local = TcpListener::bind(&node.config.instance.listen_control)
        .await
        .context("storage status listener")?;
    let (stopping, _) = watch::channel(false);
    let router = Router::new()
        .route("/status", axum::routing::get(status))
        .route("/api/v1/cli", axum::routing::post(command))
        .with_state(node.clone());
    let services = crate::runtime::listeners::serve_http(
        local,
        router,
        Arc::new(Semaphore::new(4)),
        4,
        stopping.subscribe(),
    );
    let physical = server::serve(node.clone(), stopping.subscribe());
    let driver = async {
        if let Err(error) = node.drive().await {
            tracing::warn!(%error, "storage remains available for local diagnostics");
        }
        std::future::pending::<()>().await;
    };
    tokio::pin!(services, physical, driver);
    let outcome = tokio::select! {
        value = &mut services => value,
        value = &mut physical => value,
        _ = &mut driver => unreachable!(),
        _ = crate::runtime::roles::shutdown_signal() => Ok(()),
    };
    stopping.send_replace(true);
    let run = {
        let mut state = node.connection.lock().unwrap();
        state.phase = "stopped";
        state.run.clone()
    };
    if let Some(run) = run {
        let _ = tokio::time::timeout(Duration::from_secs(5), node.client.close(run)).await;
    }
    outcome
}

async fn status(State(node): State<Arc<Node>>) -> Result<Json<Value>, crate::http::HttpError> {
    Ok(Json(node.status()?))
}

async fn command(
    State(node): State<Arc<Node>>,
    Json(command): Json<crate::cli::Command>,
) -> Result<Json<Value>, crate::http::HttpError> {
    let crate::cli::Command::Status { ready } = command else {
        return Err(anyhow::anyhow!(
            "only status is available on storage; send management commands to control"
        )
        .into());
    };
    if ready && !node.ready() {
        return Err(anyhow::anyhow!("storage is not registered").into());
    }
    Ok(Json(json!({"ok": true, "result": node.status()?})))
}
