mod body;
mod cache;
mod checks;
pub(crate) mod delivery;
mod files;
mod io;
mod observations;
pub(crate) mod reads;
mod report;
pub(crate) mod request;
pub(crate) mod resources;
mod session;
mod settings;
pub(crate) mod status;
pub(crate) mod work;
pub(crate) mod writes;

use crate::{
    config::{Budget, RuntimeSettings, node::NodeConfig},
    rpc::{client::Client, wire},
};
use anyhow::{Context, Result, ensure};
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicBool, Ordering},
};
use tokio::{sync::watch, time::Instant};
use tonic::Status;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Phase {
    Connecting,
    Ready,
    Recovering,
    Draining,
    Fenced,
    Stopped,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Running,
    Draining,
    Stopped,
}

pub(crate) struct RunLease {
    pub(crate) run: wire::Run,
    expires: Mutex<Instant>,
    invalidated: AtomicBool,
}

impl RunLease {
    pub(crate) fn deadline(&self) -> Instant {
        *self.expires.lock().unwrap()
    }
    pub(crate) fn check(&self) -> Result<(), Status> {
        if self.invalidated.load(Ordering::Acquire)
            || Instant::now() >= *self.expires.lock().unwrap()
        {
            Err(Status::failed_precondition(
                "node run has expired or stopped",
            ))
        } else {
            Ok(())
        }
    }

    fn invalidate(&self) {
        self.invalidated.store(true, Ordering::Release);
    }

    fn renew(&self, deadline: Instant) -> Result<(), Status> {
        let mut expires = self.expires.lock().unwrap();
        if self.invalidated.load(Ordering::Acquire) || Instant::now() >= *expires {
            return Err(Status::failed_precondition(
                "node run cannot be renewed after local expiry",
            ));
        }
        *expires = deadline;
        Ok(())
    }
}

pub(crate) struct Snapshot {
    pub(crate) lease: Arc<RunLease>,
    settings: settings::Settings,
}

impl Snapshot {
    pub(crate) fn runtime(&self) -> &RuntimeSettings {
        &self.settings.runtime
    }
}

#[derive(Clone)]
pub(crate) struct State {
    pub(crate) phase: Phase,
    pub(crate) epoch: u64,
    pub(crate) peer: Option<std::net::Ipv4Addr>,
    pub(crate) snapshot: Option<Arc<Snapshot>>,
    pub(crate) maintenance: bool,
    pub(crate) draining: bool,
    pub(crate) work_pending: bool,
    pub(crate) pending_uploads: Option<u64>,
    pub(crate) control_id: Option<Uuid>,
    pub(crate) last_error: Option<String>,
}

pub(crate) struct NodeContext {
    pub(crate) config: NodeConfig,
    pub(crate) budget: OnceLock<Budget>,
    body: Mutex<Option<body::Limits>>,
    uploads: Mutex<Option<writes::Limits>>,
    pub(crate) client: Client,
    pub(crate) resources: Arc<resources::Resources>,
    pub(crate) observations: observations::Reports,
    pub(crate) native_work: tokio::sync::Notify,
    pub(crate) statistics: crate::status::stats::Statistics,
    io: Arc<io::Io>,
    foreground: Mutex<std::sync::Weak<reads::Reads>>,
    pub(crate) serving: AtomicBool,
    boot: Uuid,
    current: watch::Sender<State>,
    mode: watch::Sender<Mode>,
    started: AtomicBool,
    _file_lock: std::fs::File,
}

impl NodeContext {
    pub(crate) async fn new(config: NodeConfig, role: &str) -> Result<Arc<Self>> {
        let lock = crate::runtime::lock_directory(&config.instance.data_path)?;
        let identity =
            crate::cluster::identity::read_state(&config.instance.data_path.join("node.json"))
                .await?
                .context("node.json is required; provision this node on control first")?;
        ensure!(
            identity.role == role && matches!(role, "data" | "worker"),
            "node identity does not match process role"
        );
        let client = Client::new(
            config
                .instance
                .control_url
                .as_deref()
                .context("control_url is required")?,
            identity,
        )?;
        Ok(Arc::new(Self {
            config,
            budget: OnceLock::new(),
            body: Mutex::new(None),
            uploads: Mutex::new(None),
            client,
            resources: Arc::new(resources::Resources::default()),
            observations: observations::Reports::new(),
            native_work: tokio::sync::Notify::new(),
            statistics: crate::status::stats::Statistics::default(),
            io: Arc::new(io::Io::default()),
            foreground: Mutex::default(),
            serving: AtomicBool::new(false),
            boot: Uuid::new_v4(),
            current: watch::channel(State {
                phase: Phase::Connecting,
                epoch: 0,
                peer: None,
                snapshot: None,
                maintenance: false,
                draining: false,
                work_pending: false,
                pending_uploads: None,
                control_id: None,
                last_error: None,
            })
            .0,
            mode: watch::channel(Mode::Running).0,
            started: AtomicBool::new(false),
            _file_lock: lock,
        }))
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<State> {
        self.current.subscribe()
    }

    pub(crate) fn snapshot(&self) -> Result<Arc<Snapshot>, Status> {
        let state = self.current.borrow();
        if *self.mode.borrow() != Mode::Running || state.draining || state.maintenance {
            return Err(Status::unavailable("node is draining or in maintenance"));
        }
        let snapshot = state
            .snapshot
            .clone()
            .ok_or_else(|| Status::unavailable("node is synchronizing"))?;
        snapshot.lease.check()?;
        Ok(snapshot)
    }

    pub(crate) fn background_snapshot(&self) -> Result<Arc<Snapshot>, Status> {
        if *self.mode.borrow() == Mode::Stopped {
            return Err(Status::unavailable("node is stopped"));
        }
        let snapshot = self
            .current
            .borrow()
            .snapshot
            .clone()
            .ok_or_else(|| Status::unavailable("node is synchronizing"))?;
        snapshot.lease.check()?;
        Ok(snapshot)
    }

    pub(crate) fn refresh(&self) {
        self.resources.changed.notify_one();
    }

    pub(crate) fn drain(&self) {
        self.mode.send_if_modified(|mode| {
            if *mode == Mode::Running {
                *mode = Mode::Draining;
                true
            } else {
                false
            }
        });
    }

    pub(crate) async fn close(&self) -> Result<(), Status> {
        let run = self
            .current
            .borrow()
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.lease.run.clone());
        self.stop();
        if let Some(run) = run {
            self.client.close(run).await?;
        }
        Ok(())
    }

    pub(crate) fn stop(&self) {
        self.mode.send_replace(Mode::Stopped);
        self.invalidate(Phase::Stopped);
    }

    fn invalidate(&self, phase: Phase) {
        self.current.send_modify(|state| {
            if let Some(snapshot) = &state.snapshot {
                snapshot.lease.invalidate();
            }
            state.phase = phase;
        });
    }
}
