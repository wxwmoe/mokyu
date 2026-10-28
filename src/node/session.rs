use super::{Mode, NodeContext, Phase, RunLease, Snapshot, settings::Settings};
use crate::rpc::{
    client::{Session, lease_deadline},
    wire,
};
use anyhow::{Result, ensure};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::watch,
    time::{Instant, MissedTickBehavior},
};
use tonic::{Code, Status};
use uuid::Uuid;

struct Running(Arc<NodeContext>);
impl Drop for Running {
    fn drop(&mut self) {
        let phase = self.0.current.borrow().phase;
        self.0.invalidate(if phase == Phase::Fenced {
            phase
        } else {
            Phase::Stopped
        });
    }
}

impl NodeContext {
    pub(crate) async fn run(self: Arc<Self>) -> Result<()> {
        ensure!(
            !self.started.swap(true, Ordering::AcqRel),
            "node session is already running"
        );
        let _running = Running(self.clone());
        self.observations.start(&self);
        let mut mode = self.mode.subscribe();
        let mut connection = Uuid::new_v4();
        let mut epoch = 0;
        let mut failures = 0u32;
        loop {
            if *mode.borrow() == Mode::Stopped {
                return Ok(());
            }
            // Fence old cached authorization before activation can retire its target generation.
            self.current
                .send_modify(|state| state.phase = Phase::Recovering);
            if let Some(reads) = self.foreground.lock().unwrap().upgrade() {
                reads.notify_public(true, &[]);
            }
            let result = tokio::select! {
                result = self.client.open(self.boot, connection, epoch) => result,
                _ = stopped(&mut mode) => return Ok(()),
            };
            let result = match result {
                Ok(mut session) => {
                    epoch = session.welcome.epoch;
                    // Retain a connection ID until Welcome resolves an uncertain takeover.
                    connection = Uuid::new_v4();
                    let result = tokio::select! {
                        result = self.connected(&mut session, &mut failures) => result,
                        _ = stopped(&mut mode) => return Ok(()),
                    };
                    result
                }
                Err(error) => Err(error),
            };
            let error = match result {
                Ok(()) => continue,
                Err(error) => error,
            };
            self.current
                .send_modify(|state| state.last_error = Some(format!("{:?}", error.code())));
            if matches!(
                error.code(),
                Code::Unauthenticated
                    | Code::PermissionDenied
                    | Code::InvalidArgument
                    | Code::Unimplemented
                    | Code::DataLoss
            ) {
                self.invalidate(Phase::Fenced);
                anyhow::bail!("node control rejected this runtime: {:?}", error.code());
            }
            self.current.send_modify(|state| {
                state.phase = Phase::Recovering;
                if let Some(snapshot) = &state.snapshot {
                    if snapshot.lease.check().is_err() {
                        snapshot.lease.invalidate();
                    }
                }
            });
            // A connected stream gets one immediate recovery attempt before backing off.
            let delay = if failures == 0 {
                Duration::ZERO
            } else {
                retry_delay(failures)
            };
            failures = failures.saturating_add(1);
            tracing::warn!(code=?error.code(), delay_ms=delay.as_millis(), "node session recovering");
            tokio::select! {
                _ = tokio::time::sleep(delay) => {},
                _ = stopped(&mut mode) => return Ok(()),
            }
        }
    }

    async fn connected(
        self: &Arc<Self>,
        session: &mut Session,
        failures: &mut u32,
    ) -> Result<(), Status> {
        let node = Arc::downgrade(self);
        session.on_notice = Some(Arc::new(move |reset, changes| {
            if let Some(node) = node.upgrade() {
                if let Some(reads) = node.foreground.lock().unwrap().upgrade() {
                    reads.notify_public(reset, changes);
                }
                if reset
                    || changes.iter().any(|change| {
                        matches!(change, crate::model::notification::Change::Storage { .. })
                    })
                {
                    node.cache_epoch.fetch_add(1, Ordering::AcqRel);
                }
            }
        }));
        let run = session
            .welcome
            .run
            .clone()
            .ok_or_else(|| Status::data_loss("control omitted run"))?;
        let previous = self.current.borrow().snapshot.clone();
        let lease = match previous.as_ref().map(|snapshot| snapshot.lease.clone()) {
            Some(lease) if lease.run == run && lease.check().is_ok() => {
                lease.renew(session.valid_until)?;
                lease
            }
            previous => {
                if let Some(previous) = previous {
                    previous.invalidate();
                }
                Arc::new(RunLease {
                    run,
                    expires: Mutex::new(session.valid_until),
                    invalidated: AtomicBool::new(false),
                })
            }
        };
        let mut settings =
            previous.map_or_else(Settings::default, |snapshot| snapshot.settings.clone());
        self.current.send_modify(|state| {
            state.phase = Phase::Recovering;
            state.epoch = session.welcome.epoch;
            state.peer = Some(session.peer);
            state.control_id = Uuid::parse_str(&session.welcome.control_id).ok();
        });
        session
            .during(self.synchronize(&lease, &mut settings))
            .await?;
        *failures = 0;
        let mut sequence = 0u64;
        let mut renew_run = Instant::now();
        let mut mode = self.mode.subscribe();
        loop {
            let due = renew_run.min(self.resources.next(&lease));
            let mut force = false;
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(due) => {},
                _ = tokio::time::sleep_until(session.io_deadline()) => { session.service_io().await?; },
                notice = session.notified() => { notice?; },
                _ = mode.changed() => { force=true; },
                _ = self.resources.changed.notified() => {
                    session.during(async { tokio::time::sleep(Duration::from_millis(20)).await; Ok(()) }).await?;
                },
            }
            if *mode.borrow() == Mode::Stopped {
                return Ok(());
            }
            lease.check()?;
            if session.take_work() {
                self.native_work.notify_one();
                self.current.send_modify(|state| state.work_pending = true);
            }
            let hints = session.take_settings();
            let cached = settings.cached();
            if session.take_state_change()
                || hints.iter().any(|hint| {
                    !cached
                        .iter()
                        .any(|v| v.section == hint.section && v.revision >= hint.revision)
                })
            {
                session
                    .during(self.synchronize(&lease, &mut settings))
                    .await?;
            }
            let draining = *mode.borrow() == Mode::Draining;
            for _ in 0..32 {
                let batch = self.resources.batch(&lease);
                let extend = force || Instant::now() >= renew_run;
                if !extend && batch.renew.is_empty() && batch.release.is_empty() {
                    break;
                }
                sequence = sequence
                    .checked_add(1)
                    .ok_or_else(|| Status::data_loss("maintenance sequence exhausted"))?;
                let started = Instant::now();
                let reply = session
                    .maintain_resources(
                        sequence,
                        extend && draining,
                        batch.renew.clone(),
                        batch.release.clone(),
                        extend,
                    )
                    .await?;
                lease.renew(session.valid_until)?;
                self.resources
                    .acknowledge(&lease, &batch, started, &reply.grants)?;
                if extend {
                    renew_run = started
                        + (session.valid_until.saturating_duration_since(started) / 3)
                            // Leave headroom before the 30-second contact freshness threshold.
                            .min(Duration::from_secs(25));
                    force = false;
                }
                if let Some(state) = reply.state {
                    self.publish(&lease, &settings, &state);
                    if !settings.matches(&state).map_err(invalid_settings)? {
                        session
                            .during(self.synchronize(&lease, &mut settings))
                            .await?;
                    }
                }
                if extend {
                    let peer = session.peer;
                    if !session
                        .during(async { Ok(self.client.contains(peer).await) })
                        .await?
                    {
                        return Err(Status::unavailable("session origin left control DNS"));
                    }
                }
            }
        }
    }

    async fn synchronize(
        &self,
        lease: &Arc<RunLease>,
        settings: &mut Settings,
    ) -> Result<(), Status> {
        let started = Instant::now();
        let reply = self
            .client
            .sync(lease.run.clone(), settings.cached())
            .await?;
        // Sync confirms the existing expiry; it does not renew the server's run.
        lease.renew(lease_deadline(started, reply.valid_for_ms)?)?;
        let next = settings
            .update(&self.config.instance, &reply)
            .map_err(invalid_settings)?;
        let budget = crate::config::Budget::for_runtime(&self.config.instance, &next.runtime, &[])
            .map_err(invalid_settings)?;
        if self
            .budget
            .get()
            .is_some_and(|current| budget.slot_bytes > current.slot_bytes)
        {
            return Err(Status::data_loss(
                "runtime settings require a larger stream budget; restart required",
            ));
        }
        self.budget.get_or_init(|| budget);
        self.publish(
            lease,
            &next,
            reply
                .state
                .as_ref()
                .ok_or_else(|| Status::data_loss("control omitted node state"))?,
        );
        *settings = next;
        Ok(())
    }

    fn publish(&self, lease: &Arc<RunLease>, settings: &Settings, state: &wire::NodeState) {
        if state.native_pending {
            self.native_work.notify_one();
        }
        self.current.send_modify(|current| {
            current.maintenance = state.maintenance;
            current.work_pending = state.work_pending;
            current.pending_uploads = state.draining.then_some(state.pending_uploads);
            current.draining = state.draining || *self.mode.borrow() != Mode::Running;
            current.phase = if current.draining {
                Phase::Draining
            } else {
                Phase::Ready
            };
            current.snapshot = Some(Arc::new(Snapshot {
                lease: lease.clone(),
                settings: settings.clone(),
            }));
        });
    }
}

fn invalid_settings(_: anyhow::Error) -> Status {
    Status::data_loss("control returned invalid runtime settings")
}

fn retry_delay(failures: u32) -> Duration {
    let maximum = (1000u64 << failures.saturating_sub(1).min(5)).min(30_000);
    let random = u16::from_le_bytes(Uuid::new_v4().as_bytes()[..2].try_into().unwrap());
    Duration::from_millis(maximum / 2 + u64::from(random) % (maximum / 2 + 1))
}

async fn stopped(mode: &mut watch::Receiver<Mode>) {
    while *mode.borrow_and_update() != Mode::Stopped {
        if mode.changed().await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expired_or_invalidated_handles_cannot_be_resurrected() {
        let lease = RunLease {
            run: wire::Run {
                boot_id: Uuid::new_v4().to_string(),
                fence: Uuid::new_v4().to_string(),
            },
            expires: Mutex::new(Instant::now() + Duration::from_secs(10)),
            invalidated: AtomicBool::new(false),
        };
        assert!(lease.check().is_ok());
        lease
            .renew(Instant::now() + Duration::from_secs(20))
            .unwrap();
        *lease.expires.lock().unwrap() = Instant::now() - Duration::from_millis(1);
        assert!(lease.check().is_err());
        assert!(
            lease
                .renew(Instant::now() + Duration::from_secs(90))
                .is_err()
        );
        *lease.expires.lock().unwrap() = Instant::now() + Duration::from_secs(90);
        lease.invalidate();
        assert!(lease.check().is_err());
        assert!(
            lease
                .renew(Instant::now() + Duration::from_secs(90))
                .is_err()
        );
        for failures in [1, 2, 3, 6, 7, u32::MAX] {
            let delay = retry_delay(failures);
            assert!((Duration::from_millis(500)..=Duration::from_secs(30)).contains(&delay));
        }
    }
}
