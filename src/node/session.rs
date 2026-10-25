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
            if matches!(
                error.code(),
                Code::Unauthenticated
                    | Code::PermissionDenied
                    | Code::InvalidArgument
                    | Code::Unimplemented
                    | Code::Internal
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

    async fn connected(&self, session: &mut Session, failures: &mut u32) -> Result<(), Status> {
        let run = session
            .welcome
            .run
            .clone()
            .ok_or_else(|| Status::internal("control omitted run"))?;
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
        });
        self.synchronize(&lease, &mut settings).await?;
        *failures = 0;
        let mut sequence = 0u64;
        let mut timer = tokio::time::interval(Duration::from_secs(10));
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut mode = self.mode.subscribe();
        loop {
            tokio::select! {
                _ = timer.tick() => {},
                _ = mode.changed() => {},
                _ = self.resources.changed.notified() => {},
            }
            if *mode.borrow() == Mode::Stopped {
                return Ok(());
            }
            lease.check()?;
            let draining = *mode.borrow() == Mode::Draining;
            // Drain bounded batches promptly; one batch per heartbeat cannot cover all live reads.
            for index in 0..32 {
                let batch = self.resources.batch(&lease);
                if index > 0 && batch.renew.is_empty() && batch.release.is_empty() {
                    break;
                }
                sequence = sequence
                    .checked_add(1)
                    .ok_or_else(|| Status::internal("heartbeat sequence exhausted"))?;
                let started = Instant::now();
                let reply = session
                    .pulse_resources(
                        sequence,
                        draining,
                        batch.renew.clone(),
                        batch.release.clone(),
                    )
                    .await?;
                lease.renew(session.valid_until)?;
                self.resources
                    .acknowledge(&lease, &batch, started, &reply.grants)?;
                let state = reply
                    .state
                    .ok_or_else(|| Status::internal("control omitted node state"))?;
                self.publish(&lease, &settings, &state);
                if !settings.matches(&state).map_err(invalid_settings)? {
                    self.synchronize(&lease, &mut settings).await?;
                }
            }
            if !self.client.contains(session.peer).await {
                return Err(Status::unavailable("session origin left control DNS"));
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
            return Err(Status::internal(
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
                .ok_or_else(|| Status::internal("control omitted node state"))?,
        );
        *settings = next;
        Ok(())
    }

    fn publish(&self, lease: &Arc<RunLease>, settings: &Settings, state: &wire::NodeState) {
        if state.work_pending {
            self.work.notify_one();
        }
        if state.native_pending {
            self.native_work.notify_one();
        }
        self.current.send_modify(|current| {
            current.maintenance = state.maintenance;
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
    Status::internal("control returned invalid runtime settings")
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
