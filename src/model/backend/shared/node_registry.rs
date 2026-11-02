use super::{Backend, queries::node_registry as q};
use crate::{
    authorization::Principal,
    cluster::{NodeState, credential_binding},
    content::codec::{Key, protect},
    model::{
        Error,
        cluster::NodeSessions,
        node::NodeReport,
        node_read::{GrantResult, KeepAlive},
        session::{self, Open, Run, Snapshot},
    },
};
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::types::Json;
use uuid::Uuid;

#[derive(sqlx::FromRow)]
pub(super) struct Node {
    id: Uuid,
    role: String,
    enabled: bool,
    credential_generation: i64,
    credential_encrypted: Option<Vec<u8>>,
    deployment_id: Uuid,
    run_state: Option<Json<Lease>>,
}

#[derive(Serialize, Deserialize)]
pub(super) struct Lease {
    control: i64,
    boot: Uuid,
    fence: Uuid,
    epoch: i64,
    connection: Uuid,
    sequence: i64,
    expires: DateTime<Utc>,
    draining: bool,
    closed: bool,
    report_sequence: i64,
}

impl Node {
    fn authenticate(&self, state: &NodeState, key: &Key) -> Result<()> {
        ensure!(self.enabled, Error::NodeInactive);
        ensure!(self.id == state.node_id, Error::NodeAuthentication);
        state.authenticate(
            &self.role,
            self.credential_generation,
            self.deployment_id,
            self.credential_encrypted.as_deref(),
            key,
        )
    }
}

async fn authenticated(tx: &mut q::Tx<'_>, state: &NodeState, key: &Key) -> Result<Node> {
    let row = q::node(state.node_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or(Error::NodeAuthentication)?;
    row.authenticate(state, key)?;
    Ok(row)
}

fn require(row: Node, control: i64, run: Run) -> Result<Lease> {
    let lease = row.run_state.ok_or(Error::NodeExpired)?.0;
    ensure!(
        lease.control == control
            && !lease.closed
            && lease.boot == run.boot
            && lease.fence == run.fence
            && lease.expires > Utc::now(),
        Error::NodeExpired
    );
    Ok(lease)
}

pub(super) async fn require_work(
    backend: &Backend,
    tx: &mut q::Tx<'_>,
    state: &NodeState,
    key: &Key,
    run: Run,
) -> Result<bool> {
    let lease = require(
        authenticated(tx, state, key).await?,
        q::epoch(backend)?,
        run,
    )?;
    Ok(!lease.draining)
}

fn valid_name(name: &str) -> Result<()> {
    ensure!(
        (1..=128).contains(&name.len())
            && name.trim() == name
            && !name.chars().any(char::is_control)
            && !name.contains('/'),
        s3s::s3_error!(InvalidArgument, "node name must contain 1..128 safe bytes")
    );
    Ok(())
}

async fn identity(
    tx: &mut q::Tx<'_>,
    row: Option<Node>,
    name: &str,
    role: &str,
    secret: String,
    key: &Key,
) -> Result<NodeState> {
    let (id, generation) = match &row {
        Some(row) => (
            row.id,
            row.credential_generation
                .checked_add(1)
                .context("node generation exhausted")?,
        ),
        None => (Uuid::new_v4(), 1),
    };
    let deployment = q::deployment().fetch_one(&mut **tx).await?;
    let encrypted = protect(secret.as_bytes(), key, credential_binding(id).as_bytes())?;
    if row.is_some() {
        super::queries::work::expire_node(id)
            .execute(&mut **tx)
            .await?;
        super::queries::work::requeue().execute(&mut **tx).await?;
        q::rotate(id, generation, &encrypted)
            .execute(&mut **tx)
            .await?;
        q::forget_boots(id).execute(&mut **tx).await?;
    } else {
        q::insert(id, name, role, &encrypted)
            .execute(&mut **tx)
            .await?;
    }
    Ok(NodeState {
        format: 1,
        deployment_id: deployment,
        node_id: id,
        role: role.into(),
        credential_generation: generation,
        credential: secret,
    })
}

async fn snapshot(tx: &mut q::Tx<'_>, lease: &Lease) -> Result<Snapshot> {
    let (maintenance, mode, mode_revision, work_pending) = q::mode().fetch_one(&mut **tx).await?;
    let settings = q::settings().fetch_all(&mut **tx).await?;
    Ok(Snapshot {
        mode,
        mode_revision,
        run: Run {
            boot: lease.boot,
            fence: lease.fence,
        },
        epoch: lease.epoch,
        valid_for_ms: (lease.expires - Utc::now()).num_milliseconds().max(0),
        maintenance,
        draining: lease.draining,
        settings,
        grants: Vec::new(),
        native_pending: false,
        work_pending,
        pending_uploads: 0,
    })
}

#[async_trait::async_trait]
impl NodeSessions for Backend {
    async fn register(
        &self,
        name: &str,
        role: &str,
        credential: String,
        key: &Key,
    ) -> Result<Option<NodeState>> {
        ensure!(role == "control", Error::NodeAuthentication);
        valid_name(name)?;
        let mut tx = q::begin(self).await?;
        if q::embedded().fetch_optional(&mut *tx).await?.is_some() {
            return Ok(None);
        }
        let row = q::named(name).fetch_optional(&mut *tx).await?;
        if row.as_ref().is_some_and(|r| r.role != role || r.enabled) {
            return Ok(None);
        }
        let state = identity(&mut tx, row, name, role, credential, key).await?;
        tx.commit().await?;
        Ok(Some(state))
    }
    async fn heartbeat(&self, state: &NodeState, role: &str, key: &Key) -> Result<()> {
        ensure!(
            role == "control" && state.role == role,
            Error::NodeAuthentication
        );
        let mut tx = q::begin(self).await?;
        authenticated(&mut tx, state, key).await?;
        q::seen(state.node_id).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn provision_node(
        &self,
        principal: &Principal,
        name: &str,
        role: &str,
        key: &Key,
    ) -> Result<NodeState> {
        ensure!(
            matches!(principal, Principal::Local),
            s3s::s3_error!(AccessDenied)
        );
        ensure!(
            matches!(role, "data" | "worker" | "node"),
            s3s::s3_error!(InvalidArgument)
        );
        valid_name(name)?;
        let mut tx = q::begin(self).await?;
        ensure!(
            q::named(name).fetch_optional(&mut *tx).await?.is_none(),
            s3s::s3_error!(InvalidRequest, "node name is already registered")
        );
        let state = identity(
            &mut tx,
            None,
            name,
            role,
            crate::cluster::identity::credential()?,
            key,
        )
        .await?;
        tx.commit().await?;
        Ok(state)
    }
    async fn rotate_node(&self, principal: &Principal, name: &str, key: &Key) -> Result<NodeState> {
        ensure!(
            matches!(principal, Principal::Local),
            s3s::s3_error!(AccessDenied)
        );
        let mut tx = q::begin(self).await?;
        let row = q::named(name)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(Error::NodeInactive)?;
        ensure!(row.role != "control", Error::NodeAuthentication);
        let role = row.role.clone();
        let state = identity(
            &mut tx,
            Some(row),
            name,
            &role,
            crate::cluster::identity::credential()?,
            key,
        )
        .await?;
        tx.commit().await?;
        Ok(state)
    }
    async fn release(&self, name: &str) -> Result<()> {
        let mut tx = q::membership(self, None, true).await?;
        let row = q::named(name)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(Error::NodeInactive)?;
        ensure!(row.enabled && row.role != "control", Error::NodeInactive);
        super::queries::work::expire_node(row.id)
            .execute(&mut *tx)
            .await?;
        super::queries::work::requeue().execute(&mut *tx).await?;
        q::release(row.id).execute(&mut *tx).await?;
        if matches!(row.role.as_str(), "node" | "worker") {
            q::release_handoff().execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }
    async fn open_session(&self, state: &NodeState, key: &Key, input: Open) -> Result<Snapshot> {
        ensure!(
            !input.boot.is_nil()
                && !input.connection.is_nil()
                && input.previous_epoch >= 0
                && state.role != "control",
            Error::NodeAuthentication
        );
        let mut tx = q::begin(self).await?;
        let row = authenticated(&mut tx, state, key).await?;
        let control = q::epoch(self)?;
        let now = Utc::now();
        let previous = row.run_state.map(|r| r.0);
        let mut fence = Uuid::new_v4();
        let mut epoch = 1;
        let mut draining = false;
        let mut report_sequence = 0;
        if let Some(old) = &previous {
            epoch = old.epoch.checked_add(1).ok_or(Error::NodeLimit)?;
            if old.boot == input.boot {
                ensure!(!old.closed, Error::NodeExpired);
                ensure!(
                    old.connection == input.connection || old.epoch == input.previous_epoch,
                    Error::NodeConflict
                );
                if old.control == control && old.expires > now {
                    fence = old.fence;
                }
                draining = old.draining;
                report_sequence = old.report_sequence;
            } else {
                ensure!(old.closed || old.expires <= now, Error::NodeConflict);
                ensure!(input.previous_epoch == 0, Error::NodeConflict);
            }
        } else {
            ensure!(input.previous_epoch == 0, Error::NodeConflict);
        }
        if previous.as_ref().is_none_or(|old| old.boot != input.boot) {
            ensure!(
                !q::known(state.node_id, input.boot)
                    .fetch_one(&mut *tx)
                    .await?,
                Error::NodeExpired
            );
            ensure!(
                q::boot_count(state.node_id).fetch_one(&mut *tx).await? < session::BOOT_LIMIT,
                Error::NodeLimit
            );
            q::remember(state.node_id, input.boot)
                .execute(&mut *tx)
                .await?;
            q::clear_report(state.node_id).execute(&mut *tx).await?;
        }
        if previous.as_ref().is_some_and(|old| old.fence != fence) {
            super::queries::work::expire_node(state.node_id)
                .execute(&mut *tx)
                .await?;
            super::queries::work::requeue().execute(&mut *tx).await?;
        }
        let lease = Lease {
            control,
            boot: input.boot,
            fence,
            epoch,
            connection: input.connection,
            sequence: 0,
            expires: now + chrono::Duration::seconds(session::LEASE_SECONDS),
            draining,
            closed: false,
            report_sequence,
        };
        q::save(state.node_id, &lease).execute(&mut *tx).await?;
        let reply = snapshot(&mut tx, &lease).await?;
        tx.commit().await?;
        Ok(reply)
    }
    async fn sync_node(&self, state: &NodeState, key: &Key, run: Run) -> Result<Snapshot> {
        let mut tx = q::begin(self).await?;
        let lease = require(
            authenticated(&mut tx, state, key).await?,
            q::epoch(self)?,
            run,
        )?;
        let reply = snapshot(&mut tx, &lease).await?;
        tx.commit().await?;
        Ok(reply)
    }
    async fn close_session(&self, state: &NodeState, key: &Key, run: Run) -> Result<()> {
        let mut tx = q::begin(self).await?;
        let row = authenticated(&mut tx, state, key).await?;
        let mut lease = row.run_state.ok_or(Error::NodeExpired)?.0;
        ensure!(
            lease.control == q::epoch(self)? && lease.boot == run.boot && lease.fence == run.fence,
            Error::NodeExpired
        );
        lease.closed = true;
        lease.expires = Utc::now();
        q::save(state.node_id, &lease).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn report_session(
        &self,
        state: &NodeState,
        key: &Key,
        run: Run,
        report: &NodeReport,
        _interval: u64,
    ) -> Result<bool> {
        report.validate()?;
        ensure!(report.boot_id == run.boot, Error::NodeConflict);
        let mut tx = q::membership(self, report.worker_revision(&state.role), false).await?;
        let mut lease = require(
            authenticated(&mut tx, state, key).await?,
            q::epoch(self)?,
            run,
        )?;
        if report.sequence <= lease.report_sequence {
            return Ok(false);
        }
        lease.report_sequence = report.sequence;

        let mut value = serde_json::to_value(report)?;
        if report.metadata.is_none()
            && let Some(previous) = q::report(state.node_id).fetch_one(&mut *tx).await?
            && let Some(metadata) = previous.get("metadata")
        {
            value["metadata"] = metadata.clone();
        }
        q::save_report(state.node_id, &lease, &value)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(true)
    }
    async fn pulse_session(
        &self,
        state: &NodeState,
        key: &Key,
        run: Run,
        epoch: i64,
        sequence: i64,
        draining: bool,
        resources: KeepAlive,
    ) -> Result<Snapshot> {
        let mut tx = q::begin(self).await?;
        let mut lease = require(
            authenticated(&mut tx, state, key).await?,
            q::epoch(self)?,
            run,
        )?;
        ensure!(
            lease.epoch == epoch && sequence > lease.sequence,
            Error::NodeConflict
        );
        lease.sequence = sequence;
        lease.draining = draining;
        lease.expires = Utc::now() + chrono::Duration::seconds(session::LEASE_SECONDS);
        q::save(state.node_id, &lease).execute(&mut *tx).await?;
        let mut reply = snapshot(&mut tx, &lease).await?;
        reply.grants = super::node_work::grants(&mut tx, state.node_id, run, resources).await?;
        tx.commit().await?;
        Ok(reply)
    }
    async fn maintain_session(
        &self,
        state: &NodeState,
        key: &Key,
        run: Run,
        epoch: i64,
        sequence: i64,
        resources: KeepAlive,
    ) -> Result<(i64, Vec<GrantResult>)> {
        let mut tx = q::begin(self).await?;
        let mut lease = require(
            authenticated(&mut tx, state, key).await?,
            q::epoch(self)?,
            run,
        )?;
        ensure!(
            lease.epoch == epoch && sequence > lease.sequence,
            Error::NodeConflict
        );
        lease.sequence = sequence;
        q::save(state.node_id, &lease).execute(&mut *tx).await?;
        let granted = super::node_work::grants(&mut tx, state.node_id, run, resources).await?;
        tx.commit().await?;
        Ok((
            (lease.expires - Utc::now()).num_milliseconds().max(0),
            granted,
        ))
    }
}
