use super::*;

pub(super) fn verify_node_identity(
    row: &queries::RegisteredNode,
    state: &crate::cluster::NodeState,
    role: &str,
    key: &crate::content::codec::Key,
) -> Result<()> {
    use crate::model::Error;
    ensure!(row.enabled, Error::NodeInactive);
    ensure!(
        state.format == 1
            && row.role == role
            && state.role == role
            && row.deployment_id == state.deployment_id
            && row.credential_generation == state.credential_generation,
        Error::NodeAuthentication
    );
    let expected = crate::content::codec::unprotect(
        row.credential_encrypted
            .as_deref()
            .ok_or(Error::NodeAuthentication)?,
        key,
        crate::cluster::credential_binding(state.node_id).as_bytes(),
    )?;
    ensure!(
        aws_lc_rs::constant_time::verify_slices_are_equal(
            expected.as_bytes(),
            state.credential.as_bytes()
        )
        .is_ok(),
        Error::NodeAuthentication
    );
    Ok(())
}

fn check_node_name(name: &str) -> Result<()> {
    ensure!(
        (1..=128).contains(&name.len())
            && name.trim() == name
            && !name.chars().any(char::is_control)
            && !name.contains('/'),
        s3s::s3_error!(InvalidArgument, "node name must contain 1..128 safe bytes")
    );
    Ok(())
}

async fn replace_node_identity(
    tx: &mut Transaction<'_, Postgres>,
    node: Uuid,
    name: &str,
    role: &str,
    generation: i64,
    credential: String,
    key: &crate::content::codec::Key,
) -> Result<crate::cluster::NodeState> {
    let (deployment, _) = queries::node_deployment().fetch_one(&mut **tx).await?;
    let state = crate::cluster::NodeState {
        format: 1,
        deployment_id: deployment,
        node_id: node,
        role: role.into(),
        credential_generation: generation,
        credential,
    };
    let encrypted = crate::content::codec::protect(
        state.credential.as_bytes(),
        key,
        crate::cluster::credential_binding(node).as_bytes(),
    )?;
    queries::save_node_identity(&state, name, &encrypted)
        .execute(&mut **tx)
        .await?;
    queries::clear_node_session(node).execute(&mut **tx).await?;
    queries::clear_node_boots(node).execute(&mut **tx).await?;
    Ok(state)
}

pub(in super::super) async fn register_node(
    pool: &PgPool,
    name: &str,
    role: &str,
    credential: String,
    key: &crate::content::codec::Key,
) -> Result<Option<crate::cluster::NodeState>> {
    check_node_name(name)?;
    let mut tx = pool.begin().await?;
    queries::node_registration_lock(name)
        .execute(&mut *tx)
        .await?;
    let existing = queries::node_registration(name)
        .fetch_optional(&mut *tx)
        .await?;
    let (node, generation) = if let Some((id, registered_role, enabled, generation)) = existing {
        if enabled || registered_role != role {
            queries::node_registration_conflict(id)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            return Ok(None);
        }
        (
            id,
            generation
                .checked_add(1)
                .context("node credential generation exhausted")?,
        )
    } else {
        (queries::new_node_id().fetch_one(&mut *tx).await?, 1)
    };
    let state =
        replace_node_identity(&mut tx, node, name, role, generation, credential, key).await?;
    queries::node_seen(node).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(Some(state))
}

pub(in super::super) async fn provision_node(
    pool: &PgPool,
    principal: &Principal,
    name: &str,
    role: Option<&str>,
    key: &crate::content::codec::Key,
) -> Result<crate::cluster::NodeState> {
    check_node_name(name)?;
    ensure!(
        role.is_none_or(|role| matches!(role, "data" | "worker" | "node")),
        s3s::s3_error!(InvalidArgument, "node role must be data, worker or node")
    );
    let mut tx = pool.begin().await?;
    principal.lock_admin(&mut tx).await?;
    ensure!(
        queries::node_deployment().fetch_one(&mut *tx).await?.1 == "cluster",
        crate::model::Error::NodeInactive
    );
    queries::node_registration_lock(name)
        .execute(&mut *tx)
        .await?;
    let existing = queries::node_registration(name)
        .fetch_optional(&mut *tx)
        .await?;
    let (node, role, generation) = if let Some(role) = role {
        ensure!(
            existing.is_none(),
            s3s::s3_error!(
                InvalidRequest,
                "node already exists; rotate its credential to recover"
            )
        );
        (
            queries::new_node_id().fetch_one(&mut *tx).await?,
            role.to_owned(),
            1,
        )
    } else {
        let (id, role, _, generation) =
            existing.ok_or_else(|| s3s::s3_error!(NoSuchKey, "node not found"))?;
        ensure!(
            matches!(role.as_str(), "data" | "worker" | "node"),
            s3s::s3_error!(
                InvalidRequest,
                "use local control recovery for control nodes"
            )
        );
        queries::release_node_tasks(&id).execute(&mut *tx).await?;
        queries::release_raw_uploads(id).execute(&mut *tx).await?;
        queries::release_node_permits(&id).execute(&mut *tx).await?;
        (
            id,
            role,
            generation
                .checked_add(1)
                .context("node credential generation exhausted")?,
        )
    };
    let state = replace_node_identity(
        &mut tx,
        node,
        name,
        &role,
        generation,
        crate::cluster::identity::credential()?,
        key,
    )
    .await?;
    tx.commit().await?;
    Ok(state)
}

pub(in super::super) async fn authenticate_rpc_node(
    db: &mut PgConnection,
    state: &crate::cluster::NodeState,
    key: &crate::content::codec::Key,
    exclusive: bool,
) -> Result<()> {
    let row = queries::registered_node(state.node_id, exclusive)
        .fetch_optional(db)
        .await?
        .ok_or(crate::model::Error::NodeAuthentication)?;
    verify_node_identity(&row, state, &state.role, key)?;
    ensure!(
        matches!(state.role.as_str(), "data" | "worker" | "node"),
        crate::model::Error::NodeAuthentication
    );
    ensure!(
        row.deployment_mode == "cluster",
        crate::model::Error::NodeInactive
    );
    Ok(())
}

async fn node_snapshot(
    db: &mut PgConnection,
    node: Uuid,
) -> Result<crate::model::session::Snapshot> {
    let settings = queries::node_settings().fetch_all(&mut *db).await?;
    let native_pending = queries::native::pending(node).fetch_one(&mut *db).await?;
    let work_pending = queries::work::pending(node).fetch_one(&mut *db).await?;
    let (boot, fence, epoch, valid_for_ms, maintenance, draining) =
        queries::node_session_status(node)
            .fetch_optional(&mut *db)
            .await?
            .ok_or(crate::model::Error::NodeInactive)?;
    let pending_uploads = if draining {
        queries::node_upload_backlog(Some(node))
            .fetch_one(db)
            .await?
            .0
    } else {
        0
    };
    Ok(crate::model::session::Snapshot {
        run: crate::model::session::Run { boot, fence },
        epoch,
        valid_for_ms,
        maintenance,
        draining,
        settings,
        grants: Vec::new(),
        native_pending,
        work_pending,
        pending_uploads,
    })
}

pub(in super::super) async fn open_node_session(
    pool: &PgPool,
    state: &crate::cluster::NodeState,
    key: &crate::content::codec::Key,
    input: crate::model::session::Open,
) -> Result<crate::model::session::Snapshot> {
    use crate::model::{
        Error,
        session::{BOOT_LIMIT, Run},
    };
    ensure!(
        !input.boot.is_nil() && !input.connection.is_nil() && input.previous_epoch >= 0,
        Error::NodeAuthentication
    );
    let mut tx = pool.begin().await?;
    authenticate_rpc_node(&mut tx, state, key, true).await?;
    let previous = queries::node_session(state.node_id, true)
        .fetch_optional(&mut *tx)
        .await?;
    let now = queries::node_clock().fetch_one(&mut *tx).await?;
    let mut epoch = 1;
    let mut fence = Uuid::new_v4();
    let mut draining = false;
    if let Some(previous) = &previous {
        ensure!(
            previous.credential_generation == state.credential_generation,
            Error::NodeAuthentication
        );
        epoch = previous
            .epoch
            .checked_add(1)
            .context("node session epoch exhausted")?;
        if previous.boot_id == input.boot {
            ensure!(
                !queries::node_boot_retired(state.node_id, input.boot)
                    .fetch_one(&mut *tx)
                    .await?,
                Error::NodeExpired
            );
            ensure!(
                previous.connection_id == input.connection
                    || input.previous_epoch == previous.epoch,
                Error::NodeConflict
            );
            if previous.expires_at > now {
                fence = previous.fence;
            }
            draining = previous.draining;
        } else {
            ensure!(previous.expires_at <= now, Error::NodeConflict);
            ensure!(input.previous_epoch == 0, Error::NodeConflict);
        }
    } else {
        ensure!(input.previous_epoch == 0, Error::NodeConflict);
    }
    if previous
        .as_ref()
        .is_none_or(|session| session.boot_id != input.boot)
    {
        ensure!(
            !queries::node_boot_known(state.node_id, input.boot)
                .fetch_one(&mut *tx)
                .await?,
            Error::NodeExpired
        );
        ensure!(
            queries::node_boot_count(state.node_id)
                .fetch_one(&mut *tx)
                .await?
                < BOOT_LIMIT,
            Error::NodeLimit
        );
        queries::retire_node_boots(state.node_id)
            .execute(&mut *tx)
            .await?;
        queries::insert_node_boot(state.node_id, state.credential_generation, input.boot)
            .execute(&mut *tx)
            .await?;
        queries::begin_node_report(state.node_id, input.boot, now)
            .execute(&mut *tx)
            .await?;
    }
    queries::save_node_session(
        state.node_id,
        state.credential_generation,
        Run {
            boot: input.boot,
            fence,
        },
        epoch,
        input.connection,
        draining,
    )
    .execute(&mut *tx)
    .await?;
    queries::node_seen(state.node_id).execute(&mut *tx).await?;
    let snapshot = node_snapshot(&mut tx, state.node_id).await?;
    notifications::activate(
        &mut tx,
        crate::model::notification::Consumer {
            node: state.node_id,
            generation: input.connection,
        },
        &state.role,
    )
    .await?;
    tx.commit().await?;
    Ok(snapshot)
}

pub(in super::super) async fn close_node_session(
    pool: &PgPool,
    state: &crate::cluster::NodeState,
    key: &crate::content::codec::Key,
    run: crate::model::session::Run,
) -> Result<()> {
    use crate::model::Error;
    let mut tx = pool.begin().await?;
    authenticate_rpc_node(&mut tx, state, key, true).await?;
    let current = queries::node_session(state.node_id, true)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(Error::NodeExpired)?;
    // A repeated close may observe expiry, but must never retire a replacement run.
    ensure!(
        current.credential_generation == state.credential_generation
            && current.boot_id == run.boot
            && current.fence == run.fence,
        Error::NodeExpired
    );
    queries::close_node_session(state.node_id)
        .execute(&mut *tx)
        .await?;
    queries::notifications::renew(state.node_id)
        .execute(&mut *tx)
        .await?;
    queries::retire_node_boots(state.node_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn report_node_session(
    pool: &PgPool,
    state: &crate::cluster::NodeState,
    key: &crate::content::codec::Key,
    run: crate::model::session::Run,
    report: &crate::model::node::NodeReport,
    sample_interval: u64,
) -> Result<bool> {
    report.validate()?;
    ensure!(
        report.boot_id == run.boot && sample_interval <= 86_400,
        crate::model::Error::NodeExpired
    );
    let sample = (sample_interval > 0)
        .then(|| crate::model::metrics::NodeSample::from_report(report, sample_interval))
        .transpose()?;
    if let Some(sample) = &sample {
        sample.validate()?;
    }
    let mut tx = pool.begin().await?;
    queries::runtime_diagnostic_limits()
        .execute(&mut *tx)
        .await?;
    authenticate_rpc_node(&mut tx, state, key, false).await?;
    require_node_run(&mut tx, state, run, false).await?;
    let written = queries::save_node_report(state.node_id, report)
        .execute(&mut *tx)
        .await?
        .rows_affected()
        == 1;
    if written && let Some(sample) = &sample {
        queries::node_runtime_sample(state.node_id, sample)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(written)
}

pub(in super::super) async fn require_node_run(
    db: &mut PgConnection,
    state: &crate::cluster::NodeState,
    run: crate::model::session::Run,
    exclusive: bool,
) -> Result<crate::model::session::Session> {
    use crate::model::Error;
    let session = queries::node_session(state.node_id, exclusive)
        .fetch_optional(&mut *db)
        .await?
        .ok_or(Error::NodeExpired)?;
    let now = queries::node_clock().fetch_one(db).await?;
    ensure!(
        session.credential_generation == state.credential_generation
            && session.boot_id == run.boot
            && session.fence == run.fence
            && session.expires_at > now,
        Error::NodeExpired
    );
    Ok(session)
}

pub(in super::super) async fn sync_node(
    pool: &PgPool,
    state: &crate::cluster::NodeState,
    key: &crate::content::codec::Key,
    run: crate::model::session::Run,
) -> Result<crate::model::session::Snapshot> {
    let mut tx = pool.begin().await?;
    authenticate_rpc_node(&mut tx, state, key, false).await?;
    require_node_run(&mut tx, state, run, false).await?;
    let snapshot = node_snapshot(&mut tx, state.node_id).await?;
    tx.commit().await?;
    Ok(snapshot)
}

pub(in super::super) async fn report_node_observations(
    pool: &PgPool,
    state: &crate::cluster::NodeState,
    key: &crate::content::codec::Key,
    input: crate::model::node_observation::Batch,
) -> Result<Vec<bool>> {
    use crate::model::Error;
    input.validate()?;
    let mut tx = pool.begin().await?;
    // Dropping a query future does not cancel PostgreSQL's lock wait.
    queries::observation_limits().execute(&mut *tx).await?;
    authenticate_rpc_node(&mut tx, state, key, false).await?;
    ensure!(
        matches!(state.role.as_str(), "data" | "node"),
        Error::NodeAuthentication
    );
    let session = require_node_run(&mut tx, state, input.run, input.access.is_some()).await?;
    if !input.compression.is_empty() || !input.credentials.is_empty() {
        ensure!(
            !session.draining && queries::node_read_admission().fetch_one(&mut *tx).await?,
            Error::NodeInactive
        );
    }
    if let Some(access) = input.access {
        persist_node_access(&mut tx, state.node_id, input.run.boot, access).await?;
    }
    let mut written = Vec::with_capacity(input.compression.len());
    for observation in input.compression {
        written.push(
            queries::learn_compression(observation, Some(state.node_id))
                .execute(&mut *tx)
                .await?
                .rows_affected()
                > 0,
        );
    }
    if !input.credentials.is_empty() {
        queries::touch_credentials(&input.credentials)
            .execute(&mut *tx)
            .await?;
    }
    require_node_run(&mut tx, state, input.run, false).await?;
    tx.commit().await?;
    Ok(written)
}

async fn persist_node_access(
    db: &mut PgConnection,
    node: Uuid,
    boot: Uuid,
    access: crate::model::node_observation::Access,
) -> Result<()> {
    let hash = blake3::hash(&serde_json::to_vec(&access)?);
    let previous = queries::node_access(node).fetch_optional(&mut *db).await?;
    if let Some((old_boot, sequence, old_hash, _, _, _)) = &previous
        && *old_boot == boot
        && *sequence >= access.sequence
    {
        ensure!(
            *sequence != access.sequence || old_hash.as_slice() == hash.as_bytes(),
            crate::model::Error::OperationAborted
        );
        return Ok(());
    }
    let (now, collecting) = queries::demand_clock().fetch_one(&mut *db).await?;
    let valid_hour = |at: &DateTime<Utc>| {
        *at >= now - chrono::Duration::hours(24) && *at <= now + chrono::Duration::hours(1)
    };
    ensure!(
        access.chunks.iter().all(|((_, at), _)| valid_hour(at))
            && access.packs.iter().all(|((_, at), _)| valid_hour(at))
            && access.members.iter().all(|((_, _, at), _)| valid_hour(at))
            && access.demand.iter().all(|((_, _, at), _)| valid_hour(at))
            && access.cached.iter().all(|((_, at), _)| valid_hour(at)),
        s3s::s3_error!(InvalidArgument)
    );
    let stale = previous
        .as_ref()
        .map(|row| crate::config::seconds(&row.5))
        .transpose()?
        .unwrap_or(0)
        .saturating_mul(2);
    let gap = previous
        .filter(|(old_boot, sequence, _, _, received, _)| {
            *old_boot == boot
                && *sequence == access.sequence - 1
                && !access.dropped
                && (!collecting || access.collecting)
                && (now - *received)
                    .to_std()
                    .is_ok_and(|age| age <= std::time::Duration::from_secs(stale))
        })
        .map_or(now, |(_, _, _, gap, _, _)| gap);
    queries::node_access_saved(node, boot, access.sequence, hash.as_bytes(), now, gap)
        .execute(&mut *db)
        .await?;
    persist_access(db, access.pending()).await
}

pub(in super::super) async fn maintain_node_session(
    pool: &PgPool,
    state: &NodeState,
    key: &Key,
    run: Run,
    epoch: i64,
    sequence: i64,
    resources: read::KeepAlive,
) -> Result<(i64, Vec<read::GrantResult>)> {
    let mut tx = pool.begin().await?;
    authenticate_rpc_node(&mut tx, state, key, true).await?;
    let session = require_node_run(&mut tx, state, run, true).await?;
    ensure!(
        session.epoch == epoch && sequence > session.sequence,
        Error::NodeConflict
    );
    let grants = maintain_node_reads(&mut tx, state, run, resources).await?;
    let remaining = queries::maintain_node_session(state.node_id, sequence)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok((remaining, grants))
}

pub(in super::super) async fn pulse_node_session(
    pool: &PgPool,
    state: &crate::cluster::NodeState,
    key: &crate::content::codec::Key,
    run: crate::model::session::Run,
    epoch: i64,
    sequence: i64,
    draining: bool,
    resources: crate::model::node_read::KeepAlive,
) -> Result<crate::model::session::Snapshot> {
    let mut tx = pool.begin().await?;
    authenticate_rpc_node(&mut tx, state, key, true).await?;
    let session = require_node_run(&mut tx, state, run, true).await?;
    ensure!(
        session.epoch == epoch && sequence > session.sequence,
        crate::model::Error::NodeConflict
    );
    queries::pulse_node_session(state.node_id, sequence, draining)
        .execute(&mut *tx)
        .await?;
    queries::notifications::renew(state.node_id)
        .execute(&mut *tx)
        .await?;
    queries::node_seen(state.node_id).execute(&mut *tx).await?;
    let mut snapshot = node_snapshot(&mut tx, state.node_id).await?;
    snapshot.grants = maintain_node_reads(&mut tx, state, run, resources).await?;
    tx.commit().await?;
    Ok(snapshot)
}

pub(super) async fn node_write_owned(
    db: &mut PgConnection,
    node: &NodeState,
    key: &Key,
    run: Run,
    operation: Uuid,
) -> Result<queries::NodeWrite> {
    authenticate_rpc_node(db, node, key, false).await?;
    ensure!(
        matches!(node.role.as_str(), "data" | "worker" | "node"),
        Error::NodeAuthentication
    );
    require_node_run(db, node, run, true).await?;
    let row = queries::node_write(node.node_id, operation)
        .fetch_optional(db)
        .await?
        .ok_or(Error::NodeOperationMissing)?;
    ensure!(
        row.credential_generation == node.credential_generation
            && row.boot_id == run.boot
            && row.fence == run.fence,
        Error::NodeExpired
    );
    Ok(row)
}
