use super::*;

struct FragmentDelete {
    tx: Transaction<'static, MySql>,
    id: Id,
}

#[async_trait::async_trait]
impl FragmentDeletion for FragmentDelete {
    async fn finish(self: Box<Self>) -> Result<()> {
        let Self { mut tx, id } = *self;
        queries::remove_fragment_row(id).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
}

pub(in super::super) async fn claim_fragment(
    pool: &MySqlPool,
    id: Id,
) -> Result<Option<FragmentCleanup>> {
    let mut tx = pool.begin().await?;
    let Some((location_id, sealed, owner, file, storage, key)) = queries::lock_cleanup_fragment(id)
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(None);
    };
    Ok(Some(FragmentCleanup {
        location_id,
        sealed,
        file,
        owner,
        storage,
        key,
        deletion: Box::new(FragmentDelete { tx, id }),
    }))
}

pub(in super::super) async fn cleanup_tombstones(
    pool: &MySqlPool,
    storage: Option<Uuid>,
    batch: i64,
) -> Result<u64> {
    let mut tx = pool.begin().await?;
    let count = queries::remove_tombstones(&mut tx, storage, batch).await?;
    tx.commit().await?;
    Ok(count)
}

pub(in super::super) async fn retire_chunk(
    pool: &MySqlPool,
    id: Id,
    storage: Uuid,
    grace: f64,
    active: &[Id],
) -> Result<Option<Vec<Uuid>>> {
    let mut tx = pool.begin().await?;
    let Some((owner,)) = queries::lock_reclaim_chunk(id, storage, grace)
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(None);
    };
    if owner.is_some_and(|id| active.contains(&id)) {
        return Ok(None);
    }
    queries::claim_reclaim_chunk(id).execute(&mut *tx).await?;
    let locations = queries::reclaim_locations(id).fetch_all(&mut *tx).await?;
    queries::retire_chunk_encodings(id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(Some(locations))
}

pub(in super::super) async fn mark_orphan_chunks(pool: &MySqlPool) -> Result<usize> {
    let mut tx = pool.begin().await?;
    queries::lock_accounts().execute(&mut *tx).await?;
    let changed = queries::mark_orphan_chunks()
        .execute(&mut *tx)
        .await?
        .rows_affected();
    tx.commit().await?;
    Ok(changed as usize)
}

pub(in super::super) async fn cleanup_history(
    pool: &MySqlPool,
    section: &str,
    age: f64,
    batch: i64,
    active: &[Uuid],
    timeout_ms: u128,
) -> Result<u64> {
    let deadline = tokio::time::Instant::now()
        + std::time::Duration::from_millis(timeout_ms.clamp(1, u64::MAX as u128) as u64);
    let mut connection =
        super::super::connection::DeadlineConnection::acquire(pool, deadline).await?;
    let result = tokio::time::timeout_at(deadline, async {
        let mut tx = connection.begin().await?;
        if section == "tasks" {
            super::super::replica_transactions::reap_abandoned(&mut tx, batch).await?;
            let expired = super::super::queries::replica::expired(age, batch)
                .fetch_all(&mut *tx)
                .await?;
            super::super::replica_transactions::prune(&mut tx, expired).await?;
        }
        let count = queries::cleanup_history(&mut tx, section, age, batch, active).await?;
        tx.commit().await?;
        Ok(count)
    })
    .await;
    match result {
        Ok(result) => {
            connection.release();
            result
        }
        Err(_) => Err(crate::model::Error::QueryCancelled.into()),
    }
}

pub(in super::super) async fn maintenance_mode(pool: &MySqlPool, enabled: bool) -> Result<()> {
    let mut tx = pool.begin().await?;
    queries::set_maintenance_mode(enabled)
        .execute(&mut *tx)
        .await?;
    if enabled {
        queries::pause_maintenance_tasks().execute(&mut *tx).await?;
    }
    checkpoint_event(
        &mut tx,
        "maintenance.mode",
        "deployment",
        serde_json::json!({"enabled":enabled}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn change_pack_creation(pool: &MySqlPool, paused: bool) -> Result<()> {
    let mut tx = pool.begin().await?;
    queries::lock_pack_policy("pack")
        .fetch_one(&mut *tx)
        .await?;
    ensure!(
        paused || !queries::unpacking_all().fetch_one(&mut *tx).await?,
        s3s::s3_error!(
            OperationAborted,
            "finish all-pack unpacking before enabling new packs"
        )
    );
    queries::pause_pack_creation(paused)
        .execute(&mut *tx)
        .await?;
    checkpoint_event(
        &mut tx,
        "maintenance.pack_creation",
        "pack",
        serde_json::json!({"paused":paused}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn change_maintenance_policy(
    pool: &MySqlPool,
    policy: &str,
    paused: bool,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    queries::lock_pack_policy(policy)
        .fetch_one(&mut *tx)
        .await?;
    if policy == "gc" {
        queries::pause_gc(paused).execute(&mut *tx).await?;
    }
    queries::pause_maintenance_policy(policy, paused)
        .execute(&mut *tx)
        .await?;
    checkpoint_event(
        &mut tx,
        "maintenance.policy",
        policy,
        serde_json::json!({"paused":paused}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn start_lifecycle_policy(
    pool: &MySqlPool,
    policy: &str,
) -> Result<(Uuid, bool)> {
    ensure!(
        matches!(policy, "cleanup" | "gc"),
        "invalid lifecycle policy"
    );
    let mut tx = pool.begin().await?;
    queries::lock_pack_policy(policy)
        .fetch_one(&mut *tx)
        .await?;
    if let Some(id) = queries::active_lifecycle_task(policy)
        .fetch_optional(&mut *tx)
        .await?
    {
        return Ok((id, true));
    }
    let storages = queries::pack_storages(None).fetch_all(&mut *tx).await?;
    let id = enqueue_storage_task(
        &mut tx,
        policy,
        serde_json::json!({"phase":if policy=="cleanup" {"history"} else {"storage"}}),
        &storages,
    )
    .await?;
    queries::scheduled_maintenance(policy)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok((id, false))
}

pub(in super::super) async fn finish_lifecycle_policy(
    pool: &MySqlPool,
    task: Execution,
    count: i64,
    again: bool,
    report: Value,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    let mut detail = queries::lock_task_detail(task)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(crate::model::Error::OperationAborted)?;
    if let Some(storage) = detail["storage_id"].as_str().map(str::to_owned) {
        let saved = &mut detail["report"]["storages"][storage];
        for (key, value) in report.as_object().context("invalid maintenance report")? {
            if let Some(count) = value.as_i64() {
                saved[key] = serde_json::json!(
                    saved[key]
                        .as_i64()
                        .unwrap_or(0)
                        .checked_add(count)
                        .context("maintenance report overflow")?
                );
            }
        }
    } else {
        detail["report"]["history"] = report;
    }
    let done = !again && advance_storage_scope(&mut detail)?;
    queries::advance_storage_task(task.id, detail, count, done)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    if !again && !done {
        #[cfg(feature = "fault-injection")]
        crate::faults::point("lifecycle-storage-advanced").await;
    }
    Ok(())
}

pub(in super::super) async fn create_maintenance_preview(
    pool: &MySqlPool,
    principal: &Principal,
    action: &str,
    parameters: Value,
    impact: Value,
    confirmation: String,
) -> Result<MaintenancePreview> {
    ensure!(
        matches!(action, "unpack" | "sweep" | "quarantine" | "recheck"),
        "invalid maintenance preview action"
    );
    let actor = principal.user_id().ok_or_else(denied)?;
    let mut tx = administration_transaction(pool, principal).await?;
    queries::remove_maintenance_preview(actor, principal.token_id(), action)
        .execute(&mut *tx)
        .await?;
    let id = Uuid::new_v4();
    let fingerprint = blake3::hash(&serde_json::to_vec(&impact)?)
        .to_hex()
        .to_string();
    queries::create_maintenance_preview(
        id,
        actor,
        principal.token_id(),
        action,
        serde_json::json!({"input":parameters,"confirmation":confirmation,"impact":impact}),
        fingerprint,
    )
    .execute(&mut *tx)
    .await?;
    let expires_at = queries::maintenance_preview_expiry(id)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(MaintenancePreview {
        id,
        confirmation,
        expires_at,
        impact,
    })
}

struct MaintenanceReceipt {
    tx: Transaction<'static, MySql>,
    id: Uuid,
    action: String,
    recorded: Option<Uuid>,
}

#[async_trait::async_trait]
impl MaintenanceCompletion for MaintenanceReceipt {
    async fn finish(mut self: Box<Self>, task: Uuid, audit: bool) -> Result<()> {
        if self.recorded != Some(task) {
            queries::record_maintenance_preview(self.id, task)
                .execute(&mut *self.tx)
                .await?;
        }
        if audit {
            checkpoint_event(
                &mut self.tx,
                &format!("maintenance.{}", self.action),
                &task.to_string(),
                serde_json::json!({"preview_id":self.id}),
            )
            .await?;
        }
        self.tx.commit().await?;
        Ok(())
    }
}

pub(in super::super) async fn maintenance_execution(
    pool: &MySqlPool,
    principal: &Principal,
    id: Uuid,
    action: &str,
) -> Result<Option<MaintenanceExecution>> {
    let actor = principal.user_id().ok_or_else(denied)?;
    let mut tx = pool.begin().await?;
    ensure!(
        queries::validate_identity(principal, &mut tx, true).await?,
        s3s::s3_error!(AccessDenied)
    );
    let Some((parameters, fingerprint, mut task)) =
        queries::lock_maintenance_preview(id, actor, principal.token_id(), action)
            .fetch_optional(&mut *tx)
            .await?
    else {
        return Ok(None);
    };
    let recorded = task;
    if task.is_none()
        && queries::maintenance_preview_task(id)
            .fetch_one(&mut *tx)
            .await?
    {
        task = Some(id);
    }
    Ok(Some(MaintenanceExecution {
        parameters,
        fingerprint,
        task,
        completion: Box::new(MaintenanceReceipt {
            tx,
            id,
            action: action.to_owned(),
            recorded,
        }),
    }))
}
