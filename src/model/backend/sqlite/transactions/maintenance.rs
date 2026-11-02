use super::*;

// Keep new references fenced until the local fragment file has been removed.
struct FragmentDelete {
    tx: Transaction<'static, Sqlite>,
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
    pool: &SqlitePool,
    id: Id,
) -> Result<Option<FragmentCleanup>> {
    let Some(mut tx) = try_cleanup_transaction(pool).await? else {
        return Ok(None);
    };
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
    pool: &SqlitePool,
    storage: Option<Uuid>,
    batch: i64,
) -> Result<u64> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let count = queries::remove_tombstones(&mut tx, storage, batch).await?;
    tx.commit().await?;
    Ok(count)
}

pub(in super::super) async fn retire_chunk(
    pool: &SqlitePool,
    id: Id,
    storage: Uuid,
    grace: f64,
    active: &[Id],
) -> Result<Option<Vec<Uuid>>> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
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

pub(in super::super) async fn mark_orphan_chunks(pool: &SqlitePool) -> Result<usize> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let changed = queries::mark_orphan_chunks()
        .execute(&mut *tx)
        .await?
        .rows_affected();
    tx.commit().await?;
    Ok(changed as usize)
}

pub(in super::super) async fn cleanup_history(
    pool: &SqlitePool,
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
        let mut tx = connection.begin_with(queries::BEGIN_WRITE).await?;
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
            connection.release().await?;
            result
        }
        Err(_) => Err(crate::model::Error::QueryCancelled.into()),
    }
}

pub(in super::super) async fn maintenance_mode(pool: &SqlitePool, enabled: bool) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
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

pub(in super::super) async fn change_pack_creation(pool: &SqlitePool, paused: bool) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
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
    pool: &SqlitePool,
    policy: &str,
    paused: bool,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
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
    pool: &SqlitePool,
    policy: &str,
) -> Result<(Uuid, bool)> {
    ensure!(
        matches!(policy, "cleanup" | "gc"),
        "invalid lifecycle policy"
    );
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
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
    pool: &SqlitePool,
    task: Execution,
    count: i64,
    again: bool,
    report: Value,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
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
    backend: &super::super::Backend,
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
    let _preview = backend.previews.clone().lock_owned().await;
    let pool = &backend.write;
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
        expires_at: queries::datetime(expires_at)?,
        impact,
    })
}

struct MaintenanceReceipt {
    _preview: tokio::sync::OwnedMutexGuard<()>,
    _authorization: Box<dyn crate::model::AdminGuard>,
    write: SqlitePool,
    id: Uuid,
    action: String,
    recorded: Option<Uuid>,
}

#[async_trait::async_trait]
impl MaintenanceCompletion for MaintenanceReceipt {
    async fn finish(self: Box<Self>, task: Uuid, audit: bool) -> Result<()> {
        let mut tx = self.write.begin_with(queries::BEGIN_WRITE).await?;
        if self.recorded != Some(task) {
            queries::record_maintenance_preview(self.id, task)
                .execute(&mut *tx)
                .await?;
        }
        if audit {
            checkpoint_event(
                &mut tx,
                &format!("maintenance.{}", self.action),
                &task.to_string(),
                serde_json::json!({"preview_id":self.id}),
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }
}

pub(in super::super) async fn maintenance_execution(
    backend: &super::super::Backend,
    principal: &Principal,
    id: Uuid,
    action: &str,
) -> Result<Option<MaintenanceExecution>> {
    let actor = principal.user_id().ok_or_else(denied)?;
    let preview = backend.previews.clone().lock_owned().await;
    let authorization = hold_admin(backend, principal).await?;
    let mut tx = backend.read.begin().await?;
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
    tx.commit().await?;
    Ok(Some(MaintenanceExecution {
        parameters,
        fingerprint,
        task,
        completion: Box::new(MaintenanceReceipt {
            _preview: preview,
            _authorization: authorization,
            write: backend.write.clone(),
            id,
            action: action.to_owned(),
            recorded,
        }),
    }))
}

pub(super) async fn try_cleanup_transaction(
    pool: &SqlitePool,
) -> Result<Option<Transaction<'static, Sqlite>>> {
    // Committing returns the pooled connection asynchronously; allow bounded progress.
    match tokio::time::timeout(
        std::time::Duration::from_millis(50),
        pool.begin_with(queries::BEGIN_WRITE),
    )
    .await
    {
        Ok(result) => Ok(Some(result?)),
        Err(_) => Ok(None),
    }
}

pub(in super::super) async fn read_maintenance_controls(pool: &SqlitePool) -> Result<Vec<Value>> {
    let mut tx = pool.begin().await?;
    let rows = queries::maintenance_controls(&mut tx).await?;
    tx.commit().await?;
    Ok(rows)
}

pub(in super::super) async fn gc_status(
    pool: &SqlitePool,
) -> Result<(Vec<(String, i64)>, bool, bool)> {
    let mut tx = pool.begin().await?;
    let value = queries::gc_status(&mut tx).await?;
    tx.commit().await?;
    Ok(value)
}

pub(in super::super) async fn collect_inventory(
    backend: &super::super::Backend,
    metrics: &crate::config::MetricsSettings,
    maintenance: &crate::config::Maintenance,
) -> Result<Value> {
    let deadline = tokio::time::Instant::now()
        + std::time::Duration::from_secs(crate::config::seconds(&metrics.query_timeout)?);
    let mut connection =
        super::super::connection::DeadlineConnection::acquire(&backend.read, deadline).await?;
    let result=tokio::time::timeout_at(deadline,async {

        let mut tx=connection.begin().await?;
        let as_of=chrono::Utc::now();
        let buckets=queries::inventory_buckets(&mut tx).await?;
        let mut chunks=queries::inventory_chunks().fetch_one(&mut *tx).await?;
        let physical=queries::inventory_physical(&mut tx).await?;
        for (field,states) in [("stored_bytes",&["ready","retired","deleting"][..]),("unconfirmed_bytes",&["preparing","uploading"][..])] {
            chunks[field]=json!(physical.as_array().unwrap().iter().filter(|p|states.contains(&p["state"].as_str().unwrap_or_default())).map(|p|p["stored_bytes"].as_i64().unwrap_or(0)).sum::<i64>());
        }
        let live=queries::inventory_live().fetch_one(&mut *tx).await?;
        let mut unreferenced=queries::inventory_unreferenced().fetch_one(&mut *tx).await?;
        let reclaimable=queries::inventory_reclaimable().fetch_one(&mut *tx).await?;
        unreferenced["stored_bytes"]=reclaimable["bytes"].clone();
        unreferenced["eligible_bytes"]=reclaimable["eligible"].clone();
        let tasks=queries::inventory_tasks().fetch_one(&mut *tx).await?;
        let uploads=queries::inventory_uploads().fetch_one(&mut *tx).await?;
        let cleanup=queries::inventory_cleanup(&mut tx,crate::config::seconds(&maintenance.task_retention)? as f64).await?;
        let database=queries::inventory_database().fetch_all(&mut *tx).await?;
        let usage=queries::collect_usage().fetch_all(&mut *tx).await?;
        tx.commit().await?;
        Ok::<_,anyhow::Error>((json!({"as_of":as_of,"collected_at":chrono::Utc::now(),"objects":buckets[0]["objects"],"logical_bytes":buckets[0]["logical_bytes"],"buckets":buckets.iter().skip(1).take(1000).collect::<Vec<_>>(),"buckets_truncated":buckets.len()>1001,"chunks":chunks,"live":live,"physical":physical,"unreferenced":unreferenced,"tasks":tasks,"uploads":uploads,"cleanup":cleanup,"database":database}),usage,as_of))
    }).await;
    let (value, rows, at) = match result {
        Ok(result) => {
            connection.release().await?;
            result?
        }
        Err(_) => return Err(crate::model::Error::QueryCancelled.into()),
    };
    let mut connection =
        super::super::connection::DeadlineConnection::acquire(&backend.write, deadline).await?;
    let result = tokio::time::timeout_at(deadline, async {
        let mut tx = connection.begin_with(queries::BEGIN_WRITE).await?;
        collect_usage(&mut tx, metrics, at, rows).await?;
        tx.commit().await?;
        anyhow::Ok(())
    })
    .await;
    match result {
        Ok(result) => {
            connection.release().await?;
            result?;
            Ok(value)
        }
        Err(_) => Err(crate::model::Error::QueryCancelled.into()),
    }
}

pub(in super::super) async fn collect_storage(pool: &SqlitePool, storage: Uuid) -> Result<Value> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut connection =
        super::super::connection::DeadlineConnection::acquire(pool, deadline).await?;
    let result=tokio::time::timeout_at(deadline,async {

        let mut tx=connection.begin().await?;
        let as_of=chrono::Utc::now();
        let buckets=queries::storage_inventory_buckets(storage).fetch_one(&mut *tx).await?;
        let chunks=queries::storage_inventory_chunks(storage).fetch_one(&mut *tx).await?;
        let physical=queries::storage_inventory_physical(storage).fetch_one(&mut *tx).await?;
        tx.commit().await?;
        Ok(json!({"storage_id":storage,"as_of":as_of,"buckets":buckets.0,"objects":buckets.1,"logical_bytes":buckets.2,"chunks":chunks.0,"stored_bytes":chunks.1,"physical":{"objects":physical.0,"stored_bytes":physical.1}}))
    }).await;
    match result {
        Ok(result) => {
            connection.release().await?;
            result
        }
        Err(_) => Err(crate::model::Error::QueryCancelled.into()),
    }
}

pub(in super::super) async fn read_usage(
    backend: &super::super::Backend,
    principal: &Principal,
    scope: &ScopeQuery,
    limit: usize,
) -> Result<UsageSnapshot> {
    ensure!((1..=256).contains(&limit), "invalid usage scope limit");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut write = false;
    loop {
        let pool = if write { &backend.write } else { &backend.read };
        let mut connection =
            super::super::connection::DeadlineConnection::acquire(pool, deadline).await?;
        let result = tokio::time::timeout_at(deadline, async {
            let mut tx = if write {
                connection.begin_with(queries::BEGIN_WRITE).await?
            } else {
                connection.begin().await?
            };
            let (ids, admin) = usage_scope(&mut tx, principal, scope).await?;
            let global = admin && scope.bucket.is_none() && scope.project.is_none();
            let key = if global {
                "global".to_owned()
            } else {
                blake3::hash(&serde_json::to_vec(&ids)?)
                    .to_hex()
                    .to_string()
            };
            let due = queries::usage_touch_due(&key)
                .fetch_optional(&mut *tx)
                .await?;
            if !write && due != Some(false) {
                tx.commit().await?;
                return anyhow::Ok(None);
            }
            let created = due.is_none();
            if created {
                queries::evict_usage_scopes(&mut tx, limit as i64 - 1).await?;
                queries::create_usage_scope(&key).execute(&mut *tx).await?;
                queries::usage_bucket_members(&mut tx, &key, &ids).await?;
            }
            if write {
                queries::touch_usage_scope(&key).execute(&mut *tx).await?;
            }
            let (as_of, data) = queries::usage_saved(&key)
                .fetch_optional(&mut *tx)
                .await?
                .unwrap_or_default();
            let history = queries::usage_history(&key).fetch_all(&mut *tx).await?;
            let history = history
                .into_iter()
                .map(|(at, data)| {
                    Ok(CapacityPoint {
                        at: queries::datetime(at)?,
                        usage: data.0,
                    })
                })
                .collect::<sqlx::Result<Vec<_>>>()?;
            tx.commit().await?;
            Ok(Some(UsageSnapshot {
                global,
                bucket_count: ids.len(),
                as_of: as_of.map(queries::datetime).transpose()?,
                usage: data.map(|v| v.0),
                history,
                created,
            }))
        })
        .await;
        let result = match result {
            Ok(result) => {
                connection.release().await?;
                result?
            }
            Err(_) => return Err(crate::model::Error::QueryCancelled.into()),
        };
        if let Some(snapshot) = result {
            return Ok(snapshot);
        }
        write = true;
    }
}
