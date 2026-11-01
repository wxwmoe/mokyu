use super::*;

pub(in super::super) async fn purge_batch(
    pool: &PgPool,
    id: Execution,
    bucket: Uuid,
    cursor: Option<&str>,
    batch: i64,
    active: &[Uuid],
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    lock_execution(&mut tx, id).await?;
    let keys: Vec<(String, Option<Id>)> = queries::lock_purge_objects(bucket, cursor, batch)
        .fetch_all(&mut *tx)
        .await?;
    if !keys.is_empty() {
        let last = &keys.last().unwrap().0;
        for (key, stream) in &keys {
            if let Some(stream) = stream {
                queries::retire_stream(*stream).execute(&mut *tx).await?;
            }
            queries::purge_object(bucket, key, Id::default())
                .execute(&mut *tx)
                .await?;
        }
        queries::advance_task(id.id, last, keys.len() as i64)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        return Ok(false);
    }
    queries::purge_upload_history(bucket, active, batch)
        .execute(&mut *tx)
        .await?;
    let waiting: bool = queries::purge_waiting(bucket).fetch_one(&mut *tx).await?;
    if waiting {
        tx.commit().await?;
        return Ok(false);
    }
    queries::purge_object_rows(bucket).execute(&mut *tx).await?;
    queries::lock_accounts().execute(&mut *tx).await?;
    lock_bucket_dependents(&mut tx, bucket).await?;
    queries::purge_bucket_row(bucket).execute(&mut *tx).await?;
    queries::complete_task(id.id).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(true)
}

pub(in super::super) async fn cleanup_history(
    pool: &PgPool,
    section: &str,
    age: f64,
    batch: i64,
    active: &[Uuid],
    timeout_ms: u128,
) -> Result<u64> {
    let mut tx = pool.begin().await?;
    queries::history_limits(&mut tx).await?;
    queries::cleanup_statement_timeout(timeout_ms.max(1).to_string())
        .execute(&mut *tx)
        .await?;
    if section == "tasks" {
        super::super::replica_transactions::reap_abandoned(&mut tx, batch).await?;
        let expired = super::super::queries::replica::expired(age, batch)
            .fetch_all(&mut *tx)
            .await?;
        super::super::replica_transactions::prune(&mut tx, expired).await?;
    }
    let count = queries::cleanup_history(section, age, batch, active)?
        .execute(&mut *tx)
        .await?
        .rows_affected();
    tx.commit().await?;
    Ok(count)
}

pub(in super::super) async fn recover_runtime(pool: &PgPool, maintenance: bool) -> Result<()> {
    let mut tx = pool.begin().await?;
    queries::reset_access_coverage().execute(&mut *tx).await?;
    queries::abandon_startup_streams().execute(&mut *tx).await?;
    super::super::replica_transactions::recover_admissions(&mut tx).await?;
    queries::fail_startup_chunks().execute(&mut *tx).await?;
    queries::retire_startup_encodings()
        .execute(&mut *tx)
        .await?;
    queries::retire_startup_packs().execute(&mut *tx).await?;
    queries::clear_pack_inputs().execute(&mut *tx).await?;
    queries::reset_completing_uploads()
        .execute(&mut *tx)
        .await?;
    queries::reset_startup_tasks(maintenance)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn claim_stale_stream(
    pool: &PgPool,
    id: Id,
    storage: Option<Uuid>,
    retention: Option<f64>,
    single: bool,
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    queries::cleanup_limits(&mut tx).await?;
    let claimed = claim_stale_stream_in(&mut tx, id, storage, retention, single).await?;
    if claimed {
        tx.commit().await?;
    }
    Ok(claimed)
}

pub(super) async fn claim_stale_stream_in(
    tx: &mut Transaction<'_, Postgres>,
    id: Id,
    storage: Option<Uuid>,
    retention: Option<f64>,
    single: bool,
) -> Result<bool> {
    let Some(publication) = queries::claim_stale_stream(id, retention, storage, single)
        .build_query_scalar::<Option<Uuid>>()
        .fetch_optional(&mut **tx)
        .await?
    else {
        return Ok(false);
    };
    if publication.is_some() {
        super::super::replica_transactions::reap_admission(tx, id, None).await?;
    }
    Ok(true)
}

pub(in super::super) async fn cleanup_stream(pool: &PgPool, id: Id, batch: i64) -> Result<usize> {
    let mut tx = pool.begin().await?;
    queries::cleanup_limits(&mut tx).await?;
    let count = cleanup_stream_in(&mut tx, id, batch).await?;
    tx.commit().await?;
    Ok(count)
}

pub(super) async fn cleanup_stream_in(
    tx: &mut Transaction<'_, Postgres>,
    id: Id,
    batch: i64,
) -> Result<usize> {
    let raw = queries::raw_cleanup_ranges(id, batch.clamp(1, 128))
        .fetch_all(&mut **tx)
        .await?;
    if let Some((last, _)) = raw.last() {
        let mut sources: Vec<_> = raw.iter().map(|(_, source)| *source).collect();
        sources.sort_unstable();
        sources.dedup();
        for source in sources {
            queries::raw_source(source).fetch_one(&mut **tx).await?;
            queries::raw_touch_unreferenced(source)
                .execute(&mut **tx)
                .await?;
        }
        queries::raw_detach(id, *last).execute(&mut **tx).await?;
    }
    let candidates = queries::retire_stream_encodings(id, batch)
        .execute(&mut **tx)
        .await?
        .rows_affected();
    let detached = queries::detach_stream_chunks(id, batch)
        .execute(&mut **tx)
        .await?
        .rows_affected();
    let chunks = queries::remove_stream_extents(id, batch)
        .fetch_all(&mut **tx)
        .await?;
    let count = chunks.len();
    let chunks = chunks.into_iter().flatten().collect::<Vec<_>>();
    mark_unreferenced(tx, &chunks).await?;
    let streams = queries::remove_empty_stream(id)
        .execute(&mut **tx)
        .await?
        .rows_affected();
    Ok(raw.len() + count + (candidates + detached + streams) as usize)
}

struct FragmentDelete {
    tx: Transaction<'static, Postgres>,
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
    pool: &PgPool,
    id: Id,
) -> Result<Option<FragmentCleanup>> {
    let mut tx = pool.begin().await?;
    queries::cleanup_limits(&mut tx).await?;
    let Some((location_id, sealed, owner, storage, key, file)) = queries::lock_cleanup_fragment(id)
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
    pool: &PgPool,
    storage: Option<Uuid>,
    batch: i64,
) -> Result<u64> {
    let mut tx = pool.begin().await?;
    queries::cleanup_limits(&mut tx).await?;
    let count = queries::remove_tombstones(batch, storage)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    tx.commit().await?;
    Ok(count)
}

pub(in super::super) async fn retire_chunk(
    pool: &PgPool,
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
    queries::claim_reclaim_chunk(id, storage)
        .execute(&mut *tx)
        .await?;
    let locations = queries::reclaim_locations(id).fetch_all(&mut *tx).await?;
    queries::retire_chunk_encodings(id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(Some(locations))
}

pub(in super::super) async fn mark_orphan_chunks(pool: &PgPool) -> Result<usize> {
    let mut tx = pool.begin().await?;
    queries::lock_accounts().execute(&mut *tx).await?;
    let changed = queries::mark_orphan_chunks()
        .execute(&mut *tx)
        .await?
        .rows_affected();
    tx.commit().await?;
    Ok(changed as usize)
}

pub(in super::super) async fn change_maintenance_policy(
    pool: &PgPool,
    policy: &str,
    paused: bool,
) -> Result<()> {
    let mut tx = pool.begin().await?;
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
        json!({"paused":paused}),
    )
    .await?;
    tx.commit().await?;

    Ok(())
}

pub(in super::super) async fn start_lifecycle_policy(
    pool: &PgPool,
    single: bool,
    policy: &str,
    node: Option<Uuid>,
) -> Result<(Uuid, bool)> {
    let mut tx = pool.begin().await?;
    queries::lock_pack_policy(format!("mokyu:maintenance:{policy}"))
        .execute(&mut *tx)
        .await?;
    if let Some(id) = queries::active_lifecycle_task(policy)
        .fetch_optional(&mut *tx)
        .await?
    {
        tx.commit().await?;
        return Ok((id, true));
    }
    let storages: Vec<(Uuid, String, i64)> =
        queries::lifecycle_storages().fetch_all(&mut *tx).await?;
    if single {
        let detail = json!({"phase":if policy == "cleanup" {"history"} else {"storage"}});
        let id = enqueue_storage_task(&mut tx, policy, detail, &storages).await?;
        queries::scheduled_maintenance(policy)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        return Ok((id, false));
    }
    let children = storages.len() + usize::from(policy == "cleanup");
    let id = enqueue_task(
        &mut tx,
        policy,
        None,
        json!({"role":"parent","children":children}),
        false,
    )
    .await?;
    if policy == "cleanup" {
        let child = enqueue_task(
            &mut tx,
            policy,
            None,
            json!({"role":"child","phase":"history"}),
            true,
        )
        .await?;
        queries::scope_history_task(child, id)
            .execute(&mut *tx)
            .await?;
    }
    for (storage_id, storage_type, revision) in &storages {
        let child = enqueue_task(
            &mut tx,
            policy,
            None,
            json!({"role":"child","storage_id":storage_id,"profile_revision":revision}),
            true,
        )
        .await?;
        let (scope, target_node) = if storage_type == "local" {
            ("node", node)
        } else {
            ("cluster", None)
        };
        queries::scope_pack_task(child, id, scope, target_node)
            .execute(&mut *tx)
            .await?;
    }
    queries::task_batch_total(id, children as i64)
        .execute(&mut *tx)
        .await?;
    if children == 0 {
        queries::complete_task(id).execute(&mut *tx).await?;
    }
    queries::scheduled_maintenance(policy)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;

    Ok((id, false))
}

pub(in super::super) async fn finish_single_lifecycle(
    pool: &PgPool,
    task: Execution,
    count: i64,
    again: bool,
    report: Value,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    let mut detail = queries::lock_task_detail(task)
        .fetch_optional(&mut *tx)
        .await?
        .context("task execution expired")?;
    if let Some(storage) = detail["storage_id"].as_str().map(str::to_owned) {
        let saved = &mut detail["report"]["storages"][storage];
        for (key, value) in report.as_object().context("invalid maintenance report")? {
            if let Some(count) = value.as_i64() {
                saved[key] = json!(
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
    pool: &PgPool,
    principal: &Principal,
    action: &str,
    parameters: Value,
    impact: Value,
    confirmation: String,
) -> Result<MaintenancePreview> {
    let mut tx = pool.begin().await?;
    // One bounded, expiring row replaces a caller's previous unexecuted preview of this action.
    queries::remove_maintenance_preview(principal.user_id(), principal.token_id(), action)
        .execute(&mut *tx)
        .await?;
    let fingerprint = blake3::hash(&serde_json::to_vec(&impact)?)
        .to_hex()
        .to_string();
    let (id, expires_at) = queries::create_maintenance_preview(
        principal.user_id(),
        principal.token_id(),
        action,
        json!({"input":parameters,"confirmation":confirmation,"impact":impact}),
        fingerprint,
    )
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
    tx: Transaction<'static, Postgres>,
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
                json!({"preview_id":self.id}),
            )
            .await?;
        }
        self.tx.commit().await?;
        Ok(())
    }
}

pub(in super::super) async fn maintenance_execution(
    pool: &PgPool,
    principal: &Principal,
    id: Uuid,
    action: &str,
) -> Result<Option<MaintenanceExecution>> {
    let mut tx = pool.begin().await?;
    principal.lock_admin(&mut tx).await?;
    let saved: Option<(Value, String, Option<Uuid>)> =
        queries::lock_maintenance_preview(id, principal.user_id(), principal.token_id(), action)
            .fetch_optional(&mut *tx)
            .await?;
    let Some((parameters, fingerprint, mut task)) = saved else {
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
