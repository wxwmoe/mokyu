use super::*;

pub(in super::super) async fn pin_pack_inputs(
    pool: &PgPool,
    task: Execution,
    rows: &[Mapping],
) -> Result<()> {
    let mut ids: Vec<Id> = rows.iter().map(|r| r.chunk.id).collect();
    ids.sort_unstable();
    ids.dedup();
    let mut tx = pool.begin().await?;
    lock_execution(&mut tx, task).await?;
    pin_inputs(&mut tx, task.id, &ids).await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn pin_inputs(db: &mut PgConnection, task: Uuid, ids: &[Id]) -> Result<()> {
    let ready: Vec<Id> = queries::pack_ready_inputs(ids).fetch_all(&mut *db).await?;
    ensure!(ready == ids, "pack inputs changed; retry maintenance");
    record_source_inputs(db, task, ids).await?;
    queries::insert_pack_inputs(task, ids).execute(db).await?;
    Ok(())
}

pub(in super::super) async fn start_pack(
    pool: &PgPool,
    single: bool,
    policy: &str,
    automatic: bool,
    node: Option<Uuid>,
) -> Result<Value> {
    ensure!(
        matches!(policy, "pack" | "reclaim" | "repack") && (!automatic || policy != "repack"),
        "invalid pack policy"
    );
    let mut tx = pool.begin().await?;
    queries::lock_pack_policy(format!("mokyu:pack:{policy}"))
        .execute(&mut *tx)
        .await?;
    let existing: Option<Uuid> = queries::active_pack_task(policy)
        .fetch_optional(&mut *tx)
        .await?;
    if let Some(id) = existing {
        tx.commit().await?;
        return Ok(json!({"task_id":id,"existing":true}));
    }
    let detail = json!({"policy":policy,"upper_time":chrono::Utc::now(),"automatic":automatic});
    let storages: Vec<(Uuid, String, i64)> = queries::pack_storages(Some(policy))
        .fetch_all(&mut *tx)
        .await?;
    let mut due = Vec::new();
    for row in storages {
        let profile = queries::storage_profile(row.0).fetch_one(&mut *tx).await?;
        if automatic
            && !queries::pack_policy_due(row.0, policy, profile.maintenance_interval(policy)?)
                .fetch_one(&mut *tx)
                .await?
        {
            continue;
        }
        queries::pack_policy_scheduled(row.0, policy)
            .execute(&mut *tx)
            .await?;
        due.push(row);
    }
    let storages = due;
    if automatic && storages.is_empty() {
        tx.commit().await?;
        return Ok(serde_json::json!({"idle":true}));
    }
    if single {
        let id = enqueue_storage_task(&mut tx, "pack", detail, &storages).await?;
        tx.commit().await?;
        return Ok(json!({"task_id":id}));
    }
    let mut parent_detail = detail.clone();
    parent_detail["role"] = json!("parent");
    let id = enqueue_task(&mut tx, "pack", None, parent_detail, false).await?;
    for (storage_id, storage_type, revision) in &storages {
        let mut child_detail = detail.clone();
        child_detail["role"] = json!("child");
        child_detail["storage_id"] = json!(storage_id);
        child_detail["profile_revision"] = json!(revision);
        let child = enqueue_task(&mut tx, "pack", None, child_detail, true).await?;
        let (scope, node) = if storage_type == "local" {
            ("node", node)
        } else {
            ("cluster", None)
        };
        queries::scope_pack_task(child, id, scope, node)
            .execute(&mut *tx)
            .await?;
    }
    queries::pack_parent_total(id, storages.len() as i64)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;

    Ok(json!({"task_id":id}))
}

pub(in super::super) async fn start_unpack(
    pool: &PgPool,
    single: bool,
    pack: Option<Id>,
    all: bool,
    node: Option<Uuid>,
) -> Result<Value> {
    let mut tx = pool.begin().await?;
    queries::lock_pack_policy(format!("mokyu:unpack:{all}:{pack:?}"))
        .execute(&mut *tx)
        .await?;
    if let Some(id) = queries::active_unpack_task(all.to_string(), pack.map(|v| v.to_string()))
        .fetch_optional(&mut *tx)
        .await?
    {
        tx.commit().await?;
        return Ok(json!({"task_id":id,"existing":true}));
    }
    let mut detail = json!({"pack_id":pack,"all":all});
    let storages: Vec<(Uuid, String, i64)> = if let Some(pack_id) = pack {
        queries::unpack_storage(pack_id).fetch_all(&mut *tx).await?
    } else {
        queries::pack_storages(None).fetch_all(&mut *tx).await?
    };
    if single {
        detail["upper_time"] = json!(chrono::Utc::now());
        let id = enqueue_storage_task(&mut tx, "unpack", detail, &storages).await?;
        tx.commit().await?;
        return Ok(json!({"task_id":id}));
    }
    let mut parent_detail = detail.clone();
    parent_detail["role"] = json!("parent");
    let id = enqueue_task(&mut tx, "unpack", None, parent_detail, false).await?;
    for (storage_id, storage_type, revision) in &storages {
        let mut child_detail = detail.clone();
        child_detail["role"] = json!("child");
        child_detail["storage_id"] = json!(storage_id);
        child_detail["profile_revision"] = json!(revision);
        let child = enqueue_task(&mut tx, "unpack", None, child_detail, true).await?;
        let (scope, node) = if storage_type == "local" {
            ("node", node)
        } else {
            ("cluster", None)
        };
        queries::scope_pack_task(child, id, scope, node)
            .execute(&mut *tx)
            .await?;
    }
    queries::pack_parent_total(id, storages.len() as i64)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;

    Ok(json!({"task_id":id}))
}

pub(super) async fn enqueue_storage_task(
    tx: &mut Transaction<'_, Postgres>,
    task_type: &str,
    mut detail: Value,
    storages: &[(Uuid, String, i64)],
) -> Result<Uuid> {
    detail["execution_mode"] = queries::replication::mode()
        .fetch_one(&mut **tx)
        .await?
        .into();
    detail["storages"] = json!(
        storages
            .iter()
            .map(|(id, _, revision)| { json!({"storage_id":id,"profile_revision":revision}) })
            .collect::<Vec<_>>()
    );
    detail["storage_index"] = json!(0);
    let history = detail["phase"] == "history";
    if !history && let Some((id, _, revision)) = storages.first() {
        detail["storage_id"] = json!(id);
        detail["profile_revision"] = json!(revision);
    }
    let id = enqueue_task(tx, task_type, None, detail, false).await?;
    if storages.is_empty() && !history {
        queries::complete_task(id).execute(&mut **tx).await?;
    }
    Ok(id)
}

pub(in super::super) fn advance_storage_scope(detail: &mut Value) -> Result<bool> {
    let index = if detail["phase"] == "history" {
        detail["phase"] = json!("storage");
        0
    } else {
        detail["storage_index"]
            .as_u64()
            .context("missing task storage index")?
            + 1
    };
    let next = detail["storages"]
        .as_array()
        .context("missing task storages")?
        .get(index as usize)
        .cloned();
    detail["storage_index"] = json!(index);
    if let Some(next) = &next {
        detail["storage_id"] = next["storage_id"].clone();
        detail["profile_revision"] = next["profile_revision"].clone();
    }
    Ok(next.is_none())
}

pub(in super::super) async fn advance_pack_storage(
    pool: &PgPool,
    task: Execution,
    processed: i64,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    let mut detail = queries::lock_task_detail(task)
        .fetch_optional(&mut *tx)
        .await?
        .context("task execution expired")?;
    let done = advance_storage_scope(&mut detail)?;
    queries::advance_storage_task(task.id, detail, processed, done)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    #[cfg(feature = "fault-injection")]
    if !done {
        crate::faults::point("pack-storage-advanced").await;
    }
    Ok(())
}

pub(in super::super) async fn restore_member_encoding(
    pool: &PgPool,
    c: &Chunk,
    task: Execution,
) -> Result<Id> {
    let mut tx = pool.begin().await?;
    lock_execution(&mut tx, task).await?;
    let id = queries::reserve_member_encoding(&mut tx, c, task.id).await?;
    tx.commit().await?;
    Ok(id)
}

pub(in super::super) async fn insert_pack_members(
    pool: &PgPool,
    pack: Id,
    rows: &[Mapping],
    entries: &[(Id, crate::content::pack::frame::Entry)],
) -> Result<()> {
    let mut tx = pool.begin().await?;
    ensure!(
        rows.len() == entries.len()
            && rows
                .iter()
                .zip(entries)
                .all(|(row, (id, entry))| row.chunk.id == *id
                    && row.chunk.uuid == entry.uuid
                    && row.chunk.raw_size == entry.raw_size
                    && row.chunk.hash == entry.hash),
        "pack members changed"
    );
    queries::write_pack_index(&mut tx, pack, entries).await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn commit_pack_outputs(
    pool: &PgPool,
    task: Execution,
    rows: &[Mapping],
    outputs: &[PackOutput],
    split: bool,
    settings: &crate::content::pack::Config,
    flush_seconds: f64,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    commit_pack_outputs_in(&mut tx, task, rows, outputs, split, settings, flush_seconds).await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn commit_pack_outputs_in(
    tx: &mut Transaction<'_, Postgres>,
    task: Execution,
    rows: &[Mapping],
    outputs: &[PackOutput],
    split: bool,
    settings: &crate::content::pack::Config,
    flush_seconds: f64,
) -> Result<()> {
    let mut ids: Vec<Id> = rows.iter().map(|r| r.chunk.id).collect();
    ids.sort_unstable();
    lock_execution(tx, task).await?;
    let current: Vec<(Id, Option<Id>)> =
        queries::lock_pack_layout(&ids).fetch_all(&mut **tx).await?;
    ensure!(
        current.len() == rows.len()
            && rows.iter().all(|r| current
                .iter()
                .any(|(id, p)| *id == r.chunk.id && *p == r.pack_id)),
        "pack layout changed; retry maintenance"
    );
    check_source_inputs(tx, task.id, &ids).await?;
    let before: i64 = queries::pack_layout_bytes(&ids)
        .fetch_one(&mut **tx)
        .await?;
    let added: i64 = queries::pack_added_bytes(task.id)
        .fetch_one(&mut **tx)
        .await?;
    let after: i64 = outputs
        .iter()
        .map(|o| {
            o.pack
                .as_ref()
                .and_then(|p| p.stored_size)
                .or_else(|| {
                    o.independent
                        .as_ref()
                        .and_then(|c| c.stored_size.map(i64::from))
                })
                .unwrap_or(0)
        })
        .sum();
    queries::pack_work::publish_outputs(
        tx,
        rows,
        outputs,
        split,
        config::seconds(&settings.repack_cooldown)? as f64,
    )
    .await?;
    let retained: Vec<Id> = outputs
        .iter()
        .flat_map(|o| o.chunks.iter().copied())
        .collect();
    let dead: Vec<Id> = ids
        .iter()
        .filter(|id| !retained.contains(id))
        .copied()
        .collect();
    if !dead.is_empty() {
        ensure!(
            !queries::pack_has_references(&dead)
                .fetch_one(&mut **tx)
                .await?,
            "pack acquired a new reference; retry maintenance"
        );
        queries::fail_dead_pack_chunks(&dead)
            .execute(&mut **tx)
            .await?;
    }
    let old: Vec<Id> = rows.iter().filter_map(|r| r.pack_id).collect();
    queries::retire_replaced_packs(old)
        .execute(&mut **tx)
        .await?;
    queries::record_pack_rewrite(task.id, json!({"before_bytes":before,"output_bytes":after,"temporary_added_bytes":added,"transition_bytes":before+added})).execute(&mut **tx).await?;
    Ok(())
}

pub(crate) async fn enqueue_task(
    db: &mut PgConnection,
    task_type: &str,
    bucket: Option<Uuid>,
    detail: Value,
    child: bool,
) -> Result<Uuid> {
    let id = if child {
        Uuid::new_v4()
    } else {
        crate::model::REQUEST_ID
            .try_with(|id| *id)
            .unwrap_or_else(|_| Uuid::new_v4())
    };
    insert_task(
        db,
        id,
        task_type,
        bucket,
        detail,
        crate::model::audit::current(),
    )
    .await?;
    Ok(id)
}

async fn scoped_pack_limits(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> Result<()> {
    queries::scoped_pack_limits().execute(&mut **tx).await?;
    Ok(())
}

pub(in super::super) async fn scoped_pack_page(
    pool: &PgPool,
    principal: &Principal,
    scope: &ScopeQuery,
    after: Id,
    limit: i64,
    state: Option<&str>,
) -> Result<Vec<PackSummary>> {
    let mut tx = pool.begin().await?;
    scoped_pack_limits(&mut tx).await?;
    let (ids, admin) = usage_scope(&mut tx, principal, scope).await?;
    let all = admin && scope.bucket.is_none() && scope.project.is_none();
    let rows: Vec<PackSummary> = queries::scoped_packs(after, &ids, admin, all, state, limit)
        .fetch_all(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(rows)
}

pub(in super::super) async fn scoped_pack_detail(
    pool: &PgPool,
    principal: &Principal,
    scope: &ScopeQuery,
    id: Id,
    after: Id,
    limit: i64,
) -> Result<(PackSummary, Vec<PackMember>, bool)> {
    let mut tx = pool.begin().await?;
    scoped_pack_limits(&mut tx).await?;
    let (ids, admin) = usage_scope(&mut tx, principal, scope).await?;
    let all = admin && scope.bucket.is_none() && scope.project.is_none();
    let pack: PackSummary = queries::scoped_pack(id, &ids, admin, all)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    let members: Vec<PackMember> = queries::scoped_pack_members(id, &ids, admin, all, after, limit)
        .fetch_all(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok((pack, members, !admin))
}

pub(in super::super) async fn scoped_pack_objects(
    pool: &PgPool,
    principal: &Principal,
    scope: &ScopeQuery,
    id: Id,
    cursor: Option<(Uuid, String)>,
) -> Result<Vec<PackObject>> {
    let mut tx = pool.begin().await?;
    scoped_pack_limits(&mut tx).await?;
    let (ids, admin) = usage_scope(&mut tx, principal, scope).await?;
    let rows: Vec<PackObject> = queries::scoped_pack_objects(
        id,
        ids,
        principal.user_id(),
        principal.token_id(),
        admin,
        cursor.as_ref().map(|v| v.0),
        cursor.as_ref().map(|v| &v.1),
    )
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(rows)
}
