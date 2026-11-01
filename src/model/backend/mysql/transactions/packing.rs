use super::*;

pub(in super::super) async fn scoped_pack_page(
    pool: &MySqlPool,
    principal: &Principal,
    scope: &ScopeQuery,
    after: Id,
    limit: i64,
    state: Option<&str>,
) -> Result<Vec<PackSummary>> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut connection =
        super::super::connection::DeadlineConnection::acquire(pool, deadline).await?;
    let result = tokio::time::timeout_at(deadline, async {
        let mut tx = connection.begin().await?;
        let (ids, admin) = usage_scope(&mut tx, principal, scope).await?;
        let all = admin && scope.bucket.is_none() && scope.project.is_none();
        let rows = queries::scoped_pack_page(&ids, admin, all, after, limit, state)
            .build_query_as()
            .fetch_all(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(rows)
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

pub(in super::super) async fn scoped_pack_detail(
    pool: &MySqlPool,
    principal: &Principal,
    scope: &ScopeQuery,
    id: Id,
    after: Id,
    limit: i64,
) -> Result<(PackSummary, Vec<PackMember>, bool)> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut connection =
        super::super::connection::DeadlineConnection::acquire(pool, deadline).await?;
    let result = tokio::time::timeout_at(deadline, async {
        let mut tx = connection.begin().await?;
        let (ids, admin) = usage_scope(&mut tx, principal, scope).await?;
        let all = admin && scope.bucket.is_none() && scope.project.is_none();
        let pack = queries::scoped_pack(&ids, admin, all, id)
            .build_query_as()
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
        let members = queries::scoped_pack_members(id, &ids, admin, all, after, limit)
            .build_query_as()
            .fetch_all(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok((pack, members, !admin))
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

pub(in super::super) async fn scoped_pack_objects(
    pool: &MySqlPool,
    principal: &Principal,
    scope: &ScopeQuery,
    id: Id,
    cursor: Option<(Uuid, String)>,
) -> Result<Vec<PackObject>> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut connection =
        super::super::connection::DeadlineConnection::acquire(pool, deadline).await?;
    let result = tokio::time::timeout_at(deadline, async {
        let mut tx = connection.begin().await?;
        let (ids, admin) = usage_scope(&mut tx, principal, scope).await?;
        let rows = queries::scoped_pack_objects(id, &ids, principal, admin, cursor)
            .build_query_as()
            .fetch_all(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(rows)
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

pub(in super::super) async fn start_rewrite(
    pool: &MySqlPool,
    input: &crate::model::packing::RewriteTask,
) -> Result<(Uuid, bool)> {
    let mut tx = pool.begin().await?;
    queries::lock_rewrite_storage(input.storage_id)
        .fetch_optional(&mut *tx)
        .await?
        .context("enabled storage was not found")?;
    if let Some(id) = queries::active_rewrite(input.storage_id)
        .fetch_optional(&mut *tx)
        .await?
    {
        return Ok((id, true));
    }
    ensure!(
        (input.target_algorithm == "none") == input.target_key_id.is_none(),
        "invalid rewrite encryption"
    );
    if let Some(key) = input.target_key_id {
        queries::encoding_key(key, &input.target_algorithm)
            .fetch_one(&mut *tx)
            .await?;
    }
    let upper = queries::rewrite_upper(input.storage_id)
        .fetch_one(&mut *tx)
        .await?;
    let detail = serde_json::json!({"execution_mode":"single","storage_id":input.storage_id,"upper_chunk_id":upper,"profile_revision":input.profile_revision,"target_key_id":input.target_key_id,"target_key":input.target_key,"target_algorithm":input.target_algorithm});
    let id = enqueue_task(&mut tx, "rewrite", None, detail).await?;
    if input.shards == 0 || !upper.is_valid() {
        queries::complete_task(id).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok((id, false))
}

pub(in super::super) async fn commit_rewrite_chunk(
    pool: &MySqlPool,
    task: Execution,
    source: &Chunk,
    physical: &Chunk,
) -> Result<()> {
    ensure!(
        source.id == physical.id
            && source.raw_size == physical.raw_size
            && source.hash == physical.hash,
        "rewrite content identity changed"
    );
    let mut tx = pool.begin().await?;
    lock_execution(&mut tx, task).await?;
    ensure!(
        super::super::tier_read_queries::rewrite_source(
            &mut *tx,
            crate::model::tiering::Unit::Chunk,
            source.encoding_id,
            source.address(),
            source.storage_id,
            physical.storage_id
        )
        .await?,
        crate::model::Error::OperationAborted
    );
    let current = queries::lock_rewrite_chunk(source.id)
        .fetch_optional(&mut *tx)
        .await?;
    ensure!(
        current == Some((None, source.encoding_id)),
        "chunk source changed during rewrite"
    );
    check_source_inputs(&mut tx, task.id, &[source.id]).await?;
    let owners: Vec<Uuid> = queries::pack_input_owners(&[source.id])
        .build_query_scalar()
        .fetch_all(&mut *tx)
        .await?;
    ensure!(
        owners == [task.id],
        "rewrite input is not owned by this task"
    );
    ensure!(
        queries::rewrite_encoding_owner(physical.encoding_id, task.id)
            .fetch_one(&mut *tx)
            .await?,
        "encoding output changed"
    );
    ensure!(
        queries::register_encoding(physical)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        "encoding output changed"
    );
    queries::refresh_encoding_fingerprint(&mut tx, physical.encoding_id).await?;
    ensure!(
        queries::retire_rewrite_encoding(source.encoding_id)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        "source encoding changed during rewrite"
    );
    ensure!(
        queries::complete_encoding(physical.encoding_id)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        "encoding output changed"
    );
    ensure!(
        queries::publish_chunk_encoding(source.id, physical.encoding_id)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        "chunk output changed"
    );
    queries::advance_task(task.id, &source.id.to_string(), 1)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn commit_rewrite_pack(
    pool: &MySqlPool,
    task: Execution,
    old: &Pack,
    new: &Pack,
    current: &[Id],
    members: &[Chunk],
    first: Id,
) -> Result<()> {
    ensure!(
        old.raw_size == new.raw_size
            && old.member_count == new.member_count
            && members.len() == new.member_count as usize,
        "pack content identity changed"
    );
    let mut tx = pool.begin().await?;
    lock_execution(&mut tx, task).await?;
    ensure!(
        super::super::tier_read_queries::rewrite_source(
            &mut *tx,
            crate::model::tiering::Unit::Pack,
            old.id,
            old.address(),
            old.storage_id,
            new.storage_id
        )
        .await?,
        crate::model::Error::OperationAborted
    );
    let locked = queries::lock_rewrite_members(old.id)
        .fetch_all(&mut *tx)
        .await?;
    ensure!(locked == current, "pack membership changed during rewrite");
    check_source_inputs(&mut tx, task.id, current).await?;
    let owners: Vec<Uuid> = queries::pack_input_owners(current)
        .build_query_scalar()
        .fetch_all(&mut *tx)
        .await?;
    ensure!(
        owners.len() == current.len() && owners.iter().all(|id| *id == task.id),
        "rewrite inputs are not owned by this task"
    );
    let member_ids: Vec<_> = members.iter().map(|c| c.id).collect();
    ensure!(
        queries::pack_member_ids(old.id).fetch_all(&mut *tx).await? == member_ids,
        "pack content members changed during rewrite"
    );
    ensure!(
        queries::pack_metadata(new)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        "new pack changed during rewrite"
    );
    queries::refresh_pack_fingerprint(&mut tx, new.id).await?;
    queries::insert_pack_members(new.id, members.iter())
        .build()
        .execute(&mut *tx)
        .await?;
    ensure!(
        queries::publish_prepared_pack(new.id, task.id)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        "new pack changed during rewrite"
    );
    ensure!(
        queries::replace_rewrite_pack(old.id, new.id)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == current.len() as u64,
        "pack membership changed during rewrite"
    );
    ensure!(
        queries::retire_rewrite_pack(old.id)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        "source pack changed during rewrite"
    );
    // Packs can interleave chunk IDs; advance only past this pack's scan key.
    queries::advance_task(task.id, &first.to_string(), 1)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn start_pack(
    pool: &MySqlPool,
    policy: &str,
    automatic: bool,
) -> Result<Value> {
    ensure!(
        matches!(policy, "pack" | "reuse" | "reclaim" | "repack" | "range"),
        "invalid pack policy"
    );
    let mut tx = pool.begin().await?;
    queries::lock_pack_policy(policy)
        .fetch_one(&mut *tx)
        .await?;
    if let Some(id) = queries::active_pack_task(policy)
        .fetch_optional(&mut *tx)
        .await?
    {
        return Ok(serde_json::json!({"task_id":id,"existing":true}));
    }
    let storages = queries::pack_storages(Some(policy))
        .fetch_all(&mut *tx)
        .await?;
    let detail =
        serde_json::json!({"policy":policy,"upper_time":chrono::Utc::now(),"automatic":automatic});
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
    let id = enqueue_storage_task(&mut tx, "pack", detail, &storages).await?;
    tx.commit().await?;
    Ok(serde_json::json!({"task_id":id}))
}

pub(in super::super) async fn start_unpack(
    pool: &MySqlPool,
    pack: Option<Id>,
    all: bool,
) -> Result<Value> {
    let mut tx = pool.begin().await?;
    queries::lock_pack_policy("pack")
        .fetch_one(&mut *tx)
        .await?;
    if let Some(id) = queries::active_unpack_task(all, pack)
        .fetch_optional(&mut *tx)
        .await?
    {
        return Ok(serde_json::json!({"task_id":id,"existing":true}));
    }
    let storages = if let Some(pack) = pack {
        queries::unpack_storage(pack).fetch_all(&mut *tx).await?
    } else {
        queries::pack_storages(None).fetch_all(&mut *tx).await?
    };
    let detail = serde_json::json!({"pack_id":pack,"all":all,"upper_time":chrono::Utc::now()});
    let id = enqueue_storage_task(&mut tx, "unpack", detail, &storages).await?;
    tx.commit().await?;
    Ok(serde_json::json!({"task_id":id}))
}

pub(super) async fn enqueue_storage_task(
    connection: &mut MySqlConnection,
    task_type: &str,
    mut detail: Value,
    storages: &[(Uuid, i64)],
) -> Result<Uuid> {
    detail["execution_mode"] = serde_json::json!("single");
    detail["storages"] = serde_json::json!(
        storages
            .iter()
            .map(|(id, revision)| serde_json::json!({"storage_id":id,"profile_revision":revision}))
            .collect::<Vec<_>>()
    );
    detail["storage_index"] = serde_json::json!(0);
    let history = detail["phase"] == "history";
    if !history && let Some((id, revision)) = storages.first() {
        detail["storage_id"] = serde_json::json!(id);
        detail["profile_revision"] = serde_json::json!(revision);
    }
    let id = enqueue_task(connection, task_type, None, detail).await?;
    if storages.is_empty() && !history {
        queries::complete_task(id).execute(connection).await?;
    }
    Ok(id)
}

pub(in super::super) async fn advance_pack_storage(
    pool: &MySqlPool,
    task: Execution,
    processed: i64,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    let mut detail = queries::lock_task_detail(task)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(crate::model::Error::OperationAborted)?;
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

pub(super) fn advance_storage_scope(detail: &mut Value) -> Result<bool> {
    let index = if detail["phase"] == "history" {
        detail["phase"] = serde_json::json!("storage");
        0
    } else {
        detail["storage_index"]
            .as_u64()
            .context("missing task storage index")?
            .checked_add(1)
            .context("task storage index overflow")?
    };
    let next = detail["storages"]
        .as_array()
        .context("missing task storages")?
        .get(usize::try_from(index)?)
        .cloned();
    detail["storage_index"] = serde_json::json!(index);
    if let Some(next) = &next {
        detail["storage_id"] = next["storage_id"].clone();
        detail["profile_revision"] = next["profile_revision"].clone();
    }
    Ok(next.is_none())
}

pub(in super::super) async fn finish_pack_change(
    pool: &MySqlPool,
    task: Execution,
    pack: Id,
    generation: i64,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    lock_execution(&mut tx, task).await?;
    queries::finish_pack_change(pack, generation)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn defer_pack_queue(
    pool: &MySqlPool,
    task: Execution,
    stream: Id,
    generation: i64,
    later: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    lock_execution(&mut tx, task).await?;
    queries::defer_pack_queue(stream, generation, later)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn finish_pack_queue(
    pool: &MySqlPool,
    task: Execution,
    stream: Id,
    generation: i64,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    lock_execution(&mut tx, task).await?;
    queries::finish_pack_queue(stream, generation)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn advance_pack_queue(
    pool: &MySqlPool,
    task: Execution,
    stream: Id,
    generation: i64,
    cursor: i64,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    lock_execution(&mut tx, task).await?;
    queries::advance_pack_queue(stream, generation, cursor)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn record_range(
    pool: &MySqlPool,
    task: Execution,
    pack: Id,
    processed: usize,
    decision: Value,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    lock_execution(&mut tx, task).await?;
    queries::mark_range_checked(pack).execute(&mut *tx).await?;
    queries::record_range_result(
        task.id,
        pack.to_string(),
        i64::try_from(processed)?,
        decision,
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn pin_pack_inputs(
    pool: &MySqlPool,
    task: Execution,
    rows: &[Mapping],
) -> Result<()> {
    let mut ids: Vec<_> = rows.iter().map(|r| r.chunk.id).collect();
    ids.sort_unstable();
    ids.dedup();
    if ids.is_empty() {
        return Ok(());
    }
    let mut tx = pool.begin().await?;
    lock_execution(&mut tx, task).await?;
    pin_inputs(&mut tx, task.id, &ids).await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn pin_inputs(db: &mut MySqlConnection, task: Uuid, ids: &[Id]) -> Result<()> {
    let locked: Vec<(Id, Option<Id>, Uuid)> = queries::lock_pack_layout(ids)
        .build_query_as()
        .fetch_all(&mut *db)
        .await?;
    ensure!(
        locked.len() == ids.len(),
        "pack inputs changed; retry maintenance"
    );
    let owners: Vec<Uuid> = queries::pack_input_owners(ids)
        .build_query_scalar()
        .fetch_all(&mut *db)
        .await?;
    ensure!(
        owners.iter().all(|id| *id == task),
        "pack input is already owned"
    );
    record_source_inputs(db, task, ids).await?;
    queries::insert_pack_inputs(task, ids)
        .build()
        .execute(db)
        .await?;
    Ok(())
}

pub(in super::super) async fn prepare_encoding(
    pool: &MySqlPool,
    physical: &Chunk,
    task: Execution,
) -> Result<Id> {
    let mut tx = pool.begin().await?;
    lock_execution(&mut tx, task).await?;
    if let Some(key) = physical.block_key_id {
        queries::encoding_key(key, &physical.algorithm)
            .fetch_one(&mut *tx)
            .await?;
    }
    let id = Id::try_from(
        queries::prepare_encoding(physical, task)
            .execute(&mut *tx)
            .await?
            .last_insert_id(),
    )?;
    queries::refresh_encoding_fingerprint(&mut tx, id).await?;
    tx.commit().await?;
    Ok(id)
}

pub(in super::super) async fn prepare_pack(
    pool: &MySqlPool,
    storage: Uuid,
    algorithm: &str,
    key: Option<Uuid>,
    bytes: i64,
    count: i32,
    task: Execution,
) -> Result<Pack> {
    let mut tx = pool.begin().await?;
    lock_execution(&mut tx, task).await?;
    if let Some(key) = key {
        queries::encoding_key(key, algorithm)
            .fetch_one(&mut *tx)
            .await?;
    }
    let id = Id::try_from(
        queries::prepare_pack(storage, algorithm, key, bytes, count, task)
            .execute(&mut *tx)
            .await?
            .last_insert_id(),
    )?;
    queries::refresh_pack_fingerprint(&mut tx, id).await?;
    let pack = queries::pack_record(id).fetch_one(&mut *tx).await?;
    tx.commit().await?;
    Ok(pack)
}

pub(in super::super) async fn pack_metadata(pool: &MySqlPool, pack: &Pack) -> Result<()> {
    let mut tx = pool.begin().await?;
    ensure!(
        queries::pack_metadata(pack)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        "pack output changed"
    );
    queries::refresh_pack_fingerprint(&mut tx, pack.id).await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn insert_pack_members(
    pool: &MySqlPool,
    pack: Id,
    rows: &[Mapping],
) -> Result<()> {
    let mut tx = pool.begin().await?;
    let record = queries::lock_prepared_pack(pack)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(
        rows.len() == record.member_count as usize
            && rows.iter().all(|r| r.chunk.storage_id == record.storage_id)
            && rows
                .iter()
                .map(|r| i64::from(r.chunk.raw_size))
                .sum::<i64>()
                == record.raw_size,
        "pack members changed"
    );
    queries::insert_pack_members(pack, rows.iter().map(|row| &row.chunk))
        .build()
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

async fn retire_pack_encodings(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    chunks: &[Id],
    keep: Option<Id>,
) -> Result<()> {
    // A secondary-index UPDATE can lock the first encoding of the next chunk.
    let encodings = queries::ready_pack_encodings(chunks, keep)
        .build_query_scalar::<Id>()
        .fetch_all(&mut **tx)
        .await?;
    if !encodings.is_empty() {
        queries::retire_pack_encodings(chunks, &encodings)
            .build()
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

pub(in super::super) async fn publish_pack_outputs_in(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    task: Execution,
    rows: &[Mapping],
    outputs: &[PackOutput],
    split: bool,
    range: bool,
    cooldown: f64,
    range_after: &str,
) -> Result<()> {
    for output in outputs {
        ensure!(
            !output.chunks.is_empty() && output.pack.is_some() != output.independent.is_some(),
            "invalid pack output"
        );
        if let Some(pack) = &output.pack {
            ensure!(
                output.chunks.len() == pack.member_count as usize
                    && output.chunks.iter().all(|id| rows
                        .iter()
                        .any(|r| r.chunk.id == *id && r.chunk.storage_id == pack.storage_id)),
                "pack members changed"
            );
            ensure!(
                queries::pack_member_ids(pack.id)
                    .fetch_all(&mut **tx)
                    .await?
                    == output.chunks,
                "pack members changed"
            );
            for pair in output.chunks.windows(2) {
                let a = rows.iter().find(|r| r.chunk.id == pair[0]).unwrap();
                let b = rows.iter().find(|r| r.chunk.id == pair[1]).unwrap();
                ensure!(
                    queries::same_references(pair[0], pair[1], b.offset_bytes - a.offset_bytes)
                        .fetch_one(&mut **tx)
                        .await?,
                    "pack reference boundaries changed; retry maintenance"
                );
            }
            ensure!(
                queries::publish_prepared_pack(pack.id, task.id)
                    .execute(&mut **tx)
                    .await?
                    .rows_affected()
                    == 1,
                "pack output changed"
            );
            retire_pack_encodings(tx, &output.chunks, None).await?;
        } else if let Some(chunk) = &output.independent {
            ensure!(
                output.chunks == [chunk.id],
                "independent output members changed"
            );
            let (state, owner) = queries::lock_output_encoding(chunk.encoding_id, chunk.id)
                .fetch_one(&mut **tx)
                .await?;
            if state != "ready" {
                ensure!(
                    state == "uploading" && owner == Some(task.id),
                    "encoding output changed"
                );
                retire_pack_encodings(tx, &output.chunks, Some(chunk.encoding_id)).await?;
                ensure!(
                    queries::complete_encoding(chunk.encoding_id)
                        .execute(&mut **tx)
                        .await?
                        .rows_affected()
                        == 1,
                    "encoding output changed"
                );
            }
            // Reused ready encodings keep their persisted metadata, even if the input is stale.
            ensure!(
                queries::publish_chunk_encoding(chunk.id, chunk.encoding_id)
                    .execute(&mut **tx)
                    .await?
                    .rows_affected()
                    == 1,
                "chunk output changed"
            );
        }
        queries::publish_pack_layout(
            &output.chunks,
            output.pack.as_ref().map(|p| p.id),
            split,
            if range {
                crate::config::seconds(range_after)? as f64
            } else {
                cooldown
            },
            range,
        )
        .build()
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

pub(in super::super) async fn commit_pack_outputs(
    pool: &MySqlPool,
    task: Execution,
    rows: &[Mapping],
    outputs: &[PackOutput],
    split: bool,
    settings: &crate::content::pack::Config,
    flush_seconds: f64,
) -> Result<()> {
    let mut ids: Vec<_> = rows.iter().map(|r| r.chunk.id).collect();
    ids.sort_unstable();
    let mut retained: Vec<_> = outputs
        .iter()
        .flat_map(|o| o.chunks.iter().copied())
        .collect();
    retained.sort_unstable();
    ensure!(
        !ids.is_empty()
            && ids.windows(2).all(|p| p[0] != p[1])
            && retained.windows(2).all(|p| p[0] != p[1])
            && retained.iter().all(|id| ids.binary_search(id).is_ok()),
        "invalid pack output members"
    );
    let mut tx = pool.begin().await?;
    lock_execution(&mut tx, task).await?;
    let current: Vec<(Id, Option<Id>, Uuid)> = queries::lock_pack_layout(&ids)
        .build_query_as()
        .fetch_all(&mut *tx)
        .await?;
    ensure!(
        current.len() == rows.len()
            && rows
                .iter()
                .all(|r| current.iter().any(|(id, p, s)| *id == r.chunk.id
                    && *p == r.pack_id
                    && Some(*s) == r.chunk.storage_id)),
        "pack layout changed; retry maintenance"
    );
    let owners: Vec<Uuid> = queries::pack_input_owners(&ids)
        .build_query_scalar()
        .fetch_all(&mut *tx)
        .await?;
    ensure!(
        owners.len() == ids.len() && owners.iter().all(|id| *id == task.id),
        "pack inputs are not owned by this task"
    );
    check_source_inputs(&mut tx, task.id, &ids).await?;
    let before: i64 = queries::pack_layout_bytes(&ids)
        .build_query_scalar()
        .fetch_one(&mut *tx)
        .await?;
    let added = queries::pack_added_bytes(task.id)
        .fetch_one(&mut *tx)
        .await?;
    let after: i64 = outputs.iter().map(PackOutput::size).sum();
    let (upload, range) = queries::pack_task_policy(task.id)
        .fetch_one(&mut *tx)
        .await?;
    let cooldown = crate::config::seconds(&settings.repack_cooldown)? as f64;
    if !split && !upload {
        ensure!(
            queries::pack_exclusive(
                &ids,
                cooldown,
                crate::config::seconds(&settings.range_repack_window)? as f64,
                flush_seconds
            )
            .build_query_scalar::<bool>()
            .fetch_one(&mut *tx)
            .await?,
            "pack references changed; retry maintenance"
        );
    }
    publish_pack_outputs_in(
        &mut tx,
        task,
        rows,
        outputs,
        split,
        range,
        cooldown,
        &settings.range_repack_after,
    )
    .await?;
    let dead: Vec<_> = ids
        .iter()
        .filter(|id| retained.binary_search(id).is_err())
        .copied()
        .collect();
    if !dead.is_empty() {
        ensure!(
            !queries::pack_has_references(&dead)
                .build_query_scalar::<bool>()
                .fetch_one(&mut *tx)
                .await?,
            "pack acquired a new reference; retry maintenance"
        );
        queries::fail_dead_pack_chunks(&dead)
            .build()
            .execute(&mut *tx)
            .await?;
    }
    if split {
        queries::schedule_repacked_streams(&retained, cooldown)
            .build()
            .execute(&mut *tx)
            .await?;
    }
    let old: Vec<_> = rows.iter().filter_map(|r| r.pack_id).collect();
    queries::retire_replaced_packs(&old)
        .build()
        .execute(&mut *tx)
        .await?;
    queries::record_pack_rewrite(task.id, serde_json::json!({"before_bytes":before,"output_bytes":after,"temporary_added_bytes":added,"transition_bytes":before+added})).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}
