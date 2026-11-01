use super::*;

pub(in super::super) async fn cache_stage(
    pool: &PgPool,
    input: CacheStage<'_>,
    single: bool,
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    raw_upload_processor(&mut tx, input.node).await?;
    require_chunk_writer(&mut tx, input.stream, single).await?;
    let (state, source) = queries::lock_cache_source(input.chunk.id)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(
        if input.new {
            matches!(state.as_str(), "preparing" | "uploading")
        } else {
            state == "ready"
        },
        "upload source changed"
    );
    if !input.new && source.is_none() {
        return Ok(false);
    }
    if input.new {
        ensure!(
            queries::owned_chunk(input.stream, input.chunk)
                .fetch_one(&mut *tx)
                .await?,
            crate::model::Error::OperationAborted
        );
    }
    if !input.new {
        require_sources(&mut tx, None, &[input.chunk.id]).await?;
    }
    queries::insert_pending_upload(&input, source)
        .execute(&mut *tx)
        .await?;
    queries::insert_upload_pin(input.chunk.id, source, input.stream)
        .execute(&mut *tx)
        .await?;
    if input.buffered {
        ensure!(
            input.new && input.node.is_none(),
            "buffered chunks require single mode"
        );
        queries::buffer_chunk(input.chunk, input.boot_id)
            .execute(&mut *tx)
            .await?;
    }
    queries::ready_cached_chunk(input.chunk.id)
        .execute(&mut *tx)
        .await?;
    queries::insert_cached_extent(&input)
        .execute(&mut *tx)
        .await?;
    queries::resize_cached_stream(input.stream, input.offset + i64::from(input.chunk.raw_size))
        .execute(&mut *tx)
        .await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("upload-cache-before-commit").await;
    tx.commit().await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("upload-cache-committed").await;
    Ok(true)
}

pub(in super::super) async fn cache_seal(pool: &PgPool, stream: Id, eof: bool) -> Result<bool> {
    if eof {
        queries::seal_stream_uploads(stream).execute(pool).await?;
        return Ok(true);
    }
    let rows = queries::unsealed_uploads(stream).fetch_all(pool).await?;
    let mut ids = Vec::new();
    let mut total = 0u64;
    let mut end = None;
    let mut sealed = false;
    for (id, offset, size, maximum) in rows {
        let maximum = crate::config::bytes(&maximum)?;
        if !ids.is_empty() && (end != Some(offset) || total + size as u64 > maximum) {
            sealed = true;
            break;
        }
        ids.push(id);
        total += size as u64;
        end = Some(offset + i64::from(size));
        if total == maximum {
            sealed = true;
            break;
        }
    }
    if sealed {
        queries::seal_upload_chunks(&ids).execute(pool).await?;
    }
    Ok(sealed)
}

pub(in super::super) async fn cache_flush(pool: &PgPool, node: Option<Uuid>) -> Result<Uuid> {
    let mut tx = pool.begin().await?;
    queries::deployment_mode_lock().fetch_one(&mut *tx).await?;
    let id = crate::model::REQUEST_ID
        .try_with(|id| *id)
        .unwrap_or_else(|_| Uuid::new_v4());
    let upper = queries::flush_time().fetch_one(&mut *tx).await?;
    let nodes = if node.is_some() {
        Vec::new()
    } else {
        queries::cache_nodes().fetch_all(&mut *tx).await?
    };
    insert_task(
        &mut tx,
        id,
        "cache_flush",
        None,
        json!({"upper_time":upper,"coordinator":!nodes.is_empty(),"node_id":node}),
        crate::model::audit::current(),
    )
    .await?;
    if let Some(node) = node {
        queries::scope_upload_task(id, node, None)
            .execute(&mut *tx)
            .await?;
    }
    for node in nodes {
        let child = Uuid::new_v4();
        insert_task(
            &mut tx,
            child,
            "cache_flush",
            None,
            json!({"upper_time":upper,"node_id":node}),
            crate::model::audit::current(),
        )
        .await?;
        queries::scope_upload_task(child, node, Some(id))
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(id)
}

pub(in super::super) async fn cache_enqueue(pool: &PgPool, urgent: &[Uuid]) -> Result<bool> {
    let mut tx = pool.begin().await?;
    if let Some(id) = queries::retryable_upload().fetch_optional(&mut *tx).await? {
        queries::queue_upload_retry(id).execute(&mut *tx).await?;
        tx.commit().await?;
        return Ok(true);
    }
    let Some((first, stream, source, owner)) = queries::due_upload(urgent)
        .build_query_as::<(Id, Option<Id>, Option<Id>, Option<Uuid>)>()
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(false);
    };
    let (maximum, _, _) = queries::cache_policy(first).fetch_one(&mut *tx).await?;
    let maximum = crate::config::bytes(&maximum)?;
    let id = crate::model::REQUEST_ID
        .try_with(|id| *id)
        .unwrap_or_else(|_| Uuid::new_v4());
    insert_task(
        &mut tx,
        id,
        "upload",
        None,
        json!({"first":first,"stream_id":stream,"source_pack_id":source}),
        crate::model::audit::current(),
    )
    .await?;
    if let Some(node) = owner {
        queries::scope_upload_task(id, node, None)
            .execute(&mut *tx)
            .await?;
    }
    let candidates = queries::pending_upload_segment(first, stream, source)
        .fetch_all(&mut *tx)
        .await?;
    let mut ids = Vec::new();
    let mut bytes = 0u64;
    let mut end = None;
    for (chunk, offset, size) in candidates {
        if !ids.is_empty() && (end != Some(offset) || bytes + size as u64 > maximum) {
            break;
        }
        ids.push(chunk);
        bytes += size as u64;
        end = Some(offset + i64::from(size));
    }
    if !ids.contains(&first) {
        ids = vec![first];
    }
    queries::claim_pending_uploads(&ids, id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(true)
}

pub(in super::super) async fn cache_batch(
    pool: &PgPool,
    task: Execution,
    detail: &Value,
    flush: bool,
) -> Result<CacheBatch> {
    let mut tx = pool.begin().await?;
    lock_execution(&mut tx, task).await?;
    let first = if flush {
        queries::flush_candidate(detail, task.id)
            .fetch_optional(&mut *tx)
            .await?
    } else {
        Id::from_json(&detail["first"])?
    };
    let Some(first) = first else {
        return Ok(
            if flush && queries::flush_pending(detail).fetch_one(&mut *tx).await? {
                CacheBatch::Wait
            } else {
                CacheBatch::Finished
            },
        );
    };
    let durable = queries::upload_is_durable(first)
        .fetch_one(&mut *tx)
        .await?;
    let unused = queries::upload_is_unused(first).fetch_one(&mut *tx).await?;
    if durable || unused {
        return Ok(CacheBatch::Release(first));
    }
    let (maximum, timeout, encoding) = queries::cache_policy(first).fetch_one(&mut *tx).await?;
    let encoding: Option<crate::model::cache::EncodingPolicy> =
        encoding.map(serde_json::from_value).transpose()?;
    if let Some(policy) = &encoding {
        policy.validate()?;
    }
    let maximum = crate::config::bytes(&maximum)?;
    let timeout = crate::config::seconds(&timeout)?;
    let Some((stream, offset, source, expired)) = queries::pending_upload(first, timeout)
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(CacheBatch::Finished);
    };
    if let Some(pack) = source.filter(|_| !flush) {
        let ids = queries::pending_pack_uploads(pack, task.id)
            .fetch_all(&mut *tx)
            .await?;
        record_source_inputs(&mut tx, task.id, &ids).await?;
        queries::claim_pending_uploads(&ids, task.id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        return Ok(CacheBatch::Rewrite { stream, pack, ids });
    }
    let all = queries::pending_upload_mappings(first, stream, offset, task.id, flush)
        .fetch_all(&mut *tx)
        .await?;
    let mut rows: Vec<crate::model::Mapping> = Vec::new();
    let mut raw = 0u64;
    for row in all {
        if let Some(last) = rows.last()
            && (last.offset_bytes + i64::from(last.chunk.raw_size) != row.offset_bytes
                || raw + row.chunk.raw_size as u64 > maximum
                || !queries::same_references(
                    last.chunk.id,
                    row.chunk.id,
                    row.offset_bytes - last.offset_bytes,
                )
                .fetch_one(&mut *tx)
                .await?)
        {
            break;
        }
        raw += row.chunk.raw_size as u64;
        rows.push(row);
        if flush || source.is_some() {
            break;
        }
    }
    if rows.is_empty() {
        return Ok(CacheBatch::Finished);
    }
    let mut ids: Vec<Id> = rows.iter().map(|r| r.chunk.id).collect();
    ids.sort_unstable();
    ids.dedup();
    pin_inputs(&mut tx, task.id, &ids).await?;
    let current = queries::lock_cache_inputs(&ids, task.id, flush)
        .fetch_all(&mut *tx)
        .await?;
    let mut expected: Vec<_> = rows
        .iter()
        .map(|r| {
            (
                r.chunk.id,
                r.chunk.encoding_id,
                r.pack_id,
                stream,
                r.offset_bytes,
                if r.chunk.id == first { source } else { None },
            )
        })
        .collect();
    expected.sort_unstable_by_key(|r| r.0);
    ensure!(
        current == expected,
        "upload inputs changed; retry maintenance"
    );
    queries::claim_pending_uploads(&ids, task.id)
        .execute(&mut *tx)
        .await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("upload-prepare-before-commit").await;
    tx.commit().await?;
    Ok(CacheBatch::Encode {
        stream,
        expired,
        rows,
        encoding,
    })
}

pub(in super::super) async fn cache_finish(pool: &PgPool, task: Execution) -> Result<()> {
    let mut tx = pool.begin().await?;
    lock_execution(&mut tx, task).await?;
    queries::unclaim_uploads(task.id).execute(&mut *tx).await?;
    queries::complete_upload_task(task.id)
        .execute(&mut *tx)
        .await?;
    release_task_inputs(&mut tx, task.id).await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn release_task_inputs(
    tx: &mut Transaction<'_, Postgres>,
    task: Uuid,
) -> Result<()> {
    queries::retire_task_packs(task).execute(&mut **tx).await?;
    queries::retire_task_encodings(task)
        .execute(&mut **tx)
        .await?;
    queries::release_task_inputs(task)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub(in super::super) async fn cache_release(
    pool: &PgPool,
    ids: &[Id],
    existing_pack: bool,
) -> Result<CacheRelease> {
    let mut tx = pool.begin().await?;
    let keys = queries::lock_pending_chunks(ids)
        .fetch_all(&mut *tx)
        .await?;
    let staged = queries::pending_staging_objects(ids)
        .fetch_all(&mut *tx)
        .await?;
    let mut released = Vec::new();
    let mut released_ids = std::collections::HashSet::new();
    for (id, object_key, destination) in keys {
        if queries::can_release_pending(id, existing_pack)
            .fetch_one(&mut *tx)
            .await?
        {
            queries::schedule_uploaded_pack(id)
                .execute(&mut *tx)
                .await?;
            queries::delete_pending_upload(id).execute(&mut *tx).await?;
            released_ids.insert(id);
            if let Some(key) = object_key {
                released.push((key, destination));
            }
        }
    }
    tx.commit().await?;
    // Only a released mapping permits deletion of its shared staging object.
    Ok(CacheRelease {
        keys: released,
        staged: staged
            .into_iter()
            .filter(|(id, _, _)| released_ids.contains(id))
            .map(|(_, storage, key)| (storage, key))
            .collect(),
    })
}

pub(in super::super) async fn cache_pins(
    pool: &PgPool,
    names: &[String],
    storage: Option<Uuid>,
) -> Result<Vec<String>> {
    let mut tx = pool.begin().await?;
    queries::lock_upload_pins(names, storage)
        .execute(&mut *tx)
        .await?;
    let owned = queries::owned_upload_pins(names, storage)
        .fetch_all(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(owned)
}

pub(in super::super) async fn claim_staging(
    pool: &PgPool,
    storage: Uuid,
    key: &str,
    bytes: u64,
    limit: u64,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    claim_staging_in(&mut tx, storage, key, bytes, limit).await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn claim_staging_in(
    db: &mut PgConnection,
    storage: Uuid,
    key: &str,
    bytes: u64,
    limit: u64,
) -> Result<()> {
    queries::lock_staging(format!("mokyu:staging:{storage}"))
        .execute(&mut *db)
        .await?;
    if let Some(existing) = queries::staged_size(storage, key)
        .fetch_optional(&mut *db)
        .await?
    {
        ensure!(
            existing == i64::try_from(bytes)?,
            "staging object size changed"
        );
        return Ok(());
    }
    let used = queries::staged_usage(storage).fetch_one(&mut *db).await?;
    ensure!(
        used >= 0 && (used as u64).saturating_add(bytes) <= limit,
        s3s::s3_error!(SlowDown, "shared staging quota exhausted")
    );
    queries::insert_staged_object(storage, key, i64::try_from(bytes)?)
        .execute(&mut *db)
        .await?;
    Ok(())
}
