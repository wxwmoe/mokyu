use super::*;

pub(in super::super) async fn cache_stage(pool: &MySqlPool, input: CacheStage<'_>) -> Result<bool> {
    let timeout = i64::try_from(input.timeout)?;
    ensure!(
        crate::storage::parse_upload_cache_name(input.key).is_some_and(|id| !id.is_nil()),
        "invalid local upload cache key"
    );
    ensure!(
        input.chunk.storage_id == Some(input.storage),
        "upload cache storage changed"
    );
    let mut tx = pool.begin().await?;
    ensure!(
        queries::lock_cache_stream(input.stream, input.storage)
            .fetch_optional(&mut *tx)
            .await?
            .is_some(),
        crate::model::Error::OperationAborted
    );
    let (state, source, storage, encoding) = queries::lock_cache_source(input.chunk.id)
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
    ensure!(
        storage == input.storage && encoding == input.chunk.encoding_id,
        "upload source changed"
    );
    if !input.new && source.is_none() {
        return Ok(false);
    }
    if !input.new {
        require_sources(&mut tx, None, &[input.chunk.id]).await?;
    }
    queries::insert_pending_upload(&input, source, timeout)
        .execute(&mut *tx)
        .await?;
    queries::pin_chunk(input.chunk.id, input.stream)
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
    queries::insert_chunk_extent(
        input.stream,
        input.offset,
        input.chunk.raw_size,
        input.chunk.id,
    )
    .execute(&mut *tx)
    .await?;
    if !input.new {
        queries::changed_references(&mut tx, &[input.chunk.id], true).await?;
    }
    queries::resize_writing_stream(
        input.stream,
        input
            .offset
            .checked_add(i64::from(input.chunk.raw_size))
            .context("cache extent overflow")?,
    )
    .execute(&mut *tx)
    .await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("upload-cache-before-commit").await;
    tx.commit().await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("upload-cache-committed").await;
    Ok(true)
}

pub(in super::super) async fn cache_seal(pool: &MySqlPool, stream: Id, eof: bool) -> Result<bool> {
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
        queries::seal_upload_chunks(&ids)
            .build()
            .execute(pool)
            .await?;
    }
    Ok(sealed)
}

pub(in super::super) async fn cache_flush(pool: &MySqlPool, node: Option<Uuid>) -> Result<Uuid> {
    ensure!(node.is_none(), "node flush requires a cluster database");
    let mut tx = pool.begin().await?;
    let upper = queries::flush_time().fetch_one(&mut *tx).await?;
    let id = enqueue_task(
        &mut tx,
        "cache_flush",
        None,
        serde_json::json!({"upper_time":upper}),
    )
    .await?;
    tx.commit().await?;
    Ok(id)
}

pub(in super::super) async fn cache_enqueue(pool: &MySqlPool, urgent: &[Uuid]) -> Result<bool> {
    let mut tx = pool.begin().await?;
    if let Some(id) = queries::retryable_upload().fetch_optional(&mut *tx).await? {
        queries::queue_upload_retry(id).execute(&mut *tx).await?;
        tx.commit().await?;
        return Ok(true);
    }
    let Some((first, stream, source)) = queries::due_upload(urgent)
        .build_query_as::<(Id, Option<Id>, Option<Id>)>()
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(false);
    };
    let id = enqueue_task(
        &mut tx,
        "upload",
        None,
        serde_json::json!({"first":first,"stream_id":stream,"source_pack_id":source}),
    )
    .await?;
    let (maximum, _, _) = queries::cache_policy(first).fetch_one(&mut *tx).await?;
    let maximum = crate::config::bytes(&maximum)?;
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
        .build()
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(true)
}

pub(in super::super) async fn cache_batch(
    pool: &MySqlPool,
    task: Execution,
    detail: &Value,
    flush: bool,
) -> Result<CacheBatch> {
    let mut tx = pool.begin().await?;
    lock_execution(&mut tx, task).await?;
    let upper = if flush {
        Some(serde_json::from_value::<chrono::DateTime<chrono::Utc>>(
            detail["upper_time"].clone(),
        )?)
    } else {
        None
    };
    let first = if let Some(upper) = upper {
        queries::flush_candidate(upper, task.id)
            .fetch_optional(&mut *tx)
            .await?
    } else {
        Id::from_json(&detail["first"])?
    };
    let Some(first) = first else {
        return Ok(if let Some(upper) = upper {
            if queries::flush_pending(upper).fetch_one(&mut *tx).await? {
                CacheBatch::Wait
            } else {
                CacheBatch::Finished
            }
        } else {
            CacheBatch::Finished
        });
    };
    if queries::upload_is_durable(first)
        .fetch_one(&mut *tx)
        .await?
        || queries::upload_is_unused(first).fetch_one(&mut *tx).await?
    {
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
    let Some((stream, offset, source, expired)) =
        queries::pending_upload(first, i64::try_from(timeout)?)
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
            .build()
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        return Ok(CacheBatch::Rewrite { stream, pack, ids });
    }
    let all: Vec<crate::model::Mapping> =
        queries::pending_upload_mappings(first, stream, offset, task.id, flush)
            .build_query_as()
            .fetch_all(&mut *tx)
            .await?;
    let mut rows: Vec<crate::model::Mapping> = Vec::new();
    let mut raw = 0u64;
    for row in all {
        if !rows.is_empty()
            && (raw + row.chunk.raw_size as u64 > maximum
                || rows.len() == crate::content::pack::MAX_MEMBERS)
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
    let mut ids: Vec<_> = rows.iter().map(|r| r.chunk.id).collect();
    ids.sort_unstable();
    ids.dedup();
    pin_inputs(&mut tx, task.id, &ids).await?;
    let current: Vec<(Id, Id, Option<Id>, Option<Id>, i64, Option<Id>)> =
        queries::lock_cache_inputs(&ids, task.id, flush)
            .build_query_as()
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
        .build()
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
