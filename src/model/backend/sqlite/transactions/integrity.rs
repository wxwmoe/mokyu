use super::*;

pub(in super::super) async fn checkpoint_integrity(
    pool: &SqlitePool,
    task: Execution,
    mut cursor: String,
    mut detail: Detail,
    issues: Vec<Finding>,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    lock_execution(&mut tx, task).await?;
    for issue in issues {
        let inserted = queries::insert_integrity_finding(task.id, &issue)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        detail.issues += inserted;
        if inserted > 0 {
            tracing::warn!(task_id=%task.id,chunk_id=?issue.chunk_id,stream_id=?issue.stream_id,code=%issue.code,"integrity finding");
        }
    }
    let mut advanced = false;
    if detail.phase == "done" && !detail.storages.is_empty() {
        detail.storage_index += 1;
        if let Some(next) = detail.storages.get(detail.storage_index) {
            detail.storage_id = next.storage_id;
            detail.upper_chunk_id = next.upper_chunk_id.clone();
            detail.upper_object = next.upper_object.clone();
            detail.phase = "metadata".into();
            detail.finished_at = None;
            cursor = serde_json::to_string(&Cursor::default())?;
            advanced = true;
        }
    }
    let processed = i64::try_from(
        detail
            .objects_checked
            .checked_add(detail.chunks_checked)
            .context("integrity count overflow")?,
    )?;
    queries::integrity_checkpoint(
        task.id,
        cursor,
        serde_json::to_value(&detail)?,
        processed,
        detail.phase == "done",
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    if advanced {
        #[cfg(feature = "fault-injection")]
        crate::faults::point("integrity-storage-advanced").await;
    }
    Ok(())
}

pub(in super::super) async fn integrity_objects(
    pool: &SqlitePool,
    task: Uuid,
    issue: Id,
    after: Option<(Uuid, String)>,
) -> Result<Vec<Value>> {
    let mut tx = pool.begin().await?;
    let rows = queries::integrity_objects(&mut tx, task, issue, after).await?;
    tx.commit().await?;
    Ok(rows)
}

pub(in super::super) async fn recover_runtime(pool: &SqlitePool, maintenance: bool) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
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
    pool: &SqlitePool,
    id: Id,
    storage: Option<Uuid>,
    retention: Option<f64>,
) -> Result<bool> {
    let Some(mut tx) = try_cleanup_transaction(pool).await? else {
        return Ok(false);
    };
    let value = claim_stale_stream_in(&mut tx, id, storage, retention).await?;
    tx.commit().await?;
    Ok(value)
}

pub(in super::super) async fn claim_stale_stream_in(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    id: Id,
    storage: Option<Uuid>,
    retention: Option<f64>,
) -> Result<bool> {
    let Some(publication) = queries::lock_stale_stream(id, storage, retention)
        .build_query_scalar::<Option<Uuid>>()
        .fetch_optional(&mut **tx)
        .await?
    else {
        return Ok(false);
    };
    queries::abandon_cleanup_stream(id)
        .execute(&mut **tx)
        .await?;
    if publication.is_some() {
        super::super::replica_transactions::reap_admission(tx, id, None).await?;
    }

    Ok(true)
}

pub(in super::super) async fn cleanup_stream(
    pool: &SqlitePool,
    id: Id,
    batch: i64,
) -> Result<usize> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let value = cleanup_stream_in(&mut tx, id, batch).await?;
    tx.commit().await?;
    Ok(value)
}

pub(in super::super) async fn cleanup_stream_in(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    id: Id,
    batch: i64,
) -> Result<usize> {
    let Some(bucket) = queries::cleanup_stream_bucket(id)
        .fetch_optional(&mut **tx)
        .await?
    else {
        return Ok(0);
    };
    let mut accounts = lock_quota(tx, bucket).await?;
    if queries::lock_cleanup_stream(id)
        .fetch_optional(&mut **tx)
        .await?
        .is_none()
    {
        return Ok(0);
    }
    forget_quota_write(tx, &mut accounts, id).await?;
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
    let detached = queries::detach_stream_chunks(tx, id, batch).await?;
    let extents = queries::remove_stream_extents(tx, id, batch).await?;
    let mut removed = 0;
    if queries::cleanup_stream_empty(id)
        .fetch_one(&mut **tx)
        .await?
    {
        queries::detach_upload_output(id).execute(&mut **tx).await?;
        removed = queries::remove_empty_stream(id)
            .execute(&mut **tx)
            .await?
            .rows_affected();
    }

    Ok(raw.len() + extents + (candidates + detached + removed) as usize)
}
