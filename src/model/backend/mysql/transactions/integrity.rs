use super::*;

pub(in super::super) async fn start_integrity(
    pool: &MySqlPool,
    input: IntegrityRequest,
    bucket: Option<Bucket>,
) -> Result<(Uuid, usize)> {
    let mut tx = pool.begin().await?;
    queries::lock_integrity_start().execute(&mut *tx).await?;
    ensure!(
        !queries::integrity_busy().fetch_one(&mut *tx).await?,
        s3s::s3_error!(OperationAborted, "an integrity check is already active")
    );
    let storages = if let Some(storage) = bucket.as_ref().and_then(|b| b.storage_id) {
        queries::integrity_storages(Some(storage))
            .fetch_all(&mut *tx)
            .await?
    } else if bucket.is_some() {
        Vec::new()
    } else {
        queries::integrity_storages(None)
            .fetch_all(&mut *tx)
            .await?
    };
    let storages: Vec<Option<Uuid>> = if storages.is_empty() {
        vec![None]
    } else {
        storages.into_iter().map(Some).collect()
    };
    let mut detail = Detail {
        storage_id: None,
        mode: input.mode,
        phase: "metadata".into(),
        bucket_id: bucket.as_ref().map(|b| b.id),
        bucket: input.bucket,
        key: input.key,
        upper_chunk_id: "0".into(),
        upper_object: None,
        objects_checked: 0,
        chunks_checked: 0,
        bytes_checked: 0,
        issues: 0,
        skipped: 0,
        finished_at: None,
        storages: Vec::with_capacity(storages.len()),
        storage_index: 0,
    };
    for storage in storages {
        detail.storage_id = storage;
        let upper_chunk_id = queries::integrity_upper_chunk(storage)
            .fetch_one(&mut *tx)
            .await?
            .to_string();
        let upper_object = queries::integrity_upper_object(&mut tx, &detail).await?;
        detail.storages.push(StorageSnapshot {
            storage_id: storage,
            upper_chunk_id,
            upper_object,
        });
    }
    let first = &detail.storages[0];
    detail.storage_id = first.storage_id;
    detail.upper_chunk_id = first.upper_chunk_id.clone();
    detail.upper_object = first.upper_object.clone();
    let id = enqueue_task(
        &mut tx,
        "integrity",
        detail.bucket_id,
        serde_json::to_value(detail)?,
    )
    .await?;
    tx.commit().await?;
    Ok((id, 0))
}

pub(in super::super) async fn checkpoint_integrity(
    pool: &MySqlPool,
    task: Execution,
    mut cursor: String,
    mut detail: Detail,
    issues: Vec<Finding>,
) -> Result<()> {
    let mut tx = pool.begin().await?;
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

pub(super) async fn usage_scope(
    connection: &mut MySqlConnection,
    principal: &Principal,
    scope: &ScopeQuery,
) -> Result<(Vec<Uuid>, bool)> {
    let admin = queries::validate_identity(principal, connection, true).await?;
    let ids = queries::usage_scope_buckets(
        principal.user_id(),
        admin,
        principal.token_id(),
        scope.bucket,
        scope.project,
    )
    .fetch_all(connection)
    .await?;
    ensure!(
        scope.bucket.is_none() || !ids.is_empty(),
        s3s::s3_error!(AccessDenied)
    );
    Ok((ids, admin))
}
