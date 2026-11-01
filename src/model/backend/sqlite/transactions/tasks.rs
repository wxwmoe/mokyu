use super::*;

pub(in super::super) async fn lock_execution(
    connection: &mut SqliteConnection,
    task: Execution,
) -> Result<()> {
    ensure!(
        queries::lock_task_execution(task)
            .fetch_optional(connection)
            .await?
            .is_some(),
        crate::model::Error::OperationAborted
    );
    Ok(())
}

pub(in super::super) async fn purge_batch(
    pool: &SqlitePool,
    task: Execution,
    bucket: Uuid,
    cursor: Option<&str>,
    batch: i64,
    active: &[Uuid],
) -> Result<bool> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    lock_execution(&mut tx, task).await?;
    let members = bucket_members(&mut tx, bucket).await?;
    let row = queries::bucket_by_id(bucket).fetch_one(&mut *tx).await?;
    ensure!(
        row.state == "purging",
        crate::model::Error::OperationAborted
    );
    let objects = queries::lock_purge_objects(bucket, cursor, batch)
        .fetch_all(&mut *tx)
        .await?;
    if !objects.is_empty() {
        let mut last = String::new();
        for (key, stream) in &objects {
            last = String::from_utf8(key.clone())?;
            remove_object_reference(&mut tx, bucket, &last, *stream).await?;
            if let Some(stream) = stream {
                queries::retire_stream(*stream).execute(&mut *tx).await?;
            }
        }
        queries::advance_task(task.id, &last, objects.len() as i64)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        return Ok(false);
    }
    queries::purge_upload_history(&mut tx, bucket, active, batch).await?;
    if queries::bucket_has_streams(bucket)
        .fetch_one(&mut *tx)
        .await?
    {
        tx.commit().await?;
        return Ok(false);
    }
    remove_empty_bucket(&mut tx, &row, &members).await?;
    queries::complete_task(task.id).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(true)
}

pub(in super::super) async fn remove_object_reference(
    connection: &mut SqliteConnection,
    bucket: Uuid,
    key: &str,
    old: Option<Id>,
) -> Result<()> {
    let token = Id::default();
    queries::remove_object(bucket, key, token)
        .execute(&mut *connection)
        .await?;
    supersede_object_quota(connection, bucket, key, token).await?;
    let old = if let Some(id) = old {
        Some((
            id,
            queries::stream_size_etag(id)
                .fetch_one(&mut *connection)
                .await?
                .0,
        ))
    } else {
        None
    };
    let mut accounts = lock_quota(connection, bucket).await?;
    replace_object_quota(connection, &mut accounts, bucket, key, old, None).await
}

pub(super) async fn record_media_operation(
    connection: &mut Transaction<'_, Sqlite>,
    outcome: Outcome,
) -> Result<()> {
    if let Ok((id, bucket, key, action, count)) =
        RECEIPT.try_with(|r| (r.id, r.bucket, r.key.clone(), r.action, r.count))
    {
        ensure!(
            queries::save_media_operation(id, &outcome)
                .execute(&mut **connection)
                .await?
                .rows_affected()
                == 1,
            "media operation receipt is missing"
        );
        checkpoint_event(connection,"object.change",&key,serde_json::json!({"bucket_id":bucket,"operation":action,"version":outcome.output_version,"task_id":outcome.task_id,"operation_id":id})).await?;
        if count > 1
            && let Some(id) = crate::model::audit::current()
        {
            queries::partial_audit(id)
                .execute(&mut **connection)
                .await?;
        }
    }
    Ok(())
}

pub(super) async fn complete_media_operation(
    connection: &mut Transaction<'_, Sqlite>,
    version: Option<Id>,
) -> Result<()> {
    record_media_operation(
        connection,
        Outcome {
            status: 200,
            code: None,
            output_version: version,
            task_id: None,
        },
    )
    .await
}

pub(in super::super) async fn begin_media_operation(
    pool: &SqlitePool,
    principal: &Principal,
    user: Uuid,
    client: Uuid,
    hash: &[u8],
) -> Result<(Uuid, Vec<u8>)> {
    ensure!(hash.len() == 32, s3s::s3_error!(InvalidArgument));
    ensure!(
        principal.user_id().is_none_or(|id| id == user),
        s3s::s3_error!(AccessDenied)
    );
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    queries::validate_identity(principal, &mut tx).await?;
    queries::insert_media_operation(Uuid::new_v4(), user, principal.token_id(), client, hash)
        .execute(&mut *tx)
        .await?;
    let row = queries::find_media_operation(user, principal.token_id(), client)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(row)
}

pub(in super::super) async fn audit_object_batch(
    pool: &SqlitePool,
    bucket: Uuid,
    detail: Value,
    partial: bool,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    checkpoint_event(&mut tx, "object.batch", &bucket.to_string(), detail).await?;
    if partial && let Some(id) = crate::model::audit::current() {
        queries::partial_audit(id).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn task_page(
    pool: &SqlitePool,
    principal: &Principal,
    filter: crate::model::task::JobQuery,
    cursor: Option<(chrono::DateTime<chrono::Utc>, Uuid)>,
    limit: i64,
) -> Result<Vec<Value>> {
    use sqlx::Row;
    ensure!(limit >= 0, s3s::s3_error!(InvalidArgument));
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut connection =
        super::super::connection::DeadlineConnection::acquire(pool, deadline).await?;
    let result = tokio::time::timeout_at(deadline, async {
        let mut tx = connection.begin().await?;
        ensure!(
            queries::validate_identity(principal, &mut tx).await?,
            denied()
        );
        let needle = filter.actor.as_ref().map(|value| value.to_lowercase());
        let mut after = cursor;
        let mut values = Vec::new();
        while (values.len() as i64) < limit {
            let batch = if needle.is_some() { 128 } else { limit };
            let rows = queries::task_page(&filter, after, batch)
                .build()
                .fetch_all(&mut *tx)
                .await?;
            let more = rows.len() as i64 == batch;
            if let Some(last) = rows.last() {
                after = Some((
                    queries::datetime(last.try_get("created_at")?)?,
                    last.try_get("id")?,
                ));
            }
            for row in rows {
                if needle.as_ref().is_none_or(|needle| {
                    row.get::<String, _>("created_by_label")
                        .to_lowercase()
                        .contains(needle)
                }) {
                    values.push(queries::task_value(row, true)?);
                    if values.len() as i64 == limit {
                        break;
                    }
                }
            }
            if needle.is_none() || !more {
                break;
            }
        }
        tx.commit().await?;
        Ok(values)
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

async fn release_task_inputs(connection: &mut SqliteConnection, task: Uuid) -> Result<()> {
    queries::retire_task_packs(task)
        .execute(&mut *connection)
        .await?;
    queries::retire_task_encodings(task)
        .execute(&mut *connection)
        .await?;
    queries::release_task_inputs(task)
        .execute(connection)
        .await?;
    Ok(())
}

pub(in super::super) async fn cache_finish(pool: &SqlitePool, task: Execution) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    lock_execution(&mut tx, task).await?;
    queries::unclaim_uploads(task.id).execute(&mut *tx).await?;
    queries::complete_task(task.id).execute(&mut *tx).await?;
    release_task_inputs(&mut tx, task.id).await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn cache_release(
    pool: &SqlitePool,
    ids: &[Id],
    existing_pack: bool,
) -> Result<CacheRelease> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let rows: Vec<(Id, Option<String>, Option<Uuid>)> = queries::lock_pending_chunks(ids)
        .build_query_as()
        .fetch_all(&mut *tx)
        .await?;
    let mut keys = Vec::new();
    for (id, key, destination) in rows {
        if queries::can_release_pending(id, existing_pack)
            .fetch_one(&mut *tx)
            .await?
        {
            queries::schedule_uploaded_pack(id)
                .execute(&mut *tx)
                .await?;
            queries::delete_upload_pins(id).execute(&mut *tx).await?;
            queries::delete_pending_upload(id).execute(&mut *tx).await?;
            if let Some(key) = key {
                keys.push((key, destination));
            }
        }
    }
    tx.commit().await?;
    Ok(CacheRelease {
        keys,
        staged: Vec::new(),
    })
}

pub(in super::super) async fn cache_status(pool: &SqlitePool) -> Result<(Value, Vec<Value>)> {
    let mut tx = pool.begin().await?;
    let value = queries::cache_status(&mut tx).await?;
    tx.commit().await?;
    Ok(value)
}

pub(in super::super) async fn claim_task(
    pool: &SqlitePool,
    input: crate::model::TaskClaim<'_>,
) -> Result<Option<Uuid>> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let Some(id) = queries::claim_task_candidate(input.class, input.active, input.draining)
        .build_query_scalar::<Uuid>()
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(None);
    };
    ensure!(
        queries::claim_task(id, input.token)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        "task claim changed"
    );
    tx.commit().await?;
    Ok(Some(id))
}

pub(in super::super) async fn finish_task(
    pool: &SqlitePool,
    input: crate::model::TaskFinish,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let task = input.execution;
    let Some((task_type, state)) = queries::lock_task_finish(task)
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(());
    };
    let failed = state == "running" && input.error.is_some();
    if failed && task_type == "storage_migration" {
        super::super::replica_transactions::failed(
            &mut tx,
            task.id,
            input.error.as_deref().unwrap(),
        )
        .await?;
    }
    if failed && matches!(task_type.as_str(), "pack" | "unpack") {
        queries::defer_pack_changes(task.id)
            .execute(&mut *tx)
            .await?;
        queries::defer_pack_maintenance(task.id)
            .execute(&mut *tx)
            .await?;
    }
    if failed && matches!(task_type.as_str(), "upload" | "cache_flush") {
        queries::backoff_pending_uploads(
            task.id,
            input.error.as_deref().unwrap(),
            task_type == "upload",
        )
        .execute(&mut *tx)
        .await?;
    }
    if (failed || state != "running")
        && matches!(
            task_type.as_str(),
            "pack" | "unpack" | "rewrite" | "upload" | "cache_flush"
        )
    {
        release_task_inputs(&mut tx, task.id).await?;
    }
    let next = if failed {
        "failed"
    } else if state == "running" {
        "queued"
    } else {
        &state
    };
    queries::finish_task(
        task,
        next,
        if failed { input.error.as_deref() } else { None },
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn usage_scope(
    connection: &mut SqliteConnection,
    principal: &Principal,
    scope: &ScopeQuery,
) -> Result<(Vec<Uuid>, bool)> {
    let admin = queries::validate_identity(principal, connection).await?;
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

pub(in super::super) async fn scoped_pack_page(
    pool: &SqlitePool,
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
            connection.release().await?;
            result
        }
        Err(_) => Err(crate::model::Error::QueryCancelled.into()),
    }
}
