use super::*;

pub(in super::super) async fn claim_task(
    backend: &super::super::Backend,
    input: crate::model::TaskClaim<'_>,
) -> Result<Option<Uuid>> {
    let mut tx = backend.pool.begin().await?;
    backend.fence_control(&mut tx).await?;
    let Some(id) = queries::claim_task_candidate(input.class, input.active, input.draining)
        .build_query_scalar::<Uuid>()
        .fetch_optional(&mut *tx)
        .await?
    else {
        tx.rollback().await?;
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
    pool: &MySqlPool,
    input: crate::model::TaskFinish,
) -> Result<()> {
    let mut tx = pool.begin().await?;
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

async fn release_task_inputs(connection: &mut MySqlConnection, task: Uuid) -> Result<()> {
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

pub(in super::super) async fn cache_finish(pool: &MySqlPool, task: Execution) -> Result<()> {
    let mut tx = pool.begin().await?;
    lock_execution(&mut tx, task).await?;
    queries::unclaim_uploads(task.id).execute(&mut *tx).await?;
    queries::complete_task(task.id).execute(&mut *tx).await?;
    release_task_inputs(&mut tx, task.id).await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn cache_release(
    pool: &MySqlPool,
    ids: &[Id],
    existing_pack: bool,
) -> Result<CacheRelease> {
    let mut tx = pool.begin().await?;
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

pub(in super::super) async fn cache_pins(
    pool: &MySqlPool,
    names: &[String],
    storage: Option<Uuid>,
) -> Result<Vec<String>> {
    let mut tx = pool.begin().await?;
    let keys = queries::owned_upload_pins(names, storage)
        .build_query_scalar()
        .fetch_all(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(keys)
}

pub(in super::super) async fn task_page(
    pool: &MySqlPool,
    principal: &Principal,
    filter: crate::model::task::JobQuery,
    cursor: Option<(chrono::DateTime<chrono::Utc>, Uuid)>,
    limit: i64,
) -> Result<Vec<Value>> {
    let mut tx = admin_transaction(pool, principal).await?;
    let values = queries::task_page(filter, cursor, limit)
        .build()
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .map(|row| queries::task_value(row, true))
        .collect::<Result<Vec<_>>>()?;
    tx.commit().await?;
    Ok(values)
}

pub(in super::super) async fn task_view(
    pool: &MySqlPool,
    principal: &Principal,
    id: Uuid,
) -> Result<Value> {
    let mut tx = admin_transaction(pool, principal).await?;
    let row = queries::task_view(id)
        .build()
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    let value = queries::task_value(row, true)?;
    tx.commit().await?;
    Ok(value)
}

pub(super) async fn record_media_operation(
    connection: &mut Transaction<'_, MySql>,
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
    connection: &mut Transaction<'_, MySql>,
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
    pool: &MySqlPool,
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
    let mut tx = pool.begin().await?;
    queries::validate_identity(principal, &mut tx, true).await?;
    queries::insert_media_operation(Uuid::new_v4(), user, principal.token_id(), client, hash)
        .execute(&mut *tx)
        .await?;
    let row = queries::find_media_operation(user, principal.token_id(), client)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(row)
}

pub(in super::super) async fn enqueue_task(
    connection: &mut MySqlConnection,
    task_type: &str,
    bucket: Option<Uuid>,
    detail: Value,
) -> Result<Uuid> {
    let id = crate::model::REQUEST_ID
        .try_with(|id| *id)
        .unwrap_or_else(|_| Uuid::new_v4());
    queries::insert_task(
        id,
        task_type,
        bucket,
        detail,
        crate::model::audit::current(),
    )
    .execute(connection)
    .await?;
    Ok(id)
}

pub(in super::super) async fn start_purge(
    pool: &MySqlPool,
    principal: &Principal,
    name: &str,
    id: Uuid,
    confirmation: Option<&str>,
) -> Result<Value> {
    let mut tx = administration_transaction(pool, principal).await?;
    let row = queries::lock_bucket(id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(PreconditionFailed))?;
    super::super::transactions::redundancy::removable(&mut tx, id).await?;
    ensure!(row.name == name, s3s::s3_error!(PreconditionFailed));
    if let Some(task) = queries::existing_purge(id).fetch_optional(&mut *tx).await? {
        return Ok(serde_json::json!({"task_id":task,"existing":true}));
    }
    if let Some(expected) = confirmation {
        ensure!(
            fingerprint(&queries::bucket_impact(id).fetch_one(&mut *tx).await?, None)? == expected,
            s3s::s3_error!(PreconditionFailed)
        );
    }
    queries::seal_purge_bucket(id).execute(&mut *tx).await?;
    let task = enqueue_task(
        &mut tx,
        "purge",
        Some(id),
        serde_json::json!({"bucket_id":id,"name":name}),
    )
    .await?;
    checkpoint_event(
        &mut tx,
        "bucket.purge",
        name,
        serde_json::json!({"bucket_id":id,"task_id":task}),
    )
    .await?;
    tx.commit().await?;
    Ok(serde_json::json!({"task_id":task}))
}

pub(in super::super) async fn lock_execution(
    connection: &mut MySqlConnection,
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
    pool: &MySqlPool,
    task: Execution,
    bucket: Uuid,
    cursor: Option<&str>,
    batch: i64,
    active: &[Uuid],
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    queries::lock_accounts().execute(&mut *tx).await?;
    lock_execution(&mut tx, task).await?;
    let members = lock_bucket_dependents(&mut tx, bucket).await?;
    let row = queries::lock_bucket(bucket).fetch_one(&mut *tx).await?;
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
