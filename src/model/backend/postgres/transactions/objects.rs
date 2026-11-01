use super::*;

pub(super) async fn complete_media_operation(
    tx: &mut Transaction<'_, Postgres>,
    version: Option<Id>,
) -> Result<()> {
    record_media_operation(
        tx,
        Outcome {
            status: 200,
            code: None,
            output_version: version,
            task_id: None,
        },
    )
    .await
}

async fn queue_media_operation(tx: &mut Transaction<'_, Postgres>, task_id: Uuid) -> Result<()> {
    record_media_operation(
        tx,
        Outcome {
            status: 202,
            code: None,
            output_version: None,
            task_id: Some(task_id),
        },
    )
    .await
}

async fn record_media_operation(tx: &mut Transaction<'_, Postgres>, result: Outcome) -> Result<()> {
    if let Ok((id, bucket, key, action, count)) =
        RECEIPT.try_with(|r| (r.id, r.bucket, r.key.clone(), r.action, r.count))
    {
        let changed = queries::save_media_operation(id, &result)
            .execute(&mut **tx)
            .await?
            .rows_affected();
        ensure!(changed == 1, "media operation receipt is missing");
        checkpoint_event(
            tx,
            "object.change",
            &key,
            json!({"bucket_id":bucket,"operation":action,"version":result.output_version,"task_id":result.task_id,"operation_id":id}),
        )
        .await?;
        if count > 1
            && let Some(id) = crate::model::audit::current()
        {
            partial_audit(tx, id).await?;
        }
    }
    Ok(())
}

pub(in super::super) async fn lock_media_scope(
    tx: &mut Transaction<'_, Postgres>,
    permit: &Permit,
) -> Result<()> {
    permit.lock(tx).await?;
    let buckets: Vec<Uuid> = permit.resources.keys().copied().collect();
    let blocked: bool = queries::blocked_media_buckets(&buckets)
        .fetch_one(&mut **tx)
        .await?;
    if blocked {
        return Err(s3s::s3_error!(OperationAborted).into());
    }
    queries::lock_media_projects(&buckets)
        .execute(&mut **tx)
        .await?;
    queries::media_bucket_scopes(&buckets)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub(in super::super) async fn selected_object(
    tx: &mut Transaction<'_, Postgres>,
    bucket: Uuid,
    key: &str,
    expected: Option<Id>,
) -> Result<Option<Id>> {
    let actual: Option<Id> = queries::lock_object_stream(bucket, key)
        .fetch_optional(&mut **tx)
        .await?
        .flatten();
    if actual != expected {
        return Err(s3s::s3_error!(PreconditionFailed).into());
    }
    Ok(actual)
}

pub(in super::super) async fn begin_media_operation(
    pool: &PgPool,
    principal: &Principal,
    user: Uuid,
    client: Uuid,
    hash: &[u8],
) -> Result<(Uuid, Vec<u8>)> {
    let mut tx = pool.begin().await?;
    principal.lock_identity(&mut tx).await?;
    queries::insert_media_operation(user, principal.token_id(), client, hash)
        .execute(&mut *tx)
        .await?;
    let (id, expected): (Uuid, Vec<u8>) =
        queries::find_media_operation(user, principal.token_id(), client)
            .fetch_one(&mut *tx)
            .await?;
    tx.commit().await?;
    Ok((id, expected))
}

pub(in super::super) async fn audit_object_batch(
    pool: &PgPool,
    bucket: Uuid,
    detail: Value,
    partial: bool,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    checkpoint_event(&mut tx, "object.batch", &bucket.to_string(), detail).await?;
    if partial && let Some(id) = crate::model::audit::current() {
        partial_audit(&mut tx, id).await?;
    }
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn move_object(
    pool: &PgPool,
    permit: &Permit,
    input: ObjectTransfer<'_>,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    let result = move_object_in(&mut tx, permit, input, None).await;
    if result.is_ok()
        || result
            .as_ref()
            .is_err_and(|e| e.is::<crate::model::ReplicaPending>())
    {
        tx.commit().await?;
    }
    result
}

pub(in super::super) async fn move_object_in(
    tx: &mut super::super::queries::replica::Tx<'_>,
    permit: &Permit,
    input: ObjectTransfer<'_>,
    resumed: Option<Uuid>,
) -> Result<()> {
    let target = input.target_bucket;
    lock_media_scope(tx, permit).await?;
    let required = super::super::transactions::redundancy::writable_targets(
        tx,
        input.target_bucket,
        input.target_key,
        None,
    )
    .await?;
    super::super::transactions::redundancy::writable(
        tx,
        input.source_bucket,
        input.source_key,
        None,
    )
    .await?;
    selected_object(
        tx,
        input.source_bucket,
        input.source_key,
        Some(input.source_version),
    )
    .await?;
    let key = input.target_key;
    let old = selected_object(tx, target, key, input.target_version).await?;
    let public: bool = queries::stream_public(input.source_version)
        .fetch_one(&mut **tx)
        .await?;
    if public {
        Permit::for_action(permit.principal.clone(), target, Action::Acl)
            .lock(tx)
            .await?;
    }
    let publication = if resumed.is_some() {
        resumed
    } else if required != 0 {
        Some(
            super::super::transactions::redundancy::freeze(
                tx,
                input.target_bucket,
                input.target_key,
                input.source_version,
                required,
                false,
            )
            .await?,
        )
    } else {
        None
    };
    if let Some(publication) = publication {
        let intent = input.media_intent(permit, "move", public, None);
        super::super::replica_transactions::publish(
            tx,
            publication,
            input.target_bucket,
            input.target_key,
            input.source_version,
            &intent,
        )
        .await?;
    }
    queries::tombstone_object(input.source_bucket, input.source_key, Uuid::new_v4())
        .execute(&mut **tx)
        .await?;
    queries::move_stream(input.source_version, target, key)
        .execute(&mut **tx)
        .await?;
    queries::upsert_object(target, key, input.source_version)
        .execute(&mut **tx)
        .await?;
    if let Some(old) = old {
        queries::retire_stream(old).execute(&mut **tx).await?;
    }
    complete_media_operation(tx, Some(input.source_version)).await?;
    Ok(())
}

pub(in super::super) async fn publish_copy(
    pool: &PgPool,
    permit: &Permit,
    input: ObjectTransfer<'_>,
    id: Id,
    public: bool,
    expected_public: Option<bool>,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    let result = publish_copy_in(&mut tx, permit, input, id, public, expected_public).await;
    if result.is_ok()
        || result
            .as_ref()
            .is_err_and(|e| e.is::<crate::model::ReplicaPending>())
    {
        tx.commit().await?;
    }
    result
}

pub(in super::super) async fn publish_copy_in(
    tx: &mut super::super::queries::replica::Tx<'_>,
    permit: &Permit,
    input: ObjectTransfer<'_>,
    id: Id,
    public: bool,
    expected_public: Option<bool>,
) -> Result<()> {
    let (target, key, expected) = (input.target_bucket, input.target_key, input.target_version);
    let mut authority = permit.clone();
    if public {
        authority.add(target, Action::Acl);
    }
    lock_media_scope(tx, &authority).await?;
    super::super::transactions::redundancy::writable(
        tx,
        input.target_bucket,
        input.target_key,
        None,
    )
    .await?;
    selected_object(
        tx,
        input.source_bucket,
        input.source_key,
        Some(input.source_version),
    )
    .await?;
    let old = selected_object(tx, target, key, expected).await?;
    if let Some(expected_public) = expected_public {
        let current: bool = queries::stream_public(input.source_version)
            .fetch_one(&mut **tx)
            .await?;
        if current != expected_public {
            return Err(s3s::s3_error!(PreconditionFailed).into());
        }
    }
    require_writing_stream(tx, id).await?;
    let (_, _, _, size, payload, _, publication_id) = queries::lock_writing_stream(id)
        .fetch_one(&mut **tx)
        .await?;
    if payload == "raw" {
        validate_raw(tx, id, size).await?;
    } else {
        require_sources(tx, Some(id), &[]).await?;
    }
    if let Some(publication) = publication_id {
        let intent = input.media_intent(permit, "copy", public, expected_public);
        super::super::replica_transactions::publish(
            tx,
            publication,
            input.target_bucket,
            input.target_key,
            id,
            &intent,
        )
        .await?;
    }
    queries::publish_copy_stream(id).execute(&mut **tx).await?;
    queries::upsert_object(target, key, id)
        .execute(&mut **tx)
        .await?;
    if let Some(old) = old {
        queries::retire_stream(old).execute(&mut **tx).await?;
    }
    complete_media_operation(tx, Some(id)).await?;
    Ok(())
}

pub(in super::super) async fn queue_storage_migration(
    pool: &PgPool,
    permit: &Permit,
    job: &StorageMigration,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    let mut authority = permit.clone();
    if job.public_read {
        authority.add(job.target_bucket, Action::Acl);
    }
    lock_media_scope(&mut tx, &authority).await?;
    super::super::transactions::redundancy::writable(
        &mut tx,
        job.target_bucket,
        &job.target_key,
        None,
    )
    .await?;
    if job.action == "move" {
        super::super::transactions::redundancy::writable(
            &mut tx,
            job.source_bucket,
            &job.source_key,
            None,
        )
        .await?;
    }
    selected_object(
        &mut tx,
        job.source_bucket,
        &job.source_key,
        Some(job.source_version),
    )
    .await?;
    selected_object(
        &mut tx,
        job.target_bucket,
        &job.target_key,
        job.target_version,
    )
    .await?;
    let task = enqueue_task(
        &mut tx,
        "storage_migration",
        Some(job.target_bucket),
        serde_json::json!({"job":job,"authority":authority}),
        false,
    )
    .await?;
    queue_media_operation(&mut tx, task).await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn resume_migration(
    pool: &PgPool,
    task: Execution,
    job: &StorageMigration,
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    let resumed = resume_migration_in(&mut tx, task, job).await?;
    tx.commit().await?;
    Ok(resumed)
}

pub(in super::super) async fn resume_migration_in(
    tx: &mut super::super::queries::replica::Tx<'_>,
    task: Execution,
    job: &StorageMigration,
) -> Result<bool> {
    let Some((stream, _)) = super::super::queries::replica::parked_stream(task.id)
        .fetch_optional(&mut **tx)
        .await?
    else {
        return Ok(false);
    };
    ensure!(
        super::super::queries::replication::adopt(stream, task)
            .execute(&mut **tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    complete_storage_migration_in(tx, task.id, task.token, stream, job).await?;
    Ok(true)
}

pub(in super::super) async fn complete_storage_migration(
    pool: &PgPool,
    id: Uuid,
    lease_token: Uuid,
    destination: Id,
    job: &StorageMigration,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    complete_storage_migration_in(&mut tx, id, lease_token, destination, job).await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn complete_storage_migration_in(
    tx: &mut Transaction<'_, Postgres>,
    id: Uuid,
    lease_token: Uuid,
    destination: Id,
    job: &StorageMigration,
) -> Result<()> {
    if let Err(error) = publish_migration_in(tx, id, lease_token, destination, job).await {
        if error.is::<crate::model::ReplicaPending>() {
            super::super::replica_transactions::park(
                tx,
                Execution {
                    id,
                    token: lease_token,
                },
            )
            .await?;
            return Ok(());
        }
        return Err(error);
    }
    let completed = queries::complete_migration_task(id, lease_token)
        .execute(&mut **tx)
        .await?;
    ensure!(
        completed.rows_affected() == 1,
        "storage migration lease lost before publish"
    );
    Ok(())
}

pub(in super::super) async fn publish_migration_in(
    tx: &mut Transaction<'_, Postgres>,
    id: Uuid,
    lease_token: Uuid,
    destination: Id,
    job: &StorageMigration,
) -> Result<()> {
    let owned: Option<Uuid> = queries::lock_migration_task(id, lease_token)
        .fetch_optional(&mut **tx)
        .await?;
    ensure!(
        owned.is_some(),
        "storage migration lease lost before publish"
    );
    super::super::transactions::redundancy::writable(
        tx,
        job.target_bucket,
        &job.target_key,
        Some(id),
    )
    .await?;
    if job.action == "move" {
        super::super::transactions::redundancy::writable(
            tx,
            job.source_bucket,
            &job.source_key,
            None,
        )
        .await?;
    }
    let source_current = selected_object(
        tx,
        job.source_bucket,
        &job.source_key,
        Some(job.source_version),
    )
    .await?;
    let old = selected_object(tx, job.target_bucket, &job.target_key, job.target_version).await?;
    ensure!(
        source_current == Some(job.source_version),
        "source object changed during storage migration"
    );
    require_writing_stream(tx, destination).await?;
    let (_, _, _, size, payload, _, publication_id) = queries::lock_writing_stream(destination)
        .fetch_one(&mut **tx)
        .await?;
    if payload == "raw" {
        validate_raw(tx, destination, size).await?;
    } else {
        require_sources(tx, Some(destination), &[]).await?;
    }
    if let Some(publication) = publication_id {
        let record = queries::lock_task_detail(Execution {
            id,
            token: lease_token,
        })
        .fetch_one(&mut **tx)
        .await?;
        if let Some(grant) = record.get("authority") {
            lock_media_scope(tx, &serde_json::from_value(grant.clone())?).await?;
        }
        let details =
            serde_json::json!({"operation":"task","task":id,"method":"migration","job":job});
        super::super::replica_transactions::publish(
            tx,
            publication,
            job.target_bucket,
            &job.target_key,
            destination,
            &details,
        )
        .await?;
    }
    let published = queries::publish_migration_stream(destination, id, lease_token)
        .execute(&mut **tx)
        .await?;
    ensure!(
        published.rows_affected() == 1,
        "storage migration stream was reclaimed"
    );
    queries::upsert_object(job.target_bucket, &job.target_key, destination)
        .execute(&mut **tx)
        .await?;
    if job.action == "move" {
        queries::tombstone_object(job.source_bucket, &job.source_key, Uuid::new_v4())
            .execute(&mut **tx)
            .await?;
        queries::retire_stream(job.source_version)
            .execute(&mut **tx)
            .await?;
    }
    if let Some(old) = old {
        queries::retire_stream(old).execute(&mut **tx).await?;
    }
    Ok(())
}

pub(in super::super) async fn browse_catalog(
    pool: &PgPool,
    authority: &Permit,
    bucket: Uuid,
    filter: &mut crate::model::object::CatalogFilter,
    cursor: Option<crate::model::object::CatalogCursor>,
    sizes: (Option<i64>, Option<i64>),
) -> Result<Vec<crate::model::object::MediaItem>> {
    let mut tx = pool.begin().await?;
    queries::catalog_search_settings().execute(&mut *tx).await?;
    authority.lock(&mut tx).await?;
    let rows = queries::browse_catalog(&mut tx, bucket, filter, cursor, sizes).await?;
    tx.commit().await?;
    Ok(rows)
}
