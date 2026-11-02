use super::*;

pub(in super::super) async fn audit_object_batch(
    pool: &MySqlPool,
    bucket: Uuid,
    detail: Value,
    partial: bool,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    checkpoint_event(&mut tx, "object.batch", &bucket.to_string(), detail).await?;
    if partial && let Some(id) = crate::model::audit::current() {
        queries::partial_audit(id).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn lock_media_scope(
    connection: &mut MySqlConnection,
    permit: &Permit,
) -> Result<()> {
    lock_permit(connection, permit).await?;
    for bucket in permit.resources.keys() {
        ensure!(
            queries::lock_bucket_uploads(*bucket)
                .fetch_one(&mut *connection)
                .await?
                .0
                == "active",
            crate::model::Error::OperationAborted
        );
    }
    Ok(())
}

pub(in super::super) async fn selected_object(
    connection: &mut MySqlConnection,
    bucket: Uuid,
    key: &str,
    expected: Option<Id>,
) -> Result<Option<Id>> {
    let actual = queries::lock_object_version(bucket, key)
        .fetch_optional(connection)
        .await?
        .and_then(|r| r.0);
    ensure!(actual == expected, s3s::s3_error!(PreconditionFailed));
    Ok(actual)
}

async fn selected_objects(
    connection: &mut MySqlConnection,
    input: &ObjectTransfer<'_>,
) -> Result<Option<Id>> {
    let mut objects = [
        (
            input.source_bucket,
            input.source_key,
            Some(input.source_version),
        ),
        (input.target_bucket, input.target_key, input.target_version),
    ];
    objects.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
    for (bucket, key, expected) in objects {
        selected_object(connection, bucket, key, expected).await?;
    }
    queries::lock_media_quota(connection, &[input.source_bucket, input.target_bucket]).await?;
    Ok(input.target_version)
}

pub(in super::super) async fn remove_object_reference(
    connection: &mut MySqlConnection,
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

pub(super) async fn replace_object_reference(
    connection: &mut MySqlConnection,
    stream: &crate::app::StoredStream,
    old: Option<Id>,
) -> Result<()> {
    queries::claim_object(stream.bucket_id, &stream.object_key, stream.id)
        .execute(&mut *connection)
        .await?;
    supersede_object_quota(connection, stream.bucket_id, &stream.object_key, stream.id).await?;
    let previous = if let Some(id) = old {
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
    let mut accounts = lock_quota(connection, stream.bucket_id).await?;
    replace_object_quota(
        connection,
        &mut accounts,
        stream.bucket_id,
        &stream.object_key,
        previous,
        Some((stream.id, stream.size)),
    )
    .await?;
    queries::publish_object(stream)
        .execute(&mut *connection)
        .await?;
    if let Some(old) = old
        && old != stream.id
    {
        queries::retire_stream(old).execute(connection).await?;
    }
    Ok(())
}

pub(in super::super) async fn change_object(pool: &MySqlPool, input: ObjectChange) -> Result<bool> {
    let mut tx = pool.begin().await?;
    let changed = change_object_in(&mut tx, input).await?;
    tx.commit().await?;
    Ok(changed)
}

pub(in super::super) async fn change_object_in(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    input: ObjectChange,
) -> Result<bool> {
    let mut permit = Permit::for_action(
        input.principal,
        input.bucket,
        if input.public.is_some() {
            Action::Acl
        } else {
            Action::Delete
        },
    );
    if matches!(input.if_match, Some(s3s::dto::ETagCondition::ETag(_))) {
        permit.add(input.bucket, Action::Read);
    }
    lock_media_scope(tx, &permit).await?;
    super::super::transactions::redundancy::writable(tx, input.bucket, &input.key, None).await?;
    let old = queries::lock_object_version(input.bucket, &input.key)
        .fetch_optional(&mut **tx)
        .await?
        .and_then(|r| r.0);
    ensure!(
        input.expected.is_none() || input.expected == old,
        s3s::s3_error!(PreconditionFailed)
    );
    if let Some(condition) = input.if_match.as_ref() {
        let etag = match old {
            Some(id) => Some(queries::stream_size_etag(id).fetch_one(&mut **tx).await?.1),
            None => None,
        };
        crate::model::check_write_conditions(etag.as_deref(), Some(condition), None)?;
    }
    if let Some(public) = input.public {
        let id = old.ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
        queries::set_stream_public(id, public)
            .execute(&mut **tx)
            .await?;
        queries::set_catalog_public(input.bucket, &input.key, public)
            .execute(&mut **tx)
            .await?;
        complete_media_operation(tx, Some(id)).await?;
        return Ok(false);
    }
    remove_object_reference(tx, input.bucket, &input.key, old).await?;
    if let Some(id) = old {
        queries::retire_stream(id).execute(&mut **tx).await?;
    }
    complete_media_operation(tx, None).await?;
    Ok(true)
}

pub(in super::super) async fn move_object(
    pool: &MySqlPool,
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
    ensure!(
        (input.source_bucket, input.source_key) != (input.target_bucket, input.target_key),
        s3s::s3_error!(InvalidArgument)
    );
    let mut authority = permit.clone();
    authority.add(input.source_bucket, Action::Read);
    authority.add(input.source_bucket, Action::Delete);
    authority.add(input.target_bucket, Action::Write);
    lock_media_scope(tx, &authority).await?;
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
    let old = selected_objects(tx, &input).await?;
    let mut stream = queries::publish_stream_record(input.source_version)
        .fetch_one(&mut **tx)
        .await?;
    ensure!(
        stream.state == "ready",
        crate::model::Error::OperationAborted
    );
    ensure!(
        queries::share_bucket(input.source_bucket)
            .fetch_one(&mut **tx)
            .await?
            .storage_id
            == queries::share_bucket(input.target_bucket)
                .fetch_one(&mut **tx)
                .await?
                .storage_id,
        "moving between stores requires a storage migration"
    );
    if stream.public_read {
        lock_permit(
            tx,
            &Permit::for_action(permit.principal.clone(), input.target_bucket, Action::Acl),
        )
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
        let intent = input.media_intent(permit, "move", stream.public_read, None);
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
    remove_object_reference(
        tx,
        input.source_bucket,
        input.source_key,
        Some(input.source_version),
    )
    .await?;
    queries::move_stream(stream.id, input.target_bucket, input.target_key)
        .execute(&mut **tx)
        .await?;
    stream.bucket_id = input.target_bucket;
    stream.object_key = input.target_key.into();
    replace_object_reference(tx, &stream, old).await?;
    complete_media_operation(tx, Some(stream.id)).await?;
    Ok(())
}

pub(super) async fn validate_copy(
    connection: &mut MySqlConnection,
    id: Id,
    bucket: Uuid,
    key: &str,
    public: bool,
) -> Result<crate::model::PublishingStream> {
    let record = super::super::queries::replica::candidate(id)
        .fetch_one(&mut *connection)
        .await?;
    let stream = &record.stream;
    ensure!(
        stream.state == "writing"
            && stream.stream_type == "object"
            && stream.bucket_id == bucket
            && stream.object_key == key
            && stream.public_read == public,
        crate::model::Error::OperationAborted
    );
    if stream.payload_type == "raw" {
        validate_raw(connection, id, stream.size).await?;
        return Ok(record);
    }
    ensure!(
        !queries::incomplete_extents(id)
            .fetch_one(&mut *connection)
            .await?,
        "copy mapping is incomplete"
    );
    let (end, contiguous) = queries::extent_continuity(id).fetch_one(connection).await?;
    ensure!(
        end == stream.size && contiguous,
        "copy mapping has gaps or overlaps"
    );
    Ok(record)
}

pub(in super::super) async fn publish_copy(
    pool: &MySqlPool,
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
    let mut authority = permit.clone();
    authority.add(input.source_bucket, Action::Read);
    authority.add(input.target_bucket, Action::Write);
    if public {
        authority.add(input.target_bucket, Action::Acl);
    }
    lock_media_scope(tx, &authority).await?;
    super::super::transactions::redundancy::writable(
        tx,
        input.target_bucket,
        input.target_key,
        None,
    )
    .await?;
    let old = selected_objects(tx, &input).await?;
    if let Some(expected) = expected_public {
        ensure!(
            queries::quota_stream(input.source_version)
                .fetch_one(&mut **tx)
                .await?
                .public_read
                == expected,
            s3s::s3_error!(PreconditionFailed)
        );
    }
    let crate::model::PublishingStream {
        stream,
        publication_id,
    } = validate_copy(tx, id, input.target_bucket, input.target_key, public).await?;
    require_sources(tx, Some(id), &[]).await?;
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
    queries::publish_stream(id).execute(&mut **tx).await?;
    queries::ready_stream_queues(tx, id).await?;
    replace_object_reference(tx, &stream, old).await?;
    complete_media_operation(tx, Some(id)).await?;
    Ok(())
}

pub(in super::super) async fn queue_storage_migration(
    pool: &MySqlPool,
    permit: &Permit,
    job: &StorageMigration,
) -> Result<()> {
    ensure!(
        matches!(job.action.as_str(), "copy" | "move"),
        s3s::s3_error!(InvalidArgument)
    );
    let mut authority = permit.clone();
    authority.add(job.source_bucket, Action::Read);
    authority.add(job.target_bucket, Action::Write);
    if job.action == "move" {
        authority.add(job.source_bucket, Action::Delete);
    }
    if job.public_read {
        authority.add(job.target_bucket, Action::Acl);
    }
    let mut tx = pool.begin().await?;
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
    selected_objects(
        &mut tx,
        &ObjectTransfer {
            source_bucket: job.source_bucket,
            source_key: &job.source_key,
            source_version: job.source_version,
            target_bucket: job.target_bucket,
            target_key: &job.target_key,
            target_version: job.target_version,
        },
    )
    .await?;
    let task = enqueue_task(
        &mut tx,
        "storage_migration",
        Some(job.target_bucket),
        serde_json::json!({"job":job,"authority":authority}),
    )
    .await?;
    record_media_operation(
        &mut tx,
        Outcome {
            status: 202,
            code: None,
            output_version: None,
            task_id: Some(task),
        },
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn resume_migration(
    pool: &MySqlPool,
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
    complete_storage_migration_in(tx, task, stream, job).await?;
    Ok(true)
}

pub(in super::super) async fn complete_storage_migration(
    pool: &MySqlPool,
    task: Execution,
    id: Id,
    job: &StorageMigration,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    complete_storage_migration_in(&mut tx, task, id, job).await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn complete_storage_migration_in(
    tx: &mut super::super::queries::replica::Tx<'_>,
    task: Execution,
    id: Id,
    job: &StorageMigration,
) -> Result<()> {
    if let Err(error) = publish_migration_in(tx, task, id, job).await {
        if error.is::<crate::model::ReplicaPending>() {
            super::super::replica_transactions::park(tx, task).await?;
            return Ok(());
        }
        return Err(error);
    }
    ensure!(
        queries::complete_migration_task(task)
            .execute(&mut **tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    Ok(())
}

pub(in super::super) async fn publish_migration_in(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    task: Execution,
    id: Id,
    job: &StorageMigration,
) -> Result<()> {
    ensure!(
        matches!(job.action.as_str(), "copy" | "move"),
        s3s::s3_error!(InvalidArgument)
    );
    lock_execution(tx, task).await?;
    let mut authority = Permit::for_action(Principal::System, job.source_bucket, Action::Read);
    authority.add(job.target_bucket, Action::Write);
    lock_media_scope(tx, &authority).await?;
    super::super::transactions::redundancy::writable(
        tx,
        job.target_bucket,
        &job.target_key,
        Some(task.id),
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
    let old = selected_objects(
        tx,
        &ObjectTransfer {
            source_bucket: job.source_bucket,
            source_key: &job.source_key,
            source_version: job.source_version,
            target_bucket: job.target_bucket,
            target_key: &job.target_key,
            target_version: job.target_version,
        },
    )
    .await?;
    let crate::model::PublishingStream {
        stream,
        publication_id,
    } = validate_copy(tx, id, job.target_bucket, &job.target_key, job.public_read).await?;
    require_sources(tx, Some(id), &[]).await?;
    if let Some(publication) = publication_id {
        let record = queries::lock_task_detail(task).fetch_one(&mut **tx).await?;
        if let Some(grant) = record.get("authority") {
            lock_media_scope(tx, &serde_json::from_value(grant.clone())?).await?;
        }
        let details =
            serde_json::json!({"operation":"task","task":task.id,"method":"migration","job":job});
        super::super::replica_transactions::publish(
            tx,
            publication,
            job.target_bucket,
            &job.target_key,
            id,
            &details,
        )
        .await?;
    }
    ensure!(
        queries::publish_migration_stream(id, task)
            .execute(&mut **tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    queries::ready_stream_queues(tx, id).await?;
    if job.action == "move" {
        remove_object_reference(
            tx,
            job.source_bucket,
            &job.source_key,
            Some(job.source_version),
        )
        .await?;
        queries::retire_stream(job.source_version)
            .execute(&mut **tx)
            .await?;
    }
    replace_object_reference(tx, &stream, old).await?;
    Ok(())
}

pub(in super::super) async fn abandon_copy(pool: &MySqlPool, id: Id) -> Result<()> {
    let mut tx = pool.begin().await?;
    abandon_copy_in(&mut tx, id).await?;
    tx.commit().await?;
    Ok(())
}
pub(in super::super) async fn abandon_copy_in(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    id: Id,
) -> Result<()> {
    let stream = queries::publish_stream_record(id)
        .fetch_optional(&mut **tx)
        .await?;
    if let Some(stream) = stream
        && stream.state == "writing"
        && queries::abandon_copy_stream(id)
            .execute(&mut **tx)
            .await?
            .rows_affected()
            == 1
    {
        let mut accounts = lock_quota(tx, stream.bucket_id).await?;
        forget_quota_write(tx, &mut accounts, id).await?;
    }
    Ok(())
}

pub(in super::super) async fn browse_catalog(
    pool: &MySqlPool,
    authority: &Permit,
    bucket: Uuid,
    filter: &mut CatalogFilter,
    cursor: Option<CatalogCursor>,
    sizes: (Option<i64>, Option<i64>),
) -> Result<Vec<MediaItem>> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    let mut connection =
        super::super::connection::DeadlineConnection::acquire(pool, deadline).await?;
    let result = tokio::time::timeout_at(deadline, async {
        let mut tx = connection.begin().await?;
        let mut authority = authority.clone();
        authority.add(bucket, Action::List);
        lock_permit(&mut tx, &authority).await?;
        let rows = queries::browse_catalog(&mut tx, bucket, filter, cursor, sizes).await?;
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

pub(in super::super) async fn create_bucket(
    pool: &MySqlPool,
    principal: &Principal,
    input: CreateBucket,
) -> Result<Bucket> {
    ensure!(
        s3s::path::check_bucket_name(&input.name),
        s3s::s3_error!(InvalidBucketName)
    );
    let mut tx = pool.begin().await?;
    queries::lock_accounts().execute(&mut *tx).await?;
    let admin = queries::validate_identity(principal, &mut tx, true).await?;
    let project = input.project_id.unwrap_or(DEFAULT_PROJECT);
    let allowed = queries::bucket_creation_policy(project)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    let mut selected = false;
    if !admin {
        ensure!(
            allowed && matches!(principal, Principal::User { .. }),
            s3s::s3_error!(AccessDenied)
        );
        let (role, scope) = queries::bucket_creator_membership(project, principal.user_id())
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| s3s::s3_error!(AccessDenied))?;
        ensure!(role == "maintainer", s3s::s3_error!(AccessDenied));
        selected = scope == "selected";
    }
    let available = queries::bucket_available_storages()
        .fetch_all(&mut *tx)
        .await?;
    let storage = match input.storage_id {
        Some(id) if available.contains(&id) => id,
        Some(_) => anyhow::bail!("storage is unavailable"),
        None if available.len() == 1 => available[0],
        None if available.is_empty() => anyhow::bail!("no storage configured"),
        None => anyhow::bail!("storage must be selected"),
    };
    ensure!(
        !queries::duplicate_bucket(&input.name)
            .fetch_one(&mut *tx)
            .await?,
        crate::model::Error::BucketAlreadyExists
    );
    let quota = queries::quota_account("project", project, true)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(
        quota
            .bucket_count_limit
            .is_none_or(|limit| quota.bucket_count < limit),
        crate::model::Error::QuotaExceeded
    );
    let id = Uuid::new_v4();
    queries::insert_bucket(id, &input.name, project, storage)
        .execute(&mut *tx)
        .await?;
    queries::change_project_bucket_quota(project, 1, 0, 0)
        .execute(&mut *tx)
        .await?;
    queries::insert_quota_scope("bucket", id)
        .execute(&mut *tx)
        .await?;
    if selected {
        let user = principal.user_id().context("bucket creator has no user")?;
        queries::insert_member_grant(
            user,
            project,
            &crate::model::administration::BucketGrant {
                bucket_id: id,
                actions: Action::ALL.to_vec(),
            },
        )
        .execute(&mut *tx)
        .await?;
        queries::advance_user_authorization(user)
            .execute(&mut *tx)
            .await?;
    }
    checkpoint_event(
        &mut tx,
        "bucket.create",
        &input.name,
        serde_json::json!({"bucket_id":id,"project_id":project}),
    )
    .await?;
    let row = queries::bucket_by_id(id).fetch_one(&mut *tx).await?;
    tx.commit().await?;
    Ok(row)
}
