use super::*;

#[async_trait::async_trait]
impl PublishStore for Backend {
    async fn replicas_ready(&self, publication: Uuid) -> Result<bool> {
        let (phase, result) = super::super::queries::replica::state(publication)
            .fetch_one(&self.pool)
            .await?;
        if phase == "published" {
            return Ok(true);
        }
        if phase == "aborted" {
            return Err(crate::model::publish::reject_replica(
                result.as_ref().and_then(|r| r.0["error"].as_str()),
            ));
        }
        let (_, failed) = super::super::queries::replica::readiness(publication)
            .fetch_one(&self.pool)
            .await?;
        anyhow::ensure!(!failed, crate::model::Error::OperationAborted);
        Ok(false)
    }

    async fn publish(
        &self,
        id: Id,
        if_match: Option<s3s::dto::ETagCondition>,
        if_none: Option<s3s::dto::ETagCondition>,
        completion: Option<(uuid::Uuid, String, Value, Uuid)>,
    ) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        self.fence_control(&mut tx).await?;
        let result = publish_in(&mut tx, id, if_match, if_none, completion).await;
        if matches!(&result, Ok(true))
            || result
                .as_ref()
                .is_err_and(|e| e.is::<crate::model::ReplicaPending>())
        {
            tx.commit().await?;
        } else if let Some(reason) = result
            .as_ref()
            .err()
            .and_then(crate::model::replica_rejection)
        {
            tx.rollback().await?;
            reject_admission(&self.pool, id, reason).await?;
        }
        result
    }
}

pub(in super::super) async fn publish_in(
    tx: &mut Transaction<'_, Postgres>,
    id: Id,
    if_match: Option<s3s::dto::ETagCondition>,
    if_none: Option<s3s::dto::ETagCondition>,
    completion: Option<(uuid::Uuid, String, Value, Uuid)>,
) -> Result<bool> {
    let authority: sqlx::types::Json<crate::authorization::Permit> =
        queries::write_authorization(id)
            .fetch_one(&mut **tx)
            .await?;
    authority.lock(tx).await?;
    let (bucket, object_key, state, size, payload_type, marked, publication) =
        queries::lock_writing_stream(id)
            .fetch_one(&mut **tx)
            .await?;
    let (bucket_state, _, managed, _, image_policy) =
        super::super::queries::redundancy::admission(bucket)
            .fetch_one(&mut **tx)
            .await?;
    if bucket_state != "active" {
        return Err(s3s::s3_error!(OperationAborted).into());
    }
    super::super::transactions::redundancy::admitted(
        tx,
        bucket,
        &object_key,
        None,
        managed,
        image_policy.as_deref(),
    )
    .await?;
    super::super::transactions::redundancy::accept_origin(tx, bucket, marked).await?;
    let (old, epoch): (Option<Id>, Id) = queries::lock_object_version(bucket, &object_key)
        .fetch_one(&mut **tx)
        .await?;
    if epoch != id || state != "writing" {
        return Err(s3s::s3_error!(OperationAborted, "write was superseded").into());
    }
    if let Some((upload, _, _, _)) = &completion {
        let expected: Option<(Option<Id>, i64)> = queries::web_upload_expectation(upload)
            .fetch_optional(&mut **tx)
            .await?;
        if let Some((version, expected_size)) = expected {
            if old != version {
                return Err(s3s::s3_error!(
                    PreconditionFailed,
                    "object changed since upload started"
                )
                .into());
            }
            if size != expected_size {
                return Err(s3s::s3_error!(IncompleteBody).into());
            }
        }
    }
    let previous: Option<String> = if let Some(old) = old {
        Some(queries::stream_etag(old).fetch_one(&mut **tx).await?)
    } else {
        None
    };
    check_write_conditions(previous.as_deref(), if_match.as_ref(), if_none.as_ref())?;
    if payload_type == "raw" {
        validate_raw(tx, id, size).await?;
    } else {
        let invalid: bool = queries::incomplete_extents(id).fetch_one(&mut **tx).await?;
        anyhow::ensure!(!invalid, "cannot publish an incomplete generation");
        let (end, contiguous): (i64, bool) =
            queries::extent_continuity(id).fetch_one(&mut **tx).await?;
        anyhow::ensure!(
            end == size && contiguous,
            "cannot publish a mapping with gaps or overlaps"
        );
        require_sources(tx, Some(id), &[]).await?;
    }
    if let Some(publication) = publication {
        let intent = crate::model::write_intent(if_match.as_ref(), if_none.as_ref(), &completion)?;
        super::super::replica_transactions::publish(
            tx,
            publication,
            bucket,
            &object_key,
            id,
            &intent,
        )
        .await?;
    }
    if !apply_publish(tx, id, bucket, &object_key, old, completion.as_ref()).await? {
        return Ok(false);
    }
    if let Some((upload, _, _, _)) = &completion {
        checkpoint_event(
            tx,
            "upload.complete",
            &upload.to_string(),
            serde_json::json!({"bucket_id":bucket,"key":object_key,"size":size}),
        )
        .await?;
    }
    Ok(true)
}

pub(in super::super) async fn recover_stale(pool: &PgPool) -> Result<u64> {
    let mut tx = pool.begin().await?;
    let stale: Vec<uuid::Uuid> = queries::lock_stale_tasks().fetch_all(&mut *tx).await?;
    if stale.is_empty() {
        tx.commit().await?;
        return Ok(0);
    }
    queries::abandon_task_streams(&stale)
        .execute(&mut *tx)
        .await?;
    let result = queries::requeue_stale_tasks(&stale)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(result.rows_affected())
}

pub(in super::super) async fn claim_task(
    backend: &super::super::Backend,
    input: crate::model::TaskClaim<'_>,
    node: Option<(
        &crate::cluster::NodeState,
        &str,
        &crate::content::codec::Key,
    )>,
) -> Result<Option<uuid::Uuid>> {
    let mut tx = backend.pool.begin().await?;
    backend.fence_control(&mut tx).await?;
    if let Some((state, role, key)) = node {
        validate_node(&mut tx, state, role, key).await?;
    }
    let id = queries::claim_task(
        input.class,
        input.active,
        node.map(|(state, _, _)| state.node_id),
        input.token,
        input.draining,
        &[],
        node.map(|(_, role, _)| role == "control"),
        None,
    )
    .fetch_optional(&mut *tx)
    .await?;
    // The new execution owns cleanup before any fresh physical outputs are allocated.
    if input.class != 0
        && let Some(id) = id
    {
        release_task_inputs(&mut tx, id).await?;
    }
    tx.commit().await?;
    Ok(id)
}

pub(crate) async fn validate_node(
    db: &mut PgConnection,
    state: &crate::cluster::NodeState,
    role: &str,
    key: &crate::content::codec::Key,
) -> Result<()> {
    let row = queries::registered_node(state.node_id, true)
        .fetch_optional(db)
        .await?
        .ok_or(crate::model::Error::NodeAuthentication)?;
    verify_node_identity(&row, state, role, key)?;
    Ok(())
}

pub(in super::super) async fn try_acquire_storage_permit(
    pool: &PgPool,
    storage_id: uuid::Uuid,
    direction: &str,
    limit: i32,
    holder: uuid::Uuid,
) -> Result<Option<uuid::Uuid>> {
    let token = uuid::Uuid::new_v4();
    let mut tx = pool.begin().await?;
    queries::lock_storage_permits(format!("mokyu:store:{storage_id}:{direction}"))
        .execute(&mut *tx)
        .await?;
    queries::expire_storage_permits(storage_id, direction)
        .execute(&mut *tx)
        .await?;
    let active: i64 = queries::storage_permit_count(storage_id, direction)
        .fetch_one(&mut *tx)
        .await?;
    if active >= i64::from(limit) {
        tx.rollback().await?;
        return Ok(None);
    }
    queries::insert_storage_permit(token, storage_id, direction, holder)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(Some(token))
}

pub(in super::super) async fn renew_storage_permit(
    pool: &PgPool,
    token: uuid::Uuid,
) -> Result<bool> {
    Ok(queries::renew_storage_permit(token)
        .execute(pool)
        .await?
        .rows_affected()
        == 1)
}

pub(in super::super) async fn release_storage_permit(
    pool: &PgPool,
    token: uuid::Uuid,
) -> Result<()> {
    queries::release_storage_permit(token).execute(pool).await?;
    Ok(())
}

pub(crate) async fn insert_task(
    db: &mut PgConnection,
    id: uuid::Uuid,
    task_type: &str,
    bucket: Option<uuid::Uuid>,
    detail: Value,
    audit_id: Option<Id>,
) -> Result<()> {
    queries::insert_task(id, task_type, bucket, detail, audit_id)
        .execute(&mut *db)
        .await?;
    queries::notify_work(id).execute(&mut *db).await?;
    Ok(())
}

pub(in super::super) async fn finish_stream(
    pool: &PgPool,
    id: Id,
    size: i64,
    etag: &str,
    checksums: Value,
) -> Result<bool> {
    Ok(queries::finish_stream(id, size, etag, checksums)
        .execute(pool)
        .await?
        .rows_affected()
        == 1)
}

pub(in super::super) async fn apply_publish(
    tx: &mut Transaction<'_, Postgres>,
    id: Id,
    bucket: uuid::Uuid,
    object_key: &str,
    old: Option<Id>,
    completion: Option<&(uuid::Uuid, String, Value, Uuid)>,
) -> Result<bool> {
    queries::publish_object(bucket, object_key, id)
        .execute(&mut **tx)
        .await?;
    queries::publish_stream(id).execute(&mut **tx).await?;
    if let Some(old) = old {
        queries::retire_stream(old).execute(&mut **tx).await?;
    }
    if let Some((upload, hash, result, token)) = completion {
        let changed = queries::complete_upload(upload, hash, result, id, *token)
            .execute(&mut **tx)
            .await?
            .rows_affected();
        if changed != 1 {
            return Ok(false);
        }
        queries::retire_upload_parts(&upload)
            .execute(&mut **tx)
            .await?;
        queries::delete_upload_parts(upload)
            .execute(&mut **tx)
            .await?;
    }
    Ok(true)
}

pub(in super::super) async fn apply_object_change(
    tx: &mut Transaction<'_, Postgres>,
    bucket: uuid::Uuid,
    object_key: &str,
    old: Option<Id>,
    public: Option<bool>,
) -> Result<()> {
    if let Some(public) = public {
        let id = old.context("object stream is required for ACL changes")?;
        queries::set_stream_public(id, public)
            .execute(&mut **tx)
            .await?;
    } else {
        queries::remove_object(bucket, object_key, Id::default())
            .execute(&mut **tx)
            .await?;
        if let Some(old) = old {
            queries::retire_stream(old).execute(&mut **tx).await?;
        }
    }
    Ok(())
}

pub(in super::super) async fn change_object(pool: &PgPool, input: ObjectChange) -> Result<bool> {
    let mut tx = pool.begin().await?;
    let mut permit = Permit::for_action(
        input.principal,
        input.bucket,
        if input.public.is_some() {
            crate::authorization::Action::Acl
        } else {
            crate::authorization::Action::Delete
        },
    );
    if matches!(input.if_match, Some(s3s::dto::ETagCondition::ETag(_))) {
        permit.add(input.bucket, Action::Read);
    }
    permit.lock(&mut tx).await?;
    lock_object_bucket(&mut tx, input.bucket).await?;
    let changed = change_object_in(
        &mut tx,
        input.bucket,
        &input.key,
        input.expected,
        input.public,
        input.if_match.as_ref(),
    )
    .await?;
    tx.commit().await?;
    Ok(changed)
}

async fn lock_object_bucket(tx: &mut Transaction<'_, Postgres>, bucket: Uuid) -> Result<()> {
    let state: Option<String> = queries::lock_bucket_state(bucket)
        .fetch_optional(&mut **tx)
        .await?;
    let state = state.ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    if state != "active" {
        return Err(s3s::s3_error!(OperationAborted, "bucket is being purged").into());
    }
    Ok(())
}

async fn change_object_in(
    tx: &mut Transaction<'_, Postgres>,
    bucket: Uuid,
    object_key: &str,
    expected: Option<Id>,
    public: Option<bool>,
    if_match: Option<&s3s::dto::ETagCondition>,
) -> Result<bool> {
    super::super::transactions::redundancy::writable(tx, bucket, object_key, None).await?;
    // Lock an absent key too, then read again after any concurrent insertion commits.
    queries::ensure_object(bucket, object_key)
        .execute(&mut **tx)
        .await?;
    let old: Option<Id> = queries::lock_object_stream(bucket, object_key)
        .fetch_optional(&mut **tx)
        .await?
        .flatten();
    if expected.is_some() && expected != old {
        return Err(s3s::s3_error!(
            PreconditionFailed,
            "object changed; refresh before retrying"
        )
        .into());
    }
    if let Some(condition) = if_match {
        let etag = match old {
            Some(id) => Some(queries::stream_etag(id).fetch_one(&mut **tx).await?),
            None => None,
        };
        check_write_conditions(etag.as_deref(), Some(condition), None)?;
    }
    if let Some(public) = public {
        let id = old.ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
        apply_object_change(tx, bucket, object_key, old, Some(public)).await?;
        complete_media_operation(tx, Some(id)).await?;
        return Ok(false);
    }
    apply_object_change(tx, bucket, object_key, old, None).await?;
    complete_media_operation(tx, None).await?;
    Ok(true)
}

pub(in super::super) async fn change_node_objects(
    pool: &PgPool,
    node: &NodeState,
    key: &Key,
    input: crate::model::node_object::Request,
) -> Result<crate::model::node_object::Outcome> {
    use crate::model::node_object::Outcome;
    let hash = input.validate()?;
    let mut tx = pool.begin().await?;
    authenticate_rpc_node(&mut tx, node, key, false).await?;
    ensure!(
        matches!(node.role.as_str(), "data" | "node"),
        Error::NodeAuthentication
    );
    let session = require_node_run(&mut tx, node, input.run, true).await?;
    queries::lock_staging(format!(
        "mokyu:node-object:{}:{}",
        node.node_id, input.operation
    ))
    .execute(&mut *tx)
    .await?;
    if let Some((previous, generation, outcome)) =
        queries::node_object_change(node.node_id, input.operation)
            .fetch_optional(&mut *tx)
            .await?
    {
        ensure!(previous == hash, Error::NodeOperationConflict);
        ensure!(generation == node.credential_generation, Error::NodeExpired);
        tx.commit().await?;
        return Ok(outcome.0);
    }
    let issued = write::operation_time(input.operation)?;
    let now = queries::node_clock().fetch_one(&mut *tx).await?;
    ensure!(
        issued <= now + chrono::Duration::seconds(30)
            && issued >= now - chrono::Duration::minutes(15),
        Error::NodeOperationExpired
    );
    ensure!(
        !session.draining && queries::node_read_admission().fetch_one(&mut *tx).await?,
        Error::NodeInactive
    );
    let change = &input.change;
    let route = crate::model::ReadRoute::Bucket {
        name: change.bucket.clone(),
        key: String::new(),
        alias: None,
    };
    let bucket = queries::node_read_bucket(&route)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    let mut permit = Permit::for_action(
        Principal::Service {
            access_key: change.access.clone(),
            revision: change.credential_revision,
        },
        bucket.id,
        if change.public.is_some() {
            Action::Acl
        } else {
            Action::Delete
        },
    );
    let if_match = change
        .if_match
        .as_ref()
        .map(|value| s3s::dto::ETagCondition::parse_http_header(value.as_bytes()))
        .transpose()?;
    if matches!(if_match, Some(s3s::dto::ETagCondition::ETag(_))) {
        permit.add(bucket.id, Action::Read);
    }
    permit.lock(&mut tx).await?;
    lock_object_bucket(&mut tx, bucket.id).await?;
    // All batches acquire object locks in the same order, including duplicate keys.
    let mut keys = change.keys.clone();
    keys.sort_unstable();
    keys.dedup();
    let mut outcome = Outcome::Applied;
    for object_key in keys {
        match change_object_in(
            &mut tx,
            bucket.id,
            &object_key,
            None,
            change.public,
            if_match.as_ref(),
        )
        .await
        {
            Ok(_) => {}
            Err(error)
                if error
                    .downcast_ref::<s3s::S3Error>()
                    .is_some_and(|error| error.code() == &s3s::S3ErrorCode::NoSuchKey) =>
            {
                outcome = Outcome::NoSuchKey;
            }
            Err(error)
                if error
                    .downcast_ref::<s3s::S3Error>()
                    .is_some_and(|error| error.code() == &s3s::S3ErrorCode::PreconditionFailed) =>
            {
                outcome = Outcome::PreconditionFailed;
            }
            Err(error) => return Err(error),
        }
    }
    queries::save_node_object_change(node, input.operation, &hash, outcome)
        .execute(&mut *tx)
        .await?;
    queries::expire_node_object_changes(node.node_id)
        .execute(&mut *tx)
        .await?;
    require_node_run(&mut tx, node, input.run, true).await?;
    tx.commit().await?;
    Ok(outcome)
}

#[async_trait::async_trait]
impl StreamStore for Backend {
    async fn snapshot(
        &self,
        route: &crate::model::ReadRoute,
        access: Option<&str>,
    ) -> Result<Option<crate::model::ReadSnapshot>> {
        super::super::image_transactions::snapshot(self, route, access).await
    }
    async fn cached(&self, bucket: Uuid, key: &str) -> Result<Option<crate::model::CachedStream>> {
        queries::cached_stream(&self.pool, bucket, key).await
    }

    async fn current(&self, bucket: Uuid, key: &str) -> Result<crate::app::StoredStream> {
        queries::current_stream(bucket, key, false)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchKey).into())
    }

    async fn create(&self, input: StreamCreate) -> Result<Id> {
        create_stream(&self.pool, input, None).await
    }

    async fn append_tail(&self, tail: StreamTail) -> Result<()> {
        queries::insert_fragment(&tail).execute(&self.pool).await?;
        let mut tx = self.pool.begin().await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("fragment-persisted").await;
        queries::seal_fragment(tail.fragment.into())
            .execute(&mut *tx)
            .await?;
        queries::insert_fragment_extent(tail.stream, tail.offset, tail.size, tail.fragment.into())
            .execute(&mut *tx)
            .await?;
        queries::resize_stream(tail.stream, tail.offset + i64::from(tail.size))
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn finish(&self, stream: Id, size: i64, etag: &str, checksums: Value) -> Result<bool> {
        finish_stream(&self.pool, stream, size, etag, checksums).await
    }

    async fn learn_compression(
        &self,
        observation: crate::model::CompressionObservation,
        node: Option<Uuid>,
    ) -> Result<bool> {
        observation.validate()?;
        Ok(queries::learn_compression(observation, node)
            .execute(&self.pool)
            .await?
            .rows_affected()
            == 1)
    }
}

pub(in super::super) async fn locked_current_stream(
    db: &mut PgConnection,
    bucket: Uuid,
    key: &str,
) -> Result<Option<crate::app::StoredStream>> {
    // A joined row lock can lose its match while waiting for an overwrite to commit.
    if queries::share_object_stream(bucket, key)
        .fetch_optional(&mut *db)
        .await?
        .flatten()
        .is_none()
    {
        return Ok(None);
    }
    Ok(queries::current_stream(bucket, key, true)
        .fetch_optional(db)
        .await?)
}

pub(in super::super) async fn current_stream(
    pool: &PgPool,
    bucket: Uuid,
    key: &str,
    lease_seconds: i64,
) -> Result<(crate::app::StoredStream, Uuid)> {
    let mut tx = pool.begin().await?;
    let stream = locked_current_stream(&mut tx, bucket, key)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    let lease = create_read_lease(&mut tx, stream.id, lease_seconds).await?;
    tx.commit().await?;
    Ok((stream, lease))
}

pub(in super::super) async fn publish_import_in(
    tx: &mut Transaction<'_, Postgres>,
    task: Execution,
    id: Id,
    bucket: Uuid,
    key: &str,
    expected: Option<Id>,
    public: bool,
) -> Result<()> {
    lock_execution(tx, task).await?;
    super::super::transactions::redundancy::writable(tx, bucket, key, Some(task.id)).await?;
    let old = selected_object(tx, bucket, key, expected).await?;
    require_writing_stream(tx, id).await?;
    let crate::model::PublishingStream {
        stream,
        publication_id,
    } = super::super::queries::replica::candidate(id)
        .fetch_one(&mut **tx)
        .await?;
    ensure!(
        stream.bucket_id == bucket && stream.object_key == key && stream.public_read == public,
        Error::OperationAborted
    );
    if stream.payload_type == "raw" {
        validate_raw(tx, id, stream.size).await?;
    } else {
        require_sources(tx, Some(id), &[]).await?;
    }
    if let Some(publication) = publication_id {
        let details = serde_json::json!({"operation":"task","task":task.id,"method":"import","expected":expected,"public":public});
        super::super::replica_transactions::publish(tx, publication, bucket, key, id, &details)
            .await?;
    }
    ensure!(
        queries::publish_migration_stream(id, task.id, task.token)
            .execute(&mut **tx)
            .await?
            .rows_affected()
            == 1,
        Error::OperationAborted
    );
    queries::upsert_object(bucket, key, id)
        .execute(&mut **tx)
        .await?;
    if let Some(old) = old {
        queries::retire_stream(old).execute(&mut **tx).await?;
    }
    Ok(())
}
