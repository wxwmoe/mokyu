use super::*;

pub(in super::super) async fn learn_compression(
    pool: &sqlx::MySqlPool,
    observation: crate::model::CompressionObservation,
) -> anyhow::Result<bool> {
    let mut tx = pool.begin().await?;
    if !queries::compression_writable().fetch_one(&mut *tx).await? {
        return Ok(false);
    }
    let changed = queries::learn_compression(observation)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    tx.commit().await?;
    Ok(changed == 1)
}

pub(in super::super) async fn create_stream(
    pool: &sqlx::MySqlPool,
    input: StreamCreate,
) -> Result<Id> {
    let mut tx = pool.begin().await?;
    let result = create_stream_in(&mut tx, input).await?;
    tx.commit().await?;
    Ok(result)
}

pub(in super::super) async fn install_replica(
    db: &mut super::super::queries::replica::Tx<'_>,
    bucket: Uuid,
    key: &str,
    expected: Option<Id>,
    candidate: Id,
) -> Result<()> {
    let (old, token) = queries::lock_object_version(bucket, key)
        .fetch_one(&mut **db)
        .await?;
    ensure!(
        old == expected && token == candidate,
        s3s::s3_error!(PreconditionFailed)
    );
    let stream = super::super::queries::replica::candidate(candidate)
        .fetch_one(&mut **db)
        .await?
        .stream;
    ensure!(
        stream.state == "writing" && stream.bucket_id == bucket && stream.object_key == key,
        crate::model::Error::OperationAborted
    );
    ensure!(
        super::super::queries::replication::ready(candidate, &stream.payload_type)
            .fetch_one(&mut **db)
            .await?,
        crate::model::Error::SourceUnavailable
    );
    let mut accounts = lock_quota(db, bucket).await?;
    let previous = if let Some(id) = old {
        Some((
            id,
            queries::stream_size_etag(id).fetch_one(&mut **db).await?.0,
        ))
    } else {
        None
    };
    replace_object_quota(
        db,
        &mut accounts,
        bucket,
        key,
        previous,
        Some((candidate, stream.size)),
    )
    .await?;
    queries::publish_object(&stream).execute(&mut **db).await?;
    queries::publish_stream(candidate)
        .execute(&mut **db)
        .await?;
    queries::ready_stream_queues(db, candidate).await?;
    if let Some(id) = old {
        forget_quota_write(db, &mut accounts, id).await?;
        queries::retire_stream(id).execute(&mut **db).await?;
    }
    Ok(())
}

pub(in super::super) async fn validate_replica_node(
    db: &mut super::super::queries::replica::Tx<'_>,
    stream: Id,
    node: Option<(Uuid, Uuid)>,
) -> Result<()> {
    super::super::node_writes::verify_replica(db, stream, node).await
}
pub(in super::super) async fn abort_replica_node(
    db: &mut super::super::queries::replica::Tx<'_>,
    node: Option<(Uuid, Uuid)>,
) -> Result<()> {
    super::super::node_writes::settle_replica(db, node, false).await
}
pub(in super::super) async fn finish_replica_node(
    db: &mut super::super::queries::replica::Tx<'_>,
    _stream: Id,
    node: Option<(Uuid, Uuid)>,
) -> Result<()> {
    super::super::node_writes::settle_replica(db, node, true).await
}
pub(super) async fn reject_admission(pool: &MySqlPool, id: Id, reason: &str) -> Result<()> {
    let mut tx = pool.begin().await?;
    super::super::replica_transactions::reap_admission(&mut tx, id, Some(reason)).await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn discard_replica(
    db: &mut super::super::queries::replica::Tx<'_>,
    bucket: Uuid,
    stream: Id,
) -> Result<()> {
    let row = super::super::queries::replica::candidate(stream)
        .fetch_optional(&mut **db)
        .await?;
    if let Some(row) = row
        && row.stream.state == "writing"
        && row.stream.bucket_id == bucket
    {
        super::super::queries::replica::unclaim(bucket, &row.stream.object_key, stream)
            .execute(&mut **db)
            .await?;
        let mut accounts = lock_quota(db, bucket).await?;
        forget_quota_write(db, &mut accounts, stream).await?;
        super::super::queries::replication::abandon(stream)
            .execute(&mut **db)
            .await?;
    }
    Ok(())
}

pub(in super::super) async fn enqueue_replica(
    db: &mut super::super::queries::replica::Tx<'_>,
    bucket: Uuid,
    detail: Value,
) -> Result<Uuid> {
    let id = Uuid::now_v7();
    queries::insert_task(
        id,
        "storage_migration",
        Some(bucket),
        detail,
        crate::model::audit::current(),
    )
    .execute(&mut **db)
    .await?;
    Ok(id)
}

pub(in super::super) async fn reserve_replica(
    db: &mut super::super::queries::replica::Tx<'_>,
    input: &StreamCreate,
    size: i64,
) -> Result<Id> {
    let result = queries::insert_stream(input).execute(&mut **db).await?;
    let stream = Id::try_from(result.last_insert_id())?;
    queries::claim_object(input.bucket, &input.key, stream)
        .execute(&mut **db)
        .await?;
    supersede_object_quota(db, input.bucket, &input.key, stream).await?;
    reserve_quota_in(db, stream, size, false, None).await?;
    Ok(stream)
}

pub(in super::super) async fn create_stream_in(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    mut input: StreamCreate,
) -> Result<Id> {
    if let Some((task, token)) = input.owner {
        ensure!(
            queries::lock_owned_task(task, token)
                .fetch_optional(&mut **tx)
                .await?
                .is_some(),
            "storage migration execution lost"
        );
    }
    input
        .authority
        .add(input.bucket, crate::authorization::Action::Write);
    if input.public {
        input
            .authority
            .add(input.bucket, crate::authorization::Action::Acl);
    }
    lock_permit(tx, &input.authority).await?;
    let (state, paused, managed, required, image_policy) =
        super::super::queries::redundancy::admission(input.bucket)
            .fetch_one(&mut **tx)
            .await?;
    ensure!(state == "active", crate::model::Error::OperationAborted);
    if input.owner.is_none() || input.purpose == crate::model::StreamPurpose::Object {
        super::super::transactions::redundancy::accept_origin(
            tx,
            input.bucket,
            crate::model::redundancy::marked(&input.metadata),
        )
        .await?;
        super::super::transactions::redundancy::admitted(
            tx,
            input.bucket,
            &input.key,
            input.owner.map(|(id, _)| id),
            managed,
            image_policy.as_deref(),
        )
        .await?;
    }
    if paused && input.purpose.stream_type() == "object" {
        ensure!(
            queries::completing_upload(input.completion, input.bucket, &input.key)
                .fetch_one(&mut **tx)
                .await?,
            crate::model::Error::OperationAborted
        );
    }
    let inserted = queries::insert_stream(&input).execute(&mut **tx).await?;
    let id = Id::try_from(inserted.last_insert_id())?;
    if required != 0 && input.purpose.publishes() {
        super::super::transactions::redundancy::freeze(
            tx,
            input.bucket,
            &input.key,
            id,
            required,
            true,
        )
        .await?;
    }
    if input.purpose == crate::model::StreamPurpose::Object {
        queries::claim_object(input.bucket, &input.key, id)
            .execute(&mut **tx)
            .await?;
        supersede_object_quota(tx, input.bucket, &input.key, id).await?;
    }
    Ok(id)
}

pub(in super::super) async fn append_tail(pool: &sqlx::MySqlPool, tail: StreamTail) -> Result<()> {
    ensure!(
        tail.staging_storage.is_none() && tail.staging_key.is_none(),
        "shared staging is unavailable in single mode"
    );
    let fragment = Id::try_from(
        queries::insert_fragment(&tail)
            .execute(pool)
            .await?
            .last_insert_id(),
    )?;
    let mut tx = pool.begin().await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("fragment-persisted").await;
    writing_stream(&mut tx, tail.stream).await?;
    queries::seal_fragment(fragment).execute(&mut *tx).await?;
    queries::insert_fragment_extent(&tail, fragment)
        .execute(&mut *tx)
        .await?;
    queries::resize_writing_stream(tail.stream, tail.offset + i64::from(tail.size))
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn writing_stream(connection: &mut MySqlConnection, stream: Id) -> Result<()> {
    ensure!(
        queries::lock_writing_stream(stream)
            .fetch_optional(connection)
            .await?
            .is_some(),
        crate::model::Error::OperationAborted
    );
    Ok(())
}

pub(in super::super) async fn allocate_chunk(
    pool: &sqlx::MySqlPool,
    input: ChunkCreate,
) -> Result<(Chunk, bool)> {
    let mut tx = pool.begin().await?;
    let result = allocate_chunk_in(&mut tx, input).await?;
    tx.commit().await?;
    Ok(result)
}

pub(in super::super) async fn allocate_chunk_in(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    input: ChunkCreate,
) -> Result<(Chunk, bool)> {
    writing_stream(tx, input.stream).await?;
    if let Some(key) = input.key {
        queries::encoding_key(key, &input.algorithm)
            .fetch_one(&mut **tx)
            .await?;
    }
    queries::fail_chunk_allocations(&input)
        .execute(&mut **tx)
        .await?;
    queries::retire_failed_encodings(&input)
        .execute(&mut **tx)
        .await?;
    let (chunk, reused) = if let Some(chunk) = queries::reusable_chunk(tx, &input).await? {
        (chunk, true)
    } else {
        let chunk = Id::try_from(
            queries::insert_chunk(&input)
                .execute(&mut **tx)
                .await
                .map_err(|error| {
                    if error
                        .as_database_error()
                        .is_some_and(|e| e.is_unique_violation())
                    {
                        anyhow::Error::from(crate::model::Error::OperationAborted)
                    } else {
                        anyhow::Error::from(error)
                    }
                })?
                .last_insert_id(),
        )?;
        let encoding = Id::try_from(
            queries::allocate_encoding(chunk)
                .execute(&mut **tx)
                .await?
                .last_insert_id(),
        )?;
        queries::set_chunk_encoding(chunk, encoding)
            .execute(&mut **tx)
            .await?;
        queries::refresh_encoding_fingerprint(tx, encoding).await?;
        (queries::allocated_chunk(tx, chunk).await?, false)
    };
    if reused {
        require_sources(tx, None, &[chunk.id]).await?;
    }
    Ok((chunk, reused))
}
