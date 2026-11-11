use super::*;

pub(in super::super) async fn create_stream(
    pool: &PgPool,
    input: StreamCreate,
    ttl_seconds: Option<i64>,
) -> Result<Id> {
    let mut tx = pool.begin().await?;
    let id = create_stream_in(&mut tx, input, ttl_seconds).await?;
    tx.commit().await?;
    Ok(id)
}

pub(in super::super) async fn install_replica(
    db: &mut super::super::queries::replica::Tx<'_>,
    bucket: Uuid,
    key: &str,
    expected: Option<Id>,
    candidate: Id,
) -> Result<()> {
    mirroring::publication(db, candidate).await?;
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
    ensure!(
        apply_publish(db, candidate, bucket, key, old, None).await?,
        crate::model::Error::OperationAborted
    );
    Ok(())
}

pub(in super::super) async fn validate_replica_node(
    db: &mut super::super::queries::replica::Tx<'_>,
    stream: Id,
    node: Option<(Uuid, Uuid)>,
) -> Result<()> {
    if let Some((node, operation)) = node {
        let row = queries::node_write(node, operation)
            .fetch_one(&mut **db)
            .await?;
        ensure!(
            row.state == "active"
                && row.publish_hash.is_some()
                && row.stream_id.map(Id::from) == Some(stream),
            Error::OperationAborted
        );
        if let Some(source) = &row.copy_source {
            queries::node_copy_source(source)
                .fetch_optional(&mut **db)
                .await?
                .ok_or(Error::NodeExpired)?;
        }
    }
    Ok(())
}

pub(in super::super) async fn finish_replica_node(
    db: &mut super::super::queries::replica::Tx<'_>,
    stream: Id,
    node: Option<(Uuid, Uuid)>,
) -> Result<()> {
    if let Some((node, operation)) = node {
        let row = queries::node_write(node, operation)
            .fetch_one(&mut **db)
            .await?;
        let hash = row.publish_hash.ok_or(Error::OperationAborted)?;
        let source = super::super::queries::replica::candidate(stream)
            .fetch_one(&mut **db)
            .await?
            .stream;
        let (_, modified) = queries::node_write_size(stream)
            .fetch_one(&mut **db)
            .await?;
        let result = crate::model::node_write::Published {
            identity: Some((source.bucket_id, stream)),
            size: source.size,
            etag: source.etag,
            modified,
        };
        queries::seal_stream_uploads(stream)
            .execute(&mut **db)
            .await?;
        ensure!(
            queries::publish_node_write(node, operation, &hash, &result)
                .execute(&mut **db)
                .await?
                .rows_affected()
                == 1,
            Error::OperationAborted
        );
        if let Some(lease) = row.lease_id {
            queries::release_node_write_leases(&[lease])
                .execute(&mut **db)
                .await?;
        }
    }
    Ok(())
}

pub(in super::super) async fn abort_replica_node(
    db: &mut super::super::queries::replica::Tx<'_>,
    node: Option<(Uuid, Uuid)>,
) -> Result<()> {
    if let Some((node, operation)) = node {
        let row = queries::node_write(node, operation)
            .fetch_one(&mut **db)
            .await?;
        super::super::queries::replication_import::abort_write(node, operation)
            .execute(&mut **db)
            .await?;
        if let Some(lease) = row.lease_id {
            queries::release_node_write_leases(&[lease])
                .execute(&mut **db)
                .await?;
        }
    }
    Ok(())
}

pub(super) async fn reject_admission(pool: &PgPool, id: Id, reason: &str) -> Result<()> {
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
    enqueue_task(db, "storage_migration", Some(bucket), detail, true).await
}

pub(in super::super) async fn reserve_replica(
    db: &mut super::super::queries::replica::Tx<'_>,
    input: &StreamCreate,
    size: i64,
) -> Result<Id> {
    let stream = queries::insert_stream(input).fetch_one(&mut **db).await?;
    queries::claim_object(input.bucket, &input.key, stream)
        .execute(&mut **db)
        .await?;
    queries::reserve_quota(stream, size, false, None, None)
        .fetch_one(&mut **db)
        .await?;
    Ok(stream)
}

pub(in super::super) async fn create_stream_in(
    tx: &mut Transaction<'_, Postgres>,
    mut input: StreamCreate,
    ttl_seconds: Option<i64>,
) -> Result<Id> {
    if let Some((task, token)) = input.owner.as_ref() {
        let owned: Option<Uuid> = queries::lock_owned_task(task, token)
            .fetch_optional(&mut **tx)
            .await?;
        anyhow::ensure!(owned.is_some(), "storage migration lease lost");
    }
    input
        .authority
        .add(input.bucket, crate::authorization::Action::Write);
    if input.public {
        input
            .authority
            .add(input.bucket, crate::authorization::Action::Acl);
    }
    input.authority.lock(tx).await?;
    let (state, paused, managed, required, image_policy) =
        super::super::queries::redundancy::admission(input.bucket)
            .fetch_one(&mut **tx)
            .await?;
    if state != "active" {
        return Err(s3s::s3_error!(OperationAborted).into());
    }
    if input.owner.is_none()
        || matches!(
            input.purpose,
            crate::model::StreamPurpose::Object | crate::model::StreamPurpose::Mirror
        )
    {
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
        let finishing: bool =
            queries::completing_upload(input.completion, input.bucket, &input.key)
                .fetch_one(&mut **tx)
                .await?;
        if !finishing {
            return Err(
                s3s::s3_error!(OperationAborted, "new uploads are paused for this bucket").into(),
            );
        }
    }
    let id = queries::insert_stream(&input).fetch_one(&mut **tx).await?;
    if let Some(ttl) = ttl_seconds {
        create_write_lease(tx, id, ttl).await?;
    }
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
    if input.purpose == crate::model::StreamPurpose::Mirror {
        mirroring::claim(tx, &input, id).await?;
    }
    if input.purpose == crate::model::StreamPurpose::Object {
        queries::claim_object(input.bucket, &input.key, id)
            .execute(&mut **tx)
            .await?;
    }
    Ok(id)
}

pub(in super::super) async fn create_read_lease(
    tx: &mut Transaction<'_, Postgres>,
    stream: Id,
    ttl_seconds: i64,
) -> Result<uuid::Uuid> {
    Ok(queries::create_read_lease(stream, ttl_seconds)
        .fetch_one(&mut **tx)
        .await?)
}
