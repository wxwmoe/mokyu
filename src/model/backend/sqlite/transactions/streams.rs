use super::*;

pub(in super::super) async fn change_reservation(
    connection: &mut SqliteConnection,
    accounts: &mut [QuotaAccount; 2],
    reservation: &mut QuotaReservation,
    logical: i64,
    inflight: i64,
    credit: i64,
) -> Result<()> {
    ensure!(
        reservation.bucket_id == accounts[1].scope_id,
        "quota scope mismatch"
    );
    quota_number(i128::from(logical))?;
    quota_number(i128::from(inflight))?;
    quota_number(i128::from(credit))?;
    let reserved =
        (logical - credit).max(0) - (reservation.logical_bytes - reservation.credit_bytes).max(0);
    adjust_quota(
        connection,
        accounts,
        0,
        reserved,
        inflight - reservation.inflight_bytes,
        0,
    )
    .await?;
    queries::change_quota_reservation(reservation.id, logical, inflight, credit)
        .execute(connection)
        .await?;
    reservation.logical_bytes = logical;
    reservation.inflight_bytes = inflight;
    reservation.credit_bytes = credit;
    Ok(())
}

pub(super) async fn supersede_object_quota(
    connection: &mut SqliteConnection,
    bucket: uuid::Uuid,
    key: &str,
    stream: Id,
) -> Result<()> {
    let mut accounts = lock_quota(connection, bucket).await?;
    for mut row in queries::superseded_reservations(bucket, key, stream)
        .fetch_all(&mut *connection)
        .await?
    {
        let inflight = row.inflight_bytes;
        change_reservation(connection, &mut accounts, &mut row, 0, inflight, 0).await?;
        queries::clear_quota_credit(row.id)
            .execute(&mut *connection)
            .await?;
        queries::clear_write_contributions(row.id)
            .execute(&mut *connection)
            .await?;
    }
    Ok(())
}

pub(in super::super) async fn create_stream(
    pool: &sqlx::SqlitePool,
    input: StreamCreate,
) -> Result<Id> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
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
    _db: &mut super::super::queries::replica::Tx<'_>,
    _stream: Id,
    node: Option<(Uuid, Uuid)>,
) -> Result<()> {
    ensure!(node.is_none(), crate::model::Error::OperationAborted);
    Ok(())
}

pub(in super::super) async fn abort_replica_node(
    _db: &mut super::super::queries::replica::Tx<'_>,
    node: Option<(Uuid, Uuid)>,
) -> Result<()> {
    ensure!(node.is_none(), crate::model::Error::OperationAborted);
    Ok(())
}

pub(super) async fn reject_admission(pool: &SqlitePool, id: Id, reason: &str) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
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
    let stream = Id::from(result.last_insert_rowid());
    queries::claim_object(input.bucket, &input.key, stream)
        .execute(&mut **db)
        .await?;
    supersede_object_quota(db, input.bucket, &input.key, stream).await?;
    reserve_quota_in(db, stream, size, false, None).await?;
    Ok(stream)
}

pub(in super::super) async fn create_stream_in(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
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
    let id = Id::from(inserted.last_insert_rowid());
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
