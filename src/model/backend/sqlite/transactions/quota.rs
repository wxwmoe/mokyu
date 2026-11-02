use super::*;

pub(in super::super) async fn save_quota(
    pool: &sqlx::SqlitePool,
    principal: &Principal,
    scope: &str,
    id: uuid::Uuid,
    input: Limits,
) -> Result<Quota> {
    validate_scope(scope)?;
    let (bytes, inflight, buckets) = (
        number(&input.logical_bytes_limit)?,
        number(&input.inflight_bytes_limit)?,
        number(&input.bucket_count_limit)?,
    );
    ensure!(
        scope != "bucket" || inflight.is_none() && buckets.is_none(),
        s3s::s3_error!(InvalidArgument)
    );
    let mut tx = admin_transaction(pool, principal).await?;
    queries::quota_account(scope, id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    queries::update_quota(scope, id, bytes, inflight, buckets)
        .execute(&mut *tx)
        .await?;
    let mut detail = serde_json::to_value(input)?;
    detail[format!("{scope}_id")] = serde_json::json!(id);
    checkpoint_event(&mut tx, "quota.update", &id.to_string(), detail).await?;
    let row = queries::quota_account(scope, id)
        .fetch_one(&mut *tx)
        .await?
        .view(true);
    tx.commit().await?;
    Ok(row)
}

pub(in super::super) async fn reserve_quota(
    pool: &sqlx::SqlitePool,
    stream: Id,
    required: i64,
    padding: bool,
    upload: Option<(uuid::Uuid, i32)>,
) -> Result<i64> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let reserved = reserve_quota_in(&mut tx, stream, required, padding, upload).await?;
    tx.commit().await?;
    Ok(reserved)
}

pub(in super::super) async fn reserve_quota_in(
    db: &mut SqliteConnection,
    stream: Id,
    required: i64,
    padding: bool,
    upload: Option<(uuid::Uuid, i32)>,
) -> Result<i64> {
    ensure!(required >= 0, "negative quota reservation");
    let upload_bucket = if let Some((upload, _)) = upload {
        Some(
            queries::quota_upload(upload, false)
                .fetch_optional(&mut *db)
                .await?
                .ok_or(crate::model::Error::OperationAborted)?,
        )
    } else {
        None
    };
    let info = queries::quota_stream(stream).fetch_one(&mut *db).await?;
    ensure!(
        info.state == "writing"
            && upload_bucket
                .as_ref()
                .is_none_or(|(bucket, key)| *bucket == info.bucket_id
                    && key.as_slice() == info.object_key.as_bytes()),
        crate::model::Error::OperationAborted
    );
    let mut accounts = lock_quota(&mut *db, info.bucket_id).await?;
    let accepted = if let Some((upload, part)) = upload {
        queries::part_quota(upload, part, stream)
            .fetch_optional(&mut *db)
            .await?
            .ok_or(crate::model::Error::OperationAborted)?
    } else {
        ensure!(
            queries::object_claim(info.bucket_id, &info.object_key, stream)
                .fetch_one(&mut *db)
                .await?,
            crate::model::Error::OperationAborted
        );
        0
    };
    let mut reservation = if let Some(row) =
        queries::owned_quota_reservation(stream, upload.map(|v| v.0))
            .fetch_optional(&mut *db)
            .await?
    {
        row
    } else {
        let credit = queries::quota_credit(info.bucket_id, &info.object_key)
            .fetch_optional(&mut *db)
            .await?;
        let result = queries::insert_quota_reservation(
            info.bucket_id,
            &info.object_key,
            upload.is_none().then_some(stream),
            upload.map(|v| v.0),
            credit,
        )
        .execute(&mut *db)
        .await?;
        queries::quota_reservation(Id::from(result.last_insert_rowid()))
            .fetch_one(&mut *db)
            .await?
    };
    ensure!(!reservation.closed, crate::model::Error::OperationAborted);
    let owner = reservation.id;
    queries::insert_quota_write(stream, owner)
        .execute(&mut *db)
        .await?;
    let write = queries::quota_write(stream).fetch_one(&mut *db).await?;
    ensure!(write.reservation_id == owner, "quota owner mismatch");
    let mut target = required;
    if padding && required > write.allocated_bytes {
        target = required
            .checked_add(4194303)
            .map_or(required, |n| n / 4194304 * 4194304);
        let mut available = i128::from(target - write.allocated_bytes);
        for account in &accounts {
            if let Some(limit) = account.logical_bytes_limit {
                let room = (i128::from(limit)
                    - i128::from(account.used_bytes)
                    - i128::from(account.reserved_bytes))
                .max(0)
                    + i128::from((reservation.credit_bytes - reservation.logical_bytes).max(0))
                    + i128::from((accepted - write.allocated_bytes).max(0));
                available = available.min(room);
            }
            if let Some(limit) = account.inflight_bytes_limit {
                available =
                    available.min((i128::from(limit) - i128::from(account.inflight_bytes)).max(0));
            }
        }
        target =
            required.max(target.min(quota_number(i128::from(write.allocated_bytes) + available)?));
    }
    let contribution = (target - accepted).max(0);
    let logical = quota_number(
        i128::from(reservation.logical_bytes) + i128::from(contribution)
            - i128::from(write.contribution_bytes),
    )?;
    let inflight = quota_number(
        i128::from(reservation.inflight_bytes) + i128::from(target)
            - i128::from(write.allocated_bytes),
    )?;
    let credit = reservation.credit_bytes;
    change_reservation(
        &mut *db,
        &mut accounts,
        &mut reservation,
        logical,
        inflight,
        credit,
    )
    .await?;
    queries::change_quota_write(stream, target, contribution)
        .execute(&mut *db)
        .await?;
    Ok(target)
}

pub(in super::super) async fn complete_quota(
    pool: &sqlx::SqlitePool,
    upload: uuid::Uuid,
    stream: Id,
    size: i64,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    complete_quota_in(&mut tx, upload, stream, size).await?;
    tx.commit().await?;
    Ok(())
}
pub(in super::super) async fn complete_quota_in(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    upload: Uuid,
    stream: Id,
    size: i64,
) -> Result<()> {
    let (bucket, key) = queries::quota_upload(upload, true)
        .fetch_one(&mut **tx)
        .await?;
    let output = queries::quota_stream(stream).fetch_one(&mut **tx).await?;
    ensure!(
        size >= 0
            && output.bucket_id == bucket
            && output.object_key.as_bytes() == key
            && output.state == "writing",
        crate::model::Error::OperationAborted
    );
    lock_quota(tx, bucket).await?;
    let row = queries::owned_quota_reservation(Id::default(), Some(upload))
        .fetch_one(&mut **tx)
        .await?;
    ensure!(
        row.logical_bytes >= size && row.upload_id == Some(upload) && !row.closed,
        "multipart quota reservation is missing"
    );
    queries::complete_quota(upload, stream, size)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub(in super::super) async fn forget_quota_write(
    connection: &mut SqliteConnection,
    accounts: &mut [QuotaAccount; 2],
    stream: Id,
) -> Result<()> {
    let Some(write) = queries::quota_write(stream)
        .fetch_optional(&mut *connection)
        .await?
    else {
        return Ok(());
    };
    let mut row = queries::quota_reservation(write.reservation_id)
        .fetch_one(&mut *connection)
        .await?;
    if row.stream_id.is_some() {
        change_reservation(connection, accounts, &mut row, 0, 0, 0).await?;
        queries::delete_quota_reservation(row.id)
            .execute(connection)
            .await?;
    } else {
        let (logical, inflight, credit) = (
            row.logical_bytes - write.contribution_bytes,
            row.inflight_bytes - write.allocated_bytes,
            row.credit_bytes,
        );
        change_reservation(connection, accounts, &mut row, logical, inflight, credit).await?;
        queries::delete_quota_write(stream)
            .execute(&mut *connection)
            .await?;
        if row.closed
            && !queries::remaining_quota_writes(row.id)
                .fetch_one(&mut *connection)
                .await?
        {
            change_reservation(connection, accounts, &mut row, 0, 0, 0).await?;
            queries::delete_quota_reservation(row.id)
                .execute(connection)
                .await?;
        }
    }
    Ok(())
}

pub(in super::super) async fn close_upload_quota(
    connection: &mut SqliteConnection,
    accounts: &mut [QuotaAccount; 2],
    upload: uuid::Uuid,
) -> Result<()> {
    let Some(mut row) = queries::owned_quota_reservation(Id::default(), Some(upload))
        .fetch_optional(&mut *connection)
        .await?
    else {
        return Ok(());
    };
    if row.closed {
        return Ok(());
    }
    let pending = queries::pending_quota_writes(row.id)
        .fetch_one(&mut *connection)
        .await?;
    change_reservation(connection, accounts, &mut row, 0, pending, 0).await?;
    queries::clear_write_contributions(row.id)
        .execute(&mut *connection)
        .await?;
    queries::close_quota_reservation(row.id)
        .execute(&mut *connection)
        .await?;
    if pending == 0 {
        queries::delete_quota_reservation(row.id)
            .execute(connection)
            .await?;
    }
    Ok(())
}

pub(in super::super) async fn replace_object_quota(
    connection: &mut SqliteConnection,
    accounts: &mut [QuotaAccount; 2],
    bucket: uuid::Uuid,
    key: &str,
    old: Option<(Id, i64)>,
    new: Option<(Id, i64)>,
) -> Result<()> {
    if old.map(|v| v.0) == new.map(|v| v.0) {
        return Ok(());
    }
    let (mut freed, mut inflight) = (0, 0);
    if let Some((stream, _)) = new
        && let Some(row) = queries::publication_reservation(stream)
            .fetch_optional(&mut *connection)
            .await?
    {
        ensure!(
            row.bucket_id == bucket && !row.closed,
            "invalid publication reservation"
        );
        freed = (row.logical_bytes - row.credit_bytes).max(0);
        inflight = row.inflight_bytes;
        let pending = if row.upload_id.is_some() {
            queries::pending_quota_writes(row.id)
                .fetch_one(&mut *connection)
                .await?
        } else {
            0
        };
        if pending == 0 {
            queries::delete_quota_reservation(row.id)
                .execute(&mut *connection)
                .await?;
        } else {
            queries::clear_write_contributions(row.id)
                .execute(&mut *connection)
                .await?;
            queries::change_quota_reservation(row.id, 0, pending, 0)
                .execute(&mut *connection)
                .await?;
            queries::close_quota_reservation(row.id)
                .execute(&mut *connection)
                .await?;
            inflight -= pending;
        }
    }
    let credits = queries::displaced_credits(bucket, key, old.map(|v| v.0))
        .fetch_one(&mut *connection)
        .await?;
    queries::remove_displaced_credits(bucket, key, old.map(|v| v.0))
        .execute(&mut *connection)
        .await?;
    adjust_quota(
        connection,
        accounts,
        new.map_or(0, |v| v.1) - old.map_or(0, |v| v.1),
        credits - freed,
        -inflight,
        i64::from(new.is_some()) - i64::from(old.is_some()),
    )
    .await
}
