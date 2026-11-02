use super::*;

pub(in super::super) async fn create_multipart(
    pool: &sqlx::MySqlPool,
    input: MultipartCreate<'_>,
) -> Result<(uuid::Uuid, bool)> {
    let mut tx = pool.begin().await?;
    let result = create_multipart_in(&mut tx, input).await?;
    tx.commit().await?;
    Ok(result)
}

pub(in super::super) async fn create_multipart_in(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    input: MultipartCreate<'_>,
) -> Result<(uuid::Uuid, bool)> {
    let mut authority = input.authority.clone();
    authority.add(input.bucket, crate::authorization::Action::Write);
    if input.public {
        authority.add(input.bucket, crate::authorization::Action::Acl);
    }
    lock_permit(tx, &authority).await?;
    super::super::transactions::redundancy::writable(tx, input.bucket, input.key, None).await?;
    let (state, paused) = queries::lock_bucket_uploads(input.bucket)
        .fetch_one(&mut **tx)
        .await?;
    ensure!(state == "active", crate::model::Error::OperationAborted);
    let expected = if let Some(web) = &input.web {
        if let Some((id, hash)) = queries::web_upload_identity(web.user, web.client_id)
            .fetch_optional(&mut **tx)
            .await?
        {
            ensure!(
                hash == web.request_hash,
                s3s::s3_error!(InvalidRequest, "upload identity was already used")
            );
            return Ok((id, true));
        }
        let expected = queries::web_upload_expected(input.bucket, input.key)
            .fetch_optional(&mut **tx)
            .await?
            .flatten();
        ensure!(
            expected.is_none() || web.overwrite,
            crate::model::Error::ObjectAlreadyExists
        );
        expected
    } else {
        None
    };
    ensure!(!paused, crate::model::Error::OperationAborted);
    if let Some(limit) = input.max_active {
        ensure!(
            queries::active_upload_count().fetch_one(&mut **tx).await? < limit,
            s3s::s3_error!(SlowDown)
        );
    }
    let id = uuid::Uuid::new_v4();
    queries::insert_multipart(id, &input)
        .execute(&mut **tx)
        .await?;
    if let Some(web) = input.web {
        queries::insert_web_upload(id, &web, expected)
            .execute(&mut **tx)
            .await?;
        checkpoint_event(
            tx,
            "upload.create",
            &id.to_string(),
            serde_json::json!({"bucket_id":input.bucket,"key":input.key,"size":web.size}),
        )
        .await?;
    }

    Ok((id, false))
}

pub(in super::super) async fn begin_upload_part(
    pool: &sqlx::MySqlPool,
    authority: &crate::authorization::Permit,
    upload: uuid::Uuid,
    number: i32,
    stream: Id,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    begin_upload_part_in(&mut tx, authority, upload, number, stream).await?;
    tx.commit().await?;
    Ok(())
}
pub(in super::super) async fn begin_upload_part_in(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    authority: &crate::authorization::Permit,
    upload: Uuid,
    number: i32,
    stream: Id,
) -> Result<()> {
    lock_permit(tx, authority).await?;
    let parent = queries::lock_upload(upload)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchUpload))?;
    ensure!(parent.state == "active", s3s::s3_error!(NoSuchUpload));
    let info = queries::quota_stream(stream).fetch_one(&mut **tx).await?;
    ensure!(
        info.bucket_id == parent.bucket_id
            && info.object_key == parent.object_key
            && info.stream_type == "part"
            && info.state == "writing",
        crate::model::Error::OperationAborted
    );
    if let Some((_, token, _)) = queries::lock_upload_part(upload, number)
        .fetch_optional(&mut **tx)
        .await?
        && token != stream
    {
        let mut accounts = lock_quota(tx, parent.bucket_id).await?;
        if let Some(write) = queries::quota_write(token)
            .fetch_optional(&mut **tx)
            .await?
        {
            let mut row = queries::owned_quota_reservation(Id::default(), Some(upload))
                .fetch_one(&mut **tx)
                .await?;
            ensure!(
                write.reservation_id == row.id && !row.closed,
                "invalid part reservation"
            );
            let (logical, inflight, credit) = (
                row.logical_bytes - write.contribution_bytes,
                row.inflight_bytes,
                row.credit_bytes,
            );
            change_reservation(tx, &mut accounts, &mut row, logical, inflight, credit).await?;
            queries::change_quota_write(token, write.allocated_bytes, 0)
                .execute(&mut **tx)
                .await?;
        }
    }
    queries::begin_upload_part(upload, number, stream)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub(in super::super) async fn commit_upload_part(
    pool: &sqlx::MySqlPool,
    authority: &crate::authorization::Permit,
    bucket: uuid::Uuid,
    upload: uuid::Uuid,
    number: i32,
    stream: Id,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    commit_upload_part_in(&mut tx, authority, bucket, upload, number, stream).await?;
    tx.commit().await?;
    Ok(())
}
pub(in super::super) async fn commit_upload_part_in(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    authority: &crate::authorization::Permit,
    bucket: Uuid,
    upload: Uuid,
    number: i32,
    stream: Id,
) -> Result<()> {
    lock_permit(tx, authority).await?;
    let (state, _) = queries::lock_bucket_uploads(bucket)
        .fetch_one(&mut **tx)
        .await?;
    ensure!(state == "active", crate::model::Error::OperationAborted);
    let parent = queries::lock_upload(upload)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchUpload))?;
    ensure!(parent.state == "active", s3s::s3_error!(NoSuchUpload));
    let info = queries::publish_stream_record(stream)
        .fetch_one(&mut **tx)
        .await?;
    ensure!(
        parent.bucket_id == bucket
            && info.bucket_id == bucket
            && info.object_key == parent.object_key
            && info.stream_type == "part",
        crate::model::Error::OperationAborted
    );
    let (old, token, accepted) = queries::lock_upload_part(upload, number)
        .fetch_one(&mut **tx)
        .await?;
    ensure!(token == stream, crate::model::Error::OperationAborted);
    if old == Some(stream) && info.state == "ready" {
        return Ok(());
    }
    ensure!(
        info.state == "writing",
        crate::model::Error::OperationAborted
    );
    let mut accounts = lock_quota(tx, bucket).await?;
    if info.payload_type == "raw" {
        validate_raw(tx, stream, info.size).await?;
    } else {
        let (end, contiguous) = queries::extent_continuity(stream)
            .fetch_one(&mut **tx)
            .await?;
        ensure!(
            end == info.size && contiguous,
            "part mapping has gaps or overlaps"
        );
        require_sources(tx, Some(stream), &[]).await?;
    }
    let mut row = queries::owned_quota_reservation(Id::default(), Some(upload))
        .fetch_one(&mut **tx)
        .await?;
    let write = queries::quota_write(stream).fetch_one(&mut **tx).await?;
    ensure!(
        write.reservation_id == row.id && write.allocated_bytes >= info.size && !row.closed,
        "part is not reserved"
    );
    let logical = quota_number(
        i128::from(row.logical_bytes) + i128::from(info.size)
            - i128::from(accepted)
            - i128::from(write.contribution_bytes),
    )?;
    let inflight = quota_number(
        i128::from(row.inflight_bytes) + i128::from(info.size)
            - i128::from(accepted)
            - i128::from(write.allocated_bytes),
    )?;
    let credit = row.credit_bytes;
    change_reservation(tx, &mut accounts, &mut row, logical, inflight, credit).await?;
    queries::delete_quota_write(stream)
        .execute(&mut **tx)
        .await?;
    queries::publish_upload_part(upload, number, stream, info.size)
        .execute(&mut **tx)
        .await?;
    queries::publish_stream(stream).execute(&mut **tx).await?;
    queries::ready_stream_queues(tx, stream).await?;
    if let Some(old) = old {
        forget_quota_write(tx, &mut accounts, old).await?;
        queries::retire_stream(old).execute(&mut **tx).await?;
    }
    queries::touch_upload(upload).execute(&mut **tx).await?;
    checkpoint_event(
        tx,
        "upload.part",
        &upload.to_string(),
        serde_json::json!({"bucket_id":bucket,"part":number}),
    )
    .await?;
    Ok(())
}

pub(in super::super) async fn retire_upload_work(pool: &sqlx::MySqlPool, stream: Id) -> Result<()> {
    let mut tx = pool.begin().await?;
    let info = queries::publish_stream_record(stream)
        .fetch_one(&mut *tx)
        .await?;
    let mut accounts = lock_quota(&mut tx, info.bucket_id).await?;
    forget_quota_write(&mut tx, &mut accounts, stream).await?;
    queries::retire_stream(stream).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn begin_upload_completion(
    pool: &sqlx::MySqlPool,
    input: crate::model::multipart::Completion<'_>,
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    lock_permit(&mut tx, input.authority).await?;
    let (state, hash, live) = queries::completion_state(input.upload)
        .fetch_one(&mut *tx)
        .await?;
    if matches!(state.as_str(), "completing" | "completed") {
        ensure!(
            hash.as_deref() == Some(input.hash),
            s3s::s3_error!(InvalidRequest, "upload has another completion manifest")
        );
        if live || state == "completed" {
            return Ok(false);
        }
    } else {
        ensure!(state == "active", s3s::s3_error!(OperationAborted));
    }
    let parts: std::collections::BTreeMap<_, _> = queries::completion_parts(input.upload)
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .collect();
    ensure!(
        !input.parts.is_empty()
            && input
                .parts
                .iter()
                .all(|(number, stream)| parts.get(number) == Some(stream)),
        s3s::s3_error!(InvalidPart, "selected part generation changed")
    );
    queries::begin_upload_completion(&input)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(true)
}

pub(in super::super) async fn abort_multipart(
    pool: &sqlx::MySqlPool,
    authority: &crate::authorization::Permit,
    upload: uuid::Uuid,
    expired: bool,
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    let result = abort_multipart_in(&mut tx, authority, upload, expired).await?;
    tx.commit().await?;
    Ok(result)
}

pub(in super::super) async fn abort_multipart_in(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    authority: &crate::authorization::Permit,
    upload: Uuid,
    expired: bool,
) -> Result<bool> {
    lock_permit(tx, authority).await?;
    let Some(parent) = queries::lock_upload(upload)
        .fetch_optional(&mut **tx)
        .await?
    else {
        if expired {
            return Ok(false);
        }
        return Err(s3s::s3_error!(NoSuchUpload).into());
    };
    if expired && parent.state != "active" {
        return Ok(false);
    }
    ensure!(
        matches!(parent.state.as_str(), "active" | "completing"),
        s3s::s3_error!(NoSuchUpload)
    );
    if queries::abort_multipart(upload, expired)
        .execute(&mut **tx)
        .await?
        .rows_affected()
        == 0
    {
        if expired {
            return Ok(false);
        }
        if queries::completion_state(upload)
            .fetch_optional(&mut **tx)
            .await?
            .is_some_and(|(state, _, live)| state == "completing" && live)
        {
            return Err(s3s::s3_error!(
                OperationAborted,
                "multipart publication is already in progress"
            )
            .into());
        }
        return Err(s3s::s3_error!(NoSuchUpload).into());
    }
    let mut accounts = lock_quota(tx, parent.bucket_id).await?;
    close_upload_quota(tx, &mut accounts, upload).await?;
    for stream in queries::upload_part_streams(upload)
        .fetch_all(&mut **tx)
        .await?
    {
        forget_quota_write(tx, &mut accounts, stream).await?;
        queries::retire_stream(stream).execute(&mut **tx).await?;
    }
    queries::delete_upload_parts(upload)
        .execute(&mut **tx)
        .await?;
    if authority.principal.user_id().is_some() && crate::model::audit::active() {
        checkpoint_event(
            tx,
            "upload.abort",
            &upload.to_string(),
            serde_json::json!({"bucket_id":parent.bucket_id}),
        )
        .await?;
    }

    Ok(true)
}

pub(in super::super) async fn upload_transfers(
    pool: &sqlx::MySqlPool,
    principal: &Principal,
    user: uuid::Uuid,
    filter: &TransferFilter,
    after: Option<(chrono::DateTime<chrono::Utc>, uuid::Uuid)>,
    id: Option<uuid::Uuid>,
    limit: i64,
) -> Result<(Vec<Transfer>, bool)> {
    let mut tx = pool.begin().await?;
    let admin = queries::validate_identity(principal, &mut tx, true).await?;
    let mut query =
        queries::upload_transfers(admin, user, principal.token_id(), filter, after, id, limit);
    let mut rows: Vec<Transfer> = query.build_query_as().fetch_all(&mut *tx).await?;
    let more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    let ready: Vec<_> = rows
        .iter()
        .filter(|row| row.remote_state == "stored")
        .map(|row| row.id)
        .collect();
    queries::durable_uploads(&mut tx, &ready).await?;
    tx.commit().await?;
    Ok((rows, more))
}

async fn replace_upload_range(
    connection: &mut MySqlConnection,
    stream: Id,
    offset: i64,
    length: usize,
    chunk: Id,
    source: usize,
) -> Result<()> {
    if length == 0 {
        return Ok(());
    }
    ensure!(
        queries::lock_chunk(chunk)
            .fetch_one(&mut *connection)
            .await?
            == "ready",
        crate::model::Error::OperationAborted
    );
    let end = offset + i64::try_from(length)?;
    let rows = queries::lock_upload_extents(stream, offset, end)
        .fetch_all(&mut *connection)
        .await?;
    let mut covered = offset;
    let mut previous = Vec::new();
    for row in rows {
        if let Some(chunk) = row.chunk_id {
            previous.push(chunk);
        }
        ensure!(row.offset_bytes <= covered, "extent replacement gap");
        let row_end = row.offset_bytes + i64::from(row.length);
        covered = covered.max(row_end.min(end));
        queries::remove_upload_extent(stream, row.offset_bytes)
            .execute(&mut *connection)
            .await?;
        if row.offset_bytes < offset {
            queries::insert_upload_extent(
                &row,
                row.offset_bytes,
                i32::try_from(offset - row.offset_bytes)?,
                row.source_offset,
            )
            .execute(&mut *connection)
            .await?;
        }
        if row_end > end {
            queries::insert_upload_extent(
                &row,
                end,
                i32::try_from(row_end - end)?,
                row.source_offset + i32::try_from(end - row.offset_bytes)?,
            )
            .execute(&mut *connection)
            .await?;
        }
    }
    ensure!(covered == end, "extent replacement incomplete");
    queries::insert_upload_chunk_extent(
        stream,
        offset,
        i32::try_from(length)?,
        chunk,
        i32::try_from(source)?,
    )
    .execute(&mut *connection)
    .await?;
    queries::changed_references(connection, &[chunk], true).await?;
    queries::mark_unreferenced_chunks(connection, &previous).await?;
    Ok(())
}

pub(in super::super) async fn stitch_upload_parts(
    pool: &sqlx::MySqlPool,
    input: PartStitch,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    lock_splice_sources(&mut tx, &[input.left, input.right], input.chunk).await?;
    replace_upload_range(
        &mut tx,
        input.left,
        input.offset,
        input.left_length,
        input.chunk,
        0,
    )
    .await?;
    replace_upload_range(
        &mut tx,
        input.right,
        0,
        input.right_length,
        input.chunk,
        input.left_length,
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn consume_upload_seed(
    pool: &sqlx::MySqlPool,
    input: SeedConsumption<'_>,
) -> Result<()> {
    ensure!(input.prefix <= input.consumed, "invalid consumed prefix");
    let mut tx = pool.begin().await?;
    let active = queries::lock_upload_active(input.upload)
        .fetch_one(&mut *tx)
        .await?;
    let mut streams: Vec<_> = input.spans.iter().map(|span| span.0).collect();
    streams.push(input.stream);
    streams.sort_unstable();
    streams.dedup();
    lock_splice_sources(&mut tx, &streams, input.chunk).await?;
    let mut remaining = input.prefix;
    let mut chunk_offset = 0;
    while remaining > 0 {
        let span = input.spans.first_mut().context("missing prefix span")?;
        let n = remaining.min(span.2);
        if active {
            replace_upload_range(&mut tx, span.0, span.1, n, input.chunk, chunk_offset).await?;
        }
        span.1 += n as i64;
        span.2 -= n;
        remaining -= n;
        chunk_offset += n;
        if span.2 == 0 {
            input.spans.remove(0);
        }
    }
    if input.prefix == input.consumed {
        queries::remove_upload_extent(input.stream, input.offset)
            .execute(&mut *tx)
            .await?;
        queries::mark_unreferenced_chunks(&mut tx, &[input.chunk]).await?;
    } else {
        queries::slice_upload_extent(
            input.stream,
            input.offset,
            i32::try_from(input.prefix)?,
            i32::try_from(input.consumed - input.prefix)?,
        )
        .execute(&mut *tx)
        .await?;
        queries::changed_references(&mut tx, &[input.chunk], true).await?;
    }
    queries::resize_upload_stream(
        input.stream,
        input.offset + i64::try_from(input.consumed - input.prefix)?,
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}
