use super::*;

pub(in super::super) async fn create_multipart(
    pool: &PgPool,
    input: MultipartCreate<'_>,
) -> Result<(Uuid, bool)> {
    let mut tx = pool.begin().await?;
    let result = create_multipart_in(&mut tx, input).await?;
    tx.commit().await?;
    Ok(result)
}

pub(super) async fn create_multipart_in(
    db: &mut Transaction<'_, Postgres>,
    input: MultipartCreate<'_>,
) -> Result<(Uuid, bool)> {
    input.authority.lock(db).await?;
    let (state, paused) = queries::multipart_bucket(input.bucket)
        .fetch_one(&mut **db)
        .await?;
    super::super::transactions::redundancy::writable(db, input.bucket, input.key, None).await?;
    if state != "active" {
        return Err(s3s::s3_error!(OperationAborted).into());
    }
    let expected = if let Some(web) = &input.web {
        if let Some((id, hash)) = queries::web_upload_identity(web.user, web.client_id)
            .fetch_optional(&mut **db)
            .await?
        {
            if hash != web.request_hash {
                return Err(
                    s3s::s3_error!(InvalidRequest, "upload identity was already used").into(),
                );
            }
            return Ok((id, true));
        }
        let expected = queries::web_upload_expected(input.bucket, input.key)
            .fetch_optional(&mut **db)
            .await?
            .flatten();
        if expected.is_some() && !web.overwrite {
            return Err(crate::model::Error::ObjectAlreadyExists.into());
        }
        expected
    } else {
        None
    };
    if paused {
        return Err(s3s::s3_error!(OperationAborted).into());
    }
    if let Some(limit) = input.max_active {
        queries::lock_upload_admission().execute(&mut **db).await?;
        if queries::active_upload_count().fetch_one(&mut **db).await? >= limit {
            return Err(s3s::s3_error!(SlowDown).into());
        }
    }
    let id = queries::insert_multipart(&input)
        .fetch_one(&mut **db)
        .await?;
    if let Some(web) = input.web {
        queries::insert_web_upload(id, &web, expected)
            .execute(&mut **db)
            .await?;
        checkpoint_event(
            db,
            "upload.create",
            &id.to_string(),
            json!({"bucket_id":input.bucket,"key":input.key,"size":web.size}),
        )
        .await?;
    }
    Ok((id, false))
}

pub(in super::super) async fn begin_upload_part(
    pool: &PgPool,
    authority: &Permit,
    upload: Uuid,
    number: i32,
    stream: Id,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    authority.lock(&mut tx).await?;
    if queries::lock_upload_state(upload)
        .fetch_one(&mut *tx)
        .await?
        != "active"
    {
        return Err(s3s::s3_error!(NoSuchUpload).into());
    }
    queries::begin_upload_part(upload, number, stream)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn commit_upload_part(
    pool: &PgPool,
    authority: &Permit,
    bucket: Uuid,
    upload: Uuid,
    number: i32,
    stream: Id,
    remote: Option<&RemoteCommit<'_>>,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    commit_upload_part_in(&mut tx, authority, bucket, upload, number, stream, remote).await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn commit_upload_part_in(
    tx: &mut Transaction<'_, Postgres>,
    authority: &Permit,
    bucket: Uuid,
    upload: Uuid,
    number: i32,
    stream: Id,
    remote: Option<&RemoteCommit<'_>>,
) -> Result<()> {
    authority.lock(tx).await?;
    if let Some(input) = remote {
        raw_upload_processor(tx, input.node).await?;
    }
    if queries::multipart_bucket_state(bucket)
        .fetch_one(&mut **tx)
        .await?
        != "active"
    {
        return Err(s3s::s3_error!(OperationAborted).into());
    }
    if queries::lock_upload_state(upload)
        .fetch_one(&mut **tx)
        .await?
        != "active"
    {
        return Err(s3s::s3_error!(NoSuchUpload).into());
    }
    let (old, epoch) = queries::lock_upload_part(upload, number)
        .fetch_one(&mut **tx)
        .await?;
    if epoch != stream {
        return Err(s3s::s3_error!(OperationAborted, "part was superseded").into());
    }
    require_writing_stream(tx, stream).await?;
    let (_, _, _, size, payload, _, _) = queries::lock_writing_stream(stream)
        .fetch_one(&mut **tx)
        .await?;
    if payload == "raw" {
        validate_raw(tx, stream, size).await?;
    } else {
        require_sources(tx, Some(stream), &[]).await?;
    }
    if let Some(input) = remote {
        ensure!(
            queries::remote_part_queue(input)
                .execute(&mut **tx)
                .await?
                .rows_affected()
                == 1,
            crate::model::Error::OperationAborted
        );
    }
    queries::publish_upload_part(upload, number, stream)
        .execute(&mut **tx)
        .await?;
    queries::ready_upload_stream(stream)
        .execute(&mut **tx)
        .await?;
    if let Some(old) = old {
        queries::retire_upload_stream(old)
            .execute(&mut **tx)
            .await?;
    }
    queries::touch_upload(upload).execute(&mut **tx).await?;
    checkpoint_event(
        tx,
        "upload.part",
        &upload.to_string(),
        json!({"bucket_id":bucket,"part":number}),
    )
    .await?;
    Ok(())
}

pub(in super::super) async fn consume_upload_seed(
    pool: &PgPool,
    input: SeedConsumption<'_>,
) -> Result<()> {
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
        mark_unreferenced(&mut tx, &[input.chunk]).await?;
    } else {
        queries::slice_upload_extent(
            input.stream,
            input.offset,
            input.prefix as i32,
            (input.consumed - input.prefix) as i32,
        )
        .execute(&mut *tx)
        .await?;
    }
    queries::resize_upload_stream(
        input.stream,
        input.offset + (input.consumed - input.prefix) as i64,
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn stitch_upload_parts(pool: &PgPool, input: PartStitch) -> Result<()> {
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
    if input.right_length > 0 {
        replace_upload_range(
            &mut tx,
            input.right,
            0,
            input.right_length,
            input.chunk,
            input.left_length,
        )
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn begin_upload_completion(
    pool: &PgPool,
    input: crate::model::multipart::Completion<'_>,
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    input.authority.lock(&mut tx).await?;
    raw_upload_processor(&mut tx, input.node).await?;
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
    pool: &PgPool,
    authority: &Permit,
    upload: Uuid,
    expired: bool,
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    let result = abort_multipart_in(&mut tx, authority, upload, expired).await?;
    tx.commit().await?;
    Ok(result)
}

pub(super) async fn abort_multipart_in(
    db: &mut Transaction<'_, Postgres>,
    authority: &Permit,
    upload: Uuid,
    expired: bool,
) -> Result<bool> {
    authority.lock(db).await?;
    if expired
        && queries::lock_upload_state(upload)
            .fetch_optional(&mut **db)
            .await?
            .as_deref()
            != Some("active")
    {
        return Ok(false);
    }
    if queries::abort_multipart(upload, expired)
        .execute(&mut **db)
        .await?
        .rows_affected()
        == 0
    {
        if expired {
            return Ok(false);
        }
        if queries::completion_state(upload)
            .fetch_optional(&mut **db)
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
    queries::retire_upload_parts(&upload)
        .execute(&mut **db)
        .await?;
    queries::delete_upload_parts(&upload)
        .execute(&mut **db)
        .await?;
    if authority.principal.user_id().is_some() && crate::model::audit::active() {
        let bucket = queries::upload_bucket(upload).fetch_one(&mut **db).await?;
        checkpoint_event(
            db,
            "upload.abort",
            &upload.to_string(),
            json!({"bucket_id":bucket}),
        )
        .await?;
    }
    Ok(true)
}

pub(in super::super) async fn upload_transfers(
    pool: &PgPool,
    principal: &Principal,
    user: Uuid,
    filter: &TransferFilter,
    after: Option<(chrono::DateTime<Utc>, Uuid)>,
    id: Option<Uuid>,
    limit: i64,
) -> Result<(Vec<Transfer>, bool)> {
    let mut tx = pool.begin().await?;
    let admin = principal.lock_identity(&mut tx).await?;
    let mut query =
        queries::upload_transfers(admin, user, principal.token_id(), filter, after, id, limit);
    let mut rows: Vec<Transfer> = query.build_query_as().fetch_all(&mut *tx).await?;
    let more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    let ready: Vec<Uuid> = rows
        .iter()
        .filter(|r| r.remote_state == "stored")
        .map(|r| r.id)
        .collect();
    if !ready.is_empty() {
        queries::durable_uploads(&ready).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok((rows, more))
}
