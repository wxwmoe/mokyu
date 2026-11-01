use super::*;

pub(in super::super) async fn publish(
    pool: &sqlx::MySqlPool,
    id: Id,
    if_match: Option<s3s::dto::ETagCondition>,
    if_none: Option<s3s::dto::ETagCondition>,
    completion: Option<(uuid::Uuid, String, serde_json::Value, Uuid)>,
) -> Result<bool> {
    let mut tx = pool.begin().await?;
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
        reject_admission(pool, id, reason).await?;
    }
    result
}

pub(in super::super) async fn publish_in(
    tx: &mut super::super::queries::replica::Tx<'_>,
    id: Id,
    if_match: Option<s3s::dto::ETagCondition>,
    if_none: Option<s3s::dto::ETagCondition>,
    completion: Option<(uuid::Uuid, String, serde_json::Value, Uuid)>,
) -> Result<bool> {
    let authority = queries::write_authorization(id)
        .fetch_one(&mut **tx)
        .await?
        .0;
    lock_permit(tx, &authority).await?;
    let upload_target = if let Some((upload, _, _, _)) = &completion {
        let target = queries::quota_upload(*upload, true)
            .fetch_optional(&mut **tx)
            .await?;
        if target.is_none() {
            return Ok(false);
        }
        target
    } else {
        None
    };
    let crate::model::PublishingStream {
        stream,
        publication_id,
    } = super::super::queries::replica::candidate(id)
        .fetch_one(&mut **tx)
        .await?;
    ensure!(
        upload_target
            .as_ref()
            .is_none_or(|(bucket, key)| *bucket == stream.bucket_id
                && key.as_slice() == stream.object_key.as_bytes()),
        crate::model::Error::OperationAborted
    );
    let (state, _, managed, _, image_policy) =
        super::super::queries::redundancy::admission(stream.bucket_id)
            .fetch_one(&mut **tx)
            .await?;
    ensure!(state == "active", crate::model::Error::OperationAborted);
    super::super::transactions::redundancy::admitted(
        tx,
        stream.bucket_id,
        &stream.object_key,
        None,
        managed,
        image_policy.as_deref(),
    )
    .await?;
    super::super::transactions::redundancy::accept_origin(
        tx,
        stream.bucket_id,
        crate::model::redundancy::marked(&stream.metadata),
    )
    .await?;
    let (old, token) = queries::lock_object_version(stream.bucket_id, &stream.object_key)
        .fetch_one(&mut **tx)
        .await?;
    ensure!(
        token == id && stream.state == "writing",
        crate::model::Error::OperationAborted
    );
    if let Some((upload, _, _, _)) = &completion
        && let Some((version, size)) = queries::web_upload_expectation(*upload)
            .fetch_optional(&mut **tx)
            .await?
    {
        ensure!(
            old == version,
            s3s::s3_error!(PreconditionFailed, "object changed since upload started")
        );
        ensure!(stream.size == size, s3s::s3_error!(IncompleteBody));
    }
    let previous = if let Some(old) = old {
        Some(queries::stream_size_etag(old).fetch_one(&mut **tx).await?)
    } else {
        None
    };
    crate::model::check_write_conditions(
        previous.as_ref().map(|v| v.1.as_str()),
        if_match.as_ref(),
        if_none.as_ref(),
    )?;
    let mut accounts = lock_quota(tx, stream.bucket_id).await?;
    if stream.payload_type == "raw" {
        validate_raw(tx, id, stream.size).await?;
    } else {
        ensure!(
            !queries::incomplete_extents(id).fetch_one(&mut **tx).await?,
            "cannot publish an incomplete generation"
        );
        let (end, contiguous) = queries::extent_continuity(id).fetch_one(&mut **tx).await?;
        ensure!(
            end == stream.size && contiguous,
            "cannot publish a mapping with gaps or overlaps"
        );
        require_sources(tx, Some(id), &[]).await?;
    }
    if let Some(publication) = publication_id {
        let intent = crate::model::write_intent(if_match.as_ref(), if_none.as_ref(), &completion)?;
        super::super::replica_transactions::publish(
            tx,
            publication,
            stream.bucket_id,
            &stream.object_key,
            id,
            &intent,
        )
        .await?;
    }
    replace_object_quota(
        tx,
        &mut accounts,
        stream.bucket_id,
        &stream.object_key,
        old.zip(previous.as_ref().map(|v| v.0)),
        Some((id, stream.size)),
    )
    .await?;
    queries::publish_object(&stream).execute(&mut **tx).await?;
    queries::publish_stream(id).execute(&mut **tx).await?;
    queries::ready_stream_queues(tx, id).await?;
    if let Some(old) = old {
        forget_quota_write(tx, &mut accounts, old).await?;
        queries::retire_stream(old).execute(&mut **tx).await?;
    }
    if let Some((upload, hash, result, token)) = completion {
        if queries::complete_upload(upload, id, &hash, &result, token)
            .execute(&mut **tx)
            .await?
            .rows_affected()
            != 1
        {
            return Ok(false);
        }
        close_upload_quota(tx, &mut accounts, upload).await?;
        for part in queries::upload_part_streams(upload)
            .fetch_all(&mut **tx)
            .await?
        {
            forget_quota_write(tx, &mut accounts, part).await?;
            queries::retire_stream(part).execute(&mut **tx).await?;
        }
        queries::delete_upload_parts(upload)
            .execute(&mut **tx)
            .await?;
        checkpoint_event(tx,"upload.complete",&upload.to_string(),serde_json::json!({"bucket_id":stream.bucket_id,"key":stream.object_key,"size":stream.size})).await?;
    }
    Ok(true)
}
