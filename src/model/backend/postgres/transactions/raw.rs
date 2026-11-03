use super::*;

pub(in super::super) async fn copy_raw_references(
    pool: &sqlx::PgPool,
    destination: Id,
    source: Id,
    storage: Uuid,
    revision: i64,
    rows: &[crate::model::raw::RawRange],
) -> Result<()> {
    let mut tx = pool.begin().await?;
    copy_raw_references_in(&mut tx, destination, source, storage, revision, rows).await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn copy_raw_references_in(
    tx: &mut Transaction<'_, Postgres>,
    destination: Id,
    source: Id,
    storage: Uuid,
    revision: i64,
    rows: &[crate::model::raw::RawRange],
) -> Result<()> {
    let (offset, end, sources) = crate::model::raw::copy_window(storage, rows)?;
    require_writing_stream(tx, destination).await?;
    let (storage_type, unencoded, current, cdc, _) = queries::raw_target(destination, storage)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or(crate::model::Error::OperationAborted)?;
    ensure!(
        matches!(storage_type.as_str(), "local" | "s3") && unencoded && !cdc && current == revision,
        crate::model::Error::OperationAborted
    );
    if offset == 0 {
        ensure!(
            queries::raw_prepare(destination)
                .execute(&mut **tx)
                .await?
                .rows_affected()
                == 1,
            crate::model::Error::OperationAborted
        );
    }
    ensure!(
        queries::raw_copy_position(destination, offset)
            .fetch_optional(&mut **tx)
            .await?
            == Some(destination),
        crate::model::Error::OperationAborted
    );
    let locked: Vec<Id> = queries::raw_copy_sources(storage, &sources)
        .build_query_scalar()
        .fetch_all(&mut **tx)
        .await?;
    ensure!(locked == sources, crate::model::Error::SourceUnavailable);
    ensure!(
        queries::raw_copy_ranges(destination, source, offset, end, &sources)
            .build()
            .execute(&mut **tx)
            .await?
            .rows_affected()
            == rows.len() as u64,
        crate::model::Error::OperationAborted
    );
    queries::raw_copy_reference(&sources)
        .build()
        .execute(&mut **tx)
        .await?;
    queries::resize_writing_stream(destination, end)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub(in super::super) async fn begin_raw(
    pool: &sqlx::PgPool,
    stream: Id,
    storage: Uuid,
    revision: i64,
    encoding: Option<&crate::model::cache::EncodingPolicy>,
    durability: &'static str,
) -> Result<(Id, Uuid)> {
    let mut tx = pool.begin().await?;
    let (storage_type, unencoded, current, cdc, background) = queries::raw_target(stream, storage)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(crate::model::Error::OperationAborted)?;
    ensure!(
        matches!(storage_type.as_str(), "local" | "s3")
            && current == revision
            && if encoding.is_some() {
                cdc && background
            } else {
                unencoded && !cdc
            },
        crate::model::Error::OperationAborted
    );
    ensure!(
        queries::raw_prepare(stream)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    let id = queries::raw_insert(
        stream,
        storage,
        revision,
        if storage_type == "local" {
            "local"
        } else {
            "shared_s3"
        },
        durability,
    )
    .fetch_one(&mut *tx)
    .await?;
    if let Some(encoding) = encoding {
        encoding.validate()?;
        ensure!(
            queries::conversion_key(storage, encoding.block_key_id)
                .fetch_one(&mut *tx)
                .await?,
            crate::model::Error::OperationAborted
        );
        if let Some(key) = encoding.block_key_id {
            let (_, algorithm) = queries::replacement_key(key)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(crate::model::Error::OperationAborted)?;
            ensure!(
                algorithm == encoding.algorithm,
                crate::model::Error::OperationAborted
            );
        }
        enqueue_conversion(&mut tx, stream, id.into(), encoding).await?;
    } else if background {
        queries::raw_enqueue_processing(stream, id.into())
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok((id.into(), id))
}

pub(super) async fn enqueue_conversion(
    db: &mut PgConnection,
    stream: Id,
    source: Id,
    encoding: &crate::model::cache::EncodingPolicy,
) -> Result<()> {
    encoding.validate()?;
    let (bucket, object_key, payload_revision) = queries::conversion_stream(stream)
        .fetch_one(&mut *db)
        .await?;
    let job = crate::model::raw::Conversion {
        stream,
        source,
        bucket,
        object_key,
        payload_revision,
        encoding: encoding.clone(),
        target_key_id: encoding.block_key_id,
    };
    let mut detail = serde_json::to_value(job)?;
    detail["operation"] = json!("cdc");
    let task = Uuid::new_v4();
    insert_task(&mut *db, task, "upload", Some(bucket), detail, None).await?;
    queries::conversion_link(source, task).execute(db).await?;
    Ok(())
}

pub(in super::super) async fn finish_raw(
    pool: &sqlx::PgPool,
    stream: Id,
    source: Id,
    size: i64,
    digest: &[u8],
    cached: Option<(i64, bool, bool, Option<Uuid>)>,
    node: crate::model::raw::Processor<'_>,
) -> Result<()> {
    ensure!(
        size >= 0 && digest.len() == 16,
        "invalid received raw source"
    );
    let mut tx = pool.begin().await?;
    if cached.is_some() {
        raw_upload_processor(&mut tx, node).await?;
    }
    ensure!(
        queries::raw_lock_stream(stream)
            .fetch_optional(&mut *tx)
            .await?
            .is_some(),
        crate::model::Error::OperationAborted
    );
    ensure!(
        queries::raw_finish(stream, source, size, digest, cached.is_none())
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    queries::raw_attach(stream, source, size)
        .execute(&mut *tx)
        .await?;
    if let Some((revision, deduplicate, background, boot)) = cached {
        ensure!(
            queries::raw_stage(source, revision, boot)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            crate::model::Error::OperationAborted
        );
        queries::raw_optional_delete(source)
            .execute(&mut *tx)
            .await?;
        queries::raw_queue(source, deduplicate, background, node.map(|n| n.0.node_id))
            .execute(&mut *tx)
            .await?;
    }

    tx.commit().await?;
    Ok(())
}

pub(super) async fn validate_raw(db: &mut sqlx::PgConnection, stream: Id, size: i64) -> Result<()> {
    let sources = queries::raw_sources_lock(stream)
        .fetch_all(&mut *db)
        .await?;
    ensure!(!sources.is_empty(), "raw generation has no source");
    for source in sources {
        source.validate()?;
    }
    let (end, contiguous) = queries::raw_continuity(stream).fetch_one(db).await?;
    ensure!(
        end == size && contiguous,
        "cannot publish an incomplete raw generation"
    );
    Ok(())
}

pub(in super::super) async fn deduplicate_raw(
    pool: &sqlx::PgPool,
    stream: Id,
    source: Id,
    hash: &[u8],
) -> Result<bool> {
    ensure!(hash.len() == 32, "invalid raw content hash");
    let received = queries::raw_source(source).fetch_one(pool).await?;
    let mut tx = pool.begin().await?;
    queries::claim_chunk_hash(received.storage_id, hash)
        .execute(&mut *tx)
        .await?;
    ensure!(
        queries::raw_lock_stream(stream)
            .fetch_optional(&mut *tx)
            .await?
            .is_some(),
        crate::model::Error::OperationAborted
    );
    let current = queries::raw_source(source).fetch_one(&mut *tx).await?;
    ensure!(current == received, crate::model::Error::SourceUnavailable);
    current.validate()?;
    validate_raw(&mut tx, stream, current.size).await?;
    let reused = share_raw(&mut tx, stream, &current, hash).await?;
    tx.commit().await?;
    Ok(reused)
}

pub(super) async fn share_raw(
    db: &mut sqlx::PgConnection,
    stream: Id,
    current: &crate::model::raw::RawSource,
    hash: &[u8],
) -> Result<bool> {
    let duplicate = queries::raw_duplicate(current.storage_id, hash, current.size)
        .fetch_optional(&mut *db)
        .await?;
    let mut reused = false;
    if let Some(candidate) = duplicate {
        if candidate.id == current.id {
            return Ok(false);
        }
        if candidate.validate().is_ok()
            && !candidate.pending_upload
            && !current.pending_upload
            && candidate.medium == current.medium
        {
            super::super::raw_pages::inherit(db, current.id, &candidate).await?;
            ensure!(
                queries::raw_share(stream, current.id, candidate.id, current.size)
                    .execute(&mut *db)
                    .await?
                    .rows_affected()
                    == 1,
                crate::model::Error::OperationAborted
            );
            queries::raw_reference(candidate.id)
                .execute(&mut *db)
                .await?;
            queries::raw_touch_unreferenced(current.id)
                .execute(&mut *db)
                .await?;
            reused = true;
        } else {
            queries::raw_hash(candidate.id, None)
                .execute(&mut *db)
                .await?;
        }
    }
    if !reused {
        queries::raw_hash(current.id, Some(hash))
            .execute(&mut *db)
            .await?;
    }
    Ok(reused)
}

pub(in super::super) async fn multipart_file(
    pool: &PgPool,
    upload: Uuid,
    node: Option<Uuid>,
) -> Result<Uuid> {
    let mut tx = pool.begin().await?;
    ensure!(
        queries::lock_local_upload(upload)
            .fetch_optional(&mut *tx)
            .await?
            .as_deref()
            == Some("active"),
        crate::model::Error::OperationAborted
    );
    let id = if let Some(id) = queries::current_multipart_file(upload, node)
        .fetch_optional(&mut *tx)
        .await?
    {
        id
    } else {
        queries::insert_multipart_file(upload, node)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(crate::model::Error::OperationAborted)?
    };
    tx.commit().await?;
    Ok(id)
}

pub(in super::super) async fn remap_multipart_file(
    pool: &PgPool,
    upload: Uuid,
    source: Id,
    ranges: &[crate::model::multipart::FileRemap],
) -> Result<()> {
    ensure!(
        !ranges.is_empty() && ranges.len() <= 64,
        "invalid multipart checkpoint batch"
    );
    let mut tx = pool.begin().await?;
    ensure!(
        queries::lock_local_upload(upload)
            .fetch_optional(&mut *tx)
            .await?
            .as_deref()
            == Some("completing"),
        crate::model::Error::OperationAborted
    );
    queries::insert_multipart_checkpoint(upload, source)
        .execute(&mut *tx)
        .await?;
    for range in ranges {
        ensure!(
            queries::remap_multipart_range(upload, source, range)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            crate::model::Error::OperationAborted
        );
    }
    let files: std::collections::BTreeSet<_> = ranges.iter().map(|range| range.file).collect();
    for file in files {
        queries::pending_multipart_reclaim(file)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn claim_raw_processing(
    pool: &sqlx::PgPool,
    node: crate::model::raw::Processor<'_>,
    token: Uuid,
) -> Result<Option<crate::model::raw::RawProcessing>> {
    let mut tx = pool.begin().await?;
    raw_processor(&mut tx, node).await?;
    let single = node.is_none();
    let Some(stream) = queries::raw_processing_due(single)
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(None);
    };
    queries::raw_processing_claim(stream, token, single)
        .execute(&mut *tx)
        .await?;
    let work = queries::raw_processing_record(stream)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(Some(work))
}

pub(in super::super) async fn finish_raw_processing(
    pool: &sqlx::PgPool,
    work: &crate::model::raw::RawProcessing,
    source: &crate::model::raw::RawSource,
    hash: &[u8],
    lease: Option<Uuid>,
    node: crate::model::raw::Processor<'_>,
) -> Result<bool> {
    ensure!(hash.len() == 32, "invalid raw content hash");
    let mut tx = pool.begin().await?;
    raw_processor(&mut tx, node).await?;
    let result = finish_raw_processing_in(&mut tx, work, source, hash, lease).await?;
    tx.commit().await?;
    Ok(result)
}

pub(super) async fn finish_raw_processing_in(
    tx: &mut Transaction<'_, Postgres>,
    work: &crate::model::raw::RawProcessing,
    source: &crate::model::raw::RawSource,
    hash: &[u8],
    lease: Option<Uuid>,
) -> Result<bool> {
    ensure!(hash.len() == 32, "invalid raw content hash");
    queries::claim_chunk_hash(source.storage_id, hash)
        .execute(&mut **tx)
        .await?;
    let current_stream = queries::raw_processing_stream(work)
        .fetch_optional(&mut **tx)
        .await?;
    ensure!(
        queries::raw_processing_owned(work)
            .fetch_optional(&mut **tx)
            .await?
            == Some(source.id),
        crate::model::Error::OperationAborted
    );
    if current_stream.is_none() {
        queries::raw_processing_delete(work)
            .execute(&mut **tx)
            .await?;
        return Ok(true);
    }
    if let Some(lease) = lease {
        if queries::raw_processing_readers(work.stream, lease)
            .fetch_one(&mut **tx)
            .await?
        {
            return Ok(false);
        }
    }
    let current = queries::raw_source(source.id).fetch_one(&mut **tx).await?;
    ensure!(current == *source, crate::model::Error::SourceUnavailable);
    current.validate()?;
    validate_raw(tx, work.stream, current.size).await?;
    if share_raw(tx, work.stream, &current, hash).await? {
        queries::raw_processing_changed(work.stream)
            .execute(&mut **tx)
            .await?;
    }
    queries::raw_processing_delete(work)
        .execute(&mut **tx)
        .await?;
    Ok(true)
}

async fn raw_processor(
    db: &mut PgConnection,
    node: crate::model::raw::Processor<'_>,
) -> Result<()> {
    if let Some((state, role, key)) = node {
        validate_node(&mut *db, state, role, key).await?;
        ensure!(
            queries::repair_node(state.node_id)
                .fetch_optional(&mut *db)
                .await?
                .is_some(),
            crate::model::Error::OperationAborted
        );
        ensure!(
            queries::raw_processor_mode(true).fetch_one(db).await?,
            crate::model::Error::OperationAborted
        );
    } else {
        ensure!(
            queries::raw_processor_mode(false).fetch_one(db).await?,
            crate::model::Error::OperationAborted
        );
    }
    Ok(())
}

pub(in super::super) async fn renew_raw_processing(
    pool: &PgPool,
    work: &crate::model::raw::RawProcessing,
    node: crate::model::raw::Processor<'_>,
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    raw_processor(&mut tx, node).await?;
    let renewed = queries::renew_raw_processing(work)
        .execute(&mut *tx)
        .await?
        .rows_affected()
        == 1;
    tx.commit().await?;
    Ok(renewed)
}

pub(in super::super) async fn reserve_nonce(
    pool: &PgPool,
    key: Uuid,
    type_tag: u8,
    units: u32,
) -> Result<(i64, i64)> {
    let mut tx = pool.begin().await?;
    let range = reserve_nonce_in(&mut tx, key, type_tag, units).await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("nonce-before-commit").await;
    tx.commit().await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("nonce-after-commit").await;
    Ok(range)
}

pub(super) async fn reserve_nonce_in(
    db: &mut PgConnection,
    key: Uuid,
    type_tag: u8,
    units: u32,
) -> Result<(i64, i64)> {
    ensure!(matches!(type_tag, 0 | 1), "invalid nonce type");
    let previous = queries::nonce_counter(key, type_tag)
        .fetch_optional(&mut *db)
        .await?
        .context("active encryption key nonce counter is missing")?;
    let range = crate::model::key::nonce_reservation(previous, units)?;
    ensure!(
        queries::advance_nonce(key, type_tag, previous, range.1)
            .execute(&mut *db)
            .await?
            .rows_affected()
            == 1,
        "nonce reservation changed"
    );
    Ok(range)
}

pub(in super::super) async fn reuse_raw(
    pool: &sqlx::PgPool,
    stream: Id,
    source: Id,
    storage: Uuid,
    size: i64,
    digest: &[u8],
    hash: &[u8],
) -> Result<bool> {
    ensure!(
        size >= 0 && digest.len() == 16 && hash.len() == 32,
        "invalid received raw identity"
    );
    if queries::raw_reusable(storage, hash, size, digest)
        .fetch_optional(pool)
        .await?
        .is_none()
    {
        return Ok(false);
    }
    #[cfg(feature = "fault-injection")]
    crate::faults::point("raw-candidate-found").await;
    let mut tx = pool.begin().await?;
    queries::claim_chunk_hash(storage, hash)
        .execute(&mut *tx)
        .await?;
    ensure!(
        queries::raw_lock_stream(stream)
            .fetch_optional(&mut *tx)
            .await?
            .is_some(),
        crate::model::Error::OperationAborted
    );
    ensure!(
        queries::raw_receiving(source, stream)
            .fetch_optional(&mut *tx)
            .await?
            == Some(storage),
        crate::model::Error::OperationAborted
    );
    let candidate = queries::raw_duplicate(storage, hash, size)
        .fetch_optional(&mut *tx)
        .await?;
    let Some(candidate) = candidate.filter(|candidate| {
        candidate.validate().is_ok()
            && candidate.medium == "shared_s3"
            && candidate.digest.as_deref() == Some(digest)
    }) else {
        tx.commit().await?;
        return Ok(false);
    };
    super::super::raw_pages::inherit(&mut tx, source, &candidate).await?;
    queries::raw_attach(stream, candidate.id, size)
        .execute(&mut *tx)
        .await?;
    queries::raw_reference(candidate.id)
        .execute(&mut *tx)
        .await?;
    ensure!(
        queries::raw_discard(source, stream)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    tx.commit().await?;
    Ok(true)
}

pub(in super::super) async fn claim_raw_upload(
    pool: &sqlx::PgPool,
    token: Uuid,
    before: Option<chrono::DateTime<chrono::Utc>>,
    node: crate::model::raw::Processor<'_>,
    urgent: &[Uuid],
) -> Result<Option<crate::model::raw::RawUpload>> {
    let mut tx = pool.begin().await?;
    raw_upload_processor(&mut tx, node).await?;
    let Some(source) = queries::raw_upload_due(before, node.map(|n| n.0.node_id), urgent)
        .build_query_scalar::<Id>()
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(None);
    };
    queries::raw_upload_claim(source, token, node.map(|n| n.0.node_id))
        .execute(&mut *tx)
        .await?;
    let work = queries::raw_upload_record(source)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(Some(work))
}

pub(in super::super) async fn reuse_raw_upload(
    pool: &sqlx::PgPool,
    work: &crate::model::raw::RawUpload,
    hash: &[u8],
    streams: &[Id],
    node: crate::model::raw::Processor<'_>,
) -> Result<bool> {
    ensure!(
        hash.len() == 32 && streams.len() <= 128,
        "invalid raw upload merge"
    );
    if queries::raw_reusable(
        work.source.storage_id,
        hash,
        work.source.size,
        work.source.digest.as_deref().unwrap_or_default(),
    )
    .fetch_optional(pool)
    .await?
    .is_none()
    {
        return Ok(false);
    }
    let mut tx = pool.begin().await?;
    if node.is_some() {
        raw_upload_processor(&mut tx, node).await?;
    }
    let result =
        reuse_raw_upload_in(&mut tx, work, hash, streams, node.map(|n| n.0.node_id)).await?;
    tx.commit().await?;
    Ok(result)
}

pub(super) async fn reuse_raw_upload_in(
    tx: &mut Transaction<'_, Postgres>,
    work: &crate::model::raw::RawUpload,
    hash: &[u8],
    streams: &[Id],
    node: Option<Uuid>,
) -> Result<bool> {
    queries::claim_chunk_hash(work.source.storage_id, hash)
        .execute(&mut **tx)
        .await?;
    for stream in streams {
        queries::raw_upload_stream_lock(*stream)
            .fetch_optional(&mut **tx)
            .await?;
    }
    if node.is_some() {
        ensure!(
            queries::raw_upload_owned(work, node)
                .fetch_one(&mut **tx)
                .await?,
            crate::model::Error::OperationAborted
        );
    }
    let current = queries::raw_upload_record(work.source.id)
        .fetch_one(&mut **tx)
        .await?;
    ensure!(
        current.token == work.token && current.token.is_some() && current.source == work.source,
        crate::model::Error::OperationAborted
    );
    current.source.validate()?;
    let candidate = queries::raw_duplicate(work.source.storage_id, hash, work.source.size)
        .fetch_optional(&mut **tx)
        .await?;
    let Some(candidate) = candidate.filter(|c| {
        c.id != work.source.id
            && c.validate().is_ok()
            && !c.pending_upload
            && c.medium == "shared_s3"
            && c.digest == work.source.digest
    }) else {
        return Ok(false);
    };
    super::super::raw_pages::inherit(tx, work.source.id, &candidate).await?;
    for stream in streams {
        if queries::raw_upload_share(*stream, work.source.id, candidate.id)
            .execute(&mut **tx)
            .await?
            .rows_affected()
            > 0
        {
            queries::raw_processing_changed(*stream)
                .execute(&mut **tx)
                .await?;
        }
    }
    queries::raw_reference(candidate.id)
        .execute(&mut **tx)
        .await?;
    queries::raw_touch_unreferenced(work.source.id)
        .execute(&mut **tx)
        .await?;
    if !queries::raw_upload_references(work.source.id)
        .fetch_one(&mut **tx)
        .await?
    {
        queries::raw_upload_delete(work.source.id)
            .execute(&mut **tx)
            .await?;
    } else {
        queries::raw_upload_retry(work, None)
            .execute(&mut **tx)
            .await?;
    }
    Ok(true)
}

pub(in super::super) async fn complete_raw_upload(
    pool: &sqlx::PgPool,
    work: &crate::model::raw::RawUpload,
    hash: Option<&[u8]>,
    node: crate::model::raw::Processor<'_>,
) -> Result<()> {
    ensure!(
        hash.is_none_or(|h| h.len() == 32),
        "invalid raw upload hash"
    );
    let mut tx = pool.begin().await?;
    if node.is_some() {
        raw_upload_processor(&mut tx, node).await?;
    }
    let result = complete_raw_upload_in(&mut tx, work, hash, node.map(|n| n.0.node_id)).await?;
    tx.commit().await?;
    Ok(result)
}

pub(super) async fn complete_raw_upload_in(
    tx: &mut Transaction<'_, Postgres>,
    work: &crate::model::raw::RawUpload,
    hash: Option<&[u8]>,
    node: Option<Uuid>,
) -> Result<()> {
    if let Some(hash) = hash {
        queries::claim_chunk_hash(work.source.storage_id, hash)
            .execute(&mut **tx)
            .await?;
    }
    if node.is_some() {
        ensure!(
            queries::raw_upload_owned(work, node)
                .fetch_one(&mut **tx)
                .await?,
            crate::model::Error::OperationAborted
        );
    }
    let current = queries::raw_upload_record(work.source.id)
        .fetch_one(&mut **tx)
        .await?;
    ensure!(
        current.token == work.token && current.token.is_some() && current.source == work.source,
        crate::model::Error::OperationAborted
    );
    current.source.validate()?;
    if current.source.pending_upload {
        ensure!(
            queries::raw_uploaded(work.source.id)
                .execute(&mut **tx)
                .await?
                .rows_affected()
                == 1,
            crate::model::Error::OperationAborted
        );
    }
    let mut duplicate = false;
    if let Some(hash) = hash {
        if let Some(candidate) =
            queries::raw_duplicate(work.source.storage_id, hash, work.source.size)
                .fetch_optional(&mut **tx)
                .await?
        {
            duplicate = candidate.id != work.source.id
                && candidate.validate().is_ok()
                && !candidate.pending_upload
                && candidate.medium == "shared_s3"
                && candidate.digest == work.source.digest;
            if !duplicate && candidate.id != work.source.id {
                queries::raw_hash(candidate.id, None)
                    .execute(&mut **tx)
                    .await?;
            }
        }
        if !duplicate {
            queries::raw_hash(work.source.id, Some(hash))
                .execute(&mut **tx)
                .await?;
        }
    }
    if duplicate {
        queries::raw_upload_hash(work.source.id, hash)
            .execute(&mut **tx)
            .await?;
    } else {
        queries::raw_upload_delete(work.source.id)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

pub(in super::super) async fn abandon_raw_upload(
    pool: &sqlx::PgPool,
    work: &crate::model::raw::RawUpload,
    node: crate::model::raw::Processor<'_>,
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    if node.is_some() {
        raw_upload_processor(&mut tx, node).await?;
    }
    if node.is_some() {
        ensure!(
            queries::raw_upload_owned(work, node.map(|n| n.0.node_id))
                .fetch_one(&mut *tx)
                .await?,
            crate::model::Error::OperationAborted
        );
    }
    let current = queries::raw_upload_record(work.source.id)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(
        current.token == work.token && current.token.is_some(),
        crate::model::Error::OperationAborted
    );
    if queries::raw_upload_abandon(work.source.id)
        .execute(&mut *tx)
        .await?
        .rows_affected()
        == 0
    {
        return Ok(false);
    }
    queries::raw_upload_delete(work.source.id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(true)
}

pub(in super::super) async fn renew_raw_upload(
    pool: &PgPool,
    work: &crate::model::raw::RawUpload,
    node: crate::model::raw::Processor<'_>,
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    raw_upload_processor(&mut tx, node).await?;
    let renewed = queries::renew_raw_upload(work, node.map(|n| n.0.node_id))
        .execute(&mut *tx)
        .await?
        .rows_affected()
        == 1;
    tx.commit().await?;
    Ok(renewed)
}

pub(super) async fn raw_upload_processor(
    db: &mut PgConnection,
    node: crate::model::raw::Processor<'_>,
) -> Result<()> {
    if let Some((state, role, key)) = node {
        validate_node(&mut *db, state, role, key).await?;
        ensure!(
            queries::repair_node(state.node_id)
                .fetch_optional(&mut *db)
                .await?
                .is_some(),
            crate::model::Error::OperationAborted
        );
    }
    // Cache flush remains available while maintenance blocks ordinary writes.
    ensure!(
        queries::raw_upload_mode(node.is_some())
            .fetch_one(db)
            .await?,
        crate::model::Error::OperationAborted
    );
    Ok(())
}

pub(in super::super) async fn finish_conversion(
    pool: &sqlx::PgPool,
    task: Execution,
    job: &crate::model::raw::Conversion,
    source: &crate::model::raw::RawSource,
    candidate: Id,
    lease: Option<Uuid>,
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    let done = finish_conversion_in(&mut tx, task, job, source, candidate, lease).await?;
    tx.commit().await?;
    Ok(done)
}

pub(super) async fn finish_conversion_in(
    tx: &mut Transaction<'_, Postgres>,
    task: Execution,
    job: &crate::model::raw::Conversion,
    source: &crate::model::raw::RawSource,
    candidate: Id,
    lease: Option<Uuid>,
) -> Result<bool> {
    lock_execution(tx, task).await?;
    ensure!(
        queries::conversion_owned(candidate, task.id, task.token)
            .fetch_optional(&mut **tx)
            .await?
            .is_some(),
        crate::model::Error::OperationAborted
    );
    if queries::conversion_current(job.stream, job.payload_revision)
        .fetch_optional(&mut **tx)
        .await?
        .is_none()
    {
        queries::conversion_abandon(candidate)
            .execute(&mut **tx)
            .await?;
        queries::complete_task(task.id).execute(&mut **tx).await?;
        return Ok(true);
    }
    if let Some(lease) = lease {
        if queries::raw_processing_readers(job.stream, lease)
            .fetch_one(&mut **tx)
            .await?
        {
            return Ok(false);
        }
    }

    validate_raw(tx, job.stream, source.size).await?;
    let current = queries::raw_source(source.id).fetch_one(&mut **tx).await?;
    ensure!(
        current == *source && source.id == job.source,
        crate::model::Error::SourceUnavailable
    );
    let (end, contiguous) = queries::extent_continuity(candidate)
        .fetch_one(&mut **tx)
        .await?;
    ensure!(
        end == source.size && contiguous,
        "conversion candidate is incomplete"
    );
    if !queries::conversion_no_pending(candidate)
        .fetch_one(&mut **tx)
        .await?
    {
        return Ok(false);
    }
    require_sources(tx, Some(candidate), &[]).await?;
    queries::conversion_move(candidate, job.stream)
        .execute(&mut **tx)
        .await?;
    sqlx::query("UPDATE fragments SET owner_stream=$2 WHERE owner_stream=$1")
        .bind(candidate)
        .bind(job.stream)
        .execute(&mut **tx)
        .await?;
    queries::conversion_detach(job.stream)
        .execute(&mut **tx)
        .await?;
    queries::conversion_changed(job.stream)
        .execute(&mut **tx)
        .await?;
    queries::conversion_unpin(candidate)
        .execute(&mut **tx)
        .await?;
    queries::conversion_abandon(candidate)
        .execute(&mut **tx)
        .await?;
    queries::raw_touch_unreferenced(source.id)
        .execute(&mut **tx)
        .await?;

    queries::complete_task(task.id).execute(&mut **tx).await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("cdc-switch-before-commit").await;
    Ok(true)
}

pub(in super::super) async fn inline_conversion(
    pool: &sqlx::PgPool,
    stream: Id,
    source: Id,
) -> Result<crate::model::cache::EncodingPolicy> {
    let mut tx = pool.begin().await?;
    require_writing_stream(&mut tx, stream).await?;
    ensure!(
        queries::raw_receiving(source, stream)
            .fetch_optional(&mut *tx)
            .await?
            .is_some(),
        crate::model::Error::OperationAborted
    );
    let job = queries::conversion_job(source).fetch_one(&mut *tx).await?.0;
    ensure!(
        queries::conversion_cancel(source)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    ensure!(
        queries::raw_discard(source, stream)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    ensure!(
        queries::conversion_inline(stream)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    tx.commit().await?;
    Ok(job.encoding)
}

pub(in super::super) async fn recover_chunk_memory(pool: &sqlx::PgPool, boot: Uuid) -> Result<()> {
    let mut tx = pool.begin().await?;
    queries::release_memory_pins(boot).execute(&mut *tx).await?;
    queries::release_memory_chunks(boot)
        .execute(&mut *tx)
        .await?;
    queries::retire_memory_chunks(boot)
        .execute(&mut *tx)
        .await?;
    let lost = queries::quarantine_memory_chunks(boot)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    tx.commit().await?;
    if lost != 0 {
        tracing::error!(
            lost,
            "volatile chunk sources were lost on restart and quarantined"
        );
    }
    Ok(())
}

pub(in super::super) async fn recover_raw_memory(pool: &sqlx::PgPool, boot: Uuid) -> Result<()> {
    let mut tx = pool.begin().await?;
    queries::release_memory_raw(boot).execute(&mut *tx).await?;
    queries::release_memory_processing(boot)
        .execute(&mut *tx)
        .await?;
    let lost = queries::quarantine_memory_raw(boot)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    tx.commit().await?;
    if lost != 0 {
        tracing::error!(
            lost,
            "volatile raw sources were lost on restart and quarantined"
        );
    }
    Ok(())
}
