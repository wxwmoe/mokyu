use super::*;

pub(in super::super) async fn copy_raw_references(
    pool: &sqlx::SqlitePool,
    destination: Id,
    source: Id,
    storage: Uuid,
    revision: i64,
    rows: &[crate::model::raw::RawRange],
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let result =
        copy_raw_references_in(&mut tx, destination, source, storage, revision, rows).await?;
    tx.commit().await?;
    Ok(result)
}

pub(in super::super) async fn copy_raw_references_in(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    destination: Id,
    source: Id,
    storage: Uuid,
    revision: i64,
    rows: &[crate::model::raw::RawRange],
) -> Result<()> {
    let (offset, end, sources) = crate::model::raw::copy_window(storage, rows)?;
    writing_stream(tx, destination).await?;
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
    pool: &sqlx::SqlitePool,
    stream: Id,
    storage: Uuid,
    revision: i64,
    encoding: Option<&crate::model::cache::EncodingPolicy>,
    buffered: bool,
) -> Result<(Id, Uuid)> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
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
    let location = Uuid::now_v7();
    let inserted = queries::raw_insert(
        location,
        stream,
        storage,
        revision,
        if storage_type == "local" {
            "local"
        } else {
            "shared_s3"
        },
        buffered,
    )
    .execute(&mut *tx)
    .await?;
    let id = Id::from(inserted.last_insert_rowid());
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
        let (bucket, object_key, payload_revision) = queries::conversion_stream(stream)
            .fetch_one(&mut *tx)
            .await?;
        let job = crate::model::raw::Conversion {
            stream,
            source: id.into(),
            bucket,
            object_key,
            payload_revision,
            encoding: encoding.clone(),
            target_key_id: encoding.block_key_id,
        };
        let mut detail = serde_json::to_value(job)?;
        detail["operation"] = serde_json::json!("cdc");
        let task = Uuid::new_v4();
        queries::insert_task(task, "upload", Some(bucket), detail, None)
            .execute(&mut *tx)
            .await?;
        queries::conversion_link(id.into(), task)
            .execute(&mut *tx)
            .await?;
    } else if background {
        queries::raw_enqueue_processing(stream, id)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok((id, location))
}

pub(in super::super) async fn finish_raw(
    pool: &sqlx::SqlitePool,
    stream: Id,
    source: Id,
    size: i64,
    digest: &[u8],
    cached: Option<(i64, bool, bool, Option<Uuid>)>,
) -> Result<()> {
    ensure!(
        size >= 0 && digest.len() == 16,
        "invalid received raw source"
    );
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    if cached.is_some() {
        ensure!(
            queries::repair_mode().fetch_one(&mut *tx).await?,
            crate::model::Error::OperationAborted
        );
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
        queries::raw_queue(source, deduplicate, background)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(())
}

pub(super) async fn validate_raw(
    db: &mut sqlx::SqliteConnection,
    stream: Id,
    size: i64,
) -> Result<()> {
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
    pool: &sqlx::SqlitePool,
    stream: Id,
    source: Id,
    hash: &[u8],
) -> Result<bool> {
    ensure!(hash.len() == 32, "invalid raw content hash");
    let received = queries::raw_source(source).fetch_one(pool).await?;
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
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

pub(in super::super) async fn share_raw(
    db: &mut sqlx::SqliteConnection,
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

pub(in super::super) async fn multipart_file(pool: &SqlitePool, upload: Uuid) -> Result<Uuid> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    ensure!(
        queries::lock_local_upload(upload)
            .fetch_optional(&mut *tx)
            .await?
            .as_deref()
            == Some("active"),
        crate::model::Error::OperationAborted
    );
    let id = if let Some(id) = queries::current_multipart_file(upload)
        .fetch_optional(&mut *tx)
        .await?
    {
        id
    } else {
        let id = Uuid::now_v7();
        ensure!(
            queries::insert_multipart_file(id, upload)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            crate::model::Error::OperationAborted
        );
        id
    };
    tx.commit().await?;
    Ok(id)
}

pub(in super::super) async fn remap_multipart_file(
    pool: &SqlitePool,
    upload: Uuid,
    source: Id,
    ranges: &[crate::model::multipart::FileRemap],
) -> Result<()> {
    ensure!(
        !ranges.is_empty() && ranges.len() <= 64,
        "invalid multipart checkpoint batch"
    );
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
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
    pool: &sqlx::SqlitePool,
    single: bool,
    token: Uuid,
) -> Result<Option<crate::model::raw::RawProcessing>> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
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
    pool: &sqlx::SqlitePool,
    work: &crate::model::raw::RawProcessing,
    source: &crate::model::raw::RawSource,
    hash: &[u8],
    lease: Option<Uuid>,
) -> Result<bool> {
    ensure!(hash.len() == 32, "invalid raw content hash");
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;

    let current_stream = queries::raw_processing_stream(work)
        .fetch_optional(&mut *tx)
        .await?;
    ensure!(
        queries::raw_processing_owned(work)
            .fetch_optional(&mut *tx)
            .await?
            == Some(source.id),
        crate::model::Error::OperationAborted
    );
    if current_stream.is_none() {
        queries::raw_processing_delete(work)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        return Ok(true);
    }
    let _ = lease;
    let current = queries::raw_source(source.id).fetch_one(&mut *tx).await?;
    ensure!(current == *source, crate::model::Error::SourceUnavailable);
    current.validate()?;
    validate_raw(&mut tx, work.stream, current.size).await?;
    if share_raw(&mut tx, work.stream, &current, hash).await? {
        queries::raw_processing_changed(work.stream)
            .execute(&mut *tx)
            .await?;
    }
    queries::raw_processing_delete(work)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(true)
}

pub(in super::super) async fn reserve_nonce(
    pool: &SqlitePool,
    key: Uuid,
    type_tag: u8,
    units: u32,
    force_full: bool,
) -> Result<(i64, i64)> {
    ensure!(matches!(type_tag, 0 | 1), "invalid nonce type");
    let mut connection = pool.acquire().await?;
    if force_full {
        // Discard the temporary override even when reservation is cancelled.
        connection.close_on_drop();
        queries::full_synchronous()
            .execute(&mut *connection)
            .await?;
    }
    let mut tx = connection.begin_with(queries::BEGIN_WRITE).await?;
    ensure!(
        !queries::nonce_sync_off().fetch_one(&mut *tx).await?,
        s3s::s3_error!(
            InvalidRequest,
            "encryption requires SQLite FULL or NORMAL synchronization; change the saved OFF setting"
        )
    );
    #[cfg(any(test, feature = "fault-injection"))]
    ensure!(
        queries::synchronous().fetch_one(&mut *tx).await? == 2,
        "nonce reservations require FULL synchronization"
    );
    let previous = queries::nonce_counter(key, type_tag)
        .fetch_optional(&mut *tx)
        .await?
        .context("active encryption key nonce counter is missing")?;
    let range = crate::model::key::nonce_reservation(previous, units)?;
    ensure!(
        queries::advance_nonce(key, type_tag, previous, range.1)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        "nonce reservation changed"
    );
    #[cfg(feature = "fault-injection")]
    crate::faults::point("nonce-before-commit").await;
    tx.commit().await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("nonce-after-commit").await;
    Ok(range)
}

pub(in super::super) async fn key_replacement_preview(
    pool: &SqlitePool,
    principal: &Principal,
    source: Uuid,
    input: KeyReplacementInput,
) -> Result<KeyReplacementPreview> {
    key_replacement::validate(&input, source)?;
    let actor = key_replacement::actor(principal)?;
    let mut tx = administration_transaction(pool, principal).await?;

    let storages = queries::replacement_storages(source)
        .fetch_all(&mut *tx)
        .await?;
    ensure!(
        !storages.is_empty(),
        s3s::s3_error!(InvalidRequest, "key has no associated storages")
    );
    ensure!(
        serde_json::to_vec(&storages)?.len() <= 131072,
        s3s::s3_error!(InvalidRequest, "associated storage list is too large")
    );
    let (source_name, mut algorithm) = queries::replacement_key(source)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    let id = Uuid::new_v4();
    let target = input.target.unwrap_or_else(Uuid::new_v4);
    let target_name = if input.target.is_some() {
        let (name, selected_algorithm) = queries::replacement_key(target)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| s3s::s3_error!(InvalidArgument, "target must be an active block key"))?;
        ensure!(
            queries::replacement_capacity(target)
                .fetch_one(&mut *tx)
                .await?,
            s3s::s3_error!(InvalidRequest, "target key cannot reserve more nonce IDs")
        );
        algorithm = selected_algorithm;
        name
    } else {
        input
            .name
            .unwrap_or_else(|| format!("key-{}", target.simple()))
            .trim()
            .to_owned()
    };
    for storage in &storages {
        queries::storage_profile_lock(storage.id)
            .fetch_one(&mut *tx)
            .await?
            .validate_key(Some(target))?;
    }
    let preview = KeyReplacementPreview {
        id,
        source,
        source_name,
        target,
        target_name,
        algorithm,
        create: input.target.is_none(),
        expires_at: Utc::now() + chrono::Duration::minutes(10),
        storages,
    };
    queries::insert_key_replacement(&actor, &preview)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(preview)
}

pub(in super::super) async fn key_replacement(
    pool: &SqlitePool,
    principal: &Principal,
    id: Uuid,
) -> Result<ReplacementRow> {
    let actor = key_replacement::actor(principal)?;
    let mut tx = administration_transaction(pool, principal).await?;
    let row = queries::key_replacement(id, &actor)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    tx.commit().await?;
    Ok(row)
}

pub(in super::super) async fn replace_keys(
    pool: &SqlitePool,
    principal: &Principal,
    id: Uuid,
    input: Option<ManagedKeyCreate>,
) -> Result<KeyReplacementResult> {
    let actor = key_replacement::actor(principal)?;
    let mut tx = administration_transaction(pool, principal).await?;

    let (preview, result) = queries::key_replacement(id, &actor)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    if let Some(result) = result {
        tx.commit().await?;
        return Ok(result.0);
    }
    let preview = preview.0;
    ensure!(
        preview.expires_at > Utc::now(),
        crate::model::Error::OperationAborted
    );
    let storages = queries::replacement_storages(preview.source)
        .fetch_all(&mut *tx)
        .await?;
    ensure!(
        storages == preview.storages,
        crate::model::Error::StorageProfileConflict
    );
    for storage in &storages {
        let profile = queries::storage_profile_lock(storage.id)
            .fetch_one(&mut *tx)
            .await?;
        ensure!(
            profile.revision == storage.revision,
            crate::model::Error::StorageProfileConflict
        );
        profile.validate_key(Some(preview.target))?;
    }
    ensure!(
        queries::replacement_key(preview.source)
            .fetch_optional(&mut *tx)
            .await?
            .is_some(),
        crate::model::Error::OperationAborted
    );
    if preview.create {
        let input = input.context("new key material is required")?;
        ensure!(
            input.id == preview.target
                && input.name == preview.target_name
                && input.algorithm == preview.algorithm
                && input.purpose == "block",
            s3s::s3_error!(InvalidArgument)
        );
        queries::insert_managed_key(&input)
            .execute(&mut *tx)
            .await?;
        queries::initialize_nonce_counters(input.id)
            .execute(&mut *tx)
            .await?;
    } else {
        let target = queries::replacement_key(preview.target)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(crate::model::Error::OperationAborted)?;
        ensure!(
            target == (preview.target_name.clone(), preview.algorithm.clone())
                && queries::replacement_capacity(preview.target)
                    .fetch_one(&mut *tx)
                    .await?,
            crate::model::Error::OperationAborted
        );
    }
    for storage in &storages {
        queries::update_storage_key(storage.id, Some(preview.target))
            .execute(&mut *tx)
            .await?;
        queries::bump_storage_profile(storage.id)
            .execute(&mut *tx)
            .await?;
    }
    let result = KeyReplacementResult {
        preview,
        committed_at: Utc::now(),
    };
    queries::complete_key_replacement(&result)
        .execute(&mut *tx)
        .await?;
    checkpoint_event(&mut tx, "managed_key.replace", &id.to_string(), serde_json::json!({"source":result.preview.source,"target":result.preview.target,"created":result.preview.create,"storages":result.preview.storages})).await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("key-replacement-before-commit").await;
    tx.commit().await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("key-replacement-after-commit").await;
    Ok(result)
}

pub(in super::super) async fn reuse_raw(
    pool: &sqlx::SqlitePool,
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
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;

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
    pool: &sqlx::SqlitePool,
    token: Uuid,
    before: Option<chrono::DateTime<chrono::Utc>>,
    urgent: &[Uuid],
) -> Result<Option<crate::model::raw::RawUpload>> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    ensure!(
        queries::raw_upload_mode().fetch_one(&mut *tx).await?,
        crate::model::Error::OperationAborted
    );
    let Some(source) = queries::raw_upload_due(before, urgent)
        .build_query_scalar::<Id>()
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(None);
    };
    queries::raw_upload_claim(source, token)
        .execute(&mut *tx)
        .await?;
    let work = queries::raw_upload_record(source)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(Some(work))
}

pub(in super::super) async fn reuse_raw_upload(
    pool: &sqlx::SqlitePool,
    work: &crate::model::raw::RawUpload,
    hash: &[u8],
    streams: &[Id],
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
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;

    let result = reuse_raw_upload_in(&mut tx, work, hash, streams).await?;
    tx.commit().await?;
    Ok(result)
}
pub(in super::super) async fn reuse_raw_upload_in(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    work: &crate::model::raw::RawUpload,
    hash: &[u8],
    streams: &[Id],
) -> Result<bool> {
    ensure!(
        hash.len() == 32 && streams.len() <= 128,
        crate::model::Error::NodeOperationConflict
    );
    for stream in streams {
        queries::raw_upload_stream_lock(*stream)
            .fetch_optional(&mut **tx)
            .await?;
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
    pool: &sqlx::SqlitePool,
    work: &crate::model::raw::RawUpload,
    hash: Option<&[u8]>,
) -> Result<()> {
    ensure!(
        hash.is_none_or(|h| h.len() == 32),
        "invalid raw upload hash"
    );
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;

    complete_raw_upload_in(&mut tx, work, hash).await?;
    tx.commit().await?;
    Ok(())
}
pub(in super::super) async fn complete_raw_upload_in(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    work: &crate::model::raw::RawUpload,
    hash: Option<&[u8]>,
) -> Result<()> {
    ensure!(
        hash.is_none_or(|h| h.len() == 32),
        crate::model::Error::NodeOperationConflict
    );
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
    pool: &sqlx::SqlitePool,
    work: &crate::model::raw::RawUpload,
) -> Result<bool> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
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

pub(in super::super) async fn commit_conversion_batch(
    pool: &sqlx::SqlitePool,
    task: Execution,
    candidate: Id,
    rows: &[crate::model::Mapping],
    outputs: &[crate::model::packing::PackOutput],
) -> Result<()> {
    ensure!(
        !rows.is_empty() && rows.len() <= 1024,
        "invalid conversion batch"
    );
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    lock_execution(&mut tx, task).await?;
    writing_stream(&mut tx, candidate).await?;
    ensure!(
        queries::conversion_owned(candidate, task.id, task.token)
            .fetch_optional(&mut *tx)
            .await?
            .is_some(),
        crate::model::Error::OperationAborted
    );
    let expected: std::collections::BTreeSet<_> = rows.iter().map(|r| r.chunk.id).collect();
    let produced: Vec<_> = outputs
        .iter()
        .flat_map(|o| o.chunks.iter().copied())
        .collect();
    ensure!(
        expected.len() == rows.len()
            && produced.len() == rows.len()
            && produced
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                == expected,
        "conversion outputs do not match inputs"
    );
    for row in rows {
        ensure!(
            queries::conversion_chunk(row.chunk.id, candidate)
                .fetch_optional(&mut *tx)
                .await?
                .is_some(),
            crate::model::Error::OperationAborted
        );
        queries::conversion_ready(row.chunk.id, candidate)
            .execute(&mut *tx)
            .await?;
        queries::conversion_retire_encoding(row.chunk.encoding_id)
            .execute(&mut *tx)
            .await?;
    }
    for output in outputs {
        if let Some(pack) = &output.pack {
            ensure!(
                output.independent.is_none()
                    && queries::conversion_output_pack(pack.id, task.id)
                        .fetch_optional(&mut *tx)
                        .await?
                        .is_some(),
                crate::model::Error::OperationAborted
            );
            queries::publish_prepared_pack(pack.id, task.id)
                .execute(&mut *tx)
                .await?;
        } else if let Some(chunk) = &output.independent {
            ensure!(
                output.chunks == [chunk.id]
                    && queries::conversion_output_encoding(chunk.encoding_id, task.id, chunk.id)
                        .fetch_optional(&mut *tx)
                        .await?
                        .is_some(),
                crate::model::Error::OperationAborted
            );
            queries::conversion_publish_encoding(chunk.encoding_id)
                .execute(&mut *tx)
                .await?;
            queries::publish_chunk_encoding(chunk.id, chunk.encoding_id)
                .execute(&mut *tx)
                .await?;
        } else {
            anyhow::bail!("conversion output has no physical source");
        }
        for chunk in &output.chunks {
            queries::conversion_layout(*chunk, output.pack.as_ref().map(|p| p.id))
                .execute(&mut *tx)
                .await?;
        }
    }
    for row in rows {
        queries::insert_chunk_extent(
            candidate,
            row.offset_bytes,
            row.chunk.raw_size,
            row.chunk.id,
        )
        .execute(&mut *tx)
        .await?;
        queries::changed_references(&mut tx, &[row.chunk.id], true).await?;
    }
    require_sources(&mut tx, Some(candidate), &[]).await?;
    let end = rows.last().unwrap().offset_bytes + i64::from(rows.last().unwrap().chunk.raw_size);
    queries::resize_writing_stream(candidate, end)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn finish_conversion(
    pool: &sqlx::SqlitePool,
    task: Execution,
    job: &crate::model::raw::Conversion,
    source: &crate::model::raw::RawSource,
    candidate: Id,
    lease: Option<Uuid>,
) -> Result<bool> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    lock_execution(&mut tx, task).await?;
    ensure!(
        queries::conversion_owned(candidate, task.id, task.token)
            .fetch_optional(&mut *tx)
            .await?
            .is_some(),
        crate::model::Error::OperationAborted
    );
    if queries::conversion_current(job.stream, job.payload_revision)
        .fetch_optional(&mut *tx)
        .await?
        .is_none()
    {
        queries::conversion_abandon(candidate)
            .execute(&mut *tx)
            .await?;
        queries::complete_task(task.id).execute(&mut *tx).await?;
        tx.commit().await?;
        return Ok(true);
    }
    let _ = lease;

    validate_raw(&mut tx, job.stream, source.size).await?;
    let current = queries::raw_source(source.id).fetch_one(&mut *tx).await?;
    ensure!(
        current == *source && source.id == job.source,
        crate::model::Error::SourceUnavailable
    );
    let (end, contiguous) = queries::extent_continuity(candidate)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(
        end == source.size && contiguous,
        "conversion candidate is incomplete"
    );
    if !queries::conversion_no_pending(candidate)
        .fetch_one(&mut *tx)
        .await?
    {
        return Ok(false);
    }
    require_sources(&mut tx, Some(candidate), &[]).await?;
    queries::conversion_move(candidate, job.stream)
        .execute(&mut *tx)
        .await?;
    queries::conversion_detach(job.stream)
        .execute(&mut *tx)
        .await?;
    queries::conversion_changed(job.stream)
        .execute(&mut *tx)
        .await?;
    queries::conversion_unpin(candidate)
        .execute(&mut *tx)
        .await?;
    queries::conversion_abandon(candidate)
        .execute(&mut *tx)
        .await?;
    queries::raw_touch_unreferenced(source.id)
        .execute(&mut *tx)
        .await?;
    queries::ready_stream_queues(&mut tx, job.stream).await?;
    queries::complete_task(task.id).execute(&mut *tx).await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("cdc-switch-before-commit").await;
    tx.commit().await?;
    Ok(true)
}

pub(in super::super) async fn inline_conversion(
    pool: &sqlx::SqlitePool,
    stream: Id,
    source: Id,
) -> Result<crate::model::cache::EncodingPolicy> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    writing_stream(&mut tx, stream).await?;
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

pub(in super::super) async fn recover_chunk_memory(
    pool: &sqlx::SqlitePool,
    boot: Uuid,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
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

pub(in super::super) async fn recover_raw_memory(
    pool: &sqlx::SqlitePool,
    boot: Uuid,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
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

pub(in super::super) async fn publish_import_in(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task: Execution,
    id: Id,
    bucket: Uuid,
    key: &str,
    expected: Option<Id>,
    public: bool,
) -> Result<()> {
    lock_execution(tx, task).await?;
    let authority = Permit::for_action(Principal::System, bucket, Action::Write);
    lock_media_scope(tx, &authority).await?;
    super::super::transactions::redundancy::writable(tx, bucket, key, Some(task.id)).await?;
    let old = selected_object(tx, bucket, key, expected).await?;
    let crate::model::PublishingStream {
        stream,
        publication_id,
    } = validate_copy(tx, id, bucket, key, public).await?;
    require_sources(tx, Some(id), &[]).await?;
    if let Some(publication) = publication_id {
        let details = serde_json::json!({"operation":"task","task":task.id,"method":"import","expected":expected,"public":public});
        super::super::replica_transactions::publish(tx, publication, bucket, key, id, &details)
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
    replace_object_reference(tx, &stream, old).await?;
    Ok(())
}
