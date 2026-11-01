use super::*;

pub(in super::super) async fn read_source_snapshot(
    pool: &sqlx::MySqlPool,
    scope: &crate::model::source::SourceScope,
) -> Result<crate::model::source::SourceSnapshot> {
    let mut tx = pool.begin().await?;
    let snapshot = queries::source_snapshot(&mut tx, scope, false).await?;
    tx.commit().await?;
    Ok(snapshot)
}

pub(in super::super) async fn source_update(
    pool: &sqlx::MySqlPool,
    update: crate::model::source::SourceUpdate<'_>,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    let operation = update.operation;
    ensure!(
        queries::validate_identity(&operation.principal, &mut tx, true).await?,
        s3s::s3_error!(AccessDenied)
    );
    ensure!(
        queries::source_mode(&operation.mode)
            .fetch_one(&mut *tx)
            .await?,
        crate::model::Error::OperationAborted
    );
    lock_execution(&mut tx, update.execution).await?;
    let snapshot = queries::source_snapshot(&mut tx, &operation.snapshot.scope, true).await?;
    ensure!(
        snapshot.state == "ready" && snapshot.fingerprint()? == operation.snapshot.fingerprint()?,
        crate::model::Error::OperationAborted
    );
    ensure!(
        snapshot.quarantine_code.is_none() == operation.code.is_some(),
        crate::model::Error::OperationAborted
    );
    ensure!(
        operation
            .code
            .as_deref()
            .is_none_or(crate::model::source::quarantine_code),
        s3s::s3_error!(InvalidArgument)
    );
    if update.apply {
        ensure!(
            queries::change_source(&snapshot, operation.code.as_deref())?
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            crate::model::Error::OperationAborted
        );
        let action = if operation.code.is_some() {
            "source.quarantine"
        } else {
            "source.recheck"
        };
        let detail = serde_json::json!({"source":snapshot.summary()?,"revision":(snapshot.revision+1).to_string(),"code":operation.code,"requested_by":operation.principal.user_id(),"token_id":operation.principal.token_id(),"task_id":update.execution.id});
        queries::source_result(update.execution.id, serde_json::json!({"result":detail}))
            .execute(&mut *tx)
            .await?;
        queries::complete_task(update.execution.id)
            .execute(&mut *tx)
            .await?;
        queries::source_audit(
            &operation.principal,
            action,
            &snapshot.location().to_string(),
            detail,
        )?
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

async fn require_inputs(db: &mut sqlx::MySqlConnection, chunks: &[Id]) -> Result<()> {
    require_sources(db, None, chunks).await?;
    ensure!(
        !queries::quarantined_inputs(chunks)
            .build_query_scalar::<bool>()
            .fetch_one(db)
            .await?,
        crate::model::Error::SourceUnavailable
    );
    Ok(())
}

pub(super) async fn lock_splice_sources(
    db: &mut sqlx::MySqlConnection,
    streams: &[Id],
    chunk: Id,
) -> Result<()> {
    use futures_util::TryStreamExt;
    for physical in [false, true] {
        let mut query = queries::splice_locks(streams, chunk, physical);
        let mut rows = query.build().fetch(&mut *db);
        while rows.try_next().await?.is_some() {}
    }
    require_sources(db, None, &[chunk]).await
}

async fn source_input_stamp(db: &mut sqlx::MySqlConnection, chunks: &[Id]) -> Result<Value> {
    let rows: Vec<(Id, Option<Id>, Option<i64>, Option<Id>, Option<i64>)> =
        queries::source_input_versions(chunks)
            .build_query_as()
            .fetch_all(db)
            .await?;
    Ok(Value::Object(
        rows.into_iter()
            .map(|(id, encoding, revision, pack, pack_revision)| {
                (
                    id.to_string(),
                    json!([encoding, revision, pack, pack_revision]),
                )
            })
            .collect(),
    ))
}

pub(super) async fn record_source_inputs(
    db: &mut sqlx::MySqlConnection,
    task: Uuid,
    chunks: &[Id],
) -> Result<()> {
    require_inputs(db, chunks).await?;
    let stamp = source_input_stamp(db, chunks).await?;
    queries::save_source_inputs(task, &stamp)
        .execute(db)
        .await?;
    Ok(())
}

pub(super) async fn check_source_inputs(
    db: &mut sqlx::MySqlConnection,
    task: Uuid,
    chunks: &[Id],
) -> Result<()> {
    require_inputs(db, chunks).await?;
    let stamp = source_input_stamp(db, chunks).await?;
    let saved = queries::saved_source_inputs(task).fetch_one(db).await?;
    ensure!(
        stamp
            .as_object()
            .unwrap()
            .iter()
            .all(|(id, value)| saved.get(id) == Some(value)),
        crate::model::Error::OperationAborted
    );
    Ok(())
}

async fn repair_writer(
    db: &mut sqlx::MySqlConnection,
    stream: Id,
    node: Option<&crate::model::recovery::RepairNode>,
) -> Result<()> {
    let authority = queries::write_authorization(stream)
        .fetch_one(&mut *db)
        .await?
        .0;
    lock_permit(&mut *db, &authority).await?;
    ensure!(
        node.is_none() && queries::repair_mode().fetch_one(&mut *db).await?,
        crate::model::Error::OperationAborted
    );
    let observed = queries::repair_owner(stream)
        .fetch_optional(&mut *db)
        .await?
        .ok_or(crate::model::Error::OperationAborted)?;
    let (bucket, stream_type, owner, token) = &observed;
    if let Some(owner) = *owner {
        let token = token.ok_or(crate::model::Error::OperationAborted)?;
        ensure!(
            queries::lock_owned_task(owner, token)
                .fetch_optional(&mut *db)
                .await?
                .is_some(),
            crate::model::Error::OperationAborted
        );
    }
    ensure!(
        queries::lock_bucket_uploads(*bucket)
            .fetch_one(&mut *db)
            .await?
            .0
            == "active",
        crate::model::Error::OperationAborted
    );
    if owner.is_none() && stream_type == "part" {
        // Upload ownership precedes stream locks, as in multipart publication.
        ensure!(
            queries::repair_upload(stream)
                .fetch_optional(&mut *db)
                .await?
                .is_some(),
            crate::model::Error::OperationAborted
        );
    }
    if owner.is_none() && stream_type == "part" {
        ensure!(
            queries::repair_part(stream)
                .fetch_optional(&mut *db)
                .await?
                == Some(stream),
            crate::model::Error::OperationAborted
        );
    }
    ensure!(
        queries::repair_stream(stream)
            .fetch_optional(&mut *db)
            .await?
            == Some(observed.clone()),
        crate::model::Error::OperationAborted
    );
    if owner.is_none() && stream_type == "object" {
        ensure!(
            queries::repair_object(stream)
                .fetch_optional(&mut *db)
                .await?
                == Some(stream),
            crate::model::Error::OperationAborted
        );
    }
    Ok(())
}

async fn repair_sources(
    db: &mut sqlx::MySqlConnection,
    chunk: &Chunk,
) -> Result<Vec<crate::model::source::SourceSnapshot>> {
    use crate::model::source::{SourceScope, SourceType};
    ensure!(
        !queries::repair_pending(chunk.id)
            .fetch_one(&mut *db)
            .await?,
        crate::model::Error::SourceUnavailable
    );
    let rows: Vec<(Id, Option<Id>, Option<i64>, Option<Id>, Option<i64>)> =
        queries::source_input_versions(&[chunk.id])
            .build_query_as()
            .fetch_all(&mut *db)
            .await?;
    ensure!(rows.len() == 1, crate::model::Error::OperationAborted);
    let mut sources = Vec::with_capacity(2);
    for (source, id) in [
        (SourceType::Chunk, rows[0].1),
        (SourceType::Pack, rows[0].3),
    ] {
        if let Some(id) = id {
            let scope = SourceScope {
                storage_id: chunk
                    .storage_id
                    .ok_or(crate::model::Error::OperationAborted)?,
                source,
                id: id.to_string(),
            };
            let snapshot = queries::source_snapshot(db, &scope, true).await?;
            ensure!(
                // Only lost RAM placeholders are included before encoding is ready.
                snapshot.repairable(),
                crate::model::Error::SourceUnavailable
            );
            sources.push(snapshot);
        }
    }
    ensure!(!sources.is_empty(), crate::model::Error::SourceUnavailable);
    Ok(sources)
}

pub(in super::super) async fn prepare_repair(
    pool: &sqlx::MySqlPool,
    input: ChunkCreate,
    node: Option<crate::model::recovery::RepairNode>,
) -> Result<crate::model::recovery::RepairAllocation> {
    use crate::model::recovery::{Repair, RepairAllocation};
    let mut tx = pool.begin().await?;
    repair_writer(&mut tx, input.stream, node.as_ref()).await?;
    let mut chunk = queries::reusable_chunk(&mut tx, &input)
        .await?
        .ok_or(crate::model::Error::OperationAborted)?;
    match require_sources(&mut tx, None, &[chunk.id]).await {
        Ok(()) => {
            tx.commit().await?;
            return Ok(RepairAllocation::Existing(chunk));
        }
        Err(error)
            if matches!(
                crate::model::error(&error),
                Some(crate::model::Error::SourceUnavailable)
            ) => {}
        Err(error) => return Err(error),
    }
    let sources = repair_sources(&mut tx, &chunk).await?;
    let revision = queries::repair_target(input.storage, input.key, &input.algorithm)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(crate::model::Error::OperationAborted)?;
    ensure!(
        sources
            .iter()
            .all(|source| source.storage_revision == revision),
        crate::model::Error::OperationAborted
    );
    chunk.address = None;
    chunk.location_id = Uuid::now_v7();
    chunk.algorithm = input.algorithm;
    chunk.block_key_id = input.key;
    chunk.stored_size = None;
    chunk.compressed = false;
    chunk.nonce = None;
    chunk.encoding_id = Id::try_from(
        queries::insert_repair(&chunk, input.stream)
            .execute(&mut *tx)
            .await?
            .last_insert_id(),
    )?;
    let repair = Repair {
        stream: input.stream,
        node,
        chunk,
        sources,
    };
    repair.validate()?;
    tx.commit().await?;
    Ok(RepairAllocation::Candidate(Box::new(repair)))
}

pub(in super::super) async fn repair_metadata(
    pool: &sqlx::MySqlPool,
    repair: &crate::model::recovery::Repair,
) -> Result<()> {
    repair.validate()?;
    let mut tx = pool.begin().await?;
    repair_writer(&mut tx, repair.stream, repair.node.as_ref()).await?;
    let candidate = queries::repair_candidate(repair.chunk.encoding_id, repair.stream)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(crate::model::Error::OperationAborted)?;
    repair.check_candidate(&candidate, false)?;
    ensure!(
        queries::save_repair_metadata(&repair.chunk, repair.stream)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    queries::refresh_encoding_fingerprint(&mut tx, repair.chunk.encoding_id).await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn finish_repair(
    pool: &sqlx::MySqlPool,
    repair: &crate::model::recovery::Repair,
    offset: i64,
) -> Result<Chunk> {
    repair.validate()?;
    let mut tx = pool.begin().await?;
    repair_writer(&mut tx, repair.stream, repair.node.as_ref()).await?;
    let input = ChunkCreate {
        stream: repair.stream,
        storage: repair
            .chunk
            .storage_id
            .ok_or(crate::model::Error::OperationAborted)?,
        hash: repair.chunk.hash.as_slice().try_into()?,
        raw_size: repair.chunk.raw_size,
        algorithm: repair.chunk.algorithm.clone(),
        key: repair.chunk.block_key_id,
    };
    let current = queries::reusable_chunk(&mut tx, &input)
        .await?
        .ok_or(crate::model::Error::OperationAborted)?;
    ensure!(
        current.id == repair.chunk.id,
        crate::model::Error::OperationAborted
    );
    let healthy = match require_sources(&mut tx, None, &[current.id]).await {
        Ok(()) => true,
        Err(error)
            if matches!(
                crate::model::error(&error),
                Some(crate::model::Error::SourceUnavailable)
            ) =>
        {
            false
        }
        Err(error) => return Err(error),
    };
    let sources = if healthy {
        Vec::new()
    } else {
        repair_sources(&mut tx, &current).await?
    };
    let candidate = queries::repair_candidate(repair.chunk.encoding_id, repair.stream)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(crate::model::Error::OperationAborted)?;
    repair.check_candidate(&candidate, true)?;
    let result = if healthy {
        queries::discard_verified_repair(candidate.encoding_id, repair.stream)
            .execute(&mut *tx)
            .await?;
        current
    } else {
        ensure!(
            sources.len() == repair.sources.len(),
            crate::model::Error::OperationAborted
        );
        for (current, expected) in sources.iter().zip(&repair.sources) {
            ensure!(
                current.fingerprint()? == expected.fingerprint()?,
                crate::model::Error::OperationAborted
            );
        }
        let revision = queries::repair_target(input.storage, input.key, &input.algorithm)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(crate::model::Error::OperationAborted)?;
        ensure!(
            sources
                .iter()
                .all(|source| source.storage_revision == revision),
            crate::model::Error::OperationAborted
        );
        queries::retire_repair_sources(current.id)
            .execute(&mut *tx)
            .await?;
        ensure!(
            queries::publish_repair(candidate.encoding_id, repair.stream)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            crate::model::Error::OperationAborted
        );
        ensure!(
            queries::publish_repaired_chunk(&candidate)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            crate::model::Error::OperationAborted
        );
        let authority = queries::write_authorization(repair.stream)
            .fetch_one(&mut *tx)
            .await?
            .0;
        queries::source_audit(&authority.principal, "source.recover", &current.id.to_string(), serde_json::json!({"stream_id":repair.stream,"storage_id":input.storage,"chunk_id":current.id.to_string(),"encoding_id":candidate.encoding_id.to_string(),"sources":sources.iter().map(|source| source.summary()).collect::<Result<Vec<_>>>()?}))?.execute(&mut *tx).await?;
        candidate
    };
    queries::insert_chunk_extent(repair.stream, offset, result.raw_size, result.id)
        .execute(&mut *tx)
        .await?;
    queries::changed_references(&mut tx, &[result.id], true).await?;
    queries::pin_chunk(result.id, repair.stream)
        .execute(&mut *tx)
        .await?;
    queries::attach_pending_upload(result.id, repair.stream, offset)
        .execute(&mut *tx)
        .await?;
    queries::resize_writing_stream(repair.stream, offset + i64::from(result.raw_size))
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(result)
}
