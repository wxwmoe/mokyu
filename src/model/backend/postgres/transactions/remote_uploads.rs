use super::*;

pub(in super::super) async fn renew_completion(
    pool: &PgPool,
    upload: Uuid,
    token: Uuid,
    node: crate::model::raw::Processor<'_>,
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    raw_upload_processor(&mut tx, node).await?;
    let changed = queries::renew_completion(upload, token)
        .execute(&mut *tx)
        .await?
        .rows_affected()
        == 1;
    tx.commit().await?;
    Ok(changed)
}

pub(in super::super) async fn begin_remote(
    backend: &super::super::Backend,
    upload: Uuid,
    storage: Uuid,
    revision: i64,
    node: crate::model::raw::Processor<'_>,
) -> Result<RemoteUpload> {
    let mut tx = backend.pool.begin().await?;
    backend.fence_control(&mut tx).await?;
    ensure!(node.is_some(), crate::model::Error::OperationAborted);
    raw_upload_processor(&mut tx, node).await?;
    let (state, source) = queries::remote_upload_lock(upload)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(state == "active", crate::model::Error::OperationAborted);
    if source.is_none() {
        ensure!(
            queries::insert_remote(upload, storage, revision)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            crate::model::Error::OperationAborted
        );
    }
    let value = queries::remote_upload(upload).fetch_one(&mut *tx).await?;
    tx.commit().await?;
    Ok(value)
}

pub(in super::super) async fn claim_remote(
    backend: &super::super::Backend,
    upload: Uuid,
    token: Uuid,
    node: crate::model::raw::Processor<'_>,
) -> Result<bool> {
    let mut tx = backend.pool.begin().await?;
    backend.fence_control(&mut tx).await?;
    raw_upload_processor(&mut tx, node).await?;
    let claimed = queries::claim_remote(upload, token)
        .execute(&mut *tx)
        .await?
        .rows_affected()
        == 1;
    tx.commit().await?;
    Ok(claimed)
}

pub(in super::super) async fn initialized_remote(
    backend: &super::super::Backend,
    upload: Uuid,
    token: Uuid,
    upstream: Option<&str>,
    node: crate::model::raw::Processor<'_>,
) -> Result<bool> {
    ensure!(
        upstream.is_none_or(|id| !id.is_empty() && id.len() <= 4096),
        "invalid remote upload identity"
    );
    let mut tx = backend.pool.begin().await?;
    backend.fence_control(&mut tx).await?;
    raw_upload_processor(&mut tx, node).await?;
    let changed = queries::remote_initialized(upload, token, upstream)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(changed)
}

pub(in super::super) async fn claim_remote_part(
    backend: &super::super::Backend,
    token: Uuid,
    node: crate::model::raw::Processor<'_>,
    stream: Option<Id>,
) -> Result<Option<RemotePart>> {
    let mut tx = backend.pool.begin().await?;
    backend.fence_control(&mut tx).await?;
    raw_upload_processor(&mut tx, node).await?;
    let node = node.context("native parts require a node")?.0.node_id;
    let Some((upload, number)) = queries::remote_part_due(node, stream)
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(None);
    };
    queries::remote_part_claim(upload, number, token)
        .execute(&mut *tx)
        .await?;
    let mut value = queries::remote_part(&mut tx, upload, number).await?;
    value.read_lease = Some(create_read_lease(&mut tx, value.stream.id, 120).await?);
    tx.commit().await?;
    Ok(Some(value))
}

pub(in super::super) async fn finish_remote_part(
    backend: &super::super::Backend,
    work: &RemotePart,
    etag: Option<&str>,
    node: crate::model::raw::Processor<'_>,
) -> Result<bool> {
    let mut tx = backend.pool.begin().await?;
    backend.fence_control(&mut tx).await?;
    raw_upload_processor(&mut tx, node).await?;
    let changed = queries::remote_part_finish(
        work,
        etag,
        node.context("native parts require a node")?.0.node_id,
    )
    .execute(&mut *tx)
    .await?
    .rows_affected()
        == 1;
    tx.commit().await?;
    Ok(changed)
}

pub(in super::super) async fn freeze_remote(
    backend: &super::super::Backend,
    upload: Uuid,
    token: Uuid,
    hash: &str,
) -> Result<RemoteUpload> {
    let mut tx = backend.pool.begin().await?;
    backend.fence_control(&mut tx).await?;
    ensure!(
        queries::freeze_remote(upload, token, hash)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    let value = queries::remote_upload(upload).fetch_one(&mut *tx).await?;
    tx.commit().await?;
    Ok(value)
}

pub(in super::super) async fn adopt_remote(
    backend: &super::super::Backend,
    input: RemoteOutput<'_>,
) -> Result<Uuid> {
    ensure!(
        input.size >= 0 && input.digest.len() == 16 && input.hash.len() == 32,
        "invalid completed remote source"
    );
    let mut tx = backend.pool.begin().await?;
    backend.fence_control(&mut tx).await?;
    let source = queries::remote_output_lock(input.upload, input.token)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(crate::model::Error::OperationAborted)?;
    require_writing_stream(&mut tx, input.stream).await?;
    ensure!(
        queries::raw_prepare(input.stream)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    ensure!(
        queries::remote_source_finish(source, &input)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    queries::raw_attach(input.stream, source.into(), input.size)
        .execute(&mut *tx)
        .await?;
    if input.background {
        queries::raw_enqueue_processing(input.stream, source.into())
            .execute(&mut *tx)
            .await?;
        queries::remote_processing_hash(input.stream, input.hash)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(source)
}

pub(in super::super) async fn claim_remote_cleanup(
    backend: &super::super::Backend,
    token: Uuid,
    node: crate::model::raw::Processor<'_>,
) -> Result<Option<RemoteUpload>> {
    let mut tx = backend.pool.begin().await?;
    backend.fence_control(&mut tx).await?;
    raw_upload_processor(&mut tx, node).await?;
    let Some(upload) = queries::remote_cleanup_due()
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(None);
    };
    queries::remote_cleanup_claim(upload, token)
        .execute(&mut *tx)
        .await?;
    let value = queries::remote_upload(upload).fetch_one(&mut *tx).await?;
    tx.commit().await?;
    Ok(Some(value))
}

pub(in super::super) async fn commit_conversion_batch(
    pool: &sqlx::PgPool,
    task: Execution,
    candidate: Id,
    rows: &[crate::model::Mapping],
    outputs: &[crate::model::packing::PackOutput],
) -> Result<()> {
    let mut tx = pool.begin().await?;
    commit_conversion_batch_in(&mut tx, task, candidate, rows, outputs).await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn commit_conversion_batch_in(
    tx: &mut Transaction<'_, Postgres>,
    task: Execution,
    candidate: Id,
    rows: &[crate::model::Mapping],
    outputs: &[crate::model::packing::PackOutput],
) -> Result<()> {
    ensure!(
        !rows.is_empty() && rows.len() <= 1024,
        "invalid conversion batch"
    );
    lock_execution(tx, task).await?;
    require_writing_stream(tx, candidate).await?;
    ensure!(
        queries::conversion_owned(candidate, task.id, task.token)
            .fetch_optional(&mut **tx)
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
                .fetch_optional(&mut **tx)
                .await?
                .is_some(),
            crate::model::Error::OperationAborted
        );
        queries::conversion_ready(row.chunk.id, candidate)
            .execute(&mut **tx)
            .await?;
        queries::conversion_retire_encoding(row.chunk.encoding_id)
            .execute(&mut **tx)
            .await?;
    }
    for output in outputs {
        if let Some(pack) = &output.pack {
            ensure!(
                output.independent.is_none()
                    && queries::conversion_output_pack(pack.id, task.id)
                        .fetch_optional(&mut **tx)
                        .await?
                        .is_some(),
                crate::model::Error::OperationAborted
            );
            queries::publish_prepared_pack(pack.id)
                .execute(&mut **tx)
                .await?;
        } else if let Some(chunk) = &output.independent {
            ensure!(
                output.chunks == [chunk.id]
                    && queries::conversion_output_encoding(chunk.encoding_id, task.id, chunk.id)
                        .fetch_optional(&mut **tx)
                        .await?
                        .is_some(),
                crate::model::Error::OperationAborted
            );
            queries::conversion_publish_encoding(chunk.encoding_id)
                .execute(&mut **tx)
                .await?;
            queries::publish_chunk_encoding(chunk)
                .execute(&mut **tx)
                .await?;
        } else {
            anyhow::bail!("conversion output has no physical source");
        }
        for chunk in &output.chunks {
            queries::conversion_layout(*chunk, output.pack.as_ref().map(|p| p.id))
                .execute(&mut **tx)
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
        .execute(&mut **tx)
        .await?;
    }
    require_sources(tx, Some(candidate), &[]).await?;
    let end = rows.last().unwrap().offset_bytes + i64::from(rows.last().unwrap().chunk.raw_size);
    queries::resize_writing_stream(candidate, end)
        .execute(&mut **tx)
        .await?;
    Ok(())
}
