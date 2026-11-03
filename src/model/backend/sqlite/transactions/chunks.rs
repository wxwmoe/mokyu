use super::*;

pub(in super::super) async fn append_tail(pool: &sqlx::SqlitePool, tail: StreamTail) -> Result<()> {
    tail.validate_staging()?;
    let fragment = Id::from(
        queries::insert_fragment(&tail)
            .execute(pool)
            .await?
            .last_insert_rowid(),
    );
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("fragment-persisted").await;
    writing_stream(&mut tx, tail.stream).await?;
    queries::seal_fragment(fragment).execute(&mut *tx).await?;
    queries::insert_fragment_extent(&tail, fragment)
        .execute(&mut *tx)
        .await?;
    queries::resize_writing_stream(tail.stream, tail.offset + i64::from(tail.size))
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn writing_stream(connection: &mut SqliteConnection, stream: Id) -> Result<()> {
    ensure!(
        queries::lock_writing_stream(stream)
            .fetch_optional(connection)
            .await?
            .is_some(),
        crate::model::Error::OperationAborted
    );
    Ok(())
}

pub(in super::super) async fn allocate_chunk(
    pool: &sqlx::SqlitePool,
    input: ChunkCreate,
) -> Result<(Chunk, bool)> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let result = allocate_chunk_in(&mut tx, input).await?;
    tx.commit().await?;
    Ok(result)
}

pub(in super::super) async fn allocate_chunk_in(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    input: ChunkCreate,
) -> Result<(Chunk, bool)> {
    writing_stream(tx, input.stream).await?;
    if let Some(key) = input.key {
        queries::encoding_key(key, &input.algorithm)
            .fetch_one(&mut **tx)
            .await?;
    }
    queries::fail_chunk_allocations(&input)
        .execute(&mut **tx)
        .await?;
    queries::retire_failed_encodings(&input)
        .execute(&mut **tx)
        .await?;
    let (chunk, reused) = if let Some(chunk) = queries::reusable_chunk(tx, &input).await? {
        (chunk, true)
    } else {
        let chunk = queries::insert_chunk(&input)
            .execute(&mut **tx)
            .await
            .map_err(|error| {
                if error
                    .as_database_error()
                    .is_some_and(|e| e.is_unique_violation())
                {
                    anyhow::Error::from(crate::model::Error::OperationAborted)
                } else {
                    anyhow::Error::from(error)
                }
            })?
            .last_insert_rowid()
            .into();
        let encoding = queries::allocate_encoding(chunk)
            .execute(&mut **tx)
            .await?
            .last_insert_rowid()
            .into();
        queries::set_chunk_encoding(chunk, encoding)
            .execute(&mut **tx)
            .await?;
        queries::refresh_encoding_fingerprint(tx, encoding).await?;
        (queries::allocated_chunk(tx, chunk).await?, false)
    };
    if reused {
        require_sources(tx, None, &[chunk.id]).await?;
    }
    Ok((chunk, reused))
}

pub(in super::super) async fn reference_chunk(
    pool: &sqlx::SqlitePool,
    stream: Id,
    offset: i64,
    chunk: Id,
    size: i32,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    reference_chunk_in(&mut tx, stream, offset, chunk, size).await?;
    tx.commit().await?;
    Ok(())
}
pub(in super::super) async fn reference_chunk_in(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    stream: Id,
    offset: i64,
    chunk: Id,
    size: i32,
) -> Result<()> {
    writing_stream(tx, stream).await?;
    ensure!(
        queries::lock_chunk(chunk).fetch_one(&mut **tx).await? == "ready",
        crate::model::Error::OperationAborted
    );
    require_sources(tx, None, &[chunk]).await?;
    queries::insert_chunk_extent(stream, offset, size, chunk)
        .execute(&mut **tx)
        .await?;
    queries::changed_references(tx, &[chunk], true).await?;
    queries::pin_chunk(chunk, stream).execute(&mut **tx).await?;
    queries::attach_pending_upload(chunk, stream, offset)
        .execute(&mut **tx)
        .await?;
    queries::resize_writing_stream(stream, offset + i64::from(size))
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub(in super::super) async fn complete_chunk_write(
    pool: &sqlx::SqlitePool,
    stream: Id,
    offset: i64,
    chunk: &Chunk,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    complete_chunk_write_in(&mut tx, stream, offset, chunk).await?;
    tx.commit().await?;
    Ok(())
}
pub(in super::super) async fn complete_chunk_write_in(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    stream: Id,
    offset: i64,
    chunk: &Chunk,
) -> Result<()> {
    writing_stream(tx, stream).await?;
    ensure!(
        queries::allocated_source(stream, chunk)
            .build_query_scalar::<i64>()
            .fetch_optional(&mut **tx)
            .await?
            .is_some(),
        crate::model::Error::OperationAborted
    );
    ensure!(
        queries::complete_chunk(chunk)
            .execute(&mut **tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    ensure!(
        queries::complete_encoding(chunk.encoding_id)
            .execute(&mut **tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    queries::insert_chunk_extent(stream, offset, chunk.raw_size, chunk.id)
        .execute(&mut **tx)
        .await?;
    queries::resize_writing_stream(stream, offset + i64::from(chunk.raw_size))
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub(in super::super) async fn copy_chunk_references(
    pool: &sqlx::SqlitePool,
    destination: Id,
    source: Id,
    offset: i64,
    end: i64,
    chunks: &[Id],
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let result =
        copy_chunk_references_in(&mut tx, destination, source, offset, end, chunks).await?;
    tx.commit().await?;
    Ok(result)
}

pub(in super::super) async fn copy_chunk_references_in(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    destination: Id,
    source: Id,
    offset: i64,
    end: i64,
    chunks: &[Id],
) -> Result<()> {
    writing_stream(tx, destination).await?;
    let locked = queries::lock_ready_chunks(tx, chunks).await?;
    ensure!(locked.len() == chunks.len(), "copy source is not ready");
    require_sources(tx, None, chunks).await?;
    queries::copy_extents(destination, source, offset, end)
        .execute(&mut **tx)
        .await?;
    queries::reference_chunks(tx, chunks, destination).await?;
    queries::resize_writing_stream(destination, end)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub(in super::super) async fn start_chunk_upload(
    pool: &sqlx::SqlitePool,
    stream: Id,
    chunk: &Chunk,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    start_chunk_upload_in(&mut tx, stream, chunk).await?;
    tx.commit().await?;
    Ok(())
}
pub(in super::super) async fn start_chunk_upload_in(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    stream: Id,
    chunk: &Chunk,
) -> Result<()> {
    writing_stream(tx, stream).await?;
    ensure!(
        queries::mark_chunk_uploading(stream, chunk)
            .execute(&mut **tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    ensure!(
        queries::register_encoding(chunk)
            .execute(&mut **tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    queries::refresh_encoding_fingerprint(tx, chunk.encoding_id).await?;
    Ok(())
}

pub(in super::super) async fn register_encoding(
    pool: &sqlx::SqlitePool,
    chunk: &Chunk,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    ensure!(
        queries::register_encoding(chunk)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    queries::refresh_encoding_fingerprint(&mut tx, chunk.encoding_id).await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn read_quota(
    pool: &sqlx::SqlitePool,
    principal: &Principal,
    scope: &str,
    id: uuid::Uuid,
) -> Result<Quota> {
    validate_scope(scope)?;
    let mut tx = pool.begin().await?;
    let admin = queries::validate_identity(principal, &mut tx).await?;
    let visible = if scope == "bucket" {
        lock_permit(
            &mut tx,
            &crate::authorization::Permit::for_action(
                principal.clone(),
                id,
                crate::authorization::Action::Inspect,
            ),
        )
        .await?;
        true
    } else if admin {
        true
    } else {
        ensure!(principal.token_id().is_none(), s3s::s3_error!(AccessDenied));
        queries::quota_membership(principal.user_id(), id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| s3s::s3_error!(AccessDenied))?
            == "all"
    };
    let row = queries::quota_account(scope, id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?
        .view(visible);
    tx.commit().await?;
    Ok(row)
}
