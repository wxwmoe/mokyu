use super::*;

pub(in super::super) async fn allocate_chunk(
    pool: &PgPool,
    input: ChunkCreate,
    single: bool,
) -> Result<(Chunk, bool)> {
    let mut tx = pool.begin().await?;
    let result = allocate_chunk_in(&mut tx, input, single).await?;
    tx.commit().await?;
    Ok(result)
}

pub(in super::super) async fn allocate_chunk_in(
    db: &mut PgConnection,
    input: ChunkCreate,
    single: bool,
) -> Result<(Chunk, bool)> {
    require_chunk_writer(db, input.stream, single).await?;
    if !single
        && !queries::try_claim_chunk_hash(input.storage, &input.hash)
            .fetch_one(&mut *db)
            .await?
    {
        return Err(crate::model::Error::ChunkBusy.into());
    }
    queries::fail_chunk_allocations(&input, single)
        .execute(&mut *db)
        .await?;
    let old = queries::reusable_chunk(&input)
        .fetch_optional(&mut *db)
        .await?;
    let reused = old.is_some();
    let chunk = match old {
        Some(chunk) => Some(chunk),
        None => {
            queries::insert_chunk(&input)
                .fetch_optional(&mut *db)
                .await?
        }
    };
    let Some(chunk) = chunk else {
        return Err(crate::model::Error::ChunkBusy.into());
    };
    if reused {
        require_sources(db, None, &[chunk.id]).await?;
    }
    Ok((chunk, reused))
}

pub(super) async fn require_chunk_writer(
    db: &mut PgConnection,
    stream: Id,
    single: bool,
) -> Result<()> {
    let observed = queries::repair_owner(stream)
        .fetch_optional(&mut *db)
        .await?
        .ok_or(crate::model::Error::OperationAborted)?;
    if let Some(task) = observed.2 {
        let token = observed.3.ok_or(crate::model::Error::OperationAborted)?;
        ensure!(
            queries::lock_owned_task(&task, &token)
                .fetch_optional(&mut *db)
                .await?
                .is_some(),
            crate::model::Error::OperationAborted
        );
    }
    ensure!(
        queries::repair_stream(stream)
            .fetch_optional(&mut *db)
            .await?
            == Some(observed),
        crate::model::Error::OperationAborted
    );
    if !single {
        ensure!(
            queries::repair_lease(stream)
                .fetch_optional(&mut *db)
                .await?
                .is_some(),
            crate::model::Error::OperationAborted
        );
    }
    Ok(())
}

pub(in super::super) async fn start_chunk_upload(
    pool: &PgPool,
    stream: Id,
    chunk: &Chunk,
    single: bool,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    start_chunk_upload_in(&mut tx, stream, chunk, single).await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn start_chunk_upload_in(
    db: &mut PgConnection,
    stream: Id,
    chunk: &Chunk,
    single: bool,
) -> Result<()> {
    require_chunk_writer(db, stream, single).await?;
    ensure!(
        queries::mark_chunk_uploading(stream, chunk)
            .execute(&mut *db)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    queries::register_encoding(chunk, (!single).then_some(stream))
        .execute(&mut *db)
        .await?;
    Ok(())
}

pub(in super::super) async fn reference_chunk(
    pool: &PgPool,
    stream: Id,
    offset: i64,
    chunk_id: Id,
    raw_size: i32,
    single: bool,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    reference_chunk_in(&mut tx, stream, offset, chunk_id, raw_size, single).await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn reference_chunk_in(
    db: &mut PgConnection,
    stream: Id,
    offset: i64,
    chunk_id: Id,
    raw_size: i32,
    single: bool,
) -> Result<()> {
    require_chunk_writer(db, stream, single).await?;
    let state: String = queries::lock_chunk(chunk_id).fetch_one(&mut *db).await?;
    if state != "ready" {
        return Err(s3s::s3_error!(OperationAborted, "chunk is being reclaimed").into());
    }
    require_sources(db, None, &[chunk_id]).await?;
    queries::insert_chunk_extent(stream, offset, raw_size, chunk_id)
        .execute(&mut *db)
        .await?;
    queries::reference_chunk(chunk_id).execute(&mut *db).await?;
    queries::pin_chunk(chunk_id, stream)
        .execute(&mut *db)
        .await?;
    queries::attach_pending_upload(chunk_id, stream, offset)
        .execute(&mut *db)
        .await?;
    queries::extend_writing_stream(stream, offset + raw_size as i64)
        .execute(&mut *db)
        .await?;
    Ok(())
}

pub(in super::super) async fn complete_chunk_write(
    pool: &PgPool,
    stream: Id,
    offset: i64,
    chunk: &Chunk,
    single: bool,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    complete_chunk_write_in(&mut tx, stream, offset, chunk, single).await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn complete_chunk_write_in(
    db: &mut PgConnection,
    stream: Id,
    offset: i64,
    chunk: &Chunk,
    single: bool,
) -> Result<()> {
    require_chunk_writer(db, stream, single).await?;
    ensure!(
        queries::allocated_source(stream, chunk)
            .build_query_scalar::<Id>()
            .fetch_optional(&mut *db)
            .await?
            .is_some(),
        crate::model::Error::OperationAborted
    );
    let changed = queries::complete_chunk(chunk.id)
        .execute(&mut *db)
        .await?
        .rows_affected();
    queries::complete_encoding(chunk.encoding_id)
        .execute(&mut *db)
        .await?;
    if changed != 1 {
        return Err(s3s::s3_error!(OperationAborted).into());
    }
    queries::insert_chunk_extent(stream, offset, chunk.raw_size, chunk.id)
        .execute(&mut *db)
        .await?;
    queries::resize_writing_stream(stream, offset + chunk.raw_size as i64)
        .execute(&mut *db)
        .await?;
    Ok(())
}

pub(in super::super) async fn copy_chunk_references(
    pool: &PgPool,
    destination: Id,
    source: Id,
    offset: i64,
    end: i64,
    chunks: &[Id],
) -> Result<()> {
    let mut tx = pool.begin().await?;
    copy_chunk_references_in(&mut tx, destination, source, offset, end, chunks).await?;
    tx.commit().await?;
    Ok(())
}
