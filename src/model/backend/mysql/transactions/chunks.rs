use super::*;

pub(in super::super) async fn reference_chunk(
    pool: &sqlx::MySqlPool,
    stream: Id,
    offset: i64,
    chunk: Id,
    size: i32,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    reference_chunk_in(&mut tx, stream, offset, chunk, size).await?;
    tx.commit().await?;
    Ok(())
}
pub(in super::super) async fn reference_chunk_in(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
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
    pool: &sqlx::MySqlPool,
    stream: Id,
    offset: i64,
    chunk: &Chunk,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    complete_chunk_write_in(&mut tx, stream, offset, chunk).await?;
    tx.commit().await?;
    Ok(())
}
pub(in super::super) async fn complete_chunk_write_in(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
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
    pool: &sqlx::MySqlPool,
    destination: Id,
    source: Id,
    offset: i64,
    end: i64,
    chunks: &[Id],
) -> Result<()> {
    let mut tx = pool.begin().await?;
    let result =
        copy_chunk_references_in(&mut tx, destination, source, offset, end, chunks).await?;
    tx.commit().await?;
    Ok(result)
}

pub(in super::super) async fn copy_chunk_references_in(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
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
    pool: &sqlx::MySqlPool,
    stream: Id,
    chunk: &Chunk,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    start_chunk_upload_in(&mut tx, stream, chunk).await?;
    tx.commit().await?;
    Ok(())
}
pub(in super::super) async fn start_chunk_upload_in(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
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
    pool: &sqlx::MySqlPool,
    chunk: &Chunk,
) -> Result<()> {
    let mut tx = pool.begin().await?;
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

pub(in super::super) async fn identify(
    owner: &mut MySqlConnection,
    fingerprint: &[u8],
) -> Result<()> {
    let mut tx = owner.begin().await?;
    if let Some(saved) = queries::fingerprint().fetch_one(&mut *tx).await? {
        ensure!(
            saved == fingerprint,
            "deployment key does not match this database"
        );
    } else {
        queries::set_fingerprint(fingerprint)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn list_managed_keys(
    pool: &MySqlPool,
    principal: &Principal,
) -> Result<Vec<ManagedKeyView>> {
    let mut tx = admin_transaction(pool, principal).await?;
    let rows = queries::managed_key_views(&mut tx).await?;
    tx.commit().await?;
    Ok(rows)
}

pub(in super::super) async fn create_managed_key(
    pool: &MySqlPool,
    principal: &Principal,
    input: ManagedKeyCreate,
) -> Result<ManagedKeyView> {
    let mut tx = administration_transaction(pool, principal).await?;
    queries::insert_managed_key(&input)
        .execute(&mut *tx)
        .await?;
    let result = queries::managed_key_view(&mut tx, input.id).await?;
    if input.purpose == "block" {
        queries::initialize_nonce_counters(input.id)
            .execute(&mut *tx)
            .await?;
    }
    checkpoint_event(&mut tx, "managed_key.create", &input.id.to_string(), serde_json::json!({"name":result.name,"purpose":result.purpose,"algorithm":result.algorithm,"fingerprint":result.fingerprint})).await?;
    tx.commit().await?;
    Ok(result)
}
