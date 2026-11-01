use super::*;

pub(super) async fn mark_unreferenced(
    tx: &mut Transaction<'_, Postgres>,
    chunks: &[Id],
) -> Result<()> {
    if !chunks.is_empty() {
        queries::mark_unreferenced_chunks(chunks)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

pub(super) async fn replace_upload_range(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    stream: Id,
    offset: i64,
    length: usize,
    chunk: Id,
    source: usize,
) -> Result<()> {
    if length == 0 {
        return Ok(());
    }
    let end = offset + length as i64;
    let rows: Vec<Extent> = queries::lock_upload_extents(stream, offset, end)
        .fetch_all(&mut **tx)
        .await?;
    let mut covered = offset;
    let mut previous = Vec::new();
    for row in rows {
        if let Some(id) = row.chunk_id {
            previous.push(id);
        }
        ensure!(row.offset_bytes <= covered, "extent replacement gap");
        let row_end = row.offset_bytes + row.length as i64;
        covered = covered.max(row_end.min(end));
        queries::remove_upload_extent(stream, row.offset_bytes)
            .execute(&mut **tx)
            .await?;
        if row.offset_bytes < offset {
            insert_upload_extent(
                tx,
                &row,
                row.offset_bytes,
                (offset - row.offset_bytes) as i32,
                row.source_offset,
            )
            .await?;
        }
        if row_end > end {
            insert_upload_extent(
                tx,
                &row,
                end,
                (row_end - end) as i32,
                row.source_offset + (end - row.offset_bytes) as i32,
            )
            .await?;
        }
    }
    ensure!(covered == end, "extent replacement incomplete");
    queries::insert_upload_chunk_extent(stream, offset, length as i32, chunk, source as i32)
        .execute(&mut **tx)
        .await?;
    // Retained slices and references from other streams still protect the old chunk.
    mark_unreferenced(tx, &previous).await?;
    Ok(())
}

async fn insert_upload_extent(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    e: &Extent,
    offset: i64,
    length: i32,
    source: i32,
) -> Result<()> {
    queries::insert_upload_extent(e, offset, length, source)
        .execute(&mut **tx)
        .await?;
    Ok(())
}
