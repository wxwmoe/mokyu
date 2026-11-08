use super::*;
use crate::model::{node_chunk as chunk, node_pack as wire};
use std::collections::BTreeSet;

pub(super) async fn prepare(
    db: &mut Transaction<'_, Postgres>,
    row: &queries::NodeWrite,
    items: &[chunk::Item],
) -> Result<Option<wire::Batch>> {
    let Some(task) = row.owner_task_id.filter(|_| row.profile.pack_enabled) else {
        return Ok(None);
    };
    let storage = row.storage_id.ok_or(Error::SourceUnavailable)?;
    packing::prepare_write(db, task, storage, row.block_key_id, &row.profile, items).await
}

pub(super) async fn finish(
    db: &mut Transaction<'_, Postgres>,
    row: &queries::NodeWrite,
    items: &[chunk::Item],
    batch: &wire::Batch,
    result: &wire::Encoded,
    key: &Key,
) -> Result<()> {
    let task = Execution {
        id: row.owner_task_id.ok_or(Error::NodeOperationConflict)?,
        token: row.owner_lease_token.ok_or(Error::NodeExpired)?,
    };
    let storage = row.storage_id.ok_or(Error::SourceUnavailable)?;
    if !result.packs.is_empty() {
        let allowed: bool =
            sqlx::query_scalar("SELECT NOT pack_creation_paused FROM mokyu_meta WHERE singleton")
                .fetch_one(&mut **db)
                .await?;
        ensure!(allowed, Error::OperationAborted);
    }

    let outputs = packing::outputs(
        db,
        storage,
        batch,
        result,
        &vec![None; batch.chunks.len()],
        key,
    )
    .await?;
    let mut unique = BTreeSet::new();
    let rows: Vec<_> = items
        .iter()
        .filter(|item| !item.reused && unique.insert(item.chunk.id))
        .map(|item| Mapping {
            chunk: item.chunk.clone(),
            offset_bytes: item.offset,
            pack_id: None,
        })
        .collect();
    ensure!(
        rows.iter()
            .map(|row| row.chunk.id)
            .eq(batch.chunks.iter().map(|chunk| chunk.id)),
        Error::NodeOperationConflict
    );
    queries::pack_work::record_outputs(db, task.id, &outputs).await?;
    let stream: Id = row.stream_id.ok_or(Error::NodeExpired)?.into();
    commit_conversion_batch_in(db, task, stream, &rows, &outputs).await?;
    let mut completed = BTreeSet::new();
    for item in items.iter().filter(|item| !item.reused) {
        if !completed.insert(item.chunk.id) {
            reference_chunk_in(
                db,
                stream,
                item.offset,
                item.chunk.id,
                item.chunk.raw_size,
                false,
            )
            .await?;
        }
    }
    Ok(())
}
