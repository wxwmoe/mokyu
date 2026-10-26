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
    let created = queries::node_clock().fetch_one(&mut **db).await?;
    let mut batch = wire::Batch {
        algorithm: "none".into(),
        key: row.block_key_id,
        created,
        sources: vec![],
        chunks: vec![],
        groups: vec![],
        packs: vec![],
    };
    let mut unique = BTreeSet::new();
    let maximum = config::bytes(&row.profile.pack_max_size)?;
    let mut start = 0;
    let mut bytes = 0u64;
    for item in items.iter().filter(|item| !item.reused) {
        ensure!(
            !item.chunk.compressed && item.payload_hash.as_slice() == item.chunk.hash,
            Error::NodeOperationConflict
        );
        if !unique.insert(item.chunk.id) {
            continue;
        }
        if bytes + item.chunk.raw_size as u64 > maximum && start < batch.chunks.len() {
            batch.groups.push((start, batch.chunks.len()));
            start = batch.chunks.len();
            bytes = 0;
        }
        bytes += item.chunk.raw_size as u64;
        batch.algorithm = item.chunk.algorithm.clone();
        batch.chunks.push(wire::Target {
            id: item.chunk.id,
            size: item.chunk.raw_size,
            hash: item.chunk.hash.clone(),
            previous_size: None,
            compress: true,
            location: Uuid::now_v7(),
            nonce: None,
            ready: None,
        });
    }
    if batch.chunks.is_empty() {
        return Ok(None);
    }
    batch.groups.push((start, batch.chunks.len()));
    let allowed: bool =
        sqlx::query_scalar("SELECT NOT pack_creation_paused FROM mokyu_meta WHERE singleton")
            .fetch_one(&mut **db)
            .await?;
    if allowed {
        for &(start, end) in &batch.groups {
            let middle = start + (end - start) / 2;
            for (start, end) in [(start, end), (start, middle), (middle, end)] {
                if end - start > 1 {
                    batch.packs.push(wire::Attempt {
                        start,
                        end,
                        location: Uuid::now_v7(),
                        nonce: None,
                    });
                }
            }
        }
    }
    if let Some(key) = batch.key {
        for tag in [crate::content::nonce::CHUNK, crate::content::nonce::PACK] {
            let slots: Vec<_> = if tag == crate::content::nonce::CHUNK {
                batch
                    .chunks
                    .iter_mut()
                    .map(|chunk| &mut chunk.nonce)
                    .collect()
            } else {
                batch.packs.iter_mut().map(|pack| &mut pack.nonce).collect()
            };
            if !slots.is_empty() {
                let units = slots.len().div_ceil(crate::content::nonce::UNIT as usize) as u32;
                let (first, last) = reserve_nonce_in(db, key, tag, units).await?;
                ensure!(
                    last - first + 1 >= slots.len() as i64,
                    Error::OperationAborted
                );
                for (offset, nonce) in slots.into_iter().enumerate() {
                    *nonce =
                        Some(codec::typed_nonce(created, first + offset as i64, tag)?.to_vec());
                }
            }
        }
    }
    queries::pack_work::prepare_outputs(db, task, storage, &batch).await?;
    Ok(Some(batch))
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
    queries::pack_work::record_outputs(db, task.id, &rows, &outputs).await?;
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
