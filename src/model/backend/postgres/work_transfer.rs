use super::*;
use crate::model::{
    node_pack as wire,
    transfer::{Copy, Input, Work},
};
use queries::pack_work as p;
use std::collections::BTreeSet;

pub(super) struct Prepared {
    pub work: Work,
    pub batch: wire::Batch,
    pub keys: Vec<read::BlockKey>,
}

pub(super) async fn prepare(
    db: &mut Transaction<'_, Postgres>,
    task: Execution,
    copy: &Copy,
    key: &Key,
) -> Result<Option<Prepared>> {
    copy.validate()?;
    let mut seen = BTreeSet::new();
    let mut rows: Vec<_> = copy
        .prepared
        .targets
        .iter()
        .filter(|target| !target.reused && seen.insert(target.chunk.id))
        .map(|target| Mapping {
            chunk: target.chunk.clone(),
            offset_bytes: target.offset as i64,
            pack_id: None,
        })
        .collect();
    if rows.is_empty() {
        let run = super::super::super::replication_transactions::owned(db, task).await?;
        super::super::super::transfer_transactions::copied(db, task, &run, copy, &[]).await?;
        return Ok(None);
    }
    rows.sort_by_key(|row| row.offset_bytes);
    let references = p::reference_boundaries(&mut **db, &rows.iter().collect::<Vec<_>>()).await?;
    let policy = &copy.prepared.policy;
    let maximum = config::bytes(&policy.pack_max_size)?;
    let mut batch = wire::Batch {
        algorithm: policy.algorithm.clone(),
        key: policy.block_key_id,
        created: queries::node_clock().fetch_one(&mut **db).await?,
        sources: vec![],
        chunks: vec![],
        groups: vec![],
        packs: vec![],
    };
    for row in &rows {
        batch.chunks.push(wire::Target {
            id: row.chunk.id,
            size: row.chunk.raw_size,
            hash: row.chunk.hash.clone(),
            previous_size: None,
            compress: true,
            location: Uuid::now_v7(),
            nonce: None,
            ready: None,
        });
    }
    let mut start = 0;
    while start < rows.len() {
        let mut end = start + 1;
        let mut size = rows[start].chunk.raw_size as u64;
        while end < rows.len()
            && references[end - 1]
            && size + rows[end].chunk.raw_size as u64 <= maximum
        {
            size += rows[end].chunk.raw_size as u64;
            end += 1;
        }
        batch.groups.push((start, end));
        if policy.pack_enabled && end - start > 1 {
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
        start = end;
    }
    packing::reserve_outputs(db, task.id, copy.storage, &mut batch).await?;
    let work = Work::from_copy(copy, &batch)?;
    let mut blocks = BTreeSet::new();
    blocks.extend(policy.block_key_id);
    match &copy.input {
        Input::Chunk { chunk } => blocks.extend(chunk.block_key_id),
        Input::Pack { pack, .. } => blocks.extend(pack.block_key_id),
        Input::Raw { .. } => {}
    }
    let keys = rewrite::keys(db, &blocks.into_iter().collect::<Vec<_>>(), key).await?;
    Ok(Some(Prepared { work, batch, keys }))
}

pub(super) async fn finish(
    db: &mut Transaction<'_, Postgres>,
    task: Execution,
    storage: Uuid,
    work: &Work,
    batch: &wire::Batch,
    result: &wire::Encoded,
    key: &Key,
) -> Result<()> {
    work.validate(storage, batch)?;
    let run = super::super::super::replication_transactions::owned(db, task).await?;
    let unit = super::super::super::transfer_queries::unit(task.id, work.unit)
        .fetch_one(&mut **db)
        .await?;
    let copy = Copy {
        unit: work.unit,
        revision: work.revision,
        storage,
        candidate: unit.candidate.context("transfer candidate missing")?,
        input: unit.input.context("transfer input missing")?.0,
        prepared: unit
            .output
            .context("transfer output missing")?
            .0
            .chunks()?
            .clone(),
    };
    let expected = Work::from_copy(&copy, batch)?;
    ensure!(
        serde_json::to_value(&expected)? == serde_json::to_value(work)?,
        Error::NodeOperationConflict
    );
    let outputs = packing::outputs(
        db,
        storage,
        batch,
        result,
        &vec![None; batch.chunks.len()],
        key,
    )
    .await?;
    let rows = copy.outputs(&outputs)?;
    p::record_outputs(db, task.id, &rows, &outputs).await?;
    super::super::super::transfer_transactions::copied(db, task, &run, &copy, &outputs).await
}
