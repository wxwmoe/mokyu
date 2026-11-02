use super::node_work::material;
use super::queries::pack_work as rewrite;
use super::{queries, transactions};
use crate::{
    config,
    content::codec::{self, Chunk, Key},
    model::{
        Error, Id, Mapping, node_read as read, node_work as work, packing::PackOutput,
        task::Execution,
    },
};
use anyhow::{Context, Result, ensure};
use queries::pack_work as db;
use serde_json::{Value, json};
use transactions::{check_source_inputs, commit_pack_outputs_in, pin_inputs, reserve_nonce_in};
use uuid::Uuid;
#[path = "work_packing.rs"]
pub(super) mod packing;
async fn release_task_inputs(tx: &mut db::Connection, task: Uuid) -> Result<()> {
    queries::release_task_inputs(task).execute(tx).await?;
    Ok(())
}
pub(super) async fn offer(
    tx: &mut db::Tx<'_>,
    task: Execution,
    detail: &Value,
    key: &Key,
) -> Result<Option<(work::Plan, Vec<u8>)>> {
    let storage = serde_json::from_value(detail["storage_id"].clone())?;
    let target = material(tx, storage, key).await?;
    let Some((plan, keys)) = packing::prepare(tx, task, detail, &target.profile, key).await? else {
        let row = queries::task_record(task.id).fetch_one(&mut **tx).await?;
        if row.execution_token == Some(task.token) {
            queries::finish_task(
                task,
                if row.state == "completed" {
                    "completed"
                } else {
                    "queued"
                },
                None,
            )
            .execute(&mut **tx)
            .await?;
        }
        return Ok(None);
    };
    let mut stores = std::collections::BTreeSet::new();
    if let work::Plan::Pack { batch, .. } = &plan {
        for source in &batch.sources {
            if let Some(id) = source.storage() {
                if id != storage {
                    stores.insert(id);
                }
            }
        }
    }
    let mut additional = Vec::new();
    for id in stores {
        additional.push(material(tx, id, key).await?);
    }
    Ok(Some((
        plan,
        serde_json::to_vec(&work::Materials {
            storage: target,
            additional,
            keys,
        })?,
    )))
}
pub(super) async fn finish(
    tx: &mut db::Tx<'_>,
    task: Execution,
    storage: Uuid,
    batch: crate::model::node_pack::Batch,
    outcomes: &[work::Outcome],
    key: &Key,
) -> Result<(&'static str, Option<&'static str>)> {
    let record = queries::task_record(task.id).fetch_one(&mut **tx).await?;
    ensure!(
        record.execution_token == Some(task.token),
        Error::NodeExpired
    );
    let error = packing::finish(tx, task, storage, batch, &record.detail, outcomes, key).await?;
    let current = queries::task_record(task.id).fetch_one(&mut **tx).await?;
    Ok((
        if current.state == "completed" {
            "completed"
        } else {
            "queued"
        },
        error,
    ))
}

pub(super) async fn finish_write(
    tx: &mut db::Tx<'_>,
    row: &super::node_writes::Write,
    stream: Id,
    items: &[crate::model::node_chunk::Item],
    batch: &crate::model::node_pack::Batch,
    result: &crate::model::node_pack::Encoded,
    key: &Key,
) -> Result<()> {
    let id = row.owner_task_id.ok_or(Error::NodeOperationConflict)?;
    let record = queries::task_record(id).fetch_one(&mut **tx).await?;
    let task = Execution {
        id,
        token: record.execution_token.ok_or(Error::NodeExpired)?,
    };
    if !result.packs.is_empty() {
        ensure!(
            db::creation_allowed(&mut **tx).await?,
            Error::OperationAborted
        );
    }
    let outputs = packing::outputs(
        tx,
        row.storage_id,
        batch,
        result,
        &vec![None; batch.chunks.len()],
        key,
    )
    .await?;
    let mut used = std::collections::BTreeSet::new();
    let rows = items
        .iter()
        .filter(|item| !item.reused && used.insert(item.chunk.id))
        .map(|item| Mapping {
            chunk: item.chunk.clone(),
            offset_bytes: item.offset,
            pack_id: None,
        })
        .collect::<Vec<_>>();
    ensure!(
        rows.iter()
            .map(|r| r.chunk.id)
            .eq(batch.chunks.iter().map(|c| c.id)),
        Error::NodeOperationConflict
    );
    db::record_outputs(tx, id, &rows, &outputs).await?;
    transactions::commit_conversion_batch_in(tx, task, stream, &rows, &outputs).await?;
    used.clear();
    for item in items.iter().filter(|item| !item.reused) {
        if !used.insert(item.chunk.id) {
            transactions::reference_chunk_in(
                tx,
                stream,
                item.offset,
                item.chunk.id,
                item.chunk.raw_size,
            )
            .await?;
        }
    }
    Ok(())
}
