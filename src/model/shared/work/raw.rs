use super::{
    node_work::material,
    queries::{self, node_registry as registry, work as q},
    transactions,
};
use crate::{
    content::codec::Key,
    model::{Error, Id, node_work as work, task::Execution},
};
use anyhow::{Result, ensure};
use uuid::Uuid;
pub(super) async fn schedule(tx: &mut registry::Tx<'_>, limit: usize) -> Result<()> {
    let candidates = q::hash_candidates(limit.min(work::MAX_ITEMS) as i64)
        .fetch_all(&mut **tx)
        .await?;
    let Some(&(_, storage, _)) = candidates.first() else {
        return Ok(());
    };
    let profile = queries::storage_profile(storage)
        .fetch_one(&mut **tx)
        .await?;
    let bytes = profile
        .maintenance_byte_budget
        .as_deref()
        .map(crate::config::bytes)
        .transpose()?
        .unwrap_or(64 * 1024 * 1024);
    let task = Uuid::now_v7();
    let mut total = 0u64;
    let mut streams = Vec::new();
    for (stream, id, size) in candidates {
        if id != storage {
            continue;
        }
        let size = u64::try_from(size)?;
        if !streams.is_empty() && total.saturating_add(size) > bytes {
            break;
        }
        queries::raw_processing_claim(stream, task, true)
            .execute(&mut **tx)
            .await?;
        streams.push(stream);
        total = total.saturating_add(size);
    }
    let detail =
        serde_json::json!({"operation":"deduplicate","storage_id":storage,"node_raw":streams});
    queries::insert_task(task, "upload", None, detail, None)
        .execute(&mut **tx)
        .await?;
    Ok(())
}
pub(super) async fn offer(
    tx: &mut registry::Tx<'_>,
    task: Execution,
    detail: &serde_json::Value,
    key: &Key,
) -> Result<Option<(work::Plan, Vec<u8>)>> {
    let storage = serde_json::from_value(detail["storage_id"].clone())?;
    let streams: Vec<Id> = serde_json::from_value(detail["node_raw"].clone())?;
    ensure!(streams.len() <= work::MAX_ITEMS, Error::NodeWorkLimit);
    let mut items = Vec::new();
    let mut selected = Vec::new();
    for stream in streams {
        let Some(job) = queries::raw_processing_record(stream)
            .fetch_optional(&mut **tx)
            .await?
        else {
            continue;
        };
        ensure!(job.token == task.id, Error::NodeOperationConflict);
        if queries::raw_processing_stream(&job)
            .fetch_optional(&mut **tx)
            .await?
            .is_none()
        {
            queries::raw_processing_delete(&job)
                .execute(&mut **tx)
                .await?;
            continue;
        }
        let source = queries::raw_source(job.source).fetch_one(&mut **tx).await?;
        ensure!(
            source.storage_id == storage && source.medium == "shared_s3" && !source.pending_upload,
            Error::SourceUnavailable
        );
        let hash = job
            .content_hash
            .filter(|_| job.source_revision == Some(source.revision));
        selected.push(stream);
        items.push(work::RawHash { source, hash });
    }
    if items.is_empty() {
        queries::finish_task(task, "completed", None)
            .execute(&mut **tx)
            .await?;
        return Ok(None);
    }
    let mut detail = detail.clone();
    detail["node_raw"] = serde_json::to_value(selected)?;
    ensure!(
        q::progress(task, &detail)
            .execute(&mut **tx)
            .await?
            .rows_affected()
            == 1,
        Error::OperationAborted
    );
    let context = work::Materials {
        storage: material(tx, storage, key).await?,
        additional: vec![],
        keys: vec![],
    };
    Ok(Some((
        work::Plan::HashRaw { storage, items },
        serde_json::to_vec(&context)?,
    )))
}
pub(super) async fn finish(
    tx: &mut registry::Tx<'_>,
    task: Execution,
    items: &[work::RawHash],
    outcomes: &[work::Outcome],
) -> Result<()> {
    ensure!(items.len() == outcomes.len(), Error::NodeOperationConflict);
    let record = queries::task_record(task.id).fetch_one(&mut **tx).await?;
    ensure!(
        record.state == "running" && record.execution_token == Some(task.token),
        Error::NodeExpired
    );
    let streams: Vec<Id> = serde_json::from_value(record.detail["node_raw"].clone())?;
    ensure!(streams.len() == items.len(), Error::NodeOperationConflict);
    for ((stream, item), outcome) in streams.into_iter().zip(items).zip(outcomes) {
        let Some(job) = queries::raw_processing_record(stream)
            .fetch_optional(&mut **tx)
            .await?
        else {
            continue;
        };
        if job.token != task.id {
            continue;
        }
        ensure!(job.source == item.source.id, Error::NodeOperationConflict);
        match outcome {
            work::Outcome::Hashed { hash } if hash.len() == 32 => {
                let current = queries::raw_source(job.source).fetch_one(&mut **tx).await?;
                if current == item.source {
                    transactions::finish_raw_processing_in(tx, &job, &item.source, hash).await?;
                } else {
                    q::rehash(job.stream, task.id).execute(&mut **tx).await?;
                }
            }
            work::Outcome::Retry => {
                q::rehash(job.stream, task.id).execute(&mut **tx).await?;
            }
            _ => return Err(Error::NodeOperationConflict.into()),
        }
    }
    Ok(())
}
