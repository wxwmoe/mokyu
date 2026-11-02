use super::{
    Backend, node_registry,
    queries::{self, node_files as q, node_registry as registry},
    transactions,
};
use crate::{
    cluster::NodeState,
    content::codec::Key,
    model::{
        Error, Id,
        node_native::Inventory,
        node_raw::{File, FileState},
        node_work as work,
        task::Execution,
    },
};
use anyhow::{Result, ensure};
use uuid::Uuid;
pub(super) async fn queue(
    tx: &mut registry::Tx<'_>,
    node: Uuid,
    stream: Id,
    source: Id,
    storage: Uuid,
) -> Result<()> {
    ensure!(
        q::name(source, node)
            .execute(&mut **tx)
            .await?
            .rows_affected()
            == 1,
        Error::OperationAborted
    );
    let task = Uuid::now_v7();
    let bucket = queries::quota_stream(stream)
        .fetch_one(&mut **tx)
        .await?
        .bucket_id;
    let detail = serde_json::json!({"operation":"cache","source_id":source,"source_node":node,"storage_id":storage});
    queries::insert_task(task, "upload", Some(bucket), detail, None)
        .execute(&mut **tx)
        .await?;
    q::target(task, node).execute(&mut **tx).await?;
    Ok(())
}
pub(super) async fn offer(
    tx: &mut registry::Tx<'_>,
    node: &NodeState,
    task: Execution,
    detail: &serde_json::Value,
    key: &Key,
) -> Result<Option<(work::Plan, Vec<u8>)>> {
    let source: Id = serde_json::from_value(detail["source_id"].clone())?;
    ensure!(
        serde_json::from_value::<Uuid>(detail["source_node"].clone())? == node.node_id,
        Error::NodeAuthentication
    );
    let owner = q::owner(source).fetch_optional(&mut **tx).await?;
    if owner.is_none() {
        queries::finish_task(task, "completed", None)
            .execute(&mut **tx)
            .await?;
        return Ok(None);
    }
    ensure!(owner == Some(Some(node.node_id)), Error::NodeAuthentication);
    if queries::raw_upload_abandon(source)
        .execute(&mut **tx)
        .await?
        .rows_affected()
        == 1
    {
        queries::raw_upload_delete(source)
            .execute(&mut **tx)
            .await?;
        queries::finish_task(task, "completed", None)
            .execute(&mut **tx)
            .await?;
        return Ok(None);
    }
    queries::raw_upload_claim(source, task.token)
        .execute(&mut **tx)
        .await?;
    let current = queries::raw_upload_record(source)
        .fetch_one(&mut **tx)
        .await?;
    current.source.validate()?;
    ensure!(
        matches!(
            (
                current.source.medium.as_str(),
                current.source.pending_upload
            ),
            ("local", true) | ("shared_s3", false)
        ),
        Error::SourceUnavailable
    );
    if let Some(hash) = current.content_hash.as_deref() {
        let streams = queries::raw_upload_streams(source)
            .fetch_all(&mut **tx)
            .await?;
        if transactions::reuse_raw_upload_in(tx, &current, hash, &streams).await? {
            let state = if q::exists(source).fetch_one(&mut **tx).await? {
                "queued"
            } else {
                "completed"
            };
            queries::finish_task(task, state, None)
                .execute(&mut **tx)
                .await?;
            return Ok(None);
        }
    }
    if !current.source.pending_upload {
        transactions::complete_raw_upload_in(tx, &current, current.content_hash.as_deref()).await?;
        let state = if q::exists(source).fetch_one(&mut **tx).await? {
            "queued"
        } else {
            "completed"
        };
        queries::finish_task(task, state, None)
            .execute(&mut **tx)
            .await?;
        return Ok(None);
    }
    let storage = current.source.storage_id;
    let hashing = current.deduplicate && current.content_hash.is_none();
    let plan = work::Plan::UploadRaw {
        storage,
        items: vec![work::UploadFile {
            task: task.id,
            source: current.source,
            hashing,
        }],
    };
    let materials = work::Materials {
        storage: super::node_work::material(tx, storage, key).await?,
        additional: vec![],
        keys: vec![],
    };
    Ok(Some((plan, serde_json::to_vec(&materials)?)))
}
pub(super) async fn finish(
    tx: &mut registry::Tx<'_>,
    node: Uuid,
    task: Execution,
    item: &work::UploadFile,
    outcome: &work::Outcome,
) -> Result<(&'static str, Option<&'static str>)> {
    ensure!(
        item.task == task.id
            && q::owner(item.source.id).fetch_optional(&mut **tx).await? == Some(Some(node)),
        Error::NodeExpired
    );
    let current = queries::raw_upload_record(item.source.id)
        .fetch_one(&mut **tx)
        .await?;
    ensure!(
        current.token == Some(task.token) && current.source == item.source,
        Error::NodeExpired
    );
    match outcome {
        work::Outcome::Retry => {
            queries::raw_upload_retry(&current, Some("backend_io_failed"))
                .execute(&mut **tx)
                .await?;
            Ok(("queued", Some("backend_io_failed")))
        }
        work::Outcome::Hashed { hash } if item.hashing && hash.len() == 32 => {
            queries::raw_upload_hash(item.source.id, Some(hash))
                .execute(&mut **tx)
                .await?;
            Ok(("queued", None))
        }
        work::Outcome::Uploaded if !item.hashing => {
            transactions::complete_raw_upload_in(tx, &current, current.content_hash.as_deref())
                .await?;
            Ok((
                if q::exists(item.source.id).fetch_one(&mut **tx).await? {
                    "queued"
                } else {
                    "completed"
                },
                None,
            ))
        }
        _ => Err(Error::NodeOperationConflict.into()),
    }
}
pub(super) async fn inventory(
    b: &Backend,
    node: &NodeState,
    key: &Key,
    input: Inventory,
) -> Result<Vec<File>> {
    use std::collections::HashSet;
    ensure!(
        matches!(node.role.as_str(), "data" | "node"),
        Error::NodeAuthentication
    );
    ensure!(
        input.files.len() <= 128
            && input.files.iter().all(|i| !i.is_nil())
            && input.files.iter().collect::<HashSet<_>>().len() == input.files.len(),
        Error::NodeReadLimit
    );
    let mut tx = registry::begin(b).await?;
    node_registry::require_work(b, &mut tx, node, key, input.run).await?;
    let mut files = Vec::with_capacity(input.files.len());
    for id in input.files {
        let (storage, size, pending, protected, cached, limit, entries) =
            q::snapshot(id, node.node_id).fetch_one(&mut *tx).await?;
        files.push(File {
            id,
            storage,
            size: u64::try_from(size)?,
            cache_limit: limit,
            cache_entries: entries.map(u32::try_from).transpose()?,
            state: if pending {
                FileState::Pending
            } else if protected {
                FileState::Protected
            } else if cached {
                FileState::Cached
            } else {
                FileState::Removable
            },
        });
    }
    tx.commit().await?;
    Ok(files)
}
