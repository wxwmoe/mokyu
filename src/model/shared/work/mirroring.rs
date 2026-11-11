use super::{
    Backend, node_work, node_writes,
    queries::{self, mirroring as q, node_registry::Tx, node_writes as ledger},
    transactions,
};
use crate::{
    authorization::Permit,
    cluster::NodeState,
    content::codec::Key,
    model::{
        Error, StreamCreate, StreamPurpose, mirroring as mirror, node_work as work,
        node_write as write, session::Run, task::Execution,
    },
};
use anyhow::{Result, ensure};
use uuid::Uuid;

use super::mirroring_transactions::assignment;

pub(super) async fn offer(
    tx: &mut Tx<'_>,
    task: Execution,
    key: &Key,
) -> Result<Option<(work::Plan, Vec<u8>)>> {
    let assignment = match assignment(tx, task, key).await {
        Ok(value) => value,
        Err(error) if error.downcast_ref::<s3s::S3Error>().is_some() || error.is::<Error>() => {
            queries::finish_task(task, "failed", Some("origin_authorization_changed"))
                .execute(&mut **tx)
                .await?;
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    let row = queries::task_record(task.id).fetch_one(&mut **tx).await?;
    let storage = serde_json::from_value(row.detail["storage_id"].clone())?;
    let profile = queries::storage_profile(storage)
        .fetch_one(&mut **tx)
        .await?;
    node_work::profile_permits(
        tx,
        task.token,
        &profile,
        ("migration", profile.migration_concurrency),
    )
    .await?;
    Ok(Some((
        work::Plan::Mirror { task: task.id },
        serde_json::to_vec(&assignment)?,
    )))
}

pub(super) async fn begin(
    backend: &Backend,
    node: &NodeState,
    key: &Key,
    run: Run,
    work: Uuid,
    operation: Uuid,
    input: mirror::Begin,
    cached: Vec<crate::model::node_read::Version>,
) -> Result<write::Reply> {
    let mut tx = node_writes::start(backend, node, key, run).await?;
    let (plan, task) = node_work::checkpoint(&mut tx, node, run, work).await?;
    ensure!(
        matches!(plan, work::Plan::Mirror { task: id } if id==task.id),
        Error::NodeOperationConflict
    );
    let assignment = assignment(&mut tx, task, key).await?;
    ensure!(
        input.source.is_none_or(|(id, revision)| assignment
            .connections
            .iter()
            .any(|s| s.external_id == id && s.revision == revision)),
        Error::NodeOperationConflict
    );
    ensure!(
        input.source.is_some()
            || std::iter::once(&assignment.rule.source)
                .chain(assignment.rule.backup.iter())
                .any(|s| matches!(s, mirror::Source::Http { .. })),
        Error::NodeOperationConflict
    );
    let selected = input.source;
    q::source(task.id, input.source).execute(&mut *tx).await?;
    transactions::mirroring::guard(&mut tx, task).await?;
    let input = write::Begin {
        run,
        operation,
        cached,
        intent: write::Intent {
            access: "node-mirror".into(),
            credential_revision: 0,
            bucket: assignment.bucket.to_string(),
            key: assignment.key.clone(),
            metadata: input.metadata,
            public: assignment.rule.public_read,
            size: input.size,
            copy_read: None,
            part: None,
            completion: None,
        },
    };
    input.intent.validate()?;
    let hash = crate::model::node_chunk::hash(&(&input.intent, selected))?;
    if let Some(previous) = ledger::owned(node.node_id, operation)
        .fetch_optional(&mut *tx)
        .await?
    {
        ensure!(
            previous.request_hash == hash && previous.owner_task_id == Some(task.id),
            Error::NodeOperationConflict
        );
        previous.owned(node, run)?;
        return previous
            .reply(&mut tx, node, key, run, Some(&input.cached))
            .await;
    }
    ensure!(
        ledger::count(node.node_id).fetch_one(&mut *tx).await? < write::MAX_ACTIVE,
        Error::NodeWriteLimit
    );
    let row = queries::task_record(task.id).fetch_one(&mut *tx).await?;
    let storage = serde_json::from_value(row.detail["storage_id"].clone())?;
    let target = queries::work::storage(storage).fetch_one(&mut *tx).await?;
    let mut profile = queries::storage_profile(storage)
        .fetch_one(&mut *tx)
        .await?;
    profile.upload_cache = false;
    profile.write_processing = "inline".into();
    profile.write_durability = None;
    let stream = transactions::create_stream_in(
        &mut tx,
        StreamCreate {
            authority: Permit::system(),
            bucket: assignment.bucket,
            key: assignment.key,
            purpose: StreamPurpose::Mirror,
            metadata: input.intent.metadata.clone(),
            public: input.intent.public,
            completion: None,
            owner: Some((task.id, task.token)),
        },
    )
    .await?;
    transactions::reserve_quota_in(&mut tx, stream, input.intent.size.unwrap_or(0), false, None)
        .await?;
    ledger::insert(
        node,
        &input,
        &hash,
        stream,
        assignment.bucket,
        storage,
        target.block_key_id,
        &profile,
        None,
    )
    .execute(&mut *tx)
    .await?;
    let row = node_writes::lookup(&mut tx, node.node_id, operation).await?;
    let reply = row
        .reply(&mut tx, node, key, run, Some(&input.cached))
        .await?;
    tx.commit().await?;
    Ok(reply)
}
