use super::super::super::mirroring_transactions::assignment;
use super::*;
use crate::model::mirroring as mirror;
use queries::mirroring as m;

pub(crate) async fn begin(
    backend: &Backend,
    node: &NodeState,
    key: &Key,
    run: Run,
    work: Uuid,
    operation: Uuid,
    input: mirror::Begin,
    cached: Vec<read::Version>,
) -> Result<write::Reply> {
    let mut tx = backend.pool.begin().await?;
    backend.fence_control(&mut tx).await?;
    let (plan, task) = checkpoint_owner(&mut tx, node, key, run, work).await?;
    ensure!(
        matches!(plan, work::Plan::Mirror { task:id } if id==task.id),
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
    m::source(task.id, input.source).execute(&mut *tx).await?;
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
    if let Some(previous) = queries::node_write(node.node_id, operation)
        .fetch_optional(&mut *tx)
        .await?
    {
        ensure!(
            previous.request_hash == hash
                && previous.owner_task_id == Some(task.id)
                && previous.boot_id == run.boot
                && previous.fence == run.fence,
            Error::NodeOperationConflict
        );
        return node_write_reply(&mut tx, node, key, run, previous, Some(&input.cached)).await;
    }
    ensure!(
        queries::node_write_count(node.node_id)
            .fetch_one(&mut *tx)
            .await?
            < write::MAX_ACTIVE,
        Error::NodeWriteLimit
    );
    let row = queries::task_record(task.id).fetch_one(&mut *tx).await?;
    let storage = serde_json::from_value(row.detail["storage_id"].clone())?;
    let (medium, block) = queries::node_write_storage(storage)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(medium == "s3", Error::SourceUnavailable);
    let mut profile = queries::storage_profile(storage)
        .fetch_one(&mut *tx)
        .await?;
    profile.upload_cache = false;
    profile.write_processing = "inline".into();
    profile.write_durability = None;
    let stream = create_stream_in(
        &mut tx,
        StreamCreate {
            authority: Permit::system(),
            bucket: assignment.bucket,
            key: assignment.key,
            purpose: crate::model::StreamPurpose::Mirror,
            metadata: input.intent.metadata.clone(),
            public: input.intent.public,
            completion: None,
            owner: Some((task.id, task.token)),
        },
        Some(write::LEASE_SECONDS),
    )
    .await?;
    queries::reserve_quota(stream, input.intent.size.unwrap_or(0), false, None, None)
        .fetch_one(&mut *tx)
        .await?;
    queries::insert_node_write(node, &input, &hash, stream, &profile, block, None, None)
        .execute(&mut *tx)
        .await?;
    let row = queries::node_write(node.node_id, operation)
        .fetch_one(&mut *tx)
        .await?;
    let reply = node_write_reply(&mut tx, node, key, run, row, Some(&input.cached)).await?;
    tx.commit().await?;
    Ok(reply)
}
