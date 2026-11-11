use super::super::super::mirroring_transactions::assignment;
use super::*;
use crate::model::mirroring as mirror;
use queries::mirroring as m;

pub(crate) async fn progress(
    backend: &Backend,
    node: &NodeState,
    key: &Key,
    run: Run,
    operation: Uuid,
    bytes: u64,
    publishing: bool,
) -> Result<()> {
    let mut tx = backend.pool.begin().await?;
    backend.fence_control(&mut tx).await?;
    let (plan, task) = checkpoint_owner(&mut tx, node, key, run, operation).await?;
    ensure!(
        matches!(plan, work::Plan::Mirror {task:id} if id == task.id),
        Error::NodeOperationConflict
    );
    super::super::super::mirroring_transactions::progress(&mut tx, task, bytes, publishing).await?;
    tx.commit().await?;
    Ok(())
}

impl Backend {
    pub(in super::super::super) async fn claim_mirror(
        &self,
        node: &NodeState,
        key: &Key,
        run: Run,
        id: Uuid,
    ) -> Result<Option<Vec<u8>>> {
        let mut tx = self.pool.begin().await?;
        self.fence_control(&mut tx).await?;
        authenticate_rpc_node(&mut tx, node, key, false).await?;
        require_node_run(&mut tx, node, run, true).await?;
        ensure!(
            matches!(node.role.as_str(), "data" | "node"),
            Error::NodeInactive
        );
        ensure!(
            q::writable().fetch_one(&mut *tx).await?,
            Error::OperationAborted
        );
        ensure!(
            q::active(node.node_id).fetch_one(&mut *tx).await? < 64,
            Error::NodeWorkLimit
        );
        let token = Uuid::new_v4();
        if m::claim_task(id, node.node_id, token)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 0
        {
            return Ok(None);
        }
        let assignment = assignment(&mut tx, Execution { id, token }, key).await?;
        let record = queries::task_record(id).fetch_one(&mut *tx).await?;
        let profile =
            queries::storage_profile(serde_json::from_value(record.detail["storage_id"].clone())?)
                .fetch_one(&mut *tx)
                .await?;
        let maintenance = Uuid::new_v4();
        ensure!(
            permits(
                &mut tx,
                node.node_id,
                token,
                maintenance,
                &profile,
                "migration",
                true
            )
            .await?,
            Error::NodeWriteBusy
        );
        let input = work::Exchange {
            run,
            operation: id,
            completion: None,
            class: 2,
            max_items: 1,
            cached: vec![],
        };
        let plan = work::Plan::Mirror { task: id };
        let encrypted = codec::protect(&serde_json::to_vec(&assignment)?, key, token.as_bytes())?;
        q::insert(
            node,
            &input,
            &input.hash()?,
            Some(id),
            token,
            maintenance,
            Some(&plan),
            Some(&encrypted),
        )
        .execute(&mut *tx)
        .await?;
        let result = serde_json::to_vec(&mirror::Lease {
            operation: id,
            token,
            valid_for_ms: 60_000,
            assignment,
        })?;
        tx.commit().await?;
        Ok(Some(result))
    }
}

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
    if let Some(observed) = &input.observed {
        observed.validate()?;
        ensure!(
            input.size == Some(observed.size as i64),
            Error::NodeOperationConflict
        );
    }
    let observed = input.observed;
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
    let hash = crate::model::node_chunk::hash(&(&input.intent, selected, &observed))?;
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
    let mut row = queries::task_record(task.id).fetch_one(&mut *tx).await?;
    row.detail["source_size"] = serde_json::to_value(input.intent.size)?;
    row.detail["origin"] = serde_json::to_value(&observed)?;
    row.detail["phase"] = "reading".into();
    queries::work_sweep::task_progress(task, "", 0, row.detail.clone())
        .execute(&mut *tx)
        .await?;
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
