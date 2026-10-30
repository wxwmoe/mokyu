use super::super::super::replication_import_queries as i;
use super::*;
use crate::model::{
    node_write as write,
    replication::external::{Phase, Plan},
};

pub(super) async fn prepare(
    db: &mut Transaction<'_, Postgres>,
    node: &NodeState,
    run: Run,
    task: Execution,
    plan: &mut Plan,
) -> Result<()> {
    let Some(incoming) = plan.incoming.as_ref() else {
        return Ok(());
    };
    incoming.validate(plan)?;
    let storage = incoming.storage;
    let mut profile = queries::storage_profile(storage)
        .fetch_one(&mut **db)
        .await?;
    let (medium, block_key) = queries::node_write_storage(storage)
        .fetch_one(&mut **db)
        .await?;
    ensure!(medium == "s3", Error::SourceUnavailable);
    if let Some(id) = block_key {
        queries::lock_managed_key(id)
            .fetch_optional(&mut **db)
            .await?
            .ok_or(Error::SourceUnavailable)?;
        ensure!(
            queries::valid_storage_key(id).fetch_one(&mut **db).await?,
            Error::SourceUnavailable
        );
    }
    profile.upload_cache = false;
    profile.write_processing = "inline".into();
    profile.write_durability = None;
    let operation = Uuid::now_v7();
    let input = write::Begin {
        run,
        operation,
        cached: vec![],
        intent: write::Intent {
            access: "node-replication".into(),
            credential_revision: 0,
            bucket: plan.options.target_bucket.to_string(),
            key: plan.target()?,
            metadata: crate::model::replication::import::metadata(
                plan.state
                    .source
                    .as_ref()
                    .ok_or(Error::NodeOperationConflict)?,
            )?,
            public: false,
            size: Some(i64::try_from(incoming.size)?),
            copy_read: None,
            part: None,
            completion: None,
        },
    };
    ensure!(plan.task == task.id, Error::NodeOperationConflict);
    queries::insert_node_write(
        node,
        &input,
        &input.intent.hash()?,
        incoming.stream,
        &profile,
        block_key,
        None,
        None,
    )
    .execute(&mut **db)
    .await?;
    i::part(
        plan.item.ok_or(Error::NodeOperationConflict)?,
        Some(incoming.stream),
        Some(node.node_id),
        Some(operation),
    )
    .execute(&mut **db)
    .await?;
    plan.incoming.as_mut().unwrap().operation = Some(operation);
    plan.validate()?;
    Ok(())
}

pub(super) async fn seal(
    db: &mut Transaction<'_, Postgres>,
    row: &queries::NodeWrite,
    input: &write::Publish,
) -> Result<()> {
    let task = Execution {
        id: row.owner_task_id.ok_or(Error::NodeOperationConflict)?,
        token: row.owner_lease_token.ok_or(Error::NodeOperationConflict)?,
    };
    let run = super::super::super::replication_transactions::owned(db, task).await?;
    ensure!(
        run.options.0.source_external && !run.options.0.target_external && run.phase == "verify",
        Error::NodeOperationConflict
    );
    let stream = row.stream_id.ok_or(Error::NodeOperationConflict)?.into();
    let item = i::owner(task.id, stream, row.operation_id)
        .fetch_one(&mut **db)
        .await?;
    let state = super::super::super::replication_remote_queries::state(item)
        .fetch_one(&mut **db)
        .await?
        .0;
    ensure!(
        state.phase == Phase::Import && input.if_match.is_none() && input.if_none.is_none(),
        Error::NodeOperationConflict
    );
    let stored = i::stream(stream).fetch_one(&mut **db).await?;
    let receipt =
        crate::model::replication::import::receipt(i32::from(state.position) + 1, &stored)?;
    receipt.validate(&state)?;
    ensure!(
        stored.size == input.size
            && stored.etag == input.etag
            && stored.checksums == serde_json::to_value(&input.checksums)?,
        Error::NodeOperationConflict
    );
    Ok(())
}
