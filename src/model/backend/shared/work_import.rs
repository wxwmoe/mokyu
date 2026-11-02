use super::{
    node_writes::Write,
    queries::{self, node_registry::Tx, node_writes as ledger, replication_import as i},
    transactions,
};
use crate::model::{
    node_write as write,
    replication::external::{Phase, Plan},
};
use crate::{
    cluster::NodeState,
    model::{Error, session::Run, task::Execution},
};
use anyhow::{Result, ensure};
use uuid::Uuid;

pub(super) async fn prepare(
    db: &mut Tx<'_>,
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
    let record = queries::work::storage(storage).fetch_one(&mut **db).await?;
    let block_key = record.block_key_id;
    if let Some(key) = block_key {
        queries::work::block_key(key).fetch_one(&mut **db).await?;
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
    ledger::insert(
        node,
        &input,
        &input.intent.hash()?,
        incoming.stream,
        plan.options.target_bucket,
        storage,
        block_key,
        &profile,
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

pub(super) async fn seal(db: &mut Tx<'_>, row: &Write, input: &write::Publish) -> Result<()> {
    let task = Execution {
        id: row.owner_task_id.ok_or(Error::NodeOperationConflict)?,
        token: queries::task_record(row.owner_task_id.ok_or(Error::NodeOperationConflict)?)
            .fetch_one(&mut **db)
            .await?
            .execution_token
            .ok_or(Error::NodeExpired)?,
    };
    let run = transactions::replication::owned(db, task).await?;
    ensure!(
        run.options.0.source_external && !run.options.0.target_external && run.phase == "verify",
        Error::NodeOperationConflict
    );
    let stream = row.stream_id.ok_or(Error::NodeOperationConflict)?;
    let item = i::owner(task.id, stream, row.operation_id)
        .fetch_one(&mut **db)
        .await?;
    let state = queries::replication_remote::state(item)
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
