use super::{
    queries::{self, node_registry::Tx},
    transactions::{replication, replication_remote, transfer},
    work_import,
    work_media::transfer as work_transfer,
};
use crate::{
    cluster::NodeState,
    content::codec::Key,
    model::{Error, node_work as work, replication::Step, session::Run, task::Execution},
};
use anyhow::{Result, ensure};

async fn schedule(tx: &mut Tx<'_>, task: Execution, reason: Option<&str>) -> Result<()> {
    let record = queries::task_record(task.id).fetch_one(&mut **tx).await?;
    if record.execution_token == Some(task.token) {
        let state = if record.state == "running" {
            "queued"
        } else {
            &record.state
        };
        queries::finish_task(task, state, reason)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

pub(super) async fn offer(
    tx: &mut Tx<'_>,
    node: &NodeState,
    session: Run,
    task: Execution,
    key: &Key,
) -> Result<Option<(work::Plan, Vec<u8>)>> {
    let mut step = sqlx::Acquire::begin(&mut **tx).await?;
    match prepare(&mut step, node, session, task, key).await {
        Ok(offer) => {
            step.commit().await?;
            Ok(offer)
        }
        Err(error) => {
            step.rollback().await?;
            if crate::model::error(&error) == Some(Error::RedundancyBacklog) {
                schedule(tx, task, None).await?;
                return Ok(None);
            }
            reject(tx, task, error).await?;
            Ok(None)
        }
    }
}

async fn reject(tx: &mut Tx<'_>, task: Execution, error: anyhow::Error) -> Result<()> {
    let Some(reason) = crate::model::replication::worker_error(&error) else {
        return Err(error);
    };
    super::replica_transactions::failed(tx, task.id, reason).await?;
    queries::finish_task(task, "failed", Some(reason))
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn prepare(
    tx: &mut Tx<'_>,
    node: &NodeState,
    session: Run,
    task: Execution,
    key: &Key,
) -> Result<Option<(work::Plan, Vec<u8>)>> {
    let mut run = replication::owned(tx, task).await?;
    // External transfers can advance several metadata steps before the next S3 request.
    let mut remaining = 4;
    let step = loop {
        let step = replication::step_in(tx, task, &mut run).await?;
        replication::save(tx, &run).await?;
        remaining -= 1;
        if remaining == 0
            || !matches!(step, Step::Continue)
            || !run.options.0.external()
            || matches!(run.phase.as_str(), "complete" | "cancelled")
            || queries::task_record(task.id)
                .fetch_one(&mut **tx)
                .await?
                .execution_token
                != Some(task.token)
        {
            break step;
        }
    };
    let (plan, storage, source, blocks) = match step {
        Step::Continue => {
            schedule(tx, task, None).await?;
            return Ok(None);
        }
        Step::External {
            mut plan,
            connections,
        } => {
            if !run.options.0.target_external {
                let profile = queries::storage_profile(run.storage()?)
                    .fetch_one(&mut **tx)
                    .await?;
                super::node_work::profile_permits(
                    tx,
                    task.token,
                    &profile,
                    ("migration", profile.migration_concurrency),
                )
                .await?;
            }
            work_import::prepare(tx, node, session, task, &mut plan).await?;
            let stores = connections
                .into_iter()
                .map(|row| row.open(task.id, key))
                .collect::<Result<Vec<_>>>()?;
            return Ok(Some((
                work::Plan::ExternalTransfer(plan),
                serde_json::to_vec(&stores)?,
            )));
        }
        Step::Verify { storage, sources } => {
            let items = sources
                .into_iter()
                .map(|source| work::RawHash { source, hash: None })
                .collect();
            (
                work::Plan::HashRaw { storage, items },
                storage,
                None,
                vec![],
            )
        }
        Step::Scan(scan) => {
            let storage = scan.source.storage_id;
            (work::Plan::ScanTransfer(scan), storage, None, vec![])
        }
        Step::Raw(copy) => {
            copy.validate()?;
            let storage = copy.target.storage_id;
            let source = copy.input.storage()?;
            let block = match &copy.input {
                crate::model::transfer::Input::Chunk { chunk } => chunk.block_key_id,
                crate::model::transfer::Input::Pack { pack, .. } => pack.block_key_id,
                crate::model::transfer::Input::Raw { .. } => None,
            };
            let keys =
                queries::pack_work::keys(&mut **tx, &block.into_iter().collect::<Vec<_>>(), key)
                    .await?;
            (
                work::Plan::RawTransfer {
                    storage,
                    transfer: Box::new(copy),
                    source_permits: Vec::new(),
                },
                storage,
                Some(source),
                keys,
            )
        }
        Step::Copy(copy) => {
            let storage = copy.storage;
            let source = copy.input.storage()?;
            let Some(p) = work_transfer::prepare(tx, task, &copy, key).await? else {
                schedule(tx, task, None).await?;
                return Ok(None);
            };
            (
                work::Plan::CopyTransfer {
                    storage,
                    transfer: Box::new(p.work),
                    batch: Box::new(p.batch),
                    source_permits: Vec::new(),
                },
                storage,
                Some(source),
                p.keys,
            )
        }
    };
    ensure!(
        serde_json::to_vec(&plan)?.len() <= 3 * 1024 * 1024,
        Error::NodeWorkLimit
    );
    let storage = super::node_work::material(tx, storage, key).await?;
    let mut additional = Vec::new();
    if let Some(source) = source.filter(|id| *id != storage.profile.storage_id) {
        additional.push(super::node_work::material(tx, source, key).await?)
    }
    Ok(Some((
        plan,
        serde_json::to_vec(&work::Materials {
            storage,
            additional,
            keys: blocks,
        })?,
    )))
}

pub(super) async fn finish(
    tx: &mut Tx<'_>,
    task: Execution,
    plan: &work::Plan,
    outcomes: &[work::Outcome],
    key: &Key,
) -> Result<()> {
    let mut publication = sqlx::Acquire::begin(&mut **tx).await?;
    match publish(&mut publication, task, plan, outcomes, key).await {
        Ok(()) => {
            publication.commit().await?;
            Ok(())
        }
        Err(error) => {
            publication.rollback().await?;
            reject(tx, task, error).await
        }
    }
}

async fn publish(
    tx: &mut Tx<'_>,
    task: Execution,
    plan: &work::Plan,
    outcomes: &[work::Outcome],
    key: &Key,
) -> Result<()> {
    let [outcome] = outcomes else {
        anyhow::bail!(Error::NodeOperationConflict)
    };
    if matches!(outcome, work::Outcome::Retry) {
        return schedule(tx, task, Some("backend_io_failed")).await;
    }
    let mut run = replication::owned(tx, task).await?;
    match (plan, outcome) {
        (work::Plan::ExternalTransfer(plan), work::Outcome::ExternalTransfer(out)) => {
            replication_remote::complete(tx, task, &mut run, plan, out).await?;
            replication::save(tx, &run).await?;
        }
        (work::Plan::ScanTransfer(plan), work::Outcome::Scanned(value)) => {
            plan.validate(value)?;
            transfer::scanned(tx, &run, value).await?;
        }
        (work::Plan::RawTransfer { transfer: plan, .. }, work::Outcome::Transferred { digest }) => {
            transfer::raw_progress(
                tx,
                task,
                &run,
                plan,
                &crate::model::transfer::RawProgress::Finished {
                    digest: digest.clone(),
                },
            )
            .await?;
        }
        (
            work::Plan::CopyTransfer {
                storage,
                transfer: copy,
                batch,
                ..
            },
            work::Outcome::Packed(result),
        ) => {
            work_transfer::finish(tx, task, *storage, copy, batch, result, key).await?;
        }
        (work::Plan::HashRaw { items, .. }, work::Outcome::Hashed { hash })
            if items.len() == 1 && items[0].hash.is_none() && hash.len() == 32 =>
        {
            replication::verified(tx, task, std::slice::from_ref(&items[0].source)).await?;
        }
        _ => anyhow::bail!(Error::NodeOperationConflict),
    }
    schedule(tx, task, None).await
}
