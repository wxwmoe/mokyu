use super::{
    Backend, node_registry,
    queries::{self, node_registry as registry, work as q},
    transactions::external,
};
use crate::{
    cluster::NodeState,
    content::codec::{self, Key},
    model::{
        Error,
        cluster::NodeWork,
        node_read::{GrantResult, GrantType, KeepAlive},
        node_work as work,
        session::Run,
        task::Execution,
    },
};
use anyhow::{Result, ensure};
use sqlx::types::Json;
use uuid::Uuid;

#[derive(sqlx::FromRow)]
pub(super) struct Offer {
    pub(super) boot_id: Uuid,
    pub(super) fence: Uuid,
    pub(super) request_hash: Vec<u8>,
    pub(super) task_id: Option<Uuid>,
    pub(super) token: Uuid,
    pub(super) plan: Option<Json<work::Plan>>,
    pub(super) materials_encrypted: Option<Vec<u8>>,
    pub(super) result_hash: Option<Vec<u8>>,
    pub(super) remaining: i64,
}

impl Offer {
    fn verify(&self, run: Run) -> Result<()> {
        ensure!(
            self.boot_id == run.boot && self.fence == run.fence,
            Error::NodeExpired
        );
        Ok(())
    }

    fn reply(
        self,
        node: &NodeState,
        key: &Key,
        input: &work::Exchange,
        hash: &[u8],
    ) -> Result<work::Reply> {
        self.verify(input.run)?;
        ensure!(self.request_hash == hash, Error::NodeOperationConflict);
        if self.plan.is_some() {
            ensure!(
                self.result_hash.is_none() && self.remaining > 0,
                Error::NodeExpired
            );
        }
        work::materialize(
            node,
            key,
            input,
            self.plan.map(|p| p.0),
            self.materials_encrypted.as_deref(),
            self.token,
            self.remaining,
        )
    }
}

async fn material(
    tx: &mut registry::Tx<'_>,
    storage: Uuid,
    key: &Key,
) -> Result<crate::model::node_read::Storage> {
    let record = q::storage(storage).fetch_one(&mut **tx).await?;
    let profile = queries::storage_profile(storage)
        .fetch_one(&mut **tx)
        .await?;
    let connection = crate::storage::model::Connection::unprotect(&(&record).into(), key)?;
    Ok(crate::model::node_read::Storage {
        connection,
        revision: profile.revision,
        profile,
    })
}

async fn tier_plan(
    tx: &mut registry::Tx<'_>,
    task: Execution,
    detail: &serde_json::Value,
    key: &Key,
) -> Result<Option<(work::Plan, Vec<u8>)>> {
    use anyhow::Context as _;
    let stale = detail["stale_seconds"]
        .as_i64()
        .context("tier coverage is missing")?;
    let step = super::tier_move_transactions::step(tx, task, stale, None).await?;
    let Some(work) = step.work else {
        queries::finish_task(task, "queued", None)
            .execute(&mut **tx)
            .await?;
        return Ok(None);
    };
    let storage: Uuid = serde_json::from_value(detail["storage_id"].clone())?;
    let mut ids = std::collections::BTreeSet::new();
    let mut keys = std::collections::BTreeSet::new();
    for source in std::iter::once(work.source()).chain(work.target()) {
        ids.insert(source.storage()?);
        keys.extend(source.block_key());
    }
    let mut additional = Vec::new();
    for id in ids.into_iter().filter(|id| *id != storage) {
        additional.push(material(tx, id, key).await?);
    }
    let mut blocks = Vec::new();
    for id in keys {
        blocks.push(crate::model::node_read::BlockKey::unprotect(
            q::block_key(id).fetch_one(&mut **tx).await?,
            key,
        )?);
    }
    let materials = work::Materials {
        storage: material(tx, storage, key).await?,
        additional,
        keys: blocks,
    };
    Ok(Some((
        work::Plan::Tier {
            storage,
            work: Box::new(work),
            source_permits: [Uuid::new_v4(), Uuid::new_v4()],
        },
        serde_json::to_vec(&materials)?,
    )))
}

async fn finish(
    tx: &mut registry::Tx<'_>,
    node: Uuid,
    run: Run,
    result: &work::Completion,
) -> Result<()> {
    let hash = crate::model::node_chunk::hash(&result.outcomes)?;
    let offer = q::offer(node, result.operation)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or(Error::NodeOperationMissing)?;
    offer.verify(run)?;
    if let Some(saved) = offer.result_hash {
        ensure!(saved == hash, Error::NodeOperationConflict);
        return Ok(());
    }
    ensure!(offer.remaining > 0, Error::NodeExpired);
    let task = Execution {
        id: offer.task_id.ok_or(Error::NodeOperationConflict)?,
        token: offer.token,
    };
    let mut issue = None;
    let state = match (offer.plan.map(|p| p.0), result.outcomes.as_slice()) {
        (Some(work::Plan::ExternalTest(plan)), [work::Outcome::ExternalTest(report)]) => {
            external::finish_test(tx, task, &plan, report).await?;
            if report.phase == crate::model::external_test::Phase::Done {
                "completed"
            } else if report.blocked.is_some() {
                "failed"
            } else {
                "queued"
            }
        }
        (Some(work::Plan::Tier { work: planned, .. }), [work::Outcome::Tier(outcome)]) => {
            use crate::model::tiering::{Outcome, Work};
            ensure!(
                matches!(
                    (&*planned, &**outcome),
                    (
                        Work::Copy { .. },
                        Outcome::Verified { .. } | Outcome::Checkpoint { .. } | Outcome::Discard
                    ) | (Work::Remove { .. }, Outcome::Removed)
                ),
                Error::NodeOperationConflict
            );
            super::tier_move_transactions::finish(tx, task, outcome).await?;
            if matches!(&**outcome, Outcome::Removed) {
                "completed"
            } else {
                "queued"
            }
        }
        (Some(work::Plan::Tier { .. }), [work::Outcome::Retry]) => {
            super::tier_move_transactions::finish(tx, task, &crate::model::tiering::Outcome::Retry)
                .await?;
            "queued"
        }
        (
            Some(work::Plan::Inspect {
                snapshot, inspect, ..
            }),
            outcomes,
        ) => {
            let row = queries::task_record(task.id).fetch_one(&mut **tx).await?;
            ensure!(
                row.state == "running" && row.execution_token == Some(task.token),
                Error::OperationAborted
            );
            issue = super::work_sources::finish(tx, task, row.detail, *snapshot, inspect, outcomes)
                .await?;
            if issue.is_some() {
                "failed"
            } else {
                "completed"
            }
        }
        _ => anyhow::bail!(Error::NodeOperationConflict),
    };
    ensure!(
        queries::finish_task(task, state, issue)
            .execute(&mut **tx)
            .await?
            .rows_affected()
            == 1,
        Error::OperationAborted
    );
    q::settle(node, result.operation, &hash)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

#[async_trait::async_trait]
impl NodeWork for Backend {
    async fn recover_work(&self) -> Result<u64> {
        let mut tx = registry::begin(self).await?;
        let count = q::requeue().execute(&mut *tx).await?.rows_affected();
        q::prune().execute(&mut *tx).await?;
        super::node_writes::expire(&mut tx).await?;
        super::queries::node_receipts::prune()
            .execute(&mut *tx)
            .await?;
        super::queries::read_grants::expire(&mut tx).await?;
        tx.commit().await?;
        Ok(count)
    }

    async fn exchange_work(
        &self,
        node: &NodeState,
        key: &Key,
        input: work::Exchange,
    ) -> Result<work::Reply> {
        let digest = input.hash()?;
        let issued = crate::model::node_write::operation_time(input.operation)?;
        let now = chrono::Utc::now();
        ensure!(
            issued <= now + chrono::Duration::seconds(30)
                && issued >= now - chrono::Duration::minutes(15),
            Error::NodeOperationExpired
        );
        let mut tx = registry::begin(self).await?;
        let active = node_registry::require_work(self, &mut tx, node, key, input.run).await?;
        ensure!(
            matches!(node.role.as_str(), "worker" | "node"),
            Error::NodeAuthentication
        );
        if let Some(known) = q::offer(node.node_id, input.operation)
            .fetch_optional(&mut *tx)
            .await?
        {
            return known.reply(node, key, &input, &digest);
        }
        if let Some(result) = &input.completion {
            finish(&mut tx, node.node_id, input.run, result).await?;
        }
        let token = Uuid::new_v4();
        let next = if active && input.max_items > 0 {
            q::next_task(input.class).fetch_optional(&mut *tx).await?
        } else {
            None
        };
        let mut task_id = None;
        let mut plan = None;
        let mut encrypted = None;
        if let Some((task, detail)) = next {
            ensure!(
                queries::claim_task(task, token)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected()
                    == 1,
                Error::OperationAborted
            );
            let task = Execution { id: task, token };
            let prepared = match detail["operation"].as_str() {
                Some("external_test") => {
                    let assignment =
                        external::prepare_test(&mut tx, task, Some(node.node_id)).await?;
                    let connection = crate::model::external::Snapshot::open_test(
                        assignment.external_id,
                        assignment.revision,
                        &assignment.connection_encrypted,
                        key,
                    )?;
                    Some((
                        work::Plan::ExternalTest(assignment.plan),
                        serde_json::to_vec(&connection)?,
                    ))
                }
                Some("tiering") => tier_plan(&mut tx, task, &detail, key).await?,
                _ if detail.get("source_operation").is_some() => {
                    let store = serde_json::from_value(detail["storage_id"].clone())?;
                    let storage = material(&mut tx, store, key).await?;
                    match super::work_sources::prepare(
                        self,
                        &mut tx,
                        node,
                        task,
                        &detail,
                        &storage.profile,
                        key,
                    )
                    .await?
                    {
                        Some((plan, keys)) => Some((
                            plan,
                            serde_json::to_vec(&work::Materials {
                                storage,
                                additional: Vec::new(),
                                keys,
                            })?,
                        )),
                        None => {
                            queries::finish_task(task, "queued", None)
                                .execute(&mut *tx)
                                .await?;
                            None
                        }
                    }
                }
                _ => anyhow::bail!(Error::NodeOperationConflict),
            };
            if let Some((proposal, secret)) = prepared {
                encrypted = Some(codec::protect(&secret, key, token.as_bytes())?);
                plan = Some(proposal);
                task_id = Some(task.id);
            }
        }
        q::insert(
            node.node_id,
            &input,
            &digest,
            task_id,
            token,
            plan.as_ref(),
            encrypted.as_deref(),
        )
        .execute(&mut *tx)
        .await?;
        let offer = q::offer(node.node_id, input.operation)
            .fetch_one(&mut *tx)
            .await?;
        let reply = offer.reply(node, key, &input, &digest)?;
        tx.commit().await?;
        Ok(reply)
    }
}

pub(super) async fn grants(
    tx: &mut registry::Tx<'_>,
    node: Uuid,
    run: Run,
    resources: KeepAlive,
) -> Result<Vec<GrantResult>> {
    let mut results = Vec::with_capacity(resources.renew.len() + resources.release.len());
    for (items, renew) in [(resources.renew, true), (resources.release, false)] {
        for grant in items {
            let duration = if grant.r#type == GrantType::Work {
                let held = q::touch(node, run, grant.id, renew)
                    .execute(&mut **tx)
                    .await?
                    .rows_affected()
                    == 1;
                held.then_some(90_000)
            } else if grant.r#type == GrantType::Write || grant.r#type == GrantType::StorageWrite {
                let statement = if grant.r#type == GrantType::Write {
                    queries::node_writes::touch(node, run, grant.id, renew)
                } else {
                    queries::node_writes::touch_permit(node, run, grant.id, renew)
                };
                let held = statement.execute(&mut **tx).await?.rows_affected() > 0;
                held.then_some(if grant.r#type == GrantType::Write {
                    90_000
                } else {
                    60_000
                })
            } else {
                super::queries::read_grants::touch_read(tx, node, run, grant, renew).await?
            };
            let accepted = !renew || duration.is_some();
            let valid_for_ms = if renew { duration.unwrap_or(0) } else { 0 };
            results.push(GrantResult {
                grant,
                accepted,
                valid_for_ms,
            });
        }
    }
    Ok(results)
}
