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

pub(super) async fn checkpoint(
    tx: &mut registry::Tx<'_>,
    node: &NodeState,
    run: Run,
    operation: Uuid,
) -> Result<(work::Plan, Execution)> {
    let row = q::offer(node.node_id, operation)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or(Error::NodeExpired)?;
    row.verify(run)?;
    ensure!(
        matches!(node.role.as_str(), "worker" | "node")
            || node.role == "data"
                && matches!(row.plan.as_deref(), Some(work::Plan::Mirror { .. })),
        Error::NodeAuthentication
    );
    ensure!(
        row.remaining > 0 && row.result_hash.is_none(),
        Error::NodeExpired
    );
    let task = Execution {
        id: row.task_id.ok_or(Error::NodeExpired)?,
        token: row.token,
    };
    let record = queries::task_record(task.id).fetch_one(&mut **tx).await?;
    ensure!(
        record.state == "running" && record.execution_token == Some(task.token),
        Error::NodeExpired
    );
    Ok((row.plan.ok_or(Error::NodeOperationConflict)?.0, task))
}

pub(super) async fn material(
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

async fn reserve_io(
    tx: &mut registry::Tx<'_>,
    token: Uuid,
    task_type: &str,
    plan: &work::Plan,
    secret: &[u8],
) -> Result<()> {
    if plan.storage().is_none() {
        return Ok(());
    }
    let materials: work::Materials = serde_json::from_slice(secret)?;
    let mut profiles = std::collections::BTreeMap::new();
    for storage in std::iter::once(materials.storage).chain(materials.additional) {
        profiles.insert(storage.profile.storage_id, storage.profile);
    }
    for (_, p) in profiles {
        let direction = match plan {
            _ if task_type == "storage_migration" => ("migration", p.migration_concurrency),
            work::Plan::UploadRaw { .. } => ("upload", p.upload_concurrency),
            work::Plan::Inspect { .. } | work::Plan::Audit { .. } => {
                ("integrity", Some(p.integrity_concurrency))
            }
            _ => ("maintenance", p.maintenance_concurrency),
        };
        profile_permits(tx, token, &p, direction).await?;
    }
    Ok(())
}

pub(super) async fn profile_permits(
    tx: &mut registry::Tx<'_>,
    token: Uuid,
    p: &crate::storage::profile::StorageProfile,
    direction: (&str, Option<i32>),
) -> Result<()> {
    let id = p.storage_id;
    for (scope, cap) in [("control", p.control_concurrency), direction] {
        if let Some(cap) = cap {
            queries::read_grants::lock_storage(&mut **tx, id, scope).await?;
            ensure!(
                q::lane_count(id, scope).fetch_one(&mut **tx).await? < i64::from(cap),
                Error::NodeWriteBusy
            );
            q::lane(id, scope, token).execute(&mut **tx).await?;
        }
    }
    Ok(())
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
        keys.extend(source.block_keys());
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
    key: &Key,
    wake_shared: &mut bool,
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
    let plan = offer.plan.ok_or(Error::NodeOperationConflict)?.0;
    if matches!(
        &plan,
        work::Plan::ExternalTransfer(_)
            | work::Plan::ScanTransfer(_)
            | work::Plan::RawTransfer { .. }
            | work::Plan::CopyTransfer { .. }
    ) || matches!(plan, work::Plan::HashRaw { .. })
        && queries::task_record(task.id)
            .fetch_one(&mut **tx)
            .await?
            .detail["operation"]
            == "replication"
    {
        super::work_replication::finish(tx, task, &plan, &result.outcomes, key).await?;
        if !result
            .outcomes
            .iter()
            .any(|outcome| matches!(outcome, work::Outcome::Retry))
        {
            *wake_shared = true;
        }
        return settle(tx, node, task, result.operation, &hash).await;
    }
    let mut issue = None;
    let state = match (Some(plan), result.outcomes.as_slice()) {
        (Some(work::Plan::Mirror { task: id }), [work::Outcome::Mirrored { size, missing }])
            if id == task.id =>
        {
            let state =
                super::mirroring_transactions::finish(tx, task, *size, *missing, key).await?;
            if state == "failed" {
                issue = Some("origin_fill_failed");
            }
            state
        }
        (
            Some(work::Plan::Collect {
                storage,
                delete_before,
                retention,
                metadata_count,
                items,
            }),
            outcomes,
        ) => {
            super::work_collect::finish(
                tx,
                task,
                storage,
                delete_before,
                retention,
                metadata_count,
                &items,
                outcomes,
            )
            .await?;
            return settle(tx, node, task, result.operation, &hash).await;
        }
        (Some(work::Plan::Rewrite { items, .. }), outcomes) => {
            let record = queries::task_record(task.id).fetch_one(&mut **tx).await?;
            issue =
                super::work_rewrite::finish(tx, task, &record.detail, items, outcomes, key).await?;
            if issue.is_some_and(|s| s == "backend_io_failed") {
                "queued"
            } else if issue.is_some() {
                "failed"
            } else {
                "queued"
            }
        }
        (Some(work::Plan::HashRaw { items, .. }), outcomes) => {
            super::work_raw::finish(tx, task, &items, outcomes).await?;
            "completed"
        }
        (Some(work::Plan::ConvertRaw { .. }), [outcome]) => {
            if matches!(outcome, work::Outcome::Retry) {
                issue = Some("backend_io_failed");
            }
            super::work_conversion::finish(tx, task, outcome).await?
        }
        (Some(work::Plan::Migrate { .. }), [outcome]) => {
            super::work_migration::finish(tx, task, outcome).await?;
            return settle(tx, node, task, result.operation, &hash).await;
        }
        (Some(work::Plan::Audit { cursor, items, .. }), outcomes) => {
            let record = queries::task_record(task.id).fetch_one(&mut **tx).await?;
            ensure!(
                record.execution_token == Some(task.token) && record.state == "running",
                Error::NodeExpired
            );
            issue =
                super::work_audit::finish(tx, task, record.detail, cursor, items, outcomes).await?;
            if issue.is_some() { "failed" } else { "queued" }
        }
        (
            Some(work::Plan::SweepScan {
                after,
                limit,
                bytes,
                ..
            }),
            outcomes,
        ) => {
            let record = queries::task_record(task.id).fetch_one(&mut **tx).await?;
            ensure!(
                record.execution_token == Some(task.token),
                Error::NodeExpired
            );
            let maintenance = !q::writable().fetch_one(&mut **tx).await?;
            let (done, error) = super::work_sweep::listed(
                tx,
                task,
                record.detail,
                after,
                limit,
                bytes,
                outcomes,
                maintenance,
            )
            .await?;
            issue = error;
            if issue.is_some() {
                "failed"
            } else if done {
                "completed"
            } else {
                "queued"
            }
        }
        (Some(work::Plan::SweepDelete { before, items, .. }), outcomes) => {
            let record = queries::task_record(task.id).fetch_one(&mut **tx).await?;
            ensure!(
                record.execution_token == Some(task.token),
                Error::NodeExpired
            );
            let maintenance = !q::writable().fetch_one(&mut **tx).await?;
            issue = super::work_sweep::deleted(
                tx,
                task,
                record.detail,
                before,
                items,
                outcomes,
                maintenance,
            )
            .await?;
            if issue.is_some() { "failed" } else { "queued" }
        }
        (Some(work::Plan::Pack { storage, batch, .. }), outcomes) => {
            let (state, error) =
                super::work_media::finish(tx, task, storage, *batch, outcomes, key).await?;
            queries::release_task_inputs(task.id)
                .execute(&mut **tx)
                .await?;
            issue = error;
            state
        }
        (Some(work::Plan::UploadRaw { items, .. }), [outcome]) if items.len() == 1 => {
            let (next, error) =
                super::node_files::finish(tx, node, task, &items[0], outcome, wake_shared).await?;
            issue = error;
            next
        }
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
                return settle(tx, node, task, result.operation, &hash).await;
            }
            "queued"
        }
        (Some(work::Plan::Tier { .. }), [work::Outcome::Retry]) => {
            super::tier_move_transactions::finish(tx, task, &crate::model::tiering::Outcome::Retry)
                .await?;
            return settle(tx, node, task, result.operation, &hash).await;
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
    settle(tx, node, task, result.operation, &hash).await
}
async fn settle(
    tx: &mut registry::Tx<'_>,
    node: Uuid,
    task: Execution,
    operation: Uuid,
    hash: &[u8],
) -> Result<()> {
    q::drop_lanes(task.token).execute(&mut **tx).await?;
    q::settle(node, operation, hash).execute(&mut **tx).await?;
    Ok(())
}

#[async_trait::async_trait]
impl NodeWork for Backend {
    async fn progress_mirror(
        &self,
        node: &NodeState,
        key: &Key,
        run: Run,
        operation: Uuid,
        bytes: u64,
        publishing: bool,
    ) -> Result<()> {
        let mut tx = super::node_writes::start(self, node, key, run).await?;
        let (plan, task) = checkpoint(&mut tx, node, run, operation).await?;
        ensure!(
            matches!(plan, work::Plan::Mirror {task:id} if id == task.id),
            Error::NodeOperationConflict
        );
        super::mirroring_transactions::progress(&mut tx, task, bytes, publishing).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn begin_mirror(
        &self,
        node: &NodeState,
        key: &Key,
        run: Run,
        work: Uuid,
        operation: Uuid,
        input: crate::model::mirroring::Begin,
        cached: Vec<crate::model::node_read::Version>,
    ) -> Result<crate::model::node_write::Reply> {
        super::work_mirroring::begin(self, node, key, run, work, operation, input, cached).await
    }
    async fn read_transfer(
        &self,
        node: &NodeState,
        key: &Key,
        input: work::ReadTransfer,
    ) -> Result<(
        crate::model::node_read::Reply,
        Option<crate::model::replication::external::RawRead>,
    )> {
        super::work_export::read(self, node, key, input).await
    }
    async fn checkpoint_transfer(
        &self,
        node: &NodeState,
        key: &Key,
        run: Run,
        operation: Uuid,
        progress: &crate::model::transfer::RawProgress,
    ) -> Result<()> {
        progress.validate()?;
        let mut tx = super::node_writes::start(self, node, key, run).await?;
        ensure!(
            q::writable().fetch_one(&mut *tx).await?,
            Error::OperationAborted
        );
        let (plan, task) = checkpoint(&mut tx, node, run, operation).await?;
        let work::Plan::RawTransfer { transfer: plan, .. } = plan else {
            anyhow::bail!(Error::NodeOperationConflict)
        };
        let run = super::transactions::replication::owned(&mut tx, task).await?;
        super::transactions::transfer::raw_progress(&mut tx, task, &run, &plan, progress).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn checkpoint_raw_pages(
        &self,
        node: &NodeState,
        key: &Key,
        input: work::RawPages,
    ) -> Result<()> {
        let mut db = super::node_writes::start(self, node, key, input.run).await?;
        let (plan, _) = checkpoint(&mut db, node, input.run, input.operation).await?;
        let work::Plan::Audit {
            mode: crate::worker::integrity::Mode::Full,
            items,
            ..
        } = plan
        else {
            anyhow::bail!(Error::NodeOperationConflict)
        };
        let raw = items
            .iter()
            .filter(|item| item.raw_pages && item.previous.is_none())
            .filter_map(|item| item.snapshot.as_deref())
            .find_map(|snapshot| match &snapshot.physical {
                crate::model::PhysicalSource::Raw(raw) if raw.location_id == input.source => {
                    Some(raw)
                }
                _ => None,
            })
            .ok_or(Error::NodeOperationConflict)?;
        let current = queries::raw_source(raw.id).fetch_one(&mut *db).await?;
        ensure!(
            current.size == raw.size
                && current.digest == raw.digest
                && current.quarantine_code.is_none()
                && current.state == "ready",
            Error::OperationAborted
        );
        if let Some(group) = input.group {
            group.validate_size(raw.size)?;
            super::raw_pages::put(&mut db, raw.id, &group).await?;
        } else {
            super::raw_pages::seal(
                &mut db,
                raw.id,
                raw.size,
                raw.digest.as_deref().ok_or(Error::SourceUnavailable)?,
            )
            .await?;
        }
        db.commit().await?;
        Ok(())
    }

    async fn read_migration(
        &self,
        node: &NodeState,
        key: &Key,
        input: work::ReadMigration,
    ) -> Result<crate::model::node_read::Reply> {
        super::work_migration::read(self, node, key, input).await
    }
    async fn recover_work(&self) -> Result<u64> {
        let mut tx = registry::begin(self).await?;
        let count = q::requeue(&mut tx).await?;
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
        let mut tx =
            q::begin_media(self, input.max_items > 0 && matches!(input.class, 1 | 2)).await?;
        let active = node_registry::require_work(self, &mut tx, node, key, input.run).await?;
        ensure!(
            matches!(node.role.as_str(), "data" | "worker" | "node"),
            Error::NodeAuthentication
        );
        if let Some(known) = q::offer(node.node_id, input.operation)
            .fetch_optional(&mut *tx)
            .await?
        {
            let mut reply = known.reply(node, key, &input, &digest)?;
            // The receipt is fixed; an empty replay can still prompt a fresh claim.
            if active && input.class == 2 && input.max_items > 0 && reply.plan.is_none() {
                reply.check_again = queries::node_files::pending(node.node_id)
                    .fetch_one(&mut *tx)
                    .await?;
            }
            return Ok(reply);
        }
        let mut wake_shared = false;
        if let Some(result) = &input.completion {
            finish(
                &mut tx,
                node.node_id,
                input.run,
                result,
                key,
                &mut wake_shared,
            )
            .await?;
        }
        let token = Uuid::new_v4();
        if active
            && input.max_items > 0
            && input.class == 2
            && matches!(node.role.as_str(), "worker" | "node")
        {
            super::work_raw::schedule(&mut tx, input.max_items).await?;
        }
        let next = if active && input.max_items > 0 {
            q::next_task(
                input.class,
                node.node_id,
                matches!(node.role.as_str(), "worker" | "node"),
            )
            .fetch_optional(&mut *tx)
            .await?
        } else {
            None
        };
        let mut check_again = false;
        let mut task_id = None;
        let mut plan = None;
        let mut encrypted = None;
        if let Some((task, task_type, detail)) = next {
            let replication =
                task_type == "storage_migration" && detail["operation"] == "replication";
            ensure!(
                queries::claim_task(task, token)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected()
                    == 1,
                Error::OperationAborted
            );
            let task = Execution { id: task, token };
            if input.class == 1 {
                queries::release_task_inputs(task.id)
                    .execute(&mut *tx)
                    .await?;
            }
            let prepared = match detail["operation"].as_str() {
                Some("mirroring") => super::work_mirroring::offer(&mut tx, task, key).await?,
                Some("cache") if detail.get("source_node").is_some() => {
                    super::node_files::offer(&mut tx, node, task, &detail, key).await?
                }
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
                Some("deduplicate") => super::work_raw::offer(&mut tx, task, &detail, key).await?,
                Some("cdc") => {
                    super::work_conversion::offer(&mut tx, node, input.run, task, &detail, key)
                        .await?
                }
                _ if matches!(task_type.as_str(), "gc" | "cleanup") => {
                    super::work_collect::offer(
                        &mut tx,
                        task,
                        &detail,
                        task_type == "gc",
                        input.max_items,
                        key,
                    )
                    .await?
                }
                _ if task_type == "rewrite" => {
                    super::work_rewrite::offer(&mut tx, task, &detail, input.max_items, key).await?
                }
                _ if matches!(task_type.as_str(), "pack" | "unpack") => {
                    super::work_media::offer(&mut tx, task, &detail, key).await?
                }
                _ if task_type == "storage_migration" && detail.get("job").is_some() => {
                    super::work_migration::offer(&mut tx, node, input.run, task, &detail, key)
                        .await?
                }
                Some("replication") if task_type == "storage_migration" => {
                    super::work_replication::offer(&mut tx, node, input.run, task, key).await?
                }
                _ if task_type == "sweep" => {
                    let storage = serde_json::from_value(detail["storage_id"].clone())?;
                    let profile = material(&mut tx, storage, key).await?;
                    let prefix = profile.connection.prefix.clone();
                    let maintenance = !q::writable().fetch_one(&mut *tx).await?;
                    if let Some(plan) = super::work_sweep::prepare(
                        &mut tx,
                        task,
                        detail,
                        &profile.profile,
                        &prefix,
                        input.max_items,
                        maintenance,
                    )
                    .await?
                    {
                        Some((
                            plan,
                            serde_json::to_vec(&work::Materials {
                                storage: profile,
                                additional: vec![],
                                keys: vec![],
                            })?,
                        ))
                    } else {
                        let record = queries::task_record(task.id).fetch_one(&mut *tx).await?;
                        if record.state == "running" {
                            queries::finish_task(task, "queued", None)
                                .execute(&mut *tx)
                                .await?;
                        }
                        None
                    }
                }
                Some("tiering") => tier_plan(&mut tx, task, &detail, key).await?,
                _ if task_type == "integrity" || detail.get("source_operation").is_some() => {
                    let id = serde_json::from_value(detail["storage_id"].clone())?;
                    let storage = material(&mut tx, id, key).await?;
                    let inspection = if detail.get("source_operation").is_some() {
                        super::work_sources::prepare(
                            self,
                            &mut tx,
                            node,
                            task,
                            &detail,
                            &storage.profile,
                            key,
                        )
                        .await?
                    } else {
                        super::work_audit::prepare(
                            self,
                            &mut tx,
                            task,
                            &detail,
                            &storage.profile,
                            input.max_items,
                            key,
                        )
                        .await?
                    };
                    if let Some((plan, keys)) = inspection {
                        let mut additional = Vec::new();
                        for store in plan
                            .inspection_storages()?
                            .into_iter()
                            .filter(|store| *store != id)
                        {
                            additional.push(material(&mut tx, store, key).await?);
                        }
                        Some((
                            plan,
                            serde_json::to_vec(&work::Materials {
                                storage,
                                additional,
                                keys,
                            })?,
                        ))
                    } else {
                        let record = queries::task_record(task.id).fetch_one(&mut *tx).await?;
                        if record.state == "running" {
                            queries::finish_task(task, "queued", None)
                                .execute(&mut *tx)
                                .await?;
                        }
                        None
                    }
                }
                _ => anyhow::bail!(Error::NodeOperationConflict),
            };
            if let Some((proposal, secret)) = prepared {
                reserve_io(&mut tx, token, &task_type, &proposal, &secret).await?;
                encrypted = Some(codec::protect(&secret, key, token.as_bytes())?);
                plan = Some(proposal);
                task_id = Some(task.id);
            } else if replication {
                let record = queries::task_record(task.id).fetch_one(&mut *tx).await?;
                wake_shared |= record.state == "queued"
                    && record.detail["external"] != true
                    && record.detail["phase"] != "verify";
            } else if input.class == 2 && task_type == "upload" {
                check_again = queries::task_record(task.id)
                    .fetch_one(&mut *tx)
                    .await?
                    .state
                    == "completed";
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
        if task_id.is_some() {
            q::align_lanes(node.node_id, token)
                .execute(&mut *tx)
                .await?;
        }
        let offer = q::offer(node.node_id, input.operation)
            .fetch_one(&mut *tx)
            .await?;
        let mut reply = offer.reply(node, key, &input, &digest)?;
        reply.check_again = check_again;
        reply.wake_shared = wake_shared;
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
                if held {
                    if renew {
                        q::align_lanes(node, grant.id).execute(&mut **tx).await?;
                    } else {
                        q::drop_lanes(grant.id).execute(&mut **tx).await?;
                    }
                }
                held.then_some(90_000)
            } else if grant.r#type == GrantType::Native {
                let held = queries::native::renew_native(node, run, grant.id, renew)
                    .execute(&mut **tx)
                    .await?
                    .rows_affected()
                    == 1;
                if held {
                    queries::native::touch_pin(grant.id, renew)
                        .execute(&mut **tx)
                        .await?;
                    queries::native::charge_part(grant.id, renew)
                        .execute(&mut **tx)
                        .await?;
                    queries::native::charge_upload(grant.id, renew)
                        .execute(&mut **tx)
                        .await?;
                    queries::native::charge_storage(grant.id, renew)
                        .execute(&mut **tx)
                        .await?;
                }
                held.then_some(60_000)
            } else if grant.r#type == GrantType::StorageRead
                && queries::native::touch_read(node, run, grant.id, renew)
                    .execute(&mut **tx)
                    .await?
                    .rows_affected()
                    > 0
            {
                Some(60_000)
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
