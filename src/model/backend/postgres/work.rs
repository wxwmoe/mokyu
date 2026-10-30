use super::super::external_transactions;
use super::*;
use crate::model::node_work as work;
use queries::work as q;

#[path = "work_sources.rs"]
mod sources;

#[path = "work_audit.rs"]
mod audit;

#[path = "work_sweep.rs"]
mod sweep;

#[path = "work_rewrite.rs"]
mod rewrite;

#[path = "work_packing.rs"]
mod packing;

#[path = "work_transfer.rs"]
mod transfer;

#[path = "work_raw.rs"]
mod raw;

#[path = "work_conversion.rs"]
mod conversion;

#[path = "work_migration.rs"]
pub(crate) mod migration;

#[path = "work_write_packs.rs"]
mod write_packs;

pub(super) async fn prepare_chunk_packs(
    db: &mut Transaction<'_, Postgres>,
    row: &queries::NodeWrite,
    items: &[chunk::Item],
) -> Result<Option<crate::model::node_pack::Batch>> {
    write_packs::prepare(db, row, items).await
}

pub(super) async fn finish_chunk_packs(
    db: &mut Transaction<'_, Postgres>,
    row: &queries::NodeWrite,
    items: &[chunk::Item],
    batch: &crate::model::node_pack::Batch,
    result: &crate::model::node_pack::Encoded,
    key: &Key,
) -> Result<()> {
    write_packs::finish(db, row, items, batch, result, key).await
}

pub(super) async fn seal_task_write(
    db: &mut Transaction<'_, Postgres>,
    row: &queries::NodeWrite,
    input: &write::Publish,
) -> Result<()> {
    let migration: bool =
        sqlx::query_scalar("SELECT task_type='storage_migration' FROM tasks WHERE id=$1")
            .bind(row.owner_task_id.ok_or(Error::NodeOperationConflict)?)
            .fetch_one(&mut **db)
            .await?;
    if migration {
        self::migration::seal(db, row, input).await
    } else {
        conversion::seal(db, row, input).await
    }
}

fn owned(node: &NodeState, run: Run, batch: &q::Batch) -> Result<()> {
    ensure!(
        batch.credential_generation == node.credential_generation
            && batch.boot_id == run.boot
            && batch.fence == run.fence,
        Error::NodeExpired
    );
    Ok(())
}

fn reply(
    node: &NodeState,
    key: &Key,
    input: &work::Exchange,
    row: q::Batch,
) -> Result<work::Reply> {
    owned(node, input.run, &row)?;
    ensure!(
        row.request_hash == input.hash()?,
        Error::NodeOperationConflict
    );
    if row.task_id.is_some() || row.plan.is_some() {
        ensure!(
            row.valid_for_ms > 0 && row.result_hash.is_none(),
            Error::NodeExpired
        );
    }
    let mut materials = vec![];
    let mut grants = vec![];
    let mut external = None;
    let plan = row.plan.map(|value| value.0);
    if let Some(plan) = &plan {
        let material = codec::unprotect(
            row.storage_encrypted
                .as_deref()
                .context("work storage is missing")?,
            key,
            row.token.as_bytes(),
        )?;
        if let Some(storage) = plan.storage() {
            let values: work::Materials = serde_json::from_str(&material)?;
            ensure!(
                values.storage.profile.storage_id == storage,
                Error::NodeOperationConflict
            );
            for value in std::iter::once(values.storage).chain(values.additional) {
                read::material(
                    &mut materials,
                    node.deployment_id,
                    read::MaterialType::Storage,
                    value.profile.storage_id.to_string(),
                    &value,
                    &input.cached,
                )?;
            }
            for block in values.keys {
                read::material(
                    &mut materials,
                    node.deployment_id,
                    read::MaterialType::Key,
                    block.id.to_string(),
                    &block,
                    &input.cached,
                )?;
            }
        } else {
            external = Some(serde_json::from_str(&material)?);
        }
        grants.push(read::Grant {
            r#type: read::GrantType::Work,
            id: row.token,
            valid_for_ms: row.valid_for_ms as u64,
            storage: plan.storage(),
        });
    }
    Ok(work::Reply {
        operation: input.operation,
        plan,
        materials,
        grants,
        external,
    })
}

#[cfg(test)]
#[test]
fn cleared_work_cannot_be_replayed_as_an_empty_assignment() -> Result<()> {
    let node = NodeState {
        format: 1,
        deployment_id: Uuid::new_v4(),
        node_id: Uuid::new_v4(),
        role: "worker".into(),
        credential_generation: 1,
        credential: String::new(),
    };
    let input = work::Exchange {
        run: Run {
            boot: Uuid::new_v4(),
            fence: Uuid::new_v4(),
        },
        operation: Uuid::new_v4(),
        completion: None,
        class: 0,
        max_items: 1,
        cached: vec![],
    };
    let key = Key::new("aes-256-gcm", [7; 32])?;
    for (assigned, completed, valid_for_ms) in
        [(false, false, 0), (true, false, 0), (true, true, 60_000)]
    {
        let row = q::Batch {
            credential_generation: node.credential_generation,
            boot_id: input.run.boot,
            fence: input.run.fence,
            request_hash: input.hash()?,
            task_id: assigned.then(Uuid::new_v4),
            token: Uuid::new_v4(),
            maintenance_token: Uuid::new_v4(),
            plan: None,
            storage_encrypted: None,
            valid_for_ms,
            result_hash: completed.then(|| vec![0; 32]),
        };
        match reply(&node, &key, &input, row) {
            Ok(value) => {
                assert!(!assigned);
                assert!(
                    value.plan.is_none() && value.grants.is_empty() && value.external.is_none()
                );
            }
            Err(error) => {
                assert!(assigned);
                assert!(matches!(
                    error.downcast_ref::<Error>(),
                    Some(Error::NodeExpired)
                ));
            }
        }
    }
    Ok(())
}

async fn finish(
    db: &mut Transaction<'_, Postgres>,
    node: &NodeState,
    run: Run,
    completion: &work::Completion,
    maintenance: bool,
    key: &Key,
) -> Result<()> {
    let row = q::batch(node.node_id, completion.operation)
        .fetch_optional(&mut **db)
        .await?
        .ok_or(Error::NodeOperationMissing)?;
    owned(node, run, &row)?;
    let hash = crate::model::node_chunk::hash(completion)?;
    if let Some(previous) = row.result_hash {
        ensure!(hash == previous, Error::NodeOperationConflict);
        return Ok(());
    }
    ensure!(row.valid_for_ms > 0, Error::NodeExpired);
    let mut permit_tokens = vec![row.token, row.maintenance_token];
    if let Some(plan) = &row.plan {
        permit_tokens.extend_from_slice(plan.0.source_permits());
    }
    if row
        .plan
        .as_ref()
        .is_some_and(|plan| plan.0.storage().is_some())
    {
        ensure!(
            q::live_permits(&permit_tokens).fetch_one(&mut **db).await?,
            Error::NodeExpired
        );
    }
    let task = row.task_id.ok_or(Error::NodeExpired)?;
    let (_, detail, parent) = q::task(task, row.token, node.node_id)
        .fetch_optional(&mut **db)
        .await?
        .ok_or(Error::NodeExpired)?;
    let mut deleted = 0;
    let mut error = None;
    let mut complete = false;
    match row.plan.context("work plan is missing")?.0 {
        work::Plan::ExternalTest(plan) => {
            ensure!(
                detail["operation"] == "external_test",
                Error::NodeOperationConflict
            );
            let [work::Outcome::ExternalTest(report)] = completion.outcomes.as_slice() else {
                return Err(Error::NodeOperationConflict.into());
            };
            external_transactions::finish_test(
                db,
                Execution {
                    id: task,
                    token: row.token,
                },
                &plan,
                report,
            )
            .await?;
            complete = report.phase == crate::model::external_test::Phase::Done;
            error = report
                .blocked
                .map(|_| "external connection test needs reconciliation");
        }
        work::Plan::UploadRaw { items, .. } => {
            (complete, error) = super::raw_cache::finish(
                db,
                node.node_id,
                Execution {
                    id: task,
                    token: row.token,
                },
                &items,
                &completion.outcomes,
            )
            .await?;
        }
        work::Plan::ConvertRaw { .. } => {
            ensure!(!maintenance, Error::OperationAborted);
            (complete, error) = conversion::finish(
                db,
                Execution {
                    id: task,
                    token: row.token,
                },
                &detail,
                &completion.outcomes,
            )
            .await?;
            release_task_inputs(db, task).await?;
        }
        work::Plan::Migrate { .. } => {
            ensure!(!maintenance, Error::OperationAborted);
            (complete, error) = migration::finish(
                db,
                Execution {
                    id: task,
                    token: row.token,
                },
                &detail,
                &completion.outcomes,
            )
            .await?;
            release_task_inputs(db, task).await?;
        }
        work::Plan::RawTransfer { transfer: plan, .. } => {
            ensure!(!maintenance, Error::OperationAborted);
            let [outcome] = completion.outcomes.as_slice() else {
                return Err(Error::NodeOperationConflict.into());
            };
            match outcome {
                work::Outcome::Transferred { digest } => {
                    let task = Execution {
                        id: task,
                        token: row.token,
                    };
                    let run = super::super::replication_transactions::owned(db, task).await?;
                    super::super::transfer_transactions::raw_progress(
                        db,
                        task,
                        &run,
                        &plan,
                        &crate::model::transfer::RawProgress::Finished {
                            digest: digest.clone(),
                        },
                    )
                    .await?;
                }
                work::Outcome::Retry => error = Some("backend_io_failed"),
                _ => return Err(Error::NodeOperationConflict.into()),
            }
        }
        work::Plan::CopyTransfer {
            storage,
            transfer: copied,
            batch,
            ..
        } => {
            ensure!(!maintenance, Error::OperationAborted);
            let [outcome] = completion.outcomes.as_slice() else {
                return Err(Error::NodeOperationConflict.into());
            };
            match outcome {
                work::Outcome::Packed(result) => {
                    transfer::finish(
                        db,
                        Execution {
                            id: task,
                            token: row.token,
                        },
                        storage,
                        &copied,
                        &batch,
                        result,
                        key,
                    )
                    .await?
                }
                work::Outcome::Retry => error = Some("backend_io_failed"),
                _ => return Err(Error::NodeOperationConflict.into()),
            }
        }
        work::Plan::ScanTransfer(plan) => {
            ensure!(
                !maintenance && completion.outcomes.len() == 1,
                Error::OperationAborted
            );
            match &completion.outcomes[0] {
                work::Outcome::Scanned(value) => {
                    plan.validate(value)?;
                    let run = super::super::replication_transactions::owned(
                        db,
                        Execution {
                            id: task,
                            token: row.token,
                        },
                    )
                    .await?;
                    super::super::transfer_transactions::scanned(db, &run, value).await?;
                }
                work::Outcome::Retry => error = Some("backend_io_failed"),
                _ => return Err(Error::NodeOperationConflict.into()),
            }
        }
        work::Plan::HashRaw { items, .. } => {
            ensure!(!maintenance, Error::OperationAborted);
            (deleted, complete, error) = if detail["operation"] == "replication" {
                super::super::replication_transactions::complete_verification(
                    db,
                    Execution {
                        id: task,
                        token: row.token,
                    },
                    &items,
                    &completion.outcomes,
                )
                .await?
            } else {
                raw::finish(
                    db,
                    Execution {
                        id: task,
                        token: row.token,
                    },
                    &detail,
                    items,
                    &completion.outcomes,
                )
                .await?
            };
        }
        work::Plan::Collect {
            items,
            delete_before,
            retention,
            storage,
            ..
        } => {
            ensure!(
                items.len() == completion.outcomes.len(),
                Error::NodeOperationConflict
            );
            for (item, outcome) in items.iter().zip(&completion.outcomes) {
                let stored = match outcome {
                    work::Outcome::Deleted { stored_at } => {
                        ensure!(
                            !item.inspect_age || stored_at.is_none_or(|age| age <= delete_before),
                            Error::NodeOperationConflict
                        );
                        *stored_at
                    }
                    work::Outcome::Retained { stored_at } => {
                        ensure!(
                            item.inspect_age && *stored_at > delete_before,
                            Error::NodeOperationConflict
                        );
                        Some(*stored_at)
                    }
                    work::Outcome::Retry => {
                        error = Some("backend_io_failed");
                        None
                    }
                    _ => return Err(Error::NodeOperationConflict.into()),
                };
                if let Some(stored) = stored {
                    ensure!(
                        matches!(item.source, work::Physical::Chunk | work::Physical::Pack),
                        Error::NodeOperationConflict
                    );
                    queries::physical_gc_age(item.source.packed(), item.location.into(), stored)
                        .execute(&mut **db)
                        .await?;
                }
                if matches!(outcome, work::Outcome::Deleted { .. }) {
                    match &item.source {
                        work::Physical::Chunk | work::Physical::Pack => {
                            queries::finish_physical_gc(
                                item.source.packed(),
                                item.location.into(),
                                retention as f64,
                            )
                            .execute(&mut **db)
                            .await?;
                        }
                        work::Physical::Raw { .. } => {
                            queries::raw_gc_finish(item.location.into())
                                .execute(&mut **db)
                                .await?;
                        }
                        work::Physical::Staging { key } => {
                            q::remove_staging(storage, key).execute(&mut **db).await?;
                        }
                    }
                    deleted += 1;
                }
            }
        }
        work::Plan::Inspect {
            snapshot, inspect, ..
        } => {
            error = sources::finish(
                db,
                Execution {
                    id: task,
                    token: row.token,
                },
                detail,
                *snapshot,
                inspect,
                &completion.outcomes,
            )
            .await?;
            complete = true;
        }
        work::Plan::Audit { cursor, items, .. } => {
            error = audit::finish(
                db,
                Execution {
                    id: task,
                    token: row.token,
                },
                detail,
                cursor,
                items,
                &completion.outcomes,
            )
            .await?;
        }
        work::Plan::Pack { storage, batch } => {
            ensure!(!maintenance, Error::OperationAborted);
            error = packing::finish(
                db,
                Execution {
                    id: task,
                    token: row.token,
                },
                storage,
                *batch,
                &detail,
                &completion.outcomes,
                key,
            )
            .await?;
            release_task_inputs(db, task).await?;
        }
        work::Plan::Rewrite { items, .. } => {
            ensure!(!maintenance, Error::OperationAborted);
            error = rewrite::finish(
                db,
                Execution {
                    id: task,
                    token: row.token,
                },
                &detail,
                items,
                &completion.outcomes,
                key,
            )
            .await?;
            release_task_inputs(db, task).await?;
        }
        work::Plan::SweepScan {
            after,
            limit,
            bytes,
            ..
        } => {
            (complete, error) = sweep::listed(
                db,
                Execution {
                    id: task,
                    token: row.token,
                },
                detail,
                after,
                limit,
                bytes,
                &completion.outcomes,
                maintenance,
            )
            .await?;
        }
        work::Plan::SweepDelete { before, items, .. } => {
            error = sweep::deleted(
                db,
                Execution {
                    id: task,
                    token: row.token,
                },
                detail,
                before,
                items,
                &completion.outcomes,
                maintenance,
            )
            .await?;
        }
    }
    q::result(node.node_id, completion.operation, &hash)
        .execute(&mut **db)
        .await?;
    q::release_permits(&permit_tokens)
        .execute(&mut **db)
        .await?;
    q::finish(task, row.token, deleted, complete, error)
        .execute(&mut **db)
        .await?;
    if let Some(parent) = parent {
        queries::update_parent_progress(parent)
            .execute(&mut **db)
            .await?;
    }
    Ok(())
}

async fn permits(
    db: &mut PgConnection,
    node: Uuid,
    token: Uuid,
    maintenance: Uuid,
    profile: &crate::storage::profile::StorageProfile,
    direction: &str,
) -> Result<bool> {
    let limit = match direction {
        "upload" => profile.upload_concurrency,
        "integrity" => Some(profile.integrity_concurrency),
        "migration" => profile.migration_concurrency,
        _ => profile.maintenance_concurrency,
    };
    for (direction, limit) in [("control", profile.control_concurrency), (direction, limit)] {
        queries::lock_storage_permits(format!("mokyu:store:{}:{direction}", profile.storage_id))
            .execute(&mut *db)
            .await?;
        queries::expire_storage_permits(profile.storage_id, direction)
            .execute(&mut *db)
            .await?;
        if let Some(limit) = limit
            && queries::storage_permit_count(profile.storage_id, direction)
                .fetch_one(&mut *db)
                .await?
                >= i64::from(limit)
        {
            return Ok(false);
        }
    }
    queries::insert_storage_permit(token, profile.storage_id, "control", node)
        .execute(&mut *db)
        .await?;
    queries::insert_storage_permit(maintenance, profile.storage_id, direction, node)
        .execute(&mut *db)
        .await?;
    Ok(true)
}

async fn cleanup(
    db: &mut Transaction<'_, Postgres>,
    storage: Uuid,
    maximum: usize,
    retention: u64,
) -> Result<usize> {
    let mut count = 0;
    queries::expire_stream_leases().execute(&mut **db).await?;
    for upload in queries::expired_uploads(maximum as i64, Some(storage))
        .fetch_all(&mut **db)
        .await?
    {
        count += usize::from(abort_multipart_in(db, &Permit::system(), upload, true).await?);
    }
    let retention = Some(retention as f64);
    let streams: Vec<Id> = queries::stale_streams(&[], 1, Some(storage), retention, false)
        .build_query_scalar()
        .fetch_all(&mut **db)
        .await?;
    for stream in streams {
        if queries::claim_stale_stream(stream, retention, Some(storage), false)
            .build()
            .execute(&mut **db)
            .await?
            .rows_affected()
            == 1
        {
            count += cleanup_stream_in(db, stream, maximum as i64).await?;
        }
    }
    for id in queries::stale_fragments(maximum as i64, &[], Some(storage))
        .fetch_all(&mut **db)
        .await?
    {
        if let Some((_, _, _, staged, _, file)) = queries::lock_cleanup_fragment(id)
            .fetch_optional(&mut **db)
            .await?
        {
            // Shared staging stays journaled after its last fragment is detached.
            if staged.is_some() || file.is_some() {
                queries::remove_fragment_row(id).execute(&mut **db).await?;
                count += 1;
            }
        }
    }
    count += queries::remove_tombstones(maximum as i64, Some(storage))
        .execute(&mut **db)
        .await?
        .rows_affected() as usize;
    Ok(count)
}

async fn collect(
    db: &mut Transaction<'_, Postgres>,
    storage: Uuid,
    maximum: usize,
    profile: &crate::storage::profile::StorageProfile,
    reclaim: bool,
) -> Result<work::Plan> {
    let retention = crate::config::seconds(if reclaim {
        &profile.upload_retention
    } else {
        &profile.deleted_chunk_retention
    })?;
    let mut metadata_count = cleanup(db, storage, maximum, retention).await?;
    let grace = crate::config::seconds(&profile.unreferenced_grace)? as f64;
    let minimum = profile.min_storage_seconds()?;
    let mut items = vec![];
    if reclaim {
        for (id, _) in queries::gc_candidates(storage, grace, maximum as i64)
            .fetch_all(&mut **db)
            .await?
        {
            if queries::lock_reclaim_chunk(id, storage, grace)
                .fetch_optional(&mut **db)
                .await?
                .is_some()
            {
                queries::claim_reclaim_chunk(id, storage)
                    .execute(&mut **db)
                    .await?;
                queries::retire_chunk_encodings(id)
                    .execute(&mut **db)
                    .await?;
                queries::finish_reclaim_chunk(id, storage)
                    .execute(&mut **db)
                    .await?;
                metadata_count += 1;
            }
        }
        queries::retire_empty_packs(storage)
            .execute(&mut **db)
            .await?;
        for source in [work::Physical::Chunk, work::Physical::Pack] {
            let remaining = maximum - items.len();
            if remaining == 0 {
                break;
            }
            for (id, location, unknown) in queries::physical_gc_candidates(
                source.packed(),
                grace,
                remaining as i64,
                minimum as f64,
                storage,
                false,
            )
            .fetch_all(&mut **db)
            .await?
            {
                if q::physical_active(location).fetch_one(&mut **db).await? {
                    continue;
                }
                if queries::claim_physical_gc(source.packed(), id, grace, minimum as f64)
                    .execute(&mut **db)
                    .await?
                    .rows_affected()
                    == 1
                {
                    items.push(work::Deletion {
                        source: source.clone(),
                        location,
                        inspect_age: minimum > 0 && unknown,
                    });
                }
            }
        }
        let remaining = maximum - items.len();
        if remaining > 0 {
            for source in
                queries::raw_gc_candidates(storage, grace as i64, minimum as i64, remaining as i64)
                    .fetch_all(&mut **db)
                    .await?
            {
                if source.medium != "shared_s3"
                    || q::physical_active(source.location_id)
                        .fetch_one(&mut **db)
                        .await?
                {
                    continue;
                }
                if queries::raw_gc_claim(source.id, source.revision, grace as i64, minimum as i64)
                    .execute(&mut **db)
                    .await?
                    .rows_affected()
                    == 1
                {
                    items.push(work::Deletion {
                        source: work::Physical::Raw {
                            upload: q::upload(source.id).fetch_one(&mut **db).await?,
                        },
                        location: source.location_id,
                        inspect_age: false,
                    });
                }
            }
        }
    }
    let remaining = maximum - items.len();
    if remaining > 0 {
        queries::lock_staging(format!("mokyu:staging:{storage}"))
            .execute(&mut **db)
            .await?;
        for (key,) in q::staging(storage, remaining as i64)
            .fetch_all(&mut **db)
            .await?
        {
            ensure!(key.len() <= 2048, Error::NodeWorkLimit);
            let location = Uuid::from_bytes(
                blake3::hash(format!("{storage}/{key}").as_bytes()).as_bytes()[..16]
                    .try_into()
                    .unwrap(),
            );
            if !q::physical_active(location).fetch_one(&mut **db).await? {
                items.push(work::Deletion {
                    source: work::Physical::Staging { key },
                    location,
                    inspect_age: false,
                });
            }
        }
    }
    let now = queries::node_clock().fetch_one(&mut **db).await?;
    Ok(work::Plan::Collect {
        storage,
        delete_before: now - chrono::Duration::seconds(minimum as i64),
        retention: crate::config::seconds(&profile.deleted_chunk_retention)?,
        metadata_count,
        items,
    })
}

pub(crate) async fn exchange(
    pool: &PgPool,
    node: &NodeState,
    key: &Key,
    input: work::Exchange,
) -> Result<work::Reply> {
    let hash = input.hash()?;
    let issued = write::operation_time(input.operation)?;
    let mut tx = pool.begin().await?;
    let sweep_barrier = q::sweep_pending().fetch_one(&mut *tx).await?;
    let maintenance_enabled: bool = sqlx::query_scalar(if sweep_barrier {
        "SELECT maintenance FROM mokyu_meta WHERE singleton FOR UPDATE"
    } else {
        "SELECT maintenance FROM mokyu_meta WHERE singleton FOR SHARE"
    })
    .fetch_one(&mut *tx)
    .await?;
    authenticate_rpc_node(&mut tx, node, key, false).await?;
    let session = require_node_run(&mut tx, node, input.run, true).await?;
    if let Some(row) = q::batch(node.node_id, input.operation)
        .fetch_optional(&mut *tx)
        .await?
    {
        let value = reply(node, key, &input, row)?;
        tx.commit().await?;
        return Ok(value);
    }
    let now = queries::node_clock().fetch_one(&mut *tx).await?;
    ensure!(
        issued <= now + chrono::Duration::seconds(30)
            && issued >= now - chrono::Duration::minutes(15),
        Error::NodeOperationExpired
    );
    if let Some(completion) = &input.completion {
        finish(
            &mut tx,
            node,
            input.run,
            completion,
            maintenance_enabled,
            key,
        )
        .await?;
    }
    q::cleanup(node.node_id).execute(&mut *tx).await?;
    ensure!(
        q::active(node.node_id).fetch_one(&mut *tx).await? < 64,
        Error::NodeWorkLimit
    );
    if input.class == 2
        && matches!(node.role.as_str(), "worker" | "node")
        && !sweep_barrier
        && !maintenance_enabled
        && !session.draining
    {
        raw::schedule(&mut tx, input.max_items).await?;
    }
    let token = Uuid::new_v4();
    let maintenance = Uuid::new_v4();
    let allowed = if sweep_barrier {
        vec!["sweep".to_owned()]
    } else {
        vec![
            "gc".to_owned(),
            "cleanup".to_owned(),
            "integrity".to_owned(),
            "sweep".to_owned(),
            "rewrite".to_owned(),
            "pack".to_owned(),
            "unpack".to_owned(),
            "upload".to_owned(),
            "storage_migration".to_owned(),
        ]
    };
    let mut plan = None;
    let mut encrypted = None;
    let mut assigned = None;
    for _ in 0..if input.max_items == 0 {
        0
    } else {
        work::MAX_ITEMS
    } {
        let task = queries::claim_task(
            input.class as i32,
            &[],
            Some(node.node_id),
            token,
            session.draining,
            &allowed,
            Some(false),
            None,
        )
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(task) = task {
            if input.class != 0 {
                release_task_inputs(&mut tx, task).await?;
            }
            let (policy, detail, parent) = q::task(task, token, node.node_id)
                .fetch_one(&mut *tx)
                .await?;
            if policy == "integrity" && detail["operation"] == "external_test" {
                ensure!(
                    matches!(node.role.as_str(), "worker" | "node"),
                    Error::NodeOperationConflict
                );
                let assignment = external_transactions::prepare_test(
                    &mut tx,
                    Execution { id: task, token },
                    Some(node.node_id),
                )
                .await?;
                let snapshot = crate::model::external_test::Snapshot::open(
                    assignment.external_id,
                    assignment.revision,
                    &assignment.connection_encrypted,
                    key,
                )?;
                encrypted = Some(codec::protect(
                    &serde_json::to_vec(&snapshot)?,
                    key,
                    token.as_bytes(),
                )?);
                q::task_lease(task, token).execute(&mut *tx).await?;
                assigned = Some(task);
                plan = Some(work::Plan::ExternalTest(assignment.plan));
                break;
            }
            let storage: Uuid = if policy == "upload" && detail["operation"] == "cdc" {
                let job: crate::model::raw::Conversion = serde_json::from_value(detail.clone())?;
                queries::raw_source(job.source)
                    .fetch_one(&mut *tx)
                    .await?
                    .storage_id
            } else if policy == "storage_migration" && detail["operation"] == "replication" {
                let run = super::super::replication_transactions::owned(
                    &mut tx,
                    Execution { id: task, token },
                )
                .await?;
                if let Some(unit) = super::super::transfer_queries::pending(task)
                    .fetch_optional(&mut *tx)
                    .await?
                {
                    if matches!(unit.state.as_str(), "selected" | "scanning")
                        && unit.source_type == "raw"
                    {
                        run.source_storage_id
                    } else {
                        run.storage_id
                    }
                } else {
                    run.storage_id
                }
            } else if policy == "storage_migration" && detail["operation"] != "replication" {
                migration::storage(&mut tx, &detail).await?
            } else {
                detail["storage_id"]
                    .as_str()
                    .context("work storage is missing")?
                    .parse()?
            };
            let mut profile = queries::storage_profile(storage)
                .fetch_one(&mut *tx)
                .await?;
            if policy == "upload" && detail["operation"] == "cdc" {
                conversion::freeze(&detail, &mut profile)?;
            }

            let record = queries::node_read_storages(&[storage])
                .fetch_one(&mut *tx)
                .await?;
            ensure!(record.r#type == "s3", Error::SourceUnavailable);
            let secondary_id: Option<Uuid> =
                if policy == "storage_migration" && detail["operation"] == "replication" {
                    q::transfer_source(task, storage)
                        .fetch_optional(&mut *tx)
                        .await?
                } else {
                    None
                };
            let secondary = if let Some(storage) = secondary_id {
                let profile = queries::storage_profile(storage)
                    .fetch_one(&mut *tx)
                    .await?;
                let record = queries::node_read_storages(&[storage])
                    .fetch_one(&mut *tx)
                    .await?;
                ensure!(record.r#type == "s3", Error::SourceUnavailable);
                Some((record, profile))
            } else {
                None
            };
            let source_permits = [Uuid::new_v4(), Uuid::new_v4()];
            let mut scopes = vec![(profile.clone(), token, maintenance)];
            if let Some((_, source)) = &secondary {
                scopes.push((source.clone(), source_permits[0], source_permits[1]));
            }
            scopes.sort_by_key(|(profile, _, _)| profile.storage_id);
            let permit_tokens: Vec<_> = scopes
                .iter()
                .flat_map(|(_, control, work)| [*control, *work])
                .collect();
            let direction = if policy == "integrity" {
                "integrity"
            } else if policy == "storage_migration" {
                "migration"
            } else if policy == "upload" && detail["operation"] == "cache" {
                "upload"
            } else {
                "maintenance"
            };
            let mut permitted = true;
            for (profile, control, work) in &scopes {
                if !permits(&mut tx, node.node_id, *control, *work, profile, direction).await? {
                    permitted = false;
                    break;
                }
            }
            if permitted {
                let selected = if policy == "sweep" && detail["dry_run"] == false && !sweep_barrier
                {
                    // A sweep queued after the shared mode lock needs a new exclusive admission.
                    None
                } else if policy == "sweep" {
                    sweep::prepare(
                        &mut tx,
                        Execution { id: task, token },
                        detail,
                        &profile,
                        &record.prefix,
                        input.max_items,
                        maintenance_enabled,
                    )
                    .await?
                    .map(|plan| (plan, vec![]))
                } else if policy == "upload" && detail["operation"] == "cache" {
                    super::raw_cache::prepare(
                        &mut tx,
                        node.node_id,
                        Execution { id: task, token },
                        &detail,
                        input.max_items,
                        session.draining,
                    )
                    .await?
                } else if policy == "upload" && detail["operation"] == "deduplicate" {
                    raw::prepare(&mut tx, Execution { id: task, token }, &detail, storage).await?
                } else if policy == "upload" && detail["operation"] == "cdc" {
                    conversion::prepare(
                        &mut tx,
                        node,
                        input.run,
                        Execution { id: task, token },
                        &detail,
                        profile.clone(),
                    )
                    .await?
                } else if policy == "storage_migration" && detail["operation"] == "replication" {
                    let execution = Execution { id: task, token };
                    let mut run =
                        super::super::replication_transactions::owned(&mut tx, execution).await?;
                    match super::super::replication_transactions::verification(
                        &mut tx, execution, &mut run,
                    )
                    .await?
                    {
                        crate::model::replication::Step::Verify { storage, sources } => Some((
                            work::Plan::HashRaw {
                                storage,
                                items: sources
                                    .into_iter()
                                    .map(|source| work::RawHash { source, hash: None })
                                    .collect(),
                            },
                            vec![],
                        )),
                        crate::model::replication::Step::Scan(value) => {
                            ensure!(value.source.storage_id == storage, Error::OperationAborted);
                            Some((work::Plan::ScanTransfer(value), vec![]))
                        }
                        crate::model::replication::Step::Raw(copy) => {
                            copy.validate()?;
                            ensure!(
                                copy.target.storage_id == storage
                                    && secondary_id == Some(copy.input.storage()?),
                                Error::OperationAborted
                            );
                            let block = match &copy.input {
                                crate::model::transfer::Input::Chunk { chunk } => {
                                    chunk.block_key_id
                                }
                                crate::model::transfer::Input::Pack { pack, .. } => {
                                    pack.block_key_id
                                }
                                crate::model::transfer::Input::Raw { .. } => None,
                            };
                            let keys =
                                rewrite::keys(&mut tx, &block.into_iter().collect::<Vec<_>>(), key)
                                    .await?;
                            let plan = work::Plan::RawTransfer {
                                storage,
                                transfer: Box::new(copy),
                                source_permits,
                            };
                            ensure!(
                                serde_json::to_vec(&plan)?.len() <= 3 * 1024 * 1024,
                                Error::NodeWorkLimit
                            );
                            Some((plan, keys))
                        }
                        crate::model::replication::Step::Copy(copy) => {
                            ensure!(
                                copy.storage == storage
                                    && secondary_id == Some(copy.input.storage()?),
                                Error::OperationAborted
                            );
                            if let Some(prepared) =
                                transfer::prepare(&mut tx, execution, &copy, key).await?
                            {
                                let plan = work::Plan::CopyTransfer {
                                    storage,
                                    transfer: Box::new(prepared.work),
                                    batch: Box::new(prepared.batch),
                                    source_permits,
                                };
                                ensure!(
                                    serde_json::to_vec(&plan)?.len() <= 3 * 1024 * 1024,
                                    Error::NodeWorkLimit
                                );
                                Some((plan, prepared.keys))
                            } else {
                                None
                            }
                        }
                        crate::model::replication::Step::Continue => None,
                    }
                } else if policy == "storage_migration" {
                    migration::prepare(
                        &mut tx,
                        node,
                        input.run,
                        Execution { id: task, token },
                        &detail,
                        profile.clone(),
                    )
                    .await?
                } else if matches!(policy.as_str(), "pack" | "unpack") {
                    packing::prepare(
                        &mut tx,
                        Execution { id: task, token },
                        &detail,
                        &profile,
                        key,
                    )
                    .await?
                } else if policy == "rewrite" {
                    rewrite::prepare(
                        &mut tx,
                        Execution { id: task, token },
                        &detail,
                        &profile,
                        input.max_items,
                        key,
                    )
                    .await?
                } else if policy == "integrity" && detail.get("source_operation").is_none() {
                    audit::prepare(
                        &mut tx,
                        Execution { id: task, token },
                        &detail,
                        &profile,
                        input.max_items,
                        key,
                    )
                    .await?
                } else if policy == "integrity" {
                    sources::prepare(
                        &mut tx,
                        node,
                        Execution { id: task, token },
                        &detail,
                        &profile,
                        key,
                    )
                    .await?
                } else {
                    Some((
                        collect(&mut tx, storage, input.max_items, &profile, policy == "gc")
                            .await?,
                        vec![],
                    ))
                };
                if let Some((value, keys)) = selected {
                    let mut idle = false;
                    if let work::Plan::Collect {
                        items,
                        metadata_count,
                        ..
                    } = &value
                    {
                        if *metadata_count > 0 {
                            q::metadata_progress(task, token, *metadata_count as i64)
                                .execute(&mut *tx)
                                .await?;
                        }
                        idle = items.is_empty() && *metadata_count == 0;
                    }
                    if idle {
                        q::finish(task, token, 0, true, None)
                            .execute(&mut *tx)
                            .await?;
                    } else {
                        let additional = secondary
                            .into_iter()
                            .map(|(record, profile)| {
                                Ok(read::Storage {
                                    connection: crate::storage::model::Connection::unprotect(
                                        &record, key,
                                    )?,
                                    revision: profile.revision,
                                    profile,
                                })
                            })
                            .collect::<Result<Vec<_>>>()?;
                        let material = work::Materials {
                            additional,
                            storage: read::Storage {
                                connection: crate::storage::model::Connection::unprotect(
                                    &record, key,
                                )?,
                                revision: profile.revision,
                                profile,
                            },
                            keys,
                        };
                        encrypted = Some(codec::protect(
                            &serde_json::to_vec(&material)?,
                            key,
                            token.as_bytes(),
                        )?);
                        q::task_lease(task, token).execute(&mut *tx).await?;
                        assigned = Some(task);
                        plan = Some(value);
                    }
                } else {
                    queries::requeue_owned_task(task, token)
                        .execute(&mut *tx)
                        .await?;
                }
                if assigned.is_none() {
                    q::release_permits(&permit_tokens).execute(&mut *tx).await?;
                    if let Some(parent) = parent {
                        queries::update_parent_progress(parent)
                            .execute(&mut *tx)
                            .await?;
                    }
                }
            } else {
                q::release_permits(&permit_tokens).execute(&mut *tx).await?;
                queries::requeue_owned_task(task, token)
                    .execute(&mut *tx)
                    .await?;
            }
        }

        if assigned.is_some() || task.is_none() {
            break;
        }
        let terminal: bool =
            sqlx::query_scalar("SELECT state IN ('completed','failed') FROM tasks WHERE id=$1")
                .bind(task.unwrap())
                .fetch_one(&mut *tx)
                .await?;
        if !terminal {
            break;
        }
    }

    q::insert(
        node,
        &input,
        &hash,
        assigned,
        token,
        maintenance,
        plan.as_ref(),
        encrypted.as_deref(),
    )
    .execute(&mut *tx)
    .await?;
    let row = q::batch(node.node_id, input.operation)
        .fetch_one(&mut *tx)
        .await?;
    let value = reply(node, key, &input, row)?;
    tx.commit().await?;
    Ok(value)
}

pub(crate) async fn checkpoint_transfer(
    pool: &sqlx::PgPool,
    node: &NodeState,
    key: &Key,
    run: Run,
    operation: Uuid,
    progress: &crate::model::transfer::RawProgress,
) -> Result<()> {
    progress.validate()?;
    let mut db = pool.begin().await?;
    ensure!(
        q::writable().fetch_one(&mut *db).await?,
        Error::OperationAborted
    );
    authenticate_rpc_node(&mut db, node, key, false).await?;
    require_node_run(&mut db, node, run, true).await?;
    ensure!(
        matches!(node.role.as_str(), "worker" | "node"),
        Error::NodeInactive
    );
    let row = q::batch(node.node_id, operation)
        .fetch_optional(&mut *db)
        .await?
        .ok_or(Error::NodeExpired)?;
    owned(node, run, &row)?;
    ensure!(
        row.valid_for_ms > 0 && row.result_hash.is_none(),
        Error::NodeExpired
    );
    let plan = row.plan.context("work plan missing")?.0;
    let mut tokens = vec![row.token, row.maintenance_token];
    tokens.extend_from_slice(plan.source_permits());
    ensure!(
        q::live_permits(&tokens).fetch_one(&mut *db).await?,
        Error::NodeExpired
    );
    let work::Plan::RawTransfer { transfer: plan, .. } = plan else {
        anyhow::bail!(Error::NodeOperationConflict)
    };
    let task = Execution {
        id: row.task_id.context("work task missing")?,
        token: row.token,
    };
    ensure!(
        q::task(task.id, task.token, node.node_id)
            .fetch_optional(&mut *db)
            .await?
            .is_some(),
        Error::NodeExpired
    );
    let run = super::super::replication_transactions::owned(&mut db, task).await?;
    super::super::transfer_transactions::raw_progress(&mut db, task, &run, &plan, progress).await?;
    db.commit().await?;
    Ok(())
}
