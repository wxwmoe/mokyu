use super::*;
use crate::model::node_work as work;
use queries::work as q;

#[path = "work_sources.rs"]
mod sources;

#[path = "work_audit.rs"]
mod audit;

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
    let mut materials = vec![];
    let mut grants = vec![];
    let plan = row.plan.map(|value| value.0);
    if let Some(plan) = &plan {
        ensure!(
            row.valid_for_ms > 0 && row.result_hash.is_none(),
            Error::NodeExpired
        );
        let values: work::Materials = serde_json::from_str(&codec::unprotect(
            row.storage_encrypted
                .as_deref()
                .context("work storage is missing")?,
            key,
            row.token.as_bytes(),
        )?)?;
        read::material(
            &mut materials,
            node.deployment_id,
            read::MaterialType::Storage,
            plan.storage().to_string(),
            &values.storage,
            &input.cached,
        )?;
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
        grants.push(read::Grant {
            r#type: read::GrantType::Work,
            id: row.token,
            valid_for_ms: row.valid_for_ms as u64,
            storage: Some(plan.storage()),
        });
    }
    Ok(work::Reply {
        operation: input.operation,
        plan,
        materials,
        grants,
    })
}

async fn finish(
    db: &mut Transaction<'_, Postgres>,
    node: &NodeState,
    run: Run,
    completion: &work::Completion,
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
    ensure!(
        q::live_permits(row.token, row.maintenance_token)
            .fetch_one(&mut **db)
            .await?,
        Error::NodeExpired
    );
    let task = row.task_id.ok_or(Error::NodeExpired)?;
    let (_, detail, parent) = q::task(task, row.token, node.node_id)
        .fetch_optional(&mut **db)
        .await?
        .ok_or(Error::NodeExpired)?;
    let mut deleted = 0;
    let mut error = None;
    let mut complete = false;
    match row.plan.context("work plan is missing")?.0 {
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
    }
    q::result(node.node_id, completion.operation, &hash)
        .execute(&mut **db)
        .await?;
    queries::release_storage_permit(row.token)
        .execute(&mut **db)
        .await?;
    queries::release_storage_permit(row.maintenance_token)
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
    inspect: bool,
) -> Result<bool> {
    for (direction, limit) in [
        ("control", profile.control_concurrency),
        (
            if inspect { "integrity" } else { "maintenance" },
            if inspect {
                Some(profile.integrity_concurrency)
            } else {
                profile.maintenance_concurrency
            },
        ),
    ] {
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
    queries::insert_storage_permit(
        maintenance,
        profile.storage_id,
        if inspect { "integrity" } else { "maintenance" },
        node,
    )
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
        finish(&mut tx, node, input.run, completion).await?;
    }
    q::cleanup(node.node_id).execute(&mut *tx).await?;
    ensure!(
        q::active(node.node_id).fetch_one(&mut *tx).await? < 64,
        Error::NodeWorkLimit
    );
    let token = Uuid::new_v4();
    let maintenance = Uuid::new_v4();
    let allowed = vec![
        "gc".to_owned(),
        "cleanup".to_owned(),
        "integrity".to_owned(),
    ];
    let task = queries::claim_task(
        input.class as i32,
        &[],
        Some(node.node_id),
        token,
        session.draining,
        &allowed,
        Some(false),
    )
    .fetch_optional(&mut *tx)
    .await?;
    let mut plan = None;
    let mut encrypted = None;
    let mut assigned = None;
    if let Some(task) = task {
        let (policy, detail, parent) = q::task(task, token, node.node_id)
            .fetch_one(&mut *tx)
            .await?;
        let storage: Uuid = detail["storage_id"]
            .as_str()
            .context("work storage is missing")?
            .parse()?;
        let profile = queries::storage_profile(storage)
            .fetch_one(&mut *tx)
            .await?;
        let record = queries::node_read_storages(&[storage])
            .fetch_one(&mut *tx)
            .await?;
        ensure!(record.r#type == "s3", Error::SourceUnavailable);
        if permits(
            &mut tx,
            node.node_id,
            token,
            maintenance,
            &profile,
            policy == "integrity",
        )
        .await?
        {
            let selected = if policy == "integrity" && detail.get("source_operation").is_none() {
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
                    collect(&mut tx, storage, input.max_items, &profile, policy == "gc").await?,
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
                    let material = work::Materials {
                        storage: read::Storage {
                            connection: crate::storage::model::Connection::unprotect(&record, key)?,
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
                queries::release_storage_permit(token)
                    .execute(&mut *tx)
                    .await?;
                queries::release_storage_permit(maintenance)
                    .execute(&mut *tx)
                    .await?;
                if let Some(parent) = parent {
                    queries::update_parent_progress(parent)
                        .execute(&mut *tx)
                        .await?;
                }
            }
        } else {
            queries::requeue_owned_task(task, token)
                .execute(&mut *tx)
                .await?;
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
