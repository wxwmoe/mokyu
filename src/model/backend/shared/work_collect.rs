use super::{
    node_work::material,
    queries::{self, node_registry::Tx, work as q},
    transactions,
};
use crate::{
    authorization::Permit,
    config,
    content::codec::Key,
    model::{
        Error,
        node_work::{Deletion, Materials, Outcome, Physical, Plan},
        task::Execution,
    },
};
use anyhow::{Result, ensure};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use uuid::Uuid;

async fn cleanup(tx: &mut Tx<'_>, storage: Uuid, maximum: i64, retention: f64) -> Result<usize> {
    let mut count = 0;
    for id in queries::collect_uploads(&mut **tx, maximum, Some(storage)).await? {
        count +=
            usize::from(transactions::abort_multipart_in(tx, &Permit::system(), id, true).await?);
    }
    for stream in queries::stale_streams(&[], 1, Some(storage), Some(retention))
        .build_query_scalar()
        .fetch_all(&mut **tx)
        .await?
    {
        if transactions::claim_stale_stream_in(tx, stream, Some(storage), Some(retention)).await? {
            count += transactions::cleanup_stream_in(tx, stream, maximum).await?;
        }
    }
    for id in queries::stale_fragments(maximum, &[], Some(storage))
        .build_query_scalar()
        .fetch_all(&mut **tx)
        .await?
    {
        if let Some((_, _, _, file, staged, _)) = queries::lock_cleanup_fragment(id)
            .fetch_optional(&mut **tx)
            .await?
        {
            if staged.is_some() || file.is_some() {
                queries::remove_fragment_row(id).execute(&mut **tx).await?;
                count += 1;
            }
        }
    }
    count += usize::try_from(queries::remove_tombstones(&mut **tx, Some(storage), maximum).await?)?;
    Ok(count)
}

pub(super) async fn offer(
    tx: &mut Tx<'_>,
    task: Execution,
    detail: &Value,
    reclaim: bool,
    max_items: usize,
    key: &Key,
) -> Result<Option<(Plan, Vec<u8>)>> {
    let storage = serde_json::from_value(detail["storage_id"].clone())?;
    let destination = material(tx, storage, key).await?;
    let profile = &destination.profile;
    let maximum = max_items.min(crate::model::node_work::MAX_ITEMS);
    ensure!(maximum > 0, Error::NodeWorkLimit);
    let grace = config::seconds(&profile.unreferenced_grace)?;
    let minimum = profile.min_storage_seconds()?;
    let retention = config::seconds(&profile.deleted_chunk_retention)?;
    let mut count = cleanup(
        tx,
        storage,
        maximum as i64,
        if reclaim {
            config::seconds(&profile.upload_retention)?
        } else {
            retention
        } as f64,
    )
    .await?;
    let mut items = Vec::new();
    if reclaim {
        let budget = profile
            .maintenance_byte_budget
            .as_deref()
            .map(config::bytes)
            .transpose()?
            .filter(|v| *v > 0);
        let mut bytes = 0u64;
        for (id, size) in
            queries::collect_chunks(&mut **tx, storage, grace as f64, maximum as i64).await?
        {
            let next = bytes.saturating_add(u64::try_from(size)?);
            if bytes > 0 && budget.is_some_and(|n| next > n) {
                break;
            }
            if queries::lock_reclaim_chunk(id, storage, grace as f64)
                .fetch_optional(&mut **tx)
                .await?
                .is_some()
            {
                queries::claim_reclaim_chunk(id).execute(&mut **tx).await?;
                queries::retire_chunk_encodings(id)
                    .execute(&mut **tx)
                    .await?;
                queries::finish_collect_chunk(&mut **tx, id, storage).await?;
                bytes = next;
                count += 1;
            }
        }
        queries::collect_empty_packs(&mut **tx, storage).await?;
        for source in [Physical::Chunk, Physical::Pack] {
            let remaining = maximum - items.len();
            if remaining == 0 {
                break;
            }
            for (identity, location, unknown) in queries::collect_physical(
                &mut **tx,
                source.packed(),
                grace as f64,
                remaining as i64,
                minimum as f64,
                storage,
            )
            .await?
            {
                if !q::active_location(storage, location)
                    .fetch_one(&mut **tx)
                    .await?
                    && queries::claim_collect_physical(
                        &mut **tx,
                        source.packed(),
                        identity,
                        grace as f64,
                        minimum as f64,
                    )
                    .await?
                {
                    items.push(Deletion {
                        source: source.clone(),
                        identity,
                        location,
                        inspect_age: minimum > 0 && unknown,
                    });
                }
            }
        }
        let remaining = maximum - items.len();
        if remaining > 0 {
            for raw in queries::collect_raw_sources(
                &mut **tx,
                storage,
                grace as i64,
                minimum as i64,
                remaining as i64,
            )
            .await?
            {
                if raw.medium == "shared_s3"
                    && !raw.pending_upload
                    && !q::active_location(storage, raw.address())
                        .fetch_one(&mut **tx)
                        .await?
                    && queries::claim_collect_raw(
                        &mut **tx,
                        raw.id,
                        raw.revision,
                        grace as i64,
                        minimum as i64,
                    )
                    .await?
                {
                    items.push(Deletion {
                        source: Physical::Raw {
                            upload: queries::collect_raw_upload(&mut **tx, raw.id).await?,
                        },
                        identity: raw.id,
                        location: raw.address(),
                        inspect_age: false,
                    });
                }
            }
        }
    }
    if items.len() < maximum {
        for (_, path) in queries::staging::old(Some(storage))
            .fetch_all(&mut **tx)
            .await?
        {
            ensure!(
                path.starts_with(crate::storage::STAGING_PREFIX) && path.len() <= 256,
                Error::NodeWorkLimit
            );
            let location = Uuid::from_bytes(
                blake3::hash(format!("{storage}/{path}").as_bytes()).as_bytes()[..16].try_into()?,
            );
            if !q::active_location(storage, location)
                .fetch_one(&mut **tx)
                .await?
            {
                items.push(Deletion {
                    source: Physical::Staging { key: path },
                    identity: location.into(),
                    location,
                    inspect_age: false,
                });
            }
            if items.len() == maximum {
                break;
            }
        }
    }
    if items.is_empty() {
        advance(tx, task, count, 0, count > 0).await?;
        return Ok(None);
    }
    let now = queries::pack_work::clock(&mut **tx).await?;
    let plan = Plan::Collect {
        storage,
        delete_before: now - chrono::Duration::seconds(minimum as i64),
        retention,
        metadata_count: count,
        items,
    };
    Ok(Some((
        plan,
        serde_json::to_vec(&Materials {
            storage: destination,
            additional: vec![],
            keys: vec![],
        })?,
    )))
}

async fn advance(
    tx: &mut Tx<'_>,
    task: Execution,
    count: usize,
    physical: usize,
    again: bool,
) -> Result<()> {
    transactions::finish_lifecycle_policy_in(
        tx,
        task,
        i64::try_from(count)?,
        again,
        json!({"local_cleaned":count,"physical_reclaimed":physical}),
    )
    .await?;
    let row = queries::task_record(task.id).fetch_one(&mut **tx).await?;
    if row.execution_token == Some(task.token) {
        queries::finish_task(
            task,
            if row.state == "completed" {
                "completed"
            } else {
                "queued"
            },
            None,
        )
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

pub(super) async fn finish(
    tx: &mut Tx<'_>,
    task: Execution,
    storage: Uuid,
    before: DateTime<Utc>,
    retention: u64,
    count: usize,
    items: &[Deletion],
    outcomes: &[Outcome],
) -> Result<()> {
    ensure!(items.len() == outcomes.len(), Error::NodeOperationConflict);
    let mut deleted = 0;
    let mut retry = false;
    for (item, answer) in items.iter().zip(outcomes) {
        let stored = match answer {
            Outcome::Deleted { stored_at } => {
                ensure!(
                    !item.inspect_age || stored_at.is_none_or(|v| v <= before),
                    Error::NodeOperationConflict
                );
                *stored_at
            }
            Outcome::Retained { stored_at } if item.inspect_age && *stored_at > before => {
                Some(*stored_at)
            }
            Outcome::Retry => {
                retry = true;
                continue;
            }
            _ => anyhow::bail!(Error::NodeOperationConflict),
        };
        if let Some(stored) = stored {
            ensure!(
                matches!(item.source, Physical::Chunk | Physical::Pack),
                Error::NodeOperationConflict
            );
            queries::collect_age(&mut **tx, item.source.packed(), item.identity, stored).await?;
        }
        if matches!(answer, Outcome::Deleted { .. }) {
            match &item.source {
                Physical::Chunk | Physical::Pack => {
                    queries::finish_collect_physical(
                        &mut **tx,
                        item.source.packed(),
                        item.identity,
                        retention as f64,
                    )
                    .await?
                }
                Physical::Raw { .. } => {
                    queries::finish_collect_raw(&mut **tx, item.identity).await?
                }
                Physical::Staging { key } => {
                    queries::staging::release(storage, key)
                        .execute(&mut **tx)
                        .await?;
                }
            }
            deleted += 1;
        }
    }
    advance(tx, task, count, deleted, true).await?;
    if retry {
        queries::work::retry_collect(task.id)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}
