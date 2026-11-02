use super::{
    node_work::material,
    queries::{self, pack_work as p, work_rewrite as q},
    transactions,
};
use crate::{
    config,
    content::{
        codec::{self, Chunk, Key},
        pack::Pack,
    },
    model::{
        Error, Id,
        node_work::{self as work, Outcome, Rewrite},
        packing::RewriteTask,
        task::Execution,
    },
};
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

async fn eligible(
    tx: &mut p::Tx<'_>,
    job: &RewriteTask,
    revision: i64,
) -> Result<Option<&'static str>> {
    if job.profile_revision != 0 && revision != job.profile_revision {
        return Ok(Some("rewrite_profile_changed"));
    }
    if let Some(id) = job.target_key_id {
        if !q::key(id)
            .fetch_optional(&mut **tx)
            .await?
            .is_some_and(|(n, a)| n == job.target_key && a == job.target_algorithm)
        {
            return Ok(Some("rewrite_key_changed"));
        }
    } else {
        ensure!(job.target_algorithm == "none", Error::NodeOperationConflict)
    }
    Ok(None)
}
async fn nonce(
    tx: &mut p::Tx<'_>,
    key: Option<Uuid>,
    created: DateTime<Utc>,
    tag: u8,
) -> Result<Option<Vec<u8>>> {
    let Some(key) = key else { return Ok(None) };
    let (first, _) = transactions::reserve_nonce_in(tx, key, tag, 1).await?;
    Ok(Some(codec::typed_nonce(created, first, tag)?.to_vec()))
}
async fn pack_members(
    tx: &mut p::Tx<'_>,
    source: &Pack,
) -> Result<(Vec<Chunk>, Vec<Id>, Vec<work::Member>, Vec<Id>)> {
    let rows = queries::rewrite_members(source.id)
        .build_query_as::<Chunk>()
        .fetch_all(&mut **tx)
        .await?;
    ensure!(
        rows.len() == source.member_count as usize
            && rows.len() <= crate::content::pack::MAX_MEMBERS,
        Error::NodeWorkLimit
    );
    let members = p::physical_members(tx, source.id).await?;
    crate::model::source::manifest_digest(source, &members)?;
    ensure!(
        rows.len() == members.len()
            && rows
                .iter()
                .zip(&members)
                .all(|(c, v)| Id::Uuid(c.uuid) == v.0 && c.raw_size == v.2 && c.hash == v.3),
        Error::SourceUnavailable
    );
    let current = queries::rewrite_current_members(source.id)
        .fetch_all(&mut **tx)
        .await?;
    let ids: BTreeMap<_, _> = rows.iter().map(|r| (r.id, r.uuid)).collect();
    let physical = current
        .iter()
        .map(|c| Ok(Id::Uuid(*ids.get(c).ok_or(Error::SourceUnavailable)?)))
        .collect::<Result<Vec<_>>>()?;
    Ok((rows, current, members, physical))
}
pub(super) async fn offer(
    tx: &mut p::Tx<'_>,
    task: Execution,
    detail: &Value,
    maximum: usize,
    key: &Key,
) -> Result<Option<(work::Plan, Vec<u8>)>> {
    let job: RewriteTask = serde_json::from_value(detail.clone())?;
    ensure!(
        job.shards == 1 && job.shard == 0 && maximum > 0,
        Error::NodeOperationConflict
    );
    let target = material(tx, job.storage_id, key).await?;
    if let Some(reason) = eligible(tx, &job, target.revision).await? {
        queries::finish_task(task, "failed", Some(reason))
            .execute(&mut **tx)
            .await?;
        return Ok(None);
    }
    let mut after: Id = p::cursor(tx, task.id)
        .await?
        .as_deref()
        .unwrap_or("0")
        .parse()?;
    let packed: Option<(Id, Id)> = queries::rewrite_pack_candidate(after, &job)
        .build_query_as()
        .fetch_optional(&mut **tx)
        .await?;
    let budget = target
        .profile
        .maintenance_byte_budget
        .as_deref()
        .map(config::bytes)
        .transpose()?
        .unwrap_or(64 * 1024 * 1024);
    let mut sources = vec![];
    let mut size = 0u64;
    while sources.len() < maximum {
        let Some(source) = queries::rewrite_loose_candidate(after, &job)
            .build_query_as::<Chunk>()
            .fetch_optional(&mut **tx)
            .await?
        else {
            break;
        };
        if packed.is_some_and(|(first, _)| first < source.id)
            || !sources.is_empty() && size.saturating_add(source.raw_size as u64) > budget
        {
            break;
        }
        size += source.raw_size as u64;
        after = source.id;
        sources.push(source);
    }
    let mut items = vec![];
    let mut keys = BTreeSet::new();
    keys.extend(job.target_key_id);
    if !sources.is_empty() {
        transactions::pin_inputs(
            tx,
            task.id,
            &sources.iter().map(|r| r.id).collect::<Vec<_>>(),
        )
        .await?;
        for source in sources {
            keys.extend(source.block_key_id);
            let mut output = source.clone();
            output.address = None;
            output.location_id = Uuid::now_v7();
            output.storage_id = Some(job.storage_id);
            output.stored_size = None;
            output.compressed = false;
            output.algorithm = job.target_algorithm.clone();
            output.block_key_id = job.target_key_id;
            output.created_at = p::clock(tx).await?;
            output.nonce = nonce(
                tx,
                output.block_key_id,
                output.created_at,
                crate::content::nonce::CHUNK,
            )
            .await?;
            output.encoding_id = transactions::prepare_encoding_in(tx, &output, task).await?;
            if output.nonce.is_some() {
                ensure!(
                    q::chunk_nonce(output.encoding_id, task.id, output.nonce.as_deref())
                        .execute(&mut **tx)
                        .await?
                        .rows_affected()
                        == 1,
                    Error::OperationAborted
                );
            }
            items.push(Rewrite::Chunk {
                location: source.location_id,
                source,
                target: output,
            });
        }
    } else if let Some((cursor, id)) = packed {
        let source = queries::pack_record(id).fetch_one(&mut **tx).await?;
        let (_, current, members, physical) = pack_members(tx, &source).await?;
        transactions::pin_inputs(tx, task.id, &current).await?;
        keys.extend(source.block_key_id);
        let mut output = transactions::prepare_pack_in(
            tx,
            job.storage_id,
            &job.target_algorithm,
            job.target_key_id,
            source.raw_size,
            source.member_count,
            task,
        )
        .await?;
        output.nonce = nonce(
            tx,
            output.block_key_id,
            output.created_at,
            crate::content::nonce::PACK,
        )
        .await?;
        if output.nonce.is_some() {
            ensure!(
                q::pack_nonce(output.id, task.id, output.nonce.as_deref())
                    .execute(&mut **tx)
                    .await?
                    .rows_affected()
                    == 1,
                Error::OperationAborted
            );
        }
        items.push(Rewrite::Pack {
            location: source.location_id,
            source,
            target: output,
            members,
            current: physical,
            cursor,
        });
    } else {
        queries::finish_task(task, "completed", None)
            .execute(&mut **tx)
            .await?;
        return Ok(None);
    }
    let stores: BTreeSet<_> = items
        .iter()
        .filter_map(Rewrite::storage)
        .filter(|v| *v != job.storage_id)
        .collect();
    let mut additional = vec![];
    for id in stores {
        additional.push(material(tx, id, key).await?);
    }
    let keys = p::keys(tx, &keys.into_iter().collect::<Vec<_>>(), key).await?;
    let plan = work::Plan::Rewrite {
        storage: job.storage_id,
        source_permits: vec![],
        items,
    };
    ensure!(
        serde_json::to_vec(&plan)?.len() <= 3 * 1024 * 1024,
        Error::NodeWorkLimit
    );
    Ok(Some((
        plan,
        serde_json::to_vec(&work::Materials {
            storage: target,
            additional,
            keys,
        })?,
    )))
}
pub(super) async fn finish(
    tx: &mut p::Tx<'_>,
    task: Execution,
    detail: &Value,
    items: Vec<Rewrite>,
    outcomes: &[Outcome],
    key: &Key,
) -> Result<Option<&'static str>> {
    ensure!(
        !items.is_empty() && items.len() <= work::MAX_ITEMS && items.len() == outcomes.len(),
        Error::NodeOperationConflict
    );
    let job: RewriteTask = serde_json::from_value(detail.clone())?;
    let profile = queries::storage_profile(job.storage_id)
        .fetch_one(&mut **tx)
        .await?;
    if let Some(reason) = eligible(tx, &job, profile.revision).await? {
        return Ok(Some(reason));
    }
    ensure!(
        outcomes
            .iter()
            .all(|o| matches!(o, Outcome::Retry | Outcome::Encoded { .. })),
        Error::NodeOperationConflict
    );
    if outcomes.iter().any(|o| matches!(o, Outcome::Retry)) {
        return Ok(Some("backend_io_failed"));
    }
    let keys = p::keys(tx, &job.target_key_id.into_iter().collect::<Vec<_>>(), key)
        .await?
        .into_iter()
        .map(|k| Ok((k.id, Key::new(&k.algorithm, k.material)?)))
        .collect::<Result<BTreeMap<_, _>>>()?;
    for (item, res) in items.into_iter().zip(outcomes) {
        let Outcome::Encoded {
            size,
            compressed,
            digest,
        } = res
        else {
            anyhow::bail!(Error::NodeOperationConflict)
        };
        match item {
            Rewrite::Chunk {
                source, mut target, ..
            } => {
                ensure!(digest.is_none(), Error::NodeOperationConflict);
                target.stored_size = Some(i32::try_from(*size)?);
                target.compressed = *compressed;
                codec::validate_metadata(&target, &keys)
                    .map_err(|_| Error::NodeOperationConflict)?;
                transactions::commit_rewrite_chunk_in(tx, task, &source, &target).await?;
            }
            Rewrite::Pack {
                source,
                mut target,
                members,
                current,
                cursor,
                ..
            } => {
                target.stored_size = Some(*size);
                target.compressed = *compressed;
                target.digest = digest.clone();
                target
                    .validate(&keys)
                    .map_err(|_| Error::NodeOperationConflict)?;
                let (rows, logical, manifest, physical) = pack_members(tx, &source).await?;
                if members != manifest || current != physical {
                    return Ok(Some("source_changed"));
                }
                transactions::commit_rewrite_pack_in(
                    tx, task, &source, &target, &logical, &rows, cursor,
                )
                .await?;
            }
        }
    }
    Ok(None)
}
