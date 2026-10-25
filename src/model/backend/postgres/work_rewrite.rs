use super::*;
use crate::model::packing::RewriteTask;
use std::collections::{BTreeMap, BTreeSet};

async fn validate(
    db: &mut PgConnection,
    job: &RewriteTask,
    revision: i64,
) -> Result<Option<&'static str>> {
    if job.profile_revision != 0 && job.profile_revision != revision {
        return Ok(Some("rewrite_profile_changed"));
    }
    if let Some(id) = job.target_key_id {
        let current = queries::active_block_key(id)
            .fetch_optional(&mut *db)
            .await?;
        if !current.is_some_and(|(name, algorithm)| {
            name == job.target_key && algorithm == job.target_algorithm
        }) {
            return Ok(Some("rewrite_key_changed"));
        }
    } else {
        ensure!(job.target_algorithm == "none", Error::NodeOperationConflict);
    }
    Ok(None)
}

async fn nonce(
    db: &mut PgConnection,
    key: Option<Uuid>,
    created: chrono::DateTime<chrono::Utc>,
    tag: u8,
) -> Result<Option<Vec<u8>>> {
    let Some(key) = key else {
        return Ok(None);
    };
    let (id, _) = reserve_nonce_in(db, key, tag, 1).await?;
    Ok(Some(codec::typed_nonce(created, id, tag)?.to_vec()))
}

async fn keys(db: &mut PgConnection, ids: &[Uuid], key: &Key) -> Result<Vec<read::BlockKey>> {
    queries::node_read_keys(ids)
        .fetch_all(db)
        .await?
        .into_iter()
        .map(|record| read::BlockKey::unprotect(record, key))
        .collect()
}

pub(super) async fn prepare(
    tx: &mut Transaction<'_, Postgres>,
    task: Execution,
    detail: &Value,
    profile: &crate::storage::profile::StorageProfile,
    maximum: usize,
    key: &Key,
) -> Result<Option<(work::Plan, Vec<read::BlockKey>)>> {
    let job: RewriteTask = serde_json::from_value(detail.clone())?;
    ensure!(
        job.storage_id == profile.storage_id
            && job.shards > 0
            && (0..job.shards).contains(&job.shard),
        Error::NodeOperationConflict
    );
    if let Some(error) = validate(tx, &job, profile.revision).await? {
        q::finish(task.id, task.token, 0, true, Some(error))
            .execute(&mut **tx)
            .await?;
        return Ok(None);
    }
    let cursor: Option<String> = sqlx::query_scalar("SELECT cursor FROM tasks WHERE id=$1")
        .bind(task.id)
        .fetch_one(&mut **tx)
        .await?;
    let mut after: Id = cursor.as_deref().unwrap_or("0").parse()?;
    let packed = queries::rewrite_pack_candidate(&mut **tx, after, &job).await?;
    let budget = profile
        .maintenance_byte_budget
        .as_deref()
        .map(crate::config::bytes)
        .transpose()?
        .unwrap_or(64 * 1024 * 1024);
    let mut size = 0u64;
    let mut sources = Vec::new();
    while sources.len() < maximum {
        let Some(source) = queries::rewrite_loose_candidate(&mut **tx, after, &job).await? else {
            break;
        };
        if packed.is_some_and(|(first, _)| first < source.id)
            || (!sources.is_empty() && size.saturating_add(source.raw_size as u64) > budget)
        {
            break;
        }
        size += source.raw_size as u64;
        after = source.id;
        sources.push(source);
    }
    let mut items = Vec::new();
    let mut blocks = BTreeSet::new();
    blocks.extend(job.target_key_id);
    if !sources.is_empty() {
        let ids: Vec<_> = sources.iter().map(|source| source.id).collect();
        pin_inputs(tx, task.id, &ids).await?;
        for source in sources {
            blocks.extend(source.block_key_id);
            let mut target = source.clone();
            target.algorithm = job.target_algorithm.clone();
            target.block_key_id = job.target_key_id;
            target.created_at = queries::node_clock().fetch_one(&mut **tx).await?;
            target.encoding_id = sqlx::query_scalar("INSERT INTO chunk_encodings(chunk_id,storage_id,standalone_compressed,state,created_at,unreferenced_at,owner_task,encryption_algorithm,block_key_id) VALUES($1,$2,false,'uploading',$3,clock_timestamp(),$4,$5,$6) RETURNING encoding_id")
                .bind(target.id).bind(target.storage_id).bind(target.created_at).bind(task.id).bind(&target.algorithm).bind(target.block_key_id).fetch_one(&mut **tx).await?;
            let Id::Uuid(location) = target.encoding_id else {
                return Err(Error::NodeOperationConflict.into());
            };
            target.location_id = location;
            target.stored_size = None;
            target.compressed = false;
            target.nonce = nonce(
                tx,
                target.block_key_id,
                target.created_at,
                crate::content::nonce::CHUNK,
            )
            .await?;
            sqlx::query("UPDATE chunk_encodings SET nonce=$2,nonce_id=$3 WHERE encoding_id=$1")
                .bind(target.encoding_id)
                .bind(&target.nonce)
                .bind(crate::content::nonce::id(target.nonce.as_deref()))
                .execute(&mut **tx)
                .await?;
            items.push(work::Rewrite::Chunk {
                location: source.location_id,
                source,
                target,
            });
        }
    } else if let Some((cursor, id)) = packed {
        let source = queries::rewrite_pack(id).fetch_one(&mut **tx).await?;
        let mut offset = 0;
        let members: Vec<work::Member> = queries::rewrite_members(id)
            .fetch_all(&mut **tx)
            .await?
            .into_iter()
            .map(|chunk| {
                let member = (chunk.id, offset, chunk.raw_size, chunk.hash);
                offset += i64::from(chunk.raw_size);
                member
            })
            .collect();
        ensure!(
            members.len() == source.member_count as usize
                && members.len() <= crate::content::pack::MAX_MEMBERS,
            Error::NodeOperationConflict
        );
        crate::model::source::manifest_digest(&source, &members)?;
        let current = queries::rewrite_current_members(id)
            .fetch_all(&mut **tx)
            .await?;
        pin_inputs(tx, task.id, &current).await?;
        blocks.extend(source.block_key_id);
        let mut target: crate::content::pack::Pack = sqlx::query_as("INSERT INTO packs(storage_id,encryption_algorithm,block_key_id,raw_size,member_count,state,unreferenced_at,owner_task) VALUES($1,$2,$3,$4,$5,'preparing',clock_timestamp(),$6) RETURNING *")
            .bind(job.storage_id).bind(&job.target_algorithm).bind(job.target_key_id).bind(source.raw_size).bind(source.member_count).bind(task.id).fetch_one(&mut **tx).await?;
        target.nonce = nonce(
            tx,
            target.block_key_id,
            target.created_at,
            crate::content::nonce::PACK,
        )
        .await?;
        sqlx::query("UPDATE packs SET nonce=$2,nonce_id=$3 WHERE id=$1")
            .bind(target.id)
            .bind(&target.nonce)
            .bind(crate::content::nonce::id(target.nonce.as_deref()))
            .execute(&mut **tx)
            .await?;
        items.push(work::Rewrite::Pack {
            location: source.location_id,
            source,
            target,
            members,
            current,
            cursor,
        });
    } else {
        q::finish(task.id, task.token, 0, true, None)
            .execute(&mut **tx)
            .await?;
        return Ok(None);
    }
    let keys = keys(tx, &blocks.into_iter().collect::<Vec<_>>(), key).await?;
    let plan = work::Plan::Rewrite {
        storage: job.storage_id,
        items,
    };
    ensure!(
        serde_json::to_vec(&plan)?.len() <= 3 * 1024 * 1024,
        Error::NodeWorkLimit
    );
    Ok(Some((plan, keys)))
}

pub(super) async fn finish(
    tx: &mut Transaction<'_, Postgres>,
    task: Execution,
    detail: &Value,
    items: Vec<work::Rewrite>,
    outcomes: &[work::Outcome],
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
    if let Some(error) = validate(tx, &job, profile.revision).await? {
        return Ok(Some(error));
    }
    if outcomes
        .iter()
        .any(|outcome| matches!(outcome, work::Outcome::Retry))
    {
        ensure!(
            outcomes.iter().all(|outcome| matches!(
                outcome,
                work::Outcome::Encoded { .. } | work::Outcome::Retry
            )),
            Error::NodeOperationConflict
        );
        return Ok(Some("backend_io_failed"));
    }
    let blocks: BTreeMap<_, _> = keys(tx, &job.target_key_id.into_iter().collect::<Vec<_>>(), key)
        .await?
        .into_iter()
        .map(|key| Ok((key.id, codec::Key::new(&key.algorithm, key.material)?)))
        .collect::<Result<_>>()?;
    let ids: Vec<Id> = items
        .iter()
        .flat_map(|item| match item {
            work::Rewrite::Chunk { source, .. } => vec![source.id],
            work::Rewrite::Pack { current, .. } => current.clone(),
        })
        .collect();
    if let Err(error) = check_source_inputs(tx, task.id, &ids).await {
        if error.downcast_ref::<Error>().is_some_and(|error| {
            matches!(error, Error::OperationAborted | Error::SourceUnavailable)
        }) {
            return Ok(Some("source_changed"));
        }
        return Err(error);
    }
    for (item, outcome) in items.into_iter().zip(outcomes) {
        let work::Outcome::Encoded {
            size,
            compressed,
            digest,
        } = outcome
        else {
            return Err(Error::NodeOperationConflict.into());
        };
        match item {
            work::Rewrite::Chunk {
                source, mut target, ..
            } => {
                ensure!(digest.is_none(), Error::NodeOperationConflict);
                target.stored_size =
                    Some(i32::try_from(*size).map_err(|_| Error::NodeOperationConflict)?);
                target.compressed = *compressed;
                codec::validate_metadata(&target, &blocks)
                    .map_err(|_| Error::NodeOperationConflict)?;
                commit_rewrite_chunk_in(tx, task.id, task.token, &source, &target).await?;
            }
            work::Rewrite::Pack {
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
                    .validate(&blocks)
                    .map_err(|_| Error::NodeOperationConflict)?;
                let members: Vec<_> = members
                    .iter()
                    .map(|(id, offset, _, _)| (*id, *offset))
                    .collect();
                commit_rewrite_pack_in(
                    tx, task.id, task.token, &source, &target, &current, &members, cursor,
                )
                .await?;
            }
        }
    }
    Ok(None)
}
