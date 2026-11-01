use super::*;
use crate::model::{
    node_raw::{File, FileState},
    node_work as work,
    raw::RawUpload,
};

pub(crate) async fn enqueue(
    db: &mut PgConnection,
    node: Uuid,
    source: Id,
    storage: Uuid,
) -> Result<()> {
    let task = Uuid::now_v7();
    crate::model::backend::postgres::transactions::insert_task(
        &mut *db,
        task,
        "upload",
        None,
        json!({"operation":"cache", "source_id":source, "storage_id":storage, "source_node":node}),
        None,
    )
    .await?;
    sqlx::query("UPDATE tasks SET execution_scope='node',target_node_id=$2 WHERE id=$1")
        .bind(task)
        .bind(node)
        .execute(db)
        .await?;
    Ok(())
}

pub(crate) async fn prepare(
    db: &mut Transaction<'_, Postgres>,
    node: Uuid,
    task: Execution,
    detail: &Value,
    maximum: usize,
    draining: bool,
) -> Result<Option<(work::Plan, Vec<read::BlockKey>)>> {
    let Some(first) = prepare_source(db, node, task, detail).await? else {
        return Ok(None);
    };
    const BYTES: u64 = 64 * 1024 * 1024;
    let storage = first.source.storage_id;
    let mut bytes = first.source.size as u64;
    let mut items = vec![first];
    for _ in 1..maximum {
        if bytes >= BYTES {
            break;
        }
        let next = queries::claim_task(
            2,
            &[],
            Some(node),
            task.token,
            draining,
            &["upload".to_owned()],
            Some(false),
            Some(storage),
        )
        .fetch_optional(&mut **db)
        .await?;
        let Some(id) = next else { break };
        let (_, detail, _) = queries::work::task(id, task.token, node)
            .fetch_one(&mut **db)
            .await?;
        let source: Id = serde_json::from_value(detail["source_id"].clone())?;
        let size: Option<i64> = sqlx::query_scalar("SELECT size FROM raw_sources WHERE id=$1")
            .bind(source)
            .fetch_optional(&mut **db)
            .await?;
        if size.is_some_and(|size| size >= 0 && size as u64 > BYTES - bytes) {
            queries::requeue_owned_task(id, task.token)
                .execute(&mut **db)
                .await?;
            break;
        }
        if let Some(item) = prepare_source(
            db,
            node,
            Execution {
                id,
                token: task.token,
            },
            &detail,
        )
        .await?
        {
            queries::work::task_lease(id, task.token)
                .execute(&mut **db)
                .await?;
            bytes += item.source.size as u64;
            items.push(item);
        }
    }
    Ok(Some((work::Plan::UploadRaw { storage, items }, vec![])))
}

async fn prepare_source(
    db: &mut Transaction<'_, Postgres>,
    node: Uuid,
    task: Execution,
    detail: &Value,
) -> Result<Option<work::UploadFile>> {
    let source: Id = serde_json::from_value(detail["source_id"].clone())?;
    let owner: Option<Uuid> =
        sqlx::query_scalar("SELECT node_id FROM raw_uploads WHERE source_id=$1")
            .bind(source)
            .fetch_optional(&mut **db)
            .await?
            .flatten();
    let Some(owner) = owner else {
        queries::work::finish(task.id, task.token, 0, true, None)
            .execute(&mut **db)
            .await?;
        return Ok(None);
    };
    ensure!(owner == node, Error::NodeAuthentication);
    let hash: Option<Vec<u8>> =
        sqlx::query_scalar("SELECT content_hash FROM raw_uploads WHERE source_id=$1")
            .bind(source)
            .fetch_one(&mut **db)
            .await?;
    let streams = if let Some(hash) = &hash {
        let storage: Uuid = serde_json::from_value(detail["storage_id"].clone())?;
        ensure!(
            queries::try_claim_chunk_hash(storage, hash)
                .fetch_one(&mut **db)
                .await?,
            Error::NodeWriteBusy
        );
        let streams = queries::raw_upload_streams(source)
            .fetch_all(&mut **db)
            .await?;
        for stream in &streams {
            queries::raw_upload_stream_lock(*stream)
                .fetch_optional(&mut **db)
                .await?;
        }
        streams
    } else {
        vec![]
    };
    if queries::raw_upload_abandon(source)
        .execute(&mut **db)
        .await?
        .rows_affected()
        == 1
    {
        queries::raw_upload_delete(source)
            .execute(&mut **db)
            .await?;
        queries::work::finish(task.id, task.token, 0, true, None)
            .execute(&mut **db)
            .await?;
        return Ok(None);
    }
    queries::raw_upload_claim(source, task.token, Some(node))
        .execute(&mut **db)
        .await?;
    let value = queries::raw_upload_record(source)
        .fetch_one(&mut **db)
        .await?;
    value.source.validate()?;
    ensure!(
        value.source.medium == "local" && value.source.pending_upload
            || value.source.medium == "shared_s3" && !value.source.pending_upload,
        Error::SourceUnavailable
    );
    if let Some(hash) = &value.content_hash {
        if reuse_raw_upload_in(db, &value, hash, &streams, Some(node)).await? {
            let complete = !sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM raw_uploads WHERE source_id=$1)",
            )
            .bind(source)
            .fetch_one(&mut **db)
            .await?;
            queries::work::finish(task.id, task.token, 0, complete, None)
                .execute(&mut **db)
                .await?;
            return Ok(None);
        }
    }
    if !value.source.pending_upload {
        complete_raw_upload_in(db, &value, value.content_hash.as_deref(), Some(node)).await?;
        queries::work::finish(
            task.id,
            task.token,
            0,
            !sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM raw_uploads WHERE source_id=$1)",
            )
            .bind(source)
            .fetch_one(&mut **db)
            .await?,
            None,
        )
        .execute(&mut **db)
        .await?;
        return Ok(None);
    }
    let hashing = value.deduplicate && value.content_hash.is_none();
    Ok(Some(work::UploadFile {
        task: task.id,
        source: value.source,
        hashing,
    }))
}

pub(crate) async fn finish(
    db: &mut Transaction<'_, Postgres>,
    node: Uuid,
    task: Execution,
    items: &[work::UploadFile],
    outcomes: &[work::Outcome],
) -> Result<(bool, Option<&'static str>)> {
    ensure!(
        !items.is_empty()
            && items.len() <= work::MAX_ITEMS
            && items.len() == outcomes.len()
            && items[0].task == task.id,
        Error::NodeOperationConflict
    );
    for item in items {
        queries::work::task(item.task, task.token, node)
            .fetch_optional(&mut **db)
            .await?
            .ok_or(Error::NodeExpired)?;
    }
    let mut first = None;
    for (item, outcome) in items.iter().zip(outcomes) {
        let execution = Execution {
            id: item.task,
            token: task.token,
        };
        let result =
            finish_source(db, node, execution, &item.source, item.hashing, outcome).await?;
        if item.task == task.id {
            first = Some(result);
        } else {
            queries::work::finish(item.task, task.token, 0, result.0, result.1)
                .execute(&mut **db)
                .await?;
        }
    }
    first.ok_or_else(|| Error::NodeOperationConflict.into())
}

async fn finish_source(
    db: &mut Transaction<'_, Postgres>,
    node: Uuid,
    task: Execution,
    source: &crate::model::raw::RawSource,
    hashing: bool,
    outcome: &work::Outcome,
) -> Result<(bool, Option<&'static str>)> {
    let hash: Option<Vec<u8>> =
        sqlx::query_scalar("SELECT content_hash FROM raw_uploads WHERE source_id=$1")
            .bind(source.id)
            .fetch_one(&mut **db)
            .await?;
    if let Some(hash) = &hash {
        ensure!(
            queries::try_claim_chunk_hash(source.storage_id, hash)
                .fetch_one(&mut **db)
                .await?,
            Error::NodeWriteBusy
        );
    }
    let current: RawUpload = queries::raw_upload_record(source.id)
        .fetch_one(&mut **db)
        .await?;
    ensure!(
        current.token == Some(task.token)
            && current.source == *source
            && queries::raw_upload_owned(&current, Some(node))
                .fetch_one(&mut **db)
                .await?,
        Error::NodeExpired
    );
    match outcome {
        work::Outcome::Retry => {
            queries::raw_upload_retry(&current, Some("backend_io_failed"))
                .execute(&mut **db)
                .await?;
            Ok((false, Some("backend_io_failed")))
        }
        work::Outcome::Hashed { hash } if hashing => {
            ensure!(hash.len() == 32, Error::NodeOperationConflict);
            queries::raw_upload_hash(source.id, Some(hash))
                .execute(&mut **db)
                .await?;
            Ok((false, None))
        }
        work::Outcome::Uploaded if !hashing => {
            complete_raw_upload_in(db, &current, current.content_hash.as_deref(), Some(node))
                .await?;
            let complete = !sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM raw_uploads WHERE source_id=$1)",
            )
            .bind(source.id)
            .fetch_one(&mut **db)
            .await?;
            Ok((complete, None))
        }
        _ => Err(Error::NodeOperationConflict.into()),
    }
}

pub(crate) async fn inventory(
    pool: &PgPool,
    node: &NodeState,
    key: &Key,
    input: crate::model::node_native::Inventory,
) -> Result<Vec<File>> {
    ensure!(
        input.files.len() <= 128
            && input.files.iter().all(|id| !id.is_nil())
            && input.files.iter().collect::<HashSet<_>>().len() == input.files.len(),
        Error::NodeReadLimit
    );
    let mut tx = pool.begin().await?;
    authenticate_rpc_node(&mut tx, node, key, false).await?;
    ensure!(
        matches!(node.role.as_str(), "data" | "node"),
        Error::NodeAuthentication
    );
    require_node_run(&mut tx, node, input.run, true).await?;
    let rows: Vec<(Uuid,Option<Uuid>,i64,Option<Uuid>,bool,bool,bool,Option<String>,Option<i32>)> = sqlx::query_as("SELECT f.id,coalesce(r.storage_id,w.storage_id),coalesce(w.size,r.size,0),u.node_id,coalesce(u.source_id IS NOT NULL OR (w.node_id=$2 AND w.active),false),EXISTS(SELECT 1 FROM node_read_sources n JOIN node_read_windows v ON v.id=n.window_id JOIN stream_leases l ON l.id=v.read_id WHERE n.kind='raw' AND n.location_id=f.id AND l.expires_at>clock_timestamp()) OR EXISTS(SELECT 1 FROM node_work n WHERE n.result_hash IS NULL AND n.expires_at>clock_timestamp() AND (n.plan->'source'->>'location_id'=f.id::text OR EXISTS(SELECT 1 FROM jsonb_array_elements(n.plan->'items') i WHERE i->'source'->>'location_id'=f.id::text))) OR coalesce(r.medium='local' AND EXISTS(SELECT 1 FROM raw_ranges e WHERE e.source_id=r.id),false),coalesce(r.state='ready' AND r.medium='shared_s3' AND NOT r.pending_upload AND r.quarantine_code IS NULL AND p.cache_enabled,false),p.cache_soft_limit,p.cache_entries FROM unnest($1::uuid[]) f(id) LEFT JOIN raw_sources r ON r.id=f.id LEFT JOIN raw_uploads u ON u.source_id=r.id LEFT JOIN storage_profiles p ON p.storage_id=r.storage_id LEFT JOIN LATERAL (SELECT x.node_id,x.size,n.storage_id,n.state='active' AND n.boot_id=$3 AND n.fence=$4 AND n.credential_generation=$5 AND EXISTS(SELECT 1 FROM stream_leases l WHERE l.id=n.lease_id AND l.expires_at>clock_timestamp()) AS active FROM node_write_raw x JOIN node_writes n ON n.node_id=x.node_id AND n.operation_id=x.operation_id WHERE x.source_id=f.id AND x.node_id=$2 ORDER BY n.created_at DESC LIMIT 1) w ON true ORDER BY f.id")
        .bind(&input.files).bind(node.node_id).bind(input.run.boot).bind(input.run.fence).bind(node.credential_generation).fetch_all(&mut *tx).await?;
    let mut result = Vec::with_capacity(rows.len());
    for (id, storage, size, owner, pending, protected, cached, cache_limit, cache_entries) in rows {
        ensure!(
            owner.is_none_or(|owner| owner == node.node_id),
            Error::NodeAuthentication
        );
        result.push(File {
            id,
            storage,
            size: size.try_into()?,
            cache_limit,
            cache_entries: cache_entries.map(u32::try_from).transpose()?,
            state: if pending {
                FileState::Pending
            } else if protected {
                FileState::Protected
            } else if cached {
                FileState::Cached
            } else {
                FileState::Removable
            },
        });
    }
    tx.commit().await?;
    Ok(result)
}
