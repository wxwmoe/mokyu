use super::{
    Backend,
    node_writes::{self as writes, Write},
    queries::{self, node_registry as registry, node_writes as ledger, read_grants, work as q},
    transactions,
};
use crate::{
    authorization::Permit,
    cluster::NodeState,
    content::codec::Key,
    model::{
        Error, Id, StreamCreate, StreamPurpose, node_read as read, node_work as work,
        node_write as write, object::StorageMigration, session::Run, task::Execution,
    },
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
struct Snapshot {
    candidate: Id,
    write: Uuid,
    read: Uuid,
    node: Uuid,
}

fn job(detail: &Value) -> Result<StorageMigration> {
    let job: StorageMigration =
        serde_json::from_value(detail.get("job").unwrap_or(detail).clone())?;
    ensure!(
        matches!(job.action.as_str(), "copy" | "move"),
        Error::NodeOperationConflict
    );
    Ok(job)
}
async fn source(
    tx: &mut registry::Tx<'_>,
    job: &StorageMigration,
) -> Result<crate::app::StoredStream> {
    transactions::selected_object(
        tx,
        job.source_bucket,
        &job.source_key,
        Some(job.source_version),
    )
    .await?;
    Ok(read_grants::node_read_stream(job.source_version)
        .fetch_one(&mut **tx)
        .await?)
}
pub(super) async fn offer(
    tx: &mut registry::Tx<'_>,
    node: &NodeState,
    run: Run,
    task: Execution,
    detail: &Value,
    key: &Key,
) -> Result<Option<(work::Plan, Vec<u8>)>> {
    let job = job(detail)?;
    if !detail["replica_parked"].is_null()
        && transactions::resume_migration_in(tx, task, &job).await?
    {
        return Ok(None);
    }
    let selected = async {
        let src = source(tx, &job).await?;
        transactions::selected_object(tx, job.target_bucket, &job.target_key, job.target_version)
            .await?;
        Ok::<_, anyhow::Error>(src)
    }
    .await;
    let original = match selected {
        Ok(s) => s,
        Err(err)
            if err
                .downcast_ref::<s3s::S3Error>()
                .is_some_and(|e| e.code() == &s3s::S3ErrorCode::PreconditionFailed) =>
        {
            queries::finish_task(task, "failed", Some("object_changed"))
                .execute(&mut **tx)
                .await?;
            return Ok(None);
        }
        Err(err) => return Err(err),
    };
    let target = queries::bucket_by_id(job.target_bucket)
        .fetch_one(&mut **tx)
        .await?
        .storage_id
        .ok_or(Error::SourceUnavailable)?;
    let source_storage = queries::bucket_by_id(job.source_bucket)
        .fetch_one(&mut **tx)
        .await?
        .storage_id
        .ok_or(Error::SourceUnavailable)?;
    let mut storage = super::node_work::material(tx, target, key).await?;
    let size = u64::try_from(original.size)?;
    if let Some(limit) = storage
        .profile
        .migration_byte_budget
        .as_deref()
        .map(crate::config::bytes)
        .transpose()?
        && limit > 0
        && size > limit
    {
        queries::finish_task(task, "failed", Some("migration_byte_budget_exceeded"))
            .execute(&mut **tx)
            .await?;
        return Ok(None);
    }
    if q::source_pending(job.source_version)
        .fetch_one(&mut **tx)
        .await?
    {
        queries::finish_task(task, "queued", Some("source_pending"))
            .execute(&mut **tx)
            .await?;
        return Ok(None);
    }
    storage.profile.upload_cache = false;
    storage.profile.write_processing = "inline".into();
    storage.profile.write_durability = None;
    let mut prior = None;
    if let Some(v) = detail.get("node_migration") {
        let s: Snapshot = serde_json::from_value(v.clone())?;
        if ledger::owned(s.node, s.write)
            .fetch_optional(&mut **tx)
            .await?
            .is_some_and(|w| w.stream_id == Some(s.candidate) && w.phase == "published")
            && q::allow_candidate(s.candidate, task.id)
                .fetch_one(&mut **tx)
                .await?
        {
            q::rebind_candidate(s.candidate, task)
                .execute(&mut **tx)
                .await?;
            prior = Some(s)
        } else {
            queries::conversion_abandon(s.candidate)
                .execute(&mut **tx)
                .await?;
            q::discard_write(s.node, s.write).execute(&mut **tx).await?;
        }
    }
    let sealed = prior.is_some();
    let state = if let Some(saved) = prior {
        saved
    } else {
        let block = q::storage(target).fetch_one(&mut **tx).await?.block_key_id;
        if let Some(id) = block {
            q::block_key(id).fetch_one(&mut **tx).await?;
        }
        let candidate = transactions::create_stream_in(
            tx,
            StreamCreate {
                authority: Permit::system(),
                bucket: job.target_bucket,
                key: job.target_key.clone(),
                purpose: StreamPurpose::Copy,
                metadata: original.metadata.clone(),
                public: job.public_read,
                completion: None,
                owner: Some((task.id, task.token)),
            },
        )
        .await?;
        let input = write::Begin {
            run,
            operation: Uuid::now_v7(),
            cached: vec![],
            intent: write::Intent {
                access: "node-migration".into(),
                credential_revision: 0,
                bucket: job.target_bucket.to_string(),
                key: job.target_key.clone(),
                metadata: original.metadata.clone(),
                public: job.public_read,
                size: Some(original.size),
                copy_read: None,
                part: None,
                completion: None,
            },
        };
        ledger::insert(
            node,
            &input,
            &input.intent.hash()?,
            candidate,
            job.target_bucket,
            target,
            block,
            &storage.profile,
            None,
        )
        .execute(&mut **tx)
        .await?;
        Snapshot {
            candidate,
            write: input.operation,
            read: Uuid::now_v7(),
            node: node.node_id,
        }
    };
    let mut detail = detail.clone();
    detail["node_migration"] = serde_json::to_value(&state)?;
    ensure!(
        q::progress(task, &detail)
            .execute(&mut **tx)
            .await?
            .rows_affected()
            == 1,
        Error::OperationAborted
    );
    let additional = if source_storage != target {
        vec![super::node_work::material(tx, source_storage, key).await?]
    } else {
        vec![]
    };
    Ok(Some((
        work::Plan::Migrate {
            storage: target,
            write: state.write,
            size,
            sealed,
        },
        serde_json::to_vec(&work::Materials {
            storage,
            additional,
            keys: vec![],
        })?,
    )))
}

pub(super) async fn read(
    backend: &Backend,
    node: &NodeState,
    key: &Key,
    input: work::ReadMigration,
) -> Result<read::Reply> {
    let mut tx = writes::start(backend, node, key, input.run).await?;
    let row = writes::lookup(&mut tx, node.node_id, input.operation).await?;
    let stream = row.active(&mut tx, node, input.run).await?;
    let task = queries::task_record(row.owner_task_id.ok_or(Error::NodeOperationConflict)?)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(
        task.task_type == "storage_migration" && task.state == "running",
        Error::NodeExpired
    );
    let saved: Snapshot = serde_json::from_value(task.detail["node_migration"].clone())?;
    ensure!(
        saved.node == node.node_id && saved.write == input.operation && saved.candidate == stream,
        Error::NodeOperationConflict
    );
    let original = source(&mut tx, &job(&task.detail)?).await?;
    let query = read::Query {
        run: input.run,
        operation: saved.read,
        route: crate::model::ReadRoute::Bucket {
            name: format!("migration:{}", input.operation),
            key: original.id.to_string(),
            alias: None,
        },
        access: None,
        head: false,
        web: false,
        mapping: true,
        protection: true,
        policy: false,
        headers: Default::default(),
        overrides: Default::default(),
        cached: input.cached,
        max_items: 256,
        lanes: 1,
        permits: Default::default(),
        max_bytes: read::TARGET_BYTES,
        body: None,
    };
    let mut reply = read::Reply::new(read::Outcome::Ready);
    let object = read::Object::from_stream(&original)?;
    reply.object = Some(reply.material(
        node.deployment_id,
        read::MaterialType::Object,
        object.id.to_string(),
        &object,
        &query.cached,
    )?);
    reply.length = original.size as u64;
    reply.etag = original.etag.clone();
    let reply = super::node_reads::protect_node_read(
        &mut tx,
        node,
        key,
        &query,
        &original,
        reply,
        &mut Default::default(),
    )
    .await?;
    ensure!(
        reply.outcome != read::Outcome::Pending,
        Error::NodeSourcePending
    );
    ensure!(
        reply.outcome == read::Outcome::Ready,
        Error::SourceUnavailable
    );
    tx.commit().await?;
    Ok(reply)
}
pub(super) async fn seal(
    tx: &mut registry::Tx<'_>,
    row: &Write,
    task: Uuid,
    input: &write::Publish,
) -> Result<()> {
    let record = queries::task_record(task).fetch_one(&mut **tx).await?;
    ensure!(
        record.task_type == "storage_migration" && record.state == "running",
        Error::NodeExpired
    );
    let saved: Snapshot = serde_json::from_value(record.detail["node_migration"].clone())?;
    let original = source(tx, &job(&record.detail)?).await?;
    ensure!(
        saved.write == row.operation_id
            && row.stream_id == Some(saved.candidate)
            && original.size == input.size
            && original.etag == input.etag
            && original.checksums == serde_json::to_value(&input.checksums)?
            && input.if_match.is_none()
            && input.if_none.is_none(),
        Error::NodeOperationConflict
    );
    Ok(())
}
pub(super) async fn finish(
    tx: &mut registry::Tx<'_>,
    task: Execution,
    outcome: &work::Outcome,
) -> Result<()> {
    if matches!(outcome, work::Outcome::Retry) {
        queries::finish_task(task, "queued", Some("backend_io_failed"))
            .execute(&mut **tx)
            .await?;
        return Ok(());
    }
    ensure!(
        matches!(outcome, work::Outcome::Migrated),
        Error::NodeOperationConflict
    );
    let record = queries::task_record(task.id).fetch_one(&mut **tx).await?;
    ensure!(
        record.state == "running" && record.execution_token == Some(task.token),
        Error::NodeExpired
    );
    let saved: Snapshot = serde_json::from_value(record.detail["node_migration"].clone())?;
    let job = job(&record.detail)?;
    let row = writes::lookup(tx, saved.node, saved.write).await?;
    ensure!(
        row.phase == "published" && row.stream_id == Some(saved.candidate),
        Error::NodeOperationConflict
    );
    let mut nested = sqlx::Acquire::begin(&mut **tx).await?;
    if let Err(err) =
        transactions::complete_storage_migration_in(&mut nested, task, saved.candidate, &job).await
    {
        nested.rollback().await?;
        let reason = if err
            .downcast_ref::<s3s::S3Error>()
            .is_some_and(|e| e.code() == &s3s::S3ErrorCode::PreconditionFailed)
        {
            Some("object_changed")
        } else if crate::model::error(&err) == Some(Error::QuotaExceeded) {
            Some("quota_exceeded")
        } else {
            None
        };
        if let Some(reason) = reason {
            queries::conversion_abandon(saved.candidate)
                .execute(&mut **tx)
                .await?;
            queries::finish_task(task, "failed", Some(reason))
                .execute(&mut **tx)
                .await?;
            return Ok(());
        }
        return Err(err);
    }
    nested.commit().await?;
    Ok(())
}
