use super::{
    node_work::material,
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
        node_write as write,
        raw::{Conversion, RawSource},
        session::Run,
        task::Execution,
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
    node: Uuid,
    source: RawSource,
}
async fn retire(tx: &mut registry::Tx<'_>, snap: &Snapshot) -> Result<()> {
    queries::conversion_abandon(snap.candidate)
        .execute(&mut **tx)
        .await?;
    q::discard_write(snap.node, snap.write)
        .execute(&mut **tx)
        .await?;
    Ok(())
}
pub(super) async fn offer(
    tx: &mut registry::Tx<'_>,
    node: &NodeState,
    run: Run,
    task: Execution,
    detail: &Value,
    key: &Key,
) -> Result<Option<(work::Plan, Vec<u8>)>> {
    let job: Conversion = serde_json::from_value(detail.clone())?;
    job.encoding.validate()?;
    let prior = detail
        .get("node_conversion")
        .cloned()
        .map(serde_json::from_value::<Snapshot>)
        .transpose()?;
    if queries::conversion_current(job.stream, job.payload_revision)
        .fetch_optional(&mut **tx)
        .await?
        .is_none()
    {
        if let Some(saved) = prior {
            retire(tx, &saved).await?;
        }
        queries::finish_task(task, "completed", None)
            .execute(&mut **tx)
            .await?;
        return Ok(None);
    }
    let original = read_grants::node_read_stream(job.stream)
        .fetch_one(&mut **tx)
        .await?;
    let source = queries::raw_source(job.source).fetch_one(&mut **tx).await?;
    source.validate()?;
    let local = source.pending_upload
        && source.medium == "local"
        && queries::node_files::owner(source.id)
            .fetch_optional(&mut **tx)
            .await?
            == Some(Some(node.node_id));
    ensure!(
        (local || !source.pending_upload && source.medium == "shared_s3")
            && original.size == source.size
            && original.payload_type == "raw"
            && job.target_key_id == job.encoding.block_key_id,
        Error::SourceUnavailable
    );
    let part = original.stream_type == "part";
    let mut storage = material(tx, source.storage_id, key).await?;
    job.encoding.apply(&mut storage.profile)?;
    let mut keys = Vec::new();
    if let Some(block) = job.target_key_id {
        let record = q::block_key(block).fetch_one(&mut **tx).await?;
        ensure!(
            record.algorithm == job.encoding.algorithm,
            Error::SourceUnavailable
        );
        keys.push(read::BlockKey::unprotect(record, key)?);
    }
    let mut retained = None;
    if let Some(saved) = prior {
        let valid = queries::conversion_owned(saved.candidate, task.id, task.token)
            .fetch_optional(&mut **tx)
            .await?
            .is_some()
            || q::allow_candidate(saved.candidate, task.id)
                .fetch_one(&mut **tx)
                .await?;
        let sealed = ledger::owned(saved.node, saved.write)
            .fetch_optional(&mut **tx)
            .await?
            .is_some_and(|w| w.phase == "published" && w.stream_id == Some(saved.candidate));
        if valid && sealed && source == saved.source {
            q::rebind_candidate(saved.candidate, task)
                .execute(&mut **tx)
                .await?;
            retained = Some(saved);
        } else {
            retire(tx, &saved).await?;
        }
    }
    let sealed = retained.is_some();
    let saved = if let Some(saved) = retained {
        saved
    } else {
        let stream = transactions::create_stream_in(
            tx,
            StreamCreate {
                authority: Permit::system(),
                bucket: job.bucket,
                key: job.object_key.clone(),
                purpose: if part {
                    StreamPurpose::Part
                } else {
                    StreamPurpose::Internal
                },
                metadata: original.metadata.clone(),
                public: original.public_read,
                completion: None,
                owner: Some((task.id, task.token)),
            },
        )
        .await?;
        let part = if part {
            let (upload, number) = q::part_owner(job.stream).fetch_one(&mut **tx).await?;
            Some(write::Part {
                upload,
                number,
                algorithm: None,
            })
        } else {
            None
        };
        let input = write::Begin {
            operation: Uuid::now_v7(),
            run,
            cached: vec![],
            intent: write::Intent {
                access: "node-conversion".into(),
                credential_revision: 0,
                bucket: job.bucket.to_string(),
                key: job.object_key.clone(),
                metadata: original.metadata.clone(),
                public: original.public_read,
                size: Some(source.size),
                copy_read: None,
                part,
                completion: None,
            },
        };
        ledger::insert(
            node,
            &input,
            &input.intent.hash()?,
            stream,
            job.bucket,
            source.storage_id,
            job.target_key_id,
            &storage.profile,
            None,
        )
        .execute(&mut **tx)
        .await?;
        Snapshot {
            candidate: stream,
            write: input.operation,
            node: node.node_id,
            source: source.clone(),
        }
    };
    let mut detail = detail.clone();
    detail["node_conversion"] = serde_json::to_value(&saved)?;
    ensure!(
        q::progress(task, &detail)
            .execute(&mut **tx)
            .await?
            .rows_affected()
            == 1,
        Error::OperationAborted
    );
    let storage_id = source.storage_id;
    Ok(Some((
        work::Plan::ConvertRaw {
            storage: storage_id,
            source,
            write: saved.write,
            etag: original.etag,
            checksums: original.checksums,
            sealed,
            part,
        },
        serde_json::to_vec(&work::Materials {
            storage,
            keys,
            additional: vec![],
        })?,
    )))
}
pub(super) async fn seal(
    tx: &mut registry::Tx<'_>,
    row: &Write,
    task: Uuid,
    input: &write::Publish,
) -> Result<()> {
    let record = queries::task_record(task).fetch_one(&mut **tx).await?;
    ensure!(
        record.state == "running" && record.detail["operation"] == "cdc",
        Error::NodeExpired
    );
    let saved: Snapshot = serde_json::from_value(record.detail["node_conversion"].clone())?;
    let job: Conversion = serde_json::from_value(record.detail)?;
    ensure!(
        saved.write == input.operation
            && row.stream_id == Some(saved.candidate)
            && row.owner_task_id == Some(task),
        Error::NodeOperationConflict
    );
    let original = read_grants::node_read_stream(job.stream)
        .fetch_one(&mut **tx)
        .await?;
    ensure!(
        original.payload_revision == job.payload_revision
            && original.size == input.size
            && original.etag == input.etag
            && original.checksums == serde_json::to_value(&input.checksums)?
            && input.if_match.is_none()
            && input.if_none.is_none(),
        Error::NodeOperationConflict
    );
    ensure!(
        queries::raw_source(job.source).fetch_one(&mut **tx).await? == saved.source,
        Error::SourceUnavailable
    );
    Ok(())
}
pub(super) async fn finish(
    tx: &mut registry::Tx<'_>,
    task: Execution,
    outcome: &work::Outcome,
) -> Result<&'static str> {
    if matches!(outcome, work::Outcome::Retry) {
        return Ok("queued");
    }
    ensure!(
        matches!(outcome, work::Outcome::Converted),
        Error::NodeOperationConflict
    );
    let record = queries::task_record(task.id).fetch_one(&mut **tx).await?;
    ensure!(
        record.execution_token == Some(task.token),
        Error::NodeExpired
    );
    let saved: Snapshot = serde_json::from_value(record.detail["node_conversion"].clone())?;
    let job: Conversion = serde_json::from_value(record.detail)?;
    let row = writes::lookup(tx, saved.node, saved.write).await?;
    ensure!(
        row.phase == "published" && row.stream_id == Some(saved.candidate),
        Error::NodeOperationConflict
    );
    if transactions::finish_conversion_in(tx, task, &job, &saved.source, saved.candidate).await? {
        Ok("completed")
    } else {
        Ok("queued")
    }
}
