use super::*;
use crate::model::{node_write as write, raw::Conversion};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Snapshot {
    candidate: Id,
    write: Uuid,
    node: Uuid,
    source: crate::model::raw::RawSource,
}

pub(super) fn freeze(
    detail: &Value,
    profile: &mut crate::storage::profile::StorageProfile,
) -> Result<()> {
    let job: Conversion = serde_json::from_value(detail.clone())?;
    job.encoding.validate()?;
    profile.cdc_enabled = true;
    profile.upload_cache = false;
    profile.write_processing = "inline".into();
    profile.compression_enabled = job.encoding.compression.enabled;
    profile.compression_strategy = serde_json::to_value(job.encoding.compression.strategy)?
        .as_str()
        .unwrap()
        .to_owned();
    profile.compression_level = job.encoding.compression.level;
    profile.compression_sample_level = job.encoding.compression.sample_level;
    profile.compression_min_savings_percent =
        i32::from(job.encoding.compression.min_savings_percent);
    profile.compression_min_savings_bytes =
        i64::try_from(job.encoding.compression.min_savings_bytes)?;
    profile.pack_enabled = job.encoding.pack_enabled;
    profile.pack_max_size = job.encoding.pack_max_size.clone();
    profile.pack_upload_flush_timeout = job.encoding.pack_flush_timeout;
    Ok(())
}

pub(super) async fn prepare(
    db: &mut Transaction<'_, Postgres>,
    node: &NodeState,
    run: Run,
    task: Execution,
    detail: &Value,
    profile: crate::storage::profile::StorageProfile,
) -> Result<Option<(work::Plan, Vec<read::BlockKey>)>> {
    let job: Conversion = serde_json::from_value(detail.clone())?;
    job.encoding.validate()?;
    let saved = detail
        .get("node_conversion")
        .cloned()
        .map(serde_json::from_value::<Snapshot>)
        .transpose()?;
    if queries::conversion_current(job.stream, job.payload_revision)
        .fetch_optional(&mut **db)
        .await?
        .is_none()
    {
        if let Some(saved) = saved {
            abandon(db, &saved).await?;
        }
        q::finish(task.id, task.token, 0, true, None)
            .execute(&mut **db)
            .await?;
        return Ok(None);
    }
    let original = queries::node_read_stream(job.stream)
        .fetch_one(&mut **db)
        .await?;
    let source = queries::raw_source(job.source).fetch_one(&mut **db).await?;
    source.validate()?;
    let local = source.medium == "local"
        && source.pending_upload
        && sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM raw_uploads WHERE source_id=$1 AND node_id=$2)",
        )
        .bind(source.id)
        .bind(node.node_id)
        .fetch_one(&mut **db)
        .await?;
    ensure!(
        source.storage_id == profile.storage_id
            && (local || source.medium == "shared_s3" && !source.pending_upload)
            && source.size == original.size
            && original.payload_type == "raw",
        Error::SourceUnavailable
    );
    ensure!(
        job.target_key_id == job.encoding.block_key_id,
        Error::NodeOperationConflict
    );
    if let Some(id) = job.target_key_id {
        let material = queries::node_read_keys(&[id])
            .fetch_optional(&mut **db)
            .await?
            .ok_or(Error::SourceUnavailable)?;
        ensure!(
            material.algorithm == job.encoding.algorithm,
            Error::SourceUnavailable
        );
    }
    let part = original.stream_type == "part";
    validate_raw(db, job.stream, source.size).await?;
    let mut retained = None;
    if let Some(saved) = saved {
        let sealed:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM node_writes w JOIN streams s ON s.id=w.stream_id WHERE w.node_id=$1 AND w.operation_id=$2 AND w.state='published' AND s.id=$3 AND s.state='writing' AND s.owner_task_id=$4)")
            .bind(saved.node).bind(saved.write).bind(saved.candidate).bind(task.id).fetch_one(&mut **db).await?;
        if sealed && saved.source == source {
            sqlx::query("UPDATE streams SET owner_lease_token=$2 WHERE id=$1 AND owner_task_id=$3")
                .bind(saved.candidate)
                .bind(task.token)
                .bind(task.id)
                .execute(&mut **db)
                .await?;
            retained = Some(saved);
        } else {
            abandon(db, &saved).await?;
        }
    }
    let sealed = retained.is_some();
    let snapshot = if let Some(saved) = retained {
        saved
    } else {
        let candidate = create_stream_in(
            db,
            StreamCreate {
                authority: Permit::system(),
                bucket: job.bucket,
                key: job.object_key.clone(),
                stream_type: if part { "part" } else { "object" }.into(),
                metadata: original.metadata.clone(),
                public: original.public_read,
                claim_object: false,
                completion: None,
                owner: Some((task.id, task.token)),
            },
            Some(write::LEASE_SECONDS),
        )
        .await?;
        let operation = Uuid::now_v7();
        let input = write::Begin {
            run,
            operation,
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
                part: None,
                completion: None,
            },
        };
        queries::insert_node_write(
            node,
            &input,
            &input.intent.hash()?,
            candidate,
            &profile,
            job.target_key_id,
            None,
            None,
        )
        .execute(&mut **db)
        .await?;
        Snapshot {
            candidate,
            write: operation,
            node: node.node_id,
            source: source.clone(),
        }
    };
    sqlx::query("UPDATE tasks SET detail=jsonb_set(detail,'{node_conversion}',$2) WHERE id=$1 AND lease_token=$3")
        .bind(task.id).bind(serde_json::to_value(&snapshot)?).bind(task.token).execute(&mut **db).await?;
    Ok(Some((
        work::Plan::ConvertRaw {
            storage: source.storage_id,
            source,
            write: snapshot.write,
            etag: original.etag,
            checksums: original.checksums,
            sealed,
            part,
        },
        vec![],
    )))
}

async fn abandon(db: &mut PgConnection, saved: &Snapshot) -> Result<()> {
    queries::conversion_abandon(saved.candidate)
        .execute(&mut *db)
        .await?;
    sqlx::query("UPDATE node_writes SET state='aborted',completed_at=clock_timestamp() WHERE node_id=$1 AND operation_id=$2 AND state='active'")
        .bind(saved.node).bind(saved.write).execute(&mut *db).await?;
    sqlx::query("DELETE FROM stream_leases WHERE stream_id=$1 AND lease_type='write'")
        .bind(saved.candidate)
        .execute(db)
        .await?;
    Ok(())
}

pub(super) async fn seal(
    db: &mut Transaction<'_, Postgres>,
    row: &queries::NodeWrite,
    input: &write::Publish,
) -> Result<()> {
    let task = row.owner_task_id.ok_or(Error::NodeOperationConflict)?;
    let detail:Value=sqlx::query_scalar("SELECT detail FROM tasks WHERE id=$1 AND task_type='upload' AND detail->>'operation'='cdc'").bind(task).fetch_one(&mut **db).await?;
    let saved: Snapshot = serde_json::from_value(detail["node_conversion"].clone())?;
    let job: Conversion = serde_json::from_value(detail)?;
    ensure!(
        saved.write == row.operation_id && Some(saved.candidate) == row.stream_id.map(Id::from),
        Error::NodeOperationConflict
    );
    let original = queries::node_read_stream(job.stream)
        .fetch_one(&mut **db)
        .await?;
    ensure!(
        original.payload_revision == job.payload_revision
            && input.size == original.size
            && input.etag == original.etag
            && serde_json::to_value(&input.checksums)? == original.checksums
            && input.if_match.is_none()
            && input.if_none.is_none(),
        Error::NodeOperationConflict
    );
    ensure!(
        queries::raw_source(job.source).fetch_one(&mut **db).await? == saved.source,
        Error::SourceUnavailable
    );
    Ok(())
}

pub(super) async fn finish(
    db: &mut Transaction<'_, Postgres>,
    task: Execution,
    detail: &Value,
    outcomes: &[work::Outcome],
) -> Result<(bool, Option<&'static str>)> {
    let [outcome] = outcomes else {
        return Err(Error::NodeOperationConflict.into());
    };
    if matches!(outcome, work::Outcome::Retry) {
        return Ok((false, Some("backend_io_failed")));
    }
    ensure!(
        matches!(outcome, work::Outcome::Converted),
        Error::NodeOperationConflict
    );
    let saved: Snapshot = serde_json::from_value(detail["node_conversion"].clone())?;
    let sealed:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM node_writes WHERE node_id=$1 AND operation_id=$2 AND state='published')")
        .bind(saved.node).bind(saved.write).fetch_one(&mut **db).await?;
    ensure!(sealed, Error::NodeOperationConflict);
    let job: Conversion = serde_json::from_value(detail.clone())?;
    Ok((
        finish_conversion_in(
            db,
            task,
            &job,
            &saved.source,
            saved.candidate,
            Some(Uuid::nil()),
        )
        .await?,
        None,
    ))
}
