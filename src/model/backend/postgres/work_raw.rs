use super::*;
use crate::model::raw::RawProcessing;

pub(super) async fn schedule(db: &mut PgConnection, maximum: usize) -> Result<()> {
    let rows: Vec<(Id,Uuid,i64)> = sqlx::query_as("SELECT w.stream_id,r.storage_id,r.size FROM raw_processing w JOIN streams s ON s.id=w.stream_id JOIN raw_sources r ON r.id=w.source_id JOIN storages st ON st.id=r.storage_id WHERE st.enabled AND st.type='s3' AND r.medium='shared_s3' AND r.state='ready' AND NOT EXISTS(SELECT 1 FROM raw_uploads u WHERE u.source_id=r.id) AND s.state<>'writing' AND w.next_attempt_at<=clock_timestamp() AND (w.token IS NULL OR w.lease_expires_at IS NULL OR w.lease_expires_at<clock_timestamp()) AND NOT EXISTS(SELECT 1 FROM tasks t WHERE t.id=w.token AND t.state IN ('queued','running','paused','failed')) ORDER BY w.next_attempt_at,w.stream_id LIMIT $1 FOR UPDATE OF w SKIP LOCKED")
        .bind(maximum as i64).fetch_all(&mut *db).await?;
    let Some((_, storage, _)) = rows.first().copied() else {
        return Ok(());
    };
    let profile = queries::storage_profile(storage)
        .fetch_one(&mut *db)
        .await?;
    let budget = profile
        .maintenance_byte_budget
        .as_deref()
        .map(config::bytes)
        .transpose()?
        .unwrap_or(64 * 1024 * 1024);
    let mut streams = Vec::new();
    let mut bytes = 0u64;
    for (stream, selected, size) in rows {
        if selected != storage {
            continue;
        }
        if !streams.is_empty() && bytes.saturating_add(size as u64) > budget {
            break;
        }
        streams.push(stream);
        bytes = bytes.saturating_add(size as u64);
    }
    let task=enqueue_task(db,"upload",None,json!({"operation":"deduplicate","storage_id":storage,"execution_mode":"cluster","node_raw":streams}),true).await?;
    sqlx::query("UPDATE raw_processing SET token=$2,lease_expires_at=NULL WHERE stream_id=ANY($1)")
        .bind(&streams)
        .bind(task)
        .execute(&mut *db)
        .await?;
    Ok(())
}

pub(super) async fn prepare(
    db: &mut Transaction<'_, Postgres>,
    task: Execution,
    detail: &Value,
    storage: Uuid,
) -> Result<Option<(work::Plan, Vec<read::BlockKey>)>> {
    let streams: Vec<Id> = serde_json::from_value(detail["node_raw"].clone())?;
    ensure!(streams.len() <= work::MAX_ITEMS, Error::NodeWorkLimit);
    let mut pending = Vec::new();
    let mut items = Vec::new();
    for stream in streams {
        let Some(job) = owned(&mut **db, stream, task.id).await? else {
            continue;
        };
        if !sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM streams s WHERE id=$1 AND state='ready' AND payload_type='raw' AND payload_revision=$2 AND EXISTS(SELECT 1 FROM objects WHERE stream_id=s.id))").bind(stream).bind(job.payload_revision).fetch_one(&mut **db).await? {
            queries::raw_processing_delete(&job).execute(&mut **db).await?;
            continue;
        }
        let source = queries::raw_source(job.source).fetch_one(&mut **db).await?;
        ensure!(source.storage_id == storage, Error::NodeOperationConflict);
        let hash = job
            .content_hash
            .filter(|_| job.source_revision == Some(source.revision));
        pending.push(stream);
        items.push(work::RawHash { source, hash });
    }
    sqlx::query(
        "UPDATE tasks SET detail=jsonb_set(detail,'{node_raw}',$2) WHERE id=$1 AND lease_token=$3",
    )
    .bind(task.id)
    .bind(json!(pending))
    .bind(task.token)
    .execute(&mut **db)
    .await?;
    if items.is_empty() {
        q::finish(task.id, task.token, 0, true, None)
            .execute(&mut **db)
            .await?;
        return Ok(None);
    }
    Ok(Some((work::Plan::HashRaw { storage, items }, vec![])))
}

pub(super) async fn finish(
    db: &mut Transaction<'_, Postgres>,
    task: Execution,
    detail: &Value,
    items: Vec<work::RawHash>,
    outcomes: &[work::Outcome],
) -> Result<(i64, bool, Option<&'static str>)> {
    let streams: Vec<Id> = serde_json::from_value(detail["node_raw"].clone())?;
    ensure!(
        streams.len() == items.len() && outcomes.len() == items.len(),
        Error::NodeOperationConflict
    );
    let mut order = Vec::new();
    let mut failed = Vec::new();
    let mut error = None;
    for (index, (item, outcome)) in items.iter().zip(outcomes).enumerate() {
        match outcome {
            work::Outcome::Hashed { hash } => {
                ensure!(
                    hash.len() == 32 && item.hash.as_ref().is_none_or(|saved| saved == hash),
                    Error::NodeOperationConflict
                );
                order.push((hash, streams[index], index));
            }
            work::Outcome::Retry => {
                failed.push(streams[index]);
                error = Some("backend_io_failed");
            }
            _ => return Err(Error::NodeOperationConflict.into()),
        }
    }
    order.sort_unstable();
    let mut completed = 0;
    for (hash, stream, index) in order {
        let Some(job) = owned(&mut **db, stream, task.id).await? else {
            completed += 1;
            continue;
        };
        let source = &items[index].source;
        ensure!(job.source == source.id, Error::NodeOperationConflict);
        match finish_raw_processing_in(db, &job, source, hash, Some(Uuid::nil())).await {
            Ok(true) => completed += 1,
            Ok(false) => {
                sqlx::query("UPDATE raw_processing SET next_attempt_at=clock_timestamp()+interval '1 second',last_error=NULL,content_hash=$3,source_revision=$4 WHERE stream_id=$1 AND token=$2")
                    .bind(stream).bind(task.id).bind(hash).bind(source.revision).execute(&mut **db).await?;
            }
            Err(value)
                if value.downcast_ref::<Error>().is_some_and(|e| {
                    matches!(e, Error::OperationAborted | Error::SourceUnavailable)
                }) =>
            {
                failed.push(stream);
                error = Some("source_changed");
            }
            Err(value) => return Err(value),
        }
    }
    if let Some(error) = error {
        sqlx::query("UPDATE raw_processing SET attempts=attempts+1,last_error=$3,next_attempt_at=clock_timestamp()+interval '30 seconds' WHERE token=$1 AND stream_id=ANY($2)")
            .bind(task.id).bind(failed).bind(error).execute(&mut **db).await?;
    }
    Ok((completed, completed as usize == items.len(), error))
}

async fn owned(db: &mut PgConnection, stream: Id, task: Uuid) -> Result<Option<RawProcessing>> {
    Ok(sqlx::query_as("SELECT w.stream_id AS stream,w.source_id AS source,s.bucket_id AS bucket,s.object_key AS object_key,w.payload_revision,w.content_hash,w.source_revision,w.token FROM raw_processing w JOIN streams s ON s.id=w.stream_id WHERE w.stream_id=$1 AND w.token=$2")
        .bind(stream).bind(task).fetch_optional(db).await?)
}
