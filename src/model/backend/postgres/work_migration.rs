use super::*;
use crate::model::{node_write as write, object::StorageMigration};
use serde::{Deserialize, Serialize};

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

pub(super) async fn storage(db: &mut PgConnection, detail: &Value) -> Result<Uuid> {
    queries::object_storage(job(detail)?.target_bucket)
        .fetch_one(db)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket).into())
}

async fn source(
    db: &mut Transaction<'_, Postgres>,
    job: &StorageMigration,
) -> Result<crate::app::StoredStream> {
    ensure!(
        selected_object(
            db,
            job.source_bucket,
            &job.source_key,
            Some(job.source_version)
        )
        .await?
            == Some(job.source_version),
        Error::OperationAborted
    );
    queries::node_read_stream(job.source_version)
        .fetch_one(&mut **db)
        .await
        .map_err(Into::into)
}

pub(super) async fn prepare(
    db: &mut Transaction<'_, Postgres>,
    node: &NodeState,
    run: Run,
    task: Execution,
    detail: &Value,
    mut profile: crate::storage::profile::StorageProfile,
) -> Result<Option<(work::Plan, Vec<read::BlockKey>)>> {
    let job = job(detail)?;
    let selected = async {
        let source = source(db, &job).await?;
        selected_object(db, job.target_bucket, &job.target_key, job.target_version).await?;
        Ok::<_, anyhow::Error>(source)
    }
    .await;
    let original = match selected {
        Ok(value) => value,
        Err(error)
            if error
                .downcast_ref::<s3s::S3Error>()
                .is_some_and(|error| error.code() == &s3s::S3ErrorCode::PreconditionFailed) =>
        {
            q::finish(task.id, task.token, 0, false, Some("object_changed"))
                .execute(&mut **db)
                .await?;
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    if let Some(limit) = profile
        .migration_byte_budget
        .as_deref()
        .map(crate::config::bytes)
        .transpose()?
        .filter(|limit| *limit > 0)
    {
        if original.size as u64 > limit {
            q::finish(
                task.id,
                task.token,
                0,
                false,
                Some("migration_byte_budget_exceeded"),
            )
            .execute(&mut **db)
            .await?;
            return Ok(None);
        }
    }
    let pending: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM raw_ranges e JOIN raw_sources r ON r.id=e.source_id WHERE e.stream_id=$1 AND r.pending_upload AND r.quarantine_code IS NULL) OR EXISTS(SELECT 1 FROM extents e JOIN pending_uploads p ON p.chunk_id=e.chunk_id WHERE e.stream_id=$1)")
        .bind(job.source_version).fetch_one(&mut **db).await?;
    if pending {
        return Ok(None);
    }
    let saved = detail
        .get("node_migration")
        .cloned()
        .map(serde_json::from_value::<Snapshot>)
        .transpose()?;
    let mut retained = None;
    if let Some(saved) = saved {
        let sealed: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM node_writes w JOIN streams s ON s.id=w.stream_id WHERE w.node_id=$1 AND w.operation_id=$2 AND w.state='published' AND s.id=$3 AND s.state='writing' AND s.owner_task_id=$4)")
            .bind(saved.node).bind(saved.write).bind(saved.candidate).bind(task.id).fetch_one(&mut **db).await?;
        if sealed {
            sqlx::query("UPDATE streams SET owner_lease_token=$2 WHERE id=$1 AND owner_task_id=$3")
                .bind(saved.candidate)
                .bind(task.token)
                .bind(task.id)
                .execute(&mut **db)
                .await?;
            retained = Some(saved);
        } else {
            queries::conversion_abandon(saved.candidate)
                .execute(&mut **db)
                .await?;
            sqlx::query("UPDATE node_writes SET state='aborted',completed_at=clock_timestamp() WHERE node_id=$1 AND operation_id=$2 AND state='active'")
                .bind(saved.node).bind(saved.write).execute(&mut **db).await?;
            sqlx::query("DELETE FROM stream_leases WHERE stream_id=$1 AND lease_type='write'")
                .bind(saved.candidate)
                .execute(&mut **db)
                .await?;
        }
    }
    let sealed = retained.is_some();
    let snapshot = if let Some(saved) = retained {
        saved
    } else {
        let (_, block_key) = queries::node_write_storage(profile.storage_id)
            .fetch_one(&mut **db)
            .await?;
        if let Some(id) = block_key {
            queries::lock_managed_key(id)
                .fetch_optional(&mut **db)
                .await?
                .ok_or(Error::SourceUnavailable)?;
            ensure!(
                queries::valid_storage_key(id).fetch_one(&mut **db).await?,
                Error::SourceUnavailable
            );
        }
        profile.upload_cache = false;
        profile.write_processing = "inline".into();
        profile.write_durability = None;
        let candidate = create_stream_in(
            db,
            StreamCreate {
                authority: Permit::system(),
                bucket: job.target_bucket,
                key: job.target_key.clone(),
                stream_type: "object".into(),
                metadata: original.metadata.clone(),
                public: job.public_read,
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
        queries::insert_node_write(
            node,
            &input,
            &input.intent.hash()?,
            candidate,
            &profile,
            block_key,
            None,
            None,
        )
        .execute(&mut **db)
        .await?;
        Snapshot {
            candidate,
            write: operation,
            read: Uuid::now_v7(),
            node: node.node_id,
        }
    };
    sqlx::query("UPDATE tasks SET detail=jsonb_set(detail,'{node_migration}',$2) WHERE id=$1 AND lease_token=$3")
        .bind(task.id).bind(serde_json::to_value(&snapshot)?).bind(task.token).execute(&mut **db).await?;
    Ok(Some((
        work::Plan::Migrate {
            storage: profile.storage_id,
            write: snapshot.write,
            size: original.size as u64,
            sealed,
        },
        vec![],
    )))
}

pub(crate) async fn read(
    pool: &PgPool,
    node: &NodeState,
    key: &Key,
    input: work::ReadMigration,
) -> Result<read::Reply> {
    let mut db = pool.begin().await?;
    let row = node_write_owned(&mut db, node, key, input.run, input.operation).await?;
    require_node_write(&mut db, &row).await?;
    let detail: Value = sqlx::query_scalar(
        "SELECT detail FROM tasks WHERE id=$1 AND task_type='storage_migration'",
    )
    .bind(row.owner_task_id.ok_or(Error::NodeOperationConflict)?)
    .fetch_one(&mut *db)
    .await?;
    let saved: Snapshot = serde_json::from_value(detail["node_migration"].clone())?;
    ensure!(
        saved.node == node.node_id
            && saved.write == input.operation
            && Some(saved.candidate) == row.stream_id.map(Id::from),
        Error::NodeOperationConflict
    );
    let original = source(&mut db, &job(&detail)?).await?;
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
    let reply = protect_node_read(
        &mut db,
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
    db.commit().await?;
    Ok(reply)
}

pub(super) async fn seal(
    db: &mut Transaction<'_, Postgres>,
    row: &queries::NodeWrite,
    input: &write::Publish,
) -> Result<()> {
    let detail: Value = sqlx::query_scalar(
        "SELECT detail FROM tasks WHERE id=$1 AND task_type='storage_migration'",
    )
    .bind(row.owner_task_id.ok_or(Error::NodeOperationConflict)?)
    .fetch_one(&mut **db)
    .await?;
    let saved: Snapshot = serde_json::from_value(detail["node_migration"].clone())?;
    let original = source(db, &job(&detail)?).await?;
    ensure!(
        saved.write == row.operation_id
            && Some(saved.candidate) == row.stream_id.map(Id::from)
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
        matches!(outcome, work::Outcome::Migrated),
        Error::NodeOperationConflict
    );
    let saved: Snapshot = serde_json::from_value(detail["node_migration"].clone())?;
    let sealed: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM node_writes WHERE node_id=$1 AND operation_id=$2 AND state='published')")
        .bind(saved.node).bind(saved.write).fetch_one(&mut **db).await?;
    ensure!(sealed, Error::NodeOperationConflict);
    let mut publication = sqlx::Acquire::begin(&mut **db).await?;
    if let Err(error) = complete_storage_migration_in(
        &mut publication,
        task.id,
        task.token,
        saved.candidate,
        &job(detail)?,
    )
    .await
    {
        publication.rollback().await?;
        let error = normalize_error(error);
        let code = if error
            .downcast_ref::<s3s::S3Error>()
            .is_some_and(|error| error.code() == &s3s::S3ErrorCode::PreconditionFailed)
        {
            Some("object_changed")
        } else if crate::model::error(&error) == Some(Error::QuotaExceeded) {
            Some("quota_exceeded")
        } else {
            None
        };
        if let Some(code) = code {
            queries::conversion_abandon(saved.candidate)
                .execute(&mut **db)
                .await?;
            return Ok((false, Some(code)));
        }
        return Err(error);
    }
    publication.commit().await?;
    Ok((true, None))
}
