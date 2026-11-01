use super::*;

pub(in super::super) async fn collect_inventory(
    pool: &MySqlPool,
    metrics: &crate::config::MetricsSettings,
    maintenance: &crate::config::Maintenance,
) -> Result<Value> {
    let deadline = tokio::time::Instant::now()
        + std::time::Duration::from_secs(crate::config::seconds(&metrics.query_timeout)?);
    let mut connection =
        super::super::connection::DeadlineConnection::acquire(pool, deadline).await?;
    let result=tokio::time::timeout_at(deadline,async {
        queries::inventory_isolation().execute(&mut *connection).await?;
        let mut tx=connection.begin().await?;
        let as_of=queries::inventory_time().fetch_one(&mut *tx).await?;
        let buckets=queries::inventory_buckets(&mut tx).await?;
        let mut chunks=queries::inventory_chunks(&mut tx).await?;
        let physical=queries::inventory_physical(&mut tx).await?;
        for (field,states) in [("stored_bytes",&["ready","retired","deleting"][..]),("unconfirmed_bytes",&["preparing","uploading"][..])] {
            chunks[field]=json!(physical.as_array().unwrap().iter().filter(|p|states.contains(&p["state"].as_str().unwrap_or_default())).map(|p|p["stored_bytes"].as_i64().unwrap_or(0)).sum::<i64>());
        }
        let live=queries::inventory_live().fetch_one(&mut *tx).await?;
        let mut unreferenced=queries::inventory_unreferenced().fetch_one(&mut *tx).await?;
        let reclaimable=queries::inventory_reclaimable().fetch_one(&mut *tx).await?;
        unreferenced["stored_bytes"]=reclaimable["bytes"].clone();
        unreferenced["eligible_bytes"]=reclaimable["eligible"].clone();
        let tasks=queries::inventory_tasks().fetch_one(&mut *tx).await?;
        let uploads=queries::inventory_uploads().fetch_one(&mut *tx).await?;
        let cleanup=queries::inventory_cleanup(&mut tx,crate::config::seconds(&maintenance.task_retention)? as f64).await?;
        let database=queries::inventory_database().fetch_all(&mut *tx).await?;
        collect_usage(&mut tx,metrics,as_of).await?;
        tx.commit().await?;
        Ok(json!({"as_of":as_of,"collected_at":chrono::Utc::now(),"objects":buckets[0]["objects"],"logical_bytes":buckets[0]["logical_bytes"],"buckets":buckets.iter().skip(1).take(1000).collect::<Vec<_>>(),"buckets_truncated":buckets.len()>1001,"chunks":chunks,"live":live,"physical":physical,"unreferenced":unreferenced,"tasks":tasks,"uploads":uploads,"cleanup":cleanup,"database":database}))
    }).await;
    match result {
        Ok(result) => {
            connection.release();
            result
        }
        Err(_) => Err(crate::model::Error::QueryCancelled.into()),
    }
}

pub(in super::super) async fn collect_storage(pool: &MySqlPool, storage: Uuid) -> Result<Value> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut connection =
        super::super::connection::DeadlineConnection::acquire(pool, deadline).await?;
    let result=tokio::time::timeout_at(deadline,async {
        queries::inventory_isolation().execute(&mut *connection).await?;
        let mut tx=connection.begin().await?;
        let as_of=queries::inventory_time().fetch_one(&mut *tx).await?;
        let buckets=queries::storage_inventory_buckets(storage).fetch_one(&mut *tx).await?;
        let chunks=queries::storage_inventory_chunks(storage).fetch_one(&mut *tx).await?;
        let physical=queries::storage_inventory_physical(storage).fetch_one(&mut *tx).await?;
        tx.commit().await?;
        Ok(json!({"storage_id":storage,"as_of":as_of,"buckets":buckets.0,"objects":buckets.1,"logical_bytes":buckets.2,"chunks":chunks.0,"stored_bytes":chunks.1,"physical":{"objects":physical.0,"stored_bytes":physical.1}}))
    }).await;
    match result {
        Ok(result) => {
            connection.release();
            result
        }
        Err(_) => Err(crate::model::Error::QueryCancelled.into()),
    }
}

pub(in super::super) async fn read_usage(
    pool: &MySqlPool,
    principal: &Principal,
    query: &ScopeQuery,
    limit: usize,
) -> Result<UsageSnapshot> {
    ensure!((1..=256).contains(&limit), "invalid usage scope limit");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut connection =
        super::super::connection::DeadlineConnection::acquire(pool, deadline).await?;
    let result = tokio::time::timeout_at(deadline, async {
        let mut tx = connection.begin().await?;
        let (ids, admin) = usage_scope(&mut tx, principal, query).await?;
        let global = admin && query.bucket.is_none() && query.project.is_none();
        let key = if global {
            "global".to_owned()
        } else {
            blake3::hash(&serde_json::to_vec(&ids)?)
                .to_hex()
                .to_string()
        };
        let mut created = false;
        if !queries::usage_exists(&key).fetch_one(&mut *tx).await? {
            queries::lock_usage_scopes().execute(&mut *tx).await?;
            if !queries::usage_exists(&key).fetch_one(&mut *tx).await? {
                queries::evict_usage_scopes(&mut tx, limit as i64 - 1).await?;
                queries::create_usage_scope(&key).execute(&mut *tx).await?;
                queries::usage_bucket_members(&mut tx, &key, &ids).await?;
                created = true;
            }
        }
        queries::touch_usage_scope(&key).execute(&mut *tx).await?;
        let (as_of, data) = queries::usage_saved(&key)
            .fetch_optional(&mut *tx)
            .await?
            .unwrap_or_default();
        let history = queries::usage_history(&key).fetch_all(&mut *tx).await?;
        tx.commit().await?;
        Ok(UsageSnapshot {
            global,
            bucket_count: ids.len(),
            as_of,
            usage: data.map(|v| v.0),
            history: history
                .into_iter()
                .map(|(at, data)| CapacityPoint { at, usage: data.0 })
                .collect(),
            created,
        })
    })
    .await;
    match result {
        Ok(result) => {
            connection.release();
            result
        }
        Err(_) => Err(crate::model::Error::QueryCancelled.into()),
    }
}

pub(in super::super) async fn collect_usage(
    connection: &mut MySqlConnection,
    metrics: &crate::config::MetricsSettings,
    at: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    let retention = crate::config::seconds(&metrics.storage_retention)? as f64;
    let rows = queries::collect_usage(connection).await?;
    queries::save_usage(connection, at, rows).await?;
    queries::expire_usage_scopes(retention)
        .execute(&mut *connection)
        .await?;
    queries::append_usage_history(crate::config::seconds(&metrics.storage_interval)? as f64)
        .execute(&mut *connection)
        .await?;
    queries::expire_usage_history(retention)
        .execute(connection)
        .await?;
    Ok(())
}

pub(in super::super) async fn read_runtime(
    pool: &MySqlPool,
    principal: &Principal,
) -> Result<RuntimeHistory> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut connection =
        super::super::connection::DeadlineConnection::acquire(pool, deadline).await?;
    let result = tokio::time::timeout_at(deadline, async {
        let mut tx = connection.begin().await?;
        ensure!(
            queries::validate_identity(principal, &mut tx, true).await?,
            s3s::s3_error!(AccessDenied)
        );
        let history = queries::runtime_history().fetch_all(&mut *tx).await?;
        let work = queries::runtime_work().fetch_all(&mut *tx).await?;
        let (pending_entries, pending_bytes, oldest_pending, failed_uploads) =
            queries::runtime_pending().fetch_one(&mut *tx).await?;
        tx.commit().await?;
        Ok(RuntimeHistory {
            history: history.into_iter().map(|v| v.0).collect(),
            work,
            pending_entries,
            pending_bytes,
            oldest_pending,
            failed_uploads,
        })
    })
    .await;
    match result {
        Ok(result) => {
            connection.release();
            result
        }
        Err(_) => Err(crate::model::Error::QueryCancelled.into()),
    }
}

pub(in super::super) async fn sample_runtime(
    pool: &MySqlPool,
    point: RuntimePoint,
    retention: f64,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    queries::runtime_sample(point).execute(&mut *tx).await?;
    queries::runtime_expire(retention).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn flush_access(
    pool: &MySqlPool,
    pending: AccessBatch,
    stale: f64,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    // Acquire the shared coverage row before updating any overlapping counters.
    queries::access_coverage(pending.dropped, stale)
        .execute(&mut *tx)
        .await?;
    queries::flush_chunk_access(&mut tx, &pending).await?;
    queries::flush_pack_access(&mut tx, &pending).await?;
    queries::flush_member_access(&mut tx, &pending).await?;
    super::super::tier_access::demand(&mut tx, &pending).await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn expire_access(
    pool: &MySqlPool,
    retention: f64,
    batch: i64,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut connection =
        super::super::connection::DeadlineConnection::acquire(pool, deadline).await?;
    let result = tokio::time::timeout_at(deadline, async {
        let mut tx = connection.begin().await?;
        queries::expire_access(&mut tx, retention, batch).await?;
        tx.commit().await?;
        Ok(())
    })
    .await;
    match result {
        Ok(result) => {
            connection.release();
            result
        }
        Err(_) => Err(crate::model::Error::QueryCancelled.into()),
    }
}

pub(in super::super) async fn recover_runtime(pool: &MySqlPool, maintenance: bool) -> Result<()> {
    let mut tx = pool.begin().await?;
    queries::reset_access_coverage().execute(&mut *tx).await?;
    queries::abandon_startup_streams().execute(&mut *tx).await?;
    super::super::replica_transactions::recover_admissions(&mut tx).await?;
    queries::fail_startup_chunks().execute(&mut *tx).await?;
    queries::retire_startup_encodings()
        .execute(&mut *tx)
        .await?;
    queries::retire_startup_packs().execute(&mut *tx).await?;
    queries::clear_pack_inputs().execute(&mut *tx).await?;
    queries::reset_completing_uploads()
        .execute(&mut *tx)
        .await?;
    queries::reset_startup_tasks(maintenance)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn claim_stale_stream(
    pool: &MySqlPool,
    id: Id,
    storage: Option<Uuid>,
    retention: Option<f64>,
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    let Some(publication) = queries::lock_stale_stream(id, storage, retention)
        .build_query_scalar::<Option<Uuid>>()
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(false);
    };
    queries::abandon_cleanup_stream(id)
        .execute(&mut *tx)
        .await?;
    if publication.is_some() {
        super::super::replica_transactions::reap_admission(&mut tx, id, None).await?;
    }
    tx.commit().await?;
    Ok(true)
}

pub(in super::super) async fn cleanup_stream(
    pool: &MySqlPool,
    id: Id,
    batch: i64,
) -> Result<usize> {
    let mut tx = pool.begin().await?;
    let Some(bucket) = queries::cleanup_stream_bucket(id)
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(0);
    };
    let mut accounts = lock_quota(&mut tx, bucket).await?;
    if queries::lock_cleanup_stream(id)
        .fetch_optional(&mut *tx)
        .await?
        .is_none()
    {
        return Ok(0);
    }
    forget_quota_write(&mut tx, &mut accounts, id).await?;
    let raw = queries::raw_cleanup_ranges(id, batch.clamp(1, 128))
        .fetch_all(&mut *tx)
        .await?;
    if let Some((last, _)) = raw.last() {
        let mut sources: Vec<_> = raw.iter().map(|(_, source)| *source).collect();
        sources.sort_unstable();
        sources.dedup();
        for source in sources {
            queries::raw_source(source).fetch_one(&mut *tx).await?;
            queries::raw_touch_unreferenced(source)
                .execute(&mut *tx)
                .await?;
        }
        queries::raw_detach(id, *last).execute(&mut *tx).await?;
    }
    let candidates = queries::retire_stream_encodings(id, batch)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    let detached = queries::detach_stream_chunks(&mut tx, id, batch).await?;
    let extents = queries::remove_stream_extents(&mut tx, id, batch).await?;
    let mut removed = 0;
    if queries::cleanup_stream_empty(id)
        .fetch_one(&mut *tx)
        .await?
    {
        queries::detach_upload_output(id).execute(&mut *tx).await?;
        removed = queries::remove_empty_stream(id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
    }
    tx.commit().await?;
    Ok(raw.len() + extents + (candidates + detached + removed) as usize)
}
