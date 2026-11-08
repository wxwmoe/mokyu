use super::*;

pub(in super::super) async fn collect_inventory(
    pool: &PgPool,
    metrics: &config::MetricsSettings,
    maintenance: &config::Maintenance,
) -> Result<Value> {
    let mut tx = pool.begin().await?;
    queries::inventory_isolation().execute(&mut *tx).await?;
    // One worker, bounded working memory and wait time; large sorts may spill to PostgreSQL disk.
    queries::inventory_limits().execute(&mut *tx).await?;
    let timeout = config::seconds(&metrics.query_timeout)? * 1000;
    queries::inventory_timeout(timeout.to_string())
        .execute(&mut *tx)
        .await?;
    let as_of: DateTime<Utc> = queries::inventory_time().fetch_one(&mut *tx).await?;
    let buckets: Vec<Value> = queries::inventory_buckets().fetch_all(&mut *tx).await?;
    let chunks: Value = queries::inventory_chunks().fetch_one(&mut *tx).await?;
    let physical: Value = queries::inventory_physical().fetch_one(&mut *tx).await?;
    let mut chunks = chunks;
    chunks["stored_bytes"] = json!(
        physical
            .as_array()
            .unwrap()
            .iter()
            .filter(|p| matches!(p["state"].as_str(), Some("ready" | "retired" | "deleting")))
            .map(|p| p["stored_bytes"].as_i64().unwrap_or(0))
            .sum::<i64>()
    );
    chunks["unconfirmed_bytes"] = json!(
        physical
            .as_array()
            .unwrap()
            .iter()
            .filter(|p| matches!(p["state"].as_str(), Some("preparing" | "uploading")))
            .map(|p| p["stored_bytes"].as_i64().unwrap_or(0))
            .sum::<i64>()
    );
    let live: Value = queries::inventory_live().fetch_one(&mut *tx).await?;
    let mut unreferenced: Value = queries::inventory_unreferenced()
        .fetch_one(&mut *tx)
        .await?;
    let reclaimable: Value = queries::inventory_reclaimable().fetch_one(&mut *tx).await?;
    unreferenced["stored_bytes"] = reclaimable["bytes"].clone();
    unreferenced["eligible_bytes"] = reclaimable["eligible"].clone();
    let tasks: Value = queries::inventory_tasks().fetch_one(&mut *tx).await?;
    let uploads: Value = queries::inventory_uploads().fetch_one(&mut *tx).await?;
    let cleanup: Value =
        queries::inventory_cleanup(config::seconds(&maintenance.task_retention)? as f64)
            .fetch_one(&mut *tx)
            .await?;
    let database: Vec<Value> = queries::inventory_database().fetch_all(&mut *tx).await?;
    collect_usage(&mut tx, metrics).await?;
    tx.commit().await?;
    Ok(
        json!({"as_of":as_of,"collected_at":Utc::now(),"objects":buckets[0]["objects"],
        "logical_bytes":buckets[0]["logical_bytes"],"buckets":buckets.iter().skip(1).take(1000).collect::<Vec<_>>(),
        "buckets_truncated":buckets.len()>1001,"chunks":chunks,"live":live,"physical":physical,
        "unreferenced":unreferenced,"tasks":tasks,"uploads":uploads,"cleanup":cleanup,"database":database}),
    )
}

pub(in super::super) async fn collect_storage(pool: &PgPool, storage_id: Uuid) -> Result<Value> {
    let mut tx = pool.begin().await?;
    queries::storage_inventory_limits()
        .execute(&mut *tx)
        .await?;
    let as_of: DateTime<Utc> = queries::inventory_time().fetch_one(&mut *tx).await?;
    let buckets: (i64, i64, i64) = queries::storage_inventory_buckets(storage_id)
        .fetch_one(&mut *tx)
        .await?;
    let chunks: (i64, i64) = queries::storage_inventory_chunks(storage_id)
        .fetch_one(&mut *tx)
        .await?;
    let physical: (i64, i64) = queries::storage_inventory_physical(storage_id)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(
        json!({"storage_id":storage_id,"as_of":as_of,"buckets":buckets.0,"objects":buckets.1,"logical_bytes":buckets.2,"chunks":chunks.0,"stored_bytes":chunks.1,"physical":{"objects":physical.0,"stored_bytes":physical.1}}),
    )
}

pub(in super::super) async fn start_usage(pool: &PgPool, node: Option<Uuid>) -> Result<Value> {
    let mut tx = pool.begin().await?;
    if let Some(id) = queries::active_usage_task()
        .fetch_optional(&mut *tx)
        .await?
    {
        tx.commit().await?;
        return Ok(json!({"task_id":id,"existing":true}));
    }
    let storages: Vec<(Uuid, String, i64)> =
        queries::lifecycle_storages().fetch_all(&mut *tx).await?;
    let id = enqueue_task(
        &mut *tx,
        "usage",
        None,
        json!({"role":"parent","children":storages.len()}),
        false,
    )
    .await?;
    for (storage_id, storage_type, revision) in &storages {
        let child = enqueue_task(
            &mut *tx,
            "usage",
            None,
            json!({"role":"child","storage_id":storage_id,"profile_revision":revision}),
            true,
        )
        .await?;
        let (scope, target_node) = if storage_type == "local" {
            ("node", node)
        } else {
            ("cluster", None)
        };
        queries::scope_pack_task(child, id, scope, target_node)
            .execute(&mut *tx)
            .await?;
    }
    queries::task_batch_total(id, storages.len() as i64)
        .execute(&mut *tx)
        .await?;
    if storages.is_empty() {
        queries::complete_task(id).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(json!({"task_id":id}))
}

// Every scope is derived from live permissions; a caller cannot submit a cached scope ID.
pub(super) async fn usage_scope(
    tx: &mut Transaction<'_, Postgres>,
    principal: &Principal,
    query: &ScopeQuery,
) -> Result<(Vec<Uuid>, bool)> {
    let admin = principal.lock_identity(tx).await?;
    let ids: Vec<Uuid> = queries::usage_scope_buckets(
        principal.user_id(),
        admin,
        principal.token_id(),
        query.bucket,
        query.project,
    )
    .fetch_all(&mut **tx)
    .await?;
    if query.bucket.is_some() && ids.is_empty() {
        return Err(s3s::s3_error!(AccessDenied).into());
    }
    Ok((ids, admin))
}

pub(in super::super) async fn read_usage(
    pool: &PgPool,
    principal: &Principal,
    query: &ScopeQuery,
    limit: usize,
) -> Result<UsageSnapshot> {
    let mut tx = pool.begin().await?;
    let (ids, admin) = usage_scope(&mut tx, principal, query).await?;
    let global = admin && query.bucket.is_none() && query.project.is_none();
    let key = if global {
        "global".to_owned()
    } else {
        blake3::hash(&serde_json::to_vec(&ids)?)
            .to_hex()
            .to_string()
    };
    let exists: bool = queries::usage_exists(&key).fetch_one(&mut *tx).await?;
    if !exists {
        queries::usage_scope_lock().execute(&mut *tx).await?;
        // Bound cached permission combinations, including workloads from short-lived tokens.
        queries::usage_scope_evict(limit as i64 - 1)
            .execute(&mut *tx)
            .await?;
        queries::usage_scope_create(&key, &ids)
            .execute(&mut *tx)
            .await?;
    }
    queries::usage_scope_touch(&key).execute(&mut *tx).await?;
    let row: Option<SavedStorage> = queries::usage_saved(&key).fetch_optional(&mut *tx).await?;
    let SavedStorage { as_of, data } = row.unwrap_or_default();
    let points: Vec<(DateTime<Utc>, sqlx::types::Json<StorageUsage>)> =
        queries::usage_history(&key).fetch_all(&mut *tx).await?;
    tx.commit().await?;
    Ok(UsageSnapshot {
        global,
        bucket_count: ids.len(),
        as_of,
        usage: data.map(|v| v.0),
        history: points
            .into_iter()
            .map(|(at, data)| CapacityPoint { at, usage: data.0 })
            .collect(),
        created: !exists,
    })
}

async fn collect_usage(
    tx: &mut Transaction<'_, Postgres>,
    metrics: &config::MetricsSettings,
) -> Result<()> {
    queries::usage_scope_expire(config::seconds(&metrics.storage_retention)? as f64)
        .execute(&mut **tx)
        .await?;
    queries::collect_usage().execute(&mut **tx).await?;
    queries::usage_history_append(config::seconds(&metrics.storage_interval)? as f64)
        .execute(&mut **tx)
        .await?;
    queries::usage_history_expire(config::seconds(&metrics.storage_retention)? as f64)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub(in super::super) async fn read_runtime(
    pool: &PgPool,
    principal: &Principal,
) -> Result<RuntimeHistory> {
    let mut tx = pool.begin().await?;
    queries::runtime_diagnostic_limits()
        .execute(&mut *tx)
        .await?;
    principal.lock_admin(&mut tx).await?;
    let history: Vec<sqlx::types::Json<RuntimePoint>> =
        queries::runtime_history().fetch_all(&mut *tx).await?;
    let work = queries::runtime_work().fetch_all(&mut *tx).await?;
    let (pending_entries, pending_bytes, oldest_pending, failed_uploads): (
        String,
        String,
        Option<DateTime<Utc>>,
        String,
    ) = queries::runtime_pending().fetch_one(&mut *tx).await?;
    tx.commit().await?;
    Ok(RuntimeHistory {
        history: history.into_iter().map(|v| v.0).collect(),
        work,
        pending_entries,
        pending_bytes,
        oldest_pending,
        failed_uploads,
    })
}

pub(in super::super) async fn sample_runtime(
    pool: &PgPool,
    point: RuntimePoint,
    retention: f64,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    queries::runtime_sample(point.at, sqlx::types::Json(point))
        .execute(&mut *tx)
        .await?;
    queries::runtime_expire(retention).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn flush_access(
    pool: &PgPool,
    pending: AccessBatch,
    stale: f64,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    let dropped = pending.dropped;
    persist_access(&mut tx, pending).await?;
    queries::access_coverage(dropped, stale)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn persist_access(db: &mut PgConnection, pending: AccessBatch) -> Result<()> {
    for ((stream, start, end, hour), saved) in &pending.pack_reads {
        sqlx::query("INSERT INTO pack_read_shapes(stream_id,start_bytes,end_bytes,observed_hour,saved_gets,requests) SELECT id,$2,$3,$4,$5,$6 FROM streams WHERE id=$1 ON CONFLICT(stream_id,start_bytes,end_bytes,observed_hour) DO UPDATE SET saved_gets=pack_read_shapes.saved_gets+EXCLUDED.saved_gets,requests=pack_read_shapes.requests+EXCLUDED.requests").bind(stream).bind(start).bind(end).bind(hour.timestamp()).bind(saved.saved).bind(saved.requests).execute(&mut *db).await?;
    }

    super::super::tier_access::demand(db, &pending).await?;
    if pending.chunks.is_empty() && pending.packs.is_empty() && pending.members.is_empty() {
        return Ok(());
    }
    // Each capped map is persisted in one bulk query.
    let mut ids = Vec::new();
    let mut hours = Vec::new();
    let mut reads = Vec::new();
    let mut ranges = Vec::new();
    let mut bytes = Vec::new();
    let mut origins = Vec::new();
    for ((id, hour), c) in pending.chunks {
        ids.push(id);
        hours.push(hour);
        reads.push(c.reads);
        ranges.push(c.ranges);
        bytes.push(c.bytes);
        origins.push(c.origins);
    }
    queries::access_chunks(&ids, &reads, &ranges, &bytes)
        .execute(&mut *db)
        .await?;
    queries::access_chunk_windows(&ids, hours, reads, ranges, bytes, origins)
        .execute(&mut *db)
        .await?;
    let mut ids = Vec::new();
    let mut hours = Vec::new();
    let mut reads = Vec::new();
    let mut partial = Vec::new();
    let mut partial_bytes = Vec::new();
    let mut bytes = Vec::new();
    let mut useful = Vec::new();
    for ((id, hour), p) in pending.packs {
        ids.push(id);
        hours.push(hour);
        reads.push(p.downloads);
        partial.push(p.partial);
        partial_bytes.push(p.partial_bytes);
        bytes.push(p.bytes);
        useful.push(p.useful);
    }
    queries::access_packs(ids, hours, reads, bytes, partial, useful, partial_bytes)
        .execute(&mut *db)
        .await?;
    let (mut packs, mut chunks, mut hours, mut counts) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for ((p, c, h), n) in pending.members {
        packs.push(p);
        chunks.push(c);
        hours.push(h);
        counts.push(n);
    }
    queries::access_members(packs, chunks, hours, counts)
        .execute(&mut *db)
        .await?;
    Ok(())
}

pub(in super::super) async fn aggregate_runtime(
    pool: &PgPool,
    interval: u64,
    retention: u64,
) -> Result<()> {
    use crate::model::metrics::{NodeRuntimePoint, aggregate_runtime};
    let mut tx = pool.begin().await?;
    queries::runtime_diagnostic_limits()
        .execute(&mut *tx)
        .await?;
    if !queries::runtime_aggregate_lock()
        .fetch_one(&mut *tx)
        .await?
    {
        return Ok(());
    }
    let (at, expected, due) = queries::runtime_aggregate_due(interval as f64)
        .fetch_one(&mut *tx)
        .await?;
    if due {
        let rows =
            queries::runtime_node_window(interval.saturating_mul(3).saturating_add(30) as f64)
                .fetch_all(&mut *tx)
                .await?;
        let expected = rows.first().map(|row| row.3).unwrap_or(expected);
        let samples = rows
            .into_iter()
            .filter_map(|(id, at, sample, _)| {
                Some(NodeRuntimePoint {
                    node_id: id?,
                    received_at: at?,
                    sample: sample?.0,
                })
            })
            .collect::<Vec<_>>();
        let point = aggregate_runtime(at, expected, interval, &samples)?;
        queries::cluster_runtime_sample(&point)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    if due {
        // Commit each bounded cleanup batch so a deadline cannot undo useful progress.
        for _ in 0..8 {
            let mut tx = pool.begin().await?;
            queries::runtime_diagnostic_limits()
                .execute(&mut *tx)
                .await?;
            let removed = queries::node_runtime_expire(retention as f64)
                .execute(&mut *tx)
                .await?
                .rows_affected();
            queries::cluster_runtime_expire(retention as f64)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            if removed < 1000 {
                break;
            }
        }
    }
    Ok(())
}
