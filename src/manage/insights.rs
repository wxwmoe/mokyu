use super::Identity;
use crate::{app::App, config, http::HttpError};
use anyhow::Result;
use axum::{
    Json,
    extract::{Extension, Query, State},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Postgres, Transaction};
use std::{
    sync::{Arc, atomic::Ordering::Relaxed},
    time::Duration,
};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

#[derive(Default, Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
pub(super) struct ScopeQuery {
    pub bucket: Option<Uuid>,
    pub project: Option<Uuid>,
}
#[derive(Serialize, Deserialize, ToSchema)]
pub(super) struct PhysicalUsage {
    indexed_bytes: String,
    selected_bytes: String,
    retained_bytes: String,
    gc_bytes: String,
    unconfirmed_bytes: String,
    chunk_objects: String,
    pack_objects: String,
}
#[derive(Serialize, Deserialize, ToSchema)]
pub(super) struct ObjectChunk {
    id: String,
    offset_bytes: String,
    length: i32,
    source_offset: i32,
    raw_size: i32,
    stored_size: Option<i32>,
    independent_size_hint: Option<i32>,
    payload_size: Option<i32>,
    compression: String,
    algorithm: String,
    key_id: Option<String>,
    pack_id: Option<String>,
    source: String,
    reads: Option<String>,
    range_reads: Option<String>,
}
#[derive(Serialize, ToSchema)]
pub(super) struct ChunkPage {
    pub chunks: Vec<ObjectChunk>,
    pub next_offset: Option<String>,
}
#[derive(Serialize, Deserialize, ToSchema)]
pub(super) struct StorageUsage {
    buckets: String,
    objects: String,
    logical_bytes: String,
    unique_bytes: String,
    attributed_raw_bytes: String,
    encoded_bytes: Option<String>,
    encoded_known_bytes: String,
    local_savings: String,
    shared_savings: String,
    encoding_savings: Option<String>,
    chunks: String,
    packs: String,
    packed_bytes: String,
    pending_bytes: String,
    physical: Option<PhysicalUsage>,
}
#[derive(Serialize, Deserialize, ToSchema)]
pub(super) struct RuntimePoint {
    at: DateTime<Utc>,
    epoch: DateTime<Utc>,
    requests: String,
    failed: String,
    backend_read_bytes: String,
    backend_write_bytes: String,
    cache_hits: String,
    cache_lookups: String,
    cache_bytes: String,
    cache_limit: String,
    upload_bytes: String,
    upload_limit: String,
    multipart_bytes: String,
    thumbnail_bytes: String,
    rss_bytes: Option<String>,
}
#[derive(Serialize, ToSchema)]
pub(super) struct CapacityPoint {
    at: DateTime<Utc>,
    usage: StorageUsage,
}
#[derive(Default, sqlx::FromRow)]
struct SavedStorage {
    as_of: Option<DateTime<Utc>>,
    data: Option<sqlx::types::Json<StorageUsage>>,
}
#[derive(Serialize, ToSchema)]
pub(super) struct StorageInsights {
    scope: &'static str,
    bucket_count: usize,
    as_of: Option<DateTime<Utc>>,
    collecting: bool,
    stale: bool,
    refresh_seconds: u64,
    usage: Option<StorageUsage>,
    history: Vec<CapacityPoint>,
}
#[derive(Serialize, sqlx::FromRow, ToSchema)]
pub(super) struct WorkSummary {
    id: Uuid,
    kind: String,
    state: String,
    processed: String,
    updated_at: DateTime<Utc>,
}
#[derive(Serialize, ToSchema)]
pub(super) struct RuntimeInsights {
    current: RuntimePoint,
    history: Vec<RuntimePoint>,
    work: Vec<WorkSummary>,
    pending_entries: String,
    pending_bytes: String,
    oldest_pending: Option<DateTime<Utc>>,
    failed_uploads: String,
}

// Every scope is derived from live permissions; a caller cannot submit a cached scope ID.
pub(super) async fn scope(
    tx: &mut Transaction<'_, Postgres>,
    actor: &Identity,
    query: &ScopeQuery,
) -> Result<Vec<Uuid>> {
    actor.principal.lock_identity(tx).await?;
    let ids: Vec<Uuid> = sqlx::query_scalar("SELECT b.id FROM buckets b LEFT JOIN user_bucket_access a ON a.bucket_id=b.id AND a.user_id=$1 LEFT JOIN token_bucket_access t ON t.bucket_id=b.id AND t.token_id=$3 WHERE ($2 OR 'storage.inspect'=ANY(CASE WHEN $3::uuid IS NOT NULL THEN t.actions ELSE a.actions END)) AND ($4::uuid IS NULL OR b.id=$4) AND ($5::uuid IS NULL OR b.project_id=$5) ORDER BY b.id")
        .bind(actor.id).bind(actor.admin).bind(actor.principal.token_id()).bind(query.bucket).bind(query.project).fetch_all(&mut **tx).await?;
    if query.bucket.is_some() && ids.is_empty() {
        return Err(s3s::s3_error!(AccessDenied).into());
    }
    Ok(ids)
}
#[utoipa::path(get,operation_id="storage_insights",path="/api/insights",params(ScopeQuery),responses((status=200,body=StorageInsights)))]
pub(super) async fn get(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Query(query): Query<ScopeQuery>,
) -> Result<Json<StorageInsights>, HttpError> {
    let mut tx = app.db.begin().await?;
    let ids = scope(&mut tx, &actor, &query).await?;
    let global = actor.admin && query.bucket.is_none() && query.project.is_none();
    let key = if global {
        "global".to_owned()
    } else {
        blake3::hash(&serde_json::to_vec(&ids)?)
            .to_hex()
            .to_string()
    };
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM storage_insights WHERE id=$1)")
            .bind(&key)
            .fetch_one(&mut *tx)
            .await?;
    if !exists {
        sqlx::query("SELECT pg_advisory_xact_lock(734922709851030)")
            .execute(&mut *tx)
            .await?;
        // Bound cached permission combinations, including workloads from short-lived tokens.
        sqlx::query("DELETE FROM storage_insights WHERE id IN (SELECT id FROM storage_insights WHERE id<>'global' ORDER BY requested_at DESC,id OFFSET $1)")
            .bind(app.config.statistics.scope_limit as i64-1).execute(&mut *tx).await?;
        sqlx::query(
            "INSERT INTO storage_insights(id,bucket_ids) VALUES($1,$2) ON CONFLICT DO NOTHING",
        )
        .bind(&key)
        .bind(&ids)
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query("UPDATE storage_insights SET requested_at=now() WHERE id=$1 AND requested_at<now()-interval '1 hour'").bind(&key).execute(&mut *tx).await?;
    let row: Option<SavedStorage> =
        sqlx::query_as("SELECT as_of,data FROM storage_insights WHERE id=$1")
            .bind(&key)
            .fetch_optional(&mut *tx)
            .await?;
    let SavedStorage { as_of, data } = row.unwrap_or_default();
    let points:Vec<(DateTime<Utc>,sqlx::types::Json<StorageUsage>)>=sqlx::query_as("SELECT at,data FROM (SELECT at,data FROM storage_history WHERE scope_id=$1 ORDER BY at DESC LIMIT 2048) p ORDER BY at").bind(&key).fetch_all(&mut *tx).await?;
    tx.commit().await?;
    if !exists {
        app.statistics.wake.notify_one();
    }
    let interval = config::seconds(&app.config.statistics.refresh_interval)?;
    let inventory = app.statistics.inventory(interval);
    Ok(Json(StorageInsights {
        scope: if global {
            "deployment"
        } else if query.bucket.is_some() {
            "bucket"
        } else if query.project.is_some() {
            "project"
        } else {
            "visible"
        },
        bucket_count: ids.len(),
        as_of,
        collecting: as_of.is_none() || inventory["collecting"] == true,
        stale: as_of.is_none_or(|at| {
            Utc::now().signed_duration_since(at).num_seconds() > interval.saturating_mul(2) as i64
        }) || !inventory["last_error"].is_null(),
        refresh_seconds: interval,
        usage: data.map(|v| v.0),
        history: points
            .into_iter()
            .map(|(at, data)| CapacityPoint { at, usage: data.0 })
            .collect(),
    }))
}
pub(crate) async fn collect(app: &App, tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::query("DELETE FROM storage_insights WHERE id<>'global' AND requested_at<now()-$1*interval '1 second'")
        .bind(config::seconds(&app.config.statistics.storage_retention)? as f64).execute(&mut **tx).await?;
    sqlx::raw_sql(include_str!("storage_insights.sql"))
        .execute(&mut **tx)
        .await?;
    sqlx::query("INSERT INTO storage_history(scope_id,at,data) SELECT i.id,i.as_of,i.data FROM storage_insights i WHERE i.as_of IS NOT NULL AND NOT EXISTS(SELECT 1 FROM storage_history h WHERE h.scope_id=i.id AND h.at>i.as_of-$1*interval '1 second') ON CONFLICT DO NOTHING")
        .bind(config::seconds(&app.config.statistics.storage_sample_interval)? as f64).execute(&mut **tx).await?;
    sqlx::query("DELETE FROM storage_history WHERE at<now()-$1*interval '1 second'")
        .bind(config::seconds(&app.config.statistics.storage_retention)? as f64)
        .execute(&mut **tx)
        .await?;
    Ok(())
}
async fn runtime_point(app: &App) -> Result<RuntimePoint> {
    let runtime = app.statistics.runtime();
    let io = app.storage.statistics();
    let memory = crate::stats::process_memory().await;
    let (cache_limit, upload_limit, upload_bytes) = app.storage.disk.upload_capacity()?;
    let local = app.storage.disk.used();
    let total = |key: &str| {
        ["s3", "web"]
            .iter()
            .map(|listener| runtime["http"][listener][key].as_u64().unwrap_or(0))
            .sum::<u64>()
            .to_string()
    };
    Ok(RuntimePoint {
        at: Utc::now(),
        epoch: serde_json::from_value(runtime["started_at"].clone())?,
        requests: total("started"),
        failed: total("failed"),
        backend_read_bytes: app.storage.backend_read_bytes.load(Relaxed).to_string(),
        backend_write_bytes: app.storage.backend_write_bytes.load(Relaxed).to_string(),
        cache_hits: app.storage.cache_hits.load(Relaxed).to_string(),
        cache_lookups: io["cache_lookups"].as_u64().unwrap_or(0).to_string(),
        cache_bytes: local[1].to_string(),
        cache_limit: cache_limit.to_string(),
        upload_bytes: upload_bytes.to_string(),
        upload_limit: upload_limit.to_string(),
        multipart_bytes: local[0].to_string(),
        thumbnail_bytes: local[2].to_string(),
        rss_bytes: memory["rss_bytes"].as_u64().map(|v| v.to_string()),
    })
}
#[utoipa::path(get,operation_id="runtime_insights",path="/api/insights/runtime",responses((status=200,body=RuntimeInsights)))]
pub(super) async fn runtime(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<RuntimeInsights>, HttpError> {
    let mut tx = app.db.begin().await?;
    actor.principal.lock_admin(&mut tx).await?;
    let history:Vec<sqlx::types::Json<RuntimePoint>>=sqlx::query_scalar("SELECT data FROM (SELECT at,data FROM runtime_history ORDER BY at DESC LIMIT 2048) h ORDER BY at").fetch_all(&mut *tx).await?;
    let work=sqlx::query_as("SELECT id,coalesce(detail->>'kind',kind) kind,state,processed::text,updated_at FROM tasks WHERE state IN ('queued','running','paused') ORDER BY CASE state WHEN 'running' THEN 0 WHEN 'queued' THEN 1 ELSE 2 END,updated_at DESC LIMIT 5").fetch_all(&mut *tx).await?;
    let (pending_entries,pending_bytes,oldest_pending,failed_uploads):(String,String,Option<DateTime<Utc>>,String)=sqlx::query_as("SELECT count(*)::text,coalesce(sum(cache_size),0)::text,min(created_at),count(*) FILTER(WHERE last_error IS NOT NULL)::text FROM pending_uploads").fetch_one(&mut *tx).await?;
    tx.commit().await?;
    Ok(Json(RuntimeInsights {
        current: runtime_point(&app).await?,
        history: history.into_iter().map(|v| v.0).collect(),
        work,
        pending_entries,
        pending_bytes,
        oldest_pending,
        failed_uploads,
    }))
}
pub(crate) async fn runtime_loop(app: Arc<App>) -> Result<()> {
    let interval = Duration::from_secs(config::seconds(
        &app.config.statistics.runtime_sample_interval,
    )?);
    loop {
        let result: Result<()> = async {
            let point = runtime_point(&app).await?;
            let mut tx = app.db.begin().await?;
            sqlx::query("INSERT INTO runtime_history(at,data) VALUES($1,$2)")
                .bind(point.at)
                .bind(sqlx::types::Json(point))
                .execute(&mut *tx)
                .await?;
            sqlx::query("DELETE FROM runtime_history WHERE at<now()-$1*interval '1 second'")
                .bind(config::seconds(&app.config.statistics.runtime_retention)? as f64)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            Ok(())
        }
        .await;
        if let Err(error) = result {
            tracing::warn!(%error,"runtime sample failed");
        }
        tokio::time::sleep(interval).await;
    }
}
