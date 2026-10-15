use super::Identity;
use crate::{app::App, config, http::HttpError};
use anyhow::Result;
use axum::{
    Json,
    extract::{Extension, Query, State},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{
    sync::{Arc, atomic::Ordering::Relaxed},
    time::Duration,
};
use utoipa::ToSchema;
use uuid::Uuid;

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
    block_key_id: Option<Uuid>,
    pack_id: Option<String>,
    source: String,
    reads: Option<String>,
    range_reads: Option<String>,
}
#[derive(Serialize, ToSchema)]
pub(super) struct ChunkPage {
    #[serde(rename = "items")]
    pub chunks: Vec<ObjectChunk>,
    #[serde(rename = "next_cursor")]
    pub next_offset: Option<String>,
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

#[utoipa::path(get,operation_id="usage_summary",path="/api/v1/usage/summary",params(ScopeQuery),responses((status=200,body=StorageInsights)))]
pub(super) async fn get(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Query(query): Query<ScopeQuery>,
) -> Result<Json<StorageInsights>, HttpError> {
    let snapshot = app
        .metrics
        .usage(&actor.principal, &query, app.config.metrics.scope_limit)
        .await?;
    let crate::model::metrics::UsageSnapshot {
        global,
        bucket_count,
        as_of,
        usage,
        history,
        created,
    } = snapshot;
    if created {
        app.statistics.wake.notify_one();
    }
    let interval = config::seconds(&app.config.metrics.refresh_interval)?;
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
        bucket_count,
        as_of,
        collecting: as_of.is_none() || inventory["collecting"] == true,
        stale: as_of.is_none_or(|at| {
            Utc::now().signed_duration_since(at).num_seconds() > interval.saturating_mul(2) as i64
        }) || !inventory["last_error"].is_null(),
        refresh_seconds: interval,
        usage,
        history,
    }))
}
async fn runtime_point(app: &App) -> Result<RuntimePoint> {
    let runtime = app.statistics.runtime();
    let io = app.storage.statistics();
    let memory = crate::status::stats::process_memory().await;
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
#[utoipa::path(get,operation_id="runtime_insights",path="/api/v1/usage/runtime",responses((status=200,body=RuntimeInsights)))]
pub(super) async fn runtime(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<RuntimeInsights>, HttpError> {
    let crate::model::metrics::RuntimeHistory {
        history,
        work,
        pending_entries,
        pending_bytes,
        oldest_pending,
        failed_uploads,
    } = app.metrics.runtime(&actor.principal).await?;
    Ok(Json(RuntimeInsights {
        current: runtime_point(&app).await?,
        history,
        work,
        pending_entries,
        pending_bytes,
        oldest_pending,
        failed_uploads,
    }))
}
pub(crate) async fn runtime_loop(app: Arc<App>) -> Result<()> {
    const LEADER_KEY: i64 = 734922709851032;
    let interval = Duration::from_secs(config::seconds(&app.config.metrics.runtime_interval)?);
    loop {
        let Some(leader) = crate::cluster::try_leader(&app, LEADER_KEY).await? else {
            tokio::time::sleep(interval).await;
            continue;
        };
        let result: Result<()> = async {
            let point = runtime_point(&app).await?;
            app.metrics
                .sample_runtime(
                    point,
                    config::seconds(&app.config.metrics.runtime_retention)? as f64,
                )
                .await?;
            Ok(())
        }
        .await;
        if let Err(error) = result {
            tracing::warn!(%error,"runtime sample failed");
        }
        leader.finish().await?;
        tokio::time::sleep(interval).await;
    }
}

pub(super) use crate::model::metrics::{
    CapacityPoint, RuntimePoint, ScopeQuery, StorageUsage, WorkSummary,
};
