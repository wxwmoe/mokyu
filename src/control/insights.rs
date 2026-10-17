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
    scope: &'static str,
    cluster: Option<crate::model::metrics::ClusterRuntime>,
    current: Option<RuntimePoint>,
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
#[utoipa::path(get,operation_id="runtime_insights",path="/api/v1/usage/runtime",params(crate::model::metrics::RuntimeQuery),responses((status=200,body=RuntimeInsights)))]
pub(super) async fn runtime(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Query(query): Query<crate::model::metrics::RuntimeQuery>,
) -> Result<Json<RuntimeInsights>, HttpError> {
    query
        .validate()
        .map_err(|_| s3s::s3_error!(InvalidArgument))?;
    let crate::model::metrics::RuntimeHistory {
        mut history,
        work,
        pending_entries,
        pending_bytes,
        oldest_pending,
        failed_uploads,
    } = app.metrics.runtime(&actor.principal).await?;
    let cluster = if app.role == "single" {
        None
    } else {
        let mut history = app.cluster()?.cluster_runtime(&query).await?;
        let current = history.pop();
        let stale = current.as_ref().is_none_or(|p| {
            Utc::now().signed_duration_since(p.at).num_seconds()
                > config::seconds(&app.config.metrics.runtime_interval)
                    .unwrap_or(60)
                    .saturating_mul(2)
                    .saturating_add(30) as i64
        });
        Some(crate::model::metrics::ClusterRuntime {
            current,
            history,
            stale,
        })
    };
    history.retain(|p| p.at >= Utc::now() - chrono::Duration::hours(i64::from(query.hours)));
    history.drain(..history.len().saturating_sub(query.limit as usize));
    Ok(Json(RuntimeInsights {
        cluster,
        scope: if app.role == "single" {
            "single"
        } else {
            "cluster"
        },
        current: if app.role == "single" {
            Some(runtime_point(&app).await?)
        } else {
            None
        },
        history: if app.role == "single" {
            history
        } else {
            Vec::new()
        },
        work,
        pending_entries,
        pending_bytes,
        oldest_pending,
        failed_uploads,
    }))
}
pub(crate) async fn runtime_loop(app: Arc<App>) -> Result<()> {
    // Unattributed local samples cannot represent cluster history.
    if app.role != "single" {
        return std::future::pending().await;
    }
    let interval = Duration::from_secs(config::seconds(&app.config.metrics.runtime_interval)?);
    loop {
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
        tokio::time::sleep(interval).await;
    }
}

pub(super) use crate::model::metrics::{
    CapacityPoint, RuntimePoint, ScopeQuery, StorageUsage, WorkSummary,
};
