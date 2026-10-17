use crate::{
    app::App,
    model::node::{NodeReport, REPORT_FORMAT},
};
use anyhow::Result;
use serde_json::{Value, json};
use std::{
    sync::{Arc, atomic::Ordering::Relaxed},
    time::Duration,
};

async fn collect(app: &App, restricted: bool) -> Result<(Value, Value)> {
    let runtime = app.statistics.runtime();
    let (size, idle) = app.runtime.pool();
    let (cache_limit, upload_limit, upload_bytes) = app.storage.disk.upload_capacity()?;
    let local = app.storage.disk.used();
    let metadata = json!({
        "format": REPORT_FORMAT,
        "fields": crate::control::service::loaded_config(app)?,
        "revisions": *app.runtime_revisions.read().unwrap(),
        "loaded": crate::model::runtime_defaults(&app.config).into_iter().map(|(key, value)| (key.to_owned(), value)).collect::<serde_json::Map<_, _>>()
    });
    let work = if app.recovery_required() {
        "recovery"
    } else if restricted {
        "restricted"
    } else if app.maintenance.load(Relaxed) {
        "maintenance"
    } else {
        "normal"
    };
    let memory = crate::status::stats::process_memory().await;
    let status = json!({
        "work": work, "runtime": runtime, "resources": app.budget,
        "process_memory": memory, "db_pool_size": size, "db_pool_idle": idle,
        "active_streams": app.active.lock().unwrap().len(), "data_slots_available": app.slots.available_permits(),
        "upload_slots_available": app.uploads.available_permits(), "read_slots_available": app.reads.available_permits(),
        "io": app.storage.statistics(),
        "cache": {"bytes": local[1], "limit": cache_limit, "upload_bytes": upload_bytes,
            "upload_limit": upload_limit, "multipart_bytes": local[0], "thumbnail_bytes": local[2],
            "hits": app.storage.cache_hits.load(Relaxed),
            "backend_read_bytes": app.storage.backend_read_bytes.load(Relaxed),
            "backend_write_bytes": app.storage.backend_write_bytes.load(Relaxed)},
        "response_cache": app.response_cache.snapshot(),
        "compression": app.compression_observations.snapshot(),
        "read_replica": app.runtime.read_replica()
    });
    Ok((metadata, status))
}

// A single in-flight report, independent of heartbeats and request handling.
pub(crate) async fn run(app: Arc<App>, restricted: bool) -> Result<()> {
    let (Some(state), Some(boot)) = (app.node.get(), app.node_boot.get()) else {
        return std::future::pending().await;
    };
    let mut sequence = 0i64;
    let mut sent_metadata = Value::Null;
    let sample_interval = Duration::from_secs(crate::config::seconds(
        &app.config.metrics.runtime_interval,
    )?);
    let mut sampled = None::<std::time::Instant>;
    loop {
        let result = tokio::time::timeout(Duration::from_secs(2), async {
            let (metadata, status) = collect(&app, restricted).await?;
            sequence = sequence.saturating_add(1);
            let report = NodeReport {
                boot_id: *boot,
                sequence,
                sampled_at: chrono::Utc::now(),
                metadata: (metadata != sent_metadata).then(|| metadata.clone()),
                status,
            };
            report.validate()?;
            if app
                .cluster()?
                .report(state, &app.role, &app.secrets.credential_key, &report)
                .await?
            {
                sent_metadata = metadata;
                if sampled.is_none_or(|at| at.elapsed() >= sample_interval) {
                    let sample = crate::model::metrics::NodeSample::from_report(
                        &report,
                        sample_interval.as_secs(),
                    )?;
                    if app
                        .cluster()?
                        .sample_node_runtime(state, &app.role, &app.secrets.credential_key, &sample)
                        .await?
                    {
                        sampled = Some(std::time::Instant::now());
                    }
                }
            }
            Ok::<_, anyhow::Error>(())
        })
        .await;
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::warn!(%error, "node report failed"),
            Err(_) => tracing::warn!("node report timed out"),
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}

pub(crate) async fn aggregate(app: Arc<App>) -> Result<()> {
    let interval = crate::config::seconds(&app.config.metrics.runtime_interval)?;
    let retention = crate::config::seconds(&app.config.metrics.runtime_retention)?;
    loop {
        match tokio::time::timeout(
            Duration::from_secs(5),
            app.cluster()?.aggregate_runtime(interval, retention),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::warn!(%error, "cluster runtime aggregation failed"),
            Err(_) => tracing::warn!("cluster runtime aggregation timed out"),
        }
        tokio::time::sleep(Duration::from_secs(interval)).await;
    }
}
