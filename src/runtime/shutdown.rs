use crate::app::App;
use anyhow::{Context, Result, ensure};
use std::{future::Future, sync::Arc, sync::atomic::Ordering, time::Duration};

pub(super) async fn drain(app: Arc<App>, workers: impl Future<Output = Result<()>>) -> Result<()> {
    let timeout = Duration::from_secs(app.config.io.shutdown_seconds()?);
    let deadline = tokio::time::Instant::now() + timeout;
    app.draining.store(true, Ordering::Release);
    app.stop_connections.send_replace(true);
    app.wake_tasks.notify_waiters();
    let work = async {
        {
            let _coord = app.coord.lock().await;
            app.maintenance.store(true, Ordering::Release);
        }
        #[cfg(feature = "fault-injection")]
        crate::faults::point("runtime-draining").await;
        while app.stop_connections.receiver_count() != 0 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        if !matches!(app.role.as_str(), "single" | "data" | "worker" | "node")
            || app.ensure_mode().is_err()
            || app.recovery_required()
        {
            return Ok(());
        }
        let node = app.node.get().map(|node| node.node_id);
        let id = app.upload_cache.flush(node).await?;
        tracing::info!(task_id=%id, "shutdown flush started");
        app.wake_tasks.notify_waiters();
        let completed = async {
            loop {
                let task = app
                    .task_control
                    .raw(id)
                    .await?
                    .context("shutdown flush disappeared")?;
                match task["state"].as_str() {
                    Some("completed") => return Ok::<_, anyhow::Error>(()),
                    Some("failed" | "paused") => {
                        anyhow::bail!("shutdown flush {id} stopped: {}", task["error"])
                    }
                    _ => tokio::time::sleep(Duration::from_millis(100)).await,
                }
            }
        };
        tokio::select! {
            result = completed => result,
            result = crate::worker::scheduler::flush(app.clone()) => result,
        }
    };
    let result = tokio::time::timeout_at(deadline, async {
        tokio::select! {
            result = work => result,
            result = workers => { result?; anyhow::bail!("background workers stopped before shutdown flush completed") },
        }
    }).await;
    if matches!(result, Ok(Ok(()))) {
        tracing::info!("shutdown flush completed");
    } else {
        let node = app.node.get().map(|node| node.node_id);
        let backlog = tokio::time::timeout(Duration::from_secs(1), async {
            let (uploads, buffered, chunks) =
                tokio::try_join!(app.upload_cache.backlog(node), app.raw.buffered_status(), app.physical.buffered_status())?;
            Ok::<_, anyhow::Error>(
                serde_json::json!({"uploads": uploads, "buffered_files": buffered, "buffered_chunks": chunks}),
            )
        })
        .await;
        tracing::warn!(
            ?result,
            ?node,
            ?backlog,
            active_operations = app.active_operations(),
            "shutdown flush incomplete; pending sources remain for recovery"
        );
    }
    Ok(())
}

pub(super) async fn background(app: Arc<App>) -> Result<()> {
    ensure!(
        app.role == "single",
        "local storage workers require single mode"
    );
    tokio::select! {
        result = crate::worker::scheduler::run(app.clone()) => result,
        result = crate::worker::raw::run(app.clone()) => result,
        result = crate::worker::raw_upload::run(app.clone()) => result,
        result = crate::worker::sync::run(app.clone()) => result,
        result = app.storage.watch_write_backlog() => result,
        result = app.storage.compression.run() => result,
    }
}
