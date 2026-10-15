use crate::app::App;
use crate::config;
use crate::model::task::Execution;
use anyhow::{Context, Result};
use futures_util::StreamExt;
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

pub async fn recover_stale(app: &App) -> Result<u64> {
    app.task_leases.recover().await
}

pub async fn run(app: Arc<App>) -> Result<()> {
    let listener_app = app.clone();
    tokio::spawn(async move {
        if let Err(error) = listener_app
            .task_control
            .listen(listener_app.wake_tasks.clone())
            .await
        {
            tracing::warn!(%error, "task notification listener stopped");
        }
    });
    let active = Arc::new(tokio::sync::Mutex::new(std::collections::HashSet::new()));
    let mut workers = futures_util::stream::FuturesUnordered::new();
    for class in std::iter::repeat_n(
        0,
        app.budget
            .cpu_jobs
            .min(app.budget.read_concurrency)
            .clamp(1, 4),
    )
    .chain(std::iter::repeat_n(
        1,
        app.config.local_pack.maintenance_concurrency,
    ))
    .chain(std::iter::repeat_n(
        2,
        app.budget
            .cpu_jobs
            .min(app.budget.upload_concurrency)
            .clamp(1, 4),
    )) {
        workers.push(worker(app.clone(), active.clone(), class));
    }
    workers
        .next()
        .await
        .context("maintenance workers stopped")?
}

fn renew_lease(app: Arc<App>, id: Uuid, token: Uuid) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            let result = app.task_leases.renew(id, token).await;
            match result {
                Ok(true) => {}
                Ok(_) => {
                    tracing::warn!(task_id=%id, "task lease was lost; result will be discarded");
                    break;
                }
                Err(error) => {
                    tracing::warn!(task_id=%id, %error, "task lease renewal failed");
                }
            }
        }
    })
}

async fn worker(
    app: Arc<App>,
    active: Arc<tokio::sync::Mutex<std::collections::HashSet<Uuid>>>,
    class: i32,
) -> Result<()> {
    loop {
        let lease_token = Uuid::new_v4();
        let id = {
            let mut active = active.lock().await;
            let ids: Vec<Uuid> = active.iter().copied().collect();
            let id = app
                .task_leases
                .claim(crate::model::TaskClaim {
                    class,
                    active: &ids,
                    node: app.node.get(),
                    role: &app.role,
                    credential_key: &app.secrets.credential_key,
                    token: lease_token,
                })
                .await?;
            if let Some(id) = id {
                active.insert(id);
            }
            id
        };
        let Some(id) = id else {
            if class == 2 {
                match app.enqueue_upload().await {
                    Ok(true) => continue,
                    Ok(false) => {}
                    Err(e) => tracing::warn!(error=%e,"pending upload scheduling failed"),
                }
            }
            tokio::select! {_=app.wake_tasks.notified()=>{},_=tokio::time::sleep(Duration::from_secs(1))=>{}}
            continue;
        };
        {
            let crate::model::TaskRecord {
                task_type,
                state,
                bucket_id: bucket,
                cursor,
                detail,
                lease_token: task_lease,
            } = app.task_leases.load(id).await?;
            let owned = app.task_leases.confirm(id, lease_token).await?
                && state == "running"
                && task_lease == Some(lease_token);
            if !owned {
                active.lock().await.remove(&id);
                continue;
            }
            if !crate::worker::task::fence(&app, id, lease_token).await? {
                active.lock().await.remove(&id);
                continue;
            }
            let execution = Execution {
                id,
                token: lease_token,
            };
            let renewal = renew_lease(app.clone(), id, lease_token);
            let result = if matches!(task_type.as_str(), "gc" | "cleanup") {
                app.lifecycle_batch(execution, &task_type, &detail).await
            } else if task_type == "usage" {
                app.usage_batch(execution, detail.clone()).await
            } else if task_type == "purge" {
                app.purge_batch(
                    execution,
                    bucket.context("purge bucket disappeared")?,
                    cursor.as_deref(),
                )
                .await
                .map(|_| false)
            } else if task_type == "sweep" {
                app.sweep_batch(execution, cursor.as_deref(), detail)
                    .await
                    .map(|_| false)
            } else if task_type == "integrity" {
                let timeout = if let Some(storage_id) = detail["storage_id"]
                    .as_str()
                    .and_then(|value| value.parse::<Uuid>().ok())
                {
                    app.storage_profiles
                        .load(storage_id)
                        .await
                        .map(|profile| config::seconds(&profile.integrity_batch_timeout))??
                } else {
                    config::seconds(&app.config.local_integrity.request_timeout)?
                };
                match tokio::time::timeout(
                    Duration::from_secs(timeout),
                    crate::storage::resource_gate::PRIORITY.scope(
                        crate::storage::resource_gate::MAINTENANCE,
                        app.integrity_batch(execution, cursor.as_deref(), detail),
                    ),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(anyhow::anyhow!(
                        "integrity batch timed out waiting for database, resources or backend; resume to retry"
                    )),
                }
            } else if task_type == "pack" || task_type == "unpack" {
                crate::storage::resource_gate::PRIORITY
                    .scope(
                        crate::storage::resource_gate::MAINTENANCE,
                        app.pack_batch(execution, cursor.as_deref(), detail, task_type == "unpack"),
                    )
                    .await
            } else if task_type == "upload" || task_type == "cache_flush" {
                crate::storage::resource_gate::PRIORITY
                    .scope(
                        crate::storage::resource_gate::UPLOAD,
                        crate::upload::cache::FLUSH.scope(
                            task_type == "cache_flush",
                            app.upload_batch(execution, detail, task_type == "cache_flush"),
                        ),
                    )
                    .await
            } else if task_type == "storage_migration" {
                crate::storage::resource_gate::PRIORITY
                    .scope(
                        crate::storage::resource_gate::UPLOAD,
                        app.storage_migration_batch(id, lease_token, detail),
                    )
                    .await
            } else if task_type == "rewrite" {
                let result = crate::storage::resource_gate::PRIORITY
                    .scope(
                        crate::storage::resource_gate::MAINTENANCE,
                        app.rewrite_batch(id, lease_token, cursor.as_deref(), detail),
                    )
                    .await;
                result
            } else {
                Err(anyhow::anyhow!("unknown maintenance task type"))
            };
            renewal.abort();
            app.task_leases
                .finish(crate::model::TaskFinish {
                    execution,
                    reuse_seconds: config::seconds(&app.config.local_pack.reuse_interval)?,
                    error: result.as_ref().err().map(ToString::to_string),
                })
                .await?;
            #[cfg(feature = "fault-injection")]
            crate::faults::point(&format!("task-finished-{lease_token}")).await;
            if task_type == "gc" && result.is_err() {
                app.statistics
                    .gc_failures
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            match result {
                Ok(true) => {}
                Ok(false) => {}
                Err(e) => {
                    tracing::error!(task_id=%id,error=%e,"maintenance task stopped");
                }
            }
            active.lock().await.remove(&id);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}
