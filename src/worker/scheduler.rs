use crate::app::App;
use crate::config;
use crate::model::task::Execution;
use anyhow::{Context, Result};
use futures_util::StreamExt;
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

pub async fn recover_stale(app: &App) -> Result<u64> {
    app.cluster()?.recover_tasks().await
}

pub(crate) async fn flush(app: Arc<App>) -> Result<()> {
    anyhow::ensure!(
        app.draining.load(std::sync::atomic::Ordering::Acquire),
        "flush worker requires shutdown"
    );
    // A paused CDC conversion must not occupy the last shutdown flush worker.
    worker(
        app,
        Arc::new(tokio::sync::Mutex::new(std::collections::HashSet::new())),
        2,
    )
    .await
}

pub async fn run(app: Arc<App>) -> Result<()> {
    if app.role == "single" {
        return run_single(app).await;
    }
    if app.role != "single" {
        let listener_app = app.clone();
        tokio::spawn(async move {
            loop {
                let result: Result<()> = async {
                    listener_app
                        .cluster()?
                        .listen_tasks(listener_app.wake_tasks.clone())
                        .await
                }
                .await;
                if let Err(error) = result {
                    tracing::warn!(%error, "task notification listener will retry");
                }
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        });
    }
    let active = Arc::new(tokio::sync::Mutex::new(std::collections::HashSet::new()));
    let mut workers = futures_util::stream::FuturesUnordered::new();
    for class in std::iter::repeat_n(
        0,
        app.budget
            .cpu_jobs
            .min(app.budget.read_concurrency)
            .clamp(1, 4),
    )
    .chain(std::iter::repeat_n(1, app.budget.cpu_jobs.clamp(1, 4)))
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

async fn run_single(app: Arc<App>) -> Result<()> {
    let limits = [
        app.budget
            .cpu_jobs
            .min(app.budget.read_concurrency)
            .clamp(1, 4),
        app.budget.cpu_jobs.clamp(1, 4),
        app.budget
            .cpu_jobs
            .min(app.budget.upload_concurrency)
            .clamp(1, 4),
    ];
    let (completed, receiver) = tokio::sync::mpsc::channel(limits.iter().sum());
    let mut queues = Vec::new();
    let mut workers = futures_util::stream::FuturesUnordered::new();
    for (class, limit) in limits.into_iter().enumerate() {
        let (sender, receiver) = tokio::sync::mpsc::channel(limit);
        queues.push(sender);
        let receiver = Arc::new(tokio::sync::Mutex::new(receiver));
        for _ in 0..limit {
            workers.push(single_worker(
                app.clone(),
                class,
                receiver.clone(),
                completed.clone(),
            ));
        }
    }
    tokio::select! {
        result = dispatch_single(app, limits, queues, receiver) => result,
        result = workers.next() => result.context("single workers stopped")?,
    }
}

async fn single_worker(
    app: Arc<App>,
    class: usize,
    receiver: Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<Execution>>>,
    completed: tokio::sync::mpsc::Sender<(Uuid, usize)>,
) -> Result<()> {
    loop {
        let task = receiver
            .lock()
            .await
            .recv()
            .await
            .context("single dispatcher stopped")?;
        execute(app.clone(), task.id, task.token).await?;
        completed.send((task.id, class)).await?;
    }
}

async fn dispatch_single(
    app: Arc<App>,
    limits: [usize; 3],
    queues: Vec<tokio::sync::mpsc::Sender<Execution>>,
    mut completed: tokio::sync::mpsc::Receiver<(Uuid, usize)>,
) -> Result<()> {
    let mut occupied = [0usize; 3];
    let mut active = std::collections::HashSet::new();
    loop {
        // Register before querying so a committed enqueue cannot race the wait.
        let notified = app.wake_tasks.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        for class in 0..limits.len() {
            let mut upload_enqueued = false;
            while occupied[class] < limits[class] {
                let token = Uuid::new_v4();
                let ids: Vec<Uuid> = active.iter().copied().collect();
                let id = app
                    .task_execution
                    .claim(crate::model::TaskClaim {
                        draining: app.draining.load(std::sync::atomic::Ordering::Acquire),
                        class: class as i32,
                        active: &ids,
                        token,
                    })
                    .await?;
                let Some(id) = id else {
                    if class == 2 && !upload_enqueued {
                        upload_enqueued = true;
                        match app.enqueue_upload().await {
                            Ok(true) => continue,
                            Ok(false) => {}
                            Err(error) => {
                                tracing::warn!(%error, "pending upload scheduling failed")
                            }
                        }
                    }
                    break;
                };
                upload_enqueued = false;
                active.insert(id);
                occupied[class] += 1;
                queues[class].send(Execution { id, token }).await?;
            }
        }
        let mut delay = Duration::from_secs(30);
        if occupied[2] < limits[2]
            && !app.maintenance.load(std::sync::atomic::Ordering::Acquire)
            && app.ensure_mode().is_ok()
            && let Some(next) = app.upload_cache.next_attempt().await?
        {
            delay = (next - chrono::Utc::now())
                .to_std()
                .unwrap_or(Duration::from_secs(1))
                .min(delay);
        }
        #[cfg(feature = "fault-injection")]
        crate::faults::point("scheduler-before-wait").await;
        tokio::select! {
            _ = &mut notified => {},
            result = completed.recv() => {
                let (id, class) = result.context("single workers stopped")?;
                active.remove(&id);
                occupied[class] -= 1;
            },
            _ = tokio::time::sleep(delay) => {},
        }
    }
}

fn renew_lease(app: Arc<App>, id: Uuid, token: Uuid) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            let result = async { app.cluster()?.renew_task(id, token).await }.await;
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
        let token = Uuid::new_v4();
        let id = {
            let mut active = active.lock().await;
            let ids: Vec<Uuid> = active.iter().copied().collect();
            let input = crate::model::TaskClaim {
                draining: app.draining.load(std::sync::atomic::Ordering::Acquire),
                class,
                active: &ids,
                token,
            };
            let id = if app.role == "single" {
                app.task_execution.claim(input).await?
            } else {
                app.cluster()?
                    .claim_task(
                        input,
                        app.node
                            .get()
                            .context("cluster task claim requires a registered node")?,
                        &app.role,
                        &app.secrets.credential_key,
                    )
                    .await?
            };
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
        execute(app.clone(), id, token).await?;
        active.lock().await.remove(&id);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn execute(app: Arc<App>, id: Uuid, token: Uuid) -> Result<()> {
    let crate::model::TaskRecord {
        task_type,
        state,
        bucket_id: bucket,
        cursor,
        detail,
        execution_token: task_token,
    } = app.task_execution.load(id).await?;
    let owned = app.task_execution.confirm(id, token).await?
        && state == "running"
        && task_token == Some(token);
    if !owned {
        return Ok(());
    }
    if !crate::worker::task::fence(&app, id, token).await? {
        return Ok(());
    }
    let execution = Execution { id, token };
    let renewal = (app.role != "single").then(|| renew_lease(app.clone(), id, token));
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
            config::seconds(&app.config.maintenance.max_duration)?
        };
        match tokio::time::timeout(
            Duration::from_secs(timeout),
            crate::storage::resource_gate::PRIORITY.scope(
                crate::storage::resource_gate::MAINTENANCE,
                async {
                    if detail.get("source_operation").is_some() {
                        app.source_batch(execution, detail).await
                    } else {
                        app.integrity_batch(execution, cursor.as_deref(), detail)
                            .await
                    }
                },
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
    } else if detail["operation"] == "cdc" {
        crate::storage::resource_gate::PRIORITY
            .scope(
                crate::storage::resource_gate::MAINTENANCE,
                app.convert_raw(execution, detail),
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
                app.storage_migration_batch(id, token, detail),
            )
            .await
    } else if task_type == "rewrite" {
        let result = crate::storage::resource_gate::PRIORITY
            .scope(
                crate::storage::resource_gate::MAINTENANCE,
                app.rewrite_batch(id, token, cursor.as_deref(), detail),
            )
            .await;
        result
    } else {
        Err(anyhow::anyhow!("unknown maintenance task type"))
    };
    if let Some(renewal) = renewal {
        renewal.abort();
    }
    app.task_execution
        .finish(crate::model::TaskFinish {
            execution,
            error: result.as_ref().err().map(ToString::to_string),
        })
        .await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point(&format!("task-finished-{token}")).await;
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
    Ok(())
}
