use crate::app::App;
use crate::config;
use anyhow::{Context, Result};
use futures_util::StreamExt;
use serde_json::Value;
use sqlx::postgres::PgListener;
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

pub async fn recover_stale(app: &App) -> Result<u64> {
    let mut tx = app.db.begin().await?;
    let stale: Vec<Uuid> = sqlx::query_scalar(
        "SELECT id FROM tasks WHERE state='running' AND (lease_expires_at IS NULL OR lease_expires_at<now()) FOR UPDATE",
    )
    .fetch_all(&mut *tx)
    .await?;
    if stale.is_empty() {
        tx.commit().await?;
        return Ok(0);
    }
    sqlx::query(
        "UPDATE streams SET state='abandoned',owner_task_id=NULL,owner_lease_token=NULL,touched_at=now() WHERE state='writing' AND owner_task_id=ANY($1)",
    )
    .bind(&stale)
    .execute(&mut *tx)
    .await?;
    let result = sqlx::query(
        "UPDATE tasks SET state='queued',lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=ANY($1) AND state='running'",
    )
    .bind(&stale)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(result.rows_affected())
}

pub async fn run(app: Arc<App>) -> Result<()> {
    let listener_app = app.clone();
    tokio::spawn(async move {
        let mut listener = match PgListener::connect_with(&listener_app.db).await {
            Ok(listener) => listener,
            Err(error) => {
                tracing::warn!(%error, "task notification listener unavailable");
                return;
            }
        };
        if let Err(error) = listener.listen("mokyu_tasks").await {
            tracing::warn!(%error, "task notification subscription failed");
            return;
        }
        loop {
            match listener.recv().await {
                Ok(_notification) => {
                    listener_app.wake_tasks.notify_waiters();
                }
                Err(error) => {
                    tracing::warn!(%error, "task notification listener stopped");
                    break;
                }
            }
        }
    });
    let lease_node_id = crate::cluster::local_node_id(&app).await?;
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
        workers.push(worker(app.clone(), active.clone(), class, lease_node_id));
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
            let result = sqlx::query(
                "UPDATE tasks SET lease_expires_at=now()+interval '5 minutes' WHERE id=$1 AND state='running' AND lease_token=$2 AND lease_expires_at>now()",
            )
            .bind(id)
            .bind(token)
            .execute(&app.db)
            .await;
            match result {
                Ok(result) if result.rows_affected() == 1 => {}
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
    lease_node_id: Option<Uuid>,
) -> Result<()> {
    loop {
        let lease_token = Uuid::new_v4();
        let id = {
            let mut active = active.lock().await;
            let ids: Vec<Uuid> = active.iter().copied().collect();
            let mut tx = app.db.begin().await?;
            if let Some(state) = app.node.get() {
                let role = match app.role.as_str() {
                    "single" | "control" => "control",
                    role => role,
                };
                crate::cluster::validate_node(&app, &mut *tx, state, role).await?;
            }
            let id:Option<Uuid>=sqlx::query_scalar("UPDATE tasks SET state='running',started_at=coalesce(started_at,now()),lease_node_id=$3,lease_token=$4,lease_expires_at=now()+interval '5 minutes',updated_at=now() WHERE id=(SELECT t.id FROM tasks t CROSS JOIN mokyu_meta m WHERE (t.state='queued' OR (t.state='running' AND t.lease_expires_at<now())) AND CASE WHEN t.task_type IN ('pack','unpack') THEN 1 WHEN t.task_type IN ('upload','cache_flush','storage_migration','rewrite') THEN 2 ELSE 0 END=$1 AND NOT(t.id=ANY($2)) AND (t.execution_scope <> 'node' OR t.target_node_id IS NULL OR t.target_node_id=$3) AND t.detail->>'role' IS DISTINCT FROM 'parent' AND NOT (m.maintenance AND t.task_type IN ('pack','unpack','purge','gc','storage_migration','rewrite','usage')) AND NOT EXISTS(SELECT 1 FROM maintenance_controls c WHERE c.kind=CASE WHEN t.task_type='pack' THEN t.detail->>'kind' ELSE t.task_type END AND CASE WHEN c.kind='gc' THEN m.gc_paused ELSE c.paused END) AND NOT (t.task_type='pack' AND t.detail->>'kind' IN ('pack','repack') AND m.pack_creation_paused) ORDER BY t.updated_at,t.id LIMIT 1) AND (state='queued' OR (state='running' AND lease_expires_at<now())) RETURNING id").bind(class).bind(ids).bind(lease_node_id).bind(lease_token).fetch_optional(&mut *tx).await?;
            tx.commit().await?;
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
            let (kind, state, bucket, cursor, detail, task_lease, parent_task_id): (
                String,
                String,
                Option<Uuid>,
                Option<String>,
                Value,
                Option<Uuid>,
                Option<Uuid>,
            ) = sqlx::query_as(
                "SELECT task_type,state,bucket_id,cursor,detail,lease_token,parent_task_id FROM tasks WHERE id=$1",
            )
            .bind(id)
            .fetch_one(&app.db)
            .await?;
            let owned = sqlx::query_scalar::<_, bool>("UPDATE tasks SET lease_expires_at=now()+interval '5 minutes' WHERE id=$1 AND state='running' AND lease_token=$2 RETURNING true")
                .bind(id)
                .bind(lease_token)
                .fetch_optional(&app.db)
                .await?
                .is_some_and(|value| value)
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
            let renewal = renew_lease(app.clone(), id, lease_token);
            let result = if matches!(kind.as_str(), "gc" | "cleanup") {
                app.lifecycle_batch(id, &kind, &detail).await
            } else if kind == "usage" {
                app.usage_batch(id, detail.clone()).await
            } else if kind == "purge" {
                app.purge_batch(
                    id,
                    bucket.context("purge bucket disappeared")?,
                    cursor.as_deref(),
                )
                .await
                .map(|_| false)
            } else if kind == "sweep" {
                app.sweep_batch(id, cursor.as_deref(), detail)
                    .await
                    .map(|_| false)
            } else if kind == "integrity" {
                let timeout = if let Some(storage_id) = detail["storage_id"]
                    .as_str()
                    .and_then(|value| value.parse::<Uuid>().ok())
                {
                    crate::storage::profile::StorageProfile::load(&app.db, storage_id)
                        .await
                        .map(|profile| config::seconds(&profile.integrity_batch_timeout))??
                } else {
                    config::seconds(&app.config.local_integrity.request_timeout)?
                };
                match tokio::time::timeout(
                    Duration::from_secs(timeout),
                    crate::storage::resource_gate::PRIORITY.scope(
                        crate::storage::resource_gate::MAINTENANCE,
                        app.integrity_batch(id, cursor.as_deref(), detail),
                    ),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(anyhow::anyhow!(
                        "integrity batch timed out waiting for database, resources or backend; resume to retry"
                    )),
                }
            } else if kind == "pack" || kind == "unpack" {
                crate::storage::resource_gate::PRIORITY
                    .scope(
                        crate::storage::resource_gate::MAINTENANCE,
                        app.pack_batch(id, cursor.as_deref(), detail, kind == "unpack"),
                    )
                    .await
            } else if kind == "upload" || kind == "cache_flush" {
                crate::storage::resource_gate::PRIORITY
                    .scope(
                        crate::storage::resource_gate::UPLOAD,
                        crate::upload::cache::FLUSH.scope(
                            kind == "cache_flush",
                            app.upload_batch(id, detail, kind == "cache_flush"),
                        ),
                    )
                    .await
            } else if kind == "storage_migration" {
                crate::storage::resource_gate::PRIORITY
                    .scope(
                        crate::storage::resource_gate::UPLOAD,
                        app.storage_migration_batch(id, lease_token, detail),
                    )
                    .await
            } else if kind == "rewrite" {
                let result = crate::storage::resource_gate::PRIORITY
                    .scope(
                        crate::storage::resource_gate::MAINTENANCE,
                        app.rewrite_batch(id, lease_token, cursor.as_deref(), detail),
                    )
                    .await;
                result
            } else {
                Err(anyhow::anyhow!("unknown maintenance task kind"))
            };
            renewal.abort();
            // A bounded batch which did not finish must become claimable again
            // immediately. A batch may also finish its own row (uploads and
            // GC do this while committing their final database work), so only
            // requeue rows that are still running.
            let completed = if result.is_ok() {
                let state: Option<String> =
                    sqlx::query_scalar("SELECT state FROM tasks WHERE id=$1")
                        .bind(id)
                        .fetch_optional(&app.db)
                        .await?;
                state.as_deref() == Some("completed")
            } else {
                false
            };
            if completed {
                sqlx::query("UPDATE tasks SET state='completed',lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1 AND (state='completed' OR (state='running' AND lease_token=$2))")
                    .bind(id)
                    .bind(lease_token)
                    .execute(&app.db)
                    .await?;
            } else if result.is_ok() {
                sqlx::query("UPDATE tasks SET state='queued',lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$2")
                    .bind(id)
                    .bind(lease_token)
                    .execute(&app.db)
                    .await?;
            }
            if kind == "gc" && result.is_err() {
                app.statistics
                    .gc_failures
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            if class != 0 {
                if class == 1 && result.is_err() && matches!(kind.as_str(), "pack" | "unpack") {
                    sqlx::query("UPDATE pack_changes SET next_check_at=now()+$2*interval '1 second' WHERE pack_id IN (SELECT c.pack_id FROM pack_inputs i JOIN chunks c ON c.id=i.chunk_id WHERE i.task_id=$1)").bind(id).bind(config::seconds(&app.config.local_pack.reuse_interval)? as f64).execute(&app.db).await?;
                    sqlx::query("UPDATE pack_maintenance SET next_check_at=now()+$2*interval '1 second' WHERE stream_id IN (SELECT e.stream_id FROM extents e JOIN pack_inputs i ON i.chunk_id=e.chunk_id WHERE i.task_id=$1)").bind(id).bind(config::seconds(&app.config.local_pack.reuse_interval)? as f64).execute(&app.db).await?;
                }
                if (completed || result.is_err())
                    && matches!(kind.as_str(), "pack" | "unpack" | "rewrite")
                {
                    app.finish_pack_work(id).await?;
                }
            }
            match result {
                Ok(true) => {}
                Ok(false) => {}
                Err(e) => {
                    tracing::error!(task_id=%id,error=%e,"maintenance task stopped");
                    if class == 2 && matches!(kind.as_str(), "upload" | "cache_flush") {
                        app.upload_retry(id, &e.to_string()).await?;
                    } else {
                        sqlx::query("UPDATE tasks SET state='failed',error=$2,lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$3").bind(id).bind(e.to_string()).bind(lease_token).execute(&app.db).await?;
                        if parent_task_id.is_some() {
                            if let Some(parent) = parent_task_id {
                                sqlx::query("UPDATE tasks SET state='failed',error=$2,updated_at=now() WHERE id=$1 AND state IN ('queued','running','paused')")
                                    .bind(parent)
                                    .bind(e.to_string())
                                    .execute(&app.db)
                                    .await?;
                            }
                        }
                    }
                }
            }
            if completed {
                if let Some(parent) = parent_task_id {
                    sqlx::query("UPDATE tasks p SET batch_completed=(SELECT count(*) FROM tasks c WHERE c.parent_task_id=p.id AND c.state='completed'),processed=(SELECT count(*) FROM tasks c WHERE c.parent_task_id=p.id AND c.state='completed'),state=CASE WHEN (SELECT count(*) FROM tasks c WHERE c.parent_task_id=p.id AND c.state='completed')>=coalesce(p.batch_total,0) THEN 'completed' ELSE p.state END,updated_at=now() WHERE p.id=$1 AND p.state IN ('queued','running')")
                        .bind(parent)
                        .execute(&app.db)
                        .await?;
                }
            }
            active.lock().await.remove(&id);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}
