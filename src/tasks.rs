use crate::{app::App, config};
use anyhow::{Context, Result, ensure};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use uuid::Uuid;
tokio::task_local! { pub(crate) static REQUEST_ID: Uuid; }

pub(crate) async fn insert(
    db: &mut sqlx::PgConnection,
    kind: &str,
    bucket: Option<Uuid>,
    detail: Value,
) -> Result<Uuid> {
    let id = REQUEST_ID
        .try_with(|id| *id)
        .unwrap_or_else(|_| Uuid::new_v4());
    sqlx::query("INSERT INTO tasks(id,kind,bucket_id,state,detail,created_by,source) VALUES($1,$2,$3,'queued',$4,coalesce((SELECT actor_label FROM audit_events WHERE id=$5),'Scheduler'),coalesce((SELECT source FROM audit_events WHERE id=$5),'system'))")
        .bind(id).bind(kind).bind(bucket).bind(detail).bind(crate::manage::audit::current()).execute(db).await?;
    Ok(id)
}

impl App {
    pub async fn purge_preview(&self, name: &str) -> Result<Value> {
        let b = self.bucket(name, false).await?;
        let (objects,uploads):(i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM objects WHERE bucket_id=$1 AND stream_id IS NOT NULL),(SELECT count(*) FROM uploads WHERE bucket_id=$1 AND state IN ('active','completing'))").bind(b.id).fetch_one(&self.db).await?;
        Ok(
            json!({"bucket_id":b.id,"name":b.name,"state":b.state,"objects":objects,"active_uploads":uploads,"effect":"seal bucket, remove every object and upload, then delete bucket; shared chunks use normal GC grace"}),
        )
    }
    pub async fn purge_start(
        &self,
        principal: &crate::authorization::Principal,
        name: &str,
        id: Uuid,
        confirmation: Option<&str>,
    ) -> Result<Value> {
        self.writable()?;
        let _coord = self.coord.lock().await;
        self.writable()?;
        let mut tx = crate::manage::users::admin_transaction(self, principal).await?;
        let found: Option<Uuid> =
            sqlx::query_scalar("SELECT id FROM buckets WHERE name=$1 AND id=$2 FOR UPDATE")
                .bind(name)
                .bind(id)
                .fetch_optional(&mut *tx)
                .await?;
        if found.is_none() {
            return Err(s3s::s3_error!(PreconditionFailed).into());
        }
        if let Some(task) = sqlx::query_scalar::<_, Uuid>(
            "SELECT id FROM tasks WHERE kind='purge' AND bucket_id=$1 AND state<>'completed'",
        )
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?
        {
            return Ok(json!({"task_id":task,"existing":true}));
        }
        if let Some(expected) = confirmation {
            let preview = crate::manage::buckets::impact(&mut tx, id).await?;
            if crate::manage::buckets::fingerprint(&preview, None)? != expected {
                return Err(s3s::s3_error!(PreconditionFailed).into());
            }
        }
        sqlx::query("UPDATE buckets SET state='purging' WHERE id=$1")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        let task = insert(
            &mut tx,
            "purge",
            Some(id),
            json!({"bucket_id":id,"name":name}),
        )
        .await?;
        crate::manage::audit::checkpoint(
            &mut tx,
            "bucket.purge",
            name,
            json!({"bucket_id":id,"task_id":task}),
        )
        .await?;
        tx.commit().await?;
        self.wake_tasks.notify_one();
        Ok(json!({"task_id":task}))
    }
    pub async fn sweep_start(
        &self,
        execute: bool,
        preview: Option<Uuid>,
        confirm_prefix: Option<&str>,
        older_than: &str,
    ) -> Result<Value> {
        let _coord = self.coord.lock().await;
        let age = config::seconds(older_than)?;
        ensure!(age <= i64::MAX as u64 / 1000, "age too large");
        let minimum = self.config.backend.min_storage_seconds()?;
        let prefix = self.storage.namespace().to_string();
        if execute {
            let indexed: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM chunks)")
                .fetch_one(&self.db)
                .await?;
            ensure!(
                indexed,
                "empty chunk index; restore the correct database before destructive sweep"
            );
            ensure!(
                confirm_prefix == Some(prefix.as_str()),
                "confirmed prefix must exactly match the preview backend prefix"
            );
            let previous: Value = sqlx::query_scalar(
                "SELECT detail FROM tasks WHERE id=$1 AND kind='sweep' AND state='completed'",
            )
            .bind(preview.context("--preview needs a completed dry-run task ID")?)
            .fetch_optional(&self.db)
            .await?
            .context("completed preview task not found")?;
            ensure!(
                previous["dry_run"] == true && previous["prefix"] == prefix,
                "preview scope mismatch"
            );
            ensure!(
                previous["older_than_seconds"].as_u64() == Some(age),
                "use the same --older-than as the preview"
            );
            ensure!(
                previous["min_storage_duration_seconds"]
                    .as_u64()
                    .unwrap_or(0)
                    == minimum,
                "backend.min_storage_duration changed; preview again"
            );
            ensure!(
                self.maintenance.load(std::sync::atomic::Ordering::Acquire),
                "enable maintenance before destructive backend sweep"
            );
            ensure!(
                self.active.lock().unwrap().is_empty(),
                "wait for active uploads, completions and reads to drain"
            );
        }
        let cutoff = chrono::Utc::now()
            .checked_sub_signed(chrono::TimeDelta::seconds(age.max(minimum) as i64))
            .context("age too large")?;
        let detail = json!({"dry_run":!execute,"prefix":prefix,"older_than_seconds":age,"min_storage_duration_seconds":minimum,"cutoff":cutoff.to_rfc3339(),"candidates":0,"bytes":0,"unrecognized":0,"samples":[]});
        let id = insert(&mut *self.db.acquire().await?, "sweep", None, detail).await?;
        self.wake_tasks.notify_one();
        Ok(json!({"task_id":id,"dry_run":!execute}))
    }
    pub async fn task_change(&self, id: Uuid, resume: bool) -> Result<Value> {
        let _coord = self.coord.lock().await;
        if resume {
            let (kind, detail): (String, Value) =
                sqlx::query_as("SELECT kind,detail FROM tasks WHERE id=$1")
                    .bind(id)
                    .fetch_optional(&self.db)
                    .await?
                    .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
            let policy = if kind == "pack" {
                detail["kind"].as_str().unwrap_or("")
            } else {
                &kind
            };
            if self
                .maintenance_schedule()
                .iter()
                .any(|(k, _, _)| *k == policy)
                && (self.maintenance_paused(policy).await?
                    || (matches!(policy, "pack" | "repack") && !self.pack_creation_allowed()))
            {
                return Err(s3s::s3_error!(
                    OperationAborted,
                    "resume the maintenance policy and pack creation first"
                )
                .into());
            }
            if kind == "sweep" && detail["dry_run"] == false {
                if !self.maintenance.load(std::sync::atomic::Ordering::Acquire) {
                    return Err(s3s::s3_error!(
                        OperationAborted,
                        "enable maintenance before resuming destructive sweep"
                    )
                    .into());
                }
            } else if matches!(kind.as_str(), "purge" | "pack" | "unpack" | "upload" | "gc") {
                self.writable()?;
            } else if kind == "integrity" {
                let busy: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE kind='integrity' AND state IN ('queued','running') AND id<>$1)").bind(id).fetch_one(&self.db).await?;
                if busy {
                    return Err(s3s::s3_error!(
                        OperationAborted,
                        "an integrity check is already active"
                    )
                    .into());
                }
            }
        }
        let changed = if resume {
            sqlx::query("UPDATE tasks SET state='queued',error=NULL,updated_at=now() WHERE id=$1 AND state IN ('paused','failed')").bind(id).execute(&self.db).await?
        } else {
            sqlx::query("UPDATE tasks SET state='paused',updated_at=now() WHERE id=$1 AND state IN ('queued','running')").bind(id).execute(&self.db).await?
        };
        if changed.rows_affected() != 1 {
            return Err(
                s3s::s3_error!(OperationAborted, "task not found or cannot change state").into(),
            );
        }
        self.wake_tasks.notify_one();
        Ok(json!({"task_id":id,"state":if resume{"queued"}else{"paused"}}))
    }
    async fn purge_batch(&self, id: Uuid, bucket: Uuid, cursor: Option<&str>) -> Result<bool> {
        #[cfg(feature = "fault-injection")]
        crate::faults::point("purge-before-batch").await;
        let coord = self.coord.lock().await;
        if self.maintenance.load(std::sync::atomic::Ordering::Acquire) {
            sqlx::query("UPDATE tasks SET state='paused',updated_at=now() WHERE id=$1 AND state IN ('queued','running')")
                .bind(id).execute(&self.db).await?;
            return Ok(false);
        }
        let running: bool = sqlx::query_scalar("SELECT state='running' FROM tasks WHERE id=$1")
            .bind(id)
            .fetch_one(&self.db)
            .await?;
        if !running {
            return Ok(false);
        }
        let batch = self.config.gc.batch_size as i64;
        let uploads:Vec<Uuid>=sqlx::query_scalar("SELECT id FROM uploads WHERE bucket_id=$1 AND state IN ('active','completing') ORDER BY id LIMIT $2").bind(bucket).bind(batch).fetch_all(&self.db).await?;
        if !uploads.is_empty() {
            for upload in uploads {
                let lock = self.operation_lock(upload);
                let Ok(_guard) = lock.try_lock() else {
                    continue;
                };
                self.abort_upload_locked(upload, &coord, &crate::authorization::Permit::local())
                    .await?;
            }
            return Ok(false);
        }
        let mut tx = self.db.begin().await?;
        let keys:Vec<(String,Option<Uuid>)>=sqlx::query_as("SELECT key,stream_id FROM objects WHERE bucket_id=$1 AND ($2::text IS NULL OR key>$2) ORDER BY key LIMIT $3 FOR UPDATE").bind(bucket).bind(cursor).bind(batch).fetch_all(&mut *tx).await?;
        if !keys.is_empty() {
            let last = &keys.last().unwrap().0;
            for (key, stream) in &keys {
                if let Some(stream) = stream {
                    sqlx::query("UPDATE streams SET state='retired',touched_at=now() WHERE id=$1")
                        .bind(stream)
                        .execute(&mut *tx)
                        .await?;
                }
                sqlx::query("UPDATE objects SET stream_id=NULL,write_epoch=$3 WHERE bucket_id=$1 AND key=$2").bind(bucket).bind(key).bind(Uuid::new_v4()).execute(&mut *tx).await?;
            }
            sqlx::query(
                "UPDATE tasks SET cursor=$2,processed=processed+$3,updated_at=now() WHERE id=$1",
            )
            .bind(id)
            .bind(last)
            .bind(keys.len() as i64)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            self.wake_gc.notify_one();
            return Ok(false);
        }
        let active: Vec<Uuid> = self.active.lock().unwrap().keys().copied().collect();
        sqlx::query("DELETE FROM uploads WHERE id IN (SELECT id FROM uploads WHERE bucket_id=$1 AND state IN ('completed','aborted') AND NOT (id=ANY($2)) LIMIT $3)").bind(bucket).bind(active).bind(batch).execute(&mut *tx).await?;
        let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM streams WHERE bucket_id=$1) OR EXISTS(SELECT 1 FROM uploads WHERE bucket_id=$1)").bind(bucket).fetch_one(&mut *tx).await?;
        if waiting {
            tx.commit().await?;
            self.wake_gc.notify_one();
            return Ok(false);
        }
        sqlx::query("DELETE FROM objects WHERE bucket_id=$1")
            .bind(bucket)
            .execute(&mut *tx)
            .await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(crate::manage::account::USER_LOCK)
            .execute(&mut *tx)
            .await?;
        crate::manage::buckets::lock_dependents(&mut tx, bucket).await?;
        sqlx::query("DELETE FROM buckets WHERE id=$1 AND state='purging'")
            .bind(bucket)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE tasks SET state='completed',updated_at=now() WHERE id=$1")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(true)
    }
    async fn sweep_batch(&self, id: Uuid, cursor: Option<&str>, mut detail: Value) -> Result<bool> {
        let destructive = detail["dry_run"] == false;
        if destructive {
            ensure!(
                self.maintenance.load(std::sync::atomic::Ordering::Acquire),
                "maintenance was disabled; sweep stopped"
            );
            ensure!(
                self.active.lock().unwrap().is_empty(),
                "active data operations have not drained"
            );
            ensure!(
                !self.gc_running.load(std::sync::atomic::Ordering::Acquire),
                "wait for current GC batch"
            );
        }
        let prefix = self.storage.namespace().to_string();
        ensure!(detail["prefix"] == prefix, "backend namespace changed");
        let cutoff = chrono::DateTime::parse_from_rfc3339(
            detail["cutoff"].as_str().context("missing sweep cutoff")?,
        )?
        .with_timezone(&chrono::Utc);
        // A resumed task may have been created under a shorter retention policy.
        let minimum = self.config.backend.min_storage_seconds()?;
        let cutoff = cutoff.min(
            chrono::Utc::now()
                .checked_sub_signed(chrono::TimeDelta::seconds(minimum as i64))
                .context("age too large")?,
        );
        let rows = self
            .storage
            .list_physical(cursor, self.config.gc.batch_size as usize)
            .await?;
        let Some(last) = rows.last().map(|r| r.location.to_string()) else {
            sqlx::query("UPDATE tasks SET state='completed',updated_at=now() WHERE id=$1 AND state='running'").bind(id).execute(&self.db).await?;
            return Ok(true);
        };
        for row in &rows {
            if row.last_modified > cutoff {
                continue;
            }
            let name = row.location.as_ref();
            let Some(storage) = self.storage.parse_physical_path(name) else {
                detail["unrecognized"] = json!(detail["unrecognized"].as_u64().unwrap_or(0) + 1);
                continue;
            };
            // Every indexed state protects a physical key, including preparing and deleting.
            let indexed: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM chunk_locations WHERE storage_id=$1) OR EXISTS(SELECT 1 FROM packs WHERE storage_id=$1)")
                    .bind(storage)
                    .fetch_one(&self.db)
                    .await?;
            if indexed {
                continue;
            }
            if destructive {
                let _coord = self.coord.lock().await;
                let running: bool =
                    sqlx::query_scalar("SELECT state='running' FROM tasks WHERE id=$1")
                        .bind(id)
                        .fetch_one(&self.db)
                        .await?;
                if !running {
                    return Ok(false);
                }
                ensure!(
                    self.maintenance.load(std::sync::atomic::Ordering::Acquire),
                    "maintenance was disabled; sweep stopped"
                );
                self.storage.delete_path(&row.location).await?;
            }
            detail["candidates"] = json!(detail["candidates"].as_u64().unwrap_or(0) + 1);
            detail["bytes"] = json!(detail["bytes"].as_u64().unwrap_or(0) + row.size);
            let samples = detail["samples"]
                .as_array_mut()
                .context("invalid task samples")?;
            if samples.len() < 20 {
                samples.push(json!(name));
            }
        }
        sqlx::query("UPDATE tasks SET cursor=$2,processed=processed+$3,detail=$4,updated_at=now() WHERE id=$1").bind(id).bind(last).bind(rows.len()as i64).bind(detail).execute(&self.db).await?;
        Ok(false)
    }
}
pub async fn run(app: Arc<App>) -> Result<()> {
    let active = Arc::new(tokio::sync::Mutex::new(std::collections::HashSet::new()));
    let mut workers = futures_util::stream::FuturesUnordered::new();
    for class in std::iter::once(0)
        .chain(std::iter::repeat_n(
            1,
            app.config.pack.maintenance_concurrency,
        ))
        .chain(std::iter::repeat_n(
            2,
            app.budget
                .cpu_jobs
                .min(app.budget.upload_concurrency)
                .clamp(1, 4),
        ))
    {
        workers.push(worker(app.clone(), active.clone(), class));
    }
    workers
        .next()
        .await
        .context("maintenance workers stopped")?
}
async fn worker(
    app: Arc<App>,
    active: Arc<tokio::sync::Mutex<std::collections::HashSet<Uuid>>>,
    class: i32,
) -> Result<()> {
    loop {
        let id = {
            let mut active = active.lock().await;
            let ids: Vec<Uuid> = active.iter().copied().collect();
            let id:Option<Uuid>=sqlx::query_scalar("UPDATE tasks SET state='running',started_at=coalesce(started_at,now()),updated_at=now() WHERE id=(SELECT t.id FROM tasks t CROSS JOIN mokyu_meta m WHERE t.state IN ('queued','running') AND CASE WHEN t.kind IN ('pack','unpack') THEN 1 WHEN t.kind IN ('upload','cache_flush') THEN 2 ELSE 0 END=$1 AND NOT(t.id=ANY($2)) AND NOT (m.maintenance AND t.kind IN ('pack','unpack','purge','gc')) AND NOT EXISTS(SELECT 1 FROM maintenance_controls c WHERE c.kind=CASE WHEN t.kind='pack' THEN t.detail->>'kind' ELSE t.kind END AND CASE WHEN c.kind='gc' THEN m.gc_paused ELSE c.paused END) AND NOT (t.kind='pack' AND t.detail->>'kind' IN ('pack','repack') AND m.pack_creation_paused) ORDER BY t.updated_at,t.id LIMIT 1) AND state IN ('queued','running') RETURNING id").bind(class).bind(ids).fetch_optional(&app.db).await?;
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
            let (kind, state, bucket, cursor, detail): (
                String,
                String,
                Option<Uuid>,
                Option<String>,
                Value,
            ) = sqlx::query_as("SELECT kind,state,bucket_id,cursor,detail FROM tasks WHERE id=$1")
                .bind(id)
                .fetch_one(&app.db)
                .await?;
            if state != "running" {
                active.lock().await.remove(&id);
                continue;
            }
            let result = if matches!(kind.as_str(), "gc" | "cleanup") {
                app.lifecycle_batch(id, &kind).await
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
                match tokio::time::timeout(
                    Duration::from_secs(config::seconds(&app.config.integrity.request_timeout)?),
                    crate::backend::PRIORITY.scope(
                        crate::backend::MAINTENANCE,
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
                crate::backend::PRIORITY
                    .scope(
                        crate::backend::MAINTENANCE,
                        app.pack_batch(id, cursor.as_deref(), detail, kind == "unpack"),
                    )
                    .await
            } else if kind == "upload" || kind == "cache_flush" {
                crate::backend::PRIORITY
                    .scope(
                        crate::backend::UPLOAD,
                        crate::upload_cache::FLUSH.scope(
                            kind == "cache_flush",
                            app.upload_batch(id, detail, kind == "cache_flush"),
                        ),
                    )
                    .await
            } else {
                Err(anyhow::anyhow!("unknown maintenance task kind"))
            };
            if kind == "gc" && result.is_err() {
                app.statistics
                    .gc_failures
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            if class != 0 {
                if class == 1 && result.is_err() {
                    sqlx::query("UPDATE pack_changes SET next_check_at=now()+$2*interval '1 second' WHERE pack_id IN (SELECT c.pack_id FROM pack_inputs i JOIN chunks c ON c.id=i.chunk_id WHERE i.task_id=$1)").bind(id).bind(config::seconds(&app.config.pack.reuse_interval)? as f64).execute(&app.db).await?;
                    sqlx::query("UPDATE pack_maintenance SET next_check_at=now()+$2*interval '1 second' WHERE stream_id IN (SELECT e.stream_id FROM extents e JOIN pack_inputs i ON i.chunk_id=e.chunk_id WHERE i.task_id=$1)").bind(id).bind(config::seconds(&app.config.pack.reuse_interval)? as f64).execute(&app.db).await?;
                }
                app.finish_pack_work(id).await?;
            }
            match result {
                Ok(true) => {
                    active.lock().await.remove(&id);
                    tokio::task::yield_now().await;
                    continue;
                }
                Ok(false) => {}
                Err(e) => {
                    tracing::error!(task_id=%id,error=%e,"maintenance task stopped");
                    if class == 2 {
                        app.upload_retry(id, &e.to_string()).await?;
                    } else {
                        sqlx::query("UPDATE tasks SET state='failed',error=$2,updated_at=now() WHERE id=$1 AND state='running'").bind(id).bind(e.to_string()).execute(&app.db).await?;
                    }
                }
            }
            active.lock().await.remove(&id);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}
