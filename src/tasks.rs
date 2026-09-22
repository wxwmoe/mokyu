use crate::{app::App, config};
use anyhow::{Context, Result, ensure};
use futures_util::{StreamExt, TryStreamExt};
use object_store::path::Path;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

impl App {
    pub async fn purge_preview(&self, name: &str) -> Result<Value> {
        let b = self.bucket(name, false).await?;
        let (objects,uploads):(i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM objects WHERE bucket_id=$1 AND stream_id IS NOT NULL),(SELECT count(*) FROM uploads WHERE bucket_id=$1 AND state IN ('active','completing'))").bind(b.id).fetch_one(&self.db).await?;
        Ok(
            json!({"bucket_id":b.id,"name":b.name,"state":b.state,"objects":objects,"active_uploads":uploads,"effect":"seal bucket, remove every object and upload, then delete bucket; shared chunks use normal GC grace"}),
        )
    }
    pub async fn purge_start(&self, name: &str, id: Uuid) -> Result<Value> {
        self.writable()?;
        let _coord = self.coord.lock().await;
        let mut tx = self.db.begin().await?;
        let found: Option<Uuid> =
            sqlx::query_scalar("SELECT id FROM buckets WHERE name=$1 AND id=$2 FOR UPDATE")
                .bind(name)
                .bind(id)
                .fetch_optional(&mut *tx)
                .await?;
        ensure!(found.is_some(), "bucket identity changed; preview again");
        if let Some(task) = sqlx::query_scalar::<_, Uuid>(
            "SELECT id FROM tasks WHERE kind='purge' AND bucket_id=$1 AND state<>'completed'",
        )
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?
        {
            return Ok(json!({"task_id":task,"existing":true}));
        }
        sqlx::query("UPDATE buckets SET state='purging' WHERE id=$1")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        let task = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO tasks(id,kind,bucket_id,state,detail) VALUES($1,'purge',$2,'queued',$3)",
        )
        .bind(task)
        .bind(id)
        .bind(json!({"bucket_id":id,"name":name}))
        .execute(&mut *tx)
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
        let age = config::seconds(older_than)?;
        ensure!(age <= i64::MAX as u64 / 1000, "age too large");
        let prefix = self.storage.prefix();
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
                self.maintenance.load(std::sync::atomic::Ordering::Acquire),
                "enable maintenance before destructive backend sweep"
            );
            ensure!(
                self.active.lock().unwrap().is_empty(),
                "wait for active uploads, completions and reads to drain"
            );
        }
        let id = Uuid::new_v4();
        let detail = json!({"dry_run":!execute,"prefix":prefix,"older_than_seconds":age,"cutoff":(chrono::Utc::now()-chrono::TimeDelta::seconds(age as i64)).to_rfc3339(),"candidates":0,"bytes":0,"unrecognized":0,"samples":[]});
        sqlx::query("INSERT INTO tasks(id,kind,state,detail) VALUES($1,'sweep','queued',$2)")
            .bind(id)
            .bind(detail)
            .execute(&self.db)
            .await?;
        self.wake_tasks.notify_one();
        Ok(json!({"task_id":id,"dry_run":!execute}))
    }
    pub async fn task_change(&self, id: Uuid, resume: bool) -> Result<Value> {
        let changed = if resume {
            sqlx::query("UPDATE tasks SET state='queued',error=NULL,updated_at=now() WHERE id=$1 AND state IN ('paused','failed')").bind(id).execute(&self.db).await?
        } else {
            sqlx::query("UPDATE tasks SET state='paused',updated_at=now() WHERE id=$1 AND state IN ('queued','running')").bind(id).execute(&self.db).await?
        };
        ensure!(
            changed.rows_affected() == 1,
            "task not found or cannot change state"
        );
        self.wake_tasks.notify_one();
        Ok(json!({"task_id":id,"state":if resume{"queued"}else{"paused"}}))
    }
    async fn purge_batch(&self, id: Uuid, bucket: Uuid, cursor: Option<&str>) -> Result<bool> {
        let batch = self.config.gc.batch_size as i64;
        let uploads:Vec<Uuid>=sqlx::query_scalar("SELECT id FROM uploads WHERE bucket_id=$1 AND state IN ('active','completing') ORDER BY id LIMIT $2").bind(bucket).bind(batch).fetch_all(&self.db).await?;
        if !uploads.is_empty() {
            for upload in uploads {
                let lock = self.upload_lock(upload);
                let Ok(_guard) = lock.try_lock() else {
                    continue;
                };
                self.abort_upload(upload).await?;
            }
            return Ok(false);
        }
        let _coord = self.coord.lock().await;
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
        let prefix = self.storage.prefix();
        ensure!(detail["prefix"] == prefix, "backend namespace changed");
        let cutoff = chrono::DateTime::parse_from_rfc3339(
            detail["cutoff"].as_str().context("missing sweep cutoff")?,
        )?;
        let path = Path::from(prefix.clone());
        let offset = Path::parse(cursor.unwrap_or(""))?;
        let rows: Vec<_> = self
            .storage
            .backend
            .list_with_offset(Some(&path), &offset)
            .take(self.config.gc.batch_size as usize)
            .try_collect()
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
            let Some(storage) = self.storage.parse_path(name) else {
                detail["unrecognized"] = json!(detail["unrecognized"].as_u64().unwrap_or(0) + 1);
                continue;
            };
            // Every indexed state protects a physical key, including preparing and deleting.
            let indexed: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM chunks WHERE storage_id=$1)")
                    .bind(storage)
                    .fetch_one(&self.db)
                    .await?;
            if indexed {
                continue;
            }
            if destructive {
                self.storage.delete(storage).await?;
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
    loop {
        let id:Option<Uuid>=sqlx::query_scalar("UPDATE tasks SET state='running',updated_at=now() WHERE id=(SELECT id FROM tasks WHERE state='queued' ORDER BY created_at LIMIT 1) RETURNING id").fetch_optional(&app.db).await?;
        let Some(id) = id else {
            tokio::select! {_=app.wake_tasks.notified()=>{},_=tokio::time::sleep(Duration::from_secs(30))=>{}}
            continue;
        };
        loop {
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
                break;
            }
            let result = if kind == "purge" {
                app.purge_batch(
                    id,
                    bucket.context("purge bucket disappeared")?,
                    cursor.as_deref(),
                )
                .await
            } else {
                app.sweep_batch(id, cursor.as_deref(), detail).await
            };
            match result {
                Ok(true) => break,
                Ok(false) => {}
                Err(e) => {
                    tracing::error!(task_id=%id,error=%e,"maintenance task stopped");
                    sqlx::query("UPDATE tasks SET state='failed',error=$2,updated_at=now() WHERE id=$1 AND state='running'").bind(id).bind(e.to_string()).execute(&app.db).await?;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}
