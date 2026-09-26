use crate::{app::App, config};
use anyhow::Result;
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex, atomic::Ordering},
    time::{Duration, Instant},
};
use uuid::Uuid;

#[derive(Default)]
pub struct Cleanup {
    running: tokio::sync::Mutex<()>,
    last_run: Mutex<Value>,
}

impl App {
    pub fn cleanup_status(&self) -> Value {
        let c = &self.config.cleanup;
        json!({"running":self.cleanup_state.running.try_lock().is_err(),
            "last_run":self.cleanup_state.last_run.lock().unwrap().clone(),
            "interval":c.interval,"batch_size":c.batch_size,"max_duration":c.max_duration,
            "deleted_chunk_retention":c.deleted_chunk_retention,
            "upload_retention":c.upload_retention,"task_retention":c.task_retention})
    }

    pub async fn cleanup_history(&self) -> Result<Value> {
        let Ok(guard) = self.cleanup_state.running.try_lock() else {
            return Ok(self.cleanup_status());
        };
        let c = &self.config.cleanup;
        let started_at = chrono::Utc::now();
        let start = Instant::now();
        let duration = Duration::from_secs(config::seconds(&c.max_duration)?);
        let batch = c.batch_size as i64;
        let mut deleted =
            json!({"chunks":0,"uploads":0,"tasks":0,"sessions":0,"integrity_issues":0});
        let mut batches = 0;
        let queries = [
            (
                "chunks",
                config::seconds(&c.deleted_chunk_retention)? as f64,
                "DELETE FROM chunks WHERE id IN (SELECT id FROM chunks c WHERE state='deleted' AND deleted_at<now()-$1*interval '1 second' AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id) ORDER BY deleted_at,id LIMIT $2 FOR UPDATE SKIP LOCKED)",
            ),
            (
                "uploads",
                config::seconds(&c.upload_retention)? as f64,
                "DELETE FROM uploads WHERE id IN (SELECT id FROM uploads u WHERE state IN ('completed','aborted') AND touched_at<now()-$1*interval '1 second' AND NOT (id=ANY($3)) AND NOT EXISTS(SELECT 1 FROM parts WHERE upload_id=u.id) ORDER BY touched_at,id LIMIT $2 FOR UPDATE SKIP LOCKED)",
            ),
            (
                "integrity_issues",
                config::seconds(&c.task_retention)? as f64,
                "DELETE FROM integrity_issues WHERE id IN (SELECT i.id FROM integrity_issues i JOIN tasks t ON t.id=i.task_id WHERE t.state='completed' AND t.updated_at<now()-$1*interval '1 second' ORDER BY i.id LIMIT $2 FOR UPDATE OF i SKIP LOCKED)",
            ),
            (
                "tasks",
                config::seconds(&c.task_retention)? as f64,
                "DELETE FROM tasks WHERE id IN (SELECT id FROM tasks t WHERE state='completed' AND updated_at<now()-$1*interval '1 second' AND NOT EXISTS(SELECT 1 FROM integrity_issues WHERE task_id=t.id) ORDER BY updated_at,id LIMIT $2 FOR UPDATE SKIP LOCKED)",
            ),
            (
                "sessions",
                0.0,
                "DELETE FROM sessions WHERE token_hash IN (SELECT token_hash FROM sessions WHERE expires_at<now()-$1*interval '1 second' ORDER BY expires_at,token_hash LIMIT $2 FOR UPDATE SKIP LOCKED)",
            ),
        ];
        // Only SQL is canceled at the deadline: an unfinished transaction rolls back.
        let result = tokio::time::timeout(duration, async {
            loop {
                let mut full = false;
                for (table, age, sql) in queries {
                    let mut tx = self.db.begin().await?;
                    sqlx::raw_sql("SET LOCAL lock_timeout='100ms'; SET LOCAL work_mem='4MB'")
                        .execute(&mut *tx)
                        .await?;
                    sqlx::query("SELECT set_config('statement_timeout',$1,true)")
                        .bind(
                            duration
                                .saturating_sub(start.elapsed())
                                .as_millis()
                                .max(1)
                                .to_string(),
                        )
                        .execute(&mut *tx)
                        .await?;
                    let mut query = sqlx::query(sql).bind(age).bind(batch);
                    if table == "uploads" {
                        let active: Vec<Uuid> =
                            self.active.lock().unwrap().keys().copied().collect();
                        query = query.bind(active);
                    }
                    let count = query.execute(&mut *tx).await?.rows_affected();
                    tx.commit().await?;
                    deleted[table] = json!(deleted[table].as_u64().unwrap() + count);
                    batches += 1;
                    full |= count == batch as u64;
                }
                if !full {
                    break;
                }
                tokio::task::yield_now().await;
            }
            Ok::<(), anyhow::Error>(())
        })
        .await;
        let (budget_exhausted, error) = match result {
            Ok(Ok(())) => (false, None),
            Err(_) => (true, None),
            Ok(Err(e)) => {
                let timeout = e
                    .downcast_ref::<sqlx::Error>()
                    .and_then(|e| e.as_database_error())
                    .and_then(|e| e.code())
                    .is_some_and(|code| code == "57014");
                (timeout, if timeout { None } else { Some(e) })
            }
        };
        *self.cleanup_state.last_run.lock().unwrap() = json!({
            "started_at":started_at,"finished_at":chrono::Utc::now(),
            "duration_ms":start.elapsed().as_millis() as u64,"deleted":deleted,"batches":batches,
            "budget_exhausted":budget_exhausted,"last_error":error.as_ref().map(|_| "cleanup_failed")});
        drop(guard);
        if let Some(e) = error {
            return Err(e);
        }
        Ok(self.cleanup_status())
    }

    pub async fn recover(&self) -> Result<()> {
        let mut tx = self.db.begin().await?;
        sqlx::query("UPDATE streams SET state='abandoned',touched_at=now() WHERE state='writing'")
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE chunks SET state='failed',unreferenced_at=COALESCE(unreferenced_at,now()) WHERE state IN ('preparing','uploading')").execute(&mut *tx).await?;
        sqlx::query("UPDATE uploads SET state='active' WHERE state='completing'")
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE tasks SET state='queued',updated_at=now() WHERE state='running'")
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        // Only startup scans unindexed files: no receiver can be between fsync and SQL commit yet.
        let mut files = tokio::fs::read_dir(self.storage.disk.root.join("multipart")).await?;
        while let Some(file) = files.next_entry().await? {
            if !file.file_type().await?.is_file() {
                continue;
            }
            let Ok(id) = Uuid::parse_str(&file.file_name().to_string_lossy()) else {
                continue;
            };
            let referenced: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM fragments WHERE id=$1)")
                    .bind(id)
                    .fetch_one(&self.db)
                    .await?;
            if !referenced {
                self.storage.disk.remove_fragment(id).await?;
            }
        }
        let mut cursor = Uuid::nil();
        loop {
            let rows:Vec<(Uuid,i32,Vec<u8>)>=sqlx::query_as("SELECT id,size,hash FROM fragments WHERE id>$1 AND EXISTS(SELECT 1 FROM extents WHERE fragment_id=fragments.id) ORDER BY id LIMIT 128").bind(cursor).fetch_all(&self.db).await?;
            if rows.is_empty() {
                break;
            }
            for (id, size, hash) in rows {
                let data = crate::storage::read_bounded(
                    self.storage.disk.fragment_path(id),
                    crate::codec::MAX,
                )
                .await;
                if !data.is_ok_and(|bytes| {
                    bytes.len() == size as usize
                        && blake3::hash(&bytes).as_bytes() == hash.as_slice()
                }) {
                    let uploads:Vec<Uuid>=sqlx::query_scalar("SELECT DISTINCT p.upload_id FROM parts p JOIN extents e ON e.stream_id=p.stream_id WHERE e.fragment_id=$1").bind(id).fetch_all(&self.db).await?;
                    for upload in uploads {
                        self.abort_upload(upload).await?;
                        tracing::error!(upload_id=%upload,fragment_id=%id,"multipart source missing or corrupt; upload invalidated");
                    }
                }
                cursor = id;
            }
        }
        Ok(())
    }
    pub async fn cleanup(&self) -> Result<usize> {
        let Ok(_running) = self.local_cleanup.try_lock() else {
            return Ok(0);
        };
        let deadline = Instant::now()
            + Duration::from_secs(config::seconds(&self.config.cleanup.max_duration)?);
        let mut removed = 0;
        let batch = self.config.gc.batch_size as i64;
        let idle = config::seconds(&self.config.multipart.idle_timeout)? as f64;
        let expired:Vec<Uuid>=sqlx::query_scalar("SELECT id FROM uploads WHERE state='active' AND touched_at<now()-$1*interval '1 second' ORDER BY touched_at,id LIMIT $2").bind(idle).bind(batch).fetch_all(&self.db).await?;
        for id in expired {
            if Instant::now() >= deadline {
                break;
            }
            let lock = self.upload_lock(id);
            let Ok(_guard) = lock.try_lock() else {
                continue;
            };
            if !self.is_active(id) {
                // The upload may have progressed while we waited to claim its lock.
                let expired: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM uploads WHERE id=$1 AND state='active' AND touched_at<now()-$2*interval '1 second')").bind(id).bind(idle).fetch_one(&self.db).await?;
                if expired {
                    self.abort_upload(id).await?;
                    removed += 1;
                }
            }
        }
        let active: Vec<Uuid> = self.active.lock().unwrap().keys().copied().collect();
        let candidates:Vec<Uuid>=sqlx::query_scalar("SELECT id FROM streams s WHERE state IN ('retired','abandoned','writing') AND NOT (id=ANY($1)) AND NOT EXISTS(SELECT 1 FROM objects WHERE stream_id=s.id) AND NOT EXISTS(SELECT 1 FROM parts WHERE stream_id=s.id) ORDER BY touched_at,id LIMIT $2").bind(&active).bind(batch).fetch_all(&self.db).await?;
        for id in candidates {
            if Instant::now() >= deadline {
                break;
            }
            let Ok(_coord) = self.coord.try_lock() else {
                break;
            };
            if self.is_active(id) {
                continue;
            }
            let mut tx = self.db.begin().await?;
            sqlx::raw_sql("SET LOCAL lock_timeout='100ms'; SET LOCAL statement_timeout='1s'")
                .execute(&mut *tx)
                .await?;
            let claimed = sqlx::query("UPDATE streams s SET state='abandoned' WHERE id=$1 AND state IN ('retired','abandoned','writing') AND NOT EXISTS(SELECT 1 FROM objects WHERE stream_id=s.id) AND NOT EXISTS(SELECT 1 FROM parts WHERE stream_id=s.id)")
                .bind(id)
                .execute(&mut *tx)
                .await?;
            if claimed.rows_affected() == 0 {
                continue;
            }
            // Claim under coord; this orphan can no longer be pinned through an object or part.
            tx.commit().await?;
            drop(_coord);
            #[cfg(feature = "fault-injection")]
            crate::faults::point("cleanup-stream-claimed").await;
            let mut tx = self.db.begin().await?;
            sqlx::raw_sql("SET LOCAL lock_timeout='100ms'; SET LOCAL statement_timeout='1s'")
                .execute(&mut *tx)
                .await?;
            let detached = sqlx::query("UPDATE chunks c SET owner_stream=NULL,state=CASE WHEN state IN ('preparing','uploading') THEN 'failed' ELSE state END,unreferenced_at=CASE WHEN NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id) THEN COALESCE(unreferenced_at,now()) ELSE unreferenced_at END WHERE id IN (SELECT id FROM chunks WHERE owner_stream=$1 ORDER BY id LIMIT $2)").bind(id).bind(batch).execute(&mut *tx).await?.rows_affected();
            let fragments = sqlx::query("UPDATE fragments SET owner_stream=NULL WHERE id IN (SELECT id FROM fragments WHERE owner_stream=$1 ORDER BY id LIMIT $2)").bind(id).bind(batch).execute(&mut *tx).await?.rows_affected();
            let chunks:Vec<Option<i64>>=sqlx::query_scalar("DELETE FROM extents WHERE stream_id=$1 AND offset_bytes IN (SELECT offset_bytes FROM extents WHERE stream_id=$1 ORDER BY offset_bytes LIMIT $2) RETURNING chunk_id").bind(id).bind(batch).fetch_all(&mut *tx).await?;
            let count = chunks.len();
            let chunks: Vec<i64> = chunks.into_iter().flatten().collect();
            sqlx::query("UPDATE chunks c SET unreferenced_at=now() WHERE id=ANY($1) AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id)").bind(&chunks).execute(&mut *tx).await?;
            let streams = sqlx::query("DELETE FROM streams s WHERE id=$1 AND NOT EXISTS(SELECT 1 FROM extents WHERE stream_id=s.id) AND NOT EXISTS(SELECT 1 FROM chunks WHERE owner_stream=s.id) AND NOT EXISTS(SELECT 1 FROM fragments WHERE owner_stream=s.id)").bind(id).execute(&mut *tx).await?.rows_affected();
            tx.commit().await?;
            removed += count + (detached + fragments + streams) as usize;
        }
        let active: Vec<Uuid> = self.active.lock().unwrap().keys().copied().collect();
        let fragments:Vec<Uuid>=sqlx::query_scalar("SELECT id FROM fragments f WHERE (sealed OR owner_stream IS NULL OR NOT (owner_stream=ANY($2))) AND NOT EXISTS(SELECT 1 FROM extents WHERE fragment_id=f.id) ORDER BY created_at,id LIMIT $1").bind(batch).bind(&active).fetch_all(&self.db).await?;
        for id in fragments {
            if Instant::now() >= deadline {
                break;
            }
            let Ok(coord) = self.coord.try_lock() else {
                break;
            };
            let mut tx = self.db.begin().await?;
            sqlx::raw_sql("SET LOCAL lock_timeout='100ms'; SET LOCAL statement_timeout='1s'")
                .execute(&mut *tx)
                .await?;
            let row: Option<(bool, Option<Uuid>)> = sqlx::query_as("SELECT sealed,owner_stream FROM fragments f WHERE id=$1 AND NOT EXISTS(SELECT 1 FROM extents WHERE fragment_id=f.id) FOR UPDATE").bind(id).fetch_optional(&mut *tx).await?;
            let Some((sealed, owner)) = row else {
                continue;
            };
            if !sealed && owner.is_some_and(|id| self.is_active(id)) {
                continue;
            }
            if self.storage.disk.pending(id) {
                continue;
            }
            // Keep the fragment row locked against new references, but release the global lock for I/O.
            drop(coord);
            self.storage.disk.remove_fragment(id).await?;
            sqlx::query("DELETE FROM fragments WHERE id=$1")
                .bind(id)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            removed += 1;
        }
        if Instant::now() < deadline {
            // Epoch tombstones must be checked while writers cannot create a new stream.
            if let Ok(_coord) = self.coord.try_lock() {
                let mut tx = self.db.begin().await?;
                sqlx::raw_sql("SET LOCAL lock_timeout='100ms'; SET LOCAL statement_timeout='1s'")
                    .execute(&mut *tx)
                    .await?;
                let count = sqlx::query("DELETE FROM objects WHERE (bucket_id,key) IN (SELECT bucket_id,key FROM objects o WHERE stream_id IS NULL AND NOT EXISTS(SELECT 1 FROM streams s WHERE s.bucket_id=o.bucket_id AND s.object_key=o.key AND s.state='writing') ORDER BY bucket_id,key LIMIT $1 FOR UPDATE SKIP LOCKED)").bind(batch).execute(&mut *tx).await?.rows_affected();
                tx.commit().await?;
                removed += count as usize;
            }
        }
        if removed > 0 {
            self.wake_gc.notify_one();
        }
        Ok(removed)
    }
    pub async fn reclaim(&self) -> Result<usize> {
        if self.gc_running.swap(true, Ordering::AcqRel) {
            return Ok(0);
        }
        let _running = Running(&self.gc_running);
        let paused: bool = sqlx::query_scalar("SELECT gc_paused OR maintenance FROM gateway_meta")
            .fetch_one(&self.db)
            .await?;
        if paused {
            return Ok(0);
        }
        let grace = config::seconds(&self.config.gc.unreferenced_grace)? as f64;
        let ids:Vec<i64>=sqlx::query_scalar("SELECT id FROM chunks WHERE state IN ('ready','failed','deleting') AND unreferenced_at<now()-$1*interval '1 second' ORDER BY unreferenced_at,id LIMIT $2").bind(grace).bind(self.config.gc.batch_size as i64).fetch_all(&self.db).await?;
        let mut count = 0;
        for id in ids {
            let mut tx = self.db.begin().await?;
            let row:Option<(Uuid,Option<Uuid>)>=sqlx::query_as("SELECT storage_id,owner_stream FROM chunks WHERE id=$1 AND state IN ('ready','failed','deleting') AND unreferenced_at<now()-$2*interval '1 second' AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=chunks.id) FOR UPDATE").bind(id).bind(grace).fetch_optional(&mut *tx).await?;
            let Some((storage, owner)) = row else {
                continue;
            };
            if owner.is_some_and(|id| self.is_active(id)) {
                continue;
            }
            sqlx::query("UPDATE chunks SET state='deleting' WHERE id=$1")
                .bind(id)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            #[cfg(feature = "fault-injection")]
            crate::faults::point("chunk-delete-claimed").await;
            match self.storage.delete(storage).await {
                // The immutable physical key remains journaled until the database records deletion.
                Ok(()) => {
                    #[cfg(feature = "fault-injection")]
                    crate::faults::point("chunk-deleted").await;
                    sqlx::query("UPDATE chunks SET state='deleted',deleted_at=now() WHERE id=$1 AND state='deleting'").bind(id).execute(&self.db).await?;
                    count += 1;
                    self.statistics.gc_deleted.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    self.statistics.gc_failures.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(chunk_id=id,error=%e,"chunk deletion will be retried");
                }
            }
        }
        Ok(count)
    }
    pub async fn gc_status(&self) -> Result<Value> {
        let counts: Vec<(String, i64)> =
            sqlx::query_as("SELECT state,count(*) FROM chunks GROUP BY state")
                .fetch_all(&self.db)
                .await?;
        let (paused, maintenance): (bool, bool) =
            sqlx::query_as("SELECT gc_paused,maintenance FROM gateway_meta")
                .fetch_one(&self.db)
                .await?;
        Ok(
            json!({"paused":paused,"maintenance":maintenance,"running":self.gc_running.load(Ordering::Acquire),"chunks":counts,"grace":self.config.gc.unreferenced_grace,"interval":self.config.gc.interval}),
        )
    }
}
struct Running<'a>(&'a std::sync::atomic::AtomicBool);
impl Drop for Running<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
pub async fn run(app: Arc<App>) -> Result<()> {
    let mut local = tokio::time::interval(Duration::from_secs(config::seconds(
        &app.config.multipart.sweep_interval,
    )?));
    local.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut remote = tokio::time::interval(Duration::from_secs(config::seconds(
        &app.config.gc.interval,
    )?));
    remote.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let gc = tokio::select! {_=app.wake_gc.notified()=>false,_=local.tick()=>false,_=remote.tick()=>true};
        if let Err(e) = app.cleanup().await {
            app.statistics.gc_failures.fetch_add(1, Ordering::Relaxed);
            tracing::error!(error=%e,"local cleanup failed; retaining recoverable state");
        }
        if gc {
            loop {
                match app.reclaim().await {
                    Ok(n) if n == app.config.gc.batch_size as usize => {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    Ok(_) => break,
                    Err(e) => {
                        app.statistics.gc_failures.fetch_add(1, Ordering::Relaxed);
                        tracing::error!(error=%e,"remote GC failed; retaining deletion journal");
                        break;
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

pub async fn run_history(app: Arc<App>) -> Result<()> {
    let mut timer = tokio::time::interval(Duration::from_secs(config::seconds(
        &app.config.cleanup.interval,
    )?));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        timer.tick().await;
        if let Err(e) = app.cleanup_history().await {
            tracing::error!(error=%e,"database history cleanup failed; retrying next interval");
        }
    }
}
