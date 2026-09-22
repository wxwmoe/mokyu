use crate::{app::App, config};
use anyhow::Result;
use serde_json::{Value, json};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use uuid::Uuid;

impl App {
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
        let mut removed = 0;
        let batch = self.config.gc.batch_size as i64;
        let idle = config::seconds(&self.config.multipart.idle_timeout)? as f64;
        let expired:Vec<Uuid>=sqlx::query_scalar("SELECT id FROM uploads WHERE state='active' AND touched_at<now()-$1*interval '1 second' ORDER BY touched_at,id LIMIT $2").bind(idle).bind(batch).fetch_all(&self.db).await?;
        for id in expired {
            let lock = self.upload_lock(id);
            let Ok(_guard) = lock.try_lock() else {
                continue;
            };
            if !self.is_active(id) {
                self.abort_upload(id).await?;
                removed += 1;
            }
        }
        let _coord = self.coord.lock().await;
        let active: Vec<Uuid> = self.active.lock().unwrap().keys().copied().collect();
        let candidates:Vec<Uuid>=sqlx::query_scalar("SELECT id FROM streams s WHERE state IN ('retired','abandoned','writing') AND NOT (id=ANY($1)) AND NOT EXISTS(SELECT 1 FROM objects WHERE stream_id=s.id) AND NOT EXISTS(SELECT 1 FROM parts WHERE stream_id=s.id) ORDER BY touched_at,id LIMIT $2").bind(&active).bind(batch).fetch_all(&self.db).await?;
        for id in candidates {
            let mut tx = self.db.begin().await?;
            sqlx::query("UPDATE streams SET state='abandoned' WHERE id=$1")
                .bind(id)
                .execute(&mut *tx)
                .await?;
            sqlx::query("UPDATE chunks SET state='failed',unreferenced_at=COALESCE(unreferenced_at,now()) WHERE owner_stream=$1 AND state IN ('preparing','uploading')").bind(id).execute(&mut *tx).await?;
            let chunks:Vec<Option<i64>>=sqlx::query_scalar("DELETE FROM extents WHERE stream_id=$1 AND offset_bytes IN (SELECT offset_bytes FROM extents WHERE stream_id=$1 ORDER BY offset_bytes LIMIT $2) RETURNING chunk_id").bind(id).bind(batch).fetch_all(&mut *tx).await?;
            removed += chunks.len();
            let chunks: Vec<i64> = chunks.into_iter().flatten().collect();
            sqlx::query("UPDATE chunks c SET unreferenced_at=now() WHERE id=ANY($1) AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id)").bind(&chunks).execute(&mut *tx).await?;
            removed+=sqlx::query("DELETE FROM streams s WHERE id=$1 AND NOT EXISTS(SELECT 1 FROM extents WHERE stream_id=s.id)").bind(id).execute(&mut *tx).await?.rows_affected()as usize;
            tx.commit().await?;
        }
        let fragments:Vec<Uuid>=sqlx::query_scalar("SELECT id FROM fragments f WHERE (sealed OR owner_stream IS NULL OR NOT (owner_stream=ANY($2))) AND NOT EXISTS(SELECT 1 FROM extents WHERE fragment_id=f.id) ORDER BY created_at,id LIMIT $1").bind(batch).bind(&active).fetch_all(&self.db).await?;
        for id in fragments {
            if self.storage.disk.pending(id) {
                continue;
            }
            self.storage.disk.remove_fragment(id).await?;
            sqlx::query("DELETE FROM fragments WHERE id=$1")
                .bind(id)
                .execute(&self.db)
                .await?;
            removed += 1;
        }
        sqlx::query("DELETE FROM objects o WHERE stream_id IS NULL AND NOT EXISTS(SELECT 1 FROM streams s WHERE s.bucket_id=o.bucket_id AND s.object_key=o.key AND s.state='writing')").execute(&self.db).await?;
        sqlx::query("DELETE FROM sessions WHERE token_hash IN (SELECT token_hash FROM sessions WHERE expires_at<now() LIMIT $1)").bind(batch).execute(&self.db).await?;
        sqlx::query("DELETE FROM uploads WHERE id IN (SELECT id FROM uploads WHERE state IN ('aborted','completed') AND touched_at<now()-$1*interval '1 second' AND NOT (id=ANY($2)) ORDER BY touched_at LIMIT $3)").bind(idle).bind(&active).bind(batch).execute(&self.db).await?;
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
                }
                Err(e) => {
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
                        tracing::error!(error=%e,"remote GC failed; retaining deletion journal");
                        break;
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
