use crate::{app::App, codec::Chunk, config, pack_tasks::Mapping, storage::ReadContext};
use anyhow::{Result, ensure};
use bytes::Bytes;
use serde_json::{Value, json};
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

tokio::task_local! { pub static FLUSH: bool; }
pub static FALLBACKS: AtomicU64 = AtomicU64::new(0);

impl App {
    pub fn storage_writable(&self) -> Result<()> {
        if FLUSH.try_with(|f| *f).unwrap_or(false) {
            Ok(())
        } else {
            self.writable()
        }
    }
    pub async fn stage_chunk(
        &self,
        stream: Uuid,
        offset: i64,
        c: &Chunk,
        data: Bytes,
        compressed: bool,
        new: bool,
    ) -> Result<bool> {
        if !self.config.cache.upload_cache {
            return Ok(false);
        }
        let bypass: bool =
            sqlx::query_scalar("SELECT upload_cache_bypass FROM streams WHERE id=$1")
                .bind(stream)
                .fetch_one(&self.db)
                .await?;
        if bypass {
            return Ok(false);
        }
        let staged = self.storage.stage_upload(c, data, compressed).await;
        let pin = match staged {
            Ok(Some(pin)) => pin,
            other => {
                if let Err(e) = other {
                    tracing::warn!(error=%e,"upload cache unavailable; switching request to synchronous upload");
                }
                sqlx::query("UPDATE streams SET upload_cache_bypass=true WHERE id=$1")
                    .bind(stream)
                    .execute(&self.db)
                    .await?;
                FALLBACKS.fetch_add(1, Ordering::Relaxed);
                return Ok(false);
            }
        };
        let _coord = self.coord.lock().await;
        self.writable()?;
        let mut tx = self.db.begin().await?;
        let (state, source): (String, Option<i64>) =
            sqlx::query_as("SELECT state,pack_id FROM chunks WHERE id=$1 FOR UPDATE")
                .bind(c.id)
                .fetch_one(&mut *tx)
                .await?;
        ensure!(
            if new {
                state == "uploading"
            } else {
                state == "ready"
            },
            "upload source changed"
        );
        if !new && source.is_none() {
            return Ok(false);
        }
        sqlx::query("INSERT INTO pending_uploads(chunk_id,stream_id,offset_bytes,cache_size,cache_compressed,source_pack,next_retry_at) VALUES($1,$2,$3,$4,$5,$6,now()+$7*interval '1 second')")
            .bind(c.id).bind(stream).bind(offset).bind(pin.size as i64).bind(pin.compressed).bind(source)
            .bind(config::seconds(&self.config.pack.upload_cache_timeout)? as f64).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO cache_pins(chunk_id,pin_type,owner_id) VALUES($1,$2,$3)")
            .bind(c.id)
            .bind(if source.is_some() { "pack" } else { "upload" })
            .bind(stream)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "UPDATE chunks SET state='ready',unreferenced_at=NULL,owner_stream=NULL WHERE id=$1",
        )
        .bind(c.id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO extents(stream_id,offset_bytes,length,chunk_id) VALUES($1,$2,$3,$4)",
        )
        .bind(stream)
        .bind(offset)
        .bind(c.raw_size)
        .bind(c.id)
        .execute(&mut *tx)
        .await?;
        sqlx::query("UPDATE streams SET size=GREATEST(size,$2),touched_at=now() WHERE id=$1 AND state='writing'").bind(stream).bind(offset+i64::from(c.raw_size)).execute(&mut *tx).await?;
        // Install protection before COMMIT; cancellation can leave an extra pin, never an unprotected acknowledgement.
        self.storage.hold_upload(c.storage_id, pin);
        #[cfg(feature = "fault-injection")]
        crate::faults::point("upload-cache-before-commit").await;
        tx.commit().await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("upload-cache-committed").await;
        drop(_coord);
        self.seal_uploads(stream, false).await?;
        Ok(true)
    }
    pub async fn seal_uploads(&self, stream: Uuid, eof: bool) -> Result<()> {
        // Only complete CDC chunks enter this queue. A partial multipart tail remains a fragment.
        let ready = if eof || !self.pack_creation_allowed() {
            sqlx::query("UPDATE pending_uploads SET next_retry_at=LEAST(next_retry_at,now()) WHERE owner_task IS NULL AND chunk_id IN (SELECT chunk_id FROM extents WHERE stream_id=$1)").bind(stream).execute(&self.db).await?;
            true
        } else {
            let rows:Vec<(i64,i64,i32)>=sqlx::query_as("SELECT p.chunk_id,p.offset_bytes,c.raw_size FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id WHERE p.stream_id=$1 AND p.owner_task IS NULL ORDER BY p.offset_bytes LIMIT 1026").bind(stream).fetch_all(&self.db).await?;
            let mut ids = Vec::new();
            let mut total = 0u64;
            let mut end = None;
            let mut sealed = false;
            let maximum = config::bytes(&self.config.pack.max_size)?;
            for (id, offset, size) in rows {
                if !ids.is_empty() && (end != Some(offset) || total + size as u64 > maximum) {
                    sealed = true;
                    break;
                }
                ids.push(id);
                total += size as u64;
                end = Some(offset + i64::from(size));
                if total == maximum {
                    sealed = true;
                    break;
                }
            }
            if sealed {
                sqlx::query("UPDATE pending_uploads SET next_retry_at=LEAST(next_retry_at,now()) WHERE chunk_id=ANY($1) AND owner_task IS NULL").bind(ids).execute(&self.db).await?;
            }
            sealed
        };
        if ready {
            self.wake_tasks.notify_waiters();
        }
        Ok(())
    }
    pub async fn upload_cache_status(&self) -> Result<Value> {
        let (total, limit, used) = self.storage.disk.upload_capacity()?;
        let pending:Value=sqlx::query_scalar("SELECT jsonb_build_object('entries',count(*),'bytes',COALESCE(sum(cache_size),0),'oldest_at',min(created_at),'failed_entries',count(*) FILTER(WHERE last_error IS NOT NULL)) FROM pending_uploads").fetch_one(&self.db).await?;
        let pins:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('chunk_id',p.chunk_id::text,'pin_type',p.pin_type,'owner_id',p.owner_id,'created_at',p.created_at,'last_error',u.last_error,'attempts',u.attempts,'next_retry_at',u.next_retry_at) FROM cache_pins p JOIN pending_uploads u USING(chunk_id) ORDER BY p.created_at,p.chunk_id LIMIT 100").fetch_all(&self.db).await?;
        Ok(
            json!({"enabled":self.config.cache.upload_cache,"configured_size":self.config.cache.upload_cache_size,"effective_cache_bytes":total,"effective_upload_bytes":limit,"reserved_bytes":used,"fallbacks":FALLBACKS.load(Ordering::Relaxed),"pending":pending,"pins":pins,"pins_limit":100}),
        )
    }
    pub async fn cache_flush_start(&self) -> Result<Value> {
        let _coord = self.coord.lock().await;
        if let Some(id)=sqlx::query_scalar::<_,Uuid>("SELECT id FROM tasks WHERE kind='cache_flush' AND state IN ('queued','running') LIMIT 1").fetch_optional(&self.db).await? {return Ok(json!({"task_id":id,"existing":true}));}
        let id = crate::tasks::insert(
            &mut *self.db.acquire().await?,
            "cache_flush",
            None,
            json!({"upper_time":chrono::Utc::now()}),
        )
        .await?;
        self.wake_tasks.notify_one();
        Ok(json!({"task_id":id}))
    }
    pub async fn enqueue_upload(&self) -> Result<bool> {
        if self.maintenance.load(Ordering::Acquire) {
            return Ok(false);
        }
        let _coord = self.coord.lock().await;
        let mut tx = self.db.begin().await?;
        let retry:Option<Uuid>=sqlx::query_scalar("SELECT p.owner_task FROM pending_uploads p JOIN tasks t ON t.id=p.owner_task WHERE p.next_retry_at<=now() AND t.kind='upload' AND t.state='failed' ORDER BY p.next_retry_at,p.chunk_id LIMIT 1 FOR UPDATE OF t SKIP LOCKED").fetch_optional(&mut *tx).await?;
        if let Some(id) = retry {
            sqlx::query("UPDATE tasks SET state='queued',error=NULL,updated_at=now() WHERE id=$1 AND state='failed'").bind(id).execute(&mut *tx).await?;
            tx.commit().await?;
            return Ok(true);
        }
        // One task claims one due CDC segment; the existing worker set bounds preparation.
        let row:Option<(i64,Option<Uuid>,Option<i64>)>=sqlx::query_as("SELECT chunk_id,stream_id,source_pack FROM pending_uploads WHERE owner_task IS NULL AND next_retry_at<=now() ORDER BY next_retry_at,chunk_id LIMIT 1 FOR UPDATE SKIP LOCKED").fetch_optional(&mut *tx).await?;
        let Some((first, stream, source)) = row else {
            return Ok(false);
        };
        let id = crate::tasks::insert(
            &mut tx,
            "upload",
            None,
            json!({"first":first,"stream_id":stream,"source_pack":source}),
        )
        .await?;
        let candidates:Vec<(i64,i64,i32)>=sqlx::query_as("SELECT p.chunk_id,p.offset_bytes,c.raw_size FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id WHERE p.owner_task IS NULL AND (p.chunk_id=$1 OR ($2::uuid IS NOT NULL AND p.stream_id=$2 AND p.source_pack IS NOT DISTINCT FROM $3)) ORDER BY p.offset_bytes LIMIT 1026")
            .bind(first).bind(stream).bind(source).fetch_all(&mut *tx).await?;
        let mut ids = Vec::new();
        let mut bytes = 0u64;
        let mut end = None;
        for (chunk, offset, size) in candidates {
            if !ids.is_empty()
                && (end != Some(offset)
                    || bytes + size as u64 > config::bytes(&self.config.pack.max_size)?)
            {
                break;
            }
            ids.push(chunk);
            bytes += size as u64;
            end = Some(offset + i64::from(size));
        }
        if !ids.contains(&first) {
            ids = vec![first];
        }
        sqlx::query("UPDATE pending_uploads SET owner_task=$2 WHERE chunk_id=ANY($1)")
            .bind(&ids)
            .bind(id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(true)
    }
    pub async fn upload_batch(&self, task: Uuid, detail: Value, flush: bool) -> Result<bool> {
        if !flush && self.maintenance.load(Ordering::Acquire) {
            sqlx::query("UPDATE tasks SET state='paused',updated_at=now() WHERE id=$1")
                .bind(task)
                .execute(&self.db)
                .await?;
            return Ok(false);
        }
        let selection = self.coord.lock().await;
        let first: Option<i64> = if flush {
            sqlx::query_scalar("SELECT chunk_id FROM pending_uploads u WHERE created_at<=($1->>'upper_time')::timestamptz AND NOT EXISTS(SELECT 1 FROM pack_inputs WHERE chunk_id=u.chunk_id) AND (owner_task IS NULL OR owner_task=$2 OR owner_task IN (SELECT id FROM tasks WHERE state IN ('paused','failed'))) ORDER BY created_at,chunk_id LIMIT 1")
                .bind(&detail).bind(task).fetch_optional(&self.db).await?
        } else {
            detail["first"].as_i64()
        };
        let Some(first) = first else {
            if flush && sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM pending_uploads WHERE created_at<=($1->>'upper_time')::timestamptz)").bind(&detail).fetch_one(&self.db).await? {
                drop(selection);
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                return Ok(true);
            }
            return self.finish_upload_task(task).await;
        };
        let durable:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE chunk_id=$1 AND (EXISTS(SELECT 1 FROM chunk_locations WHERE chunk_id=$1 AND state='ready') OR (c.pack_id IS NOT NULL AND c.pack_id IS DISTINCT FROM u.source_pack)))").bind(first).fetch_one(&self.db).await?;
        let unused: bool =
            sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=$1)")
                .bind(first)
                .fetch_one(&self.db)
                .await?;
        if durable || unused {
            drop(selection);
            self.release_pending(&[first], false).await?;
            return if flush {
                Ok(true)
            } else {
                self.finish_upload_task(task).await
            };
        }
        let row:Option<(Option<Uuid>,i64,Option<i64>,bool)>=sqlx::query_as("SELECT stream_id,offset_bytes,source_pack,created_at<=now()-$2*interval '1 second' FROM pending_uploads WHERE chunk_id=$1")
            .bind(first).bind(config::seconds(&self.config.pack.upload_cache_timeout)? as f64).fetch_optional(&self.db).await?;
        let Some((stream, offset, source, expired)) = row else {
            return self.finish_upload_task(task).await;
        };
        let _pin = stream.map(|id| self.pin(id));
        if let Some(id) = source.filter(|_| !flush) {
            let ids:Vec<i64>=sqlx::query_scalar("SELECT chunk_id FROM pending_uploads WHERE source_pack=$1 AND (owner_task IS NULL OR owner_task=$2)").bind(id).bind(task).fetch_all(&self.db).await?;
            sqlx::query("UPDATE pending_uploads SET owner_task=$2 WHERE chunk_id=ANY($1)")
                .bind(&ids)
                .bind(task)
                .execute(&self.db)
                .await?;
            drop(selection);
            #[cfg(feature = "fault-injection")]
            crate::faults::point("upload-before-work").await;
            self.rewrite_pack(task, id, false).await?;
            self.release_pending(&ids, true).await?;
        } else {
            let all:Vec<Mapping>=sqlx::query_as("SELECT c.*,p.offset_bytes,c.pack_id FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id WHERE (p.chunk_id=$1 OR ($2::uuid IS NOT NULL AND p.stream_id=$2 AND p.offset_bytes>$3 AND p.source_pack IS NULL)) AND (p.owner_task IS NULL OR p.owner_task=$4 OR ($5 AND p.owner_task IN (SELECT id FROM tasks WHERE state IN ('paused','failed')))) ORDER BY p.offset_bytes LIMIT 1026")
                .bind(first).bind(stream).bind(offset).bind(task).bind(flush).fetch_all(&self.db).await?;
            let mut rows: Vec<Mapping> = Vec::new();
            let mut raw = 0u64;
            for row in all {
                if let Some(last) = rows.last()
                    && (last.offset_bytes + i64::from(last.chunk.raw_size) != row.offset_bytes
                        || raw + row.chunk.raw_size as u64
                            > config::bytes(&self.config.pack.max_size)?
                        || !Self::same_references(
                            last.chunk.id,
                            row.chunk.id,
                            row.offset_bytes - last.offset_bytes,
                        )
                        .fetch_one(&self.db)
                        .await?)
                {
                    break;
                }
                raw += row.chunk.raw_size as u64;
                rows.push(row);
                if flush || source.is_some() {
                    break;
                }
            }
            if rows.is_empty() {
                return self.finish_upload_task(task).await;
            }
            let ids: Vec<i64> = rows.iter().map(|r| r.chunk.id).collect();
            sqlx::query("UPDATE pending_uploads SET owner_task=$2 WHERE chunk_id=ANY($1)")
                .bind(&ids)
                .bind(task)
                .execute(&self.db)
                .await?;
            drop(selection);
            #[cfg(feature = "fault-injection")]
            crate::faults::point("upload-before-work").await;
            self.pin_pack_inputs(task, &rows).await?;
            let metadata: Option<(Value, String)> = if let Some(stream) = stream {
                sqlx::query_as("SELECT metadata,object_key FROM streams WHERE id=$1")
                    .bind(stream)
                    .fetch_optional(&self.db)
                    .await?
            } else {
                None
            };
            let content_type = metadata.as_ref().and_then(|m| m.0["content_type"].as_str());
            let key = metadata.as_ref().map_or("", |m| m.1.as_str());
            let outputs = self
                .pack_output(
                    task,
                    &rows,
                    &ReadContext::default(),
                    content_type,
                    key,
                    self.pack_creation_allowed() && !flush && !expired,
                )
                .await?;
            self.commit_outputs(task, &rows, &outputs, false).await?;
            #[cfg(feature = "fault-injection")]
            crate::faults::point("upload-before-release").await;
            self.release_pending(&ids, false).await?;
        }
        sqlx::query("UPDATE tasks SET processed=processed+1,updated_at=now() WHERE id=$1")
            .bind(task)
            .execute(&self.db)
            .await?;
        if flush {
            Ok(true)
        } else {
            self.finish_upload_task(task).await
        }
    }
    async fn finish_upload_task(&self, id: Uuid) -> Result<bool> {
        sqlx::query("UPDATE pending_uploads SET owner_task=NULL WHERE owner_task=$1")
            .bind(id)
            .execute(&self.db)
            .await?;
        sqlx::query(
            "UPDATE tasks SET state='completed',updated_at=now() WHERE id=$1 AND state='running'",
        )
        .bind(id)
        .execute(&self.db)
        .await?;
        Ok(false)
    }
    pub async fn release_pending(&self, ids: &[i64], existing_pack: bool) -> Result<()> {
        let _coord = self.coord.lock().await;
        let mut tx = self.db.begin().await?;
        let keys: Vec<(i64, Uuid)> = sqlx::query_as(
            "SELECT id,storage_id FROM chunks WHERE id=ANY($1) ORDER BY id FOR UPDATE",
        )
        .bind(ids)
        .fetch_all(&mut *tx)
        .await?;
        let mut released = Vec::new();
        for (id, key) in keys {
            let ready:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM chunk_locations WHERE chunk_id=$1 AND state='ready') OR EXISTS(SELECT 1 FROM chunks c JOIN packs p ON p.id=c.pack_id WHERE c.id=$1 AND p.state='ready' AND ($2 OR c.pack_id IS DISTINCT FROM (SELECT source_pack FROM pending_uploads WHERE chunk_id=$1))) OR NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=$1)").bind(id).bind(existing_pack).fetch_one(&mut *tx).await?;
            if ready {
                sqlx::query("INSERT INTO pack_maintenance(stream_id,reason) SELECT DISTINCT e.stream_id,'pack' FROM extents e JOIN streams s ON s.id=e.stream_id JOIN chunks c ON c.id=e.chunk_id WHERE e.chunk_id=$1 AND s.state='ready' AND c.pack_id IS NULL ON CONFLICT(stream_id) DO UPDATE SET cursor=0,generation=pack_maintenance.generation+1,updated_at=now()")
                    .bind(id).execute(&mut *tx).await?;
                sqlx::query("DELETE FROM pending_uploads WHERE chunk_id=$1")
                    .bind(id)
                    .execute(&mut *tx)
                    .await?;
                released.push(key);
            }
        }
        tx.commit().await?;
        for key in released {
            self.storage.release_upload(key);
        }
        Ok(())
    }
    pub async fn upload_retry(&self, task: Uuid, error: &str) -> Result<()> {
        let mut tx = self.db.begin().await?;
        sqlx::query("UPDATE tasks SET state='failed',error=$2,updated_at=now() WHERE id=$1 AND state='running'").bind(task).bind(error).execute(&mut *tx).await?;
        sqlx::query("UPDATE pending_uploads SET owner_task=CASE WHEN (SELECT kind FROM tasks WHERE id=$1)='upload' THEN owner_task ELSE NULL END,attempts=LEAST(attempts+1,30),last_error=$2,next_retry_at=now()+LEAST(300,power(2,LEAST(attempts,8))) * interval '1 second' WHERE owner_task=$1").bind(task).bind(error).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn reconcile_upload_pins(&self) -> Result<()> {
        for keys in self.storage.upload_pin_ids().chunks(128) {
            let _coord = self.coord.lock().await;
            let mut tx = self.db.begin().await?;
            sqlx::query("SELECT id FROM chunks WHERE storage_id=ANY($1) ORDER BY id FOR UPDATE")
                .bind(keys)
                .execute(&mut *tx)
                .await?;
            let keep:Vec<Uuid>=sqlx::query_scalar("SELECT c.storage_id FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE c.storage_id=ANY($1)").bind(keys).fetch_all(&mut *tx).await?;
            for key in keys {
                if !keep.contains(key) {
                    self.storage.release_upload(*key);
                }
            }
            tx.commit().await?;
        }
        let dead:Vec<i64>=sqlx::query_scalar("SELECT chunk_id FROM pending_uploads u WHERE owner_task IS NULL AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=u.chunk_id) ORDER BY chunk_id LIMIT 128").fetch_all(&self.db).await?;
        self.release_pending(&dead, false).await?;
        Ok(())
    }
}
