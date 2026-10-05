use crate::{app::App, codec::Chunk, config};
use anyhow::{Context, Result, ensure};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use uuid::Uuid;
tokio::task_local! { pub(crate) static REQUEST_ID: Uuid; }

#[derive(Deserialize)]
struct StorageMigration {
    source_bucket: Uuid,
    source_key: String,
    source_version: Uuid,
    target_bucket: Uuid,
    target_key: String,
    target_version: Option<Uuid>,
    action: String,
    public_read: bool,
}

#[derive(Deserialize)]
struct RekeyTask {
    store_id: Uuid,
    #[serde(default)]
    upper_chunk_id: i64,
    target_key_id: Option<Uuid>,
    target_key: String,
    target_algorithm: String,
}

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
        .bind(id).bind(kind).bind(bucket).bind(detail).bind(crate::control::audit::current()).execute(db).await?;
    Ok(id)
}

impl App {
    pub async fn rekey_start(
        &self,
        store_id: Uuid,
        target_key: Option<&str>,
        execute: bool,
        confirmation: Option<&str>,
    ) -> Result<Value> {
        let target: Option<(Uuid, String, String)> = if let Some(name) = target_key {
            Some(
                sqlx::query_as(
                    "SELECT id,name,algorithm FROM managed_keys WHERE (id=$1::uuid OR name=$2) AND purpose='block' AND revoked_at IS NULL",
                )
                .bind(name.parse::<Uuid>().ok())
                .bind(name)
                .fetch_optional(&self.db)
                .await?
                .context("active block key was not found")?,
            )
        } else {
            None
        };
        if let Some((_, name, _)) = target.as_ref() {
            self.storage.ensure_key_loaded(name).await?;
        }
        let target_algorithm = target
            .as_ref()
            .map(|(_, _, algorithm)| algorithm.as_str())
            .unwrap_or("none");
        let target_name = target
            .as_ref()
            .map(|(_, name, _)| name.as_str())
            .unwrap_or("");
        let (loose, packed): (i64, i64) = sqlx::query_as(
            "SELECT count(*) FILTER (WHERE c.pack_id IS NULL AND (l.id IS NULL OR l.algorithm<>$2 OR l.key_id<>$3)), count(*) FILTER (WHERE c.pack_id IS NOT NULL AND (p.algorithm<>$2 OR p.key_id<>$3)) FROM chunks c LEFT JOIN chunk_locations l ON l.chunk_id=c.id AND l.state='ready' LEFT JOIN packs p ON p.id=c.pack_id AND p.state='ready' WHERE c.store_id=$1 AND c.state='ready'",
        )
        .bind(store_id)
        .bind(target_algorithm)
        .bind(target_name)
        .fetch_one(&self.db)
        .await?;
        let preview = json!({"store_id":store_id,"target_key":target_name,"target_algorithm":target_algorithm,"loose_chunks":loose,"packed_chunks":packed,"effect":"rewrite physical encodings and retain old sources until GC"});
        if !execute {
            let confirmation = blake3::hash(&serde_json::to_vec(&preview)?).to_hex().to_string();
            return Ok(json!({"preview":preview,"confirmation":confirmation}));
        }
        if confirmation != Some(blake3::hash(&serde_json::to_vec(&preview)?).to_hex().as_str()) {
            return Err(s3s::s3_error!(PreconditionFailed).into());
        }
        let mut tx = self.db.begin().await?;
        sqlx::query("SELECT id FROM stores WHERE id=$1 AND enabled FOR UPDATE")
            .bind(store_id).fetch_optional(&mut *tx).await?.context("enabled store was not found")?;
        let exists: Option<Uuid> = sqlx::query_scalar(
            "SELECT id FROM tasks WHERE kind='rekey' AND detail->>'store_id'=$1 AND state IN ('queued','running','paused') LIMIT 1",
        )
        .bind(store_id.to_string())
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(id) = exists {
            tx.commit().await?;
            return Ok(json!({"task_id":id,"existing":true}));
        }
        let upper_chunk_id: i64 = sqlx::query_scalar("SELECT coalesce(max(id),0) FROM chunks WHERE store_id=$1")
            .bind(store_id).fetch_one(&mut *tx).await?;
        let id = insert(
            &mut tx,
            "rekey",
            None,
            json!({"store_id":store_id,"upper_chunk_id":upper_chunk_id,"target_key_id":target.as_ref().map(|v|v.0),"target_key":target_name,"target_algorithm":target_algorithm}),
        )
        .await?;
        tx.commit().await?;
        self.wake_tasks.notify_one();
        Ok(json!({"task_id":id,"preview":preview}))
    }

    async fn rekey_batch(&self, id: Uuid, token: Uuid, cursor: Option<&str>, detail: Value) -> Result<bool> {
        let job: RekeyTask = serde_json::from_value(detail)?;
        let after = cursor.unwrap_or("0").parse::<i64>()?;
        if let Some(key_id) = job.target_key_id {
            let current: Option<(String, String)> = sqlx::query_as(
                "SELECT name,algorithm FROM managed_keys WHERE id=$1 AND purpose='block' AND revoked_at IS NULL",
            )
            .bind(key_id)
            .fetch_optional(&self.db)
            .await?;
            ensure!(
                current.as_ref().is_some_and(|(name, algorithm)| {
                    name == &job.target_key && algorithm == &job.target_algorithm
                }),
                "rekey target key was removed or changed"
            );
            self.storage.ensure_key_loaded(&job.target_key).await?;
        }
        let row: Option<Chunk> = sqlx::query_as(
            "SELECT c.id,l.id encoding_id,l.storage_id,c.store_id,c.hash,c.raw_size,l.stored_size,l.algorithm,l.key_id,l.compressed,l.nonce,c.format,c.state,l.created_at FROM chunks c JOIN chunk_locations l ON l.chunk_id=c.id AND l.state='ready' WHERE c.id>$1 AND c.id<=$2 AND c.store_id=$3 AND c.state='ready' AND c.pack_id IS NULL AND (l.algorithm<>$4 OR l.key_id<>$5) ORDER BY c.id LIMIT 1",
        )
        .bind(after)
        .bind(job.upper_chunk_id)
        .bind(job.store_id)
        .bind(&job.target_algorithm)
        .bind(&job.target_key)
        .fetch_optional(&self.db)
        .await?;
        let packed_candidate: Option<(i64, i64)> = sqlx::query_as(
            "SELECT min(c.id),p.id FROM chunks c JOIN packs p ON p.id=c.pack_id AND p.state='ready' WHERE c.store_id=$1 AND c.state='ready' AND c.id>$2 AND c.id<=$3 AND (p.algorithm<>$4 OR p.key_id<>$5) GROUP BY p.id ORDER BY min(c.id) LIMIT 1",
        )
        .bind(job.store_id)
        .bind(after)
        .bind(job.upper_chunk_id)
        .bind(&job.target_algorithm)
        .bind(&job.target_key)
        .fetch_optional(&self.db)
        .await?;
        let process_loose = row.as_ref().is_some_and(|source| {
            packed_candidate
                .as_ref()
                .is_none_or(|(first_id, _)| source.id < *first_id)
        });
        if process_loose {
            let source = row.expect("rekey loose candidate disappeared");
            self.pin_pack_inputs(id, &[crate::pack_tasks::Mapping { chunk: source.clone(), offset_bytes: 0, pack_id: None }]).await?;
            let raw = self.storage.get_with(&source, None).await?;
            let mut physical = source.clone();
            physical.encoding_id = sqlx::query_scalar("SELECT nextval('chunk_locations_id_seq')").fetch_one(&self.db).await?;
            physical.storage_id = Uuid::new_v4();
            physical.algorithm = job.target_algorithm.clone();
            physical.key_id = job.target_key.clone();
            physical.created_at = chrono::Utc::now();
            physical.nonce = None;
            sqlx::query("INSERT INTO chunk_locations(id,chunk_id,storage_id,store_id,compressed,state,created_at,unreferenced_at,owner_task,algorithm,key_id) VALUES($1,$2,$3,$4,false,'uploading',$5,now(),$6,$7,$8)")
                .bind(physical.encoding_id).bind(source.id).bind(physical.storage_id).bind(job.store_id).bind(physical.created_at).bind(id).bind(&physical.algorithm).bind(&physical.key_id).execute(&self.db).await?;
            let (physical, encoded, _) = self.storage.encode(physical, raw.to_vec(), source.compressed).await?;
            self.storage.put_path_for(job.store_id, &self.storage.path_for(job.store_id, physical.storage_id)?, encoded).await?;
            let mut tx = self.db.begin().await?;
            Self::rekey_lease(&mut tx, id, token).await?;
            self.storage_writable()?;
            let _gate = self.storage.source_gate.write().await;
            let current: Option<Option<i64>> = sqlx::query_scalar("SELECT pack_id FROM chunks WHERE id=$1 AND state='ready' FOR UPDATE")
                .bind(source.id).fetch_optional(&mut *tx).await?;
            ensure!(current == Some(None), "chunk source changed during rekey");
            let changed = sqlx::query("UPDATE chunk_locations SET state='retired',unreferenced_at=now() WHERE id=$1 AND state='ready'")
                .bind(source.encoding_id).execute(&mut *tx).await?.rows_affected();
            ensure!(changed == 1, "source encoding changed during rekey");
            sqlx::query("UPDATE chunk_locations SET stored_size=$2,compressed=$3,nonce=$4,state='ready',unreferenced_at=NULL,stored_at=clock_timestamp(),owner_task=NULL WHERE id=$1 AND state='uploading'")
                .bind(physical.encoding_id).bind(physical.stored_size).bind(physical.compressed).bind(&physical.nonce).execute(&mut *tx).await?;
            sqlx::query("UPDATE tasks SET cursor=$2,processed=processed+1,updated_at=now() WHERE id=$1")
                .bind(id).bind(source.id.to_string()).execute(&mut *tx).await?;
            tx.commit().await?;
            return Ok(false);
        }

        if let Some((first_id, pack_id)) = packed_candidate {
            let old: crate::pack::Pack = sqlx::query_as("SELECT * FROM packs WHERE id=$1 AND state='ready'")
                .bind(pack_id)
                .fetch_one(&self.db)
                .await?;
            let members: Vec<Chunk> = sqlx::query_as(
                "SELECT c.* FROM pack_members m JOIN chunks c ON c.id=m.chunk_id WHERE m.pack_id=$1 ORDER BY m.ordinal",
            )
            .bind(pack_id)
            .fetch_all(&self.db)
            .await?;
            ensure!(members.len() == old.member_count as usize, "pack member count changed during rekey");
            let current_members: Vec<i64> = sqlx::query_scalar("SELECT c.id FROM pack_members m JOIN chunks c ON c.id=m.chunk_id WHERE m.pack_id=$1 AND c.state='ready' ORDER BY m.ordinal")
                .bind(old.id).fetch_all(&self.db).await?;
            let mappings: Vec<_> = members.iter().filter(|c| current_members.contains(&c.id)).map(|c| crate::pack_tasks::Mapping { chunk:c.clone(),offset_bytes:0,pack_id:Some(old.id) }).collect();
            self.pin_pack_inputs(id, &mappings).await?;
            let _work = self.storage.pack_work(old.payload_size()?).await?;
            let loaded = self.storage.load_pack(old.clone()).await?;
            let mut encoded_members = Vec::with_capacity(members.len());
            for mut member in members {
                let raw = loaded.data.chunk(&member)?;
                member.algorithm = job.target_algorithm.clone();
                member.key_id = job.target_key.clone();
                encoded_members.push((member, raw));
            }
            let new_storage_id = Uuid::new_v4();
            let new_pack: crate::pack::Pack = sqlx::query_as(
                "INSERT INTO packs(storage_id,store_id,algorithm,key_id,raw_size,member_count,state,unreferenced_at,owner_task) VALUES($1,$2,$3,$4,$5,$6,'preparing',now(),$7) RETURNING *",
            )
            .bind(new_storage_id)
            .bind(job.store_id)
            .bind(&job.target_algorithm)
            .bind(&job.target_key)
            .bind(old.raw_size)
            .bind(old.member_count)
            .bind(id)
            .fetch_one(&self.db)
            .await?;
            let (new_pack, encoded) = self
                .storage
                .encode_pack(
                    new_pack,
                    encoded_members.clone(),
                    crate::compression::Strategy::Always,
                    old.compressed,
                )
                .await?;
            self.storage
                .put_path_for(
                    job.store_id,
                    &self.storage.pack_path_for(job.store_id, new_storage_id)?,
                    encoded,
                )
                .await?;
            let mut tx = self.db.begin().await?;
            Self::rekey_lease(&mut tx, id, token).await?;
            self.storage_writable()?;
            let _gate = self.storage.source_gate.write().await;
            let locked: Vec<i64> = sqlx::query_scalar("SELECT id FROM chunks WHERE pack_id=$1 AND state='ready' ORDER BY id FOR UPDATE")
                .bind(old.id).fetch_all(&mut *tx).await?;
            ensure!(locked == current_members, "pack membership changed during rekey");
            let updated = sqlx::query("UPDATE packs SET stored_size=$2,compressed=$3,nonce=$4,digest=$5,state='ready',unreferenced_at=NULL,stored_at=clock_timestamp(),owner_task=NULL WHERE id=$1 AND state='preparing'")
                .bind(new_pack.id).bind(new_pack.stored_size).bind(new_pack.compressed).bind(&new_pack.nonce).bind(&new_pack.digest).execute(&mut *tx).await?.rows_affected();
            ensure!(updated == 1, "new pack was changed during rekey");
            for (ordinal, member) in encoded_members.iter().enumerate() {
                sqlx::query("INSERT INTO pack_members(pack_id,ordinal,chunk_id,offset_bytes) VALUES($1,$2,$3,$4)")
                    .bind(new_pack.id)
                    .bind(ordinal as i32)
                    .bind(member.0.id)
                    .bind(encoded_members[..ordinal].iter().map(|(c, _)| i64::from(c.raw_size)).sum::<i64>())
                    .execute(&mut *tx)
                    .await?;
            }
            let changed = sqlx::query("UPDATE chunks SET pack_id=$2 WHERE pack_id=$1 AND state='ready'")
                .bind(old.id)
                .bind(new_pack.id)
                .execute(&mut *tx)
                .await?
                .rows_affected();
            ensure!(changed == current_members.len() as u64, "pack membership changed during rekey");
            let retired = sqlx::query("UPDATE packs SET state='retired',unreferenced_at=now(),owner_task=NULL WHERE id=$1 AND state='ready'")
                .bind(old.id)
                .execute(&mut *tx)
                .await?
                .rows_affected();
            ensure!(retired == 1, "source pack changed during rekey");
            let last_id: i64 = sqlx::query_scalar("SELECT max(chunk_id) FROM pack_members WHERE pack_id=$1")
                .bind(new_pack.id)
                .fetch_one(&mut *tx)
                .await?;
            sqlx::query("UPDATE tasks SET cursor=$2,processed=processed+1,updated_at=now() WHERE id=$1")
                .bind(id)
                .bind(last_id.max(first_id).to_string())
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            return Ok(false);
        }

            sqlx::query("UPDATE tasks SET state='completed',updated_at=now(),cursor=$2,lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL WHERE id=$1 AND state='running' AND lease_token=$3 AND lease_expires_at>clock_timestamp()")
                .bind(id).bind(after.to_string()).bind(token).execute(&self.db).await?;
            return Ok(true);
    }

    async fn rekey_lease(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, id: Uuid, token: Uuid) -> Result<()> {
        let owned: Option<Uuid> = sqlx::query_scalar("SELECT id FROM tasks WHERE id=$1 AND state='running' AND lease_token=$2 AND lease_expires_at>clock_timestamp() FOR UPDATE")
            .bind(id).bind(token).fetch_optional(&mut **tx).await?;
        ensure!(owned.is_some(), "rekey task was paused or lost its lease");
        Ok(())
    }

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
        let mut tx = crate::control::users::admin_transaction(self, principal).await?;
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
            let preview = crate::control::buckets::impact(&mut tx, id).await?;
            if crate::control::buckets::fingerprint(&preview, None)? != expected {
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
        crate::control::audit::checkpoint(
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
        store_id: Option<Uuid>,
    ) -> Result<Value> {
        let _coord = self.coord.lock().await;
        let age = config::seconds(older_than)?;
        ensure!(age <= i64::MAX as u64 / 1000, "age too large");
        let minimum = self.config.storage.min_storage_seconds()?;
        let store_id = self.storage.select_store(store_id)?;
        let prefix = self.storage.namespace_for(store_id)?;
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
            let previous_target: Uuid = previous["store_id"]
                .as_str()
                .context("preview store is missing")?
                .parse()?;
            ensure!(
                previous_target == store_id,
                "preview store mismatch"
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
        let detail = json!({"dry_run":!execute,"store_id":store_id,"prefix":prefix,"older_than_seconds":age,"min_storage_duration_seconds":minimum,"cutoff":cutoff.to_rfc3339(),"candidates":0,"bytes":0,"unrecognized":0,"samples":[]});
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
            } else if matches!(
                kind.as_str(),
                "purge" | "pack" | "unpack" | "upload" | "gc" | "storage_migration" | "rekey"
            ) {
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
            .bind(crate::control::account::USER_LOCK)
            .execute(&mut *tx)
            .await?;
        crate::control::buckets::lock_dependents(&mut tx, bucket).await?;
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
        let store_id: Uuid = detail["store_id"]
            .as_str()
            .context("missing sweep store")?
            .parse()?;
        let prefix = self.storage.namespace_for(store_id)?;
        ensure!(detail["prefix"] == prefix, "backend namespace changed");
        let cutoff = chrono::DateTime::parse_from_rfc3339(
            detail["cutoff"].as_str().context("missing sweep cutoff")?,
        )?
        .with_timezone(&chrono::Utc);
        // A resumed task may have been created under a shorter retention policy.
        let minimum = self.config.storage.min_storage_seconds()?;
        let cutoff = cutoff.min(
            chrono::Utc::now()
                .checked_sub_signed(chrono::TimeDelta::seconds(minimum as i64))
                .context("age too large")?,
        );
        let rows = self
            .storage
            .list_physical_for(store_id, cursor, self.config.gc.batch_size as usize)
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
            let Some(storage) = self.storage.parse_physical_path_for(store_id, name)? else {
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
                self.storage
                    .delete_path_for(store_id, &row.location)
                    .await?;
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

    async fn storage_migration_batch(
        &self,
        id: Uuid,
        lease_token: Uuid,
        detail: Value,
    ) -> Result<bool> {
        let job: StorageMigration = serde_json::from_value(detail)?;
        ensure!(
            matches!(job.action.as_str(), "copy" | "move"),
            "invalid storage migration action"
        );
        let (source, _source_pin) = self.current(job.source_bucket, &job.source_key).await?;
        ensure!(
            source.id == job.source_version,
            "source object changed before storage migration"
        );
        let authority = crate::authorization::Permit::local();
        let (destination, _destination_pin) = self
            .new_stream(
                &authority,
                job.target_bucket,
                &job.target_key,
                "object",
                source.metadata.clone(),
                job.public_read,
                false,
                None,
                Some((id, lease_token)),
            )
            .await?;
        let result: Result<()> = async {
            self.copy_extents(destination, &source).await?;
            self.finish_stream(
                destination,
                source.size,
                &source.etag,
                source.checksums.clone(),
            )
            .await?;
            #[cfg(feature = "fault-injection")]
            crate::faults::point("migration-before-publish").await;
            let _coord = self.coord.lock().await;
            self.writable()?;
            let mut tx = self.db.begin().await?;
            let owned: Option<Uuid> = sqlx::query_scalar("SELECT id FROM tasks WHERE id=$1 AND state='running' AND lease_token=$2 AND lease_expires_at>clock_timestamp() FOR UPDATE")
                .bind(id)
                .bind(lease_token)
                .fetch_optional(&mut *tx)
                .await?;
            ensure!(owned.is_some(), "storage migration lease lost before publish");
            let source_current = crate::control::actions::selected(
                &mut tx,
                job.source_bucket,
                &job.source_key,
                Some(job.source_version),
            )
            .await?;
            let old = crate::control::actions::selected(
                &mut tx,
                job.target_bucket,
                &job.target_key,
                job.target_version,
            )
            .await?;
            ensure!(
                source_current == Some(job.source_version),
                "source object changed during storage migration"
            );
            let published = sqlx::query("UPDATE streams SET state='ready',owner_task=NULL,owner_lease=NULL,touched_at=now() WHERE id=$1 AND state='writing' AND owner_task=$2 AND owner_lease=$3")
                .bind(destination)
                .bind(id)
                .bind(lease_token)
                .execute(&mut *tx)
                .await?;
            ensure!(published.rows_affected() == 1, "storage migration stream was reclaimed");
            sqlx::query("INSERT INTO objects(bucket_id,key,stream_id,write_epoch) VALUES($1,$2,$3,$3) ON CONFLICT(bucket_id,key) DO UPDATE SET stream_id=excluded.stream_id,write_epoch=excluded.write_epoch")
                .bind(job.target_bucket)
                .bind(&job.target_key)
                .bind(destination)
                .execute(&mut *tx)
                .await?;
            if job.action == "move" {
                sqlx::query("UPDATE objects SET stream_id=NULL,write_epoch=$3 WHERE bucket_id=$1 AND key=$2")
                    .bind(job.source_bucket)
                    .bind(&job.source_key)
                    .bind(Uuid::new_v4())
                    .execute(&mut *tx)
                    .await?;
                sqlx::query("UPDATE streams SET state='retired',touched_at=now() WHERE id=$1")
                    .bind(job.source_version)
                    .execute(&mut *tx)
                    .await?;
            }
            if let Some(old) = old {
                sqlx::query("UPDATE streams SET state='retired',touched_at=now() WHERE id=$1")
                    .bind(old)
                    .execute(&mut *tx)
                    .await?;
            }
            let completed = sqlx::query("UPDATE tasks SET state='completed',processed=processed+1,lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$2 AND lease_expires_at>clock_timestamp()")
                .bind(id)
                .bind(lease_token)
                .execute(&mut *tx)
                .await?;
            ensure!(
                completed.rows_affected() == 1,
                "storage migration lease lost before publish"
            );
            tx.commit().await?;
            Ok(())
        }
        .await;
        if result.is_err() {
            sqlx::query("UPDATE streams SET state='abandoned',owner_task=NULL,owner_lease=NULL,touched_at=now() WHERE id=$1 AND state='writing'")
                .bind(destination)
                .execute(&self.db)
                .await?;
        }
        result?;
        self.wake_gc.notify_one();
        Ok(true)
    }
}

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
        "UPDATE streams SET state='abandoned',owner_task=NULL,owner_lease=NULL,touched_at=now() WHERE state='writing' AND owner_task=ANY($1)",
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
    let lease_node_id = crate::cluster::local_node_id(&app).await?;
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
                crate::cluster::validate_node(&app, &mut *tx, state, "worker").await?;
            }
            let id:Option<Uuid>=sqlx::query_scalar("UPDATE tasks SET state='running',started_at=coalesce(started_at,now()),lease_node_id=$3,lease_token=$4,lease_expires_at=now()+interval '5 minutes',updated_at=now() WHERE id=(SELECT t.id FROM tasks t CROSS JOIN mokyu_meta m WHERE (t.state='queued' OR (t.state='running' AND t.lease_expires_at<now())) AND CASE WHEN t.kind IN ('pack','unpack') THEN 1 WHEN t.kind IN ('upload','cache_flush','storage_migration','rekey') THEN 2 ELSE 0 END=$1 AND NOT(t.id=ANY($2)) AND NOT (m.maintenance AND t.kind IN ('pack','unpack','purge','gc','storage_migration','rekey')) AND NOT EXISTS(SELECT 1 FROM maintenance_controls c WHERE c.kind=CASE WHEN t.kind='pack' THEN t.detail->>'kind' ELSE t.kind END AND CASE WHEN c.kind='gc' THEN m.gc_paused ELSE c.paused END) AND NOT (t.kind='pack' AND t.detail->>'kind' IN ('pack','repack') AND m.pack_creation_paused) ORDER BY t.updated_at,t.id LIMIT 1) AND (state='queued' OR (state='running' AND lease_expires_at<now())) RETURNING id").bind(class).bind(ids).bind(lease_node_id).bind(lease_token).fetch_optional(&mut *tx).await?;
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
            let (kind, state, bucket, cursor, detail, task_lease): (
                String,
                String,
                Option<Uuid>,
                Option<String>,
                Value,
                Option<Uuid>,
            ) = sqlx::query_as(
                "SELECT kind,state,bucket_id,cursor,detail,lease_token FROM tasks WHERE id=$1",
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
            let renewal = renew_lease(app.clone(), id, lease_token);
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
                        crate::upload_cache::FLUSH.scope(
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
            } else if kind == "rekey" {
                let result = crate::storage::resource_gate::PRIORITY
                    .scope(
                        crate::storage::resource_gate::MAINTENANCE,
                        app.rekey_batch(id, lease_token, cursor.as_deref(), detail),
                    )
                    .await;
                if matches!(result, Ok(false)) {
                    sqlx::query("UPDATE tasks SET state='queued',lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$2")
                        .bind(id)
                        .bind(lease_token)
                        .execute(&app.db)
                        .await?;
                }
                result
            } else {
                Err(anyhow::anyhow!("unknown maintenance task kind"))
            };
            renewal.abort();
            if kind == "gc" && result.is_err() {
                app.statistics
                    .gc_failures
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            if class != 0 {
                if class == 1 && result.is_err() && matches!(kind.as_str(), "pack" | "unpack") {
                    sqlx::query("UPDATE pack_changes SET next_check_at=now()+$2*interval '1 second' WHERE pack_id IN (SELECT c.pack_id FROM pack_inputs i JOIN chunks c ON c.id=i.chunk_id WHERE i.task_id=$1)").bind(id).bind(config::seconds(&app.config.pack.reuse_interval)? as f64).execute(&app.db).await?;
                    sqlx::query("UPDATE pack_maintenance SET next_check_at=now()+$2*interval '1 second' WHERE stream_id IN (SELECT e.stream_id FROM extents e JOIN pack_inputs i ON i.chunk_id=e.chunk_id WHERE i.task_id=$1)").bind(id).bind(config::seconds(&app.config.pack.reuse_interval)? as f64).execute(&app.db).await?;
                }
                if matches!(kind.as_str(), "pack" | "unpack" | "rekey") {
                    app.finish_pack_work(id).await?;
                }
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
                    if class == 2 && matches!(kind.as_str(), "upload" | "cache_flush") {
                        app.upload_retry(id, &e.to_string()).await?;
                    } else {
                        sqlx::query("UPDATE tasks SET state='failed',error=$2,lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$3").bind(id).bind(e.to_string()).bind(lease_token).execute(&app.db).await?;
                    }
                }
            }
            active.lock().await.remove(&id);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}
