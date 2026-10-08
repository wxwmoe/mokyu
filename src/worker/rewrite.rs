use crate::{
    app::App,
    content::codec::Chunk,
    worker::{insert, insert_child},
};
use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

#[derive(Deserialize)]
struct RewriteTask {
    storage_id: Uuid,
    #[serde(default)]
    upper_chunk_id: i64,
    #[serde(default)]
    shard: i64,
    #[serde(default = "one_shard")]
    shards: i64,
    #[serde(default)]
    profile_revision: i64,
    target_key_id: Option<Uuid>,
    target_key: String,
    target_algorithm: String,
}

fn one_shard() -> i64 {
    1
}

impl App {
    pub async fn rewrite_start(
        &self,
        storage_id: Uuid,
        target_key: Option<&str>,
        execute: bool,
        confirmation: Option<&str>,
    ) -> Result<Value> {
        let target: Option<(Uuid, String, String)> = if let Some(name) = target_key {
            Some(
                sqlx::query_as(
                    "SELECT id,name,key_algorithm FROM managed_keys WHERE (id=$1::uuid OR name=$2) AND purpose='block' AND revoked_at IS NULL",
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
            self.storage.ensure_key_loaded_name(name).await?;
        }
        let target_algorithm = target
            .as_ref()
            .map(|(_, _, algorithm)| algorithm.as_str())
            .unwrap_or("none");
        let target_name = target
            .as_ref()
            .map(|(_, name, _)| name.as_str())
            .unwrap_or("");
        let profile = crate::storage::profile::StorageProfile::load(&self.db, storage_id).await?;
        let (loose, packed): (i64, i64) = sqlx::query_as(
            "SELECT count(*) FILTER (WHERE c.pack_id IS NULL AND (l.encoding_id IS NULL OR l.encoding_fingerprint IS DISTINCT FROM digest(concat_ws(':',$2::text,$3::uuid::text,l.standalone_compressed::text,sp.compression_strategy,sp.compression_level::text,sp.compression_sample_level::text,sp.compression_min_savings_percent::text,sp.compression_min_savings_bytes::text,'standalone'),'sha256') OR ((NOT sp.compression_enabled OR sp.compression_strategy='never') AND l.standalone_compressed))), count(*) FILTER (WHERE c.pack_id IS NOT NULL AND (p.pack_fingerprint IS DISTINCT FROM digest(concat_ws(':',$2::text,$3::uuid::text,p.pack_compressed::text,p.member_count::text,sp.compression_strategy,sp.compression_level::text,sp.compression_sample_level::text,sp.compression_min_savings_percent::text,sp.compression_min_savings_bytes::text,sp.pack_max_size,sp.pack_range_enabled::text,sp.pack_range_min_downloads::text,sp.pack_range_min_savings_percent::text,sp.pack_range_min_savings_bytes,'pack'),'sha256') OR ((NOT sp.compression_enabled OR sp.compression_strategy='never') AND p.pack_compressed)))) FROM chunks c JOIN storage_profiles sp ON sp.storage_id=c.storage_id LEFT JOIN chunk_encodings l ON l.chunk_id=c.id AND l.state='ready' LEFT JOIN packs p ON p.id=c.pack_id AND p.state='ready' WHERE c.storage_id=$1 AND c.state='ready'",
        )
        .bind(storage_id)
        .bind(target_algorithm)
        .bind(target.as_ref().map(|v| v.0))
        .fetch_one(&self.db)
        .await?;
        let preview = json!({"storage_id":storage_id,"target_key":target_name,"target_algorithm":target_algorithm,"compression_strategy":profile.compression_strategy,"loose_chunks":loose,"packed_chunks":packed,"effect":"rewrite physical encodings and retain old sources until GC"});
        if !execute {
            let confirmation = blake3::hash(&serde_json::to_vec(&preview)?)
                .to_hex()
                .to_string();
            return Ok(json!({"preview":preview,"confirmation":confirmation}));
        }
        if confirmation
            != Some(
                blake3::hash(&serde_json::to_vec(&preview)?)
                    .to_hex()
                    .as_str(),
            )
        {
            return Err(s3s::s3_error!(PreconditionFailed).into());
        }
        let mut tx = self.db.begin().await?;
        sqlx::query("SELECT id FROM storages WHERE id=$1 AND enabled FOR UPDATE")
            .bind(storage_id)
            .fetch_optional(&mut *tx)
            .await?
            .context("enabled store was not found")?;
        let exists: Option<Uuid> = sqlx::query_scalar(
            "SELECT id FROM tasks WHERE task_type='rewrite' AND detail->>'role'='parent' AND detail->>'storage_id'=$1 AND state IN ('queued','running','paused') LIMIT 1",
        )
        .bind(storage_id.to_string())
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(id) = exists {
            tx.commit().await?;
            return Ok(json!({"task_id":id,"existing":true}));
        }
        let upper_chunk_id: i64 =
            sqlx::query_scalar("SELECT coalesce(max(id),0) FROM chunks WHERE storage_id=$1")
                .bind(storage_id)
                .fetch_one(&mut *tx)
                .await?;
        let local_storage: bool =
            sqlx::query_scalar("SELECT type='local' FROM storages WHERE id=$1")
                .bind(storage_id)
                .fetch_one(&mut *tx)
                .await?;
        let target_node_id = if local_storage {
            Some(
                self.node
                    .get()
                    .context("local rewrite requires a registered node")?
                    .node_id,
            )
        } else {
            None
        };
        let execution_scope = if local_storage { "node" } else { "cluster" };
        let shards = if loose + packed == 0 {
            0
        } else {
            (loose + packed).clamp(1, 8)
        };
        let id = insert(
            &mut tx,
            "rewrite",
            None,
            json!({"role":"parent","storage_id":storage_id,"upper_chunk_id":upper_chunk_id,"profile_revision":profile.revision,"target_key_id":target.as_ref().map(|v|v.0),"target_key":target_name,"target_algorithm":target_algorithm}),
        )
        .await?;
        sqlx::query(
            "UPDATE tasks SET execution_scope=$2,target_node_id=$3,batch_total=$4 WHERE id=$1",
        )
        .bind(id)
        .bind(execution_scope)
        .bind(target_node_id)
        .bind(shards)
        .execute(&mut *tx)
        .await?;
        for shard in 0..shards {
            let child = insert_child(
                &mut tx,
                "rewrite",
                None,
                json!({"role":"child","storage_id":storage_id,"upper_chunk_id":upper_chunk_id,"profile_revision":profile.revision,"shard":shard,"shards":shards,"target_key_id":target.as_ref().map(|v|v.0),"target_key":target_name,"target_algorithm":target_algorithm}),
            )
            .await?;
            sqlx::query("UPDATE tasks SET parent_task_id=$2,execution_scope=$3,target_node_id=$4,batch_total=$5 WHERE id=$1")
                .bind(child)
                .bind(id)
                .bind(execution_scope)
                .bind(target_node_id)
                .bind(shards)
                .execute(&mut *tx)
                .await?;
        }
        if shards == 0 {
            sqlx::query(
                "UPDATE tasks SET state='completed',batch_completed=0,updated_at=now() WHERE id=$1",
            )
            .bind(id)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        self.wake_tasks.notify_one();
        Ok(json!({"task_id":id,"preview":preview,"children":shards}))
    }

    pub(super) async fn rewrite_batch(
        &self,
        id: Uuid,
        token: Uuid,
        cursor: Option<&str>,
        detail: Value,
    ) -> Result<bool> {
        let job: RewriteTask = serde_json::from_value(detail)?;
        let after = cursor.unwrap_or("0").parse::<i64>()?;
        let shards = job.shards.max(1);
        let profile =
            crate::storage::profile::StorageProfile::load(&self.db, job.storage_id).await?;
        let _storage_permit = self
            .storage
            .profile_permit(job.storage_id, "maintenance")
            .await?;
        let strategy = match profile.compression_strategy.as_str() {
            "never" => crate::content::compression::Strategy::Never,
            "sample" => crate::content::compression::Strategy::Sample,
            _ => crate::content::compression::Strategy::Always,
        };
        ensure!(
            job.profile_revision == 0 || job.profile_revision == profile.revision,
            "storage profile changed during rewrite; preview again"
        );
        if let Some(key_id) = job.target_key_id {
            let current: Option<(String, String)> = sqlx::query_as(
                "SELECT name,key_algorithm FROM managed_keys WHERE id=$1 AND purpose='block' AND revoked_at IS NULL",
            )
            .bind(key_id)
            .fetch_optional(&self.db)
            .await?;
            ensure!(
                current.as_ref().is_some_and(|(name, algorithm)| {
                    name == &job.target_key && algorithm == &job.target_algorithm
                }),
                "rewrite target key was removed or changed"
            );
            self.storage.ensure_key_loaded_name(&job.target_key).await?;
        }
        let row: Option<Chunk> = sqlx::query_as(
            "SELECT c.id,l.encoding_id AS current_encoding_id,l.location_id,c.storage_id,c.hash,c.raw_size,l.standalone_size AS standalone_size,l.encryption_algorithm AS encryption_algorithm,l.block_key_id,l.standalone_compressed AS standalone_compressed,l.nonce,c.format,c.state,c.created_at FROM chunks c JOIN chunk_encodings l ON l.chunk_id=c.id AND l.state='ready' JOIN storage_profiles sp ON sp.storage_id=c.storage_id WHERE c.id>$1 AND c.id<=$2 AND c.storage_id=$3 AND c.state='ready' AND c.pack_id IS NULL AND (c.id % $6)=$7 AND (l.encoding_fingerprint IS DISTINCT FROM digest(concat_ws(':',$4::text,$5::uuid::text,l.standalone_compressed::text,sp.compression_strategy,sp.compression_level::text,sp.compression_sample_level::text,sp.compression_min_savings_percent::text,sp.compression_min_savings_bytes::text,'standalone'),'sha256') OR ((NOT sp.compression_enabled OR sp.compression_strategy='never') AND l.standalone_compressed)) ORDER BY c.id LIMIT 1",
        )
        .bind(after)
        .bind(job.upper_chunk_id)
        .bind(job.storage_id)
        .bind(&job.target_algorithm)
        .bind(job.target_key_id)
        .bind(shards)
        .bind(job.shard)
        .fetch_optional(&self.db)
        .await?;
        let packed_candidate: Option<(i64, i64)> = sqlx::query_as(
            "SELECT min(c.id),p.id FROM chunks c JOIN packs p ON p.id=c.pack_id AND p.state='ready' JOIN storage_profiles sp ON sp.storage_id=c.storage_id WHERE c.storage_id=$1 AND c.state='ready' AND c.id>$2 AND c.id<=$3 AND (p.pack_fingerprint IS DISTINCT FROM digest(concat_ws(':',$4::text,$5::uuid::text,p.pack_compressed::text,p.member_count::text,sp.compression_strategy,sp.compression_level::text,sp.compression_sample_level::text,sp.compression_min_savings_percent::text,sp.compression_min_savings_bytes::text,sp.pack_max_size,sp.pack_range_enabled::text,sp.pack_range_min_downloads::text,sp.pack_range_min_savings_percent::text,sp.pack_range_min_savings_bytes,'pack'),'sha256') OR ((NOT sp.compression_enabled OR sp.compression_strategy='never') AND p.pack_compressed)) GROUP BY p.id HAVING (min(c.id) % $6)=$7 ORDER BY min(c.id) LIMIT 1",
        )
        .bind(job.storage_id)
        .bind(after)
        .bind(job.upper_chunk_id)
        .bind(&job.target_algorithm)
        .bind(job.target_key_id)
        .bind(shards)
        .bind(job.shard)
        .fetch_optional(&self.db)
        .await?;
        let process_loose = row.as_ref().is_some_and(|source| {
            packed_candidate
                .as_ref()
                .is_none_or(|(first_id, _)| source.id < *first_id)
        });
        if process_loose {
            let source = row.expect("rewrite loose candidate disappeared");
            self.pin_pack_inputs(
                id,
                &[crate::worker::packing::Mapping {
                    chunk: source.clone(),
                    offset_bytes: 0,
                    pack_id: None,
                }],
            )
            .await?;
            let raw = self.storage.get_with(&source, None).await?;
            let mut physical = source.clone();
            physical.encoding_id = sqlx::query_scalar(
                "SELECT nextval(pg_get_serial_sequence('chunk_encodings','encoding_id'))",
            )
            .fetch_one(&self.db)
            .await?;
            physical.location_id = Uuid::new_v4();
            physical.algorithm = job.target_algorithm.clone();
            physical.block_key_id = job.target_key_id;
            physical.created_at = chrono::Utc::now();
            physical.nonce = None;
            sqlx::query("INSERT INTO chunk_encodings(encoding_id,chunk_id,location_id,storage_id,standalone_compressed,state,created_at,unreferenced_at,owner_task,encryption_algorithm,block_key_id) VALUES($1,$2,$3,$4,false,'uploading',$5,now(),$6,$7,$8)")
                .bind(physical.encoding_id).bind(source.id).bind(physical.location_id).bind(job.storage_id).bind(physical.created_at).bind(id).bind(&physical.algorithm).bind(physical.block_key_id).execute(&self.db).await?;
            let (physical, encoded, _) = self
                .storage
                .encode(physical, raw.to_vec(), source.compressed)
                .await?;
            self.storage
                .put_path_for(
                    job.storage_id,
                    &self
                        .storage
                        .path_for(job.storage_id, physical.location_id)?,
                    encoded,
                )
                .await?;
            let mut tx = self.db.begin().await?;
            Self::rewrite_lease(&mut tx, id, token).await?;
            self.storage_writable()?;
            let _gate = self.storage.source_gate.write().await;
            let current: Option<Option<i64>> = sqlx::query_scalar(
                "SELECT pack_id FROM chunks WHERE id=$1 AND state='ready' FOR UPDATE",
            )
            .bind(source.id)
            .fetch_optional(&mut *tx)
            .await?;
            ensure!(current == Some(None), "chunk source changed during rewrite");
            let changed = sqlx::query("UPDATE chunk_encodings SET state='retired',unreferenced_at=now() WHERE encoding_id=$1 AND state='ready'")
                .bind(source.encoding_id).execute(&mut *tx).await?.rows_affected();
            ensure!(changed == 1, "source encoding changed during rewrite");
            sqlx::query("UPDATE chunk_encodings SET standalone_size=$2,standalone_compressed=$3,nonce=$4,state='ready',unreferenced_at=NULL,stored_at=clock_timestamp(),owner_task=NULL WHERE encoding_id=$1 AND state='uploading'")
                .bind(physical.encoding_id).bind(physical.stored_size).bind(physical.compressed).bind(&physical.nonce).execute(&mut *tx).await?;
            sqlx::query("UPDATE chunks SET current_encoding_id=$2,standalone_size=$3,standalone_compressed=$4,nonce=$5,encryption_algorithm=$6,block_key_id=$7 WHERE id=$1 AND state='ready'")
                .bind(source.id).bind(physical.encoding_id).bind(physical.stored_size).bind(physical.compressed).bind(&physical.nonce).bind(&physical.algorithm).bind(physical.block_key_id)
                .execute(&mut *tx).await?;
            sqlx::query(
                "UPDATE tasks SET cursor=$2,processed=processed+1,updated_at=now() WHERE id=$1",
            )
            .bind(id)
            .bind(source.id.to_string())
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            return Ok(false);
        }

        if let Some((first_id, pack_id)) = packed_candidate {
            let old: crate::content::pack::Pack =
                sqlx::query_as("SELECT * FROM packs WHERE id=$1 AND state='ready'")
                    .bind(pack_id)
                    .fetch_one(&self.db)
                    .await?;
            let members: Vec<Chunk> = sqlx::query_as(
                "SELECT c.id,c.current_encoding_id AS current_encoding_id,COALESCE(l.location_id,'00000000-0000-0000-0000-000000000000'::uuid) AS location_id,c.storage_id,c.hash,c.raw_size,COALESCE(l.standalone_size,c.standalone_size) AS standalone_size,COALESCE(l.encryption_algorithm,c.encryption_algorithm) AS encryption_algorithm,COALESCE(l.block_key_id,c.block_key_id) AS block_key_id,COALESCE(l.standalone_compressed,c.standalone_compressed) AS standalone_compressed,COALESCE(l.nonce,c.nonce) AS nonce,c.format,c.state,c.created_at FROM pack_members m JOIN chunks c ON c.id=m.chunk_id LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' WHERE m.pack_id=$1 ORDER BY m.ordinal",
            )
            .bind(pack_id)
            .fetch_all(&self.db)
            .await?;
            ensure!(
                members.len() == old.member_count as usize,
                "pack member count changed during rewrite"
            );
            let current_members: Vec<i64> = sqlx::query_scalar("SELECT c.id FROM pack_members m JOIN chunks c ON c.id=m.chunk_id WHERE m.pack_id=$1 AND c.state='ready' ORDER BY m.ordinal")
                .bind(old.id).fetch_all(&self.db).await?;
            let mappings: Vec<_> = members
                .iter()
                .filter(|c| current_members.contains(&c.id))
                .map(|c| crate::worker::packing::Mapping {
                    chunk: c.clone(),
                    offset_bytes: 0,
                    pack_id: Some(old.id),
                })
                .collect();
            self.pin_pack_inputs(id, &mappings).await?;
            let _work = self.storage.pack_work(old.payload_size()?).await?;
            let loaded = self.storage.load_pack(old.clone()).await?;
            let mut encoded_members = Vec::with_capacity(members.len());
            for mut member in members {
                let raw = loaded.data.chunk(&member)?;
                member.algorithm = job.target_algorithm.clone();
                member.block_key_id = job.target_key_id;
                encoded_members.push((member, raw));
            }
            let new_storage_id = Uuid::new_v4();
            let new_pack: crate::content::pack::Pack = sqlx::query_as(
                "INSERT INTO packs(location_id,storage_id,encryption_algorithm,block_key_id,raw_size,member_count,state,unreferenced_at,owner_task) VALUES($1,$2,$3,$4,$5,$6,'preparing',now(),$7) RETURNING *",
            )
            .bind(new_storage_id)
            .bind(job.storage_id)
            .bind(&job.target_algorithm)
            .bind(job.target_key_id)
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
                    strategy,
                    profile.compression_enabled
                        && strategy != crate::content::compression::Strategy::Never,
                )
                .await?;
            self.storage
                .put_path_for(
                    job.storage_id,
                    &self.storage.pack_path_for(job.storage_id, new_storage_id)?,
                    encoded,
                )
                .await?;
            let mut tx = self.db.begin().await?;
            Self::rewrite_lease(&mut tx, id, token).await?;
            self.storage_writable()?;
            let _gate = self.storage.source_gate.write().await;
            let locked: Vec<i64> = sqlx::query_scalar(
                "SELECT id FROM chunks WHERE pack_id=$1 AND state='ready' ORDER BY id FOR UPDATE",
            )
            .bind(old.id)
            .fetch_all(&mut *tx)
            .await?;
            ensure!(
                locked == current_members,
                "pack membership changed during rewrite"
            );
            let updated = sqlx::query("UPDATE packs SET stored_size=$2,pack_compressed=$3,nonce=$4,digest=$5,state='ready',unreferenced_at=NULL,stored_at=clock_timestamp(),owner_task=NULL WHERE id=$1 AND state='preparing'")
                .bind(new_pack.id).bind(new_pack.stored_size).bind(new_pack.compressed).bind(&new_pack.nonce).bind(&new_pack.digest).execute(&mut *tx).await?.rows_affected();
            ensure!(updated == 1, "new pack was changed during rewrite");
            for (ordinal, member) in encoded_members.iter().enumerate() {
                sqlx::query("INSERT INTO pack_members(pack_id,ordinal,chunk_id,offset_bytes) VALUES($1,$2,$3,$4)")
                    .bind(new_pack.id)
                    .bind(ordinal as i32)
                    .bind(member.0.id)
                    .bind(encoded_members[..ordinal].iter().map(|(c, _)| i64::from(c.raw_size)).sum::<i64>())
                    .execute(&mut *tx)
                    .await?;
            }
            let changed =
                sqlx::query("UPDATE chunks SET pack_id=$2 WHERE pack_id=$1 AND state='ready'")
                    .bind(old.id)
                    .bind(new_pack.id)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected();
            ensure!(
                changed == current_members.len() as u64,
                "pack membership changed during rewrite"
            );
            let retired = sqlx::query("UPDATE packs SET state='retired',unreferenced_at=now(),owner_task=NULL WHERE id=$1 AND state='ready'")
                .bind(old.id)
                .execute(&mut *tx)
                .await?
                .rows_affected();
            ensure!(retired == 1, "source pack changed during rewrite");
            let last_id: i64 =
                sqlx::query_scalar("SELECT max(chunk_id) FROM pack_members WHERE pack_id=$1")
                    .bind(new_pack.id)
                    .fetch_one(&mut *tx)
                    .await?;
            sqlx::query(
                "UPDATE tasks SET cursor=$2,processed=processed+1,updated_at=now() WHERE id=$1",
            )
            .bind(id)
            .bind(last_id.max(first_id).to_string())
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            return Ok(false);
        }

        sqlx::query("UPDATE tasks SET cursor=$2,updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$3 AND lease_expires_at>clock_timestamp()")
                .bind(id).bind(after.to_string()).bind(token).execute(&self.db).await?;
        return Ok(true);
    }

    async fn rewrite_lease(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        id: Uuid,
        token: Uuid,
    ) -> Result<()> {
        let owned: Option<Uuid> = sqlx::query_scalar("SELECT id FROM tasks WHERE id=$1 AND state='running' AND lease_token=$2 AND lease_expires_at>clock_timestamp() FOR UPDATE")
            .bind(id).bind(token).fetch_optional(&mut **tx).await?;
        ensure!(owned.is_some(), "rewrite task was paused or lost its lease");
        Ok(())
    }
}
