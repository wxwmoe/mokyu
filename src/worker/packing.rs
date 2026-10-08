use crate::{
    app::App,
    codec::Chunk,
    config,
    pack::{CompressionStrategy, Pack},
    storage::ReadContext,
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use serde_json::{Value, json};
use std::sync::atomic::Ordering;
use uuid::Uuid;

#[derive(Clone, sqlx::FromRow)]
pub(crate) struct Mapping {
    #[sqlx(flatten)]
    pub(crate) chunk: Chunk,
    pub(crate) offset_bytes: i64,
    pub(crate) pack_id: Option<i64>,
}
pub(crate) struct Output {
    pub(crate) chunks: Vec<i64>,
    pub(crate) pack: Option<Pack>,
    pub(crate) independent: Option<Chunk>,
}
impl Output {
    pub(crate) fn size(&self) -> i64 {
        self.pack
            .as_ref()
            .and_then(|p| p.stored_size)
            .or_else(|| {
                self.independent
                    .as_ref()
                    .and_then(|c| c.stored_size.map(i64::from))
            })
            .unwrap_or(0)
    }
}

impl App {
    async fn physical_encoding(&self, c: &Chunk) -> Result<(String, Option<Uuid>)> {
        self.store_encoding(c.storage_id.context("chunk has no store")?)
            .await
    }
    pub(crate) async fn pin_pack_inputs(&self, task: Uuid, rows: &[Mapping]) -> Result<()> {
        let _coord = self.coord.lock().await;
        self.storage_writable()?;
        let mut ids: Vec<i64> = rows.iter().map(|r| r.chunk.id).collect();
        ids.sort_unstable();
        ids.dedup();
        let mut tx = self.db.begin().await?;
        ensure!(
            sqlx::query_scalar::<_, bool>(
                "SELECT state='running' FROM tasks WHERE id=$1 FOR UPDATE"
            )
            .bind(task)
            .fetch_one(&mut *tx)
            .await?,
            "physical task was paused"
        );
        let ready: Vec<i64> = sqlx::query_scalar(
            "SELECT id FROM chunks WHERE id=ANY($1) AND state='ready' ORDER BY id FOR UPDATE",
        )
        .bind(&ids)
        .fetch_all(&mut *tx)
        .await?;
        ensure!(ready == ids, "pack inputs changed; retry maintenance");
        sqlx::query("INSERT INTO pack_inputs(task_id,chunk_id) SELECT $1,unnest($2::bigint[]) ON CONFLICT(task_id,chunk_id) DO NOTHING").bind(task).bind(&ids).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn finish_pack_work(&self, task: Uuid) -> Result<()> {
        let mut tx = self.db.begin().await?;
        sqlx::query("UPDATE packs SET state='retired',unreferenced_at=now() WHERE owner_task=$1 AND state='preparing'").bind(task).execute(&mut *tx).await?;
        sqlx::query("UPDATE chunk_encodings SET state='retired',unreferenced_at=now() WHERE owner_task=$1 AND state='uploading'").bind(task).execute(&mut *tx).await?;
        sqlx::query("DELETE FROM pack_inputs WHERE task_id=$1")
            .bind(task)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn pack_status(&self) -> Result<Value> {
        let rows:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('state',state,'count',count(*),'raw_bytes',COALESCE(sum(raw_size),0),'stored_bytes',COALESCE(sum(stored_size),0)) FROM packs GROUP BY state").fetch_all(&self.db).await?;
        Ok(
            json!({"enabled":self.pack_creation_allowed(),"range_optimization":self.config.local_pack.range_optimization,"packs":rows,"max_size":self.config.local_pack.max_size}),
        )
    }
    pub async fn pack_start(&self, kind: &str) -> Result<Value> {
        self.start_pack(kind, false).await
    }
    pub(crate) async fn start_pack(&self, kind: &str, automatic: bool) -> Result<Value> {
        self.writable()?;
        if !["pack", "reuse", "reclaim", "repack", "range"].contains(&kind) {
            return Err(s3s::s3_error!(InvalidArgument, "unknown pack maintenance kind").into());
        }
        if kind == "range" && !self.config.local_pack.range_optimization {
            return Err(s3s::s3_error!(OperationAborted, "Range optimization is disabled").into());
        }
        if matches!(kind, "pack" | "repack") && !self.pack_creation_allowed() {
            return Err(s3s::s3_error!(OperationAborted, "pack creation is disabled").into());
        }
        let _coord = self.coord.lock().await;
        self.writable()?;
        if self.maintenance_paused(kind).await?
            || (matches!(kind, "pack" | "repack") && !self.pack_creation_allowed())
        {
            return Err(s3s::s3_error!(OperationAborted, "maintenance policy is paused").into());
        }
        let mut tx = self.db.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!("mokyu:pack:{kind}"))
            .execute(&mut *tx)
            .await?;
        let existing:Option<Uuid>=sqlx::query_scalar("SELECT id FROM tasks WHERE task_type='pack' AND detail->>'kind'=$1 AND state IN ('queued','running') LIMIT 1")
            .bind(kind).fetch_optional(&mut *tx).await?;
        if let Some(id) = existing {
            tx.commit().await?;
            return Ok(json!({"task_id":id,"existing":true}));
        }
        if automatic && let Some(id)=sqlx::query_scalar::<_,Uuid>("SELECT id FROM tasks WHERE task_type='pack' AND detail->>'kind'=$1 AND state='failed' AND updated_at>now()-$2*interval '1 second' ORDER BY updated_at DESC LIMIT 1").bind(kind).bind(config::seconds(&self.config.local_pack.reuse_interval)? as f64).fetch_optional(&mut *tx).await? {tx.commit().await?; return Ok(json!({"task_id":id,"retry_pending":true}));}
        let detail = json!({"kind":kind,"upper_time":chrono::Utc::now(),"automatic":automatic});
        let mut parent_detail = detail.clone();
        parent_detail["role"] = json!("parent");
        let id = crate::worker::insert(&mut *tx, "pack", None, parent_detail).await?;
        let storages: Vec<(Uuid, String, i64)> = sqlx::query_as(
            "SELECT s.id,s.type,sp.revision FROM storages s JOIN storage_profiles sp ON sp.storage_id=s.id WHERE s.enabled ORDER BY s.created_at,s.id",
        )
        .fetch_all(&mut *tx)
        .await?;
        for (storage_id, storage_type, revision) in &storages {
            let mut child_detail = detail.clone();
            child_detail["role"] = json!("child");
            child_detail["storage_id"] = json!(storage_id);
            child_detail["profile_revision"] = json!(revision);
            let child = crate::worker::insert_child(&mut *tx, "pack", None, child_detail).await?;
            let (scope, node) = if storage_type == "local" {
                ("node", self.node.get().map(|state| state.node_id))
            } else {
                ("cluster", None)
            };
            sqlx::query("UPDATE tasks SET parent_task_id=$2,batch_total=1,execution_scope=$3,target_node_id=$4 WHERE id=$1")
                .bind(child)
                .bind(id)
                .bind(scope)
                .bind(node)
                .execute(&mut *tx)
                .await?;
        }
        sqlx::query("UPDATE tasks SET batch_total=$2,state=CASE WHEN $2=0 THEN 'completed' ELSE state END WHERE id=$1")
            .bind(id)
            .bind(storages.len() as i64)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        self.wake_tasks.notify_one();
        Ok(json!({"task_id":id}))
    }
    pub async fn unpack_start(&self, pack: Option<i64>, all: bool, execute: bool) -> Result<Value> {
        self.unpack_checked(pack, all, execute, None).await
    }
    pub(crate) async fn unpack_checked(
        &self,
        pack: Option<i64>,
        all: bool,
        execute: bool,
        expected: Option<&str>,
    ) -> Result<Value> {
        let _coord = self.coord.lock().await;
        if all == pack.is_some() || pack.is_some_and(|id| id <= 0) {
            return Err(
                s3s::s3_error!(InvalidArgument, "specify one positive pack ID or --all").into(),
            );
        }
        if all && self.pack_creation_allowed() {
            return Err(s3s::s3_error!(
                OperationAborted,
                "stop new pack creation before unpacking all packs"
            )
            .into());
        }
        if let Some(id) = pack {
            ensure!(id > 0, "pack ID must be positive");
        }
        let (count,bytes):(i64,i64)=sqlx::query_as("SELECT count(*),COALESCE(sum(raw_size),0)::bigint FROM packs WHERE state='ready' AND ($1::bigint IS NULL OR id=$1)")
            .bind(pack).fetch_one(&self.db).await?;
        let (upper,ids):(String,String)=sqlx::query_as("SELECT coalesce(max(id),0)::text,coalesce(sum(id),0)::text FROM packs WHERE state='ready' AND ($1::bigint IS NULL OR id=$1)").bind(pack).fetch_one(&self.db).await?;
        let preview = json!({"preview":true,"packs":count,"raw_bytes":bytes,"upper_id":upper,"identity_sum":ids,"effect":"write independent chunk sources; old packs retain GC grace"});
        if !execute {
            return Ok(preview);
        }
        if let Some(expected) = expected
            && expected
                != blake3::hash(&serde_json::to_vec(&preview)?)
                    .to_hex()
                    .as_str()
        {
            return Err(s3s::s3_error!(PreconditionFailed).into());
        }
        self.writable()?;
        let preparing: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM packs WHERE state='preparing')")
                .fetch_one(&self.db)
                .await?;
        if all && preparing {
            return Err(s3s::s3_error!(
                OperationAborted,
                "wait for current pack generation to drain"
            )
            .into());
        }
        if let Some(id)=sqlx::query_scalar::<_,Uuid>("SELECT id FROM tasks WHERE task_type='unpack' AND state IN ('queued','running','paused','failed') AND detail->>'all'=$1 AND detail->>'pack_id' IS NOT DISTINCT FROM $2 LIMIT 1").bind(all.to_string()).bind(pack.map(|v|v.to_string())).fetch_optional(&self.db).await? { return Ok(json!({"task_id":id,"existing":true})); }
        let mut tx = self.db.begin().await?;
        let detail = json!({"pack_id":pack,"all":all});
        let mut parent_detail = detail.clone();
        parent_detail["role"] = json!("parent");
        let id = crate::worker::insert(&mut *tx, "unpack", None, parent_detail).await?;
        let storages: Vec<(Uuid, String, i64)> = if let Some(pack_id) = pack {
            sqlx::query_as("SELECT s.id,s.type,sp.revision FROM storages s JOIN storage_profiles sp ON sp.storage_id=s.id JOIN packs p ON p.storage_id=s.id WHERE p.id=$1")
                .bind(pack_id)
                .fetch_all(&mut *tx)
                .await?
        } else {
            sqlx::query_as("SELECT s.id,s.type,sp.revision FROM storages s JOIN storage_profiles sp ON sp.storage_id=s.id WHERE s.enabled ORDER BY s.created_at,s.id")
                .fetch_all(&mut *tx)
                .await?
        };
        for (storage_id, storage_type, revision) in &storages {
            let mut child_detail = detail.clone();
            child_detail["role"] = json!("child");
            child_detail["storage_id"] = json!(storage_id);
            child_detail["profile_revision"] = json!(revision);
            let child = crate::worker::insert_child(&mut *tx, "unpack", None, child_detail).await?;
            let (scope, node) = if storage_type == "local" {
                ("node", self.node.get().map(|state| state.node_id))
            } else {
                ("cluster", None)
            };
            sqlx::query("UPDATE tasks SET parent_task_id=$2,batch_total=1,execution_scope=$3,target_node_id=$4 WHERE id=$1")
                .bind(child)
                .bind(id)
                .bind(scope)
                .bind(node)
                .execute(&mut *tx)
                .await?;
        }
        sqlx::query("UPDATE tasks SET batch_total=$2,state=CASE WHEN $2=0 THEN 'completed' ELSE state END WHERE id=$1")
            .bind(id)
            .bind(storages.len() as i64)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        self.wake_tasks.notify_one();
        Ok(json!({"task_id":id}))
    }
    pub(crate) fn same_references(
        a: i64,
        b: i64,
        distance: i64,
    ) -> sqlx::query::QueryScalar<'static, sqlx::Postgres, bool, sqlx::postgres::PgArguments> {
        sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM extents a JOIN chunks c ON c.id=a.chunk_id WHERE a.chunk_id=$1 AND (a.source_offset<>0 OR a.length<>c.raw_size OR NOT EXISTS(SELECT 1 FROM extents b JOIN chunks d ON d.id=b.chunk_id WHERE b.chunk_id=$2 AND b.stream_id=a.stream_id AND b.offset_bytes=a.offset_bytes+$3 AND b.source_offset=0 AND b.length=d.raw_size))) AND NOT EXISTS(SELECT 1 FROM extents b JOIN chunks c ON c.id=b.chunk_id WHERE b.chunk_id=$2 AND (b.source_offset<>0 OR b.length<>c.raw_size OR NOT EXISTS(SELECT 1 FROM extents a JOIN chunks d ON d.id=a.chunk_id WHERE a.chunk_id=$1 AND a.stream_id=b.stream_id AND a.offset_bytes=b.offset_bytes-$3 AND a.source_offset=0 AND a.length=d.raw_size)))")
            .bind(a).bind(b).bind(distance)
    }
    async fn independent_output(&self, task: Uuid, c: &Chunk, raw: Bytes) -> Result<Output> {
        if let Some((id, size)) = sqlx::query_as::<_, (i64, i32)>(
            "SELECT encoding_id,standalone_size FROM chunk_encodings WHERE chunk_id=$1 AND state='ready'",
        )
        .bind(c.id)
        .fetch_optional(&self.db)
        .await?
        {
            let mut physical = c.clone();
            physical.encoding_id = id;
            physical.stored_size = Some(size);
            return Ok(Output {
                chunks: vec![c.id],
                pack: None,
                independent: Some(physical),
            });
        }
        let mut physical = c.clone();
        (physical.algorithm, physical.block_key_id) = self.physical_encoding(c).await?;
        physical.encoding_id = sqlx::query_scalar(
            "SELECT nextval(pg_get_serial_sequence('chunk_encodings','encoding_id'))",
        )
        .fetch_one(&self.db)
        .await?;
        physical.location_id = Uuid::new_v4();
        physical.created_at = chrono::Utc::now();
        physical.nonce = None;
        // A newly encoded physical copy always consumes a fresh persisted identity.
        sqlx::query("INSERT INTO chunk_encodings(encoding_id,chunk_id,location_id,storage_id,standalone_compressed,state,created_at,unreferenced_at,owner_task,encryption_algorithm,block_key_id) VALUES($1,$2,$3,$4,false,'uploading',$5,now(),$6,$7,$8)")
            .bind(physical.encoding_id).bind(c.id).bind(physical.location_id).bind(c.storage_id).bind(physical.created_at).bind(task).bind(&physical.algorithm).bind(physical.block_key_id).execute(&self.db).await?;
        let (physical, encoded, _) = self
            .storage
            .encode(physical, raw.to_vec(), c.compressed)
            .await?;
        sqlx::query("UPDATE chunk_encodings SET standalone_size=$2,standalone_compressed=$3,nonce=$4 WHERE encoding_id=$1")
            .bind(physical.encoding_id)
            .bind(physical.stored_size)
            .bind(physical.compressed)
            .bind(&physical.nonce)
            .execute(&self.db)
            .await?;
        let storage_id = physical.storage_id.context("chunk has no store")?;
        self.storage
            .put_path_for(
                storage_id,
                &self.storage.path_for(storage_id, physical.location_id)?,
                encoded,
            )
            .await?;
        Ok(Output {
            chunks: vec![c.id],
            pack: None,
            independent: Some(physical),
        })
    }
    pub(crate) async fn pack_output(
        &self,
        task: Uuid,
        rows: &[Mapping],
        context: &ReadContext,
        content_type: Option<&str>,
        key: &str,
        allow_pack: bool,
    ) -> Result<Vec<Output>> {
        let raw_size: usize = rows.iter().map(|r| r.chunk.raw_size as usize).sum();
        let _memory = self
            .storage
            .pack_work(raw_size + 12 + rows.len() * 44)
            .await?;
        let mut data = Vec::with_capacity(rows.len());
        for row in rows {
            let raw = self.storage.get_with(&row.chunk, Some(context)).await?;
            data.push((row.chunk.clone(), Bytes::copy_from_slice(&raw)));
        }
        if rows.len() == 1 || !allow_pack || !self.pack_creation_allowed() {
            let mut outputs = Vec::new();
            for (c, raw) in data {
                outputs.push(self.independent_output(task, &c, raw).await?);
            }
            return Ok(outputs);
        }
        if let Some(output) = self
            .try_pack_output(task, rows, &data, content_type, key)
            .await?
        {
            return Ok(vec![output]);
        }
        // At most one bisection, on a complete CDC boundary.
        let mid = rows.len() / 2;
        let mut outputs = Vec::new();
        for (rows, data) in [(&rows[..mid], &data[..mid]), (&rows[mid..], &data[mid..])] {
            if rows.len() > 1
                && let Some(output) = self
                    .try_pack_output(task, rows, data, content_type, key)
                    .await?
            {
                outputs.push(output);
                continue;
            }
            for (c, raw) in data {
                outputs.push(self.independent_output(task, c, raw.clone()).await?);
            }
        }
        Ok(outputs)
    }
    async fn try_pack_output(
        &self,
        task: Uuid,
        rows: &[Mapping],
        data: &[(Chunk, Bytes)],
        content_type: Option<&str>,
        key: &str,
    ) -> Result<Option<Output>> {
        let coord = self.coord.lock().await;
        if !self.pack_creation_allowed() {
            return Ok(None);
        }
        let raw_size: i64 = rows.iter().map(|r| i64::from(r.chunk.raw_size)).sum();
        let first = &rows[0].chunk;
        let (algorithm, block_key_id) = self.physical_encoding(first).await?;
        let storage_id = first.storage_id.context("pack member has no store")?;
        if !self.storage.pack_enabled(storage_id) {
            return Ok(None);
        }
        ensure!(
            rows.iter()
                .all(|row| row.chunk.storage_id == Some(storage_id)),
            "pack members must use one store"
        );
        let _pack_permit = self.storage.pack_permit(storage_id).await?;
        let p:Pack=sqlx::query_as("INSERT INTO packs(location_id,storage_id,encryption_algorithm,block_key_id,raw_size,member_count,state,unreferenced_at,owner_task) VALUES($1,$2,$3,$4,$5,$6,'preparing',now(),$7) RETURNING *")
            .bind(Uuid::new_v4()).bind(storage_id).bind(&algorithm).bind(block_key_id).bind(raw_size).bind(rows.len() as i32).bind(task).fetch_one(&self.db).await?;
        drop(coord);
        let strategy = match self.config.local_pack.compression_strategy {
            None => self.config.local_compression.strategy,
            Some(CompressionStrategy::Never) => crate::compression::Strategy::Never,
            Some(CompressionStrategy::Always) => crate::compression::Strategy::Always,
            Some(CompressionStrategy::Sample) => crate::compression::Strategy::Sample,
        };
        let mut compression = self.config.local_compression.clone();
        compression.strategy = strategy;
        let should_try = compression.should_try(content_type, key);
        let (p, encoded) = self
            .storage
            .encode_pack(p, data.to_vec(), strategy, should_try)
            .await?;
        let old: i64 = rows
            .iter()
            .map(|r| r.chunk.stored_size.unwrap_or(r.chunk.raw_size) as i64)
            .sum();
        if p.stored_size.unwrap_or(i64::MAX) > old + 12 + rows.len() as i64 * 44 {
            sqlx::query("UPDATE packs SET state='retired' WHERE id=$1")
                .bind(p.id)
                .execute(&self.db)
                .await?;
            return Ok(None);
        }
        sqlx::query(
            "UPDATE packs SET stored_size=$2,pack_compressed=$3,nonce=$4,digest=$5 WHERE id=$1",
        )
        .bind(p.id)
        .bind(p.stored_size)
        .bind(p.compressed)
        .bind(&p.nonce)
        .bind(&p.digest)
        .execute(&self.db)
        .await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("pack-prepared").await;
        let storage_id = p.storage_id.context("pack has no store")?;
        self.storage
            .put_path_for(
                storage_id,
                &self.storage.pack_path_for(storage_id, p.location_id)?,
                encoded,
            )
            .await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("pack-stored").await;
        let mut tx = self.db.begin().await?;
        let mut offset = 0i64;
        for (ordinal, row) in rows.iter().enumerate() {
            sqlx::query("INSERT INTO pack_members(pack_id,ordinal,chunk_id,offset_bytes) VALUES($1,$2,$3,$4)").bind(p.id).bind(ordinal as i32).bind(row.chunk.id).bind(offset).execute(&mut *tx).await?;
            offset += i64::from(row.chunk.raw_size);
        }
        tx.commit().await?;
        Ok(Some(Output {
            chunks: rows.iter().map(|r| r.chunk.id).collect(),
            pack: Some(p),
            independent: None,
        }))
    }
    pub(crate) async fn commit_outputs(
        &self,
        task: Uuid,
        rows: &[Mapping],
        outputs: &[Output],
        split: bool,
    ) -> Result<()> {
        #[cfg(feature = "fault-injection")]
        crate::faults::point("pack-before-publish").await;
        let _coord = self.coord.lock().await;
        self.storage_writable()?;
        anyhow::ensure!(
            self.pack_creation_allowed() || outputs.iter().all(|o| o.pack.is_none()),
            "new pack creation was stopped; retry as chunks"
        );
        let _sources = self.storage.source_gate.write().await;
        let mut ids: Vec<i64> = rows.iter().map(|r| r.chunk.id).collect();
        ids.sort_unstable();
        let mut tx = self.db.begin().await?;
        ensure!(
            sqlx::query_scalar::<_, bool>(
                "SELECT state='running' FROM tasks WHERE id=$1 FOR UPDATE"
            )
            .bind(task)
            .fetch_one(&mut *tx)
            .await?,
            "pack task was paused"
        );
        let current:Vec<(i64,Option<i64>)>=sqlx::query_as("SELECT id,pack_id FROM chunks WHERE id=ANY($1) AND state='ready' ORDER BY id FOR UPDATE").bind(&ids).fetch_all(&mut *tx).await?;
        ensure!(
            current.len() == rows.len()
                && rows.iter().all(|r| current
                    .iter()
                    .any(|(id, p)| *id == r.chunk.id && *p == r.pack_id)),
            "pack layout changed; retry maintenance"
        );
        let before:i64=sqlx::query_scalar("SELECT COALESCE(sum(bytes),0)::bigint FROM (SELECT standalone_size bytes FROM chunk_encodings WHERE chunk_id=ANY($1) AND state='ready' UNION ALL SELECT stored_size FROM packs WHERE id IN (SELECT pack_id FROM chunks WHERE id=ANY($1))) sources").bind(&ids).fetch_one(&mut *tx).await?;
        let added:i64=sqlx::query_scalar("SELECT COALESCE(sum(bytes),0)::bigint FROM (SELECT stored_size bytes FROM packs WHERE owner_task=$1 AND state='preparing' UNION ALL SELECT standalone_size FROM chunk_encodings WHERE owner_task=$1 AND state='uploading') sources").bind(task).fetch_one(&mut *tx).await?;
        let after: i64 = outputs
            .iter()
            .map(|o| {
                o.pack
                    .as_ref()
                    .and_then(|p| p.stored_size)
                    .or_else(|| {
                        o.independent
                            .as_ref()
                            .and_then(|c| c.stored_size.map(i64::from))
                    })
                    .unwrap_or(0)
            })
            .sum();
        let upload: bool = sqlx::query_scalar(
            "SELECT task_type IN ('upload','cache_flush') FROM tasks WHERE id=$1",
        )
        .bind(task)
        .fetch_one(&mut *tx)
        .await?;
        let range: bool = sqlx::query_scalar(
            "SELECT task_type='pack' AND detail->>'kind'='range' FROM tasks WHERE id=$1",
        )
        .bind(task)
        .fetch_one(&mut *tx)
        .await?;
        if !split && !upload {
            let exclusive:bool=sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM chunks c CROSS JOIN mokyu_meta g WHERE c.id=ANY($1) AND ((SELECT count(*) FROM extents WHERE chunk_id=c.id)<>1 OR NOT COALESCE(c.repack_after<=now(),true) OR (c.range_split_at IS NOT NULL AND (g.access_coverage_since IS NULL OR g.access_coverage_since>date_trunc('hour',now()-$3*interval '1 second') OR g.access_flushed_at IS NULL OR g.access_flushed_at<now()-$4*interval '1 second' OR EXISTS(SELECT 1 FROM chunk_access_windows w WHERE w.chunk_id=c.id AND w.range_reads>0 AND w.window_start>=date_trunc('hour',now()-$3*interval '1 second')))) OR (c.split_at IS NOT NULL AND c.reference_changed_at>now()-$2*interval '1 second')))").bind(&ids).bind(config::seconds(&self.config.local_pack.repack_cooldown)? as f64).bind(config::seconds(&self.config.local_pack.range_repack_window)? as f64).bind((2*config::seconds(&self.config.metrics.access_flush_interval)?) as f64).fetch_one(&mut *tx).await?;
            ensure!(exclusive, "pack references changed; retry maintenance");
        }
        for output in outputs.iter().filter(|o| o.pack.is_some()) {
            for pair in output.chunks.windows(2) {
                let a = rows
                    .iter()
                    .find(|r| r.chunk.id == pair[0])
                    .context("missing pack source")?;
                let b = rows
                    .iter()
                    .find(|r| r.chunk.id == pair[1])
                    .context("missing pack source")?;
                ensure!(
                    Self::same_references(pair[0], pair[1], b.offset_bytes - a.offset_bytes)
                        .fetch_one(&mut *tx)
                        .await?,
                    "pack reference boundaries changed; retry maintenance"
                );
            }
        }
        for output in outputs {
            if let Some(p) = &output.pack {
                sqlx::query("UPDATE packs SET state='ready',unreferenced_at=NULL,stored_at=clock_timestamp() WHERE id=$1 AND state='preparing'").bind(p.id).execute(&mut *tx).await?;
                sqlx::query("UPDATE chunk_encodings SET state='retired',unreferenced_at=now() WHERE chunk_id=ANY($1) AND state='ready'").bind(&output.chunks).execute(&mut *tx).await?;
            } else if let Some(c) = &output.independent {
                sqlx::query("UPDATE chunk_encodings SET state='ready',unreferenced_at=NULL,stored_at=clock_timestamp() WHERE encoding_id=$1 AND state='uploading'").bind(c.encoding_id).execute(&mut *tx).await?;
                sqlx::query("UPDATE chunks SET current_encoding_id=$2,standalone_size=$3,standalone_compressed=$4,nonce=$5,encryption_algorithm=$6,block_key_id=$7 WHERE id=$1 AND state='ready'")
                    .bind(c.id).bind(c.encoding_id).bind(c.stored_size).bind(c.compressed).bind(&c.nonce).bind(&c.algorithm).bind(c.block_key_id)
                    .execute(&mut *tx).await?;
            }
            sqlx::query("UPDATE chunks SET pack_id=$2,split_at=CASE WHEN $3 THEN now() ELSE split_at END,repack_after=CASE WHEN $3 THEN GREATEST(repack_after,now()+$4*interval '1 second') ELSE repack_after END,range_split_at=CASE WHEN $5 THEN now() WHEN NOT $3 AND $2::bigint IS NOT NULL THEN NULL ELSE range_split_at END WHERE id=ANY($1)")
                .bind(&output.chunks).bind(output.pack.as_ref().map(|p|p.id)).bind(split).bind(config::seconds(if range {&self.config.local_pack.range_repack_after}else{&self.config.local_pack.repack_cooldown})? as f64).bind(range).execute(&mut *tx).await?;
        }
        let retained: Vec<i64> = outputs
            .iter()
            .flat_map(|o| o.chunks.iter().copied())
            .collect();
        let dead: Vec<i64> = ids
            .iter()
            .filter(|id| !retained.contains(id))
            .copied()
            .collect();
        if !dead.is_empty() {
            ensure!(
                !sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS(SELECT 1 FROM extents WHERE chunk_id=ANY($1))"
                )
                .bind(&dead)
                .fetch_one(&mut *tx)
                .await?,
                "pack acquired a new reference; retry maintenance"
            );
            sqlx::query("UPDATE chunks SET state='failed',pack_id=NULL,unreferenced_at=COALESCE(unreferenced_at,now()) WHERE id=ANY($1)").bind(&dead).execute(&mut *tx).await?;
        }
        if split {
            sqlx::query("INSERT INTO pack_maintenance(stream_id,reason,next_check_at) SELECT DISTINCT e.stream_id,'repack',now()+$2*interval '1 second' FROM extents e JOIN streams s ON s.id=e.stream_id WHERE e.chunk_id=ANY($1) AND s.state='ready' ON CONFLICT(stream_id) DO UPDATE SET generation=pack_maintenance.generation+1,cursor=0,reason='repack',updated_at=now(),next_check_at=EXCLUDED.next_check_at")
                .bind(&retained).bind(config::seconds(&self.config.local_pack.repack_cooldown)? as f64).execute(&mut *tx).await?;
        }
        let old: Vec<i64> = rows.iter().filter_map(|r| r.pack_id).collect();
        sqlx::query("UPDATE packs p SET state='retired',unreferenced_at=now() WHERE id=ANY($1) AND state='ready' AND NOT EXISTS(SELECT 1 FROM chunks WHERE pack_id=p.id AND state='ready')").bind(old).execute(&mut *tx).await?;
        sqlx::query("UPDATE tasks SET detail=detail||jsonb_build_object('last_rewrite',$2::jsonb) WHERE id=$1").bind(task).bind(json!({"before_bytes":before,"output_bytes":after,"temporary_added_bytes":added,"transition_bytes":before+added})).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
    pub(crate) async fn rewrite_pack(&self, task: Uuid, id: i64, unpack: bool) -> Result<usize> {
        self.storage_writable()?;
        if !unpack
            && crate::storage::resource_gate::PRIORITY
                .try_with(|p| *p)
                .unwrap_or(crate::storage::resource_gate::FOREGROUND)
                != crate::storage::resource_gate::UPLOAD
            && sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM pending_uploads WHERE source_pack_id=$1)",
            )
            .bind(id)
            .fetch_one(&self.db)
            .await?
        {
            return Ok(0);
        }
        let rows:Vec<Mapping>=sqlx::query_as("SELECT c.id,c.current_encoding_id AS current_encoding_id,COALESCE(l.location_id,'00000000-0000-0000-0000-000000000000'::uuid) AS location_id,c.storage_id,c.hash,c.raw_size,COALESCE(l.standalone_size,c.standalone_size) AS standalone_size,COALESCE(l.encryption_algorithm,c.encryption_algorithm) AS encryption_algorithm,COALESCE(l.block_key_id,c.block_key_id) AS block_key_id,COALESCE(l.standalone_compressed,c.standalone_compressed) AS standalone_compressed,COALESCE(l.nonce,c.nonce) AS nonce,c.format,c.state,c.created_at,m.offset_bytes,c.pack_id FROM pack_members m JOIN chunks c ON c.id=m.chunk_id LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' WHERE m.pack_id=$1 AND c.pack_id=$1 AND c.state='ready' ORDER BY m.ordinal LIMIT 4096")
            .bind(id).fetch_all(&self.db).await?;
        if rows.is_empty() {
            return Ok(0);
        }
        let writing:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM extents e JOIN chunks c ON c.id=e.chunk_id LEFT JOIN chunk_encodings l ON l.chunk_id=c.id AND l.state='ready' JOIN streams s ON s.id=e.stream_id WHERE c.pack_id=$1 AND s.state='writing')").bind(id).fetch_one(&self.db).await?;
        if writing
            && !unpack
            && crate::storage::resource_gate::PRIORITY
                .try_with(|p| *p)
                .unwrap_or(crate::storage::resource_gate::FOREGROUND)
                != crate::storage::resource_gate::UPLOAD
        {
            return Ok(0);
        }
        let mut groups: Vec<Vec<Mapping>> = Vec::new();
        let total: i64 =
            sqlx::query_scalar("SELECT raw_size FROM packs WHERE id=$1 AND state='ready'")
                .bind(id)
                .fetch_one(&self.db)
                .await?;
        // Logical GC may already have retired unreferenced members; the payload still contains them.
        let mut dead_bytes =
            total.saturating_sub(rows.iter().map(|r| i64::from(r.chunk.raw_size)).sum()) as u64;
        let mut reference_split = false;
        for row in &rows {
            let alive: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM extents WHERE chunk_id=$1)")
                    .bind(row.chunk.id)
                    .fetch_one(&self.db)
                    .await?;
            if !alive {
                dead_bytes += row.chunk.raw_size as u64;
                continue;
            }
            if let Some(last) = groups.last().and_then(|g| g.last()) {
                reference_split |= !Self::same_references(
                    last.chunk.id,
                    row.chunk.id,
                    row.offset_bytes - last.offset_bytes,
                )
                .fetch_one(&self.db)
                .await?;
            }
            let join = if let Some(last) = groups.last().and_then(|g| g.last()) {
                !unpack
                    && last.offset_bytes + i64::from(last.chunk.raw_size) == row.offset_bytes
                    && Self::same_references(
                        last.chunk.id,
                        row.chunk.id,
                        row.offset_bytes - last.offset_bytes,
                    )
                    .fetch_one(&self.db)
                    .await?
                    && groups
                        .last()
                        .unwrap()
                        .iter()
                        .map(|r| r.chunk.raw_size as u64)
                        .sum::<u64>()
                        + row.chunk.raw_size as u64
                        <= config::bytes(&self.config.local_pack.max_size)?
            } else {
                false
            };
            if join {
                groups.last_mut().unwrap().push(row.clone());
            } else {
                groups.push(vec![row.clone()]);
            }
        }
        if (groups.is_empty() && !unpack)
            || (!unpack && dead_bytes == 0 && groups.len() == 1 && groups[0].len() == rows.len())
        {
            return Ok(0);
        }
        let reclaim_only = !unpack && !reference_split && dead_bytes > 0;
        let minimum = config::bytes(&self.config.local_pack.reclaim_min_savings_bytes)?;
        if reclaim_only && dead_bytes < minimum {
            return Ok(0);
        }
        self.pin_pack_inputs(task, &rows).await?;
        let context = ReadContext::default();
        let mut outputs = Vec::new();
        for group in &groups {
            outputs.extend(
                self.pack_output(
                    task,
                    group,
                    &context,
                    None,
                    "",
                    self.pack_creation_allowed() && !unpack,
                )
                .await?,
            );
        }
        if reclaim_only {
            let before: i64 = sqlx::query_scalar("SELECT stored_size FROM packs WHERE id=$1")
                .bind(id)
                .fetch_one(&self.db)
                .await?;
            let after: i64 = outputs
                .iter()
                .map(|o| {
                    o.pack
                        .as_ref()
                        .and_then(|p| p.stored_size)
                        .or_else(|| {
                            o.independent
                                .as_ref()
                                .and_then(|c| c.stored_size.map(i64::from))
                        })
                        .unwrap_or(0)
                })
                .sum();
            if before.saturating_sub(after) < minimum as i64 {
                return Ok(0);
            }
        }
        self.commit_outputs(task, &rows, &outputs, true).await?;
        Ok(groups.iter().map(Vec::len).sum())
    }
    async fn pack_stream(
        &self,
        task: Uuid,
        stream: Uuid,
        from: i64,
        repack: bool,
        byte_budget: Option<u64>,
    ) -> Result<(i64, usize)> {
        self.defer_range_repack(stream, from).await?;
        #[derive(sqlx::FromRow)]
        struct Candidate {
            #[sqlx(flatten)]
            row: Mapping,
            eligible: bool,
            unit_count: i64,
        }
        let rows:Vec<Candidate>=sqlx::query_as("SELECT c.id,c.current_encoding_id AS current_encoding_id,COALESCE(l.location_id,'00000000-0000-0000-0000-000000000000'::uuid) AS location_id,c.storage_id,c.hash,c.raw_size,COALESCE(l.standalone_size,c.standalone_size) AS standalone_size,COALESCE(l.encryption_algorithm,c.encryption_algorithm) AS encryption_algorithm,COALESCE(l.block_key_id,c.block_key_id) AS block_key_id,COALESCE(l.standalone_compressed,c.standalone_compressed) AS standalone_compressed,COALESCE(l.nonce,c.nonce) AS nonce,c.format,c.state,c.created_at,e.offset_bytes,c.pack_id,(NOT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=c.id) AND e.source_offset=0 AND e.length=c.raw_size AND (SELECT count(*) FROM extents WHERE chunk_id=c.id)=1 AND COALESCE(c.repack_after<=now(),true) AND (c.split_at IS NULL OR c.reference_changed_at<=now()-$3*interval '1 second')) eligible,CASE WHEN c.pack_id IS NULL THEN 1 ELSE (SELECT count(*) FROM chunks WHERE pack_id=c.pack_id AND state='ready') END unit_count FROM extents e JOIN chunks c ON c.id=e.chunk_id LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' JOIN streams s ON s.id=e.stream_id WHERE e.stream_id=$1 AND e.offset_bytes>=$2 AND s.state='ready' AND c.state='ready' AND EXISTS(SELECT 1 FROM objects WHERE stream_id=s.id) ORDER BY e.offset_bytes LIMIT 1026")
            .bind(stream).bind(from).bind(config::seconds(&self.config.local_pack.repack_cooldown)? as f64).fetch_all(&self.db).await?;
        let Some(first) = rows.first() else {
            return Ok((-1, 0));
        };
        let mut group = Vec::new();
        let mut size = 0u64;
        let mut cursor = first.row.offset_bytes;
        let mut rows = rows.into_iter().peekable();
        while let Some(first) = rows.next() {
            let mut unit = vec![first];
            if let Some(pack) = unit[0].row.pack_id {
                while rows.peek().is_some_and(|r| r.row.pack_id == Some(pack)) {
                    unit.push(rows.next().unwrap());
                }
            }
            let head = &unit[0];
            let end = unit.last().unwrap().row.offset_bytes
                + i64::from(unit.last().unwrap().row.chunk.raw_size);
            let length: u64 = unit.iter().map(|r| r.row.chunk.raw_size as u64).sum();
            let valid = unit.len() == head.unit_count as usize
                && unit.iter().all(|r| r.eligible)
                && (repack || head.row.pack_id.is_none())
                && unit.windows(2).all(|r| {
                    r[0].row.offset_bytes + i64::from(r[0].row.chunk.raw_size)
                        == r[1].row.offset_bytes
                });
            if !valid
                || head.row.offset_bytes != cursor
                || group.first().is_some_and(|r: &Mapping| {
                    r.chunk.algorithm != head.row.chunk.algorithm
                        || r.chunk.block_key_id != head.row.chunk.block_key_id
                })
                || size + length
                    > config::bytes(&self.config.local_pack.max_size)?
                        .min(byte_budget.unwrap_or(u64::MAX))
            {
                if !group.is_empty() {
                    break;
                }
                cursor = end;
                continue;
            }
            size += length;
            cursor = end;
            group.extend(unit.into_iter().map(|r| r.row));
        }
        if group.len() < 2 {
            return Ok((cursor, 0));
        }
        let physical: std::collections::HashSet<String> = group
            .iter()
            .map(|r| {
                r.pack_id
                    .map(|p| format!("p{p}"))
                    .unwrap_or_else(|| format!("c{}", r.chunk.id))
            })
            .collect();
        if physical.len() < 2 {
            return Ok((cursor, 0));
        }
        // Every old pack must be replaced whole; never split it just to fill a target.
        for id in group
            .iter()
            .filter_map(|r| r.pack_id)
            .collect::<std::collections::HashSet<_>>()
        {
            let count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM chunks WHERE pack_id=$1 AND state='ready'",
            )
            .bind(id)
            .fetch_one(&self.db)
            .await?;
            if count as usize != group.iter().filter(|r| r.pack_id == Some(id)).count() {
                return Ok((cursor, 0));
            }
        }
        let (metadata, key): (Value, String) =
            sqlx::query_as("SELECT metadata,object_key FROM streams WHERE id=$1")
                .bind(stream)
                .fetch_one(&self.db)
                .await?;
        let context = ReadContext::default();
        self.pin_pack_inputs(task, &group).await?;
        let outputs = self
            .pack_output(
                task,
                &group,
                &context,
                metadata["content_type"].as_str(),
                &key,
                true,
            )
            .await?;
        let ids: Vec<i64> = group.iter().map(|r| r.chunk.id).collect();
        let old_size:i64=sqlx::query_scalar("SELECT COALESCE(sum(bytes),0)::bigint FROM (SELECT standalone_size bytes FROM chunk_encodings WHERE chunk_id=ANY($1) AND state='ready' UNION ALL SELECT stored_size FROM packs WHERE id IN (SELECT pack_id FROM chunks WHERE id=ANY($1))) physical").bind(&ids).fetch_one(&self.db).await?;
        let new_size: i64 = outputs
            .iter()
            .map(|o| {
                o.pack
                    .as_ref()
                    .and_then(|p| p.stored_size)
                    .or_else(|| {
                        o.independent
                            .as_ref()
                            .and_then(|c| c.stored_size.map(i64::from))
                    })
                    .unwrap_or_else(|| {
                        group
                            .iter()
                            .filter(|r| o.chunks.contains(&r.chunk.id))
                            .map(|r| i64::from(r.chunk.stored_size.unwrap_or(r.chunk.raw_size)))
                            .sum()
                    })
            })
            .sum();
        if outputs.len() < physical.len() && new_size <= old_size + 12 + 44 * group.len() as i64 {
            self.commit_outputs(task, &group, &outputs, false).await?;
            return Ok((cursor, group.len()));
        }
        Ok((cursor, 0))
    }
    pub async fn pack_batch(
        &self,
        id: Uuid,
        cursor: Option<&str>,
        detail: Value,
        unpack: bool,
    ) -> Result<bool> {
        if self.maintenance.load(Ordering::Acquire) {
            sqlx::query(
                "UPDATE tasks SET state='paused',updated_at=now() WHERE id=$1 AND state='running'",
            )
            .bind(id)
            .execute(&self.db)
            .await?;
            return Ok(false);
        }
        let kind = detail["kind"].as_str().unwrap_or("unpack");
        let storage_id = detail["storage_id"]
            .as_str()
            .and_then(|value| value.parse::<Uuid>().ok());
        if kind == "range" {
            return self.range_batch(id, &detail).await;
        }
        if detail["automatic"] == true {
            return self.pack_queue_batch(id, kind, &detail).await;
        }
        let mut processed = 0usize;
        let next = if unpack || matches!(kind, "reuse" | "reclaim") {
            if detail["all"] == true {
                ensure!(
                    !self.pack_creation_allowed(),
                    "disable pack.enabled while unpacking all packs"
                );
            }
            let after = cursor.unwrap_or("0").parse::<i64>()?;
            let pack:Option<i64>=sqlx::query_scalar("SELECT id FROM packs WHERE state='ready' AND id>$1 AND ($2::bigint IS NULL OR id=$2) AND ($4::uuid IS NULL OR storage_id=$4) AND created_at<=COALESCE($3::timestamptz,now()) ORDER BY id LIMIT 1")
                .bind(after).bind(detail["pack_id"].as_i64()).bind(detail["upper_time"].as_str().map(|s|s.parse::<chrono::DateTime<chrono::Utc>>()).transpose()?).bind(storage_id).fetch_optional(&self.db).await?;
            if let Some(pack) = pack {
                processed = self.rewrite_pack(id, pack, unpack).await?;
                Some(pack.to_string())
            } else {
                None
            }
        } else {
            ensure!(self.pack_creation_allowed(), "pack creation is disabled");
            let (after, offset) = if let Some(c) = cursor {
                let (a, b) = c.split_once(':').context("invalid pack cursor")?;
                (Uuid::parse_str(a)?, b.parse::<i64>()?)
            } else {
                (Uuid::nil(), 0)
            };
            let stream:Option<Uuid>=sqlx::query_scalar("SELECT s.id FROM streams s JOIN objects o ON o.stream_id=s.id JOIN buckets b ON b.id=o.bucket_id WHERE s.state='ready' AND (s.id>$1 OR(s.id=$1 AND $2>=0)) AND ($4::uuid IS NULL OR b.storage_id=$4) AND s.created_at<=$3 ORDER BY s.id LIMIT 1")
                .bind(after).bind(offset).bind(detail["upper_time"].as_str().context("missing task snapshot")?.parse::<chrono::DateTime<chrono::Utc>>()?).bind(storage_id).fetch_optional(&self.db).await?;
            if let Some(stream) = stream {
                let budget = if let Some(storage) = storage_id {
                    crate::storage::profile::StorageProfile::load(&self.db, storage)
                        .await?
                        .pack_byte_budget
                        .map(|value| config::bytes(&value))
                        .transpose()?
                } else {
                    None
                };
                let (next, n) = self
                    .pack_stream(
                        id,
                        stream,
                        if stream == after { offset } else { 0 },
                        kind == "repack",
                        budget,
                    )
                    .await?;
                processed = n;
                Some(format!("{stream}:{next}"))
            } else {
                None
            }
        };
        sqlx::query("UPDATE tasks SET cursor=$2,processed=processed+$3,state=CASE WHEN $2::text IS NULL THEN 'completed' ELSE state END,updated_at=now() WHERE id=$1 AND state='running'")
            .bind(id).bind(&next).bind(processed as i64).execute(&self.db).await?;
        Ok(next.is_some())
    }
    async fn pack_queue_batch(&self, id: Uuid, kind: &str, detail: &Value) -> Result<bool> {
        let cutoff = detail["upper_time"]
            .as_str()
            .context("missing queue cutoff")?
            .parse::<chrono::DateTime<chrono::Utc>>()?;
        let mut processed = 0;
        let storage_id = detail["storage_id"]
            .as_str()
            .and_then(|value| value.parse::<Uuid>().ok());
        let next = if matches!(kind, "reuse" | "reclaim") {
            let row:Option<(i64,i64)>=sqlx::query_as("SELECT pc.pack_id,pc.generation FROM pack_changes pc JOIN packs p ON p.id=pc.pack_id WHERE pc.next_check_at<=$1 AND pc.reason=$2 AND ($3::uuid IS NULL OR p.storage_id=$3) ORDER BY pc.next_check_at,pc.pack_id LIMIT 1").bind(cutoff).bind(kind).bind(storage_id).fetch_optional(&self.db).await?;
            if let Some((pack, generation)) = row {
                processed = self.rewrite_pack(id, pack, false).await?;
                sqlx::query("DELETE FROM pack_changes WHERE pack_id=$1 AND generation=$2")
                    .bind(pack)
                    .bind(generation)
                    .execute(&self.db)
                    .await?;
                Some(pack.to_string())
            } else {
                None
            }
        } else {
            let row:Option<(Uuid,i64,i64)>=sqlx::query_as("SELECT pm.stream_id,pm.cursor,pm.generation FROM pack_maintenance pm JOIN streams s ON s.id=pm.stream_id JOIN objects o ON o.stream_id=s.id JOIN buckets b ON b.id=o.bucket_id WHERE pm.next_check_at<=$1 AND pm.reason=$2 AND ($3::uuid IS NULL OR b.storage_id=$3) ORDER BY pm.next_check_at,pm.stream_id LIMIT 1").bind(cutoff).bind(kind).bind(storage_id).fetch_optional(&self.db).await?;
            if let Some((stream, from, generation)) = row {
                let budget = match storage_id {
                    Some(storage) => {
                        crate::storage::profile::StorageProfile::load(&self.db, storage)
                            .await?
                            .pack_byte_budget
                            .map(|value| config::bytes(&value))
                            .transpose()?
                    }
                    None => None,
                };
                let (cursor, n) = self
                    .pack_stream(id, stream, from, kind == "repack", budget)
                    .await?;
                processed = n;
                if cursor < 0 {
                    let later:Option<chrono::DateTime<chrono::Utc>>=sqlx::query_scalar("SELECT max(GREATEST(c.repack_after,c.reference_changed_at+$2*interval '1 second')) FROM extents e JOIN chunks c ON c.id=e.chunk_id WHERE e.stream_id=$1 AND c.split_at IS NOT NULL AND GREATEST(c.repack_after,c.reference_changed_at+$2*interval '1 second')>now() AND (SELECT count(*) FROM extents WHERE chunk_id=c.id)=1").bind(stream).bind(config::seconds(&self.config.local_pack.repack_cooldown)? as f64).fetch_one(&self.db).await?;
                    if let Some(later) = later {
                        sqlx::query("UPDATE pack_maintenance SET cursor=0,next_check_at=$3,reason='repack' WHERE stream_id=$1 AND generation=$2").bind(stream).bind(generation).bind(later).execute(&self.db).await?;
                    } else {
                        sqlx::query(
                            "DELETE FROM pack_maintenance WHERE stream_id=$1 AND generation=$2",
                        )
                        .bind(stream)
                        .bind(generation)
                        .execute(&self.db)
                        .await?;
                    }
                } else {
                    sqlx::query("UPDATE pack_maintenance SET cursor=$3 WHERE stream_id=$1 AND generation=$2").bind(stream).bind(generation).bind(cursor).execute(&self.db).await?;
                }
                Some(format!("{stream}:{cursor}"))
            } else {
                None
            }
        };
        sqlx::query("UPDATE tasks SET cursor=$2,processed=processed+$3,state=CASE WHEN $2::text IS NULL THEN 'completed' ELSE state END,updated_at=now() WHERE id=$1 AND state='running'").bind(id).bind(&next).bind(processed as i64).execute(&self.db).await?;
        Ok(next.is_some())
    }
}
