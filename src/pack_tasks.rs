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
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
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
    fn physical_encoding(&self, c: &Chunk) -> Result<(String, String)> {
        let algorithm = &self.config.encryption.algorithm;
        ensure!(
            c.algorithm == "none" || algorithm != "none",
            "an active encryption key is required to rewrite previously encrypted data"
        );
        Ok((
            algorithm.clone(),
            if algorithm == "none" {
                String::new()
            } else {
                self.secrets.active_key.clone()
            },
        ))
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
        sqlx::query("UPDATE chunk_locations SET state='retired',unreferenced_at=now() WHERE owner_task=$1 AND state='uploading'").bind(task).execute(&mut *tx).await?;
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
            json!({"enabled":self.config.pack.enabled,"range_optimization":self.config.pack.range_optimization,"packs":rows,"max_size":self.config.pack.max_size}),
        )
    }
    pub async fn pack_start(&self, kind: &str) -> Result<Value> {
        self.start_pack(kind, false).await
    }
    async fn start_pack(&self, kind: &str, automatic: bool) -> Result<Value> {
        self.writable()?;
        if !["pack", "reuse", "reclaim", "repack", "range"].contains(&kind) {
            return Err(s3s::s3_error!(InvalidArgument, "unknown pack maintenance kind").into());
        }
        if kind == "range" && !self.config.pack.range_optimization {
            return Err(s3s::s3_error!(OperationAborted, "Range optimization is disabled").into());
        }
        if matches!(kind, "pack" | "repack") && !self.config.pack.enabled {
            return Err(s3s::s3_error!(OperationAborted, "pack creation is disabled").into());
        }
        let _coord = self.coord.lock().await;
        let existing:Option<Uuid>=sqlx::query_scalar("SELECT id FROM tasks WHERE kind='pack' AND detail->>'kind'=$1 AND state IN ('queued','running') LIMIT 1")
            .bind(kind).fetch_optional(&self.db).await?;
        if let Some(id) = existing {
            return Ok(json!({"task_id":id,"existing":true}));
        }
        if automatic && let Some(id)=sqlx::query_scalar::<_,Uuid>("SELECT id FROM tasks WHERE kind='pack' AND detail->>'kind'=$1 AND state='failed' AND updated_at>now()-$2*interval '1 second' ORDER BY updated_at DESC LIMIT 1").bind(kind).bind(config::seconds(&self.config.pack.reuse_interval)? as f64).fetch_optional(&self.db).await? {return Ok(json!({"task_id":id,"retry_pending":true}));}
        let id = Uuid::new_v4();
        sqlx::query("INSERT INTO tasks(id,kind,state,detail) VALUES($1,'pack','queued',$2)")
            .bind(id)
            .bind(json!({"kind":kind,"upper_time":chrono::Utc::now(),"automatic":automatic}))
            .execute(&self.db)
            .await?;
        self.wake_tasks.notify_one();
        Ok(json!({"task_id":id}))
    }
    pub async fn unpack_start(&self, pack: Option<i64>, all: bool, execute: bool) -> Result<Value> {
        if all == pack.is_some() || pack.is_some_and(|id| id <= 0) {
            return Err(
                s3s::s3_error!(InvalidArgument, "specify one positive pack ID or --all").into(),
            );
        }
        if all && self.config.pack.enabled {
            return Err(s3s::s3_error!(
                OperationAborted,
                "disable pack.enabled before unpacking all packs"
            )
            .into());
        }
        if let Some(id) = pack {
            ensure!(id > 0, "pack ID must be positive");
        }
        let (count,bytes):(i64,i64)=sqlx::query_as("SELECT count(*),COALESCE(sum(raw_size),0)::bigint FROM packs WHERE state='ready' AND ($1::bigint IS NULL OR id=$1)")
            .bind(pack).fetch_one(&self.db).await?;
        if !execute {
            return Ok(
                json!({"preview":true,"packs":count,"raw_bytes":bytes,"effect":"write independent chunk sources; old packs retain GC grace"}),
            );
        }
        self.writable()?;
        let id = Uuid::new_v4();
        sqlx::query("INSERT INTO tasks(id,kind,state,detail) VALUES($1,'unpack','queued',$2)")
            .bind(id)
            .bind(json!({"pack_id":pack,"all":all}))
            .execute(&self.db)
            .await?;
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
            "SELECT id,stored_size FROM chunk_locations WHERE chunk_id=$1 AND state='ready'",
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
        (physical.algorithm, physical.key_id) = self.physical_encoding(c)?;
        physical.encoding_id = sqlx::query_scalar("SELECT nextval('chunk_locations_id_seq')")
            .fetch_one(&self.db)
            .await?;
        physical.storage_id = Uuid::new_v4();
        physical.created_at = chrono::Utc::now();
        physical.nonce = None;
        // A newly encoded physical copy always consumes a fresh persisted identity.
        sqlx::query("INSERT INTO chunk_locations(id,chunk_id,storage_id,compressed,state,created_at,unreferenced_at,owner_task,algorithm,key_id) VALUES($1,$2,$3,false,'uploading',$4,now(),$5,$6,$7)")
            .bind(physical.encoding_id).bind(c.id).bind(physical.storage_id).bind(physical.created_at).bind(task).bind(&physical.algorithm).bind(&physical.key_id).execute(&self.db).await?;
        let (physical, encoded, _) = self
            .storage
            .encode(physical, raw.to_vec(), c.compressed)
            .await?;
        sqlx::query("UPDATE chunk_locations SET stored_size=$2,compressed=$3,nonce=$4 WHERE id=$1")
            .bind(physical.encoding_id)
            .bind(physical.stored_size)
            .bind(physical.compressed)
            .bind(&physical.nonce)
            .execute(&self.db)
            .await?;
        self.storage
            .put_path(&self.storage.path(physical.storage_id), encoded)
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
        if rows.len() == 1 || !allow_pack {
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
        let raw_size: i64 = rows.iter().map(|r| i64::from(r.chunk.raw_size)).sum();
        let first = &rows[0].chunk;
        let (algorithm, key_id) = self.physical_encoding(first)?;
        let p:Pack=sqlx::query_as("INSERT INTO packs(storage_id,algorithm,key_id,raw_size,member_count,state,unreferenced_at,owner_task) VALUES($1,$2,$3,$4,$5,'preparing',now(),$6) RETURNING *")
            .bind(Uuid::new_v4()).bind(&algorithm).bind(&key_id).bind(raw_size).bind(rows.len() as i32).bind(task).fetch_one(&self.db).await?;
        let strategy = match self.config.pack.compression_strategy {
            None => self.config.compression.strategy,
            Some(CompressionStrategy::Always) => crate::compression::Strategy::Always,
            Some(CompressionStrategy::Sample) => crate::compression::Strategy::Sample,
            Some(CompressionStrategy::FileType) => crate::compression::Strategy::FileType,
            Some(CompressionStrategy::ChunkHint) => {
                if rows.iter().any(|r| r.chunk.compressed) {
                    crate::compression::Strategy::Always
                } else {
                    crate::compression::Strategy::Sample
                }
            }
        };
        let mut compression = self.config.compression.clone();
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
        sqlx::query("UPDATE packs SET stored_size=$2,compressed=$3,nonce=$4,digest=$5 WHERE id=$1")
            .bind(p.id)
            .bind(p.stored_size)
            .bind(p.compressed)
            .bind(&p.nonce)
            .bind(&p.digest)
            .execute(&self.db)
            .await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("pack-prepared").await;
        self.storage
            .put_path(&self.storage.pack_path(p.storage_id), encoded)
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
        let before:i64=sqlx::query_scalar("SELECT COALESCE(sum(bytes),0)::bigint FROM (SELECT stored_size bytes FROM chunk_locations WHERE chunk_id=ANY($1) AND state='ready' UNION ALL SELECT stored_size FROM packs WHERE id IN (SELECT pack_id FROM chunks WHERE id=ANY($1))) sources").bind(&ids).fetch_one(&mut *tx).await?;
        let added:i64=sqlx::query_scalar("SELECT COALESCE(sum(bytes),0)::bigint FROM (SELECT stored_size bytes FROM packs WHERE owner_task=$1 AND state='preparing' UNION ALL SELECT stored_size FROM chunk_locations WHERE owner_task=$1 AND state='uploading') sources").bind(task).fetch_one(&mut *tx).await?;
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
        let upload: bool =
            sqlx::query_scalar("SELECT kind IN ('upload','cache_flush') FROM tasks WHERE id=$1")
                .bind(task)
                .fetch_one(&mut *tx)
                .await?;
        let range: bool = sqlx::query_scalar(
            "SELECT kind='pack' AND detail->>'kind'='range' FROM tasks WHERE id=$1",
        )
        .bind(task)
        .fetch_one(&mut *tx)
        .await?;
        if !split && !upload {
            let exclusive:bool=sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM chunks c CROSS JOIN gateway_meta g WHERE c.id=ANY($1) AND ((SELECT count(*) FROM extents WHERE chunk_id=c.id)<>1 OR NOT COALESCE(c.repack_after<=now(),true) OR (c.range_split_at IS NOT NULL AND (g.access_coverage_since IS NULL OR g.access_coverage_since>date_trunc('hour',now()-$3*interval '1 second') OR g.access_flushed_at IS NULL OR g.access_flushed_at<now()-$4*interval '1 second' OR EXISTS(SELECT 1 FROM chunk_access_windows w WHERE w.chunk_id=c.id AND w.range_reads>0 AND w.window_start>=date_trunc('hour',now()-$3*interval '1 second')))) OR (c.split_at IS NOT NULL AND c.reference_changed_at>now()-$2*interval '1 second')))").bind(&ids).bind(config::seconds(&self.config.pack.repack_cooldown)? as f64).bind(config::seconds(&self.config.pack.range_repack_window)? as f64).bind((2*config::seconds(&self.config.statistics.access_flush_interval)?) as f64).fetch_one(&mut *tx).await?;
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
                sqlx::query("UPDATE chunk_locations SET state='retired',unreferenced_at=now() WHERE chunk_id=ANY($1) AND state='ready'").bind(&output.chunks).execute(&mut *tx).await?;
            } else if let Some(c) = &output.independent {
                sqlx::query("UPDATE chunk_locations SET state='ready',unreferenced_at=NULL,stored_at=clock_timestamp() WHERE id=$1 AND state='uploading'").bind(c.encoding_id).execute(&mut *tx).await?;
            }
            sqlx::query("UPDATE chunks SET pack_id=$2,split_at=CASE WHEN $3 THEN now() ELSE split_at END,repack_after=CASE WHEN $3 THEN GREATEST(repack_after,now()+$4*interval '1 second') ELSE repack_after END,range_split_at=CASE WHEN $5 THEN now() WHEN NOT $3 AND $2::bigint IS NOT NULL THEN NULL ELSE range_split_at END WHERE id=ANY($1)")
                .bind(&output.chunks).bind(output.pack.as_ref().map(|p|p.id)).bind(split).bind(config::seconds(if range {&self.config.pack.range_repack_after}else{&self.config.pack.repack_cooldown})? as f64).bind(range).execute(&mut *tx).await?;
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
                .bind(&retained).bind(config::seconds(&self.config.pack.repack_cooldown)? as f64).execute(&mut *tx).await?;
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
            && crate::backend::PRIORITY
                .try_with(|p| *p)
                .unwrap_or(crate::backend::FOREGROUND)
                != crate::backend::UPLOAD
            && sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM pending_uploads WHERE source_pack=$1)",
            )
            .bind(id)
            .fetch_one(&self.db)
            .await?
        {
            return Ok(0);
        }
        let rows:Vec<Mapping>=sqlx::query_as("SELECT c.*,m.offset_bytes,c.pack_id FROM pack_members m JOIN chunks c ON c.id=m.chunk_id WHERE m.pack_id=$1 AND c.pack_id=$1 AND c.state='ready' ORDER BY m.ordinal LIMIT 4096")
            .bind(id).fetch_all(&self.db).await?;
        if rows.is_empty() {
            return Ok(0);
        }
        let writing:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM extents e JOIN chunks c ON c.id=e.chunk_id JOIN streams s ON s.id=e.stream_id WHERE c.pack_id=$1 AND s.state='writing')").bind(id).fetch_one(&self.db).await?;
        if writing
            && crate::backend::PRIORITY
                .try_with(|p| *p)
                .unwrap_or(crate::backend::FOREGROUND)
                != crate::backend::UPLOAD
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
                        <= config::bytes(&self.config.pack.max_size)?
            } else {
                false
            };
            if join {
                groups.last_mut().unwrap().push(row.clone());
            } else {
                groups.push(vec![row.clone()]);
            }
        }
        if groups.is_empty()
            || (!unpack && dead_bytes == 0 && groups.len() == 1 && groups[0].len() == rows.len())
        {
            return Ok(0);
        }
        let reclaim_only = !unpack && !reference_split && dead_bytes > 0;
        let minimum = config::bytes(&self.config.pack.reclaim_min_savings_bytes)?;
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
                    self.config.pack.enabled && !unpack,
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
    ) -> Result<(i64, usize)> {
        self.defer_range_repack(stream, from).await?;
        #[derive(sqlx::FromRow)]
        struct Candidate {
            #[sqlx(flatten)]
            row: Mapping,
            eligible: bool,
            unit_count: i64,
        }
        let rows:Vec<Candidate>=sqlx::query_as("SELECT c.*,e.offset_bytes,c.pack_id,(NOT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=c.id) AND e.source_offset=0 AND e.length=c.raw_size AND (SELECT count(*) FROM extents WHERE chunk_id=c.id)=1 AND COALESCE(c.repack_after<=now(),true) AND (c.split_at IS NULL OR c.reference_changed_at<=now()-$3*interval '1 second')) eligible,CASE WHEN c.pack_id IS NULL THEN 1 ELSE (SELECT count(*) FROM chunks WHERE pack_id=c.pack_id AND state='ready') END unit_count FROM extents e JOIN chunks c ON c.id=e.chunk_id JOIN streams s ON s.id=e.stream_id WHERE e.stream_id=$1 AND e.offset_bytes>=$2 AND s.state='ready' AND c.state='ready' AND EXISTS(SELECT 1 FROM objects WHERE stream_id=s.id) ORDER BY e.offset_bytes LIMIT 1026")
            .bind(stream).bind(from).bind(config::seconds(&self.config.pack.repack_cooldown)? as f64).fetch_all(&self.db).await?;
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
                        || r.chunk.key_id != head.row.chunk.key_id
                })
                || size + length > config::bytes(&self.config.pack.max_size)?
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
        let old_size:i64=sqlx::query_scalar("SELECT COALESCE(sum(bytes),0)::bigint FROM (SELECT stored_size bytes FROM chunk_locations WHERE chunk_id=ANY($1) AND state='ready' UNION ALL SELECT stored_size FROM packs WHERE id IN (SELECT pack_id FROM chunks WHERE id=ANY($1))) physical").bind(&ids).fetch_one(&self.db).await?;
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
                    !self.config.pack.enabled,
                    "disable pack.enabled while unpacking all packs"
                );
            }
            let after = cursor.unwrap_or("0").parse::<i64>()?;
            let pack:Option<i64>=sqlx::query_scalar("SELECT id FROM packs WHERE state='ready' AND id>$1 AND ($2::bigint IS NULL OR id=$2) AND created_at<=COALESCE($3::timestamptz,now()) ORDER BY id LIMIT 1")
                .bind(after).bind(detail["pack_id"].as_i64()).bind(detail["upper_time"].as_str().map(|s|s.parse::<chrono::DateTime<chrono::Utc>>()).transpose()?).fetch_optional(&self.db).await?;
            if let Some(pack) = pack {
                processed = self.rewrite_pack(id, pack, unpack).await?;
                Some(pack.to_string())
            } else {
                None
            }
        } else {
            ensure!(self.config.pack.enabled, "pack creation is disabled");
            let (after, offset) = if let Some(c) = cursor {
                let (a, b) = c.split_once(':').context("invalid pack cursor")?;
                (Uuid::parse_str(a)?, b.parse::<i64>()?)
            } else {
                (Uuid::nil(), 0)
            };
            let stream:Option<Uuid>=sqlx::query_scalar("SELECT s.id FROM streams s JOIN objects o ON o.stream_id=s.id WHERE s.state='ready' AND (s.id>$1 OR(s.id=$1 AND $2>=0)) AND s.created_at<=$3 ORDER BY s.id LIMIT 1")
                .bind(after).bind(offset).bind(detail["upper_time"].as_str().context("missing task snapshot")?.parse::<chrono::DateTime<chrono::Utc>>()?).fetch_optional(&self.db).await?;
            if let Some(stream) = stream {
                let (next, n) = self
                    .pack_stream(
                        id,
                        stream,
                        if stream == after { offset } else { 0 },
                        kind == "repack",
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
        let next = if matches!(kind, "reuse" | "reclaim") {
            let row:Option<(i64,i64)>=sqlx::query_as("SELECT pack_id,generation FROM pack_changes WHERE next_check_at<=$1 AND reason=$2 ORDER BY next_check_at,pack_id LIMIT 1").bind(cutoff).bind(kind).fetch_optional(&self.db).await?;
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
            let row:Option<(Uuid,i64,i64)>=sqlx::query_as("SELECT stream_id,cursor,generation FROM pack_maintenance WHERE next_check_at<=$1 AND reason=$2 ORDER BY next_check_at,stream_id LIMIT 1").bind(cutoff).bind(kind).fetch_optional(&self.db).await?;
            if let Some((stream, from, generation)) = row {
                let (cursor, n) = self.pack_stream(id, stream, from, kind == "repack").await?;
                processed = n;
                if cursor < 0 {
                    let later:Option<chrono::DateTime<chrono::Utc>>=sqlx::query_scalar("SELECT max(GREATEST(c.repack_after,c.reference_changed_at+$2*interval '1 second')) FROM extents e JOIN chunks c ON c.id=e.chunk_id WHERE e.stream_id=$1 AND c.split_at IS NOT NULL AND GREATEST(c.repack_after,c.reference_changed_at+$2*interval '1 second')>now() AND (SELECT count(*) FROM extents WHERE chunk_id=c.id)=1").bind(stream).bind(config::seconds(&self.config.pack.repack_cooldown)? as f64).fetch_one(&self.db).await?;
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

pub async fn run(app: Arc<App>) -> Result<()> {
    let mut last = [std::time::Instant::now(); 5];
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        if app.maintenance.load(Ordering::Acquire) {
            continue;
        }
        let changed:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pack_changes WHERE reason='reuse' AND next_check_at<=now())").fetch_one(&app.db).await?;
        if changed && let Err(e) = app.start_pack("reuse", true).await {
            tracing::warn!(error=%e,"pack reuse scheduling deferred");
        }
        for (i, (kind, interval)) in [
            ("pack", &app.config.pack.interval),
            ("reuse", &app.config.pack.reuse_interval),
            ("reclaim", &app.config.pack.reclaim_interval),
            ("repack", &app.config.pack.repack_interval),
            ("range", &app.config.pack.range_interval),
        ]
        .into_iter()
        .enumerate()
        {
            if last[i].elapsed() >= Duration::from_secs(config::seconds(interval)?) {
                last[i] = std::time::Instant::now();
                if (if kind == "range" {
                    app.config.pack.range_optimization
                } else {
                    app.config.pack.enabled || matches!(kind, "reuse" | "reclaim")
                }) && let Err(e) = app.start_pack(kind, true).await
                {
                    tracing::warn!(error=%e,kind,"pack schedule failed");
                }
            }
        }
    }
}
