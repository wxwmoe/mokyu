use crate::model::task::Execution;
use crate::{
    app::App,
    config,
    content::codec::Chunk,
    content::pack::{CompressionStrategy, Pack},
    storage::ReadContext,
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use serde_json::{Value, json};
use std::sync::atomic::Ordering;
use uuid::Uuid;

pub(crate) use crate::model::Mapping;
pub(crate) use crate::model::packing::PackOutput as Output;
impl App {
    async fn physical_encoding(&self, c: &Chunk) -> Result<(String, Option<Uuid>)> {
        self.store_encoding(c.storage_id.context("chunk has no store")?)
            .await
    }
    pub(crate) async fn pin_pack_inputs(&self, task: Execution, rows: &[Mapping]) -> Result<()> {
        let _coord = self.coord.lock().await;
        self.storage_writable()?;
        self.packing.pin_pack_inputs(task, rows).await
    }

    pub async fn pack_status(&self) -> Result<Value> {
        let rows: Vec<Value> = self.packing.pack_totals().await?;
        Ok(
            json!({"enabled":self.pack_creation_allowed(),"range_optimization":self.config.local_pack.range_optimization,"packs":rows,"max_size":self.config.local_pack.max_size}),
        )
    }
    pub async fn pack_start(&self, policy: &str) -> Result<Value> {
        self.start_pack(policy, false).await
    }
    pub(crate) async fn start_pack(&self, policy: &str, automatic: bool) -> Result<Value> {
        self.writable()?;
        if !["pack", "reuse", "reclaim", "repack", "range"].contains(&policy) {
            return Err(s3s::s3_error!(InvalidArgument, "unknown pack maintenance policy").into());
        }
        if policy == "range" && !self.config.local_pack.range_optimization {
            return Err(s3s::s3_error!(OperationAborted, "Range optimization is disabled").into());
        }
        if matches!(policy, "pack" | "repack") && !self.pack_creation_allowed() {
            return Err(s3s::s3_error!(OperationAborted, "pack creation is disabled").into());
        }
        let _coord = self.coord.lock().await;
        self.writable()?;
        if self.maintenance_paused(policy).await?
            || (matches!(policy, "pack" | "repack") && !self.pack_creation_allowed())
        {
            return Err(s3s::s3_error!(OperationAborted, "maintenance policy is paused").into());
        }
        let result = self
            .packing
            .start_pack(
                policy,
                automatic,
                config::seconds(&self.config.local_pack.reuse_interval)? as f64,
                self.node.get().map(|state| state.node_id),
            )
            .await?;
        self.wake_tasks.notify_one();
        Ok(result)
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
        let (count, bytes): (i64, i64) = self.packing.unpack_totals(pack).await?;
        let (upper, ids): (String, String) = self.packing.unpack_identity(pack).await?;
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
        let preparing: bool = self.packing.preparing_packs().await?;
        if all && preparing {
            return Err(s3s::s3_error!(
                OperationAborted,
                "wait for current pack generation to drain"
            )
            .into());
        }
        let result = self
            .packing
            .start_unpack(pack, all, self.node.get().map(|state| state.node_id))
            .await?;
        self.wake_tasks.notify_one();
        Ok(result)
    }

    async fn independent_output(&self, task: Execution, c: &Chunk, raw: Bytes) -> Result<Output> {
        if let Some((id, size)) = self.packing.ready_encoding(c.id).await? {
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
        physical.location_id = Uuid::new_v4();
        physical.created_at = chrono::Utc::now();
        physical.nonce = None;
        // A newly encoded physical copy always consumes a fresh persisted identity.
        physical.encoding_id = self.packing.prepare_encoding(&physical, task).await?;
        let (physical, encoded, _) = self
            .storage
            .encode(physical, raw.to_vec(), c.compressed)
            .await?;
        self.packing.encoding_metadata(&physical).await?;
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
        task: Execution,
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
        let mut outputs = Vec::new();
        let mut start = 0;
        while start < rows.len() {
            let storage = rows[start]
                .chunk
                .storage_id
                .context("pack member has no store")?;
            let settings = self.storage.pack_config(storage)?;
            let maximum = config::bytes(&settings.max_size)?;
            let mut end = start + 1;
            let mut size = rows[start].chunk.raw_size as u64;
            while end < rows.len()
                && rows[end].chunk.storage_id == Some(storage)
                && size + rows[end].chunk.raw_size as u64 <= maximum
            {
                size += rows[end].chunk.raw_size as u64;
                end += 1;
            }
            let rows = &rows[start..end];
            let data = &data[start..end];
            start = end;
            if rows.len() > 1
                && let Some(output) = self
                    .try_pack_output(task, rows, data, content_type, key)
                    .await?
            {
                outputs.push(output);
                continue;
            }
            // At most one bisection per bounded group, on a complete CDC boundary.
            let mid = rows.len() / 2;
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
        }
        Ok(outputs)
    }
    async fn try_pack_output(
        &self,
        task: Execution,
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
        let settings = self.storage.pack_config(storage_id)?;
        if !settings.enabled || raw_size as u64 > config::bytes(&settings.max_size)? {
            return Ok(None);
        }
        ensure!(
            rows.iter()
                .all(|row| row.chunk.storage_id == Some(storage_id)),
            "pack members must use one store"
        );
        let _pack_permit = self.storage.pack_permit(storage_id).await?;
        let p: Pack = self
            .packing
            .prepare_pack(
                Uuid::new_v4(),
                storage_id,
                &algorithm,
                block_key_id,
                raw_size,
                rows.len() as i32,
                task,
            )
            .await?;
        drop(coord);
        let strategy = match settings.compression_strategy {
            None => self.config.local_compression.strategy,
            Some(CompressionStrategy::Never) => crate::content::compression::Strategy::Never,
            Some(CompressionStrategy::Always) => crate::content::compression::Strategy::Always,
            Some(CompressionStrategy::Sample) => crate::content::compression::Strategy::Sample,
        };
        let should_try = match settings.compression_strategy {
            Some(CompressionStrategy::Never) => false,
            Some(_) => true,
            None => self.config.local_compression.should_try(content_type, key),
        };
        let (p, encoded) = self
            .storage
            .encode_pack(p, data.to_vec(), strategy, should_try)
            .await?;
        let old: i64 = rows
            .iter()
            .map(|r| r.chunk.stored_size.unwrap_or(r.chunk.raw_size) as i64)
            .sum();
        if p.stored_size.unwrap_or(i64::MAX) > old + 12 + rows.len() as i64 * 44 {
            self.packing.discard_pack(p.id).await?;
            return Ok(None);
        }
        self.packing.pack_metadata(&p).await?;
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
        self.packing.insert_pack_members(p.id, rows).await?;
        Ok(Some(Output {
            chunks: rows.iter().map(|r| r.chunk.id).collect(),
            pack: Some(p),
            independent: None,
        }))
    }
    pub(crate) async fn commit_outputs(
        &self,
        task: Execution,
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
        let settings = self.storage.pack_config(
            rows.first()
                .context("no pack inputs")?
                .chunk
                .storage_id
                .context("pack member has no store")?,
        )?;
        ensure!(
            rows.iter()
                .all(|row| row.chunk.storage_id == rows[0].chunk.storage_id),
            "pack inputs must use one store"
        );
        self.packing
            .commit_pack_outputs(
                task,
                rows,
                outputs,
                split,
                &settings,
                (2 * config::seconds(&self.config.metrics.access_flush_interval)?) as f64,
            )
            .await
    }
    pub(crate) async fn rewrite_pack(
        &self,
        task: Execution,
        id: i64,
        unpack: bool,
    ) -> Result<usize> {
        self.storage_writable()?;
        if !unpack
            && crate::storage::resource_gate::PRIORITY
                .try_with(|p| *p)
                .unwrap_or(crate::storage::resource_gate::FOREGROUND)
                != crate::storage::resource_gate::UPLOAD
            && self.packing.pack_has_pending(id).await?
        {
            return Ok(0);
        }
        let rows: Vec<Mapping> = self.packing.pack_layout(id).await?;
        if rows.is_empty() {
            return Ok(0);
        }
        let settings = self.storage.pack_config(
            rows[0]
                .chunk
                .storage_id
                .context("pack member has no store")?,
        )?;
        let writing: bool = self.packing.pack_has_writers(id).await?;
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
        let total: i64 = self.packing.pack_raw_size(id).await?;
        // Logical GC may already have retired unreferenced members; the payload still contains them.
        let mut dead_bytes =
            total.saturating_sub(rows.iter().map(|r| i64::from(r.chunk.raw_size)).sum()) as u64;
        let mut reference_split = false;
        for row in &rows {
            let alive: bool = self.packing.chunk_has_references(row.chunk.id).await?;
            if !alive {
                dead_bytes += row.chunk.raw_size as u64;
                continue;
            }
            if let Some(last) = groups.last().and_then(|g| g.last()) {
                reference_split |= !self
                    .packing
                    .same_references(
                        last.chunk.id,
                        row.chunk.id,
                        row.offset_bytes - last.offset_bytes,
                    )
                    .await?;
            }
            let join = if let Some(last) = groups.last().and_then(|g| g.last()) {
                !unpack
                    && last.offset_bytes + i64::from(last.chunk.raw_size) == row.offset_bytes
                    && self
                        .packing
                        .same_references(
                            last.chunk.id,
                            row.chunk.id,
                            row.offset_bytes - last.offset_bytes,
                        )
                        .await?
                    && groups
                        .last()
                        .unwrap()
                        .iter()
                        .map(|r| r.chunk.raw_size as u64)
                        .sum::<u64>()
                        + row.chunk.raw_size as u64
                        <= config::bytes(&settings.max_size)?
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
        let minimum = config::bytes(&settings.reclaim_min_savings_bytes)?;
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
            let before: i64 = self.packing.pack_stored_size(id).await?;
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
        task: Execution,
        stream: Uuid,
        from: i64,
        repack: bool,
        byte_budget: Option<u64>,
        settings: &crate::content::pack::Config,
    ) -> Result<(i64, usize)> {
        self.defer_range_repack(stream, from, settings).await?;
        let rows: Vec<crate::model::packing::PackCandidate> = self
            .packing
            .pack_candidates(
                stream,
                from,
                config::seconds(&settings.repack_cooldown)? as f64,
            )
            .await?;
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
                    > config::bytes(&settings.max_size)?.min(byte_budget.unwrap_or(u64::MAX))
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
            let count: i64 = self.packing.pack_member_count(id).await?;
            if count as usize != group.iter().filter(|r| r.pack_id == Some(id)).count() {
                return Ok((cursor, 0));
            }
        }
        let (metadata, key): (Value, String) = self.packing.pack_stream_metadata(stream).await?;
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
        let old_size: i64 = self.packing.physical_bytes(&ids).await?;
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
        id: Execution,
        cursor: Option<&str>,
        detail: Value,
        unpack: bool,
    ) -> Result<bool> {
        if self.maintenance.load(Ordering::Acquire) {
            self.packing.pause_pack_task(id).await?;
            return Ok(false);
        }
        let policy = detail["policy"].as_str().unwrap_or("unpack");
        let storage = detail["storage_id"]
            .as_str()
            .context("missing pack storage")?
            .parse::<Uuid>()?;
        let storage_id = Some(storage);
        let settings = self.storage.pack_config(storage)?;
        if matches!(policy, "pack" | "repack") && !settings.enabled {
            self.packing.advance_pack_task(id, &None, 0).await?;
            return Ok(false);
        }
        if policy == "range" {
            return self.range_batch(id, &detail).await;
        }
        if detail["automatic"] == true {
            return self.pack_queue_batch(id, policy, &detail).await;
        }
        let mut processed = 0usize;
        let next = if unpack || matches!(policy, "reuse" | "reclaim") {
            if detail["all"] == true {
                ensure!(
                    !self.pack_creation_allowed(),
                    "disable pack.enabled while unpacking all packs"
                );
            }
            let after = cursor.unwrap_or("0").parse::<i64>()?;
            let pack: Option<i64> = self
                .packing
                .next_pack(
                    after,
                    detail["pack_id"].as_i64(),
                    detail["upper_time"]
                        .as_str()
                        .map(|s| s.parse::<chrono::DateTime<chrono::Utc>>())
                        .transpose()?,
                    storage_id,
                )
                .await?;
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
            let stream: Option<Uuid> = self
                .packing
                .next_pack_stream(
                    after,
                    offset,
                    detail["upper_time"]
                        .as_str()
                        .context("missing task snapshot")?
                        .parse::<chrono::DateTime<chrono::Utc>>()?,
                    storage_id,
                )
                .await?;
            if let Some(stream) = stream {
                let budget = if let Some(storage) = storage_id {
                    self.storage_profiles
                        .load(storage)
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
                        policy == "repack",
                        budget,
                        &settings,
                    )
                    .await?;
                processed = n;
                Some(format!("{stream}:{next}"))
            } else {
                None
            }
        };
        self.packing
            .advance_pack_task(id, &next, processed as i64)
            .await?;
        Ok(next.is_some())
    }
    async fn pack_queue_batch(&self, id: Execution, policy: &str, detail: &Value) -> Result<bool> {
        let cutoff = detail["upper_time"]
            .as_str()
            .context("missing queue cutoff")?
            .parse::<chrono::DateTime<chrono::Utc>>()?;
        let mut processed = 0;
        let storage = detail["storage_id"]
            .as_str()
            .context("missing pack storage")?
            .parse::<Uuid>()?;
        let storage_id = Some(storage);
        let settings = self.storage.pack_config(storage)?;
        let next = if matches!(policy, "reuse" | "reclaim") {
            let row: Option<(i64, i64)> = self
                .packing
                .next_pack_change(cutoff, policy, storage_id)
                .await?;
            if let Some((pack, generation)) = row {
                processed = self.rewrite_pack(id, pack, false).await?;
                self.packing
                    .finish_pack_change(id, pack, generation)
                    .await?;
                Some(pack.to_string())
            } else {
                None
            }
        } else {
            let row: Option<(Uuid, i64, i64)> = self
                .packing
                .next_pack_queue(cutoff, policy, storage_id)
                .await?;
            if let Some((stream, from, generation)) = row {
                let budget = match storage_id {
                    Some(storage) => self
                        .storage_profiles
                        .load(storage)
                        .await?
                        .pack_byte_budget
                        .map(|value| config::bytes(&value))
                        .transpose()?,
                    None => None,
                };
                let (cursor, n) = self
                    .pack_stream(id, stream, from, policy == "repack", budget, &settings)
                    .await?;
                processed = n;
                if cursor < 0 {
                    let later: Option<chrono::DateTime<chrono::Utc>> = self
                        .packing
                        .stream_repack_after(
                            stream,
                            config::seconds(&settings.repack_cooldown)? as f64,
                        )
                        .await?;
                    if let Some(later) = later {
                        self.packing
                            .defer_pack_queue(id, stream, generation, later)
                            .await?;
                    } else {
                        self.packing
                            .finish_pack_queue(id, stream, generation)
                            .await?;
                    }
                } else {
                    self.packing
                        .advance_pack_queue(id, stream, generation, cursor)
                        .await?;
                }
                Some(format!("{stream}:{cursor}"))
            } else {
                None
            }
        };
        self.packing
            .advance_pack_task(id, &next, processed as i64)
            .await?;
        Ok(next.is_some())
    }
}
