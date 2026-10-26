use super::*;
use crate::model::{Id, node_pack as wire};
use base64::{Engine, engine::general_purpose::STANDARD};
use std::collections::BTreeSet;

impl Reader {
    pub(crate) async fn pack(
        &self,
        backend: Arc<Backend>,
        keys: Arc<BTreeMap<Uuid, codec::Key>>,
        batch: wire::Batch,
    ) -> Result<wire::Encoded> {
        let attempts = layout(&batch)?;
        let mut payload = 0u64;
        for source in &batch.sources {
            payload = payload
                .checked_add(match source {
                    wire::Source::Chunk { chunk } => {
                        codec::validate_metadata(chunk, keys.as_ref())?;
                        chunk.raw_size as u64 + 16
                    }
                    wire::Source::Pack { pack, members } => {
                        pack.validate(keys.as_ref())?;
                        crate::model::source::manifest_digest(pack, members)?;
                        pack.payload_size()? as u64 + 16
                    }
                })
                .context("pack memory overflow")?;
        }
        let config = backend.profile.compression_config("0s");
        let units = (payload.saturating_mul(4) + self.workspace.max(config.workspace_bytes()?))
            .div_ceil(1024 * 1024);
        ensure!(
            units <= u64::from(self.pack_units),
            "pack exceeds node memory budget"
        );
        let memory = Arc::new(self.packs.clone().acquire_many_owned(units as u32).await?);
        let _io = self.io.clone().acquire_owned().await?;
        backend.store.verify().await?;
        let mut raw = BTreeMap::<Id, (Vec<u8>, Bytes)>::new();
        for source in &batch.sources {
            let (path, bound) = match source {
                wire::Source::Chunk { chunk } => (
                    Path::from(crate::storage::chunk_name(chunk.location_id)),
                    codec::MAX + 16,
                ),
                wire::Source::Pack { pack, .. } => (
                    Path::from(format!(
                        "packs/{}",
                        crate::storage::chunk_name(pack.location_id)
                    )),
                    pack.payload_size()? + 16,
                ),
            };
            self.gets.fetch_add(1, Ordering::Relaxed);
            let bytes = read_bounded(backend.store.backend.as_ref(), &path, bound).await?;
            let cpu = self.cpu.clone().acquire_owned().await?;
            let hold = memory.clone();
            let source = source.clone();
            let keys = keys.clone();
            let pool = self.pool.clone();
            let (decoded, _memory) = tokio::task::spawn_blocking(move || {
                let _cpu = cpu;
                let mut data = Vec::new();
                match source {
                    wire::Source::Chunk { chunk } => {
                        let (bytes, _) =
                            codec::decode_read(&chunk, bytes, keys.as_ref(), 0, &pool, true)?;
                        data.push((chunk.id, (chunk.hash, Bytes::copy_from_slice(&bytes))));
                    }
                    wire::Source::Pack { pack, members } => {
                        let decoded = pack::decode(&pack, bytes, keys.as_ref(), &pool)?;
                        ensure!(
                            decoded.members.len() == members.len(),
                            "pack members changed"
                        );
                        for ((id, offset, size, hash), member) in
                            members.into_iter().zip(&decoded.members)
                        {
                            ensure!(
                                id == Id::Uuid(member.id)
                                    && size as usize == member.len
                                    && hash.as_slice() == member.hash
                                    && member.start
                                        == 12
                                            + pack::MEMBER_SIZE * decoded.members.len()
                                            + offset as usize,
                                "pack member metadata changed"
                            );
                            data.push((
                                id,
                                (
                                    hash,
                                    decoded.bytes.slice(member.start..member.start + member.len),
                                ),
                            ));
                        }
                    }
                }
                Ok::<_, anyhow::Error>((data, hold))
            })
            .await??;
            for (id, value) in decoded {
                raw.entry(id).or_insert(value);
            }
        }
        self.pack_data(backend, keys, batch, raw, memory, attempts)
            .await
    }

    pub(crate) async fn pack_buffers(
        &self,
        backend: Arc<Backend>,
        keys: Arc<BTreeMap<Uuid, codec::Key>>,
        batch: wire::Batch,
        raw: BTreeMap<Id, (Vec<u8>, Bytes)>,
    ) -> Result<wire::Encoded> {
        let attempts = layout(&batch)?;
        ensure!(
            batch.sources.is_empty() && raw.len() == batch.chunks.len(),
            "invalid buffered pack inputs"
        );
        let payload = raw
            .values()
            .map(|(_, bytes)| bytes.len() as u64)
            .sum::<u64>();
        let config = backend.profile.compression_config("0s");
        let units = (payload.saturating_mul(4) + self.workspace.max(config.workspace_bytes()?))
            .div_ceil(1024 * 1024);
        ensure!(
            units <= u64::from(self.pack_units),
            "pack exceeds node memory budget"
        );
        let memory = Arc::new(self.packs.clone().acquire_many_owned(units as u32).await?);
        backend.store.verify().await?;
        self.pack_data(backend, keys, batch, raw, memory, attempts)
            .await
    }

    async fn pack_data(
        &self,
        backend: Arc<Backend>,
        keys: Arc<BTreeMap<Uuid, codec::Key>>,
        batch: wire::Batch,
        raw: BTreeMap<Id, (Vec<u8>, Bytes)>,
        memory: Arc<OwnedSemaphorePermit>,
        attempts: BTreeMap<(usize, usize), usize>,
    ) -> Result<wire::Encoded> {
        let config = backend.profile.compression_config("0s");
        let storage = backend.profile.storage_id;
        let mut data = Vec::with_capacity(batch.chunks.len());
        for (index, target) in batch.chunks.iter().enumerate() {
            let (hash, bytes) = raw.get(&target.id).context("pack member body is missing")?;
            ensure!(
                hash == &target.hash && bytes.len() == target.size as usize,
                "pack member identity differs"
            );
            data.push((batch.chunk(storage, index)?, bytes.clone()));
        }
        drop(raw);
        let data = Arc::new(data);
        let batch = Arc::new(batch);
        let mut result = wire::Encoded {
            chunks: vec![0; batch.chunks.len()],
            packs: vec![],
        };
        for &(start, end) in &batch.groups {
            if self
                .pack_attempt(
                    &backend,
                    &keys,
                    &batch,
                    &data,
                    &memory,
                    &attempts,
                    start,
                    end,
                    &mut result,
                )
                .await?
            {
                continue;
            }
            let mid = start + (end - start) / 2;
            for (start, end) in [(start, mid), (mid, end)] {
                if self
                    .pack_attempt(
                        &backend,
                        &keys,
                        &batch,
                        &data,
                        &memory,
                        &attempts,
                        start,
                        end,
                        &mut result,
                    )
                    .await?
                {
                    continue;
                }
                for index in start..end {
                    let target = &batch.chunks[index];
                    if let Some(ready) = target.ready {
                        result.chunks[index] = ready;
                        continue;
                    }
                    let cpu = self.cpu.clone().acquire_owned().await?;
                    let hold = memory.clone();
                    let keys = keys.clone();
                    let pool = self.pool.clone();
                    let config = config.clone();
                    let (mut chunk, raw) = data[index].clone();
                    let should_try = target.compress;
                    let (encoded, value, _memory) = tokio::task::spawn_blocking(move || {
                        let _cpu = cpu;
                        let compressed = pool.compress_for(&raw, should_try, &config)?;
                        chunk.compressed = compressed.is_some();
                        let encoded = compressed.unwrap_or_else(|| raw.to_vec());
                        chunk.stored_size = Some(i32::try_from(
                            encoded.len() + if chunk.algorithm == "none" { 0 } else { 16 },
                        )?);
                        let encoded = codec::seal(&chunk, encoded, keys.as_ref())?;
                        let value = wire::encoded(encoded.len() as i64, chunk.compressed)?;
                        Ok::<_, anyhow::Error>((encoded, value, hold))
                    })
                    .await??;
                    backend
                        .store
                        .backend
                        .put(
                            &Path::from(crate::storage::chunk_name(target.location)),
                            Bytes::from(encoded).into(),
                        )
                        .await?;
                    result.chunks[index] = value;
                }
            }
        }
        Ok(result)
    }

    async fn pack_attempt(
        &self,
        backend: &Arc<Backend>,
        keys: &Arc<BTreeMap<Uuid, codec::Key>>,
        batch: &Arc<wire::Batch>,
        data: &Arc<Vec<(codec::Chunk, Bytes)>>,
        memory: &Arc<OwnedSemaphorePermit>,
        attempts: &BTreeMap<(usize, usize), usize>,
        start: usize,
        end: usize,
        result: &mut wire::Encoded,
    ) -> Result<bool> {
        let Some(&index) = attempts.get(&(start, end)) else {
            return Ok(false);
        };
        let target = batch.pack(backend.profile.storage_id, index)?;
        let config = backend.profile.compression_config("0s");
        let settings = backend.profile.pack_config();
        ensure!(
            target.raw_size as u64 <= crate::config::bytes(&settings.max_size)?,
            "pack exceeds profile size"
        );
        let strategy = match settings
            .compression_strategy
            .context("missing pack compression policy")?
        {
            pack::CompressionStrategy::Never => compression::Strategy::Never,
            pack::CompressionStrategy::Always => compression::Strategy::Always,
            pack::CompressionStrategy::Sample => compression::Strategy::Sample,
        };
        let old: i64 = batch.chunks[start..end]
            .iter()
            .map(|c| i64::from(c.previous_size.unwrap_or(c.size)))
            .sum();
        let cpu = self.cpu.clone().acquire_owned().await?;
        let hold = memory.clone();
        let keys = keys.clone();
        let pool = self.pool.clone();
        let values = data[start..end].to_vec();
        let (target, bytes, _memory) = tokio::task::spawn_blocking(move || {
            let _cpu = cpu;
            let (target, bytes) = pack::encode(
                target,
                &values,
                keys.as_ref(),
                &pool,
                strategy,
                !matches!(strategy, compression::Strategy::Never),
                Some(&config),
            )?;
            Ok::<_, anyhow::Error>((target, bytes, hold))
        })
        .await??;
        if bytes.len() as i64 > old + 12 + (end - start) as i64 * pack::MEMBER_SIZE as i64 {
            return Ok(false);
        }
        backend
            .store
            .backend
            .put(
                &Path::from(format!(
                    "packs/{}",
                    crate::storage::chunk_name(target.location_id)
                )),
                Bytes::from(bytes).into(),
            )
            .await?;
        result.packs.push((
            index,
            wire::encoded(
                target.stored_size.context("pack size is missing")?,
                target.compressed,
            )?,
            STANDARD.encode(target.digest.context("pack digest is missing")?),
        ));
        Ok(true)
    }
}

fn layout(batch: &wire::Batch) -> Result<BTreeMap<(usize, usize), usize>> {
    ensure!(
        !batch.chunks.is_empty() && batch.chunks.len() <= pack::MAX_MEMBERS,
        "invalid pack batch"
    );
    ensure!(
        batch.sources.len() <= 2 * pack::MAX_MEMBERS && batch.packs.len() <= pack::MAX_MEMBERS,
        "invalid pack sources"
    );
    let mut covered = 0;
    let mut allowed = BTreeSet::new();
    for &(start, end) in &batch.groups {
        ensure!(
            start == covered && start < end && end <= batch.chunks.len(),
            "invalid pack groups"
        );
        let mid = start + (end - start) / 2;
        for range in [(start, end), (start, mid), (mid, end)] {
            if range.1 - range.0 > 1 {
                allowed.insert(range);
            }
        }
        covered = end;
    }
    ensure!(covered == batch.chunks.len(), "incomplete pack groups");
    let mut attempts = BTreeMap::new();
    let mut identities = BTreeSet::new();
    for target in &batch.chunks {
        ensure!(
            !target.location.is_nil() && identities.insert(target.location),
            "duplicate chunk destination"
        );
    }
    for (index, attempt) in batch.packs.iter().enumerate() {
        ensure!(
            allowed.contains(&(attempt.start, attempt.end))
                && attempts
                    .insert((attempt.start, attempt.end), index)
                    .is_none()
                && !attempt.location.is_nil()
                && identities.insert(attempt.location),
            "invalid pack attempt"
        );
    }
    Ok(attempts)
}
