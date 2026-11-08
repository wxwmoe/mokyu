use super::*;
use crate::model::{Id, node_pack as wire};
use base64::{Engine, engine::general_purpose::STANDARD};
use std::collections::BTreeSet;

impl Reader {
    pub(super) async fn transfer_encoded(
        &self,
        source: &Backend,
        keys: Arc<BTreeMap<Uuid, codec::Key>>,
        input: &crate::model::transfer::Input,
        memory: &Arc<OwnedSemaphorePermit>,
    ) -> Result<Vec<(Id, Bytes)>> {
        use crate::model::transfer::Input;
        let (path, bound) = match input {
            Input::Chunk { chunk } => (
                crate::storage::location::Layout::S3.chunk(chunk.address()),
                codec::MAX_FILE,
            ),
            Input::Pack { pack, .. } => (
                crate::storage::location::Layout::S3.pack(pack.address()),
                pack.payload_size()? + 16,
            ),
            Input::Raw { .. } => unreachable!(),
        };
        self.gets.fetch_add(1, Ordering::Relaxed);
        let bytes = read_bounded(source.store.backend.as_ref(), &path, bound).await?;
        let input = input.clone();
        let pool = self.pool.clone();
        let secrets = keys.clone();
        let cpu = self.cpu.clone().acquire_owned().await?;
        let hold = memory.clone();
        let (data, _memory) = tokio::task::spawn_blocking(move || {
            let _cpu = cpu;
            Ok::<_, anyhow::Error>((
                crate::content::transfer::decode(&input, bytes, secrets.as_ref(), &pool)?,
                hold,
            ))
        })
        .await??;
        Ok(data)
    }

    pub(crate) async fn copy_transfer(
        &self,
        source: Arc<Backend>,
        target: Arc<Backend>,
        keys: Arc<BTreeMap<Uuid, codec::Key>>,
        work: &crate::model::transfer::Work,
        batch: wire::Batch,
    ) -> Result<(wire::Encoded, crate::storage::cache::Fills)> {
        use crate::model::transfer::Input;
        work.validate(target.profile.storage_id, &batch)?;
        ensure!(
            source.profile.storage_id == work.input.storage()?,
            "transfer source storage differs"
        );
        let attempts = layout(&batch)?;
        let payload = match &work.input {
            Input::Pack { pack, .. } => pack.payload_size()? as u64 + 16,
            Input::Raw { source } => {
                let (start, end) = crate::model::transfer::chunk_span_bounds(source, &work.spans)?;
                end - start
            }
            Input::Chunk { chunk } => chunk.file_size()? as u64,
        };
        let config = target.profile.compression_config("0s");
        let units = (payload.saturating_mul(4) + self.workspace.max(config.workspace_bytes()?))
            .div_ceil(1024 * 1024);
        ensure!(
            units <= u64::from(self.pack_units),
            "transfer exceeds node memory budget"
        );
        let memory = Arc::new(self.acquire(&self.packs, units as u32).await?);
        let _io = self.acquire(&self.io, 1).await?;
        source.store.verify().await?;
        let data = if let Input::Raw { source: raw } = &work.input {
            ensure!(
                raw.medium == "shared_s3" && !raw.pending_upload,
                codec::IntegrityError::RawMetadata
            );
            let mut file = RawFile::remote(
                source.store.backend.clone(),
                raw,
                self.gets.clone(),
                self.diagnostics.clone(),
            )
            .await?;
            let resources = crate::content::transfer::Resources {
                cpu: self.cpu.clone(),
                memory: memory.clone(),
            };
            crate::content::transfer::read_spans(raw, &work.spans, &mut file, &resources).await?
        } else {
            self.transfer_encoded(&source, keys.clone(), &work.input, &memory)
                .await?
        };
        let data: BTreeMap<_, _> = data.into_iter().collect();
        let mut raw = BTreeMap::new();
        for ((source, target), chunk) in work.targets.iter().zip(&batch.chunks) {
            raw.insert(
                *target,
                (
                    chunk.hash.clone(),
                    data.get(source)
                        .context("transfer source data missing")?
                        .clone(),
                ),
            );
        }
        target.store.verify().await?;
        self.pack_data(target, keys, batch, raw, BTreeMap::new(), memory, attempts)
            .await
    }

    pub(crate) async fn pack(
        &self,
        backend: Arc<Backend>,
        materials: &crate::node::reads::Held,
        keys: Arc<BTreeMap<Uuid, codec::Key>>,
        batch: wire::Batch,
    ) -> Result<(wire::Encoded, crate::storage::cache::Fills)> {
        let attempts = layout(&batch)?;
        let mut payload = 0u64;
        for source in &batch.sources {
            payload = payload
                .checked_add(match source {
                    wire::Source::Chunk { chunk } => {
                        codec::validate_metadata(chunk, keys.as_ref())?;
                        chunk.file_size()? as u64
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
        let memory = Arc::new(self.acquire(&self.packs, units as u32).await?);
        let _io = self.acquire(&self.io, 1).await?;
        backend.store.verify().await?;
        let mut raw = BTreeMap::<Id, (Vec<u8>, Bytes)>::new();
        let mut originals = BTreeMap::new();
        for source in &batch.sources {
            let (path, bound) = match source {
                wire::Source::Chunk { chunk } => (
                    crate::storage::location::Layout::S3.chunk(chunk.address()),
                    codec::MAX_FILE,
                ),
                wire::Source::Pack { pack, .. } => (
                    crate::storage::location::Layout::S3.pack(pack.address()),
                    pack.payload_size()? + 16,
                ),
            };
            self.gets.fetch_add(1, Ordering::Relaxed);
            let origin = crate::node::reads::backend(
                materials,
                source.storage().context("pack source storage missing")?,
            )?;
            if origin.profile.storage_id != backend.profile.storage_id {
                origin.store.verify().await?;
            }
            let bytes = read_bounded(origin.store.backend.as_ref(), &path, bound).await?;
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
                        let (raw, _) = codec::decode_read(
                            &chunk,
                            bytes.clone(),
                            keys.as_ref(),
                            0,
                            &pool,
                            true,
                        )?;
                        data.push((chunk.uuid.into(), raw, chunk, Bytes::from(bytes)));
                    }
                    wire::Source::Pack { pack, members } => {
                        pack::frame::validate_file(&pack, &bytes)?;
                        let size = bytes.len() as u64;
                        pack::frame::validate_entries(
                            &members,
                            size,
                            pack.raw_size,
                            pack.member_count,
                            true,
                        )?;
                        for member in members {
                            let span =
                                member
                                    .position
                                    .range(member.raw_size, size, pack.member_count)?;
                            let body = &bytes[span.start as usize..span.end as usize];
                            let raw = pack::frame::decode_entry(
                                &pack,
                                &member,
                                body.to_vec(),
                                keys.as_ref(),
                                &pool,
                                true,
                            )?;
                            let mut file =
                                Vec::with_capacity(codec::FILE_HEADER.len() + body.len());
                            file.extend_from_slice(codec::FILE_HEADER);
                            file.extend_from_slice(body);
                            data.push((
                                member.uuid.into(),
                                raw,
                                member.chunk(&pack)?,
                                Bytes::from(file),
                            ));
                        }
                    }
                }
                Ok::<_, anyhow::Error>((data, hold))
            })
            .await??;
            for (id, plain, chunk, file) in decoded {
                raw.entry(id).or_insert_with(|| (chunk.hash.clone(), plain));
                originals.insert(chunk.location_id, (chunk, file));
            }
        }
        self.pack_data(backend, keys, batch, raw, originals, memory, attempts)
            .await
    }

    pub(crate) async fn pack_buffers(
        &self,
        backend: Arc<Backend>,
        keys: Arc<BTreeMap<Uuid, codec::Key>>,
        batch: wire::Batch,
        raw: BTreeMap<Id, (Vec<u8>, Bytes)>,
    ) -> Result<(wire::Encoded, crate::storage::cache::Fills)> {
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
        let memory = Arc::new(self.acquire(&self.packs, units as u32).await?);
        backend.store.verify().await?;
        self.pack_data(backend, keys, batch, raw, BTreeMap::new(), memory, attempts)
            .await
    }

    async fn pack_data(
        &self,
        backend: Arc<Backend>,
        keys: Arc<BTreeMap<Uuid, codec::Key>>,
        batch: wire::Batch,
        raw: BTreeMap<Id, (Vec<u8>, Bytes)>,
        mut originals: BTreeMap<Uuid, (codec::Chunk, Bytes)>,
        memory: Arc<OwnedSemaphorePermit>,
        attempts: BTreeMap<(usize, usize), usize>,
    ) -> Result<(wire::Encoded, crate::storage::cache::Fills)> {
        let config = backend.profile.compression_config("0s");
        let storage = backend.profile.storage_id;
        let mut data = Vec::with_capacity(batch.chunks.len());
        for (index, target) in batch.chunks.iter().enumerate() {
            let source = if batch.sources.is_empty() {
                target.id
            } else {
                target.physical_id()?.into()
            };
            let (hash, bytes) = raw.get(&source).context("pack member body is missing")?;
            ensure!(
                hash == &target.hash && bytes.len() == target.size as usize,
                "pack member identity differs"
            );
            data.push((batch.chunk(storage, index)?, bytes.clone()));
        }
        drop(raw);
        let data = Arc::new(data);
        let batch = Arc::new(batch);
        let mut files = Vec::with_capacity(data.len());
        for (index, (chunk, raw)) in data.iter().enumerate() {
            if let Some((mut physical, file)) = originals.remove(&chunk.location_id) {
                ensure!(
                    physical.uuid == chunk.uuid && physical.hash == chunk.hash,
                    codec::IntegrityError::Metadata
                );
                physical.id = chunk.id;
                files.push((physical, file));
                continue;
            }
            ensure!(
                batch.chunks[index].ready.is_none() && batch.chunks[index].original.is_none(),
                "missing immutable source encoding"
            );
            let cpu = self.cpu.clone().acquire_owned().await?;
            let hold = memory.clone();
            let keys = keys.clone();
            let pool = self.pool.clone();
            let config = config.clone();
            let (mut chunk, raw) = (chunk.clone(), raw.clone());
            let compress = batch.chunks[index].compress;
            let file = tokio::task::spawn_blocking(move || {
                let (_cpu, _hold) = (cpu, hold);
                ensure!(
                    raw.len() == chunk.raw_size as usize
                        && blake3::hash(&raw).as_bytes().as_slice() == chunk.hash,
                    codec::IntegrityError::Hash
                );
                let compressed = pool.compress_for(&raw, compress, &config)?;
                chunk.compressed = compressed.is_some();
                let body = compressed.unwrap_or_else(|| raw.to_vec());
                chunk.stored_size = Some(i32::try_from(
                    body.len() + if chunk.block_key_id.is_some() { 16 } else { 0 },
                )?);
                let encoded = codec::seal(&chunk, body, keys.as_ref())?;
                Ok::<_, anyhow::Error>((chunk, Bytes::from(encoded)))
            })
            .await??;
            files.push(file);
        }
        let mut result = wire::Encoded {
            chunks: files
                .iter()
                .map(|(chunk, _)| {
                    wire::encoded(
                        i64::from(chunk.stored_size.context("missing encoded chunk size")?),
                        chunk.compressed,
                    )
                })
                .collect::<Result<Vec<_>>>()?,
            packs: Vec::new(),
        };
        for &(start, end) in &batch.groups {
            if let Some(&index) = attempts.get(&(start, end)) {
                let mut pack = batch.pack(backend.profile.storage_id, index)?;
                ensure!(
                    pack.raw_size as u64 <= crate::config::bytes(&backend.profile.pack_max_size)?,
                    "pack exceeds profile size"
                );
                let members = files[start..end].to_vec();
                let cpu = self.cpu.clone().acquire_owned().await?;
                let hold = memory.clone();
                let (pack, bytes) = tokio::task::spawn_blocking(move || {
                    let (_cpu, _hold) = (cpu, hold);
                    let (file, _) = pack::frame::assemble(&mut pack, &members)?;
                    Ok::<_, anyhow::Error>((pack, file))
                })
                .await??;
                backend
                    .store
                    .backend
                    .put(
                        &crate::storage::location::Layout::S3.pack(pack.address()),
                        Bytes::from(bytes).into(),
                    )
                    .await?;
                result.packs.push((
                    index,
                    wire::encoded(pack.stored_size.context("missing pack size")?, false)?,
                    STANDARD.encode(pack.digest.context("missing pack digest")?),
                ));
            } else {
                for index in start..end {
                    let target = &batch.chunks[index];
                    if let Some(ready) = target.ready {
                        result.chunks[index] = ready;
                    } else {
                        backend
                            .store
                            .backend
                            .put(
                                &crate::storage::location::Layout::S3.chunk(target.location),
                                files[index].1.clone().into(),
                            )
                            .await?;
                    }
                }
            }
        }
        let packed = batch.packed_chunks()?;
        let cached = data
            .iter()
            .enumerate()
            .map(|(index, (chunk, raw))| {
                let id = if packed[index] {
                    chunk.uuid
                } else {
                    chunk.location_id
                };
                (id, raw.clone())
            })
            .collect();
        let fills = self
            .files
            .prepare_batch(cached, &backend.profile, memory)
            .into_iter()
            .collect();
        Ok((result, fills))
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
        if end - start > 1 {
            allowed.insert((start, end));
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
