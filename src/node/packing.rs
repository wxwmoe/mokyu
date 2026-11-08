use super::*;
use crate::model::{Id, node_pack as wire};
use base64::{Engine, engine::general_purpose::STANDARD};
use std::collections::BTreeSet;

struct Origin {
    backend: Arc<Backend>,
    path: Path,
    chunk: codec::Chunk,
    range: Option<(std::ops::Range<u64>, u64)>,
}

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
        let _io = self.acquire(&self.io, 1).await?;
        backend.store.verify().await?;
        let mut origins = BTreeMap::new();
        for source in &batch.sources {
            let origin = crate::node::reads::backend(
                materials,
                source.storage().context("pack source storage missing")?,
            )?;
            if origin.profile.storage_id != backend.profile.storage_id {
                origin.store.ensure_verified().await?;
            }
            match source {
                wire::Source::Chunk { chunk } => {
                    codec::validate_metadata(chunk, keys.as_ref())?;
                    origins.insert(
                        chunk.location_id,
                        Origin {
                            backend: origin,
                            path: crate::storage::location::Layout::S3.chunk(chunk.address()),
                            chunk: chunk.clone(),
                            range: None,
                        },
                    );
                }
                wire::Source::Pack { pack, members } => {
                    pack.validate(keys.as_ref())?;
                    crate::model::source::manifest_digest(pack, members)?;
                    let size =
                        u64::try_from(pack.stored_size.context(codec::IntegrityError::Metadata)?)?;
                    let path = crate::storage::location::Layout::S3.pack(pack.address());
                    for member in members {
                        let range =
                            member
                                .position
                                .range(member.raw_size, size, pack.member_count)?;
                        let chunk = member.chunk(pack)?;
                        origins.entry(chunk.location_id).or_insert_with(|| Origin {
                            backend: origin.clone(),
                            path: path.clone(),
                            chunk,
                            range: Some((range, size)),
                        });
                    }
                }
            }
        }
        let output = batch
            .chunks
            .iter()
            .map(|item| u64::try_from(item.size))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .sum();
        let memory = self.pack_buffer_budget(&backend, 0, output).await?;
        self.pack_data(
            backend,
            keys,
            batch,
            BTreeMap::new(),
            origins,
            memory,
            attempts,
        )
        .await
    }

    pub(super) async fn pack_buffer_budget(
        &self,
        backend: &Backend,
        plain: u64,
        output: u64,
    ) -> Result<Arc<OwnedSemaphorePermit>> {
        let work = self
            .workspace
            .max(backend.profile.compression_config("0s").workspace_bytes()?);
        let bytes = pack::buffer_bytes(plain, output, work);
        let units = u32::try_from(bytes.div_ceil(1024 * 1024))?;
        ensure!(
            units <= self.pack_units,
            "pack buffers exceed node memory budget"
        );
        Ok(Arc::new(self.acquire(&self.packs, units).await?))
    }

    pub(crate) fn write_batch_budget(
        &self,
        backend: &Backend,
    ) -> Result<Option<(usize, Arc<OwnedSemaphorePermit>)>> {
        let limit = crate::config::bytes(&backend.profile.pack_max_size)?
            .min(crate::model::node_chunk::MAX_BYTES as u64);
        let workspace = self
            .workspace
            .max(backend.profile.compression_config("0s").workspace_bytes()?);
        // Two batches may hold input while the single publishing task encodes one output.
        let units = pack::buffer_bytes(limit * 2, limit, workspace).div_ceil(1024 * 1024);
        Ok(self
            .packs
            .clone()
            .try_acquire_many_owned(u32::try_from(units)?)
            .ok()
            .map(|permit| (limit as usize, Arc::new(permit))))
    }

    pub(crate) async fn pack_buffers(
        &self,
        backend: Arc<Backend>,
        keys: Arc<BTreeMap<Uuid, codec::Key>>,
        batch: wire::Batch,
        raw: BTreeMap<Id, (Vec<u8>, Bytes)>,
        reserved: Option<Arc<OwnedSemaphorePermit>>,
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
        let memory = match reserved {
            Some(permit) => permit,
            None => self.pack_buffer_budget(&backend, payload, payload).await?,
        };
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
        origins: BTreeMap<Uuid, Origin>,
        memory: Arc<OwnedSemaphorePermit>,
        attempts: BTreeMap<(usize, usize), usize>,
    ) -> Result<(wire::Encoded, crate::storage::cache::Fills)> {
        let config = backend.profile.compression_config("0s");
        let mut result = wire::Encoded {
            chunks: Vec::with_capacity(batch.chunks.len()),
            packs: Vec::new(),
        };
        let mut cached = Vec::new();
        for &(start, end) in &batch.groups {
            let mut pack = attempts
                .get(&(start, end))
                .map(|&attempt| batch.pack(backend.profile.storage_id, attempt))
                .transpose()?;
            let mut writer = if let Some(pack) = &pack {
                ensure!(
                    pack.raw_size as u64 <= crate::config::bytes(&backend.profile.pack_max_size)?,
                    "pack exceeds profile size"
                );
                let upper =
                    pack.raw_size as usize + 8 + pack.member_count as usize * pack::MAX_MEMBER_TAG;
                let mut writer = crate::storage::pack_writer::Writer::new(
                    backend.store.backend.clone(),
                    crate::storage::location::Layout::S3.pack(pack.address()),
                    None,
                    upper,
                )
                .await?;
                writer.put(Bytes::from_static(pack::frame::HEADER)).await?;
                Some(writer)
            } else {
                None
            };
            let mut builder = pack::frame::Builder::new();
            for index in start..end {
                let target = &batch.chunks[index];
                let mut chunk = batch.chunk(backend.profile.storage_id, index)?;
                let cpu = self.cpu.clone().acquire_owned().await?;
                let hold = memory.clone();
                let keys = keys.clone();
                let pool = self.pool.clone();
                let (file, plain) = if let Some(origin) = origins.get(&chunk.location_id) {
                    ensure!(
                        origin.chunk.uuid == chunk.uuid
                            && origin.chunk.hash == chunk.hash
                            && origin.chunk.raw_size == chunk.raw_size
                            && pack::frame::Encoding::capture(&origin.chunk)?
                                == pack::frame::Encoding::capture(&chunk)?,
                        codec::IntegrityError::Metadata
                    );
                    let len = origin.chunk.file_size()?;
                    let bound = if origin.range.is_some() {
                        len - codec::FILE_HEADER.len()
                    } else {
                        len
                    };
                    self.gets.fetch_add(1, Ordering::Relaxed);
                    let data = crate::storage::read_retry::bounded(
                        origin.backend.store.backend.as_ref(),
                        &origin.path,
                        bound,
                        origin.range.clone(),
                        None,
                    )
                    .await?;
                    let mut file = if origin.range.is_some() {
                        let mut file = Vec::with_capacity(len);
                        file.extend_from_slice(codec::FILE_HEADER);
                        file.extend_from_slice(&data);
                        file
                    } else {
                        data
                    };
                    let current = chunk.clone();
                    let file = tokio::task::spawn_blocking(move || {
                        let (_cpu, _hold) = (cpu, hold);
                        codec::decode_read(&current, file.clone(), keys.as_ref(), 0, &pool, true)?;
                        Ok::<_, anyhow::Error>(Bytes::from(std::mem::take(&mut file)))
                    })
                    .await??;
                    (file, None)
                } else {
                    ensure!(
                        target.ready.is_none() && target.original.is_none(),
                        "missing immutable source encoding"
                    );
                    let (hash, plain) =
                        raw.get(&target.id).context("pack member body is missing")?;
                    ensure!(hash == &chunk.hash, codec::IntegrityError::Hash);
                    let raw = plain.clone();
                    let compress = target.compress;
                    let config = config.clone();
                    let (encoded, file) = tokio::task::spawn_blocking(move || {
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
                        let file = codec::seal(&chunk, body, keys.as_ref())?;
                        Ok::<_, anyhow::Error>((chunk, Bytes::from(file)))
                    })
                    .await??;
                    chunk = encoded;
                    (file, Some(plain.clone()))
                };
                let count = wire::encoded(
                    i64::from(chunk.stored_size.context("missing encoded size")?),
                    chunk.compressed,
                )?;
                result.chunks.push(target.ready.unwrap_or(count));
                if let Some(writer) = &mut writer {
                    builder.append(&chunk, &file)?;
                    writer.put(file.slice(codec::FILE_HEADER.len()..)).await?;
                } else if target.ready.is_none() {
                    backend
                        .store
                        .backend
                        .put(
                            &crate::storage::location::Layout::S3.chunk(target.location),
                            file.into(),
                        )
                        .await?;
                }
                if let Some(raw) = plain {
                    cached.push((
                        if pack.is_some() {
                            chunk.uuid
                        } else {
                            target.location
                        },
                        raw,
                    ));
                }
            }
            if let (Some(pack), Some(writer)) = (pack.take(), writer) {
                let (size, digest) = builder.finish(pack.raw_size, pack.member_count)?;
                writer.finish().await?;
                result.packs.push((
                    attempts[&(start, end)],
                    wire::encoded(i64::try_from(size)?, false)?,
                    STANDARD.encode(digest.as_bytes()),
                ));
            }
        }
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
