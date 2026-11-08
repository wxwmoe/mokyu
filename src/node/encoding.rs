use super::*;
use crate::model::node_work::{Outcome, Rewrite};

impl Reader {
    pub(crate) async fn rewrite(
        &self,
        backend: Arc<Backend>,
        origin: Arc<Backend>,
        keys: Arc<BTreeMap<Uuid, codec::Key>>,
        item: Rewrite,
    ) -> Result<(Outcome, crate::storage::cache::Fills)> {
        let config = backend.profile.compression_config("0s");
        let (bound, source_path, output_path) = match &item {
            Rewrite::Chunk {
                location,
                source,
                target,
            } => {
                ensure!(
                    *location == source.location_id
                        && source.id == target.id
                        && source.hash == target.hash
                        && source.raw_size == target.raw_size
                        && source.location_id != target.location_id,
                    "invalid chunk rewrite"
                );
                codec::validate_metadata(source, keys.as_ref())?;
                (
                    codec::MAX_FILE,
                    crate::storage::location::Layout::S3.chunk(source.address()),
                    crate::storage::location::Layout::S3.chunk(target.location_id),
                )
            }
            Rewrite::Pack {
                location,
                source,
                target,
                members,
                ..
            } => {
                ensure!(
                    *location == source.location_id
                        && source.id != target.id
                        && source.raw_size == target.raw_size
                        && source.member_count == target.member_count,
                    "invalid pack rewrite"
                );
                source.validate(keys.as_ref())?;
                crate::model::source::manifest_digest(source, members)?;
                (
                    source.payload_size()? + 16,
                    crate::storage::location::Layout::S3.pack(source.address()),
                    crate::storage::location::Layout::S3.pack(target.location_id),
                )
            }
        };
        let _io = self.acquire(&self.io, 1).await?;
        backend.store.verify().await?;
        if backend.profile.storage_id != origin.profile.storage_id {
            origin.store.verify().await?;
        }
        if let Rewrite::Pack {
            source,
            target,
            members,
            replacements,
            ..
        } = &item
        {
            use crate::storage::pack_writer;
            let upper = u64::try_from(target.raw_size)?
                + 8
                + u64::try_from(target.member_count)? * pack::MAX_MEMBER_TAG as u64;
            let memory = self.pack_buffer_budget(&backend, 0, upper).await?;
            let last = replacements
                .last()
                .context(codec::IntegrityError::Metadata)?;
            let expected = last
                .position
                .offset
                .checked_add(u64::try_from(last.position.encoding.stored_size)?)
                .context(codec::IntegrityError::Length)?;
            pack::frame::validate_entries(
                replacements,
                expected,
                target.raw_size,
                target.member_count,
                true,
            )?;
            ensure!(
                members.len() == replacements.len(),
                codec::IntegrityError::Metadata
            );
            let files = pack_writer::members(
                origin.store.backend.clone(),
                source_path,
                source,
                members,
                self.gets.clone(),
                self.diagnostics.clone(),
            );
            futures_util::pin_mut!(files);
            let mut writer = pack_writer::Writer::new(
                backend.store.backend.clone(),
                output_path,
                None,
                usize::try_from(upper)?,
            )
            .await?;
            writer.put(Bytes::from_static(pack::frame::HEADER)).await?;
            let mut builder = pack::frame::Builder::new();
            for (before, after) in members.iter().zip(replacements) {
                let data = files
                    .try_next()
                    .await?
                    .context(codec::IntegrityError::Length)?;
                let (old, new, src, dst) = (
                    before.clone(),
                    after.clone(),
                    source.clone(),
                    target.clone(),
                );
                let cpu = self.cpu.clone().acquire_owned().await?;
                let held = memory.clone();
                let keys = keys.clone();
                let pool = self.pool.clone();
                let (chunk, file) = tokio::task::spawn_blocking(move || {
                    let (_cpu, _memory) = (cpu, held);
                    pack::frame::recode_member(&src, &dst, &old, &new, data, keys.as_ref(), &pool)
                })
                .await??;
                let (position, _) = builder.append(&chunk, &file)?;
                ensure!(position == after.position, codec::IntegrityError::Metadata);
                writer.put(file.slice(codec::FILE_HEADER.len()..)).await?;
            }
            ensure!(
                files.try_next().await?.is_none(),
                codec::IntegrityError::Length
            );
            let (size, digest) = builder.finish(target.raw_size, target.member_count)?;
            ensure!(size == expected, codec::IntegrityError::Length);
            writer.finish().await?;
            return Ok((
                Outcome::Encoded {
                    size: i64::try_from(size)?,
                    compressed: false,
                    digest: Some(digest.as_bytes().to_vec()),
                },
                Vec::new(),
            ));
        }
        let units = (bound as u64 * 4 + self.workspace.max(config.workspace_bytes()?))
            .div_ceil(1024 * 1024);
        ensure!(
            units <= u64::from(self.pack_units),
            "rewrite exceeds node memory budget"
        );
        let memory = Arc::new(self.acquire(&self.packs, units as u32).await?);
        self.gets.fetch_add(1, Ordering::Relaxed);
        let bytes = read_bounded(origin.store.backend.as_ref(), &source_path, bound).await?;
        let cpu = self.cpu.clone().acquire_owned().await?;
        let pool = self.pool.clone();
        let files = self.files.clone();
        let profile = backend.profile.clone();
        let (outcome, encoded, fills, _memory) = tokio::task::spawn_blocking(move || {
            let _cpu = cpu;
            let mut fills = Vec::new();
            let (outcome, encoded) = match item {
                Rewrite::Chunk {
                    source, mut target, ..
                } => {
                    let (raw, _) =
                        codec::decode_read(&source, bytes, keys.as_ref(), 0, &pool, true)?;
                    fills.extend(
                        crate::storage::resource_gate::PRIORITY
                            .sync_scope(crate::storage::resource_gate::MAINTENANCE, || {
                                files.prepare_fill(target.location_id, &raw, false, &profile)
                            }),
                    );
                    let compressed = pool.compress_for(&raw, source.compressed, &config)?;
                    target.compressed = compressed.is_some();
                    let encoded = compressed.unwrap_or_else(|| raw.to_vec());
                    target.stored_size = Some(i32::try_from(
                        encoded.len() + if target.algorithm == "none" { 0 } else { 16 },
                    )?);
                    let encoded = codec::seal(&target, encoded, keys.as_ref())?;
                    (
                        Outcome::Encoded {
                            size: i64::from(
                                target.stored_size.context("missing encoded chunk size")?,
                            ),
                            compressed: target.compressed,
                            digest: None,
                        },
                        encoded,
                    )
                }
                Rewrite::Pack { .. } => unreachable!(),
            };
            Ok::<_, anyhow::Error>((outcome, encoded, fills, memory))
        })
        .await??;
        backend
            .store
            .backend
            .put(&output_path, Bytes::from(encoded).into())
            .await?;
        Ok((outcome, fills))
    }
}
