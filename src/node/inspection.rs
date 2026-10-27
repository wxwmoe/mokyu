use super::*;
use crate::model::{
    PhysicalSource,
    node_work::{Member, Outcome},
    source::SourceSnapshot,
};

impl Reader {
    pub(crate) async fn inspect(
        &self,
        backend: Arc<Backend>,
        keys: Arc<BTreeMap<Uuid, codec::Key>>,
        snapshot: SourceSnapshot,
        members: Vec<Member>,
        mode: crate::worker::integrity::Mode,
    ) -> Result<(Outcome, crate::storage::cache::Fills)> {
        let _io = self.acquire(&self.io, 1).await?;
        let timeout = crate::config::seconds(&backend.profile.integrity_batch_timeout)?;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(timeout);
        // Always recheck the namespace; a cached identity is insufficient evidence of corruption.
        if !matches!(
            tokio::time::timeout_at(deadline, backend.store.verify()).await,
            Ok(Ok(()))
        ) {
            return Ok((Outcome::Retry, Vec::new()));
        }
        let mut fills = Vec::new();
        let result = tokio::time::timeout_at(deadline, async {
            if let PhysicalSource::Raw(raw) = &snapshot.physical {
                let mut raw = raw.clone();
                raw.quarantine_code = None;
                ensure!(
                    raw.medium == "shared_s3" && !raw.pending_upload,
                    codec::IntegrityError::RawMetadata
                );
                let _cpu = self.cpu.clone().acquire_owned().await?;
                let mut file = RawFile::remote(
                    backend.store.backend.clone(),
                    &raw,
                    self.gets.clone(),
                    self.diagnostics.clone(),
                )
                .await?;
                if mode == crate::worker::integrity::Mode::Full {
                    let mut cache = tokio::time::timeout(
                        std::time::Duration::from_millis(100),
                        self.files.begin_raw(&raw, &backend.profile),
                    )
                    .await
                    .ok()
                    .and_then(Result::ok)
                    .flatten();
                    crate::storage::raw::verify_file(&mut file, &raw, Some(&mut cache)).await?;
                    if let Some(cache) = cache {
                        fills.extend(self.files.prepare_raw(cache, &raw));
                    }
                }
                return Ok::<_, anyhow::Error>(());
            }
            let (bound, path) = match &snapshot.physical {
                PhysicalSource::Chunk(chunk) => {
                    codec::validate_metadata(chunk, keys.as_ref())?;
                    (
                        codec::MAX + 16,
                        Path::from(crate::storage::chunk_name(chunk.location_id)),
                    )
                }
                PhysicalSource::Pack(pack) => {
                    pack.validate(keys.as_ref())?;
                    ensure!(
                        Some(crate::model::source::manifest_digest(pack, &members)?)
                            == snapshot.manifest_digest,
                        codec::IntegrityError::Metadata
                    );
                    (
                        pack.payload_size()? + 16,
                        Path::from(format!(
                            "packs/{}",
                            crate::storage::chunk_name(pack.location_id)
                        )),
                    )
                }
                PhysicalSource::Raw(_) => unreachable!(),
            };
            if mode == crate::worker::integrity::Mode::Head {
                self.gets.fetch_add(1, Ordering::Relaxed);
                let meta = backend.store.backend.head(&path).await?;
                let expected = match &snapshot.physical {
                    PhysicalSource::Chunk(chunk) => chunk.stored_size.map(i64::from),
                    PhysicalSource::Pack(pack) => pack.stored_size,
                    PhysicalSource::Raw(_) => unreachable!(),
                };
                ensure!(
                    expected == Some(i64::try_from(meta.size)?),
                    codec::IntegrityError::Length
                );
                return Ok(());
            }
            let units = (bound as u64 * 4 + self.workspace).div_ceil(1024 * 1024);
            ensure!(
                units <= u64::from(self.pack_units),
                "source exceeds node memory budget"
            );
            let memory = self.acquire(&self.packs, units as u32).await?;
            self.gets.fetch_add(1, Ordering::Relaxed);
            let bytes = read_bounded(backend.store.backend.as_ref(), &path, bound).await?;
            let cpu = self.cpu.clone().acquire_owned().await?;
            let pool = self.pool.clone();
            let files = self.files.clone();
            let profile = backend.profile.clone();
            let candidates = tokio::task::spawn_blocking(move || {
                let (_cpu, _memory) = (cpu, memory);
                let mut fills = Vec::new();
                match snapshot.physical {
                    PhysicalSource::Chunk(chunk) => {
                        let (raw, _) =
                            codec::decode_read(&chunk, bytes, keys.as_ref(), 0, &pool, true)?;
                        fills.extend(
                            crate::storage::resource_gate::PRIORITY
                                .sync_scope(crate::storage::resource_gate::MAINTENANCE, || {
                                    files.prepare_fill(chunk.location_id, &raw, false, &profile)
                                }),
                        );
                    }
                    PhysicalSource::Pack(pack) => {
                        let decoded = pack::decode(&pack, bytes, keys.as_ref(), &pool)?;
                        ensure!(
                            decoded.members.len() == members.len(),
                            codec::IntegrityError::Metadata
                        );
                        for ((id, offset, size, hash), member) in
                            members.iter().zip(&decoded.members)
                        {
                            ensure!(
                                *id == crate::model::Id::Uuid(member.id)
                                    && hash.as_slice() == member.hash
                                    && *size as usize == member.len
                                    && member.start
                                        == 12
                                            + pack::MEMBER_SIZE * members.len()
                                            + *offset as usize,
                                codec::IntegrityError::Metadata
                            );
                            fills.extend(crate::storage::resource_gate::PRIORITY.sync_scope(
                                crate::storage::resource_gate::MAINTENANCE,
                                || {
                                    files.prepare_fill(
                                        member.id,
                                        &decoded.bytes[member.start..member.start + member.len],
                                        false,
                                        &profile,
                                    )
                                },
                            ));
                        }
                    }
                    PhysicalSource::Raw(_) => unreachable!(),
                }
                Ok::<_, anyhow::Error>(fills)
            })
            .await??;
            fills.extend(candidates);
            Ok(())
        })
        .await;
        let outcome = match result {
            Ok(Ok(())) => Outcome::Inspected { code: None },
            Ok(Err(error)) if super::super::work::busy(&error) => return Err(error),
            Ok(Err(error)) => {
                let code = error
                    .downcast_ref::<codec::IntegrityError>()
                    .map(|error| error.code())
                    .or_else(|| {
                        matches!(
                            error.downcast_ref::<object_store::Error>(),
                            Some(object_store::Error::NotFound { .. })
                        )
                        .then_some("remote_missing")
                    });
                match code {
                    Some(code) => Outcome::Inspected {
                        code: Some(code.into()),
                    },
                    None => Outcome::Retry,
                }
            }
            Err(_) => Outcome::Retry,
        };
        if !matches!(outcome, Outcome::Inspected { code: None }) {
            fills.clear();
        }
        Ok((outcome, fills))
    }
}
