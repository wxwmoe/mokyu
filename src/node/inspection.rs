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
    ) -> Result<Outcome> {
        let _io = self.io.clone().acquire_owned().await?;
        let timeout = crate::config::seconds(&backend.profile.integrity_batch_timeout)?;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(timeout);
        // Always recheck the namespace; a cached identity is insufficient evidence of corruption.
        if !matches!(
            tokio::time::timeout_at(deadline, backend.store.verify()).await,
            Ok(Ok(()))
        ) {
            return Ok(Outcome::Retry);
        }
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
                file.verify(&raw).await?;
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
            let units = (bound as u64 * 4 + self.workspace).div_ceil(1024 * 1024);
            ensure!(
                units <= u64::from(self.pack_units),
                "source exceeds node memory budget"
            );
            let memory = self.packs.clone().acquire_many_owned(units as u32).await?;
            self.gets.fetch_add(1, Ordering::Relaxed);
            let bytes = read_bounded(backend.store.backend.as_ref(), &path, bound).await?;
            let cpu = self.cpu.clone().acquire_owned().await?;
            let pool = self.pool.clone();
            tokio::task::spawn_blocking(move || {
                let (_cpu, _memory) = (cpu, memory);
                match snapshot.physical {
                    PhysicalSource::Chunk(chunk) => {
                        codec::decode_read(&chunk, bytes, keys.as_ref(), 0, &pool, true)?;
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
                        }
                    }
                    PhysicalSource::Raw(_) => unreachable!(),
                }
                Ok::<_, anyhow::Error>(())
            })
            .await?
        })
        .await;
        Ok(match result {
            Ok(Ok(())) => Outcome::Inspected { code: None },
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
                match code.filter(|code| crate::model::source::quarantine_code(code)) {
                    Some(code) => Outcome::Inspected {
                        code: Some(code.into()),
                    },
                    None => Outcome::Retry,
                }
            }
            Err(_) => Outcome::Retry,
        })
    }
}
