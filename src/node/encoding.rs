use super::*;
use crate::model::node_work::{Outcome, Rewrite};

impl Reader {
    pub(crate) async fn rewrite(
        &self,
        backend: Arc<Backend>,
        keys: Arc<BTreeMap<Uuid, codec::Key>>,
        item: Rewrite,
    ) -> Result<Outcome> {
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
                    codec::MAX + 16,
                    Path::from(crate::storage::chunk_name(*location)),
                    Path::from(crate::storage::chunk_name(target.location_id)),
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
                    Path::from(format!("packs/{}", crate::storage::chunk_name(*location))),
                    Path::from(format!(
                        "packs/{}",
                        crate::storage::chunk_name(target.location_id)
                    )),
                )
            }
        };
        let units = (bound as u64 * 4 + self.workspace.max(config.workspace_bytes()?))
            .div_ceil(1024 * 1024);
        ensure!(
            units <= u64::from(self.pack_units),
            "rewrite exceeds node memory budget"
        );
        let memory = Arc::new(self.acquire(&self.packs, units as u32).await?);
        let _io = self.acquire(&self.io, 1).await?;
        backend.store.verify().await?;
        self.gets.fetch_add(1, Ordering::Relaxed);
        let bytes = read_bounded(backend.store.backend.as_ref(), &source_path, bound).await?;
        let cpu = self.cpu.clone().acquire_owned().await?;
        let pool = self.pool.clone();
        let (outcome, encoded, _memory) = tokio::task::spawn_blocking(move || {
            let _cpu = cpu;
            let (outcome, encoded) = match item {
                Rewrite::Chunk {
                    source, mut target, ..
                } => {
                    let (raw, _) =
                        codec::decode_read(&source, bytes, keys.as_ref(), 0, &pool, true)?;
                    let compressed = pool.compress_for(&raw, source.compressed, &config)?;
                    target.compressed = compressed.is_some();
                    let encoded = compressed.unwrap_or_else(|| raw.to_vec());
                    target.stored_size = Some(i32::try_from(
                        encoded.len() + if target.algorithm == "none" { 0 } else { 16 },
                    )?);
                    let encoded = codec::seal(&target, encoded, keys.as_ref())?;
                    (
                        Outcome::Encoded {
                            size: encoded.len() as i64,
                            compressed: target.compressed,
                            digest: None,
                        },
                        encoded,
                    )
                }
                Rewrite::Pack {
                    source,
                    target,
                    members,
                    ..
                } => {
                    let decoded = pack::decode(&source, bytes, keys.as_ref(), &pool)?;
                    ensure!(
                        decoded.members.len() == members.len(),
                        "pack members changed"
                    );
                    let mut data = Vec::with_capacity(members.len());
                    for ((id, offset, size, hash), member) in
                        members.into_iter().zip(&decoded.members)
                    {
                        ensure!(
                            id == crate::model::Id::Uuid(member.id)
                                && size as usize == member.len
                                && hash.as_slice() == member.hash
                                && member.start
                                    == 12
                                        + pack::MEMBER_SIZE * decoded.members.len()
                                        + offset as usize,
                            "pack member metadata changed"
                        );
                        let chunk = codec::Chunk {
                            id,
                            uuid: member.id,
                            encoding_id: crate::model::Id::default(),
                            location_id: Uuid::nil(),
                            storage_id: target.storage_id,
                            hash,
                            raw_size: size,
                            stored_size: None,
                            algorithm: target.algorithm.clone(),
                            block_key_id: target.block_key_id,
                            compressed: false,
                            nonce: None,
                            format: 2,
                            state: "ready".into(),
                            created_at: target.created_at,
                        };
                        data.push((
                            chunk,
                            decoded.bytes.slice(member.start..member.start + member.len),
                        ));
                    }
                    let (target, encoded) = pack::encode(
                        target,
                        &data,
                        keys.as_ref(),
                        &pool,
                        config.strategy,
                        config.enabled,
                        Some(&config),
                    )?;
                    (
                        Outcome::Encoded {
                            size: encoded.len() as i64,
                            compressed: target.compressed,
                            digest: target.digest,
                        },
                        encoded,
                    )
                }
            };
            Ok::<_, anyhow::Error>((outcome, encoded, memory))
        })
        .await??;
        backend
            .store
            .backend
            .put(&output_path, Bytes::from(encoded).into())
            .await?;
        Ok(outcome)
    }
}
