use super::*;
use crate::model::transfer::{Input, RawCopy, RawProgress};
use md5::Digest;

impl Reader {
    pub(crate) async fn copy_raw_transfer<F, Fut>(
        &self,
        source: Arc<Backend>,
        target: Arc<Backend>,
        keys: Arc<BTreeMap<Uuid, codec::Key>>,
        plan: &RawCopy,
        checkpoint: F,
    ) -> Result<(Vec<u8>, crate::storage::cache::Fills)>
    where
        F: Fn(RawProgress) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        plan.validate()?;
        ensure!(
            plan.input.storage()? == source.profile.storage_id
                && plan.target.storage_id == target.profile.storage_id
                && plan.target.medium == "shared_s3",
            "raw transfer storage differs"
        );
        let payload = match &plan.input {
            Input::Pack { pack, .. } => pack.payload_size()? as u64 + 16,
            Input::Chunk { chunk } => chunk.raw_size as u64 + 16,
            Input::Raw { .. } => crate::model::transfer::PROOF_BYTES as u64,
        };
        let units = (payload.saturating_mul(4) + self.workspace).div_ceil(1024 * 1024);
        ensure!(
            units <= u64::from(self.pack_units),
            "raw transfer exceeds node memory budget"
        );
        let memory = Arc::new(self.acquire(&self.packs, units as u32).await?);
        let _io = self.acquire(&self.io, 1).await?;
        source.store.verify().await?;
        target.store.verify().await?;
        let (mut file, digest) = if let Input::Raw { source: raw } = &plan.input {
            ensure!(
                raw.medium == "shared_s3" && !raw.pending_upload,
                codec::IntegrityError::RawMetadata
            );
            (
                RawFile::remote(
                    source.store.backend.clone(),
                    raw,
                    self.gets.clone(),
                    self.diagnostics.clone(),
                )
                .await?,
                raw.digest
                    .as_deref()
                    .context("raw source digest missing")?
                    .try_into()?,
            )
        } else {
            let data = self
                .transfer_encoded(&source, keys, &plan.input, &memory)
                .await?;
            let hold = memory.clone();
            let cpu = self.cpu.clone().acquire_owned().await?;
            let size = plan.target.size as usize;
            let (bytes, digest, _memory) = tokio::task::spawn_blocking(move || {
                let _cpu = cpu;
                let mut bytes = Vec::with_capacity(size);
                for (_, data) in data {
                    bytes.extend_from_slice(&data);
                }
                ensure!(bytes.len() == size, "raw transfer source length differs");
                let digest: [u8; 16] = md5::Md5::digest(&bytes).into();
                Ok::<_, anyhow::Error>((Bytes::from(bytes), digest, hold))
            })
            .await??;
            (
                RawFile::Memory {
                    data: bytes,
                    offset: 0,
                },
                digest,
            )
        };
        let mut fill = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            self.files.begin_raw(&plan.target, &target.profile),
        )
        .await
        .ok()
        .and_then(Result::ok)
        .flatten();
        let destination = crate::storage::transfer::Target::Remote {
            backend: target.store.backend.clone(),
            streaming: target
                .store
                .streaming
                .as_ref()
                .context("S3 streaming backend missing")?,
            multipart: target
                .store
                .multipart
                .as_deref()
                .context("S3 multipart backend missing")?,
        };
        crate::storage::transfer::copy(destination, plan, digest, &mut file, &mut fill, checkpoint)
            .await?;
        let fills = fill
            .and_then(|fill| self.files.prepare_raw(fill, &plan.target))
            .into_iter()
            .collect();
        Ok((digest.to_vec(), fills))
    }
}
