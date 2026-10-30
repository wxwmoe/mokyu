use super::*;
use crate::model::node_work::{Outcome, RawHash};
use md5::{Digest, Md5};

impl Reader {
    pub(crate) async fn scan_transfer(
        &self,
        backend: &Backend,
        plan: &crate::model::transfer::Scan,
    ) -> Result<crate::model::transfer::Scanned> {
        plan.source.validate()?;
        ensure!(
            plan.source.medium == "shared_s3"
                && !plan.source.pending_upload
                && plan.source.storage_id == backend.profile.storage_id,
            codec::IntegrityError::RawMetadata
        );
        let resources = crate::content::transfer::Resources {
            cpu: self.cpu.clone(),
            memory: Arc::new(
                self.acquire(
                    &self.packs,
                    (crate::model::transfer::PROOF_BYTES / (1024 * 1024) + 1) as u32,
                )
                .await?,
            ),
        };
        let _io = self.acquire(&self.io, 1).await?;
        backend.store.verify().await?;
        let mut file = RawFile::remote(
            backend.store.backend.clone(),
            &plan.source,
            self.gets.clone(),
            self.diagnostics.clone(),
        )
        .await?;
        file.seek(std::io::SeekFrom::Start(plan.checksum.object.offset))
            .await?;
        crate::content::transfer::scan(plan, &mut file, &resources).await
    }

    pub(crate) async fn hash_raw(
        &self,
        backend: &Backend,
        item: RawHash,
    ) -> Result<(Outcome, crate::storage::cache::Fills)> {
        let source = item.source;
        source.validate()?;
        ensure!(
            source.medium == "shared_s3" && !source.pending_upload,
            codec::IntegrityError::RawMetadata
        );
        if let Some(hash) = item.hash {
            ensure!(hash.len() == 32, codec::IntegrityError::RawMetadata);
            return Ok((Outcome::Hashed { hash }, Vec::new()));
        }
        let _memory = self.acquire(&self.packs, 1).await?;
        let _io = self.acquire(&self.io, 1).await?;
        backend.store.verify().await?;
        let _cpu = self.cpu.clone().try_acquire_owned()?;
        let mut file = RawFile::remote(
            backend.store.backend.clone(),
            &source,
            self.gets.clone(),
            self.diagnostics.clone(),
        )
        .await?;
        let mut cache = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            self.files.begin_raw(&source, &backend.profile),
        )
        .await
        .ok()
        .and_then(Result::ok)
        .flatten();
        let mut hash = blake3::Hasher::new();
        let mut digest = Md5::new();
        let mut buffer = vec![0; RAW_BUFFER];
        let mut size = 0u64;
        loop {
            let count = file.read(&mut buffer).await?;
            if count == 0 {
                break;
            }
            size += count as u64;
            ensure!(size <= source.size as u64, codec::IntegrityError::Length);
            hash.update(&buffer[..count]);
            digest.update(&buffer[..count]);
            crate::storage::raw_cache::append(&mut cache, &buffer[..count]).await;
        }
        ensure!(size == source.size as u64, codec::IntegrityError::Length);
        ensure!(
            source.digest.as_deref() == Some(digest.finalize().as_slice()),
            codec::IntegrityError::Hash
        );
        let fills = cache
            .and_then(|cache| self.files.prepare_raw(cache, &source))
            .into_iter()
            .collect();
        Ok((
            Outcome::Hashed {
                hash: hash.finalize().as_bytes().to_vec(),
            },
            fills,
        ))
    }
}
