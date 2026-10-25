use super::*;
use crate::model::node_work::{Outcome, RawHash};
use md5::{Digest, Md5};

impl Reader {
    pub(crate) async fn hash_raw(&self, backend: &Backend, item: RawHash) -> Result<Outcome> {
        let source = item.source;
        source.validate()?;
        ensure!(
            source.medium == "shared_s3" && !source.pending_upload,
            codec::IntegrityError::RawMetadata
        );
        if let Some(hash) = item.hash {
            ensure!(hash.len() == 32, codec::IntegrityError::RawMetadata);
            return Ok(Outcome::Hashed { hash });
        }
        let _io = self.io.clone().acquire_owned().await?;
        let _memory = self.packs.clone().acquire_owned().await?;
        backend.store.verify().await?;
        let _cpu = self.cpu.clone().acquire_owned().await?;
        let mut file = RawFile::remote(
            backend.store.backend.clone(),
            &source,
            self.gets.clone(),
            self.diagnostics.clone(),
        )
        .await?;
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
        }
        ensure!(size == source.size as u64, codec::IntegrityError::Length);
        ensure!(
            source.digest.as_deref() == Some(digest.finalize().as_slice()),
            codec::IntegrityError::Hash
        );
        Ok(Outcome::Hashed {
            hash: hash.finalize().as_bytes().to_vec(),
        })
    }
}
