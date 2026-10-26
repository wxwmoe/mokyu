use super::*;
use crate::model::{node_work::Outcome, raw::RawSource};

impl Writes {
    pub(crate) async fn flush_raw(
        &self,
        backend: &super::super::body::Backend,
        source: &RawSource,
        hashing: bool,
        memory: Arc<OwnedSemaphorePermit>,
        io: Arc<OwnedSemaphorePermit>,
        grants: Vec<Arc<Grant>>,
    ) -> Result<Outcome> {
        source.validate()?;
        ensure!(
            source.medium == "local"
                && source.pending_upload
                && backend.profile.storage_id == source.storage_id,
            codec::IntegrityError::RawMetadata
        );
        self.reads
            .reader
            .files
            .recover_uploads(&self.reads.node)
            .await?;
        let path = crate::storage::cache::CacheFile::Raw.path(
            &self.reads.node.config.instance.data_path,
            source.location_id,
        );
        ensure!(
            tokio::fs::symlink_metadata(&path).await?.is_file(),
            "raw upload is not a regular file"
        );
        let file = tokio::fs::File::open(&path).await?;
        ensure!(
            file.metadata().await?.len() == source.size as u64,
            codec::IntegrityError::Length
        );
        let digest: &[u8; 16] = source
            .digest
            .as_deref()
            .context("raw digest missing")?
            .try_into()
            .map_err(|_| codec::IntegrityError::RawMetadata)?;
        if hashing {
            let _cpu = self.reads.reader.cpu.clone().acquire_owned().await?;
            let mut file = crate::storage::raw::RawFile::Local(file);
            let hash =
                crate::storage::raw_upload::hash_file(&mut file, source.size as u64, digest, true)
                    .await?
                    .context("raw hash missing")?;
            return Ok(Outcome::Hashed {
                hash: hash.to_vec(),
            });
        }
        drop(file);
        #[cfg(feature = "fault-injection")]
        crate::faults::point("node-cache-before-upload").await;
        backend.store.ensure_verified().await?;
        let upload = backend
            .store
            .streaming
            .as_ref()
            .context("S3 streaming upload missing")?;
        upload
            .put(
                &crate::storage::raw::remote_path(source.location_id),
                source.size as u64,
                digest,
                || {
                    super::native::replay(path.clone(), source.size, hex::encode(digest))
                        .map_ok(|bytes| {
                            Bytes::from_owner(Upload {
                                bytes,
                                _memory: memory.clone(),
                                _io: io.clone(),
                                _grants: grants.clone(),
                            })
                        })
                        .boxed()
                },
            )
            .await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("node-cache-uploaded").await;
        Ok(Outcome::Uploaded)
    }
}
