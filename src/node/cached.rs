use super::*;
use crate::model::{node_work::Outcome, raw::RawSource};

impl Writes {
    pub(super) async fn raw_multipart(
        &self,
        operation: &Operation,
        file: &crate::storage::raw_request::File,
        source: Uuid,
        size: u64,
        io: Arc<OwnedSemaphorePermit>,
        grants: Vec<Arc<Grant>>,
    ) -> Result<()> {
        use md5::{Digest, Md5};
        struct UploadGuard {
            backend: Arc<dyn object_store::multipart::MultipartStore>,
            key: Path,
            upload: Option<String>,
        }
        impl Drop for UploadGuard {
            fn drop(&mut self) {
                if let Some(upload) = self.upload.take() {
                    let backend = self.backend.clone();
                    let key = self.key.clone();
                    tokio::spawn(async move {
                        let _ = backend.abort_multipart(&key, &upload).await;
                    });
                }
            }
        }
        let backend = operation
            .backend
            .store
            .multipart
            .as_ref()
            .context("S3 multipart backend missing")?;
        let streaming = operation
            .backend
            .store
            .streaming
            .as_ref()
            .context("S3 streaming upload missing")?;
        let key = crate::storage::raw::remote_path(source);
        let mut upload = UploadGuard {
            backend: backend.clone(),
            upload: Some(backend.create_multipart(&key).await?),
            key,
        };
        let part_size =
            (64 * 1024 * 1024).max(size.div_ceil(10000).div_ceil(1024 * 1024) * 1024 * 1024);
        let mut parts = Vec::new();
        let mut pages = Box::<crate::content::raw_pages::Builder>::default();
        let mut offset = 0;
        while offset < size {
            let end = (offset + part_size).min(size);
            let mut digest = Md5::new();
            let mut input = file.replay_range(offset..end);
            while let Some(bytes) = input.next().await {
                let bytes = bytes?;
                digest.update(&bytes);
                let mut piece = bytes.as_ref();
                while let Some(group) = pages.feed(&mut piece) {
                    self.reads
                        .node
                        .client
                        .stage_raw_pages(wire::StageNativePagesRequest {
                            run: Some(operation.request.run.run.clone()),
                            operation_id: operation.id.clone(),
                            group: group.index as u64,
                            hashes: group.hashes,
                        })
                        .await?;
                }
            }
            let digest: [u8; 16] = digest.finalize().into();
            let part = streaming
                .part(
                    &upload.key,
                    upload.upload.as_ref().unwrap(),
                    parts.len(),
                    end - offset,
                    &digest,
                    || {
                        file.replay_range(offset..end)
                            .map_ok(|bytes| {
                                Bytes::from_owner(Upload {
                                    bytes,
                                    _memory: operation.request.memory.clone(),
                                    _io: io.clone(),
                                    _grants: grants.clone(),
                                })
                            })
                            .boxed()
                    },
                )
                .await?;
            parts.push((parts.len() as i32 + 1, part.content_id));
            offset = end;
        }
        if let Some(group) = pages.finish() {
            self.reads
                .node
                .client
                .stage_raw_pages(wire::StageNativePagesRequest {
                    run: Some(operation.request.run.run.clone()),
                    operation_id: operation.id.clone(),
                    group: group.index as u64,
                    hashes: group.hashes,
                })
                .await?;
        }
        ensure!(
            streaming
                .complete(&upload.key, upload.upload.as_ref().unwrap(), &parts)
                .await?,
            "S3 multipart completion rejected"
        );
        upload.upload = None;
        Ok(())
    }

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
        let path = crate::storage::cache::CacheFile::PendingRaw.path(
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
            let _cpu = self.reads.reader.cpu.clone().try_acquire_owned()?;
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
                &crate::storage::raw::remote_path(source.address()),
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
