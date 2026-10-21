use super::{Area, CacheFile, CachePin, Storage, UploadCharge, UploadPin};
use crate::model::{Id, raw::RawSource};
use anyhow::{Context, Result, ensure};
use md5::{Digest, Md5};
use std::sync::atomic::Ordering;

use uuid::Uuid;

pub(crate) enum Written {
    Final(Option<[u8; 32]>),
    Cached {
        revision: i64,
        deduplicate: bool,
        background: bool,
    },
    Reused,
}

pub(crate) struct Writer {
    storage: std::sync::Arc<Storage>,
    source: Id,
    location: Uuid,
    target: Uuid,
    file: Option<super::raw_request::File>,
    charge: Option<UploadCharge>,
    policy: super::CachePolicy,
    size: u64,
    revision: i64,
    deduplicate: bool,
    background: bool,
}

impl Writer {
    pub(super) async fn new(
        storage: &std::sync::Arc<Storage>,
        target: Uuid,
        source: Id,
        location: Uuid,
    ) -> Result<Self> {
        let profile = storage.profile(target)?;
        let policy = storage.local_cache(Some(target))?;
        let file = super::raw_request::File::create(storage, location, policy).await?;
        let charge = storage
            .disk
            .claim_upload_with_limit(0, target, policy.upload_capacity)
            .ok();
        Ok(Self {
            storage: storage.clone(),
            source,
            location,
            target,
            file: Some(file),
            charge,
            policy,
            size: 0,
            revision: profile.revision,
            deduplicate: profile.deduplication_enabled,
            background: profile.write_processing == "background",
        })
    }

    pub async fn write(&mut self, data: &[u8]) -> Result<()> {
        let size = self
            .size
            .checked_add(data.len() as u64)
            .context("raw receive length overflow")?;
        super::raw::remote_size(size)?;
        self.file
            .as_mut()
            .unwrap()
            .append(&self.storage, data)
            .await?;
        if let Some(charge) = self.charge.as_mut() {
            match self.storage.disk.claim_upload_with_limit(
                data.len() as u64,
                self.target,
                self.policy.upload_capacity,
            ) {
                Ok(mut added) => {
                    charge.size += added.size;
                    added.size = 0;
                }
                Err(_) => {
                    self.charge = None;
                    crate::upload::cache::FALLBACKS.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        self.size = size;
        Ok(())
    }

    pub async fn finish(mut self, stream: Id, expected: u64, digest: &[u8]) -> Result<Written> {
        ensure!(self.size == expected, "raw receive length mismatch");
        if let Some(charge) = self.charge.take() {
            let (file, published) = self
                .file
                .take()
                .unwrap()
                .publish(self.storage.clone(), self.source, self.location, charge)
                .await?;
            self.file = Some(file);
            if published {
                #[cfg(feature = "fault-injection")]
                crate::faults::point("raw-cache-durable").await;
                return Ok(Written::Cached {
                    revision: self.revision,
                    deduplicate: self.deduplicate,
                    background: self.background,
                });
            }
            crate::upload::cache::FALLBACKS.fetch_add(1, Ordering::Relaxed);
        }
        self.file.as_mut().unwrap().rewind().await?;
        let mut file = super::raw::RawFile::Local(
            self.file
                .as_ref()
                .unwrap()
                .file
                .as_ref()
                .unwrap()
                .try_clone()
                .await?,
        );
        let hash = hash_file(&mut file, expected, digest, self.deduplicate).await?;
        if let Some(hash) = hash {
            if self
                .storage
                .raw_store
                .reuse(
                    stream,
                    self.source,
                    self.target,
                    expected as i64,
                    digest,
                    &hash,
                )
                .await?
            {
                return Ok(Written::Reused);
            }
        }
        file.rewind().await?;
        self.storage
            .send_raw(
                self.target,
                self.source,
                self.location,
                &mut file,
                expected,
                digest,
                hash.as_ref(),
                None,
            )
            .await?;
        Ok(Written::Final(hash))
    }
}

pub(crate) async fn hash_file(
    file: &mut super::raw::RawFile,
    expected: u64,
    digest: &[u8],
    deduplicate: bool,
) -> Result<Option<[u8; 32]>> {
    let mut md5 = Md5::new();
    let mut hash = deduplicate.then(blake3::Hasher::new);
    let mut size = 0u64;
    let mut buffer = vec![0; 256 * 1024];
    loop {
        let count = file.read(&mut buffer).await?;
        if count == 0 {
            break;
        }
        size += count as u64;
        ensure!(
            size <= expected,
            crate::content::codec::IntegrityError::Length
        );
        md5.update(&buffer[..count]);
        if let Some(hash) = &mut hash {
            hash.update(&buffer[..count]);
        }
    }
    ensure!(
        size == expected,
        crate::content::codec::IntegrityError::Length
    );
    ensure!(
        md5.finalize().as_slice() == digest,
        crate::content::codec::IntegrityError::Hash
    );
    Ok(hash.map(|hash| *hash.finalize().as_bytes()))
}

impl Storage {
    pub(super) async fn restore_raw_uploads(&self) -> Result<()> {
        if self.cluster_mode {
            return Ok(());
        }
        let mut after = Id::default();
        loop {
            let rows = self.raw_store.uploads(after).await?;
            if rows.is_empty() {
                break;
            }
            for work in rows {
                after = work.source.id;
                if !work.source.pending_upload {
                    continue;
                }
                let source = work.source;
                let path = CacheFile::Raw.path(&self.disk.root, source.location_id);
                let metadata=tokio::fs::symlink_metadata(&path).await.context("pending raw upload is missing; preserve the raw cache and restore its durable source")?;
                ensure!(
                    metadata.is_file() && metadata.len() == source.size as u64,
                    "pending raw upload is incomplete; preserve the raw cache and restore its durable source"
                );
                let size = metadata.len();
                self.disk.account_existing(Area::Cache, size);
                let mut fifo = self.fifo.lock().await;
                fifo.insert(source.location_id, size, CacheFile::Raw);
                let pins = fifo.entries[&source.location_id].pins.clone();
                pins.fetch_add(1, Ordering::Relaxed);
                self.raw_upload_pins.lock().unwrap().insert(
                    source.location_id,
                    (
                        source.id,
                        UploadPin {
                            _pin: Some(CachePin(pins)),
                            _charge: Some(self.disk.restore_upload(source.storage_id, size)),
                            size,
                            compressed: false,
                            remote: false,
                            staging_storage_id: None,
                            staging_object_key: source.location_id.to_string(),
                        },
                    ),
                );
            }
        }
        Ok(())
    }

    pub(crate) async fn reconcile_raw_uploads(&self) -> Result<()> {
        let mut after = Id::default();
        let mut retained = std::collections::HashSet::new();
        loop {
            let rows = self.raw_store.uploads(after).await?;
            if rows.is_empty() {
                break;
            }
            for work in rows {
                after = work.source.id;
                if work.source.pending_upload {
                    retained.insert(work.source.location_id);
                }
            }
        }
        let unregistered: Vec<_> = self
            .raw_upload_pins
            .lock()
            .unwrap()
            .iter()
            .filter(|(id, _)| !retained.contains(id))
            .map(|(id, (source, _))| (*id, *source))
            .collect();
        for (location, source) in unregistered {
            if self
                .raw_store
                .current(source)
                .await?
                .is_some_and(|s| s.state == "writing")
                || self.raw_store.has_upload(source).await?
            {
                retained.insert(location);
            }
        }
        self.raw_upload_pins
            .lock()
            .unwrap()
            .retain(|id, _| retained.contains(id));
        Ok(())
    }

    pub(crate) async fn release_finished_raw(&self, source: Id, location: Uuid) -> Result<()> {
        if !self.raw_store.has_upload(source).await?
            || self
                .raw_store
                .current(source)
                .await?
                .is_none_or(|source| !source.pending_upload)
        {
            self.raw_upload_pins.lock().unwrap().remove(&location);
        }
        Ok(())
    }

    pub(super) async fn pending_raw(
        &self,
        source: &RawSource,
        verify: bool,
    ) -> Result<super::raw::RawFile> {
        let pin = {
            let mut fifo = self.fifo.lock().await;
            let entry = fifo
                .entries
                .get_mut(&source.location_id)
                .context("pending raw upload is missing")?;
            ensure!(
                entry.file == CacheFile::Raw && entry.pins.load(Ordering::Relaxed) != usize::MAX,
                "pending raw upload is unavailable"
            );
            entry.pins.fetch_add(1, Ordering::Relaxed);
            CachePin(entry.pins.clone())
        };
        let path = CacheFile::Raw.path(&self.disk.root, source.location_id);
        ensure!(
            tokio::fs::symlink_metadata(&path).await?.is_file(),
            "pending raw upload was replaced"
        );
        let file = tokio::fs::File::open(&path).await?;
        ensure!(
            file.metadata().await?.len() == source.size as u64,
            crate::content::codec::IntegrityError::Length
        );
        let mut file = super::raw::RawFile::Cached { file, _pin: pin };
        if verify {
            let _cpu = self.cpu.acquire().await?;
            super::raw::verify_file(&mut file, source, None).await?;
            file.rewind().await?;
        }
        Ok(file)
    }
}
