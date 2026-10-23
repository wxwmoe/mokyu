use super::{Area, CacheFile, CachePin, Storage, UploadCharge, UploadPin};
use crate::model::{Id, raw::RawSource};
use anyhow::{Context, Result, ensure};
use md5::{Digest, Md5};
use std::sync::atomic::Ordering;

use uuid::Uuid;

pub(crate) enum Written {
    Final(Option<[u8; 32]>),
    Chunks(super::raw_request::File),
    Cached {
        revision: i64,
        deduplicate: bool,
        background: bool,
        boot: Option<Uuid>,
    },
    Reused,
}

pub(crate) struct Writer {
    storage: std::sync::Arc<Storage>,
    source: Id,
    location: Uuid,
    target: Uuid,
    file: Option<super::raw_request::File>,
    memory: Option<super::memory::Buffer>,
    external_memory: bool,
    charge: Option<UploadCharge>,
    policy: super::CachePolicy,
    size: u64,
    revision: i64,
    deduplicate: bool,
    convert: bool,
    background: bool,
    reject_overflow: bool,
    prepared: bool,
    buffered: bool,
}

impl Writer {
    pub(super) async fn new(
        storage: &std::sync::Arc<Storage>,
        target: Uuid,
        source: Id,
        location: Uuid,
        buffered: bool,
        declared: Option<i64>,
    ) -> Result<Self> {
        let profile = storage.profile(target)?;
        let policy = storage.local_cache(Some(target))?;
        let overdue = storage.write_overdue(target, &profile);
        if overdue {
            storage.expedite_writes(target);
            crate::upload::cache::overflow(profile.upload_cache_overflow == "reject")?;
        }
        let memory = (!overdue
            && !storage.cluster_mode
            && profile.write_durability.as_deref() == Some("volatile"))
        .then(|| {
            declared
                .and_then(|size| usize::try_from(size).ok())
                .and_then(|size| storage.write_memory.buffer(size))
        })
        .flatten();
        let file = if memory.is_none() {
            Some(super::raw_request::File::create(storage, location, policy).await?)
        } else {
            None
        };
        let reject_overflow = profile.upload_cache_overflow == "reject";
        let charge = if overdue || memory.is_some() {
            None
        } else {
            match storage
                .disk
                .claim_upload_with_limit(0, target, policy.upload_capacity)
            {
                Ok(charge) => Some(charge),
                Err(_) => {
                    storage.expedite_writes(target);
                    crate::upload::cache::overflow(reject_overflow)?;
                    None
                }
            }
        };
        Ok(Self {
            storage: storage.clone(),
            source,
            location,
            target,
            file,
            memory,
            external_memory: false,
            charge,
            policy,
            size: 0,
            revision: profile.revision,
            deduplicate: profile.deduplication_enabled && !profile.cdc_enabled,
            convert: profile.cdc_enabled,
            background: profile.write_processing == "background",
            reject_overflow,
            prepared: false,
            buffered,
        })
    }

    pub fn receive_external(&mut self) {
        if let Some(memory) = &mut self.memory {
            memory.receive_external();
            self.external_memory = true;
        }
    }

    pub async fn prepare(
        &mut self,
        ranges: Vec<super::assembly::Range>,
        model: std::sync::Arc<dyn crate::model::multipart::MultipartStore>,
        upload: Uuid,
    ) -> Result<Option<super::assembly::Prepared>> {
        ensure!(
            !self.prepared && self.size == 0,
            "raw writer is already populated"
        );
        let size = ranges
            .iter()
            .try_fold(0u64, |total, range| total.checked_add(range.length as u64))
            .context("assembly size overflow")?;
        // Multipart checkpoints always own a durable file, even under volatile PUT policy.
        if self.memory.take().is_some() {
            self.file = Some(
                super::raw_request::File::create(&self.storage, self.location, self.policy).await?,
            );
            self.charge = match self.storage.disk.claim_upload_with_limit(
                0,
                self.target,
                self.policy.upload_capacity,
            ) {
                Ok(charge) => Some(charge),
                Err(_) => {
                    self.storage.expedite_writes(self.target);
                    crate::upload::cache::overflow(self.reject_overflow)?;
                    None
                }
            };
        }
        let (file, prepared) = self
            .file
            .take()
            .unwrap()
            .assemble(
                self.storage.clone(),
                ranges,
                model,
                upload,
                self.source,
                self.location,
            )
            .await?;
        self.file = Some(file);
        if let Some(charge) = self.charge.as_mut() {
            match self.storage.disk.claim_upload_with_limit(
                size,
                self.target,
                self.policy.upload_capacity,
            ) {
                Ok(mut added) => {
                    charge.size += added.size;
                    added.size = 0;
                }
                Err(_) => {
                    self.storage.expedite_writes(self.target);
                    crate::upload::cache::overflow(self.reject_overflow)?;
                    self.charge = None;
                }
            }
        }
        self.prepared = true;
        Ok(Some(prepared))
    }

    pub async fn write(&mut self, data: &[u8]) -> Result<()> {
        let size = self
            .size
            .checked_add(data.len() as u64)
            .context("raw receive length overflow")?;
        super::raw::remote_size(size)?;
        if let Some(memory) = &mut self.memory {
            if !self.external_memory {
                memory.append(data)?;
            }
            self.size = size;
            return Ok(());
        }
        if !self.prepared {
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
                        self.storage.expedite_writes(self.target);
                        crate::upload::cache::overflow(self.reject_overflow)?;
                        self.charge = None;
                    }
                }
            }
            self.file
                .as_mut()
                .unwrap()
                .append(&self.storage, data)
                .await?;
        }
        self.size = size;
        Ok(())
    }

    pub async fn finish(
        mut self,
        stream: Id,
        expected: u64,
        digest: &[u8],
        received: Option<bytes::Bytes>,
    ) -> Result<Written> {
        ensure!(self.size == expected, "raw receive length mismatch");
        if let Some(memory) = self.memory.take() {
            let data = if self.external_memory {
                memory.retain(received.context("received memory is missing")?)?
            } else {
                memory.finish()
            };
            self.storage.raw_upload_pins.lock().unwrap().insert(
                self.location,
                (
                    self.source,
                    UploadPin {
                        memory: Some(data),
                        _pin: None,
                        _charge: None,
                        size: expected,
                        compressed: false,
                        remote: false,
                        staging_storage_id: None,
                        staging_object_key: self.location.to_string(),
                    },
                ),
            );
            #[cfg(feature = "fault-injection")]
            crate::faults::point("raw-cache-memory").await;
            return Ok(Written::Cached {
                revision: self.revision,
                deduplicate: self.deduplicate,
                background: self.background,
                boot: Some(self.storage.boot_id),
            });
        }
        if let Some(charge) = self.charge.take() {
            let (file, published) = self
                .file
                .take()
                .unwrap()
                .publish(
                    self.storage.clone(),
                    self.source,
                    self.location,
                    charge,
                    self.buffered,
                )
                .await?;
            self.file = Some(file);
            if published {
                #[cfg(feature = "fault-injection")]
                crate::faults::point(if self.buffered {
                    "raw-cache-buffered"
                } else {
                    "raw-cache-durable"
                })
                .await;
                return Ok(Written::Cached {
                    revision: self.revision,
                    deduplicate: self.deduplicate,
                    background: self.background,
                    boot: None,
                });
            }
            self.storage.expedite_writes(self.target);
            crate::upload::cache::overflow(self.reject_overflow)?;
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
        if self.convert {
            return Ok(Written::Chunks(self.file.take().unwrap()));
        }
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
    pub(crate) fn raw_memory(&self, location: Uuid) -> Option<bytes::Bytes> {
        self.raw_upload_pins
            .lock()
            .unwrap()
            .get(&location)?
            .1
            .memory
            .clone()
    }

    pub(super) async fn restore_raw_uploads(&self) -> Result<()> {
        let mut after = Id::default();
        loop {
            let rows = self
                .raw_store
                .uploads(after, self.node_id.get().copied())
                .await?;
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
                            memory: None,
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

    pub(super) async fn restore_raw_checkpoints(&self) -> Result<()> {
        if self.cluster_mode {
            return Ok(());
        }
        let mut after = Uuid::nil();
        loop {
            let rows = self.raw_store.cached_checkpoints(after).await?;
            if rows.is_empty() {
                break;
            }
            for (location, source) in rows {
                after = location;
                let path = CacheFile::Raw.path(&self.disk.root, location);
                let partial = tokio::fs::symlink_metadata(path.with_extension("partial"))
                    .await
                    .context("multipart cache checkpoint is missing; restore its durable file")?;
                ensure!(partial.is_file(), "multipart cache checkpoint was replaced");
                let metadata = match tokio::fs::symlink_metadata(&path).await {
                    Ok(metadata) => metadata,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        self.disk.account_existing(Area::Cache, partial.len());
                        continue;
                    }
                    Err(error) => return Err(error.into()),
                };
                ensure!(
                    metadata.is_file() && metadata.len() == partial.len(),
                    "multipart cache publication changed"
                );
                #[cfg(unix)]
                {
                    use std::os::unix::fs::MetadataExt;
                    ensure!(
                        metadata.dev() == partial.dev() && metadata.ino() == partial.ino(),
                        "multipart cache publication was replaced"
                    );
                }
                if self.raw_upload_pins.lock().unwrap().contains_key(&location) {
                    continue;
                }
                let size = metadata.len();
                self.disk.account_existing(Area::Cache, size);
                let mut fifo = self.fifo.lock().await;
                fifo.insert(location, size, CacheFile::Raw);
                let pins = fifo.entries[&location].pins.clone();
                pins.fetch_add(1, Ordering::Relaxed);
                self.raw_upload_pins.lock().unwrap().insert(
                    location,
                    (
                        source,
                        UploadPin {
                            memory: None,
                            _pin: Some(CachePin(pins)),
                            _charge: None,
                            size,
                            compressed: false,
                            remote: false,
                            staging_storage_id: None,
                            staging_object_key: location.to_string(),
                        },
                    ),
                );
            }
        }
        Ok(())
    }

    pub(super) async fn remove_raw_checkpoint(&self, location: Uuid) -> Result<()> {
        let _write = self.cache_writes.lock().await;
        let path = CacheFile::Raw.path(&self.disk.root, location);
        let partial = path.with_extension("partial");
        let metadata = match tokio::fs::symlink_metadata(&partial).await {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        let published = self.fifo.lock().await.entries.contains_key(&location);
        if !published {
            match tokio::fs::remove_file(&path).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        tokio::fs::remove_file(&partial).await?;
        if !published {
            self.disk.release(Area::Cache, metadata.len());
        }
        Ok(())
    }

    pub(crate) async fn reconcile_raw_uploads(&self) -> Result<()> {
        let mut after = Id::default();
        let mut retained = std::collections::HashSet::new();
        loop {
            let rows = self
                .raw_store
                .uploads(after, self.node_id.get().copied())
                .await?;
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
            let current = self.raw_store.current(source).await?;
            let writing = current
                .as_ref()
                .is_some_and(|source| source.state == "writing");
            if writing
                || self.raw_store.has_upload(source).await?
                || self.raw_store.cached_checkpoint(location).await?.is_some()
            {
                retained.insert(location);
            }
            if !writing
                && current.is_none_or(|source| !source.pending_upload)
                && let Some((_, pin)) = self.raw_upload_pins.lock().unwrap().get_mut(&location)
            {
                pin._charge = None;
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
            let checkpoint = self.raw_store.cached_checkpoint(location).await?.is_some();
            let mut pins = self.raw_upload_pins.lock().unwrap();
            if checkpoint {
                if let Some((_, pin)) = pins.get_mut(&location) {
                    pin._charge = None;
                }
            } else {
                pins.remove(&location);
            }
        }
        Ok(())
    }

    pub(super) async fn pending_raw(
        &self,
        source: &RawSource,
        verify: bool,
    ) -> Result<super::raw::RawFile> {
        if let Some(data) = self.raw_memory(source.location_id) {
            ensure!(
                data.len() as i64 == source.size,
                crate::content::codec::IntegrityError::Length
            );
            let mut file = super::raw::RawFile::Memory { data, offset: 0 };
            if verify {
                let _cpu = self.cpu.acquire().await?;
                super::raw::verify_file(&mut file, source, None).await?;
                file.rewind().await?;
            }
            return Ok(file);
        }
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
