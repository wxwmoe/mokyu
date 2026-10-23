use super::{CachePin, Storage, StoreBackend, StorePermit, resource_gate};
use crate::content::codec::IntegrityError;
use crate::model::Id;
use crate::model::raw::RawSource;
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures_util::StreamExt;
use md5::{Digest, Md5};
use object_store::{
    GetOptions, GetRange, ObjectStoreExt, PutMode, PutOptions, path::Path as ObjectPath,
};
use std::{
    path::{Path, PathBuf},
    sync::atomic::Ordering,
    sync::{Arc, Mutex},
};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use uuid::Uuid;

const BUFFER: usize = 256 * 1024;

pub(crate) struct LocalWriter<'a> {
    storage: &'a Storage,
    file: tokio::fs::File,
    temporary: PathBuf,
    destination: PathBuf,
    size: u64,
    buffer: Vec<u8>,
    hash: Option<blake3::Hasher>,
    pending: Arc<Mutex<u64>>,
    floor: u64,
    prepared: bool,
    retained: bool,
    buffered: bool,
    _target: Arc<StoreBackend>,
    _permit: StorePermit,
    _global: resource_gate::Permit,
}

pub(super) async fn sync_directory(path: &Path) -> Result<()> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || std::fs::File::open(path)?.sync_all()).await??;
    Ok(())
}

async fn directory(path: &Path) -> Result<()> {
    match tokio::fs::create_dir(path).await {
        Ok(()) => sync_directory(path.parent().context("directory has no parent")?).await?,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            ensure!(
                tokio::fs::symlink_metadata(path).await?.is_dir(),
                "raw directory was replaced"
            );
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn path(root: &Path, id: Uuid) -> PathBuf {
    root.join(super::chunk_name(id))
}

impl LocalWriter<'_> {
    pub async fn write(&mut self, data: &[u8]) -> Result<()> {
        let size = self
            .size
            .checked_add(data.len() as u64)
            .context("raw length overflow")?;
        if !self.prepared {
            let mut rest = data;
            while !rest.is_empty() {
                let count = rest.len().min(BUFFER - self.buffer.len());
                self.buffer.extend_from_slice(&rest[..count]);
                rest = &rest[count..];
                if self.buffer.len() == BUFFER {
                    self.flush_buffer().await?;
                }
            }
        }
        self.size = size;
        if let Some(hash) = &mut self.hash {
            hash.update(data);
        }
        Ok(())
    }

    async fn flush_buffer(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let _space = super::local::Space::claim(
            self.destination.parent().unwrap(),
            self.floor,
            self.buffer.len() as u64,
            &self.pending,
        )?;
        self.file.write_all(&self.buffer).await?;
        self.file.flush().await?;
        self.buffer.clear();
        Ok(())
    }

    pub async fn finish(mut self, expected: u64) -> Result<Option<[u8; 32]>> {
        ensure!(self.size == expected, "raw receive length mismatch");
        self.flush_buffer().await?;
        self.file.flush().await?;
        if !self.buffered {
            self.file.sync_all().await?;
            #[cfg(feature = "fault-injection")]
            crate::faults::point("raw-file-durable").await;
        } else {
            #[cfg(feature = "fault-injection")]
            crate::faults::point("raw-file-buffered").await;
        }
        // A new immutable name must never replace an existing file, even on retry.
        tokio::fs::hard_link(&self.temporary, &self.destination).await?;
        if !self.retained {
            tokio::fs::remove_file(&self.temporary).await?;
        }
        if !self.buffered {
            sync_directory(self.destination.parent().unwrap()).await?;
        }
        self.storage.backend_puts.fetch_add(1, Ordering::Relaxed);
        self.storage
            .backend_write_bytes
            .fetch_add(expected, Ordering::Relaxed);
        #[cfg(feature = "fault-injection")]
        crate::faults::point("raw-file-published").await;
        Ok(self.hash.as_ref().map(|hash| *hash.finalize().as_bytes()))
    }
}

pub(crate) struct RawReader {
    pub file: RawFile,
    _pin: CachePin,
    _permit: Option<StorePermit>,
    _global: Option<resource_gate::Permit>,
}

impl Storage {
    pub(crate) async fn sync_raw(self: &Arc<Self>, source: &RawSource, verify: bool) -> Result<()> {
        if let Some(data) = self.raw_memory(source.location_id) {
            let mut policy = self.local_cache(Some(source.storage_id))?;
            policy.upload_capacity = None;
            let charge =
                self.disk
                    .claim_upload_with_limit(source.size as u64, source.storage_id, None)?;
            let mut file =
                super::raw_request::File::create(self, source.location_id, policy).await?;
            file.append(self, &data).await?;
            let (_, published) = file
                .publish(self.clone(), source.id, source.location_id, charge, false)
                .await?;
            ensure!(
                published,
                "write memory could not be persisted to the file cache"
            );
        }
        let target = self.store_backend(source.storage_id)?;
        if !source.pending_upload && !target.is_local {
            return Ok(());
        }
        let root = if source.pending_upload {
            self.disk.root.join("raw")
        } else {
            target
                .raw_root
                .clone()
                .context("raw local path is missing")?
        };
        let file_path = path(&root, source.location_id);
        // Distinguish a missing pending cache file from transient database/backend failures.
        tokio::fs::symlink_metadata(&file_path)
            .await
            .map_err(raw_io_error)?;
        let reader = self.raw_reader_uncached(source, verify).await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("buffered-before-sync").await;
        match &reader.file {
            RawFile::Local(file) | RawFile::Cached { file, .. } => {
                file.sync_all().await?;
                sync_directory(file_path.parent().unwrap()).await?;
                sync_directory(&root).await?;
                sync_directory(root.parent().context("raw root has no parent")?).await?;
            }
            RawFile::Remote(_) => {}
            RawFile::Memory { .. } => anyhow::bail!("write memory is not durable"),
        }
        Ok(())
    }
    pub(crate) async fn send_raw(
        self: &Arc<Self>,
        target: Uuid,
        source: Id,
        location: Uuid,
        file: &mut RawFile,
        expected: u64,
        digest: &[u8],
        hash: Option<&[u8; 32]>,
        token: Option<Uuid>,
    ) -> Result<()> {
        let mut writer = self
            .raw_writer_for(
                target,
                source,
                location,
                Some(expected as i64),
                false,
                token,
            )
            .await?;
        let mut buffer = vec![0; BUFFER];
        let mut received = 0u64;
        let mut md5 = Md5::new();
        let mut content = hash.map(|_| blake3::Hasher::new());
        loop {
            let count = file.read(&mut buffer).await?;
            if count == 0 {
                break;
            }
            received += count as u64;
            ensure!(received <= expected, IntegrityError::Length);
            md5.update(&buffer[..count]);
            if let Some(content) = &mut content {
                content.update(&buffer[..count]);
            }
            writer.write(&buffer[..count]).await?;
        }
        ensure!(received == expected, IntegrityError::Length);
        ensure!(md5.finalize().as_slice() == digest, IntegrityError::Hash);
        if let Some(hash) = hash {
            ensure!(
                content.unwrap().finalize().as_bytes() == hash,
                IntegrityError::Hash
            );
        }
        Box::pin(writer.finish_received(Id::default(), expected, digest, None)).await?;
        Ok(())
    }

    pub(crate) async fn restart_raw_upload(&self, source: &RawSource, token: Uuid) -> Result<()> {
        if let Some(upload) = self.raw_store.upload_id(source.id).await? {
            let target = self.store_backend(source.storage_id)?;
            let multipart = target
                .multipart
                .as_ref()
                .context("S3 multipart backend is missing")?;
            let _permit = target.controls.acquire().await?;
            match multipart
                .abort_multipart(&remote_path(source.location_id), &upload)
                .await
            {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(error) => return Err(error.into()),
            }
            self.raw_store
                .set_pending_upload(source.id, token, None)
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn raw_replay_writer(
        self: &Arc<Self>,
        storage: Uuid,
        source: Id,
        location: Uuid,
        size: i64,
        parts: &[i64],
    ) -> Result<RawWriter<'_>> {
        let declared = (!self.is_local(storage)?).then_some(size);
        let writer = self.raw_writer(storage, source, location, declared).await?;
        Ok(match writer {
            RawWriter::Remote(writer) if size >= PART_SIZE as i64 => {
                let part_size = replay_part_size(size as u64, parts)?;
                // Avoid rereading when the existing bounded writer sends as few requests.
                if writer.hash.is_none()
                    && (size as u64).div_ceil(PART_SIZE as u64) <= (size as u64).div_ceil(part_size)
                {
                    return Ok(RawWriter::Remote(writer));
                }
                RawWriter::Replay(ReplayWriter {
                    writer,
                    part_size,
                    filled: 0,
                    digest: Md5::new(),
                    digests: Vec::new(),
                })
            }
            writer => writer,
        })
    }

    async fn verify_raw_target(&self, target: &StoreBackend) -> Result<()> {
        if target.identity_ready.load(Ordering::Acquire) {
            return Ok(());
        }
        let _guard = target.identity_check.lock().await;
        if !target.identity_ready.load(Ordering::Acquire) {
            self.check_storage_inner(target).await?;
        }
        Ok(())
    }
    pub(crate) fn raw_policy(&self, storage: Uuid) -> Result<Option<i64>> {
        let target = self.store_backend(storage)?;
        Ok((!target.cdc_enabled).then_some(target.profile_revision))
    }
    pub(crate) fn is_local(&self, storage: Uuid) -> Result<bool> {
        Ok(self.store_backend(storage)?.is_local)
    }

    pub(crate) async fn raw_writer(
        self: &Arc<Self>,
        storage: Uuid,
        source: Id,
        location: Uuid,
        declared: Option<i64>,
    ) -> Result<RawWriter<'_>> {
        self.raw_writer_for(storage, source, location, declared, true, None)
            .await
    }

    async fn raw_writer_for(
        self: &Arc<Self>,
        storage: Uuid,
        source: Id,
        location: Uuid,
        declared: Option<i64>,
        cache: bool,
        upload_token: Option<Uuid>,
    ) -> Result<RawWriter<'_>> {
        let target = self.store_backend(storage)?;
        self.verify_raw_target(&target).await?;
        let buffered = cache
            && !self.write_overdue(storage, &target.profile)
            && !self.cluster_mode
            && (target.is_local || target.profile.upload_cache)
            && matches!(
                target.profile.write_durability.as_deref(),
                Some("buffered" | "volatile")
            );
        if buffered {
            self.buffered_writes.store(true, Ordering::Release);
            self.raw_store.buffer(source).await?;
        }
        if cache && !target.is_local && target.profile.upload_cache {
            remote_size(declared.unwrap_or(0).max(0) as u64)?;
            return Ok(RawWriter::Pending(
                super::raw_upload::Writer::new(self, storage, source, location, buffered, declared)
                    .await?,
            ));
        }
        let permit = target.writes.acquire().await?;
        let global = self.writes.acquire().await?;
        let hash =
            (cache && target.deduplication_enabled && target.profile.write_processing == "inline")
                .then(blake3::Hasher::new);
        if !target.is_local {
            remote_size(declared.unwrap_or(0).max(0) as u64)?;
            return Ok(RawWriter::Remote(RemoteWriter {
                storage: self,
                source,
                target,
                key: remote_path(location),
                location,
                upload: None,
                upload_token,
                parts: Vec::new(),
                buffer: Vec::new(),
                size: 0,
                hash,
                temporary: None,
                cache_policy: self.local_cache(Some(storage))?,
                _permit: permit,
                _global: global,
            }));
        }
        let root = target
            .raw_root
            .as_ref()
            .context("raw files require local storage")?;
        directory(root).await?;
        let destination = path(root, location);
        directory(destination.parent().unwrap()).await?;
        let temporary = destination.with_extension("partial");
        let mut options = tokio::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o644);
        let file = options.open(&temporary).await?;
        let writer = LocalWriter {
            storage: self,
            file,
            temporary,
            destination: destination.clone(),
            size: 0,
            buffer: Vec::with_capacity(BUFFER),
            hash,
            pending: self.disk.space_for(destination.parent().unwrap())?,
            floor: crate::config::bytes(
                target
                    .profile
                    .min_free_space
                    .as_deref()
                    .context("local storage floor is missing")?,
            )?,
            prepared: false,
            retained: false,
            buffered,
            _target: target,
            _permit: permit,
            _global: global,
        };
        Ok(RawWriter::Local(writer))
    }

    pub(crate) async fn raw_reader(
        self: &Arc<Self>,
        source: &RawSource,
        verify: bool,
    ) -> Result<RawReader> {
        self.raw_cached_reader(source, verify)
            .await
            .inspect_err(|error| self.diagnostics.content(error))
    }

    pub(crate) async fn raw_reader_uncached(
        &self,
        source: &RawSource,
        verify: bool,
    ) -> Result<RawReader> {
        self.raw_reader_inner(source, verify)
            .await
            .inspect_err(|error| self.diagnostics.content(error))
    }

    async fn raw_cached_reader(
        self: &Arc<Self>,
        source: &RawSource,
        verify: bool,
    ) -> Result<RawReader> {
        source.validate()?;
        if source.pending_upload {
            return self.raw_reader_inner(source, verify).await;
        }
        let target = self.store_backend(source.storage_id)?;
        let policy = self.local_cache(Some(source.storage_id))?;
        if target.is_local
            || !policy.enabled
            || policy
                .capacity
                .is_some_and(|limit| source.size as u64 > limit)
        {
            return self.raw_reader_inner(source, verify).await;
        }
        let pin = {
            let _gate = self.source_gate.read().await;
            self.physical_pin(source.location_id)
        };
        self.verify_raw_target(&target).await?;
        self.cache_lookups.fetch_add(1, Ordering::Relaxed);
        if let Some(file) = self.cached_raw(source, verify).await {
            return Ok(RawReader {
                file,
                _pin: pin,
                _permit: None,
                _global: None,
            });
        }
        let lock = self.raw_cache_lock(source.location_id);
        let _miss = lock.lock().await;
        if let Some(file) = self.cached_raw(source, verify).await {
            return Ok(RawReader {
                file,
                _pin: pin,
                _permit: None,
                _global: None,
            });
        }
        let mut reader = self.raw_reader_inner(source, false).await?;
        if verify {
            let mut cache = match self.begin_raw_cache(source).await {
                Ok(cache) => cache,
                Err(error) => {
                    tracing::warn!(error=%error, "raw cache unavailable; reading backend");
                    None
                }
            };
            let _cpu = self.cpu.acquire().await?;
            #[cfg(feature = "fault-injection")]
            crate::faults::point("raw-before-verify").await;
            let size = verify_file(&mut reader.file, source, Some(&mut cache)).await?;
            self.backend_read_bytes.fetch_add(size, Ordering::Relaxed);
            drop(_cpu);
            if let Some(cache) = cache {
                match self.finish_raw_cache(cache, source, &target).await {
                    Ok(Some(file)) => {
                        reader.file = file;
                        reader._permit = None;
                        reader._global = None;
                    }
                    Ok(None) => {}
                    Err(error) => {
                        tracing::warn!(error=%error, "raw cache publication failed; reading backend")
                    }
                }
            }
            reader.file.rewind().await?;
        }
        Ok(reader)
    }

    async fn raw_reader_inner(&self, source: &RawSource, verify: bool) -> Result<RawReader> {
        source.validate()?;
        let pin = {
            let _gate = self.source_gate.read().await;
            self.physical_pin(source.location_id)
        };
        if source.pending_upload {
            match self.pending_raw(source, verify).await {
                Ok(file) => {
                    return Ok(RawReader {
                        file,
                        _pin: pin,
                        _permit: None,
                        _global: None,
                    });
                }
                Err(error) => {
                    let current = self.raw_store.current(source.id).await?;
                    if let Some(current) = current.filter(|r| {
                        !r.pending_upload
                            && r.location_id == source.location_id
                            && r.storage_id == source.storage_id
                            && r.size == source.size
                            && r.digest == source.digest
                    }) {
                        return Box::pin(self.raw_reader_inner(&current, verify)).await;
                    }
                    if self.cluster_mode
                        && !self
                            .raw_upload_pins
                            .lock()
                            .unwrap()
                            .contains_key(&source.location_id)
                    {
                        return Err(crate::app::readiness::pending());
                    }
                    return Err(error);
                }
            }
        }
        let target = self.store_backend(source.storage_id)?;
        self.verify_raw_target(&target).await?;
        let global = self.reads.acquire().await?;
        let permit = target.reads.acquire().await?;
        let mut file = if target.is_local {
            ensure!(source.medium == "local", "raw source medium changed");
            let root = target
                .raw_root
                .as_ref()
                .context("raw local path is missing")?;
            let file_path = path(root, source.location_id);
            let metadata = tokio::fs::symlink_metadata(&file_path)
                .await
                .map_err(raw_io_error)?;
            ensure!(metadata.is_file(), IntegrityError::RawMetadata);
            let file = tokio::fs::File::open(file_path)
                .await
                .map_err(raw_io_error)?;
            ensure!(
                file.metadata().await?.len() == source.size as u64,
                IntegrityError::Length
            );
            self.backend_gets.fetch_add(1, Ordering::Relaxed);
            RawFile::Local(file)
        } else {
            ensure!(source.medium == "shared_s3", "raw source medium changed");
            let key = remote_path(source.location_id);
            let meta = target.backend.head(&key).await.map_err(remote_error)?;
            self.backend_gets.fetch_add(1, Ordering::Relaxed);
            ensure!(meta.size == source.size as u64, IntegrityError::Length);
            RawFile::Remote(RemoteReader {
                target,
                key,
                size: meta.size,
                etag: meta.e_tag,
                offset: 0,
                body: None,
                buffered: Bytes::new(),
                gets: self.backend_gets.clone(),
                diagnostics: self.diagnostics.clone(),
            })
        };
        if verify {
            let _cpu = self.cpu.acquire().await?;
            #[cfg(feature = "fault-injection")]
            crate::faults::point("raw-before-verify").await;
            let size = verify_file(&mut file, source, None).await?;
            file.rewind().await?;
            self.backend_read_bytes.fetch_add(size, Ordering::Relaxed);
        }
        Ok(RawReader {
            file,
            _pin: pin,
            _permit: Some(permit),
            _global: Some(global),
        })
    }

    pub(crate) async fn remove_raw(&self, source: &RawSource) -> Result<()> {
        let target = self.store_backend(source.storage_id)?;
        self.verify_raw_target(&target).await?;
        if !target.is_local {
            self.remove_raw_checkpoint(source.location_id).await?;
            let _permit = target.controls.acquire().await?;
            let key = remote_path(source.location_id);
            if let Some(upload) = self.raw_store.upload_id(source.id).await? {
                let multipart = target
                    .multipart
                    .as_ref()
                    .context("S3 multipart backend is missing")?;
                match multipart.abort_multipart(&key, &upload).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(error) => return Err(error.into()),
                }
            }
            match target.backend.delete(&key).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(error) => return Err(error.into()),
            }
            self.backend_deletes.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        let root = target
            .raw_root
            .as_ref()
            .context("raw files require local storage")?;
        let destination = path(root, source.location_id);
        if !tokio::fs::try_exists(&destination).await?
            && !tokio::fs::try_exists(destination.with_extension("partial")).await?
        {
            return Ok(());
        }
        let _permit = target.controls.acquire().await?;
        for file in [destination.clone(), destination.with_extension("partial")] {
            match tokio::fs::remove_file(file).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        if tokio::fs::try_exists(destination.parent().unwrap()).await? {
            sync_directory(destination.parent().unwrap()).await?;
        }
        self.backend_deletes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

pub(super) async fn verify_file(
    file: &mut RawFile,
    source: &RawSource,
    mut cache: Option<&mut Option<super::raw_cache::Write>>,
) -> Result<u64> {
    let mut digest = Md5::new();
    let mut buffer = vec![0; BUFFER];
    let mut size = 0u64;
    loop {
        let count = file.read(&mut buffer).await?;
        if count == 0 {
            break;
        }
        size += count as u64;
        ensure!(size <= source.size as u64, IntegrityError::Length);
        digest.update(&buffer[..count]);
        if let Some(cache) = cache.as_deref_mut()
            && let Some(write) = cache.as_mut()
            && let Err(error) = write
                .file
                .as_mut()
                .unwrap()
                .write_all(&buffer[..count])
                .await
        {
            tracing::warn!(error=%error, "raw cache write failed; reading backend");
            *cache = None;
        }
    }
    ensure!(size == source.size as u64, IntegrityError::Length);
    ensure!(
        Some(digest.finalize().as_slice()) == source.digest.as_deref(),
        IntegrityError::Hash
    );
    Ok(size)
}

fn raw_io_error(error: std::io::Error) -> anyhow::Error {
    if error.kind() == std::io::ErrorKind::NotFound {
        anyhow::Error::new(error).context(IntegrityError::RemoteMissing)
    } else {
        error.into()
    }
}

const PART_SIZE: usize = 5 * 1024 * 1024;
const MAX_PART_SIZE: u64 = 5 * 1024 * 1024 * 1024;
const MAX_REMOTE_SIZE: u64 = MAX_PART_SIZE * 10000;

pub(super) fn remote_size(size: u64) -> Result<()> {
    ensure!(
        size <= MAX_REMOTE_SIZE,
        s3s::s3_error!(EntityTooLarge, "raw object exceeds S3 multipart limits")
    );
    Ok(())
}

fn replay_part_size(size: u64, parts: &[i64]) -> Result<u64> {
    remote_size(size)?;
    let minimum = size.div_ceil(10000);
    let planned = (64 * 1024 * 1024).max(minimum.div_ceil(1024 * 1024) * 1024 * 1024);
    if let Some(&first) = parts.first()
        && first >= PART_SIZE as i64
        && first as u64 <= MAX_PART_SIZE
        && first as u64 <= planned.max(128 * 1024 * 1024)
        && parts.len() <= 10000
        && parts[..parts.len() - 1]
            .iter()
            .all(|&length| length == first)
        && parts
            .last()
            .is_some_and(|&length| length >= 0 && length <= first)
        && parts
            .iter()
            .try_fold(0u64, |sum, &length| sum.checked_add(length as u64))
            == Some(size)
    {
        return Ok(first as u64);
    }
    Ok(planned)
}

pub(super) fn remote_path(id: Uuid) -> ObjectPath {
    ObjectPath::from(format!("raw/{}", super::chunk_name(id)))
}

pub(crate) enum RawWriter<'a> {
    Local(LocalWriter<'a>),
    Pending(super::raw_upload::Writer),
    Remote(RemoteWriter<'a>),
    Replay(ReplayWriter<'a>),
}

impl RawWriter<'_> {
    pub fn receive_external(&mut self) {
        if let Self::Pending(writer) = self {
            writer.receive_external();
        }
    }

    pub fn can_assemble(&self) -> bool {
        matches!(self, Self::Local(_) | Self::Pending(_))
    }
    pub async fn prepare_local(
        &mut self,
        ranges: Vec<super::assembly::Range>,
        app: &crate::app::App,
        upload: Uuid,
        source: Id,
    ) -> Result<Option<super::assembly::Prepared>> {
        if let Self::Pending(writer) = self {
            return writer.prepare(ranges, app.multipart.clone(), upload).await;
        }
        let Self::Local(writer) = self else {
            return Ok(None);
        };
        ensure!(
            !writer.prepared && writer.size == 0,
            "raw writer is already populated"
        );
        let (file, prepared, retained) = super::assembly::prepare(
            writer.storage,
            &writer.temporary,
            ranges,
            app.multipart.as_ref(),
            upload,
            source,
            writer.floor,
        )
        .await?;
        writer.file = file;
        writer.prepared = true;
        writer.retained = retained;
        Ok(Some(prepared))
    }
}

pub(crate) struct ReplayWriter<'a> {
    writer: RemoteWriter<'a>,
    part_size: u64,
    filled: u64,
    digest: Md5,
    digests: Vec<[u8; 16]>,
}

pub(crate) struct RemoteWriter<'a> {
    storage: &'a Storage,
    source: Id,
    target: Arc<StoreBackend>,
    key: ObjectPath,
    location: Uuid,
    upload: Option<String>,
    upload_token: Option<Uuid>,
    parts: Vec<object_store::multipart::PartId>,
    buffer: Vec<u8>,
    size: u64,
    hash: Option<blake3::Hasher>,
    temporary: Option<super::raw_request::File>,
    cache_policy: super::CachePolicy,
    _permit: StorePermit,
    _global: resource_gate::Permit,
}

impl RemoteWriter<'_> {
    async fn start(&mut self) -> Result<()> {
        let backend = self
            .target
            .multipart
            .as_ref()
            .context("S3 multipart backend is missing")?;
        if self.upload.is_none() {
            let upload = backend.create_multipart(&self.key).await?;
            let registered = if let Some(token) = self.upload_token {
                self.storage
                    .raw_store
                    .set_pending_upload(self.source, token, Some(&upload))
                    .await
            } else {
                self.storage
                    .raw_store
                    .set_upload(self.source, &upload)
                    .await
            };
            if let Err(error) = registered {
                let _ = backend.abort_multipart(&self.key, &upload).await;
                return Err(error);
            }
            self.upload = Some(upload);
        }
        Ok(())
    }

    async fn part(&mut self) -> Result<()> {
        ensure!(self.parts.len() < 10000, s3s::s3_error!(EntityTooLarge));
        self.start().await?;
        let backend = self.target.multipart.as_ref().unwrap();
        let bytes = Bytes::from(std::mem::take(&mut self.buffer));
        let size = bytes.len() as u64;
        let id = backend
            .put_part(
                &self.key,
                self.upload.as_ref().unwrap(),
                self.parts.len(),
                bytes.into(),
            )
            .await?;
        self.storage.backend_puts.fetch_add(1, Ordering::Relaxed);
        self.storage
            .backend_write_bytes
            .fetch_add(size, Ordering::Relaxed);
        self.parts.push(id);
        #[cfg(feature = "fault-injection")]
        crate::faults::point("raw-part-uploaded").await;
        Ok(())
    }
}

impl RawWriter<'_> {
    pub async fn reuse(&self, stream: Id, digest: &[u8]) -> Result<bool> {
        let writer = match self {
            Self::Remote(writer) => writer,
            Self::Replay(replay) => &replay.writer,
            Self::Local(_) | Self::Pending(_) => return Ok(false),
        };
        let Some(hash) = &writer.hash else {
            return Ok(false);
        };
        ensure!(
            writer.upload.is_none(),
            "raw content was uploaded before deduplication"
        );
        #[cfg(feature = "fault-injection")]
        crate::faults::point("raw-before-reuse").await;
        let reused = writer
            .storage
            .raw_store
            .reuse(
                stream,
                writer.source,
                writer.target.profile.storage_id,
                writer.size as i64,
                digest,
                hash.finalize().as_bytes(),
            )
            .await?;
        #[cfg(feature = "fault-injection")]
        if reused {
            crate::faults::point("raw-reused").await;
        }
        Ok(reused)
    }

    pub async fn write(&mut self, data: &[u8]) -> Result<()> {
        match self {
            Self::Local(writer) => writer.write(data).await,
            Self::Pending(writer) => writer.write(data).await,
            Self::Replay(replay) => {
                replay.writer.size = replay
                    .writer
                    .size
                    .checked_add(data.len() as u64)
                    .context("raw length overflow")?;
                remote_size(replay.writer.size)?;
                if let Some(hash) = &mut replay.writer.hash {
                    hash.update(data);
                }
                let mut rest = data;
                while !rest.is_empty() {
                    let count = (replay.part_size - replay.filled).min(rest.len() as u64) as usize;
                    replay.digest.update(&rest[..count]);
                    replay.filled += count as u64;
                    rest = &rest[count..];
                    if replay.filled == replay.part_size {
                        replay
                            .digests
                            .push(std::mem::take(&mut replay.digest).finalize().into());
                        replay.filled = 0;
                    }
                }
                Ok(())
            }
            Self::Remote(writer) => {
                writer.size = writer
                    .size
                    .checked_add(data.len() as u64)
                    .context("raw length overflow")?;
                remote_size(writer.size)?;
                if let Some(hash) = &mut writer.hash {
                    hash.update(data);
                }
                let mut rest = data;
                while !rest.is_empty() {
                    let count = (PART_SIZE - writer.buffer.len()).min(rest.len());
                    writer.buffer.extend_from_slice(&rest[..count]);
                    rest = &rest[count..];
                    if writer.buffer.len() == PART_SIZE {
                        if writer.hash.is_some() {
                            if writer.temporary.is_none() {
                                writer.temporary = Some(
                                    super::raw_request::File::create(
                                        writer.storage,
                                        writer.location,
                                        writer.cache_policy,
                                    )
                                    .await?,
                                );
                            }
                            writer
                                .temporary
                                .as_mut()
                                .unwrap()
                                .append(writer.storage, &writer.buffer)
                                .await?;
                            writer.buffer.clear();
                        } else {
                            writer.part().await?;
                        }
                    }
                }
                Ok(())
            }
        }
    }
    pub async fn finish(
        self,
        stream: Id,
        expected: u64,
        digest: &[u8],
        received: Option<Bytes>,
    ) -> Result<super::raw_upload::Written> {
        if self.reuse(stream, digest).await? {
            return Ok(super::raw_upload::Written::Reused);
        }
        self.finish_received(stream, expected, digest, received)
            .await
    }
    async fn finish_received(
        self,
        stream: Id,
        expected: u64,
        digest: &[u8],
        received: Option<Bytes>,
    ) -> Result<super::raw_upload::Written> {
        match self {
            Self::Local(writer) => Ok(super::raw_upload::Written::Final(
                writer.finish(expected).await?,
            )),
            Self::Pending(writer) => writer.finish(stream, expected, digest, received).await,
            Self::Replay(_) => anyhow::bail!("raw upload requires its replay source"),
            Self::Remote(mut writer) => {
                ensure!(writer.size == expected, "raw receive length mismatch");
                if let Some(mut temporary) = writer.temporary.take() {
                    if !writer.buffer.is_empty() {
                        temporary.append(writer.storage, &writer.buffer).await?;
                    }
                    temporary.rewind().await?;
                    writer.buffer.clear();
                    let mut buffer = vec![0; BUFFER];
                    let mut remaining = expected;
                    let mut hash = blake3::Hasher::new();
                    while remaining > 0 {
                        let count = temporary.file.as_mut().unwrap().read(&mut buffer).await?;
                        ensure!(
                            count > 0 && count as u64 <= remaining,
                            IntegrityError::Length
                        );
                        remaining -= count as u64;
                        hash.update(&buffer[..count]);
                        let mut rest = &buffer[..count];
                        while !rest.is_empty() {
                            let count = (PART_SIZE - writer.buffer.len()).min(rest.len());
                            writer.buffer.extend_from_slice(&rest[..count]);
                            rest = &rest[count..];
                            if writer.buffer.len() == PART_SIZE {
                                writer.part().await?;
                            }
                        }
                    }
                    ensure!(
                        Some(hash.finalize()) == writer.hash.as_ref().map(|hash| hash.finalize()),
                        IntegrityError::Hash
                    );
                }
                if writer.upload.is_some() {
                    if !writer.buffer.is_empty() {
                        writer.part().await?;
                    }
                    writer
                        .target
                        .multipart
                        .as_ref()
                        .unwrap()
                        .complete_multipart(
                            &writer.key,
                            writer.upload.as_ref().unwrap(),
                            std::mem::take(&mut writer.parts),
                        )
                        .await?;
                } else {
                    writer
                        .target
                        .backend
                        .put_opts(
                            &writer.key,
                            Bytes::from(writer.buffer).into(),
                            PutOptions {
                                mode: if writer.upload_token.is_some() {
                                    PutMode::Overwrite
                                } else {
                                    PutMode::Create
                                },
                                ..Default::default()
                            },
                        )
                        .await?;
                    writer.storage.backend_puts.fetch_add(1, Ordering::Relaxed);
                    writer
                        .storage
                        .backend_write_bytes
                        .fetch_add(expected, Ordering::Relaxed);
                }
                #[cfg(feature = "fault-injection")]
                crate::faults::point("raw-file-published").await;
                Ok(super::raw_upload::Written::Final(
                    writer.hash.as_ref().map(|hash| *hash.finalize().as_bytes()),
                ))
            }
        }
    }

    pub async fn finish_replay<'a>(
        self,
        stream: Id,
        expected: u64,
        digest: &[u8],
        mut input: impl FnMut(
            std::ops::Range<u64>,
        ) -> futures_util::stream::BoxStream<'a, Result<Bytes>>,
    ) -> Result<super::raw_upload::Written> {
        if self.reuse(stream, digest).await? {
            return Ok(super::raw_upload::Written::Reused);
        }
        let Self::Replay(mut replay) = self else {
            return self.finish_received(stream, expected, digest, None).await;
        };
        ensure!(replay.writer.size == expected, "raw replay length mismatch");
        #[cfg(feature = "fault-injection")]
        crate::faults::point("raw-before-replay").await;
        if replay.filled > 0 {
            replay.digests.push(replay.digest.finalize().into());
        }
        replay.writer.start().await?;
        let mut offset = 0;
        for digest in replay.digests {
            let end = (offset + replay.part_size).min(expected);
            let id = replay
                .writer
                .target
                .streaming
                .as_ref()
                .context("S3 streaming client is missing")?
                .part(
                    &replay.writer.key,
                    replay.writer.upload.as_ref().unwrap(),
                    replay.writer.parts.len(),
                    end - offset,
                    &digest,
                    || input(offset..end),
                )
                .await?;
            replay.writer.parts.push(id);
            replay
                .writer
                .storage
                .backend_puts
                .fetch_add(1, Ordering::Relaxed);
            replay
                .writer
                .storage
                .backend_write_bytes
                .fetch_add(end - offset, Ordering::Relaxed);
            offset = end;
            #[cfg(feature = "fault-injection")]
            crate::faults::point("raw-part-uploaded").await;
        }
        ensure!(offset == expected, "raw replay is incomplete");
        Self::Remote(replay.writer)
            .finish_received(stream, expected, digest, None)
            .await
    }
}

pub(crate) enum RawFile {
    Local(tokio::fs::File),
    Memory {
        data: Bytes,
        offset: usize,
    },
    Cached {
        file: tokio::fs::File,
        _pin: CachePin,
    },
    Remote(RemoteReader),
}

pub(crate) struct RemoteReader {
    target: Arc<StoreBackend>,
    key: ObjectPath,
    size: u64,
    etag: Option<String>,
    offset: u64,
    body: Option<futures_util::stream::BoxStream<'static, object_store::Result<Bytes>>>,
    buffered: Bytes,
    gets: Arc<std::sync::atomic::AtomicU64>,
    diagnostics: Arc<super::diagnostics::Diagnostics>,
}

impl RawFile {
    pub(crate) fn account_read(&self, storage: &Storage, size: u64) {
        let counter = if matches!(self, Self::Cached { .. } | Self::Memory { .. }) {
            &storage.cache_hit_bytes
        } else {
            &storage.backend_read_bytes
        };
        counter.fetch_add(size, Ordering::Relaxed);
    }
    pub async fn seek(&mut self, position: std::io::SeekFrom) -> Result<u64> {
        match self {
            Self::Local(file) | Self::Cached { file, .. } => Ok(file.seek(position).await?),
            Self::Memory { data, offset } => {
                let std::io::SeekFrom::Start(position) = position else {
                    anyhow::bail!("raw reads require an absolute offset");
                };
                ensure!(position <= data.len() as u64, IntegrityError::Length);
                *offset = position as usize;
                Ok(position)
            }
            Self::Remote(reader) => {
                let std::io::SeekFrom::Start(offset) = position else {
                    anyhow::bail!("raw reads require an absolute offset");
                };
                ensure!(offset <= reader.size, IntegrityError::Length);
                reader.offset = offset;
                reader.body = None;
                reader.buffered = Bytes::new();
                Ok(offset)
            }
        }
    }
    pub async fn rewind(&mut self) -> Result<()> {
        self.seek(std::io::SeekFrom::Start(0)).await?;
        Ok(())
    }
    pub async fn read(&mut self, output: &mut [u8]) -> Result<usize> {
        let reader = match self {
            Self::Local(file) | Self::Cached { file, .. } => return Ok(file.read(output).await?),
            Self::Memory { data, offset } => {
                let count = output.len().min(data.len() - *offset);
                output[..count].copy_from_slice(&data[*offset..*offset + count]);
                *offset += count;
                return Ok(count);
            }
            Self::Remote(reader) => reader,
        };
        if output.is_empty() || reader.offset == reader.size {
            return Ok(0);
        }
        if reader.body.is_none() {
            let span = reader.offset..reader.size;
            let result = reader
                .target
                .backend
                .get_opts(
                    &reader.key,
                    GetOptions {
                        range: Some(GetRange::Bounded(span.clone())),
                        if_match: reader.etag.clone(),
                        ..Default::default()
                    },
                )
                .await
                .inspect_err(|error| reader.diagnostics.backend(error))
                .map_err(remote_error)?;
            ensure!(
                result.meta.size == reader.size && result.range == span,
                IntegrityError::Length
            );
            reader.gets.fetch_add(1, Ordering::Relaxed);
            reader.body = Some(result.into_stream());
        }
        while reader.buffered.is_empty() {
            let next = reader
                .body
                .as_mut()
                .unwrap()
                .next()
                .await
                .context(IntegrityError::Length)?;
            reader.buffered = next
                .inspect_err(|error| reader.diagnostics.backend(error))
                .map_err(remote_error)?;
            ensure!(
                reader.buffered.len() as u64 <= reader.size - reader.offset,
                IntegrityError::Length
            );
        }
        let count = output.len().min(reader.buffered.len());
        output[..count].copy_from_slice(&reader.buffered.split_to(count));
        reader.offset += count as u64;
        Ok(count)
    }
}

fn remote_error(error: object_store::Error) -> anyhow::Error {
    if matches!(error, object_store::Error::NotFound { .. }) {
        anyhow::Error::new(error).context(IntegrityError::RemoteMissing)
    } else {
        error.into()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn remote_parts_fit_the_s3_limit() {
        assert!(super::remote_size(0).is_ok());
        assert!(super::remote_size(super::MAX_REMOTE_SIZE).is_ok());
        assert!(super::remote_size(super::MAX_REMOTE_SIZE + 1).is_err());
        for size in [
            1,
            50 * 1024u64.pow(3),
            5 * 1024u64.pow(4),
            super::MAX_REMOTE_SIZE,
        ] {
            let part = super::replay_part_size(size, &[]).unwrap();
            assert!(part >= super::PART_SIZE as u64 && part <= super::MAX_PART_SIZE);
            assert!(size.div_ceil(part) <= 10000);
        }
        let first = 7 * 1024 * 1024 + 13;
        assert_eq!(
            super::replay_part_size(first * 2 + 17, &[first as i64, first as i64, 17]).unwrap(),
            first
        );
        let size = 8 * 1024 * 1024;
        assert_eq!(
            super::replay_part_size(size, &[5 * 1024 * 1024, 3 * 1024 * 1024]).unwrap(),
            5 * 1024 * 1024
        );
        assert_eq!(
            super::replay_part_size(512 * 1024 * 1024 + 1, &[512 * 1024 * 1024, 1]).unwrap(),
            64 * 1024 * 1024
        );
    }
}
