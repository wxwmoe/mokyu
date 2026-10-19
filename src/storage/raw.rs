use super::{CachePin, Storage, StoreBackend, StorePermit, resource_gate};
use crate::content::codec::IntegrityError;
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
    hash: Option<blake3::Hasher>,
    reserved: u64,
    pending: Arc<Mutex<u64>>,
    floor: u64,
    prepared: bool,
    retained: bool,
    _target: Arc<StoreBackend>,
    _permit: StorePermit,
    _global: resource_gate::Permit,
}

impl Drop for LocalWriter<'_> {
    fn drop(&mut self) {
        *self.pending.lock().unwrap() -= self.reserved;
        // The journaled source owns incomplete files until normal GC removes them.
    }
}

async fn sync_directory(path: &Path) -> Result<()> {
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
    fn reserve(&mut self, size: u64) -> Result<()> {
        if size <= self.reserved {
            return Ok(());
        }
        let path = self.destination.parent().unwrap().to_owned();
        let extra = size - self.reserved;
        let mut pending = self.pending.lock().unwrap();
        ensure!(
            fs2::available_space(path)?
                .saturating_sub(*pending)
                .saturating_sub(extra)
                >= self.floor,
            "filesystem free-space floor reached"
        );
        *pending = pending
            .checked_add(extra)
            .context("raw reservation overflow")?;
        self.reserved = size;
        Ok(())
    }

    pub async fn write(&mut self, data: &[u8]) -> Result<()> {
        let size = self
            .size
            .checked_add(data.len() as u64)
            .context("raw length overflow")?;
        if !self.prepared {
            self.reserve(size)?;
            self.file.write_all(data).await?;
        }
        self.size = size;
        if let Some(hash) = &mut self.hash {
            hash.update(data);
        }
        Ok(())
    }

    pub async fn finish(mut self, expected: u64) -> Result<Option<[u8; 32]>> {
        ensure!(self.size == expected, "raw receive length mismatch");
        self.file.flush().await?;
        self.file.sync_all().await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("raw-file-durable").await;
        // A new immutable name must never replace an existing file, even on retry.
        tokio::fs::hard_link(&self.temporary, &self.destination).await?;
        if !self.retained {
            tokio::fs::remove_file(&self.temporary).await?;
        }
        sync_directory(self.destination.parent().unwrap()).await?;
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
    _permit: StorePermit,
    _global: resource_gate::Permit,
}

impl Storage {
    pub(crate) async fn raw_replay_writer(
        &self,
        storage: Uuid,
        source: Uuid,
        size: i64,
        parts: &[i64],
    ) -> Result<RawWriter<'_>> {
        let declared = (!self.is_local(storage)?).then_some(size);
        let writer = self.raw_writer(storage, source, declared).await?;
        Ok(match writer {
            RawWriter::Remote(writer) if size > PART_SIZE as i64 => {
                let part_size = replay_part_size(size as u64, parts)?;
                // Avoid rereading when the existing bounded writer sends as few requests.
                if (size as u64).div_ceil(PART_SIZE as u64) <= (size as u64).div_ceil(part_size) {
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
        &self,
        storage: Uuid,
        source: Uuid,
        declared: Option<i64>,
    ) -> Result<RawWriter<'_>> {
        let target = self.store_backend(storage)?;
        self.verify_raw_target(&target).await?;
        let permit = target.writes.acquire().await?;
        let global = self.writes.acquire().await?;
        let hash = target.deduplication_enabled.then(blake3::Hasher::new);
        if !target.is_local {
            remote_size(declared.unwrap_or(0).max(0) as u64)?;
            return Ok(RawWriter::Remote(RemoteWriter {
                storage: self,
                source,
                target,
                key: remote_path(source),
                upload: None,
                parts: Vec::new(),
                buffer: Vec::new(),
                size: 0,
                hash,
                _permit: permit,
                _global: global,
            }));
        }
        let root = target
            .raw_root
            .as_ref()
            .context("raw files require local storage")?;
        directory(root).await?;
        let destination = path(root, source);
        directory(destination.parent().unwrap()).await?;
        let temporary = destination.with_extension("partial");
        let mut options = tokio::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o644);
        let file = options.open(&temporary).await?;
        let mut writer = LocalWriter {
            storage: self,
            file,
            temporary,
            destination,
            size: 0,
            hash,
            reserved: 0,
            pending: self.raw_reserved.clone(),
            floor: self.disk.floor,
            prepared: false,
            retained: false,
            _target: target,
            _permit: permit,
            _global: global,
        };
        writer.reserve(declared.unwrap_or(0).max(0) as u64)?;
        Ok(RawWriter::Local(writer))
    }

    pub(crate) async fn raw_reader(&self, source: &RawSource, verify: bool) -> Result<RawReader> {
        self.raw_reader_inner(source, verify)
            .await
            .inspect_err(|error| self.diagnostics.content(error))
    }

    async fn raw_reader_inner(&self, source: &RawSource, verify: bool) -> Result<RawReader> {
        source.validate()?;
        let pin = {
            let _gate = self.source_gate.read().await;
            self.physical_pin(source.id)
        };
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
            let file_path = path(root, source.id);
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
            let key = remote_path(source.id);
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
            }
            ensure!(size == source.size as u64, IntegrityError::Length);
            ensure!(
                Some(digest.finalize().as_slice()) == source.digest.as_deref(),
                IntegrityError::Hash
            );
            file.rewind().await?;
            self.backend_read_bytes.fetch_add(size, Ordering::Relaxed);
        }
        Ok(RawReader {
            file,
            _pin: pin,
            _permit: permit,
            _global: global,
        })
    }

    pub(crate) async fn remove_raw(&self, source: &RawSource) -> Result<()> {
        let target = self.store_backend(source.storage_id)?;
        self.verify_raw_target(&target).await?;
        if !target.is_local {
            let _permit = target.controls.acquire().await?;
            let key = remote_path(source.id);
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
        let destination = path(root, source.id);
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

fn remote_size(size: u64) -> Result<()> {
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

fn remote_path(id: Uuid) -> ObjectPath {
    ObjectPath::from(format!("raw/{}", super::chunk_name(id)))
}

pub(crate) enum RawWriter<'a> {
    Local(LocalWriter<'a>),
    Remote(RemoteWriter<'a>),
    Replay(ReplayWriter<'a>),
}

impl RawWriter<'_> {
    pub async fn prepare_local(
        &mut self,
        ranges: Vec<super::assembly::Range>,
        app: &crate::app::App,
        upload: Uuid,
        source: Uuid,
    ) -> Result<Option<super::assembly::Prepared>> {
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
    source: Uuid,
    target: Arc<StoreBackend>,
    key: ObjectPath,
    upload: Option<String>,
    parts: Vec<object_store::multipart::PartId>,
    buffer: Vec<u8>,
    size: u64,
    hash: Option<blake3::Hasher>,
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
            if let Err(error) = self
                .storage
                .raw_store
                .set_upload(self.source, &upload)
                .await
            {
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
    pub async fn write(&mut self, data: &[u8]) -> Result<()> {
        match self {
            Self::Local(writer) => writer.write(data).await,
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
                        writer.part().await?;
                    }
                }
                Ok(())
            }
        }
    }
    pub async fn finish(self, expected: u64) -> Result<Option<[u8; 32]>> {
        match self {
            Self::Local(writer) => writer.finish(expected).await,
            Self::Replay(_) => anyhow::bail!("raw upload requires its replay source"),
            Self::Remote(mut writer) => {
                ensure!(writer.size == expected, "raw receive length mismatch");
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
                                mode: PutMode::Create,
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
                Ok(writer.hash.as_ref().map(|hash| *hash.finalize().as_bytes()))
            }
        }
    }

    pub async fn finish_replay<'a>(
        self,
        expected: u64,
        mut input: impl FnMut(
            std::ops::Range<u64>,
        ) -> futures_util::stream::BoxStream<'a, Result<Bytes>>,
    ) -> Result<Option<[u8; 32]>> {
        let Self::Replay(mut replay) = self else {
            return self.finish(expected).await;
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
        Self::Remote(replay.writer).finish(expected).await
    }
}

pub(crate) enum RawFile {
    Local(tokio::fs::File),
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
    pub async fn seek(&mut self, position: std::io::SeekFrom) -> Result<u64> {
        match self {
            Self::Local(file) => Ok(file.seek(position).await?),
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
        let Self::Remote(reader) = self else {
            let Self::Local(file) = self else {
                unreachable!()
            };
            return Ok(file.read(output).await?);
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
