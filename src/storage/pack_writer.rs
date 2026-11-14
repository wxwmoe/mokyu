use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures_util::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, WriteMultipart, path::Path};
use std::{
    path::{Path as FilePath, PathBuf},
    sync::Arc,
};
use tokio::io::AsyncWriteExt;

struct Local {
    temporary: PathBuf,
    destination: PathBuf,
    file: tokio::fs::File,
    disk: Arc<super::local::Disk>,
    floor: u64,
    _space: super::local::Space,
    permit: Arc<super::StorePermit>,
}
impl Drop for Local {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.temporary);
    }
}

pub(super) async fn recover_local(root: &FilePath) -> Result<()> {
    let staging = root.join("packs/.writing");
    match tokio::fs::symlink_metadata(&staging).await {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        result => ensure!(result?.is_dir(), "invalid pack staging directory"),
    }
    ensure!(
        tokio::fs::symlink_metadata(root.join("packs"))
            .await?
            .is_dir(),
        "invalid pack directory"
    );
    let mut entries = tokio::fs::read_dir(&staging).await?;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        if entry.file_type().await?.is_file()
            && let Some(id) = name.to_str().and_then(|name| name.strip_suffix(".part"))
            && uuid::Uuid::parse_str(id).is_ok_and(|uuid| uuid.simple().to_string() == id)
        {
            match std::fs::File::open(entry.path()) {
                Ok(file) => match file.try_lock() {
                    Ok(()) => {
                        std::fs::remove_file(entry.path())?;
                    }
                    Err(std::fs::TryLockError::WouldBlock) => {}
                    Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
                },
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
    }
    Ok(())
}

const PART_SIZE: usize = 8 * 1024 * 1024;
use crate::content::pack::SINGLE_PUT;

struct Spool<'a> {
    native: Arc<super::native::store::Store>,
    path: Path,
    files: super::file_cache::Files<'a>,
    file: super::raw_request::File,
    size: u64,
    upper: u64,
    digest: blake3::Hasher,
    statistics: Option<Arc<crate::status::stats::Counters>>,
}

enum Destination<'a> {
    Local(Local),
    Single(Arc<dyn ObjectStore>, Path, Vec<Bytes>),
    Multipart(WriteMultipart),
    Spool(Box<Spool<'a>>),
    Stream {
        sink: super::native::writer::Sink,
        size: u64,
        statistics: Option<Arc<crate::status::stats::Counters>>,
        operation: Option<crate::status::stats::Operation>,
    },
}

pub(crate) struct Writer<'a>(Option<Destination<'a>>);

impl<'a> Writer<'a> {
    pub(crate) async fn spool(
        native: Arc<super::native::store::Store>,
        path: Path,
        upper: usize,
        files: super::file_cache::Files<'a>,
        policy: super::CachePolicy,
        statistics: Option<Arc<crate::status::stats::Counters>>,
    ) -> Result<Self> {
        let id = uuid::Uuid::parse_str(path.filename().context("missing pack identity")?)?;
        let file = super::raw_request::File::create_in(&files, id, policy).await?;
        Ok(Self(Some(Destination::Spool(Box::new(Spool {
            native,
            path,
            files,
            file,
            size: 0,
            upper: upper as u64,
            digest: blake3::Hasher::new(),
            statistics,
        })))))
    }

    pub(crate) fn stream(
        native: Arc<super::native::store::Store>,
        path: Path,
        size: u64,
        statistics: Option<Arc<crate::status::stats::Counters>>,
    ) -> Self {
        let operation = statistics.as_ref().map(|statistics| statistics.begin());
        Self(Some(Destination::Stream {
            sink: super::native::writer::Sink::new(native, path, size),
            size,
            statistics,
            operation,
        }))
    }

    pub(crate) async fn new(
        backend: Arc<dyn ObjectStore>,
        path: Path,
        root: Option<&FilePath>,
        upper: usize,
        space: Option<(Arc<super::local::Disk>, u64, Arc<super::StorePermit>)>,
    ) -> Result<Self> {
        let destination = if let Some(root) = root {
            let (disk, floor, permit) = space.context("local pack budget is missing")?;
            let packs = root.join("packs");
            let staging = packs.join(".writing");
            let destination = root.join(path.as_ref());
            let parent = destination.parent().context("missing pack parent")?;
            super::raw::directory(&packs).await?;
            super::raw::directory(parent).await?;
            super::raw::directory(&staging).await?;
            let temporary = staging.join(format!("{}.part", uuid::Uuid::now_v7().simple()));
            let file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)?;
            file.try_lock()?;
            let file = tokio::fs::File::from_std(file);
            let space = disk.reserve_space(root, floor, 0)?;
            Destination::Local(Local {
                temporary,
                destination,
                file,
                disk,
                floor,
                _space: space,
                permit,
            })
        } else if upper <= SINGLE_PUT {
            Destination::Single(backend, path, Vec::new())
        } else {
            Destination::Multipart(WriteMultipart::new_with_chunk_size(
                backend.put_multipart(&path).await?,
                PART_SIZE,
            ))
        };
        Ok(Self(Some(destination)))
    }

    pub(crate) async fn put(&mut self, bytes: Bytes) -> Result<()> {
        match self.0.as_mut().context("pack writer already closed")? {
            Destination::Local(local) => {
                let mut space = local.disk.reserve_space(
                    local.temporary.parent().unwrap(),
                    local.floor,
                    bytes.len() as u64,
                )?;
                space.observe(vec![local.temporary.clone()])?;
                if local.disk.data.is_none() {
                    local.file.write_all(&bytes).await?;
                    return Ok(());
                }
                let mut file = local.file.try_clone().await?;
                let permit = local.permit.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let _space = space;
                    #[cfg(feature = "fault-injection")]
                    crate::faults::point("data-directory-write").await;
                    file.write_all(&bytes).await?;
                    file.flush().await
                })
                .await??;
            }
            Destination::Single(_, _, parts) => parts.push(bytes),
            Destination::Multipart(upload) => {
                upload.wait_for_capacity(1).await?;
                upload.put(bytes);
            }
            Destination::Spool(spool) => {
                let size = spool
                    .size
                    .checked_add(bytes.len() as u64)
                    .context("pack length overflow")?;
                ensure!(size <= spool.upper, "pack exceeds its planned bound");
                spool.file.append_in(&spool.files, &bytes).await?;
                spool.digest.update(&bytes);
                spool.size = size;
            }
            Destination::Stream { sink, .. } => sink.write(&bytes).await?,
        }
        Ok(())
    }

    pub(crate) async fn finish(mut self) -> Result<()> {
        match self.0.take().context("pack writer already closed")? {
            Destination::Local(mut local) => {
                local.file.flush().await?;
                local.file.sync_all().await?;
                tokio::fs::rename(&local.temporary, &local.destination).await?;
                super::raw::sync_directory(
                    local.destination.parent().context("missing pack parent")?,
                )
                .await?;
                super::raw::sync_directory(
                    local.temporary.parent().context("missing staging parent")?,
                )
                .await?;
            }
            Destination::Single(backend, path, parts) => {
                backend.put(&path, parts.into_iter().collect()).await?;
            }
            Destination::Multipart(upload) => {
                upload.finish().await?;
            }
            Destination::Spool(spool) => {
                let mut operation = spool
                    .statistics
                    .as_ref()
                    .map(|statistics| statistics.begin());
                let expected = spool.digest.finalize();
                let mut native = spool.native.as_ref().clone();
                let result = loop {
                    let mut input = spool.file.replay();
                    let body = async_stream::try_stream! {
                        let mut digest = blake3::Hasher::new();
                        while let Some(bytes) = input.try_next().await? {
                            digest.update(&bytes);
                            yield bytes;
                        }
                        if digest.finalize() != expected {
                            Err(crate::content::codec::IntegrityError::Hash)?;
                        }
                    };
                    let result = native
                        .write(
                            &spool.path,
                            spool.size,
                            Box::pin(body),
                            object_store::PutMode::Create.into(),
                        )
                        .await;
                    if result.as_ref().err().and_then(crate::model::error)
                        == Some(crate::model::Error::StorageForwardFailed)
                        && native.region.take().is_some()
                    {
                        continue;
                    }
                    break result;
                };
                if let Some(operation) = &mut operation {
                    operation.finish(result.is_err());
                }
                result?;
                if let Some(statistics) = &spool.statistics {
                    statistics.bytes(spool.size);
                }
            }
            Destination::Stream {
                sink,
                size,
                statistics,
                mut operation,
            } => {
                let result = sink.finish().await;
                if let Some(operation) = &mut operation {
                    operation.finish(result.is_err());
                }
                result?;
                if let Some(statistics) = statistics {
                    statistics.bytes(size);
                }
            }
        }
        Ok(())
    }
}

impl Drop for Writer<'_> {
    fn drop(&mut self) {
        if let Some(Destination::Multipart(upload)) = self.0.take()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            runtime.spawn(async move {
                let _ = upload.abort().await;
            });
        }
    }
}

pub(crate) fn members<'a>(
    backend: Arc<dyn ObjectStore>,
    path: Path,
    pack: &'a crate::content::pack::Pack,
    entries: &'a [crate::content::pack::frame::Entry],
    gets: Arc<std::sync::atomic::AtomicU64>,
    diagnostics: Arc<super::diagnostics::Diagnostics>,
) -> impl futures_util::Stream<Item = Result<Vec<u8>>> + 'a {
    use crate::content::{codec::IntegrityError, pack::frame};
    async_stream::try_stream! {
        let size=u64::try_from(pack.stored_size.context(IntegrityError::Metadata)?)?;
        frame::validate_entries(entries,size,pack.raw_size,pack.member_count,false)?;
        let mut reader=super::raw::RawFile::immutable(backend,path,size,gets,diagnostics);
        let mut header=[0;8];
        reader.read_exact(&mut header).await?;
        (&header==frame::HEADER).then_some(()).ok_or(IntegrityError::Metadata)?;
        let mut digest=blake3::Hasher::new();
        digest.update(&header);
        let mut offset=header.len() as u64;
        let mut scratch=vec![0;65536];
        for entry in entries.iter().map(Some).chain(std::iter::once(None)) {
            let limit=entry.map_or(size,|e|e.position.offset);
            while offset<limit {
                let count=(limit-offset).min(scratch.len() as u64) as usize;
                reader.read_exact(&mut scratch[..count]).await?;
                digest.update(&scratch[..count]);
                offset+=count as u64;
            }
            if let Some(entry)=entry {
                let len=usize::try_from(entry.position.encoding.stored_size)?;
                let mut body=vec![0;len];
                reader.read_exact(&mut body).await?;
                digest.update(&body);
                offset+=len as u64;
                yield body;
            }
        }
        (offset==size && pack.digest.as_deref()==Some(digest.finalize().as_bytes().as_slice())).then_some(()).ok_or(IntegrityError::Hash)?;
    }
}
