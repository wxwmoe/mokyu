use anyhow::{Context, Result, ensure};
use bytes::Bytes;
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

enum Destination {
    Local(Local),
    Single(Arc<dyn ObjectStore>, Path, Vec<Bytes>),
    Multipart(WriteMultipart),
}

pub(crate) struct Writer(Option<Destination>);

impl Writer {
    pub(crate) async fn new(
        backend: Arc<dyn ObjectStore>,
        path: Path,
        root: Option<&FilePath>,
        upper: usize,
    ) -> Result<Self> {
        let destination = if let Some(root) = root {
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
            Destination::Local(Local {
                temporary,
                destination,
                file,
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
            Destination::Local(local) => local.file.write_all(&bytes).await?,
            Destination::Single(_, _, parts) => parts.push(bytes),
            Destination::Multipart(upload) => {
                upload.wait_for_capacity(1).await?;
                upload.put(bytes);
            }
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
        }
        Ok(())
    }
}

impl Drop for Writer {
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
