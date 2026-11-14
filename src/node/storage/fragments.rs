use super::files::Files;
use crate::{
    content::fragment::{self, Digest, FOOTER_SIZE, HEADER_SIZE, Header, UNIT},
    model::storage_payload::Receipt,
    storage::{Disk, local},
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::mpsc;

pub(super) type Check = Arc<dyn Fn() -> Result<()> + Send + Sync>;

pub(super) enum Input {
    Data(Bytes),
    Complete,
}

pub(super) use crate::model::storage_transfer::FragmentState as State;

pub(super) fn temporary(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".part");
    name.into()
}

pub(super) fn open(path: &Path, write: bool) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(write);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options.open(path)?;
    ensure!(file.metadata()?.is_file(), "fragment is not a regular file");
    Ok(file)
}

fn path(files: &Files, header: &Header) -> Result<PathBuf> {
    header.validate()?;
    ensure!(
        header.media == files.media.media_id || files.backup,
        "fragment belongs to another media"
    );
    Ok(files.disk.root.join(header.area.path(header.fragment)))
}

pub(super) fn check_directories(root: &Path, destination: &Path) -> Result<()> {
    let mut directory = root.to_owned();
    for component in destination
        .parent()
        .context("fragment parent is missing")?
        .strip_prefix(root)?
        .components()
    {
        ensure!(
            matches!(component, std::path::Component::Normal(_)),
            "invalid fragment directory"
        );
        directory.push(component);
        match std::fs::symlink_metadata(&directory) {
            Ok(metadata) => ensure!(metadata.is_dir(), "fragment parent is not a directory"),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn same_plan(planned: &Header, actual: &Header) -> Result<()> {
    let mut expected = actual.clone();
    if planned.digest == [0; 32] {
        expected.digest = [0; 32];
    }
    ensure!(
        &expected == planned,
        "immutable fragment identity or content differs"
    );
    Ok(())
}

pub(super) fn inspect_file(file: &mut File) -> Result<Receipt> {
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = [0; HEADER_SIZE];
    file.read_exact(&mut bytes)?;
    let header = Header::decode(&bytes).context(crate::content::codec::IntegrityError::Metadata)?;
    ensure!(
        file.metadata()?.len() == header.file_bytes(),
        crate::content::codec::IntegrityError::Length
    );
    file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
    let mut footer = [0; FOOTER_SIZE];
    file.read_exact(&mut footer)?;
    let root = fragment::verify_footer(&header, &footer, None)
        .context(crate::content::codec::IntegrityError::Metadata)?;
    let receipt = Receipt {
        commitment: fragment::commitment(&header, &root)?,
        header,
        root,
    };
    receipt.validate()?;
    Ok(receipt)
}

fn inspect_partial(file: &mut File, planned: &Header) -> Result<u64> {
    let length = file.metadata()?.len();
    ensure!(
        length <= planned.file_bytes(),
        "partial fragment exceeds its planned size"
    );
    let initial = planned.encode()?;
    let mut prefix = vec![0; length.min(HEADER_SIZE as u64) as usize];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut prefix)?;
    // Only the digest fields can change while finishing a known partial file.
    for (index, byte) in prefix.iter().enumerate() {
        if !(208..240).contains(&index) && index < 288 {
            ensure!(
                *byte == initial[index],
                "partial fragment belongs to another plan"
            );
        }
    }
    Ok(length)
}

#[derive(Default)]
struct ReadProof {
    nodes: Vec<usize>,
    hashes: Vec<fragment::Digest>,
}

fn unit(
    file: &mut File,
    receipt: &Receipt,
    index: usize,
    proof: &mut ReadProof,
) -> Result<Vec<u8>> {
    let header = &receipt.header;
    let mut bytes = vec![0; header.unit_bytes(index)?];
    file.seek(SeekFrom::Start(
        HEADER_SIZE as u64 + index as u64 * UNIT as u64,
    ))?;
    file.read_exact(&mut bytes)?;
    let nodes = fragment::proof_indices(header, index)?;
    proof.hashes.resize(nodes.len(), [0; 32]);
    for (level, &node) in nodes.iter().enumerate() {
        if proof.nodes.get(level) == Some(&node) {
            continue;
        }
        file.seek(SeekFrom::Start(
            HEADER_SIZE as u64 + header.shard_bytes + node as u64 * 32,
        ))?;
        file.read_exact(&mut proof.hashes[level])?;
    }
    fragment::verify_unit(header, index, &bytes, &proof.hashes, &receipt.root)
        .context(crate::content::codec::IntegrityError::Hash)?;
    proof.nodes = nodes;
    Ok(bytes)
}

fn io(disk: &Arc<Disk>, check: &Check) -> Result<tokio::sync::OwnedSemaphorePermit> {
    let permit = wait(disk.io.clone().acquire_owned(), check)??;
    check()?;
    Ok(permit)
}

fn validate_file(
    file: &mut File,
    receipt: &Receipt,
    disk: &Arc<Disk>,
    check: &Check,
) -> Result<()> {
    let mut hash = blake3::Hasher::new();
    let mut proof = ReadProof::default();
    for index in 0..receipt.header.leaves() {
        let _permit = io(disk, check)?;
        hash.update(&unit(file, receipt, index, &mut proof)?);
    }
    ensure!(
        hash.finalize().as_bytes() == &receipt.header.digest,
        crate::content::codec::IntegrityError::Hash
    );
    Ok(())
}

pub(super) fn directories(root: &Path, destination: &Path) -> Result<()> {
    let relative = destination
        .parent()
        .context("fragment parent is missing")?
        .strip_prefix(root)?;
    let mut parent = root.to_owned();
    for component in relative.components() {
        ensure!(
            matches!(component, std::path::Component::Normal(_)),
            "invalid fragment directory"
        );
        let next = parent.join(component);
        match std::fs::create_dir(&next) {
            Ok(()) => File::open(&parent)?.sync_all()?,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => (),
            Err(error) => return Err(error.into()),
        }
        ensure!(
            std::fs::symlink_metadata(&next)?.is_dir(),
            "fragment parent is not a directory"
        );
        parent = next;
    }
    Ok(())
}

fn exclusive(disk: &Arc<Disk>, header: &Header) -> Result<local::Fragment> {
    ensure!(
        disk.pending.lock().unwrap().insert(header.fragment),
        "fragment has an active mutation"
    );
    Ok(local::Fragment {
        disk: disk.clone(),
        id: header.fragment,
    })
}

pub(super) async fn checked<F: std::future::Future>(future: F, check: &Check) -> Result<F::Output> {
    tokio::pin!(future);
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
    loop {
        check()?;
        tokio::select! {
            biased;
            value = &mut future => return Ok(value),
            _ = interval.tick() => (),
        }
    }
}

fn wait<F: std::future::Future>(future: F, check: &Check) -> Result<F::Output> {
    tokio::runtime::Handle::current().block_on(checked(future, check))
}

fn receive(
    input: &mut mpsc::Receiver<Input>,
    expected: u64,
    disk: &Arc<Disk>,
    check: &Check,
    mut consume: impl FnMut(&[u8]) -> Result<()>,
) -> Result<Digest> {
    let mut hash = blake3::Hasher::new();
    let mut length = 0u64;
    loop {
        check()?;
        match wait(input.recv(), check)? {
            Some(Input::Data(bytes)) => {
                let _permit = io(disk, check)?;
                ensure!(
                    !bytes.is_empty() && bytes.len() <= UNIT,
                    "invalid fragment stream frame"
                );
                length = length
                    .checked_add(bytes.len() as u64)
                    .context("fragment stream overflow")?;
                ensure!(length <= expected, "fragment stream is too long");
                consume(&bytes)?;
                hash.update(&bytes);
            }
            Some(Input::Complete) => {
                ensure!(length == expected, "fragment stream is truncated");
                return Ok(*hash.finalize().as_bytes());
            }
            None => anyhow::bail!("fragment stream was cancelled"),
        }
    }
}

pub(super) async fn write(
    files: &Files,
    planned: Header,
    mut input: mpsc::Receiver<Input>,
    check: Check,
) -> Result<Receipt> {
    let destination = path(files, &planned)?;
    check()?;
    let backup = files.backup && planned.digest != [0; 32];
    let disk = files.disk.clone();
    let lock = files.lock.clone();
    let permit = checked(disk.io.clone().acquire_owned(), &check).await??;
    let mutation = files
        .mutations
        .clone()
        .try_read_owned()
        .context("storage is frozen for inventory")?;
    tokio::task::spawn_blocking(move || {
        let (_lock, _mutation_scope) = (lock, mutation);
        let _mutation = exclusive(&disk, &planned)?;
        check()?;
        check_directories(&disk.root, &destination)?;
        let pending = temporary(&destination);
        let existing = match open(&destination, false) {
            // A backup grant fixes the expected digest; stage a replacement before switching a damaged copy.
            Ok(_) if backup => None,
            Ok(mut file) => Some((inspect_file(&mut file)?, file, true)),
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
            {
                match open(&pending, false) {
                    Ok(mut file) => inspect_file(&mut file)
                        .ok()
                        .map(|receipt| (receipt, file, false)),
                    Err(error)
                        if error
                            .downcast_ref::<std::io::Error>()
                            .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
                    {
                        None
                    }
                    Err(error) => return Err(error),
                }
            }
            Err(error) => return Err(error),
        };
        if let Some((receipt, mut file, published)) = existing {
            same_plan(&planned, &receipt.header)?;
            drop(permit);
            let hash = receive(&mut input, planned.shard_bytes, &disk, &check, |_| Ok(()))?;
            ensure!(
                hash == receipt.header.digest,
                "immutable fragment content differs"
            );
            validate_file(&mut file, &receipt, &disk, &check)?;
            if !published {
                let _permit = io(&disk, &check)?;
                let _accounting = disk.mutation()?;
                file.sync_all()?;
                check()?;
                std::fs::rename(&pending, &destination)?;
                File::open(destination.parent().unwrap())?.sync_all()?;
            }
            return Ok(receipt);
        }
        let initial = planned.encode()?;
        let allocated = match open(&pending, false) {
            Ok(mut file) => {
                inspect_partial(&mut file, &planned)?;
                local::allocated(&file.metadata()?)
            }
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
            {
                0
            }
            Err(error) => return Err(error),
        };
        let mut space = disk.reserve_space(
            &disk.root,
            disk.floor,
            planned.file_bytes().saturating_sub(allocated) + 65536,
        )?;
        space.observe(vec![
            destination.clone(),
            pending.clone(),
            destination.parent().unwrap().to_owned(),
            disk.root.join(planned.area.directory()),
        ])?;
        directories(&disk.root, &destination)?;
        let mut file = match open(&pending, true) {
            Ok(file) => file,
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
            {
                OpenOptions::new()
                    .create_new(true)
                    .read(true)
                    .write(true)
                    .open(&pending)?
            }
            Err(error) => return Err(error),
        };
        file.set_len(0)?;
        file.write_all(&initial)?;
        #[cfg(feature = "fault-injection")]
        crate::faults::blocking("storage-fragment-header");
        drop(permit);
        let mut leaves = Vec::with_capacity(planned.leaves());
        let mut buffer = Vec::with_capacity(UNIT);
        let digest = receive(
            &mut input,
            planned.shard_bytes,
            &disk,
            &check,
            |mut bytes| {
                file.write_all(bytes)?;
                while !bytes.is_empty() {
                    let length = bytes.len().min(UNIT - buffer.len());
                    buffer.extend_from_slice(&bytes[..length]);
                    bytes = &bytes[length..];
                    if buffer.len() == UNIT {
                        leaves.push(fragment::leaf(leaves.len(), &buffer));
                        buffer.clear();
                    }
                }
                Ok(())
            },
        )?;
        let _permit = io(&disk, &check)?;
        if !buffer.is_empty() || leaves.is_empty() {
            leaves.push(fragment::leaf(leaves.len(), &buffer));
        }
        ensure!(
            planned.digest == [0; 32] || planned.digest == digest,
            "fragment body differs from its plan"
        );
        let header = Header { digest, ..planned };
        let tree = fragment::tree(&header, &leaves)?;
        file.write_all(tree.as_flattened())?;
        file.write_all(&fragment::footer(&header, &tree[0])?)?;
        #[cfg(feature = "fault-injection")]
        crate::faults::blocking("storage-fragment-content");
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&header.encode()?)?;
        file.sync_all()?;
        #[cfg(feature = "fault-injection")]
        crate::faults::blocking("storage-fragment-synced");
        check()?;
        std::fs::rename(&pending, &destination)?;
        #[cfg(feature = "fault-injection")]
        crate::faults::blocking("storage-fragment-renamed");
        File::open(destination.parent().unwrap())?.sync_all()?;
        #[cfg(feature = "fault-injection")]
        crate::faults::blocking("storage-fragment-published");
        let receipt = Receipt {
            commitment: fragment::commitment(&header, &tree[0])?,
            header,
            root: tree[0],
        };
        receipt.validate()?;
        Ok(receipt)
    })
    .await?
}

pub(super) async fn verify_backup(
    files: &Files,
    receipt: Receipt,
    check: Check,
) -> Result<Receipt> {
    let destination = path(files, &receipt.header)?;
    let disk = files.disk.clone();
    let lock = files.lock.clone();
    tokio::task::spawn_blocking(move || {
        let _lock = lock;
        let permit = io(&disk, &check)?;
        check_directories(&disk.root, &destination)?;
        let mut file = open(&destination, false)?;
        ensure!(
            inspect_file(&mut file)? == receipt,
            crate::content::codec::IntegrityError::Metadata
        );
        drop(permit);
        validate_file(&mut file, &receipt, &disk, &check)?;
        check()?;
        Ok(receipt)
    })
    .await?
}

pub(super) async fn inspect(files: &Files, planned: Header, check: Check) -> Result<State> {
    let destination = path(files, &planned)?;
    let lock = files.lock.clone();
    let root = files.disk.root.clone();
    let permit = checked(files.disk.io.clone().acquire_owned(), &check).await??;
    tokio::task::spawn_blocking(move || {
        let (_lock, _permit) = (lock, permit);
        check()?;
        check_directories(&root, &destination)?;
        let mut file = match open(&destination, false) {
            Ok(file) => file,
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
            {
                return match open(&temporary(&destination), false) {
                    Ok(mut file) => {
                        let bytes = inspect_partial(&mut file, &planned)?;
                        let complete = inspect_file(&mut file).ok();
                        if let Some(receipt) = &complete {
                            same_plan(&planned, &receipt.header)?;
                        }
                        Ok(State::Pending { bytes, complete })
                    }
                    Err(error)
                        if error
                            .downcast_ref::<std::io::Error>()
                            .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
                    {
                        Ok(State::Missing)
                    }
                    Err(error) => Err(error),
                };
            }
            Err(error) => return Err(error),
        };
        let receipt = inspect_file(&mut file)?;
        same_plan(&planned, &receipt.header)?;
        Ok(State::Complete(receipt))
    })
    .await?
}

pub(super) async fn read(
    files: &Files,
    receipt: Receipt,
    offset: u64,
    length: u64,
    check: Check,
) -> Result<mpsc::Receiver<Result<Bytes>>> {
    receipt.validate()?;
    let destination = path(files, &receipt.header)?;
    let end = offset
        .checked_add(length)
        .context("fragment read range overflow")?;
    ensure!(
        end <= receipt.header.shard_bytes,
        "fragment read exceeds its range"
    );
    let lock = files.lock.clone();
    let root = files.disk.root.clone();
    let disk = files.disk.clone();
    let permit = checked(disk.io.clone().acquire_owned(), &check).await??;
    let (sender, receiver) = mpsc::channel(2);
    tokio::task::spawn_blocking(move || {
        let _lock = lock;
        let result = (|| -> Result<()> {
            check()?;
            check_directories(&root, &destination)?;
            let mut file = open(&destination, false)?;
            ensure!(
                inspect_file(&mut file)? == receipt,
                crate::content::codec::IntegrityError::Metadata
            );
            if offset == 0 && length == receipt.header.shard_bytes {
                file.seek(SeekFrom::Start(HEADER_SIZE as u64 + length))?;
                let mut indexed_root = [0; 32];
                file.read_exact(&mut indexed_root)?;
                ensure!(
                    indexed_root == receipt.root,
                    crate::content::codec::IntegrityError::Metadata
                );
            }
            drop(permit);
            if length == 0 {
                return Ok(());
            }
            let first = offset as usize / UNIT;
            let last = (end - 1) as usize / UNIT;
            let mut proof = ReadProof::default();
            for index in first..=last {
                let bytes = {
                    let _permit = io(&disk, &check)?;
                    unit(&mut file, &receipt, index, &mut proof)?
                };
                let begin = offset.saturating_sub(index as u64 * UNIT as u64) as usize;
                let finish = (end - index as u64 * UNIT as u64).min(bytes.len() as u64) as usize;
                wait(
                    sender.send(Ok(Bytes::from(bytes).slice(begin..finish))),
                    &check,
                )?
                .map_err(|_| anyhow::anyhow!("fragment reader was cancelled"))?;
            }
            Ok(())
        })();
        if let Err(error) = result {
            let _ = wait(sender.send(Err(error)), &check);
        }
    });
    Ok(receiver)
}

pub(super) async fn remove(files: &Files, receipt: Receipt, check: Check) -> Result<bool> {
    receipt.validate()?;
    erase(files, receipt.header.clone(), Some(receipt), check).await
}

pub(super) async fn discard(files: &Files, header: Header, check: Check) -> Result<bool> {
    header.validate()?;
    erase(files, header, None, check).await
}

async fn erase(
    files: &Files,
    header: Header,
    receipt: Option<Receipt>,
    check: Check,
) -> Result<bool> {
    let destination = path(files, &header)?;
    let disk = files.disk.clone();
    let lock = files.lock.clone();
    let permit = checked(disk.io.clone().acquire_owned(), &check).await??;
    let mutation = files
        .mutations
        .clone()
        .try_read_owned()
        .context("storage is frozen for inventory")?;
    tokio::task::spawn_blocking(move || {
        let (_lock, _permit, _mutation_scope) = (lock, permit, mutation);
        let _mutation = exclusive(&disk, &header)?;
        check()?;
        check_directories(&disk.root, &destination)?;
        let mut removed = false;
        for (target, pending) in [
            (destination.clone(), false),
            (temporary(&destination), true),
        ] {
            let mut file = match open(&target, false) {
                Ok(file) => file,
                Err(error)
                    if error
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
                {
                    continue;
                }
                Err(error) => return Err(error),
            };
            let actual = inspect_file(&mut file);
            if pending && actual.is_err() {
                let mut planned = header.clone();
                planned.digest = [0; 32];
                inspect_partial(&mut file, &planned)?;
            } else {
                let actual = actual?;
                same_plan(&header, &actual.header)?;
                ensure!(
                    receipt.as_ref().is_none_or(|receipt| *receipt == actual),
                    "refusing to delete another fragment"
                );
            }
            let mut accounting = disk.mutation()?;
            let mut released = local::allocated(&file.metadata()?);
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                if file.metadata()?.nlink() != 1 {
                    released = 0;
                }
            }
            check()?;
            std::fs::remove_file(&target)?;
            File::open(target.parent().unwrap())?.sync_all()?;
            drop(file);
            #[cfg(feature = "fault-injection")]
            crate::faults::blocking("storage-fragment-removed");
            if let Some(accounting) = &mut accounting {
                accounting.released(released);
            }
            removed = true;
        }
        Ok(removed)
    })
    .await?
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::{
        config::{Budget, Instance, RuntimeSettings},
        content::fragment::Area,
        model::storage_cluster::Layout,
    };
    use uuid::Uuid;

    pub(in crate::node::storage) async fn fixture(
        limit: &str,
    ) -> Result<(tempfile::TempDir, Instance, Budget, Files)> {
        let directory = tempfile::tempdir()?;
        let instance = Instance {
            data_path: directory.path().into(),
            data_limit: Some(limit.into()),
            min_free_space: "1B".into(),
            cpu_jobs: Some(1),
            ..Default::default()
        };
        let budget = Budget::for_runtime(&instance, &RuntimeSettings::default(), &[])?;
        let lock = Arc::new(crate::runtime::lock_directory(directory.path())?);
        let files = Files::open(&instance, &budget, lock).await?;
        files.disk.data.as_ref().unwrap().reconcile()?;
        Ok((directory, instance, budget, files))
    }

    pub(in crate::node::storage) fn header(files: &Files, bytes: u64) -> Header {
        Header {
            area: Area::Blocks,
            namespace: Uuid::now_v7(),
            payload: Uuid::now_v7(),
            fragment: Uuid::now_v7(),
            media: files.media.media_id,
            cluster: Uuid::now_v7(),
            group: Uuid::now_v7(),
            operation: Uuid::now_v7(),
            layout_revision: 1,
            revision: 1,
            generation: 0,
            takeover_epoch: 1,
            offset: 0,
            length: bytes,
            total: bytes,
            shard_bytes: bytes,
            sequence: 0,
            layout: Layout::default(),
            copy: 0,
            shard: 0,
            digest: [0; 32],
        }
    }

    pub(in crate::node::storage) fn input(bytes: Bytes, complete: bool) -> mpsc::Receiver<Input> {
        let (sender, receiver) = mpsc::channel(2);
        tokio::spawn(async move {
            for start in (0..bytes.len()).step_by(7777) {
                if sender
                    .send(Input::Data(
                        bytes.slice(start..(start + 7777).min(bytes.len())),
                    ))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            if complete {
                let _ = sender.send(Input::Complete).await;
            }
        });
        receiver
    }

    pub(in crate::node::storage) fn allowed() -> Check {
        Arc::new(|| Ok(()))
    }

    async fn bytes(files: &Files, receipt: &Receipt, offset: u64, length: u64) -> Result<Vec<u8>> {
        let mut stream = read(files, receipt.clone(), offset, length, allowed()).await?;
        let mut output = Vec::new();
        while let Some(part) = stream.recv().await {
            output.extend_from_slice(&part?);
        }
        Ok(output)
    }

    #[tokio::test]
    async fn published_backup_rechecks_bytes_and_only_replaces_with_the_signed_content()
    -> Result<()> {
        let (_d, _i, _b, mut files) = fixture("8MiB").await?;
        files.backup = true;
        let bytes = Bytes::from(vec![41; UNIT + 17]);
        let mut h = header(&files, bytes.len() as u64);
        h.media = Uuid::now_v7();
        h.digest = *blake3::hash(&bytes).as_bytes();
        let r = write(&files, h.clone(), input(bytes.clone(), true), allowed()).await?;
        verify_backup(&files, r.clone(), allowed()).await?;
        let path = path(&files, &h)?;
        let mut file = open(&path, true)?;
        file.seek(SeekFrom::Start(HEADER_SIZE as u64 + 5))?;
        file.write_all(b"x")?;
        file.sync_all()?;
        drop(file);
        assert!(verify_backup(&files, r.clone(), allowed()).await.is_err());
        let before = std::fs::read(&path)?;
        assert!(
            write(
                &files,
                h.clone(),
                input(bytes.slice(0..10), true),
                allowed()
            )
            .await
            .is_err()
        );
        assert_eq!(std::fs::read(&path)?, before);
        assert_eq!(write(&files, h, input(bytes, true), allowed()).await?, r);
        verify_backup(&files, r, allowed()).await?;
        Ok(())
    }

    #[tokio::test]
    async fn discarded_plans_remove_partial_and_unacknowledged_files() -> Result<()> {
        let (_directory, _instance, _budget, files) = fixture("8MiB").await?;
        let data = Bytes::from(vec![37; UNIT + 19]);
        let planned = header(&files, data.len() as u64);
        let destination = path(&files, &planned)?;
        assert!(
            write(
                &files,
                planned.clone(),
                input(data.slice(..UNIT), false),
                allowed()
            )
            .await
            .is_err()
        );
        assert!(temporary(&destination).exists());
        let denied: Check = Arc::new(|| anyhow::bail!("expired cleanup grant"));
        assert!(discard(&files, planned.clone(), denied).await.is_err());
        assert!(temporary(&destination).exists());
        let mut wrong = planned.clone();
        wrong.namespace = Uuid::now_v7();
        assert!(discard(&files, wrong, allowed()).await.is_err());
        assert!(discard(&files, planned.clone(), allowed()).await?);
        assert!(!temporary(&destination).exists());
        assert!(!discard(&files, planned.clone(), allowed()).await?);
        let receipt = write(
            &files,
            planned.clone(),
            input(data.clone(), true),
            allowed(),
        )
        .await?;
        assert!(discard(&files, planned.clone(), allowed()).await?);
        assert!(!destination.exists());
        write(&files, planned.clone(), input(data, true), allowed()).await?;
        std::fs::rename(&destination, temporary(&destination))?;
        assert!(remove(&files, receipt, allowed()).await?);
        assert!(!temporary(&destination).exists());
        Ok(())
    }

    #[tokio::test]
    async fn fragments_resume_retry_and_validate_only_requested_ranges() -> Result<()> {
        let (directory, instance, budget, files) = fixture("8MiB").await?;
        let data = Bytes::from(
            (0..UNIT * 3 + 17)
                .map(|i| (i % 251) as u8)
                .collect::<Vec<_>>(),
        );
        let planned = header(&files, data.len() as u64);
        let destination = path(&files, &planned)?;
        assert!(
            write(
                &files,
                planned.clone(),
                input(data.slice(..UNIT), false),
                allowed()
            )
            .await
            .is_err()
        );
        assert!(temporary(&destination).exists());
        assert_eq!(
            inspect(&files, planned.clone(), allowed()).await?,
            State::Pending {
                bytes: HEADER_SIZE as u64 + UNIT as u64,
                complete: None
            }
        );
        let receipt = write(
            &files,
            planned.clone(),
            input(data.clone(), true),
            allowed(),
        )
        .await?;
        assert!(!temporary(&destination).exists());
        assert_eq!(
            inspect(&files, planned.clone(), allowed()).await?,
            State::Complete(receipt.clone())
        );
        assert_eq!(
            bytes(&files, &receipt, UNIT as u64 - 3, 19).await?,
            data[UNIT - 3..UNIT + 16]
        );
        assert_eq!(bytes(&files, &receipt, 0, data.len() as u64).await?, data);
        let modified = destination.metadata()?.modified()?;
        let grant = Uuid::now_v7();
        let signed = crate::model::storage_transfer::SignedReceipt::seal(
            grant,
            receipt.clone(),
            crate::model::storage_cluster::Accounting {
                boot: Uuid::now_v7(),
                revision: 1,
            },
            "storage-secret",
        )?;
        signed.verify(grant, "storage-secret")?;
        assert!(signed.verify(grant, "another-storage-secret").is_err());
        assert!(signed.verify(Uuid::now_v7(), "storage-secret").is_err());
        let mut wrong = signed.clone();
        wrong.accounting.revision += 1;
        assert!(wrong.verify(grant, "storage-secret").is_err());
        assert_eq!(
            write(
                &files,
                planned.clone(),
                input(data.clone(), true),
                allowed()
            )
            .await?,
            receipt
        );
        assert_eq!(destination.metadata()?.modified()?, modified);
        assert!(
            write(
                &files,
                planned.clone(),
                input(Bytes::from(vec![2; data.len()]), true),
                allowed()
            )
            .await
            .is_err()
        );
        let used = files.disk.data.as_ref().unwrap().snapshot().1;
        assert_eq!(files.disk.data.as_ref().unwrap().snapshot().2, 0);
        drop(files);
        let lock = Arc::new(crate::runtime::lock_directory(directory.path())?);
        let files = Files::open(&instance, &budget, lock).await?;
        assert_eq!(bytes(&files, &receipt, 0, data.len() as u64).await?, data);
        files.disk.data.as_ref().unwrap().reconcile()?;
        std::fs::rename(&destination, temporary(&destination))?;
        File::open(destination.parent().unwrap())?.sync_all()?;
        assert_eq!(
            inspect(&files, planned.clone(), allowed()).await?,
            State::Pending {
                bytes: receipt.header.file_bytes(),
                complete: Some(receipt.clone())
            }
        );
        assert_eq!(
            write(&files, planned, input(data.clone(), true), allowed()).await?,
            receipt
        );
        let mut file = open(&destination, true)?;
        file.seek(SeekFrom::Start(HEADER_SIZE as u64 + UNIT as u64 * 3))?;
        file.write_all(&[255])?;
        file.sync_all()?;
        assert_eq!(bytes(&files, &receipt, 0, 1).await?, data[..1]);
        assert!(bytes(&files, &receipt, UNIT as u64 * 3, 1).await.is_err());
        file.seek(SeekFrom::Start(HEADER_SIZE as u64 + UNIT as u64 * 3))?;
        file.write_all(&data[UNIT * 3..UNIT * 3 + 1])?;
        file.sync_all()?;
        drop(file);
        let mut wrong = receipt.clone();
        wrong.root[0] ^= 1;
        wrong.commitment = fragment::commitment(&wrong.header, &wrong.root)?;
        assert!(remove(&files, wrong, allowed()).await.is_err());
        assert!(remove(&files, receipt.clone(), allowed()).await?);
        assert!(!remove(&files, receipt, allowed()).await?);
        assert!(files.disk.data.as_ref().unwrap().snapshot().1 < used);
        Ok(())
    }

    #[tokio::test]
    async fn admission_empty_files_and_cancelled_writers_release_resources() -> Result<()> {
        let (_directory, _instance, _budget, files) = fixture("128KiB").await?;
        let too_large = header(&files, 128 * 1024);
        assert!(
            write(
                &files,
                too_large.clone(),
                input(Bytes::from(vec![1; 128 * 1024]), true),
                allowed()
            )
            .await
            .is_err()
        );
        assert!(!path(&files, &too_large)?.exists());
        assert_eq!(files.disk.data.as_ref().unwrap().snapshot().2, 0);
        let planned = header(&files, 0);
        let receipt = write(&files, planned, input(Bytes::new(), true), allowed()).await?;
        assert_eq!(receipt.header.file_bytes(), 432);
        assert!(bytes(&files, &receipt, 0, 0).await?.is_empty());
        assert!(
            read(&files, receipt.clone(), 0, 1, allowed())
                .await
                .is_err()
        );
        remove(&files, receipt, allowed()).await?;
        let planned = header(&files, 32 * 1024);
        let destination = path(&files, &planned)?;
        let (sender, receiver) = mpsc::channel(2);
        let files = Arc::new(files);
        let writer_files = files.clone();
        let available = files.disk.io.available_permits();
        let writer =
            tokio::spawn(async move { write(&writer_files, planned, receiver, allowed()).await });
        sender.send(Input::Data(Bytes::from(vec![3; 1024]))).await?;
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while std::fs::metadata(temporary(&destination))
                .map_or(true, |m| m.len() < (HEADER_SIZE + 1024) as u64)
                || files.disk.io.available_permits() != available
            {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        writer.abort();
        drop(sender);
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while files.disk.io.available_permits() != available
                || files.disk.data.as_ref().unwrap().snapshot().2 != 0
            {
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
        })
        .await?;
        assert!(files.disk.pending.lock().unwrap().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn network_backpressure_does_not_hold_disk_capacity() -> Result<()> {
        let (_directory, _, _, files) = fixture("8MiB").await?;
        let files = Arc::new(files);
        let data = Bytes::from(vec![7; UNIT * 8]);
        let planned = header(&files, data.len() as u64);
        let receipt = write(
            &files,
            planned.clone(),
            input(data.clone(), true),
            allowed(),
        )
        .await?;
        let mut stalled = read(&files, receipt.clone(), 0, data.len() as u64, allowed()).await?;
        assert!(stalled.recv().await.unwrap().is_ok());
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while stalled.len() != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            inspect(&files, planned.clone(), allowed()),
        )
        .await;
        drop(stalled);
        assert_eq!(result??, State::Complete(receipt.clone()));

        let upload = header(&files, 4 * UNIT as u64);
        let destination = path(&files, &upload)?;
        let (sender, receiver) = mpsc::channel(2);
        let writer_files = files.clone();
        let writer =
            tokio::spawn(async move { write(&writer_files, upload, receiver, allowed()).await });
        sender.send(Input::Data(Bytes::from(vec![2; UNIT]))).await?;
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while std::fs::metadata(temporary(&destination))
                .map_or(true, |m| m.len() < (HEADER_SIZE + UNIT) as u64)
            {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            inspect(&files, planned, allowed()),
        )
        .await;
        drop(sender);
        assert!(writer.await?.is_err());
        assert_eq!(result??, State::Complete(receipt));
        Ok(())
    }

    #[tokio::test]
    async fn expired_backpressure_releases_the_actual_file_work() -> Result<()> {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let (_directory, _, _, files) = fixture("8MiB").await?;
        let files = Arc::new(files);
        let data = Bytes::from(vec![7; UNIT * 8]);
        let receipt = write(
            &files,
            header(&files, data.len() as u64),
            input(data.clone(), true),
            allowed(),
        )
        .await?;
        let active = Arc::new(AtomicBool::new(true));
        let running = active.clone();
        let checks = Arc::new(AtomicUsize::new(0));
        let observed = checks.clone();
        let check: Check = Arc::new(move || {
            ensure!(
                running.load(Ordering::Acquire),
                "expired test authorization"
            );
            observed.fetch_add(1, Ordering::Release);
            Ok(())
        });
        let stalled = read(&files, receipt, 0, data.len() as u64, check.clone()).await?;
        // Expire after the third send has checked its authorization against a full queue.
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while stalled.len() != 2 || checks.load(Ordering::Acquire) < 11 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        active.store(false, Ordering::Release);
        let stopped = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while Arc::strong_count(&check) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        drop(stalled);
        stopped?;

        checks.store(0, Ordering::Release);
        active.store(true, Ordering::Release);
        let planned = header(&files, UNIT as u64);
        let destination = temporary(&path(&files, &planned)?);
        let (sender, receiver) = mpsc::channel(2);
        let writing = files.clone();
        let mut writer =
            tokio::spawn(async move { write(&writing, planned, receiver, check).await });
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !destination.exists() || checks.load(Ordering::Acquire) < 5 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        active.store(false, Ordering::Release);
        let stopped = tokio::time::timeout(std::time::Duration::from_secs(2), &mut writer).await;
        drop(sender);
        assert!(stopped??.is_err());
        assert!(files.disk.pending.lock().unwrap().is_empty());
        assert_eq!(files.disk.data.as_ref().unwrap().snapshot().2, 0);
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn expired_io_wait_does_not_hold_request_capacity() -> Result<()> {
        use std::sync::atomic::{AtomicBool, Ordering};
        let (_directory, _, _, files) = fixture("8MiB").await?;
        let files = Arc::new(files);
        let receipt = write(
            &files,
            header(&files, 3),
            input(Bytes::from_static(b"abc"), true),
            allowed(),
        )
        .await?;
        let gate = files.disk.io.clone();
        let quota = gate.available_permits() as u32;
        let held = gate.clone().acquire_many_owned(quota).await?;
        let active = Arc::new(AtomicBool::new(true));
        let checked = Arc::new(AtomicBool::new(false));
        let current = active.clone();
        let observed = checked.clone();
        let check: Check = Arc::new(move || {
            observed.store(true, Ordering::Release);
            ensure!(
                current.load(Ordering::Acquire),
                "expired test authorization"
            );
            Ok(())
        });
        let reader = files.clone();
        let mut waiting = tokio::spawn(async move { read(&reader, receipt, 0, 3, check).await });
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !checked.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        active.store(false, Ordering::Release);
        let stopped = tokio::time::timeout(std::time::Duration::from_secs(2), &mut waiting).await;
        assert_eq!(gate.available_permits(), 0);
        drop(held);
        assert!(stopped??.is_err());
        assert_eq!(gate.available_permits(), quota as usize);
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn fragment_directories_cannot_redirect_io_outside_media() -> Result<()> {
        let (directory, _, _, files) = fixture("8MiB").await?;
        let outside = tempfile::tempdir()?;
        std::os::unix::fs::symlink(outside.path(), directory.path().join("blocks"))?;
        let planned = header(&files, 3);
        assert!(
            write(
                &files,
                planned.clone(),
                input(Bytes::from_static(b"abc"), true),
                allowed()
            )
            .await
            .is_err()
        );
        assert!(inspect(&files, planned, allowed()).await.is_err());
        assert_eq!(std::fs::read_dir(outside.path())?.count(), 0);
        Ok(())
    }
}
