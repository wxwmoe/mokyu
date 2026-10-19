use crate::model::multipart::{FileRemap, MultipartStore};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use tokio::io::AsyncReadExt;
use uuid::Uuid;

const BUFFER: usize = 256 * 1024;
const CHECKPOINT: usize = 16 * 1024 * 1024;

#[derive(Clone)]
pub(crate) struct Range {
    pub path: PathBuf,
    pub offset: u64,
    pub length: usize,
    pub hash: [u8; 32],
    pub fragment: Option<Uuid>,
    pub file: Option<Uuid>,
}

pub(crate) struct Prepared {
    file: tokio::fs::File,
    ranges: Vec<Range>,
    index: usize,
    position: usize,
    hash: blake3::Hasher,
}

impl Prepared {
    pub async fn next(&mut self) -> Result<Option<Bytes>> {
        let Some(range) = self.ranges.get(self.index) else {
            return Ok(None);
        };
        let length = BUFFER.min(range.length - self.position);
        let mut bytes = vec![0; length];
        self.file.read_exact(&mut bytes).await?;
        self.hash.update(&bytes);
        self.position += length;
        if self.position == range.length {
            ensure!(
                self.hash.finalize().as_bytes() == &range.hash,
                "assembled fragment checksum mismatch"
            );
            self.index += 1;
            self.position = 0;
            self.hash = blake3::Hasher::new();
        }
        Ok(Some(Bytes::from(bytes)))
    }
}

#[derive(Default, Debug)]
struct Counts {
    reused: u64,
    cloned: u64,
    copied: u64,
}

pub(crate) async fn prepare(
    storage: &super::Storage,
    destination: &Path,
    ranges: Vec<Range>,
    model: &dyn MultipartStore,
    upload: Uuid,
    source: Uuid,
) -> Result<(tokio::fs::File, Prepared, bool)> {
    let destination = destination.to_owned();
    let checkpoint_path = destination.clone();
    let floor = storage.disk.floor;
    let support = storage.reflink.clone();
    let pending = storage.raw_reserved.clone();
    let permit = storage.disk.io.clone().acquire_owned().await?;
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    let copied = Arc::new(AtomicBool::new(false));
    let copy_done = copied.clone();
    let worker = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let mut checkpoints = Checkpoints::new(sender);
        let (file, counts) = build_with(
            &destination,
            &ranges,
            floor,
            &support,
            &pending,
            &mut |file, range, offset| checkpoints.push(file, range, offset),
        )?;
        copy_done.store(true, Ordering::Release);
        checkpoints.finish(&file)?;
        let reader = file.try_clone()?;
        tracing::debug!(
            reused_bytes = counts.reused,
            cloned_bytes = counts.cloned,
            copied_bytes = counts.copied,
            "local multipart assembled"
        );
        Ok::<_, anyhow::Error>((file, reader, ranges))
    });
    // Both halves finish before releasing the upload lock or the raw source pin.
    let (result, retained) = tokio::join!(
        worker,
        confirm(
            storage,
            model,
            upload,
            source,
            &checkpoint_path,
            receiver,
            copied
        )
    );
    let retained = retained?;
    let result = result??;
    let (file, reader, ranges) = result;
    Ok((
        tokio::fs::File::from_std(file),
        Prepared {
            file: tokio::fs::File::from_std(reader),
            ranges,
            index: 0,
            position: 0,
            hash: blake3::Hasher::new(),
        },
        retained,
    ))
}

#[cfg(test)]
fn build(
    destination: &Path,
    ranges: &[Range],
    floor: u64,
    support: &Mutex<HashMap<(u64, u64), bool>>,
    pending: &Arc<Mutex<u64>>,
) -> Result<(File, Counts)> {
    build_with(
        destination,
        ranges,
        floor,
        support,
        pending,
        &mut |_, _, _| Ok(()),
    )
}

fn build_with(
    destination: &Path,
    ranges: &[Range],
    floor: u64,
    support: &Mutex<HashMap<(u64, u64), bool>>,
    pending: &Arc<Mutex<u64>>,
    copied: &mut dyn FnMut(&File, &Range, u64) -> Result<()>,
) -> Result<(File, Counts)> {
    ensure!(
        std::fs::metadata(destination)?.len() == 0,
        "assembly destination is not empty"
    );
    let mut counts = Counts::default();
    let mut size = 0u64;
    for range in ranges {
        ensure!(
            range.length > 0 && range.length <= crate::content::codec::MAX,
            "invalid local assembly range"
        );
        size = size
            .checked_add(range.length as u64)
            .context("assembly size overflow")?;
    }
    let mut contiguous = 0;
    if let Some(first) = ranges.first()
        && ranges.iter().all(|range| {
            let matches = range.path == first.path && range.offset == contiguous;
            contiguous += range.length as u64;
            matches
        })
        && std::fs::metadata(&first.path)?.len() == size
    {
        // All appenders are fenced before this immutable inode gains its final name.
        std::fs::remove_file(destination)?;
        match std::fs::hard_link(&first.path, destination) {
            Ok(()) => {
                counts.reused = size;
                return Ok((File::open(destination)?, counts));
            }
            Err(error) if error.raw_os_error() == Some(libc::EXDEV) => {}
            Err(error) => return Err(error.into()),
        }
    }
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o644);
    }
    let mut file = options.open(destination)?;
    let mut position = 0;
    let mut buffer = vec![0; BUFFER];
    for range in ranges {
        let mut source = File::open(&range.path)?;
        ensure!(
            range
                .offset
                .checked_add(range.length as u64)
                .is_some_and(|end| end <= source.metadata().map(|m| m.len()).unwrap_or(0)),
            "multipart range exceeds source"
        );
        ensure!(
            fs2::available_space(destination.parent().unwrap())? >= floor,
            "filesystem free-space floor reached"
        );
        if clone_range(
            &source,
            &file,
            range.offset,
            position,
            range.length as u64,
            support,
        )? {
            counts.cloned += range.length as u64;
        } else {
            source.seek(SeekFrom::Start(range.offset))?;
            file.seek(SeekFrom::Start(position))?;
            let mut remaining = range.length;
            let mut hash = blake3::Hasher::new();
            while remaining > 0 {
                let count = remaining.min(buffer.len());
                source.read_exact(&mut buffer[..count])?;
                hash.update(&buffer[..count]);
                let reservation = Space::claim(destination, floor, count as u64, pending)?;
                file.write_all(&buffer[..count])?;
                drop(reservation);
                remaining -= count;
                counts.copied += count as u64;
            }
            ensure!(
                hash.finalize().as_bytes() == &range.hash,
                "copied fragment checksum mismatch"
            );
            copied(&file, range, position)?;
        }
        position += range.length as u64;
    }
    file.set_len(size)?;
    file.seek(SeekFrom::Start(0))?;
    Ok((file, counts))
}

struct Checkpoint {
    file: File,
    ranges: Vec<FileRemap>,
    acknowledged: tokio::sync::oneshot::Sender<()>,
}

struct Checkpoints {
    sender: tokio::sync::mpsc::Sender<Checkpoint>,
    previous: Option<tokio::sync::oneshot::Receiver<()>>,
    ranges: Vec<FileRemap>,
    bytes: usize,
}

impl Checkpoints {
    fn new(sender: tokio::sync::mpsc::Sender<Checkpoint>) -> Self {
        Self {
            sender,
            previous: None,
            ranges: Vec::new(),
            bytes: 0,
        }
    }
    fn push(&mut self, output: &File, range: &Range, offset: u64) -> Result<()> {
        if let (Some(fragment), Some(file)) = (range.fragment, range.file) {
            self.ranges.push(FileRemap {
                fragment,
                file,
                offset: range.offset as i64,
                destination: offset as i64,
                size: range.length as i32,
                hash: range.hash,
            });
            self.bytes += range.length;
            if self.bytes >= CHECKPOINT || self.ranges.len() == 64 {
                self.flush(output)?;
            }
        }
        Ok(())
    }
    fn wait(&mut self) -> Result<()> {
        if let Some(previous) = self.previous.take() {
            previous
                .blocking_recv()
                .context("multipart confirmation failed")?;
        }
        Ok(())
    }
    fn flush(&mut self, file: &File) -> Result<()> {
        if self.ranges.is_empty() {
            return Ok(());
        }
        self.wait()?;
        let (acknowledged, received) = tokio::sync::oneshot::channel();
        self.sender
            .blocking_send(Checkpoint {
                file: file.try_clone()?,
                ranges: std::mem::take(&mut self.ranges),
                acknowledged,
            })
            .map_err(|_| anyhow::anyhow!("multipart confirmation stopped"))?;
        self.previous = Some(received);
        self.bytes = 0;
        Ok(())
    }
    fn finish(&mut self, file: &File) -> Result<()> {
        self.flush(file)?;
        self.wait()
    }
}

async fn confirm(
    storage: &super::Storage,
    model: &dyn MultipartStore,
    upload: Uuid,
    source: Uuid,
    destination: &Path,
    mut receiver: tokio::sync::mpsc::Receiver<Checkpoint>,
    copied: Arc<AtomicBool>,
) -> Result<bool> {
    let mut retained = false;
    while let Some(batch) = receiver.recv().await {
        #[cfg(feature = "fault-injection")]
        crate::faults::point("multipart-prefix-before-sync").await;
        let parent = destination
            .parent()
            .context("checkpoint has no parent")?
            .to_owned();
        let file = batch.file;
        tokio::task::spawn_blocking(move || {
            file.sync_all()?;
            if !retained {
                File::open(parent)?.sync_all()?;
            }
            Ok::<_, std::io::Error>(())
        })
        .await??;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("multipart-prefix-durable").await;
        let _gate = storage.source_gate.write().await;
        model.remap_file(upload, source, &batch.ranges).await?;
        retained = true;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("multipart-prefix-remapped").await;
        let mut files: Vec<_> = batch.ranges.iter().map(|range| range.file).collect();
        files.sort_unstable();
        files.dedup();
        if !copied.load(Ordering::Acquire) {
            for file in files {
                compact(storage, model, file).await?;
            }
        }
        #[cfg(feature = "fault-injection")]
        crate::faults::point("multipart-prefix-reclaimed").await;
        drop(_gate);
        let _ = batch.acknowledged.send(());
    }
    Ok(retained)
}

pub(crate) async fn compact(
    storage: &super::Storage,
    model: &dyn MultipartStore,
    id: Uuid,
) -> Result<()> {
    let Some(file) = model.file_location(id).await? else {
        return Ok(());
    };
    if file.state != "sealed" {
        return Ok(());
    }
    let ranges = model.file_ranges(id).await?;
    if ranges.len() > 4096 {
        // Larger manifests keep their sources until normal file GC.
        return model.clear_reclaim(id).await;
    }
    let path = storage.multipart_path(id, Some(file.storage_id), file.source_id)?;
    let released = tokio::task::spawn_blocking(move || compact_file(&path, &ranges)).await??;
    if file.source_id.is_none() {
        storage
            .disk
            .release(super::local::Area::Multipart, released);
    }
    model.clear_reclaim(id).await
}

fn compact_file(path: &Path, ranges: &[(i64, i32)]) -> Result<u64> {
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    let metadata = file.metadata()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        // A published raw object may share this inode even after a failed Complete.
        if metadata.nlink() != 1 {
            return Ok(0);
        }
    }
    #[cfg(not(unix))]
    return Ok(0);
    #[cfg(unix)]
    {
        let mut end = 0u64;
        for &(start, size) in ranges {
            ensure!(start >= 0 && size > 0, "invalid live multipart range");
            let start = start as u64;
            let until = start
                .checked_add(size as u64)
                .context("multipart range overflow")?;
            ensure!(until <= metadata.len(), "live multipart range exceeds file");
            if start > end {
                punch(&file, end, start - end)?;
            }
            end = end.max(until);
        }
        if end < metadata.len() {
            file.set_len(end)?;
        }
        // Punched holes still count against the staging quota until the file is removed.
        Ok(metadata.len() - end)
    }
}

#[cfg(target_os = "linux")]
fn punch(file: &File, start: u64, length: u64) -> Result<()> {
    use std::os::fd::AsRawFd;
    let result = unsafe {
        libc::fallocate64(
            file.as_raw_fd(),
            libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
            start.try_into()?,
            length.try_into()?,
        )
    };
    if result == 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::EOPNOTSUPP | libc::ENOSYS | libc::EINVAL) => Ok(()),
        _ => Err(error.into()),
    }
}

#[cfg(all(unix, not(target_os = "linux")))]
fn punch(_: &File, _: u64, _: u64) -> Result<()> {
    Ok(())
}

struct Space {
    pending: Arc<Mutex<u64>>,
    bytes: u64,
}
impl Space {
    fn claim(path: &Path, floor: u64, bytes: u64, pending: &Arc<Mutex<u64>>) -> Result<Self> {
        let mut reserved = pending.lock().unwrap();
        ensure!(
            fs2::available_space(path.parent().unwrap())?
                .saturating_sub(*reserved)
                .saturating_sub(bytes)
                >= floor,
            "filesystem free-space floor reached"
        );
        *reserved = reserved
            .checked_add(bytes)
            .context("raw reservation overflow")?;
        Ok(Self {
            pending: pending.clone(),
            bytes,
        })
    }
}
impl Drop for Space {
    fn drop(&mut self) {
        *self.pending.lock().unwrap() -= self.bytes;
    }
}

#[cfg(target_os = "linux")]
fn clone_range(
    source: &File,
    destination: &File,
    start: u64,
    offset: u64,
    length: u64,
    support: &Mutex<HashMap<(u64, u64), bool>>,
) -> Result<bool> {
    use std::os::{fd::AsRawFd, unix::fs::MetadataExt};
    let devices = (source.metadata()?.dev(), destination.metadata()?.dev());
    if support.lock().unwrap().get(&devices) == Some(&false) {
        return Ok(false);
    }
    #[repr(C)]
    struct CloneRange {
        source: i64,
        start: u64,
        length: u64,
        offset: u64,
    }
    let range = CloneRange {
        source: source.as_raw_fd() as i64,
        start,
        length,
        offset,
    };
    // FICLONERANGE, using the platform's ioctl encoding from libc.
    let request = libc::_IOW::<CloneRange>(0x94, 13);
    let result = unsafe { libc::ioctl(destination.as_raw_fd(), request, &range) };
    if result == 0 {
        support.lock().unwrap().insert(devices, true);
        return Ok(true);
    }
    let error = std::io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::EOPNOTSUPP | libc::ENOTTY | libc::EXDEV) => {
            support.lock().unwrap().insert(devices, false);
            Ok(false)
        }
        Some(libc::EINVAL) => Ok(false),
        _ => Err(error.into()),
    }
}

#[cfg(not(target_os = "linux"))]
fn clone_range(
    _: &File,
    _: &File,
    _: u64,
    _: u64,
    _: u64,
    _: &Mutex<HashMap<(u64, u64), bool>>,
) -> Result<bool> {
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    #[cfg(unix)]
    fn reclaim_keeps_live_ranges_and_never_changes_shared_inodes() -> Result<()> {
        let root = std::env::temp_dir().join(format!("mokyu-reclaim-{}", Uuid::new_v4()));
        std::fs::create_dir(&root)?;
        let source = root.join("source");
        let published = root.join("published");
        let data = vec![73; 128 * 1024];
        std::fs::write(&source, &data)?;
        std::fs::hard_link(&source, &published)?;
        assert_eq!(compact_file(&source, &[(32768, 32768)])?, 0);
        assert_eq!(std::fs::read(&published)?, data);
        std::fs::remove_file(&published)?;
        assert_eq!(compact_file(&source, &[(32768, 32768)])?, 65536);
        let retained = std::fs::read(&source)?;
        assert_eq!(&retained[32768..], &data[32768..65536]);
        assert!(compact_file(&source, &[(0, 131072)]).is_err());
        assert_eq!(compact_file(&source, &[])?, 65536);
        assert_eq!(std::fs::metadata(&source)?.len(), 0);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn exact_layout_reuses_inode_and_selection_copies_only_requested_ranges() -> Result<()> {
        let root = std::env::temp_dir().join(format!("mokyu-assembly-{}", Uuid::new_v4()));
        std::fs::create_dir(&root)?;
        let source = root.join("source");
        let destination = root.join("destination");
        let data: Vec<u8> = (0..8193).map(|i| (i % 251) as u8).collect();
        std::fs::write(&source, &data)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&source, std::fs::Permissions::from_mode(0o640))?;
        }
        std::fs::write(&destination, [])?;
        let support = Mutex::new(HashMap::new());
        let pending = Arc::new(Mutex::new(0));
        let range = Range {
            path: source.clone(),
            offset: 0,
            length: data.len(),
            hash: *blake3::hash(&data).as_bytes(),
            fragment: None,
            file: None,
        };
        let (_, counts) = build(
            &destination,
            std::slice::from_ref(&range),
            0,
            &support,
            &pending,
        )?;
        assert_eq!(counts.reused, data.len() as u64);
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(
                std::fs::metadata(&source)?.ino(),
                std::fs::metadata(&destination)?.ino()
            );
            assert_eq!(std::fs::metadata(&destination)?.mode() & 0o777, 0o640);
        }
        std::fs::remove_file(&destination)?;
        std::fs::write(&destination, [])?;
        let ranges = [
            Range {
                offset: 1,
                length: 5000,
                hash: *blake3::hash(&data[1..5001]).as_bytes(),
                ..range.clone()
            },
            Range {
                offset: 8000,
                length: 193,
                hash: *blake3::hash(&data[8000..8193]).as_bytes(),
                ..range
            },
        ];
        let (_, counts) = build(&destination, &ranges, 0, &support, &pending)?;
        assert_eq!(
            std::fs::read(&destination)?,
            [&data[1..5001], &data[8000..8193]].concat()
        );
        assert_eq!(std::fs::read(&source)?, data);
        assert_eq!(counts.cloned + counts.copied, 5193);
        assert_eq!(*pending.lock().unwrap(), 0);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn prepared_file_rejects_changed_content() -> Result<()> {
        let path = std::env::temp_dir().join(format!("mokyu-assembly-{}", Uuid::new_v4()));
        std::fs::write(&path, b"changed")?;
        let mut prepared = Prepared {
            file: tokio::fs::File::open(&path).await?,
            ranges: vec![Range {
                path: path.clone(),
                offset: 0,
                length: 7,
                hash: *blake3::hash(b"initial").as_bytes(),
                fragment: None,
                file: None,
            }],
            index: 0,
            position: 0,
            hash: blake3::Hasher::new(),
        };
        assert!(prepared.next().await.is_err());
        drop(prepared);
        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    #[ignore = "requires an isolated reflink filesystem in MOKYU_TEST_REFLINK_DIR"]
    fn real_reflink_preserves_sources_and_copy_on_write() -> Result<()> {
        let root = PathBuf::from(std::env::var("MOKYU_TEST_REFLINK_DIR")?)
            .join(Uuid::new_v4().simple().to_string());
        std::fs::create_dir(&root)?;
        let first = root.join("first");
        let second = root.join("second");
        let output = root.join("output");
        let left = vec![31; 1024 * 1024];
        let right = vec![63; 1024 * 1024];
        std::fs::write(&first, &left)?;
        std::fs::write(&second, &right)?;
        std::fs::write(&output, [])?;
        let ranges = [
            Range {
                path: first.clone(),
                offset: 0,
                length: left.len(),
                hash: *blake3::hash(&left).as_bytes(),
                fragment: None,
                file: None,
            },
            Range {
                path: second.clone(),
                offset: 0,
                length: right.len(),
                hash: *blake3::hash(&right).as_bytes(),
                fragment: None,
                file: None,
            },
        ];
        let support = Mutex::new(HashMap::new());
        let (_, counts) = build(&output, &ranges, 0, &support, &Arc::new(Mutex::new(0)))?;
        assert_eq!(counts.cloned, 2 * 1024 * 1024);
        assert_eq!(counts.copied, 0);
        assert_eq!(std::fs::read(&output)?, [&left[..], &right[..]].concat());
        OpenOptions::new()
            .write(true)
            .open(&output)?
            .write_all(b"modified")?;
        assert_eq!(std::fs::read(&first)?, left);
        assert_eq!(std::fs::read(&second)?, right);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}
