//! A durable local segment and its authenticated recovery boundary.
use super::{Format, FrameType, ROOT, Sequence, segment_path, verify_prefix};
use crate::{
    model::journal::publication::File as Segment,
    storage::{marker, model},
};
use anyhow::{Context, Result, ensure};
use object_store::{local::LocalFileSystem, path::Path as ObjectPath};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Mutex,
    time::SystemTime,
};
use uuid::Uuid;

pub(crate) struct Local {
    pub root: PathBuf,
    verified: Mutex<Option<(Head, SystemTime)>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Head {
    pub first: u64,
    pub last: u64,
    pub bytes: u64,
    pub tail: [u8; 32],
    pub sealed: bool,
}

fn head_path(epoch: Uuid) -> String {
    format!("{ROOT}/frontiers/{}", epoch.simple())
}

fn directories(root: &Path, dir: &Path) -> Result<()> {
    let mut parent = root.to_owned();
    for part in dir.strip_prefix(root)?.components() {
        let next = parent.join(part);
        match fs::create_dir(&next) {
            Ok(()) => File::open(&parent)?.sync_all()?,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                ensure!(
                    fs::symlink_metadata(&next)?.is_dir(),
                    "invalid local journal directory"
                );
            }
            Err(e) => return Err(e.into()),
        }
        parent = next;
    }
    Ok(())
}

impl Local {
    pub fn new(c: &model::Connection) -> Result<Option<Self>> {
        if c.r#type != "local" {
            return Ok(None);
        }
        let store = LocalFileSystem::new_with_prefix(
            c.path.as_deref().context("missing local storage path")?,
        )?;
        let marker = ObjectPath::parse(&c.prefix)?.child("mokyu.json");
        let mut root = store.path_to_filesystem(&marker)?;
        ensure!(root.pop(), "missing local namespace");
        Ok(Some(Self {
            root,
            verified: Mutex::new(None),
        }))
    }

    pub async fn lock(&self, exclusive: bool) -> Result<File> {
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || {
            let dir = File::open(root)?;
            if exclusive {
                dir.lock()?
            } else {
                dir.lock_shared()?
            }
            Ok(dir)
        })
        .await?
    }

    // All writers and ownership transfers take the same stable namespace lock.
    pub fn replace(&self, path: &Path, data: &[u8]) -> Result<()> {
        let parent = path.parent().context("missing local journal parent")?;
        directories(&self.root, parent)?;
        let temp = self.root.join(".mokyu-journal.tmp");
        match fs::remove_file(&temp) {
            Ok(()) => (),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            Err(e) => return Err(e.into()),
        }
        let mut file = File::create_new(&temp)?;
        file.write_all(data)?;
        file.sync_all()?;
        fs::rename(temp, path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    }

    pub fn create(&self, path: &Path, data: &[u8]) -> Result<()> {
        let parent = path.parent().context("missing history directory")?;
        directories(&self.root, parent)?;
        match fs::symlink_metadata(path) {
            Ok(meta) => ensure!(meta.is_file(), "invalid takeover record file"),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            Err(e) => return Err(e.into()),
        }
        let temp = parent.join(".takeover.part");
        match fs::remove_file(&temp) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            result => result?,
        }
        let mut file = File::create_new(&temp)?;
        file.write_all(data)?;
        file.sync_all()?;
        let published = fs::hard_link(&temp, path);
        fs::remove_file(&temp)?;
        File::open(parent)?.sync_all()?;
        match published {
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
            other => Ok(other?),
        }
    }

    pub fn head(&self, f: &Format<'_>) -> Result<Option<Head>> {
        let path = head_path(f.epoch);
        let mut file = match File::open(self.root.join(&path)) {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let mut data = Vec::new();
        Read::by_ref(&mut file).take(257).read_to_end(&mut data)?;
        let frame = f.decode(FrameType::Watermark, &path, &data)?;
        ensure!(
            frame.consumed == data.len() && frame.bytes.len() == 9 && frame.bytes[8] <= 1,
            "invalid local journal frontier"
        );
        let bytes = u64::from_be_bytes(frame.bytes[..8].try_into()?);
        ensure!(
            bytes > 0 && bytes <= u64::from(crate::model::journal::policy::MAX_LIMIT),
            "invalid local journal offset"
        );
        Ok(Some(Head {
            first: frame.sequence.first,
            last: frame.sequence.last,
            tail: frame.sequence.previous,
            bytes,
            sealed: frame.bytes[8] != 0,
        }))
    }

    fn save_head(&self, f: &Format<'_>, head: Head) -> Result<()> {
        let path = head_path(f.epoch);
        let mut data = head.bytes.to_be_bytes().to_vec();
        data.push(u8::from(head.sealed));
        let envelope = f.encode(
            FrameType::Watermark,
            &path,
            Sequence {
                first: head.first,
                last: head.last,
                previous: head.tail,
            },
            &data,
        )?;
        self.replace(&self.root.join(path), &envelope)
    }

    pub fn append(&self, f: &Format<'_>, file: &Segment, start: u64, bytes: &[u8]) -> Result<()> {
        let key = file.path()?;
        let path = self.root.join(&key);
        let active = path.with_extension("part");
        let parent = path.parent().context("missing segment directory")?;
        directories(&self.root, parent)?;
        let next = Head {
            first: file.first_seq.try_into()?,
            last: file.last_seq.try_into()?,
            bytes: file.byte_count.try_into()?,
            tail: file.digest.as_slice().try_into()?,
            sealed: file.state == "sealed",
        };
        ensure!(
            start.checked_add(bytes.len() as u64) == Some(next.bytes),
            "invalid pending local journal frames"
        );
        let prior = self.head(f)?;
        let same = prior.filter(|h| h.first == next.first);
        if same.is_none() {
            ensure!(
                start == 0
                    && prior.map_or(1, |h| h.last.saturating_add(1)) == next.first
                    && prior.is_none_or(|h| h.sealed && h.tail.as_slice() == file.previous_digest),
                "local journal predecessor changed"
            );
        }
        ensure!(same.is_none_or(|h| h == next || (!h.sealed && h.bytes == start && h.last <= next.last)), "local journal frontier changed");
        let final_exists = path.try_exists()?;
        ensure!(
            !final_exists || (same == Some(next) && next.sealed && !active.try_exists()?),
            "conflicting sealed local journal"
        );
        let physical = if final_exists { &path } else { &active };
        let mut options = OpenOptions::new();
        options
            .read(true)
            .write(true)
            .create(same.is_none())
            .truncate(false);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::custom_flags(&mut options, libc::O_NOFOLLOW);
        let mut output = options.open(physical)?;
        let metadata = output.metadata()?;
        ensure!(metadata.is_file(), "invalid local segment file");
        #[cfg(unix)]
        ensure!(
            std::os::unix::fs::MetadataExt::nlink(&metadata) == 1 || final_exists,
            "aliased active local segment"
        );
        let confirmed = same.map_or(0, |h| h.bytes);
        ensure!(
            metadata.len() >= confirmed,
            "confirmed local journal is truncated"
        );
        let mut cached = self.verified.lock().unwrap();
        if let Some(h) = same
            && cached.as_ref() != Some(&(h, metadata.modified()?))
        {
            let mut prefix = Vec::with_capacity(h.bytes.try_into()?);
            Read::by_ref(&mut output)
                .take(h.bytes)
                .read_to_end(&mut prefix)?;
            let (last, tail) = verify_prefix(
                f,
                &key,
                &prefix,
                h.first,
                file.previous_digest.as_slice().try_into()?,
                !h.sealed,
                |_| Ok(()),
            )?;
            ensure!(
                h.last == last && h.tail == tail,
                "confirmed local journal digest changed"
            );
        }
        if same != Some(next) {
            let previous = file.previous_digest.as_slice().try_into()?;
            let prefix = Sequence {
                first: next.first,
                last: same.map_or(next.first - 1, |h| h.last),
                previous: same.map_or(previous, |h| h.tail),
            };
            let end = super::verify_frames(f, &key, bytes, prefix, !next.sealed, |_| Ok(()))?;
            ensure!(
                end == (next.last, next.tail),
                "pending local journal digest changed"
            );
        }
        if metadata.len() != confirmed {
            ensure!(!final_exists, "sealed local journal length changed");
            output.set_len(confirmed)?;
            if same == Some(next) {
                output.sync_data()?;
            }
        }
        if same != Some(next) {
            output.seek(SeekFrom::Start(start))?;
            output.write_all(bytes)?;
            output.sync_data()?;
            if confirmed == 0 {
                File::open(parent)?.sync_all()?;
            }
            #[cfg(feature = "fault-injection")]
            crate::faults::blocking("journal-local-data");
            self.save_head(f, next)?;
            #[cfg(feature = "fault-injection")]
            crate::faults::blocking("journal-local-confirmed");
        }
        if next.sealed && !final_exists {
            fs::rename(&active, &path)?;
            File::open(parent)?.sync_all()?;
            #[cfg(feature = "fault-injection")]
            crate::faults::blocking("journal-local-sealed");
        }
        *cached = Some((next, output.metadata()?.modified()?));
        Ok(())
    }

    pub fn verify_owner(&self, journal: &marker::Journal) -> Result<()> {
        let mut data = Vec::new();
        File::open(self.root.join("mokyu.json"))?
            .take(marker::MAX_BYTES as u64 + 1)
            .read_to_end(&mut data)?;
        ensure!(
            data.len() <= marker::MAX_BYTES
                && marker::journal(&serde_json::from_slice(&data)?)?.as_ref() == Some(journal),
            "local journal ownership changed"
        );
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../tests/storage/local_journal.rs"]
mod tests;
