use super::{Output, Source};
use crate::storage::{Area, Disk};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::OnceCell;

const ENTRIES: usize = 1024;
const HEADER: usize = 64 * 1024;
const MAGIC: &[u8; 8] = b"MOKYU\0\0\x08";
const INDEX_MEMORY: usize = 4 * 1024 * 1024;
pub(super) type Deferred = Arc<Mutex<VecDeque<(PathBuf, u64)>>>;
pub(super) fn deferred(queue: &mut VecDeque<(PathBuf, u64)>, path: PathBuf, bytes: u64) {
    if queue.len() < ENTRIES * 2 {
        queue.push_back((path, bytes));
    } else {
        tracing::warn!("image cleanup queue full; recovery will retry");
    }
}
pub(super) async fn remaining(path: &std::path::Path) -> u64 {
    let mut bytes = 0u64;
    for name in ["input", "output", "metadata"] {
        if let Ok(data) = tokio::fs::symlink_metadata(path.join(name)).await
            && data.is_file()
        {
            bytes = bytes.saturating_add(data.len());
        }
    }
    bytes
}

fn regular_bytes(path: &std::path::Path) -> u64 {
    std::fs::symlink_metadata(path)
        .ok()
        .filter(|s| s.is_file())
        .map_or(0, |s| s.len())
}

fn remove_file(path: &std::path::Path) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        result => result,
    }
}

fn clean(path: &std::path::Path) -> std::io::Result<()> {
    if path
        .parent()
        .and_then(std::path::Path::file_name)
        .is_some_and(|n| n == "images")
        && std::fs::symlink_metadata(path).is_ok_and(|m| m.is_dir())
    {
        return std::fs::remove_dir_all(path);
    }
    remove_file(&path.with_extension("meta"))?;
    #[cfg(feature = "fault-injection")]
    crate::faults::blocking("image-evict-meta");
    remove_file(path)
}

fn residue(path: &std::path::Path) -> u64 {
    if path
        .parent()
        .and_then(std::path::Path::file_name)
        .is_some_and(|n| n == "images")
        && std::fs::symlink_metadata(path).is_ok_and(|m| m.is_dir())
    {
        return ["input", "output", "metadata"]
            .iter()
            .map(|n| regular_bytes(&path.join(n)))
            .sum();
    }
    regular_bytes(path) + regular_bytes(&path.with_extension("meta"))
}

fn discard(
    disk: &Disk,
    pending: &mut VecDeque<(PathBuf, u64)>,
    path: &std::path::Path,
    charged: u64,
) {
    match clean(path) {
        Ok(()) => disk.release(Area::Images, charged),
        Err(error) => {
            let left = residue(path);
            disk.release(Area::Images, charged.saturating_sub(left));
            if left > charged {
                disk.account_existing(Area::Images, left - charged);
            }
            deferred(pending, path.to_owned(), left);
            tracing::warn!(%error,"image cache cleanup deferred");
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Metadata {
    pub key: String,
    pub source_id: String,
    pub source_revision: i64,
    pub spans: Vec<crate::app::responses::DemandSpan>,
    pub source: Source,
    pub image: Source,
    pub output: Output,
    pub digest: String,
}

pub(crate) struct Entry {
    pub metadata: Metadata,
    pub path: PathBuf,
    pub(super) disk: Arc<Disk>,
    deferred: Deferred,
    bytes: u64,
    discard: AtomicBool,
    referenced: AtomicBool,
    footprint: Mutex<Option<(i64, Arc<Vec<crate::app::responses::DemandSpan>>)>>,
}
impl Drop for Entry {
    fn drop(&mut self) {
        if self.discard.load(Ordering::Relaxed) {
            let mut pending = self.deferred.lock().unwrap();
            discard(&self.disk, &mut pending, &self.path, self.bytes);
        }
    }
}
impl Entry {
    fn memory(&self) -> usize {
        2048 + (self.metadata.spans.len()
            + self
                .footprint
                .lock()
                .unwrap()
                .as_ref()
                .map_or(0, |(_, spans)| spans.len()))
            * 128
    }
    fn footprint(&self, revision: i64) -> Option<Arc<Vec<crate::app::responses::DemandSpan>>> {
        let current = self.footprint.lock().unwrap();
        current
            .as_ref()
            .filter(|(r, _)| *r == revision)
            .map(|(_, s)| s.clone())
            .or_else(|| {
                (self.metadata.source_revision == revision && !self.metadata.spans.is_empty())
                    .then(|| Arc::new(self.metadata.spans.clone()))
            })
    }
    pub(super) fn output_path(&self) -> PathBuf {
        if self
            .path
            .file_name()
            .is_some_and(|n| n == self.metadata.key.as_str())
        {
            self.path.clone()
        } else {
            self.path.join("output")
        }
    }
    pub async fn open(&self) -> Result<tokio::fs::File> {
        let path = self.output_path();
        ensure!(
            tokio::fs::symlink_metadata(&path).await?.is_file(),
            "invalid cached image file"
        );
        let file = tokio::fs::File::open(path).await?;
        ensure!(
            file.metadata().await?.len() == self.metadata.output.bytes,
            "invalid cached image size"
        );
        Ok(file)
    }
}

#[derive(Default)]
struct Index {
    entries: HashMap<String, Arc<Entry>>,
    order: VecDeque<String>,
    transient: HashMap<String, Weak<Entry>>,
    sources: HashMap<String, Source>,
    source_order: VecDeque<String>,
    bytes: u64,
    memory: usize,
}
pub(crate) struct Cache {
    pub disk: Arc<Disk>,
    pub(super) deferred: Deferred,
    root: PathBuf,
    capacity: u64,
    ready: OnceCell<()>,
    index: Mutex<Index>,
}
impl Cache {
    pub fn new(disk: Arc<Disk>, capacity: u64) -> Self {
        Self {
            root: disk.root.join("cache/images"),
            deferred: Default::default(),
            disk,
            capacity,
            ready: OnceCell::new(),
            index: Mutex::default(),
        }
    }
    pub async fn ready(&self) -> Result<()> {
        self.ready
            .get_or_init(|| async {
                let result = async {
                    super::job::recover(&self.disk, &self.deferred).await?;
                    let mut dirs = tokio::fs::read_dir(&self.root).await?;
                    while let Some(dir) = dirs.next_entry().await? {
                        let name = dir.file_name();
                        let Some(shard) = name.to_str() else { continue };
                        if !dir.file_type().await?.is_dir() {
                            continue;
                        }
                        if digest_name(shard) {
                            let mut entries = tokio::fs::read_dir(dir.path()).await?;
                            let mut old = false;
                            let mut known = true;
                            while let Some(e) = entries.next_entry().await? {
                                known &= e.file_type().await?.is_file()
                                    && matches!(
                                        e.file_name().to_str(),
                                        Some("metadata" | "output")
                                    );
                                old = true;
                            }
                            if old && known {
                                self.remove_existing(&dir.path());
                            }
                            continue;
                        }
                        if !(2..=3).contains(&shard.len()) || !hex(shard) {
                            continue;
                        }
                        let mut files = tokio::fs::read_dir(dir.path()).await?;
                        while let Some(entry) = files.next_entry().await? {
                            let filename = entry.file_name();
                            let Some(name) = filename.to_str() else {
                                continue;
                            };
                            let key = name.strip_suffix(".meta").unwrap_or(name);
                            if !digest_name(key) || entry.file_type().await?.is_dir() {
                                continue;
                            }
                            let path = dir.path().join(key);
                            if &key[..3] != shard {
                                self.remove_existing(&path);
                                continue;
                            }
                            let sidecar = path.with_extension("meta");
                            if name == key {
                                if !tokio::fs::symlink_metadata(&sidecar)
                                    .await
                                    .is_ok_and(|m| m.is_file())
                                {
                                    self.remove_existing(&path);
                                }
                                continue;
                            }
                            let Ok((metadata, bytes)) = Self::restore(&path, key).await else {
                                self.remove_existing(&path);
                                continue;
                            };
                            let entry = Arc::new(Entry {
                                metadata,
                                path,
                                disk: self.disk.clone(),
                                deferred: self.deferred.clone(),
                                bytes,
                                discard: AtomicBool::new(true),
                                referenced: AtomicBool::new(false),
                                footprint: Mutex::default(),
                            });
                            self.disk.account_existing(Area::Images, bytes);
                            self.remember(
                                entry.metadata.source_id.clone(),
                                entry.metadata.source.clone(),
                            );
                            self.retain(entry, true);
                        }
                    }
                    let old = self.disk.root.join("thumbnails");
                    if let Ok(mut files) = tokio::fs::read_dir(old).await {
                        while let Some(file) = files.next_entry().await? {
                            if file.file_type().await?.is_file()
                                && file
                                    .file_name()
                                    .to_str()
                                    .and_then(|s| s.strip_prefix("v1-"))
                                    .and_then(|s| s.parse::<crate::model::Id>().ok())
                                    .is_some()
                            {
                                if let Err(error) = tokio::fs::remove_file(file.path()).await {
                                    tracing::warn!(%error, "obsolete thumbnail cleanup failed");
                                }
                            }
                        }
                    }
                    Ok::<_, anyhow::Error>(())
                }
                .await;
                if let Err(error) = result {
                    tracing::warn!(%error, "image cache recovery incomplete");
                }
            })
            .await;
        Ok(())
    }
    async fn restore(path: &std::path::Path, key: &str) -> Result<(Metadata, u64)> {
        let sidecar = path.with_extension("meta");
        ensure!(
            tokio::fs::symlink_metadata(&sidecar).await?.is_file(),
            "invalid image sidecar"
        );
        let data = tokio::fs::symlink_metadata(path).await?;
        ensure!(data.is_file(), "invalid image file");
        let frame = crate::storage::read_bounded(sidecar, HEADER + 40).await?;
        ensure!(
            frame.len() > 40
                && frame.starts_with(MAGIC)
                && blake3::hash(&frame[40..]).as_bytes() == &frame[8..40],
            "invalid image metadata frame"
        );
        let meta: Metadata = serde_json::from_slice(&frame[40..])?;
        ensure!(
            meta.key == key
                && digest_name(&meta.digest)
                && (1..=128 * 1024 * 1024).contains(&meta.output.bytes)
                && meta.output.bytes == data.len(),
            "invalid cached image metadata"
        );
        ensure!(
            meta.source_id.starts_with(super::job::SOURCE_PREFIX)
                && meta.source_id.len() <= 512
                && meta.spans.len() <= 256
                && meta.source.format.len() <= 32
                && meta.image.format.len() <= 32,
            "invalid image source descriptor"
        );
        Ok((meta, data.len() + frame.len() as u64))
    }
    fn remove_existing(&self, path: &std::path::Path) {
        let mut pending = self.deferred.lock().unwrap();
        if pending.iter().any(|(p, _)| p == path) {
            return;
        }
        let bytes = residue(path);
        self.disk.account_existing(Area::Images, bytes);
        discard(&self.disk, &mut pending, path, bytes);
    }
    pub(super) fn reap(&self) {
        let mut pending = self.deferred.lock().unwrap();
        for _ in 0..pending.len().min(32) {
            let (path, bytes) = pending.pop_front().unwrap();
            discard(&self.disk, &mut pending, &path, bytes);
        }
    }
    fn retain(&self, entry: Arc<Entry>, persistent: bool) {
        let mut index = self.index.lock().unwrap();
        index.transient.retain(|_, value| value.strong_count() > 0);
        if index.transient.len() < ENTRIES {
            index
                .transient
                .insert(entry.metadata.key.clone(), Arc::downgrade(&entry));
        }
        for _ in 0..index.order.len().min(128) {
            if index
                .bytes
                .saturating_add(entry.bytes)
                .max(self.disk.used()[Area::Images as usize])
                <= self.capacity
                && index.entries.len() < ENTRIES
                && index.memory + entry.memory() <= INDEX_MEMORY
            {
                break;
            }
            let key = index.order.pop_front().unwrap();
            let old = index.entries.get(&key).unwrap();
            if Arc::strong_count(old) == 1 && !old.referenced.swap(false, Ordering::Relaxed) {
                let old = index.entries.remove(&key).unwrap();
                index.bytes -= old.bytes;
                index.memory -= old.memory();
                old.discard.store(true, Ordering::Relaxed);
            } else {
                index.order.push_back(key);
            }
        }
        if persistent
            && index
                .bytes
                .saturating_add(entry.bytes)
                .max(self.disk.used()[Area::Images as usize])
                <= self.capacity
            && index.entries.len() < ENTRIES
            && index.memory + entry.memory() <= INDEX_MEMORY
        {
            entry.discard.store(false, Ordering::Relaxed);
            index.bytes += entry.bytes;
            index.memory += entry.memory();
            index.order.push_back(entry.metadata.key.clone());
            index.entries.insert(entry.metadata.key.clone(), entry);
        }
    }
    pub async fn get(&self, key: &str) -> Option<(Arc<Entry>, tokio::fs::File)> {
        let entry = {
            let index = self.index.lock().unwrap();
            index
                .entries
                .get(key)
                .cloned()
                .or_else(|| index.transient.get(key).and_then(Weak::upgrade))
        }?;
        match entry.open().await {
            Ok(file) => {
                entry.referenced.store(true, Ordering::Relaxed);
                Some((entry, file))
            }
            Err(error) => {
                tracing::debug!(%error, "image cache entry rejected");
                let mut index = self.index.lock().unwrap();
                if index
                    .entries
                    .get(key)
                    .is_some_and(|old| Arc::ptr_eq(old, &entry))
                {
                    let old = index.entries.remove(key).unwrap();
                    index.bytes -= old.bytes;
                    index.memory -= old.memory();
                    index.order.retain(|k| k != key);
                }
                if index
                    .transient
                    .get(key)
                    .is_some_and(|old| old.ptr_eq(&Arc::downgrade(&entry)))
                {
                    index.transient.remove(key);
                }
                entry.discard.store(true, Ordering::Relaxed);
                None
            }
        }
    }
    pub fn source(&self, key: &str) -> Option<Source> {
        self.index.lock().unwrap().sources.get(key).cloned()
    }
    pub fn footprint(&self, key: &str, revision: i64) -> bool {
        let index = self.index.lock().unwrap();
        index
            .entries
            .get(key)
            .is_some_and(|entry| entry.footprint(revision).is_some())
    }
    pub fn demand(
        &self,
        entry: &Arc<Entry>,
        revision: i64,
        spans: &[crate::app::responses::DemandSpan],
    ) -> Option<Arc<Vec<crate::app::responses::DemandSpan>>> {
        if let Some(known) = entry.footprint(revision) {
            return Some(known);
        }
        if spans.is_empty() || spans.len() > 256 {
            return None;
        }
        let spans = Arc::new(spans.to_vec());
        let mut index = self.index.lock().unwrap();
        let indexed = index
            .entries
            .get(&entry.metadata.key)
            .is_some_and(|e| Arc::ptr_eq(e, entry));
        let old = entry.memory();
        let next = 2048 + (entry.metadata.spans.len() + spans.len()) * 128;
        if !indexed || index.memory - old + next <= INDEX_MEMORY {
            *entry.footprint.lock().unwrap() = Some((revision, spans.clone()));
            if indexed {
                index.memory = index.memory - old + next;
            }
        }
        Some(spans)
    }
    pub fn contains(&self, key: &str) -> bool {
        let index = self.index.lock().unwrap();
        index.entries.contains_key(key)
            || index
                .transient
                .get(key)
                .is_some_and(|e| e.strong_count() > 0)
    }
    pub fn remember(&self, key: String, source: Source) {
        let mut index = self.index.lock().unwrap();
        if !index.sources.contains_key(&key) {
            if index.sources.len() == ENTRIES {
                let old = index.source_order.pop_front().unwrap();
                index.sources.remove(&old);
            }
            index.source_order.push_back(key.clone());
        }
        index.sources.insert(key, source);
    }
    pub async fn publish(
        &self,
        temporary: &mut super::job::Temporary,
        metadata: Metadata,
    ) -> Result<Arc<Entry>> {
        let path = temporary.path.clone();
        let raw = serde_json::to_vec(&metadata)?;
        ensure!(raw.len() <= HEADER, "image cache header exceeds bound");
        ensure!(digest_name(&metadata.key), "invalid image cache key");
        let mut record = Vec::with_capacity(40 + raw.len());
        record.extend_from_slice(MAGIC);
        record.extend_from_slice(blake3::hash(&raw).as_bytes());
        record.extend_from_slice(&raw);
        let bytes = metadata.output.bytes + record.len() as u64;
        tokio::fs::write(path.join("metadata"), record).await?;
        let shard = self.root.join(&metadata.key[..3]);
        let target = shard.join(&metadata.key);
        let from = path.clone();
        let to = target.clone();
        let disk = self.disk.clone();
        let pending = self.deferred.clone();
        let persistent = tokio::task::spawn_blocking(move || -> Result<bool> {
            match std::fs::create_dir(&shard) {
                Ok(()) => std::fs::File::open(shard.parent().unwrap())?.sync_all()?,
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => ensure!(
                    std::fs::symlink_metadata(&shard)?.is_dir(),
                    "invalid image cache directory"
                ),
                Err(e) => return Err(e.into()),
            }
            let image = from.join("output");
            let header = from.join("metadata");
            std::fs::File::open(&image)?.sync_all()?;
            std::fs::File::open(&header)?.sync_all()?;
            let directory = std::fs::File::open(&shard)?;
            let mut pending = pending.lock().unwrap();
            if pending.iter().any(|(p, _)| p == &to) {
                return Ok(false);
            }
            let meta = to.with_extension("meta");
            if std::fs::symlink_metadata(&to).is_ok() || std::fs::symlink_metadata(&meta).is_ok() {
                return Ok(false);
            }
            #[cfg(feature = "fault-injection")]
            crate::faults::blocking("image-before-publish");
            std::fs::rename(image, &to)?;
            let finish = (|| -> std::io::Result<()> {
                directory.sync_all()?;
                #[cfg(feature = "fault-injection")]
                crate::faults::blocking("image-body-published");
                std::fs::rename(header, &meta)?;
                directory.sync_all()
            })();
            if let Err(error) = finish {
                let left = residue(&to);
                disk.account_existing(Area::Images, left);
                discard(&disk, &mut pending, &to, left);
                return Err(error.into());
            }
            #[cfg(feature = "fault-injection")]
            crate::faults::blocking("image-pair-published");
            Ok(true)
        })
        .await??;
        let path = if persistent { target } else { path };
        temporary.charge.take().unwrap().commit_written(bytes);
        temporary.retained = !persistent;
        let entry = Arc::new(Entry {
            metadata,
            path,
            disk: self.disk.clone(),
            deferred: self.deferred.clone(),
            bytes,
            discard: AtomicBool::new(true),
            referenced: AtomicBool::new(false),
            footprint: Mutex::default(),
        });
        self.retain(entry.clone(), persistent);
        Ok(entry)
    }
}
fn hex(value: &str) -> bool {
    value
        .bytes()
        .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}
fn digest_name(value: &str) -> bool {
    value.len() == 64 && hex(value)
}
