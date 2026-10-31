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
use tokio::{io::AsyncReadExt, sync::OnceCell};

const ENTRIES: usize = 1024;
const HEADER: usize = 64 * 1024;
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
            match std::fs::remove_dir_all(&self.path) {
                Ok(()) => self.disk.release(Area::Images, self.bytes),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    self.disk.release(Area::Images, self.bytes)
                }
                Err(error) => {
                    deferred(&mut pending, self.path.clone(), self.bytes);
                    tracing::warn!(%error, "image cache cleanup deferred");
                }
            }
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
    pub async fn open(&self) -> Result<tokio::fs::File> {
        use tokio::io::AsyncSeekExt;
        let mut file = tokio::fs::File::open(self.path.join("output")).await?;
        ensure!(
            file.metadata().await?.len() == self.metadata.output.bytes,
            "invalid cached image size"
        );
        let mut digest = blake3::Hasher::new();
        let mut buffer = vec![0u8; 65536];
        loop {
            let n = file.read(&mut buffer).await?;
            if n == 0 {
                break;
            }
            digest.update(&buffer[..n]);
        }
        ensure!(
            digest.finalize().to_hex().as_str() == self.metadata.digest,
            "invalid cached image digest"
        );
        file.rewind().await?;
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
                    let mut files = tokio::fs::read_dir(&self.root).await?;
                    while let Some(file) = files.next_entry().await? {
                        let name = file.file_name();
                        let Some(name) = name.to_str().filter(|n| digest_name(n)) else {
                            continue;
                        };
                        if !file.file_type().await?.is_dir() {
                            continue;
                        }
                        let record = async {
                            let raw = crate::storage::read_bounded(
                                file.path().join("metadata"),
                                HEADER + 32,
                            )
                            .await?;
                            ensure!(
                                raw.len() > 32
                                    && blake3::hash(&raw[32..]).as_bytes().as_slice() == &raw[..32],
                                "invalid image metadata digest"
                            );
                            let metadata: Metadata = serde_json::from_slice(&raw[32..])?;
                            ensure!(
                                metadata.source_id.starts_with(super::job::SOURCE_PREFIX)
                                    && metadata.source_id.len() <= 512
                                    && metadata.spans.len() <= 256
                                    && metadata.source.format.len() <= 32
                                    && metadata.image.format.len() <= 32,
                                "invalid image source descriptor"
                            );
                            ensure!(
                                metadata.key == name
                                    && digest_name(&metadata.digest)
                                    && metadata.output.bytes > 0
                                    && metadata.output.bytes <= 128 * 1024 * 1024,
                                "invalid image cache metadata"
                            );
                            ensure!(
                                tokio::fs::metadata(file.path().join("output")).await?.len()
                                    == metadata.output.bytes,
                                "invalid image cache length"
                            );
                            Ok::<_, anyhow::Error>((metadata, raw.len() as u64))
                        }
                        .await;
                        let Ok((metadata, header)) = record else {
                            if let Err(error) = tokio::fs::remove_dir_all(file.path()).await {
                                let bytes = remaining(&file.path()).await;
                                self.disk.account_existing(Area::Images, bytes);
                                deferred(&mut self.deferred.lock().unwrap(), file.path(), bytes);
                                tracing::warn!(%error, "invalid image cache cleanup failed");
                            }
                            continue;
                        };
                        let bytes = metadata.output.bytes + header;
                        let entry = Arc::new(Entry {
                            metadata,
                            path: file.path(),
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
    pub(super) fn reap(&self) {
        let mut pending = self.deferred.lock().unwrap();
        for _ in 0..pending.len().min(32) {
            let (path, bytes) = pending.pop_front().unwrap();
            match std::fs::remove_dir_all(&path) {
                Ok(()) => self.disk.release(Area::Images, bytes),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    self.disk.release(Area::Images, bytes)
                }
                Err(_) => pending.push_back((path, bytes)),
            }
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
                if let Some(old) = index.entries.remove(key) {
                    index.bytes -= old.bytes;
                    index.memory -= old.memory();
                    index.order.retain(|k| k != key);
                }
                index.transient.remove(key);
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
        let mut record = blake3::hash(&raw).as_bytes().to_vec();
        record.extend_from_slice(&raw);
        tokio::fs::write(path.join("metadata"), &record).await?;
        let bytes = metadata.output.bytes + record.len() as u64;
        let target = self.root.join(&metadata.key);
        // Cleanup and publication share a gate so a deferred delete cannot remove a replacement.
        let pending = self.deferred.clone();
        let from = path.clone();
        let to = target.clone();
        let persistent = tokio::task::spawn_blocking(move || {
            let pending = pending.lock().unwrap();
            !pending.iter().any(|(path, _)| path == &to) && std::fs::rename(from, to).is_ok()
        })
        .await?;
        let path = if persistent { target } else { path };
        temporary.charge.take().unwrap().commit_written(bytes);
        temporary.retained = true;
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
fn digest_name(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}
