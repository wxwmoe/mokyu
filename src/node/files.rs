use super::NodeContext;
use crate::{
    content::codec,
    storage::{
        cache::{CacheFile, CachePin, CacheWork, Fifo},
        file_cache::Files,
        local::{Area, Disk, Usage},
        profile::StorageProfile,
        raw::RawFile,
        raw_cache::Write,
        target::CachePolicy,
    },
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::{OnceCell, Semaphore};
use uuid::Uuid;

mod uploads;

pub(super) struct Cache {
    root: PathBuf,
    disk: Arc<Disk>,
    fifo: tokio::sync::Mutex<Fifo>,
    writes: tokio::sync::Mutex<()>,
    ready: OnceCell<()>,
    multipart_ready: OnceCell<()>,
    uploads_ready: OnceCell<()>,
    uploads: Mutex<HashMap<Uuid, crate::storage::cache::UploadCharge>>,
    local_locks: crate::runtime::locks::Locks<Uuid>,
    entries: usize,
    slots: Arc<Semaphore>,
    memory: Arc<Semaphore>,
    pending: Arc<Mutex<HashSet<Uuid>>>,
    raw_misses: crate::runtime::locks::Locks<Uuid>,
    hits: AtomicU64,
    lookups: AtomicU64,
}

impl Cache {
    pub(super) fn new(node: &NodeContext, memory: Arc<Semaphore>) -> Result<Arc<Self>> {
        let instance = &node.config.instance;
        let budget = node.budget.get().context("node budget missing")?;
        let capacity = instance
            .cache_size
            .as_deref()
            .map(crate::config::cache_bytes)
            .transpose()?;
        let disk = Arc::new(Disk {
            root: instance.data_path.clone(),
            limits: [
                instance
                    .multipart_limit
                    .as_deref()
                    .map(crate::config::bytes)
                    .transpose()?,
                capacity,
                None,
            ],
            floor: crate::config::bytes(&instance.min_free_space)?,
            spaces: Mutex::default(),
            upload_limit: instance
                .upload_cache_size
                .as_deref()
                .map(crate::config::cache_bytes)
                .transpose()?,
            usage: Mutex::new(Usage {
                used: [0; 3],
                upload: 0,
                upload_by_storage: HashMap::new(),
            }),
            io: Arc::new(Semaphore::new(budget.cpu_jobs)),
            pending: Mutex::default(),
        });
        Ok(Arc::new(Self {
            root: instance.data_path.join("read-cache"),
            disk,
            fifo: Default::default(),
            writes: Default::default(),
            ready: OnceCell::new(),
            multipart_ready: OnceCell::new(),
            uploads_ready: OnceCell::new(),
            uploads: Mutex::default(),
            local_locks: Default::default(),
            entries: instance.cache_entries.unwrap_or(budget.cache_entries),
            slots: Arc::new(Semaphore::new(budget.cpu_jobs)),
            memory,
            pending: Arc::new(Mutex::new(HashSet::new())),
            raw_misses: Default::default(),
            hits: 0.into(),
            lookups: 0.into(),
        }))
    }

    fn files(&self) -> Files<'_> {
        Files {
            disk: &self.disk,
            root: &self.root,
            fifo: &self.fifo,
            cache_writes: &self.writes,
        }
    }

    pub(super) async fn multipart(&self) -> Result<Arc<Disk>> {
        self.multipart_ready
            .get_or_try_init(|| async {
                let root = self.disk.root.join("multipart");
                tokio::fs::create_dir_all(&root).await?;
                ensure!(
                    tokio::fs::symlink_metadata(&root).await?.is_dir(),
                    "multipart path is not a directory"
                );
                tokio::fs::File::open(&self.disk.root)
                    .await?
                    .sync_all()
                    .await?;
                let mut entries = tokio::fs::read_dir(&root).await?;
                let mut size = 0u64;
                while let Some(entry) = entries.next_entry().await? {
                    let metadata = tokio::fs::symlink_metadata(entry.path()).await?;
                    ensure!(
                        metadata.is_file(),
                        "multipart directory contains a non-file entry"
                    );
                    size = size
                        .checked_add(crate::storage::local::allocated(&metadata))
                        .context("multipart accounting overflow")?;
                }
                self.disk.account_existing(Area::Multipart, size);
                Ok::<_, anyhow::Error>(())
            })
            .await?;
        Ok(self.disk.clone())
    }

    pub(super) async fn local_lock(&self, id: Uuid) -> tokio::sync::OwnedMutexGuard<()> {
        self.local_locks.get(id).lock_owned().await
    }

    pub(super) async fn reclaim_local(
        self: &Arc<Self>,
        node: &NodeContext,
        recover: bool,
    ) -> Result<()> {
        self.recover_uploads(node).await?;
        if !recover {
            self.raw_inventory(node, false).await?;
            self.part_inventory(node, false).await?;
        }
        Ok(())
    }

    async fn part_inventory(self: &Arc<Self>, node: &NodeContext, recover: bool) -> Result<()> {
        let disk = self.multipart().await?;
        let snapshot = node.background_snapshot()?;
        let mut entries = tokio::fs::read_dir(disk.root.join("multipart")).await?;
        let mut ids = Vec::with_capacity(128);
        loop {
            let entry = entries.next_entry().await?;
            if let Some(entry) = &entry {
                if let Some(id) = entry
                    .file_name()
                    .to_str()
                    .and_then(|name| Uuid::parse_str(name).ok())
                {
                    if !disk.pending(id) {
                        ids.push(id);
                    }
                }
            }
            if !ids.is_empty() && (ids.len() == 128 || entry.is_none()) {
                snapshot.lease.check()?;
                let reply = node
                    .client
                    .check_local_files(crate::rpc::wire::LocalFilesRequest {
                        run: Some(snapshot.lease.run.clone()),
                        files: ids.iter().map(ToString::to_string).collect(),
                        recover,
                    })
                    .await?;
                let states: Vec<crate::model::node_raw::File> =
                    serde_json::from_slice(&reply.files_json)?;
                ensure!(
                    states.len() == ids.len()
                        && states
                            .iter()
                            .map(|file| file.id)
                            .collect::<HashSet<_>>()
                            .len()
                            == ids.len(),
                    "invalid multipart inventory"
                );
                for value in states {
                    use crate::model::node_raw::FileState;
                    let id = value.id;
                    ensure!(
                        ids.contains(&id),
                        "control returned an unexpected local file"
                    );
                    let _guard = self.local_lock(id).await;
                    snapshot.lease.check()?;
                    if disk.pending(id) {
                        continue;
                    }
                    if value.state == FileState::Pending {
                        let storage = value.storage.context("multipart storage missing")?;
                        self.uploads
                            .lock()
                            .unwrap()
                            .entry(id)
                            .or_insert_with(|| disk.restore_upload(storage, value.size));
                    } else {
                        self.release_part(id);
                        if value.state == FileState::Removable {
                            disk.remove_fragment(id).await?;
                        }
                    }
                }
                ids.clear();
            }
            if entry.is_none() {
                break;
            }
        }
        Ok(())
    }

    pub(super) async fn raw_request(
        &self,
        profile: &StorageProfile,
    ) -> Result<crate::storage::raw_request::File> {
        self.initialize().await?;
        crate::storage::raw_request::File::create_in(
            &self.files(),
            Uuid::new_v4(),
            self.policy(profile)?,
        )
        .await
    }

    pub(super) async fn append_request(
        &self,
        file: &mut crate::storage::raw_request::File,
        data: &[u8],
    ) -> Result<()> {
        file.append_in(&self.files(), data).await
    }

    fn policy(&self, profile: &StorageProfile) -> Result<CachePolicy> {
        Ok(CachePolicy {
            enabled: profile.cache_enabled,
            capacity: profile
                .cache_soft_limit
                .as_deref()
                .map(crate::config::cache_bytes)
                .transpose()?
                .or(self.disk.limits[1]),
            max_entries: profile
                .cache_entries
                .map(|count| count as usize)
                .unwrap_or(self.entries)
                .min(self.entries),
            upload_cache: false,
            upload_capacity: None,
        })
    }

    async fn initialize(&self) -> Result<()> {
        self.ready
            .get_or_try_init(|| async {
                for area in ["chunks", "raw"] {
                    let root = self.root.join(area);
                    tokio::fs::create_dir_all(&root).await?;
                    ensure!(
                        tokio::fs::symlink_metadata(&self.root).await?.is_dir(),
                        "read cache directory was replaced"
                    );
                    let mut directories = tokio::fs::read_dir(root).await?;
                    while let Some(directory) = directories.next_entry().await? {
                        if !directory.file_type().await?.is_dir() {
                            continue;
                        }
                        let mut files = tokio::fs::read_dir(directory.path()).await?;
                        while let Some(file) = files.next_entry().await? {
                            if !file.file_type().await?.is_file() {
                                continue;
                            }
                            let size = file.metadata().await?.len();
                            let name = format!(
                                "{}/{}",
                                directory.file_name().to_string_lossy(),
                                file.file_name().to_string_lossy()
                            );
                            let parsed = if area == "raw" {
                                crate::storage::parse_chunk_name(&name)
                                    .map(|id| (id, CacheFile::Raw))
                            } else {
                                crate::storage::parse_cache_name(&name)
                                    .map(|(id, compressed)| (id, CacheFile::Chunk(compressed)))
                            };
                            let mut fifo = self.fifo.lock().await;
                            if parsed.is_some_and(|(id, format)| {
                                fifo.entries
                                    .get(&id)
                                    .is_some_and(|entry| entry.file == format)
                            }) {
                                continue;
                            }
                            if let Some((id, format)) = parsed.filter(|(id, format)| {
                                (*format == CacheFile::Raw
                                    || (1..=codec::MAX as u64).contains(&size))
                                    && !fifo.entries.contains_key(id)
                                    && fifo.entries.len() < self.entries
                                    && self.disk.limits[1].is_none_or(|limit| {
                                        self.disk.used()[1].saturating_add(size) <= limit
                                    })
                            }) {
                                fifo.insert(id, size, format);
                                self.disk.account_existing(Area::Cache, size);
                            } else {
                                drop(fifo);
                                tokio::fs::remove_file(file.path()).await?;
                            }
                        }
                    }
                }
                Ok::<_, anyhow::Error>(())
            })
            .await?;
        Ok(())
    }

    pub(super) async fn read(
        &self,
        id: Uuid,
        profile: &StorageProfile,
    ) -> Option<(Vec<u8>, bool, CachePin)> {
        self.lookups.fetch_add(1, Ordering::Relaxed);
        if !profile.cache_enabled || self.initialize().await.is_err() {
            return None;
        }
        let (pin, file) = self.files().pin(id).await?;
        let CacheFile::Chunk(compressed) = file else {
            return None;
        };
        match crate::storage::local::read_bounded(file.path(&self.root, id), codec::MAX).await {
            Ok(bytes) => Some((bytes, compressed, pin)),
            Err(_) => {
                drop(pin);
                self.reject(id).await;
                None
            }
        }
    }

    pub(super) fn hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) async fn reject(&self, id: Uuid) {
        self.files().reject(id).await;
    }

    pub(super) fn raw_lock(
        &self,
        source: &crate::model::raw::RawSource,
        profile: &StorageProfile,
    ) -> Option<Arc<tokio::sync::Mutex<()>>> {
        let policy = self.policy(profile).ok()?;
        (policy.enabled
            && policy
                .capacity
                .is_none_or(|limit| source.size as u64 <= limit))
        .then(|| self.raw_misses.get(source.location_id))
    }

    pub(super) async fn raw(&self, source: &crate::model::raw::RawSource) -> Option<RawFile> {
        self.lookups.fetch_add(1, Ordering::Relaxed);
        if self.initialize().await.is_err() {
            return None;
        }
        let id = source.location_id;
        let (pin, format) = self.files().pin(id).await?;
        if format != CacheFile::Raw {
            return None;
        }
        let result = async {
            let path = format.path(&self.root, id);
            ensure!(
                tokio::fs::symlink_metadata(&path).await?.is_file(),
                "raw cache was replaced"
            );
            let file = tokio::fs::File::open(path).await?;
            ensure!(
                file.metadata().await?.len() == source.size as u64,
                codec::IntegrityError::Length
            );
            Ok::<_, anyhow::Error>(file)
        }
        .await;
        match result {
            Ok(file) => Some(RawFile::Cached { file, _pin: pin }),
            Err(_) => {
                drop(pin);
                self.reject(id).await;
                None
            }
        }
    }

    pub(super) async fn begin_raw(
        &self,
        source: &crate::model::raw::RawSource,
        profile: &StorageProfile,
    ) -> Result<Option<Write>> {
        let policy = self.policy(profile)?;
        let size = source.size as u64;
        if !policy.enabled
            || policy.capacity.is_some_and(|limit| size > limit)
            || self.ready.get().is_none()
        {
            return Ok(None);
        }
        let Some(work) = self.work(source.location_id, 1) else {
            return Ok(None);
        };
        self.files()
            .begin_raw(source.location_id, size, policy, work)
            .await
    }

    pub(super) async fn finish_raw(
        self: &Arc<Self>,
        write: Write,
        source: &crate::model::raw::RawSource,
        prepared: Arc<super::reads::Prepared>,
    ) -> Result<RawFile> {
        let cache = self.clone();
        let (id, size) = (source.location_id, source.size as u64);
        // Keep the read protection and disk reservation through detached publication.
        tokio::spawn(async move {
            let _write = cache.writes.lock().await;
            prepared.check()?;
            cache.files().publish_raw(write, id, size).await
        })
        .await?
    }

    fn work(&self, id: Uuid, units: u32) -> Option<CacheWork> {
        let slot = self.slots.clone().try_acquire_owned().ok()?;
        let memory = self.memory.clone().try_acquire_many_owned(units).ok()?;
        if !self.pending.lock().unwrap().insert(id) {
            return None;
        }
        Some(CacheWork {
            id,
            pending: self.pending.clone(),
            _slot: slot,
            _memory: memory,
        })
    }

    pub(super) fn fill(self: &Arc<Self>, id: Uuid, data: Bytes, profile: &StorageProfile) {
        let Ok(policy) = self.policy(profile) else {
            return;
        };
        if !policy.enabled || self.ready.get().is_none() {
            return;
        }
        if let Ok(fifo) = self.fifo.try_lock()
            && fifo.entries.get(&id).is_some_and(|entry| entry.valid)
        {
            return;
        }
        // A returned Bytes may retain a complete decoder allocation, not just this slice.
        let units = (codec::MAX as u64 * 4).div_ceil(1024 * 1024) as u32;
        let Some(work) = self.work(id, units) else {
            return;
        };
        let cache = self.clone();
        tokio::spawn(async move {
            let _work = work;
            if let Err(error) = cache.files().fill(id, &data, false, policy).await {
                tracing::debug!(%error, "node read cache fill failed");
            }
        });
    }

    pub(super) async fn snapshot(&self) -> serde_json::Value {
        let capacity = self.disk.upload_capacity().ok();
        let used = self.disk.used();
        serde_json::json!({ "hits": self.hits.load(Ordering::Relaxed), "lookups": self.lookups.load(Ordering::Relaxed),
            "entries": self.fifo.lock().await.entries.len(), "bytes": used[1], "multipart_bytes": used[0], "thumbnail_bytes": used[2],
            "limit": capacity.map(|value| value.0), "upload_limit": capacity.map(|value| value.1), "upload_bytes": capacity.map(|value| value.2),
            "fills": self.pending.lock().unwrap().len() })
    }
}

pub(super) fn id(source: &crate::model::node_read::Source) -> Option<Uuid> {
    match source {
        crate::model::node_read::Source::Chunk { chunk } => Some(chunk.location_id),
        crate::model::node_read::Source::Pack { chunk, .. } => Some(chunk.uuid),
        _ => None,
    }
}
