use super::{
    NodeContext,
    reads::{Prepared, Reads},
};
use crate::{
    content::{codec, compression, pack},
    model::node_read as read,
    rpc::wire,
    storage::{
        cache::{CacheFile, LoadedPack as Loaded},
        diagnostics::Diagnostics,
        model::{Options, Store},
        raw::RawFile,
    },
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures_util::{FutureExt, StreamExt, TryStreamExt, stream::BoxStream};
use object_store::{ObjectStoreExt, path::Path};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt},
    sync::{OwnedSemaphorePermit, Semaphore},
};
use uuid::Uuid;

#[path = "inspection.rs"]
mod inspection;

#[path = "encoding.rs"]
mod encoding;

#[path = "packing.rs"]
mod packing;

#[path = "transfer.rs"]
mod transfer;

#[path = "processing.rs"]
mod processing;

const UNIT: usize = 64 * 1024;
pub(super) const BUFFER_UNITS: usize = 16 * 1024 * 1024 / UNIT;
const RAW_BUFFER: usize = 256 * 1024;

pub(super) struct Backend {
    pub(super) store: Store,
    pub(super) profile: crate::storage::profile::StorageProfile,
}

impl Backend {
    pub(super) fn new(value: read::Storage, io: &Arc<super::io::Io>) -> Result<Self> {
        ensure!(
            value.connection.r#type == "s3",
            "nodes require shared S3 storage"
        );
        crate::storage::profile::validate(&value.profile, "s3")?;
        let mut store = Store::connect(&value.connection, Options::from_profile(&value.profile)?)?;
        store.backend = io.observe(store.backend);
        store.multipart = store.multipart.map(|store| io.multipart(store));
        if let Some(streaming) = &mut store.streaming {
            streaming.statistics = Some(io.write.clone());
        }
        Ok(Self {
            store,
            profile: value.profile,
        })
    }
}

#[derive(Clone)]
pub(super) struct Limits {
    pub(super) io: Arc<Semaphore>,
    cpu: Arc<Semaphore>,
    packs: Arc<Semaphore>,
    pub(super) http: Arc<Semaphore>,
    pool: Arc<compression::Pool>,
    pub(super) files: Arc<super::files::Cache>,
    pub(super) diagnostics: Arc<Diagnostics>,
}

pub(super) struct Reader {
    pub(super) background: bool,
    pub(super) io: Arc<Semaphore>,
    pub(super) cpu: Arc<Semaphore>,
    packs: Arc<Semaphore>,
    pub(super) http: Arc<Semaphore>,
    pack_units: u32,
    pub(super) workspace: u64,
    pub(super) pool: Arc<compression::Pool>,
    gets: Arc<AtomicU64>,
    diagnostics: Arc<Diagnostics>,
    pub(super) files: Arc<super::files::Cache>,
}

impl Reader {
    pub(super) async fn export_range(
        &self,
        prepared: &Prepared,
        range: &crate::model::replication::external::RawRead,
    ) -> Result<Bytes> {
        range.validate()?;
        ensure!(
            range.source.medium == "shared_s3" && !range.source.pending_upload,
            codec::IntegrityError::RawMetadata
        );
        let _io = self.io(prepared, Some(range.source.storage_id)).await?;
        let backend = prepared.backend(range.source.storage_id)?;
        backend.store.ensure_verified().await?;
        let mut file = RawFile::remote(
            backend.store.backend.clone(),
            &range.source,
            self.gets.clone(),
            self.diagnostics.clone(),
        )
        .await?;
        let resources = crate::content::transfer::Resources {
            cpu: self.cpu.clone(),
            memory: prepared.memory.clone(),
        };
        crate::content::transfer::read_range(range, &mut file, &resources).await
    }

    pub(super) fn new(node: &NodeContext) -> Result<Self> {
        let budget = node.budget.get().context("node budget missing")?;
        let limits = {
            let mut shared = node.body.lock().unwrap();
            if shared.is_none() {
                let config = compression::Config {
                    context_idle_timeout: node.config.instance.compression_idle_timeout.clone(),
                    ..Default::default()
                };
                let packs = Arc::new(Semaphore::new(budget.pack_memory_units as usize));
                *shared = Some(Limits {
                    files: super::files::Cache::new(node, packs.clone())?,
                    io: Arc::new(Semaphore::new(budget.storage_read_slots)),
                    cpu: Arc::new(Semaphore::new(budget.cpu_jobs)),
                    packs,
                    http: Arc::new(Semaphore::new(budget.pack_memory_units as usize)),
                    pool: Arc::new(compression::Pool::new(config, budget.cpu_jobs)?),
                    diagnostics: Arc::default(),
                });
            }
            shared.as_ref().unwrap().clone()
        };
        let files = limits.files;
        Ok(Self {
            background: false,
            io: limits.io,
            cpu: limits.cpu,
            packs: limits.packs,
            // Encoders must not consume their decoder's memory budget.
            http: limits.http,
            pack_units: budget.pack_memory_units,
            workspace: budget.compression_workspace,
            pool: limits.pool,
            gets: Arc::new(AtomicU64::new(0)),
            diagnostics: limits.diagnostics,
            files,
        })
    }

    pub(super) async fn acquire(
        &self,
        gate: &Arc<Semaphore>,
        count: u32,
    ) -> Result<OwnedSemaphorePermit> {
        if self.background {
            // Work already owns backend capacity; relinquish it instead of waiting locally.
            Ok(gate.clone().try_acquire_many_owned(count)?)
        } else {
            Ok(gate.clone().acquire_many_owned(count).await?)
        }
    }

    pub(super) async fn maintain(&self) -> Result<()> {
        self.pool.run().await
    }

    pub(super) async fn transfer_memory(&self) -> Result<Arc<OwnedSemaphorePermit>> {
        self.work_memory(crate::storage::external::objects::WINDOW)
            .await
    }
    pub(super) async fn work_memory(&self, size: usize) -> Result<Arc<OwnedSemaphorePermit>> {
        let units = (size as u64 * 4 + self.workspace).div_ceil(1024 * 1024);
        ensure!(
            units <= u64::from(self.pack_units),
            "transfer exceeds node memory budget"
        );
        Ok(Arc::new(self.acquire(&self.packs, units as u32).await?))
    }

    pub(super) async fn import_memory(&self) -> Result<Arc<OwnedSemaphorePermit>> {
        let units = (crate::storage::external::objects::WINDOW * 2).div_ceil(1024 * 1024) as u32;
        ensure!(
            units <= self.pack_units,
            "import input exceeds node memory budget"
        );
        Ok(Arc::new(self.acquire(&self.packs, units).await?))
    }

    pub(super) fn http_work(&self) -> Option<OwnedSemaphorePermit> {
        self.http.clone().try_acquire_many_owned(32).ok()
    }

    #[cfg(test)]
    pub(crate) fn backend_gets(&self) -> u64 {
        self.gets.load(Ordering::Relaxed)
    }

    async fn io(&self, prepared: &Prepared, storage: Option<Uuid>) -> Result<Io> {
        let global = self.acquire(&self.io, 1).await?;
        let local = self.storage_io(prepared, storage).await?;
        Ok(Io {
            _global: global,
            _local: local,
        })
    }

    async fn storage_io(
        &self,
        prepared: &Prepared,
        storage: Option<Uuid>,
    ) -> Result<Option<super::resources::LanePermit>> {
        let local = if let Some(id) = storage {
            let backend = prepared.backend(id)?;
            if backend.profile.read_concurrency.is_some() {
                let grants: Vec<_> = prepared
                    .grants
                    .iter()
                    .filter(|grant| grant.storage == Some(id) && !grant.returned())
                    .collect();
                ensure!(!grants.is_empty(), "storage read grant missing");
                let mut waiting: futures_util::stream::FuturesUnordered<_> =
                    grants.into_iter().map(|g| g.lane()).collect();
                loop {
                    if let Some(permit) = waiting
                        .next()
                        .await
                        .context("storage read grant missing")??
                    {
                        break Some(permit);
                    }
                }
            } else {
                None
            }
        } else {
            None
        };
        prepared.check()?;
        Ok(local)
    }

    async fn get(
        &self,
        prepared: &Prepared,
        storage: Uuid,
        path: Path,
        bound: usize,
    ) -> Result<Vec<u8>> {
        let _io = self.io(prepared, Some(storage)).await?;
        let backend = prepared.backend(storage)?;
        backend.store.ensure_verified().await?;
        self.gets.fetch_add(1, Ordering::Relaxed);
        crate::storage::read_retry::bounded(
            backend.store.backend.as_ref(),
            &path,
            bound,
            Some(prepared.recovery.configured(backend.profile.max_retries)),
        )
        .await
    }

    async fn chunk(
        &self,
        node: &NodeContext,
        prepared: Arc<Prepared>,
        source: &read::Source,
        keys: Arc<BTreeMap<Uuid, codec::Key>>,
        loaded: &mut Option<Loaded>,
    ) -> Result<(Bytes, bool)> {
        let (chunk, local) = match source {
            read::Source::Chunk { chunk } | read::Source::Pack { chunk, .. } => (chunk, false),
            read::Source::Local { node: owner, chunk } => {
                ensure!(
                    *owner == node.client.identity.node_id,
                    "local source belongs to another node"
                );
                (chunk, true)
            }
            _ => anyhow::bail!("invalid chunk source"),
        };
        ensure!(
            (1..=codec::MAX as i32).contains(&chunk.raw_size),
            codec::IntegrityError::Length
        );
        let raw = chunk.raw_size as usize;
        let bound = raw * 2
            + chunk
                .stored_size
                .unwrap_or(chunk.raw_size)
                .clamp(1, codec::MAX as i32 + 16) as usize
            + 16;
        let mut memory = Some(prepared.reserve(bound).await?);
        let verify = prepared.verify
            || if let Some(id) = source.storage()? {
                crate::storage::ReadContext::integrity(
                    &prepared.backend(id)?.profile.read_integrity,
                ) == "inline"
            } else {
                true
            };
        if !local {
            match source {
                read::Source::Pack { pack, .. } => pack.validate(keys.as_ref())?,
                _ => codec::validate_metadata(chunk, keys.as_ref())?,
            }
            let id = super::files::id(source).context("cache identity missing")?;
            let backend = prepared.backend(source.storage()?.context("source storage missing")?)?;
            let cached = {
                let _io = self.io(&prepared, None).await?;
                self.files.read(id, &backend.profile, raw).await
            };
            if let Some((bytes, compressed, pin)) = cached {
                let cpu = self.cpu.clone().acquire_owned().await?;
                let pool = self.pool.clone();
                let chunk = chunk.clone();
                let held = prepared.clone();
                let allocation = memory.take().unwrap();
                let (decoded, charge) = tokio::task::spawn_blocking(move || {
                    let (_cpu, _pin, _read) = (cpu, pin, held);
                    #[cfg(feature = "fault-injection")]
                    crate::faults::blocking("node-cache-decode");
                    let bytes = codec::decode_cache_read(
                        &chunk,
                        Bytes::from(bytes),
                        compressed,
                        &pool,
                        verify,
                    );
                    (bytes, allocation)
                })
                .await?;
                if let Ok(bytes) = decoded {
                    self.files.hit();
                    return Ok((charge.hold(bytes), false));
                }
                memory = Some(charge);
                // Local cache damage must fall back before considering the authoritative source suspect.
                self.files.reject(id).await;
            }
        }
        let memory = memory.unwrap();
        if let read::Source::Pack { pack, .. } = source {
            let current = loaded.as_ref().is_some_and(|value| {
                value.pack.location_id == pack.location_id
                    && value.pack.digest == pack.digest
                    && (!verify || value.verified)
            });
            if !current {
                *loaded = None;
                let units =
                    (pack.payload_size()? as u64 * 4 + self.workspace).div_ceil(1024 * 1024);
                ensure!(
                    units <= u64::from(self.pack_units),
                    "pack exceeds node memory budget"
                );
                let pack_memory = self.acquire(&self.packs, units as u32).await?;
                let bytes = self
                    .get(
                        &prepared,
                        pack.storage_id.context("pack storage missing")?,
                        Path::from(format!(
                            "packs/{}",
                            crate::storage::chunk_name(pack.address())
                        )),
                        pack.payload_size()? + 16,
                    )
                    .await?;
                let cpu = self.cpu.clone().acquire_owned().await?;
                let pool = self.pool.clone();
                let pack = pack.clone();
                let lease = prepared.clone();
                let keys = keys.clone();
                let access = node.access.clone();
                *loaded = Some(
                    tokio::task::spawn_blocking(move || {
                        let (_cpu, _lease) = (cpu, lease);
                        let data = pack::decode_read(&pack, bytes, keys.as_ref(), &pool, verify)?;
                        Ok::<_, anyhow::Error>(Loaded {
                            data,
                            pack,
                            verified: verify,
                            _memory: pack_memory,
                            used: Default::default(),
                            access,
                        })
                    })
                    .await??,
                );
            }
            let loaded = loaded.as_ref().unwrap();
            loaded.touch(
                chunk,
                !self.background && !prepared.verify,
                prepared.reply.http_status == 206,
            );
            return Ok((memory.hold(loaded.data.chunk(chunk)?), !current));
        }
        *loaded = None;
        let mut compressed = false;
        let encoded = if local {
            let _io = self.io(&prepared, None).await?;
            let id = if chunk.location_id.is_nil() {
                chunk.uuid
            } else {
                chunk.location_id
            };
            let root = &node.config.instance.data_path;
            match crate::storage::local::read_bounded(
                CacheFile::PendingChunk(false).path(root, id),
                codec::MAX,
            )
            .await
            {
                Ok(bytes) => bytes,
                Err(error)
                    if error
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
                {
                    compressed = true;
                    crate::storage::local::read_bounded(
                        CacheFile::PendingChunk(true).path(root, id),
                        codec::MAX,
                    )
                    .await?
                }
                Err(error) => return Err(error),
            }
        } else {
            codec::validate_metadata(chunk, keys.as_ref())?;
            self.get(
                &prepared,
                chunk.storage_id.context("chunk storage missing")?,
                Path::from(crate::storage::chunk_name(chunk.address())),
                usize::try_from(chunk.stored_size.context("chunk size missing")?)?,
            )
            .await?
        };
        let cpu = self.cpu.clone().acquire_owned().await?;
        let pool = self.pool.clone();
        let chunk = chunk.clone();
        tokio::task::spawn_blocking(move || {
            let _cpu = cpu;
            #[cfg(feature = "fault-injection")]
            crate::faults::blocking("node-read-decode");
            let bytes = if local {
                codec::decode_cache_read(&chunk, Bytes::from(encoded), compressed, &pool, true)?
            } else {
                codec::decode_read(&chunk, encoded, keys.as_ref(), 0, &pool, verify)?.0
            };
            let _read = prepared;
            Ok((memory.hold(bytes), !local))
        })
        .await?
    }

    async fn fragment(
        &self,
        node: &NodeContext,
        prepared: Arc<Prepared>,
        source: &read::Source,
    ) -> Result<Bytes> {
        let memory = prepared
            .buffers
            .clone()
            .acquire_many_owned((2 * codec::MAX).div_ceil(UNIT) as u32)
            .await?;
        let (bytes, size, hash) = match source {
            read::Source::Staged {
                storage,
                key,
                size,
                hash,
                ..
            } => {
                ensure!(
                    key.starts_with(crate::storage::STAGING_PREFIX),
                    "invalid staging key"
                );
                (
                    self.get(&prepared, *storage, Path::from(key.as_str()), codec::MAX)
                        .await?,
                    *size,
                    hash,
                )
            }
            read::Source::Fragment {
                node: owner,
                source,
            } => {
                ensure!(
                    *owner == node.client.identity.node_id
                        && (1..=codec::MAX as i32).contains(&source.size),
                    "invalid local fragment"
                );
                let _io = self.io(&prepared, None).await?;
                let root = &node.config.instance.data_path;
                let bytes = if let Some(file) = source.file_id {
                    let path = if let Some(raw) = source.raw_source {
                        CacheFile::PendingRaw
                            .path(root, raw)
                            .with_extension("partial")
                    } else {
                        root.join("pending/multipart/candidate")
                            .join(file.simple().to_string())
                    };
                    ensure!(
                        tokio::fs::symlink_metadata(&path).await?.is_file(),
                        "fragment is not a regular file"
                    );
                    let mut file = tokio::fs::File::open(path).await?;
                    let offset = source.file_offset.context("fragment offset missing")?;
                    let size = file.metadata().await?.len();
                    ensure!(
                        offset >= 0
                            && (offset as u64)
                                .checked_add(source.size as u64)
                                .is_some_and(|end| end <= size),
                        "fragment range invalid"
                    );
                    file.seek(std::io::SeekFrom::Start(offset as u64)).await?;
                    let mut bytes = vec![0; source.size as usize];
                    file.read_exact(&mut bytes).await?;
                    bytes
                } else {
                    crate::storage::local::read_bounded(
                        root.join("pending/multipart/fragment")
                            .join(source.location_id.simple().to_string()),
                        codec::MAX,
                    )
                    .await?
                };
                (bytes, i64::from(source.size), &source.hash)
            }
            _ => anyhow::bail!("invalid fragment source"),
        };
        ensure!(
            size > 0 && bytes.len() as i64 == size && hash.len() == 32,
            codec::IntegrityError::Length
        );
        let hash = hash.clone();
        let cpu = self.cpu.clone().acquire_owned().await?;
        tokio::task::spawn_blocking(move || {
            let _cpu = cpu;
            ensure!(
                blake3::hash(&bytes).as_bytes() == hash.as_slice(),
                codec::IntegrityError::Hash
            );
            Ok(owned(Bytes::from(bytes), memory, &prepared))
        })
        .await?
    }

    async fn raw(
        &self,
        node: &NodeContext,
        prepared: &Arc<Prepared>,
        source: &crate::model::raw::RawSource,
        verify_backend: bool,
    ) -> Result<(RawFile, Io)> {
        source.validate()?;
        ensure!(
            !verify_backend || !source.pending_upload,
            codec::IntegrityError::RawMetadata
        );
        let mut io = self.io(prepared, None).await?;
        if source.pending_upload {
            let path =
                CacheFile::PendingRaw.path(&node.config.instance.data_path, source.location_id);
            ensure!(
                tokio::fs::symlink_metadata(&path).await?.is_file(),
                "raw source is not a regular file"
            );
            let file = tokio::fs::File::open(path).await?;
            ensure!(
                file.metadata().await?.len() == source.size as u64,
                codec::IntegrityError::Length
            );
            return Ok((RawFile::Local(file), io));
        }
        ensure!(source.medium == "shared_s3", "invalid node raw medium");
        let backend = prepared.backend(source.storage_id)?;
        let verify = verify_backend;
        let lock = self.files.raw_lock(source, &backend.profile);
        let _miss = match &lock {
            Some(lock) => Some(lock.lock().await),
            None => None,
        };
        if !verify_backend
            && lock.is_some()
            && let Some(mut file) = self.files.raw(source).await
        {
            let checked = if verify {
                let _cpu = self.cpu.clone().acquire_owned().await?;
                #[cfg(feature = "fault-injection")]
                crate::faults::point("node-raw-cache-verify").await;
                file.verify(source).await
            } else {
                Ok(source.size as u64)
            };
            if checked.is_ok() && file.rewind().await.is_ok() {
                self.files.hit();
                return Ok((file, io));
            }
            drop(file);
            self.files.reject(source.location_id).await;
        }
        io._local = self.storage_io(prepared, Some(source.storage_id)).await?;
        backend.store.ensure_verified().await?;
        let mut file = RawFile::remote(
            backend.store.backend.clone(),
            source,
            self.gets.clone(),
            self.diagnostics.clone(),
        )
        .await?;
        if verify {
            let mut cache = match self.files.begin_raw(source, &backend.profile).await {
                Ok(cache) => cache,
                Err(error) => {
                    tracing::debug!(%error, "node raw cache unavailable; reading backend");
                    None
                }
            };
            let _cpu = self.cpu.clone().acquire_owned().await?;
            #[cfg(feature = "fault-injection")]
            crate::faults::point("node-raw-before-verify").await;
            crate::storage::raw::verify_file(&mut file, source, Some(&mut cache)).await?;
            drop(_cpu);
            if let Some(cache) = cache {
                match self.files.finish_raw(cache, source, prepared.clone()).await {
                    Ok(cached) => {
                        file = cached;
                        io._local = None;
                    }
                    Err(error) => {
                        tracing::debug!(%error, "node raw cache publication failed; reading backend")
                    }
                }
            }
            file.rewind().await?;
        }
        Ok((file, io))
    }
}

struct Io {
    _global: OwnedSemaphorePermit,
    _local: Option<super::resources::LanePermit>,
}
struct Output {
    bytes: Bytes,
    _memory: OwnedSemaphorePermit,
    _slot: Arc<OwnedSemaphorePermit>,
}
impl AsRef<[u8]> for Output {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
fn owned(bytes: Bytes, memory: OwnedSemaphorePermit, prepared: &Prepared) -> Bytes {
    Bytes::from_owner(Output {
        bytes,
        _memory: memory,
        _slot: prepared.memory.clone(),
    })
}

struct Prefetch(tokio::task::JoinHandle<Result<Prepared>>);
impl Drop for Prefetch {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl Reads {
    pub(super) async fn verify_copy_raw(
        &self,
        prepared: Prepared,
        object: &read::Object,
    ) -> Result<()> {
        ensure!(
            object.payload == "raw" && object.size > 0,
            "invalid raw copy source"
        );
        let lease = prepared
            .grants
            .iter()
            .find(|grant| grant.reference.r#type == wire::GrantType::ReadProtection as i32)
            .context("copy protection missing")?
            .reference
            .id
            .clone();
        let end = object.size as u64;
        let mut position = 0;
        let mut current = self
            .window(
                Arc::new(prepared),
                wire::ReadCursor {
                    lease_id: lease,
                    stream_id: object.id.to_string(),
                    payload_revision: object.revision as u64,
                    offset: 0,
                    end,
                },
                None,
            )
            .await?;
        loop {
            let held = Arc::new(current);
            let plan = mapping(&held, object.id, object.revision, position, end)?;
            let mut verified = std::collections::HashSet::new();
            for (index, segment) in plan.segments.iter().enumerate() {
                let read::Source::Raw { source } = &segment.source else {
                    anyhow::bail!("copy source is not raw");
                };
                if verified.insert(source.id) {
                    let result = held
                        .guard(&self.node, self.reader.raw(&self.node, &held, source, true))
                        .await;
                    if let Err(error) = &result {
                        self.report_error(held.clone(), index, &segment.source, error)
                            .await;
                    }
                    drop(result?);
                }
            }
            position = plan.end as u64;
            let Some(cursor) = held.reply.next.clone() else {
                break;
            };
            current = self.window(held, cursor, None).await?;
        }
        ensure!(position == end, "incomplete raw copy verification");
        Ok(())
    }
}

fn mapping(
    prepared: &Prepared,
    stream: crate::model::Id,
    revision: i64,
    start: u64,
    end: u64,
) -> Result<read::Mapping> {
    ensure!(
        prepared.reply.outcome == wire::ReadOutcome::Ready as i32 && !prepared.reply.body_reused,
        "read has no source plan"
    );
    let value: read::Mapping = prepared.material(
        prepared
            .reply
            .mapping
            .as_ref()
            .context("read mapping missing")?,
    )?;
    validate_mapping(&value, stream, revision, start, end)?;
    for segment in &value.segments {
        prepared.page_hashes(segment)?;
    }
    if let Some(cursor) = &prepared.reply.next {
        ensure!(
            cursor.stream_id == stream.to_string()
                && cursor.payload_revision == revision as u64
                && cursor.offset == value.end as u64
                && cursor.offset < end
                && cursor.end == end
                && prepared.grants.iter().any(|grant| grant.reference.r#type
                    == wire::GrantType::ReadProtection as i32
                    && grant.reference.id == cursor.lease_id),
            "invalid read cursor"
        );
    } else {
        ensure!(value.end as u64 == end, "read ended before requested range");
    }
    Ok(value)
}

fn validate_mapping(
    value: &read::Mapping,
    stream: crate::model::Id,
    revision: i64,
    start: u64,
    end: u64,
) -> Result<()> {
    ensure!(
        value.stream == stream
            && value.revision == revision
            && value.start >= 0
            && value.start as u64 == start
            && value.end >= value.start
            && value.end as u64 <= end
            && value.segments.len() <= read::MAX_ITEMS,
        "read mapping identity mismatch"
    );
    let mut position = value.start;
    for segment in &value.segments {
        ensure!(
            segment.offset == position && segment.length > 0 && segment.source_offset >= 0,
            "invalid read segment"
        );
        let size = match &segment.source {
            read::Source::Chunk { chunk }
            | read::Source::Pack { chunk, .. }
            | read::Source::Local { chunk, .. } => i64::from(chunk.raw_size),
            read::Source::Raw { source } => source.size,
            read::Source::Staged { size, .. } => *size,
            read::Source::Fragment { source, .. } => i64::from(source.size),
        };
        ensure!(
            segment
                .source_offset
                .checked_add(segment.length)
                .is_some_and(|limit| limit <= size),
            "source range invalid"
        );
        position = position
            .checked_add(segment.length)
            .context("mapping range overflow")?;
    }
    ensure!(
        position == value.end && (value.end > value.start || end == start),
        "incomplete mapping"
    );
    Ok(())
}

pub(super) fn stream(reads: Arc<Reads>, prepared: Prepared) -> BoxStream<'static, Result<Bytes>> {
    Box::pin(async_stream::try_stream! {
        let object: read::Object = prepared.material(prepared.reply.object.as_ref().context("object material missing")?)?;
        let mut position = prepared.reply.offset;
        let end = position.checked_add(prepared.reply.length).context("read range overflow")?;
        if !(object.size >= 0 && object.revision >= 0 && end <= object.size as u64) {
            Err(anyhow::anyhow!("invalid object range"))?;
        }
        prepared.check()?;
        if prepared.reply.outcome != wire::ReadOutcome::Ready as i32 || prepared.reply.body_reused {
            Err(anyhow::anyhow!("read has no source plan"))?;
        }
        if position == end { return; }
        if !prepared.grants.iter().any(|grant| grant.reference.r#type == wire::GrantType::ReadProtection as i32) {
            Err(anyhow::anyhow!("read protection missing"))?;
        }
        let mut current = Arc::new(prepared);
        let mut retired = None;
        let mut demand=crate::status::access::DemandRead::default();
        loop {
            current.check()?;
            let value = mapping(&current, object.id, object.revision, position, end)?;
            let keys = Arc::new(current.keys()?);
            let mut prefetch = current.reply.next.clone().map(|cursor| {
                let reads = reads.clone(); let current = current.clone();
                Prefetch(tokio::spawn(async move { reads.window(current, cursor, retired).await }))
            });
            for (batch_index,batch) in value.segments.chunks(read::LANES).enumerate() {
                let mut packs=std::collections::HashMap::new();
                let mut chunks=std::collections::HashMap::new();
                let mut tasks=Vec::with_capacity(batch.len());
                for (offset,segment) in batch.iter().enumerate() {
                    let index=batch_index*read::LANES+offset;
                    let loader = match &segment.source {
                        read::Source::Pack {pack,..} => Some(packs.entry(pack.location_id).or_insert_with(||Arc::new(tokio::sync::Mutex::new(None))).clone()),
                        _=>None,
                    };
                    let read=reads.clone();let held=current.clone();let keys=keys.clone();
                    let src=segment.source.clone();
                    let group = match &src {
                        read::Source::Chunk {chunk} | read::Source::Pack {chunk,..} | read::Source::Local {chunk,..} => Some(chunk.uuid),
                        _=>None,
                    };
                    let work=async move {
                        match &src {
                            read::Source::Raw {..} => Ok((Bytes::new(),false)),
                            read::Source::Chunk {..} | read::Source::Local {..} | read::Source::Pack {..} => {
                                if let Some(loader)=loader {
                                    let mut loaded=loader.lock().await;
                                    read.reader.chunk(&read.node,held.clone(),&src,keys,&mut loaded).await
                                } else {
                                    read.reader.chunk(&read.node,held.clone(),&src,keys,&mut None).await
                                }
                            }
                            _=>read.reader.fragment(&read.node,held.clone(),&src).await.map(|bytes|(bytes,false))
                        }
                    }.map(|result|result.map_err(Arc::new)).boxed().shared();
                    let job=if let Some(group)=group { chunks.entry(group).or_insert(work).clone() } else {work};
                    tasks.push(async move {(index,segment,crate::storage::read_retry::READ_AHEAD.scope(offset!=0,job).await)});
                }
                drop(packs);drop(chunks);
                let mut ready=futures_util::stream::iter(tasks).buffered(read::LANES);
                while let Some((index,segment,result))=current.guard(&reads.node,async {Ok(ready.next().await)}).await? {

                if let Some(capture)=&current.capture && let Some(source)=segment.source.cached_source(){
                    capture.lock().unwrap().demand(position as i64,position as i64+segment.length,source);
                }
                if let read::Source::Raw { source } = &segment.source {
                    use crate::storage::raw_segments as segments;
                    let strict=source.pending_upload || (!source.pending_upload && crate::storage::ReadContext::integrity(&current.backend(source.storage_id)?.profile.read_integrity)=="inline");
                    let audit_whole=current.verify && !strict && !source.pending_upload;
                    let opened=current.guard(&reads.node,reads.reader.raw(&reads.node,&current,source,audit_whole)).await;
                    if let Err(error)=&opened {reads.report_error(current.clone(),index,&segment.source,error).await;}
                    let (mut file,io)=opened?;
                    let mut remote=file.remote_source();
                    let mut local_io=if remote.is_some() {drop(io);None} else {Some(io)};
                    let start=segment.source_offset as u64;
                    let span=start..start+segment.length as u64;
                    let proof=current.page_hashes(segment)?.map(|bytes|segments::Proof::new(&span,bytes.to_vec(),reads.reader.cpu.clone()).map(Arc::new)).transpose()?;
                    if strict && proof.is_none(){Err(crate::model::Error::SourceUnavailable)?;}

                    let from_backend=remote.is_some();
                    let mut audit=reads.check_raw(current.clone(),index,source,start==0 && segment.length==source.size,from_backend)?;
                    let held=current.clone();
                    let retain:segments::Retain=Arc::new(move |n|{
                        let held=held.clone();Box::pin(async move{Ok(Box::new(held.reserve(n).await?) as segments::Permit)})
                    });
                    let mut received=0;
                    while received<segment.length {
                    let at=start+received as u64;
                    let span=at..start+segment.length as u64;
                    let proof=proof.clone();let retain=retain.clone();
                    let cached=matches!(&file,RawFile::Cached{..});let mut rejected=false;
                    let mut output=if let Some(remote)=remote.clone() {
                        let read=reads.clone();let held=current.clone();let id=source.storage_id;
                        let acquire:segments::Acquire=Arc::new(move || {
                            let read=read.clone();let held=held.clone();Box::pin(async move{
                                Ok(Box::new(read.reader.io(&held,Some(id)).await?) as segments::Permit)
                            })
                        });
                        let lanes=if current.backend(id)?.profile.read_concurrency.is_some() {
                            current.grants.iter().filter(|g|g.storage==Some(id) && g.reference.r#type==wire::GrantType::StorageRead as i32).count()
                        }else{read::LANES};
                        let profile=&current.backend(source.storage_id)?.profile;
                        let recovery=current.recovery.configured(profile.max_retries);
                        if let Some(cache)=reads.reader.files.partial(source,profile).await {
                            cache.stream(remote,span,strict,proof,lanes,acquire,retain,recovery)
                        }else{remote.stream(span,if strict {proof}else{None},lanes,acquire,retain,recovery).map_ok(|v|(v,true)).boxed()}
                    }else{segments::local(&mut file,span,source.size as u64,if strict {proof}else{None},retain).map_ok(|v|(v,false)).boxed()};
                    while let Some(result)=current.guard(&reads.node,async {Ok(output.next().await)}).await? {
                        if let Err(error)=&result {
                            if cached {rejected=true;break}
                            reads.report_error(current.clone(),index,&segment.source,error).await;
                        }
                        let (bytes,from_backend)=result?;received+=bytes.len() as i64;
                        if let Some(audit)=&mut audit {audit.feed(bytes.clone(),bytes.len());}
                        if received==segment.length && position+received as u64==end {current.finish_source();}
                        if !reads.reader.background && !current.verify {
                            reads.node.access.demand(&mut demand,crate::model::tiering::Source::Raw(source.location_id),bytes.len(),from_backend);
                        }
                        yield bytes;
                    }
                    drop(output);
                    if rejected {
                        drop(local_io.take());
                        let id=source.storage_id;
                        let backend=current.backend(id)?;
                        let _io=reads.reader.io(&current,Some(id)).await?;
                        backend.store.ensure_verified().await?;
                        file=RawFile::remote(backend.store.backend.clone(),source,reads.reader.gets.clone(),reads.reader.diagnostics.clone()).await?;
                        remote=file.remote_source();
                        reads.reader.diagnostics.cache_invalid.fetch_add(1,Ordering::Relaxed);
                        reads.reader.diagnostics.cache_fallback.fetch_add(1,Ordering::Relaxed);
                        reads.reader.files.reject(source.location_id).await;
                        continue;
                    }
                    if received!=segment.length {Err(codec::IntegrityError::Length)?;}
                    }
                } else {
                    if let Err(error) = &result { reads.report_error(current.clone(), index, &segment.source, error).await; }
                    let (bytes,origin)=result.map_err(|e|Arc::try_unwrap(e).unwrap_or_else(|e|anyhow::anyhow!("{e:#}")))?;
                    reads.check_chunk(current.clone(), index, &segment.source, bytes.clone())?;
                    if position + segment.length as u64 == end { current.finish_source(); }
                    if !reads.reader.background && !current.verify {
                        if let Some(source)=segment.source.demand(){
                            reads.node.access.demand(&mut demand,source,segment.length as usize,origin);
                        }
                        if let read::Source::Chunk { chunk } | read::Source::Pack { chunk, .. } | read::Source::Local { chunk, .. } = &segment.source {
                            reads.node.access.chunk(chunk.id, current.reply.http_status == 206, segment.length as usize, false);
                        }
                    }
                    yield bytes.slice(segment.source_offset as usize..(segment.source_offset + segment.length) as usize);
                }
                position += segment.length as u64;
                }
            }
            let Some(next) = &mut prefetch else { break; };
            retired = current.grants.iter().find(|grant| grant.reference.r#type == wire::GrantType::ReadWindow as i32)
                .map(|grant| Uuid::parse_str(&grant.reference.id)).transpose()?;
            let prepared = current.guard(&reads.node, async { (&mut next.0).await? }).await?;
            current = Arc::new(prepared);
        }
        if position != end { Err(anyhow::anyhow!("incomplete read body"))?; }
    })
}

#[cfg(test)]
#[path = "../../tests/rpc/body.rs"]
mod tests;

async fn read_bounded(
    backend: &dyn object_store::ObjectStore,
    path: &Path,
    bound: usize,
) -> Result<Vec<u8>> {
    let result = backend.get(&path).await?;
    ensure!(
        result.meta.size <= bound as u64,
        codec::IntegrityError::Length
    );
    let mut bytes = Vec::with_capacity(result.meta.size as usize);
    let mut stream = result.into_stream();
    while let Some(part) = stream.next().await {
        let part = part?;
        ensure!(
            bytes.len() + part.len() <= bound,
            codec::IntegrityError::Length
        );
        bytes.extend_from_slice(&part);
    }
    Ok(bytes)
}
