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
use futures_util::{StreamExt, stream::BoxStream};
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
    http: Arc<Semaphore>,
    pool: Arc<compression::Pool>,
    pub(super) files: Arc<super::files::Cache>,
    pub(super) diagnostics: Arc<Diagnostics>,
}

pub(super) struct Reader {
    pub(super) background: bool,
    pub(super) io: Arc<Semaphore>,
    pub(super) cpu: Arc<Semaphore>,
    packs: Arc<Semaphore>,
    http: Arc<Semaphore>,
    pack_units: u32,
    pub(super) workspace: u64,
    pub(super) pool: Arc<compression::Pool>,
    gets: Arc<AtomicU64>,
    diagnostics: Arc<Diagnostics>,
    pub(super) files: Arc<super::files::Cache>,
}

impl Reader {
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
    ) -> Result<Option<OwnedSemaphorePermit>> {
        let local = if let Some(id) = storage {
            let backend = prepared.backend(id)?;
            if backend.profile.read_concurrency.is_some() {
                let grant = prepared
                    .grants
                    .iter()
                    .find(|grant| grant.storage == Some(id))
                    .context("storage read grant missing")?;
                let permit = grant.io.clone().acquire_owned().await?;
                grant.check()?;
                Some(permit)
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
        read_bounded(backend.store.backend.as_ref(), &path, bound).await
    }

    async fn chunk(
        &self,
        node: &NodeContext,
        prepared: Arc<Prepared>,
        source: &read::Source,
        keys: Arc<BTreeMap<Uuid, codec::Key>>,
        loaded: &mut Option<Loaded>,
    ) -> Result<Bytes> {
        let mut memory = Some(
            prepared
                .buffers
                .clone()
                .acquire_many_owned((3 * codec::MAX + 16).div_ceil(UNIT) as u32)
                .await?,
        );
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
        let verify = prepared.verify
            || if let Some(id) = source.storage()? {
                prepared.backend(id)?.profile.read_integrity == "inline"
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
                self.files.read(id, &backend.profile).await
            };
            if let Some((bytes, compressed, pin)) = cached {
                let cpu = self.cpu.clone().acquire_owned().await?;
                let pool = self.pool.clone();
                let chunk = chunk.clone();
                let held = prepared.clone();
                let memory = memory.take().unwrap();
                let decoded = tokio::task::spawn_blocking(move || {
                    let (_cpu, _pin) = (cpu, pin);
                    #[cfg(feature = "fault-injection")]
                    crate::faults::blocking("node-cache-decode");
                    let bytes = codec::decode_cache_read(
                        &chunk,
                        Bytes::from(bytes),
                        compressed,
                        &pool,
                        verify,
                    )?;
                    Ok::<_, anyhow::Error>(owned(bytes, memory, &held))
                })
                .await?;
                if let Ok(bytes) = decoded {
                    self.files.hit();
                    return Ok(bytes);
                }
                // Local cache damage must fall back before considering the authoritative source suspect.
                self.files.reject(id).await;
            }
        }
        let memory = match memory {
            Some(memory) => memory,
            None => {
                prepared
                    .buffers
                    .clone()
                    .acquire_many_owned((3 * codec::MAX + 16).div_ceil(UNIT) as u32)
                    .await?
            }
        };
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
                            crate::storage::chunk_name(pack.location_id)
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
            return Ok(owned(loaded.data.chunk(chunk)?, memory, &prepared));
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
                Path::from(crate::storage::chunk_name(chunk.location_id)),
                codec::MAX + 16,
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
            Ok(owned(bytes, memory, &prepared))
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
            let mut file = RawFile::Local(file);
            let _cpu = self.cpu.clone().acquire_owned().await?;
            file.verify(source).await?;
            file.rewind().await?;
            return Ok((file, io));
        }
        ensure!(source.medium == "shared_s3", "invalid node raw medium");
        let backend = prepared.backend(source.storage_id)?;
        let verify =
            verify_backend || prepared.verify || backend.profile.read_integrity == "inline";
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
    _local: Option<OwnedSemaphorePermit>,
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
    stream: Uuid,
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
    stream: Uuid,
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
        let mut loaded = None;
        loop {
            current.check()?;
            let value = mapping(&current, object.id, object.revision, position, end)?;
            let keys = Arc::new(current.keys()?);
            let mut prefetch = current.reply.next.clone().map(|cursor| {
                let reads = reads.clone(); let current = current.clone();
                Prefetch(tokio::spawn(async move { reads.window(current, cursor, retired).await }))
            });
            for (index, segment) in value.segments.into_iter().enumerate() {
                if let read::Source::Raw { source } = &segment.source {
                    loaded = None;
                    let result = current.guard(&reads.node, reads.reader.raw(&reads.node, &current, source, false)).await;
                    if let Err(error) = &result { reads.report_error(current.clone(), index, &segment.source, error).await; }
                    let (mut file, _io) = result?;
                    current.guard(&reads.node, async { file.seek(std::io::SeekFrom::Start(segment.source_offset as u64)).await?; Ok(()) }).await?;
                    let mut audit = reads.check_raw(current.clone(), index, source,
                        segment.source_offset == 0 && segment.length == source.size, matches!(&file, RawFile::Remote(_)))?;
                    let mut remaining = segment.length as usize;
                    while remaining > 0 {
                        let result = current.guard(&reads.node, async {
                            let memory = current.buffers.clone().acquire_many_owned((RAW_BUFFER * 2 / UNIT) as u32).await?;
                            let mut bytes = vec![0; remaining.min(RAW_BUFFER)];
                            let count = file.read(&mut bytes).await?;
                            ensure!(count > 0, codec::IntegrityError::Length);
                            bytes.truncate(count);
                            bytes.shrink_to_fit();
                            let retained = bytes.capacity();
                            Ok((owned(Bytes::from(bytes), memory, &current), retained))
                        }).await;
                        if let Err(error) = &result { reads.report_error(current.clone(), index, &segment.source, error).await; }
                        let (bytes, retained) = result?;
                        remaining -= bytes.len();
                        if let Some(audit) = &mut audit { audit.feed(bytes.clone(), retained); }
                        if remaining == 0 && position + segment.length as u64 == end { current.finish_source(); }
                        yield bytes;
                    }
                } else {
                    let result = current.guard(&reads.node, async {
                        match &segment.source {
                            read::Source::Chunk { .. } | read::Source::Pack { .. } | read::Source::Local { .. } => reads.reader.chunk(&reads.node, current.clone(), &segment.source, keys.clone(), &mut loaded).await,
                            _ => { loaded = None; reads.reader.fragment(&reads.node, current.clone(), &segment.source).await }
                        }
                    }).await;
                    if let Err(error) = &result { reads.report_error(current.clone(), index, &segment.source, error).await; }
                    let bytes = result?;
                    reads.check_chunk(current.clone(), index, &segment.source, bytes.clone())?;
                    if position + segment.length as u64 == end { current.finish_source(); }
                    if !reads.reader.background && !current.verify {
                        if let read::Source::Chunk { chunk } | read::Source::Pack { chunk, .. } | read::Source::Local { chunk, .. } = &segment.source {
                            reads.node.access.chunk(chunk.id, current.reply.http_status == 206, segment.length as usize, false);
                        }
                    }
                    yield bytes.slice(segment.source_offset as usize..(segment.source_offset + segment.length) as usize);
                }
                position += segment.length as u64;
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
