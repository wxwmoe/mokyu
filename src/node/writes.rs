use super::{
    NodeContext,
    reads::{self, Reads},
    resources::Grant,
};
use crate::{content::codec, model::node_read as read, rpc::wire, upload::Integrity};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt};
use object_store::{ObjectStoreExt, path::Path};
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
    time::Instant,
};
use uuid::Uuid;

const BATCH_BYTES: usize = 8 * 1024 * 1024;
const MAX_OBJECT: i64 = 5 * 1024 * 1024 * 1024;

pub(crate) struct Writes {
    reads: Arc<Reads>,
    slots: Arc<Semaphore>,
    io: Arc<Semaphore>,
    parallelism: u32,
}

pub(crate) use super::request::Request;

struct Operation {
    id: String,
    stream: String,
    bucket: Uuid,
    request: Request,
    grants: Vec<Arc<Grant>>,
    backend: Arc<super::body::Backend>,
    copy_references: bool,
    part: bool,
    checksum_algorithm: Option<String>,
}

impl Operation {
    fn lookup(&self) -> wire::WriteRequest {
        wire::WriteRequest {
            run: Some(self.request.run.run.clone()),
            operation_id: self.id.clone(),
        }
    }

    fn check(&self) -> Result<()> {
        self.request.check()?;
        for grant in &self.grants {
            grant.check()?;
        }
        Ok(())
    }

    async fn guard<T>(
        &self,
        node: &NodeContext,
        grants: &[Arc<Grant>],
        future: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        let mut events = node.subscribe();
        tokio::pin!(future);
        loop {
            self.check()?;
            for grant in grants {
                grant.check()?;
            }
            let deadline = self
                .grants
                .iter()
                .chain(grants)
                .map(|grant| grant.deadline())
                .fold(self.request.run.deadline(), std::cmp::min);
            tokio::select! {
                biased;
                _ = events.changed() => {},
                _ = tokio::time::sleep_until(deadline) => {},
                result = &mut future => { self.check()?; for grant in grants { grant.check()?; } return result; }
            }
        }
    }
}

struct Payload {
    input: wire::ChunkInput,
    bytes: Vec<u8>,
    _memory: Arc<OwnedSemaphorePermit>,
}
struct Batch {
    items: Vec<Payload>,
    _slot: OwnedSemaphorePermit,
}

struct Upload {
    bytes: Bytes,
    _memory: Arc<OwnedSemaphorePermit>,
    _io: Arc<OwnedSemaphorePermit>,
    _grants: Vec<Arc<Grant>>,
}
impl AsRef<[u8]> for Upload {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

pub(crate) fn declared(headers: &hyper::HeaderMap, length: Option<i64>) -> Result<Option<i64>> {
    let length = headers
        .get("x-amz-decoded-content-length")
        .map(|value| value.to_str()?.parse::<i64>().map_err(anyhow::Error::from))
        .transpose()?
        .or(length);
    ensure!(
        length.is_none_or(|value| (0..=MAX_OBJECT).contains(&value)),
        s3s::s3_error!(EntityTooLarge)
    );
    Ok(length)
}

impl Writes {
    #[cfg(test)]
    pub(crate) fn available(&self) -> [usize; 3] {
        [
            self.slots.available_permits(),
            self.reads.reader.cpu.available_permits(),
            self.io.available_permits(),
        ]
    }

    pub(crate) fn new(reads: Arc<Reads>) -> Result<Self> {
        let budget = reads.node.budget.get().context("node budget missing")?;
        Ok(Self {
            slots: Arc::new(Semaphore::new(budget.upload_concurrency)),
            io: Arc::new(Semaphore::new(budget.storage_upload_slots)),
            parallelism: budget.storage_upload_slots.min(2) as u32,
            reads,
        })
    }

    pub(crate) async fn prepare(&self, access: String, bucket: String) -> Result<Request> {
        let memory = Arc::new(self.slots.clone().acquire_owned().await?);
        super::request::prepare(&self.reads, memory, access, bucket, None).await
    }

    async fn begin(
        &self,
        mut request: Request,
        mut input: wire::BeginWriteRequest,
    ) -> Result<Operation> {
        let credential = request.authorize(&input.access_key, &input.bucket)?;
        input.run = Some(request.run.run.clone());
        input.operation_id = Uuid::now_v7().to_string();
        input.credential_revision = Some(credential.revision as u64);
        let held = self.reads.cache.snapshot();
        input.cached = reads::cached(&held);
        let id = input.operation_id.clone();
        let part = input.part.is_some();
        let node = &self.reads.node;
        let started = Instant::now();
        let mut reply = node.client.begin_write(input).await?;
        let grants = node.resources.adopt(&request.run, &reply.grants, started)?;
        ensure!(
            reply.operation_id == id
                && reply.phase == wire::WritePhase::WriteActive as i32
                && grants
                    .iter()
                    .any(|grant| grant.reference.r#type == wire::GrantType::WriteProtection as i32),
            "invalid write admission"
        );
        Uuid::parse_str(&reply.stream_id)?;
        request.materials = self.reads.cache.apply(
            node.client.identity.deployment_id,
            held,
            std::mem::take(&mut reply.materials),
        )?;
        let storage = reply.storage.as_ref().context("write storage missing")?;
        let value: read::Storage = request.material(storage)?;
        let backend = reads::backend(&request.materials, value.connection.id)?;
        Ok(Operation {
            copy_references: reply.copy_references,
            part,
            checksum_algorithm: reply.checksum_algorithm,
            bucket: Uuid::parse_str(&reply.bucket_id)?,
            id,
            stream: reply.stream_id,
            request,
            grants,
            backend,
        })
    }

    pub(crate) async fn put(
        &self,
        request: Request,
        input: wire::BeginWriteRequest,
        body: s3s::dto::StreamingBlob,
        check: Integrity,
        trailers: Option<s3s::TrailingHeaders>,
        if_match: Option<String>,
        if_none: Option<String>,
    ) -> Result<(String, serde_json::Value)> {
        let length = input.size;
        let key = input.key.clone();
        let operation = self.begin(request, input).await?;
        let _cache = self.reads.public.changing_object(operation.bucket, &key);
        let result = async {
            let (size, etag, sums) = self
                .write_body(&operation, body, check, trailers, length)
                .await?;
            self.publish(&operation, size, &etag, &sums, if_match, if_none)
                .await?;
            Ok((etag, sums))
        }
        .await;
        if result.is_err() {
            let _ = self.reads.node.client.abort_write(operation.lookup()).await;
        }
        result
    }

    pub(crate) async fn part(
        &self,
        request: Request,
        input: wire::BeginWriteRequest,
        body: s3s::dto::StreamingBlob,
        headers: &hyper::HeaderMap,
        trailers: Option<s3s::TrailingHeaders>,
    ) -> Result<(String, serde_json::Value)> {
        let length = input.size;
        let operation = self.begin(request, input).await?;
        let result = async {
            let check = Integrity::new(headers, operation.checksum_algorithm.as_deref())?;
            let (size, etag, sums) = self
                .write_body(&operation, body, check, trailers, length)
                .await?;
            self.publish(&operation, size, &etag, &sums, None, None)
                .await?;
            Ok((etag, sums))
        }
        .await;
        if result.is_err() {
            let _ = self.reads.node.client.abort_write(operation.lookup()).await;
        }
        result
    }

    async fn write_body(
        &self,
        operation: &Operation,
        body: s3s::dto::StreamingBlob,
        check: Integrity,
        trailers: Option<s3s::TrailingHeaders>,
        length: Option<u64>,
    ) -> Result<(u64, String, serde_json::Value)> {
        let profile = &operation.backend.profile;
        ensure!(
            profile.write_processing == "inline" && !profile.upload_cache,
            s3s::s3_error!(
                NotImplemented,
                "this node write path requires inline processing without upload cache"
            )
        );
        if profile.cdc_enabled {
            let config = profile
                .compression_config(&self.reads.node.config.instance.compression_idle_timeout);
            ensure!(
                config.workspace_bytes()? <= self.reads.reader.workspace,
                "compression workspace exceeds node budget"
            );
            let (tx, rx) = mpsc::channel(1);
            let (received, ()) = operation
                .guard(&self.reads.node, &[], async {
                    tokio::try_join!(
                        self.receive(operation, body, check, trailers, length, config, tx),
                        self.batches(operation, rx)
                    )
                })
                .await?;
            let (size, etag, sums, tail) = received;
            if !tail.is_empty() {
                operation
                    .guard(
                        &self.reads.node,
                        &[],
                        self.tail(operation, size - tail.len() as u64, tail),
                    )
                    .await?;
            }
            Ok((size, etag, sums))
        } else {
            operation
                .guard(
                    &self.reads.node,
                    &[],
                    self.raw(operation, body, check, trailers, length),
                )
                .await
        }
    }

    async fn publish(
        &self,
        operation: &Operation,
        size: u64,
        etag: &str,
        sums: &serde_json::Value,
        if_match: Option<String>,
        if_none: Option<String>,
    ) -> Result<()> {
        operation.check()?;
        let reply = self
            .reads
            .node
            .client
            .publish_write(wire::PublishWriteRequest {
                run: Some(operation.request.run.run.clone()),
                operation_id: operation.id.clone(),
                size,
                etag: etag.to_owned(),
                checksums_json: serde_json::to_vec(sums)?,
                if_match,
                if_none,
            })
            .await?;
        // Publication retires the lease; a confirmed terminal receipt wins over late renewals.
        ensure!(
            reply.operation_id == operation.id
                && reply.stream_id == operation.stream
                && reply.phase == wire::WritePhase::WritePublished as i32
                && reply
                    .published
                    .is_some_and(|value| value.size == size && value.etag == etag),
            "invalid publication receipt"
        );
        Ok(())
    }

    pub(crate) async fn copy(
        &self,
        request: Request,
        mut input: wire::BeginWriteRequest,
        source: reads::Prepared,
        if_match: Option<String>,
        if_none: Option<String>,
    ) -> Result<()> {
        let object: read::Object = source.material(
            source
                .reply
                .object
                .as_ref()
                .context("copy object missing")?,
        )?;
        ensure!(
            object.size >= 0 && source.run.run == request.run.run,
            "invalid copy source"
        );
        let protection = source
            .grants
            .iter()
            .find(|grant| grant.reference.r#type == wire::GrantType::ReadProtection as i32)
            .context("copy protection missing")?
            .clone();
        input.copy_read_id = Some(protection.reference.id.clone());
        input.size = Some(object.size as u64);
        let key = input.key.clone();
        let mut operation = self.begin(request, input).await?;
        operation.grants.push(protection);
        let _cache = self.reads.public.changing_object(operation.bucket, &key);
        let result = async {
            if operation.copy_references {
                let mut offset = 0;
                let mut batch_id = 1;
                while offset < object.size as u64 {
                    let reply = operation
                        .guard(&self.reads.node, &[], async {
                            Ok(self
                                .reads
                                .node
                                .client
                                .copy_chunks(wire::CopyChunksRequest {
                                    run: Some(operation.request.run.run.clone()),
                                    operation_id: operation.id.clone(),
                                    batch_id,
                                    offset,
                                })
                                .await?)
                        })
                        .await?;
                    ensure!(
                        reply.batch_id == batch_id
                            && reply.end > offset
                            && reply.end <= object.size as u64,
                        "invalid copy batch receipt"
                    );
                    offset = reply.end;
                    batch_id += 1;
                }
            } else {
                let body = self.reads.copy_body(source).await?;
                let body = crate::http::object::blob(body.map_err(std::io::Error::other).boxed());
                let check = Integrity::new(&hyper::HeaderMap::new(), None)?;
                let (size, _, _) = self
                    .write_body(&operation, body, check, None, Some(object.size as u64))
                    .await?;
                ensure!(size == object.size as u64, s3s::s3_error!(IncompleteBody));
            }
            #[cfg(feature = "fault-injection")]
            operation
                .guard(&self.reads.node, &[], async {
                    crate::faults::point("node-copy-before-publish").await;
                    Ok(())
                })
                .await?;
            self.publish(
                &operation,
                object.size as u64,
                &object.etag,
                &object.checksums,
                if_match,
                if_none,
            )
            .await
        }
        .await;
        if result.is_err() {
            let _ = self.reads.node.client.abort_write(operation.lookup()).await;
        }
        result
    }

    async fn tail(&self, operation: &Operation, offset: u64, bytes: Vec<u8>) -> Result<()> {
        let node = &self.reads.node;
        let io = Arc::new(self.io.clone().acquire_owned().await?);
        let held = self.reads.cache.snapshot();
        let size = bytes.len() as u32;
        let started = Instant::now();
        let mut reply = node
            .client
            .prepare_tail(wire::PrepareTailRequest {
                run: Some(operation.request.run.run.clone()),
                operation_id: operation.id.clone(),
                offset,
                size,
                hash: blake3::hash(&bytes).as_bytes().to_vec(),
                cached: reads::cached(&held),
            })
            .await?;
        let fragment = Uuid::parse_str(&reply.fragment_id)?;
        ensure!(
            !fragment.is_nil()
                && reply.end == offset + u64::from(size)
                && reply.object_key
                    == format!(
                        "{}fragment/{}",
                        crate::storage::STAGING_PREFIX,
                        fragment.simple()
                    ),
            "invalid tail plan"
        );
        if !reply.committed {
            let grants = node
                .resources
                .adopt(&operation.request.run, &reply.grants, started)?;
            let materials = self.reads.cache.apply(
                node.client.identity.deployment_id,
                held,
                std::mem::take(&mut reply.materials),
            )?;
            let value: read::Storage = reads::material(
                &materials,
                reply.storage.as_ref().context("tail storage missing")?,
            )?;
            let backend = reads::backend(&materials, value.connection.id)?;
            ensure!(
                backend.profile.upload_concurrency.is_none()
                    || grants.iter().any(|grant| grant.reference.r#type
                        == wire::GrantType::StorageWrite as i32
                        && grant.storage == Some(value.connection.id)),
                "tail upload permit missing"
            );
            operation
                .guard(node, &grants, async {
                    backend.store.ensure_verified().await?;
                    backend
                        .store
                        .backend
                        .put(
                            &Path::from(reply.object_key.clone()),
                            Bytes::from_owner(Upload {
                                bytes: bytes.into(),
                                _memory: operation.request.memory.clone(),
                                _io: io,
                                _grants: grants.clone(),
                            })
                            .into(),
                        )
                        .await?;
                    Ok(())
                })
                .await?;
            #[cfg(feature = "fault-injection")]
            crate::faults::point("node-tail-uploaded").await;
            operation.check()?;
            let committed = node.client.commit_tail(operation.lookup()).await?;
            ensure!(
                committed.committed
                    && committed.fragment_id == reply.fragment_id
                    && committed.end == reply.end,
                "invalid tail receipt"
            );
        }
        Ok(())
    }

    async fn raw(
        &self,
        operation: &Operation,
        mut body: s3s::dto::StreamingBlob,
        mut check: Integrity,
        trailers: Option<s3s::TrailingHeaders>,
        length: Option<u64>,
    ) -> Result<(u64, String, serde_json::Value)> {
        let node = &self.reads.node;
        let files = &self.reads.reader.files;
        let mut file = files.raw_request(&operation.backend.profile).await?;
        let idle = Duration::from_secs(crate::config::seconds(
            &node.snapshot()?.runtime().uploads.client_idle_timeout,
        )?);
        let mut hash = operation
            .backend
            .profile
            .deduplication_enabled
            .then(blake3::Hasher::new);
        let mut size = 0u64;
        while let Some(data) = tokio::time::timeout(idle, body.next())
            .await
            .map_err(|_| s3s::s3_error!(RequestTimeout))?
        {
            let data = data.map_err(|_| s3s::s3_error!(IncompleteBody))?;
            size = size
                .checked_add(data.len() as u64)
                .context("object length overflow")?;
            ensure!(
                size <= MAX_OBJECT as u64 && length.is_none_or(|length| size <= length),
                s3s::s3_error!(EntityTooLarge)
            );
            for bytes in data.chunks(256 * 1024) {
                check.update(bytes);
                if let Some(hash) = &mut hash {
                    hash.update(bytes);
                }
                files.append_request(&mut file, bytes).await?;
            }
        }
        ensure!(
            length.is_none_or(|length| size == length),
            s3s::s3_error!(IncompleteBody)
        );
        let (etag, sums) = check.finish(trailers)?;
        let digest: [u8; 16] = hex::decode(&etag)?
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid raw digest"))?;
        // Acquire local IO before reserving shared storage capacity.
        let io = Arc::new(self.io.clone().acquire_owned().await?);
        let started = Instant::now();
        let plan = node
            .client
            .prepare_raw(wire::PrepareRawRequest {
                run: Some(operation.request.run.run.clone()),
                operation_id: operation.id.clone(),
                size,
                digest: digest.to_vec(),
                content_hash: hash
                    .map(|hash| hash.finalize().as_bytes().to_vec())
                    .unwrap_or_default(),
            })
            .await?;
        let source = Uuid::parse_str(&plan.source_id)?;
        ensure!(
            !source.is_nil() && plan.size == size,
            "invalid raw write plan"
        );
        let grants = node
            .resources
            .adopt(&operation.request.run, &plan.grants, started)?;
        if !plan.committed {
            ensure!(operation.backend.profile.upload_concurrency.is_none() || grants.iter().any(|grant|
                grant.reference.r#type == wire::GrantType::StorageWrite as i32), "raw upload permit missing");
            let upload = operation
                .backend
                .store
                .streaming
                .as_ref()
                .context("S3 streaming upload missing")?;
            let held: Vec<_> = operation.grants.iter().chain(&grants).cloned().collect();
            operation
                .guard(
                    node,
                    &grants,
                    upload.put(
                        &crate::storage::raw::remote_path(source),
                        size,
                        &digest,
                        || {
                            file.replay()
                                .map_ok(|bytes| {
                                    Bytes::from_owner(Upload {
                                        bytes,
                                        _memory: operation.request.memory.clone(),
                                        _io: io.clone(),
                                        _grants: held.clone(),
                                    })
                                })
                                .boxed()
                        },
                    ),
                )
                .await?;
            #[cfg(feature = "fault-injection")]
            crate::faults::point("node-raw-uploaded").await;
            operation.check()?;
            let reply = node
                .client
                .commit_raw(wire::CommitRawRequest {
                    run: Some(operation.request.run.run.clone()),
                    operation_id: operation.id.clone(),
                    source_id: plan.source_id.clone(),
                })
                .await?;
            ensure!(
                reply.committed && reply.source_id == plan.source_id && reply.size == size,
                "invalid raw commit receipt"
            );
        }
        Ok((size, etag, sums))
    }

    async fn payload(
        &self,
        operation: &Operation,
        raw: Vec<u8>,
        config: crate::content::compression::Config,
    ) -> Result<Payload> {
        let cpu = self.reads.reader.cpu.clone().acquire_owned().await?;
        let memory = operation.request.memory.clone();
        let grants = operation.grants.clone();
        let pool = self.reads.reader.pool.clone();
        tokio::task::spawn_blocking(move || {
            let (_cpu, _grants) = (cpu, grants);
            let hash = blake3::hash(&raw).as_bytes().to_vec();
            let raw_size = raw.len() as u32;
            let compressed = pool.compress_for(&raw, true, &config)?;
            let flag = compressed.is_some();
            let bytes = compressed.unwrap_or(raw);
            Ok(Payload {
                _memory: memory,
                input: wire::ChunkInput {
                    hash,
                    raw_size,
                    payload_hash: blake3::hash(&bytes).as_bytes().to_vec(),
                    payload_size: bytes.len() as u32,
                    compressed: flag,
                },
                bytes,
            })
        })
        .await?
    }

    #[allow(clippy::too_many_arguments)]
    async fn receive(
        &self,
        operation: &Operation,
        mut body: s3s::dto::StreamingBlob,
        mut check: Integrity,
        trailers: Option<s3s::TrailingHeaders>,
        length: Option<u64>,
        config: crate::content::compression::Config,
        tx: mpsc::Sender<Batch>,
    ) -> Result<(u64, String, serde_json::Value, Vec<u8>)> {
        let slots = Arc::new(Semaphore::new(2));
        let mut slot = Some(slots.clone().acquire_owned().await?);
        let mut items = Vec::new();
        let mut bytes = 0;
        let mut window = Vec::with_capacity(codec::MAX);
        let mut size = 0u64;
        let idle = Duration::from_secs(crate::config::seconds(
            &self
                .reads
                .node
                .snapshot()?
                .runtime()
                .uploads
                .client_idle_timeout,
        )?);
        loop {
            let next = tokio::time::timeout(idle, body.next())
                .await
                .map_err(|_| s3s::s3_error!(RequestTimeout))?;
            let eof = next.is_none();
            let data = next
                .transpose()
                .map_err(|_| s3s::s3_error!(IncompleteBody))?
                .unwrap_or_default();
            size = size
                .checked_add(data.len() as u64)
                .context("object length overflow")?;
            ensure!(
                size <= MAX_OBJECT as u64 && length.is_none_or(|length| size <= length),
                s3s::s3_error!(EntityTooLarge)
            );
            check.update(&data);
            let mut rest = data.as_ref();
            loop {
                let take = (codec::MAX - window.len()).min(rest.len());
                window.extend_from_slice(&rest[..take]);
                rest = &rest[take..];
                if window.len() == codec::MAX || (eof && !operation.part && !window.is_empty()) {
                    let take = codec::cut(&window);
                    let payload = self
                        .payload(operation, window[..take].to_vec(), config.clone())
                        .await?;
                    window.drain(..take);
                    if bytes + take > BATCH_BYTES
                        || items.len() == crate::model::node_chunk::MAX_ITEMS
                    {
                        tx.send(Batch {
                            items: std::mem::take(&mut items),
                            _slot: slot.take().unwrap(),
                        })
                        .await
                        .map_err(|_| anyhow::anyhow!("write pipeline stopped"))?;
                        slot = Some(slots.clone().acquire_owned().await?);
                        bytes = 0;
                    }
                    bytes += take;
                    items.push(payload);
                } else {
                    break;
                }
            }
            if eof {
                break;
            }
        }
        ensure!(
            length.is_none_or(|length| length == size),
            s3s::s3_error!(IncompleteBody)
        );
        let (etag, sums) = check.finish(trailers)?;
        if !items.is_empty() {
            tx.send(Batch {
                items,
                _slot: slot.take().unwrap(),
            })
            .await
            .map_err(|_| anyhow::anyhow!("write pipeline stopped"))?;
        }
        Ok((size, etag, sums, window))
    }

    async fn batches(&self, operation: &Operation, mut rx: mpsc::Receiver<Batch>) -> Result<()> {
        let node = &self.reads.node;
        let mut offset = 0;
        let mut batch_id = 1;
        while let Some(batch) = rx.recv().await {
            let io = Arc::new(self.io.clone().acquire_many_owned(self.parallelism).await?);
            let held = self.reads.cache.snapshot();
            let end = offset
                + batch
                    .items
                    .iter()
                    .map(|item| u64::from(item.input.raw_size))
                    .sum::<u64>();
            let started = Instant::now();
            let mut reply = node
                .client
                .prepare_chunks(wire::PrepareChunksRequest {
                    run: Some(operation.request.run.run.clone()),
                    operation_id: operation.id.clone(),
                    batch_id,
                    offset,
                    chunks: batch.items.iter().map(|item| item.input.clone()).collect(),
                    parallelism: self.parallelism,
                    cached: reads::cached(&held),
                })
                .await?;
            let grants = node
                .resources
                .adopt(&operation.request.run, &reply.grants, started)?;
            ensure!(
                reply.batch_id == batch_id
                    && reply.end == end
                    && reply.chunks.len() == batch.items.len(),
                "invalid chunk batch receipt"
            );
            let materials = self.reads.cache.apply(
                node.client.identity.deployment_id,
                held,
                std::mem::take(&mut reply.materials),
            )?;
            let keys = Arc::new(reads::keys(&materials)?);
            let mut seen = HashSet::new();
            let mut next = offset;
            let mut uploads = Vec::new();
            for (entry, payload) in reply.chunks.into_iter().zip(batch.items) {
                let chunk: codec::Chunk = serde_json::from_slice(&entry.chunk_json)?;
                ensure!(
                    entry.offset == next
                        && chunk.hash == payload.input.hash
                        && chunk.raw_size as u32 == payload.input.raw_size,
                    "chunk plan does not match input"
                );
                next += u64::from(payload.input.raw_size);
                if !entry.reused && !reply.committed && seen.insert(chunk.uuid) {
                    ensure!(
                        entry.payload_hash == payload.input.payload_hash
                            && chunk.compressed == payload.input.compressed,
                        "chunk payload does not match plan"
                    );
                    let backend = reads::backend(
                        &materials,
                        chunk.storage_id.context("chunk storage missing")?,
                    )?;
                    ensure!(
                        backend.profile.storage_id == operation.backend.profile.storage_id,
                        "write storage changed"
                    );
                    uploads.push((chunk, payload.bytes, backend, entry.payload_hash));
                }
            }
            let permits: Vec<_> = grants
                .iter()
                .filter(|grant| grant.reference.r#type == wire::GrantType::StorageWrite as i32)
                .collect();
            let parallelism = if !uploads.is_empty()
                && (operation.backend.profile.upload_concurrency.is_some() || !permits.is_empty())
            {
                ensure!(
                    !permits.is_empty()
                        && permits.iter().all(
                            |grant| grant.storage == Some(operation.backend.profile.storage_id)
                        ),
                    "storage write grants missing"
                );
                (self.parallelism as usize).min(permits.len())
            } else {
                self.parallelism as usize
            };
            let encoded = operation
                .guard(node, &grants, async {
                    futures_util::stream::iter(uploads.into_iter().map(
                        |(chunk, bytes, backend, hash)| {
                            let keys = keys.clone();
                            let io = io.clone();
                            let memory = operation.request.memory.clone();
                            let grants = grants.clone();
                            async move {
                                for grant in &grants {
                                    grant.check()?;
                                }
                                let cpu = self.reads.reader.cpu.clone().acquire_owned().await?;
                                let id = chunk.uuid;
                                let location = chunk.location_id;
                                let (body, digest) =
                                    tokio::task::spawn_blocking(move || -> Result<_> {
                                        let _cpu = cpu;
                                        #[cfg(feature = "fault-injection")]
                                        crate::faults::blocking("node-write-seal");
                                        ensure!(
                                            blake3::hash(&bytes).as_bytes().as_slice() == hash,
                                            "write payload changed after preparation"
                                        );
                                        let bytes = codec::seal(&chunk, bytes, keys.as_ref())?;
                                        let digest = blake3::hash(&bytes).as_bytes().to_vec();
                                        Ok((
                                            Upload {
                                                bytes: bytes.into(),
                                                _memory: memory,
                                                _io: io,
                                                _grants: grants,
                                            },
                                            digest,
                                        ))
                                    })
                                    .await??;
                                backend.store.ensure_verified().await?;
                                backend
                                    .store
                                    .backend
                                    .put(
                                        &Path::from(crate::storage::chunk_name(location)),
                                        Bytes::from_owner(body).into(),
                                    )
                                    .await?;
                                Ok::<_, anyhow::Error>(wire::EncodedChunk {
                                    chunk_id: id.to_string(),
                                    digest,
                                })
                            }
                        },
                    ))
                    .buffer_unordered(parallelism)
                    .try_collect::<Vec<_>>()
                    .await
                })
                .await?;
            operation.check()?;
            for grant in &grants {
                grant.check()?;
            }
            if !reply.committed {
                let committed = node
                    .client
                    .commit_chunks(wire::CommitChunksRequest {
                        run: Some(operation.request.run.run.clone()),
                        operation_id: operation.id.clone(),
                        batch_id,
                        chunks: encoded,
                    })
                    .await?;
                ensure!(
                    committed.batch_id == batch_id && committed.end == end,
                    "invalid chunk commit receipt"
                );
            }
            offset = end;
            batch_id += 1;
        }
        Ok(())
    }
}
