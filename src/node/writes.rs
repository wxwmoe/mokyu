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

#[path = "completion.rs"]
mod completion;
#[path = "native.rs"]
mod native;

#[path = "cached.rs"]
mod cached;
#[path = "conversion.rs"]
mod conversion;
#[path = "migration.rs"]
mod migration;

#[path = "export.rs"]
mod export;
#[path = "import.rs"]
mod import;

const BATCH_BYTES: usize = 8 * 1024 * 1024;
const MAX_OBJECT: i64 = 5 * 1024 * 1024 * 1024;

#[derive(Clone)]
pub(super) struct Limits {
    pub(super) slots: Arc<Semaphore>,
    completions: Arc<Semaphore>,
    io: Arc<Semaphore>,
}

pub(crate) struct Writes {
    reads: Arc<Reads>,
    pub(super) slots: Arc<Semaphore>,
    completions: Arc<Semaphore>,
    pub(super) io: Arc<Semaphore>,
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
    packing: bool,
    part: bool,
    cached_part: std::sync::atomic::AtomicBool,
    limit: u64,
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

    #[cfg(test)]
    pub(crate) async fn limit_io(&self) -> Result<OwnedSemaphorePermit> {
        let count = self
            .reads
            .node
            .budget
            .get()
            .context("node budget missing")?
            .storage_upload_slots
            .saturating_sub(self.parallelism as usize);
        Ok(self.io.clone().acquire_many_owned(count as u32).await?)
    }

    pub(crate) fn new(reads: Arc<Reads>) -> Result<Self> {
        let budget = reads.node.budget.get().context("node budget missing")?;
        let limits = {
            let mut shared = reads.node.uploads.lock().unwrap();
            shared
                .get_or_insert_with(|| Limits {
                    slots: Arc::new(Semaphore::new(budget.upload_concurrency)),
                    completions: Arc::new(Semaphore::new(budget.upload_concurrency)),
                    io: Arc::new(Semaphore::new(budget.storage_upload_slots)),
                })
                .clone()
        };
        Ok(Self {
            slots: limits.slots,
            completions: limits.completions,
            io: limits.io,
            parallelism: budget.storage_upload_slots.min(2) as u32,
            reads,
        })
    }

    pub(crate) async fn prepare(&self, access: String, bucket: String) -> Result<Request> {
        let acquire = self.slots.clone().acquire_owned();
        tokio::pin!(acquire);
        #[cfg(feature = "fault-injection")]
        let permit = tokio::select! {
            biased;
            value=&mut acquire=>value?,
            _=crate::faults::point("node-write-memory-waiting")=>acquire.await?,
        };
        #[cfg(not(feature = "fault-injection"))]
        let permit = acquire.await?;
        let memory = Arc::new(permit);
        super::request::prepare(&self.reads, memory, access, bucket, None).await
    }

    async fn begin(
        &self,
        request: Request,
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
        let limit = if input.copy_read_id.is_some() {
            input.size.context("copy size missing")?
        } else {
            MAX_OBJECT as u64
        };
        let node = &self.reads.node;
        let started = Instant::now();
        let reply = node.client.begin_write(input).await?;
        ensure!(reply.operation_id == id, "invalid write identity");
        self.accept(request, reply, held, started, part, limit)
    }

    fn accept(
        &self,
        mut request: Request,
        mut reply: wire::WriteReply,
        held: reads::Held,
        started: Instant,
        part: bool,
        limit: u64,
    ) -> Result<Operation> {
        let node = &self.reads.node;
        let id = reply.operation_id.clone();
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
            packing: false,
            copy_references: reply.copy_references,
            part,
            cached_part: false.into(),
            limit,
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
            if !operation.backend.profile.cdc_enabled {
                self.reads.node.native_work.notify_one();
                if !operation
                    .cached_part
                    .load(std::sync::atomic::Ordering::Acquire)
                {
                    self.wait_native(&operation.stream).await?;
                }
            }
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
        if profile.cdc_enabled && profile.upload_cache
            || !operation.part && (profile.upload_cache || profile.write_processing == "background")
        {
            return operation
                .guard(
                    &self.reads.node,
                    &[],
                    self.raw(operation, body, check, trailers, length),
                )
                .await;
        }
        if operation.part && !profile.cdc_enabled {
            return operation
                .guard(
                    &self.reads.node,
                    &[],
                    self.local_part(operation, body, check, trailers, length),
                )
                .await;
        }
        if profile.cdc_enabled {
            self.chunks(operation, body, check, trailers, length).await
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

    async fn chunks(
        &self,
        operation: &Operation,
        body: s3s::dto::StreamingBlob,
        check: Integrity,
        trailers: Option<s3s::TrailingHeaders>,
        length: Option<u64>,
    ) -> Result<(u64, String, serde_json::Value)> {
        let profile = &operation.backend.profile;
        let config =
            profile.compression_config(&self.reads.node.config.instance.compression_idle_timeout);
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
    }

    async fn publish(
        &self,
        operation: &Operation,
        size: u64,
        etag: &str,
        sums: &serde_json::Value,
        if_match: Option<String>,
        if_none: Option<String>,
    ) -> Result<wire::WriteReply> {
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
                    .as_ref()
                    .is_some_and(|value| value.size == size && value.etag == etag),
            "invalid publication receipt"
        );
        Ok(reply)
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
                if object.payload == "raw" {
                    operation
                        .guard(
                            &self.reads.node,
                            &[],
                            self.reads.verify_copy_raw(source, &object),
                        )
                        .await?;
                }
                let mut offset = 0;
                let mut batch_id = 1;
                while offset < object.size as u64 {
                    let reply = operation
                        .guard(&self.reads.node, &[], async {
                            Ok(self
                                .reads
                                .node
                                .client
                                .copy_references(wire::CopyReferencesRequest {
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
                    #[cfg(feature = "fault-injection")]
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
            .map(|_| ())
        }
        .await;
        if result.is_err() {
            let _ = self.reads.node.client.abort_write(operation.lookup()).await;
        }
        result
    }

    async fn tail(&self, operation: &Operation, offset: u64, bytes: Vec<u8>) -> Result<()> {
        let node = &self.reads.node;
        let io = Arc::new(self.reads.reader.acquire(&self.io, 1).await?);
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
        let epoch = node.cache_epoch.load(std::sync::atomic::Ordering::Acquire);
        files.recover_uploads(node).await?;
        let charge = if let Some(size) = length {
            files
                .claim_upload(node, &operation.backend.profile, size)
                .await?
        } else {
            None
        };
        if length.is_some()
            && charge.is_none()
            && operation.backend.profile.upload_cache
            && operation.backend.profile.cdc_enabled
            && (operation.part || operation.backend.profile.write_processing == "inline")
        {
            return Box::pin(self.chunks(operation, body, check, trailers, length)).await;
        }
        let mut file = files.raw_request(&operation.backend.profile).await?;
        let idle = Duration::from_secs(crate::config::seconds(
            &node
                .background_snapshot()?
                .runtime()
                .uploads
                .client_idle_timeout,
        )?);
        let mut hash = (!operation.backend.profile.cdc_enabled
            && operation.backend.profile.deduplication_enabled
            && operation.backend.profile.write_processing == "inline")
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
                size <= operation.limit && length.is_none_or(|length| size <= length),
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
        let charge = if length.is_some() {
            charge
        } else {
            files
                .claim_upload(node, &operation.backend.profile, size)
                .await?
        };
        if operation.backend.profile.cdc_enabled
            && (operation.part || operation.backend.profile.write_processing == "inline")
            && charge.is_none()
        {
            let replay = async_stream::try_stream! {
                let file = file;
                let mut stream = file.replay();
                while let Some(bytes)=stream.next().await { yield bytes.map_err(std::io::Error::other)?; }
            };
            let body = crate::http::object::blob(Box::pin(replay));
            let check = Integrity::new(&hyper::HeaderMap::new(), None)?;
            let (written, digest, _) =
                Box::pin(self.chunks(operation, body, check, None, Some(size))).await?;
            ensure!(
                written == size && digest == etag,
                codec::IntegrityError::Hash
            );
            return Ok((size, etag, sums));
        }
        // Acquire local IO before reserving shared storage capacity.
        let io = Arc::new(self.reads.reader.acquire(&self.io, 1).await?);
        let started = Instant::now();
        let plan = node
            .client
            .prepare_raw(wire::PrepareRawRequest {
                local: charge.is_some(),
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
        let mut file = Some(file);
        if !plan.committed {
            if let Some(charge) = charge {
                files
                    .publish_upload(
                        file.take().unwrap(),
                        source,
                        charge,
                        (
                            operation.request.memory.clone(),
                            io.clone(),
                            operation.grants.clone(),
                        ),
                    )
                    .await?;
            } else {
                ensure!(
                    operation.backend.profile.upload_concurrency.is_none()
                        || grants
                            .iter()
                            .any(|grant| grant.reference.r#type
                                == wire::GrantType::StorageWrite as i32),
                    "raw upload permit missing"
                );
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
                                file.as_ref()
                                    .unwrap()
                                    .replay()
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
            }
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
        if let Some(file) = file {
            files.fill_request(
                file,
                source,
                &operation.backend.profile,
                operation.request.run.clone(),
                epoch,
            );
        }
        Ok((size, etag, sums))
    }

    async fn payload(
        &self,
        operation: &Operation,
        raw: Vec<u8>,
        mut config: crate::content::compression::Config,
    ) -> Result<Payload> {
        if operation.packing {
            config.enabled = false;
        }
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
                .background_snapshot()?
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
                size <= operation.limit && length.is_none_or(|length| size <= length),
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
            let acquire = self.reads.reader.acquire(&self.io, self.parallelism);
            tokio::pin!(acquire);
            #[cfg(feature = "fault-injection")]
            let permit = tokio::select! {
                biased;
                value=&mut acquire=>value?,
                _=crate::faults::point("node-chunks-waiting")=>acquire.await?,
            };
            #[cfg(not(feature = "fault-injection"))]
            let permit = acquire.await?;
            let io = Arc::new(permit);
            #[cfg(feature = "fault-injection")]
            crate::faults::point("node-chunks-before-prepare").await;
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
            if let Some(json) = reply.packing_json.take() {
                ensure!(
                    operation.packing && !reply.committed,
                    "unexpected packed write plan"
                );
                let packing: crate::model::node_pack::Batch = serde_json::from_slice(&json)?;
                let permits: Vec<_> = grants
                    .iter()
                    .filter(|grant| grant.reference.r#type == wire::GrantType::StorageWrite as i32)
                    .collect();
                ensure!(
                    (operation.backend.profile.upload_concurrency.is_none() || !permits.is_empty())
                        && permits.iter().all(
                            |grant| grant.storage == Some(operation.backend.profile.storage_id)
                        ),
                    "storage write grants missing"
                );
                let mut raw = std::collections::BTreeMap::new();
                let mut next = offset;
                for (entry, payload) in reply.chunks.into_iter().zip(batch.items) {
                    let chunk: codec::Chunk = serde_json::from_slice(&entry.chunk_json)?;
                    ensure!(
                        entry.offset == next
                            && chunk.hash == payload.input.hash
                            && chunk.raw_size as u32 == payload.input.raw_size,
                        "chunk plan does not match input"
                    );
                    next += u64::from(payload.input.raw_size);
                    if !entry.reused {
                        ensure!(
                            !payload.input.compressed
                                && entry.payload_hash == payload.input.payload_hash,
                            "packed input differs"
                        );
                        raw.entry(chunk.id)
                            .or_insert((payload.input.hash, Bytes::from(payload.bytes)));
                    }
                }
                let backend = operation.backend.clone();
                let (packed, fills) = operation
                    .guard(
                        node,
                        &grants,
                        Box::pin(self.reads.reader.pack_buffers(backend, keys, packing, raw)),
                    )
                    .await?;
                #[cfg(feature = "fault-injection")]
                crate::faults::point("node-write-pack-uploaded").await;
                let committed = node
                    .client
                    .commit_chunks(wire::CommitChunksRequest {
                        run: Some(operation.request.run.run.clone()),
                        operation_id: operation.id.clone(),
                        batch_id,
                        chunks: vec![],
                        packed_json: Some(serde_json::to_vec(&packed)?),
                    })
                    .await?;
                ensure!(
                    committed.batch_id == batch_id && committed.end == end,
                    "invalid packed commit receipt"
                );
                crate::storage::cache::publish_fills(fills);
                offset = end;
                batch_id += 1;
                continue;
            }

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
                                let files = self.reads.reader.files.clone();
                                let profile = backend.profile.clone();
                                let priority = crate::storage::resource_gate::PRIORITY
                                    .try_with(|priority| *priority)
                                    .unwrap_or(crate::storage::resource_gate::FOREGROUND);
                                let (body, digest, fill) =
                                    tokio::task::spawn_blocking(move || -> Result<_> {
                                        let _cpu = cpu;
                                        #[cfg(feature = "fault-injection")]
                                        crate::faults::blocking("node-write-seal");
                                        ensure!(
                                            blake3::hash(&bytes).as_bytes().as_slice() == hash,
                                            "write payload changed after preparation"
                                        );
                                        let fill = crate::storage::resource_gate::PRIORITY
                                            .sync_scope(priority, || {
                                                files.prepare_fill(
                                                    chunk.location_id,
                                                    &bytes,
                                                    chunk.compressed,
                                                    &profile,
                                                )
                                            });
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
                                            fill,
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
                                Ok::<_, anyhow::Error>((
                                    wire::EncodedChunk {
                                        chunk_id: id.to_string(),
                                        digest,
                                    },
                                    fill,
                                ))
                            }
                        },
                    ))
                    .buffer_unordered(parallelism)
                    .try_collect::<Vec<_>>()
                    .await
                })
                .await?;
            let (encoded, fills): (Vec<_>, Vec<_>) = encoded.into_iter().unzip();
            operation.check()?;
            for grant in &grants {
                grant.check()?;
            }
            if !reply.committed {
                let committed = node
                    .client
                    .commit_chunks(wire::CommitChunksRequest {
                        packed_json: None,
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
            crate::storage::cache::publish_fills(fills.into_iter().flatten().collect());
            offset = end;
            batch_id += 1;
        }
        Ok(())
    }
}
