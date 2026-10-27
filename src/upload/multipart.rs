use crate::model::Id;
use crate::{
    app::{Active, App, Extent, StoredStream},
    authorization::{Action, Permit, Principal},
    content::codec::{self, Chunk, MAX},
    http::gateway::{canned, checksums, metadata, reject_features, stamp},
    upload::{Integrity, checksum},
};
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use md5::{Digest, Md5};
use s3s::{S3Request, S3Response, dto::*, s3_error};
use serde_json::{Value, json};
use std::sync::Arc;
use tracing::Instrument;
use uuid::Uuid;

pub use crate::model::multipart::Upload;
pub(crate) use crate::model::multipart::WebUpload;
use crate::model::multipart::{MultipartCreate, PartStitch, SeedConsumption};

pub(crate) fn checksum_type(
    algorithm: Option<&str>,
    requested: Option<&str>,
) -> Result<Option<String>> {
    let checksum_type = algorithm.map(|algorithm| {
        requested
            .unwrap_or(if algorithm == "CRC64NVME" {
                "FULL_OBJECT"
            } else {
                "COMPOSITE"
            })
            .to_owned()
    });
    if let Some(algorithm) = algorithm {
        Integrity::new(&hyper::HeaderMap::new(), Some(algorithm))?;
        if !matches!(checksum_type.as_deref(), Some("FULL_OBJECT" | "COMPOSITE"))
            || (checksum_type.as_deref() == Some("FULL_OBJECT")
                && !matches!(algorithm, "CRC32" | "CRC32C" | "CRC64NVME"))
            || (algorithm == "CRC64NVME" && checksum_type.as_deref() != Some("FULL_OBJECT"))
        {
            return Err(s3_error!(InvalidRequest, "invalid multipart checksum type").into());
        }
    } else if requested.is_some() {
        return Err(s3_error!(InvalidRequest).into());
    }
    Ok(checksum_type)
}

pub struct Seed {
    pub upload: Uuid,
    pub spans: Vec<(Id, i64, usize)>,
    pub bytes: Vec<u8>,
    pub pins: Vec<Active>,
}
impl Seed {
    pub fn empty(upload: Uuid) -> Self {
        Self {
            upload,
            spans: Vec::new(),
            bytes: Vec::new(),
            pins: Vec::new(),
        }
    }
}

impl App {
    async fn multipart_authority(
        &self,
        access: Option<&str>,
        principal: Option<&Principal>,
        bucket: Uuid,
        action: Action,
    ) -> Result<(String, Permit)> {
        if let Some(principal) = principal {
            let user = principal.user_id().ok_or_else(|| s3_error!(AccessDenied))?;
            Ok((
                format!("web:{user}"),
                principal
                    .require(self.authorization.as_ref(), bucket, action)
                    .await?,
            ))
        } else {
            let access = access.ok_or_else(|| s3_error!(AccessDenied))?;
            Ok((
                access.to_owned(),
                self.authorize(Some(access), bucket, action).await?,
            ))
        }
    }
    pub fn operation_lock(&self, id: Uuid) -> Arc<tokio::sync::Mutex<()>> {
        self.operation_locks.get(id)
    }
    pub async fn get_upload(
        &self,
        id: &str,
        bucket: Uuid,
        key: &str,
        access: &str,
    ) -> Result<Upload> {
        let id = Uuid::parse_str(id).map_err(|_| s3_error!(NoSuchUpload))?;
        self.multipart.upload(id, bucket, key, access).await
    }
    pub async fn create_multipart(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
        principal: Option<&Principal>,
        web: Option<WebUpload>,
    ) -> Result<S3Response<CreateMultipartUploadOutput>> {
        reject_features(&req.headers)?;
        let i = req.input;
        let bucket = self.bucket(&i.bucket, true).await?;
        let (key, mut authority) = self
            .multipart_authority(
                req.credentials.as_ref().map(|c| c.access_key.as_str()),
                principal,
                bucket.id,
                Action::Write,
            )
            .await?;
        ensure!(
            web.is_some() == principal.is_some(),
            "missing web upload context"
        );
        if canned(i.acl.as_ref())? {
            authority.add(bucket.id, Action::Acl);
        }
        if i.key.is_empty() || i.key.len() > 1024 {
            return Err(s3_error!(InvalidArgument).into());
        }
        let algorithm = i.checksum_algorithm.as_ref().map(|a| a.as_str().to_owned());
        let checksum_type = checksum_type(
            algorithm.as_deref(),
            i.checksum_type.as_ref().map(|value| value.as_str()),
        )?;
        let _coord = self.coord.lock().await;
        self.writable()?;
        let (id, existing) = self
            .multipart
            .create(MultipartCreate {
                authority: &authority,
                bucket: bucket.id,
                key: &i.key,
                access: &key,
                metadata: serde_json::to_value(metadata!(i))?,
                public: canned(i.acl.as_ref())?,
                algorithm,
                checksum_type: checksum_type.clone(),
                web,
                max_active: self.config.uploads.max_active_uploads,
            })
            .await?;
        if existing {
            return Ok(S3Response::new(CreateMultipartUploadOutput {
                bucket: Some(i.bucket),
                key: Some(i.key),
                upload_id: Some(id.to_string()),
                ..Default::default()
            }));
        }
        Ok(S3Response::new(CreateMultipartUploadOutput {
            bucket: Some(i.bucket),
            key: Some(i.key),
            upload_id: Some(id.to_string()),
            checksum_algorithm: i.checksum_algorithm,
            checksum_type: checksum_type.map(ChecksumType::from),
            ..Default::default()
        }))
    }
    pub async fn upload_part(
        &self,
        mut req: S3Request<UploadPartInput>,
        principal: Option<&Principal>,
    ) -> Result<S3Response<UploadPartOutput>> {
        reject_features(&req.headers)?;
        let _permits = self.admit(true).await?;
        let i = &req.input;
        if !(1..=10000).contains(&i.part_number) {
            return Err(s3_error!(InvalidArgument).into());
        }
        let b = self.bucket(&i.bucket, true).await?;
        let (access, authority) = self
            .multipart_authority(
                req.credentials.as_ref().map(|c| c.access_key.as_str()),
                principal,
                b.id,
                Action::Write,
            )
            .await?;
        let u = self.get_upload(&i.upload_id, b.id, &i.key, &access).await?;
        ensure!(u.state == "active", s3_error!(NoSuchUpload));
        let _upin = self.pin_upload(u.id);
        let storage = b.storage_id.context("bucket has no storage")?;
        let remote = self.remote_multipart(u.id, storage).await?;
        if i.checksum_algorithm.as_ref().is_some_and(|a| {
            u.checksum_algorithm
                .as_deref()
                .is_some_and(|b| a.as_str() != b)
        }) {
            return Err(s3_error!(
                InvalidRequest,
                "part checksum algorithm differs from upload"
            )
            .into());
        }
        let (id, _pin) = self
            .new_stream(
                &authority,
                b.id,
                &i.key,
                "part",
                u.metadata.clone(),
                false,
                false,
                None,
                None,
            )
            .await?;
        {
            let lock = self.operation_lock(u.id);
            let _guard = lock.lock().await;
            self.multipart
                .begin_part(&authority, u.id, i.part_number, id)
                .await?;
        }
        let body = req
            .input
            .body
            .take()
            .unwrap_or_else(|| StreamingBlob::from_bytes(bytes::Bytes::new()));
        let i = &req.input;
        let (size, etag, sums, _) = self
            .receive(
                id,
                b.storage_id.context("bucket has no storage")?,
                body,
                &req.headers,
                req.trailing_headers,
                i.content_length,
                u.checksum_algorithm
                    .as_deref()
                    .or(i.checksum_algorithm.as_ref().map(|a| a.as_str())),
                Some((u.id, i.part_number)),
                remote.as_ref().map(|upload| upload.profile),
                None,
            )
            .await?;
        let charge = if remote.is_some() {
            self.storage.remote_charge(storage, size as u64)?
        } else {
            None
        };
        let cached = charge.is_some();
        {
            let lock = self.operation_lock(u.id);
            let _guard = lock.lock().await;
            let _coord = self.coord.lock().await;
            self.writable()?;
            if remote.is_some() {
                self.cluster()?
                    .commit_remote_part(crate::model::multipart::RemoteCommit {
                        authority: &authority,
                        bucket: b.id,
                        upload: u.id,
                        number: i.part_number,
                        stream: id,
                        cached,
                        node: crate::worker::raw::processor(self),
                    })
                    .await?;
            } else {
                self.multipart
                    .commit_part(&authority, b.id, u.id, i.part_number, id)
                    .await?;
            }
            if let Some(charge) = charge {
                self.storage.hold_remote_charge(id, charge);
            }
        }
        drop(_permits);
        if remote.is_some() {
            self.wake_raw_uploads.notify_one();
            if !cached {
                self.wait_remote_part(u.id, id).await?;
            }
        }
        self.seal_uploads(id, false).await?;
        // A late predecessor can now transfer its tail using an already durable successor.
        if remote.is_none()
            && self
                .storage
                .raw_policy(b.storage_id.context("bucket has no storage")?)?
                .is_none()
            && let Err(e) = self.stitch_pair(u.id, i.part_number).await
        {
            tracing::warn!(error=%e,"multipart boundary remains recoverable for retry");
        }
        self.wake_gc.notify_one();
        let mut out = UploadPartOutput {
            e_tag: Some(ETag::Strong(etag)),
            ..Default::default()
        };
        checksums!(out, &sums);
        Ok(S3Response::new(out))
    }
    pub async fn multipart_seed(&self, upload: Uuid, number: i32) -> Result<Seed> {
        let lock = self.operation_lock(upload);
        let _guard = lock.lock().await;
        let mut seed = Seed::empty(upload);
        let mut number = number - 1;
        let active: bool = self.multipart.active(upload).await?;
        if !active {
            return Err(s3_error!(NoSuchUpload).into());
        }
        while number > 0 && seed.bytes.len() < MAX - 1 {
            let s: Option<StoredStream> = self.multipart.part(upload, number).await?;
            let Some(s) = s else { break };
            let tail: Option<Extent> = self.multipart.tail(s.id).await?;
            let Some(tail) = tail
                .filter(|e| e.fragment_id.is_some() && e.offset_bytes + e.length as i64 == s.size)
            else {
                break;
            };
            let pin = self.pin(s.id);
            let data = self.extent_bytes(&tail).await?;
            let n = data.len().min(MAX - 1 - seed.bytes.len());
            let mut bytes = data[data.len() - n..].to_vec();
            bytes.extend_from_slice(&seed.bytes);
            seed.bytes = bytes;
            seed.spans.insert(0, (s.id, s.size - n as i64, n));
            seed.pins.push(pin);
            if n != s.size as usize {
                break;
            }
            number -= 1;
        }
        Ok(seed)
    }
    pub async fn consume_seed(
        &self,
        stream: Id,
        offset: i64,
        c: &Chunk,
        seed: &mut Seed,
        consumed: usize,
    ) -> Result<usize> {
        let prefix = seed.spans.iter().map(|s| s.2).sum::<usize>().min(consumed);
        if prefix == 0 {
            return Ok(consumed);
        }
        #[cfg(feature = "fault-injection")]
        crate::faults::point("multipart-before-seed").await;
        let lock = self.operation_lock(seed.upload);
        let _guard = lock.lock().await;
        let _coord = self.coord.lock().await;
        self.multipart
            .consume_seed(SeedConsumption {
                upload: seed.upload,
                stream,
                offset,
                chunk: c.id,
                prefix,
                consumed,
                spans: &mut seed.spans,
            })
            .await?;
        self.wake_gc.notify_one();
        Ok(consumed - prefix)
    }
    pub async fn stitch_pair(&self, upload: Uuid, number: i32) -> Result<()> {
        let lock = self.operation_lock(upload);
        let _guard = lock.lock().await;
        let Some((metadata, rows)) = self.multipart.pair(upload, number).await? else {
            return Ok(());
        };
        if rows.len() != 2 || rows.iter().any(|row| row.payload_type == "raw") {
            return Ok(());
        }
        let left = &rows[0];
        let right = &rows[1];
        let _lpin = self.pin(left.id);
        let _rpin = self.pin(right.id);
        let tail: Option<Extent> = self.multipart.tail(left.id).await?;
        let Some(tail) = tail.filter(|e| e.fragment_id.is_some()) else {
            return Ok(());
        };
        if tail.length as i64 + right.size < MAX as i64 {
            return Ok(());
        }
        let (work, _pin) = self
            .new_stream(
                &Permit::system(),
                left.bucket_id,
                &left.object_key,
                "part",
                metadata,
                false,
                false,
                None,
                None,
            )
            .await?;
        let context = crate::storage::ReadContext::default();
        let data = self.extent_bytes_with(&tail, &context).await?;
        let mut used = 0;
        let mut window = Vec::with_capacity(MAX);
        while used < data.len() && data.len() - used + right.size as usize >= MAX {
            window.clear();
            window.extend_from_slice(&data[used..]);
            let mut pos = 0;
            while window.len() < MAX {
                let rows = self.chunks.extents(right.id, pos, right.size).await?;
                ensure!(!rows.is_empty(), "part mapping incomplete");
                for row in rows {
                    ensure!(row.offset_bytes == pos, "part mapping has gap");
                    let bytes = self.extent_bytes_with(&row, &context).await?;
                    let n = (MAX - window.len()).min(bytes.len());
                    window.extend_from_slice(&bytes[..n]);
                    pos += row.length as i64;
                    if window.len() == MAX {
                        break;
                    }
                }
            }
            let n = codec::cut(&window);
            let chunk = self
                .put_chunk(work, used as i64, window[..n].to_vec(), false)
                .await?;
            let l = (data.len() - used).min(n);
            let r = n - l;
            let _coord = self.coord.lock().await;
            self.multipart
                .stitch(PartStitch {
                    left: left.id,
                    offset: tail.offset_bytes + used as i64,
                    left_length: l,
                    right: right.id,
                    right_length: r,
                    chunk: chunk.id,
                })
                .await?;
            used += l;
        }
        self.multipart.retire_work(work).await?;
        self.wake_gc.notify_one();
        Ok(())
    }
    pub async fn complete_multipart(
        self: &Arc<Self>,
        req: S3Request<CompleteMultipartUploadInput>,
        principal: Option<&Principal>,
    ) -> Result<S3Response<CompleteMultipartUploadOutput>> {
        reject_features(&req.headers)?;
        self.writable()?;
        let i = &req.input;
        let b = self.bucket(&i.bucket, true).await?;
        let (access, authority) = self
            .multipart_authority(
                req.credentials.as_ref().map(|c| c.access_key.as_str()),
                principal,
                b.id,
                Action::Write,
            )
            .await?;
        let u = self.get_upload(&i.upload_id, b.id, &i.key, &access).await?;
        let lock = self.operation_lock(u.id);
        let guard = lock.lock_owned().await;
        let u = self.get_upload(&i.upload_id, b.id, &i.key, &access).await?;
        let specification = manifest(i, &req.headers)?;
        let manifest = &specification.parts;
        let hash = specification.hash()?;
        if u.state == "completed" {
            if u.manifest_hash.as_deref() != Some(&hash) {
                return Err(s3_error!(
                    InvalidRequest,
                    "upload already completed with another manifest"
                )
                .into());
            }
            return Ok(S3Response::new(completion_output(
                &i.bucket,
                &i.key,
                u.result.as_ref().context("missing completion result")?,
            )));
        }
        if u.state == "completing" && u.manifest_hash.as_deref() != Some(&hash) {
            return Err(s3_error!(InvalidRequest, "upload has another completion manifest").into());
        }
        if !matches!(u.state.as_str(), "active" | "completing") {
            return Err(s3_error!(OperationAborted).into());
        }
        if i.checksum_type
            .as_ref()
            .is_some_and(|t| Some(t.as_str()) != u.checksum_type.as_deref())
        {
            return Err(s3_error!(BadDigest).into());
        }
        let mut selected = Vec::with_capacity(manifest.len());
        let mut parts = Vec::with_capacity(manifest.len());
        let mut pins = Vec::with_capacity(manifest.len());
        #[cfg(feature = "fault-injection")]
        crate::faults::point("multipart-before-selection").await;
        for part in manifest {
            let Some(s) = self.multipart.part(u.id, part.number).await? else {
                let current = self.get_upload(&i.upload_id, b.id, &i.key, &access).await?;
                if current.state == "completed" && current.manifest_hash.as_deref() == Some(&hash) {
                    return Ok(S3Response::new(completion_output(
                        &i.bucket,
                        &i.key,
                        current
                            .result
                            .as_ref()
                            .context("missing completion result")?,
                    )));
                }
                return Err(s3_error!(InvalidPart).into());
            };
            selected.push((part.number, s.id));
            pins.push(self.pin(s.id));
            parts.push(s);
        }
        let selected_metadata: Vec<_> = manifest
            .iter()
            .zip(&parts)
            .map(|(part, stream)| crate::model::node_complete::Selected {
                number: part.number,
                size: stream.size,
                etag: stream.etag.clone(),
                checksums: stream.checksums.clone(),
            })
            .collect();
        let summary = specification.select(&u, &selected_metadata)?;
        let total = summary.size;
        let etag = summary.etag;
        self.multipart.seal_files(u.id).await?;
        let input_bucket = i.bucket.clone();
        let input_key = i.key.clone();
        let upload_id = i.upload_id.clone();
        let expected_hash = hash.clone();
        let context = req
            .extensions
            .get::<crate::status::stats::RequestContext>()
            .cloned();
        let token = Uuid::new_v4();
        let permit = self.completions.clone().try_acquire_owned();
        let work = if let Ok(permit) = permit {
            let claimed = self
                .multipart
                .begin_complete(crate::model::multipart::Completion {
                    authority: &authority,
                    upload: u.id,
                    hash: &hash,
                    token,
                    parts: &selected,
                    node: self
                        .node
                        .get()
                        .map(|node| (node, self.role.as_str(), &self.secrets.credential_key)),
                })
                .await?;
            drop(guard);
            if claimed {
                let app = self.clone();
                let upin = self.pin_upload(u.id);
                let input = req.input;
                let headers = req.headers;
                let audit = crate::control::audit::current();
                Some(tokio::spawn(
                    async move {
                        let _permit = permit;
                        let _pins = pins;
                        let _upin = upin;
                        let assembly = async {
                            #[cfg(feature = "fault-injection")]
                            crate::faults::point("multipart-completing").await;
                            app.wait_complete_sources(
                                &parts.iter().map(|part| part.id).collect::<Vec<_>>(),
                                None,
                            )
                            .await?;
                            app.assemble(
                                &authority, &u, &parts, &input, &headers, total, &etag, &hash,
                                token,
                            )
                            .await
                        };
                        let processing = async {
                            if let Some(id) = audit {
                                crate::control::audit::scope(id, assembly).await
                            } else {
                                assembly.await
                            }
                        };
                        let renew = async {
                            loop {
                                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                                app.ensure_mode()?;
                                ensure!(
                                    app.multipart
                                        .renew_complete(
                                            u.id,
                                            token,
                                            app.node.get().map(|node| (
                                                node,
                                                app.role.as_str(),
                                                &app.secrets.credential_key
                                            ))
                                        )
                                        .await?,
                                    crate::model::Error::OperationAborted
                                );
                            }
                        };
                        let result =
                            tokio::select! { result=processing=>result, result=renew=>result };
                        if result.is_err() {
                            let _ = app.multipart.retry_complete(u.id, token).await;
                        }
                        app.wake_gc.notify_one();
                        result.map_err(crate::app::internal)
                    }
                    .in_current_span(),
                ))
            } else {
                None
            }
        } else {
            drop(guard);
            if u.state != "completing" {
                return Err(completion_busy().into());
            }
            None
        };
        let app = self.clone();
        let bucket = b.id;
        let future = Box::pin(async move {
            let result = tokio::time::timeout(completion_timeout(), async move {
                if let Some(work) = work {
                    return work
                        .await
                        .map_err(|error| crate::app::internal(error.into()))?;
                }
                loop {
                    let current = app
                        .get_upload(&upload_id, bucket, &input_key, &access)
                        .await
                        .map_err(crate::app::internal)?;
                    if current.manifest_hash.as_deref() != Some(expected_hash.as_str()) {
                        return Err(s3_error!(
                            InvalidRequest,
                            "upload has another completion manifest"
                        ));
                    }
                    if current.state == "completed" {
                        return Ok(completion_output(
                            &input_bucket,
                            &input_key,
                            current
                                .result
                                .as_ref()
                                .ok_or_else(|| s3_error!(InternalError))?,
                        ));
                    }
                    if current.state != "completing" {
                        return Err(completion_retry());
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                }
            })
            .await
            .unwrap_or_else(|_| Err(completion_retry()));
            result.map_err(|mut error| {
                tracing::warn!(code=?error.code(), "multipart completion response failed");
                if let Some(context) = context {
                    context
                        .failed
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                    error.set_request_id(context.id);
                }
                error
            })
        });
        Ok(S3Response::new(CompleteMultipartUploadOutput {
            future: Some(future),
            ..Default::default()
        }))
    }
    async fn wait_complete_sources(&self, streams: &[Id], writing: Option<Id>) -> Result<()> {
        if self.role == "single" {
            return Ok(());
        }
        let cluster = self.cluster()?;
        let mut prioritized = false;
        loop {
            if !cluster.pending_read(streams, Uuid::nil()).await? {
                return Ok(());
            }
            if !prioritized {
                cluster.prioritize_uploads(streams).await?;
                self.wake_tasks.notify_waiters();
                prioritized = true;
            }
            self.ensure_mode()?;
            if let Some(stream) = writing {
                self.renew_write_lease(stream).await?;
            }
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        }
    }
    #[allow(clippy::too_many_arguments)] // Frozen completion inputs are used only by this worker.
    async fn assemble(
        &self,
        authority: &Permit,
        u: &Upload,
        parts: &[StoredStream],
        i: &CompleteMultipartUploadInput,
        headers: &hyper::HeaderMap,
        total: i64,
        etag: &str,
        hash: &str,
        token: Uuid,
    ) -> Result<CompleteMultipartUploadOutput> {
        if self.role != "single" && self.cluster()?.remote_upload(u.id).await?.is_some() {
            return self
                .assemble_remote(authority, u, parts, i, headers, total, etag, hash, token)
                .await;
        }
        let slot = self.slots.clone().acquire_owned().await?;
        let (id, _pin) = self
            .new_stream(
                authority,
                u.bucket_id,
                &u.object_key,
                "object",
                u.metadata.clone(),
                u.public_read,
                true,
                Some(u.id),
                None,
            )
            .await?;
        self.complete_quota(u.id, id, total).await?;
        let storage = self
            .objects
            .storage(u.bucket_id)
            .await?
            .context("bucket has no storage")?;
        let raw_revision = self.storage.raw_policy(storage)?;
        let mut raw_writer = if let Some(revision) = raw_revision {
            let (source, location) = self.raw.begin(id, storage, revision, false).await?;
            Some((
                source,
                self.storage
                    .raw_replay_writer(
                        storage,
                        source,
                        location,
                        total,
                        &parts.iter().map(|part| part.size).collect::<Vec<_>>(),
                    )
                    .await?,
            ))
        } else {
            None
        };
        let mut window = if raw_revision.is_some() {
            Vec::new()
        } else {
            Vec::with_capacity(MAX)
        };
        let mut prepared = if raw_writer
            .as_ref()
            .is_some_and(|(_, writer)| writer.can_assemble())
        {
            if let Some(ranges) = self.local_part_ranges(parts).await? {
                let (source, writer) = raw_writer.as_mut().unwrap();
                writer.prepare_local(ranges, self, u.id, *source).await?
            } else {
                None
            }
        } else {
            None
        };
        let mut offset = 0;
        let mut full = Integrity::new(&hyper::HeaderMap::new(), u.checksum_algorithm.as_deref())?;
        for part in parts {
            let mut part_hash = Md5::new();
            let mut pos = 0;
            let mut input = Box::pin(self.payload_bytes(part));
            loop {
                let data = if let Some(prepared) = prepared.as_mut() {
                    if pos == part.size {
                        None
                    } else {
                        prepared.next().await?
                    }
                } else {
                    input.next().await.transpose()?
                };
                let Some(data) = data else {
                    break;
                };
                ensure!(
                    pos + data.len() as i64 <= part.size,
                    "assembled part exceeds length"
                );
                part_hash.update(&data);
                full.update(&data);
                pos += data.len() as i64;
                if let Some((_, writer)) = raw_writer.as_mut() {
                    self.renew_write_lease(id).await?;
                    writer.write(&data).await?;
                    offset += data.len() as i64;
                    continue;
                }
                let mut rest = data.as_ref();
                while !rest.is_empty() {
                    let n = (MAX - window.len()).min(rest.len());
                    window.extend_from_slice(&rest[..n]);
                    rest = &rest[n..];
                    if window.len() == MAX {
                        let n = codec::cut(&window);
                        self.put_chunk(id, offset, window[..n].to_vec(), false)
                            .await?;
                        offset += n as i64;
                        window.drain(..n);
                    }
                }
            }
            ensure!(
                pos == part.size && hex::encode(part_hash.finalize()) == part.etag,
                "part reconstruction checksum mismatch"
            );
        }
        while !window.is_empty() {
            let n = codec::cut(&window);
            self.put_chunk(id, offset, window[..n].to_vec(), false)
                .await?;
            offset += n as i64;
            window.drain(..n);
        }
        ensure!(offset == total, "assembled size mismatch");
        let (payload_digest, sums) = completion_checksums(full, u, parts, headers)?;
        if let Some((source, writer)) = raw_writer {
            let digest = hex::decode(payload_digest)?;
            let written = writer
                .finish_replay(id, total as u64, &digest, |range| {
                    self.replay_payload(id, parts, range)
                })
                .await?;
            self.finish_raw_received(id, source, storage, total, &digest, written)
                .await?;
        }
        drop(slot);
        self.publish_multipart(id, u, i, total, etag, hash, token, sums)
            .await
    }
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn publish_multipart(
        &self,
        id: Id,
        u: &Upload,
        i: &CompleteMultipartUploadInput,
        total: i64,
        etag: &str,
        hash: &str,
        token: Uuid,
        sums: Value,
    ) -> Result<CompleteMultipartUploadOutput> {
        self.finish_stream(id, total, etag, sums.clone()).await?;
        self.seal_uploads(id, true).await?;
        self.wake_raw_uploads.notify_one();
        self.wait_complete_sources(&[id], Some(id)).await?;
        let result = json!({"etag":etag,"checksums":sums});
        self.publish_complete(
            id,
            (u.bucket_id, &u.object_key),
            i.if_match.as_ref(),
            i.if_none_match.as_ref(),
            Some((u.id, hash.to_owned(), result.clone(), token)),
        )
        .await?;
        Ok(completion_output(&i.bucket, &i.key, &result))
    }
    async fn local_part_ranges(
        &self,
        parts: &[StoredStream],
    ) -> Result<Option<Vec<crate::storage::assembly::Range>>> {
        let mut ranges = Vec::new();
        let mut fragments = std::collections::HashSet::new();
        for part in parts {
            let mut cursor = 0;
            while cursor < part.size {
                let rows = self.multipart.local_ranges(part.id, cursor).await?;
                ensure!(!rows.is_empty(), "multipart part has a missing extent");
                for row in rows {
                    let Some(id) = row.fragment_id else {
                        return Ok(None);
                    };
                    if !fragments.insert(id) {
                        return Ok(None);
                    }
                    if row.source_offset != 0 || row.size != Some(row.length) {
                        return Ok(None);
                    }
                    ensure!(
                        row.offset_bytes == cursor && row.length > 0,
                        "multipart extent is not contiguous"
                    );
                    let offset = row.file_offset.unwrap_or(0);
                    ensure!(
                        offset >= 0 && row.file_id.is_some() == row.file_offset.is_some(),
                        "invalid multipart source offset"
                    );
                    let hash: [u8; 32] = row
                        .hash
                        .context("fragment hash missing")?
                        .try_into()
                        .map_err(|_| anyhow::anyhow!("invalid fragment hash"))?;
                    ranges.push(crate::storage::assembly::Range {
                        path: if let Some(file) = row.file_id {
                            self.storage
                                .multipart_path(file, row.file_storage, row.raw_source)?
                        } else {
                            self.storage.disk.fragment_path(
                                row.fragment_location.context("missing fragment location")?,
                            )
                        },
                        offset: offset as u64,
                        length: row.length as usize,
                        hash,
                        fragment: Some(id),
                        file: row.file_id,
                    });
                    cursor += row.length as i64;
                    // Bound per-request layout memory; larger manifests retain streaming assembly.
                    if ranges.len() > 4096 {
                        return Ok(None);
                    }
                }
            }
            ensure!(cursor == part.size, "multipart extent exceeds part");
        }
        Ok(Some(ranges))
    }
    pub async fn abort_multipart(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
        principal: Option<&Principal>,
    ) -> Result<S3Response<AbortMultipartUploadOutput>> {
        let i = req.input;
        let b = self.bucket(&i.bucket, true).await?;
        let (access, authority) = self
            .multipart_authority(
                req.credentials.as_ref().map(|c| c.access_key.as_str()),
                principal,
                b.id,
                Action::Write,
            )
            .await?;
        let u = self.get_upload(&i.upload_id, b.id, &i.key, &access).await?;
        if i.if_match_initiated_time
            .is_some_and(|t| t != stamp(u.created_at))
        {
            return Err(s3_error!(PreconditionFailed).into());
        }
        let lock = self.operation_lock(u.id);
        let _guard = lock.lock().await;
        let coord = self.coord.lock().await;
        self.abort_upload_locked(u.id, &coord, &authority).await?;
        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
    }
    pub async fn abort_upload(&self, id: Uuid) -> Result<()> {
        let coord = self.coord.lock().await;
        self.abort_upload_locked(id, &coord, &Permit::system())
            .await
    }
    pub(crate) async fn abort_upload_locked(
        &self,
        id: Uuid,
        _coord: &tokio::sync::MutexGuard<'_, ()>,
        authority: &Permit,
    ) -> Result<()> {
        self.multipart.abort(authority, id).await?;
        self.wake_gc.notify_one();
        Ok(())
    }
    pub async fn list_parts(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> Result<S3Response<ListPartsOutput>> {
        let i = req.input;
        let b = self.bucket(&i.bucket, false).await?;
        let access = req
            .credentials
            .as_ref()
            .ok_or_else(|| s3_error!(AccessDenied))?
            .access_key
            .as_str();
        self.authorize(Some(access), b.id, Action::List).await?;
        let u = self.get_upload(&i.upload_id, b.id, &i.key, access).await?;
        if !matches!(u.state.as_str(), "active" | "completing") {
            return Err(s3_error!(NoSuchUpload).into());
        }
        let limit = i.max_parts.unwrap_or(1000);
        let marker = i.part_number_marker.unwrap_or(0);
        if !(0..=1000).contains(&limit) || !(0..=10000).contains(&marker) {
            return Err(s3_error!(InvalidArgument).into());
        }
        let rows: Vec<(i32, i64, String, DateTime<Utc>, Value)> =
            self.multipart.parts(u.id, marker, limit as i64 + 1).await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("list-parts-read").await;
        listed_parts(i, rows, u.checksum_algorithm, u.checksum_type)
    }
    pub async fn list_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> Result<S3Response<ListMultipartUploadsOutput>> {
        let i = req.input;
        let b = self.bucket(&i.bucket, false).await?;
        let access = req
            .credentials
            .as_ref()
            .ok_or_else(|| s3_error!(AccessDenied))?
            .access_key
            .as_str();
        self.authorize(Some(access), b.id, Action::List).await?;
        if i.encoding_type
            .as_ref()
            .is_some_and(|v| v.as_str() != "url")
        {
            return Err(s3_error!(InvalidArgument, "encoding-type must be url").into());
        }
        let limit = i.max_uploads.unwrap_or(1000);
        if !(0..=1000).contains(&limit) {
            return Err(s3_error!(InvalidArgument).into());
        }
        let prefix = i.prefix.as_deref().unwrap_or("");
        let delimiter = i.delimiter.as_deref().unwrap_or("");
        let marker = if i.key_marker.is_some() {
            i.upload_id_marker
                .as_deref()
                .map(Uuid::parse_str)
                .transpose()
                .map_err(|_| s3_error!(InvalidArgument))?
        } else {
            None
        };
        let mut page = super::listing::Page::new(
            prefix.to_owned(),
            delimiter.to_owned(),
            i.key_marker.clone(),
            marker,
            limit as usize,
        )?;
        while let Some((key, inclusive, id)) = page.position() {
            let rows = self
                .multipart
                .uploads(b.id, access, prefix, key, inclusive, id)
                .await?;
            page.append(rows.into_iter().map(Into::into).collect())?;
        }
        super::listing::output(i, page.finish())
    }
}

pub(crate) fn manifest(
    input: &CompleteMultipartUploadInput,
    headers: &hyper::HeaderMap,
) -> Result<crate::model::node_complete::Manifest> {
    use crate::model::node_complete::{Manifest, Part};
    let condition = |value: &Option<ETagCondition>| -> Result<Option<String>> {
        value
            .as_ref()
            .map(|value| Ok(value.to_http_header()?.to_str()?.to_owned()))
            .transpose()
    };
    let parts = input
        .multipart_upload
        .as_ref()
        .and_then(|value| value.parts.as_ref())
        .ok_or_else(|| s3_error!(InvalidPart))?;
    let value = Manifest {
        upload: Uuid::parse_str(&input.upload_id).map_err(|_| s3_error!(NoSuchUpload))?,
        parts: parts
            .iter()
            .map(|part| -> Result<Part> {
                Ok(Part {
                    number: part.part_number.ok_or_else(|| s3_error!(InvalidPart))?,
                    etag: part
                        .e_tag
                        .as_ref()
                        .and_then(ETag::as_strong)
                        .ok_or_else(|| s3_error!(InvalidPart))?
                        .to_owned(),
                    checksums: serde_json::from_value(part_sums(part))?,
                })
            })
            .collect::<Result<_>>()?,
        if_match: condition(&input.if_match)?,
        if_none: condition(&input.if_none_match)?,
        checksum_type: input
            .checksum_type
            .as_ref()
            .map(|value| value.as_str().to_owned()),
        size: input.mpu_object_size,
        checksums: checksum_headers(headers)?,
    };
    value.validate()?;
    Ok(value)
}

fn checksum_headers(
    headers: &hyper::HeaderMap,
) -> Result<std::collections::BTreeMap<String, String>> {
    let mut sums = std::collections::BTreeMap::new();
    for (name, value) in headers {
        if let Some(name) = name.as_str().strip_prefix("x-amz-checksum-")
            && name != "type"
        {
            let value = value.to_str()?;
            if let Some(previous) = sums.insert(name.to_owned(), value.to_owned()) {
                ensure!(previous == value, s3_error!(BadDigest));
            }
        }
    }
    Ok(sums)
}

fn part_sums(p: &CompletedPart) -> Value {
    let mut map = serde_json::Map::new();
    for (n, v) in [
        ("crc32", &p.checksum_crc32),
        ("crc32c", &p.checksum_crc32c),
        ("crc64nvme", &p.checksum_crc64nvme),
        ("sha1", &p.checksum_sha1),
        ("sha256", &p.checksum_sha256),
        ("sha512", &p.checksum_sha512),
        ("md5", &p.checksum_md5),
        ("xxhash64", &p.checksum_xxhash64),
        ("xxhash3", &p.checksum_xxhash3),
        ("xxhash128", &p.checksum_xxhash128),
    ] {
        if let Some(v) = v {
            map.insert(n.into(), json!(v));
        }
    }
    Value::Object(map)
}
pub(crate) fn completion_output(
    bucket: &str,
    key: &str,
    result: &Value,
) -> CompleteMultipartUploadOutput {
    let sums = &result["checksums"];
    let mut out = CompleteMultipartUploadOutput {
        bucket: Some(bucket.into()),
        key: Some(key.into()),
        e_tag: result["etag"].as_str().map(|e| ETag::Strong(e.into())),
        checksum_type: checksum(sums, "type").map(ChecksumType::from),
        ..Default::default()
    };
    checksums!(out, sums);
    out
}

pub(crate) fn completion_timeout() -> std::time::Duration {
    #[cfg(feature = "fault-injection")]
    if let Ok(value) = std::env::var("MOKYU_TEST_COMPLETE_TIMEOUT_SECONDS") {
        if let Ok(seconds) = value.parse::<u64>() {
            return std::time::Duration::from_secs(seconds.clamp(1, 300));
        }
    }
    std::time::Duration::from_secs(300)
}

pub(crate) fn completion_retry() -> s3s::S3Error {
    s3_error!(
        InternalError,
        "Multipart completion is still in progress. Please retry the same request."
    )
}

pub(crate) fn completion_busy() -> s3s::S3Error {
    let mut error = s3_error!(
        SlowDown,
        "multipart completion capacity is full; retry later"
    );
    let mut headers = hyper::HeaderMap::new();
    headers.insert(
        hyper::header::RETRY_AFTER,
        hyper::header::HeaderValue::from_static("60"),
    );
    headers.insert(
        hyper::header::CACHE_CONTROL,
        hyper::header::HeaderValue::from_static("no-store"),
    );
    error.set_headers(headers);
    error
}

pub(super) fn completion_checksums(
    full: Integrity,
    u: &Upload,
    parts: &[StoredStream],
    headers: &hyper::HeaderMap,
) -> Result<(String, Value)> {
    let (payload_digest, sums) = full.finish(None)?;
    let sums = crate::model::node_complete::checksums(
        sums,
        u.checksum_algorithm.as_deref(),
        u.checksum_type.as_deref(),
        parts.iter().map(|part| &part.checksums),
        &checksum_headers(headers)?,
    )?;
    Ok((payload_digest, sums))
}

pub(crate) fn listed_parts(
    i: ListPartsInput,
    rows: Vec<crate::model::multipart::UploadPart>,
    algorithm: Option<String>,
    checksum_type: Option<String>,
) -> Result<S3Response<ListPartsOutput>> {
    let limit = i.max_parts.unwrap_or(1000);
    let marker = i.part_number_marker.unwrap_or(0);
    ensure!(
        (0..=1000).contains(&limit) && (0..=10000).contains(&marker),
        s3_error!(InvalidArgument)
    );
    let truncated = rows.len() > limit as usize;
    let mut parts = Vec::new();
    let mut last = marker;
    for (number, size, etag, touched_at, checksums) in rows.into_iter().take(limit as usize) {
        let mut part = Part {
            part_number: Some(number),
            size: Some(size),
            e_tag: Some(ETag::Strong(etag)),
            last_modified: Some(stamp(touched_at)),
            ..Default::default()
        };
        checksums!(part, &checksums);
        parts.push(part);
        last = number;
    }
    Ok(S3Response::new(ListPartsOutput {
        bucket: Some(i.bucket),
        key: Some(i.key),
        upload_id: Some(i.upload_id),
        max_parts: Some(limit),
        part_number_marker: Some(marker),
        next_part_number_marker: truncated.then_some(last),
        is_truncated: Some(truncated),
        parts: Some(parts),
        checksum_algorithm: algorithm.map(ChecksumAlgorithm::from),
        checksum_type: checksum_type.map(ChecksumType::from),
        ..Default::default()
    }))
}

#[cfg(test)]
mod tests {
    #[test]
    fn completion_rejects_conflicting_checksum_headers() -> anyhow::Result<()> {
        let mut headers = hyper::HeaderMap::new();
        headers.append("x-amz-checksum-sha256", "first".parse()?);
        headers.append("x-amz-checksum-sha256", "first".parse()?);
        assert_eq!(super::checksum_headers(&headers)?["sha256"], "first");
        headers.append("x-amz-checksum-sha256", "different".parse()?);
        assert_eq!(
            super::checksum_headers(&headers)
                .unwrap_err()
                .downcast_ref::<s3s::S3Error>()
                .unwrap()
                .code(),
            &s3s::S3ErrorCode::BadDigest
        );
        Ok(())
    }
}
