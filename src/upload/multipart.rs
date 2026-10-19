use crate::{
    app::{Active, App, Extent, StoredStream},
    authorization::{Action, Permit, Principal},
    content::codec::{self, Chunk, MAX},
    http::gateway::{canned, checksums, metadata, reject_features, stamp},
    upload::{Integrity, checksum},
};
use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use md5::{Digest, Md5};
use s3s::{S3Request, S3Response, dto::*, s3_error};
use serde_json::{Value, json};
use std::sync::{Arc, Weak};
use tracing::Instrument;
use uuid::Uuid;

pub use crate::model::multipart::Upload;
pub(crate) use crate::model::multipart::WebUpload;
use crate::model::multipart::{MultipartCreate, PartStitch, SeedConsumption};

pub struct Seed {
    pub upload: Uuid,
    pub spans: Vec<(Uuid, i64, usize)>,
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
        let mut locks = self.operation_locks.lock().unwrap();
        locks.retain(|_, v| v.strong_count() > 0);
        if let Some(lock) = locks.get(&id).and_then(Weak::upgrade) {
            lock
        } else {
            let lock = Arc::new(tokio::sync::Mutex::new(()));
            locks.insert(id, Arc::downgrade(&lock));
            lock
        }
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
        let checksum_type = if algorithm.is_some() {
            Some(
                i.checksum_type
                    .as_ref()
                    .map(|t| t.as_str())
                    .unwrap_or(if algorithm.as_deref() == Some("CRC64NVME") {
                        "FULL_OBJECT"
                    } else {
                        "COMPOSITE"
                    })
                    .to_owned(),
            )
        } else {
            None
        };
        if let Some(a) = &algorithm {
            Integrity::new(&hyper::HeaderMap::new(), Some(a))?;
            if !matches!(checksum_type.as_deref(), Some("FULL_OBJECT" | "COMPOSITE"))
                || (checksum_type.as_deref() == Some("FULL_OBJECT")
                    && !matches!(a.as_str(), "CRC32" | "CRC32C" | "CRC64NVME"))
                || (a == "CRC64NVME" && checksum_type.as_deref() != Some("FULL_OBJECT"))
            {
                return Err(s3_error!(InvalidRequest, "invalid multipart checksum type").into());
            }
        }
        if algorithm.is_none() && i.checksum_type.is_some() {
            return Err(s3_error!(InvalidRequest).into());
        }
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
        let _upin = self.pin(u.id);
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
        let (_, etag, sums) = self
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
            )
            .await?;
        {
            let lock = self.operation_lock(u.id);
            let _guard = lock.lock().await;
            let _coord = self.coord.lock().await;
            self.writable()?;
            self.multipart
                .commit_part(&authority, b.id, u.id, i.part_number, id)
                .await?;
        }
        self.seal_uploads(id, false).await?;
        // A late predecessor can now transfer its tail using an already durable successor.
        if self
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
        stream: Uuid,
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
        let permits = self.admit(true).await?;
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
        let manifest = i
            .multipart_upload
            .as_ref()
            .and_then(|m| m.parts.as_ref())
            .ok_or_else(|| s3_error!(InvalidPart))?;
        if manifest.is_empty() || manifest.len() > 10000 {
            return Err(s3_error!(InvalidPart).into());
        }
        let mut digest = blake3::Hasher::new();
        let mut previous = 0;
        for part in manifest {
            let n = part.part_number.ok_or_else(|| s3_error!(InvalidPart))?;
            if n <= previous
                || n > 10000
                || (u.checksum_type.as_deref() == Some("COMPOSITE") && n != previous + 1)
            {
                return Err(s3_error!(InvalidPartOrder).into());
            }
            previous = n;
            digest.update(&n.to_be_bytes());
            digest.update(
                part.e_tag
                    .as_ref()
                    .and_then(ETag::as_strong)
                    .ok_or_else(|| s3_error!(InvalidPart))?
                    .as_bytes(),
            );
        }
        // Include checksum fields and conditional headers in retry identity without storing credentials.
        for part in manifest {
            let values = part_sums(part);
            digest.update(serde_json::to_string(&values)?.as_bytes());
        }
        digest.update(
            format!(
                "{:?}|{:?}|{:?}|{:?}",
                i.if_match, i.if_none_match, i.checksum_type, i.mpu_object_size
            )
            .as_bytes(),
        );
        for (name, value) in &req.headers {
            if name.as_str().starts_with("x-amz-checksum-") {
                digest.update(name.as_str().as_bytes());
                digest.update(value.as_bytes());
            }
        }
        let hash = digest.finalize().to_hex().to_string();
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
        if u.state != "active" {
            return Err(s3_error!(OperationAborted).into());
        }
        if i.checksum_type
            .as_ref()
            .is_some_and(|t| Some(t.as_str()) != u.checksum_type.as_deref())
        {
            return Err(s3_error!(BadDigest).into());
        }
        let mut parts = Vec::with_capacity(manifest.len());
        let mut pins = Vec::with_capacity(manifest.len());
        let mut total = 0i64;
        let mut etag_hash = Md5::new();
        for (idx, part) in manifest.iter().enumerate() {
            let s: StoredStream = self
                .multipart
                .part(
                    u.id,
                    part.part_number.ok_or_else(|| s3_error!(InvalidPart))?,
                )
                .await?
                .ok_or_else(|| s3_error!(InvalidPart))?;
            if !part
                .e_tag
                .as_ref()
                .is_some_and(|e| e.strong_cmp(&ETag::Strong(s.etag.clone())))
            {
                return Err(s3_error!(InvalidPart).into());
            }
            if idx + 1 < manifest.len() && s.size < 5 * 1024 * 1024 {
                return Err(s3_error!(EntityTooSmall).into());
            }
            let supplied = part_sums(part);
            for (name, value) in supplied.as_object().unwrap() {
                if s.checksums.get(name) != Some(value) {
                    return Err(s3_error!(BadDigest).into());
                }
            }
            if let Some(alg) = &u.checksum_algorithm {
                let name = alg.to_ascii_lowercase();
                if supplied.get(&name).is_none() {
                    return Err(s3_error!(InvalidPart, "part checksum required by upload").into());
                }
            }
            total = total
                .checked_add(s.size)
                .context("multipart length overflow")?;
            etag_hash.update(hex::decode(&s.etag)?);
            pins.push(self.pin(s.id));
            parts.push(s);
        }
        if i.mpu_object_size.is_some_and(|n| n != total) {
            return Err(s3_error!(InvalidRequest, "multipart size mismatch").into());
        }
        let etag = format!("{}-{}", hex::encode(etag_hash.finalize()), parts.len());
        if self.role == "single" {
            self.multipart.seal_files(u.id).await?;
        }
        let _coord = self.coord.lock().await;
        self.multipart
            .begin_complete(&authority, u.id, &hash)
            .await?;
        drop(_coord);
        let app = self.clone();
        let upin = self.pin(u.id);
        let context = req
            .extensions
            .get::<crate::status::stats::RequestContext>()
            .cloned();
        let input = req.input;
        let headers = req.headers;
        let audit = crate::control::audit::current();
        let work = tokio::spawn(
            async move {
                let _guard = guard;
                let _pins = pins;
                let _upin = upin;
                let _permits = permits;
                #[cfg(feature = "fault-injection")]
                crate::faults::point("multipart-completing").await;
                let assembly = app.assemble(
                    &authority, &u, &parts, &input, &headers, total, &etag, &hash,
                );
                let result = if let Some(id) = audit {
                    crate::control::audit::scope(id, assembly).await
                } else {
                    assembly.await
                };
                if result.is_err() {
                    let _ = app.multipart.retry_complete(u.id).await;
                }
                drop(_pins);
                drop(_upin);
                drop(_guard);
                app.wake_gc.notify_one();
                result.map_err(crate::app::internal)
            }
            .in_current_span(),
        );
        let future = Box::pin(async move {
            let result = work
                .await
                .map_err(|e| crate::app::internal(e.into()))
                .and_then(|r| r);
            result.map_err(|mut error| {
                tracing::warn!(code=?error.code(), "multipart completion failed");
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
    ) -> Result<CompleteMultipartUploadOutput> {
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
            let source = self.raw.begin(id, storage, revision).await?;
            Some((
                source,
                self.storage
                    .raw_replay_writer(
                        storage,
                        source,
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
        let mut prepared = if raw_revision.is_some() && self.storage.is_local(storage)? {
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
        let (payload_digest, mut sums) = full.finish(None)?;
        if let Some(alg) = &u.checksum_algorithm {
            let name = alg.to_ascii_lowercase();
            if u.checksum_type.as_deref() == Some("COMPOSITE") {
                let mut check = Integrity::new(&hyper::HeaderMap::new(), Some(alg))?;
                for part in parts {
                    check.update(&STANDARD.decode(
                        checksum(&part.checksums, &name).context("part checksum missing")?,
                    )?);
                }
                let (_, composite) = check.finish(None)?;
                sums[&name] = json!(format!(
                    "{}-{}",
                    checksum(&composite, &name).context("composite missing")?,
                    parts.len()
                ));
            }
            sums["type"] = json!(u.checksum_type);
        }
        for (name, value) in headers {
            if let Some(name) = name.as_str().strip_prefix("x-amz-checksum-")
                && name != "type"
                && sums.get(name).and_then(Value::as_str) != Some(value.to_str()?)
            {
                return Err(s3_error!(BadDigest).into());
            }
        }
        if let Some((source, writer)) = raw_writer {
            let content_hash = writer
                .finish_replay(total as u64, |range| self.replay_payload(id, parts, range))
                .await?;
            self.raw
                .finish(id, source, total, &hex::decode(payload_digest)?)
                .await?;
            self.deduplicate_raw(id, storage, source, content_hash)
                .await?;
        }
        self.finish_stream(id, total, etag, sums.clone()).await?;
        let result = json!({"etag":etag,"checksums":sums});
        self.publish_complete(
            id,
            i.if_match.as_ref(),
            i.if_none_match.as_ref(),
            Some((u.id, hash.to_owned(), result.clone())),
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
                        path: self.storage.multipart_path(
                            row.file_id.unwrap_or(id),
                            row.file_storage,
                            row.raw_source,
                        )?,
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
            checksum_algorithm: u.checksum_algorithm.map(ChecksumAlgorithm::from),
            checksum_type: u.checksum_type.map(ChecksumType::from),
            ..Default::default()
        }))
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
        let mut cursor = i.key_marker.clone().unwrap_or_else(|| prefix.to_string());
        let mut inclusive = i.key_marker.is_none();
        let mut cursor_id = if i.key_marker.is_some() {
            i.upload_id_marker
                .as_deref()
                .map(Uuid::parse_str)
                .transpose()
                .map_err(|_| s3_error!(InvalidArgument))?
        } else {
            None
        };
        let mut uploads = Vec::new();
        let mut prefixes = Vec::new();
        let mut next_key = None;
        let mut next_id = None;
        let mut truncated = false;
        let encode = |s: String| {
            if i.encoding_type.is_some() {
                crate::app::listing::encode_key(&s)
            } else {
                s
            }
        };
        if limit > 0 {
            'pages: loop {
                let rows: Vec<Upload> = self
                    .multipart
                    .uploads(b.id, access, prefix, &cursor, inclusive, cursor_id)
                    .await?;
                if rows.is_empty() {
                    break;
                }
                for u in rows {
                    if u.object_key < cursor
                        || (u.object_key == cursor
                            && !inclusive
                            && cursor_id.is_none_or(|id| u.id <= id))
                    {
                        continue;
                    }
                    let group = if delimiter.is_empty() {
                        None
                    } else {
                        u.object_key[prefix.len()..]
                            .find(delimiter)
                            .map(|n| u.object_key[..prefix.len() + n + delimiter.len()].to_string())
                    };
                    if let Some(group) = group {
                        let after = crate::app::listing::successor(&group);
                        cursor = after.clone().unwrap_or_default();
                        inclusive = true;
                        cursor_id = None;
                        if i.key_marker.as_ref().is_none_or(|m| group > *m) {
                            if uploads.len() + prefixes.len() == limit as usize {
                                truncated = true;
                                break 'pages;
                            }
                            next_key = Some(encode(group.clone()));
                            next_id = None;
                            prefixes.push(CommonPrefix {
                                prefix: Some(encode(group)),
                            });
                        }
                        if after.is_none() {
                            break 'pages;
                        }
                    } else {
                        if uploads.len() + prefixes.len() == limit as usize {
                            truncated = true;
                            break 'pages;
                        }
                        cursor = u.object_key.clone();
                        inclusive = false;
                        cursor_id = Some(u.id);
                        next_key = Some(encode(u.object_key.clone()));
                        next_id = Some(u.id.to_string());
                        uploads.push(MultipartUpload {
                            key: Some(encode(u.object_key)),
                            upload_id: Some(u.id.to_string()),
                            initiated: Some(stamp(u.created_at)),
                            storage_class: Some(StorageClass::from_static("STANDARD")),
                            checksum_algorithm: u.checksum_algorithm.map(ChecksumAlgorithm::from),
                            checksum_type: u.checksum_type.map(ChecksumType::from),
                            ..Default::default()
                        });
                    }
                }
            }
        }
        Ok(S3Response::new(ListMultipartUploadsOutput {
            bucket: Some(i.bucket),
            uploads: Some(uploads),
            common_prefixes: Some(prefixes),
            delimiter: i.delimiter.map(encode),
            is_truncated: Some(truncated),
            max_uploads: Some(limit),
            next_key_marker: if truncated { next_key } else { None },
            next_upload_id_marker: if truncated { next_id } else { None },
            key_marker: i.key_marker.map(encode),
            upload_id_marker: i.upload_id_marker,
            prefix: i.prefix.map(encode),
            encoding_type: i.encoding_type,
            ..Default::default()
        }))
    }
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
fn completion_output(bucket: &str, key: &str, result: &Value) -> CompleteMultipartUploadOutput {
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
