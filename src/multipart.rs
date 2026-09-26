use crate::{
    app::{Active, App, Extent, StoredStream},
    codec::{self, Chunk, MAX},
    s3::{canned, checksums, metadata, reject_features, stamp},
    upload::{Integrity, checksum},
};
use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use chrono::{DateTime, Utc};
use md5::{Digest, Md5};
use s3s::{S3Request, S3Response, dto::*, s3_error};
use serde_json::{Value, json};
use std::sync::{Arc, Weak};
use tracing::Instrument;
use uuid::Uuid;

#[derive(sqlx::FromRow)]
pub struct Upload {
    pub id: Uuid,
    pub bucket_id: Uuid,
    pub object_key: String,
    pub state: String,
    pub metadata: Value,
    pub public_read: bool,
    pub checksum_algorithm: Option<String>,
    pub checksum_type: Option<String>,
    pub manifest_hash: Option<String>,
    pub result: Option<Value>,
    pub created_at: DateTime<Utc>,
}
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
    pub fn upload_lock(&self, id: Uuid) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.upload_locks.lock().unwrap();
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
        sqlx::query_as("SELECT * FROM uploads WHERE id=$1 AND bucket_id=$2 AND object_key=$3 AND access_key=$4 AND state<>'aborted'").bind(id).bind(bucket).bind(key).bind(access).fetch_optional(&self.db).await?.ok_or_else(||s3_error!(NoSuchUpload).into())
    }
    pub async fn create_multipart(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> Result<S3Response<CreateMultipartUploadOutput>> {
        reject_features(&req.headers)?;
        let i = req.input;
        let bucket = self.bucket(&i.bucket, true).await?;
        let key = req
            .credentials
            .as_ref()
            .ok_or_else(|| s3_error!(AccessDenied))?
            .access_key
            .as_str();
        self.authorize(Some(key), bucket.id, true).await?;
        if i.key.is_empty() || i.key.len() > 1024 {
            return Err(s3_error!(InvalidArgument).into());
        }
        let algorithm = i.checksum_algorithm.as_ref().map(|a| a.as_str().to_owned());
        let kind = if algorithm.is_some() {
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
            if !matches!(kind.as_deref(), Some("FULL_OBJECT" | "COMPOSITE"))
                || (kind.as_deref() == Some("FULL_OBJECT")
                    && !matches!(a.as_str(), "CRC32" | "CRC32C" | "CRC64NVME"))
                || (a == "CRC64NVME" && kind.as_deref() != Some("FULL_OBJECT"))
            {
                return Err(s3_error!(InvalidRequest, "invalid multipart checksum type").into());
            }
        }
        if algorithm.is_none() && i.checksum_type.is_some() {
            return Err(s3_error!(InvalidRequest).into());
        }
        let _coord = self.coord.lock().await;
        self.writable()?;
        let mut tx = self.db.begin().await?;
        let state: String = sqlx::query_scalar("SELECT state FROM buckets WHERE id=$1 FOR SHARE")
            .bind(bucket.id)
            .fetch_one(&mut *tx)
            .await?;
        if state != "active" {
            return Err(s3_error!(OperationAborted).into());
        }
        if let Some(limit) = self.config.multipart.max_active_uploads {
            let count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM uploads WHERE state IN ('active','completing')",
            )
            .fetch_one(&mut *tx)
            .await?;
            if count >= limit {
                return Err(s3_error!(SlowDown).into());
            }
        }
        let id = Uuid::new_v4();
        sqlx::query("INSERT INTO uploads(id,bucket_id,object_key,access_key,metadata,public_read,checksum_algorithm,checksum_type) VALUES($1,$2,$3,$4,$5,$6,$7,$8)").bind(id).bind(bucket.id).bind(&i.key).bind(key).bind(serde_json::to_value(metadata!(i))?).bind(canned(i.acl.as_ref())?).bind(&algorithm).bind(&kind).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(S3Response::new(CreateMultipartUploadOutput {
            bucket: Some(i.bucket),
            key: Some(i.key),
            upload_id: Some(id.to_string()),
            checksum_algorithm: i.checksum_algorithm,
            checksum_type: kind.map(ChecksumType::from),
            ..Default::default()
        }))
    }
    pub async fn upload_part(
        &self,
        mut req: S3Request<UploadPartInput>,
    ) -> Result<S3Response<UploadPartOutput>> {
        reject_features(&req.headers)?;
        let _permits = self.admit(true).await?;
        let i = &req.input;
        if !(1..=10000).contains(&i.part_number) {
            return Err(s3_error!(InvalidArgument).into());
        }
        let b = self.bucket(&i.bucket, true).await?;
        let access = req
            .credentials
            .as_ref()
            .ok_or_else(|| s3_error!(AccessDenied))?
            .access_key
            .as_str();
        self.authorize(Some(access), b.id, true).await?;
        let u = self.get_upload(&i.upload_id, b.id, &i.key, access).await?;
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
            .new_stream(b.id, &i.key, "part", json!({}), false, false)
            .await?;
        {
            let lock = self.upload_lock(u.id);
            let _guard = lock.lock().await;
            let mut tx = self.db.begin().await?;
            let state: String =
                sqlx::query_scalar("SELECT state FROM uploads WHERE id=$1 FOR UPDATE")
                    .bind(u.id)
                    .fetch_one(&mut *tx)
                    .await?;
            if state != "active" {
                return Err(s3_error!(NoSuchUpload).into());
            }
            sqlx::query("INSERT INTO parts(upload_id,part_number,write_epoch) VALUES($1,$2,$3) ON CONFLICT(upload_id,part_number) DO UPDATE SET write_epoch=excluded.write_epoch").bind(u.id).bind(i.part_number).bind(id).execute(&mut *tx).await?;
            tx.commit().await?;
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
                body,
                &req.headers,
                req.trailing_headers,
                i.content_length,
                u.checksum_algorithm
                    .as_deref()
                    .or(i.checksum_algorithm.as_ref().map(|a| a.as_str())),
                Some((u.id, i.part_number)),
                self.config
                    .compression
                    .should_try(u.metadata["content_type"].as_str(), &u.object_key),
            )
            .await?;
        {
            let lock = self.upload_lock(u.id);
            let _guard = lock.lock().await;
            let _coord = self.coord.lock().await;
            self.writable()?;
            let mut tx = self.db.begin().await?;
            let bucket_state: String =
                sqlx::query_scalar("SELECT state FROM buckets WHERE id=$1 FOR SHARE")
                    .bind(b.id)
                    .fetch_one(&mut *tx)
                    .await?;
            if bucket_state != "active" {
                return Err(s3_error!(OperationAborted).into());
            }
            let state: String =
                sqlx::query_scalar("SELECT state FROM uploads WHERE id=$1 FOR UPDATE")
                    .bind(u.id)
                    .fetch_one(&mut *tx)
                    .await?;
            if state != "active" {
                return Err(s3_error!(NoSuchUpload).into());
            }
            let (old,epoch):(Option<Uuid>,Uuid)=sqlx::query_as("SELECT stream_id,write_epoch FROM parts WHERE upload_id=$1 AND part_number=$2 FOR UPDATE").bind(u.id).bind(i.part_number).fetch_one(&mut *tx).await?;
            if epoch != id {
                return Err(s3_error!(OperationAborted, "part was superseded").into());
            }
            sqlx::query("UPDATE parts SET stream_id=$3 WHERE upload_id=$1 AND part_number=$2")
                .bind(u.id)
                .bind(i.part_number)
                .bind(id)
                .execute(&mut *tx)
                .await?;
            sqlx::query("UPDATE streams SET state='ready' WHERE id=$1")
                .bind(id)
                .execute(&mut *tx)
                .await?;
            if let Some(old) = old {
                sqlx::query("UPDATE streams SET state='retired',touched_at=now() WHERE id=$1")
                    .bind(old)
                    .execute(&mut *tx)
                    .await?;
            }
            sqlx::query("UPDATE uploads SET touched_at=now() WHERE id=$1")
                .bind(u.id)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
        }
        // A late predecessor can now transfer its tail using an already durable successor.
        if let Err(e) = self.stitch_pair(u.id, i.part_number).await {
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
        let lock = self.upload_lock(upload);
        let _guard = lock.lock().await;
        let mut seed = Seed::empty(upload);
        let mut number = number - 1;
        let active: bool = sqlx::query_scalar("SELECT state='active' FROM uploads WHERE id=$1")
            .bind(upload)
            .fetch_one(&self.db)
            .await?;
        if !active {
            return Err(s3_error!(NoSuchUpload).into());
        }
        while number > 0 && seed.bytes.len() < MAX - 1 {
            let s:Option<StoredStream>=sqlx::query_as("SELECT s.* FROM parts p JOIN streams s ON p.stream_id=s.id WHERE p.upload_id=$1 AND p.part_number=$2 AND s.state='ready'").bind(upload).bind(number).fetch_optional(&self.db).await?;
            let Some(s) = s else { break };
            let tail: Option<Extent> = sqlx::query_as(
                "SELECT * FROM extents WHERE stream_id=$1 ORDER BY offset_bytes DESC LIMIT 1",
            )
            .bind(s.id)
            .fetch_optional(&self.db)
            .await?;
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
        let lock = self.upload_lock(seed.upload);
        let _guard = lock.lock().await;
        let _coord = self.coord.lock().await;
        let mut tx = self.db.begin().await?;
        let active: bool =
            sqlx::query_scalar("SELECT state='active' FROM uploads WHERE id=$1 FOR SHARE")
                .bind(seed.upload)
                .fetch_one(&mut *tx)
                .await?;
        let mut remaining = prefix;
        let mut chunk_offset = 0;
        while remaining > 0 {
            let span = seed.spans.first_mut().context("missing prefix span")?;
            let n = remaining.min(span.2);
            if active {
                replace_range(&mut tx, span.0, span.1, n, c.id, chunk_offset).await?;
            }
            span.1 += n as i64;
            span.2 -= n;
            remaining -= n;
            chunk_offset += n;
            if span.2 == 0 {
                seed.spans.remove(0);
            }
        }
        if prefix == consumed {
            sqlx::query("DELETE FROM extents WHERE stream_id=$1 AND offset_bytes=$2")
                .bind(stream)
                .bind(offset)
                .execute(&mut *tx)
                .await?;
            crate::lifecycle::mark_unreferenced(&mut tx, &[c.id]).await?;
        } else {
            sqlx::query("UPDATE extents SET source_offset=$3,length=$4 WHERE stream_id=$1 AND offset_bytes=$2").bind(stream).bind(offset).bind(prefix as i32).bind((consumed-prefix) as i32).execute(&mut *tx).await?;
        }
        sqlx::query("UPDATE streams SET size=$2 WHERE id=$1")
            .bind(stream)
            .bind(offset + (consumed - prefix) as i64)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        self.wake_gc.notify_one();
        Ok(consumed - prefix)
    }
    pub async fn stitch_pair(&self, upload: Uuid, number: i32) -> Result<()> {
        let lock = self.upload_lock(upload);
        let _guard = lock.lock().await;
        let metadata: Option<Value> =
            sqlx::query_scalar("SELECT metadata FROM uploads WHERE id=$1 AND state='active'")
                .bind(upload)
                .fetch_optional(&self.db)
                .await?;
        let Some(metadata) = metadata else {
            return Ok(());
        };
        let rows:Vec<StoredStream>=sqlx::query_as("SELECT s.* FROM parts p JOIN streams s ON s.id=p.stream_id WHERE p.upload_id=$1 AND p.part_number IN ($2,$2+1) AND s.state='ready' ORDER BY p.part_number").bind(upload).bind(number).fetch_all(&self.db).await?;
        if rows.len() != 2 {
            return Ok(());
        }
        let left = &rows[0];
        let right = &rows[1];
        let should_compress = self
            .config
            .compression
            .should_try(metadata["content_type"].as_str(), &left.object_key);
        let _lpin = self.pin(left.id);
        let _rpin = self.pin(right.id);
        let tail: Option<Extent> = sqlx::query_as(
            "SELECT * FROM extents WHERE stream_id=$1 ORDER BY offset_bytes DESC LIMIT 1",
        )
        .bind(left.id)
        .fetch_optional(&self.db)
        .await?;
        let Some(tail) = tail.filter(|e| e.fragment_id.is_some()) else {
            return Ok(());
        };
        if tail.length as i64 + right.size < MAX as i64 {
            return Ok(());
        }
        let (work, _pin) = self
            .new_stream(
                left.bucket_id,
                &left.object_key,
                "part",
                json!({}),
                false,
                false,
            )
            .await?;
        let data = self.extent_bytes(&tail).await?;
        let mut used = 0;
        let mut window = Vec::with_capacity(MAX);
        while used < data.len() && data.len() - used + right.size as usize >= MAX {
            window.clear();
            window.extend_from_slice(&data[used..]);
            let mut pos = 0;
            while window.len() < MAX {
                let rows = self.extents(right.id, pos, right.size).await?;
                ensure!(!rows.is_empty(), "part mapping incomplete");
                for row in rows {
                    ensure!(row.offset_bytes == pos, "part mapping has gap");
                    let bytes = self.extent_bytes(&row).await?;
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
                .put_chunk(work, used as i64, window[..n].to_vec(), should_compress)
                .await?;
            let l = (data.len() - used).min(n);
            let r = n - l;
            let _coord = self.coord.lock().await;
            let mut tx = self.db.begin().await?;
            replace_range(
                &mut tx,
                left.id,
                tail.offset_bytes + used as i64,
                l,
                chunk.id,
                0,
            )
            .await?;
            if r > 0 {
                replace_range(&mut tx, right.id, 0, r, chunk.id, l).await?;
            }
            tx.commit().await?;
            used += l;
        }
        sqlx::query("UPDATE streams SET state='retired',touched_at=now() WHERE id=$1")
            .bind(work)
            .execute(&self.db)
            .await?;
        self.wake_gc.notify_one();
        Ok(())
    }
    pub async fn complete_multipart(
        self: &Arc<Self>,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> Result<S3Response<CompleteMultipartUploadOutput>> {
        reject_features(&req.headers)?;
        let permits = self.admit(false).await?;
        let i = &req.input;
        let b = self.bucket(&i.bucket, true).await?;
        let access = req
            .credentials
            .as_ref()
            .ok_or_else(|| s3_error!(AccessDenied))?
            .access_key
            .as_str();
        self.authorize(Some(access), b.id, true).await?;
        let u = self.get_upload(&i.upload_id, b.id, &i.key, access).await?;
        let lock = self.upload_lock(u.id);
        let guard = lock.lock_owned().await;
        let u = self.get_upload(&i.upload_id, b.id, &i.key, access).await?;
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
            let s:StoredStream=sqlx::query_as("SELECT s.* FROM parts p JOIN streams s ON s.id=p.stream_id WHERE p.upload_id=$1 AND p.part_number=$2 AND s.state='ready'").bind(u.id).bind(part.part_number).fetch_optional(&self.db).await?.ok_or_else(||s3_error!(InvalidPart))?;
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
        let _coord = self.coord.lock().await;
        let changed=sqlx::query("UPDATE uploads SET state='completing',manifest_hash=$2,touched_at=now() WHERE id=$1 AND state='active'").bind(u.id).bind(&hash).execute(&self.db).await?.rows_affected();
        if changed != 1 {
            return Err(s3_error!(OperationAborted).into());
        }
        drop(_coord);
        let app = self.clone();
        let upin = self.pin(u.id);
        let context = req
            .extensions
            .get::<crate::stats::RequestContext>()
            .cloned();
        let input = req.input;
        let headers = req.headers;
        let work = tokio::spawn(async move {
            let _guard = guard;
            let _pins = pins;
            let _upin = upin;
            let _permits = permits;
            let result = app
                .assemble(&u, &parts, &input, &headers, total, &etag, &hash)
                .await;
            if result.is_err() {
                let _=sqlx::query("UPDATE uploads SET state='active',touched_at=now() WHERE id=$1 AND state='completing'").bind(u.id).execute(&app.db).await;
            }
            result.map_err(crate::app::internal)
        }.in_current_span());
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
                u.bucket_id,
                &u.object_key,
                "object",
                u.metadata.clone(),
                u.public_read,
                true,
            )
            .await?;
        let should_compress = self
            .config
            .compression
            .should_try(u.metadata["content_type"].as_str(), &u.object_key);
        let mut window = Vec::with_capacity(MAX);
        let mut offset = 0;
        let mut full = Integrity::new(&hyper::HeaderMap::new(), u.checksum_algorithm.as_deref())?;
        for part in parts {
            let mut part_hash = Md5::new();
            let mut pos = 0;
            while pos < part.size {
                let rows = self.extents(part.id, pos, part.size).await?;
                ensure!(!rows.is_empty(), "part mapping incomplete");
                for row in rows {
                    ensure!(row.offset_bytes == pos, "part mapping has gap");
                    let data = self.extent_bytes(&row).await?;
                    part_hash.update(&data);
                    full.update(&data);
                    let mut rest = data.as_ref();
                    while !rest.is_empty() {
                        let n = (MAX - window.len()).min(rest.len());
                        window.extend_from_slice(&rest[..n]);
                        rest = &rest[n..];
                        if window.len() == MAX {
                            let n = codec::cut(&window);
                            self.put_chunk(id, offset, window[..n].to_vec(), should_compress)
                                .await?;
                            offset += n as i64;
                            window.drain(..n);
                        }
                    }
                    pos += row.length as i64;
                }
            }
            ensure!(
                pos == part.size && hex::encode(part_hash.finalize()) == part.etag,
                "part reconstruction checksum mismatch"
            );
        }
        while !window.is_empty() {
            let n = codec::cut(&window);
            self.put_chunk(id, offset, window[..n].to_vec(), should_compress)
                .await?;
            offset += n as i64;
            window.drain(..n);
        }
        ensure!(offset == total, "assembled size mismatch");
        let (_, mut sums) = full.finish(None)?;
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
    pub async fn abort_multipart(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> Result<S3Response<AbortMultipartUploadOutput>> {
        let i = req.input;
        let b = self.bucket(&i.bucket, true).await?;
        let access = req
            .credentials
            .as_ref()
            .ok_or_else(|| s3_error!(AccessDenied))?
            .access_key
            .as_str();
        self.authorize(Some(access), b.id, true).await?;
        let u = self.get_upload(&i.upload_id, b.id, &i.key, access).await?;
        if i.if_match_initiated_time
            .is_some_and(|t| t != stamp(u.created_at))
        {
            return Err(s3_error!(PreconditionFailed).into());
        }
        let lock = self.upload_lock(u.id);
        let _guard = lock.lock().await;
        self.abort_upload(u.id).await?;
        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
    }
    pub async fn abort_upload(&self, id: Uuid) -> Result<()> {
        let coord = self.coord.lock().await;
        self.abort_upload_locked(id, &coord).await
    }
    pub(crate) async fn abort_upload_locked(
        &self,
        id: Uuid,
        _coord: &tokio::sync::MutexGuard<'_, ()>,
    ) -> Result<()> {
        let mut tx = self.db.begin().await?;
        let changed=sqlx::query("UPDATE uploads SET state='aborted',touched_at=now() WHERE id=$1 AND state IN ('active','completing')").bind(id).execute(&mut *tx).await?.rows_affected();
        if changed == 0 {
            return Err(s3_error!(NoSuchUpload).into());
        }
        sqlx::query("UPDATE streams SET state='retired',touched_at=now() WHERE id IN (SELECT stream_id FROM parts WHERE upload_id=$1)").bind(id).execute(&mut *tx).await?;
        sqlx::query("DELETE FROM parts WHERE upload_id=$1")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
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
        self.authorize(Some(access), b.id, false).await?;
        let u = self.get_upload(&i.upload_id, b.id, &i.key, access).await?;
        if !matches!(u.state.as_str(), "active" | "completing") {
            return Err(s3_error!(NoSuchUpload).into());
        }
        let limit = i.max_parts.unwrap_or(1000);
        let marker = i.part_number_marker.unwrap_or(0);
        if !(0..=1000).contains(&limit) || !(0..=10000).contains(&marker) {
            return Err(s3_error!(InvalidArgument).into());
        }
        let rows:Vec<(i32,Uuid)>=sqlx::query_as("SELECT part_number,stream_id FROM parts WHERE upload_id=$1 AND part_number>$2 AND stream_id IS NOT NULL ORDER BY part_number LIMIT $3").bind(u.id).bind(marker).bind(limit as i64+1).fetch_all(&self.db).await?;
        let truncated = rows.len() > limit as usize;
        let mut parts = Vec::new();
        let mut last = marker;
        for (number, id) in rows.into_iter().take(limit as usize) {
            let s: StoredStream = sqlx::query_as("SELECT * FROM streams WHERE id=$1")
                .bind(id)
                .fetch_one(&self.db)
                .await?;
            let mut part = Part {
                part_number: Some(number),
                size: Some(s.size),
                e_tag: Some(ETag::Strong(s.etag)),
                last_modified: Some(stamp(s.touched_at)),
                ..Default::default()
            };
            checksums!(part, &s.checksums);
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
        self.authorize(Some(access), b.id, false).await?;
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
                crate::listing::encode_key(&s)
            } else {
                s
            }
        };
        if limit > 0 {
            'pages: loop {
                let rows:Vec<Upload>=sqlx::query_as("SELECT * FROM uploads WHERE bucket_id=$1 AND access_key=$2 AND state IN ('active','completing') AND starts_with(object_key,$3) AND (object_key>$4 OR (object_key=$4 AND ($5 OR id>$6))) ORDER BY object_key,id LIMIT 64").bind(b.id).bind(access).bind(prefix).bind(&cursor).bind(inclusive).bind(cursor_id).fetch_all(&self.db).await?;
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
                        let after = crate::listing::successor(&group);
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

async fn replace_range(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    stream: Uuid,
    offset: i64,
    length: usize,
    chunk: i64,
    source: usize,
) -> Result<()> {
    if length == 0 {
        return Ok(());
    }
    let end = offset + length as i64;
    let rows:Vec<Extent>=sqlx::query_as("SELECT * FROM extents WHERE stream_id=$1 AND offset_bytes>=GREATEST(0,$2-4194304) AND offset_bytes<$3 AND offset_bytes+length>$2 ORDER BY offset_bytes FOR UPDATE").bind(stream).bind(offset).bind(end).fetch_all(&mut **tx).await?;
    let mut covered = offset;
    let mut previous = Vec::new();
    for row in rows {
        if let Some(id) = row.chunk_id {
            previous.push(id);
        }
        ensure!(row.offset_bytes <= covered, "extent replacement gap");
        let row_end = row.offset_bytes + row.length as i64;
        covered = covered.max(row_end.min(end));
        sqlx::query("DELETE FROM extents WHERE stream_id=$1 AND offset_bytes=$2")
            .bind(stream)
            .bind(row.offset_bytes)
            .execute(&mut **tx)
            .await?;
        if row.offset_bytes < offset {
            insert_extent(
                tx,
                &row,
                row.offset_bytes,
                (offset - row.offset_bytes) as i32,
                row.source_offset,
            )
            .await?;
        }
        if row_end > end {
            insert_extent(
                tx,
                &row,
                end,
                (row_end - end) as i32,
                row.source_offset + (end - row.offset_bytes) as i32,
            )
            .await?;
        }
    }
    ensure!(covered == end, "extent replacement incomplete");
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,chunk_id,source_offset) VALUES($1,$2,$3,$4,$5)").bind(stream).bind(offset).bind(length as i32).bind(chunk).bind(source as i32).execute(&mut **tx).await?;
    // Retained slices and references from other streams still protect the old chunk.
    crate::lifecycle::mark_unreferenced(tx, &previous).await?;
    Ok(())
}
async fn insert_extent(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    e: &Extent,
    offset: i64,
    length: i32,
    source: i32,
) -> Result<()> {
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,chunk_id,fragment_id,source_offset) VALUES($1,$2,$3,$4,$5,$6)").bind(e.stream_id).bind(offset).bind(length).bind(e.chunk_id).bind(e.fragment_id).bind(source).execute(&mut **tx).await?;
    Ok(())
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
