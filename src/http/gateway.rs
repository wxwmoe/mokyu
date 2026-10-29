use crate::{
    app::listing::encode_key,
    app::{App, Metadata, StoredStream, internal},
    authorization::Action,
    content::codec,
    upload::checksum,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use s3s::{
    S3, S3Request, S3Response, S3Result,
    access::{S3Access, S3AccessContext},
    auth::{S3Auth, SecretKey},
    dto::*,
    s3_error,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct Gateway(pub Arc<App>);
impl Gateway {
    async fn authorize_read(&self, access: Option<&str>, bucket: uuid::Uuid) -> Result<()> {
        if let Some(allowed) = access.and_then(|key| crate::app::reads::allowed(bucket, key)) {
            return if allowed {
                Ok(())
            } else {
                Err(s3_error!(AccessDenied).into())
            };
        }
        self.0.authorize(access, bucket, Action::Read).await?;
        Ok(())
    }
    async fn read_bucket(
        &self,
        name: &str,
        extensions: &axum::http::Extensions,
    ) -> Result<crate::app::Bucket> {
        if let Some(bucket) = extensions
            .get::<crate::app::Bucket>()
            .filter(|b| b.name == name)
        {
            return Ok(bucket.clone());
        }
        self.0.bucket(name, false).await
    }
}
pub fn stamp(t: DateTime<Utc>) -> Timestamp {
    Timestamp::from(std::time::SystemTime::from(
        DateTime::from_timestamp(t.timestamp(), 0).unwrap(),
    ))
}
pub fn invalid_range(size: i64) -> s3s::S3Error {
    let mut e = s3_error!(InvalidRange);
    let mut h = hyper::HeaderMap::new();
    h.insert("content-range", format!("bytes */{size}").parse().unwrap());
    e.set_headers(h);
    e
}
pub fn checked_range(range: Range, size: i64) -> S3Result<std::ops::Range<u64>> {
    if size <= 0 {
        return Err(invalid_range(size));
    }
    let span = range.check(size as u64).map_err(|_| invalid_range(size))?;
    if span.is_empty() {
        return Err(invalid_range(size));
    }
    Ok(span)
}

pub fn if_range_matches(s: &StoredStream, headers: &hyper::HeaderMap) -> bool {
    let Some(value) = headers.get("if-range") else {
        return true;
    };
    if let Ok(tag) = ETag::parse_http_header(value.as_bytes()) {
        return tag.strong_cmp(&ETag::Strong(s.etag.clone()));
    }
    // A key can be replaced twice within one second; its Last-Modified is not a strong validator.
    false
}
pub fn http_date(t: Timestamp) -> Result<String> {
    let mut text = Vec::new();
    t.format(TimestampFormat::HttpDate, &mut text)?;
    Ok(String::from_utf8(text)?)
}
pub fn canned(acl: Option<&ObjectCannedACL>) -> Result<bool> {
    match acl.map(ObjectCannedACL::as_str).unwrap_or("private") {
        "private" => Ok(false),
        "public-read" => Ok(true),
        _ => Err(s3_error!(
            NotImplemented,
            "only private and public-read ACLs are supported"
        )
        .into()),
    }
}
pub fn reject_features(headers: &hyper::HeaderMap) -> Result<()> {
    for (name, _) in headers {
        let n = name.as_str();
        if n.starts_with("x-amz-server-side-encryption")
            || n.starts_with("x-amz-copy-source-server-side-encryption")
            || n.starts_with("x-amz-object-lock")
            || n.starts_with("x-amz-grant-")
            || matches!(
                n,
                "x-amz-tagging"
                    | "x-amz-website-redirect-location"
                    | "x-amz-request-payer"
                    | "x-amz-expected-bucket-owner"
                    | "x-amz-source-expected-bucket-owner"
                    | "x-amz-write-offset-bytes"
                    | "x-amz-mfa"
                    | "x-amz-bypass-governance-retention"
                    | "x-amz-annotation-directive"
                    | "x-amz-security-token"
            )
        {
            return Err(
                s3_error!(NotImplemented, "requested storage feature is unsupported").into(),
            );
        }
    }
    if headers
        .get("x-amz-storage-class")
        .is_some_and(|s| s != "STANDARD")
    {
        return Err(s3_error!(InvalidStorageClass).into());
    }
    Ok(())
}
macro_rules! metadata {
    ($i:expr) => {
        crate::app::Metadata {
            content_type: $i.content_type.clone(),
            cache_control: $i.cache_control.clone(),
            content_disposition: $i.content_disposition.clone(),
            content_encoding: $i.content_encoding.clone(),
            content_language: $i.content_language.clone(),
            expires: $i.expires.clone(),
            user: $i.metadata.clone(),
        }
    };
}
pub(crate) use metadata;
macro_rules! checksums {
    ($o:expr,$s:expr) => {{
        let s = $s;
        $o.checksum_crc32 = checksum(s, "crc32");
        $o.checksum_crc32c = checksum(s, "crc32c");
        $o.checksum_crc64nvme = checksum(s, "crc64nvme");
        $o.checksum_sha1 = checksum(s, "sha1");
        $o.checksum_sha256 = checksum(s, "sha256");
        $o.checksum_sha512 = checksum(s, "sha512");
        $o.checksum_md5 = checksum(s, "md5");
        $o.checksum_xxhash64 = checksum(s, "xxhash64");
        $o.checksum_xxhash3 = checksum(s, "xxhash3");
        $o.checksum_xxhash128 = checksum(s, "xxhash128");
    }};
}
pub(crate) use checksums;

pub(super) fn get_read(
    read: super::object::Read,
    checksum_mode: bool,
) -> S3Result<S3Response<GetObjectOutput>> {
    if read.status == hyper::StatusCode::NOT_MODIFIED {
        let mut error = s3_error!(NotModified);
        error.set_headers(read.headers);
        return Err(error);
    }
    let mut out = GetObjectOutput {
        body: read.body,
        metadata: read.metadata.user,
        content_range: read
            .headers
            .get("content-range")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
        ..Default::default()
    };
    if checksum_mode && !read.partial {
        checksums!(out, &read.checksums);
        out.checksum_type = checksum(&read.checksums, "type").map(ChecksumType::from);
    }
    let mut response = S3Response::new(out);
    response.headers = read.headers;
    Ok(response)
}

pub(super) fn head_read(
    read: super::object::Read,
    checksum_mode: bool,
) -> S3Result<S3Response<HeadObjectOutput>> {
    if read.status == hyper::StatusCode::NOT_MODIFIED {
        let mut error = s3_error!(NotModified);
        error.set_headers(read.headers);
        return Err(error);
    }
    let mut out = HeadObjectOutput {
        metadata: read.metadata.user,
        ..Default::default()
    };
    if checksum_mode && !read.partial {
        checksums!(out, &read.checksums);
        out.checksum_type = checksum(&read.checksums, "type").map(ChecksumType::from);
    }
    let mut response = S3Response::new(out);
    response.headers = read.headers;
    Ok(response)
}

pub fn conditions(
    s: &StoredStream,
    im: Option<&ETagCondition>,
    inm: Option<&ETagCondition>,
    ims: Option<&Timestamp>,
    iums: Option<&Timestamp>,
    read: bool,
) -> Result<()> {
    let etag = ETag::Strong(s.etag.clone());
    let modified = stamp(s.touched_at);
    let matches = |c: &ETagCondition| match c {
        ETagCondition::Any => true,
        ETagCondition::ETag(e) => etag.strong_cmp(e),
    };
    if im.is_some_and(|c| !matches(c)) || (im.is_none() && iums.is_some_and(|t| modified > *t)) {
        return Err(s3_error!(PreconditionFailed).into());
    }
    let none_matches = inm.is_some_and(|c| match c {
        ETagCondition::Any => true,
        ETagCondition::ETag(e) => etag.weak_cmp(e),
    });
    if none_matches || (inm.is_none() && ims.is_some_and(|t| modified <= *t)) {
        return Err(if read {
            s3_error!(NotModified)
        } else {
            s3_error!(PreconditionFailed)
        }
        .into());
    }
    Ok(())
}
#[async_trait::async_trait]
impl S3Auth for Gateway {
    async fn get_secret_key(&self, key: &str) -> S3Result<SecretKey> {
        async {
            let encrypted = if let Some(encrypted) = crate::app::reads::secret(key) {
                encrypted
            } else {
                self.0.credentials.encrypted_secret(key).await?
            }
            .ok_or_else(|| s3_error!(InvalidAccessKeyId))?;
            Ok(SecretKey::from(codec::unprotect(
                &encrypted,
                &self.0.secrets.credential_key,
                key.as_bytes(),
            )?))
        }
        .await
        .map_err(internal)
    }
}
#[async_trait::async_trait]
impl S3Access for Gateway {
    async fn check(&self, cx: &mut S3AccessContext<'_>) -> S3Result<()> {
        check_request(cx)?;
        if let Some(credential) = cx.credentials() {
            self.0.credential_activity.record(&credential.access_key);
        }
        Ok(())
    }
}

pub(super) fn check_request(cx: &S3AccessContext<'_>) -> S3Result<()> {
    reject_features(cx.headers()).map_err(internal)?;
    if matches!(
        cx.s3_op().name(),
        "GetObject" | "HeadObject" | "GetObjectAcl" | "PutObjectAcl"
    ) && cx
        .uri()
        .query()
        .unwrap_or("")
        .split('&')
        .map(|p| crate::http::decode_path(p.split('=').next().unwrap_or("")))
        .collect::<Result<Vec<_>>>()
        .map_err(internal)?
        .iter()
        .any(|k| matches!(k.as_str(), "versionId" | "partNumber"))
    {
        return Err(s3_error!(
            NotImplemented,
            "versioning and part-number reads are unsupported"
        ));
    }
    if cx.s3_op().name() == "DeleteObject"
        && ["x-amz-if-match-last-modified-time", "x-amz-if-match-size"]
            .into_iter()
            .any(|h| cx.headers().contains_key(h))
    {
        return Err(s3_error!(
            NotImplemented,
            "delete conditions on size and modification time are unsupported"
        ));
    }
    if cx.credentials().is_none() && !matches!(cx.s3_op().name(), "GetObject" | "HeadObject") {
        return Err(s3_error!(AccessDenied));
    }
    Ok(())
}
#[async_trait::async_trait]
impl S3 for Gateway {
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        self.0
            .create_multipart(req, None, None)
            .await
            .map_err(internal)
    }
    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        self.0.upload_part(req, None).await.map_err(internal)
    }
    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        self.0.complete_multipart(req, None).await.map_err(internal)
    }
    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        self.0.abort_multipart(req, None).await.map_err(internal)
    }
    async fn list_parts(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        self.0.list_parts(req).await.map_err(internal)
    }
    async fn list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        self.0.list_uploads(req).await.map_err(internal)
    }
    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        async {
            let b = self.0.bucket(&req.input.bucket, false).await?;
            self.0
                .authorize(
                    req.credentials.as_ref().map(|c| c.access_key.as_str()),
                    b.id,
                    Action::List,
                )
                .await?;
            Ok(S3Response::new(HeadBucketOutput {
                bucket_region: Some(self.0.config.requests.s3_region.clone()),
                ..Default::default()
            }))
        }
        .await
        .map_err(internal)
    }
    async fn get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        async {
            let b = self.0.bucket(&req.input.bucket, false).await?;
            self.0
                .authorize(
                    req.credentials.as_ref().map(|c| c.access_key.as_str()),
                    b.id,
                    Action::List,
                )
                .await?;
            Ok(S3Response::new(GetBucketLocationOutput {
                location_constraint: if self.0.config.requests.s3_region == "us-east-1" {
                    None
                } else {
                    Some(BucketLocationConstraint::from(
                        self.0.config.requests.s3_region.clone(),
                    ))
                },
            }))
        }
        .await
        .map_err(internal)
    }
    async fn list_buckets(
        &self,
        req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        async {
            let key = req
                .credentials
                .as_ref()
                .ok_or_else(|| s3_error!(AccessDenied))?
                .access_key
                .as_str();
            let i = req.input;
            let limit = i.max_buckets.unwrap_or(1000);
            let prefix = i.prefix.as_deref().unwrap_or("");
            let after = crate::app::listing::bucket_cursor(
                key,
                prefix,
                i.bucket_region.as_deref(),
                i.continuation_token.as_deref(),
                limit,
            )?;
            let mut rows = self
                .0
                .credentials
                .buckets(
                    key,
                    prefix,
                    &after,
                    i.bucket_region.as_deref(),
                    &self.0.config.requests.s3_region,
                    limit as i64 + 1,
                )
                .await?;
            let truncated = rows.len() > limit as usize;
            rows.truncate(limit as usize);
            let token = if truncated {
                Some(crate::app::listing::bucket_token(
                    key,
                    prefix,
                    i.bucket_region.as_deref(),
                    &rows.last().unwrap().name,
                )?)
            } else {
                None
            };
            Ok(S3Response::new(ListBucketsOutput {
                buckets: Some(
                    rows.into_iter()
                        .map(|b| Bucket {
                            name: Some(b.name),
                            creation_date: Some(stamp(b.created_at)),
                            bucket_region: Some(self.0.config.requests.s3_region.clone()),
                            ..Default::default()
                        })
                        .collect(),
                ),
                continuation_token: token,
                prefix: i.prefix,
                ..Default::default()
            }))
        }
        .await
        .map_err(internal)
    }
    async fn put_object(
        &self,
        mut req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        async {
            reject_features(&req.headers)?;
            let _permits = self.0.admit(true).await?;
            let i = &req.input;
            let b = self.0.bucket(&i.bucket, true).await?;
            let authority = self
                .0
                .authorize(
                    req.credentials.as_ref().map(|c| c.access_key.as_str()),
                    b.id,
                    Action::Write,
                )
                .await?;
            let (id, _pin) = self
                .0
                .new_stream(
                    &authority,
                    b.id,
                    &i.key,
                    "object",
                    serde_json::to_value(metadata!(i))?,
                    canned(i.acl.as_ref())?,
                    true,
                    None,
                    None,
                )
                .await?;
            let body = req
                .input
                .body
                .take()
                .unwrap_or_else(|| StreamingBlob::from_bytes(bytes::Bytes::new()));
            let (_, etag, sums, received) = self
                .0
                .receive(
                    id,
                    b.storage_id.context("bucket has no storage")?,
                    body,
                    &req.headers,
                    req.trailing_headers,
                    req.input.content_length,
                    req.input.checksum_algorithm.as_ref().map(|a| a.as_str()),
                    None,
                    None,
                    Some((b.id, &req.input.key)),
                )
                .await?;
            self.0
                .publish(
                    id,
                    (b.id, &req.input.key),
                    req.input.if_match.as_ref(),
                    req.input.if_none_match.as_ref(),
                )
                .await?;
            self.0
                .cache_received(id, b.id, req.input.key.clone(), etag.clone(), received);
            let mut out = PutObjectOutput {
                e_tag: Some(ETag::Strong(etag)),
                ..Default::default()
            };
            checksums!(out, &sums);
            Ok(S3Response::new(out))
        }
        .await
        .map_err(internal)
    }
    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        async {
            let i = req.input;
            reject_features(&req.headers)?;
            if i.version_id.is_some() {
                return Err(s3_error!(NotImplemented, "versioning is unsupported").into());
            }
            let b = self.read_bucket(&i.bucket, &req.extensions).await?;
            if req.credentials.is_none() {
                let mut overrides = hyper::HeaderMap::new();
                for (name, value) in [
                    ("content-type", i.response_content_type),
                    ("content-disposition", i.response_content_disposition),
                    ("content-encoding", i.response_content_encoding),
                    ("content-language", i.response_content_language),
                    ("cache-control", i.response_cache_control),
                    ("expires", i.response_expires.map(http_date).transpose()?),
                ] {
                    if let Some(value) = value {
                        overrides
                            .insert(hyper::header::HeaderName::from_static(name), value.parse()?);
                    }
                }
                let read = super::object::read(
                    &self.0,
                    &b,
                    &i.key,
                    &req.headers,
                    false,
                    &overrides,
                    i.checksum_mode.is_some(),
                )
                .await?;
                return Ok(get_read(read, i.checksum_mode.is_some())?);
            }
            let light = i.if_match.is_some()
                || i.if_none_match.is_some()
                || i.if_modified_since.is_some()
                || i.if_unmodified_since.is_some();
            let deadline = crate::app::readiness::deadline();
            let permits = self.0.admit(false).await?;
            let (mut s, mut pin) = if light {
                (self.0.metadata(b.id, &i.key).await?, None)
            } else {
                let (s, pin) = self.0.current(b.id, &i.key).await?;
                (s, Some(pin))
            };
            loop {
                if req.credentials.is_some() || !s.public_read {
                    self.authorize_read(
                        req.credentials.as_ref().map(|c| c.access_key.as_str()),
                        b.id,
                    )
                    .await?;
                }
                conditions(
                    &s,
                    i.if_match.as_ref(),
                    i.if_none_match.as_ref(),
                    i.if_modified_since.as_ref(),
                    i.if_unmodified_since.as_ref(),
                    true,
                )?;
                if pin.is_some() {
                    if !self.0.wait_read(&s, deadline).await? {
                        break;
                    }
                    let current = self.0.bucket(&b.name, false).await?;
                    if current.state != "active" || current.settings_revision != b.settings_revision
                    {
                        return Err(crate::app::readiness::pending());
                    }
                }
                let current = self.0.current(b.id, &i.key).await?;
                s = current.0;
                pin = Some(current.1);
            }
            let requested = if if_range_matches(&s, &req.headers) {
                i.range
            } else {
                None
            };
            let range = match requested {
                Some(r) => checked_range(r, s.size)?,
                None => 0..s.size as u64,
            };
            let m: Metadata = serde_json::from_value(s.metadata.clone())?;
            let cache = i.checksum_mode.is_none()
                && super::object::cacheable(&req.headers)
                && [
                    m.cache_control.as_deref(),
                    i.response_cache_control.as_deref(),
                ]
                .into_iter()
                .flatten()
                .all(super::object::cache_control);
            let mut out = GetObjectOutput {
                content_length: Some((range.end - range.start) as i64),
                content_range: requested
                    .map(|_| format!("bytes {}-{}/{}", range.start, range.end - 1, s.size)),
                accept_ranges: Some("bytes".into()),
                e_tag: Some(ETag::Strong(s.etag.clone())),
                last_modified: Some(stamp(s.touched_at)),
                metadata: m.user,
                content_type: i.response_content_type.or(m.content_type),
                content_disposition: i.response_content_disposition.or(m.content_disposition),
                content_encoding: i.response_content_encoding.or(m.content_encoding),
                content_language: i.response_content_language.or(m.content_language),
                cache_control: i.response_cache_control.or(m.cache_control),
                expires: i.response_expires.map(http_date).transpose()?.or(m.expires),
                ..Default::default()
            };
            if i.checksum_mode.is_some() && requested.is_none() {
                checksums!(out, &s.checksums);
                out.checksum_type = checksum(&s.checksums, "type").map(ChecksumType::from);
            }
            out.body = Some(
                super::object::identity_body(
                    &self.0,
                    s,
                    pin.expect("body read is protected"),
                    range.start as i64..range.end as i64,
                    permits,
                    requested.is_some(),
                    cache,
                )
                .await?,
            );
            Ok(S3Response::new(out))
        }
        .await
        .map_err(internal)
    }
    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        async {
            let i = req.input;
            reject_features(&req.headers)?;
            if i.version_id.is_some() {
                return Err(s3_error!(NotImplemented, "versioning is unsupported").into());
            }
            let b = self.read_bucket(&i.bucket, &req.extensions).await?;
            if req.credentials.is_none() {
                let read = super::object::read(
                    &self.0,
                    &b,
                    &i.key,
                    &req.headers,
                    true,
                    &hyper::HeaderMap::new(),
                    i.checksum_mode.is_some(),
                )
                .await?;
                return Ok(head_read(read, i.checksum_mode.is_some())?);
            }
            let s = self.0.metadata(b.id, &i.key).await?;
            if req.credentials.is_some() || !s.public_read {
                self.authorize_read(
                    req.credentials.as_ref().map(|c| c.access_key.as_str()),
                    b.id,
                )
                .await?;
            }
            conditions(
                &s,
                i.if_match.as_ref(),
                i.if_none_match.as_ref(),
                i.if_modified_since.as_ref(),
                i.if_unmodified_since.as_ref(),
                true,
            )?;
            let range = match i.range {
                Some(r) => checked_range(r, s.size)?,
                None => 0..s.size as u64,
            };
            let m: Metadata = serde_json::from_value(s.metadata)?;
            let mut out = HeadObjectOutput {
                content_length: Some((range.end - range.start) as i64),
                accept_ranges: Some("bytes".into()),
                e_tag: Some(ETag::Strong(s.etag)),
                last_modified: Some(stamp(s.touched_at)),
                metadata: m.user,
                content_type: m.content_type,
                content_disposition: m.content_disposition,
                content_encoding: m.content_encoding,
                content_language: m.content_language,
                cache_control: m.cache_control,
                expires: m.expires,
                ..Default::default()
            };
            if i.checksum_mode.is_some() && i.range.is_none() {
                checksums!(out, &s.checksums);
                out.checksum_type = checksum(&s.checksums, "type").map(ChecksumType::from);
            }
            Ok(S3Response::new(out))
        }
        .await
        .map_err(internal)
    }
    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        async {
            let i = req.input;
            if i.version_id.is_some() {
                return Err(s3_error!(NotImplemented).into());
            }
            let b = self.0.bucket(&i.bucket, true).await?;
            let authority = self
                .0
                .authorize(
                    req.credentials.as_ref().map(|c| c.access_key.as_str()),
                    b.id,
                    Action::Delete,
                )
                .await?;
            self.0
                .change_object(&authority.principal, b.id, &i.key, None, None, i.if_match)
                .await?;
            Ok(S3Response::new(DeleteObjectOutput::default()))
        }
        .await
        .map_err(internal)
    }
    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        async {
            let i = req.input;
            let b = self.0.bucket(&i.bucket, true).await?;
            let authority = self
                .0
                .authorize(
                    req.credentials.as_ref().map(|c| c.access_key.as_str()),
                    b.id,
                    Action::Delete,
                )
                .await?;
            if i.delete.objects.len() > 1000 {
                return Err(s3_error!(InvalidArgument).into());
            }
            let mut deleted = Vec::new();
            let mut errors = Vec::new();
            for o in i.delete.objects {
                if o.version_id.is_some()
                    || o.e_tag.is_some()
                    || o.size.is_some()
                    || o.last_modified_time.is_some()
                {
                    errors.push(Error {
                        key: Some(o.key),
                        code: Some("NotImplemented".into()),
                        ..Default::default()
                    });
                    continue;
                }
                match self
                    .0
                    .delete_object(&authority.principal, b.id, &o.key)
                    .await
                {
                    Ok(()) => {
                        if i.delete.quiet != Some(true) {
                            deleted.push(DeletedObject {
                                key: Some(o.key),
                                ..Default::default()
                            });
                        }
                    }
                    Err(e) => {
                        let e = internal(e);
                        errors.push(Error {
                            key: Some(o.key),
                            code: Some(e.code().as_str().into()),
                            message: Some("delete failed".into()),
                            ..Default::default()
                        });
                    }
                }
            }
            Ok(S3Response::new(DeleteObjectsOutput {
                deleted: Some(deleted),
                errors: Some(errors),
                ..Default::default()
            }))
        }
        .await
        .map_err(internal)
    }
    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        async {
            let i = req.input;
            let b = self.0.bucket(&i.bucket, false).await?;
            self.0
                .authorize(
                    req.credentials.as_ref().map(|c| c.access_key.as_str()),
                    b.id,
                    Action::List,
                )
                .await?;
            let n = i.max_keys.unwrap_or(1000);
            if !(0..=1000).contains(&n) {
                return Err(s3_error!(InvalidArgument).into());
            }
            let list = self
                .0
                .list(
                    b.id,
                    i.prefix.as_deref().unwrap_or(""),
                    i.delimiter.as_deref().unwrap_or(""),
                    i.start_after.as_deref(),
                    i.continuation_token.as_deref(),
                    n as usize,
                )
                .await?;
            listed_objects(
                i,
                list.objects
                    .into_iter()
                    .map(|s| (s.object_key, s.size, s.etag, s.touched_at)),
                list.prefixes,
                list.next,
            )
        }
        .await
        .map_err(internal)
    }
    async fn get_object_acl(
        &self,
        req: S3Request<GetObjectAclInput>,
    ) -> S3Result<S3Response<GetObjectAclOutput>> {
        async {
            let i = req.input;
            let b = self.read_bucket(&i.bucket, &req.extensions).await?;
            self.0
                .authorize(
                    req.credentials.as_ref().map(|c| c.access_key.as_str()),
                    b.id,
                    Action::Read,
                )
                .await?;
            let s = self.0.metadata(b.id, &i.key).await?;
            Ok(S3Response::new(object_acl(&b, s.public_read)))
        }
        .await
        .map_err(internal)
    }
    async fn put_object_acl(
        &self,
        req: S3Request<PutObjectAclInput>,
    ) -> S3Result<S3Response<PutObjectAclOutput>> {
        async {
            reject_features(&req.headers)?;
            let i = req.input;
            let b = self.0.bucket(&i.bucket, true).await?;
            let authority = self
                .0
                .authorize(
                    req.credentials.as_ref().map(|c| c.access_key.as_str()),
                    b.id,
                    Action::Acl,
                )
                .await?;
            if i.access_control_policy.is_some() {
                return Err(
                    s3_error!(NotImplemented, "use private or public-read canned ACL").into(),
                );
            }
            let public = canned(i.acl.as_ref())?;
            self.0
                .change_object(&authority.principal, b.id, &i.key, None, Some(public), None)
                .await?;
            Ok(S3Response::new(PutObjectAclOutput::default()))
        }
        .await
        .map_err(internal)
    }
    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        async {
            reject_features(&req.headers)?;
            let _permits = self.0.admit(true).await?;
            let i = req.input;
            let b = self.0.bucket(&i.bucket, true).await?;
            let access = req.credentials.as_ref().map(|c| c.access_key.as_str());
            let mut authority = self.0.authorize(access, b.id, Action::Write).await?;
            let (bucket, key) = copy_source(&i)?;
            let source_bucket = self.0.bucket(bucket, false).await?;
            self.0
                .authorize(access, source_bucket.id, Action::Read)
                .await?;
            authority.add(source_bucket.id, Action::Read);
            let (source, _source_pin) = self.0.current(source_bucket.id, key).await?;
            conditions(
                &source,
                i.copy_source_if_match.as_ref(),
                i.copy_source_if_none_match.as_ref(),
                i.copy_source_if_modified_since.as_ref(),
                i.copy_source_if_unmodified_since.as_ref(),
                false,
            )?;
            let meta = if i
                .metadata_directive
                .as_ref()
                .is_some_and(|d| d.as_str() == "REPLACE")
            {
                serde_json::to_value(metadata!(i))?
            } else {
                source.metadata.clone()
            };
            let (id, _pin) = self
                .0
                .new_stream(
                    &authority,
                    b.id,
                    &i.key,
                    "object",
                    meta,
                    canned(i.acl.as_ref())?,
                    true,
                    None,
                    None,
                )
                .await?;
            self.0.reserve_quota(id, source.size, false, None).await?;
            _source_pin
                .with_read_lease(self.0.copy_extents(id, &source))
                .await?;
            self.0
                .finish_stream(id, source.size, &source.etag, source.checksums)
                .await?;
            self.0
                .publish(
                    id,
                    (b.id, &i.key),
                    i.if_match.as_ref(),
                    i.if_none_match.as_ref(),
                )
                .await?;
            Ok(S3Response::new(CopyObjectOutput {
                copy_object_result: Some(CopyObjectResult {
                    e_tag: Some(ETag::Strong(source.etag)),
                    last_modified: Some(stamp(Utc::now())),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        }
        .await
        .map_err(internal)
    }
}

pub(super) fn copy_source(input: &CopyObjectInput) -> Result<(&str, &str)> {
    if input.checksum_algorithm.is_some() {
        return Err(s3_error!(
            NotImplemented,
            "changing checksum algorithm during copy is unsupported"
        )
        .into());
    }
    if input
        .metadata_directive
        .as_ref()
        .is_some_and(|v| !matches!(v.as_str(), "COPY" | "REPLACE"))
        || input
            .tagging_directive
            .as_ref()
            .is_some_and(|v| v.as_str() != "COPY")
    {
        return Err(s3_error!(NotImplemented, "unsupported copy directive").into());
    }
    let CopySource::Bucket {
        bucket,
        key,
        version_id: None,
    } = &input.copy_source
    else {
        return Err(s3_error!(NotImplemented).into());
    };
    Ok((bucket, key))
}

#[cfg(test)]
mod range_tests {
    use super::*;

    #[test]
    fn requested_ranges_always_contain_bytes() {
        for value in ["bytes=-1", "bytes=-99", "bytes=0-", "bytes=0-0"] {
            let error = checked_range(Range::parse(value).unwrap(), 0).unwrap_err();
            assert_eq!(error.code(), &s3s::S3ErrorCode::InvalidRange);
        }
        for (value, size, expected) in [
            ("bytes=-1", 1, 0..1),
            ("bytes=-99", 3, 0..3),
            ("bytes=0-0", 3, 0..1),
            ("bytes=1-", 3, 1..3),
        ] {
            assert_eq!(
                checked_range(Range::parse(value).unwrap(), size).unwrap(),
                expected
            );
        }
        for value in ["bytes=-0", "bytes=3-", "bytes=9-10"] {
            if let Ok(range) = Range::parse(value) {
                assert!(checked_range(range, 3).is_err());
            }
        }
    }
}

pub(super) fn listed_objects(
    i: ListObjectsV2Input,
    objects: impl ExactSizeIterator<Item = (String, i64, String, DateTime<Utc>)>,
    prefixes: Vec<String>,
    next: Option<String>,
) -> Result<S3Response<ListObjectsV2Output>> {
    let n = i.max_keys.unwrap_or(1000);
    if !(0..=1000).contains(&n) {
        return Err(s3_error!(InvalidArgument).into());
    }
    if i.encoding_type
        .as_ref()
        .is_some_and(|v| v.as_str() != "url")
    {
        return Err(s3_error!(InvalidArgument, "encoding-type must be url").into());
    }
    let encoded = i.encoding_type.is_some();
    let encode = |s: String| if encoded { encode_key(&s) } else { s };
    let count = objects.len() + prefixes.len();
    let contents = objects
        .map(|(key, size, etag, modified_at)| Object {
            key: Some(encode(key)),
            size: Some(size),
            e_tag: Some(ETag::Strong(etag)),
            last_modified: Some(stamp(modified_at)),
            storage_class: Some(ObjectStorageClass::from_static("STANDARD")),
            ..Default::default()
        })
        .collect();
    Ok(S3Response::new(ListObjectsV2Output {
        name: Some(i.bucket),
        prefix: i.prefix.map(encode),
        delimiter: i.delimiter.map(encode),
        encoding_type: i.encoding_type,
        max_keys: Some(n),
        key_count: Some(count as i32),
        is_truncated: Some(next.is_some()),
        next_continuation_token: next,
        continuation_token: i.continuation_token,
        start_after: i.start_after.map(encode),
        contents: Some(contents),
        common_prefixes: Some(
            prefixes
                .into_iter()
                .map(|p| CommonPrefix {
                    prefix: Some(encode(p)),
                })
                .collect(),
        ),
        ..Default::default()
    }))
}

pub(super) fn object_acl(b: &crate::app::Bucket, public: bool) -> GetObjectAclOutput {
    let mut grants = vec![Grant {
        grantee: Some(Grantee {
            type_: Type::from_static("CanonicalUser"),
            id: Some(b.id.to_string()),
            uri: None,
            display_name: None,
            email_address: None,
        }),
        permission: Some(Permission::from_static("FULL_CONTROL")),
    }];
    if public {
        grants.push(Grant {
            grantee: Some(Grantee {
                type_: Type::from_static("Group"),
                uri: Some("http://acs.amazonaws.com/groups/global/AllUsers".into()),
                id: None,
                display_name: None,
                email_address: None,
            }),
            permission: Some(Permission::from_static("READ")),
        });
    }
    GetObjectAclOutput {
        owner: Some(Owner {
            id: Some(b.id.to_string()),
            display_name: Some(b.name.clone()),
        }),
        grants: Some(grants),
        ..Default::default()
    }
}
