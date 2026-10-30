use super::super::s3::{self, Parts, UploadedPart};
use super::*;
use axum::http::StatusCode;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const WINDOW: usize = 8 * 1024 * 1024;
pub(crate) const LIST_LIMIT: usize = 128;
const RESPONSE_LIMIT: usize = 1024 * 1024;
const MAX_SIZE: u64 = 5 * 1024 * 1024 * 1024 * 1024;
pub(crate) const MAX_PART: u64 = 5 * 1024 * 1024 * 1024;

pub(crate) fn part_size(size: u64) -> Result<u64> {
    ensure!(size <= MAX_SIZE, "external object exceeds size limit");
    Ok(size
        .div_ceil(10000)
        .max(WINDOW as u64)
        .next_multiple_of(WINDOW as u64))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    tag = "condition",
    content = "etag",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(crate) enum Condition {
    Absent,
    Match(String),
}

impl Condition {
    fn apply(&self, headers: &mut HeaderMap) -> Result<()> {
        match self {
            Self::Absent => {
                headers.insert(header::IF_NONE_MATCH, "*".parse()?);
            }
            Self::Match(etag) => {
                ensure!(strong_etag(etag), "invalid external write condition");
                headers.insert(header::IF_MATCH, etag.parse()?);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Written {
    pub etag: String,
    pub version: Option<String>,
}

impl Written {
    fn headers(headers: &HeaderMap, etag: Option<String>) -> Result<Self> {
        let header_etag = text(headers, "etag")?;
        ensure!(
            etag.as_ref()
                .zip(header_etag.as_ref())
                .is_none_or(|(body, header)| body == header),
            "external write ETags differ"
        );
        let etag = etag
            .or(header_etag)
            .context("external write ETag missing")?;
        let version = text(headers, "x-amz-version-id")?.filter(|value| value != "null");
        ensure!(
            strong_etag(&etag) && version.as_deref().is_none_or(opaque),
            "invalid external write receipt"
        );
        Ok(Self { etag, version })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Object {
    pub size: u64,
    pub etag: String,
    pub version: Option<String>,
    pub modified: Option<String>,
    pub metadata: BTreeMap<String, String>,
}

fn text(headers: &HeaderMap, name: &str) -> Result<Option<String>> {
    let mut values = headers.get_all(name).iter();
    let value = values.next().map(|value| value.to_str()).transpose()?;
    ensure!(values.next().is_none(), "duplicate external {name} header");
    ensure!(
        value.is_none_or(|value| value.len() <= 4096 && !value.chars().any(char::is_control)),
        "invalid external {name} header"
    );
    Ok(value.map(str::to_owned))
}

fn opaque(value: &str) -> bool {
    !value.is_empty() && value.len() <= 4096 && !value.chars().any(char::is_control)
}

pub(crate) fn strong_etag(value: &str) -> bool {
    value.len() >= 2
        && value.len() <= 4096
        && value.starts_with('"')
        && value.ends_with('"')
        && value[1..value.len() - 1]
            .bytes()
            .all(|byte| byte == 0x21 || (0x23..=0x7e).contains(&byte))
}

impl Object {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            self.size <= MAX_SIZE
                && strong_etag(&self.etag)
                && self
                    .version
                    .as_deref()
                    .is_none_or(|value| opaque(value) && value != "null")
                && self.modified.as_deref().is_none_or(opaque)
                && serde_json::to_vec(&self.metadata)?.len() <= 64 * 1024,
            "invalid external object snapshot"
        );
        for (name, value) in &self.metadata {
            ensure!(
                metadata_header(name) && value.len() <= 4096,
                "invalid external object metadata"
            );
            let _: header::HeaderName = name.parse()?;
            let _: header::HeaderValue = value.parse()?;
        }
        Ok(())
    }

    fn headers(headers: &HeaderMap) -> Result<Self> {
        let mut metadata = BTreeMap::new();
        for name in headers.keys().filter(|name| metadata_header(name.as_str())) {
            metadata.insert(name.to_string(), text(headers, name.as_str())?.unwrap());
        }
        let value = Self {
            size: text(headers, "content-length")?
                .context("external content length missing")?
                .parse()?,
            etag: text(headers, "etag")?.context("external ETag missing")?,
            version: text(headers, "x-amz-version-id")?.filter(|value| value != "null"),
            modified: text(headers, "last-modified")?,
            metadata,
        };
        value.validate()?;
        Ok(value)
    }
}

fn metadata_header(name: &str) -> bool {
    matches!(
        name,
        "content-type"
            | "cache-control"
            | "content-disposition"
            | "content-encoding"
            | "content-language"
            | "expires"
    ) || name.starts_with("x-amz-meta-") && name.len() > 11
}

fn write_headers(metadata: &BTreeMap<String, String>, public: Option<bool>) -> Result<HeaderMap> {
    ensure!(
        serde_json::to_vec(metadata)?.len() <= 64 * 1024,
        "external metadata exceeds limit"
    );
    let mut headers = HeaderMap::new();
    for (name, value) in metadata {
        ensure!(
            metadata_header(name) && value.len() <= 4096,
            "invalid external metadata header"
        );
        headers.insert(name.parse::<header::HeaderName>()?, value.parse()?);
    }
    if let Some(public) = public {
        headers.insert(
            "x-amz-acl",
            if public { "public-read" } else { "private" }.parse()?,
        );
    }
    Ok(headers)
}

pub(crate) fn created_upload(bytes: &[u8], key: &str) -> Result<String> {
    let mut xml = s3s::xml::Deserializer::new(bytes);
    let (mut remote_key, mut upload) = (None, None);
    xml.named_element("InitiateMultipartUploadResult", |xml| {
        xml.for_each_element(|xml, name| {
            let value: String = xml.content()?;
            match name {
                b"Key" if remote_key.is_none() => remote_key = Some(value),
                b"UploadId" if upload.is_none() => upload = Some(value),
                b"Key" | b"UploadId" => return Err(s3s::xml::DeError::UnexpectedTagName),
                _ => {}
            }
            Ok(())
        })
    })?;
    xml.expect_eof()?;
    let upload = upload.context("external upload ID missing")?;
    ensure!(
        remote_key.as_deref() == Some(key) && opaque(&upload),
        "invalid external upload identity"
    );
    Ok(upload)
}

fn result_tag(bytes: &[u8], root: &str, key: Option<&str>) -> Result<String> {
    if let Some(error) = response_error(bytes) {
        return Err(error.into());
    }
    let mut xml = s3s::xml::Deserializer::new(bytes);
    let (mut remote_key, mut etag) = (None, None);
    xml.named_element(root, |xml| {
        xml.for_each_element(|xml, name| {
            let value: String = xml.content()?;
            match name {
                b"Key" if remote_key.is_none() => remote_key = Some(value),
                b"ETag" if etag.is_none() => etag = Some(value),
                b"Key" | b"ETag" => return Err(s3s::xml::DeError::UnexpectedTagName),
                _ => {}
            }
            Ok(())
        })
    })?;
    xml.expect_eof()?;
    let etag = etag.context("external result ETag missing")?;
    let etag = if etag.starts_with('"') || etag.starts_with("W/") {
        etag
    } else {
        format!("\"{etag}\"")
    };
    ensure!(
        strong_etag(&etag) && key.is_none_or(|key| remote_key.as_deref() == Some(key)),
        "external result identity differs"
    );
    Ok(etag)
}

fn response_error(bytes: &[u8]) -> Option<s3s::S3Error> {
    let mut xml = s3s::xml::Deserializer::new(bytes);
    let mut code = None;
    xml.named_element("Error", |xml| {
        xml.for_each_element(|xml, name| {
            let value: String = xml.content()?;
            if name == b"Code" {
                if code.is_some() {
                    return Err(s3s::xml::DeError::UnexpectedTagName);
                }
                code = Some(value);
            }
            Ok(())
        })
    })
    .ok()?;
    xml.expect_eof().ok()?;
    let code = code.filter(|code| {
        !code.is_empty()
            && code.len() <= 128
            && code.bytes().all(|byte| byte.is_ascii_alphanumeric())
    })?;
    Some(s3s::S3Error::with_message(
        s3s::S3ErrorCode::from_bytes(code.as_bytes())?,
        "external object request failed",
    ))
}

fn response_status(value: StatusCode) -> s3s::S3Error {
    use s3s::S3ErrorCode as Code;
    let code = match value {
        StatusCode::NOT_FOUND => Code::NoSuchKey,
        StatusCode::PRECONDITION_FAILED => Code::PreconditionFailed,
        StatusCode::CONFLICT => Code::OperationAborted,
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Code::AccessDenied,
        StatusCode::METHOD_NOT_ALLOWED | StatusCode::NOT_IMPLEMENTED => Code::NotImplemented,
        StatusCode::BAD_REQUEST => Code::InvalidRequest,
        StatusCode::RANGE_NOT_SATISFIABLE => Code::InvalidRange,
        StatusCode::TOO_MANY_REQUESTS => Code::SlowDown,
        _ => Code::ServiceUnavailable,
    };
    s3s::S3Error::with_message(code, format!("external object request returned {value}"))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Entry {
    pub key: String,
    pub size: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Page {
    pub objects: Vec<Entry>,
    pub next: Option<String>,
}

pub(crate) fn listing(
    bytes: &[u8],
    base: &str,
    prefix: &str,
    after: Option<&str>,
    maximum: usize,
) -> Result<Page> {
    ensure!(
        maximum > 0 && maximum <= LIST_LIMIT && prefix.starts_with(base),
        "invalid external listing scope"
    );
    let mut xml = s3s::xml::Deserializer::new(bytes);
    let (mut encoding, mut truncated, mut next) = (None, None, None);
    let mut rows = Vec::new();
    xml.named_element("ListBucketResult", |xml| {
        xml.for_each_element(|xml, name| {
            match name {
                b"Contents" => {
                    if rows.len() >= maximum {
                        return Err(s3s::xml::DeError::UnexpectedTagName);
                    }
                    let (mut key, mut size) = (None, None);
                    xml.for_each_element(|xml, name| {
                        if name == b"Owner" {
                            xml.for_each_element(|xml, _| {
                                let _: String = xml.content()?;
                                Ok(())
                            })?;
                        } else {
                            let value: String = xml.content()?;
                            match name {
                                b"Key" if key.is_none() => key = Some(value),
                                b"Size" if size.is_none() => size = Some(value),
                                b"Key" | b"Size" => {
                                    return Err(s3s::xml::DeError::UnexpectedTagName);
                                }
                                _ => {}
                            }
                        }
                        Ok(())
                    })?;
                    rows.push((key, size));
                }
                b"EncodingType" if encoding.is_none() => encoding = Some(xml.content::<String>()?),
                b"IsTruncated" if truncated.is_none() => truncated = Some(xml.content::<String>()?),
                b"NextContinuationToken" if next.is_none() => next = Some(xml.content::<String>()?),
                b"EncodingType" | b"IsTruncated" | b"NextContinuationToken" | b"CommonPrefixes" => {
                    return Err(s3s::xml::DeError::UnexpectedTagName);
                }
                _ => {
                    let _: String = xml.content()?;
                }
            }
            Ok(())
        })
    })?;
    xml.expect_eof()?;
    ensure!(
        encoding.as_deref().is_none_or(|value| value == "url"),
        "unsupported external key encoding"
    );
    ensure!(
        matches!(truncated.as_deref(), Some("true" | "false")),
        "external listing pagination missing"
    );
    let next = if truncated.as_deref() == Some("true") {
        let next = next.context("external continuation missing")?;
        ensure!(
            opaque(&next) && Some(next.as_str()) != after && !rows.is_empty(),
            "external listing did not advance"
        );
        Some(next)
    } else {
        ensure!(
            next.as_deref().is_none_or(str::is_empty),
            "unexpected external continuation"
        );
        None
    };
    let mut seen = BTreeSet::new();
    let mut objects = Vec::with_capacity(rows.len());
    for (key, size) in rows {
        let key = key.context("external listing key missing")?;
        let key = if encoding.as_deref() == Some("url") {
            crate::http::decode_path(&key.replace('+', " "))?
        } else {
            key
        };
        ensure!(
            key.starts_with(prefix)
                && key.len() <= 1024
                && !key.contains('\0')
                && seen.insert(key.clone()),
            "external listing escaped its scope or repeated a key"
        );
        let key = key
            .strip_prefix(base)
            .context("external listing prefix differs")?
            .to_owned();
        let size = size
            .context("external listing size missing")?
            .parse::<u64>()?;
        ensure!(
            !key.is_empty() && size <= MAX_SIZE,
            "invalid external listed object"
        );
        objects.push(Entry { key, size });
    }
    Ok(Page { objects, next })
}

impl Client {
    async fn checked(&self, reply: Response<Body>) -> Result<Response<Body>> {
        if reply.status().is_success() {
            return Ok(reply);
        }
        let value = reply.status();
        let bytes = self.bounded(reply.into_body(), 64 * 1024).await?;
        Err(response_error(&bytes)
            .unwrap_or_else(|| response_status(value))
            .into())
    }

    async fn empty(&self, reply: Response<Body>) -> Result<HeaderMap> {
        let (parts, body) = self.checked(reply).await?.into_parts();
        let bytes = self.bounded(body, 64 * 1024).await?;
        if let Some(error) = response_error(&bytes) {
            return Err(error.into());
        }
        ensure!(
            bytes.iter().all(u8::is_ascii_whitespace),
            "unexpected external write response"
        );
        Ok(parts.headers)
    }

    fn full_key(&self, key: &str) -> Result<String> {
        ensure!(!key.is_empty(), "external object key is empty");
        let key = if self.connection.prefix.is_empty() {
            key.to_owned()
        } else {
            format!("{}/{key}", self.connection.prefix)
        };
        ensure!(
            !key.is_empty() && key.len() <= 1024 && !key.contains('\0'),
            "invalid external object key"
        );
        Ok(key)
    }

    pub(crate) async fn put(
        &self,
        key: &str,
        condition: &Condition,
        metadata: &BTreeMap<String, String>,
        public: Option<bool>,
        body: Bytes,
    ) -> Result<Written> {
        self.full_key(key)?;
        ensure!(body.len() as u64 <= MAX_PART, "external PUT exceeds limit");
        let mut headers = write_headers(metadata, public)?;
        condition.apply(&mut headers)?;
        let reply = self
            .bytes(Method::PUT, Some(key), &[], headers, body)
            .await?;
        Written::headers(&self.empty(reply).await?, None)
    }

    pub(crate) async fn start_upload(
        &self,
        key: &str,
        metadata: &BTreeMap<String, String>,
        public: Option<bool>,
    ) -> Result<String> {
        let full = self.full_key(key)?;
        let reply = self
            .bytes(
                Method::POST,
                Some(key),
                &[("uploads", "")],
                write_headers(metadata, public)?,
                Bytes::new(),
            )
            .await?;
        let reply = self.checked(reply).await?;
        created_upload(&self.bounded(reply.into_body(), 64 * 1024).await?, &full)
    }

    #[cfg(test)]
    pub(crate) async fn upload_part(
        &self,
        key: &str,
        upload: &str,
        number: u16,
        body: Bytes,
    ) -> Result<UploadedPart> {
        let size = body.len() as u64;
        let digest: [u8; 32] = Sha256::digest(&body).into();
        self.upload_part_stream(
            key,
            upload,
            number,
            size,
            &digest,
            Box::pin(futures_util::stream::once(async move { Ok(body) })),
        )
        .await
    }

    pub(crate) async fn upload_part_stream<'a>(
        &self,
        key: &str,
        upload: &str,
        number: u16,
        size: u64,
        digest: &'a [u8; 32],
        source: futures_util::stream::BoxStream<'a, Result<Bytes>>,
    ) -> Result<UploadedPart> {
        ensure!(
            opaque(upload) && (1..=10000).contains(&number) && size <= MAX_PART,
            "invalid external part request"
        );
        let number_text = number.to_string();
        let reply = self
            .stream(
                key,
                &[("uploadId", upload), ("partNumber", &number_text)],
                size,
                digest,
                source,
            )
            .await?;
        Ok(UploadedPart {
            number,
            size,
            etag: Written::headers(&self.empty(reply).await?, None)?.etag,
        })
    }

    pub(crate) async fn hash_range(
        &self,
        key: &str,
        snapshot: &Object,
        start: u64,
        size: u64,
    ) -> Result<[u8; 32]> {
        ensure!(
            start <= snapshot.size && size <= snapshot.size - start,
            "invalid external hash range"
        );
        let mut digest = Sha256::new();
        let mut offset = start;
        while offset < start + size {
            let length = (start + size - offset).min(WINDOW as u64) as usize;
            let bytes = self.read(key, snapshot, offset, length).await?;
            digest.update(&bytes);
            offset += length as u64;
        }
        Ok(digest.finalize().into())
    }

    pub(crate) async fn transfer_part(
        &self,
        key: &str,
        upload: &str,
        number: u16,
        source: &Client,
        source_key: &str,
        snapshot: &Object,
        start: u64,
        size: u64,
    ) -> Result<(UploadedPart, [u8; 32])> {
        ensure!(
            size <= MAX_PART && start <= snapshot.size && size <= snapshot.size - start,
            "invalid external transfer part"
        );
        let digest;
        let stream: futures_util::stream::BoxStream<'_, Result<Bytes>> = if size <= WINDOW as u64 {
            let bytes = if size == 0 {
                Bytes::new()
            } else {
                source
                    .read(source_key, snapshot, start, size as usize)
                    .await?
            };
            digest = Sha256::digest(&bytes).into();
            Box::pin(futures_util::stream::once(async move { Ok(bytes) }))
        } else {
            digest = source.hash_range(source_key, snapshot, start, size).await?;
            Box::pin(async_stream::try_stream! {
                let mut offset = start;
                while offset < start + size {
                    let length = (start + size - offset).min(WINDOW as u64) as usize;
                    yield source.read(source_key, snapshot, offset, length).await?;
                    offset += length as u64;
                }
            })
        };
        let part = self
            .upload_part_stream(key, upload, number, size, &digest, stream)
            .await?;
        Ok((part, digest))
    }

    #[cfg(test)]
    pub(crate) async fn copy_part(
        &self,
        key: &str,
        upload: &str,
        number: u16,
        source_key: &str,
        source: &Object,
        start: u64,
        length: u64,
    ) -> Result<UploadedPart> {
        source.validate()?;
        ensure!(
            opaque(upload)
                && (1..=10000).contains(&number)
                && length > 0
                && length <= MAX_PART
                && start
                    .checked_add(length)
                    .is_some_and(|end| end <= source.size),
            "invalid external part copy"
        );
        let mut path = format!(
            "/{}/{}",
            encode_key(&self.connection.bucket),
            encode_key(&self.full_key(source_key)?)
        );
        if let Some(version) = &source.version {
            path.push_str(&format!(
                "?versionId={}",
                encode_key(version).replace('/', "%2F")
            ));
        }
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-copy-source", path.parse()?);
        headers.insert("x-amz-copy-source-if-match", source.etag.parse()?);
        headers.insert(
            "x-amz-copy-source-range",
            format!("bytes={start}-{}", start + length - 1).parse()?,
        );
        let number_text = number.to_string();
        let reply = self
            .bytes(
                Method::PUT,
                Some(key),
                &[("uploadId", upload), ("partNumber", &number_text)],
                headers,
                Bytes::new(),
            )
            .await?;
        let reply = self.checked(reply).await?;
        let bytes = self.bounded(reply.into_body(), 64 * 1024).await?;
        Ok(UploadedPart {
            number,
            size: length,
            etag: result_tag(&bytes, "CopyPartResult", None)?,
        })
    }

    pub(crate) async fn parts(&self, key: &str, upload: &str, after: u16) -> Result<Option<Parts>> {
        ensure!(
            opaque(upload) && after < 10000,
            "invalid external part listing"
        );
        let full = self.full_key(key)?;
        let after_text = after.to_string();
        let reply = self
            .bytes(
                Method::GET,
                Some(key),
                &[
                    ("uploadId", upload),
                    ("part-number-marker", &after_text),
                    ("max-parts", "1000"),
                ],
                HeaderMap::new(),
                Bytes::new(),
            )
            .await?;
        if reply.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let reply = self.checked(reply).await?;
        let bytes = self.bounded(reply.into_body(), RESPONSE_LIMIT).await?;
        Ok(Some(s3::parts_listing(&bytes, &full, upload, after)?))
    }

    pub(crate) async fn complete_upload(
        &self,
        key: &str,
        upload: &str,
        parts: &[UploadedPart],
        condition: &Condition,
    ) -> Result<Written> {
        ensure!(
            opaque(upload) && !parts.is_empty() && parts.len() <= 10000,
            "invalid external completion"
        );
        let full = self.full_key(key)?;
        let mut body = Vec::new();
        for (index, part) in parts.iter().enumerate() {
            ensure!(
                usize::from(part.number) == index + 1
                    && part.size <= MAX_PART
                    && (index + 1 == parts.len() || part.size >= 5 * 1024 * 1024)
                    && strong_etag(&part.etag),
                "invalid external completed part"
            );
        }
        ensure!(
            parts.iter().map(|part| part.size).sum::<u64>() <= MAX_SIZE
                && parts.iter().map(|part| part.etag.len()).sum::<usize>() <= 2 * 1024 * 1024,
            "external completion exceeds limit"
        );
        s3s::xml::Serializer::new(&mut body).element("CompleteMultipartUpload", |xml| {
            for part in parts {
                xml.element("Part", |xml| {
                    xml.content("PartNumber", &i32::from(part.number))?;
                    xml.content("ETag", &part.etag)
                })?;
            }
            Ok(())
        })?;
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, "application/xml".parse()?);
        condition.apply(&mut headers)?;
        let reply = self
            .bytes(
                Method::POST,
                Some(key),
                &[("uploadId", upload)],
                headers,
                body.into(),
            )
            .await?;
        let reply = self.checked(reply).await?;
        let (parts, body) = reply.into_parts();
        let body = self.bounded(body, 64 * 1024).await?;
        Written::headers(
            &parts.headers,
            Some(result_tag(
                &body,
                "CompleteMultipartUploadResult",
                Some(&full),
            )?),
        )
    }

    #[cfg(test)]
    pub(crate) async fn uploads(&self, key: &str) -> Result<(Vec<String>, bool)> {
        let full = self.full_key(key)?;
        let reply = self
            .bytes(
                Method::GET,
                None,
                &[("uploads", ""), ("prefix", &full), ("max-uploads", "128")],
                HeaderMap::new(),
                Bytes::new(),
            )
            .await?;
        let reply = self.checked(reply).await?;
        let body = self.bounded(reply.into_body(), RESPONSE_LIMIT).await?;
        let (items, more) = s3::multipart_listing(&body)?;
        ensure!(
            items.iter().all(|(key, _)| key.starts_with(&full)),
            "external uploads escaped their scope"
        );
        Ok((
            items
                .into_iter()
                .filter(|(key, _)| key == &full)
                .map(|(_, id)| id)
                .collect(),
            more,
        ))
    }

    pub(crate) async fn abort_upload(&self, key: &str, upload: &str) -> Result<()> {
        ensure!(opaque(upload), "invalid external upload identity");
        let reply = self
            .bytes(
                Method::DELETE,
                Some(key),
                &[("uploadId", upload)],
                HeaderMap::new(),
                Bytes::new(),
            )
            .await?;
        if reply.status() == StatusCode::NOT_FOUND {
            return Ok(());
        }
        self.empty(reply).await.map(|_| ())
    }

    pub(crate) async fn acl(&self, key: &str, object: &Object) -> Result<bool> {
        object.validate()?;
        let mut query = vec![("acl", "")];
        if let Some(version) = &object.version {
            query.push(("versionId", version));
        }
        let reply = self
            .bytes(
                Method::GET,
                Some(key),
                &query,
                HeaderMap::new(),
                Bytes::new(),
            )
            .await?;
        let reply = self.checked(reply).await?;
        public_read(&self.bounded(reply.into_body(), 64 * 1024).await?)
    }

    pub(crate) async fn set_acl(&self, key: &str, object: &Object, public: bool) -> Result<()> {
        object.validate()?;
        let version = object
            .version
            .as_deref()
            .context("external ACL update requires a target version")?;
        let headers = write_headers(&BTreeMap::new(), Some(public))?;
        let reply = self
            .bytes(
                Method::PUT,
                Some(key),
                &[("acl", ""), ("versionId", version)],
                headers,
                Bytes::new(),
            )
            .await?;
        self.empty(reply).await.map(|_| ())
    }

    #[cfg(test)]
    pub(crate) async fn delete(&self, key: &str, object: &Object) -> Result<()> {
        object.validate()?;
        let query: Vec<_> = object
            .version
            .as_deref()
            .map(|version| ("versionId", version))
            .into_iter()
            .collect();
        let mut headers = HeaderMap::new();
        headers.insert(header::IF_MATCH, object.etag.parse()?);
        let reply = self
            .bytes(Method::DELETE, Some(key), &query, headers, Bytes::new())
            .await?;
        if reply.status() == StatusCode::NOT_FOUND {
            return Ok(());
        }
        self.empty(reply).await.map(|_| ())
    }

    pub(crate) async fn head(&self, key: &str, version: Option<&str>) -> Result<Option<Object>> {
        ensure!(version.is_none_or(opaque), "invalid external version");
        let query: Vec<_> = version
            .map(|value| ("versionId", value))
            .into_iter()
            .collect();
        let reply = self
            .bytes(
                Method::HEAD,
                Some(key),
                &query,
                HeaderMap::new(),
                Bytes::new(),
            )
            .await?;
        if reply.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let reply = self.checked(reply).await?;
        let object = Object::headers(reply.headers())?;
        ensure!(
            version.is_none_or(|value| object.version.as_deref() == Some(value)),
            "external object version differs"
        );
        Ok(Some(object))
    }

    pub(crate) async fn read(
        &self,
        key: &str,
        object: &Object,
        start: u64,
        length: usize,
    ) -> Result<Bytes> {
        object.validate()?;
        ensure!(
            length > 0
                && length <= WINDOW
                && start
                    .checked_add(length as u64)
                    .is_some_and(|end| end <= object.size),
            "invalid external read range"
        );
        let query: Vec<_> = object
            .version
            .as_deref()
            .map(|value| ("versionId", value))
            .into_iter()
            .collect();
        let mut headers = HeaderMap::new();
        headers.insert(header::IF_MATCH, object.etag.parse()?);
        headers.insert(
            header::RANGE,
            format!("bytes={start}-{}", start + length as u64 - 1).parse()?,
        );
        let reply = self
            .bytes(Method::GET, Some(key), &query, headers, Bytes::new())
            .await?;
        let reply = self.checked(reply).await?;
        ensure!(
            reply.status() == StatusCode::PARTIAL_CONTENT,
            "external server ignored the read range"
        );
        let expected = format!(
            "bytes {start}-{}/{}",
            start + length as u64 - 1,
            object.size
        );
        ensure!(
            text(reply.headers(), "content-range")?.as_deref() == Some(expected.as_str()),
            "external content range differs"
        );
        let mut received = Object::headers(reply.headers())?;
        ensure!(
            received.size == length as u64,
            "external range length differs"
        );
        received.size = object.size;
        ensure!(
            &received == object,
            s3s::s3_error!(PreconditionFailed, "external source changed")
        );
        let bytes = self.bounded(reply.into_body(), length).await?;
        ensure!(bytes.len() == length, "external read ended early");
        Ok(bytes)
    }

    pub(crate) async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        maximum: usize,
    ) -> Result<Page> {
        ensure!(
            prefix.len() <= 1024
                && !prefix.contains('\0')
                && after.is_none_or(opaque)
                && (1..=LIST_LIMIT).contains(&maximum),
            "invalid external listing request"
        );
        let base = if self.connection.prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", self.connection.prefix)
        };
        let scope = format!("{base}{prefix}");
        ensure!(scope.len() <= 1024, "external prefix exceeds 1024 bytes");
        let maximum_text = maximum.to_string();
        let mut query = vec![
            ("list-type", "2"),
            ("encoding-type", "url"),
            ("prefix", scope.as_str()),
            ("max-keys", maximum_text.as_str()),
        ];
        if let Some(after) = after {
            query.push(("continuation-token", after));
        }
        let reply = self
            .bytes(Method::GET, None, &query, HeaderMap::new(), Bytes::new())
            .await?;
        let reply = self.checked(reply).await?;
        let bytes = self.bounded(reply.into_body(), RESPONSE_LIMIT).await?;
        listing(&bytes, &base, &scope, after, maximum)
    }
}

#[cfg(test)]
#[path = "external_objects_tests.rs"]
mod tests;
