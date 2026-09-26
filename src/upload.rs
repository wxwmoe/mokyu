use crate::{
    app::App,
    codec::{self, MAX},
    config,
};
use anyhow::{Context, Result};
use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::StreamExt;
use hyper::HeaderMap;
use md5::{Digest, Md5};
use s3s::{TrailingHeaders, checksum::ChecksumHasher, dto::StreamingBlob, s3_error};
use serde_json::Value;
use sha2::Sha256;
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};
use uuid::Uuid;

#[derive(Default)]
struct AwsEnd {
    line: Vec<u8>,
    remaining: usize,
    terminal: bool,
    ended: bool,
}
impl AwsEnd {
    fn update(&mut self, mut data: &[u8]) -> Result<()> {
        while !data.is_empty() && !self.ended {
            if self.remaining > 0 {
                let take = data.len().min(self.remaining);
                self.remaining -= take;
                data = &data[take..];
                self.ended = self.terminal && self.remaining == 0;
            } else {
                self.line.push(data[0]);
                data = &data[1..];
                anyhow::ensure!(self.line.len() <= 1024, "AWS chunk metadata too long");
                if self.line.ends_with(b"\r\n") {
                    let header = std::str::from_utf8(&self.line[..self.line.len() - 2])?;
                    let size = usize::from_str_radix(header.split(';').next().unwrap_or(""), 16)?;
                    self.terminal = size == 0;
                    self.remaining = size.checked_add(2).context("AWS chunk size overflow")?;
                    self.line.clear();
                }
            }
        }
        Ok(())
    }
}

pub fn guard_aws_end(mut body: s3s::Body) -> s3s::Body {
    // s3s 0.16.1 permits EOF while reading final metadata; require a complete zero chunk.
    // Signature, CRLF and trailer validation remain the protocol library's responsibility.
    let stream = async_stream::try_stream! {
        let mut framing = AwsEnd::default();
        while let Some(next) = body.next().await {
            let bytes = next.map_err(std::io::Error::other)?;
            framing.update(&bytes).map_err(std::io::Error::other)?;
            yield bytes;
        }
        if !framing.ended {
            Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "missing AWS terminal chunk"))?;
        }
    };
    s3s::Body::from(StreamingBlob::wrap(
        stream.map(|r: std::result::Result<bytes::Bytes, std::io::Error>| r),
    ))
}

pub struct Integrity {
    md5: Md5,
    sha256: Option<Sha256>,
    payload_sha: Option<String>,
    content_md5: Option<String>,
    hasher: ChecksumHasher,
    expected: BTreeMap<String, String>,
    trailers: Vec<String>,
}
const ALGORITHMS: [&str; 10] = [
    "crc32",
    "crc32c",
    "crc64nvme",
    "sha1",
    "sha256",
    "sha512",
    "md5",
    "xxhash64",
    "xxhash3",
    "xxhash128",
];
impl Integrity {
    pub fn new(headers: &HeaderMap, algorithm: Option<&str>) -> Result<Self> {
        let mut this = Self {
            md5: Md5::new(),
            sha256: None,
            payload_sha: None,
            content_md5: None,
            hasher: ChecksumHasher::default(),
            expected: BTreeMap::new(),
            trailers: Vec::new(),
        };
        if let Some(v) = headers.get("content-md5") {
            let v = v.to_str()?;
            if STANDARD.decode(v).map(|b| b.len()).ok() != Some(16) {
                return Err(s3_error!(InvalidDigest).into());
            }
            this.content_md5 = Some(v.into());
        }
        if let Some(v) = headers
            .get("x-amz-content-sha256")
            .and_then(|v| v.to_str().ok())
            && v.len() == 64
        {
            this.sha256 = Some(Sha256::new());
            this.payload_sha = Some(v.into());
        }
        for alg in ALGORITHMS {
            if let Some(v) = headers.get(format!("x-amz-checksum-{alg}")) {
                this.enable(alg)?;
                this.expected.insert(alg.into(), v.to_str()?.into());
            }
        }
        if let Some(algorithm) = algorithm {
            this.enable(&algorithm.to_ascii_lowercase())?;
        }
        if let Some(trailers) = headers.get("x-amz-trailer") {
            for name in trailers.to_str()?.split(',').map(str::trim) {
                let alg = name
                    .strip_prefix("x-amz-checksum-")
                    .ok_or_else(|| s3_error!(NotImplemented, "unsupported request trailer"))?;
                this.enable(alg)?;
                this.trailers.push(alg.into());
            }
        }
        Ok(this)
    }
    fn enable(&mut self, algorithm: &str) -> Result<()> {
        match algorithm {
            "crc32" => self.hasher.crc32 = Some(Default::default()),
            "crc32c" => self.hasher.crc32c = Some(Default::default()),
            "crc64nvme" => self.hasher.crc64nvme = Some(Default::default()),
            "sha1" => self.hasher.sha1 = Some(Default::default()),
            "sha256" => self.hasher.sha256 = Some(Default::default()),
            "sha512" => self.hasher.sha512 = Some(Default::default()),
            "md5" => self.hasher.md5 = Some(Default::default()),
            "xxhash64" => self.hasher.xxhash64 = Some(Default::default()),
            "xxhash3" => self.hasher.xxhash3 = Some(Default::default()),
            "xxhash128" => self.hasher.xxhash128 = Some(Default::default()),
            _ => return Err(s3_error!(NotImplemented, "unsupported checksum algorithm").into()),
        };
        Ok(())
    }
    pub fn update(&mut self, data: &[u8]) {
        self.md5.update(data);
        if let Some(hash) = &mut self.sha256 {
            hash.update(data);
        }
        self.hasher.update(data);
    }
    pub fn finish(mut self, trailers: Option<TrailingHeaders>) -> Result<(String, Value)> {
        let trailers = trailers.and_then(|t| t.take());
        for algorithm in &self.trailers {
            let value = trailers
                .as_ref()
                .and_then(|t| t.get(format!("x-amz-checksum-{algorithm}")))
                .ok_or_else(|| s3_error!(BadDigest, "missing declared checksum trailer"))?
                .to_str()?;
            if self.expected.get(algorithm).is_some_and(|v| v != value) {
                return Err(s3_error!(BadDigest).into());
            }
            self.expected.insert(algorithm.clone(), value.into());
        }
        if let Some(trailers) = &trailers {
            for (name, _) in trailers {
                if name.as_str().starts_with("x-amz-checksum-")
                    && !self
                        .trailers
                        .iter()
                        .any(|a| name.as_str() == format!("x-amz-checksum-{a}"))
                {
                    return Err(s3_error!(BadDigest, "undeclared checksum trailer").into());
                }
            }
        }
        let md5 = self.md5.finalize();
        if self.content_md5.is_some_and(|v| STANDARD.encode(md5) != v) {
            return Err(s3_error!(BadDigest).into());
        }
        if let Some(hash) = self.sha256
            && self.payload_sha.as_ref() != Some(&hex::encode(hash.finalize()))
        {
            return Err(s3_error!(BadDigest, "x-amz-content-sha256 mismatch").into());
        }
        let sums = self.hasher.finalize();
        let mut result = BTreeMap::new();
        for (name, value) in [
            ("crc32", sums.checksum_crc32),
            ("crc32c", sums.checksum_crc32c),
            ("crc64nvme", sums.checksum_crc64nvme),
            ("sha1", sums.checksum_sha1),
            ("sha256", sums.checksum_sha256),
            ("sha512", sums.checksum_sha512),
            ("md5", sums.checksum_md5),
            ("xxhash64", sums.checksum_xxhash64),
            ("xxhash3", sums.checksum_xxhash3),
            ("xxhash128", sums.checksum_xxhash128),
        ] {
            if let Some(value) = value {
                result.insert(name.to_owned(), value);
            }
        }
        for (name, expected) in self.expected {
            if result.get(&name) != Some(&expected) {
                return Err(s3_error!(BadDigest, "checksum mismatch").into());
            }
        }
        Ok((hex::encode(md5), serde_json::to_value(result)?))
    }
}
pub fn checksum(value: &Value, name: &str) -> Option<String> {
    value.get(name).and_then(Value::as_str).map(str::to_owned)
}

impl App {
    #[allow(clippy::too_many_arguments)] // Protocol inputs stay explicit at the shared receiver.
    pub async fn receive(
        &self,
        stream: Uuid,
        mut body: StreamingBlob,
        headers: &HeaderMap,
        trailers: Option<TrailingHeaders>,
        length: Option<i64>,
        algorithm: Option<&str>,
        upload: Option<(Uuid, i32)>,
        should_compress: bool,
    ) -> Result<(i64, String, Value)> {
        let mut check = Integrity::new(headers, algorithm)?;
        let declared = if let Some(v) = headers.get("x-amz-decoded-content-length") {
            Some(v.to_str()?.parse::<i64>()?)
        } else {
            length
        };
        if declared.is_some_and(|n| !(0..=5 * 1024 * 1024 * 1024).contains(&n)) {
            return Err(s3_error!(EntityTooLarge).into());
        }
        let mut received = 0i64;
        let mut offset = 0i64;
        let mut window = Vec::with_capacity(MAX);
        let mut seed = if let Some((id, number)) = upload {
            self.multipart_seed(id, number).await?
        } else {
            crate::multipart::Seed::empty(Uuid::nil())
        };
        window.append(&mut seed.bytes);
        let idle =
            Duration::from_secs(config::seconds(&self.config.multipart.client_idle_timeout)?);
        let mut touch = Instant::now();
        loop {
            // Time spent processing a full window is not client idle time.
            let Some(data) = tokio::time::timeout(idle, body.next())
                .await
                .map_err(|_| s3_error!(RequestTimeout))?
            else {
                break;
            };
            let data = data.map_err(|_| s3_error!(IncompleteBody))?;
            received = received
                .checked_add(data.len() as i64)
                .context("object size overflow")?;
            if received > 5 * 1024 * 1024 * 1024 || declared.is_some_and(|n| received > n) {
                return Err(s3_error!(EntityTooLarge).into());
            }
            check.update(&data);
            let mut rest = data.as_ref();
            while !rest.is_empty() {
                let take = (MAX - window.len()).min(rest.len());
                window.extend_from_slice(&rest[..take]);
                rest = &rest[take..];
                if window.len() == MAX {
                    let n = codec::cut(&window);
                    let chunk = self
                        .put_chunk(stream, offset, window[..n].to_vec(), should_compress)
                        .await?;
                    let taken = self
                        .consume_seed(stream, offset, &chunk, &mut seed, n)
                        .await?;
                    window.drain(..n);
                    offset += taken as i64;
                }
            }
            if touch.elapsed() >= Duration::from_secs(5) {
                if let Some((upload, _)) = upload {
                    let changed = sqlx::query(
                        "UPDATE uploads SET touched_at=now() WHERE id=$1 AND state='active'",
                    )
                    .bind(upload)
                    .execute(&self.db)
                    .await?
                    .rows_affected();
                    if changed != 1 {
                        return Err(s3_error!(NoSuchUpload).into());
                    }
                }
                touch = Instant::now();
            }
        }
        if declared.is_some_and(|n| received != n) {
            return Err(s3_error!(IncompleteBody).into());
        }
        let (etag, sums) = check.finish(trailers)?;
        if upload.is_some() {
            let prefix = seed.spans.iter().map(|s| s.2).sum::<usize>();
            self.tail(stream, offset, &window[prefix..]).await?;
        } else {
            while !window.is_empty() {
                let n = codec::cut(&window);
                self.put_chunk(stream, offset, window[..n].to_vec(), should_compress)
                    .await?;
                window.drain(..n);
                offset += n as i64;
            }
        }
        self.finish_stream(stream, received, &etag, sums.clone())
            .await?;
        Ok((received, etag, sums))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn aws_terminal_chunk_is_required_across_network_boundaries() {
        let wire = b"3\r\nabc\r\n0\r\n\r\n";
        for width in 1..=wire.len() {
            let mut frame = AwsEnd::default();
            for bytes in wire.chunks(width) {
                frame.update(bytes).unwrap();
            }
            assert!(frame.ended);
        }
        for end in 0..wire.len() {
            let mut frame = AwsEnd::default();
            frame.update(&wire[..end]).unwrap();
            assert!(!frame.ended);
        }
    }
    #[test]
    fn payload_checksums_are_verified_before_publication() {
        let mut headers = HeaderMap::new();
        headers.insert("content-md5", "kAFQmDzST7DWlj99KOF/cg==".parse().unwrap());
        headers.insert(
            "x-amz-checksum-sha256",
            "ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0="
                .parse()
                .unwrap(),
        );
        let mut valid = Integrity::new(&headers, None).unwrap();
        valid.update(b"a");
        valid.update(b"bc");
        assert!(valid.finish(None).is_ok());
        let mut invalid = Integrity::new(&headers, None).unwrap();
        invalid.update(b"abd");
        assert!(invalid.finish(None).is_err());
        headers.insert("x-amz-trailer", "x-amz-checksum-crc32".parse().unwrap());
        let mut missing = Integrity::new(&headers, None).unwrap();
        missing.update(b"abc");
        assert!(missing.finish(None).is_err());
    }
}
