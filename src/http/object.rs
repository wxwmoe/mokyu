use super::{
    compression::{self, AcceptEncoding, Encoding},
    gateway,
};
use crate::{
    app::{App, Metadata, StoredStream},
    model::CompressionObservation,
};
use anyhow::Result;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use futures_util::StreamExt;
use s3s::dto::{Range, StreamingBlob, Timestamp, TimestampFormat};
use std::{
    io,
    sync::{Arc, Mutex},
};
use uuid::Uuid;

const POLICY: i32 = 1;

pub(crate) struct Read {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Option<StreamingBlob>,
    pub metadata: Metadata,
    pub checksums: serde_json::Value,
    pub partial: bool,
}

pub(crate) fn beneficial(raw: u64, encoded: u64) -> bool {
    raw >= 1024 && raw.saturating_sub(encoded) >= 128.max(raw.div_ceil(50))
}

fn available(stream: &StoredStream, metadata: &Metadata, encoding: Encoding) -> bool {
    if encoding == Encoding::Identity {
        return true;
    }
    if stream.size < 1024 {
        return false;
    }
    let summary = &stream.http_compression;
    if summary["version"].as_i64() == Some(i64::from(POLICY))
        && let Some(size) = summary[encoding.name()].as_u64()
    {
        return size > 0 && beneficial(stream.size as u64, size);
    }
    if let Some(estimate) = summary["estimated_size"].as_u64() {
        return beneficial(stream.size as u64, estimate);
    }
    let mime = metadata
        .content_type
        .as_deref()
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    stream.size <= 8 * 1024 * 1024
        && (mime.starts_with("text/")
            || matches!(
                mime.as_str(),
                "application/json" | "application/javascript" | "application/xml" | "image/svg+xml"
            )
            || mime.ends_with("+json")
            || mime.ends_with("+xml"))
}

fn conditions(stream: &StoredStream, request: &HeaderMap, etag: &str) -> Result<bool> {
    let date = |name: header::HeaderName| {
        request
            .get(name)
            .and_then(|s| s.to_str().ok())
            .and_then(|s| Timestamp::parse(TimestampFormat::HttpDate, s).ok())
    };
    if if request.contains_key(header::IF_MATCH) {
        !compression::matches(request, header::IF_MATCH, etag, false)
    } else {
        date(header::IF_UNMODIFIED_SINCE)
            .is_some_and(|date| gateway::stamp(stream.touched_at) > date)
    } {
        return Err(s3s::s3_error!(PreconditionFailed).into());
    }
    Ok(if request.contains_key(header::IF_NONE_MATCH) {
        compression::matches(request, header::IF_NONE_MATCH, etag, true)
    } else {
        date(header::IF_MODIFIED_SINCE)
            .is_some_and(|date| gateway::stamp(stream.touched_at) <= date)
    })
}

fn metadata_headers(metadata: &Metadata) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        metadata
            .content_type
            .as_deref()
            .unwrap_or("application/octet-stream")
            .parse()?,
    );
    for (name, value) in [
        (header::CACHE_CONTROL, &metadata.cache_control),
        (header::CONTENT_DISPOSITION, &metadata.content_disposition),
        (header::CONTENT_ENCODING, &metadata.content_encoding),
        (header::CONTENT_LANGUAGE, &metadata.content_language),
        (header::EXPIRES, &metadata.expires),
    ] {
        if let Some(value) = value {
            headers.insert(name, value.parse()?);
        }
    }
    Ok(headers)
}

pub(crate) async fn read(
    app: &Arc<App>,
    bucket: Uuid,
    key: &str,
    request: &HeaderMap,
    head: bool,
    overrides: &HeaderMap,
    checksum: bool,
) -> Result<Read> {
    let accept = AcceptEncoding::new(request);
    let light = head
        || [
            header::IF_MATCH,
            header::IF_NONE_MATCH,
            header::IF_MODIFIED_SINCE,
            header::IF_UNMODIFIED_SINCE,
        ]
        .iter()
        .any(|name| request.contains_key(name));
    let (mut stream, mut pin) = if light {
        (app.metadata(bucket, key).await?, None)
    } else {
        let (stream, pin) = app.current(bucket, key).await?;
        (stream, Some(pin))
    };
    loop {
        if !stream.public_read {
            return Err(s3s::s3_error!(AccessDenied).into());
        }
        let metadata: Metadata = serde_json::from_value(stream.metadata.clone())?;
        let mut headers = metadata_headers(&metadata)?;
        let original_transform = !compression::no_transform(&headers);
        headers.extend(overrides.clone());
        compression::vary(&mut headers, "Accept-Encoding");
        let transform = original_transform
            && !checksum
            && !compression::no_transform(request)
            && !compression::no_transform(&headers)
            && !headers.contains_key(header::CONTENT_ENCODING)
            && metadata.content_encoding.is_none();
        let range_requested =
            request.contains_key(header::RANGE) && gateway::if_range_matches(&stream, request);
        let prefer_identity =
            accept.identity() && (request.contains_key(header::IF_MATCH) || range_requested);
        let Some(mut encoding) = accept.select(|encoding| {
            encoding == Encoding::Identity
                || (!prefer_identity && transform && available(&stream, &metadata, encoding))
        }) else {
            return Err(super::problem(StatusCode::NOT_ACCEPTABLE, "NotAcceptable").0);
        };
        let original_tag = format!("\"{}\"", stream.etag);
        let mut not_modified = conditions(&stream, request, &encoding.etag(&original_tag))?;
        if !head && !not_modified && pin.is_none() {
            // Permissions and representation selection must refer to the protected current version.
            let current = app.current(bucket, key).await?;
            stream = current.0;
            pin = Some(current.1);
            continue;
        }
        let memory = (encoding != Encoding::Identity && !head && !not_modified)
            .then(|| app.storage.http_work())
            .flatten();
        if encoding != Encoding::Identity && !head && !not_modified && memory.is_none() {
            if !accept.identity() {
                return Err(s3s::s3_error!(ServiceUnavailable).into());
            }
            encoding = Encoding::Identity;
            not_modified = conditions(&stream, request, &original_tag)?;
        }
        headers.insert(header::ETAG, encoding.etag(&original_tag).parse()?);
        headers.insert(
            header::LAST_MODIFIED,
            gateway::http_date(gateway::stamp(stream.touched_at))?.parse()?,
        );
        headers.insert(
            "x-content-type-options",
            HeaderValue::from_static("nosniff"),
        );
        if not_modified {
            headers.remove(header::CONTENT_ENCODING);
            return Ok(Read {
                status: StatusCode::NOT_MODIFIED,
                headers,
                body: None,
                metadata,
                checksums: stream.checksums,
                partial: false,
            });
        }
        let range = if range_requested && encoding == Encoding::Identity {
            Some(
                Range::parse(request[header::RANGE].to_str().unwrap_or(""))
                    .map_err(|_| gateway::invalid_range(stream.size))?
                    .check(stream.size as u64)
                    .map_err(|_| gateway::invalid_range(stream.size))?,
            )
        } else {
            None
        };
        let span = range.clone().unwrap_or(0..stream.size as u64);
        if encoding == Encoding::Identity {
            headers.insert(
                header::CONTENT_LENGTH,
                (span.end - span.start).to_string().parse()?,
            );
            headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
        } else {
            headers.insert(
                header::CONTENT_ENCODING,
                HeaderValue::from_static(encoding.name()),
            );
        }
        if range.is_some() {
            headers.insert(
                header::CONTENT_RANGE,
                format!("bytes {}-{}/{}", span.start, span.end - 1, stream.size).parse()?,
            );
        }
        let checksums = stream.checksums.clone();
        let body = if head {
            None
        } else {
            let permits = app.admit(false).await?;
            let size = stream.size as u64;
            let observation = (encoding != Encoding::Identity
                && stream.http_compression["version"].as_i64().unwrap_or(0) <= i64::from(POLICY)
                && (stream.http_compression["version"].as_i64() != Some(i64::from(POLICY))
                    || stream.http_compression.get(encoding.name()).is_none()))
            .then(|| {
                let queue = Arc::downgrade(&app.compression_observations);
                let id = stream.id;
                Box::new(move |raw, encoded| {
                    if let Some(queue) = queue.upgrade()
                        && let Ok(size) = i64::try_from(encoded)
                    {
                        queue.submit(CompressionObservation {
                            stream: id,
                            version: POLICY,
                            encoding: encoding.name(),
                            size: if beneficial(raw, encoded) { size } else { 0 },
                        });
                    }
                }) as compression::Complete
            });
            let body = app.body(
                stream,
                pin.take().expect("body read is protected"),
                span.start as i64,
                span.end as i64,
                permits,
                range.is_some(),
            );
            Some(if encoding == Encoding::Identity {
                body
            } else {
                blob(compression::encode(
                    Box::pin(body.map(|part| part.map_err(io::Error::other))),
                    encoding,
                    Some(size),
                    app.storage.cpu.clone(),
                    memory.expect("encoder memory was admitted"),
                    observation,
                ))
            })
        };
        return Ok(Read {
            status: if range.is_some() {
                StatusCode::PARTIAL_CONTENT
            } else {
                StatusCode::OK
            },
            headers,
            body,
            metadata,
            checksums,
            partial: range.is_some(),
        });
    }
}

pub(crate) fn blob(stream: compression::ByteStream) -> StreamingBlob {
    let stream = Mutex::new(stream);
    StreamingBlob::wrap(futures_util::stream::poll_fn(move |cx| {
        stream.lock().unwrap().as_mut().poll_next(cx)
    }))
}
