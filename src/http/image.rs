use crate::{
    app::{Metadata, StoredStream},
    processing::image::{
        Format,
        job::{self, Service},
        request::Selection,
    },
};
use anyhow::{Result, ensure};
use axum::{
    body::{Body, Bytes},
    http::{HeaderMap, Method, StatusCode, Uri},
    response::Response,
};
use futures_util::StreamExt;
use std::{future::Future, sync::Arc};
use tokio::sync::OwnedSemaphorePermit;

fn invalid() -> anyhow::Error {
    s3s::s3_error!(InvalidArgument).into()
}
fn failure(error: anyhow::Error) -> anyhow::Error {
    if error.downcast_ref::<s3s::S3Error>().is_some() {
        return error;
    }
    tracing::debug!(%error, "public image unavailable");
    if error.is::<std::io::Error>() || error.is::<tokio::time::error::Elapsed>() {
        s3s::s3_error!(SlowDown).into()
    } else {
        super::problem(StatusCode::UNSUPPORTED_MEDIA_TYPE, "UnsupportedImage").0
    }
}

pub(crate) fn request(query: Option<&str>) -> Result<crate::processing::image::request::Request> {
    crate::processing::image::request::Request::parse(query.unwrap_or("")).map_err(|e| {
        if e.is::<s3s::S3Error>() {
            e
        } else {
            s3s::s3_error!(InvalidArgument, "{e}").into()
        }
    })
}
pub(crate) async fn single(
    app: Arc<crate::app::App>,
    prepared: Option<Arc<crate::app::reads::Prepared>>,
    uri: Uri,
    headers: HeaderMap,
    method: Method,
) -> Result<Option<Response>> {
    let Some(prepared) = prepared else {
        return Ok(None);
    };
    let Some(selection) = prepared
        .snapshot
        .image
        .clone()
        .filter(|i| i.rule.is_some() || i.settings.policy.protect_original)
    else {
        return Ok(None);
    };
    ensure!(
        prepared.snapshot.bucket.state == "active",
        s3s::s3_error!(AccessDenied)
    );
    let Some(stream) = prepared.snapshot.stream.as_ref() else {
        return Err(s3s::s3_error!(NoSuchKey).into());
    };
    ensure!(stream.public_read, s3s::s3_error!(AccessDenied));
    let bucket = prepared.snapshot.bucket.id;
    let generation = app.public_reads.generation();
    let route = prepared.route.clone();
    let expected = crate::processing::image::request::state(
        &prepared.snapshot.bucket,
        prepared.snapshot.stream.as_ref(),
        prepared.snapshot.image.as_ref(),
    )?;
    let service = app.images.clone();
    let authority = app.clone();
    let result = crate::app::reads::scope(Some(prepared), async move {
        let (stream, pin) = app.current(bucket, &selection.key).await?;
        let size = stream.size;
        let input = stream.clone();
        let spans = if selection
            .rule
            .as_ref()
            .is_some_and(|rule| service.needs_demand(&stream, rule))
        {
            app.response_mapping(&stream)
                .await?
                .map(|m| m.2)
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        respond(
            service,
            stream,
            selection,
            uri,
            headers,
            method,
            None,
            None,
            spans,
            async move {
                let permits = app.admit(false).await?;
                Ok(Body::new(s3s::Body::from(
                    app.body(input, pin, 0..size, permits, false, None).await?,
                )))
            },
        )
        .await
    })
    .await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("image-public-validation").await;
    if authority.public_reads.generation() != generation {
        let cached = authority.public_reads.get(&route, &authority.storage);
        let current = cached.as_ref().is_some_and(|entry| {
            crate::processing::image::request::state(
                &entry.snapshot.bucket,
                entry.snapshot.stream.as_ref(),
                entry.snapshot.image.as_ref(),
            )
            .is_ok_and(|state| state == expected)
        });
        if !current {
            let fresh = authority
                .prepare_read(&route, None, false)
                .await?
                .ok_or_else(|| s3s::s3_error!(OperationAborted))?;
            ensure!(
                crate::processing::image::request::state(
                    &fresh.snapshot.bucket,
                    fresh.snapshot.stream.as_ref(),
                    fresh.snapshot.image.as_ref()
                )? == expected,
                s3s::s3_error!(OperationAborted)
            );
        }
    }
    Ok(Some(result))
}
pub(crate) fn read(response: Response) -> Result<super::object::Read> {
    let (parts, body) = response.into_parts();
    Ok(super::object::Read {
        status: parts.status,
        headers: parts.headers,
        body: Some(super::object::blob(Box::pin(
            body.into_data_stream()
                .map(|r| r.map_err(std::io::Error::other)),
        ))),
        metadata: Metadata::default(),
        checksums: serde_json::json!({}),
        partial: parts.status == StatusCode::PARTIAL_CONTENT,
    })
}

fn redirect(uri: &Uri, selection: &Selection, format: Format) -> Result<Response> {
    let mut location = if let (Some(delimiter), Some(style), Some(mapping)) = (
        &selection.delimiter,
        &selection.style,
        &selection.settings.policy.mapping,
    ) && let Some(family) =
        crate::model::image_styles::Mapping::family(&style.name, &mapping.auto)
    {
        let target = mapping.formats.get(format.word()).ok_or_else(invalid)?;
        let path = crate::http::decode_path(uri.path())?;
        let tail = format!("{delimiter}{}", style.name);
        let path = path.strip_suffix(&tail).ok_or_else(invalid)?;
        crate::app::listing::encode_key(&format!("{path}{delimiter}{family}{target}"))
            .replace("%21", "!")
    } else if selection.style.is_some() {
        let suffix = selection.delimiter.as_ref().map_or_else(
            || format!("style={}", selection.style.as_ref().unwrap().name),
            |_| String::new(),
        );
        format!(
            "{}?{}format={}",
            uri.path(),
            if suffix.is_empty() {
                suffix
            } else {
                format!("{suffix}&")
            },
            format.word()
        )
    } else {
        let mut rule = selection.rule.clone().ok_or_else(invalid)?;
        rule.stages.last_mut().ok_or_else(invalid)?.format = format;
        format!("{}?{}", uri.path(), rule.text().replace('|', "%7C"))
    };
    // Relative absolute paths remain on the current origin even for keys beginning with slashes.
    if location.starts_with("//") {
        location = format!("/%2F{}", &location[2..]);
    }
    ensure!(
        location.starts_with('/')
            && !location.starts_with("//")
            && !location.contains(['\r', '\n']),
        invalid()
    );
    Ok(Response::builder()
        .status(StatusCode::FOUND)
        .header("location", location)
        .header("cache-control", "no-store")
        .header("vary", "Accept")
        .header("content-length", "0")
        .body(Body::empty())?)
}

fn candidates(selection: &Selection) -> Vec<Format> {
    if selection.delimiter.is_some()
        && let Some(style) = &selection.style
        && let Some(mapping) = &selection.settings.policy.mapping
        && crate::model::image_styles::Mapping::family(&style.name, &mapping.auto).is_some()
    {
        mapping
            .formats
            .keys()
            .filter_map(|s| s.parse().ok())
            .collect()
    } else {
        vec![
            Format::Jxl,
            Format::Avif,
            Format::Webp,
            Format::Jpeg,
            Format::Png,
        ]
    }
}

fn headers(
    stream: &StoredStream,
    metadata: &Metadata,
    request: &HeaderMap,
) -> Result<(HeaderMap, StatusCode, std::ops::Range<u64>)> {
    use s3s::dto::{ETagCondition, Range, Timestamp, TimestampFormat};
    let tag = |name: &str| {
        request
            .get(name)
            .map(|v| ETagCondition::parse_http_header(v.as_bytes()))
            .transpose()
    };
    let date = |name: &str| {
        request
            .get(name)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| Timestamp::parse(TimestampFormat::HttpDate, v).ok())
    };
    let mut headers = super::object::metadata_headers(metadata)?;
    headers.insert("etag", format!("\"{}\"", stream.etag).parse()?);
    headers.insert(
        "last-modified",
        super::gateway::http_date(super::gateway::stamp(stream.touched_at))?.parse()?,
    );
    headers.insert("x-content-type-options", "nosniff".parse()?);
    headers.insert("accept-ranges", "bytes".parse()?);
    if let Err(error) = super::gateway::conditions(
        stream,
        tag("if-match")?.as_ref(),
        tag("if-none-match")?.as_ref(),
        date("if-modified-since").as_ref(),
        date("if-unmodified-since").as_ref(),
        true,
    ) {
        if error
            .downcast_ref::<s3s::S3Error>()
            .is_some_and(|e| *e.code() == s3s::S3ErrorCode::NotModified)
        {
            return Ok((headers, StatusCode::NOT_MODIFIED, 0..0));
        }
        return Err(error);
    }
    let range = if super::gateway::if_range_matches(stream, request) {
        request
            .get("range")
            .map(|value| Range::parse(value.to_str().unwrap_or("")))
            .transpose()
            .map_err(|_| super::gateway::invalid_range(stream.size))?
            .map(|range| super::gateway::checked_range(range, stream.size))
            .transpose()?
    } else {
        None
    };
    let partial = range.is_some();
    let range = range.unwrap_or(0..stream.size as u64);
    headers.insert(
        "content-length",
        (range.end - range.start).to_string().parse()?,
    );
    if partial {
        headers.insert(
            "content-range",
            format!("bytes {}-{}/{}", range.start, range.end - 1, stream.size).parse()?,
        );
    }
    Ok((
        headers,
        if partial {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        },
        range,
    ))
}

fn non_image(bytes: &[u8], complete: bool) -> bool {
    if [b"GIF87a".as_slice(), b"GIF89a", b"BM", b"8BPS", b"qoif"]
        .iter()
        .any(|s| bytes.starts_with(s))
        || (bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP"))
        || (bytes.len() > 2
            && bytes[0] == b'P'
            && (b'1'..=b'7').contains(&bytes[1])
            && bytes[2].is_ascii_whitespace())
    {
        return false;
    }
    if bytes.starts_with(b"%PDF-")
        || bytes.starts_with(b"PK\x03\x04")
        || bytes.starts_with(b"\x1f\x8b")
        || bytes.starts_with(b"OggS")
        || bytes.starts_with(b"fLaC")
        || bytes.starts_with(b"ID3")
        || (bytes.starts_with(b"RIFF")
            && bytes
                .get(8..12)
                .is_some_and(|s| s == b"WAVE" || s == b"AVI "))
    {
        return true;
    }
    if bytes.is_empty() {
        return complete;
    }
    let Ok(text) = std::str::from_utf8(bytes).or_else(|e| {
        if e.error_len().is_none() {
            std::str::from_utf8(&bytes[..e.valid_up_to()])
        } else {
            Err(e)
        }
    }) else {
        return false;
    };
    if text
        .chars()
        .any(|c| c.is_control() && !matches!(c, '\n' | '\r' | '\t'))
    {
        return false;
    }
    let mut text = text.trim_start_matches('\u{feff}').trim_start();
    loop {
        if text.starts_with("<?") {
            let Some((_, next)) = text.split_once("?>") else {
                return false;
            };
            text = next.trim_start();
        } else if text.starts_with("<!--") {
            let Some((_, next)) = text.split_once("-->") else {
                return false;
            };
            text = next.trim_start();
        } else {
            break;
        }
    }
    if text.is_empty() {
        return complete;
    }
    let lower = text.to_ascii_lowercase();
    if lower.starts_with("<!doctype") {
        let Some((declaration, body)) = text.split_once('>') else {
            return false;
        };
        return declaration.eq_ignore_ascii_case("<!doctype html")
            && non_image(body.as_bytes(), complete);
    }
    if lower.starts_with("/* xpm */") || lower.starts_with("#define") && lower.contains("_width") {
        return false;
    }
    if let Some(xml) = lower.strip_prefix('<') {
        let Some(end) = xml.find(|c: char| c.is_ascii_whitespace() || c == '>' || c == '/') else {
            return false;
        };
        let name = &xml[..end];
        if name.rsplit(':').next() == Some("svg") {
            return false;
        }
    }
    true
}

async fn original(
    stream: StoredStream,
    request: HeaderMap,
    head: bool,
    body: Body,
) -> Result<Response> {
    let metadata: Metadata = serde_json::from_value(stream.metadata.clone())?;
    ensure!(
        !metadata
            .content_type
            .as_ref()
            .is_some_and(|value| value.trim().to_ascii_lowercase().starts_with("image/")),
        s3s::s3_error!(AccessDenied)
    );
    ensure!(
        metadata
            .content_encoding
            .as_ref()
            .is_none_or(|e| e.is_empty() || e == "identity"),
        s3s::s3_error!(AccessDenied)
    );
    let mut source = body.into_data_stream();
    let mut prefix = Vec::new();
    let mut fragments = Vec::new();
    let mut complete = false;
    while prefix.len() < 4096 {
        let Some(chunk) = source.next().await else {
            complete = true;
            break;
        };
        let chunk = chunk?;
        let count = chunk.len().min(4096 - prefix.len());
        prefix.extend_from_slice(&chunk[..count]);
        fragments.push(chunk);
    }
    ensure!(non_image(&prefix, complete), s3s::s3_error!(AccessDenied));
    let (mut headers, status, range) = headers(&stream, &metadata, &request)?;
    if let Some(user) = &metadata.user {
        for (name, value) in user {
            headers.insert(
                axum::http::HeaderName::from_bytes(format!("x-amz-meta-{name}").as_bytes())?,
                value.parse()?,
            );
        }
    }
    if status == StatusCode::OK
        && request
            .get("x-amz-checksum-mode")
            .is_some_and(|v| v == "ENABLED")
    {
        for name in [
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
            "type",
        ] {
            if let Some(value) = stream.checksums.get(name).and_then(|v| v.as_str()) {
                headers.insert(
                    axum::http::HeaderName::from_bytes(
                        format!("x-amz-checksum-{name}").as_bytes(),
                    )?,
                    value.parse()?,
                );
            }
        }
    }
    let body = if head || status == StatusCode::NOT_MODIFIED {
        Body::empty()
    } else {
        let source =
            futures_util::stream::iter(fragments.into_iter().map(Ok::<Bytes, axum::Error>))
                .chain(source);
        let filtered: std::pin::Pin<
            Box<dyn futures_util::Stream<Item = std::io::Result<Bytes>> + Send>,
        > = Box::pin(async_stream::try_stream! {
            let mut source = std::pin::pin!(source);
            let mut offset = 0u64;
            while let Some(chunk) = source.next().await {
                let chunk = chunk.map_err(std::io::Error::other)?;
                let end = offset + chunk.len() as u64;
                if end > range.start && offset < range.end {
                    let start = range.start.saturating_sub(offset) as usize;
                    let stop = (range.end - offset).min(chunk.len() as u64) as usize;
                    yield chunk.slice(start..stop);
                }
                offset = end;
                if offset >= range.end { break; }
            }
        });
        Body::from_stream(filtered)
    };
    let mut response = Response::new(body);
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    Ok(response)
}

pub(crate) async fn respond<F>(
    service: Arc<Service>,
    stream: StoredStream,
    selection: Selection,
    uri: Uri,
    request: HeaderMap,
    method: Method,
    retained: Option<Arc<OwnedSemaphorePermit>>,
    run: Option<Arc<crate::node::RunLease>>,
    spans: Vec<crate::app::responses::DemandSpan>,
    body: F,
) -> Result<Response>
where
    F: Future<Output = Result<Body>> + Send + 'static,
{
    ensure!(stream.public_read, s3s::s3_error!(AccessDenied));
    super::gateway::reject_features(&request)?;
    let axum::extract::Query(query) =
        axum::extract::Query::<Vec<(String, String)>>::try_from_uri(&uri)?;
    ensure!(
        !query.iter().any(|(name, _)| name == "versionId"),
        s3s::s3_error!(NotImplemented, "versioning is unsupported")
    );
    let head = method == Method::HEAD;
    let Some(rule) = &selection.rule else {
        return original(stream, request, head, body.await?).await;
    };
    let identity = job::identity(&stream);
    let accept = request
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let limit = selection.settings.policy.input();
    if rule.format() == Format::Auto {
        let source = service
            .inspect(identity, stream.size, limit, body)
            .await
            .map_err(failure)?;
        let format = rule
            .select(&source, accept.as_deref(), &candidates(&selection))
            .map_err(|_| super::problem(StatusCode::NOT_ACCEPTABLE, "NotAcceptable").0)?;
        return redirect(&uri, &selection, format);
    }
    let mut product = service
        .execute(
            stream.clone(),
            rule.clone(),
            None,
            limit,
            128 * 1024 * 1024,
            false,
            retained,
            spans,
            body,
        )
        .await
        .map_err(failure)?;
    product.run = run;
    let mut derived = stream.clone();
    derived.etag = product.etag.trim_matches('"').into();
    derived.size = product.output.bytes as i64;
    derived.touched_at = tokio::fs::metadata(product.path())
        .await?
        .modified()?
        .into();
    let mut metadata: Metadata = serde_json::from_value(stream.metadata)?;
    metadata.content_type = Some(format!("image/{}", product.output.format.word()));
    metadata.content_encoding = None;
    let (headers, status, range) = headers(&derived, &metadata, &request)?;
    let mut response = Response::new(if head || status == StatusCode::NOT_MODIFIED {
        Body::empty()
    } else {
        product.range(range).await?
    });
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn originals_require_confirmed_non_image_content() {
        for bytes in [
            b"\xff\xd8\xff".as_slice(),
            b"\x89PNG\r\n\x1a\n",
            b"GIF89a",
            b"<svg width=\"1\">",
            b"<?xml version=\"1.0\"?> <!--x--> <svg/>",
            b"<x:svg xmlns:x=\"http://www.w3.org/2000/svg\"/>",
            b"<!DOCTYPE svg><svg/>",
            b"<!DOCTYPE html><svg/>",
            b"/* XPM */ static char *image[] = {}",
            b"#define icon_width 32",
            b"\0\0\0\x18ftypheic",
        ] {
            assert!(!non_image(bytes, false), "{bytes:?}");
        }
        for bytes in [
            b"hello".as_slice(),
            b"{\"hello\":1}",
            b"%PDF-1.7",
            b"PK\x03\x04",
            b"<html>hello",
        ] {
            assert!(non_image(bytes, false), "{bytes:?}");
        }
    }
}
