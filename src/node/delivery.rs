use super::reads::{Prepared, Reads};
use crate::{
    app::{Bucket, Metadata},
    http::{
        compression::{self, Encoding},
        gateway, object,
    },
    model::node_read as read,
    rpc::wire,
};
use anyhow::{Context, Result, ensure};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use futures_util::StreamExt;
use std::{io, sync::Arc, time::Duration};
use tokio::sync::OwnedSemaphorePermit;

pub(crate) fn compression_work(reads: &Reads) -> Option<OwnedSemaphorePermit> {
    reads.reader.http_work()
}

pub(crate) fn bucket(prepared: &Prepared) -> Result<Option<Bucket>> {
    prepared
        .reply
        .bucket
        .as_ref()
        .map(|version| prepared.material(version))
        .transpose()
}

pub(crate) fn error(error: anyhow::Error) -> s3s::S3Error {
    if let Some(status) = error.downcast_ref::<tonic::Status>() {
        return match status.code() {
            tonic::Code::ResourceExhausted => s3s::s3_error!(SlowDown),
            tonic::Code::InvalidArgument => s3s::s3_error!(InvalidRequest),
            _ => s3s::s3_error!(ServiceUnavailable),
        };
    }
    crate::app::internal(error)
}

pub(crate) async fn response(
    reads: &Arc<Reads>,
    mut prepared: Prepared,
    head: bool,
    overrides: &HeaderMap,
    memory: Option<OwnedSemaphorePermit>,
) -> Result<object::Read> {
    prepared.check()?;
    let outcome = wire::ReadOutcome::try_from(prepared.reply.outcome)?;
    match outcome {
        wire::ReadOutcome::Ready | wire::ReadOutcome::NotModified => {}
        wire::ReadOutcome::NoSuchBucket => return Err(s3s::s3_error!(NoSuchBucket).into()),
        wire::ReadOutcome::NoSuchKey => return Err(s3s::s3_error!(NoSuchKey).into()),
        wire::ReadOutcome::AccessDenied => return Err(s3s::s3_error!(AccessDenied).into()),
        wire::ReadOutcome::PreconditionFailed => {
            return Err(s3s::s3_error!(PreconditionFailed).into());
        }
        wire::ReadOutcome::Pending => return Err(crate::app::readiness::pending()),
        wire::ReadOutcome::NotAcceptable => {
            return Err(crate::http::problem(StatusCode::NOT_ACCEPTABLE, "NotAcceptable").0);
        }
        wire::ReadOutcome::InvalidRange => {
            let object: read::Object = prepared.material(
                prepared
                    .reply
                    .object
                    .as_ref()
                    .context("read object missing")?,
            )?;
            return Err(gateway::invalid_range(object.size).into());
        }
        _ => anyhow::bail!("invalid read outcome"),
    }
    let object: read::Object = prepared.material(
        prepared
            .reply
            .object
            .as_ref()
            .context("read object missing")?,
    )?;
    let metadata: Metadata = serde_json::from_value(object.metadata.clone())?;
    let encoding = match prepared.reply.representation.as_str() {
        "identity" => Encoding::Identity,
        "zstd" => Encoding::Zstd,
        "br" => Encoding::Brotli,
        "gzip" => Encoding::Gzip,
        _ => anyhow::bail!("invalid read representation"),
    };
    ensure!(
        prepared.reply.etag == encoding.etag(&format!("\"{}\"", object.etag)),
        "read etag mismatch"
    );
    let mut headers = object::metadata_headers(&metadata)?;
    headers.extend(overrides.clone());
    compression::vary(&mut headers, "Accept-Encoding");
    headers.insert(header::ETAG, prepared.reply.etag.parse()?);
    headers.insert(
        header::LAST_MODIFIED,
        gateway::http_date(gateway::stamp(object.modified_at))?.parse()?,
    );
    headers.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    if outcome == wire::ReadOutcome::NotModified {
        headers.remove(header::CONTENT_ENCODING);
        return Ok(object::Read {
            status: StatusCode::NOT_MODIFIED,
            headers,
            body: None,
            metadata,
            checksums: object.checksums,
            partial: false,
        });
    }
    let status = StatusCode::from_u16(prepared.reply.http_status.try_into()?)?;
    let partial = status == StatusCode::PARTIAL_CONTENT;
    let end = prepared
        .reply
        .offset
        .checked_add(prepared.reply.length)
        .context("read size overflow")?;
    ensure!(
        object.size >= 0
            && end <= object.size as u64
            && matches!(status, StatusCode::OK | StatusCode::PARTIAL_CONTENT),
        "invalid read span"
    );
    ensure!(
        !partial || (encoding == Encoding::Identity && prepared.reply.length > 0),
        "invalid partial representation"
    );
    if encoding == Encoding::Identity {
        headers.insert(
            header::CONTENT_LENGTH,
            prepared.reply.length.to_string().parse()?,
        );
        headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    } else {
        headers.insert(
            header::CONTENT_ENCODING,
            HeaderValue::from_static(encoding.name()),
        );
    }
    if partial {
        headers.insert(
            header::CONTENT_RANGE,
            format!(
                "bytes {}-{}/{}",
                prepared.reply.offset,
                end - 1,
                object.size
            )
            .parse()?,
        );
    }
    let body = if head {
        None
    } else {
        let size = prepared.reply.length;
        let direct = prepared
            .cached_body
            .as_ref()
            .and_then(|entry| entry.node_body())
            .is_some_and(|proof| proof.representation == encoding.name());
        let memory = if encoding == Encoding::Identity || direct {
            None
        } else {
            Some(memory.ok_or_else(|| s3s::s3_error!(ServiceUnavailable))?)
        };
        let idle = Duration::from_secs(crate::config::seconds(
            &reads.node.snapshot()?.runtime().requests.read_idle_timeout,
        )?);
        let lease = prepared.run.clone();
        let mut events = reads.node.subscribe();
        let builder = if direct {
            None
        } else {
            reads.cache_body(&prepared, &object, encoding, &headers)?
        };
        let source: compression::ByteStream = if let Some(entry) = prepared.cached_body.take() {
            ensure!(entry.usable(0), "cached body verification failed");
            let proof = entry.node_body().context("cached body proof missing")?;
            ensure!(
                direct || proof.representation == "identity",
                "invalid cached representation"
            );
            let bytes = if proof.representation == "identity" {
                ensure!(
                    entry.bytes.len() as u64 == object.size as u64,
                    "cached body length mismatch"
                );
                entry
                    .bytes
                    .slice(prepared.reply.offset as usize..end as usize)
            } else {
                ensure!(!partial, "encoded cache cannot serve a range");
                headers.insert(
                    header::CONTENT_LENGTH,
                    entry.bytes.len().to_string().parse()?,
                );
                entry.bytes.clone()
            };
            reads.bodies.hit();
            reads.bodies.validated();
            Box::pin(async_stream::try_stream! {
                let _request = prepared;
                let _entry = entry;
                #[cfg(feature = "fault-injection")]
                crate::faults::point("response-cache-body").await;
                yield bytes;
            })
        } else {
            prepared.capture = builder.as_ref().map(|builder| builder.capture.clone());
            Box::pin(
                reads
                    .body(prepared)
                    .map(|result| result.map_err(io::Error::other)),
            )
        };
        let body = if let Some(memory) = memory {
            compression::encode(
                source,
                encoding,
                Some(size),
                reads.reader.cpu.clone(),
                memory,
                None,
            )
        } else {
            source
        };
        let body = if let Some(builder) = builder {
            object::collect(body, builder, size, encoding)
        } else {
            body
        };
        Some(object::blob(Box::pin(async_stream::try_stream! {
            let mut body = body;
            loop {
                let item = {
                    let next = body.next();
                    tokio::pin!(next);
                    let timeout = tokio::time::Instant::now() + idle;
                    loop {
                        lease.check().map_err(io::Error::other)?;
                        tokio::select! {
                            biased;
                            event = events.changed() => if event.is_err() {
                                break Some(Err(io::Error::other("node state closed")));
                            },
                            _ = tokio::time::sleep_until(lease.deadline()) => {},
                            _ = tokio::time::sleep_until(timeout) => break Some(Err(io::Error::new(io::ErrorKind::TimedOut, "response read idle timeout"))),
                            result = &mut next => break result,
                        }
                    }
                };
                lease.check().map_err(io::Error::other)?;
                let Some(bytes) = item else { break };
                yield bytes?;
            }
        })))
    };
    Ok(object::Read {
        status,
        headers,
        body,
        metadata,
        checksums: object.checksums,
        partial,
    })
}
