use super::*;
use crate::model::mirroring::{Observed, Ranged};
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) fn requested(headers: &HeaderMap) -> bool {
    headers.contains_key(header::RANGE)
        && ![
            "if-range",
            "if-match",
            "if-none-match",
            "if-modified-since",
            "if-unmodified-since",
            "x-amz-checksum-mode",
            "x-mokyu-image-source",
        ]
        .iter()
        .any(|name| headers.contains_key(*name))
}

pub(crate) fn observed(opened: &Open, source: &Source, backup: bool) -> Option<Observed> {
    let etag = opened
        .headers
        .get(header::ETAG)
        .and_then(|v| v.to_str().ok())
        .filter(|v| super::super::external::objects::strong_etag(v))
        .map(str::to_owned);
    let version = matches!(source, Source::S3 { .. })
        .then(|| opened.headers.get("x-amz-version-id"))
        .flatten()
        .and_then(|v| v.to_str().ok())
        .filter(|v| *v != "null")
        .map(str::to_owned);
    let value = Observed {
        backup,
        size: opened.length?,
        etag,
        version,
    };
    value.validate().ok()?;
    Some(value)
}

struct Report {
    bytes: Arc<AtomicU64>,
    conflict: bool,
    done: Option<tokio::sync::oneshot::Sender<(u64, bool)>>,
}
impl Drop for Report {
    fn drop(&mut self) {
        if let Some(done) = self.done.take() {
            let _ = done.send((self.bytes.load(Ordering::Relaxed), self.conflict));
        }
    }
}

fn range_headers(opened: &Open, version: &Observed, span: &std::ops::Range<u64>) -> Result<()> {
    if version.version.as_ref().is_some_and(|value| {
        opened
            .headers
            .get("x-amz-version-id")
            .and_then(|v| v.to_str().ok())
            != Some(value)
    }) || version.etag.as_ref().is_some_and(|value| {
        opened
            .headers
            .get(header::ETAG)
            .and_then(|v| v.to_str().ok())
            != Some(value)
    }) {
        return Err(s3s::s3_error!(PreconditionFailed, "origin version changed").into());
    }
    if opened.status == hyper::StatusCode::PARTIAL_CONTENT {
        let expected = format!("bytes {}-{}/{}", span.start, span.end - 1, version.size);
        ensure!(
            opened
                .headers
                .get(header::CONTENT_RANGE)
                .and_then(|v| v.to_str().ok())
                == Some(expected.as_str())
                && opened
                    .length
                    .is_none_or(|size| size == span.end - span.start),
            s3s::s3_error!(IncompleteBody, "origin range differs")
        );
    } else {
        ensure!(
            opened.status == hyper::StatusCode::OK
                && opened.length.is_none_or(|size| size == version.size),
            s3s::s3_error!(IncompleteBody, "origin object length differs")
        );
    }
    Ok(())
}

pub(crate) async fn ranged(
    plan: &Ranged,
    request: &HeaderMap,
    done: tokio::sync::oneshot::Sender<(u64, bool)>,
) -> Result<crate::http::object::Read> {
    let mut report = Report {
        bytes: Arc::new(AtomicU64::new(0)),
        conflict: false,
        done: Some(done),
    };
    let version = &plan.observed;
    version.validate()?;
    let span = crate::http::gateway::checked_range(
        s3s::dto::Range::parse(
            request
                .get(header::RANGE)
                .context("range missing")?
                .to_str()?,
        )
        .map_err(|_| s3s::s3_error!(InvalidArgument, "invalid range"))?,
        version.size as i64,
    )?;
    let assignment = &plan.assignment;
    let source = if version.backup {
        assignment
            .rule
            .backup
            .as_ref()
            .context("origin backup missing")?
    } else {
        &assignment.rule.source
    };
    let connection = match source {
        Source::S3 { external_id, .. } => assignment
            .connections
            .iter()
            .find(|v| v.external_id == *external_id),
        _ => None,
    };
    ensure!(
        version.version.is_none() || connection.is_some(),
        "HTTP origin cannot use an S3 version"
    );
    let mut extra = HeaderMap::new();
    extra.insert(
        header::RANGE,
        format!("bytes={}-{}", span.start, span.end - 1).parse()?,
    );
    if let Some(etag) = &version.etag {
        extra.insert(header::IF_MATCH, etag.parse()?);
    }
    let opened = versioned(
        source,
        &assignment.config,
        &assignment.rule.key(&assignment.key)?,
        connection,
        Method::GET,
        extra,
        version.version.as_deref(),
    )
    .await;
    let mut opened = match opened {
        Ok(value) => value,
        Err(error) => {
            report.conflict = changed(&error);
            return Err(error);
        }
    };
    report.bytes = opened.bytes.clone();
    if let Err(error) = range_headers(&opened, version, &span) {
        report.conflict = changed(&error);
        return Err(error);
    }
    let partial = opened.status == hyper::StatusCode::PARTIAL_CONTENT;
    let mut skip = if partial { 0 } else { span.start };
    let mut left = span.end - span.start;
    let metadata = metadata(&opened.headers)?;
    let mut headers = crate::http::object::metadata_headers(&metadata)?;
    headers.insert(header::CONTENT_LENGTH, left.to_string().parse()?);
    headers.insert(
        header::CONTENT_RANGE,
        format!("bytes {}-{}/{}", span.start, span.end - 1, version.size).parse()?,
    );
    headers.insert(header::ACCEPT_RANGES, "bytes".parse()?);
    let body = async_stream::try_stream! {
        let _report = report;
        while left > 0 {
            let bytes = opened.body.next().await.ok_or_else(|| std::io::Error::other("origin range is truncated"))?
                .map_err(std::io::Error::other)?;
            let discarded = skip.min(bytes.len() as u64);
            skip -= discarded;
            let bytes = bytes.slice(discarded as usize..);
            let take = left.min(bytes.len() as u64) as usize;
            if take == 0 { continue; }
            left -= take as u64;
            if left == 0 && partial {
                if bytes.len() != take || opened.body.next().await.is_some() {
                    Err(std::io::Error::other("origin range exceeds its declared length"))?;
                }
            }
            yield bytes.slice(..take);
        }
    }.boxed();
    Ok(crate::http::object::Read {
        status: hyper::StatusCode::PARTIAL_CONTENT,
        headers,
        metadata,
        body: Some(crate::http::object::blob(body)),
        checksums: serde_json::json!({}),
        partial: true,
    })
}

pub(crate) fn changed(error: &anyhow::Error) -> bool {
    error.downcast_ref::<s3s::S3Error>().is_some_and(|error| {
        matches!(
            error.code(),
            s3s::S3ErrorCode::PreconditionFailed
                | s3s::S3ErrorCode::NoSuchKey
                | s3s::S3ErrorCode::IncompleteBody
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges_require_the_same_version_and_exact_bounds() {
        let version = Observed {
            backup: false,
            size: 100,
            etag: Some("\"v1\"".into()),
            version: None,
        };
        let response = Response::builder()
            .status(206)
            .header("etag", "\"v1\"")
            .header("content-range", "bytes 10-19/100")
            .header("content-length", "10")
            .body(Body::empty())
            .unwrap();
        let mut opened = super::super::opened(response, &Config::default(), false).unwrap();
        assert!(range_headers(&opened, &version, &(10..20)).is_ok());
        assert!(range_headers(&opened, &version, &(11..21)).is_err());
        opened
            .headers
            .insert(header::ETAG, "\"v2\"".parse().unwrap());
        assert!(changed(
            &range_headers(&opened, &version, &(10..20)).unwrap_err()
        ));
        opened
            .headers
            .insert(header::ETAG, "W/\"weak\"".parse().unwrap());
        assert!(
            observed(
                &opened,
                &Source::S3 {
                    external_id: uuid::Uuid::new_v4(),
                    prefix: String::new(),
                },
                false
            )
            .is_none()
        );
    }
}
