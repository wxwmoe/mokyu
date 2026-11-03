use super::{App, StoredStream};
use crate::model::Id;
use anyhow::Result;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::{sync::OnceCell, time::Instant};

type Check = OnceCell<Result<bool, String>>;

#[derive(Default)]
pub(crate) struct Reads(Mutex<HashMap<(Id, i64), Weak<Check>>>);

pub(crate) fn pending() -> anyhow::Error {
    let mut error = s3s::s3_error!(SlowDown, "object upload is still in progress; retry later");
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
    error.into()
}

impl App {
    pub(crate) async fn wait_read(&self, stream: &StoredStream, deadline: Instant) -> Result<bool> {
        if !self.shared_storage_only() {
            return Ok(false);
        }
        let pending_source = async || {
            if self.role == "single" {
                return self.stream_store.remote_pending(stream.id).await;
            }
            let node = self.node.get().ok_or_else(pending)?;
            self.cluster()?
                .pending_read(&[stream.id], node.node_id)
                .await
        };
        let key = (stream.id, stream.payload_revision);
        let check = {
            let mut checks = self.pending_reads.0.lock().unwrap();
            if let Some(check) = checks.get(&key).and_then(Weak::upgrade) {
                check
            } else {
                checks.retain(|_, check| check.strong_count() > 0);
                let check = Arc::new(Check::new());
                checks.insert(key, Arc::downgrade(&check));
                check
            }
        };
        let result = check
            .get_or_init(|| async {
                let result: Result<bool> = async {
                    if !pending_source().await? {
                        return Ok(false);
                    }
                    for at in probes(deadline) {
                        tokio::time::sleep_until(at).await;
                        if !pending_source().await? {
                            return Ok(true);
                        }
                    }
                    Err(pending())
                }
                .await;
                result.map_err(|error| {
                tracing::debug!(%error, stream=%stream.id, "pending source readiness check failed");
                error.to_string()
            })
            })
            .await;
        match result {
            Ok(waited) => Ok(*waited),
            Err(_) => Err(pending()),
        }
    }
}

pub(crate) fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}

pub(crate) fn probes(deadline: Instant) -> impl Iterator<Item = Instant> {
    // Skip intermediate probes already overtaken by a slow control request.
    [2750, 2000, 0].into_iter().filter_map(move |remaining| {
        let at = deadline - Duration::from_millis(remaining);
        (remaining == 0 || at > Instant::now()).then_some(at)
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn pending_response_is_retryable_and_not_cacheable() {
        let error = super::pending();
        let error = error.downcast_ref::<s3s::S3Error>().unwrap();
        assert_eq!(
            error.status_code(),
            Some(hyper::StatusCode::SERVICE_UNAVAILABLE)
        );
        assert_eq!(error.code(), &s3s::S3ErrorCode::SlowDown);
        let headers = error.headers().unwrap();
        assert_eq!(headers[hyper::header::RETRY_AFTER], "60");
        assert_eq!(headers[hyper::header::CACHE_CONTROL], "no-store");
    }
}
