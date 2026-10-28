use crate::status::stats::{Counters, Operation};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering::Relaxed},
};

#[derive(Default)]
pub(super) struct Metrics {
    pub(super) unary: Arc<Counters>,
    pub(super) connect: Arc<Counters>,
    pub(super) maintenance: Arc<Counters>,
    pub(super) ping: Arc<Counters>,
    pub(super) receipts: Arc<Counters>,
    pub(super) attempts: Arc<Counters>,
    pub(super) retries: AtomicU64,
}

pub(super) struct Attempt {
    total: Operation,
    peer: Operation,
}

impl Metrics {
    pub(super) fn attempt(&self, peer: &Arc<Counters>, retry: bool) -> Attempt {
        if retry {
            self.retries.fetch_add(1, Relaxed);
        }
        Attempt {
            total: self.attempts.begin(),
            peer: peer.begin(),
        }
    }

    pub(super) fn summary(&self) -> crate::model::node::NodeRpc {
        let calls = self.unary.snapshot();
        let attempts = self.attempts.snapshot();
        crate::model::node::NodeRpc {
            requests: calls["started"].as_u64().unwrap(),
            attempts: attempts["started"].as_u64().unwrap(),
            failures: calls["failed"].as_u64().unwrap(),
            canceled: calls["canceled"].as_u64().unwrap(),
            retries: self.retries.load(Relaxed),
            p50_ms: calls["duration_ms"]["p50"].as_u64(),
            p95_ms: calls["duration_ms"]["p95"].as_u64(),
            p99_ms: calls["duration_ms"]["p99"].as_u64(),
        }
    }

    pub(super) fn snapshot(&self) -> Value {
        json!({"summary": self.summary(), "unary": self.unary.snapshot(),
            "connect": self.connect.snapshot(), "maintenance": self.maintenance.snapshot(), "ping": self.ping.snapshot(), "receipts": self.receipts.snapshot(),
            "attempts": self.attempts.snapshot()})
    }
}

impl Attempt {
    pub(super) fn finish<T>(&mut self, result: &Result<T, tonic::Status>) {
        self.total.finish(result.is_err());
        self.peer.finish(result.is_err());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cancellation_and_retry_metrics_are_bounded() {
        let metrics = Metrics::default();
        let peer = Arc::new(Counters::default());
        drop(metrics.unary.begin());
        metrics
            .attempt(&peer, false)
            .finish(&Err::<(), _>(tonic::Status::unavailable("private")));
        metrics.attempt(&peer, true).finish(&Ok(()));
        let status = metrics.summary();
        assert_eq!(
            (
                status.requests,
                status.attempts,
                status.retries,
                status.canceled
            ),
            (1, 2, 1, 1)
        );
        assert!(!metrics.snapshot().to_string().contains("private"));
    }
}
