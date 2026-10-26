use crate::model::Id;
use crate::{
    authorization::Principal,
    config::{Maintenance, MetricsSettings},
};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

#[derive(Default, Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScopeQuery {
    pub bucket: Option<Uuid>,
    pub project: Option<Uuid>,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub(crate) struct PhysicalUsage {
    pub(crate) indexed_bytes: String,
    pub(crate) selected_bytes: String,
    pub(crate) retained_bytes: String,
    pub(crate) gc_bytes: String,
    pub(crate) unconfirmed_bytes: String,
    pub(crate) chunk_objects: String,
    pub(crate) pack_objects: String,
    #[serde(default = "zero")]
    pub(crate) raw_objects: String,
}

fn zero() -> String {
    "0".into()
}

#[derive(Serialize, Deserialize, ToSchema)]
pub(crate) struct StorageUsage {
    pub(crate) buckets: String,
    pub(crate) objects: String,
    pub(crate) logical_bytes: String,
    pub(crate) unique_bytes: String,
    pub(crate) attributed_raw_bytes: String,
    pub(crate) encoded_bytes: Option<String>,
    pub(crate) encoded_known_bytes: String,
    pub(crate) local_savings: String,
    pub(crate) shared_savings: String,
    pub(crate) encoding_savings: Option<String>,
    pub(crate) chunks: String,
    pub(crate) packs: String,
    pub(crate) packed_bytes: String,
    pub(crate) pending_bytes: String,
    pub(crate) physical: Option<PhysicalUsage>,
}

#[derive(Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct RuntimePoint {
    pub(crate) at: DateTime<Utc>,
    pub(crate) epoch: DateTime<Utc>,
    pub(crate) requests: String,
    pub(crate) failed: String,
    pub(crate) backend_read_bytes: String,
    pub(crate) backend_write_bytes: String,
    pub(crate) cache_hits: String,
    pub(crate) cache_lookups: String,
    pub(crate) cache_bytes: String,
    pub(crate) cache_limit: String,
    pub(crate) upload_bytes: String,
    pub(crate) upload_limit: String,
    pub(crate) multipart_bytes: String,
    pub(crate) thumbnail_bytes: String,
    pub(crate) rss_bytes: Option<String>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct CapacityPoint {
    pub(crate) at: DateTime<Utc>,
    pub(crate) usage: StorageUsage,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct WorkSummary {
    pub(crate) id: Uuid,
    pub(crate) task_type: String,
    #[serde(rename = "status")]
    pub(crate) state: String,
    pub(crate) processed: String,
    pub(crate) updated_at: DateTime<Utc>,
}

#[derive(Default)]
pub(crate) struct ChunkCounts {
    pub(crate) reads: i64,
    pub(crate) ranges: i64,
    pub(crate) bytes: i64,
    pub(crate) origins: i64,
}

#[derive(Default)]
pub(crate) struct PackCounts {
    pub(crate) downloads: i64,
    pub(crate) bytes: i64,
    pub(crate) partial: i64,
    pub(crate) partial_bytes: i64,
    pub(crate) useful: i64,
}

#[derive(Default)]
pub(crate) struct AccessBatch {
    pub(crate) chunks: HashMap<(Id, DateTime<Utc>), ChunkCounts>,
    pub(crate) packs: HashMap<(Id, DateTime<Utc>), PackCounts>,
    pub(crate) members: HashMap<(Id, Id, DateTime<Utc>), i64>,
    pub(crate) dropped: bool,
}

pub(crate) struct UsageSnapshot {
    pub(crate) global: bool,
    pub(crate) bucket_count: usize,
    pub(crate) as_of: Option<DateTime<Utc>>,
    pub(crate) usage: Option<StorageUsage>,
    pub(crate) history: Vec<CapacityPoint>,
    pub(crate) created: bool,
}
pub(crate) struct RuntimeHistory {
    pub(crate) history: Vec<RuntimePoint>,
    pub(crate) work: Vec<WorkSummary>,
    pub(crate) pending_entries: String,
    pub(crate) pending_bytes: String,
    pub(crate) oldest_pending: Option<DateTime<Utc>>,
    pub(crate) failed_uploads: String,
}
#[async_trait]
pub(crate) trait MetricsStore: Send + Sync {
    async fn inventory(
        &self,
        metrics: &MetricsSettings,
        maintenance: &Maintenance,
    ) -> Result<Value>;
    async fn storage_inventory(&self, storage: Uuid) -> Result<Value>;
    async fn usage(
        &self,
        principal: &Principal,
        scope: &ScopeQuery,
        limit: usize,
    ) -> Result<UsageSnapshot>;
    async fn runtime(&self, principal: &Principal) -> Result<RuntimeHistory>;
    async fn sample_runtime(&self, point: RuntimePoint, retention: f64) -> Result<()>;
    async fn flush_access(&self, pending: AccessBatch, stale: f64) -> Result<()>;
    async fn expire_access(&self, retention: f64, batch: i64) -> Result<()>;
}

#[derive(Clone, Deserialize, IntoParams)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct RuntimeQuery {
    pub hours: u32,
    pub limit: i64,
}
impl Default for RuntimeQuery {
    fn default() -> Self {
        Self {
            hours: 24,
            limit: 2048,
        }
    }
}
impl RuntimeQuery {
    pub fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            (1..=168).contains(&self.hours) && (1..=2048).contains(&self.limit),
            "runtime range or limit is invalid"
        );
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct NodeSample {
    pub boot_id: Uuid,
    pub sequence: i64,
    pub uptime_seconds: u64,
    pub interval_seconds: u64,
    pub point: RuntimePoint,
    pub db_pool_size: u64,
    pub db_pool_idle: u64,
    pub queue_running: Option<u64>,
    pub queue_waiting: Option<u64>,
}
impl NodeSample {
    pub fn from_report(report: &super::node::NodeReport, interval_seconds: u64) -> Result<Self> {
        let s = &report.status;
        let number = |v: &Value| v.as_u64().unwrap_or(0);
        let count = |key: &str| {
            ["s3", "web"]
                .into_iter()
                .map(|listener| number(&s["runtime"]["http"][listener][key]))
                .sum::<u64>()
                .to_string()
        };
        let cache = &s["cache"];
        let queues = s["io"]["backend_queues"].as_object();
        Ok(Self {
            boot_id: report.boot_id,
            sequence: report.sequence,
            interval_seconds,
            uptime_seconds: number(&s["runtime"]["uptime_seconds"]),
            point: RuntimePoint {
                at: report.sampled_at,
                epoch: serde_json::from_value(s["runtime"]["started_at"].clone())?,
                requests: count("started"),
                failed: count("failed"),
                backend_read_bytes: number(&cache["backend_read_bytes"]).to_string(),
                backend_write_bytes: number(&cache["backend_write_bytes"]).to_string(),
                cache_hits: number(&cache["hits"]).to_string(),
                cache_lookups: number(&s["io"]["cache_lookups"]).to_string(),
                cache_bytes: number(&cache["bytes"]).to_string(),
                cache_limit: number(&cache["limit"]).to_string(),
                upload_bytes: number(&cache["upload_bytes"]).to_string(),
                upload_limit: number(&cache["upload_limit"]).to_string(),
                multipart_bytes: number(&cache["multipart_bytes"]).to_string(),
                thumbnail_bytes: number(&cache["thumbnail_bytes"]).to_string(),
                rss_bytes: s["process_memory"]["rss_bytes"]
                    .as_u64()
                    .map(|v| v.to_string()),
            },
            db_pool_size: number(&s["db_pool_size"]),
            db_pool_idle: number(&s["db_pool_idle"]),
            queue_running: queues.map(|q| q.values().map(|v| number(&v["running"])).sum()),
            queue_waiting: queues.map(|q| {
                q.values()
                    .filter_map(|v| v["queued"].as_object())
                    .flat_map(|v| v.values())
                    .map(number)
                    .sum()
            }),
        })
    }
    pub fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            self.sequence > 0 && self.uptime_seconds <= i64::MAX as u64,
            "invalid runtime sample"
        );
        anyhow::ensure!(
            serde_json::to_vec(self)?.len() <= 16 * 1024,
            "runtime sample is too large"
        );
        for value in self
            .counters()
            .into_iter()
            .chain(self.gauges())
            .chain(self.point.rss_bytes.as_deref())
        {
            value.parse::<u64>()?;
        }
        Ok(())
    }
    fn counters(&self) -> [&str; 6] {
        let p = &self.point;
        [
            &p.requests,
            &p.failed,
            &p.backend_read_bytes,
            &p.backend_write_bytes,
            &p.cache_hits,
            &p.cache_lookups,
        ]
    }
    fn gauges(&self) -> [&str; 6] {
        let p = &self.point;
        [
            &p.cache_bytes,
            &p.cache_limit,
            &p.upload_bytes,
            &p.upload_limit,
            &p.multipart_bytes,
            &p.thumbnail_bytes,
        ]
    }
    fn delta(&self, previous: &Self, max_seconds: u64) -> Option<([u64; 6], u64)> {
        let seconds = self.uptime_seconds.checked_sub(previous.uptime_seconds)?;
        if self.boot_id != previous.boot_id
            || self.sequence <= previous.sequence
            || seconds == 0
            || seconds > max_seconds
        {
            return None;
        }
        let mut values = [0; 6];
        for ((value, current), previous) in values
            .iter_mut()
            .zip(self.counters())
            .zip(previous.counters())
        {
            *value = current
                .parse::<u64>()
                .ok()?
                .checked_sub(previous.parse::<u64>().ok()?)?;
        }
        Some((values, seconds))
    }
}

#[derive(Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct NodeRuntimePoint {
    pub node_id: Uuid,
    pub received_at: DateTime<Utc>,
    #[serde(flatten)]
    pub sample: NodeSample,
}
#[derive(Default, Serialize, Deserialize, ToSchema)]
pub(crate) struct RuntimeGauges {
    pub cache_bytes: String,
    pub cache_limit: String,
    pub upload_bytes: String,
    pub upload_limit: String,
    pub multipart_bytes: String,
    pub thumbnail_bytes: String,
    pub rss_bytes: Option<String>,
    pub db_pool_size: String,
    pub db_pool_idle: String,
    pub queue_running: Option<String>,
    pub queue_waiting: Option<String>,
}
#[derive(Default, Serialize, Deserialize, ToSchema)]
pub(crate) struct RuntimeRates {
    pub requests: f64,
    pub failed: f64,
    pub backend_read_bytes: f64,
    pub backend_write_bytes: f64,
    pub cache_hit_ratio: Option<f64>,
}
#[derive(Serialize, Deserialize, ToSchema)]
pub(crate) struct ClusterRuntimePoint {
    pub scope: String,
    pub at: DateTime<Utc>,
    #[serde(default = "runtime_interval")]
    pub interval_seconds: u64,
    pub window_start: DateTime<Utc>,
    pub expected_nodes: i64,
    pub reported_nodes: usize,
    pub rate_nodes: usize,
    pub gauges: Option<RuntimeGauges>,
    pub rates: Option<RuntimeRates>,
}
fn runtime_interval() -> u64 {
    60
}
impl ClusterRuntimePoint {
    pub fn stale_at(&self, now: DateTime<Utc>) -> bool {
        now.signed_duration_since(self.at).num_seconds()
            > self.interval_seconds.saturating_mul(2).saturating_add(30) as i64
    }
}
#[derive(Serialize, ToSchema)]
pub(crate) struct ClusterRuntime {
    pub current: Option<ClusterRuntimePoint>,
    pub history: Vec<ClusterRuntimePoint>,
    pub stale: bool,
}
#[derive(Serialize, ToSchema)]
pub(crate) struct NodeRuntime {
    pub history: Vec<NodeRuntimePoint>,
}

// Samples are newest first per node, bounded to two observations by the backend.
pub(crate) fn aggregate_runtime(
    at: DateTime<Utc>,
    expected: i64,
    interval: u64,
    samples: &[NodeRuntimePoint],
) -> Result<ClusterRuntimePoint> {
    let mut out = ClusterRuntimePoint {
        scope: "cluster".into(),
        at,
        interval_seconds: interval,
        window_start: at,
        expected_nodes: expected,
        reported_nodes: 0,
        rate_nodes: 0,
        gauges: None,
        rates: None,
    };
    let mut totals = [0u128; 10];
    let mut rss = 0u128;
    let mut memory_known = true;
    let mut queues_known = [true; 2];
    let mut rates = [0f64; 4];
    let mut hits = 0u128;
    let mut lookups = 0u128;
    let mut seen = std::collections::HashSet::new();
    for (i, current) in samples.iter().enumerate() {
        if !seen.insert(current.node_id) {
            continue;
        }
        current.sample.validate()?;
        let age = at.signed_duration_since(current.received_at).num_seconds();
        if age < 0 || age > interval.saturating_mul(2).saturating_add(30) as i64 {
            continue;
        }
        out.reported_nodes += 1;
        for (sum, value) in totals.iter_mut().zip(current.sample.gauges()) {
            *sum += value.parse::<u64>()? as u128;
        }
        for (sum, value) in totals[6..].iter_mut().zip([
            current.sample.db_pool_size,
            current.sample.db_pool_idle,
            current.sample.queue_running.unwrap_or(0),
            current.sample.queue_waiting.unwrap_or(0),
        ]) {
            *sum += value as u128;
        }
        queues_known[0] &= current.sample.queue_running.is_some();
        queues_known[1] &= current.sample.queue_waiting.is_some();
        if let Some(value) = &current.sample.point.rss_bytes {
            rss += value.parse::<u64>()? as u128;
        } else {
            memory_known = false;
        }
        let Some(previous) = samples
            .get(i + 1)
            .filter(|p| p.node_id == current.node_id && p.received_at < current.received_at)
        else {
            continue;
        };
        let Some((delta, seconds)) = current
            .sample
            .delta(&previous.sample, interval.saturating_mul(3))
        else {
            continue;
        };
        if current
            .received_at
            .signed_duration_since(previous.received_at)
            .num_seconds()
            > interval.saturating_mul(3).saturating_add(30) as i64
        {
            continue;
        }
        out.rate_nodes += 1;
        out.window_start = out.window_start.min(previous.received_at);
        for (rate, delta) in rates.iter_mut().zip(delta) {
            *rate += delta as f64 / seconds as f64;
        }
        hits += delta[4] as u128;
        lookups += delta[5] as u128;
    }
    if out.reported_nodes > 0 {
        out.gauges = Some(RuntimeGauges {
            cache_bytes: totals[0].to_string(),
            cache_limit: totals[1].to_string(),
            upload_bytes: totals[2].to_string(),
            upload_limit: totals[3].to_string(),
            multipart_bytes: totals[4].to_string(),
            thumbnail_bytes: totals[5].to_string(),
            rss_bytes: memory_known.then(|| rss.to_string()),
            db_pool_size: totals[6].to_string(),
            db_pool_idle: totals[7].to_string(),
            queue_running: queues_known[0].then(|| totals[8].to_string()),
            queue_waiting: queues_known[1].then(|| totals[9].to_string()),
        });
    }
    if out.rate_nodes > 0 {
        out.rates = Some(RuntimeRates {
            requests: rates[0],
            failed: rates[1],
            backend_read_bytes: rates[2],
            backend_write_bytes: rates[3],
            cache_hit_ratio: (lookups > 0).then(|| (hits as f64 / lookups as f64).min(1.0)),
        });
    }
    Ok(out)
}

#[cfg(test)]
mod runtime_tests {
    use super::*;
    pub(super) fn sample(
        id: Uuid,
        boot: Uuid,
        sequence: i64,
        seconds: u64,
        requests: u64,
        hits: u64,
        lookups: u64,
    ) -> NodeRuntimePoint {
        let at = Utc::now();
        NodeRuntimePoint {
            node_id: id,
            received_at: at,
            sample: NodeSample {
                boot_id: boot,
                sequence,
                uptime_seconds: seconds,
                interval_seconds: 60,
                point: RuntimePoint {
                    at,
                    epoch: at,
                    requests: requests.to_string(),
                    failed: "0".into(),
                    backend_read_bytes: (requests * 100).to_string(),
                    backend_write_bytes: "0".into(),
                    cache_hits: hits.to_string(),
                    cache_lookups: lookups.to_string(),
                    cache_bytes: "10".into(),
                    cache_limit: "100".into(),
                    upload_bytes: "5".into(),
                    upload_limit: "50".into(),
                    multipart_bytes: "3".into(),
                    thumbnail_bytes: "2".into(),
                    rss_bytes: Some("1000".into()),
                },
                db_pool_size: 3,
                db_pool_idle: 2,
                queue_running: Some(1),
                queue_waiting: Some(4),
            },
        }
    }
    #[test]
    fn cluster_runtime_weights_deltas_and_preserves_missing_coverage() -> Result<()> {
        let at = Utc::now();
        let point = aggregate_runtime(at, 1, 300, &[])?;
        assert!(!point.stale_at(at + chrono::Duration::seconds(400)));
        assert!(point.stale_at(at + chrono::Duration::seconds(631)));
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let boot = Uuid::new_v4();
        let at = Utc::now();
        let mut points = vec![
            sample(a, boot, 2, 120, 60, 1, 1),
            sample(a, boot, 1, 60, 0, 0, 0),
            sample(b, boot, 2, 120, 300, 9, 99),
            sample(b, boot, 1, 60, 0, 0, 0),
        ];
        for (i, p) in points.iter_mut().enumerate() {
            p.received_at = at - chrono::Duration::seconds(if i % 2 == 0 { 1 } else { 61 });
        }
        let out = aggregate_runtime(at, 3, 60, &points)?;
        assert_eq!(
            (out.expected_nodes, out.reported_nodes, out.rate_nodes),
            (3, 2, 2)
        );
        let rates = out.rates.unwrap();
        assert_eq!(rates.requests, 6.0);
        assert_eq!(rates.backend_read_bytes, 600.0);
        assert_eq!(rates.cache_hit_ratio, Some(0.1));
        let gauges = out.gauges.unwrap();
        assert_eq!(gauges.cache_bytes, "20");
        assert_eq!(gauges.rss_bytes.as_deref(), Some("2000"));
        assert_eq!(gauges.queue_waiting.as_deref(), Some("8"));
        points[2].sample.queue_waiting = None;
        assert!(
            aggregate_runtime(at, 3, 60, &points)?
                .gauges
                .unwrap()
                .queue_waiting
                .is_none()
        );
        points[2].sample.boot_id = Uuid::new_v4();
        let out = aggregate_runtime(at, 3, 60, &points)?;
        assert_eq!(out.rate_nodes, 1);
        assert_eq!(out.rates.unwrap().requests, 1.0);
        points[2].received_at = at - chrono::Duration::seconds(151);
        assert_eq!(aggregate_runtime(at, 3, 60, &points)?.reported_nodes, 1);
        points[0].sample.point.requests = "0".into();
        points[1].sample.point.requests = "1".into();
        let out = aggregate_runtime(at, 3, 60, &points)?;
        assert_eq!(out.rate_nodes, 0);
        assert!(out.rates.is_none());
        assert!(aggregate_runtime(at, 3, 60, &[])?.gauges.is_none());
        Ok(())
    }
    #[test]
    fn cluster_runtime_rejects_reset_gaps_and_unbounded_queries() -> Result<()> {
        let id = Uuid::new_v4();
        let boot = Uuid::new_v4();
        let mut first = sample(id, boot, 1, 60, 10, 1, 10);
        first.received_at = Utc::now() - chrono::Duration::seconds(60);
        let mut next = sample(id, boot, 2, 60, 20, 2, 20);
        let at = next.received_at;
        assert!(
            aggregate_runtime(at, 1, 60, &[next.clone(), first.clone()])?
                .rates
                .is_none()
        );
        next.sample.uptime_seconds = 600;
        assert!(
            aggregate_runtime(at, 1, 60, &[next.clone(), first.clone()])?
                .rates
                .is_none()
        );
        next.sample.uptime_seconds = 120;
        next.sample.point.rss_bytes = None;
        assert!(
            aggregate_runtime(at, 1, 60, &[next, first])?
                .gauges
                .unwrap()
                .rss_bytes
                .is_none()
        );
        assert!(
            RuntimeQuery {
                hours: 169,
                limit: 2048
            }
            .validate()
            .is_err()
        );
        assert!(
            RuntimeQuery {
                hours: 24,
                limit: 2049
            }
            .validate()
            .is_err()
        );
        assert!(RuntimeQuery { hours: 0, limit: 1 }.validate().is_err());
        Ok(())
    }
}
