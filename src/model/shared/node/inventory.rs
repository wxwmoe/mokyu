use super::{
    Backend, node_registry,
    queries::{self, node_inventory as q, node_registry as registry},
};
use crate::{
    cluster::NodeState,
    content::codec::Key,
    model::{
        Error,
        cluster::{ClusterStatus, NodeInventory, NodeView},
        metrics::{
            self, ClusterRuntimePoint, NodeRuntimePoint, NodeSample, RuntimeQuery, WorkSummary,
        },
        node::{
            CONTACT_SECONDS, NodeCount, NodeFilter, NodePage, NodeReport, REPORT_FORMAT,
            SavedReport, SettingCount,
        },
    },
};
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, TimeDelta, Utc};
use serde_json::{Value, json};
use sqlx::types::Json;
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(sqlx::FromRow)]
pub(super) struct Observation {
    id: Uuid,
    name: String,
    role: String,
    enabled: bool,
    last_seen: Option<i64>,
    created: i64,
    received: Option<i64>,
    report: Option<Json<Value>>,
    run_state: Option<Json<Value>>,
}
fn date(n: i64) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp_micros(n).context("invalid node timestamp")
}
fn fresh(time: Option<DateTime<Utc>>, now: DateTime<Utc>) -> bool {
    time.is_some_and(|t| t >= now - TimeDelta::seconds(CONTACT_SECONDS))
}
impl Observation {
    fn report(&self) -> &Value {
        self.report.as_deref().unwrap_or(&Value::Null)
    }
    fn ready(&self, now: DateTime<Utc>, revision: i64) -> Result<bool> {
        let run = self.run_state.as_deref().unwrap_or(&Value::Null);
        let status = &self.report()["status"];
        let expires = run["expires"]
            .as_str()
            .and_then(|s| s.parse::<DateTime<Utc>>().ok());
        Ok(self.enabled
            && self.role != "control"
            && run["closed"] == false
            && run["draining"] == false
            && expires.is_some_and(|v| v > now)
            && status["ready"] == true
            && status["draining"] == false
            && status["role"] == self.role
            && status["deployment_mode_revision"].as_i64() == Some(revision)
            && fresh(self.received.map(date).transpose()?, now)
            && fresh(self.last_seen.map(date).transpose()?, now))
    }
    fn view(self, now: DateTime<Utc>, desired: &Value) -> Result<NodeView> {
        let last_seen_at = self.last_seen.map(date).transpose()?;
        let received_at = self.received.map(date).transpose()?;
        let report = self.report();
        let metadata = &report["metadata"];
        let sampled_at = report["sampled_at"]
            .as_str()
            .map(str::parse::<DateTime<Utc>>)
            .transpose()?;
        let contact = if !self.enabled {
            "released"
        } else if last_seen_at.is_none() {
            "not_reported"
        } else if fresh(last_seen_at, now) {
            "online"
        } else {
            "unresponsive"
        };
        let report_fresh = contact == "online" && sampled_at.is_some() && fresh(received_at, now);
        let known = metadata["format"].as_u64() == Some(u64::from(REPORT_FORMAT));
        let work = match report["status"]["work"].as_str() {
            Some("starting") if self.enabled && fresh(received_at, now) => "starting",
            Some(value @ ("normal" | "maintenance" | "restricted" | "recovery"))
                if report_fresh && known =>
            {
                value
            }
            _ => "unknown",
        }
        .to_owned();
        let mut settings = serde_json::Map::new();
        if let Some(sections) = desired[&self.role].as_object() {
            for (section, wanted) in sections {
                let fields = wanted["value"]
                    .as_object()
                    .context("invalid desired settings")?;
                let loaded = metadata["loaded"][section].as_object();
                let state = if fields.is_empty() {
                    "not_applicable"
                } else if !report_fresh
                    || !known
                    || !loaded.is_some_and(|m| fields.keys().all(|k| m.contains_key(k)))
                {
                    "unknown"
                } else if !fields
                    .iter()
                    .all(|(k, v)| loaded.and_then(|m| m.get(k)) == Some(v))
                {
                    "restart_required"
                } else if metadata["revisions"][section] == wanted["revision"] {
                    "applied"
                } else {
                    "equivalent"
                };
                settings.insert(section.clone(), state.into());
            }
        }
        let configuration = if settings.is_empty() || settings.values().any(|v| v == "unknown") {
            "unknown"
        } else if settings.values().any(|v| v == "restart_required") {
            "restart_required"
        } else if settings.values().any(|v| v == "equivalent") {
            "equivalent"
        } else {
            "applied"
        };
        Ok(NodeView {
            id: self.id,
            name: self.name,
            role: self.role,
            enabled: self.enabled,
            created_at: date(self.created)?,
            last_seen_at,
            received_at,
            sampled_at,
            report_fresh,
            work,
            contact: contact.into(),
            configuration: configuration.into(),
            settings: Value::Object(settings),
        })
    }
}
fn matches(row: &NodeView, filter: &NodeFilter) -> bool {
    let config = filter
        .section
        .as_ref()
        .map_or(Some(row.configuration.as_str()), |k| {
            row.settings[k].as_str()
        });
    filter
        .name
        .as_ref()
        .is_none_or(|v| row.name.to_lowercase().contains(&v.to_lowercase()))
        && filter.role.as_ref().is_none_or(|v| *v == row.role)
        && filter
            .contact
            .as_ref()
            .map_or(row.enabled, |v| *v == row.contact)
        && filter.work.as_ref().is_none_or(|v| *v == row.work)
        && filter
            .configuration
            .as_deref()
            .is_none_or(|v| Some(v) == config)
}
async fn scan(
    b: &Backend,
    mut cursor: Option<Uuid>,
    mut visit: impl FnMut(Observation) -> Result<bool>,
) -> Result<()> {
    loop {
        let page = q::page(cursor, 128).fetch_all(q::pool(b)).await?;
        let more = page.len() == 128;
        for row in page {
            cursor = Some(row.id);
            if !visit(row)? {
                return Ok(());
            }
        }
        if !more {
            return Ok(());
        }
    }
}
pub(super) fn merge(
    report: &NodeReport,
    previous: Option<Value>,
    now: DateTime<Utc>,
) -> Result<Value> {
    let mut value = serde_json::to_value(report)?;
    if let Some(old) = previous.filter(|v| v["boot_id"] == json!(report.boot_id)) {
        value["started_at"] = old["started_at"].clone();
        if report.metadata.is_none() {
            value["metadata"] = old["metadata"].clone();
        }
    }
    if value["started_at"].is_null() {
        value["started_at"] = report
            .status
            .pointer("/runtime/started_at")
            .cloned()
            .unwrap_or(json!(now));
    }
    Ok(value)
}
pub(super) async fn record_sample(
    tx: &mut registry::Tx<'_>,
    id: Uuid,
    sample: &NodeSample,
) -> Result<bool> {
    sample.validate()?;
    let at = queries::pack_work::clock(&mut **tx).await?;
    let previous = q::last_sample(id)
        .fetch_one(&mut **tx)
        .await?
        .map(date)
        .transpose()?;
    if previous
        .is_some_and(|t| at.signed_duration_since(t).num_seconds() < sample.interval_seconds as i64)
    {
        return Ok(false);
    }
    q::sample(id, sample).execute(&mut **tx).await?;
    Ok(true)
}
fn point((node_id, at, sample): (Uuid, i64, Json<NodeSample>)) -> Result<NodeRuntimePoint> {
    Ok(NodeRuntimePoint {
        node_id,
        received_at: date(at)?,
        sample: sample.0,
    })
}
#[async_trait::async_trait]
impl NodeInventory for Backend {
    async fn begin_report(
        &self,
        state: &NodeState,
        role: &str,
        key: &Key,
        boot: Uuid,
        started: DateTime<Utc>,
    ) -> Result<()> {
        ensure!(
            role == "control" && state.role == role,
            Error::NodeAuthentication
        );
        let mut tx = registry::begin(self).await?;
        node_registry::authenticated(&mut tx, state, key).await?;
        q::primary(state.node_id,&json!({"boot_id":boot,"started_at":started,"sequence":0,"status":{"work":"starting"},"metadata":{}})).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn report(
        &self,
        state: &NodeState,
        role: &str,
        key: &Key,
        report: &NodeReport,
    ) -> Result<bool> {
        report.validate()?;
        ensure!(
            state.role == role && role == "control",
            Error::NodeAuthentication
        );
        let mut tx = registry::begin(self).await?;
        node_registry::authenticated(&mut tx, state, key).await?;
        let old = registry::report(state.node_id)
            .fetch_one(&mut *tx)
            .await?
            .ok_or(Error::NodeExpired)?;
        ensure!(old["boot_id"] == json!(report.boot_id), Error::NodeExpired);
        if old["sequence"].as_i64().unwrap_or(0) >= report.sequence {
            return Ok(false);
        }
        let now = queries::pack_work::clock(&mut tx).await?;
        q::primary(state.node_id, &merge(report, Some(old), now)?)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(true)
    }
    async fn status(&self) -> Result<ClusterStatus> {
        let (mode, mode_revision, remote_work, local_storages) =
            q::status().fetch_one(q::pool(self)).await?;
        let mut status = ClusterStatus {
            mode,
            mode_revision,
            remote_work,
            local_storages,
            enabled_nodes: 0,
            remote_nodes: 0,
            ready_data: 0,
            ready_workers: 0,
        };
        let at = Utc::now();
        scan(self, None, |r| {
            if r.enabled {
                status.enabled_nodes += 1;
                if r.role != "control" {
                    status.remote_nodes += 1
                }
                if r.ready(at, mode_revision)? {
                    status.ready_data += i64::from(matches!(r.role.as_str(), "data" | "node"));
                    status.ready_workers += i64::from(matches!(r.role.as_str(), "worker" | "node"));
                }
            }
            Ok(true)
        })
        .await?;
        Ok(status)
    }
    async fn nodes(&self, filter: &NodeFilter, desired: &Value) -> Result<NodePage> {
        let mut items = vec![];
        let at = Utc::now();
        let limit = usize::try_from(filter.limit)?;
        scan(self, filter.cursor()?, |r| {
            let row = r.view(at, desired)?;
            if matches(&row, filter) {
                items.push(row)
            }
            Ok(items.len() <= limit)
        })
        .await?;
        let more = items.len() > limit;
        items.truncate(limit);
        let next_cursor = if more {
            items.last().map(|v| filter.next(v.id)).transpose()?
        } else {
            None
        };
        Ok(NodePage { items, next_cursor })
    }
    async fn node(&self, id: Uuid, desired: &Value) -> Result<Option<NodeView>> {
        q::node(id)
            .fetch_optional(q::pool(self))
            .await?
            .map(|r| r.view(Utc::now(), desired))
            .transpose()
    }
    async fn node_counts(&self, desired: &Value) -> Result<Vec<NodeCount>> {
        let at = Utc::now();
        let mut groups = BTreeMap::new();
        scan(self, None, |r| {
            if r.enabled {
                let n = r.view(at, desired)?;
                *groups
                    .entry((n.role, n.contact, n.work, n.configuration))
                    .or_insert(0u64) += 1
            }
            Ok(true)
        })
        .await?;
        Ok(groups
            .into_iter()
            .map(|((role, contact, work, configuration), n)| NodeCount {
                role,
                contact,
                work,
                configuration,
                count: n.to_string(),
            })
            .collect())
    }
    async fn setting_counts(&self, desired: &Value) -> Result<Vec<SettingCount>> {
        let at = Utc::now();
        let mut groups = BTreeMap::new();
        scan(self, None, |r| {
            if r.enabled {
                let n = r.view(at, desired)?;
                for (section, state) in n
                    .settings
                    .as_object()
                    .context("invalid setting observation")?
                {
                    *groups
                        .entry((
                            section.clone(),
                            state.as_str().context("invalid setting state")?.to_owned(),
                        ))
                        .or_insert(0u64) += 1
                }
            }
            Ok(true)
        })
        .await?;
        Ok(groups
            .into_iter()
            .map(|((section, state), n)| SettingCount {
                section,
                state,
                count: n.to_string(),
            })
            .collect())
    }
    async fn node_report(&self, id: Uuid) -> Result<Option<SavedReport>> {
        let Some(row) = q::node(id).fetch_optional(q::pool(self)).await? else {
            return Ok(None);
        };
        let Some(v) = row.report else { return Ok(None) };
        Ok(Some(SavedReport {
            boot_id: serde_json::from_value(v["boot_id"].clone())?,
            started_at: serde_json::from_value(v["started_at"].clone())?,
            metadata: v["metadata"].clone(),
            status: v["status"].clone(),
        }))
    }
    async fn node_tasks(&self, id: Uuid) -> Result<Vec<WorkSummary>> {
        Ok(q::tasks(id).fetch_all(q::pool(self)).await?)
    }
    async fn sample_node_runtime(
        &self,
        state: &NodeState,
        role: &str,
        key: &Key,
        sample: &NodeSample,
    ) -> Result<bool> {
        ensure!(
            state.role == role && role == "control",
            Error::NodeAuthentication
        );
        let mut tx = registry::begin(self).await?;
        node_registry::authenticated(&mut tx, state, key).await?;
        let saved = registry::report(state.node_id)
            .fetch_one(&mut *tx)
            .await?
            .ok_or(Error::NodeExpired)?;
        ensure!(
            saved["boot_id"] == json!(sample.boot_id) && saved["sequence"] == sample.sequence,
            Error::NodeExpired
        );
        let accepted = record_sample(&mut tx, state.node_id, sample).await?;
        tx.commit().await?;
        Ok(accepted)
    }
    async fn aggregate_runtime(&self, interval: u64, retention: u64) -> Result<()> {
        let mut tx = registry::begin(self).await?;
        let at = queries::pack_work::clock(&mut tx).await?;
        let last = q::last_aggregate()
            .fetch_one(&mut *tx)
            .await?
            .map(date)
            .transpose()?;
        if last.is_some_and(|last| at.signed_duration_since(last).num_seconds() < interval as i64) {
            return Ok(());
        }
        let since = (at
            - TimeDelta::seconds((interval.saturating_mul(3).saturating_add(30)) as i64))
        .timestamp_micros();
        let expected = q::enabled().fetch_one(&mut *tx).await?;
        let samples = q::window(since)
            .fetch_all(&mut *tx)
            .await?
            .into_iter()
            .map(point)
            .collect::<Result<Vec<_>>>()?;
        let totals = metrics::aggregate_runtime(at, expected, interval, &samples)?;
        q::aggregate(&totals).execute(&mut *tx).await?;
        let before = (at - TimeDelta::seconds(retention as i64)).timestamp_micros();
        q::prune_nodes(before).execute(&mut *tx).await?;
        q::prune_totals(before).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn cluster_runtime(&self, query: &RuntimeQuery) -> Result<Vec<ClusterRuntimePoint>> {
        query.validate()?;
        let since = (Utc::now() - TimeDelta::hours(i64::from(query.hours))).timestamp_micros();
        let rows = q::history(since, query.limit)
            .fetch_all(q::pool(self))
            .await?;
        Ok(rows.into_iter().rev().map(|r| r.0).collect())
    }
    async fn node_runtime(&self, id: Uuid, query: &RuntimeQuery) -> Result<Vec<NodeRuntimePoint>> {
        query.validate()?;
        let since = (Utc::now() - TimeDelta::hours(i64::from(query.hours))).timestamp_micros();
        q::node_samples(id, since, query.limit)
            .fetch_all(q::pool(self))
            .await?
            .into_iter()
            .rev()
            .map(point)
            .collect()
    }
}
