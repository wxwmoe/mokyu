use anyhow::{Result, ensure};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

pub(crate) const REPORT_FORMAT: u32 = 1;
pub(crate) const CONTACT_SECONDS: i64 = 30;

#[derive(Clone, Deserialize, Serialize, IntoParams)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct NodeFilter {
    pub role: Option<String>,
    pub contact: Option<String>,
    pub work: Option<String>,
    pub configuration: Option<String>,
    pub section: Option<String>,
    pub after: Option<String>,
    pub limit: i64,
}
impl Default for NodeFilter {
    fn default() -> Self {
        Self {
            role: None,
            contact: None,
            work: None,
            configuration: None,
            section: None,
            after: None,
            limit: 50,
        }
    }
}
impl NodeFilter {
    pub fn cursor(&self) -> Result<Option<Uuid>> {
        ensure!((1..=200).contains(&self.limit), "node limit must be 1..200");
        for (value, allowed) in [
            (&self.role, &["control", "data", "worker"][..]),
            (
                &self.contact,
                &["online", "unresponsive", "not_reported", "released"][..],
            ),
            (
                &self.work,
                &[
                    "normal",
                    "starting",
                    "maintenance",
                    "restricted",
                    "recovery",
                    "unknown",
                ][..],
            ),
            (
                &self.configuration,
                &[
                    "applied",
                    "equivalent",
                    "restart_required",
                    "unknown",
                    "not_applicable",
                ][..],
            ),
            (
                &self.section,
                &["web", "requests", "uploads", "metrics", "maintenance"][..],
            ),
        ] {
            ensure!(
                value
                    .as_deref()
                    .is_none_or(|value| allowed.contains(&value)),
                "invalid node filter"
            );
        }
        self.after
            .as_ref()
            .map(|token| {
                ensure!(token.len() <= 512, "invalid node cursor");
                let (scope, id): (String, Uuid) =
                    serde_json::from_slice(&URL_SAFE_NO_PAD.decode(token)?)?;
                ensure!(
                    scope == self.scope()?,
                    "node cursor belongs to another filter"
                );
                Ok(id)
            })
            .transpose()
    }
    fn scope(&self) -> Result<String> {
        let mut filter = self.clone();
        filter.after = None;
        Ok(blake3::hash(&serde_json::to_vec(&filter)?)
            .to_hex()
            .to_string())
    }
    pub fn next(&self, id: Uuid) -> Result<String> {
        Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&(self.scope()?, id))?))
    }
}

#[derive(Serialize, ToSchema)]
pub(crate) struct NodePage {
    pub items: Vec<super::cluster::NodeView>,
    pub next_cursor: Option<String>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct NodeCount {
    pub role: String,
    pub contact: String,
    pub work: String,
    pub configuration: String,
    pub count: String,
}
#[derive(Serialize, ToSchema)]
pub(crate) struct SettingCount {
    pub section: String,
    pub state: String,
    pub count: String,
}
#[derive(Serialize, ToSchema)]
pub(crate) struct ClusterOverview {
    #[serde(flatten)]
    pub deployment: super::cluster::ClusterStatus,
    pub nodes: Vec<NodeCount>,
    pub settings: Vec<SettingCount>,
    pub observed_at: DateTime<Utc>,
}
pub(crate) struct NodeReport {
    pub boot_id: Uuid,
    pub sequence: i64,
    pub sampled_at: DateTime<Utc>,
    pub metadata: Option<Value>,
    pub status: Value,
}
impl NodeReport {
    pub fn validate(&self) -> Result<()> {
        ensure!(self.sequence > 0, "report sequence must be positive");
        ensure!(
            serde_json::to_vec(&self.status)?.len() <= 30 * 1024,
            "node status is too large"
        );
        if let Some(metadata) = &self.metadata {
            ensure!(
                serde_json::to_vec(metadata)?.len() <= 60 * 1024,
                "node metadata is too large"
            );
        }
        Ok(())
    }
}

#[derive(Serialize, ToSchema)]
pub(crate) struct NodeStatus {
    pub node: super::cluster::NodeView,
    pub boot_id: Option<Uuid>,
    pub started_at: Option<DateTime<Utc>>,
    pub status: Option<Value>,
    pub tasks: Vec<super::metrics::WorkSummary>,
    pub more_tasks: bool,
}
#[derive(Serialize, ToSchema)]
pub(crate) struct NodeConfig {
    pub node: super::cluster::NodeView,
    pub fields: Vec<super::runtime::ConfigField>,
    pub revisions: Value,
}
pub(crate) struct SavedReport {
    pub boot_id: Uuid,
    pub started_at: DateTime<Utc>,
    pub metadata: Value,
    pub status: Value,
}

pub(crate) fn applicable(role: &str, section: &str, mut value: Value) -> Value {
    if section == "web" && role != "control" {
        return json!({});
    }
    if let Some(fields) = value.as_object_mut() {
        if section == "requests" && role != "data" {
            fields.remove("s3_domain");
            fields.remove("s3_region");
        }
        if section == "uploads" && role == "worker" {
            fields.remove("client_idle_timeout");
            fields.remove("max_active_uploads");
        }
        if section == "uploads" {
            fields.remove("sweep_interval");
        }
        if section == "metrics" && role != "control" {
            fields.remove("scope_limit");
        }
    }
    value
}

pub(crate) async fn desired(app: &crate::app::App) -> Result<Value> {
    let sections = app.runtime_settings.inspect().await?;
    let mut roles = serde_json::Map::new();
    for role in ["control", "data", "worker"] {
        let mut values = serde_json::Map::new();
        for (section, value, revision, _) in &sections {
            values.insert(
                section.clone(),
                json!({"revision": revision, "value": applicable(role, section, value.clone())}),
            );
        }
        roles.insert(role.into(), Value::Object(values));
    }
    Ok(Value::Object(roles))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cursor_is_bounded_and_scoped() {
        let mut filter = NodeFilter::default();
        let id = Uuid::new_v4();
        filter.after = Some(filter.next(id).unwrap());
        assert_eq!(filter.cursor().unwrap(), Some(id));
        filter.role = Some("data".into());
        assert!(filter.cursor().is_err());
        filter.after = None;
        filter.limit = 201;
        assert!(filter.cursor().is_err());
    }
    #[test]
    fn shared_budgets_and_io_are_not_data_only() {
        let requests = json!({"s3_domain":null,"s3_region":"region","s3_chunk_limit":"8MiB","read_idle_timeout":"120s","upload_idle_timeout":"120s"});
        assert_eq!(
            applicable("worker", "requests", requests.clone())
                .as_object()
                .unwrap()
                .len(),
            3
        );
        assert_eq!(applicable("data", "requests", requests.clone()), requests);
        assert_eq!(
            applicable("data", "web", json!({"origin":"example"})),
            json!({})
        );
        let uploads =
            json!({"client_idle_timeout":"120s","max_active_uploads":8,"sweep_interval":"5m"});
        assert_eq!(
            applicable("control", "uploads", uploads.clone())["max_active_uploads"],
            8
        );
        assert_eq!(applicable("worker", "uploads", uploads), json!({}));
        assert_eq!(
            applicable("worker", "metrics", json!({"query_timeout":"2m"}))["query_timeout"],
            "2m"
        );
    }
}
