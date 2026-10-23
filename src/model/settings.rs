use crate::authorization::Principal;
use crate::config::{Config, WebSettings};
use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::collections::HashMap;
use utoipa::ToSchema;

#[async_trait]
pub(crate) trait RuntimeSettingsStore: Send + Sync {
    async fn revisions(&self) -> Result<Vec<(String, i64)>>;
    async fn inspect(&self) -> Result<Vec<(String, Value, i64, bool)>>;
    async fn row(&self, section: &str) -> Result<RuntimeSettingsRow>;
    async fn sections(&self, principal: &Principal) -> Result<Vec<(String, Value, i64)>>;
    async fn section(&self, principal: &Principal, section: &str) -> Result<RuntimeSettingsRow>;
    async fn save(
        &self,
        principal: &Principal,
        section: &str,
        revision: Option<i64>,
        value: Value,
    ) -> Result<RuntimeSettingsRow>;
    async fn reset(
        &self,
        principal: &Principal,
        section: &str,
        value: Value,
    ) -> Result<RuntimeSettingsRow>;
    async fn update_web(
        &self,
        principal: &Principal,
        input: ControlSettingsPatch,
    ) -> Result<(WebSettings, i64)>;
    async fn ensure(&self, defaults: &[(&str, Value)]) -> Result<()>;
    async fn load(&self) -> Result<Vec<(String, Value, i64)>>;
    async fn value(&self, section: &str) -> Result<Value>;
}

pub(crate) async fn load_runtime_settings(
    store: &dyn RuntimeSettingsStore,
    config: &mut Config,
) -> Result<HashMap<String, i64>> {
    let mut revisions = HashMap::new();
    for (section, value, revision) in store.load().await? {
        apply_runtime_section(config, &section, value)?;
        revisions.insert(section, revision);
    }
    config.validate()?;
    Ok(revisions)
}

fn overlay<T>(current: &T, patch: &Value) -> Result<T>
where
    T: Serialize + DeserializeOwned,
{
    let mut value = serde_json::to_value(current)?;
    let object = value
        .as_object_mut()
        .context("runtime settings target must be an object")?;
    let patch = patch
        .as_object()
        .context("runtime settings value must be an object")?;
    for (key, value) in patch {
        object.insert(key.clone(), value.clone());
    }
    Ok(serde_json::from_value(value)?)
}

pub fn runtime_defaults(configured: &Config) -> Vec<(&'static str, Value)> {
    let mut values = vec![
        (
            "web",
            json!({
                "origin": configured.web.origin.clone(),
                "secure_cookie": configured.web.secure_cookie,
                "session_lifetime": configured.web.session_lifetime.clone(),
                "gravatar_base_url": configured.web.gravatar_base_url.clone(),
            }),
        ),
        (
            "requests",
            json!({
                "s3_domain": configured.requests.s3_domain.clone(),
                "s3_region": configured.requests.s3_region.clone(),
                "s3_chunk_limit": configured.requests.s3_chunk_limit.clone(),
                "cors_cache_size": configured.requests.cors_cache_size.clone(),
                "cors_cache_entries": configured.requests.cors_cache_entries,
                "read_idle_timeout": configured.requests.read_idle_timeout.clone(),
                "upload_idle_timeout": configured.requests.upload_idle_timeout.clone(),
            }),
        ),
        (
            "uploads",
            json!({
                "sweep_interval": configured.uploads.sweep_interval.clone(),
                "client_idle_timeout": configured.uploads.client_idle_timeout.clone(),
                "max_active_uploads": configured.uploads.max_active_uploads,
                "staging_storage_id": configured.uploads.staging_storage_id,
                "staging_bytes_limit": configured.uploads.staging_bytes_limit.clone(),
            }),
        ),
        (
            "metrics",
            json!({
                "scope_limit": configured.metrics.scope_limit,
                "storage_interval": configured.metrics.storage_interval.clone(),
                "storage_retention": configured.metrics.storage_retention.clone(),
                "runtime_interval": configured.metrics.runtime_interval.clone(),
                "runtime_retention": configured.metrics.runtime_retention.clone(),
                "refresh_interval": configured.metrics.refresh_interval.clone(),
                "query_timeout": configured.metrics.query_timeout.clone(),
                "access_flush_interval": configured.metrics.access_flush_interval.clone(),
                "access_retention": configured.metrics.access_retention.clone(),
            }),
        ),
        (
            "maintenance",
            json!({
                "interval": configured.maintenance.interval.clone(),
                "batch_size": configured.maintenance.batch_size,
                "max_duration": configured.maintenance.max_duration.clone(),
                "task_retention": configured.maintenance.task_retention.clone(),
                "audit_retention": configured.maintenance.audit_retention.clone(),
            }),
        ),
    ];
    let mut io = json!({"shutdown_flush_timeout": configured.io.shutdown_flush_timeout});
    if configured.database.backend == crate::config::DatabaseType::Sqlite {
        io["sqlite_synchronous"] = json!(configured.io.sqlite_synchronous);
    }
    values.push(("io", io));
    if !configured.database.backend.supports_cluster() {
        let uploads = values
            .iter_mut()
            .find(|(section, _)| *section == "uploads")
            .unwrap();
        let uploads = uploads.1.as_object_mut().unwrap();
        uploads.remove("staging_storage_id");
        uploads.remove("staging_bytes_limit");
    }
    values
}

pub(crate) fn apply_runtime_section(
    config: &mut Config,
    section: &str,
    value: Value,
) -> Result<()> {
    anyhow::ensure!(
        section != "io"
            || value.get("sqlite_synchronous").is_none()
            || config.database.backend == crate::config::DatabaseType::Sqlite,
        s3s::s3_error!(
            InvalidArgument,
            "SQLite synchronization settings require SQLite"
        )
    );
    if section == "uploads"
        && !config.database.backend.supports_cluster()
        && (value.get("staging_storage_id").is_some() || value.get("staging_bytes_limit").is_some())
    {
        return Err(s3s::s3_error!(InvalidArgument, "shared staging requires PostgreSQL").into());
    }
    match section {
        "web" => config.web = overlay(&config.web, &value)?,
        "requests" => config.requests = overlay(&config.requests, &value)?,
        "uploads" => config.uploads = overlay(&config.uploads, &value)?,
        "metrics" => config.metrics = overlay(&config.metrics, &value)?,
        "maintenance" => config.maintenance = overlay(&config.maintenance, &value)?,
        "io" => config.io = overlay(&config.io, &value)?,
        _ => anyhow::bail!("unknown runtime settings section {section}"),
    }
    Ok(())
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct ControlSettingsPatch {
    pub(crate) origin: Option<String>,
    pub(crate) secure_cookie: Option<bool>,
    pub(crate) session_lifetime: Option<String>,
    pub(crate) gravatar_base_url: Option<String>,
    pub(crate) revision: Option<i64>,
}

#[derive(Clone)]
pub(crate) struct RuntimeSettingsRow {
    pub value: Value,
    pub revision: i64,
}

impl RuntimeSettingsRow {
    pub(crate) fn control(&self) -> Result<WebSettings> {
        let value = self.value.as_object().cloned().unwrap_or_default();
        let mut control = WebSettings::default();
        control.origin = value
            .get("origin")
            .and_then(Value::as_str)
            .context("runtime web origin is missing")?
            .to_owned();
        control.secure_cookie = value
            .get("secure_cookie")
            .and_then(Value::as_bool)
            .context("runtime web secure_cookie is missing")?;
        control.session_lifetime = value
            .get("session_lifetime")
            .and_then(Value::as_str)
            .context("runtime web session_lifetime is missing")?
            .to_owned();
        control.gravatar_base_url = value
            .get("gravatar_base_url")
            .and_then(Value::as_str)
            .context("runtime web gravatar_base_url is missing")?
            .to_owned();
        Ok(control)
    }
}
