use super::seconds;
use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, RwLock},
};
use uuid::Uuid;

#[derive(Clone, Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Instance {
    pub deployment_key: Option<String>,
    pub name: Option<String>,
    pub listen_control: String,
    pub listen_web: String,
    pub listen_s3: String,
    pub data_path: PathBuf,
    pub min_free_space: String,
    pub multipart_limit: Option<String>,
    pub cache_size: Option<String>,
    pub cache_entries: Option<usize>,
    pub upload_cache_size: Option<String>,
    pub thumbnail_cache_size: String,
    pub request_connections: Option<usize>,
    pub database_connections: Option<u32>,
    pub cpu_jobs: Option<usize>,
    pub stream_memory_budget: Option<String>,
    pub storage_read_slots: Option<usize>,
    pub storage_upload_slots: Option<usize>,
    pub storage_control_slots: Option<usize>,
    pub compression_idle_timeout: String,
}

impl Default for Instance {
    fn default() -> Self {
        Self {
            deployment_key: None,
            name: None,
            listen_s3: "0.0.0.0:9000".into(),
            listen_web: "0.0.0.0:9001".into(),
            listen_control: "0.0.0.0:9002".into(),
            data_path: "/data".into(),
            min_free_space: "1GiB".into(),
            multipart_limit: None,
            cache_size: None,
            cache_entries: None,
            upload_cache_size: None,
            thumbnail_cache_size: "256MiB".into(),
            request_connections: None,
            database_connections: None,
            cpu_jobs: None,
            stream_memory_budget: None,
            storage_read_slots: None,
            storage_upload_slots: None,
            storage_control_slots: None,
            compression_idle_timeout: "30s".into(),
        }
    }
}

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RuntimeSettings {
    pub web: WebSettings,
    pub requests: RequestSettings,
    pub uploads: UploadSettings,
    pub metrics: MetricsSettings,
    pub maintenance: Maintenance,
}

impl Default for RuntimeSettings {
    fn default() -> Self {
        Self {
            web: WebSettings::default(),
            requests: RequestSettings::default(),
            uploads: UploadSettings::default(),
            metrics: MetricsSettings::default(),
            maintenance: Default::default(),
        }
    }
}

#[derive(Clone, Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct MetricsSettings {
    pub scope_limit: usize,
    pub storage_interval: String,
    pub storage_retention: String,
    pub runtime_interval: String,
    pub runtime_retention: String,
    pub refresh_interval: String,
    pub query_timeout: String,
    pub access_flush_interval: String,
    pub access_retention: String,
}
impl Default for MetricsSettings {
    fn default() -> Self {
        Self {
            scope_limit: 64,
            storage_interval: "15m".into(),
            storage_retention: "7d".into(),
            runtime_interval: "1m".into(),
            runtime_retention: "24h".into(),
            refresh_interval: "15m".into(),
            query_timeout: "2m".into(),
            access_flush_interval: "1m".into(),
            access_retention: "14d".into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DatabaseType {
    #[default]
    Postgres,
    Mysql,
}

impl DatabaseType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::Mysql => "mysql",
        }
    }

    pub fn default_port(self) -> Option<u16> {
        match self {
            Self::Postgres => Some(5432),
            Self::Mysql => Some(3306),
        }
    }

    pub fn validate_role(self, role: &str) -> Result<()> {
        ensure!(
            role == "single" || self.supports_cluster(),
            "database backend '{}' does not support cluster mode",
            self.as_str()
        );
        Ok(())
    }

    pub fn supports_cluster(self) -> bool {
        self == Self::Postgres
    }
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Database {
    #[serde(rename = "type", default)]
    pub backend: DatabaseType,
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub user: String,
    pub password: Option<String>,
    pub password_file: Option<PathBuf>,
    #[serde(default = "postgres_ssl_mode")]
    pub ssl_mode: String,
}

impl Database {
    pub fn effective_port(&self) -> Option<u16> {
        self.backend
            .default_port()
            .map(|port| self.port.unwrap_or(port))
    }
}

fn postgres_ssl_mode() -> String {
    "prefer".into()
}
#[derive(Clone, Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct BackendDefaults {
    #[serde(default = "connect_timeout")]
    pub connect_timeout: String,
    #[serde(default = "request_timeout")]
    pub request_timeout: String,
    #[serde(default = "backend_retries")]
    pub max_retries: usize,
    #[serde(default = "retry_timeout")]
    pub retry_timeout: String,
    #[serde(default = "priority_aging")]
    pub priority_aging: String,
    #[serde(default = "no_minimum_storage_duration")]
    pub min_storage_duration: String,
}
impl Default for BackendDefaults {
    fn default() -> Self {
        Self {
            connect_timeout: connect_timeout(),
            request_timeout: request_timeout(),
            max_retries: backend_retries(),
            retry_timeout: retry_timeout(),
            priority_aging: priority_aging(),
            min_storage_duration: no_minimum_storage_duration(),
        }
    }
}
impl BackendDefaults {
    pub fn min_storage_seconds(&self) -> Result<u64> {
        if self.min_storage_duration == "0s" {
            Ok(0)
        } else {
            seconds(&self.min_storage_duration).context("invalid storage.min_storage_duration")
        }
    }
}
fn no_minimum_storage_duration() -> String {
    "0s".into()
}
fn connect_timeout() -> String {
    "5s".into()
}
fn request_timeout() -> String {
    "30s".into()
}
fn retry_timeout() -> String {
    "120s".into()
}
fn priority_aging() -> String {
    "30s".into()
}
fn backend_retries() -> usize {
    3
}
#[derive(Clone, Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct WebSettings {
    pub origin: String,
    pub secure_cookie: bool,
    pub session_lifetime: String,
    pub gravatar_base_url: String,
}
impl Default for WebSettings {
    fn default() -> Self {
        Self {
            origin: "http://localhost:9002".into(),
            secure_cookie: true,
            session_lifetime: "12h".into(),
            gravatar_base_url: "https://www.gravatar.com/avatar/".into(),
        }
    }
}
impl WebSettings {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.origin.starts_with("https://") || self.origin.starts_with("http://"),
            "control.origin must be an absolute HTTP origin"
        );
        ensure!(
            !self.origin.ends_with('/'),
            "control.origin must not have a trailing slash"
        );
        self.gravatar_origin()?;
        ensure!(
            (1..=30 * 86400).contains(&seconds(&self.session_lifetime)?),
            "control.session_lifetime must be 1s..30d"
        );
        Ok(())
    }

    pub fn gravatar_origin(&self) -> Result<String> {
        let uri: hyper::Uri = self
            .gravatar_base_url
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid control.gravatar_base_url"))?;
        let authority = uri.authority().context("Gravatar URL requires a host")?;
        ensure!(
            uri.scheme_str() == Some("https")
                && !authority.as_str().contains('@')
                && authority
                    .as_str()
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b".-_:[]".contains(&b))
                && uri.query().is_none()
                && !self.gravatar_base_url.contains('#'),
            "Gravatar URL requires an HTTPS host without credentials, query parameters or fragments"
        );
        Ok(format!("https://{authority}"))
    }
}
#[derive(Clone, Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RequestSettings {
    pub s3_domain: Option<String>,
    pub s3_region: String,
    pub s3_chunk_limit: String,
    #[serde(default = "data_idle")]
    pub read_idle_timeout: String,
    #[serde(default = "data_idle")]
    pub upload_idle_timeout: String,
}
fn data_idle() -> String {
    "120s".into()
}
impl Default for RequestSettings {
    fn default() -> Self {
        Self {
            s3_domain: None,
            s3_region: "us-east-1".into(),
            s3_chunk_limit: "8MiB".into(),
            read_idle_timeout: data_idle(),
            upload_idle_timeout: data_idle(),
        }
    }
}
#[derive(Clone, Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct UploadSettings {
    pub staging_storage_id: Option<uuid::Uuid>,
    pub staging_bytes_limit: Option<String>,
    pub idle_timeout: String,
    pub sweep_interval: String,
    pub client_idle_timeout: String,
    pub max_active_uploads: Option<i64>,
}
impl Default for UploadSettings {
    fn default() -> Self {
        Self {
            staging_storage_id: None,
            staging_bytes_limit: None,
            idle_timeout: "24h".into(),
            sweep_interval: "5m".into(),
            client_idle_timeout: "120s".into(),
            max_active_uploads: None,
        }
    }
}
#[derive(Clone, Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct LocalCache {
    pub upload_cache: bool,
    pub min_compression_savings_percent: u8,
}
impl Default for LocalCache {
    fn default() -> Self {
        Self {
            upload_cache: true,
            min_compression_savings_percent: 20,
        }
    }
}
#[derive(Clone, Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Maintenance {
    pub interval: String,
    pub batch_size: u32,
    pub max_duration: String,
    pub task_retention: String,
    pub audit_retention: String,
    // These storage-scoped defaults remain in memory until storage profiles load.
    pub unreferenced_grace: String,
    pub deleted_chunk_retention: String,
    pub upload_retention: String,
}
impl Default for Maintenance {
    fn default() -> Self {
        Self {
            interval: "30m".into(),
            batch_size: 1000,
            max_duration: "5s".into(),
            task_retention: "30d".into(),
            audit_retention: "90d".into(),
            unreferenced_grace: "48h".into(),
            deleted_chunk_retention: "7d".into(),
            upload_retention: "24h".into(),
        }
    }
}

pub struct Secrets {
    pub database_password: String,
    pub credential_key: crate::content::codec::Key,
    pub keys: BTreeMap<String, crate::content::codec::Key>,
    pub runtime_keys: Arc<RwLock<BTreeMap<String, crate::content::codec::Key>>>,
    pub key_names: Arc<RwLock<BTreeMap<Uuid, String>>>,
}

impl Secrets {
    pub fn key(&self, name: &str) -> Option<crate::content::codec::Key> {
        if let Some(key) = self.runtime_keys.read().ok()?.get(name) {
            return Some(key.clone());
        }
        self.keys.get(name).cloned()
    }

    pub fn key_name(&self, id: Option<Uuid>) -> Option<String> {
        let id = id?;
        self.key_names.read().ok()?.get(&id).cloned()
    }

    pub fn key_for_id(&self, id: Option<Uuid>) -> Option<crate::content::codec::Key> {
        self.key_name(id).and_then(|name| self.key(&name))
    }

    pub fn publish_key(&self, name: String, key: crate::content::codec::Key) -> Result<()> {
        self.publish_key_with_id(None, name, key)
    }

    pub fn publish_key_with_id(
        &self,
        id: Option<Uuid>,
        name: String,
        key: crate::content::codec::Key,
    ) -> Result<()> {
        if let Some(id) = id {
            self.key_names
                .write()
                .map_err(|_| anyhow::anyhow!("key registry is unavailable"))?
                .insert(id, name.clone());
        }
        self.runtime_keys
            .write()
            .map_err(|_| anyhow::anyhow!("key registry is unavailable"))?
            .insert(name, key);
        Ok(())
    }
}
