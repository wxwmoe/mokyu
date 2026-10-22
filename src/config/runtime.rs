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
    pub response_cache_size: Option<String>,
    pub write_memory_size: Option<String>,
    pub upload_cache_size: Option<String>,
    pub thumbnail_cache_size: String,
    pub request_connections: Option<usize>,
    pub database_connections: Option<u32>,
    pub database_read_connections: Option<u32>,
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
            response_cache_size: None,
            write_memory_size: None,
            upload_cache_size: None,
            thumbnail_cache_size: "256MiB".into(),
            request_connections: None,
            database_connections: None,
            database_read_connections: None,
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
    pub io: IoSettings,
    pub web: WebSettings,
    pub requests: RequestSettings,
    pub uploads: UploadSettings,
    pub metrics: MetricsSettings,
    pub maintenance: Maintenance,
}

impl Default for RuntimeSettings {
    fn default() -> Self {
        Self {
            io: IoSettings::default(),
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
pub struct IoSettings {
    pub shutdown_flush_timeout: String,
    pub sqlite_synchronous: String,
}

impl Default for IoSettings {
    fn default() -> Self {
        Self {
            sqlite_synchronous: "full".into(),
            shutdown_flush_timeout: "30s".into(),
        }
    }
}

impl IoSettings {
    pub(crate) fn shutdown_seconds(&self) -> Result<u64> {
        if self.shutdown_flush_timeout == "0s" {
            return Ok(0);
        }
        let seconds = super::seconds(&self.shutdown_flush_timeout)?;
        ensure!(seconds <= 1800, "shutdown_flush_timeout must be 0s..30m");
        Ok(seconds)
    }
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            matches!(self.sqlite_synchronous.as_str(), "full" | "normal" | "off"),
            "sqlite_synchronous must be full, normal or off"
        );
        self.shutdown_seconds()?;
        Ok(())
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
    Sqlite,
}

impl DatabaseType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::Mysql => "mysql",
            Self::Sqlite => "sqlite",
        }
    }

    pub fn default_port(self) -> Option<u16> {
        match self {
            Self::Postgres => Some(5432),
            Self::Mysql => Some(3306),
            Self::Sqlite => None,
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
    #[serde(default)]
    pub path: Option<PathBuf>,
}

impl Database {
    pub fn effective_port(&self) -> Option<u16> {
        self.backend
            .default_port()
            .map(|port| self.port.unwrap_or(port))
    }

    pub fn sqlite_path(&self, data_path: &std::path::Path) -> Option<PathBuf> {
        (self.backend == DatabaseType::Sqlite).then(|| {
            self.path
                .clone()
                .unwrap_or_else(|| data_path.join("mokyu.db"))
        })
    }
}

fn postgres_ssl_mode() -> String {
    "prefer".into()
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicaDatabase {
    pub host: String,
    pub port: Option<u16>,
    pub name: Option<String>,
    pub user: Option<String>,
    pub password: Option<String>,
    pub password_file: Option<PathBuf>,
    pub ssl_mode: Option<String>,
}

impl ReplicaDatabase {
    pub fn resolve(&self, primary: &Database) -> Result<Database> {
        ensure!(
            primary.backend == DatabaseType::Postgres,
            "replica_database requires PostgreSQL"
        );
        ensure!(
            !self.host.trim().is_empty(),
            "replica_database.host is required"
        );
        ensure!(
            self.name.as_ref().is_none_or(|name| name == &primary.name),
            "replica_database.name must match database.name"
        );
        ensure!(
            self.user
                .as_ref()
                .is_none_or(|user| !user.trim().is_empty()),
            "replica_database.user must not be empty"
        );
        ensure!(
            self.port != Some(0),
            "replica_database.port must be positive"
        );
        let supplied = self.password.is_some() || self.password_file.is_some();
        ensure!(
            self.password.is_none() || self.password_file.is_none(),
            "configure only one replica database password source"
        );
        ensure!(
            self.user.is_none() || supplied,
            "replica_database.user requires password or password_file"
        );
        let ssl_mode = self.ssl_mode.as_ref().unwrap_or(&primary.ssl_mode).clone();
        ssl_mode
            .parse::<sqlx::postgres::PgSslMode>()
            .map_err(|_| anyhow::anyhow!("invalid replica_database.ssl_mode"))?;
        Ok(Database {
            backend: DatabaseType::Postgres,
            host: self.host.clone(),
            port: self.port.or(primary.effective_port()),
            name: primary.name.clone(),
            user: self.user.as_ref().unwrap_or(&primary.user).clone(),
            password: if supplied {
                self.password.clone()
            } else {
                primary.password.clone()
            },
            password_file: if supplied {
                self.password_file.clone()
            } else {
                primary.password_file.clone()
            },
            ssl_mode,
            path: None,
        })
    }
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
    pub sweep_interval: String,
    pub client_idle_timeout: String,
    pub max_active_uploads: Option<i64>,
}
impl Default for UploadSettings {
    fn default() -> Self {
        Self {
            staging_storage_id: None,
            staging_bytes_limit: None,
            sweep_interval: "5m".into(),
            client_idle_timeout: "120s".into(),
            max_active_uploads: None,
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
}
impl Default for Maintenance {
    fn default() -> Self {
        Self {
            interval: "30m".into(),
            batch_size: 1000,
            max_duration: "5s".into(),
            task_retention: "30d".into(),
            audit_retention: "90d".into(),
        }
    }
}

pub struct Secrets {
    pub database_password: String,
    pub replica_password: Option<String>,
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
        let mut names = self
            .key_names
            .write()
            .map_err(|_| anyhow::anyhow!("key registry is unavailable"))?;
        let mut keys = self
            .runtime_keys
            .write()
            .map_err(|_| anyhow::anyhow!("key registry is unavailable"))?;
        for (existing_name, existing) in self.keys.iter().chain(keys.iter()) {
            ensure!(
                if existing_name == &name {
                    existing.algorithm == key.algorithm && existing.material == key.material
                } else {
                    existing.material != key.material
                },
                "managed key identity and material must be immutable and unique"
            );
        }
        if let Some(id) = id {
            ensure!(
                names.iter().all(|(existing_id, existing_name)| {
                    (*existing_id == id) == (existing_name == &name)
                }),
                "managed key identity cannot change"
            );
            names.insert(id, name.clone());
        }
        keys.insert(name, key);
        Ok(())
    }
}
