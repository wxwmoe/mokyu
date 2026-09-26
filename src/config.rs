use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub listen: Listen,
    pub database: Database,
    pub backend: Backend,
    pub security: Security,
    #[serde(default)]
    pub manage: Manage,
    #[serde(default)]
    pub storage: Storage,
    #[serde(default)]
    pub processing: Processing,
    #[serde(default)]
    pub multipart: Multipart,
    #[serde(default)]
    pub cache: Cache,
    #[serde(default)]
    pub compression: crate::compression::Config,
    #[serde(default)]
    pub encryption: Encryption,
    #[serde(default)]
    pub gc: Gc,
    #[serde(default)]
    pub cleanup: Cleanup,
    #[serde(default)]
    pub statistics: Statistics,
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Statistics {
    pub refresh_interval: String,
    pub query_timeout: String,
}
impl Default for Statistics {
    fn default() -> Self {
        Self {
            refresh_interval: "15m".into(),
            query_timeout: "2m".into(),
        }
    }
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Listen {
    pub s3: String,
    pub web: String,
    pub manage: String,
    pub admin_socket: PathBuf,
    pub s3_domain: Option<String>,
    pub region: String,
    pub aws_chunk_limit: String,
}
impl Default for Listen {
    fn default() -> Self {
        Self {
            s3: "0.0.0.0:9000".into(),
            web: "0.0.0.0:9001".into(),
            manage: "0.0.0.0:9002".into(),
            admin_socket: "/run/media-gateway/admin.sock".into(),
            s3_domain: None,
            region: "us-east-1".into(),
            aws_chunk_limit: "8MiB".into(),
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Database {
    pub host: String,
    #[serde(default = "postgres_port")]
    pub port: u16,
    pub name: String,
    pub user: String,
    pub password: Option<String>,
    pub password_file: Option<PathBuf>,
    #[serde(default = "postgres_ssl_mode")]
    pub ssl_mode: String,
    pub max_connections: Option<u32>,
}
fn postgres_port() -> u16 {
    5432
}
fn postgres_ssl_mode() -> String {
    "prefer".into()
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Backend {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    #[serde(default)]
    pub prefix: String,
    pub access_key: Option<String>,
    pub access_key_file: Option<PathBuf>,
    pub secret_key: Option<String>,
    pub secret_key_file: Option<PathBuf>,
    #[serde(default)]
    pub allow_http: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Security {
    pub credential_key_file: PathBuf,
}
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Manage {
    pub origin: String,
    pub secure_cookie: bool,
    pub session_lifetime: String,
}
impl Default for Manage {
    fn default() -> Self {
        Self {
            origin: "http://localhost:9002".into(),
            secure_cookie: true,
            session_lifetime: "12h".into(),
        }
    }
}
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Storage {
    pub data: PathBuf,
    pub free_space_floor: String,
}
impl Default for Storage {
    fn default() -> Self {
        Self {
            data: "/data".into(),
            free_space_floor: "1GiB".into(),
        }
    }
}
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Processing {
    pub cpu_jobs: Option<usize>,
    pub inflight_bytes: Option<String>,
    pub upload_concurrency: Option<usize>,
    pub read_concurrency: Option<usize>,
    pub backend_concurrency: Option<usize>,
    pub connections: Option<usize>,
}
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Multipart {
    pub local_limit: Option<String>,
    pub idle_timeout: String,
    pub sweep_interval: String,
    pub client_idle_timeout: String,
    pub max_active_uploads: Option<i64>,
}
impl Default for Multipart {
    fn default() -> Self {
        Self {
            local_limit: None,
            idle_timeout: "24h".into(),
            sweep_interval: "5m".into(),
            client_idle_timeout: "120s".into(),
            max_active_uploads: None,
        }
    }
}
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Cache {
    pub max_size: Option<String>,
    pub max_entries: Option<usize>,
    pub min_compression_savings_percent: u8,
}
impl Default for Cache {
    fn default() -> Self {
        Self {
            max_size: None,
            max_entries: None,
            min_compression_savings_percent: 20,
        }
    }
}
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Encryption {
    pub algorithm: String,
    pub keyring_file: Option<PathBuf>,
}
impl Default for Encryption {
    fn default() -> Self {
        Self {
            algorithm: "aes-256-gcm".into(),
            keyring_file: None,
        }
    }
}
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Gc {
    pub unreferenced_grace: String,
    pub interval: String,
    pub batch_size: u32,
}
impl Default for Gc {
    fn default() -> Self {
        Self {
            unreferenced_grace: "48h".into(),
            interval: "30m".into(),
            batch_size: 128,
        }
    }
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Cleanup {
    pub interval: String,
    pub batch_size: u32,
    pub max_duration: String,
    pub deleted_chunk_retention: String,
    pub upload_retention: String,
    pub task_retention: String,
}
impl Default for Cleanup {
    fn default() -> Self {
        Self {
            interval: "5m".into(),
            batch_size: 1000,
            max_duration: "5s".into(),
            deleted_chunk_retention: "7d".into(),
            upload_retention: "24h".into(),
            task_retention: "30d".into(),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyFile {
    active: String,
    keys: BTreeMap<String, KeyEntry>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyEntry {
    algorithm: String,
    key: String,
}
pub struct Secrets {
    pub database_password: String,
    pub backend_access: String,
    pub backend_secret: String,
    pub credential_key: crate::codec::Key,
    pub active_key: String,
    pub keys: BTreeMap<String, crate::codec::Key>,
}
#[derive(Clone, serde::Serialize)]
pub struct Budget {
    pub available_cpus: usize,
    pub memory_bytes: u64,
    pub cpu_jobs: usize,
    pub data_slots: usize,
    pub slot_bytes: u64,
    pub upload_concurrency: usize,
    pub read_concurrency: usize,
    pub backend_concurrency: usize,
    pub connections: usize,
    pub db_connections: u32,
    pub cache_entries: usize,
}

pub const SLOT_BYTES: u64 = 32 * 1024 * 1024;

pub fn quantity(value: &str, time: bool) -> Result<u64> {
    let n = value
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(value.len());
    ensure!(n > 0, "quantity needs a positive integer and unit");
    let number: u64 = value[..n].parse().context("invalid quantity")?;
    let factor = match (&value[n..], time) {
        ("B", false) => 1,
        ("KB", false) => 1_000,
        ("MB", false) => 1_000_000,
        ("GB", false) => 1_000_000_000,
        ("TB", false) => 1_000_000_000_000,
        ("KiB", false) => 1024,
        ("MiB", false) => 1024 * 1024,
        ("GiB", false) => 1024 * 1024 * 1024,
        ("TiB", false) => 1024u64.pow(4),
        ("s", true) => 1,
        ("m", true) => 60,
        ("h", true) => 3600,
        ("d", true) => 86400,
        _ => bail!("invalid quantity unit"),
    };
    ensure!(number > 0, "quantity must be positive");
    number.checked_mul(factor).context("quantity overflow")
}
pub fn bytes(s: &str) -> Result<u64> {
    quantity(s, false)
}
pub fn seconds(s: &str) -> Result<u64> {
    quantity(s, true)
}
fn secret(path: &Path, base: &Path) -> Result<String> {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    };
    Ok(fs::read_to_string(&path)
        .with_context(|| format!("cannot read secret file {}", path.display()))?
        .trim()
        .to_owned())
}
fn secret_value(
    value: Option<&str>,
    file: Option<&Path>,
    base: &Path,
    name: &str,
) -> Result<String> {
    let value = match (value, file) {
        (Some(value), None) => value.to_owned(),
        (None, Some(file)) => secret(file, base)?,
        _ => bail!("configure exactly one of {name} and {name}_file"),
    };
    ensure!(!value.is_empty(), "{name} must not be empty");
    Ok(value)
}
fn key(value: &str) -> Result<[u8; 32]> {
    hex::decode(value)
        .ok()
        .and_then(|b| b.try_into().ok())
        .context("key must be 64 hexadecimal characters")
}

impl Config {
    pub fn load(path: &Path) -> Result<(Self, Secrets, Budget)> {
        let text = fs::read_to_string(path).context("cannot read configuration")?;
        // TOML errors may include the offending line; never include configuration values in errors.
        let c: Self = toml::from_str(&text).map_err(|_| {
            anyhow::anyhow!("invalid TOML configuration, unknown field or wrong type")
        })?;
        let base = path.parent().unwrap_or(Path::new("."));
        c.compression.validate()?;
        for listen in [&c.listen.s3, &c.listen.web, &c.listen.manage] {
            listen
                .parse::<std::net::SocketAddr>()
                .context("invalid listen address")?;
        }
        ensure!(
            c.listen.s3 != c.listen.web
                && c.listen.s3 != c.listen.manage
                && c.listen.web != c.listen.manage,
            "listeners must be distinct"
        );
        ensure!(
            !c.database.host.is_empty()
                && c.database.port > 0
                && !c.database.name.is_empty()
                && !c.database.user.is_empty(),
            "database host, name, user and port are required"
        );
        c.database
            .ssl_mode
            .parse::<sqlx::postgres::PgSslMode>()
            .map_err(|_| anyhow::anyhow!("invalid database.ssl_mode"))?;
        ensure!(
            c.backend.endpoint.starts_with("https://")
                || (c.backend.allow_http && c.backend.endpoint.starts_with("http://")),
            "backend requires HTTPS unless allow_http is enabled"
        );
        ensure!(
            !c.backend.prefix.starts_with('/')
                && !c.backend.prefix.split('/').any(|p| p == "." || p == ".."),
            "backend prefix must be relative and cannot contain . or .. segments"
        );
        ensure!(
            c.manage.origin.starts_with("https://") || c.manage.origin.starts_with("http://"),
            "manage.origin must be an absolute HTTP origin"
        );
        ensure!(
            !c.manage.origin.ends_with('/'),
            "manage.origin must not have a trailing slash"
        );
        ensure!(
            matches!(
                c.encryption.algorithm.as_str(),
                "none" | "aes-256-gcm" | "chacha20-poly1305"
            ),
            "unknown encryption algorithm"
        );
        for value in [
            &c.multipart.idle_timeout,
            &c.multipart.sweep_interval,
            &c.multipart.client_idle_timeout,
            &c.gc.interval,
            &c.gc.unreferenced_grace,
            &c.cleanup.interval,
            &c.cleanup.max_duration,
            &c.cleanup.deleted_chunk_retention,
            &c.cleanup.upload_retention,
            &c.cleanup.task_retention,
            &c.manage.session_lifetime,
            &c.statistics.refresh_interval,
            &c.statistics.query_timeout,
        ] {
            ensure!(
                seconds(value)? <= i64::MAX as u64 / 1000,
                "duration too large"
            );
        }
        bytes(&c.storage.free_space_floor)?;
        ensure!(
            seconds(&c.statistics.query_timeout)? <= i32::MAX as u64 / 1000,
            "statistics.query_timeout exceeds PostgreSQL's statement timeout limit"
        );
        if let Some(v) = &c.multipart.local_limit {
            ensure!(
                bytes(v)? >= 4 * 1024 * 1024,
                "multipart.local_limit must allow a CDC window"
            );
        }
        if let Some(v) = &c.cache.max_size {
            bytes(v)?;
        }
        ensure!(
            c.cache.min_compression_savings_percent <= 100,
            "cache.min_compression_savings_percent must be 0..100"
        );
        ensure!(
            c.gc.batch_size > 0 && c.gc.batch_size <= 10_000,
            "gc.batch_size must be 1..10000"
        );
        ensure!(
            c.cleanup.batch_size > 0 && c.cleanup.batch_size <= 10_000,
            "cleanup.batch_size must be 1..10000"
        );
        ensure!(
            seconds(&c.cleanup.max_duration)? <= 30,
            "cleanup.max_duration must be 1s..30s"
        );
        ensure!(
            c.multipart.max_active_uploads.is_none_or(|n| n > 0),
            "max_active_uploads must be positive"
        );
        let (active_key, keys) = if let Some(file) = &c.encryption.keyring_file {
            let raw = secret(file, base)?;
            let ring: KeyFile =
                toml::from_str(&raw).map_err(|_| anyhow::anyhow!("invalid keyring TOML"))?;
            let mut keys = BTreeMap::<String, crate::codec::Key>::new();
            for (id, entry) in ring.keys {
                ensure!(!id.is_empty() && id.len() <= 128, "invalid key ID");
                ensure!(
                    matches!(
                        entry.algorithm.as_str(),
                        "aes-256-gcm" | "chacha20-poly1305"
                    ),
                    "invalid key algorithm"
                );
                let material = key(&entry.key)?;
                ensure!(
                    !keys.values().any(|k| k.material == material),
                    "each encryption key must have distinct material"
                );
                keys.insert(id, crate::codec::Key::new(&entry.algorithm, material)?);
            }
            if c.encryption.algorithm != "none" {
                ensure!(
                    keys.get(&ring.active)
                        .is_some_and(|k| k.algorithm == c.encryption.algorithm),
                    "active key missing or algorithm mismatch"
                );
            }
            (ring.active, keys)
        } else {
            ensure!(
                c.encryption.algorithm == "none",
                "encryption.keyring_file is required"
            );
            (String::new(), BTreeMap::new())
        };
        let secrets = Secrets {
            database_password: secret_value(
                c.database.password.as_deref(),
                c.database.password_file.as_deref(),
                base,
                "database.password",
            )?,
            backend_access: secret_value(
                c.backend.access_key.as_deref(),
                c.backend.access_key_file.as_deref(),
                base,
                "backend.access_key",
            )?,
            backend_secret: secret_value(
                c.backend.secret_key.as_deref(),
                c.backend.secret_key_file.as_deref(),
                base,
                "backend.secret_key",
            )?,
            credential_key: crate::codec::Key::new(
                "aes-256-gcm",
                key(&secret(&c.security.credential_key_file, base)?)?,
            )?,
            active_key,
            keys,
        };
        ensure!(
            !secrets
                .keys
                .values()
                .any(|k| k.material == secrets.credential_key.material),
            "credential protection and chunk keys must differ"
        );
        let budget = c.budget()?;
        Ok((c, secrets, budget))
    }

    fn budget(&self) -> Result<Budget> {
        let mut cpus = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1);
        // Rust's estimate may floor a fractional cgroup quota before we inspect it.
        // Read the actual affinity first, then apply the quota with rounding below.
        if let Ok(status) = fs::read_to_string("/proc/self/status")
            && let Some(list) = status
                .lines()
                .find_map(|line| line.strip_prefix("Cpus_allowed_list:"))
            && let Some(count) = list.trim().split(',').try_fold(0usize, |total, range| {
                let (start, end) = range.split_once('-').unwrap_or((range, range));
                let start = start.parse::<usize>().ok()?;
                let end = end.parse::<usize>().ok()?;
                total.checked_add(end.checked_sub(start)?.checked_add(1)?)
            })
            && count > 0
        {
            cpus = count;
        }
        let meminfo = fs::read_to_string("/proc/meminfo").unwrap_or_default();
        let mut memory = meminfo
            .lines()
            .find(|s| s.starts_with("MemAvailable:"))
            .and_then(|s| s.split_whitespace().nth(1))
            .and_then(|s| s.parse::<u64>().ok())
            .map(|n| n.saturating_mul(1024))
            .unwrap_or(512 * 1024 * 1024);
        let mut paths = vec![PathBuf::from("/sys/fs/cgroup")];
        if let Ok(cgroup) = fs::read_to_string("/proc/self/cgroup")
            && let Some(p) = cgroup.lines().find_map(|s| s.strip_prefix("0::"))
        {
            let mut p = PathBuf::from("/sys/fs/cgroup").join(p.trim_start_matches('/'));
            while p.starts_with("/sys/fs/cgroup") {
                paths.push(p.clone());
                if !p.pop() {
                    break;
                }
            }
        }
        for p in paths {
            if let Ok(v) = fs::read_to_string(p.join("memory.max"))
                && let Ok(n) = v.trim().parse::<u64>()
            {
                memory = memory.min(n);
            }
            if let Ok(v) = fs::read_to_string(p.join("cpu.max")) {
                let f: Vec<_> = v.split_whitespace().collect();
                if f.len() == 2
                    && let (Ok(q), Ok(t)) = (f[0].parse::<u64>(), f[1].parse::<u64>())
                    && t > 0
                {
                    cpus = cpus.min(q.div_ceil(t).max(1) as usize);
                }
            }
        }
        let inflight = self
            .processing
            .inflight_bytes
            .as_deref()
            .map(bytes)
            .transpose()?
            .unwrap_or(memory / 4);
        let aws_chunk = bytes(&self.listen.aws_chunk_limit)?;
        let slot_bytes = SLOT_BYTES
            .checked_add(self.compression.workspace_bytes()?)
            .context("compression workspace budget overflow")?
            .checked_add(
                aws_chunk
                    .saturating_sub(8 * 1024 * 1024)
                    .checked_mul(2)
                    .context("AWS chunk limit overflow")?,
            )
            .context("AWS chunk limit overflow")?;
        ensure!(
            slot_bytes <= usize::MAX as u64 && slot_bytes <= inflight / 2 && inflight <= memory / 2,
            "inflight_bytes must allow two data slots ({} bytes each at compression.level={}) and leave half of memory for runtime/cache/OS",
            slot_bytes,
            self.compression.level
        );
        let slots = (inflight / slot_bytes) as usize;
        let cpu_jobs = self.processing.cpu_jobs.unwrap_or(cpus.min(slots));
        let uploads = self
            .processing
            .upload_concurrency
            .unwrap_or(cpus.min(slots / 2).max(1));
        let reads = self
            .processing
            .read_concurrency
            .unwrap_or((cpus * 2).min(slots).max(1));
        let backend = self
            .processing
            .backend_concurrency
            .unwrap_or((cpus * 4).min(slots * 2));
        let connections = self.processing.connections.unwrap_or((slots * 16).max(32));
        let entries = self
            .cache
            .max_entries
            .unwrap_or((memory / 64 / 256).min(usize::MAX as u64) as usize);
        let db = self
            .database
            .max_connections
            .unwrap_or((cpus * 2 + 4).min(64) as u32);
        ensure!(
            [
                cpu_jobs,
                uploads,
                reads,
                backend,
                connections,
                entries,
                db as usize
            ]
            .into_iter()
            .all(|n| n > 0),
            "resource limits must be positive"
        );
        ensure!(
            cpu_jobs <= slots
                && uploads < slots
                && reads <= slots
                && entries as u64 <= memory / 256 / 8,
            "explicit resource budgets exceed effective memory"
        );
        Ok(Budget {
            available_cpus: cpus,
            memory_bytes: memory,
            cpu_jobs,
            data_slots: slots,
            slot_bytes,
            upload_concurrency: uploads,
            read_concurrency: reads,
            backend_concurrency: backend,
            connections,
            db_connections: db,
            cache_entries: entries,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn secret_sources_are_exclusive_and_preserve_literal_passwords() {
        let base = std::env::temp_dir().join(format!("mgw-config-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&base).unwrap();
        fs::write(base.join("password"), "file-password\n").unwrap();
        let file = Some(Path::new("password"));
        assert_eq!(
            secret_value(None, file, &base, "password").unwrap(),
            "file-password"
        );
        assert_eq!(
            secret_value(Some(" @:/?# space "), None, &base, "password").unwrap(),
            " @:/?# space "
        );
        for (value, path) in [(None, None), (Some("do-not-leak"), file), (Some(""), None)] {
            let error = secret_value(value, path, &base, "password")
                .unwrap_err()
                .to_string();
            assert!(!error.contains("do-not-leak"));
        }
        fs::remove_dir_all(base).unwrap();
    }
    #[test]
    fn quantities_reject_ambiguity_and_overflow() {
        assert_eq!(bytes("2GB").unwrap(), 2_000_000_000);
        assert_eq!(bytes("2GiB").unwrap(), 2_147_483_648);
        assert_eq!(seconds("24h").unwrap(), 86400);
        for s in [
            "",
            "0GB",
            "-1GB",
            "auto",
            "1G",
            "1.5GB",
            "18446744073709551615GB",
        ] {
            assert!(bytes(s).is_err());
        }
    }
}
