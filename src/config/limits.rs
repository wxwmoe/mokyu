use super::Config;
use anyhow::{Context, Result, bail, ensure};
use std::{fs, path::PathBuf};

#[derive(Clone, serde::Serialize)]
pub struct Budget {
    #[serde(skip)]
    pub environment: Environment,
    pub available_cpus: usize,
    pub memory_bytes: u64,
    pub cpu_jobs: usize,
    pub data_slots: usize,
    pub slot_bytes: u64,
    #[serde(skip)]
    pub compression_workspace: u64,
    pub upload_concurrency: usize,
    pub read_concurrency: usize,
    pub storage_concurrency: usize,
    pub storage_read_slots: usize,
    pub storage_upload_slots: usize,
    pub storage_control_slots: usize,
    pub request_connections: usize,
    pub database_connections: u32,
    pub database_read_connections: u32,
    pub cache_entries: usize,
    pub pack_memory_units: u32,
    pub response_cache_bytes: u64,
    pub write_memory_bytes: u64,
}

#[derive(Clone, Default)]
pub struct Environment {
    pub available_cpus: Option<usize>,
    pub cpu_quota: Option<f64>,
    pub cpu_quota_read: bool,
    pub memory_limit: Option<u64>,
    pub memory_limit_read: bool,
    pub memory_available_at_start: Option<u64>,
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
pub fn cache_bytes(s: &str) -> Result<u64> {
    if s == "0B" { Ok(0) } else { bytes(s) }
}
pub fn seconds(s: &str) -> Result<u64> {
    let seconds = quantity(s, true)?;
    ensure!(seconds <= i64::MAX as u64 / 1000, "duration too large");
    Ok(seconds)
}
impl Config {
    pub fn validate(&self) -> Result<()> {
        let c = self;
        c.io.validate()?;
        c.web.validate()?;
        cache_bytes(&c.instance.thumbnail_cache_size)?;
        seconds(&c.instance.compression_idle_timeout)?;
        bytes(&c.requests.s3_chunk_limit)?;
        ensure!(
            bytes(&c.requests.cors_cache_size)? >= 32 * 1024,
            "cors_cache_size must be at least 32KiB"
        );
        ensure!(
            (1..=1_000_000).contains(&c.requests.cors_cache_entries),
            "cors_cache_entries must be 1..1000000"
        );
        ensure!(
            !c.requests.s3_region.trim().is_empty(),
            "s3.signature_region must not be empty"
        );
        if let Some(domain) = &c.requests.s3_domain {
            s3s::host::SingleDomain::new(domain)?;
        }
        seconds(&c.requests.read_idle_timeout)?;
        seconds(&c.requests.upload_idle_timeout)?;
        seconds(&c.metrics.access_flush_interval)?;
        ensure!(
            seconds(&c.metrics.access_retention)? >= 7 * 24 * 3600,
            "statistics.access_retention must cover seven days and the configured Range windows"
        );
        for value in [
            c.instance.request_connections,
            c.instance.database_connections.map(|v| v as usize),
            c.instance.database_read_connections.map(|v| v as usize),
            c.instance.storage_read_slots,
            c.instance.storage_upload_slots,
            c.instance.storage_control_slots,
        ]
        .into_iter()
        .flatten()
        {
            ensure!(
                value > 0,
                "connection and storage slot budgets must be positive"
            );
        }
        for listen in [
            &c.instance.listen_s3,
            &c.instance.listen_web,
            &c.instance.listen_control,
        ] {
            listen
                .parse::<std::net::SocketAddr>()
                .context("invalid listen address")?;
        }
        ensure!(
            c.instance.listen_s3 != c.instance.listen_web
                && c.instance.listen_s3 != c.instance.listen_control
                && c.instance.listen_web != c.instance.listen_control,
            "listeners must be distinct"
        );
        if let Some(replica) = &c.replica_database {
            replica.resolve(&c.database)?;
        }
        ensure!(
            c.replica_database.is_some() || c.instance.database_read_connections.is_none(),
            "database_read_connections requires replica_database"
        );
        match c.database.backend {
            super::DatabaseType::Postgres | super::DatabaseType::Mysql => {
                ensure!(
                    !c.database.host.is_empty()
                        && c.database.effective_port().is_some_and(|port| port > 0)
                        && !c.database.name.is_empty()
                        && !c.database.user.is_empty(),
                    "database host, name, user and port are required"
                );
                if c.database.backend == super::DatabaseType::Postgres {
                    c.database
                        .ssl_mode
                        .parse::<sqlx::postgres::PgSslMode>()
                        .map_err(|_| anyhow::anyhow!("invalid database.ssl_mode"))?;
                }
            }
            super::DatabaseType::Sqlite => {
                ensure!(
                    c.database
                        .sqlite_path(&c.instance.data_path)
                        .is_some_and(|path| !path.as_os_str().is_empty()),
                    "database.path must not be empty"
                );
            }
        }
        ensure!(
            c.database.backend.supports_cluster()
                || (c.uploads.staging_storage_id.is_none()
                    && c.uploads.staging_bytes_limit.is_none()),
            "shared staging requires PostgreSQL"
        );
        for value in [
            &c.uploads.sweep_interval,
            &c.uploads.client_idle_timeout,
            &c.maintenance.interval,
            &c.maintenance.max_duration,
            &c.maintenance.task_retention,
            &c.maintenance.audit_retention,
            &c.metrics.refresh_interval,
            &c.metrics.query_timeout,
        ] {
            ensure!(
                seconds(value)? <= i64::MAX as u64 / 1000,
                "duration too large"
            );
        }
        bytes(&c.instance.min_free_space)?;
        if let Some(name) = &c.instance.name {
            ensure!(
                (1..=128).contains(&name.trim().len())
                    && !name.chars().any(char::is_control)
                    && !name.contains('/'),
                "instance.name must contain 1..128 safe bytes"
            );
        }
        ensure!(
            seconds(&c.metrics.query_timeout)? <= i32::MAX as u64 / 1000,
            "statistics.query_timeout exceeds PostgreSQL's statement timeout limit"
        );
        ensure!(
            (1..=256).contains(&c.metrics.scope_limit),
            "statistics.scope_limit must be 1..256"
        );
        for (interval, retention) in [
            (&c.metrics.storage_interval, &c.metrics.storage_retention),
            (&c.metrics.runtime_interval, &c.metrics.runtime_retention),
        ] {
            let interval = seconds(interval)?;
            let retention = seconds(retention)?;
            ensure!(
                interval >= 60 && retention >= interval && retention <= 90 * 86400,
                "statistics sampling requires an interval of at least one minute and retention between one interval and 90 days"
            );
        }
        if let Some(v) = &c.instance.multipart_limit {
            ensure!(
                bytes(v)? >= 4 * 1024 * 1024,
                "multipart.local_limit must allow a CDC window"
            );
        }
        if let Some(v) = &c.instance.cache_size {
            cache_bytes(v)?;
        }
        if let Some(v) = &c.instance.upload_cache_size {
            cache_bytes(v)?;
        }
        if let Some(v) = &c.instance.write_memory_size {
            cache_bytes(v)?;
        }
        if let Some(v) = &c.instance.response_cache_size {
            cache_bytes(v)?;
        }
        ensure!(
            c.maintenance.batch_size > 0 && c.maintenance.batch_size <= 10_000,
            "cleanup.batch_size must be 1..10000"
        );
        ensure!(
            seconds(&c.maintenance.max_duration)? <= 30,
            "cleanup.max_duration must be 1s..30s"
        );
        ensure!(
            c.uploads.max_active_uploads.is_none_or(|n| n > 0),
            "max_active_uploads must be positive"
        );
        Ok(())
    }

    pub fn budget(&self) -> Result<Budget> {
        self.budget_for(&[])
    }

    pub(crate) fn budget_for(
        &self,
        profiles: &[crate::storage::profile::StorageProfile],
    ) -> Result<Budget> {
        let mut environment = Environment {
            available_cpus: std::thread::available_parallelism().ok().map(usize::from),
            ..Environment::default()
        };
        let mut cpus = environment.available_cpus.unwrap_or(1);
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
            environment.available_cpus = Some(count);
        }
        let meminfo = fs::read_to_string("/proc/meminfo").unwrap_or_default();
        environment.memory_available_at_start = meminfo
            .lines()
            .find(|s| s.starts_with("MemAvailable:"))
            .and_then(|s| s.split_whitespace().nth(1))
            .and_then(|s| s.parse::<u64>().ok())
            .map(|n| n.saturating_mul(1024));
        let mut memory = environment
            .memory_available_at_start
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
            if let Ok(v) = fs::read_to_string(p.join("memory.max")) {
                if let Ok(n) = v.trim().parse::<u64>() {
                    memory = memory.min(n);
                    environment.memory_limit =
                        Some(environment.memory_limit.map_or(n, |old| old.min(n)));
                    environment.memory_limit_read = true;
                } else if v.trim() == "max" {
                    environment.memory_limit_read = true;
                }
            }
            if let Ok(v) = fs::read_to_string(p.join("cpu.max")) {
                let f: Vec<_> = v.split_whitespace().collect();
                if f.len() == 2
                    && let (Ok(q), Ok(t)) = (f[0].parse::<u64>(), f[1].parse::<u64>())
                    && t > 0
                {
                    cpus = cpus.min(q.div_ceil(t).max(1) as usize);
                    let quota = q as f64 / t as f64;
                    environment.cpu_quota =
                        Some(environment.cpu_quota.map_or(quota, |old| old.min(quota)));
                    environment.cpu_quota_read = true;
                } else if f.len() == 2 && f[0] == "max" && f[1].parse::<u64>().is_ok_and(|t| t > 0)
                {
                    environment.cpu_quota_read = true;
                }
            }
        }
        if environment.available_cpus.is_some() {
            environment.available_cpus = Some(cpus);
        }
        let inflight = self
            .instance
            .stream_memory_budget
            .as_deref()
            .map(bytes)
            .transpose()?
            .unwrap_or(memory / 4);
        let aws_chunk = bytes(&self.requests.s3_chunk_limit)?;
        // Reserve room for a newly created default backend as well as existing profiles.
        let mut workspace = crate::content::compression::Config::default().workspace_bytes()?;
        let mut maximum = 32 * 1024 * 1024;
        for profile in profiles {
            workspace = workspace.max(
                profile
                    .compression_config(&self.instance.compression_idle_timeout)
                    .workspace_bytes()?,
            );
            if profile.pack_enabled {
                maximum = maximum.max(bytes(&profile.pack_max_size)?);
            }
        }
        let slot_bytes = SLOT_BYTES
            .checked_add(workspace)
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
            "stream_memory_budget must allow two data slots ({} bytes each) and leave half of memory for runtime/cache/OS",
            slot_bytes
        );
        ensure!(
            cache_bytes(&self.requests.cors_cache_size)? <= memory / 16,
            "cors_cache_size must fit within one sixteenth of instance memory"
        );
        let slots = (inflight / slot_bytes) as usize;
        let remaining = memory
            .saturating_sub(slots as u64 * slot_bytes)
            .saturating_sub(memory / 4);
        let pack_required = {
            let payload = maximum
                + 12
                + crate::content::pack::MEMBER_SIZE as u64
                    * (maximum / crate::content::codec::MIN as u64 + 1);
            (payload * 4 + workspace).div_ceil(1024 * 1024) * 1024 * 1024
        };
        let write_memory_bytes = self
            .instance
            .write_memory_size
            .as_deref()
            .map(cache_bytes)
            .transpose()?
            .unwrap_or(0);
        ensure!(
            write_memory_bytes == 0 || write_memory_bytes >= 4096,
            "write_memory_size must be 0B or at least 4KiB"
        );
        ensure!(
            write_memory_bytes <= remaining.saturating_sub(pack_required.saturating_mul(2))
                && write_memory_bytes <= memory / 4
                && write_memory_bytes / 4096 <= u64::from(u32::MAX),
            "write_memory_size must fit alongside stream, response cache and pack budgets"
        );
        let write_memory_bytes = write_memory_bytes / 4096 * 4096;
        let remaining = remaining - write_memory_bytes;
        let cache_maximum = remaining
            .saturating_sub(pack_required.saturating_mul(2))
            .min(memory / 4)
            .min(u64::from(u32::MAX) * 4096);
        let response_cache_bytes = match self.instance.response_cache_size.as_deref() {
            Some(value) => {
                let size = cache_bytes(value)?;
                ensure!(
                    size <= cache_maximum && (size == 0 || size >= 4 * 1024 * 1024),
                    "response_cache_size must be 0B or at least 4MiB and fit alongside stream and pack budgets"
                );
                size
            }
            // Existing packs can be larger than the current creation policy; keep their read budget by default.
            None => 0,
        } / 65536
            * 65536;
        let pack_memory_units =
            ((remaining - response_cache_bytes) / 2 / (1024 * 1024)).min(u32::MAX as u64) as u32;
        {
            let payload = maximum
                + 12
                + crate::content::pack::MEMBER_SIZE as u64
                    * (maximum / crate::content::codec::MIN as u64 + 1);
            ensure!(
                (payload * 4 + workspace).div_ceil(1024 * 1024) <= u64::from(pack_memory_units),
                "pack.max_size exceeds effective memory; lower the pack size or increase the container memory limit"
            );
        }
        let cpu_jobs = self.instance.cpu_jobs.unwrap_or(cpus.min(slots));
        let uploads = cpus.min(slots / 2).max(1);
        let reads = (cpus * 2).min(slots).max(1);
        let storage_read_slots = self
            .instance
            .storage_read_slots
            .unwrap_or((cpus * 4).min(slots * 2));
        let storage_upload_slots = self
            .instance
            .storage_upload_slots
            .unwrap_or((cpus * 4).min(slots * 2));
        let storage_control_slots = self
            .instance
            .storage_control_slots
            .unwrap_or(cpus.clamp(1, 8));
        let backend = storage_read_slots;
        let connections = self
            .instance
            .request_connections
            .unwrap_or((slots * 16).max(32));
        let entries = self
            .instance
            .cache_entries
            .unwrap_or((memory / 64 / 256).min(usize::MAX as u64) as usize);
        let db = self
            .instance
            .database_connections
            .unwrap_or((cpus * 2 + 4).min(64) as u32);
        ensure!(
            [
                cpu_jobs,
                uploads,
                reads,
                storage_read_slots,
                storage_upload_slots,
                storage_control_slots,
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
            environment,
            available_cpus: cpus,
            memory_bytes: memory,
            cpu_jobs,
            data_slots: slots,
            slot_bytes,
            compression_workspace: workspace,
            upload_concurrency: uploads,
            read_concurrency: reads,
            storage_concurrency: backend,
            storage_read_slots,
            storage_upload_slots,
            storage_control_slots,
            request_connections: connections,
            database_connections: db,
            database_read_connections: if self.replica_database.is_some() {
                self.instance
                    .database_read_connections
                    .unwrap_or((db / 4).clamp(1, 4))
            } else {
                0
            },
            cache_entries: entries,
            pack_memory_units,
            response_cache_bytes,
            write_memory_bytes,
        })
    }
}
