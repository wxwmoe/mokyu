use super::Config;
use anyhow::{Context, Result, bail, ensure};
use std::{fs, path::PathBuf};

#[derive(Clone, serde::Serialize)]
pub struct Budget {
    pub available_cpus: usize,
    pub memory_bytes: u64,
    pub cpu_jobs: usize,
    pub data_slots: usize,
    pub slot_bytes: u64,
    pub upload_concurrency: usize,
    pub read_concurrency: usize,
    pub storage_concurrency: usize,
    pub storage_read_slots: usize,
    pub storage_upload_slots: usize,
    pub storage_control_slots: usize,
    pub request_connections: usize,
    pub database_connections: u32,
    pub cache_entries: usize,
    pub pack_memory_units: u32,
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
        c.web.validate()?;
        cache_bytes(&c.instance.thumbnail_cache_size)?;
        seconds(&c.instance.compression_idle_timeout)?;
        c.local_compression.validate()?;
        c.local_pack.validate()?;
        bytes(&c.requests.s3_chunk_limit)?;
        ensure!(
            !c.requests.s3_region.trim().is_empty(),
            "s3.signature_region must not be empty"
        );
        if let Some(domain) = &c.requests.s3_domain {
            s3s::host::SingleDomain::new(domain)?;
        }
        ensure!(
            c.backend_defaults.max_retries <= 100,
            "storage.max_retries must be 0..100"
        );
        seconds(&c.requests.read_idle_timeout)?;
        seconds(&c.requests.upload_idle_timeout)?;
        seconds(&c.metrics.access_flush_interval)?;
        ensure!(
            seconds(&c.metrics.access_retention)?
                >= seconds(&c.local_pack.range_repack_window)?
                    .max(seconds(&c.local_pack.range_window)?)
                    .max(7 * 24 * 3600),
            "statistics.access_retention must cover seven days and the configured Range windows"
        );
        for value in [
            c.instance.request_connections,
            c.instance.database_connections.map(|v| v as usize),
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
        for value in [
            &c.backend_defaults.connect_timeout,
            &c.backend_defaults.request_timeout,
            &c.backend_defaults.retry_timeout,
            &c.backend_defaults.priority_aging,
        ] {
            seconds(value)?;
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
        c.backend_defaults.min_storage_seconds()?;
        for value in [
            &c.uploads.idle_timeout,
            &c.uploads.sweep_interval,
            &c.uploads.client_idle_timeout,
            &c.maintenance.interval,
            &c.maintenance.unreferenced_grace,
            &c.maintenance.interval,
            &c.maintenance.max_duration,
            &c.maintenance.deleted_chunk_retention,
            &c.maintenance.upload_retention,
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
        c.local_integrity.validate()?;
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
        ensure!(
            c.local_cache.min_compression_savings_percent <= 100,
            "cache.min_compression_savings_percent must be 0..100"
        );
        ensure!(
            c.maintenance.batch_size > 0 && c.maintenance.batch_size <= 10_000,
            "gc.batch_size must be 1..10000"
        );
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
            .instance
            .stream_memory_budget
            .as_deref()
            .map(bytes)
            .transpose()?
            .unwrap_or(memory / 4);
        let aws_chunk = bytes(&self.requests.s3_chunk_limit)?;
        let mut compression_budget = self.local_compression.clone();
        if matches!(
            self.local_pack.compression_strategy,
            Some(crate::content::pack::CompressionStrategy::Sample)
        ) {
            compression_budget.strategy = crate::content::compression::Strategy::Sample;
        }
        let slot_bytes = SLOT_BYTES
            .checked_add(compression_budget.workspace_bytes()?)
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
            "stream_memory_budget must allow two data slots ({} bytes each at compression.level={}, compression.sample_level={}) and leave half of memory for runtime/cache/OS",
            slot_bytes,
            self.local_compression.level,
            self.local_compression.sample_level
        );
        let slots = (inflight / slot_bytes) as usize;
        let pack_memory_units = (memory
            .saturating_sub(slots as u64 * slot_bytes)
            .saturating_sub(memory / 4)
            / 2
            / (1024 * 1024))
            .min(u32::MAX as u64) as u32;
        if self.local_pack.enabled {
            let maximum = bytes(&self.local_pack.max_size)?;
            let payload = maximum + 12 + 44 * (maximum / crate::content::codec::MIN as u64 + 1);
            ensure!(
                (payload * 4 + compression_budget.workspace_bytes()?).div_ceil(1024 * 1024)
                    <= u64::from(pack_memory_units),
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
            available_cpus: cpus,
            memory_bytes: memory,
            cpu_jobs,
            data_slots: slots,
            slot_bytes,
            upload_concurrency: uploads,
            read_concurrency: reads,
            storage_concurrency: backend,
            storage_read_slots,
            storage_upload_slots,
            storage_control_slots,
            request_connections: connections,
            database_connections: db,
            cache_entries: entries,
            pack_memory_units,
        })
    }
}
