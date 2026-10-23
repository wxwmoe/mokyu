use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

/// Typed, revisioned policy shared by every node that serves a storage.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct StorageProfile {
    pub storage_id: Uuid,
    pub revision: i64,
    #[serde(default = "cdc_enabled")]
    pub cdc_enabled: bool,
    #[serde(default)]
    pub deduplication_enabled: bool,
    #[serde(default = "write_processing")]
    pub write_processing: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_durability: Option<String>,
    #[serde(default = "write_flush_interval")]
    pub write_flush_interval: String,
    #[serde(default = "write_backlog_max_age")]
    pub write_backlog_max_age: String,
    #[serde(default = "background_scheduling")]
    pub background_scheduling: String,
    #[serde(default = "public_cache_validation")]
    pub public_cache_validation: String,
    #[serde(default = "public_cache_refresh_interval")]
    pub public_cache_refresh_interval: String,
    #[serde(default = "public_cache_max_age")]
    pub public_cache_max_age: String,
    pub connect_timeout: String,
    pub request_timeout: String,
    pub max_retries: i32,
    pub retry_timeout: String,
    pub priority_aging: String,
    pub min_storage_duration: String,
    pub unreferenced_grace: String,
    pub deleted_chunk_retention: String,
    pub upload_retention: String,
    pub multipart_idle_timeout: String,
    pub min_free_space: Option<String>,
    pub cache_enabled: bool,
    pub cache_soft_limit: Option<String>,
    pub cache_entries: Option<i64>,
    pub upload_cache: bool,
    pub upload_cache_size: Option<String>,
    #[serde(default = "upload_cache_overflow")]
    pub upload_cache_overflow: String,
    pub compression_enabled: bool,
    pub compression_strategy: String,
    pub compression_level: i32,
    pub compression_sample_level: i32,
    pub compression_min_savings_percent: i32,
    pub compression_min_savings_bytes: i64,
    pub pack_enabled: bool,
    pub pack_max_size: String,
    pub pack_upload_flush_timeout: String,
    pub pack_interval: String,
    pub pack_reuse_interval: String,
    pub pack_reclaim_interval: String,
    pub pack_repack_interval: String,
    pub pack_repack_cooldown: String,
    pub pack_reclaim_min_savings_bytes: String,
    pub pack_range_enabled: bool,
    pub pack_range_interval: String,
    pub pack_range_window: String,
    pub pack_range_min_downloads: i32,
    pub pack_range_min_savings_percent: i32,
    pub pack_range_min_savings_bytes: String,
    pub pack_range_repack_after: String,
    pub pack_range_repack_window: String,
    pub pack_range_repack_retry_interval: String,
    pub pack_concurrency: i32,
    pub integrity_concurrency: i32,
    pub integrity_rps: Option<i64>,
    pub integrity_bandwidth: Option<String>,
    pub integrity_batch_timeout: String,
    pub read_concurrency: Option<i32>,
    pub upload_concurrency: Option<i32>,
    pub control_concurrency: Option<i32>,
    pub pack_byte_budget: Option<String>,
    pub integrity_byte_budget: Option<String>,
    pub maintenance_concurrency: Option<i32>,
    pub maintenance_byte_budget: Option<String>,
    pub migration_concurrency: Option<i32>,
    pub migration_byte_budget: Option<String>,
}

fn cdc_enabled() -> bool {
    true
}

fn write_processing() -> String {
    "inline".into()
}

fn upload_cache_overflow() -> String {
    "fallback".into()
}

fn write_flush_interval() -> String {
    "0s".into()
}
fn write_backlog_max_age() -> String {
    "5m".into()
}
fn background_scheduling() -> String {
    "balanced".into()
}
fn public_cache_validation() -> String {
    "strict".into()
}
fn public_cache_refresh_interval() -> String {
    "1s".into()
}
fn public_cache_max_age() -> String {
    "5s".into()
}

impl StorageProfile {
    pub(crate) fn public_cache_times(&self) -> Result<(u64, u64)> {
        let invalid = || {
            s3s::s3_error!(
                InvalidArgument,
                "public cache refresh must be 1s..60s and no greater than max age (1s..5m)"
            )
        };
        let refresh =
            crate::config::seconds(&self.public_cache_refresh_interval).map_err(|_| invalid())?;
        let age = crate::config::seconds(&self.public_cache_max_age).map_err(|_| invalid())?;
        anyhow::ensure!(
            (1..=60).contains(&refresh) && (refresh..=300).contains(&age),
            invalid()
        );
        anyhow::ensure!(
            matches!(
                self.public_cache_validation.as_str(),
                "strict" | "background"
            ),
            s3s::s3_error!(
                InvalidArgument,
                "public_cache_validation must be strict or background"
            )
        );
        Ok((refresh, age))
    }
    pub(crate) fn write_flush_seconds(&self) -> Result<u64> {
        if self.write_flush_interval == "0s" {
            return Ok(0);
        }
        let invalid = || s3s::s3_error!(InvalidArgument, "write_flush_interval must be 0s..60s");
        let seconds = crate::config::seconds(&self.write_flush_interval).map_err(|_| invalid())?;
        anyhow::ensure!(seconds <= 60, invalid());
        Ok(seconds)
    }

    pub(crate) fn write_backlog_seconds(&self) -> Result<u64> {
        let invalid = || s3s::s3_error!(InvalidArgument, "write_backlog_max_age must be 1s..24h");
        let seconds = crate::config::seconds(&self.write_backlog_max_age).map_err(|_| invalid())?;
        anyhow::ensure!(seconds <= 86400, invalid());
        Ok(seconds)
    }
    pub(crate) fn validate_key(&self, key: Option<Uuid>) -> Result<()> {
        anyhow::ensure!(
            self.cdc_enabled || key.is_none(),
            "raw storage cannot enable encryption"
        );
        Ok(())
    }
    pub(crate) fn pack_config(&self) -> crate::content::pack::Config {
        use crate::content::pack::{CompressionStrategy, Config};
        Config {
            enabled: self.pack_enabled,
            max_size: self.pack_max_size.clone(),
            upload_cache_timeout: self.pack_upload_flush_timeout.clone(),
            compression_strategy: Some(if !self.compression_enabled {
                CompressionStrategy::Never
            } else {
                match self.compression_strategy.as_str() {
                    "never" => CompressionStrategy::Never,
                    "sample" => CompressionStrategy::Sample,
                    _ => CompressionStrategy::Always,
                }
            }),
            interval: self.pack_interval.clone(),
            reuse_interval: self.pack_reuse_interval.clone(),
            reclaim_interval: self.pack_reclaim_interval.clone(),
            repack_interval: self.pack_repack_interval.clone(),
            repack_cooldown: self.pack_repack_cooldown.clone(),
            reclaim_min_savings_bytes: self.pack_reclaim_min_savings_bytes.clone(),
            range_optimization: self.pack_range_enabled,
            range_interval: self.pack_range_interval.clone(),
            range_window: self.pack_range_window.clone(),
            range_min_downloads: self.pack_range_min_downloads as u32,
            range_min_savings_percent: self.pack_range_min_savings_percent as u8,
            range_min_savings_bytes: self.pack_range_min_savings_bytes.clone(),
            range_repack_after: self.pack_range_repack_after.clone(),
            range_repack_window: self.pack_range_repack_window.clone(),
            range_repack_retry_interval: self.pack_range_repack_retry_interval.clone(),
            ..Default::default()
        }
    }

    pub(crate) fn maintenance_interval(&self, policy: &str) -> Result<u64> {
        crate::config::seconds(match policy {
            "pack" => &self.pack_interval,
            "reuse" => &self.pack_reuse_interval,
            "reclaim" => &self.pack_reclaim_interval,
            "repack" => &self.pack_repack_interval,
            "range" => &self.pack_range_interval,
            _ => anyhow::bail!("unknown storage maintenance policy"),
        })
    }

    pub(crate) fn validate_budget(
        &self,
        config: &crate::config::Config,
        budget: &crate::config::Budget,
    ) -> Result<()> {
        let workspace = self
            .compression_config(&config.instance.compression_idle_timeout)
            .workspace_bytes()?;
        if self.pack_enabled {
            let maximum = crate::config::bytes(&self.pack_max_size)?;
            let payload = maximum
                + 12
                + crate::content::pack::MEMBER_SIZE as u64
                    * (maximum / crate::content::codec::MIN as u64 + 1);
            anyhow::ensure!(
                (payload * 4 + workspace).div_ceil(1024 * 1024)
                    <= u64::from(budget.pack_memory_units),
                "storage pack_max_size exceeds node memory budget"
            );
        }
        anyhow::ensure!(
            crate::config::seconds(&config.metrics.access_retention)?
                >= crate::config::seconds(&self.pack_range_window)?
                    .max(crate::config::seconds(&self.pack_range_repack_window)?),
            "statistics.access_retention must cover storage Range windows"
        );
        Ok(())
    }

    pub(crate) fn min_storage_seconds(&self) -> Result<u64> {
        if self.min_storage_duration == "0s" {
            Ok(0)
        } else {
            crate::config::seconds(&self.min_storage_duration)
                .context("storage min_storage_duration")
        }
    }

    pub(crate) fn compression_config(
        &self,
        idle_timeout: &str,
    ) -> crate::content::compression::Config {
        let mut config = crate::content::compression::Config {
            context_idle_timeout: idle_timeout.into(),
            ..Default::default()
        };
        config.enabled = self.compression_enabled;
        config.strategy = match self.compression_strategy.as_str() {
            "never" => crate::content::compression::Strategy::Never,
            "sample" => crate::content::compression::Strategy::Sample,
            _ => crate::content::compression::Strategy::Always,
        };
        config.level = self.compression_level;
        config.sample_level = self.compression_sample_level;
        config.min_savings_percent = self.compression_min_savings_percent as u8;
        config.min_savings_bytes = self.compression_min_savings_bytes as u64;
        config
    }
}

pub(crate) fn validate(profile: &StorageProfile, storage_type: &str) -> anyhow::Result<()> {
    use crate::config::{bytes, cache_bytes, seconds};
    profile.public_cache_times()?;
    profile.write_flush_seconds()?;
    profile.write_backlog_seconds()?;
    anyhow::ensure!(
        matches!(
            profile.background_scheduling.as_str(),
            "balanced" | "foreground"
        ),
        s3s::s3_error!(
            InvalidArgument,
            "background_scheduling must be balanced or foreground"
        )
    );
    anyhow::ensure!(
        matches!(
            profile.upload_cache_overflow.as_str(),
            "fallback" | "reject"
        ),
        s3s::s3_error!(
            InvalidArgument,
            "upload_cache_overflow must be fallback or reject"
        )
    );
    anyhow::ensure!(
        matches!(
            profile.write_durability.as_deref(),
            None | Some("durable" | "buffered" | "volatile")
        ),
        s3s::s3_error!(
            InvalidArgument,
            "write_durability must be durable, buffered or volatile"
        )
    );
    anyhow::ensure!(
        profile.write_durability.as_deref() != Some("volatile")
            || (storage_type == "s3" && profile.upload_cache),
        s3s::s3_error!(InvalidArgument, "volatile writes require S3 upload cache")
    );
    match (storage_type, profile.min_free_space.as_deref()) {
        ("local", Some(floor)) => {
            bytes(floor)?;
        }
        ("s3", None) => {}
        _ => anyhow::bail!("min_free_space is required only for local storage"),
    }
    anyhow::ensure!(
        profile.cdc_enabled
            || (!profile.compression_enabled
                && !profile.pack_enabled
                && !profile.pack_range_enabled),
        "raw storage cannot enable compression or packs"
    );
    anyhow::ensure!(
        matches!(profile.write_processing.as_str(), "inline" | "background"),
        s3s::s3_error!(
            InvalidArgument,
            "write_processing must be inline or background"
        )
    );
    if storage_type == "local"
        && (profile.cache_enabled
            || profile.upload_cache
            || profile.pack_enabled
            || profile.pack_range_enabled)
    {
        return Err(crate::model::Error::LocalStorageFeatures.into());
    }
    anyhow::ensure!(
        matches!(
            profile.compression_strategy.as_str(),
            "never" | "always" | "sample"
        ),
        "compression strategy must be never, always, or sample"
    );
    profile.compression_config("30s").validate()?;
    for duration in [
        &profile.connect_timeout,
        &profile.request_timeout,
        &profile.retry_timeout,
        &profile.priority_aging,
        &profile.unreferenced_grace,
        &profile.deleted_chunk_retention,
        &profile.upload_retention,
        &profile.multipart_idle_timeout,
        &profile.pack_upload_flush_timeout,
        &profile.pack_interval,
        &profile.pack_reuse_interval,
        &profile.pack_reclaim_interval,
        &profile.pack_repack_interval,
        &profile.pack_repack_cooldown,
        &profile.pack_range_interval,
        &profile.pack_range_window,
        &profile.pack_range_repack_after,
        &profile.pack_range_repack_window,
        &profile.pack_range_repack_retry_interval,
        &profile.integrity_batch_timeout,
    ] {
        anyhow::ensure!(
            seconds(duration)? <= i64::MAX as u64 / 1_000_000,
            "storage duration exceeds the database timestamp range"
        );
    }
    profile.pack_config().validate()?;
    if profile.min_storage_duration != "0s" {
        anyhow::ensure!(
            seconds(&profile.min_storage_duration)? <= i64::MAX as u64 / 1_000_000,
            "storage duration exceeds the database timestamp range"
        );
    }
    for size in [
        &profile.pack_max_size,
        &profile.pack_reclaim_min_savings_bytes,
        &profile.pack_range_min_savings_bytes,
    ] {
        bytes(size)?;
    }
    for size in [&profile.cache_soft_limit, &profile.upload_cache_size]
        .into_iter()
        .flatten()
    {
        cache_bytes(size)?;
    }
    for size in [
        &profile.integrity_bandwidth,
        &profile.pack_byte_budget,
        &profile.integrity_byte_budget,
        &profile.maintenance_byte_budget,
        &profile.migration_byte_budget,
    ]
    .into_iter()
    .flatten()
    {
        bytes(size)?;
    }
    anyhow::ensure!(
        (0..=100).contains(&profile.max_retries),
        "retries must be 0..100"
    );
    anyhow::ensure!(
        (0..=100).contains(&profile.compression_min_savings_percent)
            && profile.compression_min_savings_bytes >= 0,
        "compression savings must be nonnegative and at most 100 percent"
    );
    for limit in [
        profile.read_concurrency,
        profile.upload_concurrency,
        profile.control_concurrency,
        profile.maintenance_concurrency,
        profile.migration_concurrency,
        Some(profile.pack_concurrency),
        Some(profile.integrity_concurrency),
    ]
    .into_iter()
    .flatten()
    {
        anyhow::ensure!(limit > 0, "concurrency must be positive");
    }
    anyhow::ensure!(
        profile.integrity_rps.is_none_or(|v| v > 0) && profile.cache_entries.is_none_or(|v| v > 0),
        "rate and entry limits must be positive"
    );
    anyhow::ensure!(
        profile.pack_range_min_downloads > 0
            && (1..=100).contains(&profile.pack_range_min_savings_percent),
        "invalid pack range thresholds"
    );
    Ok(())
}
