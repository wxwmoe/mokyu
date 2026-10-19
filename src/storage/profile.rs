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
    pub connect_timeout: String,
    pub request_timeout: String,
    pub max_retries: i32,
    pub retry_timeout: String,
    pub priority_aging: String,
    pub min_storage_duration: String,
    pub unreferenced_grace: String,
    pub deleted_chunk_retention: String,
    pub upload_retention: String,
    pub cache_enabled: bool,
    pub cache_soft_limit: Option<String>,
    pub cache_entries: Option<i64>,
    pub upload_cache: bool,
    pub upload_cache_size: Option<String>,
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

impl StorageProfile {
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
        base: &crate::content::compression::Config,
    ) -> crate::content::compression::Config {
        let mut config = base.clone();
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
    anyhow::ensure!(
        profile.cdc_enabled
            || (!profile.compression_enabled
                && !profile.pack_enabled
                && !profile.pack_range_enabled),
        "raw storage cannot enable compression or packs"
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
    profile
        .compression_config(&crate::content::compression::Config::default())
        .validate()?;
    for duration in [
        &profile.connect_timeout,
        &profile.request_timeout,
        &profile.retry_timeout,
        &profile.priority_aging,
        &profile.unreferenced_grace,
        &profile.deleted_chunk_retention,
        &profile.upload_retention,
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
        seconds(duration)?;
    }
    if profile.min_storage_duration != "0s" {
        seconds(&profile.min_storage_duration)?;
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
