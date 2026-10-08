use anyhow::Result;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use utoipa::ToSchema;
use uuid::Uuid;

/// Typed, revisioned policy shared by every node that serves a storage.
#[derive(Clone, Debug, Deserialize, Serialize, sqlx::FromRow, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct StorageProfile {
    pub storage_id: Uuid,
    pub revision: i64,
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

impl StorageProfile {
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

    pub(crate) async fn load(pool: &PgPool, storage_id: Uuid) -> Result<Self> {
        Ok(sqlx::query_as("SELECT storage_id,revision,connect_timeout,request_timeout,max_retries,retry_timeout,priority_aging,min_storage_duration,unreferenced_grace,deleted_chunk_retention,upload_retention,cache_enabled,cache_soft_limit,cache_entries,upload_cache,upload_cache_size,compression_enabled,compression_strategy,compression_level,compression_sample_level,compression_min_savings_percent,compression_min_savings_bytes,pack_enabled,pack_max_size,pack_upload_flush_timeout,pack_interval,pack_reuse_interval,pack_reclaim_interval,pack_repack_interval,pack_repack_cooldown,pack_reclaim_min_savings_bytes,pack_range_enabled,pack_range_interval,pack_range_window,pack_range_min_downloads,pack_range_min_savings_percent,pack_range_min_savings_bytes,pack_range_repack_after,pack_range_repack_window,pack_range_repack_retry_interval,pack_concurrency,integrity_concurrency,integrity_rps,integrity_bandwidth,integrity_batch_timeout,read_concurrency,upload_concurrency,control_concurrency,pack_byte_budget,integrity_byte_budget,maintenance_concurrency,maintenance_byte_budget,migration_concurrency,migration_byte_budget FROM storage_profiles WHERE storage_id=$1")
            .bind(storage_id)
            .fetch_one(pool)
            .await?)
    }
}
