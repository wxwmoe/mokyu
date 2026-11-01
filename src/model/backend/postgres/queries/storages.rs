use super::*;

#[async_trait::async_trait]
impl StorageRecordStore for Backend {
    async fn list(&self) -> Result<Vec<StorageView>> {
        Ok(sqlx::query_as(VIEW_ORDERED).fetch_all(&self.pool).await?)
    }
    async fn view(&self, id: Uuid) -> Result<StorageView> {
        Ok(sqlx::query_as(VIEW_BY_ID)
            .bind(id)
            .fetch_one(&self.pool)
            .await?)
    }
    async fn named(&self, name: &str) -> Result<Option<StorageView>> {
        Ok(sqlx::query_as(VIEW_BY_NAME)
            .bind(name)
            .fetch_optional(&self.pool)
            .await?)
    }
    async fn id(&self, name: &str, parsed: Option<Uuid>) -> Result<Option<Uuid>> {
        Ok(
            sqlx::query_scalar("SELECT id FROM storages WHERE name=$1 OR id=$2::uuid")
                .bind(name)
                .bind(parsed)
                .fetch_optional(&self.pool)
                .await?,
        )
    }
    async fn record(&self, name: &str) -> Result<Option<StorageRow>> {
        Ok(sqlx::query_as("SELECT id,name,type,path,endpoint_url,remote_bucket_name,region,prefix,access_key_encrypted,secret_key_encrypted,identity_marker,block_key_id,enabled FROM storages WHERE name=$1").bind(name).fetch_optional(&self.pool).await?)
    }
    async fn creation(&self) -> Result<StorageCreation> {
        transactions::storage_creation(&self.pool).await
    }
    async fn edit(&self, name: &str) -> Result<StorageEdit> {
        transactions::storage_edit(&self.pool, name).await
    }
    async fn current_key(&self, name: &str, key: Option<&str>) -> Result<Uuid> {
        transactions::storage_current_key(&self.pool, name, key).await
    }

    async fn enabled(&self) -> Result<Vec<StorageRecord>> {
        Ok(enabled_storages().fetch_all(&self.pool).await?)
    }

    async fn enabled_by_id(&self, id: Uuid) -> Result<Option<StorageRecord>> {
        Ok(enabled_storage(id).fetch_optional(&self.pool).await?)
    }
}

#[async_trait::async_trait]
impl StorageProfileStore for Backend {
    async fn update(
        &self,
        name: String,
        profile: crate::storage::profile::StorageProfile,
    ) -> Result<Uuid> {
        transactions::update_storage_profile(&self.pool, name, profile).await
    }

    async fn enabled_profiles(&self) -> Result<Vec<StorageProfile>> {
        Ok(sqlx::query_as("SELECT p.* FROM storage_profiles p JOIN storages s ON s.id=p.storage_id WHERE s.enabled ORDER BY s.id").fetch_all(&self.pool).await?)
    }
    async fn load(
        &self,
        storage_id: uuid::Uuid,
    ) -> Result<crate::storage::profile::StorageProfile> {
        Ok(storage_profile(storage_id).fetch_one(&self.pool).await?)
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for StorageView {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            storage_type: row.try_get("storage_type")?,
            path: row.try_get("path")?,
            endpoint: row.try_get("endpoint")?,
            bucket: row.try_get("bucket")?,
            region: row.try_get("region")?,
            prefix: row.try_get("prefix")?,
            block_key_id: row.try_get("block_key_id")?,
            profile_revision: row.try_get("profile_revision")?,
            read_concurrency: row.try_get("read_concurrency")?,
            upload_concurrency: row.try_get("upload_concurrency")?,
            control_concurrency: row.try_get("control_concurrency")?,
            enabled: row.try_get("enabled")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for StorageRow {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            r#type: row.try_get("type")?,
            path: row.try_get("path")?,
            endpoint_url: row.try_get("endpoint_url")?,
            remote_bucket_name: row.try_get("remote_bucket_name")?,
            region: row.try_get("region")?,
            prefix: row.try_get("prefix")?,
            access_key_encrypted: row.try_get("access_key_encrypted")?,
            secret_key_encrypted: row.try_get("secret_key_encrypted")?,
            identity_marker: row.try_get("identity_marker")?,
            block_key_id: row.try_get("block_key_id")?,
            enabled: row.try_get("enabled")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for StorageRecord {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            store_type: row.try_get("store_type")?,
            path: row.try_get("path")?,
            endpoint: row.try_get("endpoint")?,
            bucket: row.try_get("bucket")?,
            region: row.try_get("region")?,
            prefix: row.try_get("prefix")?,
            access: row.try_get("access")?,
            secret: row.try_get("secret")?,
            identity_marker: row.try_get("identity_marker")?,
            revision: row.try_get("revision")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for crate::storage::profile::StorageProfile {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            storage_id: row.try_get("storage_id")?,
            revision: row.try_get("revision")?,
            cdc_enabled: row.try_get("cdc_enabled")?,
            deduplication_enabled: row.try_get("deduplication_enabled")?,
            write_processing: row.try_get("write_processing")?,
            write_durability: row.try_get("write_durability")?,
            write_flush_interval: format!("{}s", row.try_get::<i64, _>("write_flush_interval")?),
            write_backlog_max_age: format!("{}s", row.try_get::<i64, _>("write_backlog_max_age")?),
            background_scheduling: row.try_get("background_scheduling")?,
            public_cache_validation: row.try_get("public_cache_validation")?,
            cors_cache_enabled: row.try_get("cors_cache_enabled")?,
            public_cache_refresh_interval: row.try_get("public_cache_refresh_interval")?,
            public_cache_max_age: row.try_get("public_cache_max_age")?,
            read_source_validation: row.try_get("read_source_validation")?,
            source_validation_max_age: row.try_get("source_validation_max_age")?,
            read_integrity: row.try_get("read_integrity")?,
            file_delivery: row.try_get("file_delivery")?,
            file_delivery_prefix: row.try_get("file_delivery_prefix")?,
            connect_timeout: row.try_get("connect_timeout")?,
            request_timeout: row.try_get("request_timeout")?,
            max_retries: row.try_get("max_retries")?,
            retry_timeout: row.try_get("retry_timeout")?,
            priority_aging: row.try_get("priority_aging")?,
            min_storage_duration: row.try_get("min_storage_duration")?,
            unreferenced_grace: row.try_get("unreferenced_grace")?,
            deleted_chunk_retention: row.try_get("deleted_chunk_retention")?,
            upload_retention: row.try_get("upload_retention")?,
            multipart_idle_timeout: row.try_get("multipart_idle_timeout")?,
            min_free_space: row.try_get("min_free_space")?,
            cache_enabled: row.try_get("cache_enabled")?,
            cache_soft_limit: row.try_get("cache_soft_limit")?,
            cache_entries: row.try_get("cache_entries")?,
            upload_cache: row.try_get("upload_cache")?,
            upload_cache_size: row.try_get("upload_cache_size")?,
            upload_cache_overflow: row.try_get("upload_cache_overflow")?,
            compression_enabled: row.try_get("compression_enabled")?,
            compression_strategy: row.try_get("compression_strategy")?,
            compression_level: row.try_get("compression_level")?,
            compression_sample_level: row.try_get("compression_sample_level")?,
            compression_min_savings_percent: row.try_get("compression_min_savings_percent")?,
            compression_min_savings_bytes: row.try_get("compression_min_savings_bytes")?,
            pack_enabled: row.try_get("pack_enabled")?,
            pack_max_size: row.try_get("pack_max_size")?,
            pack_upload_flush_timeout: row.try_get("pack_upload_flush_timeout")?,
            pack_interval: row.try_get("pack_interval")?,
            pack_reuse_interval: row.try_get("pack_reuse_interval")?,
            pack_reclaim_interval: row.try_get("pack_reclaim_interval")?,
            pack_repack_interval: row.try_get("pack_repack_interval")?,
            pack_repack_cooldown: row.try_get("pack_repack_cooldown")?,
            pack_reclaim_min_savings_bytes: row.try_get("pack_reclaim_min_savings_bytes")?,
            pack_range_enabled: row.try_get("pack_range_enabled")?,
            pack_range_interval: row.try_get("pack_range_interval")?,
            pack_range_window: row.try_get("pack_range_window")?,
            pack_range_min_downloads: row.try_get("pack_range_min_downloads")?,
            pack_range_min_savings_percent: row.try_get("pack_range_min_savings_percent")?,
            pack_range_min_savings_bytes: row.try_get("pack_range_min_savings_bytes")?,
            pack_range_repack_after: row.try_get("pack_range_repack_after")?,
            pack_range_repack_window: row.try_get("pack_range_repack_window")?,
            pack_range_repack_retry_interval: row.try_get("pack_range_repack_retry_interval")?,
            pack_concurrency: row.try_get("pack_concurrency")?,
            integrity_concurrency: row.try_get("integrity_concurrency")?,
            integrity_rps: row.try_get("integrity_rps")?,
            integrity_bandwidth: row.try_get("integrity_bandwidth")?,
            integrity_batch_timeout: row.try_get("integrity_batch_timeout")?,
            read_concurrency: row.try_get("read_concurrency")?,
            upload_concurrency: row.try_get("upload_concurrency")?,
            control_concurrency: row.try_get("control_concurrency")?,
            pack_byte_budget: row.try_get("pack_byte_budget")?,
            integrity_byte_budget: row.try_get("integrity_byte_budget")?,
            maintenance_concurrency: row.try_get("maintenance_concurrency")?,
            maintenance_byte_budget: row.try_get("maintenance_byte_budget")?,
            migration_concurrency: row.try_get("migration_concurrency")?,
            migration_byte_budget: row.try_get("migration_byte_budget")?,
        })
    }
}

const PROFILE_COLUMNS: &[&str] = &[
    "cdc_enabled",
    "deduplication_enabled",
    "write_processing",
    "write_durability",
    "write_flush_interval",
    "write_backlog_max_age",
    "background_scheduling",
    "public_cache_validation",
    "cors_cache_enabled",
    "public_cache_refresh_interval",
    "public_cache_max_age",
    "read_source_validation",
    "source_validation_max_age",
    "read_integrity",
    "file_delivery",
    "file_delivery_prefix",
    "connect_timeout",
    "request_timeout",
    "max_retries",
    "retry_timeout",
    "priority_aging",
    "min_storage_duration",
    "unreferenced_grace",
    "deleted_chunk_retention",
    "upload_retention",
    "multipart_idle_timeout",
    "min_free_space",
    "cache_enabled",
    "cache_soft_limit",
    "cache_entries",
    "upload_cache",
    "upload_cache_size",
    "upload_cache_overflow",
    "compression_enabled",
    "compression_strategy",
    "compression_level",
    "compression_sample_level",
    "compression_min_savings_percent",
    "compression_min_savings_bytes",
    "pack_enabled",
    "pack_max_size",
    "pack_upload_flush_timeout",
    "pack_interval",
    "pack_reuse_interval",
    "pack_reclaim_interval",
    "pack_repack_interval",
    "pack_repack_cooldown",
    "pack_reclaim_min_savings_bytes",
    "pack_range_enabled",
    "pack_range_interval",
    "pack_range_window",
    "pack_range_min_downloads",
    "pack_range_min_savings_percent",
    "pack_range_min_savings_bytes",
    "pack_range_repack_after",
    "pack_range_repack_window",
    "pack_range_repack_retry_interval",
    "pack_concurrency",
    "integrity_concurrency",
    "integrity_rps",
    "integrity_bandwidth",
    "integrity_batch_timeout",
    "read_concurrency",
    "upload_concurrency",
    "control_concurrency",
    "pack_byte_budget",
    "integrity_byte_budget",
    "maintenance_concurrency",
    "maintenance_byte_budget",
    "migration_concurrency",
    "migration_byte_budget",
];

pub(in super::super) fn save_storage_profile(
    profile: &crate::storage::profile::StorageProfile,
) -> Result<Statement<'static>> {
    let columns = PROFILE_COLUMNS.join(",");
    let requested = PROFILE_COLUMNS
        .iter()
        .map(|column| format!("requested.{column}"))
        .collect::<Vec<_>>()
        .join(",");
    let statement = format!(
        "UPDATE storage_profiles AS current SET ({columns})=(SELECT {requested} FROM jsonb_populate_record(current,$2::jsonb) AS requested),revision=revision+1,updated_at=now() WHERE current.storage_id=$1 AND current.revision=$3"
    );
    let mut value = serde_json::to_value(profile)?;
    value["write_flush_interval"] = serde_json::json!(profile.write_flush_seconds()?);
    value["write_backlog_max_age"] = serde_json::json!(profile.write_backlog_seconds()?);
    value["write_durability"] =
        serde_json::json!(profile.write_durability.as_deref().unwrap_or("durable"));
    Ok(sqlx::query(sqlx::AssertSqlSafe(statement))
        .bind(profile.storage_id)
        .bind(value)
        .bind(profile.revision))
}

pub(in super::super) fn valid_storage_key(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM managed_keys WHERE id=$1 AND purpose='block' AND revoked_at IS NULL)").bind(id)
}

pub(in super::super) fn storage_mode_lock() -> Rows<'static, (Uuid, String)> {
    sqlx::query_as(
        "SELECT deployment_id,deployment_mode FROM mokyu_meta WHERE singleton FOR UPDATE",
    )
}

pub(in super::super) fn storage_relocation_ready() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT maintenance AND NOT EXISTS(SELECT 1 FROM tasks WHERE state IN ('queued','running','paused')) AND NOT EXISTS(SELECT 1 FROM uploads WHERE state IN ('active','completing')) AND NOT EXISTS(SELECT 1 FROM streams WHERE state='writing') AND NOT EXISTS(SELECT 1 FROM pending_uploads) AND NOT EXISTS(SELECT 1 FROM extents e JOIN streams s ON s.id=e.stream_id WHERE e.fragment_id IS NOT NULL AND s.state IN ('ready','writing')) AND NOT EXISTS(SELECT 1 FROM raw_sources WHERE state='writing' OR pending_upload OR durability<>'durable' OR medium='memory' OR upload_id IS NOT NULL) FROM mokyu_meta WHERE singleton AND deployment_mode='single'",
    )
}

pub(in super::super) fn relocate_raw_sources(id: Uuid, medium: &'static str) -> Statement<'static> {
    sqlx::query("UPDATE raw_sources SET medium=$1,revision=revision+1 WHERE storage_id=$2 AND medium IN ('local','shared_s3')").bind(medium).bind(id)
}

pub(in super::super) fn storage_row_lock<'q>(
    name: &'q str,
) -> Rows<'q, crate::model::storage::StorageRow> {
    sqlx::query_as("SELECT id,name,type,path,endpoint_url,remote_bucket_name,region,prefix,access_key_encrypted,secret_key_encrypted,identity_marker,block_key_id,enabled FROM storages WHERE name=$1 FOR UPDATE").bind(name)
}

const VIEW_ORDERED: &str = "SELECT s.id,s.name,s.type AS storage_type,s.path,s.endpoint_url AS endpoint,s.remote_bucket_name AS bucket,s.region,s.prefix,s.block_key_id,p.revision AS profile_revision,p.read_concurrency,p.upload_concurrency,p.control_concurrency,s.enabled,s.created_at,s.updated_at FROM storages s JOIN storage_profiles p ON p.storage_id=s.id ORDER BY s.name";

const VIEW_BY_ID: &str = "SELECT s.id,s.name,s.type AS storage_type,s.path,s.endpoint_url AS endpoint,s.remote_bucket_name AS bucket,s.region,s.prefix,s.block_key_id,p.revision AS profile_revision,p.read_concurrency,p.upload_concurrency,p.control_concurrency,s.enabled,s.created_at,s.updated_at FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.id=$1";

pub(in super::super) fn storage_profile_target<'q>(
    name: &'q str,
) -> Rows<'q, (Uuid, String, Option<Uuid>)> {
    sqlx::query_as("SELECT id,type,block_key_id FROM storages WHERE name=$1 FOR UPDATE").bind(name)
}

pub(in super::super) fn insert_storage_record<'q>(
    record: &'q BackendRecord,
    key: Option<Uuid>,
) -> Statement<'q> {
    sqlx::query("INSERT INTO storages(id,name,type,path,endpoint_url,remote_bucket_name,region,prefix,identity_marker,block_key_id,access_key_encrypted,secret_key_encrypted) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)").bind(record.id).bind(&record.name).bind(&record.r#type).bind(&record.path).bind(&record.endpoint_url).bind(&record.remote_bucket_name).bind(&record.region).bind(&record.prefix).bind(&record.identity_marker).bind(key).bind(&record.access_key_encrypted).bind(&record.secret_key_encrypted)
}

pub(in super::super) fn insert_storage_profile(
    id: Uuid,
    read: Option<i32>,
    upload: Option<i32>,
    control: Option<i32>,
    remote: bool,
) -> Statement<'static> {
    sqlx::query("INSERT INTO storage_profiles(storage_id,read_concurrency,upload_concurrency,control_concurrency,cache_enabled,upload_cache,compression_enabled,pack_enabled,pack_range_enabled,min_free_space) VALUES($1,$2,$3,$4,$5,$5,$5,$5,$5,$6)").bind(id).bind(read).bind(upload).bind(control).bind(remote).bind((!remote).then_some("1GiB"))
}

pub(in super::super) fn storage_profile_lock(id: Uuid) -> Rows<'static, StorageProfile> {
    sqlx::query_as("SELECT storage_id,revision,cdc_enabled,deduplication_enabled,write_processing,CASE WHEN (SELECT deployment_mode FROM mokyu_meta WHERE singleton)='single' THEN write_durability END AS write_durability,write_flush_interval,write_backlog_max_age,background_scheduling,public_cache_validation,cors_cache_enabled,public_cache_refresh_interval,public_cache_max_age,read_source_validation,source_validation_max_age,read_integrity,file_delivery,file_delivery_prefix,connect_timeout,request_timeout,max_retries,retry_timeout,priority_aging,min_storage_duration,unreferenced_grace,deleted_chunk_retention,upload_retention,multipart_idle_timeout,min_free_space,cache_enabled,cache_soft_limit,cache_entries,upload_cache,upload_cache_size,upload_cache_overflow,compression_enabled,compression_strategy,compression_level,compression_sample_level,compression_min_savings_percent,compression_min_savings_bytes,pack_enabled,pack_max_size,pack_upload_flush_timeout,pack_interval,pack_reuse_interval,pack_reclaim_interval,pack_repack_interval,pack_repack_cooldown,pack_reclaim_min_savings_bytes,pack_range_enabled,pack_range_interval,pack_range_window,pack_range_min_downloads,pack_range_min_savings_percent,pack_range_min_savings_bytes,pack_range_repack_after,pack_range_repack_window,pack_range_repack_retry_interval,pack_concurrency,integrity_concurrency,integrity_rps,integrity_bandwidth,integrity_batch_timeout,read_concurrency,upload_concurrency,control_concurrency,pack_byte_budget,integrity_byte_budget,maintenance_concurrency,maintenance_byte_budget,migration_concurrency,migration_byte_budget FROM storage_profiles WHERE storage_id=$1 FOR UPDATE").bind(id)
}

pub(in super::super) fn update_storage_record<'q>(
    record: &'q BackendRecord,
    key: Option<Uuid>,
) -> Statement<'q> {
    sqlx::query("UPDATE storages SET type=$2,path=$3,endpoint_url=$4,remote_bucket_name=$5,region=$6,prefix=$7,block_key_id=$8,access_key_encrypted=$9,secret_key_encrypted=$10,enabled=$11,updated_at=now() WHERE id=$1").bind(record.id).bind(&record.r#type).bind(&record.path).bind(&record.endpoint_url).bind(&record.remote_bucket_name).bind(&record.region).bind(&record.prefix).bind(key).bind(&record.access_key_encrypted).bind(&record.secret_key_encrypted).bind(record.enabled)
}

pub(in super::super) fn current_storage_key<'q>(
    id: Option<Uuid>,
    name: &'q str,
) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("SELECT id FROM managed_keys WHERE ((($1::uuid IS NOT NULL AND id=$1) OR name=$2) AND purpose='block' AND revoked_at IS NULL)").bind(id).bind(name)
}

pub(in super::super) fn update_storage_key(id: Uuid, key: Option<Uuid>) -> Statement<'static> {
    sqlx::query("UPDATE storages SET block_key_id=$2,updated_at=now() WHERE id=$1")
        .bind(id)
        .bind(key)
}

pub(in super::super) fn bump_storage_profile(id: Uuid) -> Statement<'static> {
    sqlx::query(
        "UPDATE storage_profiles SET revision=revision+1,updated_at=now() WHERE storage_id=$1",
    )
    .bind(id)
}

const VIEW_BY_NAME: &str = "SELECT s.id,s.name,s.type AS storage_type,s.path,s.endpoint_url AS endpoint,s.remote_bucket_name AS bucket,s.region,s.prefix,s.block_key_id,p.revision AS profile_revision,p.read_concurrency,p.upload_concurrency,p.control_concurrency,s.enabled,s.created_at,s.updated_at FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.name=$1";
