use super::*;

pub(in super::super) fn storage_deployment() -> Rows<'static, (Uuid, String)> {
    sqlx::query_as("SELECT deployment_id,deployment_mode FROM mokyu_meta WHERE id=1")
}

pub(in super::super) fn valid_storage_key(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM managed_keys WHERE id=? AND purpose='block' AND NOT recovery_read_only AND revoked_at IS NULL",
    )
    .bind(id)
}

pub(in super::super) fn insert_storage_record(
    record: &BackendRecord,
    key: Option<Uuid>,
) -> Statement<'_> {
    sqlx::query("INSERT INTO storages(id,name,type,path,endpoint_url,remote_bucket_name,region,prefix,identity_marker,block_key_id,access_key_encrypted,secret_key_encrypted) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)").bind(record.id).bind(&record.name).bind(&record.r#type).bind(&record.path).bind(&record.endpoint_url).bind(&record.remote_bucket_name).bind(&record.region).bind(&record.prefix).bind(&record.identity_marker).bind(key).bind(&record.access_key_encrypted).bind(&record.secret_key_encrypted)
}

pub(in super::super) fn insert_storage_profile(
    id: Uuid,
    read: Option<i32>,
    upload: Option<i32>,
    control: Option<i32>,
    remote: bool,
) -> Statement<'static> {
    sqlx::query("INSERT INTO storage_profiles(storage_id,read_concurrency,upload_concurrency,control_concurrency,cache_enabled,upload_cache,compression_enabled,pack_enabled,min_free_space) VALUES(?,?,?,?,?,?,?,?,?)").bind(id).bind(read).bind(upload).bind(control).bind(remote).bind(remote).bind(remote).bind(true).bind((!remote).then_some("1GiB"))
}

pub(in super::super) fn update_storage_record(
    record: &BackendRecord,
    key: Option<Uuid>,
) -> Statement<'_> {
    sqlx::query("UPDATE storages SET type=?,path=?,endpoint_url=?,remote_bucket_name=?,region=?,prefix=?,block_key_id=?,access_key_encrypted=?,secret_key_encrypted=?,enabled=?,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=?").bind(&record.r#type).bind(&record.path).bind(&record.endpoint_url).bind(&record.remote_bucket_name).bind(&record.region).bind(&record.prefix).bind(key).bind(&record.access_key_encrypted).bind(&record.secret_key_encrypted).bind(record.enabled).bind(record.id)
}

pub(in super::super) fn current_storage_key(id: Option<Uuid>, name: &str) -> Scalar<'_, Uuid> {
    sqlx::query_scalar("SELECT id FROM managed_keys WHERE (id=? OR name=?) AND purpose='block' AND NOT recovery_read_only AND revoked_at IS NULL").bind(id).bind(name)
}

pub(in super::super) fn update_storage_key(id: Uuid, key: Option<Uuid>) -> Statement<'static> {
    sqlx::query("UPDATE storages SET block_key_id=?,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=?")
        .bind(key)
        .bind(id)
}

pub(in super::super) fn bump_storage_profile(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE storage_profiles SET revision=revision+1,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE storage_id=?").bind(id)
}

pub(in super::super) fn enabled_storages() -> Rows<'static, StorageRecord> {
    sqlx::query_as(
        "SELECT s.id,s.type AS store_type,s.path,s.endpoint_url AS endpoint,s.remote_bucket_name AS bucket,s.region,s.prefix,s.access_key_encrypted AS access,s.secret_key_encrypted AS secret,s.identity_marker,p.revision FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.enabled ORDER BY s.created_at,s.id",
    )
}

pub(in super::super) fn enabled_storage(id: Uuid) -> Rows<'static, StorageRecord> {
    sqlx::query_as("SELECT s.id,s.type AS store_type,s.path,s.endpoint_url AS endpoint,s.remote_bucket_name AS bucket,s.region,s.prefix,s.access_key_encrypted AS access,s.secret_key_encrypted AS secret,s.identity_marker,p.revision FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.id=? AND s.enabled")
        .bind(id)
}

pub(in super::super) fn storage_profile(
    id: Uuid,
) -> Rows<'static, crate::storage::profile::StorageProfile> {
    sqlx::query_as("SELECT storage_id,revision,cdc_enabled,deduplication_enabled,write_processing,write_durability,write_flush_interval,write_backlog_max_age,background_scheduling,public_cache_validation,cors_cache_enabled,public_cache_refresh_interval,public_cache_max_age,read_source_validation,source_validation_max_age,read_integrity,file_delivery,file_delivery_prefix,http_compression_enabled,connect_timeout,request_timeout,max_retries,retry_timeout,priority_aging,min_storage_duration,unreferenced_grace,deleted_chunk_retention,upload_retention,multipart_idle_timeout,min_free_space,cache_enabled,cache_soft_limit,cache_entries,upload_cache,upload_cache_size,upload_cache_overflow,compression_enabled,compression_strategy,compression_level,compression_sample_level,compression_min_savings_percent,compression_min_savings_bytes,pack_enabled,pack_max_size,pack_upload_flush_timeout,pack_interval,pack_reclaim_interval,pack_repack_cooldown,pack_reclaim_min_savings_bytes,pack_concurrency,integrity_concurrency,integrity_rps,integrity_bandwidth,integrity_batch_timeout,read_concurrency,upload_concurrency,control_concurrency,pack_byte_budget,integrity_byte_budget,maintenance_concurrency,maintenance_byte_budget,migration_concurrency,migration_byte_budget FROM storage_profiles WHERE storage_id=?")
        .bind(id)
}

pub(in super::super) fn storage_relocation_ready() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT maintenance AND NOT EXISTS(SELECT 1 FROM tasks WHERE state IN ('queued','running','paused')) AND NOT EXISTS(SELECT 1 FROM uploads WHERE state IN ('active','completing')) AND NOT EXISTS(SELECT 1 FROM streams WHERE state='writing') AND NOT EXISTS(SELECT 1 FROM pending_uploads) AND NOT EXISTS(SELECT 1 FROM extents e JOIN streams s ON s.id=e.stream_id WHERE e.fragment_id IS NOT NULL AND s.state IN ('ready','writing')) AND NOT EXISTS(SELECT 1 FROM raw_sources WHERE state='writing' OR pending_upload OR durability<>'durable' OR medium='memory' OR upload_id IS NOT NULL) FROM mokyu_meta WHERE id=1",
    )
}

pub(in super::super) fn relocate_raw_sources(id: Uuid, medium: &'static str) -> Statement<'static> {
    sqlx::query("UPDATE raw_sources SET medium=?,revision=revision+1 WHERE storage_id=? AND medium IN ('local','shared_s3')").bind(medium).bind(id)
}

pub(in super::super) fn storage_row_lock<'q>(
    name: &'q str,
) -> Rows<'q, crate::model::storage::StorageRow> {
    sqlx::query_as("SELECT id,name,type,path,endpoint_url,remote_bucket_name,region,prefix,access_key_encrypted,secret_key_encrypted,identity_marker,block_key_id,enabled FROM storages WHERE name=?").bind(name)
}

pub(in super::super) fn storage_profile_target<'q>(
    name: &'q str,
) -> Rows<'q, (Uuid, String, Option<Uuid>)> {
    sqlx::query_as("SELECT id,type,block_key_id FROM storages WHERE name=?").bind(name)
}

pub(in super::super) fn storage_profile_lock(id: Uuid) -> Rows<'static, StorageProfile> {
    sqlx::query_as("SELECT storage_id,revision,cdc_enabled,deduplication_enabled,write_processing,write_durability,write_flush_interval,write_backlog_max_age,background_scheduling,public_cache_validation,cors_cache_enabled,public_cache_refresh_interval,public_cache_max_age,read_source_validation,source_validation_max_age,read_integrity,file_delivery,file_delivery_prefix,http_compression_enabled,connect_timeout,request_timeout,max_retries,retry_timeout,priority_aging,min_storage_duration,unreferenced_grace,deleted_chunk_retention,upload_retention,multipart_idle_timeout,min_free_space,cache_enabled,cache_soft_limit,cache_entries,upload_cache,upload_cache_size,upload_cache_overflow,compression_enabled,compression_strategy,compression_level,compression_sample_level,compression_min_savings_percent,compression_min_savings_bytes,pack_enabled,pack_max_size,pack_upload_flush_timeout,pack_interval,pack_reclaim_interval,pack_repack_cooldown,pack_reclaim_min_savings_bytes,pack_concurrency,integrity_concurrency,integrity_rps,integrity_bandwidth,integrity_batch_timeout,read_concurrency,upload_concurrency,control_concurrency,pack_byte_budget,integrity_byte_budget,maintenance_concurrency,maintenance_byte_budget,migration_concurrency,migration_byte_budget FROM storage_profiles WHERE storage_id=?").bind(id)
}

const VIEW_ORDERED: &str = "SELECT s.id,s.name,s.type AS storage_type,s.path,s.endpoint_url AS endpoint,s.remote_bucket_name AS bucket,s.region,s.prefix,s.block_key_id,p.revision AS profile_revision,p.read_concurrency,p.upload_concurrency,p.control_concurrency,s.enabled,s.created_at,s.updated_at FROM storages s JOIN storage_profiles p ON p.storage_id=s.id ORDER BY s.name";

const VIEW_BY_ID: &str = "SELECT s.id,s.name,s.type AS storage_type,s.path,s.endpoint_url AS endpoint,s.remote_bucket_name AS bucket,s.region,s.prefix,s.block_key_id,p.revision AS profile_revision,p.read_concurrency,p.upload_concurrency,p.control_concurrency,s.enabled,s.created_at,s.updated_at FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.id=?";

const VIEW_BY_NAME: &str = "SELECT s.id,s.name,s.type AS storage_type,s.path,s.endpoint_url AS endpoint,s.remote_bucket_name AS bucket,s.region,s.prefix,s.block_key_id,p.revision AS profile_revision,p.read_concurrency,p.upload_concurrency,p.control_concurrency,s.enabled,s.created_at,s.updated_at FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.name=?";

#[async_trait::async_trait]
impl StorageRecordStore for Backend {
    async fn journal_descriptor(
        &self,
        id: Uuid,
    ) -> Result<Option<crate::storage::marker::Journal>> {
        let value: Option<sqlx::types::Json<crate::storage::marker::Journal>> = sqlx::query_scalar(
            "SELECT descriptor FROM journal_stores WHERE storage_id=? AND descriptor IS NOT NULL",
        )
        .bind(id)
        .fetch_optional(&self.read)
        .await?;
        Ok(value.map(|v| v.0))
    }
    async fn list(&self) -> Result<Vec<StorageView>> {
        Ok(sqlx::query_as(VIEW_ORDERED).fetch_all(&self.read).await?)
    }
    async fn view(&self, id: Uuid) -> Result<StorageView> {
        Ok(sqlx::query_as(VIEW_BY_ID)
            .bind(id)
            .fetch_one(&self.read)
            .await?)
    }
    async fn named(&self, name: &str) -> Result<Option<StorageView>> {
        Ok(sqlx::query_as(VIEW_BY_NAME)
            .bind(name)
            .fetch_optional(&self.read)
            .await?)
    }
    async fn id(&self, name: &str, parsed: Option<Uuid>) -> Result<Option<Uuid>> {
        Ok(
            sqlx::query_scalar("SELECT id FROM storages WHERE name=? OR id=?")
                .bind(name)
                .bind(parsed)
                .fetch_optional(&self.read)
                .await?,
        )
    }
    async fn record(&self, name: &str) -> Result<Option<StorageRow>> {
        Ok(sqlx::query_as("SELECT id,name,type,path,endpoint_url,remote_bucket_name,region,prefix,access_key_encrypted,secret_key_encrypted,identity_marker,block_key_id,enabled FROM storages WHERE name=?").bind(name).fetch_optional(&self.read).await?)
    }
    async fn creation(&self) -> Result<StorageCreation> {
        transactions::storage_creation(&self.read, &self.write).await
    }
    async fn edit(&self, name: &str) -> Result<StorageEdit> {
        transactions::storage_edit(&self.read, &self.write, name).await
    }
    async fn current_key(&self, name: &str, key: Option<&str>) -> Result<Uuid> {
        transactions::storage_current_key(&self.write, name, key).await
    }

    async fn enabled(&self) -> Result<Vec<StorageRecord>> {
        Ok(enabled_storages().fetch_all(&self.read).await?)
    }

    async fn enabled_by_id(&self, id: Uuid) -> Result<Option<StorageRecord>> {
        Ok(enabled_storage(id).fetch_optional(&self.read).await?)
    }
}

#[async_trait::async_trait]
impl StorageProfileStore for Backend {
    async fn update(
        &self,
        name: String,
        profile: crate::storage::profile::StorageProfile,
    ) -> Result<Uuid> {
        transactions::update_storage_profile(&self.write, name, profile).await
    }

    async fn enabled_profiles(&self) -> Result<Vec<StorageProfile>> {
        Ok(sqlx::query_as("SELECT p.* FROM storage_profiles p JOIN storages s ON s.id=p.storage_id WHERE s.enabled ORDER BY s.id").fetch_all(&self.read).await?)
    }
    async fn load(
        &self,
        storage_id: uuid::Uuid,
    ) -> Result<crate::storage::profile::StorageProfile> {
        Ok(storage_profile(storage_id).fetch_one(&self.read).await?)
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for StorageView {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
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
            created_at: date(row, "created_at")?,
            updated_at: date(row, "updated_at")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for StorageRow {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
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

impl<'r> sqlx::FromRow<'r, SqliteRow> for StorageRecord {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
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

impl<'r> sqlx::FromRow<'r, SqliteRow> for crate::storage::profile::StorageProfile {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
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
            http_compression_enabled: row.try_get("http_compression_enabled")?,
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
            pack_reclaim_interval: row.try_get("pack_reclaim_interval")?,
            pack_repack_cooldown: row.try_get("pack_repack_cooldown")?,
            pack_reclaim_min_savings_bytes: row.try_get("pack_reclaim_min_savings_bytes")?,
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

pub(in super::super) fn save_storage_profile(profile: &StorageProfile) -> Result<Statement<'_>> {
    Ok(sqlx::query("UPDATE storage_profiles SET cdc_enabled=?,deduplication_enabled=?,write_processing=?,write_durability=?,write_flush_interval=?,write_backlog_max_age=?,background_scheduling=?,public_cache_validation=?,cors_cache_enabled=?,public_cache_refresh_interval=?,public_cache_max_age=?,read_source_validation=?,source_validation_max_age=?,read_integrity=?,file_delivery=?,file_delivery_prefix=?,http_compression_enabled=?,connect_timeout=?,request_timeout=?,max_retries=?,retry_timeout=?,priority_aging=?,min_storage_duration=?,unreferenced_grace=?,deleted_chunk_retention=?,upload_retention=?,multipart_idle_timeout=?,min_free_space=?,cache_enabled=?,cache_soft_limit=?,cache_entries=?,upload_cache=?,upload_cache_size=?,upload_cache_overflow=?,compression_enabled=?,compression_strategy=?,compression_level=?,compression_sample_level=?,compression_min_savings_percent=?,compression_min_savings_bytes=?,pack_enabled=?,pack_max_size=?,pack_upload_flush_timeout=?,pack_interval=?,pack_reclaim_interval=?,pack_repack_cooldown=?,pack_reclaim_min_savings_bytes=?,pack_concurrency=?,integrity_concurrency=?,integrity_rps=?,integrity_bandwidth=?,integrity_batch_timeout=?,read_concurrency=?,upload_concurrency=?,control_concurrency=?,pack_byte_budget=?,integrity_byte_budget=?,maintenance_concurrency=?,maintenance_byte_budget=?,migration_concurrency=?,migration_byte_budget=?,revision=revision+1,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE storage_id=? AND revision=?")
        .bind(profile.cdc_enabled)
        .bind(profile.deduplication_enabled)
        .bind(&profile.write_processing)
        .bind(profile.write_durability.as_deref().unwrap_or("durable"))
        .bind(profile.write_flush_seconds()? as i64)
        .bind(profile.write_backlog_seconds()? as i64)
        .bind(&profile.background_scheduling)
        .bind(&profile.public_cache_validation)
        .bind(profile.cors_cache_enabled)
        .bind(&profile.public_cache_refresh_interval)
        .bind(&profile.public_cache_max_age)
        .bind(&profile.read_source_validation)
        .bind(&profile.source_validation_max_age)
        .bind(&profile.read_integrity)
        .bind(&profile.file_delivery)
        .bind(&profile.file_delivery_prefix)
        .bind(profile.http_compression_enabled)
        .bind(&profile.connect_timeout)
        .bind(&profile.request_timeout)
        .bind(profile.max_retries)
        .bind(&profile.retry_timeout)
        .bind(&profile.priority_aging)
        .bind(&profile.min_storage_duration)
        .bind(&profile.unreferenced_grace)
        .bind(&profile.deleted_chunk_retention)
        .bind(&profile.upload_retention)
        .bind(&profile.multipart_idle_timeout)
        .bind(&profile.min_free_space)
        .bind(profile.cache_enabled)
        .bind(&profile.cache_soft_limit)
        .bind(profile.cache_entries)
        .bind(profile.upload_cache)
        .bind(&profile.upload_cache_size)
        .bind(&profile.upload_cache_overflow)
        .bind(profile.compression_enabled)
        .bind(&profile.compression_strategy)
        .bind(profile.compression_level)
        .bind(profile.compression_sample_level)
        .bind(profile.compression_min_savings_percent)
        .bind(profile.compression_min_savings_bytes)
        .bind(profile.pack_enabled)
        .bind(&profile.pack_max_size)
        .bind(&profile.pack_upload_flush_timeout)
        .bind(&profile.pack_interval)
        .bind(&profile.pack_reclaim_interval)
        .bind(&profile.pack_repack_cooldown)
        .bind(&profile.pack_reclaim_min_savings_bytes)
        .bind(profile.pack_concurrency)
        .bind(profile.integrity_concurrency)
        .bind(profile.integrity_rps)
        .bind(&profile.integrity_bandwidth)
        .bind(&profile.integrity_batch_timeout)
        .bind(profile.read_concurrency)
        .bind(profile.upload_concurrency)
        .bind(profile.control_concurrency)
        .bind(&profile.pack_byte_budget)
        .bind(&profile.integrity_byte_budget)
        .bind(profile.maintenance_concurrency)
        .bind(&profile.maintenance_byte_budget)
        .bind(profile.migration_concurrency)
        .bind(&profile.migration_byte_budget)
        .bind(profile.storage_id).bind(profile.revision))
}

impl<'r> sqlx::FromRow<'r, sqlx::sqlite::SqliteRow> for crate::storage::model::Record {
    fn from_row(row: &'r sqlx::sqlite::SqliteRow) -> sqlx::Result<Self> {
        Ok((&StorageRow::from_row(row)?).into())
    }
}
