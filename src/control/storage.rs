use super::Identity;
use crate::{
    app::App,
    codec,
    http::HttpError,
    storage::{
        model::{Record as BackendRecord, identity_marker as storage_identity_marker},
        profile::StorageProfile,
    },
};
use anyhow::Context;
use axum::{
    Json,
    extract::{Extension, Path, State},
    http::StatusCode,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Serialize, sqlx::FromRow, ToSchema)]
pub(crate) struct StorageView {
    id: Uuid,
    name: String,
    #[serde(rename = "type")]
    storage_type: String,
    path: Option<String>,
    endpoint: Option<String>,
    bucket: Option<String>,
    region: Option<String>,
    prefix: String,
    block_key_id: Option<Uuid>,
    profile_revision: i64,
    read_concurrency: Option<i32>,
    upload_concurrency: Option<i32>,
    control_concurrency: Option<i32>,
    enabled: bool,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct StorageInput {
    pub(crate) profile: Option<StorageProfile>,
    pub(crate) name: String,
    #[serde(rename = "type")]
    pub(crate) storage_type: String,
    pub(crate) path: Option<String>,
    pub(crate) endpoint: Option<String>,
    pub(crate) bucket: Option<String>,
    pub(crate) region: Option<String>,
    #[serde(default)]
    pub(crate) prefix: String,
    pub(crate) block_key_id: Option<Uuid>,
    pub(crate) initialize: bool,
    pub(crate) read_concurrency: Option<i32>,
    pub(crate) upload_concurrency: Option<i32>,
    pub(crate) control_concurrency: Option<i32>,
    pub(crate) access_key: Option<String>,
    pub(crate) secret_key: Option<String>,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoragePatch {
    pub(crate) profile: Option<StorageProfile>,
    #[serde(rename = "type")]
    pub(crate) storage_type: Option<String>,
    pub(crate) path: Option<String>,
    pub(crate) endpoint: Option<String>,
    pub(crate) bucket: Option<String>,
    pub(crate) region: Option<String>,
    pub(crate) prefix: Option<String>,
    #[serde(default, deserialize_with = "nullable_patch")]
    pub(crate) block_key_id: Option<Option<Uuid>>,
    pub(crate) access_key: Option<String>,
    pub(crate) secret_key: Option<String>,
    pub(crate) enabled: Option<bool>,
    #[serde(default, deserialize_with = "nullable_patch")]
    pub(crate) read_concurrency: Option<Option<i32>>,
    #[serde(default, deserialize_with = "nullable_patch")]
    pub(crate) upload_concurrency: Option<Option<i32>>,
    #[serde(default, deserialize_with = "nullable_patch")]
    pub(crate) control_concurrency: Option<Option<i32>>,
}

fn nullable_patch<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct StorageKeyInput {
    pub(crate) key: Option<String>,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct StorageUnpackInput {
    action: String,
    pack_id: i64,
    confirmation: Option<String>,
}

#[derive(Clone, sqlx::FromRow)]
struct StorageRow {
    id: Uuid,
    name: String,
    r#type: String,
    path: Option<String>,
    endpoint_url: Option<String>,
    remote_bucket_name: Option<String>,
    region: Option<String>,
    prefix: String,
    access_key_encrypted: Option<Vec<u8>>,
    secret_key_encrypted: Option<Vec<u8>>,
    identity_marker: serde_json::Value,
    block_key_id: Option<Uuid>,
    enabled: bool,
}

fn validate_name(value: &str) -> anyhow::Result<String> {
    let value = value.trim();
    anyhow::ensure!(
        (1..=128).contains(&value.len())
            && !value.contains('/')
            && !value.chars().any(char::is_control),
        "storage name must contain 1..128 safe bytes"
    );
    Ok(value.to_owned())
}

fn validate_storage_type(value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        matches!(value, "local" | "s3"),
        "storage type is local or s3"
    );
    Ok(())
}

fn normalize_prefix(value: String) -> anyhow::Result<String> {
    let value = value.trim_matches('/').to_owned();
    if value.is_empty() {
        return Ok(value);
    }
    anyhow::ensure!(
        value.len() <= 1024
            && !value.chars().any(char::is_control)
            && !value
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == ".."),
        "storage prefix is invalid"
    );
    Ok(value)
}

fn validate_concurrency(value: Option<i32>, field: &'static str) -> anyhow::Result<()> {
    anyhow::ensure!(
        value.is_none_or(|n| n > 0),
        "{field} concurrency must be positive"
    );
    Ok(())
}

async fn validate_block_key(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    key_id: Option<Uuid>,
) -> anyhow::Result<()> {
    let Some(key_id) = key_id else {
        return Ok(());
    };
    let valid: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM managed_keys WHERE id=$1 AND purpose='block' AND revoked_at IS NULL)",
    )
    .bind(key_id)
    .fetch_one(&mut **tx)
    .await?;
    anyhow::ensure!(valid, "active block key was not found");
    Ok(())
}

fn required(value: Option<String>, field: &'static str) -> anyhow::Result<String> {
    let value = value.context(field)?;
    anyhow::ensure!(!value.trim().is_empty(), "{field} must not be empty");
    Ok(value.trim().to_owned())
}

fn endpoint(value: String) -> anyhow::Result<String> {
    let value = required(Some(value), "S3 endpoint")?;
    anyhow::ensure!(
        (value.starts_with("http://") || value.starts_with("https://"))
            && !value.chars().any(char::is_whitespace)
            && !value.contains(['?', '#']),
        "S3 endpoint must be an absolute HTTP or HTTPS URL without a query"
    );
    Ok(value)
}

const VIEW_ORDERED: &str = "SELECT s.id,s.name,s.type AS storage_type,s.path,s.endpoint_url AS endpoint,s.remote_bucket_name AS bucket,s.region,s.prefix,s.block_key_id,p.revision AS profile_revision,p.read_concurrency,p.upload_concurrency,p.control_concurrency,s.enabled,s.created_at,s.updated_at FROM storages s JOIN storage_profiles p ON p.storage_id=s.id ORDER BY s.name";
const VIEW_BY_ID: &str = "SELECT s.id,s.name,s.type AS storage_type,s.path,s.endpoint_url AS endpoint,s.remote_bucket_name AS bucket,s.region,s.prefix,s.block_key_id,p.revision AS profile_revision,p.read_concurrency,p.upload_concurrency,p.control_concurrency,s.enabled,s.created_at,s.updated_at FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.id=$1";

fn row_record(row: &StorageRow) -> BackendRecord {
    BackendRecord {
        id: row.id,
        name: row.name.clone(),
        r#type: row.r#type.clone(),
        path: row.path.clone(),
        endpoint_url: row.endpoint_url.clone(),
        remote_bucket_name: row.remote_bucket_name.clone(),
        region: row.region.clone(),
        prefix: row.prefix.clone(),
        access_key_encrypted: row.access_key_encrypted.clone(),
        secret_key_encrypted: row.secret_key_encrypted.clone(),
        identity_marker: row.identity_marker.clone(),
        enabled: row.enabled,
    }
}

fn build_record(
    app: &App,
    id: Uuid,
    name: String,
    kind: String,
    path: Option<String>,
    endpoint_value: Option<String>,
    bucket: Option<String>,
    region: Option<String>,
    prefix: String,
    access_key: Option<String>,
    secret_key: Option<String>,
    identity_marker: serde_json::Value,
) -> anyhow::Result<BackendRecord> {
    let (path, endpoint_url, remote_bucket_name, region, access, secret) = if kind == "local" {
        anyhow::ensure!(
            endpoint_value.is_none()
                && bucket.is_none()
                && region.is_none()
                && access_key.is_none()
                && secret_key.is_none(),
            "local storages cannot include S3 settings"
        );
        (
            Some(required(
                path.or_else(|| Some("/data/storage".into())),
                "storage path",
            )?),
            None,
            None,
            None,
            None,
            None,
        )
    } else {
        anyhow::ensure!(path.is_none(), "S3 storages cannot include a local path");
        let access_key = required(access_key, "S3 access key")?;
        let secret_key = required(secret_key, "S3 secret key")?;
        (
            None,
            Some(endpoint(required(endpoint_value, "S3 endpoint")?)?),
            Some(required(bucket, "S3 bucket")?),
            Some(required(region, "S3 region")?),
            Some(codec::protect(
                access_key.as_bytes(),
                &app.secrets.credential_key,
                format!("storage-{id}-access").as_bytes(),
            )?),
            Some(codec::protect(
                secret_key.as_bytes(),
                &app.secrets.credential_key,
                format!("storage-{id}-secret").as_bytes(),
            )?),
        )
    };
    Ok(BackendRecord {
        id,
        name,
        r#type: kind,
        path,
        endpoint_url,
        remote_bucket_name,
        region,
        prefix,
        access_key_encrypted: access,
        secret_key_encrypted: secret,
        identity_marker,
        enabled: true,
    })
}

async fn lock_mode(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<(Uuid, String)> {
    Ok(sqlx::query_as(
        "SELECT deployment_id,deployment_mode FROM mokyu_meta WHERE singleton FOR UPDATE",
    )
    .fetch_one(&mut **tx)
    .await?)
}

async fn load_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    name: &str,
) -> anyhow::Result<StorageRow> {
    Ok(sqlx::query_as("SELECT id,name,type,path,endpoint_url,remote_bucket_name,region,prefix,access_key_encrypted,secret_key_encrypted,identity_marker,block_key_id,enabled FROM storages WHERE name=$1 FOR UPDATE").bind(name).fetch_optional(&mut **tx).await?.context("storage was not found")?)
}

async fn list_rows(db: &sqlx::PgPool) -> anyhow::Result<Vec<StorageView>> {
    Ok(sqlx::query_as(VIEW_ORDERED).fetch_all(db).await?)
}

#[utoipa::path(get, operation_id = "storages", path = "/api/v1/storages", responses((status = 200, body = Vec<StorageView>)))]
pub(crate) async fn list(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<Vec<StorageView>>, HttpError> {
    let tx = super::users::admin_transaction(&app, &actor.principal).await?;
    tx.commit().await?;
    Ok(Json(list_rows(&app.db).await?))
}

#[utoipa::path(
    get,
    operation_id = "storage_profile",
    path = "/api/v1/storages/{name}/profile",
    params(("name" = String, Path)),
    responses((status = 200, body = StorageProfile), (status = 404, body = crate::control::contract::ErrorBody))
)]
pub(crate) async fn profile(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(name): Path<String>,
) -> Result<Json<StorageProfile>, HttpError> {
    actor.recent(&app).await?;
    let storage_id: Uuid = sqlx::query_scalar("SELECT id FROM storages WHERE name=$1")
        .bind(&name)
        .fetch_optional(&app.db)
        .await?
        .context("storage was not found")?;
    Ok(Json(StorageProfile::load(&app.db, storage_id).await?))
}

const PROFILE_COLUMNS: &[&str] = &[
    "connect_timeout",
    "request_timeout",
    "max_retries",
    "retry_timeout",
    "priority_aging",
    "min_storage_duration",
    "unreferenced_grace",
    "deleted_chunk_retention",
    "upload_retention",
    "cache_enabled",
    "cache_soft_limit",
    "cache_entries",
    "upload_cache",
    "upload_cache_size",
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

fn validate_profile(profile: &StorageProfile, storage_type: &str) -> anyhow::Result<()> {
    use crate::config::{bytes, cache_bytes, seconds};
    if storage_type == "local" {
        anyhow::ensure!(
            !profile.cache_enabled
                && !profile.upload_cache
                && !profile.pack_enabled
                && !profile.pack_range_enabled,
            "local storage cannot enable cache or packing"
        );
    }
    anyhow::ensure!(
        matches!(
            profile.compression_strategy.as_str(),
            "never" | "always" | "sample"
        ),
        "compression strategy must be never, always, or sample"
    );
    profile
        .compression_config(&crate::compression::Config::default())
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

async fn save_profile(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    profile: &StorageProfile,
) -> anyhow::Result<()> {
    let columns = PROFILE_COLUMNS.join(",");
    let requested = PROFILE_COLUMNS
        .iter()
        .map(|column| format!("requested.{column}"))
        .collect::<Vec<_>>()
        .join(",");
    let statement = format!(
        "UPDATE storage_profiles AS current SET ({columns})=(SELECT {requested} FROM jsonb_populate_record(current,$2::jsonb) AS requested),revision=revision+1,updated_at=now() WHERE current.storage_id=$1 AND current.revision=$3"
    );
    let affected = sqlx::query(sqlx::AssertSqlSafe(statement))
        .bind(profile.storage_id)
        .bind(serde_json::to_value(profile)?)
        .bind(profile.revision)
        .execute(&mut **tx)
        .await?
        .rows_affected();
    anyhow::ensure!(affected == 1, "storage profile revision conflict");
    Ok(())
}

pub(crate) async fn update_profile(
    app: &App,
    name: String,
    profile: StorageProfile,
) -> anyhow::Result<StorageProfile> {
    let mut tx = app.db.begin().await?;
    let (storage_id, storage_type): (Uuid, String) =
        sqlx::query_as("SELECT id,type FROM storages WHERE name=$1 FOR UPDATE")
            .bind(&name)
            .fetch_optional(&mut *tx)
            .await?
            .context("storage was not found")?;
    anyhow::ensure!(
        profile.storage_id == storage_id,
        "storage profile belongs to another storage"
    );
    validate_profile(&profile, &storage_type)?;
    save_profile(&mut tx, &profile).await?;
    super::audit::checkpoint(
        &mut tx,
        "storage.profile.update",
        &name,
        serde_json::json!({"revision": profile.revision + 1}),
    )
    .await?;
    sqlx::query("SELECT pg_notify('mokyu_storage_profiles',$1)")
        .bind(storage_id.to_string())
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    app.storage.refresh_store(storage_id).await?;
    Ok(StorageProfile::load(&app.db, storage_id).await?)
}
#[utoipa::path(
    put,
    operation_id = "storage_profile_update",
    path = "/api/v1/storages/{name}/profile",
    params(("name" = String, Path)),
    request_body = StorageProfile,
    responses((status = 200, body = StorageProfile), (status = 409, body = crate::control::contract::ErrorBody))
)]
pub(crate) async fn update_profile_route(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(name): Path<String>,
    Json(profile): Json<StorageProfile>,
) -> Result<Json<StorageProfile>, HttpError> {
    actor.recent(&app).await?;
    let tx = super::users::admin_transaction(&app, &actor.principal).await?;
    tx.commit().await?;
    Ok(Json(update_profile(&app, name, profile).await?))
}

pub(crate) async fn create_record(app: &App, input: StorageInput) -> anyhow::Result<StorageView> {
    let mut tx = app.db.begin().await?;
    let (deployment_id, mode) = lock_mode(&mut tx).await?;
    let name = validate_name(&input.name)?;
    validate_storage_type(&input.storage_type)?;
    validate_concurrency(input.read_concurrency, "read")?;
    validate_concurrency(input.upload_concurrency, "upload")?;
    validate_concurrency(input.control_concurrency, "control")?;
    anyhow::ensure!(input.initialize, "store initialization must be explicit");
    anyhow::ensure!(
        !(mode == "cluster" && input.storage_type == "local"),
        "local storages are unavailable in cluster mode"
    );
    validate_block_key(&mut tx, input.block_key_id).await?;
    let id = Uuid::new_v4();
    let record = build_record(
        app,
        id,
        name,
        input.storage_type,
        input.path,
        input.endpoint,
        input.bucket,
        input.region,
        normalize_prefix(input.prefix)?,
        input.access_key,
        input.secret_key,
        storage_identity_marker(deployment_id, id)?,
    )?;
    if let Some(profile) = &input.profile {
        validate_profile(profile, &record.r#type)?;
    }
    sqlx::query("INSERT INTO storages(id,name,type,path,endpoint_url,remote_bucket_name,region,prefix,identity_marker,block_key_id,access_key_encrypted,secret_key_encrypted) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)").bind(record.id).bind(&record.name).bind(&record.r#type).bind(&record.path).bind(&record.endpoint_url).bind(&record.remote_bucket_name).bind(&record.region).bind(&record.prefix).bind(&record.identity_marker).bind(input.block_key_id).bind(&record.access_key_encrypted).bind(&record.secret_key_encrypted).execute(&mut *tx).await?;
    sqlx::query("INSERT INTO storage_profiles(storage_id,read_concurrency,upload_concurrency,control_concurrency,cache_enabled,upload_cache,compression_enabled,pack_enabled) VALUES($1,$2,$3,$4,$5,$5,$5,$5)")
        .bind(record.id)
        .bind(input.read_concurrency)
        .bind(input.upload_concurrency)
        .bind(input.control_concurrency)
        .bind(record.r#type == "s3")
        .execute(&mut *tx)
        .await?;
    let profile_for_init = input.profile.clone();
    if let Some(mut profile) = input.profile {
        profile.storage_id = record.id;
        profile.revision = 1;
        save_profile(&mut tx, &profile).await?;
    }
    app.storage
        .initialize_store_with_profile(record.clone(), profile_for_init.as_ref())
        .await?;
    sqlx::query("SELECT pg_notify('mokyu_storage_profiles',$1)")
        .bind(record.id.to_string())
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    app.storage.refresh_store(record.id).await?;
    Ok(sqlx::query_as(VIEW_BY_ID)
        .bind(record.id)
        .fetch_one(&app.db)
        .await?)
}

#[utoipa::path(post, operation_id = "storage_create", path = "/api/v1/storages", request_body = StorageInput, responses((status = 201, body = StorageView)))]
pub(crate) async fn create(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<StorageInput>,
) -> Result<(StatusCode, Json<StorageView>), HttpError> {
    actor.recent(&app).await?;
    let tx = super::users::admin_transaction(&app, &actor.principal).await?;
    tx.commit().await?;
    Ok((StatusCode::CREATED, Json(create_record(&app, input).await?)))
}

pub(crate) async fn update_record(
    app: &App,
    name: String,
    input: StoragePatch,
) -> anyhow::Result<StorageView> {
    let mut tx = app.db.begin().await?;
    let (_deployment_id, mode) = lock_mode(&mut tx).await?;
    let row = load_row(&mut tx, &name).await?;
    let profile: StorageProfile = sqlx::query_as(
        "SELECT storage_id,revision,connect_timeout,request_timeout,max_retries,retry_timeout,priority_aging,min_storage_duration,unreferenced_grace,deleted_chunk_retention,upload_retention,cache_enabled,cache_soft_limit,cache_entries,upload_cache,upload_cache_size,compression_enabled,compression_strategy,compression_level,compression_sample_level,compression_min_savings_percent,compression_min_savings_bytes,pack_enabled,pack_max_size,pack_upload_flush_timeout,pack_interval,pack_reuse_interval,pack_reclaim_interval,pack_repack_interval,pack_repack_cooldown,pack_reclaim_min_savings_bytes,pack_range_enabled,pack_range_interval,pack_range_window,pack_range_min_downloads,pack_range_min_savings_percent,pack_range_min_savings_bytes,pack_range_repack_after,pack_range_repack_window,pack_range_repack_retry_interval,pack_concurrency,integrity_concurrency,integrity_rps,integrity_bandwidth,integrity_batch_timeout,read_concurrency,upload_concurrency,control_concurrency,pack_byte_budget,integrity_byte_budget,maintenance_concurrency,maintenance_byte_budget,migration_concurrency,migration_byte_budget FROM storage_profiles WHERE storage_id=$1 FOR UPDATE",
    )
    .bind(row.id)
    .fetch_one(&mut *tx)
    .await?;
    anyhow::ensure!(
        row.identity_marker != serde_json::json!({}),
        "storage identity marker is empty; initialize and check it before changing the connection"
    );
    let kind = input
        .storage_type
        .clone()
        .unwrap_or_else(|| row.r#type.clone());
    validate_storage_type(&kind)?;
    anyhow::ensure!(
        !(mode == "cluster" && kind == "local"),
        "local storages are unavailable in cluster mode"
    );
    let prefix = normalize_prefix(input.prefix.clone().unwrap_or_else(|| row.prefix.clone()))?;
    let read_concurrency = input.read_concurrency.unwrap_or(profile.read_concurrency);
    let upload_concurrency = input
        .upload_concurrency
        .unwrap_or(profile.upload_concurrency);
    let control_concurrency = input
        .control_concurrency
        .unwrap_or(profile.control_concurrency);
    validate_concurrency(read_concurrency, "read")?;
    validate_concurrency(upload_concurrency, "upload")?;
    validate_concurrency(control_concurrency, "control")?;
    let mut record = if kind == "local" {
        anyhow::ensure!(
            input.endpoint.is_none()
                && input.bucket.is_none()
                && input.region.is_none()
                && input.access_key.is_none()
                && input.secret_key.is_none(),
            "local storages cannot include S3 settings"
        );
        build_record(
            app,
            row.id,
            row.name.clone(),
            kind,
            input.path.or(row.path),
            None,
            None,
            None,
            prefix,
            None,
            None,
            row.identity_marker.clone(),
        )?
    } else {
        anyhow::ensure!(
            input.path.is_none(),
            "S3 storages cannot include a local path"
        );
        let access = match input.access_key {
            Some(value) => value,
            None => codec::unprotect(
                row.access_key_encrypted
                    .as_deref()
                    .context("S3 access key is missing")?,
                &app.secrets.credential_key,
                format!("storage-{}-access", row.id).as_bytes(),
            )?,
        };
        let secret = match input.secret_key {
            Some(value) => value,
            None => codec::unprotect(
                row.secret_key_encrypted
                    .as_deref()
                    .context("S3 secret key is missing")?,
                &app.secrets.credential_key,
                format!("storage-{}-secret", row.id).as_bytes(),
            )?,
        };
        build_record(
            app,
            row.id,
            row.name.clone(),
            kind,
            None,
            input.endpoint.or(row.endpoint_url),
            input.bucket.or(row.remote_bucket_name),
            input.region.or(row.region),
            prefix,
            Some(access),
            Some(secret),
            row.identity_marker.clone(),
        )?
    };
    record.enabled = input.enabled.unwrap_or(row.enabled);
    app.storage.verify_store_record(record.clone()).await?;
    let block_key_id = input.block_key_id.unwrap_or(row.block_key_id);
    validate_block_key(&mut tx, block_key_id).await?;
    sqlx::query("UPDATE storages SET type=$2,path=$3,endpoint_url=$4,remote_bucket_name=$5,region=$6,prefix=$7,block_key_id=$8,access_key_encrypted=$9,secret_key_encrypted=$10,enabled=$11,updated_at=now() WHERE id=$1").bind(row.id).bind(&record.r#type).bind(&record.path).bind(&record.endpoint_url).bind(&record.remote_bucket_name).bind(&record.region).bind(&record.prefix).bind(block_key_id).bind(&record.access_key_encrypted).bind(&record.secret_key_encrypted).bind(record.enabled).execute(&mut *tx).await?;
    let requested = if let Some(mut requested) = input.profile {
        anyhow::ensure!(
            requested.storage_id == row.id,
            "storage profile belongs to another storage"
        );
        anyhow::ensure!(
            requested.revision == profile.revision,
            "storage profile revision conflict"
        );
        if record.r#type == "local" {
            requested.pack_enabled = false;
        }
        requested
    } else {
        let mut requested = profile;
        requested.read_concurrency = read_concurrency;
        requested.upload_concurrency = upload_concurrency;
        requested.control_concurrency = control_concurrency;
        if record.r#type == "local" {
            requested.pack_enabled = false;
        }
        requested
    };
    validate_profile(&requested, &record.r#type)?;
    save_profile(&mut tx, &requested).await?;
    sqlx::query("SELECT pg_notify('mokyu_storage_profiles',$1)")
        .bind(row.id.to_string())
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    app.storage.refresh_store(row.id).await?;
    Ok(sqlx::query_as(VIEW_BY_ID)
        .bind(row.id)
        .fetch_one(&app.db)
        .await?)
}

pub(crate) async fn set_current_key(
    app: &App,
    name: String,
    key: Option<String>,
) -> anyhow::Result<StorageView> {
    let mut tx = app.db.begin().await?;
    let row = load_row(&mut tx, &name).await?;
    let key_id = if let Some(key) = key {
        let parsed = key.parse::<Uuid>().ok();
        sqlx::query_scalar::<_, Uuid>(
            "SELECT id FROM managed_keys WHERE ((($1::uuid IS NOT NULL AND id=$1) OR name=$2) AND purpose='block' AND revoked_at IS NULL)",
        )
        .bind(parsed)
        .bind(key.trim())
        .fetch_optional(&mut *tx)
        .await?
        .context("active block key was not found")
        .map(Some)?
    } else {
        None
    };
    sqlx::query("UPDATE storages SET block_key_id=$2,updated_at=now() WHERE id=$1")
        .bind(row.id)
        .bind(key_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "UPDATE storage_profiles SET revision=revision+1,updated_at=now() WHERE storage_id=$1",
    )
    .bind(row.id)
    .execute(&mut *tx)
    .await?;
    sqlx::query("SELECT pg_notify('mokyu_storage_profiles',$1)")
        .bind(row.id.to_string())
        .execute(&mut *tx)
        .await?;
    super::audit::checkpoint(
        &mut tx,
        "storage.current_key.update",
        &name,
        serde_json::json!({"block_key_id":key_id}),
    )
    .await?;
    tx.commit().await?;
    app.storage.refresh_store(row.id).await?;
    Ok(sqlx::query_as(VIEW_BY_ID)
        .bind(row.id)
        .fetch_one(&app.db)
        .await?)
}

#[utoipa::path(
    put,
    operation_id = "storage_key",
    path = "/api/v1/storages/{name}/key",
    params(("name" = String, Path)),
    request_body = StorageKeyInput,
    responses((status = 200, body = StorageView), (status = 404, body = crate::control::contract::ErrorBody))
)]
pub(crate) async fn key(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(name): Path<String>,
    Json(input): Json<StorageKeyInput>,
) -> Result<Json<StorageView>, HttpError> {
    actor.recent(&app).await?;
    Ok(Json(set_current_key(&app, name, input.key).await?))
}

#[utoipa::path(
    post,
    operation_id = "storage_unpack",
    path = "/api/v1/storages/{name}/unpack",
    params(("name" = String, Path)),
    request_body = StorageUnpackInput,
    responses((status = 200, body = Value), (status = 409, body = crate::control::contract::ErrorBody))
)]
pub(crate) async fn unpack(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(name): Path<String>,
    Json(input): Json<StorageUnpackInput>,
) -> Result<Json<Value>, HttpError> {
    actor.recent(&app).await?;
    let storage_id: Uuid = sqlx::query_scalar("SELECT id FROM storages WHERE name=$1")
        .bind(&name)
        .fetch_optional(&app.db)
        .await?
        .context("storage was not found")?;
    let belongs: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM packs WHERE id=$1 AND storage_id=$2 AND state='ready')",
    )
    .bind(input.pack_id)
    .bind(storage_id)
    .fetch_one(&app.db)
    .await?;
    if !belongs {
        return Err(anyhow::anyhow!("pack was not found in storage").into());
    }
    match input.action.as_str() {
        "preview" => Ok(Json(app.unpack_start(Some(input.pack_id), false, false).await?)),
        "start" => {
            let confirmation = input
                .confirmation
                .as_deref()
                .context("confirmation is required")?;
            let preview = app.unpack_start(Some(input.pack_id), false, false).await?;
            let expected = blake3::hash(&serde_json::to_vec(&preview)?)
                .to_hex()
                .to_string();
            if confirmation != expected {
                return Err(anyhow::anyhow!("preview confirmation mismatch").into());
            }
            Ok(Json(
                app.unpack_checked(Some(input.pack_id), false, true, Some(&expected))
                    .await?,
            ))
        }
        "status" => Ok(Json(
            sqlx::query_scalar::<_, Value>(
                "SELECT to_jsonb(t) FROM tasks t WHERE task_type='unpack' AND detail->>'pack_id'=$1 ORDER BY created_at DESC LIMIT 1",
            )
            .bind(input.pack_id.to_string())
            .fetch_optional(&app.db)
            .await?
            .unwrap_or_else(|| serde_json::json!({"status":"missing"})),
        )),
        _ => Err(s3s::s3_error!(InvalidArgument).into()),
    }
}

#[utoipa::path(put, operation_id = "storage_update", path = "/api/v1/storages/{name}", params(("name" = String, Path)), request_body = StoragePatch, responses((status = 200, body = StorageView)))]
pub(crate) async fn update(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(name): Path<String>,
    Json(input): Json<StoragePatch>,
) -> Result<Json<StorageView>, HttpError> {
    actor.recent(&app).await?;
    let tx = super::users::admin_transaction(&app, &actor.principal).await?;
    tx.commit().await?;
    Ok(Json(update_record(&app, name, input).await?))
}

#[utoipa::path(post, operation_id = "storage_check", path = "/api/v1/storages/{name}/check", params(("name" = String, Path)), responses((status = 200, body = StorageView)))]
pub(crate) async fn check(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(name): Path<String>,
) -> Result<Json<StorageView>, HttpError> {
    actor.recent(&app).await?;
    let tx = super::users::admin_transaction(&app, &actor.principal).await?;
    tx.commit().await?;
    let row: StorageRow = sqlx::query_as("SELECT id,name,type,path,endpoint_url,remote_bucket_name,region,prefix,access_key_encrypted,secret_key_encrypted,identity_marker,block_key_id,enabled FROM storages WHERE name=$1").bind(&name).fetch_optional(&app.db).await?.context("storage was not found")?;
    app.storage.verify_store_record(row_record(&row)).await?;
    Ok(Json(
        sqlx::query_as(VIEW_BY_ID)
            .bind(row.id)
            .fetch_one(&app.db)
            .await?,
    ))
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct RewriteRequest {
    action: String,
    target_key: Option<String>,
    confirmation: Option<String>,
}

#[utoipa::path(
    post,
    operation_id = "storage_rewrite",
    path = "/api/v1/storages/{name}/rewrite",
    params(("name" = String, Path)),
    request_body = RewriteRequest,
    responses((status = 200, body = Value))
)]
pub(crate) async fn rewrite(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(name): Path<String>,
    Json(input): Json<RewriteRequest>,
) -> Result<Json<serde_json::Value>, HttpError> {
    actor.recent(&app).await?;
    let storage_id: Uuid = sqlx::query_scalar("SELECT id FROM storages WHERE name=$1")
        .bind(&name)
        .fetch_optional(&app.db)
        .await?
        .context("storage was not found")?;
    let value = match input.action.as_str() {
        "preview" => app
            .rewrite_start(storage_id, input.target_key.as_deref(), false, None)
            .await?,
        "start" => app
            .rewrite_start(
                storage_id,
                input.target_key.as_deref(),
                true,
                input.confirmation.as_deref(),
            )
            .await?,
        "status" => sqlx::query_scalar::<_, serde_json::Value>(
            "SELECT to_jsonb(t) FROM tasks t WHERE task_type='rewrite' AND detail->>'role'='parent' AND detail->>'storage_id'=$1 ORDER BY created_at DESC LIMIT 1",
        )
        .bind(storage_id.to_string())
        .fetch_optional(&app.db)
        .await?
        .context("rewrite task not found")?,
        "pause" | "resume" | "retry" => {
            let task: Uuid = sqlx::query_scalar(
                "SELECT id FROM tasks WHERE task_type='rewrite' AND detail->>'role'='parent' AND detail->>'storage_id'=$1 ORDER BY created_at DESC LIMIT 1",
            )
            .bind(storage_id.to_string())
            .fetch_optional(&app.db)
            .await?
            .context("rewrite task not found")?;
            app.task_change(task, input.action != "pause").await?
        }
        _ => return Err(s3s::s3_error!(InvalidArgument).into()),
    };
    Ok(Json(value))
}
