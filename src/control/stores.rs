use super::Identity;
use crate::{
    app::App,
    codec,
    http::HttpError,
    storage::store::{Record as StoreRecord, metadata as store_metadata},
};
use anyhow::Context;
use axum::{
    Json,
    extract::{Extension, Path, State},
    http::StatusCode,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Serialize, sqlx::FromRow, ToSchema)]
pub(crate) struct StoreView {
    id: Uuid,
    name: String,
    #[serde(rename = "type")]
    store_type: String,
    path: Option<String>,
    endpoint: Option<String>,
    bucket: Option<String>,
    region: Option<String>,
    prefix: String,
    current_key_id: Option<Uuid>,
    read_concurrency: Option<i32>,
    upload_concurrency: Option<i32>,
    control_concurrency: Option<i32>,
    enabled: bool,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoreInput {
    pub(crate) name: String,
    #[serde(rename = "type")]
    pub(crate) store_type: String,
    pub(crate) path: Option<String>,
    pub(crate) endpoint: Option<String>,
    pub(crate) bucket: Option<String>,
    pub(crate) region: Option<String>,
    #[serde(default)]
    pub(crate) prefix: String,
    pub(crate) current_key_id: Option<Uuid>,
    pub(crate) read_concurrency: Option<i32>,
    pub(crate) upload_concurrency: Option<i32>,
    pub(crate) control_concurrency: Option<i32>,
    pub(crate) access_key: Option<String>,
    pub(crate) secret_key: Option<String>,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct StorePatch {
    #[serde(rename = "type")]
    pub(crate) store_type: Option<String>,
    pub(crate) path: Option<String>,
    pub(crate) endpoint: Option<String>,
    pub(crate) bucket: Option<String>,
    pub(crate) region: Option<String>,
    pub(crate) prefix: Option<String>,
    pub(crate) current_key_id: Option<Option<Uuid>>,
    pub(crate) access_key: Option<String>,
    pub(crate) secret_key: Option<String>,
    pub(crate) enabled: Option<bool>,
    pub(crate) read_concurrency: Option<i32>,
    pub(crate) upload_concurrency: Option<i32>,
    pub(crate) control_concurrency: Option<i32>,
}

#[derive(Clone, sqlx::FromRow)]
struct StoreRow {
    id: Uuid,
    name: String,
    r#type: String,
    path: Option<String>,
    endpoint_url: Option<String>,
    remote_bucket: Option<String>,
    region: Option<String>,
    prefix: String,
    access_key_encrypted: Option<Vec<u8>>,
    secret_key_encrypted: Option<Vec<u8>>,
    metadata: serde_json::Value,
    current_key_id: Option<Uuid>,
    read_concurrency: Option<i32>,
    upload_concurrency: Option<i32>,
    control_concurrency: Option<i32>,
    enabled: bool,
}

fn validate_name(value: &str) -> anyhow::Result<String> {
    let value = value.trim();
    anyhow::ensure!(
        (1..=128).contains(&value.len())
            && !value.contains('/')
            && !value.chars().any(char::is_control),
        "store name must contain 1..128 safe bytes"
    );
    Ok(value.to_owned())
}

fn validate_type(value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        matches!(value, "local" | "s3"),
        "store type is local or s3"
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
        "store prefix is invalid"
    );
    Ok(value)
}

fn validate_concurrency(value: Option<i32>, field: &'static str) -> anyhow::Result<()> {
    anyhow::ensure!(value.is_none_or(|n| n > 0), "{field} concurrency must be positive");
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

const VIEW_ORDERED: &str = "SELECT id,name,type AS store_type,path,endpoint_url AS endpoint,remote_bucket AS bucket,region,prefix,current_key_id,read_concurrency,upload_concurrency,control_concurrency,enabled,created_at,updated_at FROM stores ORDER BY name";
const VIEW_BY_ID: &str = "SELECT id,name,type AS store_type,path,endpoint_url AS endpoint,remote_bucket AS bucket,region,prefix,current_key_id,read_concurrency,upload_concurrency,control_concurrency,enabled,created_at,updated_at FROM stores WHERE id=$1";

fn row_record(row: &StoreRow) -> StoreRecord {
    StoreRecord {
        id: row.id,
        name: row.name.clone(),
        r#type: row.r#type.clone(),
        path: row.path.clone(),
        endpoint_url: row.endpoint_url.clone(),
        remote_bucket: row.remote_bucket.clone(),
        region: row.region.clone(),
        prefix: row.prefix.clone(),
        access_key_encrypted: row.access_key_encrypted.clone(),
        secret_key_encrypted: row.secret_key_encrypted.clone(),
        metadata: row.metadata.clone(),
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
    metadata: serde_json::Value,
) -> anyhow::Result<StoreRecord> {
    let (path, endpoint_url, remote_bucket, region, access, secret) = if kind == "local" {
        anyhow::ensure!(
            endpoint_value.is_none()
                && bucket.is_none()
                && region.is_none()
                && access_key.is_none()
                && secret_key.is_none(),
            "local stores cannot include S3 settings"
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
        anyhow::ensure!(
            path.is_none(),
            "S3 stores cannot include a local path"
        );
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
                format!("storage-target-{id}-access").as_bytes(),
            )?),
            Some(codec::protect(
                secret_key.as_bytes(),
                &app.secrets.credential_key,
                format!("storage-target-{id}-secret").as_bytes(),
            )?),
        )
    };
    Ok(StoreRecord {
        id,
        name,
        r#type: kind,
        path,
        endpoint_url,
        remote_bucket,
        region,
        prefix,
        access_key_encrypted: access,
        secret_key_encrypted: secret,
        metadata,
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
) -> anyhow::Result<StoreRow> {
    Ok(sqlx::query_as("SELECT id,name,type,path,endpoint_url,remote_bucket,region,prefix,access_key_encrypted,secret_key_encrypted,metadata,current_key_id,read_concurrency,upload_concurrency,control_concurrency,enabled FROM stores WHERE name=$1 FOR UPDATE").bind(name).fetch_optional(&mut **tx).await?.context("store was not found")?)
}

async fn list_rows(db: &sqlx::PgPool) -> anyhow::Result<Vec<StoreView>> {
    Ok(sqlx::query_as(VIEW_ORDERED).fetch_all(db).await?)
}

#[utoipa::path(get, operation_id = "stores", path = "/api/stores", responses((status = 200, body = Vec<StoreView>)))]
pub(crate) async fn list(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<Vec<StoreView>>, HttpError> {
    let tx = super::users::admin_transaction(&app, &actor.principal).await?;
    tx.commit().await?;
    Ok(Json(list_rows(&app.db).await?))
}

pub(crate) async fn create_record(
    app: &App,
    input: StoreInput,
) -> anyhow::Result<StoreView> {
    let mut tx = app.db.begin().await?;
    let (deployment_id, mode) = lock_mode(&mut tx).await?;
    let name = validate_name(&input.name)?;
    validate_type(&input.store_type)?;
    validate_concurrency(input.read_concurrency, "read")?;
    validate_concurrency(input.upload_concurrency, "upload")?;
    validate_concurrency(input.control_concurrency, "control")?;
    anyhow::ensure!(
        !(mode == "cluster" && input.store_type == "local"),
        "local stores are unavailable in cluster mode"
    );
    let id = Uuid::new_v4();
    let record = build_record(
        app,
        id,
        name,
        input.store_type,
        input.path,
        input.endpoint,
        input.bucket,
        input.region,
        normalize_prefix(input.prefix)?,
        input.access_key,
        input.secret_key,
        store_metadata(deployment_id, id)?,
    )?;
    app.storage.initialize_store(record.clone()).await?;
    sqlx::query("INSERT INTO stores(id,name,type,path,endpoint_url,remote_bucket,region,prefix,metadata,current_key_id,read_concurrency,upload_concurrency,control_concurrency) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)").bind(record.id).bind(&record.name).bind(&record.r#type).bind(&record.path).bind(&record.endpoint_url).bind(&record.remote_bucket).bind(&record.region).bind(&record.prefix).bind(&record.metadata).bind(input.current_key_id).bind(input.read_concurrency).bind(input.upload_concurrency).bind(input.control_concurrency).execute(&mut *tx).await?;
    tx.commit().await?;
    app.storage.refresh_store(record.id).await?;
    Ok(sqlx::query_as(VIEW_BY_ID)
        .bind(record.id)
        .fetch_one(&app.db)
        .await?)
}

#[utoipa::path(post, operation_id = "store_create", path = "/api/stores", request_body = StoreInput, responses((status = 201, body = StoreView)))]
pub(crate) async fn create(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<StoreInput>,
) -> Result<(StatusCode, Json<StoreView>), HttpError> {
    actor.recent(&app).await?;
    let tx = super::users::admin_transaction(&app, &actor.principal).await?;
    tx.commit().await?;
    Ok((StatusCode::CREATED, Json(create_record(&app, input).await?)))
}

pub(crate) async fn update_record(
    app: &App,
    name: String,
    input: StorePatch,
) -> anyhow::Result<StoreView> {
    let mut tx = app.db.begin().await?;
    let (_deployment_id, mode) = lock_mode(&mut tx).await?;
    let row = load_row(&mut tx, &name).await?;
    anyhow::ensure!(
        row.metadata != serde_json::json!({}),
        "store metadata is empty; initialize and check it before changing the connection"
    );
    let kind = input
        .store_type
        .clone()
        .unwrap_or_else(|| row.r#type.clone());
    validate_type(&kind)?;
    anyhow::ensure!(
        !(mode == "cluster" && kind == "local"),
        "local stores are unavailable in cluster mode"
    );
    let prefix = normalize_prefix(input.prefix.clone().unwrap_or_else(|| row.prefix.clone()))?;
    validate_concurrency(input.read_concurrency.or(row.read_concurrency), "read")?;
    validate_concurrency(input.upload_concurrency.or(row.upload_concurrency), "upload")?;
    validate_concurrency(input.control_concurrency.or(row.control_concurrency), "control")?;
    let mut record = if kind == "local" {
        anyhow::ensure!(
            input.endpoint.is_none()
                && input.bucket.is_none()
                && input.region.is_none()
                && input.access_key.is_none()
                && input.secret_key.is_none(),
            "local stores cannot include S3 settings"
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
            row.metadata.clone(),
        )?
    } else {
        anyhow::ensure!(
            input.path.is_none(),
            "S3 stores cannot include a local path"
        );
        let access = match input.access_key {
            Some(value) => value,
            None => codec::unprotect(
                row.access_key_encrypted
                    .as_deref()
                    .context("S3 access key is missing")?,
                &app.secrets.credential_key,
                format!("storage-target-{}-access", row.id).as_bytes(),
            )?,
        };
        let secret = match input.secret_key {
            Some(value) => value,
            None => codec::unprotect(
                row.secret_key_encrypted
                    .as_deref()
                    .context("S3 secret key is missing")?,
                &app.secrets.credential_key,
                format!("storage-target-{}-secret", row.id).as_bytes(),
            )?,
        };
        build_record(
            app,
            row.id,
            row.name.clone(),
            kind,
            None,
            input.endpoint.or(row.endpoint_url),
            input.bucket.or(row.remote_bucket),
            input.region.or(row.region),
            prefix,
            Some(access),
            Some(secret),
            row.metadata.clone(),
        )?
    };
    record.enabled = input.enabled.unwrap_or(row.enabled);
    app.storage.verify_store_record(record.clone()).await?;
    let current_key_id = input.current_key_id.unwrap_or(row.current_key_id);
    sqlx::query("UPDATE stores SET type=$2,path=$3,endpoint_url=$4,remote_bucket=$5,region=$6,prefix=$7,current_key_id=$8,access_key_encrypted=$9,secret_key_encrypted=$10,read_concurrency=$11,upload_concurrency=$12,control_concurrency=$13,enabled=$14,updated_at=now() WHERE id=$1").bind(row.id).bind(&record.r#type).bind(&record.path).bind(&record.endpoint_url).bind(&record.remote_bucket).bind(&record.region).bind(&record.prefix).bind(current_key_id).bind(&record.access_key_encrypted).bind(&record.secret_key_encrypted).bind(input.read_concurrency.or(row.read_concurrency)).bind(input.upload_concurrency.or(row.upload_concurrency)).bind(input.control_concurrency.or(row.control_concurrency)).bind(record.enabled).execute(&mut *tx).await?;
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
) -> anyhow::Result<StoreView> {
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
    sqlx::query("UPDATE stores SET current_key_id=$2,updated_at=now() WHERE id=$1")
        .bind(row.id)
        .bind(key_id)
        .execute(&mut *tx)
        .await?;
    super::audit::checkpoint(
        &mut tx,
        "store.current_key.update",
        &name,
        serde_json::json!({"key_id":key_id}),
    )
    .await?;
    tx.commit().await?;
    app.storage.refresh_store(row.id).await?;
    Ok(sqlx::query_as(VIEW_BY_ID)
        .bind(row.id)
        .fetch_one(&app.db)
        .await?)
}

#[utoipa::path(put, operation_id = "store_update", path = "/api/stores/{name}", params(("name" = String, Path)), request_body = StorePatch, responses((status = 200, body = StoreView)))]
pub(crate) async fn update(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(name): Path<String>,
    Json(input): Json<StorePatch>,
) -> Result<Json<StoreView>, HttpError> {
    actor.recent(&app).await?;
    let tx = super::users::admin_transaction(&app, &actor.principal).await?;
    tx.commit().await?;
    Ok(Json(update_record(&app, name, input).await?))
}

#[utoipa::path(post, operation_id = "store_check", path = "/api/stores/{name}/check", params(("name" = String, Path)), responses((status = 200, body = StoreView)))]
pub(crate) async fn check(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(name): Path<String>,
) -> Result<Json<StoreView>, HttpError> {
    actor.recent(&app).await?;
    let tx = super::users::admin_transaction(&app, &actor.principal).await?;
    tx.commit().await?;
    let row: StoreRow = sqlx::query_as("SELECT id,name,type,path,endpoint_url,remote_bucket,region,prefix,access_key_encrypted,secret_key_encrypted,metadata,current_key_id,read_concurrency,upload_concurrency,control_concurrency,enabled FROM stores WHERE name=$1").bind(&name).fetch_optional(&app.db).await?.context("store was not found")?;
    app.storage.verify_store_record(row_record(&row)).await?;
    Ok(Json(
        sqlx::query_as(VIEW_BY_ID)
            .bind(row.id)
            .fetch_one(&app.db)
            .await?,
    ))
}
