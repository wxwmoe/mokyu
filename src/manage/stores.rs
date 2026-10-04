use super::Identity;
use crate::{
    app::App,
    codec,
    http::HttpError,
    storage::target::{Record as TargetRecord, metadata as target_metadata},
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
pub(crate) struct StorageTargetView {
    id: Uuid,
    name: String,
    #[serde(rename = "type")]
    target_type: String,
    path: Option<String>,
    endpoint: Option<String>,
    bucket: Option<String>,
    region: Option<String>,
    prefix: String,
    enabled: bool,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct StorageTargetInput {
    pub(crate) name: String,
    #[serde(rename = "type")]
    pub(crate) target_type: String,
    pub(crate) path: Option<String>,
    pub(crate) endpoint: Option<String>,
    pub(crate) bucket: Option<String>,
    pub(crate) region: Option<String>,
    #[serde(default)]
    pub(crate) prefix: String,
    pub(crate) access_key: Option<String>,
    pub(crate) secret_key: Option<String>,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct StorageTargetPatch {
    #[serde(rename = "type")]
    pub(crate) target_type: Option<String>,
    pub(crate) path: Option<String>,
    pub(crate) endpoint: Option<String>,
    pub(crate) bucket: Option<String>,
    pub(crate) region: Option<String>,
    pub(crate) prefix: Option<String>,
    pub(crate) access_key: Option<String>,
    pub(crate) secret_key: Option<String>,
    pub(crate) enabled: Option<bool>,
}

#[derive(Clone, sqlx::FromRow)]
struct TargetRow {
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
    enabled: bool,
}

fn validate_name(value: &str) -> anyhow::Result<String> {
    let value = value.trim();
    anyhow::ensure!(
        (1..=128).contains(&value.len())
            && !value.contains('/')
            && !value.chars().any(char::is_control),
        "storage target name must contain 1..128 safe bytes"
    );
    Ok(value.to_owned())
}

fn validate_type(value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        matches!(value, "local" | "s3"),
        "storage target type is local or s3"
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
        "storage target prefix is invalid"
    );
    Ok(value)
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

const VIEW_ORDERED: &str = "SELECT id,name,type AS target_type,path,endpoint_url AS endpoint,remote_bucket AS bucket,region,prefix,enabled,created_at,updated_at FROM storage_targets ORDER BY name";
const VIEW_BY_ID: &str = "SELECT id,name,type AS target_type,path,endpoint_url AS endpoint,remote_bucket AS bucket,region,prefix,enabled,created_at,updated_at FROM storage_targets WHERE id=$1";

fn row_record(row: &TargetRow) -> TargetRecord {
    TargetRecord {
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
) -> anyhow::Result<TargetRecord> {
    let (path, endpoint_url, remote_bucket, region, access, secret) = if kind == "local" {
        anyhow::ensure!(
            endpoint_value.is_none()
                && bucket.is_none()
                && region.is_none()
                && access_key.is_none()
                && secret_key.is_none(),
            "local storage targets cannot include S3 settings"
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
            "S3 storage targets cannot include a local path"
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
    Ok(TargetRecord {
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
) -> anyhow::Result<TargetRow> {
    Ok(sqlx::query_as("SELECT id,name,type,path,endpoint_url,remote_bucket,region,prefix,access_key_encrypted,secret_key_encrypted,metadata,enabled FROM storage_targets WHERE name=$1 FOR UPDATE").bind(name).fetch_optional(&mut **tx).await?.context("storage target was not found")?)
}

async fn list_rows(db: &sqlx::PgPool) -> anyhow::Result<Vec<StorageTargetView>> {
    Ok(sqlx::query_as(VIEW_ORDERED).fetch_all(db).await?)
}

#[utoipa::path(get, operation_id = "stores", path = "/api/stores", responses((status = 200, body = Vec<StorageTargetView>)))]
pub(crate) async fn list(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<Vec<StorageTargetView>>, HttpError> {
    let tx = super::users::admin_transaction(&app, &actor.principal).await?;
    tx.commit().await?;
    Ok(Json(list_rows(&app.db).await?))
}

pub(crate) async fn create_record(
    app: &App,
    input: StorageTargetInput,
) -> anyhow::Result<StorageTargetView> {
    let mut tx = app.db.begin().await?;
    let (deployment_id, mode) = lock_mode(&mut tx).await?;
    let name = validate_name(&input.name)?;
    validate_type(&input.target_type)?;
    anyhow::ensure!(
        !(mode == "cluster" && input.target_type == "local"),
        "local storage targets are unavailable in cluster mode"
    );
    let id = Uuid::new_v4();
    let record = build_record(
        app,
        id,
        name,
        input.target_type,
        input.path,
        input.endpoint,
        input.bucket,
        input.region,
        normalize_prefix(input.prefix)?,
        input.access_key,
        input.secret_key,
        target_metadata(deployment_id, id)?,
    )?;
    app.storage.initialize_target(record.clone()).await?;
    sqlx::query("INSERT INTO storage_targets(id,name,type,path,endpoint_url,remote_bucket,region,prefix,access_key_encrypted,secret_key_encrypted,metadata) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)").bind(record.id).bind(&record.name).bind(&record.r#type).bind(&record.path).bind(&record.endpoint_url).bind(&record.remote_bucket).bind(&record.region).bind(&record.prefix).bind(&record.access_key_encrypted).bind(&record.secret_key_encrypted).bind(&record.metadata).execute(&mut *tx).await?;
    tx.commit().await?;
    app.storage.refresh_target(record.id).await?;
    Ok(sqlx::query_as(VIEW_BY_ID)
        .bind(record.id)
        .fetch_one(&app.db)
        .await?)
}

#[utoipa::path(post, operation_id = "storage_target_create", path = "/api/stores", request_body = StorageTargetInput, responses((status = 201, body = StorageTargetView)))]
pub(crate) async fn create(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<StorageTargetInput>,
) -> Result<(StatusCode, Json<StorageTargetView>), HttpError> {
    actor.recent(&app).await?;
    let tx = super::users::admin_transaction(&app, &actor.principal).await?;
    tx.commit().await?;
    Ok((StatusCode::CREATED, Json(create_record(&app, input).await?)))
}

pub(crate) async fn update_record(
    app: &App,
    name: String,
    input: StorageTargetPatch,
) -> anyhow::Result<StorageTargetView> {
    let mut tx = app.db.begin().await?;
    let (_deployment_id, mode) = lock_mode(&mut tx).await?;
    let row = load_row(&mut tx, &name).await?;
    anyhow::ensure!(
        row.metadata != serde_json::json!({}),
        "storage target metadata is empty; initialize and check it before changing the connection"
    );
    let kind = input
        .target_type
        .clone()
        .unwrap_or_else(|| row.r#type.clone());
    validate_type(&kind)?;
    anyhow::ensure!(
        !(mode == "cluster" && kind == "local"),
        "local storage targets are unavailable in cluster mode"
    );
    let prefix = normalize_prefix(input.prefix.clone().unwrap_or_else(|| row.prefix.clone()))?;
    let mut record = if kind == "local" {
        anyhow::ensure!(
            input.endpoint.is_none()
                && input.bucket.is_none()
                && input.region.is_none()
                && input.access_key.is_none()
                && input.secret_key.is_none(),
            "local storage targets cannot include S3 settings"
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
            "S3 storage targets cannot include a local path"
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
    app.storage.verify_target_record(record.clone()).await?;
    sqlx::query("UPDATE storage_targets SET type=$2,path=$3,endpoint_url=$4,remote_bucket=$5,region=$6,prefix=$7,access_key_encrypted=$8,secret_key_encrypted=$9,enabled=$10,updated_at=now() WHERE id=$1").bind(row.id).bind(&record.r#type).bind(&record.path).bind(&record.endpoint_url).bind(&record.remote_bucket).bind(&record.region).bind(&record.prefix).bind(&record.access_key_encrypted).bind(&record.secret_key_encrypted).bind(record.enabled).execute(&mut *tx).await?;
    tx.commit().await?;
    app.storage.refresh_target(row.id).await?;
    Ok(sqlx::query_as(VIEW_BY_ID)
        .bind(row.id)
        .fetch_one(&app.db)
        .await?)
}

#[utoipa::path(put, operation_id = "storage_target_update", path = "/api/stores/{name}", params(("name" = String, Path)), request_body = StorageTargetPatch, responses((status = 200, body = StorageTargetView)))]
pub(crate) async fn update(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(name): Path<String>,
    Json(input): Json<StorageTargetPatch>,
) -> Result<Json<StorageTargetView>, HttpError> {
    actor.recent(&app).await?;
    let tx = super::users::admin_transaction(&app, &actor.principal).await?;
    tx.commit().await?;
    Ok(Json(update_record(&app, name, input).await?))
}

#[utoipa::path(post, operation_id = "storage_target_check", path = "/api/stores/{name}/check", params(("name" = String, Path)), responses((status = 200, body = StorageTargetView)))]
pub(crate) async fn check(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(name): Path<String>,
) -> Result<Json<StorageTargetView>, HttpError> {
    actor.recent(&app).await?;
    let tx = super::users::admin_transaction(&app, &actor.principal).await?;
    tx.commit().await?;
    let row: TargetRow = sqlx::query_as("SELECT id,name,type,path,endpoint_url,remote_bucket,region,prefix,access_key_encrypted,secret_key_encrypted,metadata,enabled,created_at,updated_at FROM storage_targets WHERE name=$1").bind(&name).fetch_optional(&app.db).await?.context("storage target was not found")?;
    app.storage.verify_target_record(row_record(&row)).await?;
    Ok(Json(
        sqlx::query_as(VIEW_BY_ID)
            .bind(row.id)
            .fetch_one(&app.db)
            .await?,
    ))
}
