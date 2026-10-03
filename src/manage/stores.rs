use super::Identity;
use crate::{app::App, codec, http::HttpError};
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
    name: String,
    #[serde(rename = "type")]
    target_type: String,
    path: Option<String>,
    endpoint: Option<String>,
    bucket: Option<String>,
    region: Option<String>,
    #[serde(default)]
    prefix: String,
    access_key: Option<String>,
    secret_key: Option<String>,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct StorageTargetPatch {
    path: Option<String>,
    endpoint: Option<String>,
    bucket: Option<String>,
    region: Option<String>,
    prefix: Option<String>,
    access_key: Option<String>,
    secret_key: Option<String>,
    enabled: Option<bool>,
}

fn validate_name(name: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        (1..=128).contains(&name.trim().len())
            && !name.trim().contains('/')
            && !name.chars().any(char::is_control),
        "storage target name must contain 1..128 safe bytes"
    );
    Ok(())
}

fn validate_kind(kind: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        matches!(kind, "local" | "s3"),
        "storage target type is local or s3"
    );
    Ok(())
}

async fn list_rows(db: &sqlx::PgPool) -> anyhow::Result<Vec<StorageTargetView>> {
    Ok(sqlx::query_as("SELECT id,name,type AS target_type,path,endpoint_url AS endpoint,remote_bucket AS bucket,region,prefix,enabled,created_at,updated_at FROM storage_targets ORDER BY name")
        .fetch_all(db)
        .await?)
}

#[utoipa::path(get, operation_id = "stores", path = "/api/stores", responses((status = 200, body = Vec<StorageTargetView>)))]
pub(crate) async fn list(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<Vec<StorageTargetView>>, HttpError> {
    let tx = super::users::admin_transaction(&app, &actor.principal).await?;
    tx.commit().await?;
    let rows = list_rows(&app.db).await?;
    Ok(Json(rows))
}

#[utoipa::path(post, operation_id = "storage_target_create", path = "/api/stores", request_body = StorageTargetInput, responses((status = 201, body = StorageTargetView)))]
pub(crate) async fn create(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<StorageTargetInput>,
) -> Result<(StatusCode, Json<StorageTargetView>), HttpError> {
    actor.recent(&app).await?;
    let mut tx = super::users::admin_transaction(&app, &actor.principal).await?;
    validate_name(&input.name)?;
    validate_kind(&input.target_type)?;
    let id = Uuid::new_v4();
    let (path, endpoint, bucket, region, access, secret) = if input.target_type == "local" {
        if !(input.endpoint.is_none()
            && input.bucket.is_none()
            && input.region.is_none()
            && input.access_key.is_none()
            && input.secret_key.is_none())
        {
            return Err(anyhow::anyhow!("local storage targets cannot include S3 settings").into());
        }
        (
            Some(input.path.unwrap_or_else(|| "/data/storage".into())),
            None,
            None,
            None,
            None,
            None,
        )
    } else {
        let endpoint = input
            .endpoint
            .ok_or_else(|| s3s::s3_error!(InvalidArgument))?;
        let bucket = input
            .bucket
            .ok_or_else(|| s3s::s3_error!(InvalidArgument))?;
        let region = input
            .region
            .ok_or_else(|| s3s::s3_error!(InvalidArgument))?;
        let access_key = input
            .access_key
            .ok_or_else(|| s3s::s3_error!(InvalidArgument))?;
        let secret_key = input
            .secret_key
            .ok_or_else(|| s3s::s3_error!(InvalidArgument))?;
        if endpoint.is_empty() || bucket.is_empty() || region.is_empty() {
            return Err(anyhow::anyhow!("S3 connection settings must not be empty").into());
        }
        (
            None,
            Some(endpoint),
            Some(bucket),
            Some(region),
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
    sqlx::query("INSERT INTO storage_targets(id,name,type,path,endpoint_url,remote_bucket,region,prefix,access_key_encrypted,secret_key_encrypted) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)")
        .bind(id)
        .bind(input.name.trim())
        .bind(&input.target_type)
        .bind(path)
        .bind(endpoint)
        .bind(bucket)
        .bind(region)
        .bind(input.prefix)
        .bind(access)
        .bind(secret)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    let row: StorageTargetView = sqlx::query_as("SELECT id,name,type AS target_type,path,endpoint_url AS endpoint,remote_bucket AS bucket,region,prefix,enabled,created_at,updated_at FROM storage_targets WHERE id=$1")
        .bind(id)
        .fetch_one(&app.db)
        .await?;
    Ok((StatusCode::CREATED, Json(row)))
}

#[utoipa::path(put, operation_id = "storage_target_update", path = "/api/stores/{name}", params(("name" = String, Path)), request_body = StorageTargetPatch, responses((status = 200, body = StorageTargetView)))]
pub(crate) async fn update(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(name): Path<String>,
    Json(input): Json<StorageTargetPatch>,
) -> Result<Json<StorageTargetView>, HttpError> {
    actor.recent(&app).await?;
    let mut tx = super::users::admin_transaction(&app, &actor.principal).await?;
    let (id, kind, current_path, current_endpoint, current_bucket, current_region, current_prefix, current_access, current_secret, metadata): (Uuid,String,Option<String>,Option<String>,Option<String>,Option<String>,String,Option<Vec<u8>>,Option<Vec<u8>>,serde_json::Value) = sqlx::query_as("SELECT id,type,path,endpoint_url,remote_bucket,region,prefix,access_key_encrypted,secret_key_encrypted,metadata FROM storage_targets WHERE name=$1 FOR UPDATE")
        .bind(&name).fetch_optional(&mut *tx).await?.ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    if metadata == serde_json::json!({}) {
        return Err(anyhow::anyhow!(
            "storage target metadata is empty; initialize and check it before changing the connection"
        )
        .into());
    }
    let path = input.path.or(current_path);
    let endpoint = input.endpoint.or(current_endpoint);
    let bucket = input.bucket.or(current_bucket);
    let region = input.region.or(current_region);
    let prefix = input.prefix.unwrap_or(current_prefix);
    let access = match input.access_key {
        Some(value) => Some(codec::protect(
            value.as_bytes(),
            &app.secrets.credential_key,
            format!("storage-target-{id}-access").as_bytes(),
        )?),
        None => current_access,
    };
    let secret = match input.secret_key {
        Some(value) => Some(codec::protect(
            value.as_bytes(),
            &app.secrets.credential_key,
            format!("storage-target-{id}-secret").as_bytes(),
        )?),
        None => current_secret,
    };
    if kind == "local" {
        if !(path.is_some()
            && endpoint.is_none()
            && bucket.is_none()
            && region.is_none()
            && access.is_none()
            && secret.is_none())
        {
            return Err(anyhow::anyhow!("invalid local storage target settings").into());
        }
    } else {
        if !(path.is_none()
            && endpoint.is_some()
            && bucket.is_some()
            && region.is_some()
            && access.is_some()
            && secret.is_some())
        {
            return Err(anyhow::anyhow!("invalid S3 storage target settings").into());
        }
    }
    sqlx::query("UPDATE storage_targets SET path=$2,endpoint_url=$3,remote_bucket=$4,region=$5,prefix=$6,access_key_encrypted=$7,secret_key_encrypted=$8,enabled=coalesce($9,enabled),updated_at=now() WHERE id=$1")
        .bind(id).bind(path).bind(endpoint).bind(bucket).bind(region).bind(prefix).bind(access).bind(secret).bind(input.enabled).execute(&mut *tx).await?;
    tx.commit().await?;
    let row: StorageTargetView = sqlx::query_as("SELECT id,name,type AS target_type,path,endpoint_url AS endpoint,remote_bucket AS bucket,region,prefix,enabled,created_at,updated_at FROM storage_targets WHERE id=$1").bind(id).fetch_one(&app.db).await?;
    Ok(Json(row))
}

#[utoipa::path(post, operation_id = "storage_target_check", path = "/api/stores/{name}/check", params(("name" = String, Path)), responses((status = 200, body = StorageTargetView)))]
pub(crate) async fn check(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(name): Path<String>,
) -> Result<Json<StorageTargetView>, HttpError> {
    actor.recent(&app).await?;
    let row: StorageTargetView = sqlx::query_as("SELECT id,name,type AS target_type,path,endpoint_url AS endpoint,remote_bucket AS bucket,region,prefix,enabled,created_at,updated_at FROM storage_targets WHERE name=$1")
        .bind(name).fetch_optional(&app.db).await?.ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    if row.target_type == "local" {
        if !row
            .path
            .as_deref()
            .is_some_and(|path| std::path::Path::new(path).exists())
        {
            return Err(anyhow::anyhow!("local storage target path does not exist").into());
        }
    }
    Ok(Json(row))
}
