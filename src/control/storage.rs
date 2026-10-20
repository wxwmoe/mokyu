use super::Identity;
use crate::model::Id;
use crate::{
    app::App,
    content::codec,
    http::{HttpError, problem},
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
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use utoipa::ToSchema;
use uuid::Uuid;

pub(crate) use crate::model::storage::StorageView;
use crate::model::storage::{StorageEdit, StorageRow};
use crate::storage::profile::validate as validate_profile;
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
    pack_id: String,
    confirmation: Option<String>,
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
    storage_type: String,
    path: Option<String>,
    endpoint_value: Option<String>,
    bucket: Option<String>,
    region: Option<String>,
    prefix: String,
    access_key: Option<String>,
    secret_key: Option<String>,
    identity_marker: serde_json::Value,
) -> anyhow::Result<BackendRecord> {
    let (path, endpoint_url, remote_bucket_name, region, access, secret) =
        if storage_type == "local" {
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
    if let Some(path) = &path {
        crate::storage::local::validate_storage_path(
            &app.config.instance.data_path,
            &std::path::PathBuf::from(path).join(prefix.trim_matches('/')),
        )?;
    }
    Ok(BackendRecord {
        id,
        name,
        r#type: storage_type,
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

#[utoipa::path(get, operation_id = "storages", path = "/api/v1/storages", responses((status = 200, body = Vec<StorageView>)))]
pub(crate) async fn list(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<Vec<StorageView>>, HttpError> {
    app.authorization
        .admin(&actor.principal)
        .await?
        .finish()
        .await?;
    Ok(Json(app.storage_records.list().await?))
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
    app.authorization
        .admin(&actor.principal)
        .await?
        .finish()
        .await?;
    let storage_id: Uuid = app
        .storage_records
        .id(&name, None)
        .await?
        .context("storage was not found")?;
    Ok(Json(app.storage_profiles.load(storage_id).await?))
}

async fn validate_profile_capacity(app: &App, profile: &StorageProfile) -> anyhow::Result<()> {
    let mut profiles = app.storage_profiles.enabled_profiles().await?;
    profiles.retain(|current| current.storage_id != profile.storage_id);
    profiles.push(profile.clone());
    let budget = app
        .config
        .budget_for(&profiles)
        .map_err(|error| s3s::s3_error!(InvalidArgument, "{error}"))?;
    profile
        .validate_budget(&app.config, &budget)
        .map_err(|error| s3s::s3_error!(InvalidArgument, "{error}").into())
}

pub(crate) async fn update_profile(
    app: &App,
    name: String,
    profile: StorageProfile,
) -> anyhow::Result<StorageProfile> {
    validate_profile_capacity(app, &profile).await?;
    let id = app.storage_profiles.update(name, profile).await?;
    app.storage.refresh_store(id).await?;
    app.storage_profiles.load(id).await
}
#[utoipa::path(
    put,
    operation_id = "storage_profile_update",
    path = "/api/v1/storages/{name}/profile",
    params(("name" = String, Path)),
    request_body = StorageProfile,
    responses((status = 200, body = StorageProfile), (status = 400, body = crate::control::contract::ErrorBody), (status = 409, body = crate::control::contract::ErrorBody))
)]
pub(crate) async fn update_profile_route(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(name): Path<String>,
    Json(profile): Json<StorageProfile>,
) -> Result<Json<StorageProfile>, HttpError> {
    actor.recent(&app).await?;
    app.authorization
        .admin(&actor.principal)
        .await?
        .finish()
        .await?;
    Ok(Json(update_profile(&app, name, profile).await?))
}

pub(crate) async fn create_record(app: &App, input: StorageInput) -> anyhow::Result<StorageView> {
    let mut profiles = app.storage_profiles.enabled_profiles().await?;
    let crate::model::storage::StorageCreation {
        deployment: deployment_id,
        mode,
        mut writer,
    } = app.storage_records.creation().await?;
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
        for (alias, value) in [
            (input.read_concurrency, profile.read_concurrency),
            (input.upload_concurrency, profile.upload_concurrency),
            (input.control_concurrency, profile.control_concurrency),
        ] {
            anyhow::ensure!(
                alias.is_none() || alias == value,
                "concurrency conflicts with storage profile"
            );
        }
        validate_profile(profile, &record.r#type)?;
    }
    let profile_for_init = writer
        .prepare(
            &record,
            input.block_key_id,
            input.read_concurrency,
            input.upload_concurrency,
            input.control_concurrency,
            input.profile,
        )
        .await?;
    profiles.push(profile_for_init.clone());
    let budget = app
        .config
        .budget_for(&profiles)
        .map_err(|error| s3s::s3_error!(InvalidArgument, "{error}"))?;
    profile_for_init
        .validate_budget(&app.config, &budget)
        .map_err(|error| s3s::s3_error!(InvalidArgument, "{error}"))?;
    app.storage
        .initialize_store_with_profile(record.clone(), &profile_for_init)
        .await?;
    writer.finish().await?;
    app.storage.refresh_store(record.id).await?;
    app.storage_records.view(record.id).await
}

#[utoipa::path(post, operation_id = "storage_create", path = "/api/v1/storages", request_body = StorageInput, responses((status = 201, body = StorageView), (status = 400, body = crate::control::contract::ErrorBody)))]
pub(crate) async fn create(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<StorageInput>,
) -> Result<(StatusCode, Json<StorageView>), HttpError> {
    actor.recent(&app).await?;
    app.authorization
        .admin(&actor.principal)
        .await?
        .finish()
        .await?;
    Ok((StatusCode::CREATED, Json(create_record(&app, input).await?)))
}

pub(crate) async fn update_record(
    app: &App,
    name: String,
    input: StoragePatch,
) -> anyhow::Result<StorageView> {
    let changing_type = if let Some(requested) = &input.storage_type {
        validate_storage_type(requested)?;
        app.storage_records
            .record(&name)
            .await?
            .is_some_and(|row| row.r#type != *requested)
    } else {
        false
    };
    let _coord = if changing_type {
        Some(app.coord.lock().await)
    } else {
        None
    };
    let mut _cleanup = None;
    let mut _local = None;
    if changing_type {
        use std::sync::atomic::Ordering;
        anyhow::ensure!(app.role == "single", crate::model::Error::StorageNotDrained);
        app.ensure_mode()?;
        anyhow::ensure!(
            app.maintenance.load(Ordering::Acquire),
            crate::model::Error::StorageNotDrained
        );
        anyhow::ensure!(
            app.active_operations() == 0,
            crate::model::Error::StorageNotDrained
        );
        anyhow::ensure!(
            !app.gc_running.load(Ordering::Acquire),
            crate::model::Error::StorageNotDrained
        );
        _cleanup = Some(
            app.cleanup_state
                .running
                .try_lock()
                .map_err(|_| crate::model::Error::StorageNotDrained)?,
        );
        _local = Some(
            app.local_cleanup
                .try_lock()
                .map_err(|_| crate::model::Error::StorageNotDrained)?,
        );
    }
    let StorageEdit {
        row,
        profile,
        mode,
        update,
    } = app.storage_records.edit(&name).await?;
    anyhow::ensure!(
        row.identity_marker != serde_json::json!({}),
        "storage identity marker is empty; initialize and check it before changing the connection"
    );
    let storage_type = input
        .storage_type
        .clone()
        .unwrap_or_else(|| row.r#type.clone());
    validate_storage_type(&storage_type)?;
    anyhow::ensure!(
        row.r#type == storage_type || changing_type,
        "storage changed while preparing the update; retry"
    );
    anyhow::ensure!(
        !(mode == "cluster" && storage_type == "local"),
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
    let mut record = if storage_type == "local" {
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
            storage_type,
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
            storage_type,
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
    app.storage
        .verify_store_record(record.clone())
        .await
        .map_err(|error| {
            tracing::warn!(%error, storage_id=%row.id, "storage verification failed");
            problem(StatusCode::CONFLICT, "StorageVerificationFailed").0
        })?;
    let block_key_id = input.block_key_id.unwrap_or(row.block_key_id);
    let mut requested = if let Some(requested) = input.profile {
        anyhow::ensure!(
            requested.storage_id == row.id,
            "storage profile belongs to another storage"
        );
        if requested.revision != profile.revision {
            return Err(crate::model::Error::StorageProfileConflict.into());
        }
        requested
    } else {
        let mut requested = profile;
        requested.read_concurrency = read_concurrency;
        requested.upload_concurrency = upload_concurrency;
        requested.control_concurrency = control_concurrency;
        requested
    };
    if changing_type {
        requested.min_free_space = if record.r#type == "local" {
            Some(requested.min_free_space.unwrap_or_else(|| "1GiB".into()))
        } else {
            None
        };
    }
    if record.r#type == "local" {
        requested.cache_enabled = false;
        requested.upload_cache = false;
        requested.pack_enabled = false;
        requested.pack_range_enabled = false;
    }
    validate_profile(&requested, &record.r#type)?;
    validate_profile_capacity(app, &requested).await?;
    if changing_type {
        // Never let old connections resume after a successful or uncertain relocation.
        app.restart_required
            .store(true, std::sync::atomic::Ordering::Release);
    }
    #[cfg(feature = "fault-injection")]
    if changing_type {
        crate::faults::point("storage-relocating").await;
    }
    let result = update.finish(&record, block_key_id, &requested).await;
    if let Err(error) = result {
        if changing_type
            && app
                .storage_records
                .record(&name)
                .await?
                .is_some_and(|current| current.r#type == row.r#type)
        {
            app.restart_required
                .store(false, std::sync::atomic::Ordering::Release);
        }
        return Err(error);
    }
    #[cfg(feature = "fault-injection")]
    if changing_type {
        crate::faults::point("storage-relocated").await;
    }
    if !changing_type {
        app.storage.refresh_store(row.id).await?;
    }
    app.storage_records.view(row.id).await
}

pub(crate) async fn set_current_key(
    app: &App,
    name: String,
    key: Option<String>,
) -> anyhow::Result<StorageView> {
    let id = app
        .storage_records
        .current_key(&name, key.as_deref())
        .await?;
    app.storage.refresh_store(id).await?;
    app.storage_records.view(id).await
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
    let storage_id: Uuid = app
        .storage_records
        .id(&name, None)
        .await?
        .context("storage was not found")?;
    let pack_id = Id::parse(&input.pack_id, app.config.database.backend)?;
    let belongs = app.packing.in_storage(pack_id, storage_id).await?;
    if !belongs {
        return Err(anyhow::anyhow!("pack was not found in storage").into());
    }
    match input.action.as_str() {
        "preview" => Ok(Json(app.unpack_start(Some(pack_id), false, false).await?)),
        "start" => {
            let confirmation = input
                .confirmation
                .as_deref()
                .context("confirmation is required")?;
            let preview = app.unpack_start(Some(pack_id), false, false).await?;
            let expected = blake3::hash(&serde_json::to_vec(&preview)?)
                .to_hex()
                .to_string();
            if confirmation != expected {
                return Err(anyhow::anyhow!("preview confirmation mismatch").into());
            }
            Ok(Json(
                app.unpack_checked(Some(pack_id), false, true, Some(&expected))
                    .await?,
            ))
        }
        "status" => Ok(Json(
            app.task_control
                .unpack_status(pack_id)
                .await?
                .unwrap_or_else(|| serde_json::json!({"status":"missing"})),
        )),
        _ => Err(s3s::s3_error!(InvalidArgument).into()),
    }
}

#[utoipa::path(put, operation_id = "storage_update", path = "/api/v1/storages/{name}", params(("name" = String, Path)), request_body = StoragePatch, responses((status = 200, body = StorageView), (status = 409, body = crate::control::contract::ErrorBody)))]
pub(crate) async fn update(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(name): Path<String>,
    Json(input): Json<StoragePatch>,
) -> Result<Json<StorageView>, HttpError> {
    actor.recent(&app).await?;
    app.authorization
        .admin(&actor.principal)
        .await?
        .finish()
        .await?;
    Ok(Json(update_record(&app, name, input).await?))
}

#[utoipa::path(post, operation_id = "storage_check", path = "/api/v1/storages/{name}/check", params(("name" = String, Path)), responses((status = 200, body = StorageView), (status = 409, body = crate::control::contract::ErrorBody)))]
pub(crate) async fn check(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(name): Path<String>,
) -> Result<Json<StorageView>, HttpError> {
    actor.recent(&app).await?;
    app.authorization
        .admin(&actor.principal)
        .await?
        .finish()
        .await?;
    let row = app
        .storage_records
        .record(&name)
        .await?
        .context("storage was not found")?;
    app.storage
        .verify_store_record(row_record(&row))
        .await
        .map_err(|error| {
            tracing::warn!(%error, storage_id=%row.id, "storage verification failed");
            problem(StatusCode::CONFLICT, "StorageVerificationFailed").0
        })?;
    Ok(Json(app.storage_records.view(row.id).await?))
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
    let storage_id: Uuid = app
        .storage_records
        .id(&name, None)
        .await?
        .context("storage was not found")?;
    let value = match input.action.as_str() {
        "preview" => {
            app.rewrite_start(storage_id, input.target_key.as_deref(), false, None)
                .await?
        }
        "start" => {
            app.rewrite_start(
                storage_id,
                input.target_key.as_deref(),
                true,
                input.confirmation.as_deref(),
            )
            .await?
        }
        "status" => app
            .task_control
            .rewrite_status(storage_id)
            .await?
            .context("rewrite task not found")?,
        "pause" | "resume" | "retry" => {
            let task: Uuid = app
                .task_control
                .rewrite_task(storage_id)
                .await?
                .context("rewrite task not found")?;
            app.task_change(task, input.action != "pause").await?
        }
        _ => return Err(s3s::s3_error!(InvalidArgument).into()),
    };
    Ok(Json(value))
}
