use super::Identity;
use crate::{
    app::App,
    authorization::Principal,
    config::Control,
    http::HttpError,
};
use anyhow::{Context, Result};
use axum::{
    Json,
    extract::{Extension, Path, State},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use utoipa::ToSchema;

const SECTIONS: &[&str] = &[
    "web",
    "s3",
    "storage",
    "requests",
    "uploads",
    "cache",
    "compression",
    "packing",
    "garbage_collection",
    "retention",
    "statistics",
    "integrity",
];

#[derive(Clone, sqlx::FromRow)]
struct RuntimeSettingsRow {
    value: Value,
    revision: i64,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct ControlSettingsView {
    origin: String,
    secure_cookie: bool,
    session_lifetime: String,
    gravatar_base_url: String,
    thumbnail_cache_size: String,
    revision: i64,
    applied_revision: i64,
    scope: &'static str,
    restart_required: bool,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct ControlSettingsPatch {
    pub(crate) origin: Option<String>,
    pub(crate) secure_cookie: Option<bool>,
    pub(crate) session_lifetime: Option<String>,
    pub(crate) gravatar_base_url: Option<String>,
    pub(crate) thumbnail_cache_size: Option<String>,
    pub(crate) revision: Option<i64>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct RuntimeSettingsView {
    section: String,
    value: Value,
    revision: i64,
    applied_revision: i64,
    scope: &'static str,
    restart_required: bool,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct RuntimeSettingsPatch {
    value: Value,
    revision: i64,
}

fn valid_section(section: &str) -> Result<()> {
    anyhow::ensure!(SECTIONS.contains(&section), "unknown runtime settings section");
    Ok(())
}

fn validate_value(section: &str, value: &Value) -> Result<()> {
    valid_section(section)?;
    let object = value
        .as_object()
        .context("runtime settings value must be an object")?;
    let allowed: &[&str] = match section {
        "web" => &["origin", "secure_cookie", "session_lifetime", "avatar_url", "thumbnail_cache_size"],
        "s3" => &["domain", "signature_region", "max_stream_chunk_size"],
        "storage" => &["connect_timeout", "request_timeout", "max_retries", "retry_timeout", "priority_aging", "min_retention"],
        "requests" => &["read_idle_timeout", "upload_idle_timeout"],
        "uploads" => &["idle_timeout", "sweep_interval", "client_idle_timeout", "max_active"],
        "cache" => &["upload_enabled", "min_savings_percent"],
        "compression" => &["strategy", "level", "sample_level", "min_savings_percent", "min_savings_bytes", "context_idle_timeout", "skip_mime_types"],
        "packing" => &["enabled", "max_size", "upload_flush_timeout", "range_optimization", "range_interval", "range_window", "range_min_downloads", "range_min_savings_percent", "range_min_savings_bytes", "range_repack_after", "range_repack_window", "range_repack_retry_interval", "compression_strategy", "maintenance_concurrency", "interval", "reuse_interval", "reclaim_interval", "repack_interval", "repack_cooldown", "reclaim_min_savings_bytes"],
        "garbage_collection" => &["unreferenced_grace", "interval", "batch_size"],
        "retention" => &["interval", "batch_size", "max_duration", "deleted_chunk_retention", "upload_retention", "task_retention", "audit_retention"],
        "statistics" => &["scope_limit", "storage_sample_interval", "storage_retention", "runtime_sample_interval", "runtime_retention", "refresh_interval", "query_timeout", "access_flush_interval", "access_retention"],
        "integrity" => &["concurrency", "request_timeout", "requests_per_second", "bandwidth"],
        _ => &[],
    };
    for key in object.keys() {
        anyhow::ensure!(allowed.contains(&key.as_str()), "unknown field in runtime settings");
    }
    match section {
        "web" => {
            let control = Control {
                origin: object.get("origin").and_then(Value::as_str).unwrap_or_default().to_owned(),
                secure_cookie: object.get("secure_cookie").and_then(Value::as_bool).unwrap_or(false),
                session_lifetime: object.get("session_lifetime").and_then(Value::as_str).unwrap_or_default().to_owned(),
                gravatar_base_url: object.get("avatar_url").and_then(Value::as_str).unwrap_or_default().to_owned(),
                thumbnail_cache_size: object.get("thumbnail_cache_size").and_then(Value::as_str).unwrap_or_default().to_owned(),
            };
            control.validate()?;
        }
        "compression" => serde_json::from_value::<crate::compression::Config>(value.clone())?.validate()?,
        "packing" => serde_json::from_value::<crate::pack::Config>(value.clone())?.validate()?,
        "garbage_collection" => {
            let config: crate::config::Gc = serde_json::from_value(value.clone())?;
            anyhow::ensure!(config.batch_size > 0 && config.batch_size <= 10_000, "garbage_collection.batch_size must be 1..10000");
            crate::config::seconds(&config.interval)?;
            crate::config::seconds(&config.unreferenced_grace)?;
        }
        "retention" => {
            let config: crate::config::Cleanup = serde_json::from_value(value.clone())?;
            anyhow::ensure!(config.batch_size > 0 && config.batch_size <= 10_000, "retention.batch_size must be 1..10000");
            for item in [&config.interval, &config.max_duration, &config.deleted_chunk_retention, &config.upload_retention, &config.task_retention, &config.audit_retention] {
                crate::config::seconds(item)?;
            }
            anyhow::ensure!(crate::config::seconds(&config.max_duration)? <= 30, "retention.max_duration must be 1s..30s");
        }
        "statistics" => {
            let config: crate::config::Statistics = serde_json::from_value(value.clone())?;
            anyhow::ensure!((1..=256).contains(&config.scope_limit), "statistics.scope_limit must be 1..256");
            for item in [&config.storage_sample_interval, &config.storage_retention, &config.runtime_sample_interval, &config.runtime_retention, &config.refresh_interval, &config.query_timeout, &config.access_flush_interval, &config.access_retention] {
                crate::config::seconds(item)?;
            }
        }
        "integrity" => serde_json::from_value::<crate::integrity::Config>(value.clone())?.validate()?,
        "storage" => {
            for key in ["connect_timeout", "request_timeout", "retry_timeout", "priority_aging", "min_retention"] {
                if let Some(item) = object.get(key).and_then(Value::as_str) { crate::config::seconds(item)?; }
            }
            anyhow::ensure!(object.get("max_retries").and_then(Value::as_u64).is_some_and(|n| n <= 100), "storage.max_retries must be 0..100");
        }
        "requests" => {
            for key in ["read_idle_timeout", "upload_idle_timeout"] {
                crate::config::seconds(object.get(key).and_then(Value::as_str).unwrap_or_default())?;
            }
        }
        "uploads" => {
            for key in ["idle_timeout", "sweep_interval", "client_idle_timeout"] {
                crate::config::seconds(object.get(key).and_then(Value::as_str).unwrap_or_default())?;
            }
            anyhow::ensure!(object.get("max_active").is_none_or(|n| n.is_null() || n.as_i64().is_some_and(|n| n > 0)), "uploads.max_active must be positive");
        }
        "cache" => anyhow::ensure!(object.get("min_savings_percent").and_then(Value::as_u64).is_some_and(|n| n <= 100), "cache.min_savings_percent must be 0..100"),
        "s3" => {
            crate::config::bytes(object.get("max_stream_chunk_size").and_then(Value::as_str).unwrap_or_default())?;
        }
        _ => {}
    }
    Ok(())
}

fn runtime_view(app: &App, section: &str, value: Value, revision: i64) -> RuntimeSettingsView {
    let applied_revision = app
        .runtime_revisions
        .read()
        .ok()
        .and_then(|revisions| revisions.get(section).copied())
        .unwrap_or(0);
    RuntimeSettingsView {
        section: section.to_owned(),
        value,
        revision,
        applied_revision,
        scope: "deployment",
        restart_required: applied_revision != revision,
    }
}

pub(crate) async fn get_section_value(app: &App, section: &str) -> Result<Value> {
    valid_section(section)?;
    let value: Value = sqlx::query_scalar("SELECT value FROM runtime_settings WHERE section=$1")
        .bind(section)
        .fetch_one(&app.db)
        .await?;
    Ok(value)
}

pub(crate) async fn set_section_value(
    app: &App,
    principal: &Principal,
    section: &str,
    revision: i64,
    value: Value,
) -> Result<Value> {
    validate_value(section, &value)?;
    let mut tx = super::users::admin_transaction(app, principal).await?;
    let updated: Option<Value> = sqlx::query_scalar(
        "UPDATE runtime_settings SET value=$2,revision=revision+1,updated_at=now(),updated_by=$3 WHERE section=$1 AND revision=$4 RETURNING value",
    )
    .bind(section)
    .bind(value)
    .bind(principal.user_id())
    .bind(revision)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(value) = updated else {
        anyhow::bail!("settings revision conflict; reload before saving");
    };
    tx.commit().await?;
    Ok(value)
}

pub(crate) async fn reset_section_value(
    app: &App,
    principal: &Principal,
    section: &str,
) -> Result<Value> {
    valid_section(section)?;
    let value = crate::db::runtime_default(&app.config, section)
        .context("runtime settings default is unavailable")?;
    let mut tx = super::users::admin_transaction(app, principal).await?;
    let value: Value = sqlx::query_scalar(
        "UPDATE runtime_settings SET value=$2,revision=revision+1,updated_at=now(),updated_by=$3 WHERE section=$1 RETURNING value",
    )
    .bind(section)
    .bind(value)
    .bind(principal.user_id())
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(value)
}

impl RuntimeSettingsRow {
    fn control(&self) -> Result<Control> {
        let value = self.value.as_object().cloned().unwrap_or_default();
        Ok(Control {
            origin: value.get("origin").and_then(Value::as_str).context("runtime web origin is missing")?.to_owned(),
            secure_cookie: value.get("secure_cookie").and_then(Value::as_bool).context("runtime web secure_cookie is missing")?,
            session_lifetime: value.get("session_lifetime").and_then(Value::as_str).context("runtime web session_lifetime is missing")?.to_owned(),
            gravatar_base_url: value.get("avatar_url").and_then(Value::as_str).context("runtime web avatar_url is missing")?.to_owned(),
            thumbnail_cache_size: value.get("thumbnail_cache_size").and_then(Value::as_str).context("runtime web thumbnail_cache_size is missing")?.to_owned(),
        })
    }
}

fn view(app: &App, control: Control, revision: i64) -> ControlSettingsView {
    let restart_required = control.origin != app.config.control.origin
        || control.secure_cookie != app.config.control.secure_cookie
        || control.session_lifetime != app.config.control.session_lifetime
        || control.gravatar_base_url != app.config.control.gravatar_base_url
        || control.thumbnail_cache_size != app.config.control.thumbnail_cache_size;
    ControlSettingsView {
        origin: control.origin,
        secure_cookie: control.secure_cookie,
        session_lifetime: control.session_lifetime,
        gravatar_base_url: control.gravatar_base_url,
        thumbnail_cache_size: control.thumbnail_cache_size,
        revision,
        applied_revision: if restart_required { 0 } else { revision },
        scope: "deployment",
        restart_required,
    }
}

async fn row(app: &App) -> Result<RuntimeSettingsRow> {
    sqlx::query_as(
        "SELECT value,revision FROM runtime_settings WHERE section='web'",
    )
    .fetch_one(&app.db)
    .await
    .context("control settings are missing")
}

pub(crate) async fn read(app: &App) -> Result<ControlSettingsView> {
    let row = row(app).await?;
    Ok(view(app, row.control()?, row.revision))
}

pub(crate) async fn update(
    app: &App,
    principal: &Principal,
    input: ControlSettingsPatch,
) -> Result<ControlSettingsView> {
    let mut tx = super::users::admin_transaction(app, principal).await?;
    let current: RuntimeSettingsRow = sqlx::query_as(
        "SELECT value,revision FROM runtime_settings WHERE section='web' FOR UPDATE",
    )
    .fetch_one(&mut *tx)
    .await?;
    let mut control = current.control()?;
    if let Some(value) = input.origin {
        control.origin = value;
    }
    if let Some(value) = input.secure_cookie {
        control.secure_cookie = value;
    }
    if let Some(value) = input.session_lifetime {
        control.session_lifetime = value;
    }
    if let Some(value) = input.gravatar_base_url {
        control.gravatar_base_url = value;
    }
    if let Some(value) = input.thumbnail_cache_size {
        control.thumbnail_cache_size = value;
    }
    control.validate()?;
    if let Some(revision) = input.revision {
        anyhow::ensure!(revision == current.revision, "settings revision conflict; reload before saving");
    }
    let value = serde_json::json!({
        "origin": control.origin,
        "secure_cookie": control.secure_cookie,
        "session_lifetime": control.session_lifetime,
        "avatar_url": control.gravatar_base_url,
        "thumbnail_cache_size": control.thumbnail_cache_size,
    });
    let next_revision = current.revision.checked_add(1).context("settings revision overflow")?;
    sqlx::query(
        "UPDATE runtime_settings SET value=$1,revision=$2,updated_at=now(),updated_by=$3 WHERE section='web' AND revision=$4",
    )
    .bind(value)
    .bind(next_revision)
    .bind(principal.user_id())
    .bind(current.revision)
    .execute(&mut *tx)
    .await?;
    super::audit::checkpoint(
        &mut tx,
        "control.settings",
        "",
        serde_json::json!({
            "origin": control.origin,
            "secure_cookie": control.secure_cookie,
            "session_lifetime": control.session_lifetime,
            "gravatar_base_url": control.gravatar_base_url,
            "thumbnail_cache_size": control.thumbnail_cache_size,
        }),
    )
    .await?;
    tx.commit().await?;
    Ok(view(app, control, next_revision))
}

#[utoipa::path(get, operation_id="runtime_settings", path="/api/settings", responses((status=200, body=Vec<RuntimeSettingsView>)))]
pub(super) async fn list(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<Vec<RuntimeSettingsView>>, HttpError> {
    let mut tx = super::users::admin_transaction(&app, &actor.principal).await?;
    let rows: Vec<(String, Value, i64)> = sqlx::query_as(
        "SELECT section,value,revision FROM runtime_settings ORDER BY section",
    )
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Json(
        rows.into_iter()
            .map(|(section, value, revision)| runtime_view(&app, &section, value, revision))
            .collect(),
    ))
}

#[utoipa::path(get, operation_id="runtime_settings_section", path="/api/settings/{section}", params(("section" = String, Path)), responses((status=200, body=RuntimeSettingsView)))]
pub(super) async fn get_section(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(section): Path<String>,
) -> Result<Json<RuntimeSettingsView>, HttpError> {
    valid_section(&section)?;
    let mut tx = super::users::admin_transaction(&app, &actor.principal).await?;
    let row: (Value, i64) = sqlx::query_as(
        "SELECT value,revision FROM runtime_settings WHERE section=$1",
    )
    .bind(&section)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Json(runtime_view(&app, &section, row.0, row.1)))
}

#[utoipa::path(put, operation_id="runtime_settings_save", path="/api/settings/{section}", params(("section" = String, Path)), request_body=RuntimeSettingsPatch, responses((status=200, body=RuntimeSettingsView)))]
pub(super) async fn save_section(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(section): Path<String>,
    Json(input): Json<RuntimeSettingsPatch>,
) -> Result<Json<RuntimeSettingsView>, HttpError> {
    actor.recent(&app).await?;
    validate_value(&section, &input.value)?;
    let mut tx = super::users::admin_transaction(&app, &actor.principal).await?;
    let changed: Option<(Value, i64)> = sqlx::query_as(
        "UPDATE runtime_settings SET value=$2,revision=revision+1,updated_at=now(),updated_by=$3 WHERE section=$1 AND revision=$4 RETURNING value,revision",
    )
    .bind(&section)
    .bind(input.value)
    .bind(actor.principal.user_id())
    .bind(input.revision)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((value, revision)) = changed else {
        return Err(anyhow::anyhow!("settings revision conflict; reload before saving").into());
    };
    super::audit::checkpoint(
        &mut tx,
        "runtime.settings",
        &section,
        serde_json::json!({"revision": revision}),
    )
    .await?;
    tx.commit().await?;
    Ok(Json(runtime_view(&app, &section, value, revision)))
}

#[utoipa::path(post, operation_id="runtime_settings_reset", path="/api/settings/{section}/reset", params(("section" = String, Path)), responses((status=200, body=RuntimeSettingsView)))]
pub(super) async fn reset_section(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(section): Path<String>,
) -> Result<Json<RuntimeSettingsView>, HttpError> {
    valid_section(&section)?;
    let value = crate::db::runtime_default(&app.config, &section)
        .context("runtime settings default is unavailable")?;
    let mut tx = super::users::admin_transaction(&app, &actor.principal).await?;
    let row: (Value, i64) = sqlx::query_as(
        "UPDATE runtime_settings SET value=$2,revision=revision+1,updated_at=now(),updated_by=$3 WHERE section=$1 RETURNING value,revision",
    )
    .bind(&section)
    .bind(value)
    .bind(actor.principal.user_id())
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Json(runtime_view(&app, &section, row.0, row.1)))
}
