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

fn validate_value(configured: &crate::config::Config, section: &str, value: &Value) -> Result<()> {
    valid_section(section)?;
    let object = value
        .as_object()
        .context("runtime settings value must be an object")?;
    let allowed: &[&str] = match section {
        "web" => &["origin", "secure_cookie", "session_lifetime", "avatar_url"],
        "s3" => &["domain", "signature_region", "max_stream_chunk_size"],
        "storage" => &["connect_timeout", "request_timeout", "max_retries", "retry_timeout", "priority_aging", "min_retention"],
        "requests" => &["read_idle_timeout", "upload_idle_timeout"],
        "uploads" => &["idle_timeout", "sweep_interval", "client_idle_timeout", "max_active"],
        "cache" => &["upload_enabled", "min_savings_percent"],
        "compression" => &["strategy", "level", "sample_level", "min_savings_percent", "min_savings_bytes", "skip_mime_types"],
        "packing" => &["enabled", "max_size", "upload_flush_timeout", "range_enabled", "range_interval", "range_window", "range_min_downloads", "range_min_savings_percent", "range_min_savings_bytes", "range_repack_after", "range_repack_window", "range_repack_retry_interval", "compression_strategy", "interval", "reuse_interval", "reclaim_interval", "repack_interval", "repack_cooldown", "reclaim_min_savings_bytes"],
        "garbage_collection" => &["grace_period", "interval", "batch_size"],
        "retention" => &["interval", "batch_size", "max_duration", "deleted_chunks", "uploads", "tasks", "audit"],
        "statistics" => &["scope_limit", "storage_interval", "storage_retention", "runtime_interval", "runtime_retention", "refresh_interval", "query_timeout", "access_flush_interval", "access_retention"],
        "integrity" => &["request_timeout"],
        _ => &[],
    };
    for key in object.keys() {
        anyhow::ensure!(allowed.contains(&key.as_str()), "unknown field in runtime settings");
    }
    let mut config = configured.clone();
    crate::db::apply_runtime_section(&mut config, section, value.clone())?;
    config.validate()?;
    config.budget()?;
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
    validate_value(&app.config, section, &value)?;
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
    let value = app.runtime_defaults.get(section).cloned()
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
        let mut control = Control::default();
        control.origin = value.get("origin").and_then(Value::as_str).context("runtime web origin is missing")?.to_owned();
        control.secure_cookie = value.get("secure_cookie").and_then(Value::as_bool).context("runtime web secure_cookie is missing")?;
        control.session_lifetime = value.get("session_lifetime").and_then(Value::as_str).context("runtime web session_lifetime is missing")?.to_owned();
        control.gravatar_base_url = value.get("avatar_url").and_then(Value::as_str).context("runtime web avatar_url is missing")?.to_owned();
        Ok(control)
    }
}

fn view(app: &App, control: Control, revision: i64) -> ControlSettingsView {
    let restart_required = control.origin != app.config.control.origin
        || control.secure_cookie != app.config.control.secure_cookie
        || control.session_lifetime != app.config.control.session_lifetime
        || control.gravatar_base_url != app.config.control.gravatar_base_url;
    ControlSettingsView {
        origin: control.origin,
        secure_cookie: control.secure_cookie,
        session_lifetime: control.session_lifetime,
        gravatar_base_url: control.gravatar_base_url,
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
    control.validate()?;
    if let Some(revision) = input.revision {
        anyhow::ensure!(revision == current.revision, "settings revision conflict; reload before saving");
    }
    let value = serde_json::json!({
        "origin": control.origin,
        "secure_cookie": control.secure_cookie,
        "session_lifetime": control.session_lifetime,
        "avatar_url": control.gravatar_base_url,
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
    validate_value(&app.config, &section, &input.value)?;
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
    let value = app.runtime_defaults.get(&section).cloned()
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
