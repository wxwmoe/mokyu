use super::Identity;
use crate::{app::App, authorization::Principal, config::WebSettings, http::HttpError};
use anyhow::{Context, Result};
use axum::{
    Json,
    extract::{Extension, Path, State},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use utoipa::ToSchema;

const SECTIONS: &[&str] = &["web", "requests", "uploads", "metrics", "maintenance", "io"];

pub(crate) use crate::model::settings::ControlSettingsPatch;
use crate::model::settings::RuntimeSettingsRow;

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
    anyhow::ensure!(
        SECTIONS.contains(&section),
        "unknown runtime settings section"
    );
    Ok(())
}

fn validate_value(configured: &crate::config::Config, section: &str, value: &Value) -> Result<()> {
    (|| -> Result<()> {
        valid_section(section)?;
        let object = value
            .as_object()
            .context("runtime settings value must be an object")?;
        let allowed: &[&str] = match section {
            "io" => &["sqlite_synchronous", "shutdown_flush_timeout"],
            "web" => &[
                "origin",
                "secure_cookie",
                "session_lifetime",
                "gravatar_base_url",
            ],
            "requests" => &[
                "s3_domain",
                "s3_region",
                "s3_chunk_limit",
                "read_idle_timeout",
                "upload_idle_timeout",
            ],
            "uploads" => &[
                "sweep_interval",
                "client_idle_timeout",
                "max_active_uploads",
                "staging_storage_id",
                "staging_bytes_limit",
            ],
            "metrics" => &[
                "scope_limit",
                "storage_interval",
                "storage_retention",
                "runtime_interval",
                "runtime_retention",
                "refresh_interval",
                "query_timeout",
                "access_flush_interval",
                "access_retention",
            ],
            "maintenance" => &[
                "interval",
                "batch_size",
                "max_duration",
                "task_retention",
                "audit_retention",
            ],
            _ => &[],
        };
        for key in object.keys() {
            anyhow::ensure!(
                allowed.contains(&key.as_str()),
                "unknown field in runtime settings"
            );
        }
        let mut config = configured.clone();
        crate::model::apply_runtime_section(&mut config, section, value.clone())?;
        config.validate()?;
        config.budget()?;
        Ok(())
    })()
    .map_err(|error| s3s::s3_error!(InvalidArgument, "{error}").into())
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
    app.runtime_settings.value(section).await
}

pub(crate) async fn set_section_value(
    app: &App,
    principal: &Principal,
    section: &str,
    revision: Option<i64>,
    value: Value,
) -> Result<Value> {
    validate_value(&app.config, section, &value)?;
    let mut proposed = app.config.clone();
    crate::model::apply_runtime_section(&mut proposed, section, value.clone())?;
    for profile in app.storage_profiles.enabled_profiles().await? {
        profile.validate_budget(&proposed, &app.budget)?;
    }
    Ok(app
        .runtime_settings
        .save(principal, section, revision, value)
        .await?
        .value)
}

pub(crate) async fn reset_section_value(
    app: &App,
    principal: &Principal,
    section: &str,
) -> Result<Value> {
    valid_section(section)?;
    let value = app
        .runtime_defaults
        .get(section)
        .cloned()
        .context("runtime settings default is unavailable")?;
    Ok(app
        .runtime_settings
        .reset(principal, section, value)
        .await?
        .value)
}

fn view(app: &App, control: WebSettings, revision: i64) -> ControlSettingsView {
    let restart_required = control.origin != app.config.web.origin
        || control.secure_cookie != app.config.web.secure_cookie
        || control.session_lifetime != app.config.web.session_lifetime
        || control.gravatar_base_url != app.config.web.gravatar_base_url;
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
    app.runtime_settings
        .row("web")
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
    let (control, revision) = app.runtime_settings.update_web(principal, input).await?;
    Ok(view(app, control, revision))
}

#[utoipa::path(get, operation_id="runtime_settings", path="/api/v1/runtime-settings", responses((status=200, body=Vec<RuntimeSettingsView>)))]
pub(super) async fn list(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<Vec<RuntimeSettingsView>>, HttpError> {
    let rows = app.runtime_settings.sections(&actor.principal).await?;
    Ok(Json(
        rows.into_iter()
            .map(|(section, value, revision)| runtime_view(&app, &section, value, revision))
            .collect(),
    ))
}

#[utoipa::path(get, operation_id="runtime_settings_section", path="/api/v1/runtime-settings/{section}", params(("section" = String, Path)), responses((status=200, body=RuntimeSettingsView)))]
pub(super) async fn get_section(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(section): Path<String>,
) -> Result<Json<RuntimeSettingsView>, HttpError> {
    valid_section(&section)?;
    let row = app
        .runtime_settings
        .section(&actor.principal, &section)
        .await?;
    Ok(Json(runtime_view(&app, &section, row.value, row.revision)))
}

#[utoipa::path(put, operation_id="runtime_settings_save", path="/api/v1/runtime-settings/{section}", params(("section" = String, Path)), request_body=RuntimeSettingsPatch, responses((status=200, body=RuntimeSettingsView)))]
pub(super) async fn save_section(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(section): Path<String>,
    Json(input): Json<RuntimeSettingsPatch>,
) -> Result<Json<RuntimeSettingsView>, HttpError> {
    actor.recent(&app).await?;
    validate_value(&app.config, &section, &input.value)?;
    let row = app
        .runtime_settings
        .save(
            &actor.principal,
            &section,
            Some(input.revision),
            input.value,
        )
        .await?;
    Ok(Json(runtime_view(&app, &section, row.value, row.revision)))
}

#[utoipa::path(post, operation_id="runtime_settings_reset", path="/api/v1/runtime-settings/{section}/reset", params(("section" = String, Path)), responses((status=200, body=RuntimeSettingsView)))]
pub(super) async fn reset_section(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(section): Path<String>,
) -> Result<Json<RuntimeSettingsView>, HttpError> {
    valid_section(&section)?;
    let value = app
        .runtime_defaults
        .get(&section)
        .cloned()
        .context("runtime settings default is unavailable")?;
    let row = app
        .runtime_settings
        .reset(&actor.principal, &section, value)
        .await?;
    Ok(Json(runtime_view(&app, &section, row.value, row.revision)))
}
