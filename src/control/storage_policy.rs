use super::Identity;
use crate::{app::App, http::HttpError, storage::profile::StorageProfile};
use anyhow::{Context, Result, ensure};
use axum::{
    Json,
    extract::{Extension, Path, Query, State},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};
use utoipa::{IntoParams, ToSchema};

#[derive(Serialize, ToSchema)]
pub(crate) struct IoPreset {
    name: String,
    values: BTreeMap<String, Value>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct IoConstraints {
    single: bool,
    database: Option<String>,
    sqlite_synchronous: Option<String>,
    write_memory_bytes: Option<u64>,
    response_cache_bytes: Option<u64>,
    shutdown_flush_timeout: String,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct IoPresets {
    presets: Vec<IoPreset>,
    selected: Option<String>,
    constraints: IoConstraints,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct ProfileChange {
    field: String,
    before: Value,
    after: Value,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct IoEffects {
    write_ack: String,
    public_window_seconds: u64,
    source_window_seconds: u64,
    read_integrity: String,
    warnings: Vec<String>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct ProfilePreview {
    pub profile: StorageProfile,
    changes: Vec<ProfileChange>,
    selected: Option<String>,
    effects: IoEffects,
    constraints: IoConstraints,
}

fn templates(local: bool, single: bool) -> Vec<IoPreset> {
    ["reliable", "cached", "throughput"]
        .into_iter()
        .map(|name| {
            let fast = name == "throughput";
            let values = json!({
                "public_cache_validation": if name == "reliable" { "strict" } else { "background" },
                "public_cache_refresh_interval": "1s",
                "public_cache_max_age": if fast { "30s" } else { "5s" },
                "read_source_validation": if fast { "background" } else { "strict" },
                "source_validation_max_age": if fast { "30s" } else { "5s" },
                "read_integrity": if fast { "background" } else { "inline" },
                "upload_cache": !local,
                "write_processing": if fast { "background" } else { "inline" },
                "write_flush_interval": if fast { "1s" } else { "0s" },
                "write_backlog_max_age": "300s",
                "upload_cache_overflow": "fallback",
                "background_scheduling": if name == "reliable" { "balanced" } else { "foreground" },
            });
            let mut values: BTreeMap<_, _> =
                values.as_object().unwrap().clone().into_iter().collect();
            if single {
                values.insert(
                    "write_durability".into(),
                    json!(if !fast {
                        "durable"
                    } else if local {
                        "buffered"
                    } else {
                        "volatile"
                    }),
                );
            }
            IoPreset {
                name: name.into(),
                values,
            }
        })
        .collect()
}

fn selected(presets: &[IoPreset], profile: &Value) -> Option<String> {
    presets
        .iter()
        .find(|preset| {
            preset
                .values
                .iter()
                .all(|(key, value)| profile.get(key) == Some(value))
        })
        .map(|preset| preset.name.clone())
}

fn constraints(app: &App) -> IoConstraints {
    let info = app.runtime.database_info();
    let single = app.role == "single";
    IoConstraints {
        single,
        database: info.map(|info| info.program.to_owned()),
        sqlite_synchronous: info.and_then(|info| info.synchronous.clone()),
        // Control-node budgets do not describe data-node capacity.
        write_memory_bytes: single.then_some(app.budget.write_memory_bytes),
        response_cache_bytes: single.then_some(app.budget.response_cache_bytes),
        shutdown_flush_timeout: app.config.io.shutdown_flush_timeout.clone(),
    }
}

fn catalog(app: &App, storage_type: &str, profile: Option<&StorageProfile>) -> Result<IoPresets> {
    ensure!(
        matches!(storage_type, "local" | "s3"),
        s3s::s3_error!(InvalidArgument, "invalid storage type")
    );
    ensure!(
        app.role == "single" || storage_type == "s3",
        s3s::s3_error!(InvalidArgument, "local storage requires single mode")
    );
    let presets = templates(storage_type == "local", app.role == "single");
    let selected = profile
        .map(serde_json::to_value)
        .transpose()?
        .and_then(|profile| selected(&presets, &profile));
    Ok(IoPresets {
        presets,
        selected,
        constraints: constraints(app),
    })
}

pub(crate) async fn named(app: &App, name: &str) -> Result<IoPresets> {
    let record = app
        .storage_records
        .record(name)
        .await?
        .context("storage was not found")?;
    let profile = app.storage_profiles.load(record.id).await?;
    catalog(app, &record.r#type, Some(&profile))
}

pub(crate) async fn preset(app: &App, name: &str, preset: &str) -> Result<ProfilePreview> {
    let record = app
        .storage_records
        .record(name)
        .await?
        .context("storage was not found")?;
    let profile = app.storage_profiles.load(record.id).await?;
    let presets = catalog(app, &record.r#type, Some(&profile))?;
    let preset = presets
        .presets
        .into_iter()
        .find(|entry| entry.name == preset)
        .ok_or_else(|| s3s::s3_error!(InvalidArgument, "unknown I/O preset"))?;
    let mut document = serde_json::to_value(profile)?;
    document.as_object_mut().unwrap().extend(preset.values);
    preview(app, name, serde_json::from_value(document)?).await
}

pub(crate) async fn preview(
    app: &App,
    name: &str,
    profile: StorageProfile,
) -> Result<ProfilePreview> {
    let record = app
        .storage_records
        .record(name)
        .await?
        .context("storage was not found")?;
    let current = app.storage_profiles.load(record.id).await?;
    ensure!(
        profile.storage_id == record.id,
        s3s::s3_error!(InvalidArgument, "profile belongs to another storage")
    );
    ensure!(
        profile.revision == current.revision,
        crate::model::Error::StorageProfileConflict
    );
    crate::storage::profile::validate(&profile, &record.r#type)?;
    super::storage::validate_profile_capacity(app, &profile).await?;
    let before = serde_json::to_value(current)?;
    let after = serde_json::to_value(&profile)?;
    let changes = after
        .as_object()
        .unwrap()
        .iter()
        .filter(|(key, value)| before.get(*key) != Some(*value))
        .map(|(field, after)| ProfileChange {
            field: field.clone(),
            before: before[field].clone(),
            after: after.clone(),
        })
        .collect();
    let catalog = catalog(app, &record.r#type, Some(&profile))?;
    let mut warnings = Vec::new();
    if profile.write_durability.as_deref() == Some("volatile") && app.budget.write_memory_bytes == 0
    {
        warnings.push("write_memory_disabled".into());
    }
    if catalog
        .constraints
        .sqlite_synchronous
        .as_deref()
        .is_some_and(|mode| !mode.eq_ignore_ascii_case("full"))
    {
        warnings.push("sqlite_weak".into());
    }
    let write_ack = if record.r#type == "s3" && !profile.upload_cache {
        "remote".into()
    } else {
        format!(
            "{}_{}",
            if record.r#type == "local" {
                "local"
            } else {
                "cache"
            },
            profile.write_durability.as_deref().unwrap_or("durable")
        )
    };
    let effects = IoEffects {
        write_ack,
        public_window_seconds: if profile.public_cache_validation == "background" {
            crate::config::seconds(&profile.public_cache_max_age)?
        } else {
            0
        },
        source_window_seconds: if profile.read_source_validation == "background" {
            crate::config::seconds(&profile.source_validation_max_age)?
        } else {
            0
        },
        read_integrity: profile.read_integrity.clone(),
        warnings,
    };
    Ok(ProfilePreview {
        profile,
        changes,
        selected: catalog.selected,
        constraints: catalog.constraints,
        effects,
    })
}

#[derive(Deserialize, IntoParams)]
pub(crate) struct PresetQuery {
    #[serde(rename = "type")]
    pub storage_type: String,
}

#[utoipa::path(get, operation_id="storage_preset_templates", path="/api/v1/storage-profile-presets", params(PresetQuery), responses((status=200, body=IoPresets)))]
pub(crate) async fn templates_route(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Query(query): Query<PresetQuery>,
) -> Result<Json<IoPresets>, HttpError> {
    app.authorization
        .admin(&actor.principal)
        .await?
        .finish()
        .await?;
    Ok(Json(catalog(&app, &query.storage_type, None)?))
}

#[utoipa::path(get, operation_id="storage_profile_presets", path="/api/v1/storages/{name}/profile/presets", params(("name"=String, Path)), responses((status=200, body=IoPresets)))]
pub(crate) async fn named_route(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(name): Path<String>,
) -> Result<Json<IoPresets>, HttpError> {
    app.authorization
        .admin(&actor.principal)
        .await?
        .finish()
        .await?;
    Ok(Json(named(&app, &name).await?))
}

#[utoipa::path(post, operation_id="storage_profile_preview", path="/api/v1/storages/{name}/profile/preview", params(("name"=String, Path)), request_body=StorageProfile, responses((status=200, body=ProfilePreview), (status=409, body=super::contract::ErrorBody)))]
pub(crate) async fn preview_route(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(name): Path<String>,
    Json(profile): Json<StorageProfile>,
) -> Result<Json<ProfilePreview>, HttpError> {
    app.authorization
        .admin(&actor.principal)
        .await?
        .finish()
        .await?;
    Ok(Json(preview(&app, &name, profile).await?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn presets_only_change_applicable_io_fields_and_match_exactly() {
        for (local, single) in [(true, true), (false, true), (false, false)] {
            let presets = templates(local, single);
            assert_eq!(presets.len(), 3);
            for preset in &presets {
                assert_eq!(preset.values.len(), if single { 13 } else { 12 });
                for field in [
                    "storage_id",
                    "revision",
                    "cdc_enabled",
                    "deduplication_enabled",
                    "compression_enabled",
                    "pack_enabled",
                    "cache_enabled",
                    "sqlite_synchronous",
                    "cache_soft_limit",
                ] {
                    assert!(!preset.values.contains_key(field));
                }
                assert_eq!(preset.values["upload_cache"], !local);
                let mut document =
                    json!({"cdc_enabled": false, "compression_enabled": false, "unrelated": true});
                document
                    .as_object_mut()
                    .unwrap()
                    .extend(preset.values.clone());
                assert_eq!(
                    selected(&presets, &document).as_deref(),
                    Some(preset.name.as_str())
                );
                document["read_integrity"] = json!("off");
                assert!(selected(&presets, &document).is_none());
            }
        }
    }
}
