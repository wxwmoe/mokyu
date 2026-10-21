use super::Identity;
use crate::{app::App, http::HttpError};
use anyhow::Result;
use axum::{
    Json,
    extract::{Extension, State},
};
use serde::Serialize;
use serde_json::{Value, json};
use std::sync::Arc;
use utoipa::ToSchema;

#[derive(Serialize, ToSchema)]
pub(super) struct RuntimeCounter {
    key: String,
    value: String,
}
#[derive(Serialize, ToSchema)]
pub(super) struct ServiceStatus {
    nonce_alerts: Vec<crate::model::key_usage::NonceCapacity>,
    deployment_mode: String,
    current_node: Option<NodeContext>,
    version: &'static str,
    started_at: String,
    maintenance: bool,
    mode_mismatch: bool,
    recovery_required: bool,
    restart_required: bool,
    counters: Vec<RuntimeCounter>,
    backend_queues: Value,
    backend_operations: Value,
    inventory_as_of: Option<String>,
    inventory_error: Option<String>,
    inventory_stale: bool,
}
#[derive(Serialize, ToSchema)]
pub(super) struct NodeContext {
    id: Option<uuid::Uuid>,
    name: Option<String>,
    role: String,
}
fn text(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string())
}
#[utoipa::path(get,operation_id="service_status",path="/api/v1/system/status",responses((status=200,body=ServiceStatus)))]
pub(super) async fn status(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<ServiceStatus>, HttpError> {
    let access = app.authorization.admin(&actor.principal).await?;
    access.finish().await?;
    let raw = app.status().await?;
    let loaded_revisions = app
        .runtime_revisions
        .read()
        .map(|revisions| revisions.clone())
        .unwrap_or_default();
    let revisions = app.runtime_settings.revisions().await?;
    let restart_required = app
        .restart_required
        .load(std::sync::atomic::Ordering::Acquire)
        || revisions.into_iter().any(|(section, revision)| {
            loaded_revisions.get(&section).copied().unwrap_or_default() != revision
        });
    let mut counters = Vec::new();
    for key in [
        "db_pool_size",
        "db_pool_idle",
        "data_slots_available",
        "active_streams",
        "upload_slots_available",
        "read_slots_available",
        "backend_gets",
        "backend_puts",
        "backend_deletes",
        "backend_read_bytes",
        "backend_write_bytes",
    ] {
        counters.push(RuntimeCounter {
            key: key.into(),
            value: text(&raw[key]),
        });
    }
    for key in ["uptime_seconds", "gc_deleted", "gc_failures"] {
        counters.push(RuntimeCounter {
            key: key.into(),
            value: text(&raw["runtime"][key]),
        });
    }
    for (key, value) in raw["resources"].as_object().unwrap() {
        counters.push(RuntimeCounter {
            key: format!("resources.{key}"),
            value: text(value),
        });
    }
    for listener in ["s3", "web", "control"] {
        for key in ["started", "active", "failed"] {
            counters.push(RuntimeCounter {
                key: format!("{listener}.{key}"),
                value: text(&raw["runtime"]["http"][listener][key]),
            });
        }
    }
    counters.push(RuntimeCounter {
        key: "rss_bytes".into(),
        value: text(&raw["process_memory"]["rss_bytes"]),
    });
    for (group, values) in raw["io"]["diagnostics"].as_object().into_iter().flatten() {
        if let Some(values) = values.as_object() {
            for (key, value) in values {
                counters.push(RuntimeCounter {
                    key: format!("storage.{group}.{key}"),
                    value: text(value),
                });
            }
        } else {
            counters.push(RuntimeCounter {
                key: format!("storage.{group}"),
                value: text(values),
            });
        }
    }
    let nonce_alerts = app
        .managed_keys
        .nonce_status()
        .await?
        .into_iter()
        .filter(|row| (row.storage_count > 0 || row.pending) && row.state != "normal")
        .collect();
    Ok(Json(ServiceStatus {
        nonce_alerts,
        deployment_mode: text(&raw["deployment_mode"]),
        current_node: (raw["deployment_mode"] == "cluster").then(|| NodeContext {
            id: app.node.get().map(|node| node.node_id),
            name: app.config.instance.name.clone(),
            role: app.role.to_owned(),
        }),
        version: env!("CARGO_PKG_VERSION"),
        started_at: text(&raw["runtime"]["started_at"]),
        maintenance: raw["maintenance"] == true,
        mode_mismatch: raw["mode_mismatch"] == true,
        recovery_required: app.recovery_required(),
        restart_required,
        counters,
        backend_queues: raw["io"]["backend_queues"].clone(),
        backend_operations: raw["io"]["backend"].clone(),
        inventory_as_of: raw["storage"]["snapshot"]["as_of"]
            .as_str()
            .map(str::to_owned),
        inventory_error: raw["storage"]["last_error"].as_str().map(str::to_owned),
        inventory_stale: raw["storage"]["stale"] == true,
    }))
}
pub(crate) use crate::model::runtime::ConfigField;
#[utoipa::path(get,operation_id="service_config",path="/api/v1/system/config",responses((status=200,body=Vec<ConfigField>)))]
pub(super) async fn config(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<Vec<ConfigField>>, HttpError> {
    let access = app.authorization.admin(&actor.principal).await?;
    access.finish().await?;
    let fields = config_fields(&app, app.runtime_settings.inspect().await?)?;
    Ok(Json(fields))
}

pub(crate) fn loaded_config(app: &App) -> Result<Vec<ConfigField>> {
    let revisions = app.runtime_revisions.read().unwrap();
    let values = crate::model::runtime_defaults(&app.config)
        .into_iter()
        .map(|(section, value)| {
            (
                section.to_owned(),
                value,
                revisions.get(section).copied().unwrap_or_default(),
                false,
            )
        })
        .collect();
    drop(revisions);
    config_fields(app, values)
}

fn config_fields(app: &App, runtime: Vec<(String, Value, i64, bool)>) -> Result<Vec<ConfigField>> {
    let c = &app.config;
    let d = &c.database;
    // Only current file sections and database-backed runtime sections are serialized. Secret-bearing fields use an allowlist.
    let mut sections = serde_json::Map::new();
    let mut instance = serde_json::to_value(&c.instance)?;
    instance.as_object_mut().unwrap().retain(|key, _| {
        matches!(
            key.as_str(),
            "name"
                | "listen_control"
                | "listen_web"
                | "listen_s3"
                | "data_path"
                | "min_free_space"
                | "multipart_limit"
                | "cache_size"
                | "cache_entries"
                | "response_cache_size"
                | "upload_cache_size"
                | "thumbnail_cache_size"
                | "request_connections"
                | "database_connections"
                | "database_read_connections"
                | "cpu_jobs"
                | "stream_memory_budget"
                | "storage_read_slots"
                | "storage_upload_slots"
                | "storage_control_slots"
                | "compression_idle_timeout"
        )
    });
    instance["deployment_key"] = json!("[redacted]");
    sections.insert("instance".into(), instance);
    let database = if d.backend == crate::config::DatabaseType::Sqlite {
        json!({"type":d.backend.as_str(),"path":d.sqlite_path(&c.instance.data_path)})
    } else {
        json!({"type":d.backend.as_str(),"host":d.host,"port":d.effective_port(),"name":d.name,"user":d.user,"password":"[redacted]","password_file":"[redacted]","ssl_mode":d.ssl_mode})
    };
    sections.insert("database".into(), database);
    if let Some(replica) = &c.replica_database {
        let d = replica.resolve(d)?;
        sections.insert("replica_database".into(), json!({"host":d.host,"port":d.effective_port(),"name":d.name,"user":d.user,"password":"[redacted]","password_file":"[redacted]","ssl_mode":d.ssl_mode}));
    }
    let loaded_revisions = app
        .runtime_revisions
        .read()
        .map(|revisions| revisions.clone())
        .unwrap_or_default();
    let mut runtime_restart = std::collections::HashMap::new();
    let mut runtime_defaults = std::collections::HashMap::new();
    for (section, value, revision, is_default) in runtime {
        runtime_restart.insert(
            section.clone(),
            loaded_revisions.get(&section).copied().unwrap_or_default() != revision,
        );
        runtime_defaults.insert(section.clone(), is_default);
        sections.insert(section, value);
    }
    let runtime_sections: std::collections::HashSet<&str> = sections
        .keys()
        .filter(|key| !matches!(key.as_str(), "instance" | "database" | "replica_database"))
        .map(String::as_str)
        .collect();
    let mut fields = Vec::new();
    for (section, values) in &sections {
        for (field, value) in values.as_object().unwrap() {
            let key = format!("{section}.{field}");
            let (origin, value_mode) = if runtime_sections.contains(section.as_str()) {
                if runtime_defaults.get(section).copied().unwrap_or(false) {
                    if c.configured_fields.contains(&key) {
                        ("config", "set")
                    } else {
                        ("database", "default")
                    }
                } else {
                    ("database", "set")
                }
            } else if c.configured_fields.contains(&key) {
                ("config", "set")
            } else if section == "replica_database"
                && matches!(field.as_str(), "port" | "name" | "user" | "ssl_mode")
            {
                ("config", "inherited")
            } else {
                ("config", "default")
            };
            fields.push(ConfigField {
                value_state: None,
                key,
                value: text(value),
                origin: origin.into(),
                value_mode: value_mode.into(),
                restart: runtime_restart
                    .get(section.as_str())
                    .copied()
                    .unwrap_or(true),
            });
        }
    }
    for (key, value) in serde_json::to_value(&app.budget)?.as_object().unwrap() {
        fields.push(ConfigField {
            value_state: None,
            key: format!("effective.{key}"),
            value: text(value),
            origin: "config".into(),
            value_mode: "budget".into(),
            restart: true,
        });
    }
    fields.push(ConfigField {
        value_state: None,
        key: "pack.creation_enabled".into(),
        value: app.pack_creation_allowed().to_string(),
        origin: "database".into(),
        value_mode: "set".into(),
        restart: false,
    });
    fields.extend(environment(app));
    Ok(fields)
}

fn observed(key: &str, value: Value, missing: &'static str) -> ConfigField {
    ConfigField {
        key: key.into(),
        value_state: value.is_null().then(|| missing.to_owned()),
        value: if value.is_null() {
            String::new()
        } else {
            text(&value)
        },
        origin: "environment".into(),
        value_mode: "read".into(),
        restart: false,
    }
}

fn environment(app: &App) -> Vec<ConfigField> {
    let mut fields = Vec::new();
    let info = serde_json::to_value(app.runtime.database_info()).unwrap_or(Value::Null);
    for key in ["program", "version", "schema_version"] {
        fields.push(observed(
            &format!("database.{key}"),
            info[key].clone(),
            "unavailable",
        ));
    }
    for key in ["journal_mode", "synchronous"] {
        if !info[key].is_null() {
            fields.push(observed(
                &format!("database.{key}"),
                info[key].clone(),
                "unavailable",
            ));
        }
    }
    let e = &app.budget.environment;
    for (key, value) in [
        ("version", json!(env!("CARGO_PKG_VERSION"))),
        ("os", json!(std::env::consts::OS)),
        ("arch", json!(std::env::consts::ARCH)),
        ("available_cpus", json!(e.available_cpus)),
        (
            "memory_available_at_start",
            json!(e.memory_available_at_start),
        ),
    ] {
        fields.push(observed(&format!("runtime.{key}"), value, "unavailable"));
    }
    fields.push(observed(
        "runtime.cpu_quota",
        json!(e.cpu_quota),
        if e.cpu_quota_read {
            "unlimited"
        } else {
            "unavailable"
        },
    ));
    fields.push(observed(
        "runtime.memory_limit",
        json!(e.memory_limit),
        if e.memory_limit_read {
            "unlimited"
        } else {
            "unavailable"
        },
    ));
    if app.config.replica_database.is_some() {
        let replica = app.runtime.read_replica();
        let missing = if replica.is_some() {
            "not_read"
        } else {
            "not_applicable"
        };
        let replica = replica.unwrap_or(Value::Null);
        for key in ["program", "version", "schema_version", "observed_at"] {
            fields.push(observed(
                &format!("replica_database.{key}"),
                replica["database"][key].clone(),
                missing,
            ));
        }
    }
    fields
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observations_distinguish_zero_and_missing() {
        let zero = observed("runtime.cpu_quota", json!(0), "unavailable");
        assert_eq!(zero.value, "0");
        assert_eq!(zero.value_state, None);
        let missing = observed("runtime.cpu_quota", Value::Null, "unlimited");
        assert_eq!(missing.value, "");
        assert_eq!(missing.value_state.as_deref(), Some("unlimited"));
        assert_eq!(missing.origin, "environment");
        assert!(!missing.restart);
    }
}
