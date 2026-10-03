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
    version: &'static str,
    started_at: String,
    maintenance: bool,
    counters: Vec<RuntimeCounter>,
    backend_queues: Value,
    backend_operations: Value,
    catalog: Value,
    inventory_as_of: Option<String>,
    inventory_error: Option<String>,
    inventory_stale: bool,
}
fn text(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string())
}
#[utoipa::path(get,operation_id="service_status",path="/api/service/status",responses((status=200,body=ServiceStatus)))]
pub(super) async fn status(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<ServiceStatus>, HttpError> {
    let mut tx = app.db.begin().await?;
    actor.principal.lock_admin(&mut tx).await?;
    let raw = app.status().await?;
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
    for listener in ["s3", "web", "manage"] {
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
    tx.commit().await?;
    Ok(Json(ServiceStatus {
        version: env!("CARGO_PKG_VERSION"),
        started_at: text(&raw["runtime"]["started_at"]),
        maintenance: raw["maintenance"] == true,
        counters,
        backend_queues: raw["io"]["backend_queues"].clone(),
        backend_operations: raw["io"]["backend"].clone(),
        catalog: raw["catalog"].clone(),
        inventory_as_of: raw["storage"]["snapshot"]["as_of"]
            .as_str()
            .map(str::to_owned),
        inventory_error: raw["storage"]["last_error"].as_str().map(str::to_owned),
        inventory_stale: raw["storage"]["stale"] == true,
    }))
}
#[derive(Serialize, ToSchema)]
pub(super) struct ConfigField {
    key: String,
    value: String,
    source: &'static str,
    restart: bool,
}
#[utoipa::path(get,operation_id="service_config",path="/api/service/config",responses((status=200,body=Vec<ConfigField>)))]
pub(super) async fn config(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<Vec<ConfigField>>, HttpError> {
    let mut tx = app.db.begin().await?;
    actor.principal.lock_admin(&mut tx).await?;
    let c = &app.config;
    let d = &c.database;
    // Only explicitly selected non-secret sections are serialized. Secret-bearing sections use an allowlist.
    let sections = json!({"listen":c.listen,"manage":c.manage,"processing":c.processing,"multipart":c.multipart,"cache":c.cache,"compression":c.compression,"gc":c.gc,"cleanup":c.cleanup,"statistics":c.statistics,"integrity":c.integrity,"pack":c.pack,
        "encryption":{"algorithm":c.encryption.algorithm,"deployment_key":"[redacted]","keyring_file":"[redacted]"},
        "database":{"host":d.host,"port":d.port,"name":d.name,"user":d.user,"password":"[redacted]","password_file":"[redacted]","ssl_mode":d.ssl_mode,"max_connections":d.max_connections},
        "storage":{"data":c.storage.data,"free_space_floor":c.storage.free_space_floor,"read_concurrency":c.storage.read_concurrency,"upload_concurrency":c.storage.upload_concurrency,"control_concurrency":c.storage.control_concurrency,"connect_timeout":c.storage.connect_timeout,"request_timeout":c.storage.request_timeout,"max_retries":c.storage.max_retries,"retry_timeout":c.storage.retry_timeout,"priority_aging":c.storage.priority_aging,"min_storage_duration":c.storage.min_storage_duration}});
    let mut fields = Vec::new();
    for (section, values) in sections.as_object().unwrap() {
        for (field, value) in values.as_object().unwrap() {
            let key = format!("{section}.{field}");
            let source = if c.configured_fields.contains(&key) {
                "config"
            } else {
                "default"
            };
            fields.push(ConfigField {
                key,
                value: text(value),
                source,
                restart: true,
            });
        }
    }
    for (key, value) in serde_json::to_value(&app.budget)?.as_object().unwrap() {
        fields.push(ConfigField {
            key: format!("effective.{key}"),
            value: text(value),
            source: "computed",
            restart: true,
        });
    }
    fields.push(ConfigField {
        key: "pack.creation_enabled".into(),
        value: app.pack_creation_allowed().to_string(),
        source: "database",
        restart: false,
    });
    tx.commit().await?;
    Ok(Json(fields))
}
