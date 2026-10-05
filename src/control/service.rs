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
    // Only current file sections and database-backed runtime sections are serialized. Secret-bearing fields use an allowlist.
    let mut sections = serde_json::Map::new();
    let mut instance = serde_json::to_value(&c.instance)?;
    instance["deployment_key"] = json!("[redacted]");
    sections.insert("instance".into(), instance);
    sections.insert(
        "database".into(),
        json!({"host":d.host,"port":d.port,"name":d.name,"user":d.user,"password":"[redacted]","password_file":"[redacted]","ssl_mode":d.ssl_mode}),
    );
    let mut cluster = serde_json::to_value(&c.cluster)?;
    cluster["join_token"] = json!("[redacted]");
    sections.insert("cluster".into(), cluster);
    let runtime: Vec<(String, serde_json::Value)> = sqlx::query_as(
        "SELECT section,value FROM runtime_settings ORDER BY section",
    )
    .fetch_all(&mut *tx)
    .await?;
    for (section, value) in runtime {
        sections.insert(section, value);
    }
    let runtime_sections: std::collections::HashSet<&str> = sections
        .keys()
        .filter(|key| !matches!(key.as_str(), "instance" | "database" | "cluster"))
        .map(String::as_str)
        .collect();
    let mut fields = Vec::new();
    for (section, values) in &sections {
        for (field, value) in values.as_object().unwrap() {
            let key = format!("{section}.{field}");
            let source = if runtime_sections.contains(section.as_str()) {
                "database"
            } else if c.configured_fields.contains(&key) {
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
