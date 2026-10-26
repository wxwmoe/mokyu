use super::NodeContext;
use crate::{
    model::{
        node::{NodeReport, REPORT_FORMAT},
        runtime::ConfigField,
    },
    rpc::wire,
};
use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

fn text(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string())
}

impl NodeContext {
    fn metadata(&self, settings: Value) -> Result<Value> {
        let mut fields = Vec::new();
        let mut instance = crate::control::service::instance_config(&self.config.instance)?;
        for key in ["database_connections", "database_read_connections"] {
            instance.as_object_mut().unwrap().remove(key);
        }
        let mut sections = settings["loaded"]
            .as_object()
            .context("settings missing")?
            .clone();
        sections.insert("instance".into(), instance);
        for (section, values) in sections {
            let values =
                crate::model::node::applicable(&self.client.identity.role, &section, values);
            for (name, value) in values.as_object().context("settings section missing")? {
                let key = format!("{section}.{name}");
                let configured = self.config.configured_fields.contains(&key);
                fields.push(ConfigField {
                    key,
                    value: text(value),
                    value_state: None,
                    origin: if section == "instance" {
                        "config"
                    } else {
                        "database"
                    }
                    .into(),
                    value_mode: if section == "instance" && !configured {
                        "default"
                    } else {
                        "set"
                    }
                    .into(),
                    restart: section == "instance"
                        || (section == "requests"
                            && matches!(
                                name.as_str(),
                                "s3_domain"
                                    | "s3_region"
                                    | "s3_chunk_limit"
                                    | "cors_cache_size"
                                    | "cors_cache_entries"
                            )),
                });
            }
        }
        let budget = self.budget.get().context("budget missing")?;
        for (name, value) in serde_json::to_value(budget)?.as_object().unwrap() {
            fields.push(ConfigField {
                key: format!("effective.{name}"),
                value: text(value),
                origin: "config".into(),
                value_mode: "budget".into(),
                restart: true,
                value_state: None,
            });
        }
        fields.extend(crate::control::service::runtime_environment(budget));
        Ok(json!({"format": REPORT_FORMAT, "fields": fields,
            "loaded": settings["loaded"], "revisions": settings["revisions"]}))
    }

    pub(crate) async fn report(self: &Arc<Self>) -> Result<()> {
        let mut sequence = 0i64;
        let mut sent_metadata = Value::Null;
        let mut sampled = None::<Instant>;
        loop {
            // Reporting cannot occupy the lease stream or block request handling.
            let result = tokio::time::timeout(Duration::from_secs(2), async {
                let snapshot = self.background_snapshot()?;
                let metadata = self.metadata(snapshot.settings.view())?;
                let status = self.status().await;
                let interval =
                    crate::config::seconds(&snapshot.runtime().metrics.runtime_interval)?;
                let sample = sampled.is_none_or(|at| at.elapsed() >= Duration::from_secs(interval))
                    && status["cache"]["limit"].is_u64()
                    && status["cache"]["upload_limit"].is_u64();
                sequence = sequence
                    .checked_add(1)
                    .context("node report sequence exhausted")?;
                let report = NodeReport {
                    boot_id: self.boot,
                    sequence,
                    sampled_at: chrono::Utc::now(),
                    metadata: (sent_metadata != metadata).then(|| metadata.clone()),
                    status,
                };
                report.validate()?;
                if self
                    .client
                    .report(wire::NodeReportRequest {
                        run: Some(snapshot.lease.run.clone()),
                        report_json: serde_json::to_vec(&report)?,
                        sample_interval_seconds: if sample { interval } else { 0 },
                    })
                    .await?
                {
                    sent_metadata = metadata;
                    if sample {
                        sampled = Some(Instant::now());
                    }
                }
                Ok::<_, anyhow::Error>(())
            })
            .await;
            if !matches!(result, Ok(Ok(()))) {
                tracing::debug!("node report was not acknowledged");
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    }
}
