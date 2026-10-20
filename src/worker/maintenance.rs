use crate::model::task::Execution;
use crate::{app::App, config};
use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use uuid::Uuid;

impl App {
    pub(crate) async fn refresh_maintenance(&self) -> Result<()> {
        let _coord = self.coord.lock().await;
        if self.draining.load(Ordering::Acquire) {
            return Ok(());
        }
        let state = self.runtime.state().await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("maintenance-refresh").await;
        self.maintenance.store(state.maintenance, Ordering::Release);
        Ok(())
    }

    pub(crate) fn pack_creation_allowed(&self) -> bool {
        !self.pack_creation_paused.load(Ordering::Acquire)
    }
    pub async fn pack_creation_change(&self, paused: bool) -> Result<Value> {
        let _coord = self.coord.lock().await;
        self.maintenance_store.pack_creation(paused).await?;
        self.pack_creation_paused.store(paused, Ordering::Release);
        self.wake_tasks.notify_waiters();
        let configured = self
            .storage_profiles
            .enabled_profiles()
            .await?
            .iter()
            .any(|profile| profile.pack_enabled);
        Ok(json!({"paused":paused,"enabled":configured && self.pack_creation_allowed()}))
    }
    pub(crate) async fn maintenance_schedule(&self) -> Result<Vec<(&'static str, u64, bool)>> {
        let profiles = self.storage_profiles.enabled_profiles().await?;
        let mut schedule = Vec::new();
        for policy in ["pack", "reuse", "reclaim", "repack", "range"] {
            let minimum = profiles
                .iter()
                .filter(|profile| match policy {
                    "pack" | "repack" => profile.pack_enabled && self.pack_creation_allowed(),
                    "range" => profile.pack_range_enabled,
                    _ => true,
                })
                .map(|profile| profile.maintenance_interval(policy))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .min();
            schedule.push((policy, minimum.unwrap_or(0), minimum.is_some()));
        }
        let interval = config::seconds(&self.config.maintenance.interval)?;
        schedule.extend([("gc", interval, true), ("cleanup", interval, true)]);
        Ok(schedule)
    }

    pub(crate) async fn maintenance_paused(&self, policy: &str) -> Result<bool> {
        self.maintenance_store.policy_paused(policy).await
    }

    pub async fn maintenance_status(&self) -> Result<Value> {
        let mut rows: Vec<Value> = self.maintenance_store.controls().await?;
        let global = self.maintenance.load(Ordering::Acquire);
        let schedule = self.maintenance_schedule().await?;
        let profiles = self.storage_profiles.enabled_profiles().await?;
        let configured = profiles.iter().any(|profile| profile.pack_enabled);
        for row in &mut rows {
            let (_, seconds, enabled) = schedule
                .iter()
                .copied()
                .find(|(policy, _, _)| row["policy"] == *policy)
                .unwrap();
            row["enabled"] = json!(enabled);
            row["interval_seconds"] = json!(seconds);
            row["next_run_at"] =
                if !enabled || (global && row["policy"] != "cleanup") || row["paused"] == true {
                    Value::Null
                } else {
                    let last = row["last_scheduled_at"]
                        .as_str()
                        .and_then(|v| chrono::DateTime::parse_from_rfc3339(v).ok())
                        .map(|v| v.with_timezone(&chrono::Utc));
                    json!(last.and_then(|v| {
                        v.checked_add_signed(chrono::TimeDelta::seconds(seconds as i64))
                    }))
                };
        }
        let preparing: String = self.maintenance_store.preparing_packs().await?;
        let backend_prefixes: Vec<Value> = self
            .storage
            .storage_ids()
            .into_iter()
            .filter_map(|id| {
                self.storage
                    .namespace_for(id)
                    .ok()
                    .map(|prefix| json!({"id": id, "prefix": prefix}))
            })
            .collect();
        Ok(
            json!({"maintenance":global,"controls":rows,"concurrency":self.budget.cpu_jobs.clamp(1, 4),
            "pack_configured":configured,"pack_creation_paused":self.pack_creation_paused.load(Ordering::Acquire),
            "pack_creation_enabled":configured && self.pack_creation_allowed(),"preparing_packs":preparing,"active_operations":self.active_operations(),
            "backend_prefixes":backend_prefixes,"profiles":profiles}),
        )
    }

    pub async fn maintenance_change(&self, policy: &str, paused: bool) -> Result<Value> {
        if !self
            .maintenance_schedule()
            .await?
            .iter()
            .any(|(k, _, _)| *k == policy)
        {
            return Err(s3s::s3_error!(InvalidArgument, "unknown maintenance policy").into());
        }
        let _coord = self.coord.lock().await;
        self.maintenance_store.change_policy(policy, paused).await?;
        self.wake_tasks.notify_waiters();
        Ok(json!({"policy":policy,"paused":paused}))
    }

    pub async fn maintenance_start(&self, policy: &str) -> Result<Value> {
        if !matches!(policy, "gc" | "cleanup") {
            return self.pack_start(policy).await;
        }
        let _coord = self.coord.lock().await;
        self.ensure_mode()?;
        if policy != "cleanup" {
            self.writable()?;
        }
        if self.maintenance_paused(policy).await? {
            return Err(s3s::s3_error!(
                OperationAborted,
                "resume this maintenance policy before starting it"
            )
            .into());
        }
        let (id, existing) = self
            .maintenance_store
            .start_policy(policy, self.node.get().map(|node| node.node_id))
            .await?;
        if existing {
            return Ok(json!({"task_id":id,"existing":true}));
        }
        self.wake_tasks.notify_one();
        Ok(json!({"task_id":id}))
    }

    pub(crate) async fn lifecycle_batch(
        &self,
        id: Execution,
        policy: &str,
        detail: &Value,
    ) -> Result<bool> {
        let scoped_storage = detail["storage_id"]
            .as_str()
            .and_then(|value| value.parse::<Uuid>().ok());
        let profile = if let Some(storage_id) = scoped_storage {
            Some(self.storage_profiles.load(storage_id).await?)
        } else {
            None
        };
        let _storage_permit = if let Some(storage_id) = scoped_storage {
            Some(
                self.storage
                    .profile_permit(storage_id, "maintenance")
                    .await?,
            )
        } else {
            None
        };
        let batch = self.config.maintenance.batch_size as usize;
        let maintenance_byte_budget = profile
            .as_ref()
            .and_then(|value| value.maintenance_byte_budget.as_deref())
            .map(config::bytes)
            .transpose()?;
        let (count, again, report) = if policy == "gc" {
            if self.maintenance.load(Ordering::Acquire) || self.maintenance_paused(policy).await? {
                return Ok(false);
            }
            let cleaned = if let Some(storage_id) = scoped_storage {
                self.cleanup_scoped(
                    storage_id,
                    batch,
                    profile
                        .as_ref()
                        .context("missing scoped storage profile")
                        .and_then(|profile| config::seconds(&profile.upload_retention))?,
                )
                .await?
            } else {
                self.cleanup().await?
            };
            let (count, physical) = if let Some(profile) = profile.as_ref() {
                self.reclaim_scoped(
                    profile.storage_id,
                    batch,
                    config::seconds(&profile.unreferenced_grace)?,
                    maintenance_byte_budget.filter(|value| *value > 0),
                    profile.min_storage_seconds()?,
                )
                .await?
            } else {
                (self.reclaim().await?, 0)
            };
            (
                count as i64,
                count >= batch || physical >= batch || cleaned >= batch,
                json!({"local_cleaned":cleaned,"chunks_reclaimed":count,"physical_reclaimed":physical}),
            )
        } else {
            let report = if let Some(storage_id) = scoped_storage {
                json!({"storage_id":storage_id,"deleted":self.cleanup_scoped(storage_id,batch,config::seconds(&profile.as_ref().context("missing scoped storage profile")?.deleted_chunk_retention)?).await?})
            } else {
                self.cleanup_history().await?
            };
            let count = scoped_storage
                .and_then(|_| report["deleted"].as_i64())
                .or_else(|| {
                    report["last_run"]["deleted"]
                        .as_object()
                        .map(|v| v.values().filter_map(Value::as_i64).sum())
                })
                .unwrap_or(0);
            (
                count,
                scoped_storage.is_some_and(|_| count >= batch as i64),
                report,
            )
        };
        self.maintenance_store
            .finish_policy(id, count, again, report)
            .await?;
        Ok(again)
    }
}

pub async fn run(app: Arc<App>) -> Result<()> {
    const LEADER_KEY: i64 = 734922709851034;
    loop {
        let leader = if app.role == "single" {
            None
        } else if let Some(leader) = app.cluster()?.leader(LEADER_KEY).await? {
            Some(leader)
        } else {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        };
        {
            let controls: Vec<(String, bool, Option<f64>)> =
                app.maintenance_store.schedule().await?;
            for (policy, interval, enabled) in app.maintenance_schedule().await? {
                let (_, paused, age) = controls.iter().find(|(k, _, _)| k == policy).unwrap();
                if !enabled
                    || *paused
                    || (policy != "cleanup" && app.maintenance.load(Ordering::Acquire))
                {
                    continue;
                }
                let due = age.is_none_or(|age| age >= interval as f64);
                let changed = policy == "reuse" && app.maintenance_store.reuse_due().await?;
                if due || changed {
                    let result = if matches!(policy, "gc" | "cleanup") {
                        app.maintenance_start(policy).await
                    } else {
                        app.start_pack(policy, true).await
                    };
                    if let Err(error) = result {
                        tracing::warn!(%error,policy,"maintenance scheduling deferred");
                    }
                    // Limit retries as well as successful periodic runs; explicit runs remain available.
                    app.maintenance_store.scheduled(policy).await?;
                }
            }
        }
        if let Some(leader) = leader {
            leader.finish().await?;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
