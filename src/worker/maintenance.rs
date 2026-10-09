use crate::{app::App, config};
use anyhow::Result;
use serde_json::{Value, json};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use uuid::Uuid;

impl App {
    pub(crate) fn pack_creation_allowed(&self) -> bool {
        self.config.local_pack.enabled && !self.pack_creation_paused.load(Ordering::Acquire)
    }
    pub async fn pack_creation_change(&self, paused: bool) -> Result<Value> {
        let _coord = self.coord.lock().await;
        if !paused {
            let unpacking:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE task_type='unpack' AND detail->>'all'='true' AND state<>'completed')").fetch_one(&self.db).await?;
            if unpacking {
                return Err(s3s::s3_error!(
                    OperationAborted,
                    "finish all-pack unpacking before enabling new packs"
                )
                .into());
            }
        }
        let mut tx = self.db.begin().await?;
        sqlx::query("UPDATE mokyu_meta SET pack_creation_paused=$1")
            .bind(paused)
            .execute(&mut *tx)
            .await?;
        crate::control::audit::checkpoint(
            &mut tx,
            "maintenance.pack_creation",
            "pack",
            json!({"paused":paused}),
        )
        .await?;
        tx.commit().await?;
        self.pack_creation_paused.store(paused, Ordering::Release);
        self.wake_tasks.notify_waiters();
        Ok(json!({"paused":paused,"enabled":self.pack_creation_allowed()}))
    }
    pub(crate) fn maintenance_schedule(&self) -> [(&'static str, &str, bool); 7] {
        let p = &self.config.local_pack;
        [
            ("pack", &p.interval, self.pack_creation_allowed()),
            ("reuse", &p.reuse_interval, true),
            ("reclaim", &p.reclaim_interval, true),
            ("repack", &p.repack_interval, self.pack_creation_allowed()),
            ("range", &p.range_interval, p.range_optimization),
            ("gc", &self.config.maintenance.interval, true),
            ("cleanup", &self.config.maintenance.interval, true),
        ]
    }

    pub(crate) async fn maintenance_paused(&self, kind: &str) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT CASE WHEN c.kind='gc' THEN m.gc_paused ELSE c.paused END FROM maintenance_controls c CROSS JOIN mokyu_meta m WHERE c.kind=$1")
            .bind(kind).fetch_one(&self.db).await?)
    }

    pub async fn maintenance_status(&self) -> Result<Value> {
        let mut rows: Vec<Value> = sqlx::query_scalar("SELECT to_jsonb(c)||jsonb_build_object('paused',CASE WHEN c.kind='gc' THEN m.gc_paused ELSE c.paused END,'latest',latest.value,'active',active.value) FROM maintenance_controls c CROSS JOIN mokyu_meta m LEFT JOIN LATERAL (SELECT to_jsonb(t) value FROM tasks t WHERE coalesce(t.detail->>'kind',t.task_type)=c.kind ORDER BY created_at DESC,id DESC LIMIT 1) latest ON true LEFT JOIN LATERAL (SELECT to_jsonb(t) value FROM tasks t WHERE coalesce(t.detail->>'kind',t.task_type)=c.kind AND t.state IN ('queued','running','paused') ORDER BY created_at,id LIMIT 1) active ON true ORDER BY c.kind")
            .fetch_all(&self.db).await?;
        let global = self.maintenance.load(Ordering::Acquire);
        for row in &mut rows {
            let (_, interval, enabled) = self
                .maintenance_schedule()
                .into_iter()
                .find(|(kind, _, _)| row["kind"] == *kind)
                .unwrap();
            let seconds = config::seconds(interval)?;
            row["enabled"] = json!(enabled);
            row["interval_seconds"] = json!(seconds);
            row["next_run_at"] =
                if !enabled || (global && row["kind"] != "cleanup") || row["paused"] == true {
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
        let preparing: String =
            sqlx::query_scalar("SELECT count(*)::text FROM packs WHERE state='preparing'")
                .fetch_one(&self.db)
                .await?;
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
            json!({"maintenance":global,"controls":rows,"concurrency":self.config.local_pack.maintenance_concurrency,
            "pack_configured":self.config.local_pack.enabled,"pack_creation_paused":self.pack_creation_paused.load(Ordering::Acquire),
            "pack_creation_enabled":self.pack_creation_allowed(),"preparing_packs":preparing,"active_operations":self.active.lock().unwrap().len(),
            "backend_prefixes":backend_prefixes,"min_storage_duration":self.config.backend_defaults.min_storage_duration}),
        )
    }

    pub async fn maintenance_change(&self, kind: &str, paused: bool) -> Result<Value> {
        if !self
            .maintenance_schedule()
            .iter()
            .any(|(k, _, _)| *k == kind)
        {
            return Err(s3s::s3_error!(InvalidArgument, "unknown maintenance kind").into());
        }
        let _coord = self.coord.lock().await;
        let mut tx = self.db.begin().await?;
        if kind == "gc" {
            sqlx::query("UPDATE mokyu_meta SET gc_paused=$1")
                .bind(paused)
                .execute(&mut *tx)
                .await?;
        }
        sqlx::query("UPDATE maintenance_controls SET paused=$2,updated_at=now() WHERE kind=$1")
            .bind(kind)
            .bind(paused)
            .execute(&mut *tx)
            .await?;
        crate::control::audit::checkpoint(
            &mut tx,
            "maintenance.policy",
            kind,
            json!({"paused":paused}),
        )
        .await?;
        tx.commit().await?;
        self.wake_tasks.notify_waiters();
        Ok(json!({"kind":kind,"paused":paused}))
    }

    pub async fn maintenance_start(&self, kind: &str) -> Result<Value> {
        if !matches!(kind, "gc" | "cleanup") {
            return self.pack_start(kind).await;
        }
        let _coord = self.coord.lock().await;
        if kind != "cleanup" {
            self.writable()?;
        }
        if self.maintenance_paused(kind).await? {
            return Err(s3s::s3_error!(
                OperationAborted,
                "resume this maintenance kind before starting it"
            )
            .into());
        }
        let mut tx = self.db.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!("mokyu:maintenance:{kind}"))
            .execute(&mut *tx)
            .await?;
        if let Some(id) = sqlx::query_scalar::<_, Uuid>("SELECT id FROM tasks WHERE task_type=$1 AND state IN ('queued','running','paused') ORDER BY created_at LIMIT 1")
            .bind(kind).fetch_optional(&mut *tx).await? {
            tx.commit().await?;
            return Ok(json!({"task_id":id,"existing":true}));
        }
        let storages: Vec<(Uuid, String, i64)> = sqlx::query_as(
            "SELECT s.id,s.type,sp.revision FROM storages s JOIN storage_profiles sp ON sp.storage_id=s.id WHERE s.enabled ORDER BY s.created_at,s.id",
        )
        .fetch_all(&mut *tx)
        .await?;
        let children = storages.len() + usize::from(kind == "cleanup");
        let id = crate::worker::insert(
            &mut tx,
            kind,
            None,
            json!({"role":"parent","children":children}),
        )
        .await?;
        if kind == "cleanup" {
            let child = crate::worker::insert_child(
                &mut tx,
                kind,
                None,
                json!({"role":"child","phase":"history"}),
            )
            .await?;
            sqlx::query("UPDATE tasks SET parent_task_id=$2,batch_total=1,execution_scope='cluster' WHERE id=$1")
                .bind(child)
                .bind(id)
                .execute(&mut *tx)
                .await?;
        }
        for (storage_id, storage_type, revision) in &storages {
            let child = crate::worker::insert_child(
                &mut tx,
                kind,
                None,
                json!({"role":"child","storage_id":storage_id,"profile_revision":revision}),
            )
            .await?;
            let (scope, node) = if storage_type == "local" {
                ("node", self.node.get().map(|state| state.node_id))
            } else {
                ("cluster", None)
            };
            sqlx::query("UPDATE tasks SET parent_task_id=$2,batch_total=1,execution_scope=$3,target_node_id=$4 WHERE id=$1")
                .bind(child)
                .bind(id)
                .bind(scope)
                .bind(node)
                .execute(&mut *tx)
                .await?;
        }
        sqlx::query("UPDATE tasks SET batch_total=$2 WHERE id=$1")
            .bind(id)
            .bind(children as i64)
            .execute(&mut *tx)
            .await?;
        if children == 0 {
            sqlx::query("UPDATE tasks SET state='completed',updated_at=now() WHERE id=$1")
                .bind(id)
                .execute(&mut *tx)
                .await?;
        }
        sqlx::query("UPDATE maintenance_controls SET last_scheduled_at=now() WHERE kind=$1")
            .bind(kind)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        self.wake_tasks.notify_one();
        Ok(json!({"task_id":id}))
    }

    pub(crate) async fn lifecycle_batch(
        &self,
        id: Uuid,
        kind: &str,
        detail: &Value,
    ) -> Result<bool> {
        let scoped_storage = detail["storage_id"]
            .as_str()
            .and_then(|value| value.parse::<Uuid>().ok());
        let profile = if let Some(storage_id) = scoped_storage {
            Some(crate::storage::profile::StorageProfile::load(&self.db, storage_id).await?)
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
        let batch = profile
            .as_ref()
            .and_then(|value| value.maintenance_concurrency)
            .unwrap_or(self.config.maintenance.batch_size as i32)
            .max(1) as usize;
        let maintenance_byte_budget = profile
            .as_ref()
            .and_then(|value| value.maintenance_byte_budget.as_deref())
            .map(config::bytes)
            .transpose()?;
        let (count, again, report) = if kind == "gc" {
            if self.maintenance.load(Ordering::Acquire) || self.maintenance_paused(kind).await? {
                return Ok(false);
            }
            let cleaned = if let Some(storage_id) = scoped_storage {
                self.cleanup_scoped(
                    storage_id,
                    batch,
                    profile
                        .as_ref()
                        .map(|value| config::seconds(&value.upload_retention))
                        .transpose()?
                        .unwrap_or(config::seconds(&self.config.maintenance.upload_retention)?),
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
                json!({"storage_id":storage_id,"deleted":self.cleanup_scoped(storage_id,batch,profile.as_ref().map(|value| config::seconds(&value.deleted_chunk_retention)).transpose()?.unwrap_or(config::seconds(&self.config.maintenance.deleted_chunk_retention)?)).await?})
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
        sqlx::query("UPDATE tasks SET processed=processed+$2,state=CASE WHEN $3 THEN 'queued' ELSE 'completed' END,lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,detail=detail||jsonb_build_object('report',$4::jsonb),updated_at=now() WHERE id=$1 AND state='running'")
            .bind(id).bind(count).bind(again).bind(report).execute(&self.db).await?;
        Ok(again)
    }
}

pub async fn run(app: Arc<App>) -> Result<()> {
    const LEADER_KEY: i64 = 734922709851034;
    loop {
        let Some(leader) = crate::cluster::try_leader(&app, LEADER_KEY).await? else {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        };
        {
            let controls: Vec<(String, bool, Option<f64>)> = sqlx::query_as("SELECT c.kind,CASE WHEN c.kind='gc' THEN m.gc_paused ELSE c.paused END,extract(epoch FROM now()-c.last_scheduled_at)::double precision FROM maintenance_controls c CROSS JOIN mokyu_meta m")
                .fetch_all(&app.db).await?;
            for (kind, interval, enabled) in app.maintenance_schedule() {
                let (_, paused, age) = controls.iter().find(|(k, _, _)| k == kind).unwrap();
                if !enabled
                    || *paused
                    || (kind != "cleanup" && app.maintenance.load(Ordering::Acquire))
                {
                    continue;
                }
                let due = age.is_none_or(|age| age >= config::seconds(interval).unwrap() as f64);
                let changed = kind == "reuse" && sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM pack_changes WHERE reason='reuse' AND next_check_at<=now())")
                    .fetch_one(&app.db).await?;
                if due || changed {
                    let result = if matches!(kind, "gc" | "cleanup") {
                        app.maintenance_start(kind).await
                    } else {
                        app.start_pack(kind, true).await
                    };
                    if let Err(error) = result {
                        tracing::warn!(%error,kind,"maintenance scheduling deferred");
                    }
                    // Limit retries as well as successful periodic runs; explicit runs remain available.
                    sqlx::query(
                        "UPDATE maintenance_controls SET last_scheduled_at=now() WHERE kind=$1",
                    )
                    .bind(kind)
                    .execute(&app.db)
                    .await?;
                }
            }
        }
        leader.commit().await?;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
