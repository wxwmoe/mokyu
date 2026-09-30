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
        self.config.pack.enabled && !self.pack_creation_paused.load(Ordering::Acquire)
    }
    pub async fn pack_creation_change(&self, paused: bool) -> Result<Value> {
        let _coord = self.coord.lock().await;
        if !paused {
            let unpacking:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE kind='unpack' AND detail->>'all'='true' AND state<>'completed')").fetch_one(&self.db).await?;
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
        crate::manage::audit::checkpoint(
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
        let p = &self.config.pack;
        [
            ("pack", &p.interval, self.pack_creation_allowed()),
            ("reuse", &p.reuse_interval, true),
            ("reclaim", &p.reclaim_interval, true),
            ("repack", &p.repack_interval, self.pack_creation_allowed()),
            ("range", &p.range_interval, p.range_optimization),
            ("gc", &self.config.gc.interval, true),
            ("cleanup", &self.config.cleanup.interval, true),
        ]
    }

    pub(crate) async fn maintenance_paused(&self, kind: &str) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT CASE WHEN c.kind='gc' THEN m.gc_paused ELSE c.paused END FROM maintenance_controls c CROSS JOIN mokyu_meta m WHERE c.kind=$1")
            .bind(kind).fetch_one(&self.db).await?)
    }

    pub async fn maintenance_status(&self) -> Result<Value> {
        let mut rows: Vec<Value> = sqlx::query_scalar("SELECT to_jsonb(c)||jsonb_build_object('paused',CASE WHEN c.kind='gc' THEN m.gc_paused ELSE c.paused END,'latest',latest.value,'active',active.value) FROM maintenance_controls c CROSS JOIN mokyu_meta m LEFT JOIN LATERAL (SELECT to_jsonb(t) value FROM tasks t WHERE coalesce(t.detail->>'kind',t.kind)=c.kind ORDER BY created_at DESC,id DESC LIMIT 1) latest ON true LEFT JOIN LATERAL (SELECT to_jsonb(t) value FROM tasks t WHERE coalesce(t.detail->>'kind',t.kind)=c.kind AND t.state IN ('queued','running','paused') ORDER BY created_at,id LIMIT 1) active ON true ORDER BY c.kind")
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
        Ok(
            json!({"maintenance":global,"controls":rows,"concurrency":self.config.pack.maintenance_concurrency,
            "pack_configured":self.config.pack.enabled,"pack_creation_paused":self.pack_creation_paused.load(Ordering::Acquire),
            "pack_creation_enabled":self.pack_creation_allowed(),"preparing_packs":preparing,"active_operations":self.active.lock().unwrap().len(),
            "backend_prefix":self.storage.namespace(),"min_storage_duration":self.config.backend.min_storage_duration}),
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
        crate::manage::audit::checkpoint(
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
        if let Some(id) = sqlx::query_scalar::<_, Uuid>("SELECT id FROM tasks WHERE kind=$1 AND state IN ('queued','running','paused') ORDER BY created_at LIMIT 1")
            .bind(kind).fetch_optional(&self.db).await? {
            return Ok(json!({"task_id":id,"existing":true}));
        }
        let mut tx = self.db.begin().await?;
        let id = crate::tasks::insert(&mut tx, kind, None, json!({})).await?;
        sqlx::query("UPDATE maintenance_controls SET last_scheduled_at=now() WHERE kind=$1")
            .bind(kind)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        self.wake_tasks.notify_one();
        Ok(json!({"task_id":id}))
    }

    pub(crate) async fn lifecycle_batch(&self, id: Uuid, kind: &str) -> Result<bool> {
        let (count, again, report) = if kind == "gc" {
            if self.maintenance.load(Ordering::Acquire) || self.maintenance_paused(kind).await? {
                return Ok(false);
            }
            let cleaned = self.cleanup().await?;
            let count = self.reclaim().await?;
            (
                count as i64,
                count >= self.config.gc.batch_size as usize,
                json!({"local_cleaned":cleaned,"chunks_reclaimed":count}),
            )
        } else {
            let report = self.cleanup_history().await?;
            let count = report["last_run"]["deleted"]
                .as_object()
                .map(|v| v.values().filter_map(Value::as_i64).sum())
                .unwrap_or(0);
            (count, false, report)
        };
        sqlx::query("UPDATE tasks SET processed=processed+$2,state=CASE WHEN $3 THEN state ELSE 'completed' END,detail=detail||jsonb_build_object('report',$4::jsonb),updated_at=now() WHERE id=$1 AND state='running'")
            .bind(id).bind(count).bind(again).bind(report).execute(&self.db).await?;
        Ok(again)
    }
}

pub async fn run(app: Arc<App>) -> Result<()> {
    loop {
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
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
