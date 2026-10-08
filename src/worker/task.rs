use crate::app::App;
use anyhow::Result;
use serde_json::{Value, json};
use uuid::Uuid;

tokio::task_local! { pub(crate) static REQUEST_ID: Uuid; }

/// Check the lease and the deployment mode immediately before a worker touches
/// data.  A mode transition or paused parent fences the child without allowing
/// an in-flight batch to publish stale work.
pub(crate) async fn fence(app: &App, id: Uuid, token: Uuid) -> Result<bool> {
    let row: Option<(String, Option<Uuid>, Option<Uuid>, String, serde_json::Value)> = sqlx::query_as(
        "SELECT t.state,t.lease_token,t.parent_task_id,m.deployment_mode,t.detail FROM tasks t CROSS JOIN mokyu_meta m WHERE t.id=$1",
    )
    .bind(id)
    .fetch_optional(&app.db)
    .await?;
    let Some((state, lease, parent, mode, detail)) = row else {
        return Ok(false);
    };
    let expected = if app.role == "single" {
        "single"
    } else {
        "cluster"
    };
    if mode != expected {
        sqlx::query("UPDATE tasks SET state='paused',lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$2")
            .bind(id)
            .bind(token)
            .execute(&app.db)
            .await?;
        return Ok(false);
    }
    if let (Some(storage), Some(revision)) = (
        detail["storage_id"]
            .as_str()
            .and_then(|value| value.parse::<Uuid>().ok()),
        detail["profile_revision"].as_i64(),
    ) {
        let current: Option<i64> =
            sqlx::query_scalar("SELECT revision FROM storage_profiles WHERE storage_id=$1")
                .bind(storage)
                .fetch_optional(&app.db)
                .await?;
        if current != Some(revision) {
            sqlx::query("UPDATE tasks SET state='paused',error='storage profile changed; resume to refresh',lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$2")
                .bind(id)
                .bind(token)
                .execute(&app.db)
                .await?;
            return Ok(false);
        }
    }
    if state != "running" || lease != Some(token) {
        return Ok(false);
    }
    if let Some(parent) = parent {
        let parent_state: Option<String> =
            sqlx::query_scalar("SELECT state FROM tasks WHERE id=$1")
                .bind(parent)
                .fetch_optional(&app.db)
                .await?;
        if !matches!(parent_state.as_deref(), Some("queued" | "running")) {
            sqlx::query("UPDATE tasks SET state='paused',lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$2")
                .bind(id)
                .bind(token)
                .execute(&app.db)
                .await?;
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) async fn insert(
    db: &mut sqlx::PgConnection,
    kind: &str,
    bucket: Option<Uuid>,
    detail: Value,
) -> Result<Uuid> {
    let id = REQUEST_ID
        .try_with(|id| *id)
        .unwrap_or_else(|_| Uuid::new_v4());
    insert_with_id(db, id, kind, bucket, detail).await
}

/// Child tasks must always get their own identity even when the parent was
/// created inside a request-id scope (for example a confirmed maintenance run).
pub(crate) async fn insert_child(
    db: &mut sqlx::PgConnection,
    kind: &str,
    bucket: Option<Uuid>,
    detail: Value,
) -> Result<Uuid> {
    insert_with_id(db, Uuid::new_v4(), kind, bucket, detail).await
}

async fn insert_with_id(
    db: &mut sqlx::PgConnection,
    id: Uuid,
    kind: &str,
    bucket: Option<Uuid>,
    detail: Value,
) -> Result<Uuid> {
    sqlx::query("INSERT INTO tasks(id,task_type,bucket_id,state,detail,created_by_label,source) VALUES($1,$2,$3,'queued',$4,coalesce((SELECT actor_label FROM audit_events WHERE id=$5),'Scheduler'),coalesce((SELECT source FROM audit_events WHERE id=$5),'system'))")
        .bind(id).bind(kind).bind(bucket).bind(detail).bind(crate::control::audit::current()).execute(&mut *db).await?;
    sqlx::query("SELECT pg_notify('mokyu_tasks',$1)")
        .bind(id.to_string())
        .execute(&mut *db)
        .await?;
    Ok(id)
}

impl App {
    pub async fn task_change(&self, id: Uuid, resume: bool) -> Result<Value> {
        let _coord = self.coord.lock().await;
        let parent_task: bool = sqlx::query_scalar(
            "SELECT coalesce(detail->>'role'='parent',false) FROM tasks WHERE id=$1",
        )
        .bind(id)
        .fetch_optional(&self.db)
        .await?
        .unwrap_or(false);
        if resume {
            let (kind, detail): (String, Value) =
                sqlx::query_as("SELECT task_type,detail FROM tasks WHERE id=$1")
                    .bind(id)
                    .fetch_optional(&self.db)
                    .await?
                    .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
            let policy = if kind == "pack" {
                detail["kind"].as_str().unwrap_or("")
            } else {
                &kind
            };
            if self
                .maintenance_schedule()
                .iter()
                .any(|(k, _, _)| *k == policy)
                && (self.maintenance_paused(policy).await?
                    || (matches!(policy, "pack" | "repack") && !self.pack_creation_allowed()))
            {
                return Err(s3s::s3_error!(
                    OperationAborted,
                    "resume the maintenance policy and pack creation first"
                )
                .into());
            }
            if kind == "sweep" && detail["dry_run"] == false {
                if !self.maintenance.load(std::sync::atomic::Ordering::Acquire) {
                    return Err(s3s::s3_error!(
                        OperationAborted,
                        "enable maintenance before resuming destructive sweep"
                    )
                    .into());
                }
            } else if matches!(
                kind.as_str(),
                "purge" | "pack" | "unpack" | "upload" | "gc" | "storage_migration" | "rewrite"
            ) {
                self.writable()?;
            } else if kind == "integrity" {
                let busy: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE task_type='integrity' AND state IN ('queued','running') AND id<>$1)").bind(id).fetch_one(&self.db).await?;
                if busy {
                    return Err(s3s::s3_error!(
                        OperationAborted,
                        "an integrity check is already active"
                    )
                    .into());
                }
            }
        }
        let changed = if resume {
            if parent_task {
                sqlx::query("UPDATE tasks SET state='queued',error=NULL,updated_at=now() WHERE (id=$1 OR parent_task_id=$1) AND state IN ('paused','failed')").bind(id).execute(&self.db).await?
            } else {
                sqlx::query("UPDATE tasks SET state='queued',error=NULL,updated_at=now() WHERE id=$1 AND state IN ('paused','failed')").bind(id).execute(&self.db).await?
            }
        } else {
            if parent_task {
                sqlx::query("UPDATE tasks SET state='paused',updated_at=now() WHERE (id=$1 OR parent_task_id=$1) AND state IN ('queued','running')").bind(id).execute(&self.db).await?
            } else {
                sqlx::query("UPDATE tasks SET state='paused',updated_at=now() WHERE id=$1 AND state IN ('queued','running')").bind(id).execute(&self.db).await?
            }
        };
        if changed.rows_affected() == 0 {
            return Err(
                s3s::s3_error!(OperationAborted, "task not found or cannot change state").into(),
            );
        }
        sqlx::query("SELECT pg_notify('mokyu_tasks',$1)")
            .bind(id.to_string())
            .execute(&self.db)
            .await?;
        self.wake_tasks.notify_one();
        Ok(json!({"task_id":id,"state":if resume{"queued"}else{"paused"}}))
    }
}
