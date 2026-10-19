use crate::app::App;
use anyhow::Result;
use serde_json::{Value, json};
use uuid::Uuid;

/// Check the lease and the deployment mode immediately before a worker touches
/// data.  A mode transition or paused parent fences the child without allowing
/// an in-flight batch to publish stale work.
pub(crate) async fn fence(app: &App, id: Uuid, token: Uuid) -> Result<bool> {
    let expected = if app.role == "single" {
        "single"
    } else {
        "cluster"
    };
    app.task_execution.fence(expected, id, token).await
}

impl App {
    pub async fn task_change(&self, id: Uuid, resume: bool) -> Result<Value> {
        let _coord = self.coord.lock().await;
        let task = self.task_control.load(id).await?;
        let parent_task = task.as_ref().is_some_and(|(parent, _, _)| *parent);
        if resume {
            self.ensure_mode()?;
            let (_, task_type, detail) = task.ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
            let mode = if self.role == "single" {
                "single"
            } else {
                "cluster"
            };
            anyhow::ensure!(
                detail["execution_mode"]
                    .as_str()
                    .is_none_or(|saved| saved == mode),
                "cannot resume a task from a different deployment mode"
            );
            let policy = if task_type == "pack" {
                detail["policy"].as_str().unwrap_or("")
            } else {
                &task_type
            };
            if self
                .maintenance_schedule()
                .await?
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
            if task_type == "sweep" && detail["dry_run"] == false {
                if !self.maintenance.load(std::sync::atomic::Ordering::Acquire) {
                    return Err(s3s::s3_error!(
                        OperationAborted,
                        "enable maintenance before resuming destructive sweep"
                    )
                    .into());
                }
            } else if matches!(
                task_type.as_str(),
                "purge" | "pack" | "unpack" | "upload" | "gc" | "storage_migration" | "rewrite"
            ) {
                self.writable()?;
            } else if detail.get("source_operation").is_some() {
                self.source_writable()?;
            } else if task_type == "integrity" {
                let busy: bool = self.task_control.other_integrity(id).await?;
                if busy {
                    return Err(s3s::s3_error!(
                        OperationAborted,
                        "an integrity check is already active"
                    )
                    .into());
                }
            }
        }
        if !self
            .task_control
            .change_state(id, parent_task, resume)
            .await?
        {
            return Err(
                s3s::s3_error!(OperationAborted, "task not found or cannot change state").into(),
            );
        }
        self.wake_tasks.notify_one();
        Ok(json!({"task_id":id,"state":if resume{"queued"}else{"paused"}}))
    }
}
