use anyhow::Result;
use async_trait::async_trait;
use uuid::Uuid;

#[derive(Clone, Copy, Debug)]
pub(crate) struct Execution {
    pub(crate) id: Uuid,
    pub(crate) token: Uuid,
}

/// Persistent task control operations shared by single-container and cluster
/// schedulers. Callers wake local workers after committed state transitions.
#[async_trait]
pub(crate) trait TaskControlStore: Send + Sync {
    async fn unpack_status(&self, pack: i64) -> Result<Option<Value>>;
    async fn rewrite_status(&self, storage: Uuid) -> Result<Option<Value>>;
    async fn rewrite_task(&self, storage: Uuid) -> Result<Option<Uuid>>;

    async fn page(
        &self,
        principal: &Principal,
        filter: JobQuery,
        cursor: Option<(DateTime<Utc>, Uuid)>,
        limit: i64,
    ) -> Result<Vec<Value>>;
    async fn view(&self, principal: &Principal, task: Uuid) -> Result<Value>;
    async fn all(&self) -> Result<Vec<Value>>;
    async fn raw(&self, task: Uuid) -> Result<Option<Value>>;

    async fn other_integrity(&self, task: Uuid) -> Result<bool>;
    async fn enqueue(
        &self,
        task_type: &str,
        bucket: Option<Uuid>,
        detail: serde_json::Value,
    ) -> Result<Uuid>;
    async fn sweep_preview(&self, task: Uuid) -> Result<Option<serde_json::Value>>;
    async fn pause(&self, task: Execution) -> Result<()>;
    async fn running(&self, task: Execution) -> Result<bool>;
    async fn complete(&self, task: Execution) -> Result<()>;
    async fn progress(
        &self,
        task: Execution,
        cursor: &str,
        processed: i64,
        detail: serde_json::Value,
    ) -> Result<()>;

    async fn load(&self, id: Uuid) -> Result<Option<(bool, String, serde_json::Value)>>;
    async fn change_state(&self, id: Uuid, parent: bool, resume: bool) -> Result<bool>;
}

pub(crate) struct TaskClaim<'a> {
    pub(crate) class: i32,
    pub(crate) active: &'a [Uuid],
    pub(crate) token: Uuid,
}

pub(crate) struct TaskRecord {
    pub(crate) task_type: String,
    pub(crate) state: String,
    pub(crate) bucket_id: Option<Uuid>,
    pub(crate) cursor: Option<String>,
    pub(crate) detail: serde_json::Value,
    pub(crate) execution_token: Option<Uuid>,
}

pub(crate) struct TaskFinish {
    pub(crate) execution: Execution,
    pub(crate) reuse_seconds: u64,
    pub(crate) error: Option<String>,
}

/// Persistent task claims and execution ownership shared by both deployment modes.
#[async_trait]
pub(crate) trait TaskExecutionStore: Send + Sync {
    async fn claim(&self, input: TaskClaim<'_>) -> Result<Option<Uuid>>;
    async fn load(&self, id: Uuid) -> Result<TaskRecord>;
    async fn fence(&self, expected_mode: &str, id: Uuid, token: Uuid) -> Result<bool>;
    async fn confirm(&self, id: Uuid, token: Uuid) -> Result<bool>;
    async fn finish(&self, input: TaskFinish) -> Result<()>;
}

tokio::task_local! { pub(crate) static REQUEST_ID: Uuid; }

use crate::authorization::Principal;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;
use utoipa::IntoParams;
#[derive(Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
pub(crate) struct JobQuery {
    pub(crate) state: Option<String>,
    pub(crate) policy: Option<String>,
    pub(crate) bucket: Option<Uuid>,
    pub(crate) storage: Option<Uuid>,
    pub(crate) actor: Option<String>,
    pub(crate) token: Option<String>,
    pub(crate) limit: Option<i64>,
}
