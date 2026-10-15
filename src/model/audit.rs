use crate::authorization::Principal;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::future::Future;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

#[async_trait]
pub(crate) trait AuditStore: Send + Sync {
    async fn begin(
        &self,
        principal: &Principal,
        action: &str,
        request_id: Option<&str>,
    ) -> Result<i64>;
    async fn finish(&self, id: i64, status: u16, partial: bool) -> Result<()>;
    async fn events(
        &self,
        admin: bool,
        user: Option<Uuid>,
        before: Option<i64>,
        filter: Filter,
        limit: i64,
        export: bool,
    ) -> Result<Vec<Event>>;
    async fn event(&self, id: i64, admin: bool, user: Uuid) -> Result<Option<Event>>;
}

tokio::task_local! { static EVENT: i64; }
pub(crate) fn active() -> bool {
    EVENT.try_with(|_| ()).is_ok()
}
pub(crate) fn current() -> Option<i64> {
    EVENT.try_with(|id| *id).ok()
}

pub(crate) async fn scope<T>(id: i64, future: impl Future<Output = T>) -> T {
    EVENT.scope(id, future).await
}

#[derive(Serialize, ToSchema)]
pub(crate) struct Event {
    pub(crate) id: String,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) finished_at: Option<DateTime<Utc>>,
    pub(crate) actor_id: Option<Uuid>,
    pub(crate) actor_label: String,
    pub(crate) token_id: Option<Uuid>,
    pub(crate) source: String,
    pub(crate) action: String,
    pub(crate) target: String,
    pub(crate) project_id: Option<Uuid>,
    pub(crate) bucket_id: Option<Uuid>,
    pub(crate) outcome: String,
    pub(crate) request_id: Option<String>,
    pub(crate) status: Option<i32>,
    pub(crate) detail: Value,
}

#[derive(Deserialize, IntoParams, Default)]
pub(crate) struct Filter {
    pub after: Option<String>,
    pub limit: Option<i64>,
    pub actor: Option<String>,
    pub action: Option<String>,
    pub source: Option<String>,
    pub outcome: Option<String>,
    pub project: Option<Uuid>,
    pub bucket: Option<Uuid>,
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
}
