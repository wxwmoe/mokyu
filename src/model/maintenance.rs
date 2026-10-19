use crate::model::task::Execution;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(sqlx::FromRow)]
pub(crate) struct RecoveryFragment {
    pub(crate) id: Uuid,
    #[sqlx(flatten)]
    pub(crate) source: crate::model::FragmentSource,
}

#[must_use = "keep the deletion guard alive until fragment I/O completes"]
pub(crate) struct FragmentCleanup {
    pub(crate) sealed: bool,
    pub(crate) owner: Option<Uuid>,
    pub(crate) storage: Option<Uuid>,
    pub(crate) key: Option<String>,
    pub(crate) file: Option<Uuid>,
    pub(crate) deletion: Box<dyn FragmentDeletion>,
}
#[async_trait]
pub(crate) trait FragmentDeletion: Send {
    async fn finish(self: Box<Self>) -> Result<()>;
}

#[async_trait]
pub(crate) trait MaintenanceStore: Send + Sync {
    async fn preview(
        &self,
        principal: &Principal,
        action: &str,
        parameters: Value,
        impact: Value,
        confirmation: String,
    ) -> Result<MaintenancePreview>;
    async fn execution(
        &self,
        principal: &Principal,
        id: Uuid,
        action: &str,
    ) -> Result<Option<MaintenanceExecution>>;
    async fn destructive_sweep(&self) -> Result<bool>;
    async fn mode(&self, enabled: bool) -> Result<()>;

    async fn pack_creation(&self, paused: bool) -> Result<()>;
    async fn policy_paused(&self, policy: &str) -> Result<bool>;
    async fn controls(&self) -> Result<Vec<serde_json::Value>>;
    async fn preparing_packs(&self) -> Result<String>;
    async fn change_policy(&self, policy: &str, paused: bool) -> Result<()>;
    async fn start_policy(&self, policy: &str, node: Option<Uuid>) -> Result<(Uuid, bool)>;
    async fn finish_policy(
        &self,
        id: Execution,
        count: i64,
        again: bool,
        report: serde_json::Value,
    ) -> Result<()>;
    async fn schedule(&self) -> Result<Vec<(String, bool, Option<f64>)>>;
    async fn reuse_due(&self) -> Result<bool>;
    async fn scheduled(&self, policy: &str) -> Result<()>;

    async fn chunk_candidates(
        &self,
        storage: Uuid,
        grace: f64,
        batch: i64,
    ) -> Result<Vec<(i64, i32)>>;
    async fn cleanup_history(
        &self,
        section: &str,
        age: f64,
        batch: i64,
        active: &[Uuid],
        timeout_ms: u128,
    ) -> Result<u64>;
    async fn recover(&self, maintenance: bool) -> Result<()>;
    async fn fragment_exists(&self, id: Uuid) -> Result<bool>;
    async fn recovery_fragments(&self, after: Uuid) -> Result<Vec<RecoveryFragment>>;
    async fn fragment_uploads(&self, id: Uuid) -> Result<Vec<Uuid>>;
    async fn expired_uploads(&self, batch: i64, storage: Option<Uuid>) -> Result<Vec<Uuid>>;
    async fn stale_streams(
        &self,
        active: &[Uuid],
        batch: i64,
        storage: Option<Uuid>,
        retention: Option<f64>,
    ) -> Result<Vec<Uuid>>;
    async fn claim_stream(
        &self,
        id: Uuid,
        storage: Option<Uuid>,
        retention: Option<f64>,
    ) -> Result<bool>;
    async fn cleanup_stream(&self, id: Uuid, batch: i64) -> Result<usize>;
    async fn stale_fragments(
        &self,
        batch: i64,
        active: &[Uuid],
        storage: Option<Uuid>,
    ) -> Result<Vec<Uuid>>;
    async fn claim_fragment(&self, id: Uuid) -> Result<Option<FragmentCleanup>>;
    async fn cleanup_tombstones(&self, storage: Option<Uuid>, batch: i64) -> Result<u64>;
    async fn gc_paused(&self) -> Result<bool>;
    async fn storages(&self) -> Result<Vec<Uuid>>;
    async fn retire_chunk(
        &self,
        id: i64,
        storage: Uuid,
        grace: f64,
        active: &[Uuid],
    ) -> Result<Option<Vec<Uuid>>>;
    async fn delete_chunk(&self, id: i64, storage: Uuid) -> Result<()>;
    async fn retire_packs(&self, storage: Uuid) -> Result<()>;
    async fn physical_candidates(
        &self,
        packed: bool,
        grace: f64,
        batch: i64,
        minimum: f64,
        storage: Uuid,
    ) -> Result<Vec<(i64, Uuid, bool)>>;
    async fn physical_age(&self, packed: bool, id: i64, stored: DateTime<Utc>) -> Result<()>;
    async fn claim_physical(&self, packed: bool, id: i64, grace: f64, minimum: f64)
    -> Result<bool>;
    async fn delete_physical(&self, packed: bool, id: i64, retention: f64) -> Result<()>;
    async fn gc_status(&self) -> Result<(Vec<(String, i64)>, bool, bool)>;
    async fn mark_orphans(&self) -> Result<usize>;
}

use crate::authorization::Principal;
use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;

#[derive(Serialize, ToSchema)]
pub(crate) struct MaintenancePreview {
    pub(crate) id: Uuid,
    pub(crate) confirmation: String,
    pub(crate) expires_at: DateTime<Utc>,
    pub(crate) impact: Value,
}

#[must_use = "keep the preview locked until its action is recorded"]
pub(crate) struct MaintenanceExecution {
    pub(crate) parameters: Value,
    pub(crate) fingerprint: String,
    pub(crate) task: Option<Uuid>,
    pub(crate) completion: Box<dyn MaintenanceCompletion>,
}
#[async_trait]
pub(crate) trait MaintenanceCompletion: Send {
    async fn finish(self: Box<Self>, task: Uuid, audit: bool) -> Result<()>;
}
