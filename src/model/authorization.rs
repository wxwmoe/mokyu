use crate::authorization::{Action, Principal};
use anyhow::Result;
use async_trait::async_trait;
use uuid::Uuid;

#[async_trait]
pub(crate) trait AuthorizationStore: Send + Sync {
    async fn admin(&self, principal: &Principal) -> Result<Box<dyn AdminGuard>>;
    async fn service(&self, access_key: &str) -> Result<Principal>;
    async fn actions(&self, principal: &Principal, bucket: Uuid) -> Result<Vec<Action>>;
}

/// Holds administrator validity until the operation and audit complete.
#[async_trait]
pub(crate) trait AdminGuard: Send {
    async fn checkpoint(
        &mut self,
        action: &str,
        target: &str,
        detail: serde_json::Value,
    ) -> Result<()>;
    async fn finish(self: Box<Self>) -> Result<()>;
}
