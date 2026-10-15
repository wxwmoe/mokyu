use anyhow::Result;
use async_trait::async_trait;
use uuid::Uuid;

/// Storage concurrency permits are durable backend state. The PostgreSQL
/// implementation serializes acquisition with an advisory transaction lock;
/// other backends can use their native single-container locking strategy.
#[async_trait]
pub(crate) trait StoragePermitStore: Send + Sync {
    async fn acquire(
        &self,
        storage_id: Uuid,
        direction: &str,
        limit: i32,
        holder: Uuid,
    ) -> Result<Option<Uuid>>;
    async fn renew(&self, token: Uuid) -> Result<bool>;
    async fn release(&self, token: Uuid) -> Result<()>;
}
