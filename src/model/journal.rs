use anyhow::Result;
use async_trait::async_trait;

#[derive(serde::Serialize, utoipa::ToSchema, sqlx::FromRow)]
pub(crate) struct BucketJournal {
    pub generation: String,
    pub captured_sequence: String,
    pub published_sequence: String,
    pub pending_records: String,
    pub scanning: bool,
    pub dirty: bool,
}

#[async_trait]
pub(crate) trait JournalStore: Send + Sync {
    async fn bucket(&self, bucket: uuid::Uuid) -> Result<Option<BucketJournal>>;
    /// Capture one page of an enabled bucket's initial object inventory.
    async fn seed_page(&self, limit: i64) -> Result<bool>;
}
