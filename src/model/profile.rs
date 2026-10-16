use crate::storage::profile::StorageProfile;
use anyhow::Result;
use async_trait::async_trait;
use uuid::Uuid;

#[async_trait]
pub(crate) trait StorageProfileStore: Send + Sync {
    async fn update(&self, name: String, profile: StorageProfile) -> Result<Uuid>;

    async fn exists(&self, storage_id: Uuid) -> Result<bool>;
    async fn load(&self, storage_id: Uuid) -> Result<StorageProfile>;
}
