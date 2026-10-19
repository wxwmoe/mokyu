use crate::storage::profile::StorageProfile;
use anyhow::Result;
use async_trait::async_trait;
use uuid::Uuid;

#[async_trait]
pub(crate) trait StorageProfileStore: Send + Sync {
    async fn initialize(&self, min_free_space: &str) -> Result<()>;
    async fn update(&self, name: String, profile: StorageProfile) -> Result<Uuid>;

    async fn enabled_profiles(&self) -> Result<Vec<StorageProfile>>;
    async fn load(&self, storage_id: Uuid) -> Result<StorageProfile>;
}
