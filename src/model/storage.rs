use crate::storage::{model::Record as BackendRecord, profile::StorageProfile};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Serialize;
use utoipa::ToSchema;
use uuid::Uuid;

pub(crate) struct StorageRecord {
    pub(crate) id: Uuid,
    pub(crate) store_type: String,
    pub(crate) path: Option<String>,
    pub(crate) endpoint: Option<String>,
    pub(crate) bucket: Option<String>,
    pub(crate) region: Option<String>,
    pub(crate) prefix: String,
    pub(crate) access: Option<Vec<u8>>,
    pub(crate) secret: Option<Vec<u8>>,
    pub(crate) identity_marker: serde_json::Value,
    pub(crate) revision: i64,
}

#[async_trait]
pub(crate) trait StorageRecordStore: Send + Sync {
    async fn list(&self) -> Result<Vec<StorageView>>;
    async fn view(&self, id: Uuid) -> Result<StorageView>;
    async fn named(&self, name: &str) -> Result<Option<StorageView>>;
    async fn id(&self, name: &str, parsed: Option<Uuid>) -> Result<Option<Uuid>>;
    async fn record(&self, name: &str) -> Result<Option<StorageRow>>;
    async fn creation(&self) -> Result<StorageCreation>;
    async fn edit(&self, name: &str) -> Result<StorageEdit>;
    async fn current_key(&self, name: &str, key: Option<&str>) -> Result<Uuid>;

    async fn enabled(&self) -> anyhow::Result<Vec<StorageRecord>>;
    async fn enabled_by_id(&self, id: Uuid) -> anyhow::Result<Option<StorageRecord>>;
}

#[derive(Serialize, ToSchema)]
pub(crate) struct StorageView {
    pub(crate) id: Uuid,
    pub(crate) name: String,
    #[serde(rename = "type")]
    pub(crate) storage_type: String,
    pub(crate) path: Option<String>,
    pub(crate) endpoint: Option<String>,
    pub(crate) bucket: Option<String>,
    pub(crate) region: Option<String>,
    pub(crate) prefix: String,
    pub(crate) block_key_id: Option<Uuid>,
    pub(crate) profile_revision: i64,
    pub(crate) read_concurrency: Option<i32>,
    pub(crate) upload_concurrency: Option<i32>,
    pub(crate) control_concurrency: Option<i32>,
    pub(crate) enabled: bool,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) updated_at: DateTime<Utc>,
}

#[derive(Clone)]
pub(crate) struct StorageRow {
    pub(crate) id: Uuid,
    pub(crate) name: String,
    pub(crate) r#type: String,
    pub(crate) path: Option<String>,
    pub(crate) endpoint_url: Option<String>,
    pub(crate) remote_bucket_name: Option<String>,
    pub(crate) region: Option<String>,
    pub(crate) prefix: String,
    pub(crate) access_key_encrypted: Option<Vec<u8>>,
    pub(crate) secret_key_encrypted: Option<Vec<u8>>,
    pub(crate) identity_marker: serde_json::Value,
    pub(crate) block_key_id: Option<Uuid>,
    pub(crate) enabled: bool,
}

#[must_use = "keep the deployment lock until storage initialization finishes"]
pub(crate) struct StorageCreation {
    pub(crate) deployment: Uuid,
    pub(crate) mode: String,
    pub(crate) writer: Box<dyn StorageInitializer>,
}
#[async_trait]
pub(crate) trait StorageInitializer: Send {
    async fn prepare(
        &mut self,
        record: &BackendRecord,
        key: Option<Uuid>,
        read: Option<i32>,
        upload: Option<i32>,
        control: Option<i32>,
        profile: Option<StorageProfile>,
    ) -> Result<StorageProfile>;
    async fn finish(self: Box<Self>) -> Result<()>;
}
#[must_use = "keep the storage locks until verification and update finish"]
pub(crate) struct StorageEdit {
    pub(crate) row: StorageRow,
    pub(crate) profile: StorageProfile,
    pub(crate) mode: String,
    pub(crate) update: Box<dyn StorageUpdate>,
}
#[async_trait]
pub(crate) trait StorageUpdate: Send {
    async fn finish(
        self: Box<Self>,
        record: &BackendRecord,
        key: Option<Uuid>,
        profile: &StorageProfile,
    ) -> Result<()>;
}
