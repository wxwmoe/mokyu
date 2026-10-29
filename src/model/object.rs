use crate::authorization::{Permit, Principal};
use crate::model::Id;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

pub(crate) struct ObjectChange {
    pub(crate) principal: Principal,
    pub(crate) bucket: Uuid,
    pub(crate) key: String,
    pub(crate) expected: Option<Id>,
    pub(crate) public: Option<bool>,
    pub(crate) if_match: Option<s3s::dto::ETagCondition>,
}

#[derive(Clone, Copy, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum MediaAction {
    Delete,
    Private,
    PublicRead,
    Copy,
    Move,
    Metadata,
}

#[derive(Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct Outcome {
    pub(crate) status: u16,
    pub(crate) code: Option<String>,
    pub(crate) output_version: Option<Id>,
    pub(crate) task_id: Option<Uuid>,
}

pub(crate) struct Receipt {
    pub(crate) id: Uuid,
    pub(crate) bucket: Uuid,
    pub(crate) key: String,
    pub(crate) action: MediaAction,
    pub(crate) count: usize,
}

tokio::task_local! { pub(crate) static RECEIPT: Receipt; }

pub(crate) struct ObjectTransfer<'a> {
    pub(crate) source_bucket: Uuid,
    pub(crate) source_key: &'a str,
    pub(crate) source_version: Id,
    pub(crate) target_bucket: Uuid,
    pub(crate) target_key: &'a str,
    pub(crate) target_version: Option<Id>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct StorageMigration {
    pub(crate) source_bucket: Uuid,
    pub(crate) source_key: String,
    pub(crate) source_version: Id,
    pub(crate) target_bucket: Uuid,
    pub(crate) target_key: String,
    pub(crate) target_version: Option<Id>,
    pub(crate) action: String,
    pub(crate) public_read: bool,
}

#[async_trait]
pub(crate) trait ObjectStore: Send + Sync {
    async fn browse(
        &self,
        authority: &Permit,
        bucket: Uuid,
        filter: &mut CatalogFilter,
        cursor: Option<CatalogCursor>,
        sizes: (Option<i64>, Option<i64>),
    ) -> Result<Vec<MediaItem>>;
    async fn chunks(&self, stream: Id, after: i64, limit: i64) -> Result<Vec<Value>>;
    async fn change(&self, input: ObjectChange) -> Result<bool>;
    async fn begin_operation(
        &self,
        principal: &Principal,
        user: Uuid,
        client: Uuid,
        hash: &[u8],
    ) -> Result<(Uuid, Vec<u8>)>;
    async fn operation(&self, id: Uuid) -> Result<Option<Outcome>>;
    async fn fail_operation(&self, id: Uuid, outcome: &Outcome) -> Result<()>;
    async fn audit_batch(&self, bucket: Uuid, detail: Value, partial: bool) -> Result<()>;
    async fn storage(&self, bucket: Uuid) -> Result<Option<Uuid>>;
    async fn move_object(&self, permit: &Permit, input: ObjectTransfer<'_>) -> Result<()>;
    async fn publish_copy(
        &self,
        permit: &Permit,
        input: ObjectTransfer<'_>,
        stream: Id,
        public: bool,
        expected_public: Option<bool>,
    ) -> Result<()>;
    async fn queue_migration(&self, permit: &Permit, job: &StorageMigration) -> Result<()>;
    async fn complete_migration(
        &self,
        task: Uuid,
        token: Uuid,
        stream: Id,
        job: &StorageMigration,
    ) -> Result<()>;
    async fn abandon_copy(&self, stream: Id) -> Result<()>;
    async fn abandon_migration(&self, stream: Id) -> Result<()>;
}

#[derive(Serialize, Deserialize, IntoParams)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct CatalogFilter {
    pub(crate) prefix: String,
    pub(crate) q: String,
    pub(crate) mode: String,
    pub(crate) search_in: String,
    pub(crate) category: Option<String>,
    pub(crate) public: Option<bool>,
    pub(crate) min_size: Option<String>,
    pub(crate) max_size: Option<String>,
    pub(crate) since: Option<DateTime<Utc>>,
    pub(crate) until: Option<DateTime<Utc>>,
    pub(crate) sort: String,
    pub(crate) order: String,
    pub(crate) recursive: bool,
    pub(crate) after: Option<String>,
    pub(crate) limit: usize,
}
impl Default for CatalogFilter {
    fn default() -> Self {
        Self {
            prefix: String::new(),
            q: String::new(),
            mode: "contains".into(),
            search_in: "name".into(),
            category: None,
            public: None,
            min_size: None,
            max_size: None,
            since: None,
            until: None,
            sort: "name".into(),
            order: "asc".into(),
            recursive: false,
            after: None,
            limit: 100,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct CatalogCursor {
    pub(crate) scope: String,
    pub(crate) key: String,
    pub(crate) size: String,
    pub(crate) modified: DateTime<Utc>,
    pub(crate) folder: Option<String>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct MediaItem {
    pub(crate) id: Id,
    pub(crate) object_key: String,
    pub(crate) size: String,
    pub(crate) content_type: String,
    pub(crate) public_read: bool,
    pub(crate) modified_at: DateTime<Utc>,
}
