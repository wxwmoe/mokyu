use crate::{
    authorization::Principal,
    storage::journal::snapshot::{BucketCut, Page},
};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use sqlx::types::Json;
use uuid::Uuid;

#[derive(Clone, Copy, Deserialize, Serialize, utoipa::ToSchema)]
#[schema(as=MetadataSnapshotAction)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Action {
    Append,
    Compact,
    New,
}
impl Action {
    pub fn name(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Compact => "compact",
            Self::New => "new",
        }
    }
}

#[derive(sqlx::FromRow)]
pub(crate) struct Job {
    pub encrypt_frames: bool,
    pub id: Uuid,
    pub storage_id: Uuid,
    pub kind: String,
    pub state: String,
    pub parent: Option<Uuid>,
    pub revision: i64,
    pub sequence_no: i64,
    pub digest: Option<Vec<u8>>,
    pub epoch: Option<Uuid>,
    pub page_limit: i64,
    pub covered_bytes: i64,
    pub manifest_etag: Option<String>,
    pub index_etag: Option<String>,
}
#[derive(sqlx::FromRow)]
pub(crate) struct Scope {
    pub bucket_id: Uuid,
    pub bucket_name: String,
    pub generation: i64,
    pub watermark: i64,
    pub included: bool,
    pub mode_revision: i64,
}
impl Scope {
    pub fn cut(&self) -> BucketCut {
        BucketCut {
            id: self.bucket_id,
            name: self.bucket_name.clone(),
            generation: self.generation,
            sequence: self.watermark,
            stopped: !self.included,
        }
    }
}
#[derive(Clone, sqlx::FromRow)]
pub(crate) struct Item {
    pub bucket_id: Uuid,
    pub generation: i64,
    pub seq: i64,
    pub object_key: String,
    pub deleted: bool,
}
pub(crate) struct Member {
    pub item: Item,
    pub bytes: i64,
}
#[derive(sqlx::FromRow)]
pub(crate) struct Part {
    pub owner: Uuid,
    pub page_no: i64,
    pub family: i64,
    pub bucket_id: Uuid,
    pub description: Json<Page>,
    pub etag: Option<String>,
}
#[derive(sqlx::FromRow)]
pub(crate) struct Garbage {
    pub storage_id: Uuid,
    pub object_id: Uuid,
    pub page_no: i64,
    pub kind: String,
    pub file_path: String,
    pub etag: String,
    pub remote_version: Option<String>,
    pub byte_count: i64,
}

#[derive(Serialize, utoipa::ToSchema)]
#[schema(as=MetadataSnapshotStatus)]
pub(crate) struct Status {
    pub current: Option<Uuid>,
    pub revision: String,
    pub pending_log_bytes: String,
    pub delete_log_bytes: String,
    pub invalid_bytes: String,
    pub gc_bytes: String,
    pub pages: String,
    pub source_entries: String,
    pub snapshot_deletions: String,
    pub task: Option<Uuid>,
    pub operation: Option<String>,
    pub task_state: Option<String>,
    pub error: Option<String>,
}
#[async_trait::async_trait]
pub(crate) trait SnapshotStore: Send + Sync {
    async fn snapshot_status(&self, storage: Uuid) -> Result<Status>;
    async fn snapshot_request(
        &self,
        principal: &Principal,
        storage: Uuid,
        action: Action,
    ) -> Result<Uuid>;
    async fn snapshot_sources(&self) -> Result<Vec<Uuid>>;
    async fn snapshot_claim(&self, storage: Uuid, token: Uuid) -> Result<bool>;
    async fn snapshot_renew(&self, storage: Uuid, token: Uuid) -> Result<bool>;
    async fn snapshot_release(&self, storage: Uuid, token: Uuid) -> Result<()>;
    async fn snapshot_begin(&self, storage: Uuid, token: Uuid) -> Result<Option<Job>>;
    async fn snapshot_scopes(&self, job: Uuid) -> Result<Vec<Scope>>;
    async fn snapshot_items(
        &self,
        job: Uuid,
        bucket: Uuid,
        after: Option<&str>,
        limit: i64,
    ) -> Result<Vec<Item>>;
    async fn snapshot_input(
        &self,
        bucket: Uuid,
        seq: i64,
        after: i32,
    ) -> Result<Vec<(i32, Vec<u8>)>>;
    async fn snapshot_pages(&self, job: Uuid) -> Result<Vec<Part>>;
    async fn snapshot_page(
        &self,
        job: &Job,
        token: Uuid,
        position: i64,
        family: i64,
        bucket: Uuid,
        parts: &[(Page, Vec<u8>)],
        members: &[Member],
    ) -> Result<()>;
    async fn snapshot_seal(
        &self,
        job: &Job,
        token: Uuid,
        manifest: &[u8],
        index: &[u8],
    ) -> Result<()>;
    async fn snapshot_blobs(&self, job: Uuid) -> Result<(Vec<u8>, Vec<u8>)>;
    async fn snapshot_bytes(&self, owner: Uuid, number: i64) -> Result<Vec<u8>>;
    async fn snapshot_receipt(
        &self,
        job: &Job,
        token: Uuid,
        number: Option<i64>,
        etag: &str,
        version: Option<&str>,
    ) -> Result<()>;
    async fn snapshot_scope_changed(&self, job: Uuid) -> Result<bool>;
    async fn snapshot_abandon(&self, job: &Job, token: Uuid) -> Result<()>;
    async fn snapshot_parent_etag(&self, parent: Uuid) -> Result<String>;
    async fn snapshot_commit(
        &self,
        job: &Job,
        token: Uuid,
        etag: &str,
        checkpoint: &str,
        version: Option<&str>,
        grace: i64,
    ) -> Result<()>;
    async fn snapshot_fail(
        &self,
        job: &Job,
        token: Uuid,
        error: &str,
        retryable: bool,
    ) -> Result<()>;
    async fn snapshot_gc_failure(&self, storage: Uuid, token: Uuid, reason: &str) -> Result<()>;
    async fn snapshot_gc(&self, storage: Uuid, token: Uuid, grace: i64) -> Result<Vec<Garbage>>;
    async fn snapshot_reclaimed(&self, token: Uuid, file: &Garbage) -> Result<()>;
}
