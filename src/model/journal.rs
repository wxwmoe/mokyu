pub(crate) mod policy;
pub(crate) mod publication;
pub(crate) mod record;

use crate::{
    config::Secrets,
    storage::{journal::Format, marker::Journal},
};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use policy::Policy;
use serde::Serialize;
use sqlx::types::Json;
use uuid::Uuid;

#[derive(serde::Serialize, utoipa::ToSchema, sqlx::FromRow)]
pub(crate) struct BucketJournal {
    pub generation: String,
    pub captured_sequence: String,
    pub published_sequence: String,
    pub pending_records: String,
    pub scanning: bool,
    pub dirty: bool,
    pub baseline_ready: bool,
    pub error: Option<String>,
}

#[derive(sqlx::FromRow)]
pub(crate) struct Entry {
    pub encrypt_frames: bool,
    pub bucket_id: Uuid,
    pub bucket_name: String,
    pub generation: i64,
    pub seq: i64,
    pub operation: String,
    pub object_key: Option<String>,
    pub stream_id: Option<super::Id>,
    pub attributes: Option<Json<record::Attributes>>,
}

#[derive(Clone, sqlx::FromRow)]
pub(crate) struct Ledger {
    pub storage_id: Uuid,
    pub policy: Json<Policy>,
    pub revision: i64,
    pub descriptor: Option<Json<Journal>>,
    pub marker_installed: bool,
    pub allocated_seq: i64,
    pub allocated_digest: Option<Vec<u8>>,
    pub emitted_seq: i64,
    pub open_file: Option<Uuid>,
    pub flush_requested: bool,
    pub last_bucket: Option<Uuid>,
    pub now_us: i64,
}

#[derive(sqlx::FromRow)]
pub(crate) struct Cursor {
    pub page: i32,
    pub end_offset: i64,
    pub range_count: i64,
    pub range_digest: Vec<u8>,
    pub source_revision: Option<i64>,
    pub final_page: bool,
}

#[derive(sqlx::FromRow)]
pub(crate) struct Draft {
    pub bucket_id: Uuid,
    pub seq: i64,
    pub page: i32,
    pub encrypted: Vec<u8>,
    pub final_page: bool,
    pub recorded_us: i64,
}

#[derive(Serialize, utoipa::ToSchema)]
#[schema(as = MetadataJournalStatus)]
pub(crate) struct Status {
    pub policy: Policy,
    pub pending_policy: bool,
    pub revision: String,
    pub emitted_sequence: String,
    pub open_file: Option<Uuid>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub error: Option<String>,
}

#[async_trait]
pub(crate) trait JournalStore: Send + Sync {
    async fn bucket(&self, bucket: uuid::Uuid) -> Result<Option<BucketJournal>>;
    async fn marker_intents(&self) -> Result<Vec<Uuid>>;
    async fn installed(
        &self,
        storage: Uuid,
        token: Uuid,
        previous: &serde_json::Value,
        updated: &serde_json::Value,
    ) -> Result<()>;
    async fn sources(&self) -> Result<Vec<Uuid>>;
    async fn claim(&self, storage: Uuid, token: Uuid) -> Result<Option<Ledger>>;
    async fn renew(&self, storage: Uuid, token: Uuid) -> Result<bool>;
    async fn release(&self, storage: Uuid, token: Uuid, error: Option<&str>) -> Result<()>;
    async fn descriptor(&self, storage: Uuid, token: Uuid, journal: &Journal) -> Result<()>;
    async fn due_buckets(
        &self,
        storage: Uuid,
        after: Option<Uuid>,
        limit: i64,
    ) -> Result<Vec<Uuid>>;
    async fn prepare_error(
        &self,
        storage: Uuid,
        token: Uuid,
        bucket: Uuid,
        error: &str,
    ) -> Result<()>;
    async fn prepare_page(
        &self,
        storage: Uuid,
        bucket: Uuid,
        token: Uuid,
        format: &Format<'_>,
        secrets: &Secrets,
    ) -> Result<bool>;
    async fn drafts(&self, bucket: Uuid, limit: i64) -> Result<Vec<Draft>>;
    async fn open(&self, storage: Uuid, id: Uuid) -> Result<publication::File>;
    async fn next_file(&self, storage: Uuid) -> Result<Option<publication::File>>;
    async fn known(
        &self,
        storage: Uuid,
        epoch: Uuid,
        ids: &[[u8; 32]],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
    async fn allocate(
        &self,
        storage: Uuid,
        token: Uuid,
        plan: &publication::Allocation,
    ) -> Result<()>;
    async fn file_frames(&self, file: Uuid) -> Result<Vec<Vec<u8>>>;
    async fn start_upload(&self, storage: Uuid, file: Uuid, token: Uuid) -> Result<bool>;
    async fn upload_id(&self, storage: Uuid, file: Uuid, token: Uuid, upload: &str) -> Result<()>;
    async fn parts(&self, file: Uuid) -> Result<Vec<publication::Part>>;
    async fn part(&self, storage: Uuid, token: Uuid, part: &publication::Part) -> Result<()>;
    async fn acknowledge(
        &self,
        storage: Uuid,
        token: Uuid,
        file: &publication::File,
        etag: Option<&str>,
        version: Option<&str>,
    ) -> Result<()>;
    async fn idle(&self, storage: Uuid, token: Uuid) -> Result<()>;
    async fn journal_status(&self, storage: Uuid) -> Result<Status>;
    async fn policy(
        &self,
        principal: &crate::authorization::Principal,
        storage: Uuid,
        revision: i64,
        policy: &Policy,
    ) -> Result<()>;
    async fn flush(&self, principal: &crate::authorization::Principal, storage: Uuid)
    -> Result<()>;
    /// Capture one page of an enabled bucket's initial object inventory.
    async fn seed_page(&self, limit: i64) -> Result<bool>;
}
