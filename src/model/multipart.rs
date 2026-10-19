use crate::app::{Extent, StoredStream};
use crate::authorization::{Permit, Principal};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

pub struct Upload {
    pub id: Uuid,
    pub bucket_id: Uuid,
    pub object_key: String,
    pub state: String,
    pub metadata: Value,
    pub public_read: bool,
    pub checksum_algorithm: Option<String>,
    pub checksum_type: Option<String>,
    pub manifest_hash: Option<String>,
    pub result: Option<Value>,
    pub created_at: DateTime<Utc>,
}

pub(crate) struct WebUpload {
    pub user: Uuid,
    pub client_id: Uuid,
    pub request_hash: Vec<u8>,
    pub file_name: String,
    pub size: i64,
    pub part_size: i64,
    pub modified_at: Option<i64>,
    pub overwrite: bool,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct Transfer {
    pub(crate) id: Uuid,
    pub(crate) bucket_id: Uuid,
    pub(crate) bucket_name: String,
    pub(crate) object_key: String,
    #[serde(rename = "status")]
    pub(crate) state: String,
    pub(crate) source: String,
    pub(crate) user_id: Option<Uuid>,
    pub(crate) owner: String,
    pub(crate) file_name: Option<String>,
    pub(crate) expected_size: Option<String>,
    pub(crate) part_size: Option<String>,
    pub(crate) modified_at: Option<i64>,
    pub(crate) received_bytes: String,
    pub(crate) parts: i64,
    pub(crate) remote_state: String,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) touched_at: DateTime<Utc>,
    pub(crate) expires_at: Option<DateTime<Utc>>,
    pub(crate) can_resume: bool,
}

#[derive(Default, Deserialize, Serialize, IntoParams)]
pub(crate) struct TransferFilter {
    pub(crate) bucket: Option<Uuid>,
    pub(crate) state: Option<String>,
    pub(crate) source: Option<String>,
    pub(crate) own: Option<bool>,
    pub(crate) after: Option<String>,
    pub(crate) limit: Option<i64>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct ReceivedPart {
    pub(crate) number: i32,
    pub(crate) size: String,
    pub(crate) etag: String,
    pub(crate) sha256: Option<String>,
}

pub(crate) type UploadPart = (i32, i64, String, DateTime<Utc>, Value);
pub(crate) type SeedSpan = (Uuid, i64, usize);

#[derive(sqlx::FromRow)]
pub(crate) struct LocalPartRange {
    pub(crate) offset_bytes: i64,
    pub(crate) length: i32,
    pub(crate) source_offset: i32,
    pub(crate) fragment_id: Option<Uuid>,
    pub(crate) size: Option<i32>,
    pub(crate) hash: Option<Vec<u8>>,
    pub(crate) file_id: Option<Uuid>,
    pub(crate) file_offset: Option<i64>,
    pub(crate) file_storage: Option<Uuid>,
    pub(crate) raw_source: Option<Uuid>,
}

#[derive(sqlx::FromRow)]
pub(crate) struct MultipartFile {
    pub(crate) id: Uuid,
    pub(crate) upload_id: Option<Uuid>,
    pub(crate) storage_id: Uuid,
    pub(crate) source_id: Option<Uuid>,
    pub(crate) state: String,
}

pub(crate) struct FileRemap {
    pub(crate) fragment: Uuid,
    pub(crate) file: Uuid,
    pub(crate) offset: i64,
    pub(crate) destination: i64,
    pub(crate) size: i32,
    pub(crate) hash: [u8; 32],
}

pub(crate) struct MultipartCreate<'a> {
    pub authority: &'a Permit,
    pub bucket: Uuid,
    pub key: &'a str,
    pub access: &'a str,
    pub metadata: Value,
    pub public: bool,
    pub algorithm: Option<String>,
    pub checksum_type: Option<String>,
    pub web: Option<WebUpload>,
    pub max_active: Option<i64>,
}
pub(crate) struct SeedConsumption<'a> {
    pub upload: Uuid,
    pub stream: Uuid,
    pub offset: i64,
    pub chunk: i64,
    pub prefix: usize,
    pub consumed: usize,
    pub spans: &'a mut Vec<SeedSpan>,
}
pub(crate) struct PartStitch {
    pub left: Uuid,
    pub offset: i64,
    pub left_length: usize,
    pub right: Uuid,
    pub right_length: usize,
    pub chunk: i64,
}

#[async_trait]
pub(crate) trait MultipartStore: Send + Sync {
    async fn file(&self, upload: Uuid) -> Result<Uuid>;
    async fn seal_files(&self, upload: Uuid) -> Result<()>;
    async fn local_ranges(&self, stream: Uuid, after: i64) -> Result<Vec<LocalPartRange>>;
    async fn file_location(&self, id: Uuid) -> Result<Option<MultipartFile>>;
    async fn file_ranges(&self, id: Uuid) -> Result<Vec<(i64, i32)>>;
    async fn remap_file(&self, upload: Uuid, source: Uuid, ranges: &[FileRemap]) -> Result<()>;
    async fn pending_files(&self, storage: Option<Uuid>, limit: i64) -> Result<Vec<MultipartFile>>;
    async fn clear_reclaim(&self, id: Uuid) -> Result<()>;
    async fn unused_files(&self, storage: Option<Uuid>, limit: i64) -> Result<Vec<MultipartFile>>;
    async fn claim_file(&self, id: Uuid) -> Result<bool>;
    async fn remove_file(&self, id: Uuid) -> Result<()>;
    async fn upload(&self, id: Uuid, bucket: Uuid, key: &str, access: &str) -> Result<Upload>;
    async fn create(&self, input: MultipartCreate<'_>) -> Result<(Uuid, bool)>;
    async fn begin_part(
        &self,
        authority: &Permit,
        upload: Uuid,
        number: i32,
        stream: Uuid,
    ) -> Result<()>;
    async fn commit_part(
        &self,
        authority: &Permit,
        bucket: Uuid,
        upload: Uuid,
        number: i32,
        stream: Uuid,
    ) -> Result<()>;
    async fn active(&self, upload: Uuid) -> Result<bool>;
    async fn part(&self, upload: Uuid, number: i32) -> Result<Option<StoredStream>>;
    async fn tail(&self, stream: Uuid) -> Result<Option<Extent>>;
    async fn pair(&self, upload: Uuid, number: i32) -> Result<Option<(Value, Vec<StoredStream>)>>;
    async fn consume_seed(&self, input: SeedConsumption<'_>) -> Result<()>;
    async fn stitch(&self, input: PartStitch) -> Result<()>;
    async fn retire_work(&self, stream: Uuid) -> Result<()>;
    async fn begin_complete(&self, authority: &Permit, upload: Uuid, hash: &str) -> Result<()>;
    async fn retry_complete(&self, upload: Uuid) -> Result<()>;
    async fn abort(&self, authority: &Permit, upload: Uuid) -> Result<()>;
    async fn parts(&self, upload: Uuid, after: i32, limit: i64) -> Result<Vec<UploadPart>>;
    async fn uploads(
        &self,
        bucket: Uuid,
        access: &str,
        prefix: &str,
        after: &str,
        inclusive: bool,
        id: Option<Uuid>,
    ) -> Result<Vec<Upload>>;
    async fn touch(&self, upload: Uuid) -> Result<()>;
    async fn transfers(
        &self,
        principal: &Principal,
        user: Uuid,
        filter: &TransferFilter,
        after: Option<(DateTime<Utc>, Uuid)>,
        id: Option<Uuid>,
        limit: i64,
    ) -> Result<(Vec<Transfer>, bool)>;
    async fn bucket_name(&self, bucket: Uuid) -> Result<String>;
    async fn web_parts(&self, upload: Uuid, after: i32) -> Result<Vec<ReceivedPart>>;
}
