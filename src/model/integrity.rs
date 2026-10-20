use crate::model::Id;
use crate::model::task::Execution;
use crate::{
    app::{Bucket, StoredStream},
    content::codec::Chunk,
    worker::integrity::{Mode, Request},
};
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Deserialize, Serialize)]
pub(crate) struct Detail {
    #[serde(default)]
    pub(crate) storage_id: Option<Uuid>,
    pub(crate) mode: Mode,
    pub(crate) phase: String,
    pub(crate) bucket_id: Option<Uuid>,
    pub(crate) bucket: Option<String>,
    pub(crate) key: Option<String>,
    pub(crate) upper_chunk_id: String,
    pub(crate) upper_object: Option<(Uuid, String)>,
    pub(crate) objects_checked: u64,
    pub(crate) chunks_checked: u64,
    pub(crate) bytes_checked: u64,
    pub(crate) issues: u64,
    pub(crate) skipped: u64,
    pub(crate) finished_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) storages: Vec<StorageSnapshot>,
    #[serde(default)]
    pub(crate) storage_index: usize,
}

#[derive(Deserialize, Serialize)]
pub(crate) struct StorageSnapshot {
    pub(crate) storage_id: Option<Uuid>,
    pub(crate) upper_chunk_id: String,
    pub(crate) upper_object: Option<(Uuid, String)>,
}
#[derive(Default, Deserialize, Serialize)]
pub(crate) struct Cursor {
    pub(crate) bucket: Option<Uuid>,
    pub(crate) key: Option<String>,
    pub(crate) stream: Option<Id>,
    pub(crate) after_extent: i64,
    pub(crate) position: i64,
    pub(crate) chunk: Id,
}
pub(crate) struct Finding {
    pub(crate) subject: String,
    pub(crate) code: String,
    pub(crate) chunk_id: Option<Id>,
    pub(crate) location_id: Option<Uuid>,
    pub(crate) stream_id: Option<Id>,
    pub(crate) bucket_id: Option<Uuid>,
    pub(crate) key: Option<String>,
    pub(crate) detail: Value,
}

pub(crate) struct MappingRange {
    pub(crate) offset_bytes: i64,
    pub(crate) length: i32,
    pub(crate) source_offset: i32,
    pub(crate) fragment_id: Option<Id>,
    pub(crate) chunk_id: Option<Id>,
    pub(crate) state: Option<String>,
    pub(crate) raw_size: Option<i32>,
}

#[async_trait]
pub(crate) trait IntegrityStore: Send + Sync {
    async fn start(
        &self,
        input: Request,
        bucket: Option<Bucket>,
        node: Option<Uuid>,
    ) -> Result<(Uuid, usize)>;
    async fn chunks(&self, detail: &Detail, after: Id, limit: i64) -> Result<Vec<Chunk>>;
    async fn mapping(
        &self,
        detail: &Detail,
        cursor: &Cursor,
    ) -> Result<Option<(StoredStream, Uuid, String)>>;
    async fn ranges(&self, stream: Id, after: i64) -> Result<Vec<MappingRange>>;
    async fn current(&self, bucket: Uuid, key: &str, stream: Id) -> Result<bool>;
    async fn chunk_stream(&self, chunk: Id, detail: &Detail) -> Result<Option<Id>>;
    async fn checkpoint(
        &self,
        id: Execution,
        cursor: String,
        detail: Detail,
        issues: Vec<Finding>,
    ) -> Result<()>;
    async fn exists(&self, task: Uuid) -> Result<bool>;
    async fn report_task(&self, task: Uuid) -> Result<Option<Value>>;
    async fn report_upper(&self, task: Uuid) -> Result<Id>;
    async fn report_page(&self, task: Uuid, after: Id, upper: Id) -> Result<Vec<Value>>;
    async fn findings(
        &self,
        task: Uuid,
        after: Id,
        limit: i64,
        code: Option<&str>,
    ) -> Result<Vec<Value>>;
    async fn groups(&self, task: Uuid) -> Result<Vec<(String, String)>>;
    async fn objects(
        &self,
        task: Uuid,
        issue: Id,
        after: Option<(Uuid, String)>,
    ) -> Result<Vec<Value>>;
}
