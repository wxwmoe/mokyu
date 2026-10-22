use crate::content::{codec::Chunk, pack::Pack};
use crate::model::Id;
use anyhow::Result;
use async_trait::async_trait;
use uuid::Uuid;

pub(crate) type StagedUpload = (Id, Option<Uuid>, Option<String>, i64, Uuid);

#[derive(Clone, serde::Deserialize, serde::Serialize)]
#[serde(tag = "source", content = "metadata", rename_all = "snake_case")]
pub(crate) enum PhysicalSource {
    Chunk(Chunk),
    Pack(Pack),
    Raw(crate::model::raw::RawSource),
}

#[async_trait]
pub(crate) trait PhysicalStore: Send + Sync {
    async fn has_chunks(&self) -> Result<bool>;
    async fn indexed(&self, location: Uuid) -> Result<bool>;

    async fn upload_pins(&self, after: Id, owner: Option<Uuid>) -> Result<Vec<StagedUpload>>;
    async fn pending_source(&self, chunk: Id) -> Result<Option<(Uuid, String)>>;
    async fn pending_only(&self, chunk: Id) -> Result<bool>;
    async fn register_encoding(&self, chunk: &Chunk) -> Result<()>;
    async fn recover_memory(&self, boot: Uuid) -> Result<()>;
    async fn buffer_chunk(&self, chunk: &Chunk, boot: Option<Uuid>) -> Result<()>;
    async fn buffered_chunks(
        &self,
        after: Id,
        before: Option<chrono::DateTime<chrono::Utc>>,
        stream: Option<Id>,
        chunk: Option<Id>,
        files_only: bool,
    ) -> Result<Vec<Chunk>>;
    async fn synced_chunk(&self, chunk: &Chunk, failure: Option<&str>) -> Result<()>;
    async fn buffered_status(&self) -> Result<serde_json::Value>;
    async fn source(&self, chunk: Id, inspection: bool) -> Result<PhysicalSource>;
    async fn source_snapshot(
        &self,
        scope: &crate::model::source::SourceScope,
    ) -> Result<crate::model::source::SourceSnapshot>;
    async fn quarantines(
        &self,
        storage: Uuid,
        packed: bool,
        after: Id,
        limit: i64,
    ) -> Result<Vec<crate::model::source::QuarantinedSource>>;
    async fn source_update(&self, update: crate::model::source::SourceUpdate<'_>) -> Result<()>;
    async fn pack_chunks(&self, pack: Id) -> Result<Vec<Chunk>>;
    async fn pack_contains(&self, pack: Id, chunk: Id) -> Result<bool>;
    async fn inspection(&self, task: Uuid, pack: Id) -> Result<Option<Option<String>>>;
    async fn inspection_members(&self, pack: Id) -> Result<Vec<(Chunk, i64)>>;
    async fn record_inspection(&self, task: Uuid, pack: Id, code: Option<&str>) -> Result<()>;
}
