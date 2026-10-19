use crate::content::{codec::Chunk, pack::Pack};
use anyhow::Result;
use async_trait::async_trait;
use uuid::Uuid;

pub(crate) type StagedUpload = (i64, Option<Uuid>, Option<String>, i64);

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

    async fn upload_pins(&self, after: i64) -> Result<Vec<StagedUpload>>;
    async fn pending_source(&self, chunk: i64) -> Result<Option<(Uuid, String)>>;
    async fn pending_only(&self, chunk: i64) -> Result<bool>;
    async fn register_encoding(&self, chunk: &Chunk) -> Result<()>;
    async fn source(&self, chunk: i64, inspection: bool) -> Result<PhysicalSource>;
    async fn source_snapshot(
        &self,
        scope: &crate::model::source::SourceScope,
    ) -> Result<crate::model::source::SourceSnapshot>;
    async fn quarantines(
        &self,
        storage: Uuid,
        packed: bool,
        after: i64,
        limit: i64,
    ) -> Result<Vec<crate::model::source::QuarantinedSource>>;
    async fn source_update(&self, update: crate::model::source::SourceUpdate<'_>) -> Result<()>;
    async fn ready_chunks(&self, ids: &[i64]) -> Result<Vec<Chunk>>;
    async fn pack_contains(&self, pack: i64, chunk: i64) -> Result<bool>;
    async fn inspection(&self, task: Uuid, pack: i64) -> Result<Option<Option<String>>>;
    async fn inspection_members(&self, pack: i64) -> Result<Vec<(Chunk, i64)>>;
    async fn record_inspection(&self, task: Uuid, pack: i64, code: Option<&str>) -> Result<()>;
}
