use crate::{app::Extent, content::codec::Chunk};
use anyhow::Result;
use async_trait::async_trait;
use uuid::Uuid;

pub(crate) struct ChunkCreate {
    pub(crate) storage: Uuid,
    pub(crate) stream: Uuid,
    pub(crate) hash: [u8; 32],
    pub(crate) raw_size: i32,
    pub(crate) algorithm: String,
    pub(crate) key: Option<Uuid>,
}

pub(crate) struct ReadExtent {
    pub(crate) extent: Extent,
    pub(crate) chunk: Chunk,
}

pub(crate) struct FragmentSource {
    pub(crate) size: i32,
    pub(crate) hash: Vec<u8>,
    pub(crate) staging_storage_id: Option<Uuid>,
    pub(crate) staging_object_key: Option<String>,
}

#[async_trait]
pub(crate) trait ChunkStore: Send + Sync {
    async fn storage_for_stream(&self, stream: Uuid) -> Result<Option<Uuid>>;
    async fn extents_for_copy(&self, stream: Uuid, from: i64) -> Result<Vec<Extent>>;
    async fn ready_chunk(&self, id: i64) -> Result<Option<Chunk>>;
    async fn fragment(&self, id: Uuid) -> Result<FragmentSource>;
    async fn extents(&self, stream: Uuid, from: i64, until: i64) -> Result<Vec<Extent>>;
    async fn read_extents(&self, stream: Uuid, from: i64, until: i64) -> Result<Vec<ReadExtent>>;
    async fn read_start(&self, stream: Uuid, position: i64) -> Result<Option<i64>>;
    /// The caller serializes this storage/hash until its write or staging finishes.
    async fn allocate(&self, input: ChunkCreate) -> Result<(Chunk, bool)>;
    async fn can_stage_reuse(&self, id: i64) -> Result<bool>;
    async fn mark_uploading(&self, chunk: &Chunk) -> Result<()>;
    async fn reference(
        &self,
        stream: Uuid,
        offset: i64,
        chunk_id: i64,
        raw_size: i32,
    ) -> Result<()>;
    async fn complete(&self, stream: Uuid, offset: i64, chunk: &Chunk) -> Result<()>;
    async fn copy_references(
        &self,
        destination: Uuid,
        source: Uuid,
        offset: i64,
        end: i64,
        chunks: &[i64],
    ) -> Result<()>;
}

#[derive(Clone)]
pub(crate) struct Mapping {
    pub(crate) chunk: Chunk,
    pub(crate) offset_bytes: i64,
    pub(crate) pack_id: Option<i64>,
}
