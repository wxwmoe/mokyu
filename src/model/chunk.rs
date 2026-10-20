use crate::model::Id;
use crate::{app::Extent, content::codec::Chunk};
use anyhow::Result;
use async_trait::async_trait;
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct ChunkCreate {
    pub(crate) storage: Uuid,
    pub(crate) stream: Id,
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
    pub(crate) location_id: Uuid,
    pub(crate) size: i32,
    pub(crate) hash: Vec<u8>,
    pub(crate) staging_storage_id: Option<Uuid>,
    pub(crate) staging_object_key: Option<String>,
    pub(crate) file_id: Option<Uuid>,
    pub(crate) file_offset: Option<i64>,
    pub(crate) file_storage: Option<Uuid>,
    pub(crate) raw_source: Option<Uuid>,
}

#[async_trait]
pub(crate) trait ChunkStore: Send + Sync {
    async fn storage_for_stream(&self, stream: Id) -> Result<Option<Uuid>>;
    async fn extents_for_copy(&self, stream: Id, from: i64) -> Result<Vec<Extent>>;
    async fn ready_chunk(&self, id: Id) -> Result<Option<Chunk>>;
    async fn fragment(&self, id: Id) -> Result<FragmentSource>;
    async fn extents(&self, stream: Id, from: i64, until: i64) -> Result<Vec<Extent>>;
    async fn read_extents(&self, stream: Id, from: i64, until: i64) -> Result<Vec<ReadExtent>>;
    async fn read_start(&self, stream: Id, position: i64) -> Result<Option<i64>>;
    /// The caller serializes this storage/hash until its write or staging finishes.
    async fn allocate(&self, input: ChunkCreate) -> Result<(Chunk, bool)>;
    async fn prepare_repair(
        &self,
        input: ChunkCreate,
        node: Option<crate::model::recovery::RepairNode>,
    ) -> Result<super::recovery::RepairAllocation>;
    async fn repair_metadata(&self, repair: &super::recovery::Repair) -> Result<()>;
    async fn finish_repair(&self, repair: &super::recovery::Repair, offset: i64) -> Result<Chunk>;
    async fn can_stage_reuse(&self, id: Id) -> Result<bool>;
    async fn mark_uploading(&self, chunk: &Chunk) -> Result<()>;
    async fn reference(&self, stream: Id, offset: i64, chunk_id: Id, raw_size: i32) -> Result<()>;
    async fn complete(&self, stream: Id, offset: i64, chunk: &Chunk) -> Result<()>;
    async fn copy_references(
        &self,
        destination: Id,
        source: Id,
        offset: i64,
        end: i64,
        chunks: &[Id],
    ) -> Result<()>;
}

#[derive(Clone)]
pub(crate) struct Mapping {
    pub(crate) chunk: Chunk,
    pub(crate) offset_bytes: i64,
    pub(crate) pack_id: Option<Id>,
}
