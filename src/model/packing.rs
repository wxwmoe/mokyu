use crate::model::Id;
use crate::model::task::Execution;
use crate::{
    content::{codec::Chunk, pack::Pack},
    model::Mapping,
};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use uuid::Uuid;
pub(crate) struct PackOutput {
    pub(crate) chunks: Vec<Id>,
    pub(crate) pack: Option<Pack>,
    pub(crate) independent: Option<Chunk>,
}
impl PackOutput {
    pub(crate) fn size(&self) -> i64 {
        self.pack
            .as_ref()
            .and_then(|p| p.stored_size)
            .or_else(|| {
                self.independent
                    .as_ref()
                    .and_then(|c| c.stored_size.map(i64::from))
            })
            .unwrap_or(0)
    }
}

pub(crate) struct PackCandidate {
    pub(crate) row: Mapping,
    pub(crate) eligible: bool,
    pub(crate) unit_count: i64,
}

#[async_trait]
pub(crate) trait PackingStore: Send + Sync {
    async fn in_storage(&self, pack: Id, storage: Uuid) -> Result<bool>;

    async fn scoped_page(
        &self,
        principal: &Principal,
        scope: &ScopeQuery,
        after: Id,
        limit: i64,
        state: Option<&str>,
    ) -> Result<Vec<PackSummary>>;
    async fn scoped_detail(
        &self,
        principal: &Principal,
        scope: &ScopeQuery,
        id: Id,
        after: Id,
        limit: i64,
    ) -> Result<(PackSummary, Vec<PackMember>, bool)>;
    async fn scoped_objects(
        &self,
        principal: &Principal,
        scope: &ScopeQuery,
        id: Id,
        cursor: Option<(Uuid, String)>,
    ) -> Result<Vec<PackObject>>;

    async fn page(&self, after: Id, limit: i64) -> Result<Vec<Value>>;
    async fn detail(&self, id: Id) -> Result<(Value, Vec<Value>)>;
    async fn rewrite_counts(
        &self,
        storage: Uuid,
        algorithm: &str,
        key: Option<Uuid>,
    ) -> Result<(i64, i64)>;
    async fn start_rewrite(&self, job: &RewriteTask, node: Option<Uuid>) -> Result<(Uuid, bool)>;
    async fn rewrite_candidates(
        &self,
        after: Id,
        job: &RewriteTask,
    ) -> Result<(Option<Chunk>, Option<(Id, Id)>)>;
    async fn rewrite_pack_inputs(&self, pack: Id) -> Result<(Pack, Vec<Chunk>, Vec<Id>)>;
    async fn commit_rewrite_chunk(
        &self,
        task: Uuid,
        token: Uuid,
        source: &Chunk,
        physical: &Chunk,
    ) -> Result<()>;
    async fn commit_rewrite_pack(
        &self,
        task: Uuid,
        token: Uuid,
        old: &Pack,
        new: &Pack,
        current: &[Id],
        members: &[Chunk],
        first: Id,
    ) -> Result<()>;
    async fn complete_rewrite(&self, task: Uuid, token: Uuid, after: Id) -> Result<()>;

    async fn pack_totals(&self) -> Result<Vec<Value>>;
    async fn unpack_totals(&self, pack: Option<Id>) -> Result<(i64, i64)>;
    async fn unpack_identity(&self, pack: Option<Id>) -> Result<(String, String)>;
    async fn preparing_packs(&self) -> Result<bool>;
    async fn ready_encoding(&self, chunk: Id) -> Result<Option<(Id, Uuid, i32)>>;
    async fn prepare_encoding(&self, physical: &Chunk, task: Execution) -> Result<Id>;
    async fn encoding_metadata(&self, physical: &Chunk) -> Result<()>;
    async fn prepare_pack(
        &self,
        storage: Uuid,
        algorithm: &str,
        key: Option<Uuid>,
        bytes: i64,
        count: i32,
        task: Execution,
    ) -> Result<Pack>;
    async fn discard_pack(&self, pack: Id) -> Result<()>;
    async fn pack_metadata(&self, pack: &Pack) -> Result<()>;
    async fn pack_has_pending(&self, pack: Id) -> Result<bool>;
    async fn pack_layout(&self, pack: Id) -> Result<Vec<Mapping>>;
    async fn pack_has_writers(&self, pack: Id) -> Result<bool>;
    async fn pack_raw_size(&self, pack: Id) -> Result<i64>;
    async fn chunk_has_references(&self, chunk: Id) -> Result<bool>;
    async fn pack_stored_size(&self, pack: Id) -> Result<i64>;
    async fn pack_candidates(
        &self,
        stream: Id,
        offset: i64,
        cooldown: f64,
    ) -> Result<Vec<PackCandidate>>;
    async fn pack_member_count(&self, pack: Id) -> Result<i64>;
    async fn pack_stream_metadata(&self, stream: Id) -> Result<(Value, String)>;
    async fn physical_bytes(&self, ids: &[Id]) -> Result<i64>;
    async fn pause_pack_task(&self, task: Execution) -> Result<()>;
    async fn next_pack(
        &self,
        after: Id,
        pack: Option<Id>,
        upper: Option<DateTime<Utc>>,
        storage: Option<Uuid>,
    ) -> Result<Option<Id>>;
    async fn next_pack_stream(
        &self,
        after: Id,
        offset: i64,
        upper: DateTime<Utc>,
        storage: Option<Uuid>,
    ) -> Result<Option<Id>>;
    async fn advance_pack_task(
        &self,
        task: Execution,
        next: &Option<String>,
        processed: i64,
    ) -> Result<()>;
    async fn next_pack_change(
        &self,
        cutoff: DateTime<Utc>,
        policy: &str,
        storage: Option<Uuid>,
    ) -> Result<Option<(Id, i64)>>;
    async fn finish_pack_change(&self, task: Execution, pack: Id, generation: i64) -> Result<()>;
    async fn next_pack_queue(
        &self,
        cutoff: DateTime<Utc>,
        policy: &str,
        storage: Option<Uuid>,
    ) -> Result<Option<(Id, i64, i64)>>;
    async fn stream_repack_after(&self, stream: Id, cooldown: f64)
    -> Result<Option<DateTime<Utc>>>;
    async fn defer_pack_queue(
        &self,
        task: Execution,
        stream: Id,
        generation: i64,
        later: DateTime<Utc>,
    ) -> Result<()>;
    async fn finish_pack_queue(&self, task: Execution, stream: Id, generation: i64) -> Result<()>;
    async fn advance_pack_queue(
        &self,
        task: Execution,
        stream: Id,
        generation: i64,
        cursor: i64,
    ) -> Result<()>;
    async fn pin_pack_inputs(&self, task: Execution, rows: &[Mapping]) -> Result<()>;

    async fn start_pack(&self, policy: &str, automatic: bool, node: Option<Uuid>) -> Result<Value>;
    async fn start_unpack(&self, pack: Option<Id>, all: bool, node: Option<Uuid>) -> Result<Value>;
    async fn insert_pack_members(&self, pack: Id, rows: &[Mapping]) -> Result<()>;
    async fn commit_pack_outputs(
        &self,
        task: Execution,
        rows: &[Mapping],
        outputs: &[PackOutput],
        split: bool,
        settings: &crate::content::pack::Config,
        flush_seconds: f64,
    ) -> Result<()>;
    async fn same_references(&self, a: Id, b: Id, distance: i64) -> Result<bool>;
    async fn next_range_pack(
        &self,
        storage: Uuid,
        cutoff: DateTime<Utc>,
        window: f64,
        minimum: i64,
    ) -> Result<Option<Id>>;
    async fn complete_range_task(&self, task: Execution) -> Result<()>;
    async fn range_pack_blocked(&self, pack: Id) -> Result<bool>;
    async fn pack_range_totals(&self, pack: Id, window: f64) -> Result<(i64, i64, i64)>;
    async fn pack_member_hits(&self, pack: Id, window: f64) -> Result<Vec<(Id, i64)>>;
    async fn pack_dimensions(&self, pack: Id) -> Result<(i64, i64, i32)>;
    async fn range_pack_layout(&self, pack: Id) -> Result<Vec<Mapping>>;
    async fn defer_range_repack(
        &self,
        stream: Id,
        window: f64,
        flush: f64,
        retry: f64,
        offset: i64,
    ) -> Result<()>;
    async fn record_range(
        &self,
        task: Execution,
        id: Id,
        processed: usize,
        decision: Value,
    ) -> Result<()>;
}

#[derive(Clone, serde::Deserialize)]
pub(crate) struct RewriteTask {
    pub(crate) storage_id: Uuid,
    #[serde(default)]
    pub(crate) upper_chunk_id: Id,
    #[serde(default)]
    pub(crate) shard: i64,
    #[serde(default = "one_shard")]
    pub(crate) shards: i64,
    #[serde(default)]
    pub(crate) profile_revision: i64,
    pub(crate) target_key_id: Option<Uuid>,
    pub(crate) target_key: String,
    pub(crate) target_algorithm: String,
}

fn one_shard() -> i64 {
    1
}

#[derive(Serialize, Deserialize, ToSchema)]
pub(crate) struct PackSummary {
    pub(crate) id: String,
    #[serde(rename = "status")]
    pub(crate) state: String,
    pub(crate) compressed: bool,
    pub(crate) raw_size: Option<String>,
    pub(crate) stored_size: Option<String>,
    pub(crate) member_count: Option<i32>,
    pub(crate) created_at: DateTime<Utc>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct PackMember {
    pub(crate) chunk_id: String,
    pub(crate) ordinal: Option<i32>,
    pub(crate) raw_size: Option<i32>,
    pub(crate) visible_bytes: String,
    pub(crate) current_source: bool,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct PackObject {
    pub(crate) bucket_id: Uuid,
    pub(crate) bucket_name: String,
    pub(crate) key: String,
    pub(crate) version: Id,
    pub(crate) size: String,
    pub(crate) content_type: String,
    pub(crate) public_read: bool,
}

use crate::{authorization::Principal, model::metrics::ScopeQuery};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
