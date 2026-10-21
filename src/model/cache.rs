use crate::model::Id;
use crate::model::task::Execution;
use crate::{content::codec::Chunk, model::Mapping};
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use uuid::Uuid;

pub(crate) struct CacheStage<'a> {
    pub chunk: &'a Chunk,
    pub stream: Id,
    pub offset: i64,
    pub new: bool,
    pub size: i64,
    pub storage: Uuid,
    pub key: &'a str,
    pub timeout: u64,
    pub node: crate::model::raw::Processor<'a>,
}

pub(crate) enum CacheBatch {
    Wait,
    Finished,
    Release(Id),
    Rewrite {
        stream: Option<Id>,
        pack: Id,
        ids: Vec<Id>,
    },
    // Selection has durably claimed the inputs, pinned them and saved their source revisions.
    Encode {
        stream: Option<Id>,
        expired: bool,
        rows: Vec<Mapping>,
    },
}

pub(crate) struct CacheRelease {
    pub keys: Vec<String>,
    pub staged: Vec<(Uuid, String)>,
}

#[async_trait]
pub(crate) trait UploadCacheStore: Send + Sync {
    async fn bypass(&self, stream: Id) -> Result<bool>;
    async fn disable(&self, stream: Id) -> Result<()>;
    async fn stage(&self, input: CacheStage<'_>) -> Result<bool>;
    async fn seal(&self, stream: Id, eof: bool) -> Result<bool>;
    async fn status(&self) -> Result<(Value, Vec<Value>)>;
    async fn flush(&self) -> Result<(Uuid, bool)>;
    async fn enqueue(&self) -> Result<bool>;
    async fn next_attempt(&self) -> Result<Option<chrono::DateTime<chrono::Utc>>>;
    async fn pause(&self, task: Execution) -> Result<()>;
    async fn batch(&self, task: Execution, detail: &Value, flush: bool) -> Result<CacheBatch>;
    async fn metadata(&self, stream: Id) -> Result<Option<(Value, String)>>;
    async fn processed(&self, task: Execution) -> Result<()>;
    async fn finish(&self, task: Execution) -> Result<()>;
    async fn release(&self, ids: &[Id], existing_pack: bool) -> Result<CacheRelease>;

    async fn pins(&self, names: &[String], storage: Option<Uuid>) -> Result<Vec<String>>;
    async fn unused(&self, storage: Option<Uuid>) -> Result<Vec<Id>>;
}
