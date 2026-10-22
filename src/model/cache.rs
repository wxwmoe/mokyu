use crate::model::Id;
use crate::model::task::Execution;
use crate::{content::codec::Chunk, model::Mapping};
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use uuid::Uuid;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EncodingPolicy {
    pub compression: crate::content::compression::Config,
    pub pack_enabled: bool,
    pub pack_max_size: String,
    pub pack_flush_timeout: String,
    pub algorithm: String,
    pub block_key_id: Option<Uuid>,
}

impl EncodingPolicy {
    pub(crate) fn pack_config(&self) -> crate::content::pack::Config {
        use crate::content::{
            compression::Strategy,
            pack::{CompressionStrategy, Config},
        };
        Config {
            enabled: self.pack_enabled,
            max_size: self.pack_max_size.clone(),
            upload_cache_timeout: self.pack_flush_timeout.clone(),
            compression_strategy: Some(if !self.compression.enabled {
                CompressionStrategy::Never
            } else {
                match self.compression.strategy {
                    Strategy::Never => CompressionStrategy::Never,
                    Strategy::Always => CompressionStrategy::Always,
                    Strategy::Sample => CompressionStrategy::Sample,
                }
            }),
            ..Default::default()
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        self.compression.validate()?;
        anyhow::ensure!(
            (crate::content::codec::MAX as u64..=crate::content::pack::MAX_RAW as u64)
                .contains(&crate::config::bytes(&self.pack_max_size)?),
            "invalid cached pack size"
        );
        crate::config::seconds(&self.pack_flush_timeout)?;
        anyhow::ensure!(
            (self.algorithm != "none") == self.block_key_id.is_some(),
            "invalid cached encryption policy"
        );
        Ok(())
    }
}

pub(crate) struct CacheStage<'a> {
    pub chunk: &'a Chunk,
    pub stream: Id,
    pub offset: i64,
    pub new: bool,
    pub size: i64,
    pub storage: Uuid,
    pub key: &'a str,
    pub timeout: u64,
    pub encoding: Option<&'a Value>,
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
        encoding: Option<EncodingPolicy>,
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
    async fn flush(&self, node: Option<Uuid>) -> Result<Uuid>;
    async fn backlog(&self, node: Option<Uuid>) -> Result<Value>;
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
