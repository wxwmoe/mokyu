use crate::model::Id;
use crate::{
    app::{Bucket, StoredStream},
    authorization::Permit,
};
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use uuid::Uuid;

#[cfg(test)]
#[path = "../../tests/recovery/http_compression.rs"]
pub(crate) mod compression_tests;

pub(crate) struct StreamCreate {
    pub(crate) authority: Permit,
    pub(crate) bucket: Uuid,
    pub(crate) key: String,
    pub(crate) stream_type: String,
    pub(crate) metadata: Value,
    pub(crate) public: bool,
    pub(crate) claim_object: bool,
    pub(crate) completion: Option<Uuid>,
    pub(crate) owner: Option<(Uuid, Uuid)>,
}

pub(crate) struct StreamTail {
    pub(crate) fragment: Uuid,
    pub(crate) stream: Id,
    pub(crate) offset: i64,
    pub(crate) size: i32,
    pub(crate) hash: [u8; 32],
    pub(crate) staging_storage: Option<Uuid>,
    pub(crate) staging_key: Option<String>,
    pub(crate) file: Option<Uuid>,
    pub(crate) file_offset: Option<i64>,
}

#[derive(sqlx::FromRow)]
pub(crate) struct CachedExtent {
    pub cache_offset: Option<i64>,
    pub cache_length: Option<i32>,
    pub cache_source: Option<i32>,
    pub cache_chunk: Option<Id>,
    pub cache_ready: Option<Id>,
    pub cache_size: Option<i32>,
    pub cache_hash: Option<Vec<u8>>,
}

pub(crate) struct CachedStream {
    pub stream: StoredStream,
    pub bucket_revision: i64,
    pub bucket_active: bool,
    pub extents: Vec<CachedExtent>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) enum ReadRoute {
    Bucket {
        name: String,
        key: String,
        alias: Option<(String, String)>,
    },
    Host {
        host: String,
        key: String,
    },
}

impl ReadRoute {
    pub fn key<'a>(&'a self, bucket: &Bucket) -> &'a str {
        match self {
            Self::Bucket { key, alias, .. } => alias
                .as_ref()
                .filter(|(name, _)| name == &bucket.name)
                .map_or(key.as_str(), |(_, key)| key),
            Self::Host { key, .. } => key,
        }
    }
}

#[derive(Clone)]
pub(crate) struct ReadSnapshot {
    pub bucket: Bucket,
    pub stream: Option<StoredStream>,
    pub raw: Option<crate::model::raw::RawRange>,
    pub secret: Option<Vec<u8>>,
    pub allowed: bool,
}

#[derive(Clone, Copy)]
pub(crate) struct CompressionObservation {
    pub stream: Id,
    pub version: i32,
    pub encoding: &'static str,
    pub size: i64,
}

impl CompressionObservation {
    pub fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            self.version > 0 && self.size >= 0 && matches!(self.encoding, "zstd" | "br" | "gzip"),
            "invalid HTTP compression observation"
        );
        Ok(())
    }
}

#[async_trait]
pub(crate) trait StreamStore: Send + Sync {
    async fn snapshot(
        &self,
        route: &ReadRoute,
        access: Option<&str>,
    ) -> Result<Option<ReadSnapshot>>;
    async fn current(&self, bucket: Uuid, key: &str) -> Result<StoredStream>;
    async fn cached(&self, bucket: Uuid, key: &str) -> Result<Option<CachedStream>>;
    async fn create(&self, input: StreamCreate) -> Result<Id>;
    async fn append_tail(&self, tail: StreamTail) -> Result<()>;
    async fn finish(&self, stream: Id, size: i64, etag: &str, checksums: Value) -> Result<bool>;
    async fn learn_compression(
        &self,
        observation: CompressionObservation,
        node: Option<Uuid>,
    ) -> Result<bool>;
}
