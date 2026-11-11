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

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamPurpose {
    Object,
    Mirror,
    Copy,
    Part,
    Internal,
}

impl StreamPurpose {
    pub(crate) fn stream_type(self) -> &'static str {
        if self == Self::Part { "part" } else { "object" }
    }

    pub(crate) fn publishes(self) -> bool {
        matches!(self, Self::Object | Self::Mirror | Self::Copy)
    }
}

pub(crate) struct StreamCreate {
    pub(crate) authority: Permit,
    pub(crate) bucket: Uuid,
    pub(crate) key: String,
    pub(crate) purpose: StreamPurpose,
    pub(crate) metadata: Value,
    pub(crate) public: bool,
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

impl StreamTail {
    pub(crate) fn validate_staging(&self) -> Result<()> {
        match (self.staging_storage, self.staging_key.as_deref()) {
            (None, None) => Ok(()),
            (Some(_), Some(key))
                if self.file.is_none() && key == crate::storage::fragment_key(self.fragment) =>
            {
                Ok(())
            }
            _ => anyhow::bail!("invalid fragment staging location"),
        }
    }
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
    Image {
        route: Box<ReadRoute>,
        request: crate::processing::image::request::Request,
    },
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
    pub fn base(&self) -> &Self {
        match self {
            Self::Image { route, .. } => route.base(),
            _ => self,
        }
    }
    pub fn image(&self) -> Option<&crate::processing::image::request::Request> {
        match self {
            Self::Image { request, .. } => Some(request),
            _ => None,
        }
    }
    pub fn bytes(&self) -> usize {
        match self {
            Self::Image { route, request } => {
                route.bytes() + serde_json::to_vec(request).map_or(usize::MAX / 2, |v| v.len()) * 4
            }
            Self::Host { host, key } => host.len() + key.len(),
            Self::Bucket { name, key, alias } => {
                name.len() + key.len() + alias.as_ref().map_or(0, |(a, b)| a.len() + b.len())
            }
        }
    }
    pub fn key<'a>(&'a self, bucket: &Bucket) -> &'a str {
        match self.base() {
            Self::Image { .. } => unreachable!(),
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
    pub image: Option<crate::processing::image::request::Selection>,
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
    async fn remote_pending(&self, stream: Id) -> Result<bool>;
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
