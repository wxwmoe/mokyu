use crate::{app::StoredStream, authorization::Permit};
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use uuid::Uuid;

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
    pub(crate) lease_seconds: i64,
}

pub(crate) struct StreamTail {
    pub(crate) fragment: Uuid,
    pub(crate) stream: Uuid,
    pub(crate) offset: i64,
    pub(crate) size: i32,
    pub(crate) hash: [u8; 32],
    pub(crate) staging_storage: Option<Uuid>,
    pub(crate) staging_key: Option<String>,
}

#[async_trait]
pub(crate) trait StreamStore: Send + Sync {
    async fn current(
        &self,
        bucket: Uuid,
        key: &str,
        lease_seconds: i64,
    ) -> Result<(StoredStream, Uuid)>;
    async fn create(&self, input: StreamCreate) -> Result<Uuid>;
    async fn append_tail(&self, tail: StreamTail) -> Result<()>;
    async fn finish(&self, stream: Uuid, size: i64, etag: &str, checksums: Value) -> Result<bool>;
}

/// Persistent stream lease operations shared by object readers and writers.
#[async_trait]
pub(crate) trait StreamLeaseStore: Send + Sync {
    async fn renew_read(&self, id: Uuid, ttl_seconds: i64) -> Result<()>;
    async fn release_read(&self, id: Uuid) -> Result<()>;
    async fn renew_write(&self, stream: Uuid, ttl_seconds: i64) -> Result<()>;
}
