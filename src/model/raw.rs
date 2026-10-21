use crate::model::Id;
use anyhow::{Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(crate) type Processor<'a> = Option<(
    &'a crate::cluster::NodeState,
    &'a str,
    &'a crate::content::codec::Key,
)>;

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, sqlx::FromRow)]
pub(crate) struct RawSource {
    #[sqlx(rename = "raw_id")]
    pub id: Id,
    #[sqlx(rename = "raw_location")]
    pub location_id: Uuid,
    #[sqlx(rename = "raw_storage")]
    pub storage_id: Uuid,
    #[sqlx(rename = "raw_size")]
    pub size: i64,
    #[sqlx(rename = "raw_digest")]
    pub digest: Option<Vec<u8>>,
    #[sqlx(rename = "raw_state")]
    pub state: String,
    #[sqlx(rename = "raw_medium")]
    pub medium: String,
    #[serde(default)]
    #[sqlx(rename = "raw_pending")]
    pub pending_upload: bool,
    #[sqlx(rename = "raw_revision")]
    pub revision: i64,
    #[sqlx(rename = "raw_quarantine")]
    pub quarantine_code: Option<String>,
}

impl RawSource {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.state == "ready" && self.quarantine_code.is_none(),
            crate::model::Error::SourceUnavailable
        );
        ensure!(
            self.size >= 0
                && self
                    .digest
                    .as_ref()
                    .is_some_and(|digest| digest.len() == 16),
            crate::content::codec::IntegrityError::RawMetadata
        );
        Ok(())
    }
}

#[derive(Clone, Debug, sqlx::FromRow)]
pub(crate) struct RawRange {
    #[sqlx(flatten)]
    pub source: RawSource,
    pub offset_bytes: i64,
    pub length: i64,
    pub source_offset: i64,
}

#[derive(Clone, Debug, sqlx::FromRow)]
pub(crate) struct RawProcessing {
    pub stream: Id,
    pub source: Id,
    pub bucket: Uuid,
    pub object_key: String,
    pub payload_revision: i64,
    pub content_hash: Option<Vec<u8>>,
    pub source_revision: Option<i64>,
    pub token: Uuid,
}

impl RawRange {
    pub fn validate(&self) -> Result<()> {
        self.source.validate()?;
        ensure!(
            self.offset_bytes >= 0
                && self.length >= 0
                && self.source_offset >= 0
                && self
                    .source_offset
                    .checked_add(self.length)
                    .is_some_and(|end| end <= self.source.size)
                && self.offset_bytes.checked_add(self.length).is_some(),
            crate::content::codec::IntegrityError::RawMetadata
        );
        Ok(())
    }
}

#[derive(Clone, Debug, sqlx::FromRow)]
pub(crate) struct RawUpload {
    #[sqlx(flatten)]
    pub source: RawSource,
    pub token: Option<Uuid>,
    pub deduplicate: bool,
    pub background: bool,
    pub content_hash: Option<Vec<u8>>,
}

#[async_trait]
pub(crate) trait RawStore: Send + Sync {
    async fn claim_processing(
        &self,
        token: Uuid,
        node: Processor<'_>,
    ) -> Result<Option<RawProcessing>>;
    async fn renew_processing(&self, work: &RawProcessing, node: Processor<'_>) -> Result<bool>;
    async fn save_processing_hash(
        &self,
        work: &RawProcessing,
        revision: i64,
        hash: &[u8],
    ) -> Result<()>;
    async fn retry_processing(&self, work: &RawProcessing, error: Option<&str>) -> Result<()>;
    async fn cancel_processing(&self, work: &RawProcessing) -> Result<()>;
    async fn finish_processing(
        &self,
        work: &RawProcessing,
        source: &RawSource,
        hash: &[u8],
        lease: Option<Uuid>,
        node: Processor<'_>,
    ) -> Result<bool>;
    async fn processing_status(&self) -> Result<serde_json::Value>;
    async fn stage_upload(
        &self,
        stream: Id,
        source: Id,
        size: i64,
        digest: &[u8],
        revision: i64,
        deduplicate: bool,
        background: bool,
    ) -> Result<()>;
    async fn uploads(&self, after: Id) -> Result<Vec<RawUpload>>;
    async fn claim_upload(
        &self,
        token: Uuid,
        before: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Option<RawUpload>>;
    async fn retry_upload(&self, work: &RawUpload, error: Option<&str>) -> Result<()>;
    async fn set_pending_upload(&self, source: Id, token: Uuid, upload: Option<&str>)
    -> Result<()>;
    async fn upload_streams(&self, source: Id) -> Result<Vec<Id>>;
    async fn reuse_upload(&self, work: &RawUpload, hash: &[u8], streams: &[Id]) -> Result<bool>;
    async fn complete_upload(&self, work: &RawUpload, hash: Option<&[u8]>) -> Result<()>;
    async fn current(&self, source: Id) -> Result<Option<RawSource>>;
    async fn cached_checkpoint(&self, location: Uuid) -> Result<Option<Id>>;
    async fn cached_checkpoints(&self, after: Uuid) -> Result<Vec<(Uuid, Id)>>;
    async fn has_upload(&self, source: Id) -> Result<bool>;
    async fn upload_status(&self) -> Result<serde_json::Value>;
    async fn uploads_before(&self, before: chrono::DateTime<chrono::Utc>) -> Result<bool>;
    async fn abandon_upload(&self, work: &RawUpload) -> Result<bool>;

    async fn reuse(
        &self,
        stream: Id,
        source: Id,
        storage: Uuid,
        size: i64,
        digest: &[u8],
        hash: &[u8],
    ) -> Result<bool>;
    async fn begin(&self, stream: Id, storage: Uuid, profile_revision: i64) -> Result<(Id, Uuid)>;
    async fn finish(&self, stream: Id, source: Id, size: i64, digest: &[u8]) -> Result<()>;
    async fn set_upload(&self, source: Id, upload: &str) -> Result<()>;
    async fn upload_id(&self, source: Id) -> Result<Option<String>>;
    async fn deduplicate(&self, stream: Id, source: Id, hash: &[u8]) -> Result<bool>;
    async fn page(&self, stream: Id, start: i64, end: i64) -> Result<Vec<RawRange>>;
    async fn quarantines(
        &self,
        storage: Uuid,
        after: Id,
        limit: i64,
    ) -> Result<Vec<crate::model::source::QuarantinedSource>>;
    async fn candidates(
        &self,
        storage: Uuid,
        grace: i64,
        minimum: i64,
        limit: i64,
    ) -> Result<Vec<RawSource>>;
    async fn claim(&self, source: Id, revision: i64, grace: i64, minimum: i64) -> Result<bool>;
    async fn removed(&self, source: Id) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unavailable_sources_and_overflowing_ranges() {
        let mut range = RawRange {
            source: RawSource {
                id: Id::from(1),
                location_id: Uuid::now_v7(),
                storage_id: Uuid::new_v4(),
                size: 1024,
                digest: Some(vec![0; 16]),
                state: "ready".into(),
                medium: "local".into(),
                pending_upload: false,
                revision: 1,
                quarantine_code: None,
            },
            offset_bytes: 0,
            length: 1024,
            source_offset: 0,
        };
        assert!(range.validate().is_ok());
        range.source_offset = 1;
        assert!(range.validate().is_err());
        range.source_offset = 0;
        range.offset_bytes = i64::MAX;
        assert!(range.validate().is_err());
        range.offset_bytes = 0;
        range.source.quarantine_code = Some("hash_mismatch".into());
        assert_eq!(
            crate::model::error(&range.validate().unwrap_err()),
            Some(crate::model::Error::SourceUnavailable)
        );
        range.source.quarantine_code = None;
        range.source.state = "deleting".into();
        assert!(range.validate().is_err());
        range.source.state = "ready".into();
        range.source.size = 0;
        range.length = 0;
        assert!(range.validate().is_ok());
        range.source.digest = None;
        assert!(range.validate().is_err());
    }
}
