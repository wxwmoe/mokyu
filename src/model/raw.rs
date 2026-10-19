use anyhow::{Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, sqlx::FromRow)]
pub(crate) struct RawSource {
    #[sqlx(rename = "raw_id")]
    pub id: Uuid,
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

#[async_trait]
pub(crate) trait RawStore: Send + Sync {
    async fn begin(&self, stream: Uuid, storage: Uuid, profile_revision: i64) -> Result<Uuid>;
    async fn finish(&self, stream: Uuid, source: Uuid, size: i64, digest: &[u8]) -> Result<()>;
    async fn set_upload(&self, source: Uuid, upload: &str) -> Result<()>;
    async fn upload_id(&self, source: Uuid) -> Result<Option<String>>;
    async fn deduplicate(&self, stream: Uuid, source: Uuid, hash: &[u8]) -> Result<bool>;
    async fn page(&self, stream: Uuid, start: i64, end: i64) -> Result<Vec<RawRange>>;
    async fn quarantines(
        &self,
        storage: Uuid,
        after: Uuid,
        limit: i64,
    ) -> Result<Vec<crate::model::source::QuarantinedSource>>;
    async fn candidates(
        &self,
        storage: Uuid,
        grace: i64,
        minimum: i64,
        limit: i64,
    ) -> Result<Vec<RawSource>>;
    async fn claim(&self, source: Uuid, revision: i64, grace: i64, minimum: i64) -> Result<bool>;
    async fn removed(&self, source: Uuid) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unavailable_sources_and_overflowing_ranges() {
        let mut range = RawRange {
            source: RawSource {
                id: Uuid::new_v4(),
                storage_id: Uuid::new_v4(),
                size: 1024,
                digest: Some(vec![0; 16]),
                state: "ready".into(),
                medium: "local".into(),
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
