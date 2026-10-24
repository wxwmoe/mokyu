use super::{node_read, session::Run};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(crate) const LEASE_SECONDS: i64 = 90;
pub(crate) const MAX_ACTIVE: i64 = 4096;
pub(crate) const REQUEST_LIMIT: usize = 64 * 1024;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Intent {
    pub access: String,
    pub bucket: String,
    pub key: String,
    pub metadata: serde_json::Value,
    pub public: bool,
    pub size: Option<i64>,
}

impl Intent {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            !self.access.is_empty()
                && self.access.len() <= 256
                && !self.bucket.is_empty()
                && self.bucket.len() <= 255
                && !self.key.is_empty()
                && self.key.len() <= 1024
                && [&self.access, &self.bucket, &self.key]
                    .into_iter()
                    .all(|value| !value.contains('\0'))
                && self.size.is_none_or(|size| size >= 0)
                && self.metadata.is_object()
                && serde_json::to_vec(&self.metadata)?.len() <= 16 * 1024,
            "invalid write intent"
        );
        let metadata: crate::app::Metadata = serde_json::from_value(self.metadata.clone())?;
        ensure!(
            self.metadata
                .as_object()
                .unwrap()
                .keys()
                .all(|name| matches!(
                    name.as_str(),
                    "content_type"
                        | "cache_control"
                        | "content_disposition"
                        | "content_encoding"
                        | "content_language"
                        | "expires"
                        | "user"
                )),
            "unknown write metadata"
        );
        if let Some(user) = metadata.user {
            for (name, value) in user {
                ensure!(!name.is_empty(), "empty metadata name");
                axum::http::HeaderName::from_bytes(format!("x-amz-meta-{name}").as_bytes())?;
                axum::http::HeaderValue::from_str(&value)?;
            }
        }
        for value in [
            metadata.content_type,
            metadata.cache_control,
            metadata.content_disposition,
            metadata.content_encoding,
            metadata.content_language,
            metadata.expires,
        ]
        .into_iter()
        .flatten()
        {
            axum::http::HeaderValue::from_str(&value)?;
        }
        Ok(())
    }

    pub(crate) fn hash(&self) -> Result<Vec<u8>> {
        let canonical = serde_json::to_value(self)?;
        Ok(blake3::hash(&serde_json::to_vec(&canonical)?)
            .as_bytes()
            .to_vec())
    }
}

pub(crate) struct Begin {
    pub run: Run,
    pub operation: Uuid,
    pub intent: Intent,
    pub cached: Vec<node_read::Version>,
}

pub(crate) struct Lookup {
    pub run: Run,
    pub operation: Uuid,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    Active,
    Aborted,
    Expired,
}

pub(crate) struct Reply {
    pub operation: Uuid,
    pub stream: Option<Uuid>,
    pub phase: Phase,
    pub materials: Vec<node_read::Material>,
    pub storage: Option<node_read::Version>,
    pub grants: Vec<node_read::Grant>,
}

pub(crate) fn operation_time(id: Uuid) -> Result<chrono::DateTime<chrono::Utc>> {
    ensure!(id.get_version_num() == 7, "write operation requires UUIDv7");
    let (seconds, nanos) = id
        .get_timestamp()
        .ok_or_else(|| anyhow::anyhow!("write operation timestamp missing"))?
        .to_unix();
    chrono::DateTime::from_timestamp(i64::try_from(seconds)?, nanos)
        .ok_or_else(|| anyhow::anyhow!("write operation timestamp invalid"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_operations_bind_canonical_intent_and_timestamp() -> Result<()> {
        let intent = Intent {
            access: "writer".into(),
            bucket: "bucket".into(),
            key: "object".into(),
            metadata: serde_json::json!({"content_type":"text/plain","user":{"b":"2","a":"1"}}),
            public: false,
            size: Some(3),
        };
        intent.validate()?;
        let mut reordered = intent.clone();
        reordered.metadata =
            serde_json::from_str(r#"{"user":{"a":"1","b":"2"},"content_type":"text/plain"}"#)?;
        assert_eq!(intent.hash()?, reordered.hash()?);
        reordered.size = Some(4);
        assert_ne!(intent.hash()?, reordered.hash()?);
        reordered.size = Some(-1);
        assert!(reordered.validate().is_err());
        reordered.size = None;
        reordered.metadata = serde_json::json!({"user":{"name":"value\nheader"}});
        assert!(reordered.validate().is_err());
        reordered.metadata = serde_json::json!({"unrecognized":true});
        assert!(reordered.validate().is_err());
        reordered.metadata = serde_json::json!({"content_type":"text/plain\r\nx-injected: true"});
        assert!(reordered.validate().is_err());
        assert!(operation_time(Uuid::new_v4()).is_err());
        assert!(
            (chrono::Utc::now() - operation_time(Uuid::now_v7())?)
                .num_seconds()
                .abs()
                < 2
        );
        Ok(())
    }
}
