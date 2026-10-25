use super::{node_read, session::Run};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(crate) const LEASE_SECONDS: i64 = 90;
pub(crate) const MAX_ACTIVE: i64 = 4096;
pub(crate) const REQUEST_LIMIT: usize = 64 * 1024;
pub(crate) const MAX_PART_BYTES: i64 = 5 * 1024 * 1024 * 1024;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Intent {
    pub access: String,
    pub credential_revision: i64,
    pub bucket: String,
    pub key: String,
    pub metadata: serde_json::Value,
    pub public: bool,
    pub size: Option<i64>,
    pub copy_read: Option<Uuid>,
    pub part: Option<Part>,
    pub completion: Option<super::node_complete::Manifest>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Part {
    pub upload: Uuid,
    pub number: i32,
    pub algorithm: Option<String>,
}

impl Intent {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            !self.access.is_empty()
                && self.credential_revision >= 0
                && self.access.len() <= 256
                && !self.bucket.is_empty()
                && self.bucket.len() <= 255
                && !self.key.is_empty()
                && self.key.len() <= 1024
                && [&self.access, &self.bucket, &self.key]
                    .into_iter()
                    .all(|value| !value.contains('\0'))
                && self.size.is_none_or(|size| size >= 0)
                && self.copy_read.is_none_or(|id| !id.is_nil())
                && self.metadata.is_object()
                && serde_json::to_vec(&self.metadata)?.len() <= 16 * 1024,
            "invalid write intent"
        );
        if let Some(part) = &self.part {
            ensure!(
                !part.upload.is_nil()
                    && (1..=10000).contains(&part.number)
                    && self.copy_read.is_none()
                    && self.completion.is_none()
                    && !self.public
                    && self.metadata.as_object().unwrap().is_empty()
                    && self.size.is_none_or(|size| size <= MAX_PART_BYTES),
                "invalid multipart write"
            );
            crate::upload::multipart::checksum_type(part.algorithm.as_deref(), None)?;
        }
        if let Some(completion) = &self.completion {
            ensure!(
                self.part.is_none()
                    && self.copy_read.is_none()
                    && !self.public
                    && self.metadata.as_object().unwrap().is_empty()
                    && self.size.is_none(),
                "invalid completion write"
            );
            completion.validate()?;
        }
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

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct CopySource {
    pub read: Uuid,
    pub stream: super::Id,
    pub revision: i64,
    pub size: i64,
    pub etag: String,
    pub checksums: serde_json::Value,
    pub payload: String,
    pub storage: Uuid,
}

impl CopySource {
    pub(crate) fn references(&self, profile: &crate::storage::profile::StorageProfile) -> bool {
        self.payload == "chunks" && profile.cdc_enabled && self.storage == profile.storage_id
    }
}

pub(crate) struct CopyBatch {
    pub run: Run,
    pub operation: Uuid,
    pub batch: i64,
    pub offset: i64,
}

impl CopyBatch {
    pub(crate) fn hash(&self) -> Result<Vec<u8>> {
        ensure!(
            self.batch > 0 && self.batch < i64::MAX && self.offset >= 0,
            "invalid copy batch"
        );
        super::node_chunk::hash(&serde_json::json!(["copy", self.batch, self.offset]))
    }
}

pub(crate) struct Lookup {
    pub run: Run,
    pub operation: Uuid,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    Active,
    Busy,
    Aborted,
    Expired,
    Published,
}

pub(crate) struct Reply {
    pub operation: Uuid,
    pub bucket: Option<Uuid>,
    pub stream: Option<Uuid>,
    pub phase: Phase,
    pub materials: Vec<node_read::Material>,
    pub storage: Option<node_read::Version>,
    pub grants: Vec<node_read::Grant>,
    pub published: Option<Published>,
    pub copy_references: bool,
    pub checksum_algorithm: Option<String>,
    pub completion: Option<super::node_complete::Summary>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Published {
    pub size: i64,
    pub etag: String,
    pub modified: chrono::DateTime<chrono::Utc>,
}

pub(crate) struct Publish {
    pub run: Run,
    pub operation: Uuid,
    pub size: i64,
    pub etag: String,
    pub checksums: std::collections::BTreeMap<String, String>,
    pub if_match: Option<String>,
    pub if_none: Option<String>,
}

impl Publish {
    pub(crate) fn validate(&self) -> Result<()> {
        let mut etag = self.etag.split('-');
        let digest = etag.next().unwrap_or_default();
        ensure!(
            self.size >= 0
                && digest.len() == 32
                && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
                && etag.next().is_none_or(|parts| parts
                    .parse::<u32>()
                    .is_ok_and(|parts| (1..=10000).contains(&parts)))
                && etag.next().is_none()
                && self.checksums.len() <= 11,
            "invalid publication"
        );
        for (name, value) in &self.checksums {
            ensure!(
                matches!(
                    name.as_str(),
                    "crc32"
                        | "crc32c"
                        | "crc64nvme"
                        | "sha1"
                        | "sha256"
                        | "sha512"
                        | "md5"
                        | "xxhash64"
                        | "xxhash3"
                        | "xxhash128"
                        | "type"
                ) && !value.is_empty()
                    && value.len() <= 128,
                "invalid checksum metadata"
            );
            axum::http::HeaderValue::from_str(value)?;
        }
        for value in [&self.if_match, &self.if_none].into_iter().flatten() {
            ensure!(value.len() <= 256, "write condition too long");
            axum::http::HeaderValue::from_str(value)?;
            s3s::dto::ETagCondition::parse_http_header(value.as_bytes())?;
        }
        Ok(())
    }

    pub(crate) fn hash(&self) -> Result<Vec<u8>> {
        self.validate()?;
        super::node_chunk::hash(
            &serde_json::json!({"size":self.size,"etag":self.etag,"checksums":self.checksums,"if_match":self.if_match,"if_none":self.if_none}),
        )
    }
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
    fn publication_binds_conditions_and_validates_headers() -> Result<()> {
        let mut input = Publish {
            run: Run {
                boot: Uuid::new_v4(),
                fence: Uuid::new_v4(),
            },
            operation: Uuid::now_v7(),
            size: 0,
            etag: "d41d8cd98f00b204e9800998ecf8427e".into(),
            checksums: Default::default(),
            if_match: None,
            if_none: None,
        };
        let original = input.hash()?;
        input.if_none = Some("*".into());
        assert_ne!(input.hash()?, original);
        input.if_match = Some("invalid\r\ncondition".into());
        assert!(input.validate().is_err());
        input.if_match = None;
        input.etag.push_str("-0");
        assert!(input.validate().is_err());
        input.etag = "d41d8cd98f00b204e9800998ecf8427e-2".into();
        input.validate()?;
        input.checksums.insert("sha256".into(), "x\ny".into());
        assert!(input.validate().is_err());
        Ok(())
    }

    #[test]
    fn write_operations_bind_canonical_intent_and_timestamp() -> Result<()> {
        let intent = Intent {
            access: "writer".into(),
            credential_revision: 1,
            copy_read: None,
            part: None,
            completion: None,
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
