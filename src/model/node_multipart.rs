use super::{node_write, session::Run};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Serialize)]
pub(crate) enum Operation {
    Create {
        metadata: serde_json::Value,
        public: bool,
        algorithm: Option<String>,
        checksum_type: Option<String>,
    },
    Abort {
        upload: Uuid,
        initiated: Option<String>,
    },
}

#[derive(Clone, Serialize)]
pub(crate) struct Intent {
    pub access: String,
    pub credential_revision: i64,
    pub bucket: String,
    pub key: String,
    pub operation: Operation,
}

impl Intent {
    pub(crate) fn hash(&self) -> Result<Vec<u8>> {
        let text =
            |value: &str, limit| !value.is_empty() && value.len() <= limit && !value.contains('\0');
        ensure!(
            text(&self.access, 256)
                && self.credential_revision >= 0
                && text(&self.bucket, 255)
                && text(&self.key, 1024),
            "invalid multipart request"
        );
        match &self.operation {
            Operation::Create {
                metadata,
                algorithm,
                checksum_type,
                ..
            } => {
                ensure!(
                    metadata.is_object() && serde_json::to_vec(metadata)?.len() <= 16 * 1024,
                    "invalid multipart metadata"
                );
                ensure!(
                    crate::upload::multipart::checksum_type(
                        algorithm.as_deref(),
                        checksum_type.as_deref()
                    )? == *checksum_type,
                    "noncanonical multipart checksum type"
                );
            }
            Operation::Abort { upload, initiated } => {
                ensure!(
                    !upload.is_nil() && initiated.as_ref().is_none_or(|value| value.len() <= 64),
                    "invalid multipart identity"
                );
                if let Some(value) = initiated {
                    s3s::dto::Timestamp::parse(s3s::dto::TimestampFormat::HttpDate, value)?;
                }
            }
        }
        Ok(blake3::hash(&serde_json::to_vec(self)?).as_bytes().to_vec())
    }
}

pub(crate) struct Request {
    pub run: Run,
    pub operation: Uuid,
    pub intent: Intent,
}

impl Request {
    pub(crate) fn validate(&self) -> Result<Vec<u8>> {
        node_write::operation_time(self.operation)?;
        self.intent.hash()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Outcome {
    Created { upload: Uuid },
    Aborted,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multipart_intents_bind_metadata_checksums_and_operation() -> Result<()> {
        let mut intent = Intent {
            access: "writer".into(),
            credential_revision: 1,
            bucket: "bucket".into(),
            key: "key".into(),
            operation: Operation::Create {
                metadata: serde_json::json!({}),
                public: false,
                algorithm: Some("CRC64NVME".into()),
                checksum_type: Some("FULL_OBJECT".into()),
            },
        };
        let hash = intent.hash()?;
        if let Operation::Create { checksum_type, .. } = &mut intent.operation {
            *checksum_type = Some("COMPOSITE".into());
        }
        assert!(intent.hash().is_err());
        intent.operation = Operation::Abort {
            upload: Uuid::new_v4(),
            initiated: None,
        };
        assert_ne!(hash, intent.hash()?);
        intent.credential_revision = -1;
        assert!(intent.hash().is_err());
        Ok(())
    }
}
