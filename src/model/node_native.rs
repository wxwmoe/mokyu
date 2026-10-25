use super::{node_read, session::Run};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(crate) struct LocalPart {
    pub run: Run,
    pub operation: Uuid,
    pub size: i64,
    pub hashes: Vec<[u8; 32]>,
}

impl LocalPart {
    pub(crate) fn hash(&self) -> Result<Vec<u8>> {
        ensure!(
            (0..=super::node_write::MAX_PART_BYTES).contains(&self.size)
                && self.hashes.len() == (self.size as usize).div_ceil(crate::content::codec::MAX),
            "invalid local part manifest"
        );
        super::node_chunk::hash(&serde_json::json!([self.size, self.hashes]))
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Action {
    Initialize,
    Part {
        stream: Uuid,
        file: Uuid,
        number: i32,
        size: i64,
        etag: String,
    },
    Abort,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Work {
    pub upload: super::multipart::RemoteUpload,
    pub action: Action,
}

pub(crate) struct Claim {
    pub run: Run,
    pub operation: Uuid,
    pub stream: Option<Uuid>,
    pub cached: Vec<node_read::Version>,
}

pub(crate) struct Reply {
    pub operation: Uuid,
    pub work: Option<Work>,
    pub ready: bool,
    pub materials: Vec<node_read::Material>,
    pub storage: Option<node_read::Version>,
    pub grants: Vec<node_read::Grant>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResultValue {
    pub value: Option<String>,
    pub retry: bool,
    pub source_error: Option<SourceError>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SourceError {
    Missing,
    Corrupt,
}

impl SourceError {
    pub(crate) fn code(self) -> &'static str {
        match self {
            Self::Missing => "local_source_missing",
            Self::Corrupt => "local_source_corrupt",
        }
    }
}

impl std::fmt::Display for SourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}

impl std::error::Error for SourceError {}

impl ResultValue {
    pub(crate) fn hash(&self) -> Result<Vec<u8>> {
        ensure!(
            self.value.as_ref().is_none_or(|value| !value.is_empty()
                && value.len() <= 4096
                && !value.contains('\0'))
                && (!self.retry || self.value.is_none())
                && (self.source_error.is_none() || self.retry),
            "invalid native result"
        );
        super::node_chunk::hash(self)
    }
}

pub(crate) struct Finish {
    pub run: Run,
    pub operation: Uuid,
    pub result: ResultValue,
}

pub(crate) struct Inventory {
    pub run: Run,
    pub files: Vec<Uuid>,
    pub recover: bool,
}

pub(crate) struct Complete {
    pub run: Run,
    pub operation: Uuid,
    pub resend: bool,
}

#[derive(Serialize, Deserialize, sqlx::FromRow)]
pub(crate) struct Part {
    pub number: i32,
    pub size: i64,
    pub etag: String,
    pub remote_etag: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Completion {
    pub upload: super::multipart::RemoteUpload,
    pub parts: Vec<Part>,
}

pub(crate) struct PreparedCompletion {
    pub value: Completion,
    pub grants: Vec<node_read::Grant>,
}

pub(crate) struct Adopt {
    pub run: Run,
    pub operation: Uuid,
    pub digest: [u8; 16],
    pub hash: [u8; 32],
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_manifest_is_bounded_and_exact() -> Result<()> {
        let mut part = LocalPart {
            run: Run {
                boot: Uuid::new_v4(),
                fence: Uuid::new_v4(),
            },
            operation: Uuid::now_v7(),
            size: 0,
            hashes: vec![],
        };
        part.hash()?;
        part.size = 1;
        assert!(part.hash().is_err());
        part.hashes.push([1; 32]);
        let hash = part.hash()?;
        part.hashes[0][0] = 2;
        assert_ne!(part.hash()?, hash);
        part.size = super::super::node_write::MAX_PART_BYTES + 1;
        assert!(part.hash().is_err());
        Ok(())
    }
}
