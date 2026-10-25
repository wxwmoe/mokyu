use super::{node_write, session::Run};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(crate) const REQUEST_LIMIT: usize = 2 * 1024 * 1024;

#[derive(Clone, Serialize)]
pub(crate) struct Change {
    pub access: String,
    pub credential_revision: i64,
    pub bucket: String,
    pub keys: Vec<String>,
    pub public: Option<bool>,
}

impl Change {
    pub(crate) fn hash(&self) -> Result<Vec<u8>> {
        let name =
            |value: &str, limit| !value.is_empty() && value.len() <= limit && !value.contains('\0');
        ensure!(
            name(&self.access, 256)
                && self.credential_revision >= 0
                && name(&self.bucket, 255)
                && self.keys.len() <= 1000
                && self.keys.iter().all(|key| name(key, 1024))
                && (self.public.is_none() || self.keys.len() == 1),
            "invalid object change"
        );
        Ok(blake3::hash(&serde_json::to_vec(self)?).as_bytes().to_vec())
    }
}

pub(crate) struct Request {
    pub run: Run,
    pub operation: Uuid,
    pub change: Change,
}

impl Request {
    pub(crate) fn validate(&self) -> Result<Vec<u8>> {
        node_write::operation_time(self.operation)?;
        self.change.hash()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Outcome {
    Applied,
    NoSuchKey,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn changes_bind_order_authority_and_acl_with_bounded_keys() -> Result<()> {
        let mut change = Change {
            access: "writer".into(),
            credential_revision: 1,
            bucket: "bucket".into(),
            keys: vec!["a".into(), "b".into()],
            public: None,
        };
        let hash = change.hash()?;
        change.keys.reverse();
        assert_ne!(hash, change.hash()?);
        change.public = Some(true);
        assert!(change.hash().is_err());
        change.keys.truncate(1);
        change.hash()?;
        change.credential_revision = -1;
        assert!(change.hash().is_err());
        change.credential_revision = 1;
        change.keys = vec!["x".repeat(1025)];
        assert!(change.hash().is_err());
        change.public = None;
        change.keys = vec!["x".repeat(1024); 1000];
        change.hash()?;
        change.keys.push("x".into());
        assert!(change.hash().is_err());
        Ok(())
    }
}
