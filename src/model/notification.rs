use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(crate) const LIMIT: usize = 128;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "scope", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Change {
    Object { bucket: Uuid, key: String },
    Bucket { bucket: Uuid },
    Storage { storage: Uuid },
}

impl Change {
    pub(crate) fn validate(&self) -> Result<()> {
        let id = match self {
            Self::Object { bucket, key } => {
                ensure!(
                    !key.is_empty() && key.len() <= 1024,
                    "invalid object notification"
                );
                *bucket
            }
            Self::Bucket { bucket } => *bucket,
            Self::Storage { storage } => *storage,
        };
        ensure!(!id.is_nil(), "invalid notification identity");
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Consumer {
    pub node: Uuid,
    pub generation: Uuid,
}

#[derive(Clone, Debug)]
pub(crate) struct Delivery {
    pub consumer: Consumer,
    pub id: Uuid,
    pub change: Change,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notification_targets_validate_bytes_and_identity() {
        let bucket = Uuid::new_v4();
        assert!(
            Change::Object {
                bucket,
                key: "x".repeat(1024)
            }
            .validate()
            .is_ok()
        );
        assert!(
            Change::Object {
                bucket,
                key: "\u{e9}".repeat(513)
            }
            .validate()
            .is_err()
        );
        assert!(
            Change::Object {
                bucket,
                key: String::new()
            }
            .validate()
            .is_err()
        );
        assert!(
            Change::Storage {
                storage: Uuid::nil()
            }
            .validate()
            .is_err()
        );
        let value = Change::Bucket { bucket };
        assert_eq!(
            value,
            serde_json::from_slice(&serde_json::to_vec(&value).unwrap()).unwrap()
        );
    }
}
