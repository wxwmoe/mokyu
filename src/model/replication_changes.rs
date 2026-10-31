use super::{Id, Snapshot};
use crate::storage::external::objects::Object;
use serde::{Deserialize, Serialize};
use sqlx::types::Json;

pub(crate) fn prefix_limit(prefix: &str) -> Option<String> {
    let mut limit = prefix.to_owned();
    while let Some(last) = limit.pop() {
        let next = match last as u32 {
            0x10ffff => continue,
            0xd7ff => 0xe000,
            value => value + 1,
        };
        limit.push(char::from_u32(next).unwrap());
        return Some(limit);
    }
    None
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "endpoint", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Confirmation {
    Internal {
        version: Id,
        snapshot: Snapshot,
    },
    External {
        object: Object,
        public: Option<bool>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Target {
    pub object: Object,
    pub public: Option<bool>,
}

impl Target {
    pub(crate) fn matches(&self, object: Option<&Object>, public: Option<bool>) -> bool {
        object == Some(&self.object) && (self.public.is_none() || self.public == public)
    }
}

impl Confirmation {
    pub(crate) fn matches(&self, version: Id, current: &Snapshot) -> bool {
        match self {
            Self::Internal {
                version: expected,
                snapshot,
            } => {
                let mut snapshot = snapshot.clone();
                snapshot.payload.clone_from(&current.payload);
                *expected == version && snapshot == *current
            }
            Self::External { .. } => false,
        }
    }

    pub(crate) fn target(&self) -> anyhow::Result<Target> {
        Ok(match self {
            Self::Internal { version, snapshot } => {
                let native = super::external::Native::new(*version, snapshot)?;
                Target {
                    object: native.object,
                    public: Some(native.public),
                }
            }
            Self::External { object, public } => Target {
                object: object.clone(),
                public: *public,
            },
        })
    }
}

#[derive(sqlx::FromRow)]
pub(crate) struct Change {
    pub object_key: String,
    pub source_version: Option<Id>,
    pub snapshot: Option<Json<Snapshot>>,
    pub revision: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_limits_preserve_byte_order_without_splitting_utf8() {
        for (prefix, end) in [
            ("a/%", "a/&"),
            ("x\u{d7ff}", "x\u{e000}"),
            ("a\u{10ffff}", "b"),
            ("\u{3042}", "\u{3043}"),
        ] {
            let limit = prefix_limit(prefix).unwrap();
            assert_eq!(limit, end);
            assert!(format!("{prefix}\u{10ffff}/suffix").as_bytes() < limit.as_bytes());
        }
        assert_eq!(prefix_limit(""), None);
        assert_eq!(prefix_limit("\u{10ffff}"), None);
    }
}
