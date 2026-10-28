use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(crate) const LIMIT: usize = 128;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Signal {
    Changes,
    Reconnected,
    State,
    Work(Option<Uuid>),
    Task(Uuid),
    Settings { section: String, revision: i64 },
    Control,
}

impl Signal {
    pub(crate) fn parse(payload: &str) -> Option<Self> {
        match payload {
            "node-state" => return Some(Self::State),
            "work" => return Some(Self::Work(None)),
            "control" => return Some(Self::Control),
            _ => {}
        }
        if let Some(node) = payload.strip_prefix("work:") {
            return node
                .parse::<Uuid>()
                .ok()
                .filter(|node| !node.is_nil())
                .map(|node| Self::Work(Some(node)));
        }
        if let Some(task) = payload.strip_prefix("task:") {
            return task
                .parse::<Uuid>()
                .ok()
                .filter(|task| !task.is_nil())
                .map(Self::Task);
        }
        let (section, revision) = payload.strip_prefix("settings:")?.split_once(':')?;
        if !matches!(
            section,
            "requests" | "uploads" | "metrics" | "maintenance" | "io"
        ) {
            return None;
        }
        let revision = revision
            .parse::<i64>()
            .ok()
            .filter(|revision| *revision > 0)?;
        Some(Self::Settings {
            section: section.into(),
            revision,
        })
    }
}

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
    fn ordinary_signals_reject_unknown_sections_and_invalid_targets() {
        assert_eq!(Signal::parse("work"), Some(Signal::Work(None)));
        assert_eq!(
            Signal::parse("settings:io:2"),
            Some(Signal::Settings {
                section: "io".into(),
                revision: 2
            })
        );
        for value in [
            "settings:web:1",
            "settings:io:0",
            "settings:io:-1",
            "settings:io:1:2",
            "work:00000000-0000-0000-0000-000000000000",
            "work:invalid",
        ] {
            assert!(Signal::parse(value).is_none());
        }
    }

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
