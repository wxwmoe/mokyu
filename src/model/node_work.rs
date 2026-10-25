use super::{node_read, session::Run};
use anyhow::{Result, ensure};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(crate) const MAX_ITEMS: usize = 64;
pub(crate) const SCAN_ITEMS: usize = 32;
pub(crate) type Member = (super::Id, i64, i32, Vec<u8>);

#[derive(Serialize, Deserialize)]
pub(crate) struct Materials {
    pub storage: node_read::Storage,
    pub keys: Vec<node_read::BlockKey>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Physical {
    Chunk,
    Pack,
    Raw { upload: Option<String> },
    Staging { key: String },
}

impl Physical {
    pub(crate) fn packed(&self) -> bool {
        matches!(self, Self::Pack)
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Deletion {
    pub source: Physical,
    pub location: Uuid,
    pub inspect_age: bool,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum AuditTarget {
    Chunk {
        id: super::Id,
    },
    Raw {
        bucket: Uuid,
        key: String,
        stream: super::Id,
        offset: i64,
    },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Inspection {
    pub location: Uuid,
    pub snapshot: Option<Box<super::source::SourceSnapshot>>,
    pub members: Vec<Member>,
    pub targets: Vec<AuditTarget>,
    pub previous: Option<Outcome>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ListedObject {
    pub path: String,
    pub size: u64,
    pub modified: DateTime<Utc>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Plan {
    Collect {
        storage: Uuid,
        delete_before: DateTime<Utc>,
        retention: u64,
        metadata_count: usize,
        items: Vec<Deletion>,
    },
    Inspect {
        snapshot: Box<super::source::SourceSnapshot>,
        location: Uuid,
        members: Vec<Member>,
        inspect: bool,
    },
    Audit {
        storage: Uuid,
        mode: crate::worker::integrity::Mode,
        cursor: String,
        items: Vec<Inspection>,
    },
    SweepScan {
        storage: Uuid,
        after: Option<String>,
        limit: usize,
        bytes: Option<u64>,
    },
    SweepDelete {
        storage: Uuid,
        before: DateTime<Utc>,
        items: Vec<ListedObject>,
    },
}

impl Plan {
    pub(crate) fn storage(&self) -> Uuid {
        match self {
            Self::Collect { storage, .. }
            | Self::Audit { storage, .. }
            | Self::SweepScan { storage, .. }
            | Self::SweepDelete { storage, .. } => *storage,
            Self::Inspect { snapshot, .. } => snapshot.scope.storage_id,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Outcome {
    Deleted { stored_at: Option<DateTime<Utc>> },
    Retained { stored_at: DateTime<Utc> },
    Retry,
    Inspected { code: Option<String> },
    Listed { items: Vec<ListedObject> },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Completion {
    pub operation: Uuid,
    pub outcomes: Vec<Outcome>,
}

pub(crate) struct Exchange {
    pub run: Run,
    pub operation: Uuid,
    pub completion: Option<Completion>,
    pub class: u32,
    pub max_items: usize,
    pub cached: Vec<node_read::Version>,
}

impl Exchange {
    pub(crate) fn hash(&self) -> Result<Vec<u8>> {
        ensure!(
            self.class <= 2 && (1..=MAX_ITEMS).contains(&self.max_items),
            "invalid work bounds"
        );
        if let Some(completion) = &self.completion {
            ensure!(
                completion.operation != self.operation
                    && !completion.operation.is_nil()
                    && completion.outcomes.len() <= MAX_ITEMS,
                "invalid work result"
            );
        }
        super::node_chunk::hash(&(self.class, self.max_items, &self.completion))
    }
}

pub(crate) struct Reply {
    pub operation: Uuid,
    pub plan: Option<Plan>,
    pub materials: Vec<node_read::Material>,
    pub grants: Vec<node_read::Grant>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exchange_identity_covers_results_and_limits() -> Result<()> {
        let mut input = Exchange {
            run: Run {
                boot: Uuid::new_v4(),
                fence: Uuid::new_v4(),
            },
            operation: Uuid::now_v7(),
            completion: None,
            class: 0,
            max_items: 64,
            cached: vec![],
        };
        let first = input.hash()?;
        input.completion = Some(Completion {
            operation: Uuid::now_v7(),
            outcomes: vec![Outcome::Retry],
        });
        assert_ne!(input.hash()?, first);
        input.max_items = 65;
        assert!(input.hash().is_err());
        input.max_items = 64;
        input.completion.as_mut().unwrap().operation = input.operation;
        assert!(input.hash().is_err());
        Ok(())
    }
}
