use super::{node_read, session::Run};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Input {
    pub size: i64,
    pub digest: [u8; 16],
    pub hash: Option<[u8; 32]>,
}

impl Input {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            (0..=5 * 1024 * 1024 * 1024).contains(&self.size),
            "invalid raw size"
        );
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct Prepare {
    pub run: Run,
    pub operation: Uuid,
    pub input: Input,
}

pub(crate) struct Commit {
    pub run: Run,
    pub operation: Uuid,
    pub source: Uuid,
}

pub(crate) struct Prepared {
    pub source: Uuid,
    pub size: i64,
    pub committed: bool,
    pub grants: Vec<node_read::Grant>,
}
