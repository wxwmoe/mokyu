use super::{node_read, session::Run};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(crate) const PREPARE_LIMIT: usize = 160 * 1024 + 1024;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Input {
    pub local: bool,
    pub size: i64,
    pub digest: [u8; 16],
    pub hash: Option<[u8; 32]>,
    pub page_hashes: Vec<u8>,
}

impl Input {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            (0..=5 * 1024 * 1024 * 1024).contains(&self.size),
            "invalid raw size"
        );
        ensure!(
            self.page_hashes.len()
                == usize::try_from(crate::content::raw_pages::count(self.size)?)? * 32,
            "invalid raw page count"
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

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FileState {
    Pending,
    Protected,
    Cached,
    Removable,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct File {
    pub id: Uuid,
    pub storage: Option<Uuid>,
    pub size: u64,
    pub state: FileState,
    pub cache_limit: Option<String>,
    pub cache_entries: Option<u32>,
}
