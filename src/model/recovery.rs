use crate::model::Id;
use crate::{content::codec::Chunk, model::source::SourceSnapshot};
use anyhow::{Result, ensure};

#[cfg(test)]
#[path = "../../tests/recovery/source_recovery.rs"]
mod tests;

pub(crate) enum RepairAllocation {
    Existing(Chunk),
    Candidate(Box<Repair>),
}

#[derive(Clone)]
pub(crate) struct RepairNode {
    pub state: crate::cluster::NodeState,
    pub role: String,
    pub key: crate::content::codec::Key,
}

#[derive(Clone)]
pub(crate) struct Repair {
    pub stream: Id,
    pub node: Option<RepairNode>,
    pub chunk: Chunk,
    pub sources: Vec<SourceSnapshot>,
}

impl Repair {
    pub fn check_candidate(&self, candidate: &Chunk, complete: bool) -> Result<()> {
        let expected = &self.chunk;
        ensure!(
            candidate.id == expected.id
                && candidate.encoding_id == expected.encoding_id
                && candidate.storage_id == expected.storage_id
                && candidate.location_id == expected.location_id
                && candidate.raw_size == expected.raw_size
                && candidate.hash == expected.hash
                && candidate.algorithm == expected.algorithm
                && candidate.block_key_id == expected.block_key_id
                && candidate.created_at == expected.created_at
                && candidate.format == expected.format
                && candidate.state == "ready",
            crate::model::Error::OperationAborted
        );
        if complete {
            ensure!(
                candidate.stored_size.is_some()
                    && candidate.stored_size == expected.stored_size
                    && candidate.compressed == expected.compressed
                    && candidate.nonce == expected.nonce,
                crate::model::Error::OperationAborted
            );
        }
        Ok(())
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.sources.is_empty() && self.sources.len() <= 2,
            crate::model::Error::SourceUnavailable
        );
        for source in &self.sources {
            ensure!(
                source.state == "ready"
                    && source.quarantine_code.is_some()
                    && Some(source.scope.storage_id) == self.chunk.storage_id,
                crate::model::Error::SourceUnavailable
            );
        }
        Ok(())
    }
}
