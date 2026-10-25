use super::{Id, node_work::Member};
use crate::content::{codec::Chunk, pack::Pack};
use anyhow::{Result, ensure};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(crate) const RESULT_LIMIT: usize = 256 * 1024;

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Source {
    Chunk { chunk: Chunk },
    Pack { pack: Pack, members: Vec<Member> },
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Target {
    pub id: Id,
    pub size: i32,
    pub hash: Vec<u8>,
    pub previous_size: Option<i32>,
    pub compress: bool,
    pub location: Uuid,
    pub nonce: Option<Vec<u8>>,
    pub ready: Option<u32>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Attempt {
    pub start: usize,
    pub end: usize,
    pub location: Uuid,
    pub nonce: Option<Vec<u8>>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Batch {
    pub algorithm: String,
    pub key: Option<Uuid>,
    pub created: DateTime<Utc>,
    pub sources: Vec<Source>,
    pub chunks: Vec<Target>,
    pub groups: Vec<(usize, usize)>,
    pub packs: Vec<Attempt>,
}

impl Batch {
    pub(crate) fn chunk(&self, storage: Uuid, index: usize) -> Result<Chunk> {
        let item = &self.chunks[index];
        let Id::Uuid(uuid) = item.id else {
            anyhow::bail!("invalid pack member identity");
        };
        Ok(Chunk {
            id: item.id,
            uuid,
            encoding_id: Id::Uuid(item.location),
            location_id: item.location,
            storage_id: Some(storage),
            hash: item.hash.clone(),
            raw_size: item.size,
            stored_size: None,
            algorithm: self.algorithm.clone(),
            block_key_id: self.key,
            compressed: false,
            nonce: item.nonce.clone(),
            format: 2,
            state: "ready".into(),
            created_at: self.created,
        })
    }

    pub(crate) fn pack(&self, storage: Uuid, index: usize) -> Result<Pack> {
        let item = &self.packs[index];
        ensure!(
            item.start < item.end && item.end <= self.chunks.len(),
            "invalid pack range"
        );
        let pack = Pack {
            id: Id::Uuid(item.location),
            location_id: item.location,
            storage_id: Some(storage),
            algorithm: self.algorithm.clone(),
            block_key_id: self.key,
            raw_size: self.chunks[item.start..item.end]
                .iter()
                .map(|c| i64::from(c.size))
                .sum(),
            stored_size: None,
            compressed: false,
            nonce: item.nonce.clone(),
            digest: None,
            member_count: i32::try_from(item.end - item.start)?,
            state: "preparing".into(),
            created_at: self.created,
        };
        pack.payload_size()?;
        Ok(pack)
    }
}

// Positions bind outcomes to immutable destinations; zero means an unused chunk output.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Encoded {
    pub chunks: Vec<u32>,
    pub packs: Vec<(usize, u32, String)>,
}

pub(crate) fn size(value: u32) -> (i64, bool) {
    (i64::from(value >> 1), value & 1 != 0)
}

pub(crate) fn encoded(size: i64, compressed: bool) -> Result<u32> {
    ensure!(
        size > 0 && size <= i64::from(u32::MAX >> 1),
        "invalid encoded size"
    );
    Ok((size as u32) << 1 | u32::from(compressed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_pack_completion_remains_bounded() -> Result<()> {
        let result = Encoded {
            chunks: vec![encoded(crate::content::codec::MAX as i64 + 16, true)?; 4096],
            packs: (0..2048)
                .map(|i| (i * 3, u32::MAX, "x".repeat(44)))
                .collect(),
        };
        assert!(serde_json::to_vec(&result)?.len() < RESULT_LIMIT - 8192);
        assert_eq!(size(encoded(1234, true)?), (1234, true));
        assert!(encoded(0, false).is_err());
        Ok(())
    }
}
