use super::{node_read, session::Run};
use crate::content::codec::Chunk;
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(crate) const MAX_ITEMS: usize = 128;
pub(crate) const MAX_BYTES: i64 = 32 * 1024 * 1024;
pub(crate) const MAX_PENDING: i64 = 4;
pub(crate) const MAX_PARALLEL: u32 = 16;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct Input {
    pub hash: [u8; 32],
    pub raw_size: i32,
    pub payload_hash: [u8; 32],
    pub payload_size: i32,
    pub compressed: bool,
}

#[derive(Clone)]
pub(crate) struct Prepare {
    pub run: Run,
    pub operation: Uuid,
    pub batch: i64,
    pub offset: i64,
    pub items: Vec<Input>,
    pub parallelism: u32,
    pub cached: Vec<node_read::Version>,
}

impl Prepare {
    pub(crate) fn end(&self) -> Result<i64> {
        let total: i64 = self.items.iter().map(|item| i64::from(item.raw_size)).sum();
        ensure!(
            self.batch > 0
                && self.offset >= 0
                && !self.items.is_empty()
                && self.items.len() <= MAX_ITEMS
                && (1..=MAX_BYTES).contains(&total)
                && (1..=MAX_PARALLEL).contains(&self.parallelism),
            "invalid chunk batch"
        );
        let mut unique = std::collections::HashMap::new();
        for item in &self.items {
            ensure!(
                item.raw_size > 0
                    && item.raw_size as usize <= crate::content::codec::MAX
                    && item.payload_size > 0
                    && item.payload_size <= item.raw_size
                    && (item.compressed
                        || (item.raw_size == item.payload_size && item.hash == item.payload_hash)),
                "invalid chunk input"
            );
            if let Some(previous) = unique.insert((item.hash, item.raw_size), item) {
                ensure!(previous == item, "duplicate chunk encoding differs");
            }
        }
        self.offset
            .checked_add(total)
            .ok_or_else(|| anyhow::anyhow!("chunk offset overflow"))
    }

    pub(crate) fn hash(&self) -> Result<Vec<u8>> {
        self.end()?;
        hash(
            &serde_json::json!({"offset":self.offset,"items":self.items,"parallelism":self.parallelism}),
        )
    }
}

pub(crate) fn hash(value: &impl Serialize) -> Result<Vec<u8>> {
    Ok(blake3::hash(&serde_json::to_vec(value)?)
        .as_bytes()
        .to_vec())
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Item {
    pub offset: i64,
    pub chunk: Chunk,
    pub reused: bool,
    pub payload_hash: [u8; 32],
}

pub(crate) struct Assembly {
    pub items: Vec<Item>,
    pub ready: bool,
    pub committed: bool,
}

pub(crate) struct Nonce {
    pub chunk: Uuid,
    pub value: Vec<u8>,
}

pub(crate) struct Prepared {
    pub batch: i64,
    pub end: i64,
    pub committed: bool,
    pub items: Vec<Item>,
    pub materials: Vec<node_read::Material>,
    pub storage: Option<node_read::Version>,
    pub grants: Vec<node_read::Grant>,
}

#[derive(Clone, Serialize)]
pub(crate) struct Encoded {
    pub chunk: Uuid,
    pub digest: [u8; 32],
}

pub(crate) struct Commit {
    pub run: Run,
    pub operation: Uuid,
    pub batch: i64,
    pub encoded: Vec<Encoded>,
}

impl Commit {
    pub(crate) fn hash(&mut self) -> Result<Vec<u8>> {
        ensure!(
            self.batch > 0 && self.encoded.len() <= MAX_ITEMS,
            "invalid chunk commit"
        );
        self.encoded.sort_by_key(|item| item.chunk);
        ensure!(
            self.encoded.iter().all(|item| !item.chunk.is_nil())
                && self
                    .encoded
                    .windows(2)
                    .all(|pair| pair[0].chunk != pair[1].chunk),
            "duplicate chunk receipt"
        );
        hash(&self.encoded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_inputs_bind_encoding_and_bound_memory_offsets_and_receipts() -> Result<()> {
        let input = Input {
            hash: [3; 32],
            raw_size: 4096,
            payload_hash: [3; 32],
            payload_size: 4096,
            compressed: false,
        };
        let mut request = Prepare {
            run: Run {
                boot: Uuid::new_v4(),
                fence: Uuid::new_v4(),
            },
            operation: Uuid::now_v7(),
            batch: 1,
            offset: 0,
            items: vec![input.clone(), input],
            parallelism: 2,
            cached: vec![],
        };
        assert_eq!(request.end()?, 8192);
        let first = request.hash()?;
        request.items[1].compressed = true;
        assert!(request.end().is_err());
        request.items.pop();
        assert_ne!(request.hash()?, first);
        request.items[0].payload_size = 5000;
        assert!(request.end().is_err());
        request.items[0].payload_size = 4096;
        request.offset = i64::MAX;
        assert!(request.end().is_err());
        request.offset = 0;
        request.items = vec![request.items[0].clone(); MAX_ITEMS + 1];
        assert!(request.end().is_err());
        let a = Encoded {
            chunk: Uuid::new_v4(),
            digest: [1; 32],
        };
        let b = Encoded {
            chunk: Uuid::new_v4(),
            digest: [2; 32],
        };
        let mut commit = Commit {
            run: request.run,
            operation: request.operation,
            batch: 1,
            encoded: vec![a.clone(), b.clone()],
        };
        let first = commit.hash()?;
        commit.encoded = vec![b, a.clone()];
        assert_eq!(commit.hash()?, first);
        commit.encoded.push(a);
        assert!(commit.hash().is_err());
        Ok(())
    }
}
