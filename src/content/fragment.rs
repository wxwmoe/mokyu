//! Self-describing storage fragments and bounded proofs for range reads.
use crate::model::storage_cluster::Layout;
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use uuid::Uuid;

pub(crate) const MAGIC: &[u8; 8] = b"MOKYU\x00\x00\x09";
pub(crate) const HEADER_SIZE: usize = 320;
pub(crate) const FOOTER_SIZE: usize = 80;
pub(crate) const UNIT: usize = 64 * 1024;
pub(crate) const FILE_LIMIT: u64 = 1024 * 1024 * 1024;
pub(crate) type Digest = [u8; 32];

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Area {
    Blocks,
    Recovery,
}

impl Area {
    pub fn directory(self) -> &'static str {
        match self {
            Self::Blocks => "blocks",
            Self::Recovery => "recovery",
        }
    }

    pub fn path(self, fragment: Uuid) -> PathBuf {
        let name = fragment.simple().to_string();
        PathBuf::from(self.directory())
            .join(name.chars().rev().take(3).collect::<String>())
            .join(name)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Header {
    pub area: Area,
    pub namespace: Uuid,
    pub payload: Uuid,
    pub fragment: Uuid,
    pub media: Uuid,
    pub cluster: Uuid,
    pub group: Uuid,
    pub operation: Uuid,
    pub layout_revision: u64,
    pub revision: u64,
    pub takeover_epoch: u64,
    pub offset: u64,
    pub length: u64,
    pub total: u64,
    pub shard_bytes: u64,
    pub sequence: u32,
    pub layout: Layout,
    pub copy: u16,
    pub shard: u16,
    pub digest: Digest,
}

impl Header {
    pub fn validate(&self) -> Result<()> {
        self.layout.validate()?;
        ensure!(
            [
                self.namespace,
                self.payload,
                self.fragment,
                self.media,
                self.cluster,
                self.group,
                self.operation
            ]
            .iter()
            .all(|id| !id.is_nil())
                && self.layout_revision > 0
                && self.revision > 0
                && self.takeover_epoch > 0,
            "fragment identity is missing"
        );
        let row = u64::from(self.layout.data_shards) * u64::from(self.layout.stripe_bytes);
        let end = self
            .offset
            .checked_add(self.length)
            .context("fragment coverage overflow")?;
        ensure!(
            self.total <= i64::MAX as u64
                && end <= self.total
                && self.offset.is_multiple_of(row)
                && (self.length > 0 || (self.total == 0 && self.sequence == 0))
                && (end == self.total || self.length.is_multiple_of(row))
                && self.copy <= self.layout.extra_copies
                && u32::from(self.shard)
                    < u32::from(self.layout.data_shards) + u32::from(self.layout.parity_shards),
            "invalid fragment coverage or layout slot"
        );
        let stripe = u64::from(self.layout.stripe_bytes);
        let expected = if self.layout.parity_shards > 0 {
            self.length
                .div_ceil(row)
                .checked_mul(stripe)
                .context("fragment length overflow")?
        } else {
            self.length / row * stripe
                + (self.length % row)
                    .saturating_sub(u64::from(self.shard) * stripe)
                    .min(stripe)
        };
        ensure!(
            self.shard_bytes == expected && expected <= FILE_LIMIT,
            "invalid fragment length"
        );
        Ok(())
    }

    pub fn encode(&self) -> Result<[u8; HEADER_SIZE]> {
        self.validate()?;
        let mut bytes = [0; HEADER_SIZE];
        bytes[..8].copy_from_slice(MAGIC);
        bytes[8..10].copy_from_slice(&1u16.to_be_bytes());
        bytes[10..12].copy_from_slice(&(HEADER_SIZE as u16).to_be_bytes());
        bytes[12] = u8::from(self.area == Area::Recovery);
        bytes[13] = u8::from(self.layout.parity_shards > 0);
        bytes[14] = 1;
        for (index, id) in [
            self.namespace,
            self.payload,
            self.fragment,
            self.media,
            self.cluster,
            self.group,
            self.operation,
        ]
        .iter()
        .enumerate()
        {
            bytes[16 + index * 16..32 + index * 16].copy_from_slice(id.as_bytes());
        }
        for (index, value) in [
            self.layout_revision,
            self.revision,
            self.takeover_epoch,
            self.offset,
            self.length,
            self.total,
            self.shard_bytes,
        ]
        .iter()
        .enumerate()
        {
            bytes[128 + index * 8..136 + index * 8].copy_from_slice(&value.to_be_bytes());
        }
        bytes[184..188].copy_from_slice(&self.sequence.to_be_bytes());
        bytes[188..192].copy_from_slice(&self.layout.stripe_bytes.to_be_bytes());
        for (index, value) in [
            self.layout.data_shards,
            self.layout.parity_shards,
            self.layout.extra_copies,
            self.copy,
            self.shard,
        ]
        .iter()
        .enumerate()
        {
            bytes[192 + index * 2..194 + index * 2].copy_from_slice(&value.to_be_bytes());
        }
        bytes[204..208].copy_from_slice(&(UNIT as u32).to_be_bytes());
        bytes[208..240].copy_from_slice(&self.digest);
        let hash = blake3::hash(&bytes[..288]);
        bytes[288..].copy_from_slice(hash.as_bytes());
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(bytes.len() == HEADER_SIZE, "truncated fragment header");
        ensure!(
            &bytes[..8] == MAGIC
                && bytes[8..10] == 1u16.to_be_bytes()
                && bytes[10..12] == (HEADER_SIZE as u16).to_be_bytes()
                && bytes[12] <= 1
                && bytes[13] <= 1
                && bytes[14] == 1
                && bytes[15] == 0
                && bytes[202..204] == [0; 2]
                && bytes[240..288] == [0; 48]
                && bytes[204..208] == (UNIT as u32).to_be_bytes(),
            "unsupported fragment format"
        );
        ensure!(
            blake3::hash(&bytes[..288]).as_bytes() == &bytes[288..],
            "fragment header checksum mismatch"
        );
        let id = |at| Uuid::from_slice(&bytes[at..at + 16]);
        let long = |at| u64::from_be_bytes(bytes[at..at + 8].try_into().unwrap());
        let short = |at| u16::from_be_bytes(bytes[at..at + 2].try_into().unwrap());
        let value = Self {
            area: if bytes[12] == 0 {
                Area::Blocks
            } else {
                Area::Recovery
            },
            namespace: id(16)?,
            payload: id(32)?,
            fragment: id(48)?,
            media: id(64)?,
            cluster: id(80)?,
            group: id(96)?,
            operation: id(112)?,
            layout_revision: long(128),
            revision: long(136),
            takeover_epoch: long(144),
            offset: long(152),
            length: long(160),
            total: long(168),
            shard_bytes: long(176),
            sequence: u32::from_be_bytes(bytes[184..188].try_into()?),
            layout: Layout {
                data_shards: short(192),
                parity_shards: short(194),
                extra_copies: short(196),
                stripe_bytes: u32::from_be_bytes(bytes[188..192].try_into()?),
            },
            copy: short(198),
            shard: short(200),
            digest: bytes[208..240].try_into()?,
        };
        value.validate()?;
        ensure!(
            bytes[13] == u8::from(value.layout.parity_shards > 0),
            "fragment coding algorithm mismatch"
        );
        Ok(value)
    }

    pub fn leaves(&self) -> usize {
        self.shard_bytes.div_ceil(UNIT as u64).max(1) as usize
    }

    pub fn tree_nodes(&self) -> usize {
        2 * self.leaves().next_power_of_two() - 1
    }

    pub fn file_bytes(&self) -> u64 {
        HEADER_SIZE as u64 + self.shard_bytes + self.tree_nodes() as u64 * 32 + FOOTER_SIZE as u64
    }

    pub fn unit_bytes(&self, index: usize) -> Result<usize> {
        ensure!(index < self.leaves(), "fragment unit is out of range");
        Ok(self
            .shard_bytes
            .saturating_sub(index as u64 * UNIT as u64)
            .min(UNIT as u64) as usize)
    }
}

pub(crate) fn leaf(index: usize, bytes: &[u8]) -> Digest {
    *blake3::Hasher::new()
        .update(b"mokyu/fragment/09/leaf")
        .update(&(index as u64).to_be_bytes())
        .update(&(bytes.len() as u64).to_be_bytes())
        .update(bytes)
        .finalize()
        .as_bytes()
}

fn parent(left: &Digest, right: &Digest) -> Digest {
    *blake3::Hasher::new()
        .update(b"mokyu/fragment/09/node")
        .update(left)
        .update(right)
        .finalize()
        .as_bytes()
}

pub(crate) fn tree(header: &Header, leaves: &[Digest]) -> Result<Vec<Digest>> {
    header.validate()?;
    ensure!(
        leaves.len() == header.leaves(),
        "fragment checksum count mismatch"
    );
    let count = header.leaves().next_power_of_two();
    let mut nodes = vec![[0; 32]; 2 * count - 1];
    nodes[count - 1..count - 1 + leaves.len()].copy_from_slice(leaves);
    for index in leaves.len()..count {
        nodes[count - 1 + index] = leaf(index, &[]);
    }
    for index in (0..count - 1).rev() {
        nodes[index] = parent(&nodes[index * 2 + 1], &nodes[index * 2 + 2]);
    }
    Ok(nodes)
}

pub(crate) fn proof_indices(header: &Header, unit: usize) -> Result<Vec<usize>> {
    header.unit_bytes(unit)?;
    let mut index = header.leaves().next_power_of_two() - 1 + unit;
    let mut proof = Vec::with_capacity(14);
    while index > 0 {
        proof.push(if index % 2 == 1 { index + 1 } else { index - 1 });
        index = (index - 1) / 2;
    }
    Ok(proof)
}

pub(crate) fn verify_unit(
    header: &Header,
    unit: usize,
    bytes: &[u8],
    proof: &[Digest],
    root: &Digest,
) -> Result<()> {
    ensure!(
        bytes.len() == header.unit_bytes(unit)?,
        "fragment unit length mismatch"
    );
    ensure!(
        proof.len() == proof_indices(header, unit)?.len(),
        "fragment proof length mismatch"
    );
    let mut index = header.leaves().next_power_of_two() - 1 + unit;
    let mut digest = leaf(unit, bytes);
    for sibling in proof {
        digest = if index % 2 == 1 {
            parent(&digest, sibling)
        } else {
            parent(sibling, &digest)
        };
        index = (index - 1) / 2;
    }
    ensure!(&digest == root, "fragment range checksum mismatch");
    Ok(())
}

pub(crate) fn commitment(header: &Header, root: &Digest) -> Result<Digest> {
    Ok(*blake3::Hasher::new()
        .update(b"mokyu/fragment/09/complete")
        .update(&header.encode()?[288..])
        .update(root)
        .update(&header.file_bytes().to_be_bytes())
        .finalize()
        .as_bytes())
}

pub(crate) fn footer(header: &Header, root: &Digest) -> Result<[u8; FOOTER_SIZE]> {
    let mut bytes = [0; FOOTER_SIZE];
    bytes[..8].copy_from_slice(MAGIC);
    bytes[8..16].copy_from_slice(&header.file_bytes().to_be_bytes());
    bytes[16..48].copy_from_slice(root);
    bytes[48..].copy_from_slice(&commitment(header, root)?);
    Ok(bytes)
}

pub(crate) fn verify_footer(
    header: &Header,
    bytes: &[u8],
    expected: Option<&Digest>,
) -> Result<Digest> {
    ensure!(bytes.len() == FOOTER_SIZE, "truncated fragment footer");
    let root: Digest = bytes[16..48].try_into()?;
    ensure!(
        bytes == footer(header, &root)?,
        "fragment completion mismatch"
    );
    ensure!(
        expected.is_none_or(|digest| bytes[48..] == *digest),
        "fragment differs from its published receipt"
    );
    Ok(root)
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn sample(bytes: u64) -> Header {
        Header {
            area: Area::Blocks,
            namespace: Uuid::from_u128(1),
            payload: Uuid::from_u128(2),
            fragment: Uuid::from_u128(0x123456789),
            media: Uuid::from_u128(4),
            cluster: Uuid::from_u128(5),
            group: Uuid::from_u128(6),
            operation: Uuid::from_u128(7),
            layout_revision: 3,
            revision: 4,
            takeover_epoch: 1,
            offset: 0,
            length: bytes,
            total: bytes,
            shard_bytes: bytes,
            sequence: 0,
            layout: Layout::default(),
            copy: 0,
            shard: 0,
            digest: [0; 32],
        }
    }

    #[test]
    fn fixed_layout_rejects_corrupt_and_impossible_headers() -> Result<()> {
        let header = sample(7);
        let encoded = header.encode()?;
        assert_eq!(&encoded[..16], b"MOKYU\0\0\x09\0\x01\x01\x40\0\0\x01\0");
        assert_eq!(&encoded[128..136], &3u64.to_be_bytes());
        assert_eq!(&encoded[176..184], &7u64.to_be_bytes());
        assert_eq!(
            &encoded[192..208],
            &[0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0]
        );
        assert_eq!(Header::decode(&encoded)?, header);
        assert_eq!(
            header.area.path(header.fragment),
            PathBuf::from("blocks/987/00000000000000000000000123456789")
        );
        for index in [0, 8, 10, 12, 13, 14, 15, 64, 202, 204, 240, 319] {
            let mut bad = encoded;
            bad[index] ^= 0x80;
            assert!(Header::decode(&bad).is_err(), "{index}");
        }
        for size in [0, 7, HEADER_SIZE - 1] {
            assert!(Header::decode(&encoded[..size]).is_err());
        }
        let mut invalid = sample(FILE_LIMIT + 1);
        assert!(invalid.validate().is_err());
        invalid = sample(1);
        invalid.offset = u64::MAX;
        assert!(invalid.validate().is_err());
        invalid = sample(1);
        invalid.shard = 1;
        assert!(invalid.validate().is_err());
        Ok(())
    }

    #[test]
    fn range_proofs_bind_offset_content_and_published_commitment() -> Result<()> {
        let bytes = vec![9; UNIT * 2 + 17];
        let mut header = sample(bytes.len() as u64);
        header.digest = *blake3::hash(&bytes).as_bytes();
        let leaves: Vec<_> = bytes
            .chunks(UNIT)
            .enumerate()
            .map(|(n, data)| leaf(n, data))
            .collect();
        let nodes = tree(&header, &leaves)?;
        assert_eq!(nodes.len(), 7);
        for (unit, data) in bytes.chunks(UNIT).enumerate() {
            let proof: Vec<_> = proof_indices(&header, unit)?
                .iter()
                .map(|&index| nodes[index])
                .collect();
            verify_unit(&header, unit, data, &proof, &nodes[0])?;
            let mut corrupt = data.to_vec();
            corrupt[0] ^= 1;
            assert!(verify_unit(&header, unit, &corrupt, &proof, &nodes[0]).is_err());
        }
        let proof: Vec<_> = proof_indices(&header, 0)?
            .iter()
            .map(|&index| nodes[index])
            .collect();
        assert!(verify_unit(&header, 1, &bytes[..UNIT], &proof, &nodes[0]).is_err());
        let end = footer(&header, &nodes[0])?;
        let receipt = commitment(&header, &nodes[0])?;
        verify_footer(&header, &end, Some(&receipt))?;
        header.namespace = Uuid::from_u128(42);
        assert!(verify_footer(&header, &end, Some(&receipt)).is_err());
        assert!(proof_indices(&header, 3).is_err());
        let empty = sample(0);
        let nodes = tree(&empty, &[leaf(0, &[])])?;
        verify_unit(&empty, 0, &[], &[], &nodes[0])?;
        assert_eq!(empty.file_bytes(), 432);
        Ok(())
    }
}
