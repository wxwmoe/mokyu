use crate::{
    content::{codec::Chunk, pack::Pack},
    model::{Id, raw::RawSource},
};
use anyhow::{Context, Result, ensure};
use md5_checkpoint::{
    Digest, Md5,
    block_api::Md5Core,
    digest::{
        block_api::{Buffer, CoreProxy},
        common::hazmat::{SerializableState, SerializedState},
    },
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(crate) const SCAN_BYTES: u64 = 64 * 1024 * 1024;
pub(crate) const PROOF_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "source_type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Input {
    Chunk { chunk: Chunk },
    Pack { pack: Pack, members: Vec<Member> },
    Raw { source: RawSource },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Member {
    pub id: Id,
    pub uuid: Uuid,
    pub offset: u64,
    pub size: u32,
    pub hash: Vec<u8>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Target {
    pub source: Id,
    pub offset: u64,
    pub chunk: Chunk,
    pub reused: bool,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Prepared {
    pub start: u64,
    pub end: u64,
    pub policy: crate::model::cache::EncodingPolicy,
    pub targets: Vec<Target>,
    pub spans: Vec<Span>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "payload", content = "value", rename_all = "snake_case")]
pub(crate) enum Output {
    Chunks(Prepared),
    Raw(RawSource),
}

impl Output {
    pub(crate) fn chunks(&self) -> Result<&Prepared> {
        let Self::Chunks(value) = self else {
            anyhow::bail!("transfer destination is not chunks")
        };
        Ok(value)
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawCopy {
    pub unit: Id,
    pub revision: i64,
    pub input: Input,
    pub candidate: Id,
    pub target: RawSource,
    pub upload: Option<String>,
    pub parts: Vec<Part>,
    pub receipts: Vec<Option<String>>,
}

impl RawCopy {
    pub(crate) fn validate(&self) -> Result<()> {
        self.input.validate()?;
        ensure!(
            self.revision > 0
                && self.unit.is_valid()
                && self.candidate.is_valid()
                && self.target.id.is_valid()
                && self.target.revision > 0
                && self.target.quarantine_code.is_none()
                && !self.target.pending_upload
                && self.target.state == "writing"
                && self.target.storage_id != self.input.storage()?
                && !self.target.location_id.is_nil()
                && matches!(self.target.medium.as_str(), "local" | "shared_s3")
                && self.target.size == self.input.size()? as i64
                && self.parts.len() == self.receipts.len()
                && self.parts.len() <= 10000,
            "invalid raw transfer"
        );
        if let Input::Raw { source } = &self.input {
            ensure!(
                self.target.digest == source.digest,
                "raw transfer digest differs"
            );
            let mut offset = 0;
            for (index, part) in self.parts.iter().enumerate() {
                ensure!(
                    usize::from(part.number) == index + 1
                        && part.offset == offset
                        && part.size > 0
                        && part.size <= 5 * 1024 * 1024 * 1024
                        && part.digest.len() == 16
                        && (index + 1 == self.parts.len() || part.size >= 5 * 1024 * 1024),
                    "invalid raw transfer part"
                );
                offset = offset
                    .checked_add(part.size)
                    .context("raw transfer size overflow")?;
            }
            ensure!(
                offset == source.size as u64,
                "incomplete raw transfer parts"
            );
        } else {
            ensure!(
                self.parts.is_empty() && self.upload.is_none() && self.target.digest.is_none(),
                "unexpected encoded-source checkpoint"
            );
        }
        if let Some(upload) = &self.upload {
            ensure!(
                self.target.medium == "shared_s3" && !self.parts.is_empty(),
                "invalid raw upload"
            );
            RawProgress::Started {
                upload: upload.clone(),
            }
            .validate()?;
        }
        let mut pending = false;
        for receipt in &self.receipts {
            let Some(receipt) = receipt else {
                pending = true;
                continue;
            };
            ensure!(
                !pending && receipt.len() == 64 && receipt.bytes().all(|b| b.is_ascii_hexdigit()),
                "invalid raw part receipt"
            );
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum RawProgress {
    Started {
        upload: String,
    },
    Part {
        upload: Option<String>,
        number: u16,
        etag: String,
    },
    Reset {
        upload: String,
    },
    Finished {
        digest: Vec<u8>,
    },
}

impl RawProgress {
    pub(crate) fn validate(&self) -> Result<()> {
        let valid = |value: &str| {
            !value.is_empty() && value.len() <= 2048 && !value.bytes().any(|b| b < 32 || b == 127)
        };
        ensure!(
            match self {
                Self::Started { upload } | Self::Reset { upload } => valid(upload),
                Self::Part {
                    upload,
                    number,
                    etag,
                } =>
                    (1..=10000).contains(number)
                        && valid(etag)
                        && upload.as_ref().is_none_or(|value| valid(value)),
                Self::Finished { digest } => digest.len() == 16,
            },
            "invalid raw transfer checkpoint"
        );
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Copy {
    pub unit: Id,
    pub revision: i64,
    pub input: Input,
    pub candidate: Id,
    pub storage: Uuid,
    pub prepared: Prepared,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Work {
    pub unit: Id,
    pub revision: i64,
    pub input: Input,
    pub spans: Vec<Span>,
    pub targets: Vec<(Id, Id)>,
}

impl Work {
    pub(crate) fn from_copy(copy: &Copy, batch: &super::node_pack::Batch) -> Result<Self> {
        copy.validate()?;
        ensure!(
            batch.algorithm == copy.prepared.policy.algorithm
                && batch.key == copy.prepared.policy.block_key_id,
            "transfer encoding policy differs"
        );
        let mut expected = std::collections::BTreeMap::new();
        for target in copy.prepared.targets.iter().filter(|target| !target.reused) {
            ensure!(
                target.chunk.id == Id::Uuid(target.chunk.uuid),
                "invalid node target identity"
            );
            expected.entry(target.chunk.id).or_insert(target);
        }
        ensure!(
            batch.chunks.len() == expected.len(),
            "transfer target count differs"
        );
        let targets = batch
            .chunks
            .iter()
            .map(|chunk| {
                let target = expected
                    .get(&chunk.id)
                    .context("unexpected transfer destination")?;
                Ok((target.source, chunk.id))
            })
            .collect::<Result<_>>()?;
        let work = Self {
            unit: copy.unit,
            revision: copy.revision,
            input: copy.input.clone(),
            spans: copy.prepared.spans.clone(),
            targets,
        };
        work.validate(copy.storage, batch)?;
        Ok(work)
    }

    pub(crate) fn validate(&self, storage: Uuid, batch: &super::node_pack::Batch) -> Result<()> {
        self.input.validate()?;
        ensure!(
            self.revision > 0
                && storage != self.input.storage()?
                && batch.sources.is_empty()
                && !self.targets.is_empty()
                && self.targets.len() == batch.chunks.len()
                && self.targets.len() <= crate::content::pack::MAX_MEMBERS,
            "invalid transfer work"
        );
        let sources: std::collections::BTreeMap<_, _> = match &self.input {
            Input::Chunk { chunk } => {
                ensure!(self.spans.is_empty(), "unexpected chunk proofs");
                [(chunk.id, (chunk.hash.as_slice(), chunk.raw_size as u32))].into()
            }
            Input::Pack { members, .. } => {
                ensure!(self.spans.is_empty(), "unexpected pack proofs");
                members
                    .iter()
                    .map(|member| (member.id, (member.hash.as_slice(), member.size)))
                    .collect()
            }
            Input::Raw { source } => {
                chunk_span_bounds(source, &self.spans)?;
                self.spans
                    .iter()
                    .map(|span| (span.id, (span.hash.as_slice(), span.size)))
                    .collect()
            }
        };
        let mut ids = std::collections::BTreeSet::new();
        for ((source, target), chunk) in self.targets.iter().zip(&batch.chunks) {
            let (hash, size) = sources.get(source).context("transfer source is missing")?;
            ensure!(
                *target == chunk.id
                    && ids.insert(*target)
                    && chunk.hash == *hash
                    && chunk.size as u32 == *size
                    && chunk.ready.is_none(),
                "transfer destination differs"
            );
        }
        Ok(())
    }
}

pub(crate) fn chunk_span_bounds(source: &RawSource, spans: &[Span]) -> Result<(u64, u64)> {
    source.validate()?;
    ensure!(
        !spans.is_empty() && spans.len() <= 64,
        "invalid proof batch"
    );
    let start = spans[0].offset;
    let mut end = start;
    let mut ids = std::collections::BTreeSet::new();
    for span in spans {
        ensure!(
            span.offset == end
                && ids.insert(span.id)
                && span.hash.len() == 32
                && (1..=crate::content::codec::MAX as u32).contains(&span.size),
            "invalid transfer proof"
        );
        end = end
            .checked_add(u64::from(span.size))
            .context("transfer offset overflow")?;
    }
    ensure!(
        end <= source.size as u64 && end - start <= SCAN_BYTES,
        "invalid proof coverage"
    );
    Ok((start, end))
}

impl Copy {
    pub(crate) fn outputs(
        &self,
        outputs: &[crate::model::packing::PackOutput],
    ) -> Result<Vec<crate::model::Mapping>> {
        self.validate()?;
        let mut expected = std::collections::BTreeMap::new();
        for target in self.prepared.targets.iter().filter(|target| !target.reused) {
            expected
                .entry(target.chunk.id)
                .or_insert(crate::model::Mapping {
                    chunk: target.chunk.clone(),
                    offset_bytes: i64::try_from(target.offset)?,
                    pack_id: None,
                });
        }
        let mut produced = std::collections::BTreeSet::new();
        for output in outputs {
            ensure!(
                !output.chunks.is_empty() && output.pack.is_some() != output.independent.is_some(),
                "invalid transfer output"
            );
            let mut raw = 0;
            for id in &output.chunks {
                ensure!(produced.insert(*id), "duplicate transfer output");
                raw += i64::from(
                    expected
                        .get(id)
                        .context("unexpected transfer output")?
                        .chunk
                        .raw_size,
                );
            }
            let policy = &self.prepared.policy;
            if let Some(pack) = &output.pack {
                ensure!(
                    pack.storage_id == Some(self.storage)
                        && pack.raw_size == raw
                        && pack.member_count as usize == output.chunks.len()
                        && pack.algorithm == policy.algorithm
                        && pack.block_key_id == policy.block_key_id,
                    "transfer pack metadata differs"
                );
            }
            if let Some(chunk) = &output.independent {
                let original = &expected
                    .get(&chunk.id)
                    .context("unexpected transfer chunk")?
                    .chunk;
                ensure!(
                    output.chunks == [chunk.id]
                        && chunk.uuid == original.uuid
                        && chunk.hash == original.hash
                        && chunk.raw_size == original.raw_size
                        && chunk.storage_id == Some(self.storage)
                        && chunk.encoding_id != original.encoding_id
                        && chunk.algorithm == policy.algorithm
                        && chunk.block_key_id == policy.block_key_id,
                    "transfer chunk metadata differs"
                );
            }
        }
        ensure!(
            produced.len() == expected.len(),
            "incomplete transfer outputs"
        );
        Ok(expected.into_values().collect())
    }

    pub(crate) fn validate(&self) -> Result<()> {
        self.input.validate()?;
        self.prepared.policy.validate()?;
        let value = &self.prepared;
        ensure!(
            self.revision > 0
                && self.storage != self.input.storage()?
                && value.start < value.end
                && value.end <= self.input.size()?
                && !value.targets.is_empty()
                && value.targets.len() <= crate::content::pack::MAX_MEMBERS,
            "invalid transfer copy"
        );
        let sources: Vec<_> = match &self.input {
            Input::Chunk { chunk } => {
                ensure!(
                    value.start == 0 && value.spans.is_empty(),
                    "invalid chunk transfer window"
                );
                vec![(chunk.id, chunk.hash.as_slice(), chunk.raw_size as u32)]
            }
            Input::Pack { members, .. } => {
                ensure!(
                    value.start == 0 && value.spans.is_empty(),
                    "invalid pack transfer window"
                );
                members
                    .iter()
                    .map(|member| (member.id, member.hash.as_slice(), member.size))
                    .collect()
            }
            Input::Raw { .. } => {
                ensure!(
                    value.end - value.start <= SCAN_BYTES,
                    "raw transfer window is too large"
                );
                let mut end = value.start;
                for span in &value.spans {
                    ensure!(
                        span.offset == end
                            && span.hash.len() == 32
                            && (1..=crate::content::codec::MAX as u32).contains(&span.size),
                        "invalid raw transfer proof"
                    );
                    end = end
                        .checked_add(u64::from(span.size))
                        .context("transfer offset overflow")?;
                }
                ensure!(end == value.end, "raw transfer coverage differs");
                value
                    .spans
                    .iter()
                    .map(|span| (span.id, span.hash.as_slice(), span.size))
                    .collect()
            }
        };
        ensure!(
            sources.len() == value.targets.len(),
            "transfer source count differs"
        );
        let mut end = value.start;
        for ((id, hash, size), target) in sources.into_iter().zip(&value.targets) {
            ensure!(
                target.source == id
                    && target.offset == end
                    && target.chunk.storage_id == Some(self.storage)
                    && !target.chunk.uuid.is_nil()
                    && target.chunk.hash == hash
                    && target.chunk.raw_size == size as i32,
                "transfer target differs from source"
            );
            end = end
                .checked_add(u64::from(size))
                .context("transfer offset overflow")?;
        }
        ensure!(end == value.end, "transfer target coverage differs");
        Ok(())
    }
}

impl Input {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(!self.storage()?.is_nil(), "invalid transfer store");
        match self {
            Self::Chunk { chunk } => {
                ensure!(
                    chunk.state == "ready"
                        && !chunk.uuid.is_nil()
                        && !chunk.location_id.is_nil()
                        && chunk.hash.len() == 32
                        && (1..=crate::content::codec::MAX as i32).contains(&chunk.raw_size),
                    "invalid transfer chunk"
                );
            }
            Self::Pack { pack, members } => {
                ensure!(
                    pack.state == "ready"
                        && !members.is_empty()
                        && members.len() <= crate::content::pack::MAX_MEMBERS
                        && members.len() <= usize::try_from(pack.member_count)?,
                    "invalid transfer pack"
                );
                let mut ids = std::collections::BTreeSet::new();
                let mut uuids = std::collections::BTreeSet::new();
                let mut end = 0;
                for member in members {
                    ensure!(
                        ids.insert(member.id)
                            && uuids.insert(member.uuid)
                            && !member.uuid.is_nil()
                            && member.hash.len() == 32
                            && (1..=crate::content::codec::MAX as u32).contains(&member.size)
                            && member.offset >= end,
                        "invalid transfer member"
                    );
                    end = member
                        .offset
                        .checked_add(u64::from(member.size))
                        .context("transfer offset overflow")?;
                    ensure!(
                        end <= u64::try_from(pack.raw_size)?,
                        "transfer member exceeds pack"
                    );
                }
            }
            Self::Raw { source } => source.validate()?,
        }
        Ok(())
    }

    pub(crate) fn storage(&self) -> Result<Uuid> {
        match self {
            Self::Chunk { chunk } => chunk.storage_id,
            Self::Pack { pack, .. } => pack.storage_id,
            Self::Raw { source } => Some(source.storage_id),
        }
        .context("transfer source has no store")
    }

    pub(crate) fn size(&self) -> Result<u64> {
        match self {
            Self::Chunk { chunk } => {
                u64::try_from(chunk.raw_size).context("invalid transfer chunk size")
            }
            Self::Pack { members, .. } => members.iter().try_fold(0u64, |size, member| {
                size.checked_add(u64::from(member.size))
                    .context("transfer size overflow")
            }),
            Self::Raw { source } => u64::try_from(source.size).context("invalid transfer raw size"),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Span {
    pub id: Id,
    pub offset: u64,
    pub size: u32,
    pub hash: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Proof {
    pub offset: u64,
    pub size: u32,
    pub hash: Vec<u8>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Checkpoints {
    pub object: Checksum,
    pub part: Option<Checksum>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Part {
    pub number: u16,
    pub offset: u64,
    pub size: u64,
    pub digest: Vec<u8>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Scan {
    pub unit: Id,
    pub revision: i64,
    pub source: RawSource,
    pub checksum: Checkpoints,
    pub position: u64,
    pub until: u64,
    pub chunking: Option<bool>,
    pub part_size: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Scanned {
    pub unit: Id,
    pub revision: i64,
    pub start: u64,
    pub end: u64,
    pub checksum: Option<Checkpoints>,
    pub digest: Option<Vec<u8>>,
    pub spans: Vec<Proof>,
    pub part: Option<Part>,
}

impl Scan {
    pub(crate) fn next(
        unit: Id,
        revision: i64,
        source: RawSource,
        position: u64,
        checksum: Checkpoints,
        chunking: Option<bool>,
    ) -> Result<Self> {
        source.validate()?;
        let size = u64::try_from(source.size)?;
        ensure!(position <= size, "transfer scan is past the source");
        let part_size = if chunking.is_none() {
            let size = size
                .div_ceil(10000)
                .max(SCAN_BYTES)
                .div_ceil(PROOF_BYTES as u64)
                * PROOF_BYTES as u64;
            ensure!(
                size <= 5 * 1024 * 1024 * 1024,
                "transfer source exceeds multipart limit"
            );
            Some(size)
        } else {
            None
        };
        let mut until = size.min(position.saturating_add(SCAN_BYTES));
        if let Some(part) = part_size {
            until = until.min((position / part + 1) * part);
        }
        Ok(Self {
            unit,
            revision,
            source,
            checksum,
            position,
            until,
            chunking,
            part_size,
        })
    }

    pub(crate) fn validate(&self, result: &Scanned) -> Result<()> {
        self.source.validate()?;
        let size = u64::try_from(self.source.size)?;
        ensure!(
            self.position <= self.until
                && self.until <= size
                && result.unit == self.unit
                && result.revision == self.revision
                && result.start == self.position
                && result.end <= self.until
                && (result.end > result.start || size == 0)
                && if self.chunking == Some(true) {
                    result.end == size
                        || self.until - result.end < crate::content::codec::MAX as u64
                } else {
                    result.end == self.until
                },
            "transfer scan identity changed"
        );
        let maximum = if self.chunking.is_some() {
            crate::content::codec::MAX
        } else {
            PROOF_BYTES
        };
        ensure!(
            result.spans.len() <= (SCAN_BYTES as usize / crate::content::codec::MIN) + 1,
            "transfer scan has too many segments"
        );
        let mut position = result.start;
        for proof in &result.spans {
            ensure!(
                proof.offset == position
                    && proof.size > 0
                    && proof.size as usize <= maximum
                    && proof.hash.len() == 32,
                "invalid transfer segment"
            );
            position = position
                .checked_add(u64::from(proof.size))
                .context("transfer scan overflow")?;
        }
        ensure!(
            position == result.end,
            "transfer scan has incomplete coverage"
        );
        if result.end == size {
            ensure!(
                result.checksum.is_none() && result.digest == self.source.digest,
                "transfer source checksum differs"
            );
        } else {
            ensure!(
                result.digest.is_none(),
                "premature transfer source checksum"
            );
            let checkpoint = result
                .checksum
                .as_ref()
                .context("transfer checkpoint missing")?;
            ensure!(
                checkpoint.object.offset == result.end / 64 * 64,
                "transfer checkpoint position differs"
            );
            checkpoint.object.resume()?;
        }
        if let Some(length) = self.part_size {
            ensure!(
                (5 * 1024 * 1024..=5 * 1024 * 1024 * 1024).contains(&length) && length % 64 == 0,
                "invalid transfer part size"
            );
            let start = self.position / length * length;
            let complete = result.end == size || result.end == start + length;
            if complete && size > 0 {
                let part = result
                    .part
                    .as_ref()
                    .context("transfer part checksum missing")?;
                ensure!(
                    u64::from(part.number) == start / length + 1
                        && part.offset == start
                        && part.size == result.end - start
                        && part.digest.len() == 16,
                    "transfer part identity differs"
                );
                ensure!(
                    result.checksum.as_ref().is_none_or(|c| c.part.is_none()),
                    "completed transfer part retains a checkpoint"
                );
            } else if !complete {
                ensure!(result.part.is_none(), "premature transfer part checksum");
                let checkpoint = result
                    .checksum
                    .as_ref()
                    .and_then(|c| c.part.as_ref())
                    .context("transfer part checkpoint missing")?;
                ensure!(
                    checkpoint.offset == (result.end - start) / 64 * 64,
                    "transfer part checkpoint position differs"
                );
                checkpoint.resume()?;
            } else {
                ensure!(result.part.is_none(), "empty source has a transfer part");
            }
        } else {
            ensure!(
                result.part.is_none() && result.checksum.as_ref().is_none_or(|c| c.part.is_none()),
                "unexpected transfer part checksum"
            );
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Checksum {
    pub offset: u64,
    pub state: Vec<u8>,
}

impl Checksum {
    pub(crate) fn resume(&self) -> Result<Md5> {
        ensure!(self.offset % 64 == 0, "unaligned transfer checksum");
        if self.offset == 0 {
            ensure!(
                self.state.is_empty(),
                "unexpected initial transfer checksum"
            );
            return Ok(Md5::new());
        }
        let state = SerializedState::<Md5Core>::try_from(self.state.as_slice())
            .context("invalid transfer checksum length")?;
        let core = Md5Core::deserialize(&state).context("invalid transfer checksum state")?;
        Ok(Md5::compose(core, Buffer::<Md5Core>::default()))
    }

    pub(crate) fn checkpoint(offset: u64, digest: Md5) -> Result<Self> {
        let (core, buffer) = digest.decompose();
        ensure!(
            buffer.get_pos() as u64 == offset % 64,
            "invalid transfer checksum position"
        );
        let offset = offset - buffer.get_pos() as u64;
        // Replay the partial block; never persist its source plaintext.
        if offset == 0 {
            return Ok(Self::default());
        }
        Ok(Self {
            offset,
            state: core.serialize().to_vec(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_copy_bounds_full_pack_and_checks_source_identity() -> Result<()> {
        use super::super::{node_pack as wire, node_work::Plan};
        let storage = Uuid::new_v4();
        let count = crate::content::pack::MAX_MEMBERS;
        let members: Vec<_> = (0..count)
            .map(|index| {
                let uuid = Uuid::now_v7();
                Member {
                    id: Id::Uuid(uuid),
                    uuid,
                    offset: index as u64 * 4096,
                    size: 4096,
                    hash: vec![255; 32],
                }
            })
            .collect();
        let chunks: Vec<_> = members
            .iter()
            .map(|member| wire::Target {
                id: Id::Uuid(Uuid::now_v7()),
                size: member.size as i32,
                hash: member.hash.clone(),
                previous_size: None,
                compress: true,
                location: Uuid::now_v7(),
                nonce: Some(vec![255; 12]),
                ready: None,
            })
            .collect();
        let work = Work {
            unit: Id::Uuid(Uuid::now_v7()),
            revision: 1,
            targets: members
                .iter()
                .zip(&chunks)
                .map(|(source, target)| (source.id, target.id))
                .collect(),
            input: Input::Pack {
                pack: Pack {
                    id: Id::Uuid(Uuid::now_v7()),
                    location_id: Uuid::now_v7(),
                    storage_id: Some(Uuid::new_v4()),
                    algorithm: "aes-256-gcm".into(),
                    block_key_id: Some(Uuid::new_v4()),
                    raw_size: count as i64 * 4096,
                    stored_size: Some(count as i64 * 4096),
                    compressed: false,
                    nonce: Some(vec![255; 12]),
                    digest: Some(vec![255; 32]),
                    member_count: count as i32,
                    state: "ready".into(),
                    created_at: chrono::Utc::now(),
                },
                members,
            },
            spans: vec![],
        };
        let mut batch = wire::Batch {
            algorithm: "chacha20-poly1305".into(),
            key: Some(Uuid::new_v4()),
            created: chrono::Utc::now(),
            sources: vec![],
            chunks,
            groups: (0..count).map(|index| (index, index + 1)).collect(),
            packs: vec![],
        };
        work.validate(storage, &batch)?;
        let plan = Plan::CopyTransfer {
            storage,
            transfer: Box::new(work.clone()),
            batch: Box::new(batch.clone()),
            source_permits: [Uuid::new_v4(), Uuid::new_v4()],
        };
        let encoded = serde_json::to_vec(&plan)?;
        assert!(encoded.len() <= 3 * 1024 * 1024, "{}", encoded.len());
        let _: Plan = serde_json::from_slice(&encoded)?;
        batch.chunks[0].hash[0] ^= 1;
        assert!(work.validate(storage, &batch).is_err());
        batch.chunks[0].hash[0] ^= 1;
        let mut duplicate = work.clone();
        duplicate.targets[1] = duplicate.targets[0];
        batch.chunks[1] = batch.chunks[0].clone();
        assert!(duplicate.validate(storage, &batch).is_err());
        assert!(work.validate(work.input.storage()?, &batch).is_err());
        Ok(())
    }

    #[test]
    fn checksum_resumes_at_multiple_boundaries() -> Result<()> {
        let data: Vec<_> = (0..8197).map(|n| (n * 37) as u8).collect();
        let mut state = Checksum::default();
        for end in [1, 63, 64, 129, 1023, 4097, 8192] {
            let mut digest = state.resume()?;
            digest.update(&data[state.offset as usize..end]);
            state = Checksum::checkpoint(end as u64, digest)?;
            state = serde_json::from_slice(&serde_json::to_vec(&state)?)?;
        }
        let mut digest = state.resume()?;
        digest.update(&data[state.offset as usize..]);
        assert_eq!(
            digest.finalize().as_slice(),
            <md5::Md5 as md5::Digest>::digest(&data).as_slice()
        );
        assert_eq!(state.state.len(), 24);
        Ok(())
    }

    #[test]
    fn checksum_rejects_partial_or_malformed_states() -> Result<()> {
        assert!(
            Checksum {
                offset: 64,
                state: vec![0; 23]
            }
            .resume()
            .is_err()
        );
        assert!(
            Checksum {
                offset: 1,
                state: vec![]
            }
            .resume()
            .is_err()
        );
        assert!(
            Checksum {
                offset: 0,
                state: vec![0]
            }
            .resume()
            .is_err()
        );
        let mut digest = Md5::new();
        digest.update(b"private content");
        assert!(Checksum::checkpoint(64, digest).is_err());
        assert_eq!(Checksum::checkpoint(0, Md5::new())?, Checksum::default());
        Ok(())
    }
}
