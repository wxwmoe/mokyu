use super::{FRAME_LIMIT, Format, FrameType, Sequence};
use crate::model::journal::record::{Change, Material, Reference, Span, WrappedKey};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub(crate) enum Record {
    Key(WrappedKey),
    Material(Material),
    Ranges {
        bucket: Uuid,
        generation: i64,
        seq: i64,
        offset: i64,
        ranges: Vec<Span>,
    },
    Change {
        value: Change,
        range_count: u64,
        range_digest: [u8; 32],
    },
}

#[cfg(test)]
pub(crate) fn records(
    events: Vec<crate::model::journal::record::Prepared>,
    format: &Format<'_>,
) -> Result<Vec<Record>> {
    let mut keys = BTreeMap::new();
    let mut materials = BTreeMap::<Reference, Material>::new();
    let mut changes = Vec::new();
    for item in events {
        item.change.validate()?;
        for key in item.keys {
            if let Some(old) = keys.insert(key.id, key.material) {
                ensure!(old == key.material, "conflicting journal key");
            } else {
                changes.push(Record::Key(WrappedKey::seal(&key, format)?));
            }
        }
        for material in item.materials {
            let id = material.reference();
            if let Some(old) = materials.get(&id) {
                ensure!(
                    serde_json::to_vec(old)? == serde_json::to_vec(&material)?,
                    "conflicting journal material"
                );
            } else {
                materials.insert(id, material);
            }
        }
        let mut value = item.change;
        let ranges = std::mem::take(&mut value.ranges);
        let range_count = u64::try_from(ranges.len())?;
        let mut digest = [0; 32];
        for page in ranges.chunks(128) {
            digest = range_digest(digest, page)?;
            changes.push(Record::Ranges {
                bucket: value.bucket,
                generation: value.generation,
                seq: value.seq,
                offset: page[0].offset,
                ranges: page.to_vec(),
            });
        }
        changes.push(Record::Change {
            value,
            range_count,
            range_digest: digest,
        });
    }
    let mut result: Vec<_> = materials.into_values().map(Record::Material).collect();
    result.extend(changes);
    Ok(result)
}

pub(crate) struct PlainFrame {
    pub bytes: Vec<u8>,
    pub records: u32,
    pub origins: Vec<(usize, usize, bool)>,
}

// Keep records intact where possible; oversized records carry their offset and content digest.
pub(crate) fn frames<T: Serialize>(
    records: &[T],
    segment_limit: usize,
    encrypted: bool,
) -> Result<Vec<PlainFrame>> {
    let max = segment_limit
        .checked_sub(2 * Format::overhead(encrypted))
        .context("journal segment is too small")?
        .min(FRAME_LIMIT);
    ensure!(max >= 128, "journal frame is too small");
    let mut groups = Vec::new();
    let mut frame = PlainFrame {
        bytes: Vec::new(),
        records: 0,
        origins: Vec::new(),
    };
    for (index, record) in records.iter().enumerate() {
        let data = serde_json::to_vec(record)?;
        ensure!(
            data.len() <= FRAME_LIMIT,
            "journal record exceeds frame limit"
        );
        if data.len() + 5 <= max {
            if frame.bytes.len() + data.len() + 5 > max {
                groups.push(frame);
                frame = PlainFrame {
                    bytes: Vec::new(),
                    records: 0,
                    origins: Vec::new(),
                };
            }
            frame.bytes.push(0);
            frame
                .bytes
                .extend_from_slice(&(data.len() as u32).to_be_bytes());
            frame.bytes.extend_from_slice(&data);
            frame.records += 1;
            frame.origins.push((index, data.len() + 5, true));
        } else {
            if frame.records != 0 {
                groups.push(frame);
            }
            frame = PlainFrame {
                bytes: Vec::new(),
                records: 0,
                origins: Vec::new(),
            };
            let digest = blake3::hash(&data);
            let width = max - 45;
            for (i, bytes) in data.chunks(width).enumerate() {
                let mut encoded = vec![1];
                encoded.extend_from_slice(digest.as_bytes());
                encoded.extend_from_slice(&(data.len() as u32).to_be_bytes());
                encoded.extend_from_slice(&((i * width) as u32).to_be_bytes());
                encoded.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
                encoded.extend_from_slice(bytes);
                groups.push(PlainFrame {
                    bytes: encoded,
                    records: 1,
                    origins: vec![(
                        index,
                        45 + bytes.len(),
                        i * width + bytes.len() == data.len(),
                    )],
                });
            }
        }
    }
    if frame.records > 0 {
        groups.push(frame);
    }
    Ok(groups)
}

pub(crate) fn encode_frame(
    format: &Format<'_>,
    path: &str,
    first: u64,
    prior: [u8; 32],
    group: &PlainFrame,
) -> Result<(Vec<u8>, u64, [u8; 32])> {
    ensure!(group.records > 0, "empty journal group");
    let last = first
        .checked_add(u64::from(group.records) - 1)
        .context("journal sequence exhausted")?;
    let data = format.encode(
        FrameType::Changes,
        path,
        Sequence {
            first,
            last,
            previous: prior,
        },
        &group.bytes,
    )?;
    let digest = *blake3::hash(&data).as_bytes();
    Ok((data, last, digest))
}

fn u32_head(bytes: &[u8], offset: usize) -> Result<usize> {
    Ok(u32::from_be_bytes(
        bytes
            .get(offset..offset + 4)
            .context("truncated journal length")?
            .try_into()?,
    ) as usize)
}

#[derive(Default)]
pub(crate) struct Decoder {
    partial: Option<([u8; 32], usize, Vec<u8>)>,
}

impl Decoder {
    pub fn read<T: serde::de::DeserializeOwned>(
        &mut self,
        mut bytes: &[u8],
        expected: u64,
        mut visit: impl FnMut(T) -> Result<()>,
    ) -> Result<()> {
        let mut count = 0u64;
        while let Some(tag) = bytes.first() {
            match tag {
                0 => {
                    ensure!(self.partial.is_none(), "interrupted journal record");
                    let n = u32_head(bytes, 1)?;
                    let record = bytes.get(5..5 + n).context("truncated journal record")?;
                    visit(serde_json::from_slice(record)?)?;
                    bytes = &bytes[5 + n..];
                }
                1 => {
                    let hash: [u8; 32] = bytes
                        .get(1..33)
                        .context("truncated journal digest")?
                        .try_into()?;
                    let total = u32_head(bytes, 33)?;
                    let offset = u32_head(bytes, 37)?;
                    let n = u32_head(bytes, 41)?;
                    ensure!(
                        total <= FRAME_LIMIT
                            && n > 0
                            && offset.checked_add(n).is_some_and(|end| end <= total),
                        "invalid journal piece length"
                    );
                    let part = bytes.get(45..45 + n).context("truncated journal piece")?;
                    let (old, size, buffer) = self
                        .partial
                        .get_or_insert_with(|| (hash, total, Vec::new()));
                    ensure!(
                        *old == hash && *size == total && offset == buffer.len(),
                        "discontinuous journal record"
                    );
                    buffer.extend_from_slice(part);
                    if buffer.len() == total {
                        ensure!(
                            blake3::hash(buffer).as_bytes() == &hash,
                            "journal record digest mismatch"
                        );
                        visit(serde_json::from_slice(buffer)?)?;
                        self.partial = None;
                    }
                    bytes = &bytes[45 + n..];
                }
                _ => anyhow::bail!("unsupported journal record encoding"),
            }
            count += 1;
        }
        ensure!(count == expected, "journal frame record count mismatch");
        Ok(())
    }

    pub fn complete(&self) -> bool {
        self.partial.is_none()
    }
}

pub(crate) fn range_digest(previous: [u8; 32], ranges: &[Span]) -> Result<[u8; 32]> {
    let mut digest = blake3::Hasher::new();
    digest.update(&previous);
    serde_json::to_writer(&mut digest, ranges)?;
    Ok(*digest.finalize().as_bytes())
}

#[derive(Default)]
pub(crate) struct ObjectRanges {
    ranges: Vec<Span>,
    digest: [u8; 32],
    next: i64,
}
impl ObjectRanges {
    pub fn append(&mut self, offset: i64, ranges: Vec<Span>) -> Result<()> {
        ensure!(
            self.next == offset && !ranges.is_empty() && ranges.len() <= 128,
            "discontinuous journal range page"
        );
        self.digest = range_digest(self.digest, &ranges)?;
        for span in &ranges {
            ensure!(
                span.offset == self.next && span.length > 0,
                "invalid journal range"
            );
            self.next = self
                .next
                .checked_add(span.length)
                .context("journal offset overflow")?;
        }
        self.ranges.extend(ranges);
        Ok(())
    }

    pub fn finish(self, mut change: Change, count: u64, digest: [u8; 32]) -> Result<Change> {
        ensure!(
            change.ranges.is_empty() && self.ranges.len() as u64 == count && self.digest == digest,
            "incomplete journal range map"
        );
        change.ranges = self.ranges;
        change.validate()?;
        Ok(change)
    }
}
