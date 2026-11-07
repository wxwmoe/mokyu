use super::{
    Format,
    batch::{Decoder, ObjectRanges, Record},
    verify_segment,
};
use crate::model::journal::record::{BlockKey, Change, Kind, Material, Operation, Reference};
use anyhow::{Context, Result, ensure};
use serde::Serialize;
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Default)]
pub(crate) struct Catalog {
    pub keys: BTreeMap<Uuid, BlockKey>,
    pub materials: BTreeMap<Reference, Material>,
    pub buckets: BTreeMap<Uuid, Bucket>,
    working: BTreeMap<Uuid, Bucket>,
    watermarks: BTreeMap<Uuid, i64>,
    ranges: BTreeMap<(Uuid, i64, i64), ObjectRanges>,
    changes: BTreeMap<(Uuid, i64), Change>,
    accepted: BTreeMap<(Uuid, i64), [u8; 32]>,
}

pub(crate) struct Bucket {
    pub id: Uuid,
    pub name: String,
    pub generation: i64,
    pub sequence: i64,
    pub paused: bool,
    pub objects: BTreeMap<String, Change>,
}

#[derive(Clone, Serialize, utoipa::ToSchema)]
#[schema(as = MetadataRecoveryBucket)]
pub(crate) struct Summary {
    pub id: Uuid,
    pub name: String,
    pub generation: String,
    pub sequence: String,
    pub stopped: bool,
    pub objects: String,
    pub logical_bytes: String,
}

impl Bucket {
    pub fn summary(&self) -> Result<Summary> {
        let bytes = self
            .objects
            .values()
            .try_fold(0i64, |size, c| {
                size.checked_add(c.attributes.as_ref()?.size)
            })
            .context("journal bucket size overflow")?;
        Ok(Summary {
            id: self.id,
            name: self.name.clone(),
            generation: self.generation.to_string(),
            sequence: self.sequence.to_string(),
            stopped: self.paused,
            objects: self.objects.len().to_string(),
            logical_bytes: bytes.to_string(),
        })
    }
}

impl Catalog {
    pub fn stage(&mut self, record: Record, format: &Format<'_>) -> Result<()> {
        match record {
            Record::Key(wrapped) => {
                let key = wrapped.open(format)?;
                ensure!(
                    !key.id.is_nil() && !key.name.is_empty() && key.name.len() <= 128,
                    "invalid recovery key identity"
                );
                crate::content::codec::Key::new(&key.algorithm, key.material)?;
                if let Some(old) = self.keys.get(&key.id) {
                    ensure!(
                        old.material == key.material
                            && old.algorithm == key.algorithm
                            && old.name == key.name,
                        "conflicting recovery key"
                    );
                } else {
                    self.keys.insert(key.id, key);
                }
            }
            Record::Material(material) => {
                let id = material.reference();
                ensure!(
                    !id.id.is_nil() && material.size() >= 0,
                    "invalid recovery material"
                );
                material.hold()?;
                if let Some(old) = self.materials.get(&id) {
                    ensure!(
                        serde_json::to_vec(old)? == serde_json::to_vec(&material)?,
                        "conflicting recovery material"
                    );
                } else {
                    self.materials.insert(id, material);
                }
            }
            Record::Ranges {
                bucket,
                generation,
                seq,
                offset,
                ranges,
            } => {
                self.ranges
                    .entry((bucket, generation, seq))
                    .or_default()
                    .append(offset, ranges)?;
            }
            Record::Change {
                value,
                range_count,
                range_digest,
            } => {
                let ranges = self
                    .ranges
                    .remove(&(value.bucket, value.generation, value.seq))
                    .unwrap_or_default();
                let value = ranges.finish(value, range_count, range_digest)?;
                let key = (value.bucket, value.seq);
                if let Some(hash) = self.accepted.get(&key) {
                    ensure!(
                        blake3::hash(&serde_json::to_vec(&value)?).as_bytes() == hash,
                        "conflicting repeated journal event"
                    );
                    return Ok(());
                }
                if let Some(old) = self.changes.get(&key) {
                    ensure!(
                        serde_json::to_vec(old)? == serde_json::to_vec(&value)?,
                        "conflicting bucket sequence"
                    );
                } else {
                    self.changes.insert(key, value);
                }
            }
        }
        Ok(())
    }

    pub fn apply(&mut self) -> Result<()> {
        for ((id, seq), change) in std::mem::take(&mut self.changes) {
            let previous = self.watermarks.entry(id).or_insert(0);
            ensure!(
                previous.checked_add(1) == Some(seq),
                "missing or repeated journal sequence"
            );
            *previous = seq;
            self.accepted.insert(
                (id, seq),
                *blake3::hash(&serde_json::to_vec(&change)?).as_bytes(),
            );
            if change.operation == Operation::Start {
                let generation = self
                    .working
                    .get(&id)
                    .or_else(|| self.buckets.get(&id))
                    .map_or(0, |b| b.generation);
                ensure!(
                    change.generation == generation + 1,
                    "journal baseline generation mismatch"
                );
                self.working.insert(
                    id,
                    Bucket {
                        id,
                        name: change.name,
                        generation: change.generation,
                        sequence: seq,
                        paused: false,
                        objects: BTreeMap::new(),
                    },
                );
                continue;
            }
            let bucket = if let Some(b) = self.working.get_mut(&id) {
                b
            } else {
                self.buckets
                    .get_mut(&id)
                    .context("missing journal baseline")?
            };
            ensure!(
                bucket.generation == change.generation && bucket.name == change.name,
                "journal bucket identity changed"
            );
            bucket.sequence = seq;
            match change.operation {
                Operation::Put => {
                    for span in &change.ranges {
                        let material = self
                            .materials
                            .get(&span.material)
                            .context("journal range material missing")?;
                        ensure!(
                            span.source_offset
                                .checked_add(span.length)
                                .is_some_and(|end| end <= material.size()),
                            "journal range exceeds source"
                        );
                        let holder = if let Material::Member { pack, .. } = material {
                            self.materials
                                .get(&Reference {
                                    kind: Kind::Pack,
                                    id: *pack,
                                    member: None,
                                })
                                .context("journal pack missing")?
                        } else {
                            material
                        };
                        if let Some(hold) = holder.hold()?
                            && let Some(key) = hold.key
                        {
                            ensure!(self.keys.contains_key(&key), "journal block key missing");
                        }
                    }
                    bucket
                        .objects
                        .insert(change.key.clone().context("missing object key")?, change);
                }
                Operation::Delete => {
                    bucket
                        .objects
                        .remove(change.key.as_ref().context("missing object key")?);
                }
                Operation::Pause => bucket.paused = true,
                Operation::Resume => bucket.paused = false,
                Operation::Ready => {
                    let ready = self
                        .working
                        .remove(&id)
                        .context("initial journal recovery point already published")?;
                    self.buckets.insert(id, ready);
                }
                Operation::Drop => {
                    self.working.remove(&id);
                    self.buckets.remove(&id);
                }
                Operation::Start => unreachable!(),
            }
        }
        Ok(())
    }
}

// The caller owns staging. Its recovery transaction is committed only after the seal validates.
pub(crate) fn stage_segment(
    format: &Format<'_>,
    path: &str,
    data: &[u8],
    first: u64,
    previous: [u8; 32],
    decoder: &mut Decoder,
    mut stage: impl FnMut(Record) -> Result<()>,
) -> Result<(u64, [u8; 32])> {
    verify_segment(format, path, data, first, previous, |frame| {
        decoder.read(
            &frame.bytes,
            frame.sequence.last - frame.sequence.first + 1,
            &mut stage,
        )
    })
}
