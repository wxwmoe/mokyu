use super::{
    Format,
    batch::{Decoder, ObjectRanges, Record},
    verify_segment,
};
use crate::model::journal::record::{BlockKey, Change, Kind, Material, Operation, Reference};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Default)]
pub(crate) struct Catalog {
    pub keys: BTreeMap<Uuid, BlockKey>,
    selected: Option<std::collections::BTreeSet<Uuid>>,
    pub materials: BTreeMap<Reference, Material>,
    pub buckets: BTreeMap<Uuid, Bucket>,
    working: BTreeMap<Uuid, Bucket>,
    watermarks: BTreeMap<Uuid, i64>,
    excluded: BTreeMap<Uuid, i64>,
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

#[derive(Clone, Deserialize, Serialize, utoipa::ToSchema)]
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
    pub fn select(&mut self, buckets: Option<&std::collections::BTreeSet<Uuid>>) {
        self.selected = buckets.cloned();
    }
    pub fn validate_pack_members(&self) -> Result<()> {
        use crate::content::pack::frame;
        let mut seen = std::collections::BTreeSet::new();
        let mut packs = BTreeMap::<Uuid, Vec<frame::Entry>>::new();
        for bucket in self.buckets.values() {
            for change in bucket.objects.values() {
                for span in &change.ranges {
                    if span.material.kind == Kind::Member && seen.insert(&span.material) {
                        let material = self
                            .materials
                            .get(&span.material)
                            .context("missing recovery member")?;
                        let (pack, entry) =
                            material.pack_member()?.context("invalid recovery member")?;
                        packs.entry(pack).or_default().push(entry);
                    }
                }
            }
        }
        for (id, mut entries) in packs {
            let reference = Reference {
                kind: Kind::Pack,
                id,
                member: None,
            };
            let Some(Material::Pack { pack }) = self.materials.get(&reference) else {
                anyhow::bail!("missing recovery pack")
            };
            entries.sort_unstable_by_key(|e| e.position.ordinal);
            frame::validate_entries(
                &entries,
                pack.stored_size.context("missing pack size")?.try_into()?,
                pack.raw_size,
                pack.member_count,
                false,
            )?;
        }
        Ok(())
    }

    pub fn required_keys(&self, bucket: &Bucket) -> Result<std::collections::BTreeSet<Uuid>> {
        let mut ids = std::collections::BTreeSet::new();
        for change in bucket.objects.values() {
            for span in &change.ranges {
                let m = self
                    .materials
                    .get(&span.material)
                    .context("missing recovery material")?;
                if let Material::Member { pack, .. } = m {
                    self.materials
                        .get(&Reference {
                            kind: Kind::Pack,
                            id: *pack,
                            member: None,
                        })
                        .context("missing recovery pack")?;
                }
                ensure!(
                    span.source_offset
                        .checked_add(span.length)
                        .is_some_and(|end| end <= self.materials[&span.material].size()),
                    "snapshot range exceeds material"
                );
                if let Some(id) = m.hold()?.and_then(|h| h.key) {
                    ensure!(self.keys.contains_key(&id), "missing recovery block key");
                    ids.insert(id);
                }
            }
        }
        Ok(ids)
    }

    pub fn replace_scope(
        &mut self,
        cuts: &[super::snapshot::BucketCut],
        excluded: &[super::snapshot::BucketCut],
    ) -> Result<()> {
        let replaced: std::collections::BTreeSet<_> =
            cuts.iter().chain(excluded).map(|c| c.id).collect();
        for id in &replaced {
            self.buckets.remove(id);
            self.working.remove(id);
            self.watermarks.remove(id);
            self.excluded.remove(id);
        }
        self.accepted.retain(|(id, _), _| !replaced.contains(id));
        self.ranges.retain(|(id, _, _), _| !replaced.contains(id));
        self.seed(cuts)?;
        self.exclude(excluded)
    }

    pub fn exclude(&mut self, cuts: &[super::snapshot::BucketCut]) -> Result<()> {
        for c in cuts {
            ensure!(
                !c.id.is_nil()
                    && c.generation > 0
                    && c.sequence > 0
                    && self.watermarks.insert(c.id, c.sequence).is_none(),
                "duplicate excluded snapshot bucket"
            );
            self.excluded.insert(c.id, c.generation);
        }
        Ok(())
    }

    pub fn seed(&mut self, cuts: &[super::snapshot::BucketCut]) -> Result<()> {
        for cut in cuts {
            ensure!(
                cut.generation > 0
                    && cut.sequence > 0
                    && !cut.id.is_nil()
                    && s3s::path::check_bucket_name(&cut.name),
                "invalid snapshot bucket"
            );
            ensure!(
                self.watermarks.insert(cut.id, cut.sequence).is_none(),
                "duplicate snapshot bucket"
            );
            self.buckets.insert(
                cut.id,
                Bucket {
                    id: cut.id,
                    name: cut.name.clone(),
                    generation: cut.generation,
                    sequence: cut.sequence,
                    paused: cut.stopped,
                    objects: BTreeMap::new(),
                },
            );
        }
        Ok(())
    }

    pub fn snapshot_row(&mut self, row: super::snapshot::Row, format: &Format<'_>) -> Result<()> {
        use super::snapshot::Row;
        match row {
            Row::Key(key) => self.stage(Record::Key(key), format),
            Row::Material(material) => self.stage(Record::Material(material), format),
            Row::Bucket(_) | Row::Drop { .. } => Ok(()),
            Row::Ranges {
                bucket,
                seq,
                offset,
                ranges,
                ..
            } => self
                .ranges
                .entry((bucket, 0, seq))
                .or_default()
                .append(offset, ranges),
            Row::Object {
                bucket,
                key,
                generation,
                seq,
                attributes,
                range_count,
                range_digest,
            } => {
                let ranges = self.ranges.remove(&(bucket, 0, seq)).unwrap_or_default();
                let Some(state) = self.buckets.get_mut(&bucket) else {
                    return Ok(());
                };
                if generation != state.generation {
                    return Ok(());
                }
                ensure!(seq <= state.sequence, "snapshot object beyond cut");
                let change = ranges.finish(
                    Change {
                        bucket,
                        name: state.name.clone(),
                        generation,
                        seq,
                        operation: Operation::Put,
                        key: Some(key.clone()),
                        attributes: Some(attributes),
                        ranges: Vec::new(),
                    },
                    range_count,
                    range_digest,
                )?;
                if state.objects.get(&key).is_none_or(|old| old.seq < seq) {
                    state.objects.insert(key, change);
                }
                Ok(())
            }
            Row::Delete {
                bucket,
                key,
                generation,
                seq,
            } => {
                if let Some(state) = self.buckets.get_mut(&bucket)
                    && generation == state.generation
                {
                    ensure!(seq <= state.sequence, "snapshot deletion beyond cut");
                    if state.objects.get(&key).is_some_and(|row| row.seq <= seq) {
                        state.objects.remove(&key);
                    }
                }
                Ok(())
            }
        }
    }

    pub fn stage(&mut self, record: Record, format: &Format<'_>) -> Result<()> {
        if let Some(ids) = &self.selected {
            let id = match &record {
                Record::Change { value, .. } => Some(value.bucket),
                Record::Ranges { bucket, .. } => Some(*bucket),
                _ => None,
            };
            if id.is_some_and(|b| !ids.contains(&b)) {
                return Ok(());
            }
        }
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
                        old.material == key.material && old.algorithm == key.algorithm,
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
                    .map_or_else(|| self.excluded.remove(&id).unwrap_or(0), |b| b.generation);
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
            if let Some(generation) = self.excluded.get(&id) {
                ensure!(
                    change.generation == *generation,
                    "unexpected excluded bucket generation"
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
                        if let Material::Member { pack, .. } = material {
                            self.materials
                                .get(&Reference {
                                    kind: Kind::Pack,
                                    id: *pack,
                                    member: None,
                                })
                                .context("journal pack is missing")?;
                        }
                        if let Some(hold) = material.hold()?
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
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn historical_key_alias_may_change_across_writers() {
        let mut catalog = Catalog::default();
        let root = crate::content::codec::Key::new("aes-256-gcm", [0x1a; 32]).unwrap();
        let format = Format {
            storage: Uuid::new_v4(),
            journal: Uuid::new_v4(),
            epoch: Uuid::new_v4(),
            key: &root,
            encrypted: false,
        };
        let wrapped = |key: &BlockKey| {
            Record::Key(crate::model::journal::record::WrappedKey::seal(key, &format).unwrap())
        };
        let mut key = BlockKey {
            id: Uuid::new_v4(),
            name: "old".into(),
            algorithm: "aes-256-gcm".into(),
            material: [4; 32],
        };
        let sealed = crate::model::journal::record::WrappedKey::seal(&key, &format).unwrap();
        let value = serde_json::to_value(&sealed).unwrap();
        assert!(value.get("material").is_none());
        assert_eq!(value["wrapped_material"].as_str().unwrap().len(), 80);
        let later = Format {
            epoch: Uuid::new_v4(),
            ..format
        };
        assert_eq!(sealed.open(&later).unwrap().material, key.material);
        for wrong in [
            Format {
                journal: Uuid::new_v4(),
                ..format
            },
            Format {
                storage: Uuid::new_v4(),
                ..format
            },
        ] {
            assert!(sealed.open(&wrong).is_err());
        }
        let mut changed = sealed.clone();
        changed.id = Uuid::new_v4();
        assert!(changed.open(&format).is_err());
        catalog.stage(wrapped(&key), &format).unwrap();
        key.name = "restored".into();
        catalog.stage(wrapped(&key), &format).unwrap();
        key.material = [7; 32];
        assert!(catalog.stage(wrapped(&key), &format).is_err());
    }
}
