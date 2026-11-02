use crate::{content::raw_pages::PAGE, model::raw::RawSource};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{ops::Range, path::Path};
use uuid::Uuid;

const MAGIC: &[u8; 8] = b"MKYRAW01";
pub(super) const LIMIT: usize = 2 * 1024 * 1024;

#[derive(Default, Serialize, Deserialize)]
pub(super) struct Runs(Vec<Range<u64>>);
impl Runs {
    pub(super) fn mark(&mut self, mut range: Range<u64>) {
        if range.is_empty() {
            return;
        }
        let at = self.0.partition_point(|r| r.end < range.start);
        let count = self.0[at..].partition_point(|r| r.start <= range.end);
        if count > 0 {
            range.start = range.start.min(self.0[at].start);
            range.end = range.end.max(self.0[at + count - 1].end);
        }
        self.0.splice(at..at + count, std::iter::once(range));
    }
    pub(super) fn next(&self, start: u64, end: u64) -> (bool, u64) {
        match self.0.get(self.0.partition_point(|r| r.end <= start)) {
            Some(r) if r.start <= start => (true, r.end.min(end)),
            Some(r) => (false, r.start.min(end)),
            None => (false, end),
        }
    }
    pub(super) fn covers(&self, r: Range<u64>) -> bool {
        self.next(r.start, r.end) == (true, r.end)
    }
    fn valid(&self, end: u64) -> bool {
        self.0.iter().all(|r| r.start < r.end && r.end <= end)
            && self.0.windows(2).all(|p| p[0].end < p[1].start)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Map {
    id: Uuid,
    address: Uuid,
    revision: i64,
    size: u64,
    digest: [u8; 16],
    pub(super) written: Runs,
    pub(super) verified: Runs,
}

impl Map {
    pub(super) fn new(source: &RawSource) -> Result<Self> {
        Ok(Self {
            id: source.location_id,
            address: source.address(),
            revision: source.revision,
            size: source.size.try_into()?,
            digest: source
                .digest
                .as_deref()
                .context("missing raw identity")?
                .try_into()?,
            written: Default::default(),
            verified: Default::default(),
        })
    }
    pub(super) fn same(&self, r: &RawSource) -> bool {
        self.id == r.location_id
            && self.address == r.address()
            && self.revision == r.revision
            && self.size == r.size as u64
            && Some(self.digest.as_slice()) == r.digest.as_deref()
    }
    pub(super) fn id(&self) -> Uuid {
        self.id
    }
    pub(super) fn size(&self) -> u64 {
        self.size
    }
    pub(super) fn high_water(&self) -> u64 {
        self.written.0.last().map_or(0, |r| r.end)
    }
    pub(super) fn encode(&self) -> Result<Vec<u8>> {
        let value = serde_json::to_vec(self)?;
        ensure!(value.len() <= LIMIT, "partial raw map is full");
        let mut data = Vec::with_capacity(40 + value.len());
        data.extend_from_slice(MAGIC);
        data.extend_from_slice(blake3::hash(&value).as_bytes());
        data.extend(value);
        Ok(data)
    }
    pub(super) async fn load(path: &Path) -> Result<Self> {
        ensure!(
            tokio::fs::symlink_metadata(path).await?.is_file(),
            "invalid raw coverage file"
        );
        let data = super::local::read_bounded(path, LIMIT + 40).await?;
        ensure!(
            data.len() >= 40
                && &data[..8] == MAGIC
                && blake3::hash(&data[40..]).as_bytes() == &data[8..40],
            "invalid partial raw map"
        );
        let map: Self = serde_json::from_slice(&data[40..])?;
        ensure!(
            map.written.valid(map.size)
                && map.verified.valid(map.size.div_ceil(PAGE as u64))
                && map.verified.0.iter().all(|r| map.written.covers(
                    r.start * PAGE as u64..r.end.saturating_mul(PAGE as u64).min(map.size)
                )),
            "invalid partial raw coverage"
        );
        Ok(map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn partial_intervals_preserve_holes_until_neighbors_meet() {
        let mut r = Runs::default();
        r.mark(40..80);
        r.mark(0..4);
        r.mark(20..30);
        r.mark(25..45);
        assert_eq!(r.0, [0..4, 20..80]);
        assert_eq!(r.next(4, 99), (false, 20));
        assert_eq!(r.next(35, 99), (true, 80));
        r.mark(4..20);
        assert!(r.covers(0..80));
    }
}
