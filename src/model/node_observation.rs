use super::{CompressionObservation, session::Run};
use super::{
    Id,
    metrics::{AccessBatch, ChunkCounts, PackCounts},
};
use anyhow::{Result, ensure};
use chrono::{DateTime, Timelike, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

pub(crate) const LIMIT: usize = 64;
pub(crate) const ACCESS_LIMIT: usize = 8192;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Access {
    pub(crate) sequence: i64,
    pub(crate) chunks: Vec<((Id, DateTime<Utc>), ChunkCounts)>,
    pub(crate) packs: Vec<((Id, DateTime<Utc>), PackCounts)>,
    pub(crate) members: Vec<((Id, Id, DateTime<Utc>), i64)>,
    pub(crate) dropped: bool,
}

impl Access {
    pub(crate) fn new(sequence: i64, pending: AccessBatch) -> Self {
        let mut chunks: Vec<_> = pending.chunks.into_iter().collect();
        let mut packs: Vec<_> = pending.packs.into_iter().collect();
        let mut members: Vec<_> = pending.members.into_iter().collect();
        chunks.sort_by_key(|row| row.0);
        packs.sort_by_key(|row| row.0);
        members.sort_by_key(|row| row.0);
        Self {
            sequence,
            chunks,
            packs,
            members,
            dropped: pending.dropped,
        }
    }

    pub(crate) fn pending(self) -> AccessBatch {
        AccessBatch {
            chunks: self.chunks.into_iter().collect(),
            packs: self.packs.into_iter().collect(),
            members: self.members.into_iter().collect(),
            dropped: self.dropped,
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        let invalid = || s3s::s3_error!(InvalidArgument, "invalid access observation");
        ensure!(
            self.sequence > 0
                && self.chunks.len() + self.packs.len() + self.members.len() <= ACCESS_LIMIT,
            invalid()
        );
        let id = |value: Id| matches!(value, Id::Uuid(id) if !id.is_nil());
        let hour = |value: DateTime<Utc>| {
            value.minute() == 0 && value.second() == 0 && value.nanosecond() == 0
        };
        let count = |value: i64| (0..=1_000_000_000).contains(&value);
        let bytes = |value: i64| (0..=1i64 << 50).contains(&value);
        let mut seen = HashSet::new();
        for ((chunk, at), c) in &self.chunks {
            ensure!(
                id(*chunk)
                    && hour(*at)
                    && seen.insert((*chunk, *at))
                    && count(c.reads)
                    && count(c.ranges)
                    && count(c.origins)
                    && bytes(c.bytes)
                    && c.ranges <= c.reads,
                invalid()
            );
        }
        seen.clear();
        for ((pack, at), p) in &self.packs {
            ensure!(
                id(*pack)
                    && hour(*at)
                    && seen.insert((*pack, *at))
                    && count(p.downloads)
                    && count(p.partial)
                    && p.partial <= p.downloads
                    && bytes(p.bytes)
                    && bytes(p.partial_bytes)
                    && bytes(p.useful)
                    && p.partial_bytes <= p.bytes
                    && p.useful <= p.bytes,
                invalid()
            );
        }
        let mut seen = HashSet::new();
        for ((pack, chunk, at), n) in &self.members {
            ensure!(
                id(*pack)
                    && id(*chunk)
                    && hour(*at)
                    && seen.insert((*pack, *chunk, *at))
                    && count(*n)
                    && *n > 0,
                invalid()
            );
        }
        Ok(())
    }
}

pub(crate) struct Batch {
    pub(crate) run: Run,
    pub(crate) compression: Vec<CompressionObservation>,
    pub(crate) credentials: Vec<String>,
    pub(crate) access: Option<Access>,
}

impl Batch {
    pub(crate) fn validate(&self) -> Result<()> {
        let invalid = || s3s::s3_error!(InvalidArgument, "invalid observation batch");
        ensure!(
            self.compression.len() + self.credentials.len() <= LIMIT
                && (self.access.is_some()
                    || !self.compression.is_empty()
                    || !self.credentials.is_empty()),
            invalid()
        );
        let mut keys = HashSet::new();
        if let Some(access) = &self.access {
            access.validate()?;
        }
        for observation in &self.compression {
            observation.validate()?;
            ensure!(
                matches!(observation.stream, super::Id::Uuid(id) if !id.is_nil())
                    && keys.insert((
                        observation.stream,
                        observation.version,
                        observation.encoding
                    )),
                invalid()
            );
        }
        let mut credentials = HashSet::new();
        for access in &self.credentials {
            ensure!(
                !access.is_empty()
                    && access.len() <= 128
                    && access.bytes().all(|byte| byte.is_ascii_graphic())
                    && credentials.insert(access),
                invalid()
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn access_observations_reject_invalid_counts_and_identities() {
        let at = Utc::now()
            .with_minute(0)
            .unwrap()
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap();
        let key = (Id::Uuid(uuid::Uuid::new_v4()), at);
        let mut pending = AccessBatch::default();
        pending.chunks.insert(
            key,
            ChunkCounts {
                reads: 2,
                ranges: 1,
                bytes: 100,
                origins: 1,
            },
        );
        let valid = Access::new(1, pending);
        valid.validate().unwrap();
        let mut invalid = valid.clone();
        invalid.chunks.push(invalid.chunks[0].clone());
        assert!(invalid.validate().is_err());
        invalid = valid.clone();
        invalid.chunks[0].1.bytes = -1;
        assert!(invalid.validate().is_err());
        invalid = valid.clone();
        invalid.chunks[0].1.ranges = 3;
        assert!(invalid.validate().is_err());
        invalid = valid.clone();
        invalid.chunks[0].0.0 = Id::Integer(1);
        assert!(invalid.validate().is_err());
        invalid = valid;
        invalid.sequence = 0;
        assert!(invalid.validate().is_err());
    }
}
