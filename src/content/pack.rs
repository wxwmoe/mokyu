pub(crate) mod frame;

use crate::model::Id;
use crate::{
    config,
    content::codec::{self, Chunk},
    content::compression::Pool,
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const MAX_RAW: usize = 256 * 1024 * 1024;
pub const MAX_MEMBERS: usize = 4096;
pub const MAX_MEMBER_TAG: usize = 16;
pub const MAX_PAYLOAD: usize = MAX_RAW + frame::HEADER.len() + MAX_MEMBERS * MAX_MEMBER_TAG;

#[derive(Clone, Copy, Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompressionStrategy {
    Never,
    Always,
    Sample,
}

#[derive(Clone, Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub enabled: bool,
    pub max_size: String,
    #[serde(rename = "upload_flush_timeout")]
    pub upload_cache_timeout: String,
    pub compression_strategy: Option<CompressionStrategy>,
    pub maintenance_concurrency: usize,
    pub interval: String,
    pub reuse_interval: String,
    pub reclaim_interval: String,
    pub repack_interval: String,
    pub repack_cooldown: String,
    pub reclaim_min_savings_bytes: String,
    pub range_optimization: bool,
    pub range_interval: String,
    pub range_window: String,
    pub range_min_downloads: u32,
    pub range_min_savings_percent: u8,
    pub range_min_savings_bytes: String,
    pub range_repack_after: String,
    pub range_repack_window: String,
    pub range_repack_retry_interval: String,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: true,
            max_size: "32MiB".into(),
            upload_cache_timeout: "60s".into(),
            compression_strategy: None,
            maintenance_concurrency: 1,
            interval: "1h".into(),
            reuse_interval: "5m".into(),
            reclaim_interval: "30m".into(),
            repack_interval: "24h".into(),
            repack_cooldown: "1h".into(),
            reclaim_min_savings_bytes: "4MiB".into(),
            range_optimization: true,
            range_interval: "15m".into(),
            range_window: "24h".into(),
            range_min_downloads: 8,
            range_min_savings_percent: 50,
            range_min_savings_bytes: "64MiB".into(),
            range_repack_after: "30d".into(),
            range_repack_window: "7d".into(),
            range_repack_retry_interval: "7d".into(),
        }
    }
}
impl Config {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            (codec::MAX as u64..=MAX_RAW as u64).contains(&config::bytes(&self.max_size)?),
            "pack.max_size must be 4MiB..256MiB"
        );
        ensure!(
            self.maintenance_concurrency > 0,
            "pack.maintenance_concurrency must be positive"
        );
        config::bytes(&self.reclaim_min_savings_bytes)?;
        config::bytes(&self.range_min_savings_bytes)?;
        ensure!(
            self.range_min_downloads > 0 && (1..=100).contains(&self.range_min_savings_percent),
            "pack Range thresholds must be positive; percentage must be 1..100"
        );
        for time in [
            &self.upload_cache_timeout,
            &self.interval,
            &self.reuse_interval,
            &self.reclaim_interval,
            &self.repack_interval,
            &self.repack_cooldown,
            &self.range_interval,
            &self.range_window,
            &self.range_repack_after,
            &self.range_repack_window,
            &self.range_repack_retry_interval,
        ] {
            config::seconds(time)?;
        }
        Ok(())
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct TierIndex {
    pub home_digest: Vec<u8>,
    pub entries: Vec<frame::Entry>,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct Pack {
    pub id: Id,
    pub location_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub home_storage_id: Option<Uuid>,
    pub storage_id: Option<Uuid>,
    pub algorithm: String,
    pub block_key_id: Option<Uuid>,
    pub raw_size: i64,
    pub stored_size: Option<i64>,
    pub compressed: bool,
    pub nonce: Option<Vec<u8>>,
    pub digest: Option<Vec<u8>>,
    pub member_count: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) tier_index: Option<Box<TierIndex>>,
    pub state: String,
    pub created_at: DateTime<Utc>,
}
impl Pack {
    pub fn home_storage(&self) -> Option<Uuid> {
        self.home_storage_id.or(self.storage_id)
    }

    pub fn address(&self) -> Uuid {
        self.address.unwrap_or(self.location_id)
    }

    pub(crate) fn home_digest(&self) -> Option<&[u8]> {
        self.tier_index
            .as_ref()
            .map(|idx| idx.home_digest.as_slice())
            .or(self.digest.as_deref())
    }

    pub(crate) fn resolve_entry(&self, entry: &mut frame::Entry) -> Result<()> {
        if let Some(tier) = &self.tier_index {
            let index = tier
                .entries
                .binary_search_by_key(&entry.position.ordinal, |e| e.position.ordinal)
                .map_err(|_| codec::IntegrityError::Metadata)?;
            let current = &tier.entries[index];
            ensure!(
                current.uuid == entry.uuid
                    && current.hash == entry.hash
                    && current.raw_size == entry.raw_size
                    && current.position.ordinal == entry.position.ordinal,
                codec::IntegrityError::Metadata
            );
            entry.position.clone_from(&current.position);
        }
        Ok(())
    }

    pub fn validate(&self, _keys: &dyn codec::KeySource) -> Result<()> {
        let maximum = self.payload_size()?;
        ensure!(
            !self.location_id.is_nil()
                && self.algorithm == "none"
                && self.block_key_id.is_none()
                && self.nonce.is_none()
                && !self.compressed
                && self.digest.as_ref().is_some_and(|hash| hash.len() == 32)
                && self.stored_size.is_some_and(|size| size
                    >= frame::HEADER.len() as i64 + i64::from(self.member_count)
                    && size <= maximum as i64),
            codec::IntegrityError::Metadata
        );
        Ok(())
    }

    pub fn payload_size(&self) -> Result<usize> {
        ensure!(
            self.id.is_valid()
                && (2..=MAX_MEMBERS as i32).contains(&self.member_count)
                && (i64::from(self.member_count)..=MAX_RAW as i64).contains(&self.raw_size),
            "invalid pack dimensions"
        );
        Ok(frame::HEADER.len() + self.raw_size as usize + self.member_count as usize * 16)
    }
}

pub struct Member {
    pub id: Uuid,
    pub hash: [u8; 32],
    pub start: usize,
    pub len: usize,
}
pub struct Decoded {
    pub bytes: Bytes,
    pub members: Vec<Member>,
}
impl Decoded {
    pub fn chunk(&self, c: &Chunk) -> Result<Bytes> {
        let m = self
            .members
            .iter()
            .find(|m| m.id == c.uuid)
            .context("chunk absent from pack")?;
        ensure!(
            m.len == c.raw_size as usize && m.hash.as_slice() == c.hash,
            "pack member metadata mismatch"
        );
        // A returned chunk must not retain an unaccounted whole-pack allocation.
        Ok(Bytes::copy_from_slice(
            &self.bytes[m.start..m.start + m.len],
        ))
    }
}

pub(crate) fn decode_index(
    pack: &Pack,
    file: Vec<u8>,
    entries: &[frame::Entry],
    keys: &dyn codec::KeySource,
    pool: &Pool,
    verify: bool,
) -> Result<Decoded> {
    frame::validate_file(pack, &file)?;
    frame::validate_entries(
        entries,
        file.len() as u64,
        pack.raw_size,
        pack.member_count,
        false,
    )?;
    let mut bytes = Vec::with_capacity(entries.iter().map(|entry| entry.raw_size as usize).sum());
    let mut members = Vec::with_capacity(entries.len());
    for entry in entries {
        let range = entry
            .position
            .range(entry.raw_size, file.len() as u64, pack.member_count)?;
        let body = file[range.start as usize..range.end as usize].to_vec();
        let raw = frame::decode_entry(pack, entry, body, keys, pool, verify)?;
        members.push(Member {
            id: entry.uuid,
            hash: entry.hash,
            start: bytes.len(),
            len: raw.len(),
        });
        bytes.extend_from_slice(&raw);
    }
    Ok(Decoded {
        bytes: bytes.into(),
        members,
    })
}
