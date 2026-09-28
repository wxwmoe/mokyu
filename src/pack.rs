use crate::{
    codec::{self, Chunk},
    compression::{Pool, Strategy},
    config::{self, Secrets},
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const MAX_RAW: usize = 256 * 1024 * 1024;
pub const MAX_MEMBERS: usize = 4096;
pub const MAX_PAYLOAD: usize = MAX_RAW + 12 + MAX_MEMBERS * 44;
const MAGIC: &[u8; 8] = b"MGWPACK\x01";

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompressionStrategy {
    Always,
    Sample,
    FileType,
    ChunkHint,
}

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub enabled: bool,
    pub max_size: String,
    pub upload_cache_timeout: String,
    pub compression_strategy: Option<CompressionStrategy>,
    pub maintenance_concurrency: usize,
    pub interval: String,
    pub reuse_interval: String,
    pub reclaim_interval: String,
    pub repack_interval: String,
    pub repack_cooldown: String,
    pub reclaim_min_savings_bytes: String,
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
        for time in [
            &self.upload_cache_timeout,
            &self.interval,
            &self.reuse_interval,
            &self.reclaim_interval,
            &self.repack_interval,
            &self.repack_cooldown,
        ] {
            config::seconds(time)?;
        }
        Ok(())
    }
}

#[derive(Clone, sqlx::FromRow, Serialize)]
pub struct Pack {
    pub id: i64,
    pub storage_id: Uuid,
    pub algorithm: String,
    pub key_id: String,
    pub raw_size: i64,
    pub stored_size: Option<i64>,
    pub compressed: bool,
    pub nonce: Option<Vec<u8>>,
    pub digest: Option<Vec<u8>>,
    pub member_count: i32,
    pub state: String,
    pub created_at: DateTime<Utc>,
}
impl Pack {
    pub fn validate(&self, secrets: &Secrets) -> Result<()> {
        self.aad().context(codec::IntegrityError::Metadata)?;
        let tag = if self.algorithm == "none" { 0 } else { 16 };
        ensure!(
            self.stored_size
                .is_some_and(|n| n > tag && n <= self.payload_size().unwrap_or(0) as i64 + tag),
            codec::IntegrityError::Metadata
        );
        ensure!(
            self.compressed || self.stored_size == Some(self.payload_size()? as i64 + tag),
            codec::IntegrityError::Metadata
        );
        if self.algorithm == "none" {
            ensure!(
                self.key_id.is_empty() && self.nonce.is_none(),
                codec::IntegrityError::Metadata
            );
        } else {
            let key = secrets
                .keys
                .get(&self.key_id)
                .context(codec::IntegrityError::MissingKey)?;
            ensure!(
                key.algorithm == self.algorithm
                    && self.nonce.as_ref().is_some_and(|n| n.len() == 12),
                codec::IntegrityError::Metadata
            );
        }
        Ok(())
    }
    pub fn payload_size(&self) -> Result<usize> {
        ensure!(
            self.id > 0
                && (1..=MAX_RAW as i64).contains(&self.raw_size)
                && (2..=MAX_MEMBERS as i32).contains(&self.member_count),
            "invalid pack dimensions"
        );
        Ok(self.raw_size as usize + 12 + self.member_count as usize * 44)
    }
    fn aad(&self) -> Result<Vec<u8>> {
        self.payload_size()?;
        let stored = self.stored_size.context("missing pack size")?;
        ensure!(
            stored > 0 && stored <= MAX_PAYLOAD as i64 + 16,
            "invalid pack stored size"
        );
        let digest = self.digest.as_deref().context("missing pack digest")?;
        ensure!(digest.len() == 32, "invalid pack digest");
        let mut aad = MAGIC.to_vec();
        aad.extend_from_slice(&self.id.to_be_bytes());
        aad.extend_from_slice(self.storage_id.as_bytes());
        aad.extend_from_slice(&self.raw_size.to_be_bytes());
        aad.extend_from_slice(&stored.to_be_bytes());
        aad.extend_from_slice(&self.member_count.to_be_bytes());
        aad.extend_from_slice(digest);
        aad.push(u8::from(self.compressed));
        aad.push(match self.algorithm.as_str() {
            "none" => 0,
            "aes-256-gcm" => 1,
            "chacha20-poly1305" => 2,
            _ => anyhow::bail!("invalid pack algorithm"),
        });
        aad.extend_from_slice(&u16::try_from(self.key_id.len())?.to_be_bytes());
        aad.extend_from_slice(self.key_id.as_bytes());
        Ok(aad)
    }
}

pub struct Member {
    pub id: i64,
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
            .find(|m| m.id == c.id)
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

pub fn encode(
    mut p: Pack,
    members: &[(Chunk, Bytes)],
    secrets: &Secrets,
    pool: &Pool,
    strategy: Strategy,
    should_try: bool,
) -> Result<(Pack, Vec<u8>)> {
    let size = p.payload_size()?;
    ensure!(
        members.len() == p.member_count as usize,
        "pack member count mismatch"
    );
    let mut raw = Vec::with_capacity(size);
    raw.extend_from_slice(MAGIC);
    raw.extend_from_slice(&(members.len() as u32).to_be_bytes());
    let mut seen = std::collections::HashSet::new();
    for (c, data) in members {
        ensure!(
            seen.insert(c.id)
                && c.id > 0
                && !data.is_empty()
                && data.len() <= codec::MAX
                && data.len() == c.raw_size as usize
                && blake3::hash(data).as_bytes().as_slice() == c.hash
                && c.algorithm == members[0].0.algorithm
                && c.key_id == members[0].0.key_id,
            "invalid pack member"
        );
        raw.extend_from_slice(&c.id.to_be_bytes());
        raw.extend_from_slice(&c.hash);
        raw.extend_from_slice(&(data.len() as u32).to_be_bytes());
    }
    for (_, data) in members {
        raw.extend_from_slice(data);
    }
    ensure!(raw.len() == size, "pack raw length mismatch");
    p.digest = Some(blake3::hash(&raw).as_bytes().to_vec());
    let compressed = pool.compress_with(&raw, should_try, strategy)?;
    p.compressed = compressed.is_some();
    let mut encoded = compressed.unwrap_or(raw);
    p.stored_size = Some(encoded.len() as i64 + if p.algorithm == "none" { 0 } else { 16 });
    if p.algorithm != "none" {
        let key = secrets.keys.get(&p.key_id).context("missing pack key")?;
        ensure!(key.algorithm == p.algorithm, "pack key algorithm mismatch");
        let n = codec::typed_nonce(p.created_at, p.id, 1)?;
        p.nonce = Some(n.to_vec());
        key.seal(n, &p.aad()?, &mut encoded)?;
    } else {
        ensure!(
            p.key_id.is_empty() && p.nonce.is_none(),
            "invalid unencrypted pack"
        );
    }
    Ok((p, encoded))
}

pub fn decode(p: &Pack, mut data: Vec<u8>, secrets: &Secrets, pool: &Pool) -> Result<Decoded> {
    let size = p.payload_size().context(codec::IntegrityError::Metadata)?;
    ensure!(
        Some(data.len() as i64) == p.stored_size,
        codec::IntegrityError::Length
    );
    let aad = p.aad().context(codec::IntegrityError::Metadata)?;
    if p.algorithm != "none" {
        let key = secrets
            .keys
            .get(&p.key_id)
            .context(codec::IntegrityError::MissingKey)?;
        ensure!(
            key.algorithm == p.algorithm,
            codec::IntegrityError::Metadata
        );
        key.open(
            p.nonce
                .as_deref()
                .context(codec::IntegrityError::Metadata)?
                .try_into()
                .context(codec::IntegrityError::Metadata)?,
            &aad,
            &mut data,
        )
        .context(codec::IntegrityError::Authentication)?;
    } else {
        ensure!(
            p.key_id.is_empty() && p.nonce.is_none(),
            codec::IntegrityError::Metadata
        );
    }
    let raw = if p.compressed {
        pool.decompress_pack(&data, size)
            .context(codec::IntegrityError::Decompression)?
    } else {
        data
    };
    ensure!(
        raw.len() == size && Some(blake3::hash(&raw).as_bytes().as_slice()) == p.digest.as_deref(),
        codec::IntegrityError::Hash
    );
    ensure!(
        &raw[..8] == MAGIC && u32::from_be_bytes(raw[8..12].try_into()?) == p.member_count as u32,
        codec::IntegrityError::Metadata
    );
    let mut members = Vec::with_capacity(p.member_count as usize);
    let mut offset = 12 + p.member_count as usize * 44;
    let mut seen = std::collections::HashSet::new();
    for entry in raw[12..offset].chunks_exact(44) {
        let id = i64::from_be_bytes(entry[..8].try_into()?);
        let hash: [u8; 32] = entry[8..40].try_into()?;
        let len = u32::from_be_bytes(entry[40..44].try_into()?) as usize;
        ensure!(
            id > 0
                && seen.insert(id)
                && (1..=codec::MAX).contains(&len)
                && offset + len <= raw.len(),
            codec::IntegrityError::Metadata
        );
        ensure!(
            blake3::hash(&raw[offset..offset + len]).as_bytes() == &hash,
            codec::IntegrityError::Hash
        );
        members.push(Member {
            id,
            hash,
            start: offset,
            len,
        });
        offset += len;
    }
    ensure!(offset == raw.len(), codec::IntegrityError::Length);
    Ok(Decoded {
        bytes: Bytes::from(raw),
        members,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pack_authenticates_index_members_and_opaque_nonce() {
        for algorithm in ["none", "aes-256-gcm", "chacha20-poly1305"] {
            let key_id = if algorithm == "none" { "" } else { "test" };
            let secrets = Secrets {
                database_password: String::new(),
                backend_access: String::new(),
                backend_secret: String::new(),
                credential_key: codec::Key::new("aes-256-gcm", [4; 32]).unwrap(),
                active_key: key_id.into(),
                keys: if key_id.is_empty() {
                    Default::default()
                } else {
                    std::collections::BTreeMap::from([(
                        key_id.into(),
                        codec::Key::new(algorithm, [7; 32]).unwrap(),
                    )])
                },
            };
            let pool = Pool::new(Default::default(), 1).unwrap();
            let date = "2026-09-28T00:00:00Z".parse().unwrap();
            let members: Vec<_> = (1..=3)
                .map(|id| {
                    let raw = Bytes::from(vec![id as u8; codec::MIN + id as usize]);
                    (
                        Chunk {
                            id,
                            encoding_id: id,
                            storage_id: Uuid::new_v4(),
                            hash: blake3::hash(&raw).as_bytes().to_vec(),
                            raw_size: raw.len() as i32,
                            stored_size: None,
                            algorithm: algorithm.into(),
                            key_id: key_id.into(),
                            compressed: false,
                            nonce: None,
                            format: 1,
                            state: "ready".into(),
                            created_at: date,
                        },
                        raw,
                    )
                })
                .collect();
            let p = Pack {
                id: 7,
                storage_id: Uuid::new_v4(),
                algorithm: algorithm.into(),
                key_id: key_id.into(),
                raw_size: members.iter().map(|(c, _)| c.raw_size as i64).sum(),
                stored_size: None,
                compressed: false,
                nonce: None,
                digest: None,
                member_count: 3,
                state: "preparing".into(),
                created_at: date,
            };
            let (p, encoded) =
                encode(p, &members, &secrets, &pool, Strategy::Always, true).unwrap();
            let decoded = decode(&p, encoded.clone(), &secrets, &pool).unwrap();
            for (c, raw) in &members {
                assert_eq!(decoded.chunk(c).unwrap(), *raw);
            }
            let mut changed = p.clone();
            changed.raw_size += 1;
            assert!(decode(&changed, encoded.clone(), &secrets, &pool).is_err());
            let mut damaged = encoded.clone();
            damaged[0] ^= 1;
            assert!(decode(&p, damaged, &secrets, &pool).is_err());
            let mut changed = members[0].0.clone();
            changed.hash[0] ^= 1;
            assert!(decoded.chunk(&changed).is_err());
            let chunk_nonce = codec::typed_nonce(date, 7, 0).unwrap();
            let pack_nonce = codec::typed_nonce(date, 7, 1).unwrap();
            assert_ne!(chunk_nonce, pack_nonce);
            assert_eq!(pack_nonce[0] >> 3, 1);
            if algorithm != "none" {
                assert_eq!(p.nonce.unwrap(), pack_nonce);
            }
        }
    }
}
