use crate::config::Secrets;
use anyhow::{Context, Result, bail, ensure};
use aws_lc_rs::aead::{AES_256_GCM, Aad, CHACHA20_POLY1305, LessSafeKey, Nonce, UnboundKey};
use chrono::{DateTime, Datelike, Utc};
use serde::Serialize;
use uuid::Uuid;

pub const MIN: usize = 256 * 1024;
pub const AVG: usize = 1024 * 1024;
pub const MAX: usize = 4 * 1024 * 1024;

#[derive(Clone, sqlx::FromRow, Serialize)]
pub struct Chunk {
    pub id: i64,
    pub storage_id: Uuid,
    pub hash: Vec<u8>,
    pub raw_size: i32,
    pub stored_size: Option<i32>,
    pub algorithm: String,
    pub key_id: String,
    pub compressed: bool,
    pub nonce: Option<Vec<u8>>,
    pub format: i32,
    pub state: String,
    pub created_at: DateTime<Utc>,
}

pub fn cut(input: &[u8]) -> usize {
    fastcdc::v2020::FastCDC::new(input, MIN, AVG, MAX)
        .next()
        .map(|c| c.length)
        .unwrap_or(0)
}
pub fn nonce(date: DateTime<Utc>, id: i64) -> Result<[u8; 12]> {
    ensure!(id > 0, "chunk ID must be positive");
    let day = u32::try_from(date.year())?
        .checked_mul(10_000)
        .and_then(|n| n.checked_add(date.month() * 100 + date.day()))
        .context("invalid nonce date")?;
    let mut result = [0; 12];
    result[..4].copy_from_slice(&day.to_be_bytes());
    result[4..].copy_from_slice(&(id as u64).to_be_bytes());
    Ok(result)
}
fn cipher(algorithm: &str, material: &[u8; 32]) -> Result<LessSafeKey> {
    let a = match algorithm {
        "aes-256-gcm" => &AES_256_GCM,
        "chacha20-poly1305" => &CHACHA20_POLY1305,
        _ => bail!("unknown encryption algorithm"),
    };
    Ok(LessSafeKey::new(
        UnboundKey::new(a, material).map_err(|_| anyhow::anyhow!("invalid encryption key"))?,
    ))
}
fn aad(c: &Chunk) -> Result<Vec<u8>> {
    ensure!(
        c.format == 1 && c.hash.len() == 32 && (1..=MAX as i32).contains(&c.raw_size),
        "invalid chunk metadata"
    );
    let mut aad = Vec::with_capacity(128 + c.key_id.len());
    aad.extend_from_slice(b"MGWCHUNK\x01");
    aad.extend_from_slice(&c.id.to_be_bytes());
    aad.extend_from_slice(c.storage_id.as_bytes());
    aad.extend_from_slice(&c.hash);
    aad.extend_from_slice(&c.raw_size.to_be_bytes());
    aad.extend_from_slice(&c.stored_size.context("missing encoded size")?.to_be_bytes());
    aad.push(u8::from(c.compressed));
    aad.push(match c.algorithm.as_str() {
        "none" => 0,
        "aes-256-gcm" => 1,
        "chacha20-poly1305" => 2,
        _ => bail!("unknown chunk algorithm"),
    });
    aad.extend_from_slice(&u16::try_from(c.key_id.len())?.to_be_bytes());
    aad.extend_from_slice(c.key_id.as_bytes());
    Ok(aad)
}
pub fn encode(mut c: Chunk, input: &[u8], secrets: &Secrets) -> Result<(Chunk, Vec<u8>)> {
    ensure!(
        input.len() == c.raw_size as usize && blake3::hash(input).as_bytes() == c.hash.as_slice(),
        "chunk input mismatch"
    );
    let compressed = zstd::bulk::compress(input, 3)?;
    c.compressed = compressed.len() < input.len();
    let mut encoded = if c.compressed {
        compressed
    } else {
        input.to_vec()
    };
    let tag_len = if c.algorithm == "none" { 0 } else { 16 };
    c.stored_size = Some(i32::try_from(encoded.len() + tag_len)?);
    if c.algorithm != "none" {
        let (alg, material) = secrets.keys.get(&c.key_id).context("missing chunk key")?;
        ensure!(*alg == c.algorithm, "chunk key algorithm mismatch");
        let n = nonce(c.created_at, c.id)?;
        c.nonce = Some(n.to_vec());
        #[cfg(feature = "fault-injection")]
        crate::faults::blocking("chunk-encrypting");
        cipher(alg, material)?
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(n),
                Aad::from(aad(&c)?),
                &mut encoded,
            )
            .map_err(|_| anyhow::anyhow!("chunk encryption failed"))?;
    }
    Ok((c, encoded))
}
pub fn decode(c: &Chunk, mut encoded: Vec<u8>, secrets: &Secrets) -> Result<Vec<u8>> {
    ensure!(
        encoded.len() <= MAX + 16 && Some(encoded.len() as i32) == c.stored_size,
        "encoded chunk length mismatch"
    );
    let metadata = aad(c)?;
    let plain = if c.algorithm == "none" {
        ensure!(
            c.key_id.is_empty() && c.nonce.is_none(),
            "invalid unencrypted chunk metadata"
        );
        encoded.as_slice()
    } else {
        let (alg, material) = secrets
            .keys
            .get(&c.key_id)
            .context("missing historical chunk key")?;
        ensure!(*alg == c.algorithm, "chunk key algorithm mismatch");
        let n: [u8; 12] = c
            .nonce
            .as_deref()
            .context("missing nonce")?
            .try_into()
            .context("invalid nonce")?;
        cipher(alg, material)?
            .open_in_place(
                Nonce::assume_unique_for_key(n),
                Aad::from(metadata),
                &mut encoded,
            )
            .map_err(|_| anyhow::anyhow!("chunk authentication failed"))?
    };
    let raw = if c.compressed {
        zstd::bulk::decompress(plain, c.raw_size as usize)?
    } else {
        plain.to_vec()
    };
    ensure!(
        raw.len() == c.raw_size as usize && blake3::hash(&raw).as_bytes() == c.hash.as_slice(),
        "chunk integrity mismatch"
    );
    Ok(raw)
}

pub fn protect(secret: &[u8], key: &[u8; 32], binding: &[u8]) -> Result<Vec<u8>> {
    let mut nonce = [0; 12];
    aws_lc_rs::rand::fill(&mut nonce).map_err(|_| anyhow::anyhow!("randomness unavailable"))?;
    let mut data = secret.to_vec();
    cipher("aes-256-gcm", key)?
        .seal_in_place_append_tag(
            Nonce::assume_unique_for_key(nonce),
            Aad::from(binding),
            &mut data,
        )
        .map_err(|_| anyhow::anyhow!("credential encryption failed"))?;
    let mut result = nonce.to_vec();
    result.extend_from_slice(&data);
    Ok(result)
}
pub fn unprotect(data: &[u8], key: &[u8; 32], binding: &[u8]) -> Result<String> {
    ensure!(data.len() >= 28, "invalid protected credential");
    let nonce: [u8; 12] = data[..12].try_into()?;
    let mut data = data[12..].to_vec();
    let plain = cipher("aes-256-gcm", key)?
        .open_in_place(
            Nonce::assume_unique_for_key(nonce),
            Aad::from(binding),
            &mut data,
        )
        .map_err(|_| anyhow::anyhow!("credential authentication failed"))?;
    Ok(std::str::from_utf8(plain)?.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    #[test]
    fn fixed_format_vectors() {
        let input = b"media-gateway-v1";
        for algorithm in ["none", "aes-256-gcm", "chacha20-poly1305"] {
            let key_id = if algorithm == "none" { "" } else { "test-key" };
            let secrets = Secrets {
                database_password: String::new(),
                backend_access: String::new(),
                backend_secret: String::new(),
                credential_key: [8; 32],
                active_key: key_id.into(),
                keys: BTreeMap::from([(key_id.into(), (algorithm.into(), [7; 32]))]),
            };
            let c = Chunk {
                id: 42,
                storage_id: Uuid::from_u128(42),
                hash: blake3::hash(input).as_bytes().to_vec(),
                raw_size: input.len() as i32,
                stored_size: None,
                algorithm: algorithm.into(),
                key_id: key_id.into(),
                compressed: false,
                nonce: None,
                format: 1,
                state: "preparing".into(),
                created_at: "2026-09-21T00:00:00Z".parse().unwrap(),
            };
            let (c, encoded) = encode(c, input, &secrets).unwrap();
            assert_eq!(
                hex::encode(&c.hash),
                "1ed8177cb9b303cde3647b13887acb45f852af54364751de2067abb8fcd7c257"
            );
            let expected = match algorithm {
                "none" => "6d656469612d676174657761792d7631",
                "aes-256-gcm" => "9e55ce18aa0784ec76541d24e91cf9330cfc645d27fce034cef1ab9891fc04a1",
                _ => "c698b8ca807c7636e6bcef57aea0c94181c4264391d7b7e202797b24c5a2e090",
            };
            assert_eq!(hex::encode(&encoded), expected);
            assert_eq!(decode(&c, encoded, &secrets).unwrap(), input);
        }
    }
    #[test]
    fn formats_authenticate_all_metadata_and_payload() {
        for algorithm in ["none", "aes-256-gcm", "chacha20-poly1305"] {
            let key_id = if algorithm == "none" { "" } else { "test-key" };
            let secrets = Secrets {
                database_password: String::new(),
                backend_access: String::new(),
                backend_secret: String::new(),
                credential_key: [8; 32],
                active_key: key_id.into(),
                keys: BTreeMap::from([(key_id.into(), (algorithm.into(), [7; 32]))]),
            };
            for input in [vec![3; MIN - 1], {
                let mut v = vec![0; MAX];
                aws_lc_rs::rand::fill(&mut v).unwrap();
                v
            }] {
                let c = Chunk {
                    id: 42,
                    storage_id: Uuid::from_u128(42),
                    hash: blake3::hash(&input).as_bytes().to_vec(),
                    raw_size: input.len() as i32,
                    stored_size: None,
                    algorithm: algorithm.into(),
                    key_id: key_id.into(),
                    compressed: false,
                    nonce: None,
                    format: 1,
                    state: "preparing".into(),
                    created_at: "2026-09-21T00:00:00Z".parse().unwrap(),
                };
                let (c, stored) = encode(c, &input, &secrets).unwrap();
                assert_eq!(decode(&c, stored.clone(), &secrets).unwrap(), input);
                let mut changed = stored.clone();
                changed[0] ^= 1;
                assert!(decode(&c, changed, &secrets).is_err());
                let mut changed = c.clone();
                changed.hash[0] ^= 1;
                assert!(decode(&changed, stored.clone(), &secrets).is_err());
                let mut changed = stored.clone();
                changed.push(0);
                assert!(decode(&c, changed, &secrets).is_err());
                if algorithm != "none" {
                    let mut changed = c.clone();
                    changed.storage_id = Uuid::new_v4();
                    assert!(decode(&changed, stored, &secrets).is_err());
                }
            }
        }
        assert_eq!(
            hex::encode(nonce("2026-09-21T00:00:00Z".parse().unwrap(), 42).unwrap()),
            "01352839000000000000002a"
        );
    }
    #[test]
    fn windows_keep_the_whole_file_boundaries() {
        let mut input = vec![0; MAX * 3 + MIN];
        aws_lc_rs::rand::fill(&mut input).unwrap();
        let expected: Vec<_> = fastcdc::v2020::FastCDC::new(&input, MIN, AVG, MAX)
            .map(|c| c.length)
            .collect();
        let mut actual = Vec::new();
        let mut offset = 0;
        while offset < input.len() {
            let len = cut(&input[offset..(offset + MAX).min(input.len())]);
            actual.push(len);
            offset += len;
        }
        assert_eq!(actual, expected);
        assert_eq!(cut(&[]), 0);
        assert_eq!(cut(&input[..MIN - 1]), MIN - 1);
    }
}
