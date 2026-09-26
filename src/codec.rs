use crate::config::Secrets;
use anyhow::{Context, Result, bail, ensure};
use aws_lc_rs::aead::{AES_256_GCM, Aad, CHACHA20_POLY1305, LessSafeKey, Nonce, UnboundKey};
use bytes::Bytes;
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
pub struct Key {
    pub algorithm: String,
    pub material: [u8; 32],
    cipher: LessSafeKey,
}
impl Key {
    pub fn new(algorithm: &str, material: [u8; 32]) -> Result<Self> {
        let a = match algorithm {
            "aes-256-gcm" => &AES_256_GCM,
            "chacha20-poly1305" => &CHACHA20_POLY1305,
            _ => bail!("unknown encryption algorithm"),
        };
        Ok(Self {
            algorithm: algorithm.into(),
            material,
            cipher: LessSafeKey::new(
                UnboundKey::new(a, &material)
                    .map_err(|_| anyhow::anyhow!("invalid encryption key"))?,
            ),
        })
    }
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
pub fn cache_compressed(c: &Chunk, min_savings_percent: u8) -> bool {
    let tag = if c.algorithm == "none" { 0 } else { 16 };
    c.compressed
        && c.stored_size.is_some_and(|size| {
            (i64::from(size) - tag) * 100
                <= i64::from(c.raw_size) * (100 - i64::from(min_savings_percent))
        })
}
pub fn encode(
    mut c: Chunk,
    input: Vec<u8>,
    secrets: &Secrets,
    min_savings_percent: u8,
    compression: &crate::compression::Pool,
    should_compress: bool,
) -> Result<(Chunk, Vec<u8>, Bytes)> {
    ensure!(
        input.len() == c.raw_size as usize && blake3::hash(&input).as_bytes() == c.hash.as_slice(),
        "chunk input mismatch"
    );
    let compressed = compression.compress(&input, should_compress)?;
    c.compressed = compressed.is_some();
    let tag_len = if c.algorithm == "none" { 0 } else { 16 };
    let mut encoded = compressed.unwrap_or_else(|| {
        let mut encoded = Vec::with_capacity(input.len() + tag_len);
        encoded.extend_from_slice(&input);
        encoded
    });
    c.stored_size = Some(i32::try_from(encoded.len() + tag_len)?);
    let cache = if cache_compressed(&c, min_savings_percent) {
        Bytes::copy_from_slice(&encoded)
    } else {
        Bytes::from(input)
    };
    if c.algorithm != "none" {
        let key = secrets.keys.get(&c.key_id).context("missing chunk key")?;
        ensure!(key.algorithm == c.algorithm, "chunk key algorithm mismatch");
        let n = nonce(c.created_at, c.id)?;
        c.nonce = Some(n.to_vec());
        #[cfg(feature = "fault-injection")]
        crate::faults::blocking("chunk-encrypting");
        key.cipher
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(n),
                Aad::from(aad(&c)?),
                &mut encoded,
            )
            .map_err(|_| anyhow::anyhow!("chunk encryption failed"))?;
    }
    Ok((c, encoded, cache))
}
pub fn decode(
    c: &Chunk,
    mut encoded: Vec<u8>,
    secrets: &Secrets,
    min_savings_percent: u8,
    compression: &crate::compression::Pool,
) -> Result<(Bytes, Bytes)> {
    ensure!(
        encoded.len() <= MAX + 16 && Some(encoded.len() as i32) == c.stored_size,
        "encoded chunk length mismatch"
    );
    let metadata = aad(c)?;
    let plain_len = if c.algorithm == "none" {
        ensure!(
            c.key_id.is_empty() && c.nonce.is_none(),
            "invalid unencrypted chunk metadata"
        );
        encoded.len()
    } else {
        let key = secrets
            .keys
            .get(&c.key_id)
            .context("missing historical chunk key")?;
        ensure!(key.algorithm == c.algorithm, "chunk key algorithm mismatch");
        let n: [u8; 12] = c
            .nonce
            .as_deref()
            .context("missing nonce")?
            .try_into()
            .context("invalid nonce")?;
        key.cipher
            .open_in_place(
                Nonce::assume_unique_for_key(n),
                Aad::from(metadata),
                &mut encoded,
            )
            .map_err(|_| anyhow::anyhow!("chunk authentication failed"))?
            .len()
    };
    encoded.truncate(plain_len);
    let plain = Bytes::from(encoded);
    let raw = decode_cache(c, plain.clone(), c.compressed, compression)?;
    let cache = if cache_compressed(c, min_savings_percent) {
        plain
    } else {
        raw.clone()
    };
    Ok((raw, cache))
}
pub fn decode_cache(
    c: &Chunk,
    data: Bytes,
    compressed: bool,
    compression: &crate::compression::Pool,
) -> Result<Bytes> {
    ensure!(
        c.format == 1 && c.hash.len() == 32 && (1..=MAX as i32).contains(&c.raw_size),
        "invalid chunk metadata"
    );
    let raw = if compressed {
        let tag = match c.algorithm.as_str() {
            "none" => 0,
            "aes-256-gcm" | "chacha20-poly1305" => 16,
            _ => bail!("unknown chunk algorithm"),
        };
        ensure!(
            c.compressed && data.len() <= MAX && Some(data.len() as i32 + tag) == c.stored_size,
            "compressed cache length mismatch"
        );
        Bytes::from(compression.decompress(&data, c.raw_size as usize)?)
    } else {
        data
    };
    ensure!(
        raw.len() == c.raw_size as usize && blake3::hash(&raw).as_bytes() == c.hash.as_slice(),
        "chunk integrity mismatch"
    );
    Ok(raw)
}

pub fn protect(secret: &[u8], key: &Key, binding: &[u8]) -> Result<Vec<u8>> {
    let mut nonce = [0; 12];
    aws_lc_rs::rand::fill(&mut nonce).map_err(|_| anyhow::anyhow!("randomness unavailable"))?;
    let mut data = Vec::with_capacity(secret.len() + 16);
    data.extend_from_slice(secret);
    key.cipher
        .seal_in_place_append_tag(
            Nonce::assume_unique_for_key(nonce),
            Aad::from(binding),
            &mut data,
        )
        .map_err(|_| anyhow::anyhow!("credential encryption failed"))?;
    let mut result = Vec::with_capacity(nonce.len() + data.len());
    result.extend_from_slice(&nonce);
    result.extend_from_slice(&data);
    Ok(result)
}
pub fn unprotect(data: &[u8], key: &Key, binding: &[u8]) -> Result<String> {
    ensure!(data.len() >= 28, "invalid protected credential");
    let nonce: [u8; 12] = data[..12].try_into()?;
    let mut data = data[12..].to_vec();
    let plain = key
        .cipher
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

    fn secrets_for(algorithm: &str) -> Secrets {
        Secrets {
            database_password: String::new(),
            backend_access: String::new(),
            backend_secret: String::new(),
            credential_key: Key::new("aes-256-gcm", [8; 32]).unwrap(),
            active_key: "test-key".into(),
            keys: if algorithm == "none" {
                BTreeMap::new()
            } else {
                BTreeMap::from([("test-key".into(), Key::new(algorithm, [7; 32]).unwrap())])
            },
        }
    }
    #[test]
    fn fixed_format_vectors() {
        let compression = crate::compression::Pool::new(Default::default(), 1).unwrap();
        let input = b"media-gateway-v1";
        for algorithm in ["none", "aes-256-gcm", "chacha20-poly1305"] {
            let key_id = if algorithm == "none" { "" } else { "test-key" };
            let secrets = secrets_for(algorithm);
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
            let (c, encoded, cache) =
                encode(c, input.to_vec(), &secrets, 20, &compression, true).unwrap();
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
            assert_eq!(
                decode(&c, encoded, &secrets, 20, &compression)
                    .unwrap()
                    .0
                    .as_ref(),
                input
            );
            assert_eq!(
                decode_cache(&c, cache, false, &compression)
                    .unwrap()
                    .as_ref(),
                input
            );

            // Older writers stored even small gains, below the current default savings gate.
            let legacy = crate::compression::Pool::new(
                crate::compression::Config {
                    min_savings_percent: 0,
                    min_savings_bytes: 0,
                    ..Default::default()
                },
                1,
            )
            .unwrap();
            let small = vec![42; 128];
            let mut historical = c;
            historical.hash = blake3::hash(&small).as_bytes().to_vec();
            historical.raw_size = small.len() as i32;
            let (historical, encoded, cache) =
                encode(historical, small.clone(), &secrets, 20, &legacy, true).unwrap();
            assert!(historical.compressed);
            assert_eq!(
                decode(&historical, encoded, &secrets, 20, &compression)
                    .unwrap()
                    .0
                    .as_ref(),
                small
            );
            assert_eq!(
                decode_cache(&historical, cache, true, &compression)
                    .unwrap()
                    .as_ref(),
                small
            );
        }
    }
    #[test]
    fn formats_authenticate_all_metadata_and_payload() {
        let compression = crate::compression::Pool::new(Default::default(), 1).unwrap();
        for algorithm in ["none", "aes-256-gcm", "chacha20-poly1305"] {
            let key_id = if algorithm == "none" { "" } else { "test-key" };
            let secrets = secrets_for(algorithm);
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
                let (c, stored, cache) =
                    encode(c, input.clone(), &secrets, 20, &compression, true).unwrap();
                let (raw, fetched_cache) =
                    decode(&c, stored.clone(), &secrets, 20, &compression).unwrap();
                assert_eq!(raw.as_ref(), input);
                assert_eq!(cache, fetched_cache);
                assert_eq!(
                    decode_cache(&c, cache.clone(), cache_compressed(&c, 20), &compression)
                        .unwrap()
                        .as_ref(),
                    input
                );
                let mut bad_cache = cache.to_vec();
                bad_cache[0] ^= 1;
                assert!(
                    decode_cache(
                        &c,
                        Bytes::from(bad_cache),
                        cache_compressed(&c, 20),
                        &compression
                    )
                    .is_err()
                );
                let mut oversized = c.clone();
                oversized.raw_size = MAX as i32 + 1;
                assert!(
                    decode_cache(
                        &oversized,
                        cache.clone(),
                        cache_compressed(&c, 20),
                        &compression
                    )
                    .is_err()
                );
                for threshold in [0, 100] {
                    let (c, encoded, cache) = encode(
                        c.clone(),
                        input.clone(),
                        &secrets,
                        threshold,
                        &compression,
                        true,
                    )
                    .unwrap();
                    assert_eq!(stored, encoded);
                    assert_eq!(
                        decode(&c, encoded, &secrets, threshold, &compression)
                            .unwrap()
                            .1,
                        cache
                    );
                    assert_eq!(
                        decode_cache(&c, cache, cache_compressed(&c, threshold), &compression)
                            .unwrap()
                            .as_ref(),
                        input
                    );
                }
                let mut changed = stored.clone();
                changed[0] ^= 1;
                assert!(decode(&c, changed, &secrets, 20, &compression).is_err());
                let mut changed = c.clone();
                changed.hash[0] ^= 1;
                assert!(decode(&changed, stored.clone(), &secrets, 20, &compression).is_err());
                let mut changed = stored.clone();
                changed.push(0);
                assert!(decode(&c, changed, &secrets, 20, &compression).is_err());
                if algorithm != "none" {
                    let mut changed = c.clone();
                    changed.storage_id = Uuid::new_v4();
                    assert!(decode(&changed, stored, &secrets, 20, &compression).is_err());
                }
                let mut boundary = c.clone();
                boundary.raw_size = 1000;
                boundary.compressed = true;
                let tag = if algorithm == "none" { 0 } else { 16 };
                for size in [799, 800, 801] {
                    boundary.stored_size = Some(size + tag);
                    assert_eq!(cache_compressed(&boundary, 20), size <= 800);
                }
            }
        }
        assert_eq!(
            hex::encode(nonce("2026-09-21T00:00:00Z".parse().unwrap(), 42).unwrap()),
            "01352839000000000000002a"
        );
    }
    #[test]
    fn shared_keys_keep_operations_independent() {
        let mut secrets = secrets_for("aes-256-gcm");
        secrets.keys.insert(
            "next-key".into(),
            Key::new("chacha20-poly1305", [9; 32]).unwrap(),
        );
        secrets.active_key = "next-key".into();
        let compression = crate::compression::Pool::new(Default::default(), 4).unwrap();
        std::thread::scope(|scope| {
            for worker in 0..4 {
                let (secrets, compression) = (&secrets, &compression);
                scope.spawn(move || {
                    for (i, (key_id, key)) in secrets.keys.iter().enumerate() {
                        let input = vec![worker as u8; MIN + i];
                        let c = Chunk {
                            id: 1 + worker * 2 + i as i64,
                            storage_id: Uuid::new_v4(),
                            hash: blake3::hash(&input).as_bytes().to_vec(),
                            raw_size: input.len() as i32,
                            stored_size: None,
                            algorithm: key.algorithm.clone(),
                            key_id: key_id.clone(),
                            compressed: false,
                            nonce: None,
                            format: 1,
                            state: "preparing".into(),
                            created_at: "2026-09-26T00:00:00Z".parse().unwrap(),
                        };
                        let (c, encoded, _) =
                            encode(c, input.clone(), secrets, 20, compression, true).unwrap();
                        assert_eq!(
                            decode(&c, encoded.clone(), secrets, 20, compression)
                                .unwrap()
                                .0
                                .as_ref(),
                            input
                        );
                        let mut wrong = c;
                        wrong.key_id = "missing-key".into();
                        assert!(decode(&wrong, encoded, secrets, 20, compression).is_err());
                        let binding = format!("access-{worker}-{i}");
                        let mut protected = protect(
                            b"credential-secret",
                            &secrets.credential_key,
                            binding.as_bytes(),
                        )
                        .unwrap();
                        assert_eq!(
                            unprotect(&protected, &secrets.credential_key, binding.as_bytes())
                                .unwrap(),
                            "credential-secret"
                        );
                        assert!(
                            unprotect(&protected, &secrets.credential_key, b"other-access")
                                .is_err()
                        );
                        protected[12] ^= 1;
                        assert!(
                            unprotect(&protected, &secrets.credential_key, binding.as_bytes())
                                .is_err()
                        );
                    }
                });
            }
        });
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
