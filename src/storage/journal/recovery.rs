//! Native recovery records keep physical mappings public and bind both sections together.
use super::{FRAME_LIMIT, Format, Frame, FrameType, Sequence};
use crate::content::codec::Key;
use anyhow::{Result, ensure};
use uuid::Uuid;

const MAGIC: &[u8; 8] = b"MOKYU\x00\x00\x0a";
const HEADER: usize = 256;
const LIMIT: usize = 32 * 1024 * 1024;

pub(crate) struct Envelope {
    pub payload: Uuid,
    pub revision: u64,
    pub batch: Uuid,
    pub kind: FrameType,
    pub sequence: Sequence,
}

pub(crate) struct Public<'a> {
    pub namespace: Uuid,
    pub journal: Uuid,
    pub epoch: Uuid,
    pub envelope: Envelope,
    pub path: &'a str,
    pub physical: Vec<u8>,
    pub encrypted: bool,
    pub digest: [u8; 32],
    data: &'a [u8],
    binding_end: usize,
    body_start: usize,
    body_len: usize,
    plain_len: usize,
}

pub(crate) fn path(value: &str) -> Result<()> {
    ensure!(
        value.len() <= 1024
            && value.starts_with("metadata/")
            && value
                .split('/')
                .all(|s| !s.is_empty() && s != "." && s != "..")
            && !value.contains('\\')
            && !value.ends_with(".part")
            && !value.chars().any(char::is_control),
        "invalid recovery logical path"
    );
    Ok(())
}

fn compressed(plain: &[u8]) -> Result<(Vec<u8>, bool)> {
    ensure!(plain.len() <= FRAME_LIMIT, "recovery section is too large");
    if plain.len() >= 256 {
        let result = zstd::bulk::compress(plain, 1)?;
        if result.len() < plain.len() {
            return Ok((result, true));
        }
    }
    Ok((plain.to_vec(), false))
}

fn decompress(bytes: &[u8], plain_len: usize, zstd: bool) -> Result<Vec<u8>> {
    let result = if zstd {
        let mut decoder = zstd::bulk::Decompressor::new()?;
        decoder.set_parameter(zstd::zstd_safe::DParameter::WindowLogMax(23))?;
        decoder.decompress(bytes, plain_len)?
    } else {
        bytes.to_vec()
    };
    ensure!(
        result.len() == plain_len,
        "recovery section length mismatch"
    );
    Ok(result)
}

fn binding(bytes: &[u8]) -> Vec<u8> {
    let mut result = b"mokyu/recovery/0a/frame".to_vec();
    result.extend_from_slice(bytes);
    result
}

fn aead(key: &Key, aad: &[u8]) -> Result<Key> {
    let root = blake3::derive_key("mokyu/recovery/0a/frame-aead", &key.material);
    Key::new("aes-256-gcm", *blake3::keyed_hash(&root, aad).as_bytes())
}

fn mac(key: &Key, aad: &[u8], body: &[u8]) -> blake3::Hash {
    let root = blake3::derive_key("mokyu/recovery/0a/frame-mac", &key.material);
    blake3::Hasher::new_keyed(&root)
        .update(aad)
        .update(body)
        .finalize()
}

pub(crate) fn encode(
    format: &Format<'_>,
    logical: &str,
    envelope: &Envelope,
    physical: &[u8],
    business: &[u8],
) -> Result<Vec<u8>> {
    let mut salt = [0; 32];
    aws_lc_rs::rand::fill(&mut salt)
        .map_err(|_| anyhow::anyhow!("recovery randomness unavailable"))?;
    encode_salted(format, logical, envelope, physical, business, salt)
}

fn encode_salted(
    format: &Format<'_>,
    logical: &str,
    envelope: &Envelope,
    physical: &[u8],
    business: &[u8],
    salt: [u8; 32],
) -> Result<Vec<u8>> {
    path(logical)?;
    ensure!(
        [
            format.storage,
            format.journal,
            format.epoch,
            envelope.payload,
            envelope.batch
        ]
        .iter()
        .all(|id| !id.is_nil())
            && envelope.revision > 0
            && envelope.sequence.first <= envelope.sequence.last,
        "invalid recovery identity or sequence"
    );
    let (physical_body, physical_zstd) = compressed(physical)?;
    let (mut body, business_zstd) = compressed(business)?;
    let mut bytes = vec![0; HEADER];
    bytes[..8].copy_from_slice(MAGIC);
    bytes[8..10].copy_from_slice(&1u16.to_be_bytes());
    bytes[10..12].copy_from_slice(&(HEADER as u16).to_be_bytes());
    bytes[12] = envelope.kind as u8;
    bytes[13] = u8::from(physical_zstd)
        | (u8::from(business_zstd) << 1)
        | (u8::from(format.encrypted) << 2);
    for (index, id) in [
        format.storage,
        envelope.payload,
        format.journal,
        format.epoch,
        envelope.batch,
    ]
    .iter()
    .enumerate()
    {
        bytes[16 + index * 16..32 + index * 16].copy_from_slice(id.as_bytes());
    }
    for (index, number) in [
        envelope.revision,
        envelope.sequence.first,
        envelope.sequence.last,
    ]
    .iter()
    .enumerate()
    {
        bytes[96 + index * 8..104 + index * 8].copy_from_slice(&number.to_be_bytes());
    }
    bytes[120..152].copy_from_slice(&envelope.sequence.previous);
    for (index, length) in [
        physical.len(),
        physical_body.len(),
        business.len(),
        body.len(),
    ]
    .iter()
    .enumerate()
    {
        bytes[152 + index * 4..156 + index * 4].copy_from_slice(&(*length as u32).to_be_bytes());
    }
    bytes[168..200].copy_from_slice(&salt);
    bytes[200..232].copy_from_slice(blake3::hash(&physical_body).as_bytes());
    bytes[232..234].copy_from_slice(&(logical.len() as u16).to_be_bytes());
    bytes.extend_from_slice(logical.as_bytes());
    let aad = binding(&bytes);
    if format.encrypted {
        aead(format.key, &aad)?.seal([0; 12], &aad, &mut body)?;
    } else {
        let tag = mac(format.key, &aad, &body);
        body.extend_from_slice(tag.as_bytes());
    }
    bytes.extend_from_slice(&physical_body);
    bytes.extend_from_slice(&body);
    let digest = blake3::hash(&bytes);
    bytes.extend_from_slice(digest.as_bytes());
    ensure!(bytes.len() <= LIMIT, "recovery payload is too large");
    Ok(bytes)
}

// The caller must separately check the digest against the published recovery proof.
pub(crate) fn inspect(bytes: &[u8]) -> Result<Public<'_>> {
    ensure!(
        (HEADER + 48..=LIMIT).contains(&bytes.len()),
        "invalid recovery payload size"
    );
    let header = &bytes[..HEADER];
    ensure!(
        &header[..8] == MAGIC
            && header[8..10] == 1u16.to_be_bytes()
            && header[10..12] == (HEADER as u16).to_be_bytes()
            && header[13] & !7 == 0
            && header[14..16] == [0; 2]
            && header[234..] == [0; 22],
        "unsupported recovery format"
    );
    let kind = match header[12] {
        1 => FrameType::Changes,
        2 => FrameType::Seal,
        3 => FrameType::Snapshot,
        4 => FrameType::Manifest,
        5 => FrameType::Index,
        6 => FrameType::Watermark,
        7 => FrameType::Draft,
        8 => FrameType::PartCommit,
        9 => FrameType::History,
        _ => anyhow::bail!("unknown recovery record type"),
    };
    let id = |at| Uuid::from_slice(&header[at..at + 16]);
    let long = |at| u64::from_be_bytes(header[at..at + 8].try_into().unwrap());
    let size = |at| u32::from_be_bytes(header[at..at + 4].try_into().unwrap()) as usize;
    let physical_plain = size(152);
    let physical_len = size(156);
    let plain_len = size(160);
    let body_len = size(164);
    ensure!(
        [physical_plain, physical_len, plain_len, body_len]
            .iter()
            .all(|&n| n <= FRAME_LIMIT),
        "recovery section exceeds its bound"
    );
    let path_len = u16::from_be_bytes(header[232..234].try_into()?) as usize;
    ensure!(path_len <= 1024, "recovery path exceeds its bound");
    let binding_end = HEADER + path_len;
    let body_start = binding_end + physical_len;
    let encrypted = header[13] & 4 != 0;
    ensure!(
        body_start + body_len + if encrypted { 16 } else { 32 } + 32 == bytes.len(),
        "recovery payload length mismatch"
    );
    let logical = std::str::from_utf8(&bytes[HEADER..binding_end])?;
    path(logical)?;
    let digest = *blake3::hash(&bytes[..bytes.len() - 32]).as_bytes();
    ensure!(
        digest == bytes[bytes.len() - 32..],
        "recovery completion checksum mismatch"
    );
    let physical = &bytes[binding_end..body_start];
    ensure!(
        blake3::hash(physical).as_bytes() == &header[200..232],
        "recovery physical section checksum mismatch"
    );
    let namespace = id(16)?;
    let journal = id(48)?;
    let epoch = id(64)?;
    let envelope = Envelope {
        payload: id(32)?,
        revision: long(96),
        batch: id(80)?,
        kind,
        sequence: Sequence {
            first: long(104),
            last: long(112),
            previous: header[120..152].try_into()?,
        },
    };
    ensure!(
        [namespace, journal, epoch, envelope.payload, envelope.batch]
            .iter()
            .all(|id| !id.is_nil())
            && envelope.revision > 0
            && envelope.sequence.first <= envelope.sequence.last,
        "invalid recovery identity or sequence"
    );
    Ok(Public {
        namespace,
        journal,
        epoch,
        envelope,
        path: logical,
        physical: decompress(physical, physical_plain, header[13] & 1 != 0)?,
        encrypted,
        digest,
        data: bytes,
        binding_end,
        body_start,
        body_len,
        plain_len,
    })
}

impl Public<'_> {
    pub fn open(&self, format: &Format<'_>) -> Result<Frame> {
        ensure!(
            self.namespace == format.storage
                && self.journal == format.journal
                && self.epoch == format.epoch,
            "recovery namespace or journal mismatch"
        );
        let aad = binding(&self.data[..self.binding_end]);
        let mut body = self.data[self.body_start..self.data.len() - 32].to_vec();
        if self.encrypted {
            aead(format.key, &aad)?.open([0; 12], &aad, &mut body)?;
        } else {
            let (payload, tag) = body.split_at(self.body_len);
            aws_lc_rs::constant_time::verify_slices_are_equal(
                mac(format.key, &aad, payload).as_bytes(),
                tag,
            )
            .map_err(|_| anyhow::anyhow!("invalid recovery authentication"))?;
            body.truncate(self.body_len);
        }
        Ok(Frame {
            sequence: self.envelope.sequence,
            bytes: decompress(&body, self.plain_len, self.data[13] & 2 != 0)?,
            encrypted: self.encrypted,
            consumed: self.data.len(),
            digest: self.digest,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_mapping_and_protected_business_share_one_authenticated_frame() -> Result<()> {
        let key = Key::new("aes-256-gcm", [7; 32])?;
        let format = Format {
            storage: Uuid::from_u128(1),
            encrypted: true,
            journal: Uuid::from_u128(2),
            epoch: Uuid::from_u128(3),
            key: &key,
        };
        let envelope = Envelope {
            payload: Uuid::from_u128(4),
            revision: 2,
            batch: Uuid::from_u128(5),
            kind: FrameType::Changes,
            sequence: Sequence {
                first: 3,
                last: 8,
                previous: [9; 32],
            },
        };
        let path = "metadata/changes/epoch/00003";
        for encrypted in [false, true] {
            let format = Format {
                encrypted,
                ..format
            };
            let bytes = encode_salted(
                &format,
                path,
                &envelope,
                b"physical mappings",
                b"private object keys",
                [11; 32],
            )?;
            assert_eq!(&bytes[..13], b"MOKYU\0\0\x0a\0\x01\x01\0\x01");
            assert!(!bytes.windows(8).any(|value| value == b"MOKYU\0\0\x06"));
            let public = inspect(&bytes)?;
            assert_eq!(public.physical, b"physical mappings");
            assert_eq!(public.path, path);
            assert_eq!(public.open(&format)?.bytes, b"private object keys");
            let wrong_key = Key::new("aes-256-gcm", [8; 32])?;
            assert!(
                public
                    .open(&Format {
                        key: &wrong_key,
                        ..format
                    })
                    .is_err()
            );
            assert!(
                public
                    .open(&Format {
                        storage: Uuid::from_u128(99),
                        ..format
                    })
                    .is_err()
            );
            let mut modified = bytes.clone();
            modified[HEADER + path.len()] ^= 1;
            let end = modified.len() - 32;
            let hash = blake3::hash(&modified[HEADER + path.len()..HEADER + path.len() + 17]);
            modified[200..232].copy_from_slice(hash.as_bytes());
            let hash = blake3::hash(&modified[..end]);
            modified[end..].copy_from_slice(hash.as_bytes());
            assert!(inspect(&modified)?.open(&format).is_err());
            for end in [0, 8, HEADER - 1, bytes.len() - 1] {
                assert!(inspect(&bytes[..end]).is_err());
            }
            if encrypted {
                assert!(
                    !bytes
                        .windows(19)
                        .any(|value| value == b"private object keys")
                );
            }
        }
        Ok(())
    }

    #[test]
    fn compressed_sections_keep_independent_bounded_lengths() -> Result<()> {
        let key = Key::new("aes-256-gcm", [7; 32])?;
        let format = Format {
            storage: Uuid::from_u128(1),
            encrypted: false,
            journal: Uuid::from_u128(2),
            epoch: Uuid::from_u128(3),
            key: &key,
        };
        let envelope = Envelope {
            payload: Uuid::from_u128(4),
            revision: 1,
            batch: Uuid::from_u128(5),
            kind: FrameType::Snapshot,
            sequence: Sequence {
                first: 0,
                last: 0,
                previous: [0; 32],
            },
        };
        let physical = vec![3; 10000];
        let business = vec![5; 20000];
        let bytes = encode(
            &format,
            "metadata/snapshots/one",
            &envelope,
            &physical,
            &business,
        )?;
        assert!(bytes.len() < 1024);
        let public = inspect(&bytes)?;
        assert_eq!(public.physical, physical);
        assert_eq!(public.open(&format)?.bytes, business);
        let mut huge = bytes.clone();
        huge[152..156].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(inspect(&huge).is_err());
        assert!(encode(&format, "metadata/../one", &envelope, &[], &[]).is_err());
        Ok(())
    }
}
