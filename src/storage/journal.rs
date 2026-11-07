//! Authenticated records shared by the metadata journal and its snapshots.
pub(crate) mod batch;
pub(crate) mod history;
pub(crate) mod import;
pub(crate) mod local;
pub(crate) mod remote;
pub(crate) mod replay;
pub(crate) mod snapshot;
pub(crate) mod source;

use crate::content::codec::Key;
use anyhow::{Context, Result, ensure};
use uuid::Uuid;

pub(crate) const ROOT: &str = "metadata";
pub(crate) const FRAME_LIMIT: usize = 8 * 1024 * 1024;
const HEADER: usize = 98;
const AEAD_TAG: usize = 16;
const MAC_TAG: usize = 32;
const MAGIC: &[u8; 8] = b"MOKYU\0\0\x06";

#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(u8)]
pub(crate) enum FrameType {
    Changes = 1,
    Seal = 2,
    Snapshot = 3,
    Manifest = 4,
    Index = 5,
    Watermark = 6,
    Draft = 7,
    PartCommit = 8,
    History = 9,
}

#[derive(Clone, Copy)]
pub(crate) struct Sequence {
    pub first: u64,
    pub last: u64,
    pub previous: [u8; 32],
}

pub(crate) struct Frame {
    pub sequence: Sequence,
    pub bytes: Vec<u8>,
    pub encrypted: bool,
    pub consumed: usize,
    pub digest: [u8; 32],
}

#[derive(Clone, Copy)]
pub(crate) struct Format<'a> {
    pub storage: Uuid,
    pub encrypted: bool,
    pub journal: Uuid,
    pub epoch: Uuid,
    pub key: &'a Key,
}

pub(crate) fn segment_path(epoch: Uuid, first: u64) -> String {
    format!("{ROOT}/changes/{}/{first:020}", epoch.simple())
}

pub(crate) fn draft_path(bucket: Uuid, seq: i64, page: i32) -> String {
    format!("{ROOT}/draft/{}/{seq:020}/{page:08}", bucket.simple())
}

impl Format<'_> {
    pub const fn overhead(encrypted: bool) -> usize {
        HEADER + if encrypted { AEAD_TAG } else { MAC_TAG }
    }

    fn binding(&self, path: &str, header: &[u8]) -> Result<Vec<u8>> {
        ensure!(
            path.len() <= 1024
                && path.starts_with("metadata/")
                && path
                    .split('/')
                    .all(|s| !s.is_empty() && s != "." && s != "..")
                && !path.contains('\\'),
            "invalid journal path"
        );
        let mut aad = b"mokyu/journal/06/frame".to_vec();
        aad.extend_from_slice(self.journal.as_bytes());
        aad.extend_from_slice(self.epoch.as_bytes());
        aad.extend_from_slice(&(path.len() as u16).to_be_bytes());
        aad.extend_from_slice(path.as_bytes());
        aad.extend_from_slice(header);
        Ok(aad)
    }

    fn aead(&self, aad: &[u8]) -> Result<Key> {
        let root = blake3::derive_key("mokyu/journal/06/frame-aead", &self.key.material);
        Key::new("aes-256-gcm", *blake3::keyed_hash(&root, aad).as_bytes())
    }

    fn mac(&self, aad: &[u8], body: &[u8]) -> blake3::Hash {
        let key = blake3::derive_key("mokyu/journal/06/frame-mac", &self.key.material);
        blake3::Hasher::new_keyed(&key)
            .update(aad)
            .update(body)
            .finalize()
    }

    pub fn encode(
        &self,
        ty: FrameType,
        path: &str,
        seq: Sequence,
        plain: &[u8],
    ) -> Result<Vec<u8>> {
        let mut salt = [0; 32];
        aws_lc_rs::rand::fill(&mut salt)
            .map_err(|_| anyhow::anyhow!("journal randomness unavailable"))?;
        self.encode_with_salt(ty, path, seq, plain, salt)
    }

    fn encode_with_salt(
        &self,
        ty: FrameType,
        path: &str,
        seq: Sequence,
        plain: &[u8],
        salt: [u8; 32],
    ) -> Result<Vec<u8>> {
        ensure!(
            seq.first <= seq.last && plain.len() <= FRAME_LIMIT,
            "invalid journal frame bounds"
        );
        let compressed = if plain.len() >= 256 {
            zstd::bulk::compress(plain, 1)?
        } else {
            Vec::new()
        };
        let use_zstd = !compressed.is_empty() && compressed.len() < plain.len();
        let mut payload = if use_zstd { compressed } else { plain.to_vec() };
        let mut header = Vec::with_capacity(Self::overhead(self.encrypted) + payload.len());
        header.extend_from_slice(MAGIC);
        header.push(ty as u8);
        header.push(u8::from(use_zstd) | (u8::from(self.encrypted) << 1));
        header.extend_from_slice(&seq.first.to_be_bytes());
        header.extend_from_slice(&seq.last.to_be_bytes());
        header.extend_from_slice(&(plain.len() as u32).to_be_bytes());
        header.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        header.extend_from_slice(&seq.previous);
        header.extend_from_slice(&salt);
        let aad = self.binding(path, &header)?;
        if self.encrypted {
            self.aead(&aad)?.seal([0; 12], &aad, &mut payload)?;
        } else {
            let tag = self.mac(&aad, &payload);
            payload.extend_from_slice(tag.as_bytes());
        }
        header.extend_from_slice(&payload);
        Ok(header)
    }

    pub fn decode(&self, ty: FrameType, path: &str, data: &[u8]) -> Result<Frame> {
        let header = data.get(..HEADER).context("truncated journal header")?;
        ensure!(
            &header[..8] == MAGIC && header[8] == ty as u8 && header[9] & !3 == 0,
            "unsupported journal frame"
        );
        let first = u64::from_be_bytes(header[10..18].try_into()?);
        let last = u64::from_be_bytes(header[18..26].try_into()?);
        let plain_len = u32::from_be_bytes(header[26..30].try_into()?) as usize;
        let body_len = u32::from_be_bytes(header[30..34].try_into()?) as usize;
        ensure!(
            first <= last && plain_len <= FRAME_LIMIT && body_len <= FRAME_LIMIT,
            "invalid journal frame bounds"
        );
        let encrypted = header[9] & 2 != 0;
        let consumed = Self::overhead(encrypted) + body_len;
        let mut body = data
            .get(HEADER..consumed)
            .context("truncated journal payload")?
            .to_vec();
        let aad = self.binding(path, header)?;
        if encrypted {
            self.aead(&aad)?.open([0; 12], &aad, &mut body)?;
        } else {
            let (payload, tag) = body.split_at(body_len);
            aws_lc_rs::constant_time::verify_slices_are_equal(
                self.mac(&aad, payload).as_bytes(),
                tag,
            )
            .map_err(|_| anyhow::anyhow!("invalid journal frame authentication"))?;
            body.truncate(body_len);
        }
        let bytes = if header[9] & 1 != 0 {
            let mut decoder = zstd::bulk::Decompressor::new()?;
            decoder.set_parameter(zstd::zstd_safe::DParameter::WindowLogMax(23))?;
            decoder.decompress(&body, plain_len)?
        } else {
            body
        };
        ensure!(bytes.len() == plain_len, "journal length mismatch");
        Ok(Frame {
            sequence: Sequence {
                first,
                last,
                previous: header[34..66].try_into()?,
            },
            bytes,
            encrypted,
            consumed,
            digest: *blake3::hash(&data[..consumed]).as_bytes(),
        })
    }
}

#[cfg(test)]
#[path = "../../tests/storage/journal.rs"]
mod publication_tests;

/// Stage visited frames and publish them only after the final seal has been verified.
pub(crate) fn verify_segment(
    format: &Format<'_>,
    path: &str,
    data: &[u8],
    first: u64,
    previous: [u8; 32],
    visit: impl FnMut(Frame) -> Result<()>,
) -> Result<(u64, [u8; 32])> {
    verify_prefix(format, path, data, first, previous, false, visit)
}

pub(crate) fn verify_prefix(
    format: &Format<'_>,
    path: &str,
    data: &[u8],
    first: u64,
    previous: [u8; 32],
    part_prefix: bool,
    visit: impl FnMut(Frame) -> Result<()>,
) -> Result<(u64, [u8; 32])> {
    let last = first
        .checked_sub(1)
        .context("invalid first journal sequence")?;
    verify_frames(
        format,
        path,
        data,
        Sequence {
            first,
            last,
            previous,
        },
        part_prefix,
        visit,
    )
}

fn verify_frames(
    format: &Format<'_>,
    path: &str,
    mut rest: &[u8],
    confirmed: Sequence,
    part_prefix: bool,
    mut visit: impl FnMut(Frame) -> Result<()>,
) -> Result<(u64, [u8; 32])> {
    let first = confirmed.first;
    let mut next = confirmed
        .last
        .checked_add(1)
        .context("journal sequence exhausted")?;
    let mut prior = confirmed.previous;
    let mut part_end = confirmed.last;
    loop {
        if rest.get(8) == Some(&(FrameType::PartCommit as u8)) {
            let part = format.decode(FrameType::PartCommit, path, rest)?;
            ensure!(
                next > first
                    && next - 1 > part_end
                    && part.sequence.first == first
                    && part.sequence.last == next - 1
                    && part.sequence.previous == prior
                    && part.bytes.is_empty(),
                "invalid journal part boundary"
            );
            part_end = next - 1;
            prior = part.digest;
            rest = &rest[part.consumed..];
            if part_prefix && rest.is_empty() {
                return Ok((next - 1, prior));
            }
            continue;
        }
        if rest.get(8) == Some(&(FrameType::Seal as u8)) {
            let seal = format.decode(FrameType::Seal, path, rest)?;
            ensure!(
                next > first
                    && seal.consumed == rest.len()
                    && seal.bytes.is_empty()
                    && seal.sequence.first == first
                    && seal.sequence.last == next - 1
                    && seal.sequence.previous == prior,
                "invalid journal seal"
            );
            return Ok((next - 1, seal.digest));
        }
        let frame = format.decode(FrameType::Changes, path, rest)?;
        ensure!(
            frame.sequence.first == next && frame.sequence.previous == prior,
            "discontinuous journal frame"
        );
        next = frame
            .sequence
            .last
            .checked_add(1)
            .context("journal sequence exhausted")?;
        prior = frame.digest;
        rest = &rest[frame.consumed..];
        visit(frame)?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_requires_a_contiguous_chain_and_exact_seal() {
        let key = Key::new("aes-256-gcm", [2; 32]).unwrap();
        let format = Format {
            storage: Uuid::from_u128(77),
            encrypted: true,
            journal: Uuid::from_u128(3),
            epoch: Uuid::from_u128(4),
            key: &key,
        };
        let path = segment_path(format.epoch, 8);
        let first = Sequence {
            first: 8,
            last: 9,
            previous: [1; 32],
        };
        let mut data = format
            .encode(FrameType::Changes, &path, first, b"first")
            .unwrap();
        let next = Sequence {
            first: 10,
            last: 10,
            previous: *blake3::hash(&data).as_bytes(),
        };
        let second = format
            .encode(FrameType::Changes, &path, next, b"second")
            .unwrap();
        let seal = Sequence {
            first: 8,
            last: 10,
            previous: *blake3::hash(&second).as_bytes(),
        };
        data.extend_from_slice(&second);
        assert!(verify_segment(&format, &path, &data, 8, [1; 32], |_| Ok(())).is_err());
        let trailer = format.encode(FrameType::Seal, &path, seal, b"").unwrap();
        data.extend_from_slice(&trailer);
        let mut records = Vec::new();
        let (last, digest) = verify_segment(&format, &path, &data, 8, [1; 32], |f| {
            records.push(f.bytes);
            Ok(())
        })
        .unwrap();
        assert_eq!((last, digest), (10, *blake3::hash(&trailer).as_bytes()));
        assert_eq!(records, [b"first".to_vec(), b"second".to_vec()]);
        assert!(verify_segment(&format, &path, &data, 8, [2; 32], |_| Ok(())).is_err());
        data.push(0);
        assert!(verify_segment(&format, &path, &data, 8, [1; 32], |_| Ok(())).is_err());
    }

    #[test]
    fn frame_binds_purpose_sequence_identity_and_path() {
        let key = Key::new("aes-256-gcm", [7; 32]).unwrap();
        let format = Format {
            storage: Uuid::from_u128(77),
            encrypted: true,
            journal: Uuid::from_u128(1),
            epoch: Uuid::from_u128(2),
            key: &key,
        };
        let path = segment_path(format.epoch, 12);
        let seq = Sequence {
            first: 12,
            last: 13,
            previous: [9; 32],
        };
        let frame = format
            .encode_with_salt(FrameType::Changes, &path, seq, b"record", [4; 32])
            .unwrap();
        let decoded = format.decode(FrameType::Changes, &path, &frame).unwrap();
        assert_eq!(
            hex::encode(decoded.digest),
            "65d05784fd3a3846804baaaaec571cc0fce9072434a8e4ce83129bf1de47f932"
        );
        let plain = Format {
            encrypted: false,
            ..format
        };
        let authenticated = plain
            .encode_with_salt(FrameType::Changes, &path, seq, b"record", [4; 32])
            .unwrap();
        assert_eq!(&authenticated[..10], b"MOKYU\0\0\x06\x01\x00");
        assert_eq!(&authenticated[HEADER..HEADER + 6], b"record");
        assert_eq!(
            blake3::hash(&authenticated).to_hex().as_str(),
            "9c65f25a47a7152542e8bb18fc6db3781a7dae3cae9ef315306e1ccb5525a2ab"
        );
        let recovered = format
            .decode(FrameType::Changes, &path, &authenticated)
            .unwrap();
        assert!(!recovered.encrypted && recovered.bytes == b"record");
        assert!(
            !plain
                .decode(FrameType::Changes, &path, &frame)
                .unwrap()
                .bytes
                .is_empty()
        );
        assert!(
            plain
                .decode(
                    FrameType::Changes,
                    &(path.clone() + ".part"),
                    &authenticated
                )
                .is_err()
        );
        for pos in [0, 9, 35, HEADER, authenticated.len() - 1] {
            let mut changed = authenticated.clone();
            changed[pos] ^= 4;
            assert!(plain.decode(FrameType::Changes, &path, &changed).is_err());
        }
        assert_eq!(decoded.bytes, b"record");
        assert_eq!(decoded.sequence.previous, seq.previous);
        assert_eq!(decoded.sequence.first, 12);
        assert_eq!(decoded.sequence.last, 13);
        assert_eq!(decoded.consumed, frame.len());
        for pos in [0, 8, 9, 17, 25, 26, 30, 34, 66, HEADER, frame.len() - 1] {
            let mut damaged = frame.clone();
            damaged[pos] ^= 1;
            assert!(format.decode(FrameType::Changes, &path, &damaged).is_err());
        }
        for end in [0, HEADER - 1, HEADER, frame.len() - 1] {
            assert!(
                format
                    .decode(FrameType::Changes, &path, &frame[..end])
                    .is_err()
            );
        }
        assert!(format.decode(FrameType::Snapshot, &path, &frame).is_err());
        assert!(
            format
                .decode(FrameType::Changes, &segment_path(format.epoch, 13), &frame)
                .is_err()
        );
        let other = Format {
            journal: Uuid::from_u128(3),
            ..format
        };
        assert!(other.decode(FrameType::Changes, &path, &frame).is_err());
        let wrong = Key::new("aes-256-gcm", [6; 32]).unwrap();
        assert!(
            Format {
                key: &wrong,
                ..format
            }
            .decode(FrameType::Changes, &path, &frame)
            .is_err()
        );
        let plain = vec![3; 65536];
        let compressed = format
            .encode(FrameType::Changes, &path, seq, &plain)
            .unwrap();
        assert!(compressed.len() < plain.len());
        assert_eq!(
            format
                .decode(FrameType::Changes, &path, &compressed)
                .unwrap()
                .bytes,
            plain
        );
    }
}
