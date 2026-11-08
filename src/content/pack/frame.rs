use super::{MAX_MEMBERS, MAX_RAW, Pack};
use crate::content::codec::{self, Chunk};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, ops::Range};
use uuid::Uuid;

pub(crate) const HEADER: &[u8; 8] = b"MOKYU\0\0\x05";

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub(crate) struct Encoding {
    pub location_id: Uuid,
    pub stored_size: i32,
    pub compressed: bool,
    pub algorithm: String,
    pub block_key_id: Option<Uuid>,
    pub nonce: Option<Vec<u8>>,
}

impl Encoding {
    pub fn capture(chunk: &Chunk) -> Result<Self> {
        chunk.file_size()?;
        let encoding = Self {
            location_id: chunk.location_id,
            stored_size: chunk.stored_size.context("missing member size")?,
            compressed: chunk.compressed,
            algorithm: chunk.algorithm.clone(),
            block_key_id: chunk.block_key_id,
            nonce: chunk.nonce.clone(),
        };
        encoding.validate(chunk.raw_size)?;
        Ok(encoding)
    }

    pub fn validate(&self, raw_size: i32) -> Result<()> {
        let encrypted = match self.algorithm.as_str() {
            "none" => false,
            "aes-256-gcm" | "chacha20-poly1305" => true,
            _ => anyhow::bail!("invalid member algorithm"),
        };
        let tag = if encrypted { 16 } else { 0 };
        ensure!(
            !self.location_id.is_nil()
                && (1..=codec::MAX as i32).contains(&raw_size)
                && (tag + 1..=raw_size + tag).contains(&self.stored_size)
                && (self.compressed || self.stored_size == raw_size + tag)
                && self.block_key_id.is_some() == encrypted
                && self.block_key_id.is_none_or(|id| !id.is_nil())
                && if encrypted {
                    self.nonce.as_deref().is_some_and(|n| {
                        n.len() == 12
                            && n[0] >> 3 == crate::content::nonce::CHUNK
                            && crate::content::nonce::id(Some(n)).is_some()
                    })
                } else {
                    self.nonce.is_none()
                },
            codec::IntegrityError::Metadata
        );
        Ok(())
    }

    pub fn apply(&self, member: &mut Chunk) -> Result<()> {
        self.validate(member.raw_size)?;
        member.location_id = self.location_id;
        member.address = None;
        member.stored_size = Some(self.stored_size);
        member.compressed = self.compressed;
        member.algorithm.clone_from(&self.algorithm);
        member.block_key_id = self.block_key_id;
        member.nonce.clone_from(&self.nonce);
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub(crate) struct Position {
    pub ordinal: u32,
    pub offset: u64,
    pub encoding: Encoding,
}

impl Position {
    pub fn range(&self, raw_size: i32, file_size: u64, member_count: i32) -> Result<Range<u64>> {
        self.encoding.validate(raw_size)?;
        let end = self
            .offset
            .checked_add(u64::try_from(self.encoding.stored_size)?)
            .context(codec::IntegrityError::Length)?;
        ensure!(
            (2..=MAX_MEMBERS as i32).contains(&member_count)
                && self.ordinal < member_count as u32
                && self.offset >= HEADER.len() as u64 + u64::from(self.ordinal)
                && end <= file_size.saturating_sub((member_count as u32 - self.ordinal - 1) as u64)
                && (self.ordinal != 0 || self.offset == HEADER.len() as u64)
                && (self.ordinal + 1 != member_count as u32 || end == file_size),
            codec::IntegrityError::Metadata
        );
        Ok(self.offset..end)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub(crate) struct Entry {
    pub uuid: Uuid,
    #[serde(with = "crate::content::codec::digest")]
    pub hash: [u8; 32],
    pub raw_size: i32,
    pub position: Position,
}

impl Entry {
    pub fn new(chunk: &Chunk, position: Position) -> Result<Self> {
        ensure!(!chunk.uuid.is_nil(), codec::IntegrityError::Metadata);
        Ok(Self {
            uuid: chunk.uuid,
            hash: chunk
                .hash
                .as_slice()
                .try_into()
                .context(codec::IntegrityError::Metadata)?,
            raw_size: chunk.raw_size,
            position,
        })
    }

    pub fn chunk(&self, pack: &Pack) -> Result<Chunk> {
        let encoding = &self.position.encoding;
        encoding.validate(self.raw_size)?;
        Ok(Chunk {
            id: self.uuid.into(),
            encoding_id: encoding.location_id.into(),
            uuid: self.uuid,
            location_id: encoding.location_id,
            address: None,
            home_storage_id: pack.home_storage(),
            storage_id: pack.storage_id,
            raw_size: self.raw_size,
            hash: self.hash.to_vec(),
            stored_size: Some(encoding.stored_size),
            compressed: encoding.compressed,
            algorithm: encoding.algorithm.clone(),
            block_key_id: encoding.block_key_id,
            nonce: encoding.nonce.clone(),
            format: 2,
            state: "ready".into(),
            created_at: pack.created_at,
        })
    }
}

// Recovery catalogs can reference only some members of a shared pack.
pub fn validate_entries(
    entries: &[Entry],
    size: u64,
    raw_size: i64,
    count: i32,
    complete: bool,
) -> Result<()> {
    ensure!(
        (2..=MAX_MEMBERS as i32).contains(&count)
            && (i64::from(count)..=MAX_RAW as i64).contains(&raw_size)
            && size >= HEADER.len() as u64 + count as u64
            && size <= HEADER.len() as u64 + raw_size as u64 + 16 * count as u64
            && !entries.is_empty()
            && entries.len() <= count as usize
            && (!complete || entries.len() == count as usize),
        codec::IntegrityError::Metadata
    );
    let mut previous_end = HEADER.len() as u64;
    let mut next_ordinal = 0;
    let mut seen = HashSet::new();
    let mut total = 0i64;
    for entry in entries {
        let range = entry.position.range(entry.raw_size, size, count)?;
        let missing = entry
            .position
            .ordinal
            .checked_sub(next_ordinal)
            .context(codec::IntegrityError::Metadata)?;
        ensure!(
            !entry.uuid.is_nil()
                && seen.insert(entry.uuid)
                && if missing == 0 {
                    range.start == previous_end
                } else {
                    range.start >= previous_end + u64::from(missing)
                },
            codec::IntegrityError::Metadata
        );
        previous_end = range.end;
        next_ordinal = entry.position.ordinal + 1;
        total += i64::from(entry.raw_size);
    }
    ensure!(
        total + (i64::from(count) - entries.len() as i64) <= raw_size
            && (entries.len() != count as usize || total == raw_size),
        codec::IntegrityError::Length
    );
    Ok(())
}

pub(crate) struct Builder {
    bytes: u64,
    raw: u64,
    members: HashSet<Uuid>,
    hash: blake3::Hasher,
}

impl Builder {
    pub fn new() -> Self {
        let mut hash = blake3::Hasher::new();
        hash.update(HEADER);
        Self {
            bytes: HEADER.len() as u64,
            raw: 0,
            members: HashSet::new(),
            hash,
        }
    }

    pub fn append<'a>(&mut self, chunk: &Chunk, file: &'a [u8]) -> Result<(Position, &'a [u8])> {
        let body = chunk.file_body(file)?;
        let encoding = Encoding::capture(chunk)?;
        let raw = self
            .raw
            .checked_add(u64::try_from(chunk.raw_size)?)
            .context(codec::IntegrityError::Length)?;
        ensure!(
            raw <= MAX_RAW as u64
                && self.members.len() < MAX_MEMBERS
                && !chunk.uuid.is_nil()
                && chunk.hash.len() == 32
                && self.members.insert(chunk.uuid),
            codec::IntegrityError::Metadata
        );
        let pos = Position {
            ordinal: (self.members.len() - 1) as u32,
            offset: self.bytes,
            encoding,
        };
        self.bytes += body.len() as u64;
        self.raw = raw;
        self.hash.update(body);
        Ok((pos, body))
    }

    pub fn finish(self, raw_size: i64, count: i32) -> Result<(u64, blake3::Hash)> {
        ensure!(
            (2..=MAX_MEMBERS as i32).contains(&count)
                && self.members.len() == count as usize
                && self.raw == u64::try_from(raw_size)?,
            codec::IntegrityError::Metadata
        );
        Ok((self.bytes, self.hash.finalize()))
    }
}

pub(crate) fn index<'a>(
    chunks: impl IntoIterator<Item = &'a Chunk>,
) -> Result<Vec<(crate::model::Id, Entry)>> {
    let mut offset = HEADER.len() as u64;
    let mut entries = Vec::new();
    for chunk in chunks {
        ensure!(entries.len() < MAX_MEMBERS, codec::IntegrityError::Metadata);
        let position = Position {
            ordinal: entries.len() as u32,
            offset,
            encoding: Encoding::capture(chunk)?,
        };
        offset += position.encoding.stored_size as u64;
        entries.push((chunk.id, Entry::new(chunk, position)?));
    }
    Ok(entries)
}

pub(crate) fn assemble(
    pack: &mut Pack,
    files: &[(Chunk, bytes::Bytes)],
) -> Result<(Vec<u8>, Vec<(crate::model::Id, Entry)>)> {
    let capacity = files.iter().try_fold(HEADER.len(), |len, (chunk, _)| {
        let size = chunk.file_size()? - codec::FILE_HEADER.len();
        len.checked_add(size).context(codec::IntegrityError::Length)
    })?;
    ensure!(
        capacity <= HEADER.len() + MAX_RAW + MAX_MEMBERS * 16,
        codec::IntegrityError::Length
    );
    let mut bytes = Vec::with_capacity(capacity);
    bytes.extend_from_slice(HEADER);
    let mut builder = Builder::new();
    let mut entries = Vec::with_capacity(files.len());
    for (chunk, file) in files {
        let (position, body) = builder.append(chunk, file)?;
        entries.push((chunk.id, Entry::new(chunk, position)?));
        bytes.extend_from_slice(body);
    }
    let (size, digest) = builder.finish(pack.raw_size, pack.member_count)?;
    pack.stored_size = Some(i64::try_from(size)?);
    pack.digest = Some(digest.as_bytes().to_vec());
    Ok((bytes, entries))
}

pub(crate) fn rewrite_entries(
    members: &[Entry],
    algorithm: &str,
    key: Option<Uuid>,
    created: chrono::DateTime<chrono::Utc>,
    first: Option<i64>,
    allow_compression: bool,
) -> Result<Vec<Entry>> {
    ensure!(
        key.is_some() == first.is_some() && (algorithm != "none") == key.is_some(),
        codec::IntegrityError::Metadata
    );
    let mut offset = HEADER.len() as u64;
    let mut output = Vec::with_capacity(members.len());
    for (index, source) in members.iter().enumerate() {
        let old = &source.position.encoding;
        let payload = if allow_compression {
            old.stored_size - if old.block_key_id.is_some() { 16 } else { 0 }
        } else {
            source.raw_size
        };
        let encoding = Encoding {
            location_id: Uuid::now_v7(),
            stored_size: payload + if key.is_some() { 16 } else { 0 },
            compressed: allow_compression && old.compressed,
            algorithm: algorithm.to_owned(),
            block_key_id: key,
            nonce: first
                .map(|n| -> Result<_> {
                    Ok(codec::typed_nonce(
                        created,
                        n.checked_add(index as i64).context("nonce overflow")?,
                        crate::content::nonce::CHUNK,
                    )?
                    .to_vec())
                })
                .transpose()?,
        };
        encoding.validate(source.raw_size)?;
        let end = offset + encoding.stored_size as u64;
        output.push(Entry {
            position: Position {
                ordinal: index as u32,
                offset,
                encoding,
            },
            ..source.clone()
        });
        offset = end;
    }
    Ok(output)
}

pub(crate) fn recode(
    source: &Pack,
    target: &mut Pack,
    file: &[u8],
    old: &[Entry],
    new: &[Entry],
    keys: &dyn codec::KeySource,
    pool: &crate::content::compression::Pool,
) -> Result<Vec<u8>> {
    validate_file(source, file)?;
    validate_entries(
        old,
        file.len() as u64,
        source.raw_size,
        source.member_count,
        true,
    )?;
    ensure!(
        old.len() == new.len()
            && source.raw_size == target.raw_size
            && source.member_count == target.member_count,
        codec::IntegrityError::Metadata
    );
    let last = new.last().context(codec::IntegrityError::Metadata)?;
    let size = last
        .position
        .offset
        .checked_add(u64::try_from(last.position.encoding.stored_size)?)
        .context(codec::IntegrityError::Length)?;
    validate_entries(new, size, target.raw_size, target.member_count, true)?;
    let mut output = Vec::with_capacity(usize::try_from(size)?);
    output.extend_from_slice(HEADER);
    for (before, after) in old.iter().zip(new) {
        ensure!(
            before.uuid == after.uuid
                && before.hash == after.hash
                && before.raw_size == after.raw_size
                && (!after.position.encoding.compressed || before.position.encoding.compressed)
                && before.position.encoding.location_id != after.position.encoding.location_id,
            codec::IntegrityError::Metadata
        );
        let range =
            before
                .position
                .range(before.raw_size, file.len() as u64, source.member_count)?;
        let chunk = before.chunk(source)?;
        let body = codec::open_body(
            &chunk,
            file[range.start as usize..range.end as usize].to_vec(),
            keys,
        )?;
        let body = bytes::Bytes::from(body);
        let raw = codec::decode_cache_read(&chunk, body.clone(), chunk.compressed, pool, true)?;
        let payload = if after.position.encoding.compressed {
            body
        } else {
            raw
        };
        output.extend_from_slice(&codec::seal_body(
            &after.chunk(target)?,
            payload.to_vec(),
            keys,
        )?);
    }
    ensure!(output.len() as u64 == size, codec::IntegrityError::Length);
    target.stored_size = Some(i64::try_from(size)?);
    target.digest = Some(blake3::hash(&output).as_bytes().to_vec());
    Ok(output)
}

pub(crate) fn validate_file(pack: &Pack, file: &[u8]) -> Result<()> {
    ensure!(file.starts_with(HEADER), codec::IntegrityError::Metadata);
    ensure!(
        pack.stored_size == Some(i64::try_from(file.len())?),
        codec::IntegrityError::Length
    );
    ensure!(
        pack.digest.as_deref() == Some(blake3::hash(file).as_bytes().as_slice()),
        codec::IntegrityError::Hash
    );
    Ok(())
}

pub(crate) fn decode_entry(
    pack: &Pack,
    entry: &Entry,
    bytes: Vec<u8>,
    keys: &dyn codec::KeySource,
    pool: &crate::content::compression::Pool,
    verify: bool,
) -> Result<bytes::Bytes> {
    let size = u64::try_from(pack.stored_size.context(codec::IntegrityError::Metadata)?)?;
    entry
        .position
        .range(entry.raw_size, size, pack.member_count)?;
    let chunk = entry.chunk(pack)?;
    let (raw, _) = codec::decode_member(&chunk, bytes, keys, 0, pool, verify)?;
    Ok(raw)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        content::{compression, nonce},
        model::Id,
    };
    use chrono::Utc;
    use std::collections::BTreeMap;

    #[test]
    fn packed_encodings_keep_their_identity_and_bytes() -> Result<()> {
        let key = Uuid::new_v4();
        let keys = BTreeMap::from([(key, codec::Key::new("aes-256-gcm", [6; 32])?)]);
        let pool = compression::Pool::new(compression::Config::default(), 1)?;
        let now = Utc::now();
        let mut file = HEADER.to_vec();
        let mut builder = Builder::new();
        let mut members = Vec::new();
        for (i, encrypted, compress) in [(1, false, false), (2, true, false), (3, true, true)] {
            let plain = vec![i as u8; 32768];
            let chunk = Chunk {
                id: Id::from(i as i64),
                uuid: Uuid::now_v7(),
                encoding_id: Id::from(i as i64),
                location_id: Uuid::now_v7(),
                address: None,
                home_storage_id: None,
                storage_id: Some(Uuid::now_v7()),
                hash: blake3::hash(&plain).as_bytes().to_vec(),
                raw_size: plain.len() as i32,
                stored_size: None,
                algorithm: if encrypted { "aes-256-gcm" } else { "none" }.to_owned(),
                block_key_id: encrypted.then_some(key),
                compressed: false,
                nonce: encrypted
                    .then(|| codec::typed_nonce(now, i, nonce::CHUNK).unwrap().to_vec()),
                format: 2,
                state: "ready".into(),
                created_at: now,
            };
            let (chunk, standalone, _) =
                codec::encode(chunk, plain.clone(), &keys, 0, &pool, compress)?;
            let (position, body) = builder.append(&chunk, &standalone)?;
            assert_eq!(position.offset as usize, file.len());
            assert_eq!(body, &standalone[8..]);
            file.extend_from_slice(body);
            assert!(builder.append(&chunk, &standalone).is_err());
            members.push((chunk, plain, standalone, position));
        }
        let (size, hash) = builder.finish(3 * 32768, 3)?;
        assert_eq!(hash, blake3::hash(&file));
        assert_eq!(size, file.len() as u64);
        let mut pack = Pack {
            id: 5.into(),
            location_id: Uuid::now_v7(),
            address: None,
            home_storage_id: None,
            storage_id: members[0].0.storage_id,
            algorithm: "none".into(),
            block_key_id: None,
            raw_size: 3 * 32768,
            stored_size: None,
            compressed: false,
            nonce: None,
            digest: None,
            tier_index: None,
            member_count: 3,
            state: "ready".into(),
            created_at: now,
        };
        let files = members
            .iter()
            .map(|(chunk, _, standalone, _)| {
                (chunk.clone(), bytes::Bytes::copy_from_slice(standalone))
            })
            .collect::<Vec<_>>();
        let (sealed, entries) = assemble(&mut pack, &files)?;
        assert_eq!(sealed, file);
        validate_file(&pack, &sealed)?;
        let entries = entries
            .into_iter()
            .map(|(_, entry)| entry)
            .collect::<Vec<_>>();
        validate_entries(&entries, size, pack.raw_size, 3, true)?;
        let sparse = vec![entries[0].clone(), entries[2].clone()];
        validate_entries(&sparse, size, pack.raw_size, 3, false)?;
        assert!(validate_entries(&sparse, size, pack.raw_size, 3, true).is_err());
        let mut overlap = sparse.clone();
        overlap[1].position.offset = entries[0].position.offset + 1;
        assert!(validate_entries(&overlap, size, pack.raw_size, 3, false).is_err());
        assert!(
            validate_entries(
                &[entries[1].clone(), entries[0].clone()],
                size,
                pack.raw_size,
                3,
                false
            )
            .is_err()
        );
        let mut damaged = sealed.clone();
        *damaged.last_mut().unwrap() ^= 1;
        assert!(validate_file(&pack, &damaged).is_err());
        assert!(validate_file(&pack, &sealed[1..]).is_err());
        for (member, entry) in members.iter().zip(&entries) {
            let r = entry.position.range(member.0.raw_size, size, 3)?;
            assert_eq!(
                decode_entry(
                    &pack,
                    entry,
                    file[r.start as usize..r.end as usize].to_vec(),
                    &keys,
                    &pool,
                    true
                )?
                .as_ref(),
                member.1
            );
        }
        for (chunk, plain, standalone, pos) in &members {
            let range = pos.range(chunk.raw_size, size, 3)?;
            let body = file[range.start as usize..range.end as usize].to_vec();
            assert_eq!([codec::FILE_HEADER.as_slice(), &body].concat(), *standalone);
            let mut encoding = chunk.clone();
            encoding.location_id = Uuid::nil();
            pos.encoding.apply(&mut encoding)?;
            assert_eq!(
                codec::decode_member(&encoding, body.clone(), &keys, 0, &pool, true)?
                    .0
                    .as_ref(),
                plain
            );
            if chunk.block_key_id.is_some() {
                encoding.location_id = Uuid::now_v7();
                assert!(
                    codec::decode_member(&encoding, body.clone(), &keys, 0, &pool, true).is_err()
                );
            }
            let mut wrong = pos.clone();
            wrong.offset = size;
            assert!(wrong.range(chunk.raw_size, size, 3).is_err());
        }
        Ok(())
    }
}
