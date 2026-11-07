use super::{
    Format, FrameType, ROOT, Sequence,
    batch::{Decoder, frames},
};
use crate::model::journal::record::{Attributes, Material, Span, WrappedKey};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct BucketCut {
    pub id: Uuid,
    pub name: String,
    pub generation: i64,
    pub sequence: i64,
    pub stopped: bool,
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub(crate) enum Row {
    Key(WrappedKey),
    Material(Material),
    Bucket(BucketCut),
    Ranges {
        bucket: Uuid,
        key: String,
        seq: i64,
        offset: i64,
        ranges: Vec<Span>,
    },
    Object {
        bucket: Uuid,
        key: String,
        generation: i64,
        seq: i64,
        attributes: Attributes,
        range_count: u64,
        range_digest: [u8; 32],
    },
    Delete {
        bucket: Uuid,
        key: String,
        generation: i64,
        seq: i64,
    },
    Drop {
        bucket: Uuid,
        generation: i64,
        seq: i64,
    },
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct Page {
    pub owner: Uuid,
    pub number: u32,
    pub bytes: u64,
    pub items: u64,
    pub digest: [u8; 32],
    pub first_bucket: Option<Uuid>,
    pub last_bucket: Option<Uuid>,
}
#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct Manifest {
    pub format_version: u8,
    pub id: Uuid,
    pub parent: Option<Uuid>,
    pub sequence: u64,
    pub tail: [u8; 32],
    pub pages: Vec<Page>,
}
#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct Index {
    pub format_version: u8,
    pub snapshot: Uuid,
    pub operation: Uuid,
    pub revision: u64,
    pub sequence: u64,
    pub manifest_digest: [u8; 32],
    pub buckets: Vec<BucketCut>,
    pub bucket_count: u64,
}

pub(crate) fn page_path(owner: Uuid, number: u32) -> String {
    format!("{ROOT}/snapshots/{}/{number:08}", owner.simple())
}
pub(crate) fn manifest_path(owner: Uuid) -> String {
    format!("{ROOT}/snapshots/{}/manifest", owner.simple())
}
pub(crate) fn index_path() -> String {
    format!("{ROOT}/index")
}

pub(crate) fn envelope<T: Serialize>(
    format: &Format<'_>,
    ty: FrameType,
    path: &str,
    revision: u64,
    value: &T,
) -> Result<Vec<u8>> {
    ensure!(revision > 0, "invalid snapshot envelope revision");
    format.encode(
        ty,
        path,
        Sequence {
            first: revision,
            last: revision,
            previous: [0; 32],
        },
        &serde_json::to_vec(value)?,
    )
}
pub(crate) fn open_envelope<T: serde::de::DeserializeOwned>(
    format: &Format<'_>,
    ty: FrameType,
    path: &str,
    data: &[u8],
) -> Result<(u64, T)> {
    let frame = format.decode(ty, path, data)?;
    ensure!(
        frame.consumed == data.len()
            && frame.sequence.first > 0
            && frame.sequence.first == frame.sequence.last
            && frame.sequence.previous == [0; 32],
        "invalid snapshot envelope"
    );
    Ok((frame.sequence.first, serde_json::from_slice(&frame.bytes)?))
}

pub(crate) fn encode_pages(
    format: &Format<'_>,
    owner: Uuid,
    first_page: u32,
    items: &[Row],
    limit: usize,
    first_bucket: Option<Uuid>,
    last_bucket: Option<Uuid>,
) -> Result<Vec<(Page, Vec<u8>)>> {
    let mut pages = Vec::new();
    let mut number = first_page;
    let mut data = Vec::new();
    let mut next = 1u64;
    let mut prior = [0; 32];
    for plain in frames(items, limit, format.encrypted)? {
        let encode = |number: u32, first: u64, previous: [u8; 32]| -> Result<_> {
            let last = first
                .checked_add(u64::from(plain.records) - 1)
                .context("snapshot page sequence exhausted")?;
            let bytes = format.encode(
                FrameType::Snapshot,
                &page_path(owner, number),
                Sequence {
                    first,
                    last,
                    previous,
                },
                &plain.bytes,
            )?;
            Ok((last, bytes))
        };
        let (mut last, mut bytes) = encode(number, next, prior)?;
        if !data.is_empty() && data.len() + bytes.len() > limit {
            pages.push((
                Page {
                    owner,
                    number,
                    bytes: data.len() as u64,
                    items: next - 1,
                    digest: *blake3::hash(&data).as_bytes(),
                    first_bucket,
                    last_bucket,
                },
                std::mem::take(&mut data),
            ));
            number = number
                .checked_add(1)
                .context("snapshot page count exhausted")?;
            (last, bytes) = encode(number, 1, [0; 32])?;
        }
        ensure!(
            data.len() + bytes.len() <= limit,
            "snapshot frame exceeds page limit"
        );
        prior = *blake3::hash(&bytes).as_bytes();
        data.extend_from_slice(&bytes);
        next = last.checked_add(1).context("snapshot sequence exhausted")?;
    }
    if !data.is_empty() {
        pages.push((
            Page {
                owner,
                number,
                bytes: data.len() as u64,
                items: next - 1,
                digest: *blake3::hash(&data).as_bytes(),
                first_bucket,
                last_bucket,
            },
            data,
        ));
    }
    Ok(pages)
}

pub(crate) fn decode_page(
    format: &Format<'_>,
    page: &Page,
    data: &[u8],
    decoder: &mut Decoder,
    mut visit: impl FnMut(Row) -> Result<()>,
) -> Result<()> {
    ensure!(
        page.bytes == data.len() as u64 && *blake3::hash(data).as_bytes() == page.digest,
        "snapshot page digest mismatch"
    );
    let path = page_path(page.owner, page.number);
    let mut rest = data;
    let mut first = 1;
    let mut previous = [0; 32];
    while !rest.is_empty() {
        let frame = format.decode(FrameType::Snapshot, &path, rest)?;
        ensure!(
            frame.sequence.first == first && frame.sequence.previous == previous,
            "discontinuous snapshot page"
        );
        decoder.read(&frame.bytes, frame.sequence.last - first + 1, &mut visit)?;
        first = frame
            .sequence
            .last
            .checked_add(1)
            .context("snapshot sequence exhausted")?;
        previous = frame.digest;
        rest = &rest[frame.consumed..];
    }
    ensure!(first - 1 == page.items, "snapshot page length mismatch");
    Ok(())
}
