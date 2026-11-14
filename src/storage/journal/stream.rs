//! Replay authenticated frames into either online recovery or an on-disk catalog.
use super::{
    Format, Frame, FrameType,
    batch::{Decoder, Record},
    replay::Catalog,
    snapshot::{self, BucketCut, Page, Row},
};
use anyhow::{Context, Result, ensure};
use futures_util::{Stream, TryStreamExt};
use object_store::{GetOptions, ObjectStore, path::Path};
use std::{collections::BTreeMap, io, pin::Pin};
use tokio::io::AsyncReadExt;
use tokio_util::io::StreamReader;

pub(crate) trait Sink {
    fn scope(&mut self, cuts: &[BucketCut], excluded: &[BucketCut]) -> Result<()>;
    fn row(&mut self, row: Row, f: &Format<'_>) -> Result<()>;
    fn stage(&mut self, record: Record, f: &Format<'_>) -> Result<()>;
    fn apply(&mut self) -> Result<()>;
    fn finish(&mut self) -> Result<()>;
    fn list(&mut self, first: u64, path: String) -> Result<()>;
    fn pop(&mut self) -> Result<Option<(u64, String)>>;
}

#[derive(Default)]
pub(crate) struct Memory {
    pub catalog: Catalog,
    paths: BTreeMap<u64, String>,
}
impl Sink for Memory {
    fn scope(&mut self, cuts: &[BucketCut], excluded: &[BucketCut]) -> Result<()> {
        self.catalog.replace_scope(cuts, excluded)
    }
    fn row(&mut self, row: Row, f: &Format<'_>) -> Result<()> {
        self.catalog.snapshot_row(row, f)
    }
    fn stage(&mut self, record: Record, f: &Format<'_>) -> Result<()> {
        self.catalog.stage(record, f)
    }
    fn apply(&mut self) -> Result<()> {
        self.catalog.apply()
    }
    fn finish(&mut self) -> Result<()> {
        self.catalog.validate_pack_members()
    }
    fn list(&mut self, first: u64, path: String) -> Result<()> {
        ensure!(
            self.paths.insert(first, path).is_none(),
            "duplicate journal segment"
        );
        Ok(())
    }
    fn pop(&mut self) -> Result<Option<(u64, String)>> {
        Ok(self.paths.pop_first())
    }
}

type Body = Pin<Box<dyn Stream<Item = io::Result<bytes::Bytes>> + Send>>;
pub(crate) struct Frames {
    reader: StreamReader<Body, bytes::Bytes>,
    pub(crate) remaining: u64,
    digest: blake3::Hasher,
}
impl Frames {
    pub(crate) fn new(body: Body, size: u64) -> Result<Self> {
        ensure!(
            size > 0 && size <= u64::from(crate::model::journal::policy::MAX_LIMIT),
            "invalid recovery file length"
        );
        Ok(Self {
            reader: StreamReader::new(body),
            remaining: size,
            digest: blake3::Hasher::new(),
        })
    }
    async fn open(store: &dyn ObjectStore, path: &str, prefix: Option<u64>) -> Result<Self> {
        let options = GetOptions {
            range: prefix.map(|size| (0..size).into()),
            ..Default::default()
        };
        let file = store.get_opts(&Path::from(path), options).await?;
        let size = prefix.unwrap_or(file.meta.size);
        ensure!(
            size > 0
                && size <= u64::from(crate::model::journal::policy::MAX_LIMIT)
                && file.range == (0..size),
            "invalid recovery file length for {path}"
        );
        let body: Body = Box::pin(file.into_stream().map_err(io::Error::other));
        Self::new(body, size)
    }
    async fn next(&mut self, f: &Format<'_>, path: &str) -> Result<(FrameType, Frame)> {
        let (ty, encoded) = self.encoded().await?;
        ensure!(
            matches!(
                ty,
                FrameType::Changes
                    | FrameType::Seal
                    | FrameType::Snapshot
                    | FrameType::PartCommit
                    | FrameType::Mappings
            ),
            "unexpected frame type in {path}"
        );
        Ok((ty, f.decode(ty, path, &encoded)?))
    }

    pub(crate) async fn encoded(&mut self) -> Result<(FrameType, Vec<u8>)> {
        ensure!(
            self.remaining >= super::HEADER as u64,
            "truncated journal header"
        );
        let mut magic = [0; 8];
        self.reader.read_exact(&mut magic).await?;
        let native = &magic == super::recovery::MAGIC;
        ensure!(
            native || &magic == super::MAGIC,
            "unsupported recovery file"
        );
        let head_size = if native {
            super::recovery::HEADER
        } else {
            super::HEADER
        };
        ensure!(
            self.remaining >= head_size as u64,
            "truncated metadata header"
        );
        let mut header = vec![0; head_size];
        header[..8].copy_from_slice(&magic);
        self.reader.read_exact(&mut header[8..]).await?;
        let ty = FrameType::read(&header)?;
        let size = if native {
            super::recovery::frame_size(&header)?
        } else {
            let count = u32::from_be_bytes(header[30..34].try_into()?) as usize;
            ensure!(count <= super::FRAME_LIMIT, "invalid journal frame length");
            Format::overhead(header[9] & 2 != 0) + count
        };
        ensure!(
            size as u64 <= self.remaining,
            "invalid journal frame length"
        );
        let mut encoded = vec![0; size];
        encoded[..head_size].copy_from_slice(&header);
        self.reader.read_exact(&mut encoded[head_size..]).await?;
        self.remaining -= size as u64;
        self.digest.update(&encoded);
        Ok((ty, encoded))
    }
}

pub(super) async fn page(
    store: &dyn ObjectStore,
    f: &Format<'_>,
    page: &Page,
    decoder: &mut Decoder,
    sink: &mut impl Sink,
) -> Result<()> {
    let path = snapshot::page_path(page.owner, page.number);
    let mut frames = Frames::open(store, &path, None).await?;
    ensure!(
        frames.remaining == page.bytes,
        "snapshot page size mismatch"
    );
    let (mut sequence, mut prior) = (0u64, [0; 32]);
    while frames.remaining > 0 {
        let (ty, frame) = frames.next(f, &path).await?;
        if ty == FrameType::Mappings {
            ensure!(
                sequence.checked_add(1) == Some(frame.sequence.first)
                    && frame.sequence.previous == prior
                    && frame.bytes.is_empty(),
                "invalid snapshot mapping boundary"
            );
            sequence = frame.sequence.last;
            prior = frame.digest;
            continue;
        }
        ensure!(
            ty == FrameType::Snapshot
                && sequence.checked_add(1) == Some(frame.sequence.first)
                && frame.sequence.previous == prior,
            "discontinuous snapshot page"
        );
        decoder.read(&frame.bytes, frame.sequence.last - sequence, |row| {
            sink.row(row, f)
        })?;
        sequence = frame.sequence.last;
        prior = frame.digest;
    }
    ensure!(
        sequence == page.items && frames.digest.finalize().as_bytes() == &page.digest,
        "invalid snapshot page"
    );
    Ok(())
}

pub(super) struct Segment<'a> {
    pub physical: &'a str,
    pub first: u64,
    pub previous: [u8; 32],
    pub prefix: Option<u64>,
}
impl Segment<'_> {
    pub async fn read(
        &self,
        store: &dyn ObjectStore,
        f: &Format<'_>,
        decoder: &mut Decoder,
        sink: &mut impl Sink,
    ) -> Result<(u64, [u8; 32], [u8; 32])> {
        let path = super::segment_path(f.epoch, self.first);
        let mut frames = Frames::open(store, self.physical, self.prefix).await?;
        let mut sequence = self
            .first
            .checked_sub(1)
            .context("invalid first journal sequence")?;
        let mut boundary = sequence;
        let mut prior = self.previous;
        loop {
            let (ty, frame) = frames.next(f, &path).await?;
            ensure!(
                frame.sequence.previous == prior,
                "discontinuous journal frame"
            );
            prior = frame.digest;
            match ty {
                FrameType::Mappings => {
                    ensure!(
                        sequence.checked_add(1) == Some(frame.sequence.first)
                            && frame.bytes.is_empty(),
                        "invalid journal mapping boundary"
                    );
                    sequence = frame.sequence.last;
                }
                FrameType::Changes => {
                    ensure!(
                        sequence.checked_add(1) == Some(frame.sequence.first),
                        "discontinuous journal sequence"
                    );
                    decoder.read(&frame.bytes, frame.sequence.last - sequence, |rec| {
                        sink.stage(rec, f)
                    })?;
                    sequence = frame.sequence.last;
                }
                FrameType::PartCommit | FrameType::Seal => {
                    ensure!(
                        sequence >= self.first
                            && frame.sequence.first == self.first
                            && frame.sequence.last == sequence
                            && frame.bytes.is_empty(),
                        "invalid journal boundary"
                    );
                    if ty == FrameType::Seal {
                        ensure!(frames.remaining == 0, "trailing bytes after journal seal");
                        break;
                    }
                    ensure!(sequence > boundary, "repeated journal part boundary");
                    boundary = sequence;
                    if self.prefix.is_some() && frames.remaining == 0 {
                        break;
                    }
                }
                _ => anyhow::bail!("unexpected segment frame"),
            }
        }
        Ok((sequence, prior, *frames.digest.finalize().as_bytes()))
    }
}

#[cfg(test)]
#[path = "../../../tests/storage/native_journal.rs"]
mod native_tests;
