use super::scan::Scanner;
use crate::{
    model::{
        storage_recovery::{DecodedFile, Part, verify},
        storage_transfer::Action,
    },
    storage::{
        Disk,
        journal::{Format, FrameType, recovery, stream::Frames},
    },
};
use anyhow::{Context, Result, ensure};
use futures_util::{StreamExt, TryStreamExt};
use std::sync::Arc;
use tokio::{
    fs::File,
    io::{AsyncSeekExt, AsyncWriteExt},
};
use uuid::Uuid;

pub(super) type Candidate = (crate::model::storage_payload::Receipt, Uuid, bool);

impl Scanner {
    pub(super) async fn range(
        self: &Arc<Self>,
        intent: &crate::model::storage_payload::Intent,
        candidates: &[Candidate],
        start: u64,
        end: u64,
    ) -> Result<(
        u64,
        futures_util::stream::BoxStream<'static, Result<bytes::Bytes>>,
    )> {
        use crate::storage::native::reader;
        ensure!(
            start < end && end <= intent.total && intent.layout.supported(),
            "invalid recovery read range"
        );
        let (wanted, offset, contiguous) =
            crate::content::erasure::locate(&intent.layout, start % intent.span());
        let length = contiguous
            .min(end - start)
            .min(reader::part_length(&intent.layout))
            .min(intent.span() - start % intent.span());
        let mut copies = Vec::new();
        let mut recovery = Vec::new();
        for (receipt, media, backup) in candidates {
            intent.accepts(&receipt.header)?;
            if u64::from(receipt.header.sequence) != start / intent.span() {
                continue;
            }
            let direct = receipt.header.shard == wanted;
            if !direct && intent.layout.parity_shards == 0 {
                continue;
            }
            ensure!(
                offset
                    .checked_add(length)
                    .is_some_and(|n| n <= receipt.header.shard_bytes),
                "recovery range exceeds its fragment"
            );
            let action = if *backup {
                Action::BackupRead {
                    receipt: receipt.clone(),
                    offset,
                    length,
                }
            } else {
                Action::Read {
                    receipt: receipt.clone(),
                    offset,
                    length,
                }
            };
            if let Ok(target) = self.target(*media, action).await {
                if direct {
                    copies.push(target);
                } else {
                    recovery.push(target);
                }
            }
        }
        Ok((
            length,
            reader::part(
                self.pool.clone(),
                self.clone(),
                reader::Part {
                    offset: start,
                    length,
                    copies,
                    recovery,
                },
            )
            .await?,
        ))
    }
}

struct Spool {
    file: Option<File>,
    space: Option<crate::storage::local::Space>,
}

impl Drop for Spool {
    fn drop(&mut self) {
        let Some(mut file) = self.file.take() else {
            return;
        };
        let space = self.space.take();
        tokio::spawn(async move {
            let _ = file.flush().await;
            drop(file);
            drop(space);
        });
    }
}

impl Scanner {
    pub async fn decode(
        self: &Arc<Self>,
        catalog: Uuid,
        part: &Part,
        through: u64,
        format: Option<&Format<'_>>,
        decoder: &mut verify::Decoders,
        disk: &Arc<Disk>,
    ) -> Result<DecodedFile> {
        let indexed = self
            .clusters
            .catalog_file(catalog, &part.path)
            .await?
            .context("unindexed recovery file")?;
        ensure!(
            indexed.part == *part && indexed.through == through,
            "recovery file differs from its fixed index"
        );
        let authentication = format.map_or([0; 32], |format| {
            let key = blake3::derive_key("mokyu/storage/recovery-decoded/v1", &format.key.material);
            *blake3::keyed_hash(&key, &part.digest).as_bytes()
        });
        if let Some(summary) = indexed.decoded
            && let Some(format) = format
            && summary.starts_complete
            && summary.complete
            && decoder.complete()
            && summary.journal == format.journal
            && summary.epoch == format.epoch
            && summary.authentication == authentication
        {
            return Ok(summary);
        }
        let starts_complete = decoder.complete();
        let receipts =
            verify::fragments(self.clusters.as_ref(), catalog, part, &self.anchor).await?;
        ensure!(
            part.intent.layout.supported(),
            "unsupported recovery fragment layout"
        );
        let root = disk.root.clone();
        let mut space = disk.reserve_space(&root, disk.floor, part.intent.total)?;
        space.defer();
        let file = File::from_std(
            tokio::task::spawn_blocking(move || tempfile::tempfile_in(root)).await??,
        );
        let mut spool = Spool {
            file: Some(file),
            space: Some(space),
        };
        let mut candidates = Vec::new();
        for receipt in receipts {
            if self.backup_group.is_none() {
                let media = receipt.header.media;
                candidates.push((receipt, media, false));
                continue;
            }
            let mut after = None;
            loop {
                let page = self
                    .clusters
                    .catalog_page(catalog, 2, Some(receipt.header.fragment), after)
                    .await?;
                for record in page.entries {
                    if let crate::model::storage_recovery::CatalogEntry::BackupLocation {
                        media,
                        fragment,
                        ..
                    } = record
                    {
                        ensure!(
                            fragment == receipt.header.fragment
                                && self
                                    .anchor
                                    .members
                                    .iter()
                                    .any(|m| m.media_id == media && m.topology.purpose == "backup"),
                            "untrusted backup recovery location"
                        );
                        candidates.push((receipt.clone(), media, true));
                    }
                }
                match page.after {
                    Some(next) => after = Some(next),
                    None => break,
                }
            }
        }
        let file = spool.file.as_mut().unwrap();
        let mut digest = blake3::Hasher::new();
        let mut pos = 0;
        while pos < part.intent.total {
            let (len, mut stream) = self
                .range(&part.intent, &candidates, pos, part.intent.total)
                .await?;
            let start = pos;
            while let Some(bytes) = stream.try_next().await? {
                let _io = disk.io.acquire().await?;
                file.write_all(&bytes).await?;
                digest.update(&bytes);
                pos += bytes.len() as u64;
            }
            ensure!(
                pos - start == len,
                "recovery range ended before its planned length"
            );
        }
        file.flush().await?;
        ensure!(
            digest.finalize().as_bytes() == &part.digest,
            "recovered bytes differ from their published digest"
        );
        file.rewind().await?;
        spool.space.as_mut().unwrap().materialized();
        // Validate the complete published bytes before any decoded row reaches shared staging.
        let file = spool.file.as_ref().unwrap().try_clone().await?;
        let body = tokio_util::io::ReaderStream::new(file).boxed();
        let mut frames = Frames::new(body, part.intent.total)?;
        let mut identity = None;
        let mut summary: Option<DecodedFile> = None;
        let mut sealed = false;
        while frames.remaining > 0 {
            let (_, encoded) = frames.encoded().await?;
            let public = recovery::inspect(&encoded)?;
            let current = (public.journal, public.epoch);
            ensure!(
                identity.is_none_or(|old| old == current),
                "recovery part contains multiple journals"
            );
            identity = Some(current);
            let kind = public.envelope.kind;
            let seq = public.envelope.sequence;
            if let Some(summary) = &mut summary {
                ensure!(
                    !sealed && seq.previous == summary.tail,
                    "recovery frame chain is incomplete"
                );
                if kind == FrameType::Seal {
                    ensure!(
                        matches!(summary.kind, FrameType::Mappings | FrameType::Changes)
                            && seq.first == summary.first
                            && seq.last == summary.last
                            && frames.remaining == 0,
                        "invalid recovery segment seal"
                    );
                    summary.kind = FrameType::Changes;
                    sealed = true;
                } else {
                    ensure!(
                        matches!(
                            kind,
                            FrameType::Mappings | FrameType::Changes | FrameType::Snapshot
                        ) && (summary.kind == FrameType::Mappings
                            || kind == FrameType::Mappings
                            || kind == summary.kind)
                            && summary.last.checked_add(1) == Some(seq.first),
                        "discontinuous recovery records"
                    );
                    if kind != FrameType::Mappings {
                        summary.kind = kind;
                    }
                    summary.last = seq.last;
                }
                summary.tail = public.digest;
            } else {
                ensure!(
                    matches!(
                        kind,
                        FrameType::Mappings
                            | FrameType::Changes
                            | FrameType::Snapshot
                            | FrameType::Manifest
                            | FrameType::Index
                            | FrameType::History
                    ),
                    "invalid first recovery frame"
                );
                if matches!(
                    kind,
                    FrameType::Manifest | FrameType::Index | FrameType::History
                ) {
                    ensure!(
                        frames.remaining == 0 && seq.first == seq.last,
                        "recovery envelope contains extra frames"
                    );
                } else {
                    ensure!(seq.first > 0, "invalid recovery sequence");
                }
                summary = Some(DecodedFile {
                    journal: current.0,
                    epoch: current.1,
                    kind,
                    first: seq.first,
                    last: seq.last,
                    previous: seq.previous,
                    tail: public.digest,
                    starts_complete,
                    complete: false,
                    authentication,
                });
            }
            let mut entries = Vec::new();
            verify::frame(part, public, through, format, decoder, |entry| {
                entries.push(entry);
                Ok(())
            })?;
            let mut start = 0;
            while start < entries.len() {
                let mut end = start;
                let mut bytes = 0;
                while end < entries.len() && end - start < 256 {
                    let size = serde_json::to_vec(&entries[end])?.len();
                    if end > start && bytes + size > 8 * 1024 * 1024 {
                        break;
                    }
                    bytes += size;
                    end += 1;
                }
                self.clusters
                    .catalog_stage(catalog, &entries[start..end])
                    .await?;
                start = end;
            }
        }
        ensure!(identity.is_some(), "empty recovery payload");
        let mut summary = summary.context("empty recovery summary")?;
        summary.complete = decoder.complete();
        ensure!(
            summary.kind != FrameType::Mappings && (summary.kind != FrameType::Changes || sealed),
            "recovery segment is not sealed"
        );
        if summary.kind == FrameType::Snapshot {
            ensure!(
                summary.first == 1 && summary.previous == [0; 32],
                "invalid snapshot page boundary"
            );
        }
        if format.is_some() {
            self.clusters
                .catalog_decoded(catalog, part.intent.operation, &summary)
                .await?;
        }
        drop(frames);
        Ok(summary)
    }
}
