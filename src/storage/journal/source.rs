use super::{
    Format, FrameType, ROOT,
    batch::Decoder,
    replay::{self, Catalog},
    segment_path, snapshot,
};
use crate::{
    content::codec::Key,
    model::journal::policy::MAX_LIMIT,
    storage::{
        marker::{self, EpochCut},
        model,
    },
};
use anyhow::{Context, Result, ensure};
use futures_util::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use uuid::Uuid;

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct Source {
    pub connection: model::Connection,
    pub journal: marker::Journal,
    pub material: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub(crate) struct Cut {
    pub point: EpochCut,
    pub checkpoint: Option<(Uuid, [u8; 32])>,
}

pub(crate) fn options() -> model::Options {
    model::Options {
        connect_timeout: Duration::from_secs(10),
        request_timeout: Duration::from_secs(120),
        retry_timeout: Duration::from_secs(30),
        max_retries: 2,
    }
}

pub(crate) async fn bytes(
    store: &dyn ObjectStore,
    path: &str,
    max: u64,
) -> Result<Option<Vec<u8>>> {
    let object = match store.get(&Path::from(path)).await {
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        value => value?,
    };
    let size = object.meta.size;
    ensure!(size <= max, "recovery file exceeds size limit");
    let mut data = Vec::with_capacity(usize::try_from(size)?);
    let mut chunks = object.into_stream();
    while let Some(chunk) = chunks.try_next().await? {
        ensure!(
            chunk.len() <= size as usize - data.len(),
            "recovery file grew during read"
        );
        data.extend_from_slice(&chunk);
    }
    ensure!(data.len() as u64 == size, "incomplete recovery file");
    Ok(Some(data))
}

impl Source {
    pub fn connect(&self) -> Result<Arc<dyn ObjectStore>> {
        Ok(model::Store::connect(&self.connection, options())?.backend)
    }

    pub fn client(&self) -> Result<crate::storage::external::Client> {
        client(&self.connection)
    }

    pub async fn salvage(&self, store: &dyn ObjectStore, old: &Cut) -> Result<Cut> {
        use crate::storage::external::objects::Condition;
        let first = old
            .point
            .sequence
            .checked_add(1)
            .context("journal sequence exhausted")?;
        let path = segment_path(old.point.epoch, first);
        let client = self.client()?;
        let mut loaded = bytes(store, &path, u64::from(MAX_LIMIT)).await?;
        if loaded.is_none() {
            let (ids, more) = client.uploads(&path).await?;
            ensure!(!more && ids.len() <= 1, "ambiguous journal uploads");
            let Some(upload) = ids.first() else {
                return Ok(old.clone());
            };
            let mut parts = Vec::new();
            let mut after = 0;
            let mut size = 0u64;
            loop {
                let page = client
                    .parts(&path, upload, after)
                    .await?
                    .context("journal upload disappeared")?;
                for part in page.items {
                    ensure!(
                        usize::from(part.number) == parts.len() + 1 && part.size > 0,
                        "missing journal part"
                    );
                    size = size
                        .checked_add(part.size)
                        .context("journal upload size overflow")?;
                    ensure!(size <= u64::from(MAX_LIMIT), "journal upload exceeds limit");
                    parts.push(part);
                }
                let Some(next) = page.next else { break };
                ensure!(
                    next > after && Some(next) == parts.last().map(|p| p.number),
                    "invalid journal parts cursor"
                );
                after = next;
            }
            if parts.is_empty() {
                return Ok(old.clone());
            }
            let result = client
                .complete_upload(&path, upload, &parts, &Condition::Absent)
                .await;
            loaded = bytes(store, &path, u64::from(MAX_LIMIT)).await?;
            if loaded.is_none() {
                result?;
                anyhow::bail!("completed journal file is missing");
            }
        }
        let data = loaded.unwrap();
        let key = Key::new("aes-256-gcm", self.material)?;
        let format = Format {
            storage: self.connection.id,
            encrypted: true,
            journal: self.journal.journal_id,
            epoch: old.point.epoch,
            key: &key,
        };
        let previous = old.point.tail;
        let (sequence, tail, salvaged) = if let Ok((last, digest)) =
            super::verify_segment(&format, &path, &data, first, previous, |_| Ok(()))
        {
            (last, digest, None)
        } else {
            let mut at = 0usize;
            let mut boundary = 0usize;
            while data.len() - at >= super::HEADER {
                let h = &data[at..];
                ensure!(&h[..8] == super::MAGIC, "corrupt journal upload");
                let n = u32::from_be_bytes(h[30..34].try_into()?) as usize;
                ensure!(n <= super::FRAME_LIMIT, "invalid journal frame length");
                ensure!(h[9] & !3 == 0, "invalid journal flags");
                let length = n + Format::overhead(h[9] & 2 != 0);
                if h.len() < length {
                    break;
                }
                let ty = match h[8] {
                    1 => FrameType::Changes,
                    8 => FrameType::PartCommit,
                    _ => anyhow::bail!("invalid journal upload tail"),
                };
                format.decode(ty, &path, &h[..length])?;
                at += length;
                if ty == FrameType::PartCommit {
                    boundary = at
                }
            }
            ensure!(
                boundary > 0,
                "journal upload has no confirmed part boundary"
            );
            let prefix = &data[..boundary];
            let (last, digest) =
                super::verify_prefix(&format, &path, prefix, first, previous, true, |_| Ok(()))?;
            (
                last,
                digest,
                Some(marker::Salvaged {
                    first,
                    bytes: boundary as u64,
                    digest: *blake3::hash(prefix).as_bytes(),
                }),
            )
        };
        Ok(Cut {
            point: EpochCut {
                epoch: old.point.epoch,
                sequence,
                tail,
                salvaged,
            },
            checkpoint: old.checkpoint,
        })
    }

    pub async fn read(
        &self,
        store: &dyn ObjectStore,
        fixed: Option<&Cut>,
    ) -> Result<(Catalog, Cut)> {
        let key = Key::new("aes-256-gcm", self.material)?;
        let history = super::history::chain(store, &self.journal, self.connection.id, &key).await?;
        let mut catalog = Catalog::default();
        let mut checkpoint = None;
        let mut seed = None;
        let index_path = snapshot::index_path();
        if let Some(encoded) = bytes(
            store,
            &index_path,
            (super::FRAME_LIMIT + Format::overhead(false)) as u64,
        )
        .await?
        {
            let mut index = None;
            for epoch in std::iter::once(self.journal.writer_epoch)
                .chain(history.iter().rev().map(|v| v.epoch))
            {
                let format = Format {
                    storage: self.connection.id,
                    encrypted: true,
                    journal: self.journal.journal_id,
                    epoch,
                    key: &key,
                };
                if let Ok((revision, entry)) = snapshot::open_envelope::<snapshot::Index>(
                    &format,
                    FrameType::Index,
                    &index_path,
                    &encoded,
                ) {
                    ensure!(
                        entry.format_version == 1
                            && revision == entry.revision
                            && entry.bucket_count == entry.buckets.len() as u64,
                        "invalid journal checkpoint"
                    );
                    index = Some((epoch, entry));
                    break;
                }
            }
            let (epoch, index) = index.context("cannot authenticate recovery index")?;
            let format = Format {
                storage: self.connection.id,
                encrypted: true,
                journal: self.journal.journal_id,
                epoch,
                key: &key,
            };
            checkpoint = Some((index.snapshot, index.manifest_digest));
            let path = snapshot::manifest_path(index.snapshot);
            let manifest = bytes(
                store,
                &path,
                (super::FRAME_LIMIT + Format::overhead(false)) as u64,
            )
            .await?
            .context("missing snapshot manifest")?;
            ensure!(
                *blake3::hash(&manifest).as_bytes() == index.manifest_digest,
                "snapshot manifest digest mismatch"
            );
            let (_, manifest) = snapshot::open_envelope::<snapshot::Manifest>(
                &format,
                FrameType::Manifest,
                &path,
                &manifest,
            )?;
            ensure!(
                manifest.format_version == 1
                    && manifest.id == index.snapshot
                    && manifest.sequence == index.sequence,
                "invalid snapshot manifest"
            );
            catalog.seed(&index.buckets)?;
            let mut decoder = Decoder::default();
            let mut seen = std::collections::HashSet::new();
            for page in manifest.pages {
                ensure!(
                    seen.insert((page.owner, page.number)) && page.bytes <= u64::from(MAX_LIMIT),
                    "invalid snapshot page reference"
                );
                let path = snapshot::page_path(page.owner, page.number);
                let data = bytes(store, &path, page.bytes)
                    .await?
                    .context("missing snapshot page")?;
                snapshot::decode_page(&format, &page, &data, &mut decoder, |row| {
                    catalog.snapshot_row(row, &format)
                })?;
            }
            ensure!(decoder.complete(), "incomplete snapshot record");
            seed = Some(EpochCut {
                epoch,
                sequence: manifest.sequence,
                tail: manifest.tail,
                salvaged: None,
            });
        }
        if let Some(fixed) = fixed {
            ensure!(
                fixed.checkpoint == checkpoint,
                "recovery checkpoint changed; repeat preview"
            );
        }
        let mut reached = seed.is_none();
        let mut last = None;
        for (epoch, end) in history
            .iter()
            .map(|p| (p.epoch, Some(p)))
            .chain(std::iter::once((
                self.journal.writer_epoch,
                fixed.map(|c| &c.point),
            )))
        {
            let start = seed.as_ref().filter(|s| s.epoch == epoch);
            reached |= start.is_some();
            if !reached {
                continue;
            }
            let mut sequence = start.map_or(0, |s| s.sequence);
            let mut tail = start.map_or([0; 32], |s| s.tail);
            let prefix = Path::from(format!("{ROOT}/changes/{}", epoch.simple()));
            let mut listed = store.list(Some(&prefix));
            let mut paths = BTreeMap::new();
            while let Some(meta) = listed.try_next().await? {
                let path = meta.location.to_string();
                let first = path
                    .rsplit('/')
                    .next()
                    .context("unexpected journal file")?
                    .parse::<u64>()?;
                ensure!(
                    first > 0 && path == segment_path(epoch, first),
                    "invalid journal segment path"
                );
                if first > sequence && end.is_none_or(|p| first <= p.sequence) {
                    ensure!(
                        paths.insert(first, path).is_none(),
                        "duplicate journal segment"
                    );
                }
            }
            let format = Format {
                storage: self.connection.id,
                encrypted: true,
                journal: self.journal.journal_id,
                epoch,
                key: &key,
            };
            let mut decoder = Decoder::default();
            for (first, path) in paths {
                ensure!(
                    sequence.checked_add(1) == Some(first),
                    "missing journal segment"
                );
                let data = bytes(store, &path, u64::from(MAX_LIMIT))
                    .await?
                    .context("missing journal segment")?;
                (sequence, tail) = if let Some(rescue) = end
                    .and_then(|p| p.salvaged.as_ref())
                    .filter(|s| s.first == first)
                {
                    let prefix = data
                        .get(..usize::try_from(rescue.bytes)?)
                        .context("missing salvaged journal prefix")?;
                    ensure!(
                        *blake3::hash(prefix).as_bytes() == rescue.digest,
                        "salvaged prefix changed"
                    );
                    super::verify_prefix(&format, &path, prefix, first, tail, true, |f| {
                        decoder.read(&f.bytes, f.sequence.last - f.sequence.first + 1, |r| {
                            catalog.stage(r, &format)
                        })
                    })?
                } else {
                    replay::stage_segment(&format, &path, &data, first, tail, &mut decoder, |r| {
                        catalog.stage(r, &format)
                    })?
                };
                catalog.apply()?;
            }
            let point = EpochCut {
                epoch,
                sequence,
                tail,
                salvaged: end.and_then(|v| v.salvaged.clone()),
            };
            ensure!(end.is_none_or(|v| v == &point), "recovery cut changed");
            last = Some(point);
        }
        Ok((
            catalog,
            Cut {
                point: last.context("unknown checkpoint epoch")?,
                checkpoint,
            },
        ))
    }
}
pub(crate) fn client(c: &model::Connection) -> Result<crate::storage::external::Client> {
    crate::storage::external::Connection {
        endpoint_url: c.endpoint_url.clone().context("missing S3 endpoint")?,
        region: c.region.clone().context("missing S3 region")?,
        bucket: c.remote_bucket_name.clone().context("missing S3 bucket")?,
        prefix: c.prefix.clone(),
        allow_http: true,
        virtual_hosted: false,
    }
    .connect(
        c.access.as_deref().context("missing S3 access key")?,
        c.secret.as_deref().context("missing S3 secret key")?,
        options(),
    )
}
#[cfg(test)]
#[path = "../../../tests/recovery/journal_upload.rs"]
mod tests;
