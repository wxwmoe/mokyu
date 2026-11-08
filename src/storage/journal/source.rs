use super::{
    Format, FrameType, ROOT,
    batch::Decoder,
    replay::Catalog,
    segment_path, snapshot,
    stream::{self, Sink},
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
use std::{sync::Arc, time::Duration};
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
    #[serde(default)]
    pub checkpoint_revision: Option<(Uuid, u64)>,
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
            checkpoint_revision: old.checkpoint_revision,
        })
    }

    pub async fn read(
        &self,
        store: &dyn ObjectStore,
        fixed: Option<&Cut>,
    ) -> Result<(Catalog, Cut)> {
        self.read_selected(store, fixed, None).await
    }
    pub async fn read_selected(
        &self,
        store: &dyn ObjectStore,
        fixed: Option<&Cut>,
        buckets: Option<&std::collections::BTreeSet<Uuid>>,
    ) -> Result<(Catalog, Cut)> {
        let local = super::local::Local::new(&self.connection)?;
        let _pin = match &local {
            Some(local) => Some(local.lock(false).await?),
            None => None,
        };
        self.read_locked(store, fixed, buckets, local.as_ref())
            .await
    }

    pub(crate) async fn read_locked(
        &self,
        store: &dyn ObjectStore,
        fixed: Option<&Cut>,
        buckets: Option<&std::collections::BTreeSet<Uuid>>,
        local: Option<&super::local::Local>,
    ) -> Result<(Catalog, Cut)> {
        let mut memory = stream::Memory::default();
        memory.catalog.select(buckets);
        let cut = self.walk(store, fixed, buckets, local, &mut memory).await?;
        Ok((memory.catalog, cut))
    }

    pub(crate) async fn walk(
        &self,
        store: &dyn ObjectStore,
        fixed: Option<&Cut>,
        buckets: Option<&std::collections::BTreeSet<Uuid>>,
        local: Option<&super::local::Local>,
        catalog: &mut (impl Sink + Send),
    ) -> Result<Cut> {
        let key = Key::new("aes-256-gcm", self.material)?;
        let history = super::history::chain(store, &self.journal, self.connection.id, &key).await?;
        let mut checkpoint = None;
        let mut checkpoint_revision = None;
        let index_path = snapshot::index_path();
        let limit = (super::FRAME_LIMIT + Format::overhead(false)) as u64;
        let mut current = None;
        if fixed.is_none()
            && let Some(data) = bytes(store, &index_path, limit).await?
        {
            for epoch in std::iter::once(self.journal.writer_epoch)
                .chain(history.iter().rev().map(|p| p.epoch))
            {
                let f = Format {
                    storage: self.connection.id,
                    encrypted: true,
                    journal: self.journal.journal_id,
                    epoch,
                    key: &key,
                };
                if let Ok((revision, index)) = snapshot::open_envelope::<snapshot::Index>(
                    &f,
                    FrameType::Index,
                    &index_path,
                    &data,
                ) {
                    ensure!(revision == index.revision, "invalid checkpoint revision");
                    current = Some((epoch, index));
                    break;
                }
            }
            ensure!(current.is_some(), "cannot authenticate recovery index");
        }
        let mut last = None;
        for (epoch, end) in history
            .iter()
            .map(|p| (p.epoch, Some(p)))
            .chain(std::iter::once((
                self.journal.writer_epoch,
                fixed.map(|c| &c.point),
            )))
        {
            let format = Format {
                storage: self.connection.id,
                encrypted: true,
                journal: self.journal.journal_id,
                epoch,
                key: &key,
            };
            let mut root = current
                .as_ref()
                .filter(|(e, _)| *e == epoch)
                .map(|(_, i)| i.clone());
            let pinned = fixed
                .and_then(|c| c.checkpoint_revision)
                .filter(|(e, _)| *e == epoch)
                .map(|(_, r)| r);
            let mut best = pinned;
            if pinned.is_none() && (fixed.is_none() || epoch != self.journal.writer_epoch) {
                let prefix = Path::from(format!("{ROOT}/checkpoints/{}", epoch.simple()));
                let mut anchors = store.list(Some(&prefix));
                while let Some(entry) = anchors.try_next().await? {
                    let path = entry.location.as_ref();
                    let revision = path
                        .rsplit('/')
                        .next()
                        .context("unexpected checkpoint file")?
                        .parse::<u64>()?;
                    ensure!(
                        revision > 0 && path == snapshot::checkpoint_path(epoch, revision),
                        "invalid checkpoint filename"
                    );
                    best = Some(best.unwrap_or(0).max(revision));
                }
            }
            if let Some(revision) = best
                && root.as_ref().is_none_or(|r| r.revision < revision)
            {
                let path = snapshot::checkpoint_path(epoch, revision);
                let data = match bytes(store, &path, limit).await? {
                    Some(data) => data,
                    None if pinned == Some(revision) => bytes(store, &index_path, limit)
                        .await?
                        .context("missing pinned recovery index")?,
                    None => anyhow::bail!("missing confirmed checkpoint"),
                };
                let (r, index) = snapshot::open_envelope::<snapshot::Index>(
                    &format,
                    FrameType::Index,
                    &index_path,
                    &data,
                )?;
                ensure!(
                    r == revision && index.revision == revision,
                    "checkpoint filename and record differ"
                );
                root = Some(index);
            }
            let (mut sequence, mut tail) = (0, [0; 32]);
            if let Some(index) = root {
                ensure!(
                    index.format_version == 1
                        && index.bucket_count == index.buckets.len() as u64
                        && end.is_none_or(|e| index.sequence <= e.sequence),
                    "invalid journal checkpoint"
                );
                checkpoint = Some((index.snapshot, index.manifest_digest));
                checkpoint_revision = Some((epoch, index.revision));
                let path = snapshot::manifest_path(index.snapshot);
                let manifest = bytes(store, &path, limit)
                    .await?
                    .context("missing snapshot manifest")?;
                ensure!(
                    *blake3::hash(&manifest).as_bytes() == index.manifest_digest,
                    "snapshot manifest digest mismatch"
                );
                let (revision, manifest) = snapshot::open_envelope::<snapshot::Manifest>(
                    &format,
                    FrameType::Manifest,
                    &path,
                    &manifest,
                )?;
                ensure!(
                    revision == index.revision
                        && manifest.format_version == 1
                        && manifest.id == index.snapshot
                        && manifest.sequence == index.sequence,
                    "invalid snapshot manifest"
                );
                catalog.scope(&index.buckets, &index.excluded)?;
                let mut decoder = Decoder::default();
                let mut seen = std::collections::HashSet::new();
                for page in manifest.pages {
                    ensure!(
                        seen.insert((page.owner, page.number))
                            && page.bytes <= u64::from(MAX_LIMIT),
                        "invalid snapshot page reference"
                    );
                    if let Some(selected) = buckets
                        && page.first_bucket == page.last_bucket
                        && page.first_bucket.is_some_and(|id| !selected.contains(&id))
                    {
                        ensure!(
                            decoder.complete(),
                            "snapshot record crosses bucket boundary"
                        );
                        continue;
                    }
                    stream::page(store, &format, &page, &mut decoder, catalog).await?;
                }
                ensure!(decoder.complete(), "incomplete snapshot record");
                sequence = manifest.sequence;
                tail = manifest.tail;
            }
            let prefix = Path::from(format!("{ROOT}/changes/{}", epoch.simple()));
            let head = local.map(|l| l.head(&format)).transpose()?.flatten();
            let mut listed = store.list(Some(&prefix));
            while let Some(meta) = listed.try_next().await? {
                let name = meta.location.to_string();
                let active = local.is_some() && name.ends_with(".part");
                let logical = if active {
                    name.strip_suffix(".part").unwrap()
                } else {
                    &name
                };
                let first = logical
                    .rsplit('/')
                    .next()
                    .context("unexpected journal file")?
                    .parse::<u64>()?;
                ensure!(
                    first > 0 && logical == segment_path(epoch, first),
                    "invalid journal segment path"
                );
                if active && head.is_none_or(|h| h.first != first) {
                    continue;
                }
                if first > sequence && end.is_none_or(|p| first <= p.sequence) {
                    catalog.list(first, name)?;
                }
            }
            let mut decoder = Decoder::default();
            let mut salvaged = end.and_then(|p| p.salvaged.clone());
            while let Some((first, physical)) = catalog.pop()? {
                let path = segment_path(epoch, first);
                ensure!(
                    sequence.checked_add(1) == Some(first),
                    "missing journal segment"
                );
                let pinned = salvaged.as_ref().filter(|s| s.first == first);
                let open = head.filter(|h| h.first == first && !h.sealed);
                let prefix_len = pinned.map(|v| v.bytes).or_else(|| open.map(|h| h.bytes));
                let segment = stream::Segment {
                    physical: &physical,
                    first,
                    previous: tail,
                    prefix: prefix_len,
                };
                let (last, digest, bytes_hash) = segment
                    .read(store, &format, &mut decoder, catalog)
                    .await
                    .with_context(|| format!("cannot read {path}"))?;
                if let Some(size) = prefix_len {
                    ensure!(
                        pinned.is_none_or(|p| p.digest == bytes_hash),
                        "salvaged prefix changed"
                    );
                    if pinned.is_none() {
                        ensure!(
                            open.is_some_and(|h| h.last == last && h.tail == digest),
                            "local journal frontier disagrees"
                        );
                        salvaged = Some(marker::Salvaged {
                            first,
                            bytes: size,
                            digest: bytes_hash,
                        });
                    }
                }
                sequence = last;
                tail = digest;
                catalog.apply()?;
            }
            if end.is_none()
                && let Some(h) = head
            {
                ensure!(
                    sequence == h.last && tail == h.tail,
                    "missing confirmed local journal prefix"
                );
            }
            let point = EpochCut {
                epoch,
                sequence,
                tail,
                salvaged,
            };
            ensure!(end.is_none_or(|v| v == &point), "recovery cut changed");
            last = Some(point);
        }
        if let Some(fixed) = fixed {
            ensure!(
                fixed.checkpoint == checkpoint,
                "fixed recovery checkpoint changed"
            );
        }
        catalog.finish()?;
        Ok(Cut {
            point: last.context("unknown checkpoint epoch")?,
            checkpoint,
            checkpoint_revision,
        })
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
