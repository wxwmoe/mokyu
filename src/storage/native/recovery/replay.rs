use super::scan::Scanner;
use crate::{
    content::codec::Key,
    model::storage_recovery::{CatalogEntry, DecodedFile, Proof, RecoveryFile, verify::Decoders},
    storage::{
        Disk,
        journal::{
            Format, FrameType, ROOT, snapshot,
            source::{Cut, Source},
            stream::{Memory, Sink},
        },
        marker::EpochCut,
    },
};
use anyhow::{Context, Result, ensure};
use std::{collections::BTreeSet, sync::Arc};
use uuid::Uuid;

struct Replay<'a> {
    scanner: &'a Arc<Scanner>,
    catalog: Uuid,
    source: &'a Source,
    disk: &'a Arc<Disk>,
    key: Key,
}

impl Replay<'_> {
    fn format(&self, epoch: Uuid) -> Format<'_> {
        Format {
            storage: self.source.connection.id,
            journal: self.source.journal.journal_id,
            epoch,
            encrypted: true,
            key: &self.key,
        }
    }

    async fn file(
        &self,
        path: &str,
        epoch: Uuid,
        decoder: &mut Decoders,
    ) -> Result<(RecoveryFile, DecodedFile)> {
        let file = self
            .scanner
            .clusters
            .catalog_file(self.catalog, path)
            .await?
            .context("required recovery file is missing")?;
        let summary = self
            .scanner
            .decode(
                self.catalog,
                &file.part,
                file.through,
                Some(&self.format(epoch)),
                decoder,
                self.disk,
            )
            .await?;
        Ok((file, summary))
    }

    async fn rows(
        &self,
        file: &RecoveryFile,
        summary: &DecodedFile,
        mut visit: impl FnMut(CatalogEntry) -> Result<()>,
    ) -> Result<()> {
        let mut after = None;
        loop {
            let page = self
                .scanner
                .clusters
                .catalog_page(self.catalog, 6, Some(file.part.intent.payload), after)
                .await?;
            for row in page.entries {
                let position = row.identity().3;
                if (summary.first..=summary.last).contains(&position) {
                    visit(row)?;
                }
            }
            match page.after {
                Some(next) => after = Some(next),
                None => break,
            }
        }
        Ok(())
    }

    async fn one(
        &self,
        path: &str,
        epoch: Uuid,
        kind: FrameType,
    ) -> Result<(RecoveryFile, DecodedFile, CatalogEntry)> {
        let mut decoder = Decoders::default();
        let (file, summary) = self.file(path, epoch, &mut decoder).await?;
        ensure!(
            summary.kind == kind && summary.first == summary.last && decoder.complete(),
            "invalid recovery envelope"
        );
        let mut row = None;
        self.rows(&file, &summary, |value| {
            ensure!(row.is_none(), "recovery envelope has multiple records");
            row = Some(value);
            Ok(())
        })
        .await?;
        Ok((
            file,
            summary,
            row.context("recovery envelope has no record")?,
        ))
    }

    async fn index(&self, path: &str, epoch: Uuid) -> Result<(u64, snapshot::Index)> {
        let (_, summary, row) = self.one(path, epoch, FrameType::Index).await?;
        let CatalogEntry::SnapshotIndex { index, .. } = row else {
            anyhow::bail!("invalid recovery index record");
        };
        ensure!(
            summary.first == index.revision,
            "recovery index revision differs"
        );
        Ok((summary.first, index))
    }

    async fn checkpoint(&self, epoch: Uuid) -> Result<Option<u64>> {
        let prefix = format!("{ROOT}/checkpoints/{}/", epoch.simple());
        let mut after = Uuid::nil();
        let mut best = None;
        loop {
            let files = self
                .scanner
                .clusters
                .catalog_files(self.catalog, after)
                .await?;
            if files.is_empty() {
                break;
            }
            for file in files {
                after = file.part.intent.operation;
                if let Some(name) = file.part.path.strip_prefix(&prefix) {
                    let revision: u64 = name.parse()?;
                    ensure!(
                        revision > 0
                            && file.part.path == snapshot::checkpoint_path(epoch, revision),
                        "invalid checkpoint path"
                    );
                    best = Some(best.unwrap_or(0).max(revision));
                }
            }
        }
        Ok(best)
    }

    async fn history(&self) -> Result<Vec<EpochCut>> {
        let mut expected = self.source.journal.head()?;
        let mut epoch = self.source.journal.writer_epoch;
        let mut visited = BTreeSet::from([epoch]);
        let mut cuts = Vec::new();
        while let Some(digest) = expected {
            let path = format!("{ROOT}/history/{}", epoch.simple());
            let (file, summary, row) = self.one(&path, epoch, FrameType::History).await?;
            let CatalogEntry::History { record, .. } = row else {
                anyhow::bail!("invalid recovery history record");
            };
            ensure!(
                file.part.digest == digest
                    && record.storage == self.source.connection.id
                    && record.next == epoch
                    && !record.cut.epoch.is_nil()
                    && summary.first == record.cut.sequence
                    && visited.insert(record.cut.epoch),
                "invalid journal takeover chain"
            );
            expected = (summary.previous != [0; 32]).then_some(summary.previous);
            epoch = record.cut.epoch;
            cuts.push(record.cut);
        }
        cuts.reverse();
        Ok(cuts)
    }

    async fn walk(
        &self,
        head: &Proof,
        sink: &mut (impl Sink + Send),
    ) -> Result<(Cut, Vec<EpochCut>)> {
        let mut history = self.history().await?;
        let mut current = None;
        if self
            .scanner
            .clusters
            .catalog_file(self.catalog, &snapshot::index_path())
            .await?
            .is_some()
        {
            for epoch in std::iter::once(self.source.journal.writer_epoch)
                .chain(history.iter().rev().map(|p| p.epoch))
            {
                if let Ok((_, index)) = self.index(&snapshot::index_path(), epoch).await {
                    current = Some((epoch, index));
                    break;
                }
            }
            ensure!(current.is_some(), "cannot authenticate recovery index");
        }
        let mut checkpoint = None;
        let mut checkpoint_revision = None;
        let mut point = None;
        for (epoch, end) in history
            .iter()
            .map(|p| (p.epoch, Some(p)))
            .chain(std::iter::once((self.source.journal.writer_epoch, None)))
        {
            let mut index = current
                .as_ref()
                .filter(|(e, _)| *e == epoch)
                .map(|(_, i)| i.clone());
            if let Some(revision) = self.checkpoint(epoch).await?
                && index.as_ref().is_none_or(|i| i.revision < revision)
            {
                let (_, value) = self
                    .index(&snapshot::checkpoint_path(epoch, revision), epoch)
                    .await?;
                ensure!(
                    value.revision == revision,
                    "checkpoint filename differs from its record"
                );
                index = Some(value);
            }
            let (mut sequence, mut tail) = (0, [0; 32]);
            if let Some(index) = index {
                ensure!(
                    index.format_version == 1
                        && index.bucket_count == index.buckets.len() as u64
                        && end.is_none_or(|p| index.sequence <= p.sequence),
                    "invalid journal checkpoint"
                );
                checkpoint = Some((index.snapshot, index.manifest_digest));
                checkpoint_revision = Some((epoch, index.revision));
                let path = snapshot::manifest_path(index.snapshot);
                let (file, summary, row) = self.one(&path, epoch, FrameType::Manifest).await?;
                let CatalogEntry::SnapshotManifest { manifest, .. } = row else {
                    anyhow::bail!("invalid snapshot manifest record");
                };
                ensure!(
                    file.part.digest == index.manifest_digest
                        && summary.first == index.revision
                        && manifest.format_version == 1
                        && manifest.id == index.snapshot
                        && manifest.sequence == index.sequence,
                    "snapshot manifest differs from its checkpoint"
                );
                sink.scope(&index.buckets, &index.excluded)?;
                let mut decoder = Decoders::default();
                let mut seen = BTreeSet::new();
                for page in manifest.pages {
                    ensure!(
                        seen.insert((page.owner, page.number)),
                        "duplicate snapshot page"
                    );
                    let (file, summary) = self
                        .file(
                            &snapshot::page_path(page.owner, page.number),
                            epoch,
                            &mut decoder,
                        )
                        .await?;
                    ensure!(
                        file.part.digest == page.digest
                            && file.part.intent.total == page.bytes
                            && summary.kind == FrameType::Snapshot
                            && summary.first == 1
                            && summary.last == page.items,
                        "snapshot page differs from its manifest"
                    );
                    self.rows(&file, &summary, |row| match row {
                        CatalogEntry::Snapshot { row, .. } => sink.row(row, &self.format(epoch)),
                        _ => anyhow::bail!("unexpected snapshot page record"),
                    })
                    .await?;
                }
                ensure!(decoder.complete(), "incomplete snapshot record");
                sequence = manifest.sequence;
                tail = manifest.tail;
            }
            let through = end.map_or(head.publication.through, |p| p.sequence);
            let mut decoder = Decoders::default();
            ensure!(sequence <= through, "checkpoint exceeds recovery watermark");
            while sequence < through {
                let first = sequence
                    .checked_add(1)
                    .context("journal sequence exhausted")?;
                let (file, summary) = self
                    .file(
                        &crate::storage::journal::segment_path(epoch, first),
                        epoch,
                        &mut decoder,
                    )
                    .await?;
                ensure!(
                    summary.kind == FrameType::Changes
                        && summary.first == first
                        && summary.last <= through
                        && summary.previous == tail,
                    "recovery journal chain has a gap or changed boundary"
                );
                self.rows(&file, &summary, |row| match row {
                    CatalogEntry::Business { record, .. } => {
                        sink.stage(record, &self.format(epoch))
                    }
                    _ => anyhow::bail!("unexpected journal business record"),
                })
                .await?;
                sink.apply()?;
                sequence = summary.last;
                tail = summary.tail;
            }
            ensure!(decoder.complete(), "incomplete journal record");
            let cut = EpochCut {
                epoch,
                sequence,
                tail,
                salvaged: None,
            };
            ensure!(
                end.is_none_or(|p| *p == cut),
                "recovery epoch boundary changed"
            );
            point = Some(cut);
        }
        sink.finish()?;
        let point = point.context("missing recovery epoch")?;
        history.push(point.clone());
        Ok((
            Cut {
                point,
                checkpoint,
                checkpoint_revision,
            },
            history,
        ))
    }
}

impl Scanner {
    pub async fn replay(
        self: &Arc<Self>,
        catalog: Uuid,
        source: &Source,
        head: &Proof,
        disk: &Arc<Disk>,
        fixed: Option<&Cut>,
    ) -> Result<(crate::storage::journal::replay::Catalog, Cut)> {
        let (rows, cut, _) = self
            .replay_epochs(catalog, source, head, disk, fixed)
            .await?;
        Ok((rows, cut))
    }

    pub async fn recover(
        self: &Arc<Self>,
        catalog: Uuid,
        source: &Source,
        head: &Proof,
        disk: &Arc<Disk>,
        fixed: Option<&Cut>,
    ) -> Result<(crate::storage::journal::replay::Catalog, Cut)> {
        self.map(catalog, source, head, disk, fixed, true).await
    }

    pub async fn map(
        self: &Arc<Self>,
        catalog: Uuid,
        source: &Source,
        head: &Proof,
        disk: &Arc<Disk>,
        fixed: Option<&Cut>,
        install: bool,
    ) -> Result<(crate::storage::journal::replay::Catalog, Cut)> {
        let (rows, cut, epochs) = self
            .replay_epochs(catalog, source, head, disk, fixed)
            .await?;
        if let Some(root) = &head.publication.root {
            self.clusters
                .catalog_map(catalog, root.intent.operation, &epochs, install)
                .await?;
        }
        let mut after_file = Uuid::nil();
        loop {
            let files = self.clusters.catalog_files(catalog, after_file).await?;
            if files.is_empty() {
                break;
            }
            for file in files {
                after_file = file.part.intent.operation;
                if !file.retired && file.decoded.is_some() {
                    self.clusters
                        .catalog_map(catalog, after_file, &epochs, install)
                        .await?;
                }
            }
        }
        let mut after = None;
        loop {
            let page = self.clusters.catalog_page(catalog, 3, None, after).await?;
            for entry in page.entries {
                let CatalogEntry::Physical(
                    _,
                    crate::model::storage_recovery::Physical::Payload { intent, .. },
                ) = entry
                else {
                    anyhow::bail!("invalid recovered payload record");
                };
                self.clusters
                    .catalog_map(catalog, intent.operation, &epochs, install)
                    .await?;
            }
            match page.after {
                Some(next) => after = Some(next),
                None => break,
            }
        }
        Ok((rows, cut))
    }

    async fn replay_epochs(
        self: &Arc<Self>,
        catalog: Uuid,
        source: &Source,
        head: &Proof,
        disk: &Arc<Disk>,
        fixed: Option<&Cut>,
    ) -> Result<(crate::storage::journal::replay::Catalog, Cut, Vec<EpochCut>)> {
        ensure!(
            head.publication.namespace == source.connection.id
                && head.publication.identity.as_ref() == Some(&source.connection.identity_marker),
            "recovery source differs from its proof"
        );
        let replay = Replay {
            scanner: self,
            catalog,
            source,
            disk,
            key: Key::new("aes-256-gcm", source.material)?,
        };
        let mut memory = Memory::default();
        let (cut, epochs) = replay.walk(head, &mut memory).await?;
        ensure!(
            fixed.is_none_or(|p| *p == cut),
            "fixed recovery point changed"
        );
        Ok((memory.catalog, cut, epochs))
    }
}
