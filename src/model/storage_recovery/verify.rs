use super::{CatalogEntry, Part, Physical, Proof, RecoveryCatalog};
use crate::model::{storage_cluster::Manifest, storage_payload::Receipt};
use anyhow::{Context, Result, ensure};
use uuid::Uuid;

#[derive(Default)]
pub(crate) struct Decoders {
    physical: crate::storage::journal::batch::Decoder,
    business: crate::storage::journal::batch::Decoder,
}

impl Decoders {
    pub fn complete(&self) -> bool {
        self.physical.complete() && self.business.complete()
    }
}

// Stage candidates until the complete publication and journal chains have been verified.
pub(crate) fn visit(
    part: &Part,
    bytes: &[u8],
    through: u64,
    format: Option<&crate::storage::journal::Format<'_>>,
    decoder: &mut Decoders,
    mut visit: impl FnMut(CatalogEntry) -> Result<()>,
) -> Result<()> {
    part.visit(bytes, |public| {
        frame(part, public, through, format, decoder, &mut visit)
    })
}

pub(crate) fn frame(
    part: &Part,
    public: crate::storage::journal::recovery::Public<'_>,
    through: u64,
    format: Option<&crate::storage::journal::Format<'_>>,
    decoder: &mut Decoders,
    mut visit: impl FnMut(CatalogEntry) -> Result<()>,
) -> Result<()> {
    use crate::storage::journal::FrameType;
    ensure!(
        public.namespace == part.intent.namespace
            && public.envelope.payload == part.intent.payload
            && public.envelope.revision == part.intent.revision
            && public.path == part.path,
        "recovery frame belongs to another published part"
    );
    let kind = public.envelope.kind;
    let physical = |record: Physical| -> Result<()> {
        let sequence = match &record {
            Physical::Payload { sequence, .. }
            | Physical::Fragment { sequence, .. }
            | Physical::State { sequence, .. } => *sequence,
        };
        ensure!(
            sequence <= through,
            "physical record exceeds its published watermark"
        );
        let entry = CatalogEntry::Physical(public.epoch, record);
        entry.validate(part.intent.namespace, part.intent.cluster)?;
        visit(entry)
    };
    let count = public
        .envelope
        .sequence
        .last
        .checked_sub(public.envelope.sequence.first)
        .and_then(|n| n.checked_add(1))
        .context("recovery record count overflow")?;
    if kind == FrameType::Mappings {
        decoder.physical.read(&public.physical, count, physical)?;
    } else if !public.physical.is_empty() {
        let records: Vec<Physical> = serde_json::from_slice(&public.physical)?;
        ensure!(
            records.len() <= 256,
            "recovery physical batch exceeds its bound"
        );
        let mut physical = physical;
        for record in records {
            physical(record)?;
        }
    }
    if let Some(format) = format {
        let frame = public.open(format)?;
        if matches!(kind, FrameType::Mappings | FrameType::Seal) {
            ensure!(
                frame.bytes.is_empty(),
                "recovery boundary contains business data"
            );
            return Ok(());
        }
        let mut position = frame.sequence.first;
        let payload = part.intent.payload;
        match kind {
            FrameType::Changes => decoder.business.read(&frame.bytes, count, |record| {
                visit(CatalogEntry::Business {
                    payload,
                    position,
                    record,
                })?;
                position = position
                    .checked_add(1)
                    .context("recovery record position overflow")?;
                Ok(())
            })?,
            FrameType::Snapshot => decoder.business.read(&frame.bytes, count, |row| {
                visit(CatalogEntry::Snapshot {
                    payload,
                    position,
                    row,
                })?;
                position = position
                    .checked_add(1)
                    .context("recovery record position overflow")?;
                Ok(())
            })?,
            FrameType::Manifest | FrameType::Index => {
                ensure!(
                    count == 1 && frame.sequence.previous == [0; 32] && decoder.business.complete(),
                    "invalid recovery snapshot envelope"
                );
                visit(if kind == FrameType::Manifest {
                    CatalogEntry::SnapshotManifest {
                        payload,
                        position,
                        manifest: serde_json::from_slice(&frame.bytes)?,
                    }
                } else {
                    CatalogEntry::SnapshotIndex {
                        payload,
                        position,
                        index: serde_json::from_slice(&frame.bytes)?,
                    }
                })?;
            }
            FrameType::History => {
                ensure!(
                    count == 1 && decoder.business.complete(),
                    "invalid recovery history envelope"
                );
                visit(CatalogEntry::History {
                    payload,
                    position,
                    record: serde_json::from_slice(&frame.bytes)?,
                })?;
            }
            _ => anyhow::bail!("unexpected recovery record type"),
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn records(
    part: &Part,
    bytes: &[u8],
    through: u64,
    format: Option<&crate::storage::journal::Format<'_>>,
    decoder: &mut Decoders,
) -> Result<Vec<CatalogEntry>> {
    let mut entries = Vec::new();
    visit(part, bytes, through, format, decoder, |entry| {
        entries.push(entry);
        Ok(())
    })?;
    Ok(entries)
}
async fn history(
    catalog: &dyn RecoveryCatalog,
    id: Uuid,
    revision: i64,
    digest: &str,
) -> Result<Manifest> {
    let mut after = None;
    loop {
        let page = catalog.catalog_page(id, 0, None, after).await?;
        for entry in page.entries {
            if let CatalogEntry::Manifest(value) = entry {
                if value.revision == revision && value.digest()? == digest {
                    return Ok(value);
                }
            }
        }
        match page.after {
            Some(next) => after = Some(next),
            None => anyhow::bail!("historical recovery layout is missing"),
        }
    }
}

pub(crate) async fn layout(
    catalog: &dyn RecoveryCatalog,
    id: Uuid,
    anchor: &Manifest,
    revision: u64,
) -> Result<Manifest> {
    anchor.validate()?;
    ensure!(
        revision > 0 && revision <= anchor.revision as u64,
        "recovery layout is outside its trusted history"
    );
    let mut current = anchor.clone();
    while current.revision as u64 > revision {
        let previous = current
            .previous
            .as_deref()
            .context("recovery layout predecessor is missing")?;
        let old = history(catalog, id, current.revision - 1, previous).await?;
        current.follows(&old)?;
        current = old;
    }
    Ok(current)
}

pub(crate) async fn chain(
    catalog: &dyn RecoveryCatalog,
    id: Uuid,
    anchor: &Manifest,
) -> Result<Proof> {
    let mut after = None;
    let mut head: Option<Proof> = None;
    let mut count = 0u64;
    loop {
        let page = catalog.catalog_page(id, 1, None, after).await?;
        for entry in page.entries {
            let CatalogEntry::Proof(proof) = entry else {
                anyhow::bail!("invalid proof catalog category");
            };
            count = count
                .checked_add(1)
                .context("recovery publication count overflow")?;
            if head
                .as_ref()
                .is_none_or(|old| old.publication.revision < proof.publication.revision)
            {
                head = Some(proof);
            }
        }
        match page.after {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    let head = head.context("no recovery publication was found")?;
    ensure!(
        head.publication.revision == count,
        "recovery history contains a gap or a competing branch"
    );
    walk(catalog, id, anchor, &head, &[]).await?;
    Ok(head)
}

pub(crate) async fn rooted_chain(
    catalog: &dyn RecoveryCatalog,
    id: Uuid,
    anchor: &Manifest,
    heads: &[super::Link],
) -> Result<Proof> {
    ensure!(
        !heads.is_empty() && heads.len() <= 256,
        "no committed recovery head was found"
    );
    let mut after = None;
    let mut head: Option<Proof> = None;
    loop {
        let page = catalog.catalog_page(id, 1, None, after).await?;
        for entry in page.entries {
            let CatalogEntry::Proof(proof) = entry else {
                anyhow::bail!("invalid proof catalog category");
            };
            if heads.contains(&proof.link()?)
                && head
                    .as_ref()
                    .is_none_or(|old| old.publication.revision < proof.publication.revision)
            {
                head = Some(proof);
            }
        }
        match page.after {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    let head = head.context("committed recovery proof is missing")?;
    walk(catalog, id, anchor, &head, heads).await?;
    Ok(head)
}

async fn walk(
    catalog: &dyn RecoveryCatalog,
    id: Uuid,
    anchor: &Manifest,
    head: &Proof,
    heads: &[super::Link],
) -> Result<()> {
    let mut pending = heads.to_vec();
    let mut current = head.clone();
    loop {
        let link = current.link()?;
        pending.retain(|head| head != &link);
        let mut trusted = anchor.clone();
        while trusted.digest()? != current.publication.manifest {
            ensure!(
                trusted.revision > 1,
                "recovery publication uses an unknown layout"
            );
            trusted = layout(catalog, id, &trusted, trusted.revision as u64 - 1).await?;
        }
        current.verify(&trusted)?;
        if let Some(root) = &current.publication.root {
            let origin = layout(catalog, id, &trusted, root.intent.layout_revision).await?;
            root.validate_layout(&origin)?;
            for r in &root.receipts {
                if r.header.layout_revision != root.intent.layout_revision {
                    let placed = layout(catalog, id, &trusted, r.header.layout_revision).await?;
                    root.intent.placement(&r.header, &origin, &placed)?;
                }
            }
        }
        for part in &current.publication.parts {
            part.validate_layout(
                &layout(catalog, id, &trusted, part.intent.layout_revision).await?,
            )?;
        }
        if let Some(link) = &current.publication.previous {
            let entry = catalog
                .catalog_entry(id, 1, link.id, current.publication.revision - 1)
                .await?
                .context("recovery publication predecessor is missing")?;
            let CatalogEntry::Proof(previous) = entry else {
                anyhow::bail!("invalid predecessor category");
            };
            current.follows(Some(&previous))?;
            current = previous;
        } else {
            current.follows(None)?;
            break;
        }
    }
    ensure!(
        pending.is_empty(),
        "recovery heads contain a missing or competing branch"
    );
    Ok(())
}

// These are scan candidates; the caller still verifies the selected bytes against Part.digest.
pub(crate) async fn fragments(
    catalog: &dyn RecoveryCatalog,
    id: Uuid,
    part: &Part,
    manifest: &Manifest,
) -> Result<Vec<Receipt>> {
    part.validate(part.intent.namespace)?;
    let origin = if manifest.revision as u64 == part.intent.layout_revision {
        manifest.clone()
    } else {
        layout(catalog, id, manifest, part.intent.layout_revision).await?
    };
    part.validate_layout(&origin)?;
    let group = origin
        .groups
        .iter()
        .find(|g| g.id == part.intent.group)
        .context("recovery group is missing")?;
    let width = usize::from(group.layout.data_shards + group.layout.parity_shards);
    let mut placements =
        std::collections::BTreeMap::from([(part.intent.layout_revision, origin.clone())]);
    let mut slots = vec![None; group.layout.width()];
    let mut after = None;
    loop {
        let page = catalog
            .catalog_page(id, 2, Some(part.intent.operation), after)
            .await?;
        for entry in page.entries {
            let CatalogEntry::Fragment(receipt) = entry else {
                anyhow::bail!("invalid fragment catalog category");
            };
            let header = &receipt.header;
            part.intent.accepts(header)?;
            ensure!(
                header.sequence == 0,
                "recovery part exceeds one file sequence"
            );
            let slot = usize::from(header.copy) * width + usize::from(header.shard);
            if let std::collections::btree_map::Entry::Vacant(entry) =
                placements.entry(header.layout_revision)
            {
                entry.insert(layout(catalog, id, manifest, header.layout_revision).await?);
            }
            part.intent
                .placement(header, &origin, &placements[&header.layout_revision])?;
            if slots[slot]
                .as_ref()
                .map(|old| receipt.newer_than(old))
                .transpose()?
                .unwrap_or(true)
            {
                slots[slot] = Some(receipt);
            }
        }
        match page.after {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    let independent = (0..width)
        .filter(|&shard| (0..slots.len() / width).any(|copy| slots[copy * width + shard].is_some()))
        .count();
    ensure!(
        independent >= usize::from(group.layout.data_shards),
        "recovery part has too few independent fragments"
    );
    Ok(slots.into_iter().flatten().collect())
}
