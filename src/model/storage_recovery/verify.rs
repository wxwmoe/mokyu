use super::{CatalogEntry, Part, Physical, Proof, RecoveryCatalog};
use crate::model::{storage_cluster::Manifest, storage_payload::Receipt};
use anyhow::{Context, Result, ensure};
use uuid::Uuid;

pub(crate) fn records(
    part: &Part,
    bytes: &[u8],
    through: u64,
    format: Option<&crate::storage::journal::Format<'_>>,
    decoder: &mut crate::storage::journal::batch::Decoder,
) -> Result<Vec<CatalogEntry>> {
    let public = part.inspect(bytes)?;
    let physical: Vec<Physical> = serde_json::from_slice(&public.physical)?;
    ensure!(
        physical.len() <= 256,
        "recovery physical batch exceeds its bound"
    );
    let mut entries = Vec::with_capacity(physical.len());
    for record in physical {
        let sequence = match &record {
            Physical::Payload { sequence, .. }
            | Physical::Fragment { sequence, .. }
            | Physical::State { sequence, .. } => *sequence,
        };
        ensure!(
            sequence <= through,
            "physical record exceeds its published watermark"
        );
        let entry = CatalogEntry::Physical(record);
        entry.validate(part.intent.namespace, part.intent.cluster)?;
        entries.push(entry);
    }
    if let Some(format) = format {
        ensure!(
            matches!(
                public.envelope.kind,
                crate::storage::journal::FrameType::Changes
                    | crate::storage::journal::FrameType::Snapshot
            ),
            "recovery payload is not a record batch"
        );
        let frame = public.open(format)?;
        let mut position = frame.sequence.first;
        let count = frame
            .sequence
            .last
            .checked_sub(position)
            .and_then(|n| n.checked_add(1))
            .context("recovery record count overflow")?;
        decoder.read(&frame.bytes, count, |record| {
            entries.push(CatalogEntry::Business {
                payload: part.intent.payload,
                position,
                record,
            });
            position = position
                .checked_add(1)
                .context("recovery record position overflow")?;
            Ok(())
        })?;
    }
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
        ensure!(
            old.cluster_id == anchor.cluster_id && old.takeover_epoch <= current.takeover_epoch,
            "recovery layout chain changed identity"
        );
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
    let mut current = head.clone();
    loop {
        let mut trusted = anchor.clone();
        while trusted.digest()? != current.publication.manifest {
            ensure!(
                trusted.revision > 1,
                "recovery publication uses an unknown layout"
            );
            trusted = layout(catalog, id, &trusted, trusted.revision as u64 - 1).await?;
        }
        current.verify(&trusted)?;
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
    Ok(head)
}

// These are scan candidates; the caller still verifies the selected bytes against Part.digest.
pub(crate) async fn fragments(
    catalog: &dyn RecoveryCatalog,
    id: Uuid,
    part: &Part,
    manifest: &Manifest,
) -> Result<Vec<Receipt>> {
    part.validate(part.intent.namespace)?;
    part.validate_layout(manifest)?;
    let group = manifest
        .groups
        .iter()
        .find(|g| g.id == part.intent.group)
        .context("recovery group is missing")?;
    let width = usize::from(group.layout.data_shards + group.layout.parity_shards);
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
            ensure!(
                group.members.get(slot) == Some(&header.media),
                "recovery fragment is on another historical member"
            );
            if let Some(old) = &slots[slot] {
                ensure!(
                    old == &receipt,
                    "competing fragments occupy one recovery slot"
                );
            } else {
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
