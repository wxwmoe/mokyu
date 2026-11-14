use super::{Backend, queries::storage_recovery as q};
use crate::model::{Error, storage_cluster::Manifest, storage_recovery::*};
use anyhow::{Context, Result, ensure};
use uuid::Uuid;

const ENTRY_LIMIT: usize = 8 * 1024 * 1024;

async fn indexed(tx: &mut q::Tx<'_>, id: Uuid) -> Result<()> {
    let (head, indexed) = q::root(id).fetch_one(&mut **tx).await?;
    ensure!(
        head.is_some_and(|p| p.publication.revision == indexed as u64),
        "recovery file index is incomplete"
    );
    Ok(())
}

fn recovery_file(row: q::FileRow, namespace: Uuid) -> Result<RecoveryFile> {
    let (part, through, revision, retired, decoded) = row;
    part.validate(namespace)?;
    ensure!(
        through >= 0 && revision > 0,
        "invalid recovery file position"
    );
    Ok(RecoveryFile {
        part: part.0,
        through: through as u64,
        revision: revision as u64,
        retired,
        decoded: decoded.map(|value| value.0),
    })
}

fn encoded(entries: &[CatalogEntry]) -> Result<Vec<([u8; 32], usize)>> {
    ensure!(
        entries.len() <= 256,
        "recovery batch exceeds its entry limit"
    );
    let mut total = 0usize;
    entries
        .iter()
        .map(|entry| {
            let bytes = serde_json::to_vec(entry)?;
            ensure!(
                bytes.len() <= ENTRY_LIMIT,
                "recovery entry exceeds its byte limit"
            );
            total = total
                .checked_add(bytes.len())
                .context("recovery batch size overflow")?;
            ensure!(
                total <= 2 * ENTRY_LIMIT,
                "recovery batch exceeds its byte limit"
            );
            Ok((*blake3::hash(&bytes).as_bytes(), bytes.len()))
        })
        .collect()
}

async fn catalog(tx: &mut q::Tx<'_>, id: Uuid) -> Result<(Uuid, Manifest)> {
    let (namespace, manifest) = q::catalog(id)
        .fetch_optional(&mut **tx)
        .await?
        .context("recovery catalog is missing")?;
    manifest.validate()?;
    Ok((namespace, manifest.0))
}

async fn insert(
    tx: &mut q::Tx<'_>,
    id: Uuid,
    entries: &[CatalogEntry],
    encoded: &[([u8; 32], usize)],
) -> Result<()> {
    let mut pending = std::collections::BTreeMap::new();
    for (entry, &(hash, bytes)) in entries.iter().zip(encoded) {
        let (category, _, key, position) = entry.identity();
        if let Some((_, old, _)) = pending.insert((category, key, position), (entry, hash, bytes)) {
            ensure!(old == hash, "conflicting recovery catalog entry");
        }
    }
    if pending.is_empty() {
        return Ok(());
    }
    let keys: Vec<_> = pending.keys().copied().collect();
    for (category, key, position, hash) in q::hashes(tx, id, &keys).await? {
        let (_, expected, _) = pending
            .remove(&(category, key, position as u64))
            .context("unexpected recovery catalog entry")?;
        ensure!(hash == expected, "conflicting recovery catalog entry");
    }
    let pending: Vec<_> = pending.into_values().collect();
    let mut start = 0;
    let mut bytes = 0;
    for (index, entry) in pending.iter().enumerate() {
        // Keep combined JSON below the supported servers' packet limits.
        if index > start && bytes + entry.2 > ENTRY_LIMIT {
            q::insert(tx, id, &pending[start..index]).await?;
            start = index;
            bytes = 0;
        }
        bytes += entry.2;
    }
    if start < pending.len() {
        q::insert(tx, id, &pending[start..]).await?;
    }
    Ok(())
}

async fn page_in(
    tx: &mut q::Tx<'_>,
    id: Uuid,
    namespace: Uuid,
    cluster: Uuid,
    category: i16,
    owner: Option<Uuid>,
    after: Option<(Uuid, u64)>,
) -> Result<CatalogPage> {
    let keys = q::page(
        id,
        category,
        owner,
        after.map_or((Uuid::nil(), -1), |(id, position)| (id, position as i64)),
    )
    .fetch_all(&mut **tx)
    .await?;
    let mut total = 0usize;
    let mut entries = Vec::new();
    let mut after = None;
    for (key, position, size) in keys {
        ensure!(
            (1..=ENTRY_LIMIT as i32).contains(&size),
            "invalid recovery entry size"
        );
        if total + size as usize > ENTRY_LIMIT {
            break;
        }
        total += size as usize;
        let (hash, entry) = q::entry(id, category, key, position as u64)
            .fetch_one(&mut **tx)
            .await?;
        let bytes = serde_json::to_vec(&entry.0)?;
        ensure!(
            bytes.len() == size as usize && hash == blake3::hash(&bytes).as_bytes(),
            "persisted recovery entry is damaged"
        );
        entry.validate(namespace, cluster)?;
        entries.push(entry.0);
        after = Some((key, position as u64));
    }
    Ok(CatalogPage { entries, after })
}

async fn restore_receipt(
    tx: &mut q::Tx<'_>,
    intent: &crate::model::storage_payload::Intent,
    members: &[Uuid],
    receipt: &crate::model::storage_payload::Receipt,
) -> Result<()> {
    use super::queries::storage_payload as physical;
    let operation = intent.operation;
    let h = &receipt.header;
    intent.accepts(h)?;
    ensure!(
        members.get(usize::from(h.copy)) == Some(&h.media),
        "recovered fragment differs from its historical member"
    );
    if let Some(old) = physical::slot(h).fetch_optional(&mut **tx).await? {
        let old = super::storage_payload::mapping(old)?
            .receipt
            .context("recovered slot has no receipt")?;
        if receipt.newer_than(&old)? {
            ensure!(
                super::queries::storage_repair::replace(
                    old.header.fragment,
                    receipt,
                    h.encode()?.to_vec()
                )
                .execute(&mut **tx)
                .await?
                .rows_affected()
                    == 1,
                "recovered slot changed"
            );
        }
    } else {
        physical::stage(h, h.encode()?.to_vec(), intent.required(h.copy, h.shard))
            .execute(&mut **tx)
            .await?;
        physical::receipt(receipt, h.encode()?.to_vec())
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

#[async_trait::async_trait]
impl RecoveryCatalog for Backend {
    async fn catalog_restore(
        &self,
        id: Uuid,
        operation: Uuid,
        epochs: &[crate::storage::marker::EpochCut],
    ) -> Result<()> {
        use super::queries::{storage_native as objects, storage_payload as physical};
        use crate::content::fragment::Area;
        let mut order = std::collections::BTreeMap::new();
        for (index, cut) in epochs.iter().enumerate() {
            ensure!(
                !cut.epoch.is_nil() && order.insert(cut.epoch, (index, cut.sequence)).is_none(),
                "invalid recovery epoch order"
            );
        }
        let last = epochs.last().context("missing recovery epochs")?;
        let mut tx = q::begin(self).await?;
        let (namespace, anchor) = catalog(&mut tx, id).await?;
        indexed(&mut tx, id).await?;
        let (head, _) = q::root(id).fetch_one(&mut *tx).await?;
        let head = head.context("missing recovery head")?;
        let descriptor = crate::storage::marker::journal(
            head.publication
                .identity
                .as_ref()
                .context("missing recovered identity")?,
        )?
        .context("missing recovered journal")?;
        ensure!(
            descriptor.writer_epoch == last.epoch && head.publication.through == last.sequence,
            "physical recovery exceeds its verified epoch boundary"
        );
        ensure!(
            physical::current_manifest(anchor.cluster_id)
                .fetch_one(&mut *tx)
                .await?
                .0
                == anchor,
            "physical recovery membership changed"
        );
        let rank = |epoch, sequence| -> Result<(usize, u64)> {
            let (index, through) = order
                .get(&epoch)
                .context("physical record belongs to an unverified epoch")?;
            ensure!(
                sequence <= *through,
                "physical record exceeds its recovery cut"
            );
            Ok((*index, sequence))
        };
        let root = head
            .publication
            .root
            .as_ref()
            .filter(|root| root.intent.operation == operation);
        let position = (epochs.len() - 1, last.sequence);
        let mut payload = root.map(|root| {
            (
                position,
                root.intent.clone(),
                if head.publication.prefix.is_empty() {
                    "mokyu.json".to_owned()
                } else {
                    format!("{}/mokyu.json", head.publication.prefix)
                },
            )
        });
        let mut state = root.map(|_| (position, true));
        let file = q::file(id, operation)
            .fetch_optional(&mut *tx)
            .await?
            .map(|row| recovery_file(row, namespace))
            .transpose()?;
        if let Some(file) = &file {
            ensure!(!file.retired, "cannot restore a retired recovery file");
            let decoded = file
                .decoded
                .as_ref()
                .context("recovery file was not authenticated")?;
            let position = rank(decoded.epoch, file.through)?;
            payload = Some((
                position,
                file.part.intent.clone(),
                if head.publication.prefix.is_empty() {
                    file.part.path.clone()
                } else {
                    format!("{}/{}", head.publication.prefix, file.part.path)
                },
            ));
            state = Some((position, true));
        }
        for category in [3, 5] {
            let mut after = None;
            loop {
                let page = page_in(
                    &mut tx,
                    id,
                    namespace,
                    anchor.cluster_id,
                    category,
                    Some(operation),
                    after,
                )
                .await?;
                for entry in page.entries {
                    match entry {
                        CatalogEntry::Physical(
                            epoch,
                            Physical::Payload {
                                sequence,
                                intent,
                                path,
                            },
                        ) => {
                            let position = rank(epoch, sequence)?;
                            if payload.as_ref().is_none_or(|(old, _, _)| position > *old) {
                                payload = Some((position, intent, path));
                            }
                        }
                        CatalogEntry::Physical(
                            epoch,
                            Physical::State {
                                sequence,
                                published,
                                ..
                            },
                        ) => {
                            let position = rank(epoch, sequence)?;
                            if state.as_ref().is_none_or(|(old, _)| position > *old) {
                                state = Some((position, published));
                            }
                        }
                        _ => anyhow::bail!("invalid physical recovery category"),
                    }
                }
                match page.after {
                    Some(next) => after = Some(next),
                    None => break,
                }
            }
        }
        let (_, intent, path) = payload.context("physical payload record missing")?;
        let (_, published) = state.context("physical publication record missing")?;
        ensure!(
            intent.operation == operation
                && intent.area
                    == if file.is_some() {
                        Area::Recovery
                    } else {
                        Area::Blocks
                    }
                && intent.layout.data_shards == 1
                && intent.layout.parity_shards == 0,
            "unsupported physical recovery layout"
        );
        let historical = physical::manifest(anchor.cluster_id, intent.layout_revision)
            .fetch_one(&mut *tx)
            .await?;
        ensure!(
            historical.takeover_epoch == intent.takeover_epoch,
            "physical recovery ownership differs"
        );
        let group = historical
            .groups
            .iter()
            .find(|g| g.id == intent.group)
            .context("physical recovery group missing")?;
        let members = intent.members(group)?;
        if let Some(root) = root {
            root.validate(namespace, anchor.cluster_id)?;
            root.validate_layout(&historical)?;
        }
        if let Some(file) = &file {
            file.part.validate_layout(&historical)?;
        }
        let hash = crate::model::storage_native::path(&path)?.to_vec();
        objects::insert(namespace, intent.payload, hash.clone(), &path)
            .execute(&mut *tx)
            .await?;
        let previous = {
            let old = objects::object(namespace, hash).fetch_one(&mut *tx).await?;
            ensure!(
                old.0 == intent.payload && old.1 == path && old.3.is_none(),
                "physical recovery conflicts with a live write"
            );
            if old.4 >= intent.revision as i64 {
                ensure!(
                    old.4 > intent.revision as i64 || old.2 == published.then_some(operation),
                    "physical recovery revision conflicts at {path} for {operation} (current {:?}, revision {})",
                    old.2,
                    old.4
                );
                if old.4 == intent.revision as i64 {
                    ensure!(
                        super::storage_payload::current(&mut tx, operation)
                            .await?
                            .intent
                            == intent,
                        "physical recovery intent conflicts"
                    );
                }
                tx.commit().await?;
                return Ok(());
            }
            old.2
        };
        ensure!(
            objects::unregistered(namespace).fetch_one(&mut *tx).await?,
            "cannot restore over a registered backend"
        );
        physical::insert(&intent).execute(&mut *tx).await?;
        let saved = super::storage_payload::current(&mut tx, operation).await?;
        ensure!(
            saved.intent == intent && saved.state == "prepared",
            "physical recovery operation conflicts"
        );
        if published {
            if let Some(root) = root {
                for receipt in &root.receipts {
                    restore_receipt(&mut tx, &intent, members, receipt).await?;
                }
            }
            let mut after = None;
            loop {
                let page = page_in(
                    &mut tx,
                    id,
                    namespace,
                    anchor.cluster_id,
                    if file.is_some() { 2 } else { 4 },
                    Some(operation),
                    after,
                )
                .await?;
                for entry in page.entries {
                    let receipt = match entry {
                        CatalogEntry::Physical(epoch, Physical::Fragment { sequence, receipt })
                            if file.is_none() =>
                        {
                            rank(epoch, sequence)?;
                            receipt
                        }
                        CatalogEntry::Fragment(receipt) if file.is_some() => receipt,
                        _ => anyhow::bail!("invalid recovered fragment record"),
                    };
                    restore_receipt(&mut tx, &intent, members, &receipt).await?;
                }
                match page.after {
                    Some(next) => after = Some(next),
                    None => break,
                }
            }
            ensure!(
                q::covered(operation).fetch_one(&mut *tx).await?.0 as u64 == intent.sequences(),
                "recovered payload has missing file sequences"
            );
            physical::publish(operation).execute(&mut *tx).await?;
            // Keep the recovery job free to conditionally replace the restored root.
            objects::bind(
                operation,
                operation,
                &crate::model::storage_native::Condition::Create,
                None,
            )
            .execute(&mut *tx)
            .await?;
        } else {
            physical::retire(operation).execute(&mut *tx).await?;
        }
        objects::reserve(namespace, intent.payload, intent.revision as i64, operation)
            .execute(&mut *tx)
            .await?;
        if published {
            ensure!(
                objects::publish(
                    namespace,
                    intent.payload,
                    operation,
                    chrono::Utc::now().timestamp_micros()
                )
                .execute(&mut *tx)
                .await?
                .rows_affected()
                    == 1,
                "physical recovery publication changed"
            );
        } else {
            objects::clear(namespace, intent.payload)
                .execute(&mut *tx)
                .await?;
        }
        if let Some(previous) = previous {
            physical::retire(previous).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn catalog_bind(&self, id: Uuid, head: &Proof) -> Result<()> {
        let mut tx = q::begin(self).await?;
        let (namespace, _) = catalog(&mut tx, id).await?;
        head.publication.validate()?;
        ensure!(
            head.publication.namespace == namespace,
            "foreign recovery head"
        );
        let (_, entry) = q::entry(id, 1, head.publication.id, head.publication.revision)
            .fetch_one(&mut *tx)
            .await?;
        ensure!(
            matches!(entry.0, CatalogEntry::Proof(ref stored) if stored == head),
            "recovery head was not scanned"
        );
        let (old, _) = q::root(id).fetch_one(&mut *tx).await?;
        if let Some(old) = old {
            ensure!(old.0 == *head, "fixed recovery head changed");
        } else {
            q::bind_head(id, head).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn catalog_proofs(&self, id: Uuid, after: u64, through: u64) -> Result<Vec<Proof>> {
        ensure!(
            after <= through && through <= i64::MAX as u64,
            "invalid recovery proof range"
        );
        let mut tx = q::begin(self).await?;
        let (namespace, anchor) = catalog(&mut tx, id).await?;
        let mut proofs = Vec::new();
        for (hash, entry) in q::proofs(id, after, through).fetch_all(&mut *tx).await? {
            ensure!(
                hash == blake3::hash(&serde_json::to_vec(&entry.0)?).as_bytes(),
                "persisted recovery proof is damaged"
            );
            entry.validate(namespace, anchor.cluster_id)?;
            let CatalogEntry::Proof(proof) = entry.0 else {
                anyhow::bail!("invalid recovery proof category");
            };
            proofs.push(proof);
        }
        tx.commit().await?;
        Ok(proofs)
    }

    async fn catalog_index(&self, id: Uuid, proof: &Proof) -> Result<()> {
        let mut tx = q::begin(self).await?;
        let (namespace, _) = catalog(&mut tx, id).await?;
        proof.publication.validate()?;
        let (head, indexed) = q::root(id).fetch_one(&mut *tx).await?;
        let head = head.context("recovery head is not fixed")?;
        ensure!(
            proof.publication.namespace == namespace
                && proof.publication.revision <= head.publication.revision,
            "recovery publication exceeds the fixed head"
        );
        let (_, stored) = q::entry(id, 1, proof.publication.id, proof.publication.revision)
            .fetch_one(&mut *tx)
            .await?;
        ensure!(
            matches!(stored.0, CatalogEntry::Proof(ref p) if p == proof),
            "unscanned recovery publication"
        );
        if proof.publication.revision <= indexed as u64 {
            tx.commit().await?;
            return Ok(());
        }
        ensure!(
            proof.publication.revision == indexed as u64 + 1,
            "recovery publication indexing skipped a revision"
        );
        if let Some(previous) = &proof.publication.previous {
            let (_, stored) = q::entry(id, 1, previous.id, indexed as u64)
                .fetch_one(&mut *tx)
                .await?;
            let CatalogEntry::Proof(previous) = stored.0 else {
                anyhow::bail!("invalid recovery predecessor");
            };
            proof.follows(Some(&previous))?;
        } else {
            proof.follows(None)?;
        }
        if proof.publication.revision == head.publication.revision {
            ensure!(
                *proof == head.0,
                "recovery publication reached another head"
            );
        }
        for part in &proof.publication.parts {
            if let Some(old) = q::file(id, part.intent.operation)
                .fetch_optional(&mut *tx)
                .await?
            {
                ensure!(
                    old.0.0 == *part && !old.3,
                    "recovery publication changed or revived a retired file"
                );
                q::reference_file(id, part.intent.operation, proof.publication.revision)
                    .execute(&mut *tx)
                    .await?;
            } else {
                q::insert_file(
                    id,
                    part,
                    crate::model::storage_native::path(&part.path)?.to_vec(),
                    proof.publication.revision,
                    proof.publication.through,
                )
                .execute(&mut *tx)
                .await?;
            }
        }
        for operation in &proof.publication.retired {
            ensure!(
                q::file(id, *operation)
                    .fetch_optional(&mut *tx)
                    .await?
                    .is_some(),
                "recovery retirement refers to an unknown file"
            );
            q::retire_file(id, *operation).execute(&mut *tx).await?;
        }
        q::indexed(id, proof.publication.revision)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn catalog_file(&self, id: Uuid, path: &str) -> Result<Option<RecoveryFile>> {
        let hash = crate::model::storage_native::path(path)?;
        let mut tx = q::begin(self).await?;
        let (namespace, _) = catalog(&mut tx, id).await?;
        indexed(&mut tx, id).await?;
        let row = q::file_path(id, &hash, path)
            .fetch_optional(&mut *tx)
            .await?;
        let file = row
            .map(|row| recovery_file(row, namespace))
            .transpose()?
            .filter(|row| !row.retired);
        tx.commit().await?;
        Ok(file)
    }

    async fn catalog_files(&self, id: Uuid, after: Uuid) -> Result<Vec<RecoveryFile>> {
        let mut tx = q::begin(self).await?;
        let (namespace, _) = catalog(&mut tx, id).await?;
        indexed(&mut tx, id).await?;
        let rows = q::files(id, after).fetch_all(&mut *tx).await?;
        let files = rows
            .into_iter()
            .map(|row| recovery_file(row, namespace))
            .collect::<Result<_>>()?;
        tx.commit().await?;
        Ok(files)
    }

    async fn catalog_decoded(
        &self,
        id: Uuid,
        operation: Uuid,
        summary: &DecodedFile,
    ) -> Result<()> {
        ensure!(
            !summary.journal.is_nil()
                && !summary.epoch.is_nil()
                && summary.first <= summary.last
                && summary.last <= i64::MAX as u64,
            "invalid decoded recovery summary"
        );
        let mut tx = q::begin(self).await?;
        let (namespace, _) = catalog(&mut tx, id).await?;
        indexed(&mut tx, id).await?;
        let file = recovery_file(q::file(id, operation).fetch_one(&mut *tx).await?, namespace)?;
        ensure!(
            !file.retired && file.decoded.as_ref().is_none_or(|old| old == summary),
            "decoded recovery file changed"
        );
        q::decoded(id, operation, summary).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn catalog_stage(&self, id: Uuid, entries: &[CatalogEntry]) -> Result<()> {
        let encoded = encoded(entries)?;
        let mut tx = q::begin(self).await?;
        let (namespace, anchor) = catalog(&mut tx, id).await?;
        for entry in entries {
            ensure!(
                (3..=6).contains(&entry.identity().0),
                "only decoded records may enter recovery staging"
            );
            entry.validate(namespace, anchor.cluster_id)?;
        }
        insert(&mut tx, id, entries, &encoded).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn catalog_entry(
        &self,
        id: Uuid,
        category: i16,
        key: Uuid,
        position: u64,
    ) -> Result<Option<CatalogEntry>> {
        ensure!(
            (0..=6).contains(&category) && position <= i64::MAX as u64,
            "invalid recovery entry key"
        );
        let mut tx = q::begin(self).await?;
        let (namespace, anchor) = catalog(&mut tx, id).await?;
        let entry = q::entry(id, category, key, position)
            .fetch_optional(&mut *tx)
            .await?;
        let result = if let Some((hash, entry)) = entry {
            let bytes = serde_json::to_vec(&entry.0)?;
            ensure!(
                bytes.len() <= ENTRY_LIMIT && hash == blake3::hash(&bytes).as_bytes(),
                "persisted recovery entry is damaged"
            );
            entry.validate(namespace, anchor.cluster_id)?;
            Some(entry.0)
        } else {
            None
        };
        tx.commit().await?;
        Ok(result)
    }

    async fn catalog_open(&self, id: Uuid, namespace: Uuid, anchor: &Manifest) -> Result<()> {
        ensure!(
            !id.is_nil() && !namespace.is_nil(),
            "invalid recovery catalog identity"
        );
        anchor.validate()?;
        let mut tx = q::begin(self).await?;
        q::create(id, namespace, anchor).execute(&mut *tx).await?;
        let current = catalog(&mut tx, id).await?;
        ensure!(
            current.0 == namespace && current.1 == *anchor,
            Error::OperationAborted
        );
        let entry = CatalogEntry::Manifest(anchor.clone());
        let entries = [entry];
        insert(&mut tx, id, &entries, &encoded(&entries)?).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn catalog_checkpoint(
        &self,
        id: Uuid,
        revision: i64,
        cursor: &Cursor,
        entries: &[CatalogEntry],
    ) -> Result<i64> {
        cursor.validate()?;
        ensure!(
            revision >= 0 && revision < i64::MAX && entries.len() <= 256,
            "invalid recovery checkpoint"
        );
        let mut hash = blake3::Hasher::new();
        hash.update(b"mokyu/storage/recovery-checkpoint/v1");
        let encoded = encoded(entries)?;
        for (digest, _) in &encoded {
            hash.update(digest);
        }
        let hash = hash.finalize();
        let mut tx = q::begin(self).await?;
        let (namespace, anchor) = catalog(&mut tx, id).await?;
        ensure!(
            anchor
                .members
                .iter()
                .any(|member| member.media_id == cursor.media)
                && cursor.manifest == anchor.digest()?,
            "recovery source differs from its frozen manifest"
        );
        let old = q::cursor(id, cursor.media, &cursor.scope)
            .fetch_optional(&mut *tx)
            .await?;
        if let Some((stored_revision, stored, digest)) = &old {
            if *stored_revision == revision + 1 && stored.0 == *cursor && digest == hash.as_bytes()
            {
                tx.commit().await?;
                return Ok(*stored_revision);
            }
            ensure!(
                *stored_revision == revision && stored.manifest == cursor.manifest,
                Error::OperationAborted
            );
            if stored.scan == cursor.scan {
                ensure!(
                    stored.boot == cursor.boot
                        && stored.fence == cursor.fence
                        && !stored.complete
                        && stored.page.checked_add(1) == Some(cursor.page)
                        && cursor.prefix >= stored.prefix,
                    "recovery scan page is out of sequence"
                );
            } else {
                ensure!(
                    cursor.page == 0 && cursor.prefix <= stored.prefix,
                    "recovery restart would skip a directory"
                );
            }
        } else {
            ensure!(revision == 0 && cursor.page == 0, Error::OperationAborted);
        }
        for entry in entries {
            entry.validate(namespace, anchor.cluster_id)?;
        }
        insert(&mut tx, id, entries, &encoded).await?;
        if old.is_some() {
            ensure!(
                q::update_cursor(id, cursor, hash.as_bytes().to_vec(), revision)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected()
                    == 1,
                Error::OperationAborted
            );
        } else {
            q::insert_cursor(id, cursor, hash.as_bytes().to_vec())
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(revision + 1)
    }

    async fn catalog_cursors(
        &self,
        id: Uuid,
        after: Option<(Uuid, String)>,
    ) -> Result<Vec<(i64, Cursor)>> {
        let mut tx = q::begin(self).await?;
        catalog(&mut tx, id).await?;
        let (media, scope) = after.unwrap_or_default();
        let rows = q::cursors(id, media, scope).fetch_all(&mut *tx).await?;
        let mut result = Vec::with_capacity(rows.len());
        for (revision, cursor, _) in rows {
            cursor.validate()?;
            result.push((revision, cursor.0));
        }
        tx.commit().await?;
        Ok(result)
    }

    async fn catalog_page(
        &self,
        id: Uuid,
        category: i16,
        owner: Option<Uuid>,
        after: Option<(Uuid, u64)>,
    ) -> Result<CatalogPage> {
        ensure!(
            (0..=6).contains(&category) && after.is_none_or(|(_, value)| value <= i64::MAX as u64),
            "invalid recovery catalog page"
        );
        let mut tx = q::begin(self).await?;
        let (namespace, anchor) = catalog(&mut tx, id).await?;
        let page = page_in(
            &mut tx,
            id,
            namespace,
            anchor.cluster_id,
            category,
            owner,
            after,
        )
        .await?;
        tx.commit().await?;
        Ok(page)
    }
}

#[cfg(test)]
#[path = "../../../tests/recovery/storage_recovery.rs"]
mod tests;
