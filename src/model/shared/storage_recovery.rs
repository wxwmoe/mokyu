use super::{Backend, queries::storage_recovery as q};
use crate::model::{Error, storage_cluster::Manifest, storage_recovery::*};
use anyhow::{Context, Result, ensure};
use uuid::Uuid;

const ENTRY_LIMIT: usize = 8 * 1024 * 1024;

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

#[async_trait::async_trait]
impl RecoveryCatalog for Backend {
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
        let keys = q::page(
            id,
            category,
            owner,
            after.map_or((Uuid::nil(), -1), |(id, position)| (id, position as i64)),
        )
        .fetch_all(&mut *tx)
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
                .fetch_one(&mut *tx)
                .await?;
            let bytes = serde_json::to_vec(&entry.0)?;
            ensure!(
                bytes.len() == size as usize && hash == blake3::hash(&bytes).as_bytes(),
                "persisted recovery entry is damaged"
            );
            entry.validate(namespace, anchor.cluster_id)?;
            entries.push(entry.0);
            after = Some((key, position as u64));
        }
        tx.commit().await?;
        Ok(CatalogPage { entries, after })
    }
}

#[cfg(test)]
#[path = "../../../tests/recovery/storage_recovery.rs"]
mod tests;
