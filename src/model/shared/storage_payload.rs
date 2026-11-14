use super::{Backend, queries::storage_payload as q};
use crate::content::fragment::Header;
use crate::model::{Error, storage_payload::*};
use anyhow::{Context, Result, ensure};
use uuid::Uuid;

#[cfg(test)]
#[path = "../../../tests/recovery/storage_payload.rs"]
pub(crate) mod tests;

pub(super) fn payload(row: q::PayloadRow) -> Result<Payload> {
    row.0.validate()?;
    Ok(Payload {
        intent: row.0.0,
        state: row.1,
        protected_until: row.2,
        write_until: row.3,
    })
}

pub(super) fn mapping(row: q::FragmentRow) -> Result<Mapping> {
    let header = Header::decode(&row.0)?;
    let receipt = match (row.1, row.2) {
        (Some(root), Some(commitment)) => {
            let value = Receipt {
                header: header.clone(),
                root: root
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("invalid persisted checksum root"))?,
                commitment: commitment
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("invalid persisted commitment"))?,
            };
            value.validate()?;
            Some(value)
        }
        (None, None) => None,
        _ => anyhow::bail!("incomplete persisted fragment receipt"),
    };
    Ok(Mapping {
        header,
        receipt,
        deleted: row.3,
    })
}

pub(super) async fn current(tx: &mut q::Tx<'_>, operation: Uuid) -> Result<Payload> {
    payload(
        q::payload(operation)
            .fetch_optional(&mut **tx)
            .await?
            .ok_or(Error::OperationAborted)?,
    )
}

fn writing(value: &Payload) -> Result<()> {
    ensure!(
        value.state != "retired" && value.write_until > chrono::Utc::now().timestamp_micros(),
        Error::OperationAborted
    );
    Ok(())
}

async fn reclaimable(tx: &mut q::Tx<'_>, value: &Payload) -> Result<()> {
    let now = chrono::Utc::now().timestamp_micros();
    ensure!(
        value.state == "retired" && value.write_until <= now && value.protected_until <= now,
        Error::OperationAborted
    );
    ensure!(
        !super::queries::storage_backup::pending(value.intent.operation)
            .fetch_one(&mut **tx)
            .await?,
        Error::OperationAborted
    );
    recovery_clear(tx, value).await
}

async fn recovery_clear(tx: &mut q::Tx<'_>, value: &Payload) -> Result<()> {
    ensure!(
        super::queries::journal_restore::native_pending(value.intent.namespace)
            .fetch_optional(&mut **tx)
            .await?
            .is_none(),
        Error::OperationAborted
    );
    for proof in super::queries::storage_journal::roots(value.intent.namespace)
        .fetch_all(&mut **tx)
        .await?
    {
        ensure!(
            proof
                .publication
                .root
                .as_ref()
                .is_none_or(|root| root.intent.operation != value.intent.operation),
            Error::OperationAborted
        );
    }
    ensure!(
        !super::queries::journal_snapshots::pinned(value.intent.namespace)
            .fetch_one(&mut **tx)
            .await?,
        Error::OperationAborted
    );
    Ok(())
}

async fn repair_reclaimable(tx: &mut q::Tx<'_>, value: &Payload) -> Result<()> {
    ensure!(
        value.state == "published" && value.intent.area == crate::content::fragment::Area::Blocks,
        Error::OperationAborted
    );
    recovery_clear(tx, value).await?;
    ensure!(
        super::queries::storage_repair::handoff(value.intent.operation)
            .fetch_one(&mut **tx)
            .await?,
        Error::OperationAborted
    );
    Ok(())
}

pub(super) async fn collected(tx: &mut q::Tx<'_>, operation: Uuid) -> Result<bool> {
    let value = current(tx, operation).await?;
    if value.state == "published" {
        return Ok(super::queries::storage_repair::live_garbage(
            operation,
            Uuid::nil(),
            chrono::Utc::now().timestamp_micros(),
        )
        .fetch_optional(&mut **tx)
        .await?
        .is_none());
    }
    Ok(q::garbage(operation, Uuid::nil())
        .fetch_optional(&mut **tx)
        .await?
        .is_none()
        && super::queries::storage_repair::garbage(operation, Uuid::nil())
            .fetch_optional(&mut **tx)
            .await?
            .is_none())
}

pub(super) async fn collect_in(
    tx: &mut q::Tx<'_>,
    namespace: Uuid,
    limit: usize,
) -> Result<Vec<Uuid>> {
    ensure!(
        limit <= crate::model::node_work::MAX_ITEMS,
        "invalid physical cleanup batch"
    );
    let mut selected = Vec::new();
    if limit == 0 {
        return Ok(selected);
    }
    super::storage_native::collect_parts(tx, namespace, limit).await?;
    let now = chrono::Utc::now().timestamp_micros();
    for operation in q::collect_candidates(namespace, now)
        .fetch_all(&mut **tx)
        .await?
    {
        let value = current(tx, operation).await?;
        // Rotate failed and protected candidates without changing their read/write protection.
        if q::claim_collection(operation, now, now + 30_000_000)
            .execute(&mut **tx)
            .await?
            .rows_affected()
            == 0
        {
            continue;
        }
        let eligible = if value.state == "published" {
            repair_reclaimable(tx, &value).await
        } else {
            reclaimable(tx, &value).await
        };
        match eligible {
            Ok(()) => selected.push(operation),
            Err(error)
                if matches!(error.downcast_ref::<Error>(), Some(Error::OperationAborted)) =>
            {
                ()
            }
            Err(error) => return Err(error),
        }
        if selected.len() == limit {
            break;
        }
    }
    Ok(selected)
}

async fn read_mappings(
    backend: &Backend,
    operation: Uuid,
    first: u32,
    count: u32,
    until: i64,
    resume: bool,
) -> Result<Vec<Mapping>> {
    let now = chrono::Utc::now().timestamp_micros();
    ensure!(
        until > now && until <= now + 90_000_000,
        "invalid physical read lease"
    );
    let mut tx = q::begin(backend).await?;
    let value = current(&mut tx, operation).await?;
    ensure!(
        value.state == "published"
            || resume && value.state == "retired" && value.protected_until > now,
        Error::OperationAborted
    );
    let end = u64::from(first) + u64::from(count);
    ensure!(
        count > 0
            && u64::from(count) * value.intent.layout.width() as u64 <= BATCH_LIMIT as u64
            && end <= value.intent.sequences(),
        "invalid mapping window"
    );
    q::protect(operation, until).execute(&mut *tx).await?;
    let rows = q::range(operation, first, end).fetch_all(&mut *tx).await?;
    ensure!(
        rows.len() <= BATCH_LIMIT,
        "mapping window exceeded its limit"
    );
    let damaged = super::queries::storage_repair::pending(operation, first, end)
        .fetch_all(&mut *tx)
        .await?;
    let result = rows
        .into_iter()
        .map(mapping)
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .filter(|entry| !damaged.contains(&entry.header.fragment))
        .collect();
    tx.commit().await?;
    Ok(result)
}

pub(super) async fn publish_in(tx: &mut q::Tx<'_>, operation: Uuid) -> Result<()> {
    let value = current(tx, operation).await?;
    ensure!(value.state != "retired", Error::OperationAborted);
    if value.state == "published" {
        return Ok(());
    }
    ensure!(
        q::complete(operation).fetch_one(&mut **tx).await? as u64 == value.intent.required_files(),
        Error::OperationAborted
    );
    q::publish(operation).execute(&mut **tx).await?;
    super::storage_backup::enqueue(tx, &value.intent).await?;
    super::queries::storage_journal::dirty(operation)
        .execute(&mut **tx)
        .await?;
    if value.intent.required_files() < value.intent.sequences() * value.intent.layout.width() as u64
    {
        q::enqueue_fill(operation, value.intent.namespace)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

pub(super) async fn fill_plan(
    tx: &mut q::Tx<'_>,
    operation: Uuid,
) -> Result<Option<crate::model::storage_native::Fill>> {
    if let Some((source, _, done)) = super::queries::storage_backup::row(operation)
        .fetch_optional(&mut **tx)
        .await?
    {
        return super::storage_backup::plan(tx, operation, source, done.is_some()).await;
    }
    if super::queries::storage_repair::operation(operation)
        .fetch_optional(&mut **tx)
        .await?
        .is_some()
    {
        return super::storage_repair::plan(tx, operation).await;
    }
    let value = current(tx, operation).await?;
    if value.state != "published" {
        return Ok(None);
    }
    let Some(sequence) = q::missing_sequence(operation).fetch_one(&mut **tx).await? else {
        return Ok(None);
    };
    q::protect(
        operation,
        chrono::Utc::now().timestamp_micros() + 90_000_000,
    )
    .execute(&mut **tx)
    .await?;
    Ok(Some(crate::model::storage_native::Fill {
        storage: value.intent.namespace,
        operation,
        sequence: sequence.try_into()?,
        repair: None,
        backup: None,
        shard: 0,
    }))
}

pub(super) async fn filled(
    tx: &mut q::Tx<'_>,
    plan: &crate::model::storage_native::Fill,
) -> Result<()> {
    let value = current(tx, plan.operation).await?;
    if let Some(id) = plan.backup {
        return super::storage_backup::captured(tx, id, plan).await;
    }
    if value.state == "retired" {
        return Ok(());
    }
    if let Some(id) = plan.repair {
        let repair = super::storage_repair::current(tx, id).await?;
        ensure!(
            matches!(repair.state.as_str(), "installed" | "healthy"),
            Error::OperationAborted
        );
        return Ok(());
    }
    let rows = q::range(plan.operation, plan.sequence, u64::from(plan.sequence) + 1)
        .fetch_all(&mut **tx)
        .await?;
    ensure!(
        rows.len() == value.intent.layout.width() && rows.iter().all(|r| r.1.is_some() && !r.3),
        Error::OperationAborted
    );
    Ok(())
}

pub(super) async fn receipts_in(
    tx: &mut q::Tx<'_>,
    operation: Uuid,
    receipts: &[Receipt],
) -> Result<()> {
    ensure!(
        !receipts.is_empty() && receipts.len() <= BATCH_LIMIT,
        "invalid receipt batch"
    );
    let value = current(tx, operation).await?;
    writing(&value)?;
    let mut changed = false;
    for receipt in receipts {
        receipt.validate()?;
        value.intent.accepts(&receipt.header)?;
        let existing = mapping(
            q::fragment(receipt.header.fragment, operation)
                .fetch_optional(&mut **tx)
                .await?
                .context("fragment was not prepared")?,
        )?;
        ensure!(!existing.deleted, Error::OperationAborted);
        let mut expected = receipt.header.clone();
        if existing.header.digest == [0; 32] {
            expected.digest = [0; 32];
        }
        ensure!(expected == existing.header, Error::OperationAborted);
        if let Some(saved) = existing.receipt {
            ensure!(saved == *receipt, Error::OperationAborted);
        } else {
            if value.intent.layout.extra_copies > 0
                && let Some(peer) = q::receipt_peer(&receipt.header)
                    .fetch_optional(&mut **tx)
                    .await?
            {
                let peer = mapping(peer)?.receipt.context("copy receipt is missing")?;
                ensure!(
                    peer.header.digest == receipt.header.digest && peer.root == receipt.root,
                    "copies contain different fragment bytes"
                );
            }
            q::receipt(receipt, receipt.header.encode()?.to_vec())
                .execute(&mut **tx)
                .await?;
            changed = true;
        }
    }
    if changed {
        super::queries::storage_journal::dirty(operation)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

pub(super) async fn prepare_in(tx: &mut q::Tx<'_>, intent: &Intent, until: i64) -> Result<Payload> {
    intent.validate()?;
    let now = chrono::Utc::now().timestamp_micros();
    ensure!(
        until > now && until <= now + 90_000_000,
        "invalid physical write lease"
    );
    if let Some(row) = q::payload(intent.operation)
        .fetch_optional(&mut **tx)
        .await?
    {
        let mut existing = payload(row)?;
        ensure!(&existing.intent == intent, Error::OperationAborted);
        ensure!(existing.state != "retired", Error::OperationAborted);
        q::renew(intent.operation, until).execute(&mut **tx).await?;
        existing.write_until = existing.write_until.max(until);
        return Ok(existing);
    }
    let head = q::current_manifest(intent.cluster)
        .fetch_one(&mut **tx)
        .await?;
    ensure!(
        head.revision as u64 == intent.layout_revision
            && head.takeover_epoch == intent.takeover_epoch,
        Error::OperationAborted
    );
    let manifest = q::manifest(intent.cluster, intent.layout_revision)
        .fetch_optional(&mut **tx)
        .await?
        .context("payload layout is unavailable")?;
    manifest.validate()?;
    ensure!(
        manifest.takeover_epoch == intent.takeover_epoch
            && manifest
                .groups
                .iter()
                .any(|group| group.id == intent.group && group.layout == intent.layout),
        "payload group differs from its historical layout"
    );
    intent.members(
        manifest
            .groups
            .iter()
            .find(|group| group.id == intent.group)
            .unwrap(),
    )?;
    q::insert(intent).execute(&mut **tx).await?;
    let mut value = current(tx, intent.operation).await?;
    ensure!(&value.intent == intent, Error::OperationAborted);
    q::renew(intent.operation, until).execute(&mut **tx).await?;
    value.write_until = value.write_until.max(until);
    Ok(value)
}

#[async_trait::async_trait]
impl StoragePayloads for Backend {
    async fn prepare(&self, intent: &Intent, until: i64) -> Result<Payload> {
        let mut tx = q::begin(self).await?;
        let value = prepare_in(&mut tx, intent, until).await?;
        tx.commit().await?;
        Ok(value)
    }

    async fn stage(&self, operation: Uuid, headers: &[Header]) -> Result<()> {
        ensure!(
            !headers.is_empty() && headers.len() <= BATCH_LIMIT,
            "invalid fragment plan batch"
        );
        let mut tx = q::begin(self).await?;
        let value = current(&mut tx, operation).await?;
        writing(&value)?;
        let manifest = q::manifest(value.intent.cluster, value.intent.layout_revision)
            .fetch_one(&mut *tx)
            .await?;
        manifest.validate()?;
        let group = manifest
            .groups
            .iter()
            .find(|g| g.id == value.intent.group)
            .context("payload group is missing")?;
        let members = value.intent.members(group)?;
        for header in headers {
            value.intent.accepts(header)?;
            let slot = usize::from(header.copy)
                * (usize::from(header.layout.data_shards)
                    + usize::from(header.layout.parity_shards))
                + usize::from(header.shard);
            ensure!(
                members.get(slot) == Some(&header.media),
                "fragment targets another layout member"
            );
            if let Some(row) = q::fragment(header.fragment, operation)
                .fetch_optional(&mut *tx)
                .await?
            {
                let mut existing = mapping(row)?;
                ensure!(!existing.deleted, Error::OperationAborted);
                if header.digest == [0; 32] {
                    existing.header.digest = [0; 32];
                }
                ensure!(existing.header == *header, Error::OperationAborted);
            } else {
                q::stage(
                    header,
                    header.encode()?.to_vec(),
                    value.intent.required(header.copy, header.shard),
                )
                .execute(&mut *tx)
                .await?;
            }
        }
        tx.commit().await?;
        Ok(())
    }

    async fn receipts(&self, operation: Uuid, receipts: &[Receipt]) -> Result<()> {
        let mut tx = q::begin(self).await?;
        receipts_in(&mut tx, operation, receipts).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn publish(&self, operation: Uuid) -> Result<()> {
        let mut tx = q::begin(self).await?;
        publish_in(&mut tx, operation).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn get(&self, operation: Uuid) -> Result<Payload> {
        let mut tx = q::begin(self).await?;
        let result = current(&mut tx, operation).await?;
        tx.commit().await?;
        Ok(result)
    }

    async fn read(
        &self,
        operation: Uuid,
        first: u32,
        count: u32,
        until: i64,
    ) -> Result<Vec<Mapping>> {
        read_mappings(self, operation, first, count, until, false).await
    }

    async fn resume_read(
        &self,
        operation: Uuid,
        first: u32,
        count: u32,
        until: i64,
    ) -> Result<Vec<Mapping>> {
        read_mappings(self, operation, first, count, until, true).await
    }

    async fn write_window(
        &self,
        operation: Uuid,
        first: u32,
        count: u32,
        until: i64,
    ) -> Result<Vec<Mapping>> {
        let now = chrono::Utc::now().timestamp_micros();
        ensure!(
            until > now && until <= now + 90_000_000,
            "invalid physical write lease"
        );
        let mut tx = q::begin(self).await?;
        let value = current(&mut tx, operation).await?;
        ensure!(value.state != "retired", Error::OperationAborted);
        let end = u64::from(first) + u64::from(count);
        ensure!(
            count > 0
                && u64::from(count) * value.intent.layout.width() as u64 <= BATCH_LIMIT as u64
                && end <= value.intent.sequences(),
            "invalid mapping window"
        );
        q::renew(operation, until).execute(&mut *tx).await?;
        let rows = q::range(operation, first, end).fetch_all(&mut *tx).await?;
        ensure!(
            rows.len() <= BATCH_LIMIT,
            "mapping window exceeded its limit"
        );
        let mut values = rows.into_iter().map(mapping).collect::<Result<Vec<_>>>()?;
        let manifest = q::manifest(value.intent.cluster, value.intent.layout_revision)
            .fetch_one(&mut *tx)
            .await?;
        let group = manifest
            .groups
            .iter()
            .find(|group| group.id == value.intent.group)
            .context("payload group is missing")?;
        let members = value.intent.members(group)?;
        for sequence in first..end as u32 {
            for copy in 0..=value.intent.layout.extra_copies {
                for shard in 0..value.intent.layout.data_shards + value.intent.layout.parity_shards
                {
                    if values.iter().any(|entry| {
                        entry.header.sequence == sequence
                            && entry.header.copy == copy
                            && entry.header.shard == shard
                    }) {
                        continue;
                    }
                    let index = usize::from(copy)
                        * (usize::from(value.intent.layout.data_shards)
                            + usize::from(value.intent.layout.parity_shards))
                        + usize::from(shard);
                    let header = value.intent.header(
                        sequence,
                        copy,
                        shard,
                        members[index],
                        Uuid::now_v7(),
                    )?;
                    q::stage(
                        &header,
                        header.encode()?.to_vec(),
                        value.intent.required(copy, shard),
                    )
                    .execute(&mut *tx)
                    .await?;
                    values.push(Mapping {
                        header,
                        receipt: None,
                        deleted: false,
                    });
                }
            }
        }
        values.sort_by_key(|entry| (entry.header.sequence, entry.header.copy, entry.header.shard));
        tx.commit().await?;
        Ok(values)
    }

    async fn retire(&self, operation: Uuid) -> Result<()> {
        let mut tx = q::begin(self).await?;
        current(&mut tx, operation).await?;
        q::retire(operation).execute(&mut *tx).await?;
        super::queries::storage_journal::dirty(operation)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn reclaim(&self, operation: Uuid, after: Option<Uuid>) -> Result<Vec<Mapping>> {
        let mut tx = q::begin(self).await?;
        let value = current(&mut tx, operation).await?;
        if value.state == "published" {
            repair_reclaimable(&mut tx, &value).await?;
            let mut result = Vec::new();
            for row in super::queries::storage_repair::live_garbage(
                operation,
                after.unwrap_or(Uuid::nil()),
                chrono::Utc::now().timestamp_micros(),
            )
            .fetch_all(&mut *tx)
            .await?
            {
                let repair = super::storage_repair::decode(row)?;
                super::queries::storage_repair::collecting(repair.original.header.fragment)
                    .execute(&mut *tx)
                    .await?;
                result.push(Mapping {
                    header: repair.original.header.clone(),
                    receipt: Some(repair.original),
                    deleted: false,
                });
            }
            tx.commit().await?;
            return Ok(result);
        }
        reclaimable(&mut tx, &value).await?;
        ensure!(
            !super::queries::storage_journal::held(operation)
                .fetch_one(&mut *tx)
                .await?,
            Error::OperationAborted
        );
        let rows = q::garbage(operation, after.unwrap_or(Uuid::nil()))
            .fetch_all(&mut *tx)
            .await?;
        let mut result = rows.into_iter().map(mapping).collect::<Result<Vec<_>>>()?;
        result.extend(
            super::storage_repair::garbage(&mut tx, operation, after.unwrap_or(Uuid::nil()))
                .await?,
        );
        result.sort_by_key(|entry| entry.header.fragment);
        tx.commit().await?;
        Ok(result)
    }

    async fn collect_candidates(&self, namespace: Uuid, limit: usize) -> Result<Vec<Uuid>> {
        let mut tx = q::begin(self).await?;
        let result = collect_in(&mut tx, namespace, limit).await?;
        tx.commit().await?;
        Ok(result)
    }

    async fn removed(&self, operation: Uuid, fragments: &[Uuid]) -> Result<()> {
        ensure!(
            !fragments.is_empty() && fragments.len() <= BATCH_LIMIT,
            "invalid deleted fragment batch"
        );
        let mut tx = q::begin(self).await?;
        let value = current(&mut tx, operation).await?;
        if value.state == "published" {
            for id in fragments {
                let repair = super::storage_repair::current(&mut tx, *id).await?;
                ensure!(
                    repair.original.header.operation == operation
                        && repair.state == "installed"
                        && repair.collecting,
                    Error::OperationAborted
                );
                super::queries::storage_repair::removed_original(operation, *id)
                    .execute(&mut *tx)
                    .await?;
            }
            tx.commit().await?;
            return Ok(());
        }
        reclaimable(&mut tx, &value).await?;
        ensure!(
            !super::queries::storage_journal::held(operation)
                .fetch_one(&mut *tx)
                .await?,
            Error::OperationAborted
        );
        for id in fragments {
            q::removed(operation, *id).execute(&mut *tx).await?;
            super::queries::storage_repair::removed_original(operation, *id)
                .execute(&mut *tx)
                .await?;
            super::queries::storage_repair::removed_candidate(operation, *id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }
}
