use super::{Backend, queries::storage_payload as q};
use crate::content::fragment::Header;
use crate::model::{Error, storage_payload::*};
use anyhow::{Context, Result, ensure};
use uuid::Uuid;

#[cfg(test)]
#[path = "../../../tests/recovery/storage_payload.rs"]
mod tests;

fn payload(row: q::PayloadRow) -> Result<Payload> {
    row.0.validate()?;
    Ok(Payload {
        intent: row.0.0,
        state: row.1,
        protected_until: row.2,
        write_until: row.3,
    })
}

fn mapping(row: q::FragmentRow) -> Result<Mapping> {
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

async fn current(tx: &mut q::Tx<'_>, operation: Uuid) -> Result<Payload> {
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

fn reclaimable(value: &Payload) -> Result<()> {
    let now = chrono::Utc::now().timestamp_micros();
    ensure!(
        value.state == "retired" && value.write_until <= now && value.protected_until <= now,
        Error::OperationAborted
    );
    Ok(())
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
    Ok(())
}

#[async_trait::async_trait]
impl StoragePayloads for Backend {
    async fn prepare(&self, intent: &Intent, until: i64) -> Result<Payload> {
        intent.validate()?;
        let now = chrono::Utc::now().timestamp_micros();
        ensure!(
            until > now && until <= now + 90_000_000,
            "invalid physical write lease"
        );
        let mut tx = q::begin(self).await?;
        if let Some(row) = q::payload(intent.operation)
            .fetch_optional(&mut *tx)
            .await?
        {
            let mut existing = payload(row)?;
            ensure!(&existing.intent == intent, Error::OperationAborted);
            ensure!(existing.state != "retired", Error::OperationAborted);
            q::renew(intent.operation, until).execute(&mut *tx).await?;
            existing.write_until = existing.write_until.max(until);
            tx.commit().await?;
            return Ok(existing);
        }
        let head = q::current_manifest(intent.cluster)
            .fetch_one(&mut *tx)
            .await?;
        ensure!(
            head.revision as u64 == intent.layout_revision
                && head.takeover_epoch == intent.takeover_epoch,
            Error::OperationAborted
        );
        let manifest = q::manifest(intent.cluster, intent.layout_revision)
            .fetch_optional(&mut *tx)
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
        q::insert(intent).execute(&mut *tx).await?;
        let mut value = current(&mut tx, intent.operation).await?;
        ensure!(&value.intent == intent, Error::OperationAborted);
        q::renew(intent.operation, until).execute(&mut *tx).await?;
        value.write_until = value.write_until.max(until);
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
        for header in headers {
            value.intent.accepts(header)?;
            let slot = usize::from(header.copy)
                * (usize::from(header.layout.data_shards)
                    + usize::from(header.layout.parity_shards))
                + usize::from(header.shard);
            ensure!(
                group.members.get(slot) == Some(&header.media),
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
        ensure!(
            !receipts.is_empty() && receipts.len() <= BATCH_LIMIT,
            "invalid receipt batch"
        );
        let mut tx = q::begin(self).await?;
        let value = current(&mut tx, operation).await?;
        writing(&value)?;
        for receipt in receipts {
            receipt.validate()?;
            value.intent.accepts(&receipt.header)?;
            let existing = mapping(
                q::fragment(receipt.header.fragment, operation)
                    .fetch_optional(&mut *tx)
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
                        .fetch_optional(&mut *tx)
                        .await?
                {
                    let peer = mapping(peer)?.receipt.context("copy receipt is missing")?;
                    ensure!(
                        peer.header.digest == receipt.header.digest && peer.root == receipt.root,
                        "copies contain different fragment bytes"
                    );
                }
                q::receipt(receipt, receipt.header.encode()?.to_vec())
                    .execute(&mut *tx)
                    .await?;
            }
        }
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
        let now = chrono::Utc::now().timestamp_micros();
        ensure!(
            until > now && until <= now + 90_000_000,
            "invalid physical read lease"
        );
        let mut tx = q::begin(self).await?;
        let value = current(&mut tx, operation).await?;
        ensure!(value.state == "published", Error::OperationAborted);
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
        let result = rows.into_iter().map(mapping).collect::<Result<_>>()?;
        tx.commit().await?;
        Ok(result)
    }

    async fn retire(&self, operation: Uuid) -> Result<()> {
        let mut tx = q::begin(self).await?;
        current(&mut tx, operation).await?;
        q::retire(operation).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn reclaim(&self, operation: Uuid, after: Option<Uuid>) -> Result<Vec<Mapping>> {
        let mut tx = q::begin(self).await?;
        reclaimable(&current(&mut tx, operation).await?)?;
        let rows = q::garbage(operation, after.unwrap_or(Uuid::nil()))
            .fetch_all(&mut *tx)
            .await?;
        let result = rows.into_iter().map(mapping).collect::<Result<_>>()?;
        tx.commit().await?;
        Ok(result)
    }

    async fn removed(&self, operation: Uuid, fragments: &[Uuid]) -> Result<()> {
        ensure!(
            !fragments.is_empty() && fragments.len() <= BATCH_LIMIT,
            "invalid deleted fragment batch"
        );
        let mut tx = q::begin(self).await?;
        reclaimable(&current(&mut tx, operation).await?)?;
        for id in fragments {
            q::removed(operation, *id).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }
}
