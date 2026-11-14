use super::{
    Backend,
    queries::{storage_backup as q, storage_payload as p},
    storage_capacity, storage_payload,
};
use crate::{
    content::codec::Key,
    model::{
        Error,
        storage_backup::{Backups, File, choose},
        storage_native::Fill,
        storage_payload::{Intent, Payload},
        storage_transfer::{Action, Completion},
    },
};
use anyhow::{Context, Result, ensure};
use uuid::Uuid;

pub(super) async fn enqueue(tx: &mut p::Tx<'_>, intent: &Intent) -> Result<()> {
    let copies = q::copies(intent.cluster).fetch_one(&mut **tx).await?;
    ensure!((0..=256).contains(&copies), "invalid backup count");
    add_copies(tx, intent, 0, copies).await
}

pub(super) async fn recheck(tx: &mut p::Tx<'_>, intent: &Intent, stamp: i64) -> Result<()> {
    for id in q::queued_checks(intent.operation, stamp)
        .fetch_all(&mut **tx)
        .await?
    {
        q::reset(id, stamp).execute(&mut **tx).await?;
        p::enqueue_fill(id, intent.namespace)
            .execute(&mut **tx)
            .await?;
        super::queries::storage_repair::restart_task(id)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

async fn add_copies(tx: &mut p::Tx<'_>, intent: &Intent, existing: i64, copies: i64) -> Result<()> {
    let now = chrono::Utc::now().timestamp_micros();
    for ordinal in existing..copies {
        let id = Uuid::now_v7();
        q::add(id, intent.operation, ordinal as i32, now)
            .execute(&mut **tx)
            .await?;
        p::enqueue_fill(id, intent.namespace)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

pub(super) async fn plan(
    tx: &mut p::Tx<'_>,
    id: Uuid,
    operation: Uuid,
    done: bool,
) -> Result<Option<Fill>> {
    if done {
        return Ok(None);
    }
    let value = storage_payload::current(tx, operation).await?;
    let width = u64::from(value.intent.layout.data_shards);
    let count = u64::try_from(q::count(id).fetch_one(&mut **tx).await?)?;
    let stale = q::stale(id).fetch_optional(&mut **tx).await?;
    if stale.is_none() && count == value.intent.sequences() * width {
        q::finish(id, chrono::Utc::now().timestamp_micros())
            .execute(&mut **tx)
            .await?;
        return Ok(None);
    }
    ensure!(
        count <= value.intent.sequences() * width,
        Error::OperationAborted
    );
    let (seq, shard) = stale.unwrap_or(((count / width).try_into()?, (count % width).try_into()?));
    p::protect(
        operation,
        chrono::Utc::now().timestamp_micros() + 90_000_000,
    )
    .execute(&mut **tx)
    .await?;
    Ok(Some(Fill {
        storage: value.intent.namespace,
        operation,
        sequence: seq.try_into()?,
        shard: shard.try_into()?,
        backup: Some(id),
        repair: None,
    }))
}

pub(super) async fn captured(tx: &mut p::Tx<'_>, id: Uuid, plan: &Fill) -> Result<()> {
    let row = q::file(id, plan.sequence, plan.shard)
        .fetch_one(&mut **tx)
        .await?;
    let cutoff = q::cutoff(id).fetch_one(&mut **tx).await?;
    ensure!(
        row.1.header.operation == plan.operation && row.2.is_some_and(|v| v >= cutoff),
        Error::OperationAborted
    );
    Ok(())
}

fn decoded(
    (media, receipt, verified): (
        Uuid,
        sqlx::types::Json<crate::model::storage_payload::Receipt>,
        Option<i64>,
    ),
) -> Result<File> {
    receipt.validate()?;
    Ok(File {
        media,
        receipt: receipt.0,
        verified: verified.is_some(),
        recheck: false,
    })
}

#[async_trait::async_trait]
impl Backups for Backend {
    async fn backfill_backups(&self) -> Result<bool> {
        let mut tx = p::begin(self).await?;
        let Some(cluster) = q::due().fetch_optional(&mut *tx).await? else {
            return Ok(false);
        };
        let Some((after, stamp)) = q::cursor(cluster).fetch_optional(&mut *tx).await? else {
            return Ok(true);
        };
        let copies = q::copies(cluster).fetch_one(&mut *tx).await?;
        ensure!((0..=256).contains(&copies), "invalid backup count");
        let rows = q::scan(cluster, after).fetch_all(&mut *tx).await?;
        for op in &rows {
            let value = storage_payload::current(&mut tx, *op).await?;
            if value.state == "published" {
                let count = q::slots(*op).fetch_one(&mut *tx).await?;
                add_copies(&mut tx, &value.intent, count, copies).await?;
                if stamp > 0 {
                    recheck(&mut tx, &value.intent, stamp).await?;
                }
            }
        }
        if rows.len() < 16 {
            q::end(cluster).execute(&mut *tx).await?;
        } else {
            q::advance(cluster, *rows.last().unwrap())
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(true)
    }
    async fn backup_file(&self, id: Uuid, sequence: u32, shard: u16) -> Result<File> {
        let mut tx = p::begin(self).await?;
        let (operation, _, _) = q::row(id).fetch_one(&mut *tx).await?;
        let value = storage_payload::current(&mut tx, operation).await?;
        // Acquire the cluster allocation lock before locking the payload to allocate a destination.
        let cluster = value.intent.cluster;
        tx.commit().await?;
        let mut tx = p::begin(self).await?;
        let allocation = storage_capacity::lock(self, &mut tx, cluster).await?;
        let (checked, group, done) = q::row(id).fetch_one(&mut *tx).await?;
        let value: Payload = storage_payload::current(&mut tx, operation).await?;
        ensure!(
            checked == operation
                && value.intent.cluster == cluster
                && !matches!(value.state.as_str(), "prepared")
                && u64::from(sequence) < value.intent.sequences()
                && shard < value.intent.layout.data_shards,
            Error::OperationAborted
        );
        let result = if let Some(existing) = q::file(id, sequence, shard)
            .fetch_optional(&mut *tx)
            .await?
        {
            let cutoff = q::cutoff(id).fetch_one(&mut *tx).await?;
            let checked = existing.2;
            let mut file = decoded(existing)?;
            file.recheck = checked.is_some_and(|t| t < cutoff);
            file.verified = checked.is_some_and(|t| t >= cutoff);
            file
        } else {
            let count = u64::try_from(q::count(id).fetch_one(&mut *tx).await?)?;
            ensure!(
                done.is_none()
                    && count
                        == u64::from(sequence) * u64::from(value.intent.layout.data_shards)
                            + u64::from(shard),
                Error::OperationAborted
            );
            let mut source = None;
            for row in p::range(operation, sequence, u64::from(sequence) + 1)
                .fetch_all(&mut *tx)
                .await?
            {
                let mapping = storage_payload::mapping(row)?;
                if mapping.header.shard == shard
                    && !mapping.deleted
                    && let Some(receipt) = mapping.receipt
                {
                    source = Some(receipt);
                    break;
                }
            }
            let receipt = source.context("no durable backup source")?;
            let occupied = q::groups(operation)
                .fetch_all(&mut *tx)
                .await?
                .into_iter()
                .collect();
            let bytes = receipt.header.file_bytes() + 65536;
            let member = choose(
                &allocation.cluster.manifest.members,
                &allocation.status,
                group.as_deref(),
                &occupied,
                bytes,
            )
            .ok_or_else(|| {
                s3s::s3_error!(
                    SlowDown,
                    "no independent backup group has available capacity"
                )
            })?;
            if group.is_none() {
                ensure!(
                    q::assign(id, &member.topology.storage_group)
                        .execute(&mut *tx)
                        .await?
                        .rows_affected()
                        == 1,
                    Error::OperationAborted
                );
            }
            super::queries::storage_repair::reserve(
                operation,
                member.media_id,
                cluster,
                bytes.try_into()?,
            )
            .execute(&mut *tx)
            .await?;
            q::insert(id, member.media_id, &receipt)
                .execute(&mut *tx)
                .await?;
            File {
                media: member.media_id,
                receipt,
                verified: false,
                recheck: false,
            }
        };
        tx.commit().await?;
        Ok(result)
    }

    async fn backup_link(&self, id: Uuid) -> Result<crate::model::storage_recovery::Link> {
        let mut tx = p::begin(self).await?;
        let (op, _, _) = q::row(id).fetch_one(&mut *tx).await?;
        let value = storage_payload::current(&mut tx, op).await?;
        let ns = value.intent.namespace;
        let rev = u64::try_from(q::publication(ns, op).fetch_one(&mut *tx).await?)?;
        let (proof, manifest) = super::queries::storage_journal::history(ns, rev - 1, rev)
            .fetch_one(&mut *tx)
            .await?;
        proof.verify(&manifest)?;
        proof.link()
    }

    async fn reset_evidence(&self, id: Uuid, media: Uuid, sequence: u32, shard: u16) -> Result<()> {
        let mut tx = p::begin(self).await?;
        let (op, _, _) = q::row(id).fetch_one(&mut *tx).await?;
        let value = storage_payload::current(&mut tx, op).await?;
        let (member, _, _) = q::file(id, sequence, shard).fetch_one(&mut *tx).await?;
        ensure!(
            member == media && value.intent.area == crate::content::fragment::Area::Recovery,
            Error::OperationAborted
        );
        q::reset_evidence(value.intent.namespace, media)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn backup_evidence(
        &self,
        id: Uuid,
        media: Uuid,
    ) -> Result<
        Option<(
            crate::model::storage_recovery::Proof,
            crate::model::storage_cluster::Manifest,
        )>,
    > {
        let mut tx = p::begin(self).await?;
        let (op, _, _) = q::row(id).fetch_one(&mut *tx).await?;
        let value = storage_payload::current(&mut tx, op).await?;
        if value.intent.area != crate::content::fragment::Area::Recovery {
            return Ok(None);
        }
        let namespace = value.intent.namespace;
        let desired = q::publication(namespace, op).fetch_one(&mut *tx).await?;
        let after = super::queries::storage_journal::evidence(namespace, media)
            .fetch_optional(&mut *tx)
            .await?
            .unwrap_or(0);
        let result = if after < desired {
            let (proof, manifest) = super::queries::storage_journal::history(
                namespace,
                after.try_into()?,
                (after + 1).try_into()?,
            )
            .fetch_one(&mut *tx)
            .await?;
            proof.verify(&manifest)?;
            Some((proof.0, manifest.0))
        } else {
            None
        };
        tx.commit().await?;
        Ok(result)
    }
    async fn backup_published(
        &self,
        id: Uuid,
        media: Uuid,
        sequence: u32,
        shard: u16,
        revision: u64,
        link: &crate::model::storage_recovery::Link,
    ) -> Result<()> {
        let mut tx = p::begin(self).await?;
        let (op, _, _) = q::row(id).fetch_one(&mut *tx).await?;
        let value = storage_payload::current(&mut tx, op).await?;
        let ns = value.intent.namespace;
        let assigned = q::file(id, sequence, shard).fetch_one(&mut *tx).await?;
        ensure!(
            assigned.0 == media && value.intent.area == crate::content::fragment::Area::Recovery,
            Error::OperationAborted
        );
        super::queries::storage_journal::ensure_evidence(ns, media)
            .execute(&mut *tx)
            .await?;
        let after = super::queries::storage_journal::evidence(ns, media)
            .fetch_one(&mut *tx)
            .await?;
        if after < i64::try_from(revision)? {
            let (proof, _) =
                super::queries::storage_journal::history(ns, after.try_into()?, revision)
                    .fetch_one(&mut *tx)
                    .await?;
            ensure!(
                proof.link()? == *link && revision == u64::try_from(after)? + 1,
                Error::OperationAborted
            );
            ensure!(
                super::queries::storage_journal::receipt(ns, media, revision)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected()
                    == 1,
                Error::OperationAborted
            );
        }
        tx.commit().await?;
        Ok(())
    }
    async fn backup_accept(&self, key: &Key, id: Uuid, proof: &Completion) -> Result<()> {
        let receipt = &proof.receipt.receipt;
        let h = &receipt.header;
        let mut tx = p::begin(self).await?;
        let allocation = storage_capacity::lock(self, &mut tx, h.cluster).await?;
        let target = allocation
            .targets
            .iter()
            .find(|v| v.device.media_id == proof.authorization.grant.media)
            .context("backup target is missing")?;
        proof.verify(key, &allocation.cluster.manifest, target)?;
        let (operation, group, _) = q::row(id).fetch_one(&mut *tx).await?;
        let f = decoded(q::file(id, h.sequence, h.shard).fetch_one(&mut *tx).await?)?;
        ensure!(
            operation == h.operation
                && f.receipt == *receipt
                && f.media == target.device.media_id
                && target.device.announcement.topology.purpose == "backup"
                && group.as_deref()
                    == Some(target.device.announcement.topology.storage_group.as_str())
                && (matches!(&proof.authorization.grant.action,Action::Backup {header} if *header==*h)
                    || matches!(&proof.authorization.grant.action,Action::BackupCheck {receipt:expected,..} if expected==receipt)),
            "backup receipt differs from its assigned source or group"
        );
        let cutoff = q::cutoff(id).fetch_one(&mut *tx).await?;
        ensure!(
            cutoff == 0 || (proof.authorization.grant.issued + 1) * 1_000_000 >= cutoff,
            Error::OperationAborted
        );
        {
            if h.area == crate::content::fragment::Area::Recovery {
                let desired = q::publication(h.namespace, h.operation)
                    .fetch_one(&mut *tx)
                    .await?;
                super::queries::storage_journal::ensure_evidence(h.namespace, f.media)
                    .execute(&mut *tx)
                    .await?;
                let after = super::queries::storage_journal::evidence(h.namespace, f.media)
                    .fetch_one(&mut *tx)
                    .await?;
                ensure!(after >= desired, "backup evidence chain has a gap");
                if let Action::BackupCheck { evidence, .. } = &proof.authorization.grant.action {
                    let (published, manifest) = super::queries::storage_journal::history(
                        h.namespace,
                        (desired - 1).try_into()?,
                        desired.try_into()?,
                    )
                    .fetch_one(&mut *tx)
                    .await?;
                    published.verify(&manifest)?;
                    ensure!(
                        Some(published.link()?) == *evidence,
                        "backup verification used another recovery publication"
                    );
                }
            }
            ensure!(
                q::checked(
                    id,
                    h.sequence,
                    h.shard,
                    chrono::Utc::now().timestamp_micros()
                )
                .execute(&mut *tx)
                .await?
                .rows_affected()
                    == 1,
                Error::OperationAborted
            );
        }
        let complete = !q::outstanding(f.media, h.operation)
            .fetch_one(&mut *tx)
            .await?;
        super::queries::storage_capacity::stamp(
            h.operation,
            f.media,
            proof.receipt.accounting,
            complete,
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }
}
