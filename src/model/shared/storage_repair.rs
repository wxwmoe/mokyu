use super::{
    Backend,
    queries::{storage_payload as p, storage_repair as q},
    storage_payload,
};
use crate::{
    authorization::Principal,
    content::codec::Key,
    model::{
        Error,
        storage_repair::{Repair, Repairs},
        storage_transfer::Completion,
    },
};
use anyhow::{Context, Result, ensure};
use uuid::Uuid;

pub(super) fn decode(row: q::Row) -> Result<Repair> {
    row.0.validate()?;
    if let Some(candidate) = &row.1 {
        candidate.validate()?;
        let mut expected = row.0.header().clone();
        expected.fragment = candidate.fragment;
        expected.digest = row.0.evidence().header.digest;
        ensure!(
            candidate.layout_revision >= expected.layout_revision
                && (expected.media == candidate.media
                    || candidate.layout_revision > expected.layout_revision),
            "repair placement revision is invalid"
        );
        expected.media = candidate.media;
        expected.layout_revision = candidate.layout_revision;
        expected.generation = expected
            .generation
            .checked_add(1)
            .context("fragment generation exhausted")?;
        ensure!(
            expected == candidate.0,
            "repair candidate changed its content or layout"
        );
    }
    Ok(Repair {
        original: row.0.0,
        candidate: row.1.map(|v| v.0),
        state: row.2,
        original_removed: row.3,
        candidate_removed: row.4,
        protected_until: row.5,
        collecting: row.6,
    })
}

pub(super) async fn current(tx: &mut p::Tx<'_>, id: Uuid) -> Result<Repair> {
    decode(
        q::row(id)
            .fetch_optional(&mut **tx)
            .await?
            .ok_or(Error::OperationAborted)?,
    )
}

pub(super) async fn plan(
    tx: &mut p::Tx<'_>,
    id: Uuid,
) -> Result<Option<crate::model::storage_native::Fill>> {
    let operation = q::operation(id).fetch_one(&mut **tx).await?;
    let value = storage_payload::current(tx, operation).await?;
    let repair = current(tx, id).await?;
    let header = repair.original.header();
    if value.state != "published" || matches!(repair.state.as_str(), "installed" | "healthy") {
        return Ok(None);
    }
    p::protect(
        header.operation,
        chrono::Utc::now().timestamp_micros() + 90_000_000,
    )
    .execute(&mut **tx)
    .await?;
    Ok(Some(crate::model::storage_native::Fill {
        storage: header.namespace,
        operation: header.operation,
        sequence: header.sequence,
        repair: Some(id),
        backup: None,
        shard: header.shard,
    }))
}

pub(super) async fn garbage(
    tx: &mut p::Tx<'_>,
    operation: Uuid,
    after: Uuid,
) -> Result<Vec<crate::model::storage_payload::Mapping>> {
    let mut result = Vec::new();
    for row in q::garbage(operation, after).fetch_all(&mut **tx).await? {
        let repair = decode(row)?;
        if repair.state == "installed"
            && !repair.original_removed
            && repair.original.header().fragment > after
        {
            result.push(crate::model::storage_payload::Mapping {
                header: repair.original.header().clone(),
                receipt: repair.original.receipt().cloned(),
                deleted: false,
            });
        } else if repair.state == "writing"
            && !repair.candidate_removed
            && let Some(header) = repair.candidate
            && header.fragment > after
        {
            result.push(crate::model::storage_payload::Mapping {
                header,
                receipt: None,
                deleted: false,
            });
        }
    }
    Ok(result)
}

#[async_trait::async_trait]
impl Repairs for Backend {
    async fn request_repair(
        &self,
        principal: &Principal,
        cluster: Uuid,
        fragment: Uuid,
    ) -> Result<Uuid> {
        let mut tx = p::begin(self).await?;
        super::queries::storage_cluster::admin(&mut tx, principal).await?;
        let operation = q::locate(fragment)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(Error::OperationAborted)?;
        let value = storage_payload::current(&mut tx, operation).await?;
        ensure!(
            value.intent.cluster == cluster && value.state == "published",
            Error::OperationAborted
        );
        ensure!(
            super::queries::journal_restore::native_pending(value.intent.namespace)
                .fetch_optional(&mut *tx)
                .await?
                .is_none(),
            s3s::s3_error!(ServiceUnavailable, "storage takeover is not complete")
        );
        if let Some(row) = q::row(fragment).fetch_optional(&mut *tx).await? {
            let repair = decode(row)?;
            ensure!(
                repair.original.header().operation == operation,
                Error::OperationAborted
            );
            if repair.state == "healthy" {
                q::restart(fragment).execute(&mut *tx).await?;
                q::restart_task(fragment).execute(&mut *tx).await?;
            }
        } else {
            let mapping = storage_payload::mapping(
                p::fragment(fragment, operation).fetch_one(&mut *tx).await?,
            )?;
            let original = mapping
                .receipt
                .context("repair requires a published receipt")?;
            ensure!(
                !mapping.deleted
                    && original.header.layout.data_shards == 1
                    && original.header.layout.parity_shards == 0,
                "unsupported repair layout"
            );
            q::create(&crate::model::storage_repair::Origin::Durable(original))
                .execute(&mut *tx)
                .await?;
            p::enqueue_fill(fragment, value.intent.namespace)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(fragment)
    }

    async fn repair(&self, id: Uuid) -> Result<Repair> {
        let mut tx = p::begin(self).await?;
        let value = current(&mut tx, id).await?;
        tx.commit().await?;
        Ok(value)
    }

    async fn prepare_repair(&self, id: Uuid, damaged: bool) -> Result<Repair> {
        let observed = self.repair(id).await?;
        let header = observed.original.header();
        let mut tx = p::begin(self).await?;
        let allocation = super::storage_capacity::lock(self, &mut tx, header.cluster).await?;
        let payload = storage_payload::current(&mut tx, header.operation).await?;
        ensure!(
            super::queries::journal_restore::native_pending(payload.intent.namespace)
                .fetch_optional(&mut *tx)
                .await?
                .is_none(),
            Error::OperationAborted
        );
        ensure!(payload.state == "published", Error::OperationAborted);
        let mut repair = current(&mut tx, id).await?;
        ensure!(
            repair.original == observed.original,
            Error::OperationAborted
        );
        let movement = super::queries::storage_move::assigned(header.cluster, header.media)
            .fetch_optional(&mut *tx)
            .await?;
        ensure!(
            movement.as_ref().is_none_or(|job| job.5 != "paused"),
            Error::OperationAborted
        );
        if repair.state == "queued" {
            let mapped = storage_payload::mapping(
                p::fragment(id, header.operation)
                    .fetch_one(&mut *tx)
                    .await?,
            )?;
            ensure!(
                mapped.header == *repair.original.header()
                    && mapped.receipt.as_ref() == repair.original.receipt()
                    && !mapped.deleted,
                Error::OperationAborted
            );
            if damaged || movement.is_some() {
                let mut candidate = header.clone();
                candidate.digest = repair.original.evidence().header.digest;
                candidate.fragment = Uuid::now_v7();
                candidate.generation = candidate
                    .generation
                    .checked_add(1)
                    .context("fragment generation exhausted")?;
                if let Some(job) = &movement {
                    ensure!(job.2 == header.group, Error::OperationAborted);
                    candidate.media = job.4;
                    candidate.layout_revision = allocation.cluster.revision as u64;
                    let origin = p::manifest(header.cluster, payload.intent.layout_revision)
                        .fetch_one(&mut *tx)
                        .await?;
                    payload
                        .intent
                        .placement(&candidate, &origin, &allocation.cluster.manifest)?;
                }
                candidate.validate()?;
                allocation.reserve_repair(&mut tx, &candidate).await?;
                q::prepare(id, &candidate).execute(&mut *tx).await?;
                repair.candidate = Some(candidate);
                repair.state = "writing".into();
            } else {
                q::healthy(id).execute(&mut *tx).await?;
                repair.state = "healthy".into();
            }
        }
        p::renew(
            header.operation,
            chrono::Utc::now().timestamp_micros() + 90_000_000,
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(repair)
    }

    async fn resume_repair(
        &self,
        id: Uuid,
        until: i64,
    ) -> Result<Option<crate::model::storage_payload::Mapping>> {
        let now = chrono::Utc::now().timestamp_micros();
        ensure!(
            until > now && until <= now + 90_000_000,
            "invalid repair read lease"
        );
        let mut tx = p::begin(self).await?;
        let Some(operation) = q::operation(id).fetch_optional(&mut *tx).await? else {
            return Ok(None);
        };
        let payload = storage_payload::current(&mut tx, operation).await?;
        let repair = current(&mut tx, id).await?;
        if repair.state != "installed" {
            return Ok(None);
        }
        ensure!(
            repair.original.receipt().is_some()
                && !repair.original_removed
                && !repair.collecting
                && repair.protected_until > now
                && (payload.state == "published" || payload.protected_until > now),
            Error::OperationAborted
        );
        q::protect(id, until).execute(&mut *tx).await?;
        p::protect(operation, until).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(Some(crate::model::storage_payload::Mapping {
            header: repair.original.header().clone(),
            receipt: repair.original.receipt().cloned(),
            deleted: false,
        }))
    }

    async fn accept_repair(&self, key: &Key, id: Uuid, proof: &Completion) -> Result<()> {
        let receipt = &proof.receipt.receipt;
        let header = &receipt.header;
        let mut tx = p::begin(self).await?;
        let allocation = super::storage_capacity::lock(self, &mut tx, header.cluster).await?;
        let target = allocation
            .targets
            .iter()
            .find(|target| target.device.media_id == header.media)
            .context("repair target is unavailable")?;
        proof.verify(key, &allocation.cluster.manifest, target)?;
        let payload = storage_payload::current(&mut tx, header.operation).await?;
        ensure!(
            payload.state == "published"
                && payload.write_until > chrono::Utc::now().timestamp_micros(),
            Error::OperationAborted
        );
        let repair = current(&mut tx, id).await?;
        ensure!(
            repair.candidate.as_ref() == Some(header)
                && receipt.root == repair.original.evidence().root
                && header.digest == repair.original.evidence().header.digest,
            "repair bytes differ from the original receipt"
        );
        if repair.state == "installed" {
            tx.commit().await?;
            return Ok(());
        }
        ensure!(repair.state == "writing", Error::OperationAborted);
        let source = repair.original.header().media;
        if source != header.media {
            let job = super::queries::storage_move::assigned(header.cluster, source)
                .fetch_optional(&mut *tx)
                .await?;
            // A candidate already written before cancellation can finish, then be moved back.
            let allowed = job.is_some_and(|j| j.4 == header.media && j.5 != "paused")
                || super::queries::storage_move::assigned(header.cluster, header.media)
                    .fetch_optional(&mut *tx)
                    .await?
                    .is_some_and(|j| j.5 == "reverting" && j.4 == source);
            ensure!(allowed, Error::OperationAborted);
        }
        let mapped = storage_payload::mapping(
            p::fragment(id, header.operation)
                .fetch_one(&mut *tx)
                .await?,
        )?;
        ensure!(
            mapped.header == *repair.original.header()
                && mapped.receipt.as_ref() == repair.original.receipt()
                && !mapped.deleted,
            Error::OperationAborted
        );
        ensure!(
            q::replace(id, receipt, header.encode()?.to_vec())
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            Error::OperationAborted
        );
        q::installed(id, header, payload.protected_until)
            .execute(&mut *tx)
            .await?;
        if repair.original.header().media != header.media {
            if let Some(path) = super::queries::storage_move::root_path(header.operation)
                .fetch_optional(&mut *tx)
                .await?
            {
                super::queries::storage_journal::track(header.operation, header.namespace, &path)
                    .execute(&mut *tx)
                    .await?;
            }
        }
        super::queries::storage_journal::dirty(header.operation)
            .execute(&mut *tx)
            .await?;
        allocation
            .received(&mut tx, &payload.intent, std::slice::from_ref(proof))
            .await?;
        tx.commit().await?;
        Ok(())
    }
}
