use super::{
    Backend,
    queries::{journal, storage_journal as q, storage_payload as payload},
    storage_payload,
};
use crate::model::{
    journal::{
        native::{Capture, NativeJournal, SnapshotFile},
        publication::Allocation,
        record::Hold,
    },
    storage_recovery::Physical,
};
use anyhow::{Context, Result, ensure};
use uuid::Uuid;

async fn source(
    db: &mut super::queries::pack_work::Connection,
    hold: &Hold,
) -> Result<Option<(Uuid, String)>> {
    super::storage_native::source(db, hold.storage, hold.unit, hold.address).await
}

pub(super) async fn track(
    db: &mut super::queries::pack_work::Connection,
    hold: &Hold,
) -> Result<()> {
    if let Some((operation, path)) = source(db, hold).await? {
        q::track(operation, hold.storage, &path).execute(db).await?;
    }
    Ok(())
}

pub(super) async fn snapshot_confirmed(
    db: &mut super::queries::pack_work::Connection,
    job: &crate::model::journal::snapshots::Job,
    file: SnapshotFile,
    etag: &str,
) -> Result<()> {
    if q::identity(job.storage_id)
        .fetch_optional(&mut *db)
        .await?
        .is_none()
    {
        return Ok(());
    }
    let operation = Uuid::parse_str(etag.trim_matches('"'))?;
    let (proof, settled) = q::publication(operation).fetch_one(&mut *db).await?;
    ensure!(
        settled
            && proof.publication.namespace == job.storage_id
            && proof.publication.parts.len() == 1
            && proof.publication.parts[0].intent.operation == operation
            && proof.publication.parts[0].path == file.path(job)?,
        "snapshot recovery evidence must settle before its receipt"
    );
    ensure!(
        super::queries::storage_native::binding(operation)
            .fetch_one(db)
            .await?
            .0
            == job.id,
        "snapshot receipt belongs to another job"
    );
    Ok(())
}

async fn prepare(
    tx: &mut payload::Tx<'_>,
    storage: Uuid,
    id: Uuid,
    through: u64,
    part: &crate::model::storage_recovery::Part,
    retired: &[Uuid],
    key: &crate::content::codec::Key,
) -> Result<crate::model::storage_recovery::Proof> {
    use crate::model::storage_recovery::{Proof, Publication};
    if let Some((proof, _)) = q::publication(id).fetch_optional(&mut **tx).await? {
        ensure!(
            proof.publication.parts.as_slice() == std::slice::from_ref(part)
                && proof.publication.retired == retired,
            "native recovery publication changed"
        );
        return Ok(proof.0);
    }
    let (cluster, identity, prefix) = q::identity(storage).fetch_one(&mut **tx).await?;
    let manifest = payload::current_manifest(cluster)
        .fetch_one(&mut **tx)
        .await?;
    let ledger = journal::ledger(storage).fetch_one(&mut **tx).await?;
    let descriptor = crate::storage::marker::journal(&identity)?
        .context("native journal descriptor is missing")?;
    ensure!(
        ledger.descriptor.as_ref().map(|d| &d.0) == Some(&descriptor),
        "native journal ownership changed"
    );
    descriptor.unlock(storage, key)?;
    let value = storage_payload::current(tx, part.intent.operation).await?;
    ensure!(
        value.intent == part.intent
            && value.state != "retired"
            && payload::complete(part.intent.operation)
                .fetch_one(&mut **tx)
                .await? as u64
                == part.intent.required_files(),
        "native journal dependencies are incomplete"
    );
    let previous = q::latest(storage).fetch_optional(&mut **tx).await?;
    ensure!(
        previous.as_ref().is_none_or(|p| p.1),
        crate::model::Error::OperationAborted
    );
    let previous = previous.map(|p| p.0.0);
    let marker_path = if prefix.is_empty() {
        "mokyu.json".to_owned()
    } else {
        format!("{prefix}/mokyu.json")
    };
    let (marker_operation,) = q::source(
        storage,
        crate::model::storage_native::path(&marker_path)?.to_vec(),
        &marker_path,
    )
    .fetch_one(&mut **tx)
    .await?;
    let root = crate::model::storage_recovery::Root {
        intent: storage_payload::current(tx, marker_operation).await?.intent,
        receipts: q::fragments(marker_operation, Uuid::nil())
            .fetch_all(&mut **tx)
            .await?
            .into_iter()
            .map(|row| {
                storage_payload::mapping(row)?
                    .receipt
                    .context("root receipt missing")
            })
            .collect::<Result<_>>()?,
    };
    root.validate(storage, cluster)?;
    let proof = Proof::sign(
        Publication {
            format_version: 1,
            id,
            namespace: storage,
            cluster,
            manifest: manifest.digest()?,
            takeover_epoch: manifest.takeover_epoch,
            revision: previous.as_ref().map_or(Ok(1), |p| {
                p.publication
                    .revision
                    .checked_add(1)
                    .context("recovery publication exhausted")
            })?,
            previous: previous.as_ref().map(Proof::link).transpose()?,
            through,
            parts: vec![part.clone()],
            retired: retired.to_vec(),
            identity: Some(identity.0),
            root: Some(root),
            prefix,
        },
        key,
        &manifest,
    )?;
    proof.follows(previous.as_ref())?;
    q::prepare(&proof, manifest.revision)
        .execute(&mut **tx)
        .await?;
    Ok(proof)
}

async fn settle(
    tx: &mut payload::Tx<'_>,
    storage: Uuid,
    owner: Uuid,
    proof: &crate::model::storage_recovery::Proof,
) -> Result<()> {
    let (saved, _) = q::publication(proof.publication.id)
        .fetch_one(&mut **tx)
        .await?;
    ensure!(
        saved.0 == *proof && proof.publication.namespace == storage,
        "native recovery receipt changed"
    );
    let part = proof
        .publication
        .parts
        .first()
        .context("native recovery part missing")?;
    ensure!(
        proof.publication.parts.len() == 1 && !part.intent.members.is_empty(),
        "invalid native publication members"
    );
    for media in &part.intent.members {
        ensure!(
            q::evidence(storage, *media)
                .fetch_optional(&mut **tx)
                .await?
                .unwrap_or(0)
                >= proof.publication.revision as i64,
            "native recovery evidence is incomplete"
        );
    }
    super::storage_native::publish_in(tx, storage, part.intent.payload, owner).await?;
    q::settled(proof.publication.id).execute(&mut **tx).await?;
    if !proof.publication.retired.is_empty() {
        super::storage_backup::recheck(tx, &part.intent, chrono::Utc::now().timestamp_micros())
            .await?;
    }
    Ok(())
}

async fn capture(
    tx: &mut payload::Tx<'_>,
    storage: Uuid,
    operation: Uuid,
    revision: i64,
    after: Uuid,
    path: String,
    sequence: u64,
) -> Result<Capture> {
    let value = storage_payload::current(tx, operation).await?;
    ensure!(
        value.intent.namespace == storage && value.state != "prepared",
        "invalid captured native source"
    );
    let mut records = Vec::new();
    if after.is_nil() {
        records.push(Physical::Payload {
            sequence,
            intent: value.intent,
            path,
        });
    }
    let rows = q::fragments(operation, after).fetch_all(&mut **tx).await?;
    let complete = rows.len() < 254;
    let mut next = after;
    for row in rows {
        let receipt = storage_payload::mapping(row)?
            .receipt
            .context("captured receipt is missing")?;
        next = receipt.header.fragment;
        records.push(Physical::Fragment { sequence, receipt });
    }
    if complete {
        records.push(Physical::State {
            sequence,
            operation,
            published: value.state == "published",
        });
    }
    Ok(Capture {
        operation,
        revision,
        after,
        next,
        complete,
        records,
    })
}

fn retirement(operation: Uuid) -> Uuid {
    let digest = blake3::derive_key("mokyu/storage/recovery-retirement/v1", operation.as_bytes());
    Uuid::from_bytes(digest[..16].try_into().unwrap())
}

pub(super) async fn garbage_confirmed(
    tx: &mut payload::Tx<'_>,
    file: &crate::model::journal::snapshots::Garbage,
) -> Result<()> {
    if q::identity(file.storage_id)
        .fetch_optional(&mut **tx)
        .await?
        .is_none()
    {
        return Ok(());
    }
    let operation = Uuid::parse_str(file.etag.trim_matches('"'))?;
    ensure!(
        q::garbage(file.storage_id, operation)
            .fetch_one(&mut **tx)
            .await?
            == *file,
        "native garbage receipt changed"
    );
    let (proof, settled) = q::publication(retirement(operation))
        .fetch_one(&mut **tx)
        .await?;
    ensure!(
        settled
            && proof.publication.namespace == file.storage_id
            && proof.publication.retired == [operation],
        "native retirement evidence is incomplete"
    );
    ensure!(
        storage_payload::current(tx, operation).await?.state == "retired"
            && payload::garbage(operation, Uuid::nil())
                .fetch_all(&mut **tx)
                .await?
                .is_empty(),
        "native recovery fragments have not been reclaimed"
    );
    Ok(())
}

#[async_trait::async_trait]
impl NativeJournal for Backend {
    async fn native_restore_publication(
        &self,
        job: Uuid,
        token: Uuid,
        proof: &crate::model::storage_recovery::Proof,
        manifest: &crate::model::storage_cluster::Manifest,
    ) -> Result<()> {
        use super::queries::journal_restore as restore;
        proof.verify(manifest)?;
        let mut tx = payload::begin(self).await?;
        restore::fence(job, token).fetch_one(&mut *tx).await?;
        let value = restore::job(job).fetch_one(&mut *tx).await?;
        ensure!(
            value.source_storage == proof.publication.namespace
                && value.target_storage == Some(value.source_storage),
            "invalid native recovery scope"
        );
        if let Some((saved, settled)) = q::publication(proof.publication.id)
            .fetch_optional(&mut *tx)
            .await?
        {
            ensure!(
                saved.0 == *proof && settled,
                "recovered publication conflicts"
            );
        } else {
            let previous = q::latest(value.source_storage)
                .fetch_optional(&mut *tx)
                .await?;
            proof.follows(previous.as_ref().map(|p| &p.0.0))?;
            q::prepare(proof, manifest.revision)
                .execute(&mut *tx)
                .await?;
            q::settled(proof.publication.id).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn native_restore_receipt(
        &self,
        job: Uuid,
        token: Uuid,
        media: Uuid,
        head: &crate::model::storage_recovery::Proof,
    ) -> Result<()> {
        use super::queries::journal_restore as restore;
        let mut tx = payload::begin(self).await?;
        restore::fence(job, token).fetch_one(&mut *tx).await?;
        let value = restore::job(job).fetch_one(&mut *tx).await?;
        ensure!(
            value.source_storage == head.publication.namespace
                && value.target_storage == Some(value.source_storage),
            "invalid recovered receipt scope"
        );
        let (saved, _) = q::publication(head.publication.id)
            .fetch_one(&mut *tx)
            .await?;
        ensure!(
            saved.0 == *head,
            "recovered media has a competing publication"
        );
        q::ensure_evidence(value.source_storage, media)
            .execute(&mut *tx)
            .await?;
        q::restore_receipt(value.source_storage, media, head.publication.revision)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn native_takeover_prepare(
        &self,
        job: Uuid,
        token: Uuid,
        part: &crate::model::storage_recovery::Part,
        identity: &serde_json::Value,
        key: &crate::content::codec::Key,
    ) -> Result<crate::model::storage_recovery::Proof> {
        use super::queries::journal_restore as restore;
        let mut tx = payload::begin(self).await?;
        restore::fence(job, token).fetch_one(&mut *tx).await?;
        let value = restore::job(job).fetch_one(&mut *tx).await?;
        let unlocked = crate::model::journal::restore::Unlocked::open(&value.source, job, key)?;
        let native = unlocked
            .native
            .as_ref()
            .context("missing native recovery source")?;
        let claim = unlocked
            .claim
            .as_ref()
            .context("missing native takeover intent")?;
        ensure!(
            value.source_storage == part.intent.namespace
                && value.target_storage == Some(value.source_storage)
                && native.head.publication.through == unlocked.cut.point.sequence,
            "invalid native takeover scope"
        );
        part.inspect(claim.bytes())?;
        ensure!(
            part.path == claim.path()
                && part.intent.payload == claim.descriptor.writer_epoch
                && crate::storage::marker::journal(identity)?.as_ref() == Some(&claim.descriptor),
            "native takeover differs from its intent"
        );
        let mut expected = unlocked.source.connection.identity_marker.clone();
        expected["metadata_journal"] = serde_json::to_value(&claim.descriptor)?;
        crate::storage::marker::verify(identity, &expected)?;
        let storage = value.source_storage;
        let (_, current, _) = q::identity(storage).fetch_one(&mut *tx).await?;
        ensure!(
            crate::storage::marker::verify(&current, &unlocked.source.connection.identity_marker)
                .is_ok()
                || crate::storage::marker::verify(&current, &expected).is_ok(),
            "native recovery ownership changed"
        );
        q::restore_identity(storage, identity)
            .execute(&mut *tx)
            .await?;
        journal::ensure_store(storage).execute(&mut *tx).await?;
        restore::adopt(&claim.descriptor, storage, &claim.descriptor)
            .execute(&mut *tx)
            .await?;
        let proof = prepare(&mut tx, storage, part.intent.operation, 0, part, &[], key).await?;
        tx.commit().await?;
        Ok(proof)
    }

    async fn native_takeover_acknowledge(
        &self,
        job: Uuid,
        token: Uuid,
        proof: &crate::model::storage_recovery::Proof,
    ) -> Result<()> {
        let mut tx = payload::begin(self).await?;
        super::queries::journal_restore::fence(job, token)
            .fetch_one(&mut *tx)
            .await?;
        ensure!(
            super::queries::journal_restore::native_pending(proof.publication.namespace)
                .fetch_one(&mut *tx)
                .await?
                == job,
            "native recovery scope changed"
        );
        settle(&mut tx, proof.publication.namespace, job, proof).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn native_gc_prepare(
        &self,
        token: Uuid,
        file: &crate::model::journal::snapshots::Garbage,
        key: &crate::content::codec::Key,
    ) -> Result<crate::model::storage_recovery::Proof> {
        use super::queries::journal_snapshots as snapshots;
        let storage = file.storage_id;
        let operation = Uuid::parse_str(file.etag.trim_matches('"'))?;
        let id = retirement(operation);
        let mut tx = payload::begin(self).await?;
        snapshots::fence(storage, token).fetch_one(&mut *tx).await?;
        let actual = q::garbage(storage, operation)
            .fetch_one(&mut *tx)
            .await
            .context("native garbage is not eligible for reclamation")?;
        ensure!(actual == *file, "native garbage receipt changed");
        if let Some((proof, _)) = q::publication(id).fetch_optional(&mut *tx).await? {
            ensure!(
                proof.publication.retired == [operation],
                "native retirement changed"
            );
            tx.commit().await?;
            return Ok(proof.0);
        }
        ensure!(
            !snapshots::pinned(storage).fetch_one(&mut *tx).await?,
            crate::model::Error::OperationAborted
        );
        let (job, checkpoint, index) = q::checkpoint(storage).fetch_one(&mut *tx).await?;
        let index_operation = Uuid::parse_str(index.trim_matches('"'))?;
        let path = crate::storage::journal::snapshot::index_path();
        ensure!(
            q::source(
                storage,
                crate::model::storage_native::path(&path)?.to_vec(),
                &path
            )
            .fetch_one(&mut *tx)
            .await?
            .0 == index_operation,
            "native checkpoint changed; journal GC paused"
        );
        let checkpoint = Uuid::parse_str(checkpoint.trim_matches('"'))?;
        let (proof, settled) = q::publication(checkpoint).fetch_one(&mut *tx).await?;
        let current = snapshots::job(job).fetch_one(&mut *tx).await?;
        ensure!(
            settled
                && proof.publication.parts.len() == 1
                && proof.publication.parts[0].path == SnapshotFile::Checkpoint.path(&current)?,
            "native checkpoint evidence is incomplete"
        );
        let (head,) = q::source(
            storage,
            crate::model::storage_native::path(&file.file_path)?.to_vec(),
            &file.file_path,
        )
        .fetch_one(&mut *tx)
        .await?;
        ensure!(head == operation, "native garbage object changed");
        let ledger = journal::ledger(storage).fetch_one(&mut *tx).await?;
        let proof = prepare(
            &mut tx,
            storage,
            id,
            ledger.emitted_seq.try_into()?,
            &proof.publication.parts[0],
            &[operation],
            key,
        )
        .await?;
        tx.commit().await?;
        Ok(proof)
    }

    async fn native_gc_pending(
        &self,
        storage: Uuid,
        token: Uuid,
    ) -> Result<Option<crate::model::storage_recovery::Proof>> {
        let mut tx = payload::begin(self).await?;
        super::queries::journal_snapshots::fence(storage, token)
            .fetch_one(&mut *tx)
            .await?;
        let value = q::latest(storage).fetch_optional(&mut *tx).await?;
        tx.commit().await?;
        Ok(value.and_then(|(proof, settled)| {
            (!settled && !proof.publication.retired.is_empty()).then_some(proof.0)
        }))
    }

    async fn native_gc_acknowledge(
        &self,
        token: Uuid,
        proof: &crate::model::storage_recovery::Proof,
    ) -> Result<()> {
        let storage = proof.publication.namespace;
        ensure!(
            proof.publication.retired.len() == 1 && proof.publication.parts.len() == 1,
            "invalid native retirement"
        );
        let operation = proof.publication.retired[0];
        ensure!(
            proof.publication.id == retirement(operation),
            "invalid native retirement identity"
        );
        let mut tx = payload::begin(self).await?;
        super::queries::journal_snapshots::fence(storage, token)
            .fetch_one(&mut *tx)
            .await?;
        let (saved, settled) = q::publication(proof.publication.id)
            .fetch_one(&mut *tx)
            .await?;
        ensure!(saved.0 == *proof, "native retirement receipt changed");
        if !settled {
            ensure!(
                !super::queries::journal_snapshots::pinned(storage)
                    .fetch_one(&mut *tx)
                    .await?,
                crate::model::Error::OperationAborted
            );
            let file = q::garbage(storage, operation).fetch_one(&mut *tx).await?;
            let owner = super::queries::storage_native::binding(
                proof.publication.parts[0].intent.operation,
            )
            .fetch_one(&mut *tx)
            .await?
            .0;
            settle(&mut tx, storage, owner, proof).await?;
            super::storage_native::remove_in(
                &mut tx,
                storage,
                &file.file_path,
                Some(operation),
                None,
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn native_snapshot_material(
        &self,
        job: &crate::model::journal::snapshots::Job,
        token: Uuid,
        hold: &Hold,
        after: Option<(Uuid, i64, Uuid)>,
    ) -> Result<Capture> {
        use super::queries::journal_snapshots as snapshots;
        ensure!(
            hold.storage == job.storage_id,
            "snapshot material belongs to another backend"
        );
        let mut tx = payload::begin(self).await?;
        snapshots::fence(job.storage_id, token)
            .fetch_one(&mut *tx)
            .await?;
        let actual = snapshots::job(job.id).fetch_one(&mut *tx).await?;
        ensure!(
            actual.storage_id == job.storage_id
                && actual.state == "building"
                && actual.revision == job.revision,
            "snapshot material cut changed"
        );
        let (operation, path) = source(&mut tx, hold)
            .await?
            .context("snapshot source is not native")?;
        let revision = q::source_revision(operation).fetch_one(&mut *tx).await?;
        ensure!(
            after.is_none_or(|(old, rev, _)| old == operation && rev == revision),
            crate::model::Error::OperationAborted
        );
        let result = capture(
            &mut tx,
            job.storage_id,
            operation,
            revision,
            after.map_or(Uuid::nil(), |c| c.2),
            path,
            job.sequence_no.try_into()?,
        )
        .await?;
        tx.commit().await?;
        Ok(result)
    }

    async fn native_snapshot_prepare(
        &self,
        job: &crate::model::journal::snapshots::Job,
        token: Uuid,
        file: SnapshotFile,
        part: &crate::model::storage_recovery::Part,
        key: &crate::content::codec::Key,
    ) -> Result<crate::model::storage_recovery::Proof> {
        use super::queries::journal_snapshots as snapshots;
        part.validate(job.storage_id)?;
        ensure!(part.path == file.path(job)?, "native snapshot path changed");
        let mut tx = payload::begin(self).await?;
        snapshots::fence(job.storage_id, token)
            .fetch_one(&mut *tx)
            .await?;
        let actual = snapshots::job(job.id).fetch_one(&mut *tx).await?;
        ensure!(
            actual.storage_id == job.storage_id
                && actual.revision == job.revision
                && actual.epoch == job.epoch
                && actual.state == "prepared",
            "native snapshot changed"
        );
        if let Some((proof, _)) = q::publication(part.intent.operation)
            .fetch_optional(&mut *tx)
            .await?
        {
            ensure!(
                proof.publication.namespace == job.storage_id
                    && proof.publication.parts.as_slice() == std::slice::from_ref(part),
                "snapshot publication changed"
            );
            tx.commit().await?;
            return Ok(proof.0);
        }
        let bytes = match file {
            SnapshotFile::Page(number) => {
                snapshots::bytes(job.id, i64::from(number))
                    .fetch_one(&mut *tx)
                    .await?
            }
            _ => {
                let (manifest, index, checkpoint) =
                    snapshots::blobs(job.id).fetch_one(&mut *tx).await?;
                match file {
                    SnapshotFile::Manifest => manifest,
                    SnapshotFile::Index => index,
                    SnapshotFile::Checkpoint => checkpoint,
                    _ => unreachable!(),
                }
            }
        };
        part.inspect(&bytes)?;
        let public = crate::storage::journal::recovery::inspect_prefix(&bytes)?;
        ensure!(
            public.envelope.batch == job.id && Some(public.epoch) == job.epoch,
            "snapshot payload belongs to another job"
        );
        let ledger = journal::ledger(job.storage_id).fetch_one(&mut *tx).await?;
        ensure!(
            ledger.emitted_seq >= job.sequence_no,
            "snapshot exceeds the journal watermark"
        );
        let proof = prepare(
            &mut tx,
            job.storage_id,
            part.intent.operation,
            ledger.emitted_seq.try_into()?,
            part,
            &[],
            key,
        )
        .await?;
        tx.commit().await?;
        Ok(proof)
    }

    async fn native_snapshot_acknowledge(
        &self,
        job: &crate::model::journal::snapshots::Job,
        token: Uuid,
        file: SnapshotFile,
        proof: &crate::model::storage_recovery::Proof,
    ) -> Result<()> {
        use super::queries::journal_snapshots as snapshots;
        let mut tx = payload::begin(self).await?;
        snapshots::fence(job.storage_id, token)
            .fetch_one(&mut *tx)
            .await?;
        let actual = snapshots::job(job.id).fetch_one(&mut *tx).await?;
        ensure!(
            actual.storage_id == job.storage_id
                && actual.state == "prepared"
                && actual.revision == job.revision,
            "native snapshot is no longer prepared"
        );
        let part = proof
            .publication
            .parts
            .first()
            .context("snapshot recovery part missing")?;
        ensure!(
            part.path == file.path(job)?,
            "snapshot receipt path changed"
        );
        settle(&mut tx, job.storage_id, job.id, proof).await?;
        let version = part.intent.operation.to_string();
        let tag = format!("\"{version}\"");
        match file {
            SnapshotFile::Page(number) => {
                snapshots::receipt(&tag, Some(&version), job.id, i64::from(number))
                    .execute(&mut *tx)
                    .await?;
            }
            SnapshotFile::Manifest => {
                snapshots::manifest(&tag, Some(&version), job.id)
                    .execute(&mut *tx)
                    .await?;
            }
            _ => {}
        }
        tx.commit().await?;
        Ok(())
    }

    async fn native_prepare(
        &self,
        storage: Uuid,
        token: Uuid,
        file: &crate::model::journal::publication::File,
        part: &crate::model::storage_recovery::Part,
        key: &crate::content::codec::Key,
    ) -> Result<crate::model::storage_recovery::Proof> {
        part.validate(storage)?;
        ensure!(
            part.intent.payload == file.id
                && part.path == file.path()?
                && part.intent.total == file.byte_count as u64,
            "recovery part differs from its journal file"
        );
        let mut tx = payload::begin(self).await?;
        journal::fence(storage, token).fetch_one(&mut *tx).await?;
        let actual = journal::file(storage, file.id).fetch_one(&mut *tx).await?;
        ensure!(
            actual.state == "sealed" || actual.state == "published",
            "unsealed native journal"
        );
        ensure!(
            actual.digest == file.digest
                && actual.last_seq == file.last_seq
                && actual.byte_count == file.byte_count,
            "native journal file changed"
        );
        let proof = prepare(
            &mut tx,
            storage,
            file.id,
            file.last_seq.try_into()?,
            part,
            &[],
            key,
        )
        .await?;
        tx.commit().await?;
        Ok(proof)
    }

    async fn native_history(
        &self,
        storage: Uuid,
        after: u64,
        through: u64,
    ) -> Result<
        Vec<(
            crate::model::storage_recovery::Proof,
            crate::model::storage_cluster::Manifest,
        )>,
    > {
        ensure!(
            after <= through && through <= i64::MAX as u64,
            "invalid native evidence window"
        );
        let mut tx = payload::begin(self).await?;
        let rows = q::history(storage, after, through)
            .fetch_all(&mut *tx)
            .await?;
        let mut result = Vec::new();
        for (proof, manifest) in rows {
            proof.verify(&manifest)?;
            result.push((proof.0, manifest.0));
        }
        tx.commit().await?;
        Ok(result)
    }

    async fn native_evidence(&self, storage: Uuid, media: Uuid) -> Result<u64> {
        let mut tx = payload::begin(self).await?;
        let revision = q::evidence(storage, media)
            .fetch_optional(&mut *tx)
            .await?
            .unwrap_or(0);
        tx.commit().await?;
        Ok(revision.try_into()?)
    }

    async fn native_receipt(
        &self,
        storage: Uuid,
        token: Uuid,
        media: Uuid,
        revision: u64,
    ) -> Result<()> {
        ensure!(
            !media.is_nil() && revision > 0 && revision <= i64::MAX as u64,
            "invalid native evidence receipt"
        );
        let mut tx = payload::begin(self).await?;
        if let Some(job) = super::queries::journal_restore::native_pending(storage)
            .fetch_optional(&mut *tx)
            .await?
        {
            super::queries::journal_restore::fence(job, token)
                .fetch_one(&mut *tx)
                .await?;
        } else {
            q::fence(storage, token).fetch_one(&mut *tx).await?;
        }
        ensure!(
            q::history(storage, revision - 1, revision)
                .fetch_optional(&mut *tx)
                .await?
                .is_some(),
            "unknown recovery publication"
        );
        q::ensure_evidence(storage, media).execute(&mut *tx).await?;
        if q::evidence(storage, media).fetch_one(&mut *tx).await? < revision as i64 {
            ensure!(
                q::receipt(storage, media, revision)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected()
                    == 1,
                "recovery receipt has a gap"
            );
        }
        tx.commit().await?;
        Ok(())
    }

    async fn native_acknowledge(
        &self,
        storage: Uuid,
        token: Uuid,
        file: &crate::model::journal::publication::File,
        proof: &crate::model::storage_recovery::Proof,
    ) -> Result<()> {
        let mut tx = payload::begin(self).await?;
        journal::fence(storage, token).fetch_one(&mut *tx).await?;
        ensure!(
            proof.publication.id == file.id && proof.publication.through == file.last_seq as u64,
            "native journal receipt changed"
        );
        settle(&mut tx, storage, file.id, proof).await?;
        let part = &proof.publication.parts[0];
        let version = part.intent.operation.to_string();
        super::journal::acknowledge(
            &mut tx,
            storage,
            token,
            file,
            Some(&format!("\"{version}\"")),
            Some(&version),
        )
        .await?;
        q::confirm_sources(file.id).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn native_capture(&self, storage: Uuid, token: Uuid) -> Result<Option<Capture>> {
        let mut tx = payload::begin(self).await?;
        journal::fence(storage, token).fetch_one(&mut *tx).await?;
        let ledger = journal::ledger(storage).fetch_one(&mut *tx).await?;
        let Some((operation, revision, after, path)) =
            q::pending(storage).fetch_optional(&mut *tx).await?
        else {
            tx.commit().await?;
            return Ok(None);
        };
        let sequence = u64::try_from(ledger.allocated_seq)?
            .checked_add(1)
            .context("journal sequence exhausted")?;
        let captured =
            capture(&mut tx, storage, operation, revision, after, path, sequence).await?;
        tx.commit().await?;
        Ok(Some(captured))
    }

    async fn native_allocate(
        &self,
        storage: Uuid,
        token: Uuid,
        capture: &Capture,
        plan: &Allocation,
    ) -> Result<()> {
        ensure!(
            !plan.files.is_empty() && plan.pages.is_empty(),
            "native mapping allocation must precede business pages"
        );
        let mut tx = payload::begin(self).await?;
        super::journal::allocate(&mut tx, storage, token, plan).await?;
        ensure!(
            q::captured(storage, capture, plan.files.last().unwrap().id)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            "native source changed during journal capture"
        );
        tx.commit().await?;
        Ok(())
    }
}
