use super::Format;
use crate::storage::marker;
use crate::{
    config::Secrets,
    model::{
        StorageProfileStore, StorageRecordStore,
        journal::{
            JournalStore, Ledger,
            publication::{File, Part},
        },
    },
    storage::{
        external::{
            Client, Connection,
            objects::{Condition, Written},
        },
        model::{self, Options},
        s3::UploadedPart,
    },
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures_util::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use std::{collections::BTreeMap, sync::Arc};
use uuid::Uuid;

pub(crate) struct Remote {
    id: Uuid,
    pub journal: marker::Journal,
    pub key: crate::content::codec::Key,
    pub backend: Arc<dyn ObjectStore>,
    pub native: Option<Arc<crate::storage::native::store::Store>>,
    client: Option<Client>,
    pub local: Option<Arc<super::local::Local>>,
    pub revision: i64,
}

impl Remote {
    pub fn format(&self) -> Format<'_> {
        Format {
            storage: self.id,
            encrypted: true,
            journal: self.journal.journal_id,
            epoch: self.journal.writer_epoch,
            key: &self.key,
        }
    }

    pub async fn connect(
        db: &dyn JournalStore,
        records: &dyn StorageRecordStore,
        profiles: &dyn StorageProfileStore,
        secrets: &Secrets,
        ledger: &Ledger,
        token: Uuid,
        native: Option<Arc<crate::storage::native::store::Store>>,
    ) -> Result<Self> {
        let row = records
            .enabled_by_id(ledger.storage_id)
            .await?
            .context("metadata journal storage is disabled")?;
        let mut revision = row.revision;
        let record = model::Record::from(row);
        let profile = profiles.load(record.id).await?;
        let opts = Options::from_profile(&profile)?;
        let conn = model::Connection::unprotect(&record, &secrets.credential_key)?;
        ensure!(
            (conn.r#type == "mokyu") == native.is_some(),
            "native journal connection is missing"
        );
        let backend: Arc<dyn ObjectStore> = match &native {
            Some(store) => {
                ensure!(
                    store.namespace == record.id
                        && Some(store.cluster) == record.cluster_id
                        && store.prefix == Path::parse(&conn.prefix)?,
                    "native journal connection changed"
                );
                store.clone()
            }
            None => model::Store::connect(&conn, opts)?.backend,
        };
        let local = super::local::Local::new(&conn)?.map(Arc::new);
        let _lock = match &local {
            Some(l) => Some(l.lock(true).await?),
            None => None,
        };
        let client = if conn.r#type == "s3" {
            Some(
                Connection {
                    endpoint_url: conn.endpoint_url.context("missing S3 endpoint")?,
                    region: conn.region.context("missing S3 region")?,
                    bucket: conn.remote_bucket_name.context("missing S3 bucket")?,
                    prefix: conn.prefix,
                    virtual_hosted: false,
                    allow_http: true,
                }
                .connect(
                    conn.access.as_deref().context("missing S3 access")?,
                    conn.secret.as_deref().context("missing S3 secret")?,
                    opts,
                )?,
            )
        } else {
            None
        };
        let doc = marker::Document::probe(backend.as_ref())
            .await?
            .context("missing storage marker")?;
        let existing = marker::journal(&doc.value)?;
        let mut actual_identity = doc.value.clone();
        let mut registered_identity = record.identity_marker.clone();
        for value in [&mut actual_identity, &mut registered_identity] {
            value
                .as_object_mut()
                .context("invalid storage marker")?
                .remove("metadata_journal");
        }
        marker::verify(&actual_identity, &registered_identity)?;
        let journal = if let Some(value) = &ledger.descriptor {
            value.0.clone()
        } else {
            ensure!(existing.is_none(), "existing journal requires recovery");
            let journal = marker::Journal::create(record.id, &secrets.credential_key)?;
            db.descriptor(record.id, token, &journal).await?;
            journal
        };
        let key = journal.unlock(record.id, &secrets.credential_key)?;
        let updated = match existing {
            Some(other) => {
                ensure!(other == journal, "journal identity or ownership changed");
                doc.value
            }
            None if let Some(local) = &local => {
                doc.set_local_locked(backend.as_ref(), local, &journal)
                    .await?
            }
            None => doc.set_journal(backend.as_ref(), &journal).await?,
        };
        if updated != record.identity_marker || !ledger.marker_installed {
            db.installed(record.id, token, &record.identity_marker, &updated)
                .await?;
            revision = records
                .enabled_by_id(record.id)
                .await?
                .context("journal storage disabled during activation")?
                .revision;
        }
        Ok(Self {
            id: record.id,
            journal,
            key,
            backend,
            native,
            client,
            local,
            revision,
        })
    }

    async fn existing(&self, path: &str, expected: &[u8]) -> Result<Option<Written>> {
        let object = match self.backend.get(&Path::from(path)).await {
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            result => result?,
        };
        ensure!(
            object.meta.size == expected.len() as u64,
            "journal path has conflicting size"
        );
        let etag = object
            .meta
            .e_tag
            .clone()
            .context("journal object has no ETag")?;
        let version = object.meta.version.clone();
        let mut stream = object.into_stream();
        let mut at = 0usize;
        while let Some(part) = stream.try_next().await? {
            let end = at
                .checked_add(part.len())
                .context("journal length overflow")?;
            ensure!(
                expected.get(at..end) == Some(part.as_ref()),
                "journal path has conflicting contents"
            );
            at = end;
        }
        ensure!(at == expected.len(), "truncated journal object");
        Ok(Some(Written { etag, version }))
    }

    pub(crate) async fn publish_native(
        &self,
        db: &dyn JournalStore,
        token: Uuid,
        file: &File,
        bytes: Bytes,
        credential: &crate::content::codec::Key,
    ) -> Result<bool> {
        let native = self
            .native
            .as_ref()
            .context("native journal connection missing")?;
        let storage = self.id;
        let path = file.path()?;
        let size = usize::try_from(file.byte_count)?;
        ensure!(bytes.len() == size, "native journal file is incomplete");
        use crate::{
            content::fragment::Area,
            model::{storage_native::RecoveryIdentity, storage_recovery::Part},
            storage::native::store::Publication,
        };
        use futures_util::StreamExt;
        ensure!(
            file.mode == "put" && file.state == "sealed",
            "native journal must be sealed before writing"
        );
        let (last, digest) = super::verify_frames(
            &self.format(),
            &path,
            &bytes,
            super::Sequence {
                first: file.first_seq.try_into()?,
                last: (file.first_seq - 1).try_into()?,
                previous: file.previous_digest.as_slice().try_into()?,
            },
            false,
            |_| Ok(()),
        )?;
        ensure!(
            last == file.last_seq as u64 && digest.as_slice() == file.digest,
            "native journal chain differs from its ledger"
        );
        let mut options = object_store::PutOptions {
            mode: object_store::PutMode::Create,
            ..Default::default()
        };
        options.extensions.insert(Publication {
            owner: file.id,
            area: Area::Recovery,
        });
        options.extensions.insert(RecoveryIdentity {
            payload: file.id,
            revision: 1,
        });
        let intent = native
            .write(
                &Path::from(path.clone()),
                size as u64,
                futures_util::stream::iter([Ok(bytes.clone())]).boxed(),
                options,
            )
            .await?;
        let part = Part {
            intent,
            path,
            digest: *blake3::hash(&bytes).as_bytes(),
        };
        part.inspect(&bytes)?;
        let proof = db
            .native_prepare(storage, token, file, &part, credential)
            .await?;
        native.publish_evidence(db, token, &proof).await?;
        db.native_acknowledge(storage, token, file, &proof).await?;
        Ok(true)
    }

    pub async fn publish(
        &self,
        db: &dyn JournalStore,
        space: &crate::storage::Storage,
        storage: Uuid,
        token: Uuid,
        file: &File,
        credential: &crate::content::codec::Key,
    ) -> Result<bool> {
        let path = file.path()?;
        let frames = db.file_frames(file.id).await?;
        let size = usize::try_from(file.byte_count)?;
        ensure!(
            size <= crate::model::journal::policy::MAX_LIMIT as usize,
            "journal file exceeds limit"
        );
        if let Some(local) = &self.local {
            let pending: usize = frames.iter().map(Vec::len).sum();
            let start = size
                .checked_sub(pending)
                .context("invalid pending local frames")?;
            let guard = local.lock(true).await?;
            let target = space.store_backend(storage)?;
            let physical = local.root.join(&path);
            let active = physical.with_extension("part");
            let existing =
                super::super::data_space::allocated_files(&[physical.clone(), active.clone()])?;
            let mut reservation =
                space.local_space(&target, (size as u64).saturating_sub(existing) + 8192)?;
            if let Some(reservation) = &mut reservation {
                let head = local
                    .root
                    .join(super::local::head_path(self.journal.writer_epoch));
                let temporary = head.parent().unwrap().join(".mokyu-journal.tmp");
                reservation.observe(vec![physical, active, head, temporary])?;
            }
            let (local, segment, key, journal) = (
                local.clone(),
                file.clone(),
                self.key.clone(),
                self.journal.clone(),
            );
            let storage_id = self.id;
            tokio::task::spawn_blocking(move || {
                let (_guard, _space) = (guard, reservation);
                local.verify_owner(&journal)?;
                local.append(
                    &Format {
                        storage: storage_id,
                        encrypted: segment.encrypt_frames,
                        journal: journal.journal_id,
                        epoch: journal.writer_epoch,
                        key: &key,
                    },
                    &segment,
                    start as u64,
                    &frames.concat(),
                )
            })
            .await??;
            let etag = if file.state == "sealed" {
                Some(
                    self.backend
                        .head(&Path::from(path))
                        .await?
                        .e_tag
                        .context("missing local segment revision")?,
                )
            } else {
                None
            };
            db.acknowledge(storage, token, file, etag.as_deref(), None)
                .await?;
            return Ok(true);
        }
        let mut body = Vec::with_capacity(size);
        let mut cuts = Vec::new();
        for frame in frames {
            ensure!(
                frame.len() <= size - body.len(),
                "journal file length mismatch"
            );
            let boundary = super::FrameType::read(&frame)? == super::FrameType::PartCommit;
            body.extend_from_slice(&frame);
            if boundary {
                cuts.push(body.len());
            }
        }
        ensure!(body.len() == size, "journal file is incomplete");
        let bytes = Bytes::from(body);
        if self.native.is_some() {
            return self
                .publish_native(db, token, file, bytes, credential)
                .await;
        }
        let client = self.client.as_ref().context("missing S3 journal client")?;
        let written = if file.mode == "put" {
            ensure!(file.state == "sealed", "unsealed journal PUT");
            match client
                .put(
                    &path,
                    &Condition::Absent,
                    &BTreeMap::new(),
                    None,
                    bytes.clone(),
                )
                .await
            {
                Ok(written) => written,
                Err(error) => self.existing(&path, &bytes).await?.ok_or(error)?,
            }
        } else {
            if file.state == "sealed" {
                cuts.push(size);
            }
            if cuts.is_empty() {
                return Ok(false);
            }
            let upload = match &file.upload_id {
                Some(id) => id.clone(),
                None => {
                    let recovered = if db.start_upload(storage, file.id, token).await? {
                        None
                    } else {
                        let (previous, truncated) = client.uploads(&path).await?;
                        ensure!(
                            !truncated && previous.len() <= 1,
                            "ambiguous journal Multipart uploads"
                        );
                        previous.into_iter().next()
                    };
                    let id = match recovered {
                        Some(id) => id,
                        None => client.start_upload(&path, &BTreeMap::new(), None).await?,
                    };
                    db.upload_id(storage, file.id, token, &id).await?;
                    id
                }
            };
            let receipts = db.parts(file.id).await?;
            let mut parts = Vec::new();
            let mut start = 0;
            for (i, end) in cuts.into_iter().enumerate() {
                let number = u16::try_from(i + 1)?;
                let data = bytes.slice(start..end);
                if let Some(old) = receipts.get(i)
                    && let Some(etag) = &old.etag
                {
                    ensure!(
                        old.number == i32::from(number)
                            && old.byte_offset == start as i64
                            && old.byte_count == data.len() as i64
                            && old.digest == blake3::hash(&data).as_bytes(),
                        "invalid journal part receipt"
                    );
                    parts.push(UploadedPart {
                        number,
                        size: data.len() as u64,
                        etag: etag.clone(),
                    });
                } else {
                    let mut intent = Part {
                        file_id: file.id,
                        number: i32::from(number),
                        byte_offset: start as i64,
                        byte_count: data.len() as i64,
                        digest: blake3::hash(&data).as_bytes().to_vec(),
                        etag: None,
                    };
                    db.part(storage, token, &intent).await?;
                    let result = client.upload_part(&path, &upload, number, data).await?;
                    intent.etag = Some(result.etag.clone());
                    db.part(storage, token, &intent).await?;
                    parts.push(result);
                }
                start = end;
            }
            if file.state != "sealed" {
                return Ok(parts.len() > receipts.iter().filter(|p| p.etag.is_some()).count());
            }
            match client
                .complete_upload(&path, &upload, &parts, &Condition::Absent)
                .await
            {
                Ok(written) => written,
                Err(error) => self.existing(&path, &bytes).await?.ok_or(error)?,
            }
        };
        db.acknowledge(
            storage,
            token,
            file,
            Some(&written.etag),
            written.version.as_deref(),
        )
        .await?;
        Ok(true)
    }
}

pub(crate) async fn resume_markers(stores: &crate::model::Stores, secrets: &Secrets) -> Result<()> {
    for storage in stores.journal.marker_intents().await? {
        if stores
            .storage_records
            .enabled_by_id(storage)
            .await?
            .is_some_and(|s| s.store_type == "mokyu")
        {
            continue;
        }
        let token = Uuid::new_v4();
        if let Some(ledger) = stores.journal.claim(storage, token).await? {
            let result = Remote::connect(
                stores.journal.as_ref(),
                stores.storage_records.as_ref(),
                stores.storage_profiles.as_ref(),
                secrets,
                &ledger,
                token,
                None,
            )
            .await;
            stores.journal.release(storage, token, None).await?;
            result?;
        }
    }
    Ok(())
}

pub(crate) async fn publish_snapshot(
    native: &crate::storage::native::store::Store,
    db: &dyn JournalStore,
    credential: &crate::content::codec::Key,
    job: &crate::model::journal::snapshots::Job,
    token: Uuid,
    file: crate::model::journal::native::SnapshotFile,
    data: Bytes,
    condition: Condition,
) -> Result<Written> {
    let path = file.path(job)?;
    let path = path.as_str();
    let size = data.len();
    let digest = blake3::hash(&data);
    use crate::{
        content::fragment::Area,
        model::{storage_native::RecoveryIdentity, storage_recovery::Part},
        storage::native::store::Publication,
    };
    use futures_util::StreamExt;
    use object_store::{PutMode, PutOptions, UpdateVersion};
    let public = crate::storage::journal::recovery::inspect_prefix(&data)?;
    let mut options = PutOptions {
        mode: match condition {
            Condition::Absent => PutMode::Create,
            Condition::Match(etag) => PutMode::Update(UpdateVersion {
                e_tag: Some(etag),
                version: None,
            }),
        },
        ..Default::default()
    };
    options.extensions.insert(Publication {
        owner: job.id,
        area: Area::Recovery,
    });
    options.extensions.insert(RecoveryIdentity {
        payload: public.envelope.payload,
        revision: public.envelope.revision,
    });
    let bytes = data;
    let intent = native
        .write(
            &Path::from(path),
            size as u64,
            futures_util::stream::iter([Ok(bytes.clone())]).boxed(),
            options,
        )
        .await?;
    let part = Part {
        intent,
        path: path.into(),
        digest: *digest.as_bytes(),
    };
    part.inspect(&bytes)?;
    let proof = db
        .native_snapshot_prepare(job, token, file, &part, credential)
        .await?;
    native.publish_evidence(db, token, &proof).await?;
    db.native_snapshot_acknowledge(job, token, file, &proof)
        .await?;
    let version = part.intent.operation.to_string();
    Ok(Written {
        etag: format!("\"{version}\""),
        version: Some(version),
    })
}
