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
        let backend = model::Store::connect(&conn, opts)?.backend;
        let local = super::local::Local::new(&conn)?.map(Arc::new);
        let _lock = match &local {
            Some(l) => Some(l.lock(true).await?),
            None => None,
        };
        let client = if local.is_none() {
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

    pub async fn publish(
        &self,
        db: &dyn JournalStore,
        space: &crate::storage::Storage,
        storage: Uuid,
        token: Uuid,
        file: &File,
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
            let reservation = space.local_space(&target, pending as u64 + 4096)?;
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
        let client = self.client.as_ref().context("missing S3 journal client")?;
        let mut body = Vec::with_capacity(size);
        let mut cuts = Vec::new();
        for frame in frames {
            ensure!(
                frame.len() <= size - body.len(),
                "journal file length mismatch"
            );
            let boundary = frame.get(8) == Some(&(super::FrameType::PartCommit as u8));
            body.extend_from_slice(&frame);
            if boundary {
                cuts.push(body.len());
            }
        }
        ensure!(body.len() == size, "journal file is incomplete");
        let bytes = Bytes::from(body);
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
        let token = Uuid::new_v4();
        if let Some(ledger) = stores.journal.claim(storage, token).await? {
            let result = Remote::connect(
                stores.journal.as_ref(),
                stores.storage_records.as_ref(),
                stores.storage_profiles.as_ref(),
                secrets,
                &ledger,
                token,
            )
            .await;
            stores.journal.release(storage, token, None).await?;
            result?;
        }
    }
    Ok(())
}
