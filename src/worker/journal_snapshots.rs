use crate::{
    app::App,
    content::codec::Key,
    model::journal::{
        native::SnapshotFile,
        policy::MAX_LIMIT,
        snapshots::{Item, Job, Member, SnapshotStore},
    },
    storage::{
        external::{
            Client,
            objects::{Condition, Written},
        },
        journal::{
            Format, FrameType,
            batch::Record,
            draft_path,
            snapshot::{self, Index, Manifest, Row},
            source,
        },
        marker, model,
    },
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures_util::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};
use uuid::Uuid;

struct Target {
    backend: Arc<dyn ObjectStore>,
    native: Option<Arc<crate::storage::native::store::Store>>,
    client: Option<Client>,
    local: Option<Arc<crate::storage::journal::local::Local>>,
    storage: Arc<crate::storage::Storage>,
    id: Uuid,
    descriptor: marker::Journal,
    key: Key,
    grace: i64,
}
impl Target {
    async fn open(app: &App, id: Uuid) -> Result<Self> {
        let row = app
            .storage_records
            .enabled_by_id(id)
            .await?
            .context("snapshot storage disabled")?;
        let profile = app.storage_profiles.load(id).await?;
        let marker_value = row.identity_marker.clone();
        let descriptor =
            marker::journal(&marker_value)?.context("missing metadata journal descriptor")?;
        let key = descriptor.unlock(id, &app.secrets.credential_key)?;
        let connection =
            model::Connection::unprotect(&model::Record::from(row), &app.secrets.credential_key)?;
        let native = app.storage.store_backend(id)?.native.clone();
        let backend: Arc<dyn ObjectStore> = match &native {
            Some(native) => native.clone(),
            None => {
                model::Store::connect(&connection, model::Options::from_profile(&profile)?)?.backend
            }
        };
        marker::verify_store(backend.as_ref(), &marker_value).await?;
        let grace = i64::try_from(
            crate::config::seconds(&profile.unreferenced_grace)?
                .max(profile.min_storage_seconds()?),
        )?;
        Ok(Self {
            backend,
            native,
            client: (connection.r#type == "s3")
                .then(|| source::client(&connection))
                .transpose()?,
            local: crate::storage::journal::local::Local::new(&connection)?.map(Arc::new),
            storage: app.storage.clone(),
            id,
            descriptor,
            key,
            grace,
        })
    }
    fn format(&self, encrypted: bool) -> Format<'_> {
        Format {
            storage: self.id,
            encrypted,
            journal: self.descriptor.journal_id,
            epoch: self.descriptor.writer_epoch,
            key: &self.key,
        }
    }
    async fn envelope<T: serde::Serialize>(
        &self,
        job: &Job,
        file: SnapshotFile,
        kind: FrameType,
        value: &T,
    ) -> Result<Vec<u8>> {
        let path = file.path(job)?;
        let format = self.format(job.encrypt_frames);
        if let Some(native) = &self.native {
            snapshot::native_envelope(
                &format,
                native
                    .next_recovery_identity(&Path::from(path.clone()))
                    .await?,
                job.id,
                kind,
                &path,
                job.revision.try_into()?,
                value,
            )
        } else {
            snapshot::envelope(&format, kind, &path, job.revision.try_into()?, value)
        }
    }
    async fn put(
        &self,
        app: &App,
        job: &Job,
        token: Uuid,
        file: SnapshotFile,
        data: Vec<u8>,
        condition: Condition,
    ) -> Result<Written> {
        let path = file.path(job)?;
        let path = path.as_str();
        let size = data.len();
        let digest = blake3::hash(&data);
        if let Some(native) = &self.native {
            return crate::storage::journal::remote::publish_snapshot(
                native,
                app.journal.as_ref(),
                &app.secrets.credential_key,
                job,
                token,
                file,
                Bytes::from(data),
                condition,
            )
            .await;
        }

        let _guard = match &self.local {
            Some(l) => Some(l.lock(true).await?),
            None => None,
        };
        let result = if let Some(local) = &self.local {
            local.verify_owner(&self.descriptor)?;
            let location = Path::from(path);
            let etag = match self.backend.head(&location).await {
                Ok(m) => Some(m.e_tag.context("missing local snapshot revision")?),
                Err(object_store::Error::NotFound { .. }) => None,
                Err(e) => return Err(e.into()),
            };
            let expected = match &condition {
                Condition::Absent => None,
                Condition::Match(s) => Some(s),
            };
            if etag.as_ref() == expected {
                let target = self.storage.store_backend(self.id)?;
                let mut space = self.storage.local_space(&target, size as u64)?;
                let physical = local.root.join(path);
                if let Some(space) = &mut space {
                    space.observe(vec![
                        physical.clone(),
                        physical.parent().unwrap().join(".mokyu-journal.tmp"),
                    ])?;
                }
                let local = local.clone();
                let _lock = tokio::task::spawn_blocking(move || {
                    let _space = space;
                    local.replace(&physical, &data)?;
                    Ok::<_, anyhow::Error>(_guard)
                })
                .await??;
                Ok(Written {
                    etag: self
                        .backend
                        .head(&location)
                        .await?
                        .e_tag
                        .context("missing local snapshot revision")?,
                    version: None,
                })
            } else {
                Err(anyhow::anyhow!("local snapshot revision changed"))
            }
        } else {
            self.client
                .as_ref()
                .context("missing S3 snapshot client")?
                .put(path, &condition, &BTreeMap::new(), None, Bytes::from(data))
                .await
        };
        match result {
            Ok(receipt) => Ok(receipt),
            Err(cause) => {
                let result = match self.backend.get(&Path::from(path)).await {
                    Ok(v) => v,
                    Err(object_store::Error::NotFound { .. }) => return Err(cause),
                    Err(e) => return Err(e.into()),
                };
                ensure!(
                    result.meta.size == size as u64,
                    "snapshot publication conflicts"
                );
                let etag = result
                    .meta
                    .e_tag
                    .clone()
                    .context("missing remote snapshot ETag")?;
                let version = result.meta.version.clone();
                let mut hash = blake3::Hasher::new();
                let mut stream = result.into_stream();
                while let Some(part) = stream.try_next().await? {
                    hash.update(&part);
                }
                ensure!(hash.finalize() == digest, "snapshot publication conflicts");
                Ok(Written { etag, version })
            }
        }
    }
}

struct Group {
    rows: Vec<Row>,
    members: Vec<Member>,
    raw_bytes: usize,
    keys: BTreeSet<Uuid>,
    materials: BTreeSet<crate::model::journal::record::Reference>,
}
impl Group {
    fn new() -> Self {
        Self {
            rows: Vec::new(),
            members: Vec::new(),
            raw_bytes: 0,
            keys: BTreeSet::new(),
            materials: BTreeSet::new(),
        }
    }
    fn append(&mut self, row: Row) -> Result<()> {
        if matches!(&row,Row::Key(k) if !self.keys.insert(k.id))
            || matches!(&row,Row::Material(m) if !self.materials.insert(m.reference()))
        {
            return Ok(());
        }
        self.raw_bytes = self
            .raw_bytes
            .checked_add(serde_json::to_vec(&row)?.len() + 16)
            .context("snapshot size overflow")?;
        self.rows.push(row);
        Ok(())
    }
}

struct Builder<'a> {
    db: &'a dyn crate::model::journal::JournalStore,
    native: bool,
    job: &'a Job,
    token: Uuid,
    format: Format<'a>,
    position: i64,
    page: u32,
    group: Group,
}
impl Builder<'_> {
    async fn save(
        &mut self,
        bucket: Uuid,
        family: u32,
        parts: &[(snapshot::Page, Vec<u8>)],
        members: &[Member],
    ) -> Result<()> {
        self.db
            .snapshot_page(
                self.job,
                self.token,
                self.position,
                i64::from(family),
                bucket,
                parts,
                members,
            )
            .await?;
        self.page = self
            .page
            .checked_add(u32::try_from(parts.len())?)
            .context("snapshot page count overflow")?;
        self.position += i64::try_from(parts.len())?;
        Ok(())
    }
    async fn flush(&mut self, bucket: Uuid, family: u32) -> Result<()> {
        if self.group.rows.is_empty() {
            return Ok(());
        }
        let mut group = std::mem::replace(&mut self.group, Group::new());
        let mut physical = 0i64;
        if self.native {
            let mut seen = BTreeSet::new();
            for row in &group.rows {
                let Row::Material(material) = row else {
                    continue;
                };
                let Some(hold) = material.hold()? else {
                    continue;
                };
                if !seen.insert((hold.unit, hold.address)) {
                    continue;
                }
                let mut after = None;
                loop {
                    let captured = self
                        .db
                        .native_snapshot_material(self.job, self.token, &hold, after)
                        .await?;
                    let parts = snapshot::encode_pages(
                        &self.format,
                        self.job.id,
                        self.page,
                        &[],
                        self.job.page_limit.try_into()?,
                        Some(bucket),
                        Some(bucket),
                        Some(&captured.records),
                    )?;
                    for (page, _) in &parts {
                        physical = physical
                            .checked_add(page.bytes.try_into()?)
                            .context("snapshot size overflow")?;
                    }
                    self.save(bucket, family, &parts, &[]).await?;
                    if captured.complete {
                        break;
                    }
                    after = Some((captured.operation, captured.revision, captured.next));
                }
            }
        }
        let parts = snapshot::encode_pages(
            &self.format,
            self.job.id,
            self.page,
            &group.rows,
            usize::try_from(self.job.page_limit)?,
            Some(bucket),
            Some(bucket),
            self.native.then_some(&[]),
        )?;
        let physical = parts.iter().try_fold(physical, |v, (p, _)| {
            v.checked_add(p.bytes as i64)
                .context("snapshot size overflow")
        })?;
        let total = group.members.iter().map(|m| m.bytes).sum::<i64>().max(1);
        let mut remaining = physical;
        let last = group.members.len().saturating_sub(1);
        for (i, m) in group.members.iter_mut().enumerate() {
            m.bytes = if i == last {
                remaining
            } else {
                (i128::from(physical) * i128::from(m.bytes) / i128::from(total)) as i64
            };
            remaining -= m.bytes;
        }
        self.save(bucket, family, &parts, &group.members).await?;
        Ok(())
    }
    async fn item(&mut self, bucket: Uuid, item: Item, family: &mut u32) -> Result<()> {
        let cap = usize::try_from(self.job.page_limit)?;
        if item.deleted {
            self.group.append(Row::Delete {
                bucket,
                key: item.object_key.clone(),
                generation: item.generation,
                seq: item.seq,
            })?;
            let bytes = i64::try_from(item.object_key.len() + 64)?;
            self.group.members.push(Member { item, bytes });
        } else {
            let mut page = -1;
            let mut ended = false;
            let mut size = 0i64;
            let mut large = false;
            loop {
                let rows = self.db.snapshot_input(bucket, item.seq, page).await?;
                ensure!(
                    !rows.is_empty(),
                    "published object has no retained metadata"
                );
                for (number, bytes) in rows {
                    ensure!(number == page + 1 && !ended, "snapshot input has a gap");
                    let frame = self.format.decode(
                        FrameType::Draft,
                        &draft_path(bucket, item.seq, number),
                        &bytes,
                    )?;
                    ensure!(
                        frame.consumed == bytes.len()
                            && frame.sequence.first == u64::try_from(number)? + 1
                            && frame.sequence.first == frame.sequence.last
                            && frame.sequence.previous == [0; 32],
                        "invalid snapshot input"
                    );
                    if !large
                        && size == 0
                        && self.group.raw_bytes + frame.bytes.len() + 512 > cap
                        && !self.group.rows.is_empty()
                    {
                        self.flush(bucket, *family).await?;
                        *family = self.page;
                    }
                    size += i64::try_from(frame.bytes.len())?;
                    for record in serde_json::from_slice::<Vec<Record>>(&frame.bytes)? {
                        let row = match record {
                            Record::Key(key) => Row::Key(key),
                            Record::Material(m) => Row::Material(m),
                            Record::Ranges {
                                bucket: b,
                                generation,
                                seq,
                                offset,
                                ranges,
                            } => {
                                ensure!(
                                    b == bucket && generation == item.generation && seq == item.seq,
                                    "snapshot extent identity mismatch"
                                );
                                Row::Ranges {
                                    bucket,
                                    key: item.object_key.clone(),
                                    seq,
                                    offset,
                                    ranges,
                                }
                            }
                            Record::Change {
                                value,
                                range_count,
                                range_digest,
                            } => {
                                ensure!(
                                    value.bucket == bucket
                                        && value.seq == item.seq
                                        && value.generation == item.generation
                                        && value.key.as_ref() == Some(&item.object_key),
                                    "snapshot object identity mismatch"
                                );
                                ended = true;
                                Row::Object {
                                    bucket,
                                    key: item.object_key.clone(),
                                    generation: item.generation,
                                    seq: item.seq,
                                    attributes: value
                                        .attributes
                                        .context("missing confirmed object attributes")?,
                                    range_count,
                                    range_digest,
                                }
                            }
                        };
                        self.group.append(row)?;
                    }
                    page = number;
                    if !ended && self.group.raw_bytes >= cap {
                        large = true;
                        self.flush(bucket, *family).await?;
                    }
                }
                if ended {
                    break;
                }
            }
            self.group.members.push(Member {
                item,
                bytes: size.max(1),
            });
            if large {
                self.flush(bucket, *family).await?;
                *family = self.page;
                return Ok(());
            }
        }
        if self.group.raw_bytes >= cap.saturating_sub(512) {
            self.flush(bucket, *family).await?;
            *family = self.page;
        }
        Ok(())
    }
}

async fn build(app: &App, job: &Job, token: Uuid, remote: &Target) -> Result<()> {
    let db = app.journal.as_ref();
    let mut builder = Builder {
        db,
        native: remote.native.is_some(),
        job,
        token,
        format: remote.format(job.encrypt_frames),
        position: i64::try_from(db.snapshot_pages(job.id).await?.len())?,
        page: 0,
        group: Group::new(),
    };
    let scopes = db.snapshot_scopes(job.id).await?;
    let mut included = Vec::new();
    let mut excluded = Vec::new();
    for scope in scopes {
        if !scope.included {
            excluded.push(scope.cut());
            continue;
        }
        let bucket = scope.bucket_id;
        included.push(scope.cut());
        let mut after = None;
        let mut family = builder.page;
        loop {
            let items = db
                .snapshot_items(job.id, bucket, after.as_deref(), 128)
                .await?;
            if items.is_empty() {
                break;
            }
            after = items.last().map(|i| i.object_key.clone());
            for item in items {
                builder.item(bucket, item, &mut family).await?;
            }
        }
        builder.flush(bucket, family).await?;
    }
    let pages = db
        .snapshot_pages(job.id)
        .await?
        .into_iter()
        .map(|p| p.description.0)
        .collect();
    let seq = u64::try_from(job.sequence_no)?;
    let rev = u64::try_from(job.revision)?;
    let manifest = Manifest {
        format_version: 1,
        id: job.id,
        parent: job.parent,
        sequence: seq,
        tail: job
            .digest
            .as_deref()
            .context("missing snapshot tail")?
            .try_into()?,
        pages,
    };
    let data = remote
        .envelope(job, SnapshotFile::Manifest, FrameType::Manifest, &manifest)
        .await?;
    let index = Index {
        format_version: 1,
        snapshot: job.id,
        operation: job.id,
        revision: rev,
        sequence: seq,
        manifest_digest: *blake3::hash(&data).as_bytes(),
        bucket_count: included.len() as u64,
        buckets: included,
        excluded,
    };
    let root = remote
        .envelope(job, SnapshotFile::Index, FrameType::Index, &index)
        .await?;
    let checkpoint = if remote.native.is_some() {
        Some(
            remote
                .envelope(job, SnapshotFile::Checkpoint, FrameType::Index, &index)
                .await?,
        )
    } else {
        None
    };
    db.snapshot_seal(job, token, &data, &root, checkpoint.as_deref())
        .await
}

async fn publish(app: &App, job: &Job, token: Uuid, remote: &Target) -> Result<()> {
    let db = app.journal.as_ref();
    for row in db.snapshot_pages(job.id).await? {
        if row.owner != job.id || row.etag.is_some() {
            continue;
        }
        let page = row.description;
        let data = db.snapshot_bytes(job.id, row.page_no).await?;
        ensure!(
            data.len() as u64 == page.bytes
                && page.bytes <= u64::from(MAX_LIMIT)
                && *blake3::hash(&data).as_bytes() == page.digest,
            "staged snapshot page changed"
        );
        let tag = remote
            .put(
                app,
                job,
                token,
                SnapshotFile::Page(page.number),
                data,
                Condition::Absent,
            )
            .await?;
        db.snapshot_receipt(
            job,
            token,
            Some(row.page_no),
            &tag.etag,
            tag.version.as_deref(),
        )
        .await?;
    }
    let (manifest, index, checkpoint) = db.snapshot_blobs(job.id).await?;
    if job.manifest_etag.is_none() {
        let tag = remote
            .put(
                app,
                job,
                token,
                SnapshotFile::Manifest,
                manifest,
                Condition::Absent,
            )
            .await?;
        db.snapshot_receipt(job, token, None, &tag.etag, tag.version.as_deref())
            .await?;
    }
    if db.snapshot_scope_changed(job.id).await?
        && source::bytes(
            remote.backend.as_ref(),
            &snapshot::index_path(),
            (crate::storage::journal::FRAME_LIMIT + Format::overhead(false)) as u64,
        )
        .await?
        .as_deref()
            != Some(&index)
    {
        db.snapshot_abandon(job, token).await?;
        return Ok(());
    }
    let condition = if let Some(parent) = job.parent {
        Condition::Match(db.snapshot_parent_etag(parent).await?)
    } else {
        Condition::Absent
    };
    let tag = remote
        .put(
            app,
            job,
            token,
            SnapshotFile::Index,
            index.clone(),
            condition,
        )
        .await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("journal-snapshot-switched").await;
    let witness = remote
        .put(
            app,
            job,
            token,
            SnapshotFile::Checkpoint,
            checkpoint,
            Condition::Absent,
        )
        .await?;
    db.snapshot_commit(
        job,
        token,
        &tag.etag,
        &witness.etag,
        witness.version.as_deref(),
        remote.grace,
    )
    .await
}

async fn step(app: &App, storage: Uuid, token: Uuid) -> Result<()> {
    let db = app.journal.as_ref();
    let remote = Target::open(app, storage).await?;
    if let Some(native) = &remote.native {
        if let Some(proof) = db.native_gc_pending(storage, token).await? {
            native.publish_evidence(db, token, &proof).await?;
            db.native_gc_acknowledge(token, &proof).await?;
        }
    }
    if let Some(job) = db.snapshot_begin(storage, token).await? {
        let result = async {
            ensure!(
                Some(remote.descriptor.writer_epoch) == job.epoch,
                "snapshot writer epoch changed"
            );
            if job.state == "building" {
                build(app, &job, token, &remote).await?;
            }
            publish(app, &job, token, &remote).await
        }
        .await;
        if let Err(error) = result {
            let retryable = error.chain().any(|e| {
                e.downcast_ref::<reqwest::Error>().is_some()
                    || e.downcast_ref::<sqlx::Error>().is_some()
                    || matches!(
                        e.downcast_ref::<crate::model::Error>(),
                        Some(crate::model::Error::OperationAborted)
                    )
                    || e.downcast_ref::<tonic::Status>().is_some_and(|status| {
                        matches!(
                            status.code(),
                            tonic::Code::Unavailable
                                | tonic::Code::DeadlineExceeded
                                | tonic::Code::ResourceExhausted
                                | tonic::Code::Aborted
                        )
                    })
            }) || error.to_string() == "bucket settings changed during snapshot";
            db.snapshot_fail(
                &job,
                token,
                &error.to_string().chars().take(512).collect::<String>(),
                retryable,
            )
            .await?;
            return Err(error);
        }
    }
    let batch = db.snapshot_gc(storage, token, remote.grace).await?;
    if !batch.is_empty() {
        let _guard = match &remote.local {
            Some(local) => {
                let lock = local.lock(true).await?;
                local.verify_owner(&remote.descriptor)?;
                Some(lock)
            }
            None => None,
        };
        let current = db
            .snapshot_status(storage)
            .await?
            .current
            .context("journal GC has no checkpoint")?;
        let expected = db.snapshot_parent_etag(current).await?;
        let head = match remote
            .backend
            .head(&Path::from(snapshot::index_path()))
            .await
        {
            Ok(head) => head,
            Err(object_store::Error::NotFound { .. }) => {
                db.snapshot_gc_failure(storage, token, "checkpoint missing; journal GC paused")
                    .await?;
                anyhow::bail!("journal checkpoint missing");
            }
            Err(error) => return Err(error.into()),
        };
        if head.e_tag.as_deref() != Some(&expected) {
            db.snapshot_gc_failure(storage, token, "checkpoint changed; journal GC paused")
                .await?;
            anyhow::bail!("journal checkpoint changed");
        }
        for file in batch {
            if let Some(native) = &remote.native {
                let proof = db
                    .native_gc_prepare(token, &file, &app.secrets.credential_key)
                    .await?;
                native.publish_evidence(db, token, &proof).await?;
                db.native_gc_acknowledge(token, &proof).await?;
                if !native
                    .reclaim_recovery(proof.publication.retired[0])
                    .await?
                {
                    continue;
                }
            } else if let Some(local) = &remote.local {
                let path = Path::parse(&file.file_path)?;
                match remote.backend.head(&path).await {
                    Ok(m) => {
                        if m.e_tag.as_ref() != Some(&file.etag) {
                            db.snapshot_gc_failure(
                                storage,
                                token,
                                "cleanup object changed; journal GC paused",
                            )
                            .await?;
                            anyhow::bail!("local cleanup object changed");
                        }
                        let physical = local.root.join(path.as_ref());
                        tokio::fs::remove_file(&physical).await?;
                        crate::storage::raw::sync_directory(physical.parent().unwrap()).await?;
                    }
                    Err(object_store::Error::NotFound { .. }) => (),
                    Err(e) => return Err(e.into()),
                }
            } else if let Err(cause) = remote
                .client
                .as_ref()
                .context("missing S3 snapshot client")?
                .delete_version(&file.file_path, &file.etag, file.remote_version.as_deref())
                .await
            {
                match remote
                    .client
                    .as_ref()
                    .context("missing S3 snapshot client")?
                    .head(&file.file_path, file.remote_version.as_deref())
                    .await?
                {
                    None => {}
                    Some(found) if found.etag != file.etag => {
                        db.snapshot_gc_failure(
                            storage,
                            token,
                            "cleanup object changed; journal GC paused",
                        )
                        .await?;
                        return Err(cause);
                    }
                    Some(_) => return Err(cause),
                }
            }
            #[cfg(feature = "fault-injection")]
            crate::faults::point("journal-snapshot-deleted").await;
            db.snapshot_reclaimed(token, &file).await?;
        }
    }
    Ok(())
}

async fn store(app: &App, storage: Uuid) -> Result<()> {
    let token = Uuid::new_v4();
    if !app.journal.snapshot_claim(storage, token).await? {
        return Ok(());
    }
    let result = {
        let work = step(app, storage, token);
        tokio::pin!(work);
        let mut ticker = tokio::time::interval(Duration::from_secs(15));
        ticker.tick().await;
        loop {
            tokio::select! {
                result=&mut work=>break result,
                _=ticker.tick()=>ensure!(app.journal.snapshot_renew(storage,token).await?,"snapshot lease lost"),
            }
        }
    };
    app.journal.snapshot_release(storage, token).await?;
    result
}
pub(super) async fn run(app: Arc<App>) -> Result<()> {
    loop {
        let wake = app.wake_maintenance.notified();
        tokio::pin!(wake);
        wake.as_mut().enable();
        if app.writable().is_ok() {
            match app.journal.snapshot_sources().await {
                Ok(ids) => {
                    for id in ids {
                        if let Err(error) = store(&app, id).await {
                            tracing::warn!(%error,storage=%id,"snapshot maintenance interrupted");
                        }
                    }
                }
                Err(error) => tracing::warn!(%error,"cannot list snapshot jobs"),
            }
        }
        tokio::select! {_=wake=>(),_=tokio::time::sleep(Duration::from_secs(3))=>()}
    }
}
