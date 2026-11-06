use super::{
    Backend, node_registry,
    node_writes::{self as writes, Write},
    queries::{
        self, native as q, node_multipart as multipart, node_registry as registry,
        node_writes as ledger, read_grants,
    },
    transactions,
};
use crate::{
    cluster::NodeState,
    content::codec::Key,
    model::{
        Error, Id,
        cluster::NativeParts,
        multipart::RemoteUpload,
        node_native as native,
        node_raw::{File, FileState},
        node_read as read, node_write as write,
        session::Run,
    },
    storage::profile::StorageProfile,
};
use anyhow::{Result, ensure};
use sqlx::types::Json;
use uuid::Uuid;

#[derive(sqlx::FromRow)]
pub(super) struct Offer {
    credential_generation: i64,
    boot_id: Uuid,
    fence: Uuid,
    target_stream: Option<Id>,
    token: Uuid,
    storage_id: Uuid,
    work: Json<native::Work>,
    remaining: i64,
    result_hash: Option<Vec<u8>>,
}
impl Offer {
    fn own(&self, node: &NodeState, run: Run) -> Result<()> {
        ensure!(
            self.credential_generation == node.credential_generation
                && self.boot_id == run.boot
                && self.fence == run.fence,
            Error::NodeExpired
        );
        Ok(())
    }
    async fn reply(
        self,
        tx: &mut registry::Tx<'_>,
        node: &NodeState,
        key: &Key,
        input: &native::Claim,
    ) -> Result<native::Reply> {
        self.own(node, input.run)?;
        ensure!(
            self.target_stream == input.stream,
            Error::NodeOperationConflict
        );
        ensure!(
            self.remaining > 0 && self.result_hash.is_none(),
            Error::NodeExpired
        );
        let storage = super::node_work::material(tx, self.storage_id, key).await?;
        let mut materials = vec![];
        let version = read::material(
            &mut materials,
            node.deployment_id,
            read::MaterialType::Storage,
            self.storage_id.to_string(),
            &storage,
            &input.cached,
        )?;
        Ok(native::Reply {
            operation: input.operation,
            work: Some(self.work.0),
            ready: false,
            storage: Some(version),
            materials,
            grants: vec![read::Grant {
                r#type: read::GrantType::Native,
                id: self.token,
                storage: Some(self.storage_id),
                valid_for_ms: self.remaining as u64,
            }],
        })
    }
}
fn ready(input: &native::Claim, ready: bool) -> native::Reply {
    native::Reply {
        operation: input.operation,
        work: None,
        ready,
        materials: vec![],
        storage: None,
        grants: vec![],
    }
}
pub(super) async fn open(
    tx: &mut registry::Tx<'_>,
    upload: Uuid,
    storage: Uuid,
    revision: i64,
    stream: Id,
    node: Uuid,
    key: Option<Uuid>,
    profile: &StorageProfile,
) -> Result<()> {
    ensure!(
        !profile.compression_enabled && !profile.pack_enabled && key.is_none(),
        Error::OperationAborted
    );
    q::open(upload, storage, revision, Uuid::now_v7())
        .execute(&mut **tx)
        .await?;
    let owner = q::upload(upload).fetch_one(&mut **tx).await?;
    ensure!(
        owner.state == "active" && owner.storage == storage,
        Error::OperationAborted
    );
    q::new_file(Uuid::now_v7(), upload, storage, node, stream)
        .execute(&mut **tx)
        .await?;
    Ok(())
}
pub(super) async fn publish_part(
    tx: &mut registry::Tx<'_>,
    row: &Write,
    node: Uuid,
    part: &write::Part,
    stream: Id,
    size: i64,
) -> Result<()> {
    let (file, state, hash) = q::file_state(stream, node).fetch_one(&mut **tx).await?;
    ensure!(
        state == "sealed" && hash.is_some() && row.next_offset == size,
        s3s::s3_error!(IncompleteBody)
    );
    q::queue(
        part.upload,
        part.number,
        stream,
        Some(file),
        node,
        size,
        row.profile.upload_cache,
    )
    .execute(&mut **tx)
    .await?;
    Ok(())
}
async fn start<'a>(
    b: &'a Backend,
    node: &NodeState,
    key: &Key,
    run: Run,
) -> Result<registry::Tx<'a>> {
    ensure!(
        matches!(node.role.as_str(), "data" | "node"),
        Error::NodeAuthentication
    );
    let tx = writes::start(b, node, key, run).await?;
    Ok(tx)
}
async fn completion(
    tx: &mut registry::Tx<'_>,
    row: &Write,
    node: &NodeState,
    run: Run,
) -> Result<(Id, native::Completion)> {
    let stream = row.active(tx, node, run).await?;
    let manifest = row
        .intent
        .completion
        .as_ref()
        .ok_or(Error::NodeOperationConflict)?;
    let mut upload = q::upload(manifest.upload)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or(Error::SourceUnavailable)?;
    ensure!(
        !q::has_error(upload.upload).fetch_one(&mut **tx).await?,
        Error::SourceUnavailable
    );
    let hash = manifest.hash()?;
    if let Some(saved) = &upload.manifest {
        ensure!(saved == &hash, Error::NodeOperationConflict);
    }
    q::manifest(upload.upload, &hash).execute(&mut **tx).await?;
    upload.manifest = Some(hash);
    let parts = q::manifest_parts(upload.upload)
        .fetch_all(&mut **tx)
        .await?;
    ensure!(
        parts.len() == manifest.parts.len(),
        Error::SourceUnavailable
    );
    Ok((stream, native::Completion { upload, parts }))
}
pub(super) async fn candidate(
    tx: &mut registry::Tx<'_>,
    stream: Id,
    upload: &RemoteUpload,
) -> Result<Id> {
    if let Some(id) = q::raw(upload.upload).fetch_one(&mut **tx).await? {
        let current = queries::raw_source(id).fetch_one(&mut **tx).await?;
        ensure!(
            current.location_id == upload.source
                && current.storage_id == upload.storage
                && current.medium == "shared_s3",
            Error::OperationAborted
        );
        if current.state == "writing" {
            q::rebind(id, stream).execute(&mut **tx).await?;
        } else {
            ensure!(current.state == "ready", Error::SourceUnavailable);
        }
        return Ok(id);
    }
    let id = ledger::raw_source(
        tx,
        upload.source,
        stream,
        upload.storage,
        upload.profile,
        "shared_s3",
    )
    .await?;
    ensure!(
        q::bind_raw(upload.upload, id)
            .execute(&mut **tx)
            .await?
            .rows_affected()
            == 1,
        Error::OperationAborted
    );
    Ok(id)
}
pub(super) async fn attach(
    tx: &mut registry::Tx<'_>,
    stream: Id,
    source: Id,
    size: i64,
    digest: &[u8],
) -> Result<()> {
    let current = queries::raw_source(source).fetch_one(&mut **tx).await?;
    if current.state == "ready" {
        ensure!(
            current.size == size && current.digest.as_deref() == Some(digest),
            Error::NodeOperationConflict
        );
    } else {
        ensure!(
            queries::raw_finish(stream, source, size, digest, true)
                .execute(&mut **tx)
                .await?
                .rows_affected()
                == 1,
            Error::OperationAborted
        );
    }
    queries::raw_attach(stream, source, size)
        .execute(&mut **tx)
        .await?;
    Ok(())
}
#[async_trait::async_trait]
impl NativeParts for Backend {
    async fn prepare_local_part(
        &self,
        node: &NodeState,
        key: &Key,
        input: native::LocalPart,
    ) -> Result<write::Reply> {
        let hash = input.hash()?;
        let mut tx = start(self, node, key, input.run).await?;
        let row = writes::lookup(&mut tx, node.node_id, input.operation).await?;
        let stream = row.active(&mut tx, node, input.run).await?;
        let part = row
            .intent
            .part
            .as_ref()
            .filter(|_| !row.profile.cdc_enabled)
            .ok_or(Error::NodeOperationConflict)?;
        let (file, state, previous) = q::file_state(stream, node.node_id)
            .fetch_one(&mut *tx)
            .await?;
        if let Some(prior) = previous {
            ensure!(prior == hash, Error::NodeOperationConflict);
        } else {
            ensure!(
                state == "open"
                    && row.next_offset == 0
                    && row.next_batch == 1
                    && row.intent.size.is_none_or(|s| s == input.size),
                Error::NodeOperationConflict
            );
            transactions::reserve_quota_in(
                &mut tx,
                stream,
                input.size,
                false,
                Some((part.upload, part.number)),
            )
            .await?;
            ensure!(
                q::seal(file, &hash)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected()
                    == 1,
                Error::OperationAborted
            );
            let mut offset = 0;
            for hash in &input.hashes {
                let size = (input.size - offset).min(crate::content::codec::MAX as i64) as i32;
                let tail = crate::model::StreamTail {
                    fragment: Uuid::now_v7(),
                    stream,
                    offset,
                    size,
                    hash: *hash,
                    staging_storage: None,
                    staging_key: None,
                    file: Some(file),
                    file_offset: Some(offset),
                };
                let id = multipart::fragment(&mut tx, &tail).await?;
                queries::seal_fragment(id).execute(&mut *tx).await?;
                multipart::attach(stream, offset, size, id)
                    .execute(&mut *tx)
                    .await?;
                offset += i64::from(size);
            }
            ensure!(offset == input.size, Error::NodeOperationConflict);
            queries::resize_writing_stream(stream, input.size)
                .execute(&mut *tx)
                .await?;
            ledger::advance(node.node_id, input.operation, input.size)
                .execute(&mut *tx)
                .await?;
        }
        let row = writes::lookup(&mut tx, node.node_id, input.operation).await?;
        let reply = row.reply(&mut tx, node, key, input.run, None).await?;
        node_registry::validate_run(&mut tx, node, input.run).await?;
        tx.commit().await?;
        Ok(reply)
    }
    async fn claim_native(
        &self,
        node: &NodeState,
        key: &Key,
        input: native::Claim,
    ) -> Result<native::Reply> {
        if let Some(id) = input.stream {
            id.validate(read_grants::DATABASE)?;
        }
        let issued = write::operation_time(input.operation)?;
        let now = chrono::Utc::now();
        let mut tx = start(self, node, key, input.run).await?;
        if let Some(offer) = q::work(node.node_id, input.operation)
            .fetch_optional(&mut *tx)
            .await?
        {
            return offer.reply(&mut tx, node, key, &input).await;
        }
        ensure!(
            issued <= now + chrono::Duration::seconds(30)
                && issued >= now - chrono::Duration::minutes(15),
            Error::NodeOperationExpired
        );
        if let Some(stream) = input.stream {
            let (synced, error) = q::ready(node.node_id, stream)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(Error::NodeOperationMissing)?;
            ensure!(
                !error
                    .as_deref()
                    .is_some_and(|e| matches!(e, "local_source_missing" | "local_source_corrupt")),
                Error::SourceUnavailable
            );
            if synced {
                return Ok(ready(&input, true));
            }
        }
        q::prune().execute(&mut *tx).await?;
        q::prune_pins().execute(&mut *tx).await?;
        ensure!(
            q::count(node.node_id).fetch_one(&mut *tx).await? < 64,
            Error::NodeWriteLimit
        );
        let token = Uuid::new_v4();
        let work = if let Some((upload, number, stream, file, size, etag)) =
            q::due(&mut tx, node.node_id, input.stream).await?
        {
            let upload = q::upload(upload).fetch_one(&mut *tx).await?;
            let action = if upload.upstream.is_none() {
                ensure!(
                    q::claim_init(upload.upload, token)
                        .execute(&mut *tx)
                        .await?
                        .rows_affected()
                        == 1,
                    Error::OperationAborted
                );
                native::Action::Initialize
            } else {
                ensure!(
                    q::claim_part(upload.upload, number, stream, token)
                        .execute(&mut *tx)
                        .await?
                        .rows_affected()
                        == 1,
                    Error::OperationAborted
                );
                q::pin(stream, token).execute(&mut *tx).await?;
                native::Action::Part {
                    stream,
                    file: file.ok_or(Error::NodeAuthentication)?,
                    number,
                    size,
                    etag,
                }
            };
            native::Work { upload, action }
        } else if input.stream.is_none()
            && let Some(id) = q::abort_due().fetch_optional(&mut *tx).await?
        {
            ensure!(
                q::claim_abort(id, token)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected()
                    == 1,
                Error::OperationAborted
            );
            native::Work {
                upload: q::upload(id).fetch_one(&mut *tx).await?,
                action: native::Action::Abort,
            }
        } else {
            return Ok(ready(&input, false));
        };
        let storage = work.upload.storage;
        let profile = queries::storage_profile(storage)
            .fetch_one(&mut *tx)
            .await?;
        read_grants::lock_storage(&mut tx, storage, "upload").await?;
        if let Some(limit) = profile.upload_concurrency {
            ensure!(
                ledger::pool_load(storage).fetch_one(&mut *tx).await? < i64::from(limit),
                Error::NodeWriteBusy
            );
        }
        q::storage_permit(node.node_id, token, storage)
            .execute(&mut *tx)
            .await?;
        q::save(node, &input, token, &work)
            .execute(&mut *tx)
            .await?;
        let answer = q::work(node.node_id, input.operation)
            .fetch_one(&mut *tx)
            .await?
            .reply(&mut tx, node, key, &input)
            .await?;
        node_registry::validate_run(&mut tx, node, input.run).await?;
        tx.commit().await?;
        Ok(answer)
    }
    async fn finish_native(
        &self,
        node: &NodeState,
        key: &Key,
        input: native::Finish,
    ) -> Result<()> {
        let hash = input.result.hash()?;
        let mut tx = start(self, node, key, input.run).await?;
        let offer = q::work(node.node_id, input.operation)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(Error::NodeOperationMissing)?;
        offer.own(node, input.run)?;
        if let Some(prior) = offer.result_hash {
            ensure!(prior == hash, Error::NodeOperationConflict);
            return Ok(());
        }
        ensure!(offer.remaining > 0, Error::NodeExpired);
        let result = &input.result;
        let task = &offer.work;
        ensure!(
            result.source_error.is_none() || matches!(task.action, native::Action::Part { .. }),
            Error::NodeOperationConflict
        );
        let value = result.value.as_deref();
        ensure!(
            result.source_error.is_none() || result.retry,
            Error::NodeOperationConflict
        );
        if let (native::Action::Part { .. }, Some(etag)) = (&task.action, value) {
            ensure!(
                !etag.is_empty()
                    && etag.len() <= 128
                    && etag.bytes().all(|b| (33..=126).contains(&b)),
                Error::NodeOperationConflict
            );
        }
        let changed = match task.action {
            native::Action::Initialize => {
                ensure!(
                    result.retry == value.is_none(),
                    Error::NodeOperationConflict
                );
                q::set_upstream(task.upload.upload, offer.token, value)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected()
            }
            native::Action::Part { stream, number, .. } => {
                ensure!(
                    result.retry == value.is_none(),
                    Error::NodeOperationConflict
                );
                q::set_part(
                    task.upload.upload,
                    number,
                    stream,
                    offer.token,
                    value,
                    result
                        .source_error
                        .map(|e| e.code())
                        .or(result.retry.then_some("backend_io_failed")),
                )
                .execute(&mut *tx)
                .await?
                .rows_affected()
            }
            native::Action::Abort => {
                ensure!(value.is_none(), Error::NodeOperationConflict);
                q::set_upstream(
                    task.upload.upload,
                    offer.token,
                    if result.retry {
                        task.upload.upstream.as_deref()
                    } else {
                        None
                    },
                )
                .execute(&mut *tx)
                .await?
                .rows_affected()
            }
        };
        ensure!(changed == 1, Error::NodeExpired);
        q::done(node.node_id, input.operation, &hash)
            .execute(&mut *tx)
            .await?;
        q::unpin(offer.token).execute(&mut *tx).await?;
        q::drop_permit(offer.token).execute(&mut *tx).await?;
        node_registry::validate_run(&mut tx, node, input.run).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn check_local_files(
        &self,
        node: &NodeState,
        key: &Key,
        input: native::Inventory,
    ) -> Result<Vec<File>> {
        use std::collections::HashSet;
        ensure!(
            input.files.len() <= 128
                && input.files.iter().all(|id| !id.is_nil())
                && input.files.iter().collect::<HashSet<_>>().len() == input.files.len(),
            Error::NodeReadLimit
        );
        let mut tx = start(self, node, key, input.run).await?;
        let mut files = Vec::new();
        for id in input.files {
            if input.recover {
                q::retry_file(node.node_id, id).execute(&mut *tx).await?;
            }
            let status = q::inventory(node.node_id, id)
                .fetch_optional(&mut *tx)
                .await?;
            let (storage, size, state) = match status {
                Some((storage, size, pending, protected, other)) => (
                    Some(storage),
                    u64::try_from(size)?,
                    if other {
                        FileState::Protected
                    } else if pending {
                        FileState::Pending
                    } else if protected {
                        FileState::Protected
                    } else if q::forget_file(id, node.node_id)
                        .execute(&mut *tx)
                        .await?
                        .rows_affected()
                        == 1
                    {
                        FileState::Removable
                    } else {
                        FileState::Protected
                    },
                ),
                None => (
                    None,
                    0,
                    if q::unknown_file(id).fetch_one(&mut *tx).await? {
                        FileState::Protected
                    } else {
                        FileState::Removable
                    },
                ),
            };
            files.push(File {
                id,
                storage,
                size,
                state,
                cache_limit: None,
                cache_entries: None,
            });
        }
        tx.commit().await?;
        Ok(files)
    }
    async fn prepare_native_complete(
        &self,
        node: &NodeState,
        key: &Key,
        input: native::Complete,
    ) -> Result<native::PreparedCompletion> {
        let mut tx = start(self, node, key, input.run).await?;
        let row = writes::lookup(&mut tx, node.node_id, input.operation).await?;
        let (_, mut value) = completion(&mut tx, &row, node, input.run).await?;
        if input.resend {
            q::retry_parts(value.upload.upload)
                .execute(&mut *tx)
                .await?;
            value.parts = q::manifest_parts(value.upload.upload)
                .fetch_all(&mut *tx)
                .await?;
        }
        let ready =
            value.upload.upstream.is_some() && value.parts.iter().all(|p| p.remote_etag.is_some());
        let mut grants = writes::permits(
            &mut tx,
            &row,
            node.node_id,
            usize::from(ready),
            -2,
            row.storage_id,
            row.profile.upload_concurrency,
        )
        .await?;
        if ready {
            let current = queries::storage_profile(row.storage_id)
                .fetch_one(&mut *tx)
                .await?;
            let limit = match (row.profile.read_concurrency, current.read_concurrency) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
            if let Some(limit) = limit {
                read_grants::lock_storage(&mut tx, row.storage_id, "read").await?;
                let (id, left) = if let Some(token) = q::read_permit(node.node_id, input.operation)
                    .fetch_optional(&mut *tx)
                    .await?
                {
                    token
                } else {
                    ensure!(
                        q::read_load(row.storage_id).fetch_one(&mut *tx).await? < i64::from(limit),
                        Error::NodeWriteBusy
                    );
                    let token = Uuid::new_v4();
                    q::add_read_permit(token, row.storage_id, node.node_id)
                        .execute(&mut *tx)
                        .await?;
                    ledger::assign(token, node.node_id, input.operation, -3)
                        .execute(&mut *tx)
                        .await?;
                    (token, 60_000)
                };
                grants.push(read::Grant {
                    r#type: read::GrantType::StorageRead,
                    id,
                    storage: Some(row.storage_id),
                    valid_for_ms: left as u64,
                });
            }
        }
        node_registry::validate_run(&mut tx, node, input.run).await?;
        tx.commit().await?;
        Ok(native::PreparedCompletion { value, grants })
    }
    async fn stage_native_pages(
        &self,
        node: &NodeState,
        key: &Key,
        input: native::Pages,
    ) -> Result<()> {
        input.group.validate()?;
        let mut tx = start(self, node, key, input.run).await?;
        let row = writes::lookup(&mut tx, node.node_id, input.operation).await?;
        let (stream, value) = completion(&mut tx, &row, node, input.run).await?;
        let sum = multipart::record(value.upload.upload)
            .fetch_one(&mut *tx)
            .await?
            .summary;
        let groups =
            crate::content::raw_pages::count(sum.size)? / crate::content::raw_pages::GROUP as i64;
        ensure!(
            input.group.index < groups
                && input.group.hashes.len() == crate::content::raw_pages::GROUP * 32,
            Error::NodeOperationConflict
        );
        let source = candidate(&mut tx, stream, &value.upload).await?;
        super::raw_pages::put(&mut tx, source, &input.group).await?;
        node_registry::validate_run(&mut tx, node, input.run).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn adopt_native_complete(
        &self,
        node: &NodeState,
        key: &Key,
        input: native::Adopt,
    ) -> Result<write::Reply> {
        let mut tx = start(self, node, key, input.run).await?;
        let row = writes::lookup(&mut tx, node.node_id, input.operation).await?;
        let (stream, value) = completion(&mut tx, &row, node, input.run).await?;
        ensure!(
            value.upload.upstream.is_some() && value.parts.iter().all(|p| p.remote_etag.is_some()),
            Error::NodeOperationConflict
        );
        let size = multipart::record(value.upload.upload)
            .fetch_one(&mut *tx)
            .await?
            .summary
            .size;
        let pages = crate::content::raw_pages::count(size)?;
        let group = crate::content::raw_pages::GROUP as i64;
        ensure!(
            input.page_tail.len() == (pages % group) as usize * 32,
            Error::NodeOperationConflict
        );
        let data = crate::model::node_raw::Input {
            local: false,
            size,
            digest: input.digest,
            hash: row.profile.deduplication_enabled.then_some(input.hash),
            page_hashes: input.page_tail.clone(),
        };
        let hash = crate::model::node_chunk::hash(&data)?;
        if let Some(raw) = ledger::raw(node.node_id, input.operation)
            .fetch_optional(&mut *tx)
            .await?
        {
            ensure!(raw.matches(&hash), Error::NodeOperationConflict);
        } else {
            ensure!(
                row.next_offset == 0 && row.next_batch == 1,
                Error::NodeOperationConflict
            );
            let source = candidate(&mut tx, stream, &value.upload).await?;
            if !input.page_tail.is_empty() {
                super::raw_pages::put(
                    &mut tx,
                    source,
                    &crate::content::raw_pages::Group {
                        index: pages / group,
                        hashes: input.page_tail,
                    },
                )
                .await?;
            }
            super::raw_pages::seal(&mut tx, source, size, &input.digest).await?;
            ensure!(
                queries::raw_prepare(stream)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected()
                    == 1,
                Error::OperationAborted
            );
            attach(&mut tx, stream, source, size, &input.digest).await?;
            if row.profile.deduplication_enabled {
                let current = queries::raw_source(source).fetch_one(&mut *tx).await?;
                transactions::share_raw(&mut tx, stream, &current, &input.hash).await?;
            }
            queries::resize_writing_stream(stream, size)
                .execute(&mut *tx)
                .await?;
            ledger::advance(node.node_id, input.operation, size)
                .execute(&mut *tx)
                .await?;
            ledger::add_raw(
                node.node_id,
                input.operation,
                &hash,
                source,
                value.upload.source,
                &data,
                true,
            )
            .execute(&mut *tx)
            .await?;
            ledger::release(node.node_id, input.operation)
                .execute(&mut *tx)
                .await?;
        }
        let row = writes::lookup(&mut tx, node.node_id, input.operation).await?;
        let reply = row.reply(&mut tx, node, key, input.run, None).await?;
        node_registry::validate_run(&mut tx, node, input.run).await?;
        tx.commit().await?;
        Ok(reply)
    }
}
