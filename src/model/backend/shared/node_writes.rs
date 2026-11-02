use super::queries::node_multipart as mp;
use super::{
    Backend, node_registry,
    queries::{
        self, node_registry as registry, node_requests as requests, node_writes as q, read_grants,
    },
    transactions,
};
use crate::model::node_complete as complete;
use crate::{
    authorization::{Action, Permit, Principal},
    cluster::NodeState,
    content::codec::Key,
    model::{
        Error, Id, ReadRoute, StreamCreate, StreamPurpose, cluster::NodeWrites, node_chunk,
        node_raw, node_read as read, node_write as write, session::Run,
    },
    storage::profile::StorageProfile,
};
use anyhow::{Result, ensure};
use sqlx::types::Json;
use uuid::Uuid;

#[derive(sqlx::FromRow)]
pub(super) struct Write {
    pub(super) owner_task_id: Option<Uuid>,
    pub(super) operation_id: Uuid,
    pub(super) lease_id: Uuid,
    pub(super) boot_id: Uuid,
    pub(super) fence: Uuid,
    pub(super) credential_generation: i64,
    pub(super) request_hash: Vec<u8>,
    pub(super) stream_id: Option<Id>,
    pub(super) bucket_id: Uuid,
    pub(super) storage_id: Uuid,
    pub(super) block_key_id: Option<Uuid>,
    pub(super) profile: Json<StorageProfile>,
    pub(super) intent: Json<write::Intent>,
    pub(super) copy_source: Option<Json<write::CopySource>>,
    pub(super) next_offset: i64,
    pub(super) next_batch: i64,
    pub(super) phase: String,
    pub(super) publish_hash: Option<Vec<u8>>,
    pub(super) published: Option<Json<write::Published>>,
    pub(super) remaining: i64,
}
#[derive(sqlx::FromRow)]
pub(super) struct Tail {
    request_hash: Vec<u8>,
    fragment_id: Id,
    location_id: Uuid,
    storage_id: Uuid,
    object_key: String,
    offset_bytes: i64,
    size: i32,
    committed: bool,
}
impl Tail {
    pub(super) fn reply(&self) -> crate::model::node_tail::Prepared {
        crate::model::node_tail::Prepared {
            fragment: self.location_id,
            object_key: self.object_key.clone(),
            end: self.offset_bytes + i64::from(self.size),
            committed: self.committed,
            materials: Vec::new(),
            storage: None,
            grants: Vec::new(),
        }
    }
}
#[derive(sqlx::FromRow)]
pub(super) struct Completion {
    manifest_hash: String,
    conditions: Json<complete::Conditions>,
    pub(super) summary: Json<complete::Summary>,
}
async fn completion(
    tx: &mut registry::Tx<'_>,
    input: &write::Begin,
    bucket: Uuid,
    authority: &Permit,
    manifest: &complete::Manifest,
) -> Result<(crate::model::multipart::Upload, complete::Summary, bool)> {
    let hash = manifest.hash()?;
    queries::lock_bucket_uploads(bucket)
        .fetch_one(&mut **tx)
        .await?;
    let upload = requests::multipart_upload(
        manifest.upload,
        bucket,
        &input.intent.key,
        &input.intent.access,
    )
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| s3s::s3_error!(NoSuchUpload))?;
    let (state, previous, live) = queries::completion_state(upload.id)
        .fetch_one(&mut **tx)
        .await?;
    ensure!(
        matches!(state.as_str(), "active" | "completing" | "completed"),
        s3s::s3_error!(NoSuchUpload)
    );
    let mut summary = if let Some(record) = mp::record(upload.id).fetch_optional(&mut **tx).await? {
        ensure!(
            record.manifest_hash == hash,
            s3s::s3_error!(InvalidRequest, "upload has another completion manifest")
        );
        record.summary.0
    } else {
        ensure!(
            state == "active" && previous.is_none(),
            s3s::s3_error!(InvalidRequest, "upload has another completion manifest")
        );
        let numbers: Vec<_> = manifest.parts.iter().map(|p| p.number).collect();
        let selected = mp::selected(upload.id, &numbers)
            .fetch_all(&mut **tx)
            .await?;
        let summary = manifest.select(&upload, &selected)?;
        let conditions = complete::Conditions {
            if_match: manifest.if_match.clone(),
            if_none: manifest.if_none.clone(),
            checksums: manifest.checksums.clone(),
        };
        mp::save(upload.id, &hash, &conditions, &summary)
            .execute(&mut **tx)
            .await?;
        ensure!(
            mp::freeze(upload.id, &numbers)
                .execute(&mut **tx)
                .await?
                .rows_affected()
                == numbers.len() as u64,
            Error::OperationAborted
        );
        summary
    };
    summary.result = upload.result.clone();
    if !live && state != "completed" {
        queries::begin_upload_completion(&crate::model::multipart::Completion {
            authority,
            upload: upload.id,
            hash: &hash,
            token: input.operation,
            parts: &[],
            node: None,
        })
        .execute(&mut **tx)
        .await?;
    }
    Ok((upload, summary, live))
}

#[derive(sqlx::FromRow)]
pub(super) struct Batch {
    request_hash: Vec<u8>,
    end_bytes: i64,
    plan: Json<Vec<node_chunk::Item>>,
    ready: bool,
    commit_hash: Option<Vec<u8>>,
}
#[derive(sqlx::FromRow)]
pub(super) struct Raw {
    request_hash: Vec<u8>,
    source_id: Id,
    location_id: Uuid,
    size: i64,
    digest: Vec<u8>,
    content_hash: Option<Vec<u8>>,
    committed: bool,
}
impl Raw {
    pub(super) fn matches(&self, hash: &[u8]) -> bool {
        self.committed && self.request_hash == hash
    }
}
impl Write {
    pub(super) fn owned(&self, node: &NodeState, run: Run) -> Result<()> {
        ensure!(
            self.credential_generation == node.credential_generation
                && self.boot_id == run.boot
                && self.fence == run.fence,
            Error::NodeExpired
        );
        Ok(())
    }
    pub(super) async fn active(
        &self,
        tx: &mut registry::Tx<'_>,
        node: &NodeState,
        run: Run,
    ) -> Result<Id> {
        self.owned(node, run)?;
        ensure!(
            self.phase == "active" && self.remaining > 0,
            Error::NodeExpired
        );
        let stream = self.stream_id.ok_or(Error::NodeExpired)?;
        let authority = queries::write_authorization(stream)
            .fetch_one(&mut **tx)
            .await?;
        transactions::lock_permit(tx, &authority).await?;
        Ok(stream)
    }
    pub(super) async fn reply(
        &self,
        tx: &mut registry::Tx<'_>,
        node: &NodeState,
        key: &Key,
        run: Run,
        cached: Option<&[read::Version]>,
    ) -> Result<write::Reply> {
        let phase = match self.phase.as_str() {
            "published" => write::Phase::Published,
            "preparing" => write::Phase::Preparing,
            "aborted" => write::Phase::Aborted,
            "active" if self.remaining > 0 && self.owned(node, run).is_ok() => write::Phase::Active,
            _ => write::Phase::Expired,
        };
        let mut reply = write::Reply {
            operation: self.operation_id,
            bucket: Some(self.bucket_id),
            stream: self
                .stream_id
                .or_else(|| self.published.as_ref()?.identity.map(|(_, stream)| stream)),
            local_file: if self.intent.part.is_some()
                && !self.profile.cdc_enabled
                && let Some(stream) = self.stream_id
            {
                super::queries::native::file(stream, node.node_id)
                    .fetch_optional(&mut **tx)
                    .await?
            } else {
                None
            },
            phase,
            materials: vec![],
            storage: None,
            grants: vec![],
            published: self
                .published
                .as_ref()
                .filter(|_| phase == write::Phase::Published)
                .map(|p| p.0.clone()),
            copy_references: self.copy_source.as_ref().is_some_and(|v| v.references),
            checksum_algorithm: self.intent.part.as_ref().and_then(|p| p.algorithm.clone()),
            completion: if let Some(manifest) = &self.intent.completion {
                let mut summary = mp::record(manifest.upload)
                    .fetch_one(&mut **tx)
                    .await?
                    .summary
                    .0;
                summary.result = mp::result(manifest.upload).fetch_one(&mut **tx).await?;
                Some(summary)
            } else {
                None
            },
        };
        if phase == write::Phase::Active
            && let Some(cached) = cached
        {
            let material = queries::work::storage(self.storage_id)
                .fetch_one(&mut **tx)
                .await?;
            let storage = read::Storage {
                connection: crate::storage::model::Connection::unprotect(&(&material).into(), key)?,
                revision: self.profile.revision,
                profile: self.profile.0.clone(),
            };
            reply.storage = Some(read::material(
                &mut reply.materials,
                node.deployment_id,
                read::MaterialType::Storage,
                self.storage_id.to_string(),
                &storage,
                cached,
            )?);
            reply.grants.push(read::Grant {
                r#type: read::GrantType::Write,
                id: self.lease_id,
                valid_for_ms: self.remaining as u64,
                storage: None,
            });
        }
        Ok(reply)
    }
}
pub(super) async fn start<'a>(
    b: &'a Backend,
    node: &NodeState,
    key: &Key,
    run: Run,
) -> Result<registry::Tx<'a>> {
    let mut tx = registry::begin(b).await?;
    ensure!(
        matches!(node.role.as_str(), "data" | "node" | "worker"),
        Error::NodeAuthentication
    );
    node_registry::require_work(b, &mut tx, node, key, run).await?;
    Ok(tx)
}
pub(super) async fn lookup(tx: &mut registry::Tx<'_>, node: Uuid, op: Uuid) -> Result<Write> {
    q::owned(node, op)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| Error::NodeOperationMissing.into())
}
async fn end(
    tx: &mut registry::Tx<'_>,
    node: Uuid,
    op: Uuid,
    stream: Option<Id>,
    phase: &str,
) -> Result<()> {
    mp::release_completion(op).execute(&mut **tx).await?;
    if let Some(stream) = stream {
        transactions::abandon_copy_in(tx, stream).await?;
    }
    q::phase(node, op, phase, None, None)
        .execute(&mut **tx)
        .await?;
    q::release(node, op).execute(&mut **tx).await?;
    Ok(())
}
pub(super) async fn expire(tx: &mut registry::Tx<'_>) -> Result<()> {
    for (node, op, stream) in q::expired().fetch_all(&mut **tx).await? {
        end(tx, node, op, stream, "expired").await?;
    }
    q::prune().execute(&mut **tx).await?;
    Ok(())
}
pub(super) async fn permits(
    tx: &mut registry::Tx<'_>,
    row: &Write,
    node: Uuid,
    wanted: usize,
    batch: i64,
    storage: Uuid,
    admitted: Option<i32>,
) -> Result<Vec<read::Grant>> {
    if wanted == 0 {
        return Ok(Vec::new());
    }
    let current = queries::storage_profile(storage)
        .fetch_one(&mut **tx)
        .await?
        .upload_concurrency;
    let limit = match (admitted, current) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    };
    let Some(limit) = limit else {
        return Ok(Vec::new());
    };
    read_grants::lock_storage(tx, storage, "upload").await?;
    let mut held = q::permits(node, row.operation_id, storage, batch)
        .fetch_all(&mut **tx)
        .await?;
    let mut count = q::pool_load(storage).fetch_one(&mut **tx).await?;
    while held.len() < wanted && count < i64::from(limit) {
        let token = Uuid::new_v4();
        q::permit(token, storage, node).execute(&mut **tx).await?;
        q::assign(token, node, row.operation_id, batch)
            .execute(&mut **tx)
            .await?;
        held.push((token, 60_000));
        count += 1;
    }
    ensure!(!held.is_empty(), Error::NodeWriteBusy);
    Ok(held
        .into_iter()
        .take(wanted)
        .map(|(id, left)| read::Grant {
            r#type: read::GrantType::StorageWrite,
            id,
            valid_for_ms: left as u64,
            storage: Some(storage),
        })
        .collect())
}
#[async_trait::async_trait]
impl NodeWrites for Backend {
    async fn check_raw_files(
        &self,
        node: &NodeState,
        key: &Key,
        input: crate::model::node_native::Inventory,
    ) -> Result<Vec<crate::model::node_raw::File>> {
        super::node_files::inventory(self, node, key, input).await
    }
    async fn prepare_tail(
        &self,
        node: &NodeState,
        key: &Key,
        input: crate::model::node_tail::Prepare,
    ) -> Result<crate::model::node_tail::Prepared> {
        let end = input.end()?;
        let hash = input.hash()?;
        let mut tx = start(self, node, key, input.run).await?;
        let row = lookup(&mut tx, node.node_id, input.operation).await?;
        row.owned(node, input.run)?;
        let tail = if let Some(tail) = mp::tail(node.node_id, input.operation)
            .fetch_optional(&mut *tx)
            .await?
        {
            ensure!(tail.request_hash == hash, Error::NodeOperationConflict);
            tail
        } else {
            let stream = row.active(&mut tx, node, input.run).await?;
            let part = row
                .intent
                .part
                .as_ref()
                .filter(|_| row.profile.cdc_enabled && row.copy_source.is_none())
                .ok_or_else(|| s3s::s3_error!(InvalidRequest))?;
            ensure!(
                row.next_offset == input.offset
                    && row.intent.size.is_none_or(|n| n == end)
                    && q::pending(node.node_id, input.operation)
                        .fetch_one(&mut *tx)
                        .await?
                        == 0,
                s3s::s3_error!(InvalidArgument)
            );
            if row.owner_task_id.is_none() {
                transactions::reserve_quota_in(
                    &mut tx,
                    stream,
                    end,
                    false,
                    Some((part.upload, part.number)),
                )
                .await?;
            }
            let settings = mp::settings().fetch_one(&mut *tx).await?;
            let storage = settings
                .staging_storage_id
                .ok_or(Error::SourceUnavailable)?;
            let limit = settings
                .staging_bytes_limit
                .as_deref()
                .map(crate::config::bytes)
                .transpose()?
                .unwrap_or(crate::storage::DEFAULT_STAGING_LIMIT);
            let id = Uuid::now_v7();
            let path = format!("{}fragment/{}", crate::storage::STAGING_PREFIX, id.simple());
            super::staging::reserve(&mut tx, storage, &path, input.size as u64, limit).await?;
            let value = crate::model::StreamTail {
                fragment: id,
                stream,
                offset: input.offset,
                size: input.size,
                hash: input.hash,
                staging_storage: Some(storage),
                staging_key: Some(path),
                file: None,
                file_offset: None,
            };
            let fragment = mp::fragment(&mut tx, &value).await?;
            mp::add_tail(node.node_id, input.operation, &hash, fragment, &value)
                .execute(&mut *tx)
                .await?;
            mp::tail(node.node_id, input.operation)
                .fetch_one(&mut *tx)
                .await?
        };
        let mut reply = tail.reply();
        if !tail.committed {
            row.active(&mut tx, node, input.run).await?;
            let record = queries::work::storage(tail.storage_id)
                .fetch_one(&mut *tx)
                .await?;
            let profile = queries::storage_profile(tail.storage_id)
                .fetch_one(&mut *tx)
                .await?;
            reply.grants = permits(
                &mut tx,
                &row,
                node.node_id,
                1,
                -1,
                tail.storage_id,
                profile.upload_concurrency,
            )
            .await?;
            let material = read::Storage {
                connection: crate::storage::model::Connection::unprotect(&(&record).into(), key)?,
                revision: profile.revision,
                profile,
            };
            reply.storage = Some(read::material(
                &mut reply.materials,
                node.deployment_id,
                read::MaterialType::Storage,
                tail.storage_id.to_string(),
                &material,
                &input.cached,
            )?);
        }
        node_registry::validate_run(&mut tx, node, input.run).await?;
        tx.commit().await?;
        Ok(reply)
    }
    async fn commit_tail(
        &self,
        node: &NodeState,
        key: &Key,
        input: write::Lookup,
    ) -> Result<crate::model::node_tail::Prepared> {
        let mut tx = start(self, node, key, input.run).await?;
        let row = lookup(&mut tx, node.node_id, input.operation).await?;
        row.owned(node, input.run)?;
        let mut tail = mp::tail(node.node_id, input.operation)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(Error::NodeOperationMissing)?;
        if !tail.committed {
            let stream = row.active(&mut tx, node, input.run).await?;
            ensure!(
                row.next_offset == tail.offset_bytes,
                Error::NodeOperationConflict
            );
            ensure!(
                queries::seal_fragment(tail.fragment_id)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected()
                    == 1,
                Error::SourceUnavailable
            );
            mp::attach(stream, tail.offset_bytes, tail.size, tail.fragment_id)
                .execute(&mut *tx)
                .await?;
            let end = tail.offset_bytes + i64::from(tail.size);
            queries::resize_writing_stream(stream, end)
                .execute(&mut *tx)
                .await?;
            q::advance(node.node_id, input.operation, end)
                .execute(&mut *tx)
                .await?;
            mp::finish_tail(node.node_id, input.operation)
                .execute(&mut *tx)
                .await?;
            q::release_batch(node.node_id, input.operation, -1)
                .execute(&mut *tx)
                .await?;
            tail.committed = true;
        }
        node_registry::validate_run(&mut tx, node, input.run).await?;
        tx.commit().await?;
        Ok(tail.reply())
    }
    async fn completion_status(
        &self,
        node: &NodeState,
        key: &Key,
        input: complete::Lookup,
    ) -> Result<(write::Phase, complete::Summary)> {
        input.validate()?;
        let mut tx = start(self, node, key, input.run).await?;
        ensure!(
            matches!(node.role.as_str(), "data" | "node"),
            Error::NodeAuthentication
        );
        let route = ReadRoute::Bucket {
            name: input.bucket,
            key: input.key.clone(),
            alias: None,
        };
        let bucket = requests::node_read_bucket(&route)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
        let permit = Permit::for_action(
            Principal::Service {
                access_key: input.access.clone(),
                revision: input.revision,
            },
            bucket.id,
            Action::Write,
        );
        transactions::lock_permit(&mut tx, &permit).await?;
        let upload = requests::multipart_upload(input.upload, bucket.id, &input.key, &input.access)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchUpload))?;
        let (state, _, live) = queries::completion_state(input.upload)
            .fetch_one(&mut *tx)
            .await?;
        ensure!(
            matches!(state.as_str(), "completing" | "completed"),
            s3s::s3_error!(NoSuchUpload)
        );
        let record = mp::record(input.upload).fetch_one(&mut *tx).await?;
        ensure!(
            record.manifest_hash == input.hash,
            s3s::s3_error!(InvalidRequest, "upload has another completion manifest")
        );
        let mut summary = record.summary.0;
        summary.result = upload.result;
        tx.commit().await?;
        Ok((
            if state == "completed" {
                write::Phase::Published
            } else if live {
                write::Phase::Busy
            } else {
                write::Phase::Expired
            },
            summary,
        ))
    }
    async fn complete_sources(
        &self,
        node: &NodeState,
        key: &Key,
        input: complete::Sources,
    ) -> Result<complete::SourceBatch> {
        use std::collections::{BTreeMap, HashSet};
        ensure!(
            (0..10000).contains(&input.first)
                && !input.reads.is_empty()
                && input.reads.len() <= complete::SOURCES
                && input.reads.iter().all(|id| !id.is_nil())
                && input.reads.iter().collect::<HashSet<_>>().len() == input.reads.len()
                && input.shared_permits.len() <= 16
                && input.shared_permits.iter().collect::<HashSet<_>>().len()
                    == input.shared_permits.len(),
            s3s::s3_error!(InvalidArgument)
        );
        let mut tx = start(self, node, key, input.run).await?;
        let row = lookup(&mut tx, node.node_id, input.operation).await?;
        row.active(&mut tx, node, input.run).await?;
        let upload = row
            .intent
            .completion
            .as_ref()
            .ok_or_else(|| s3s::s3_error!(InvalidRequest))?
            .upload;
        let record = mp::record(upload).fetch_one(&mut *tx).await?;
        ensure!(
            (input.first as usize) < record.summary.parts,
            s3s::s3_error!(InvalidArgument)
        );
        let count = (record.summary.parts - input.first as usize).min(input.reads.len());
        let streams = mp::sources(upload, input.first, count as i64)
            .fetch_all(&mut *tx)
            .await?;
        ensure!(streams.len() == count, Error::SourceUnavailable);
        let rows = mp::shared(node, input.run, &input.shared_permits)
            .fetch_all(&mut *tx)
            .await?;
        ensure!(rows.len() == input.shared_permits.len(), Error::NodeExpired);
        let mut shared: BTreeMap<_, _> = rows
            .into_iter()
            .map(|(id, storage, left)| {
                (
                    storage,
                    read::Grant {
                        r#type: read::GrantType::StorageRead,
                        id,
                        storage: Some(storage),
                        valid_for_ms: left as u64,
                    },
                )
            })
            .collect();
        ensure!(
            shared.len() == input.shared_permits.len(),
            s3s::s3_error!(InvalidArgument)
        );
        let mut parts = Vec::with_capacity(count);
        for (operation, stream) in input.reads.into_iter().zip(streams) {
            let query = read::Query {
                run: input.run,
                operation,
                route: ReadRoute::Bucket {
                    name: format!("completion:{}", input.operation),
                    key: stream.id.to_string(),
                    alias: None,
                },
                access: None,
                head: false,
                web: false,
                mapping: true,
                protection: true,
                policy: false,
                headers: Default::default(),
                overrides: Default::default(),
                cached: input.cached.clone(),
                max_items: 16,
                lanes: 1,
                permits: Default::default(),
                max_bytes: 128 * 1024,
                body: None,
            };
            let mut reply = read::Reply::new(read::Outcome::Ready);
            let object = read::Object::from_stream(&stream)?;
            reply.object = Some(reply.material(
                node.deployment_id,
                read::MaterialType::Object,
                object.id.to_string(),
                &object,
                &query.cached,
            )?);
            reply.length = stream.size as u64;
            reply.etag = stream.etag.clone();
            let value = super::node_reads::protect_node_read(
                &mut tx,
                node,
                key,
                &query,
                &stream,
                reply,
                &mut shared,
            )
            .await?;
            ensure!(
                value.outcome != read::Outcome::Pending,
                Error::NodeSourcePending
            );
            ensure!(
                value.outcome == read::Outcome::Ready,
                Error::SourceUnavailable
            );
            parts.push(value);
        }
        node_registry::validate_run(&mut tx, node, input.run).await?;
        tx.commit().await?;
        let next = input.first + count as i32;
        Ok(complete::SourceBatch {
            first: input.first,
            next: ((next as usize) < record.summary.parts).then_some(next),
            parts,
        })
    }
    async fn prepare_chunks(
        &self,
        node: &NodeState,
        key: &Key,
        input: node_chunk::Prepare,
    ) -> Result<node_chunk::Assembly> {
        use std::collections::{BTreeMap, HashMap};
        let end = input.end()?;
        let hash = input.hash()?;
        let mut tx = start(self, node, key, input.run).await?;
        let row = lookup(&mut tx, node.node_id, input.operation).await?;
        row.owned(node, input.run)?;
        if let Some(batch) = q::batch(node.node_id, input.operation, input.batch)
            .fetch_optional(&mut *tx)
            .await?
        {
            ensure!(batch.request_hash == hash, Error::NodeOperationConflict);
            if batch.commit_hash.is_none() {
                row.active(&mut tx, node, input.run).await?;
            }
            return Ok(node_chunk::Assembly {
                items: batch.plan.0,
                ready: batch.ready,
                committed: batch.commit_hash.is_some(),
            });
        }
        let stream = row.active(&mut tx, node, input.run).await?;
        ensure!(
            row.profile.cdc_enabled
                && mp::tail(node.node_id, input.operation)
                    .fetch_optional(&mut *tx)
                    .await?
                    .is_none()
                && row.next_offset == input.offset
                && row.next_batch == input.batch
                && input.batch < i64::MAX
                && row.intent.size.is_none_or(|n| end <= n)
                && q::raw(node.node_id, input.operation)
                    .fetch_optional(&mut *tx)
                    .await?
                    .is_none(),
            s3s::s3_error!(InvalidRequest)
        );
        ensure!(
            row.intent.part.is_none() || end <= write::MAX_PART_BYTES,
            s3s::s3_error!(EntityTooLarge)
        );
        ensure!(
            q::pending(node.node_id, input.operation)
                .fetch_one(&mut *tx)
                .await?
                < node_chunk::MAX_PENDING,
            Error::NodeWriteBusy
        );
        if row.owner_task_id.is_none() && row.intent.completion.is_none() {
            transactions::reserve_quota_in(
                &mut tx,
                stream,
                end,
                false,
                row.intent.part.as_ref().map(|p| (p.upload, p.number)),
            )
            .await?;
        }
        let algorithm = if let Some(id) = row.block_key_id {
            queries::work::block_key(id)
                .fetch_one(&mut *tx)
                .await?
                .algorithm
        } else {
            "none".to_owned()
        };
        let mut unique = HashMap::new();
        for (identity, item) in input
            .items
            .iter()
            .map(|i| ((i.hash, i.raw_size), i))
            .collect::<BTreeMap<_, _>>()
        {
            let (mut chunk, reused) = transactions::allocate_chunk_in(
                &mut tx,
                crate::model::ChunkCreate {
                    storage: row.storage_id,
                    stream,
                    hash: item.hash,
                    raw_size: item.raw_size,
                    algorithm: algorithm.clone(),
                    key: row.block_key_id,
                },
            )
            .await?;
            if !reused {
                chunk.compressed = item.compressed;
                chunk.stored_size =
                    Some(item.payload_size + if algorithm == "none" { 0 } else { 16 });
            }
            unique.insert(identity, (chunk, reused));
        }
        let mut plan = Vec::with_capacity(input.items.len());
        let mut offset = input.offset;
        for item in &input.items {
            let (chunk, reused) = &unique[&(item.hash, item.raw_size)];
            if *reused {
                transactions::reference_chunk_in(&mut tx, stream, offset, chunk.id, chunk.raw_size)
                    .await?;
            }
            plan.push(node_chunk::Item {
                offset,
                chunk: chunk.clone(),
                reused: *reused,
                payload_hash: item.payload_hash,
            });
            offset += i64::from(item.raw_size);
        }
        q::add_batch(
            node.node_id,
            input.operation,
            input.batch,
            &hash,
            end,
            &plan,
            false,
        )
        .execute(&mut *tx)
        .await?;
        q::advance(node.node_id, input.operation, end)
            .execute(&mut *tx)
            .await?;
        node_registry::validate_run(&mut tx, node, input.run).await?;
        tx.commit().await?;
        Ok(node_chunk::Assembly {
            items: plan,
            ready: false,
            committed: false,
        })
    }
    async fn activate_chunks(
        &self,
        node: &NodeState,
        key: &Key,
        input: node_chunk::Prepare,
        nonces: Vec<node_chunk::Nonce>,
    ) -> Result<node_chunk::Prepared> {
        use std::collections::{HashMap, HashSet};
        let hash = input.hash()?;
        let mut tx = start(self, node, key, input.run).await?;
        let row = lookup(&mut tx, node.node_id, input.operation).await?;
        row.owned(node, input.run)?;
        let mut batch = q::batch(node.node_id, input.operation, input.batch)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(Error::NodeOperationMissing)?;
        ensure!(batch.request_hash == hash, Error::NodeOperationConflict);
        if batch.commit_hash.is_some() {
            return Ok(node_chunk::Prepared {
                packing: None,
                batch: input.batch,
                end: batch.end_bytes,
                committed: true,
                items: vec![],
                materials: vec![],
                storage: None,
                grants: vec![],
            });
        }
        let stream = row.active(&mut tx, node, input.run).await?;
        let needed: HashSet<_> = batch
            .plan
            .iter()
            .filter(|i| !i.reused)
            .map(|i| i.chunk.uuid)
            .collect();
        let mut grants = permits(
            &mut tx,
            &row,
            node.node_id,
            needed.len().min(input.parallelism as usize),
            input.batch,
            row.storage_id,
            row.profile.upload_concurrency,
        )
        .await?;
        if !batch.ready {
            let required: HashSet<_> = batch
                .plan
                .iter()
                .filter(|i| !i.reused && i.chunk.algorithm != "none")
                .map(|i| i.chunk.uuid)
                .collect();
            ensure!(nonces.len() == required.len(), Error::OperationAborted);
            let nonces: HashMap<_, _> = nonces.into_iter().map(|n| (n.chunk, n.value)).collect();
            ensure!(
                nonces.len() == required.len() && required.iter().all(|id| nonces.contains_key(id)),
                Error::OperationAborted
            );
            let mut registered = HashSet::new();
            for item in batch.plan.iter_mut().filter(|i| !i.reused) {
                if let Some(value) = nonces.get(&item.chunk.uuid) {
                    let id =
                        crate::content::nonce::id(Some(value)).ok_or(Error::OperationAborted)?;
                    ensure!(
                        value.as_slice()
                            == crate::content::codec::typed_nonce(
                                item.chunk.created_at,
                                id,
                                crate::content::nonce::CHUNK
                            )?,
                        Error::OperationAborted
                    );
                    item.chunk.nonce = Some(value.clone());
                }
                if registered.insert(item.chunk.uuid) {
                    transactions::start_chunk_upload_in(&mut tx, stream, &item.chunk).await?;
                }
                item.chunk.state = "uploading".into();
            }
            ensure!(
                q::activate(node.node_id, input.operation, input.batch, &batch.plan)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected()
                    == 1,
                Error::OperationAborted
            );
        }
        let mut reply = row
            .reply(&mut tx, node, key, input.run, Some(&input.cached))
            .await?;
        grants.extend(reply.grants);
        let ids: Vec<_> = batch
            .plan
            .iter()
            .filter(|i| !i.reused)
            .filter_map(|i| i.chunk.block_key_id)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let records = queries::node_read_keys(&ids).fetch_all(&mut *tx).await?;
        ensure!(records.len() == ids.len(), Error::SourceUnavailable);
        for record in records {
            let block = read::BlockKey::unprotect(record, key)?;
            read::material(
                &mut reply.materials,
                node.deployment_id,
                read::MaterialType::Key,
                block.id.to_string(),
                &block,
                &input.cached,
            )?;
        }
        node_registry::validate_run(&mut tx, node, input.run).await?;
        tx.commit().await?;
        Ok(node_chunk::Prepared {
            packing: None,
            batch: input.batch,
            end: batch.end_bytes,
            committed: false,
            items: batch.plan.0,
            materials: reply.materials,
            storage: reply.storage,
            grants,
        })
    }
    async fn commit_chunks(
        &self,
        node: &NodeState,
        key: &Key,
        mut input: node_chunk::Commit,
    ) -> Result<i64> {
        use std::collections::HashSet;
        let hash = input.hash()?;
        let mut tx = start(self, node, key, input.run).await?;
        let row = lookup(&mut tx, node.node_id, input.operation).await?;
        row.owned(node, input.run)?;
        let batch = q::batch(node.node_id, input.operation, input.batch)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(Error::NodeOperationMissing)?;
        if let Some(prior) = batch.commit_hash {
            ensure!(prior == hash, Error::NodeOperationConflict);
            return Ok(batch.end_bytes);
        }
        let stream = row.active(&mut tx, node, input.run).await?;
        ensure!(
            batch.ready && input.packed.is_none(),
            Error::NodeOperationConflict
        );
        let mut needed: HashSet<_> = batch
            .plan
            .iter()
            .filter(|i| !i.reused)
            .map(|i| i.chunk.uuid)
            .collect();
        ensure!(
            needed.len() == input.encoded.len()
                && input.encoded.iter().all(|i| needed.contains(&i.chunk)),
            Error::OperationAborted
        );
        for item in batch.plan.iter().filter(|i| !i.reused) {
            if needed.remove(&item.chunk.uuid) {
                transactions::complete_chunk_write_in(&mut tx, stream, item.offset, &item.chunk)
                    .await?;
            } else {
                transactions::reference_chunk_in(
                    &mut tx,
                    stream,
                    item.offset,
                    item.chunk.id,
                    item.chunk.raw_size,
                )
                .await?;
            }
        }
        transactions::require_sources(&mut tx, Some(stream), &[]).await?;
        queries::resize_writing_stream(stream, batch.end_bytes)
            .execute(&mut *tx)
            .await?;
        ensure!(
            q::commit_batch(node.node_id, input.operation, input.batch, &hash)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            Error::OperationAborted
        );
        q::release_batch(node.node_id, input.operation, input.batch)
            .execute(&mut *tx)
            .await?;
        node_registry::validate_run(&mut tx, node, input.run).await?;
        tx.commit().await?;
        Ok(batch.end_bytes)
    }
    async fn copy_write_references(
        &self,
        node: &NodeState,
        key: &Key,
        input: write::CopyBatch,
    ) -> Result<i64> {
        let hash = input.hash()?;
        let mut tx = start(self, node, key, input.run).await?;
        let row = lookup(&mut tx, node.node_id, input.operation).await?;
        row.owned(node, input.run)?;
        if let Some(batch) = q::batch(node.node_id, input.operation, input.batch)
            .fetch_optional(&mut *tx)
            .await?
        {
            ensure!(
                batch.request_hash == hash && batch.commit_hash.as_deref() == Some(hash.as_slice()),
                Error::NodeOperationConflict
            );
            return Ok(batch.end_bytes);
        }
        let stream = row.active(&mut tx, node, input.run).await?;
        let copy = row
            .copy_source
            .as_ref()
            .filter(|c| c.references)
            .ok_or_else(|| s3s::s3_error!(InvalidRequest))?;
        ensure!(
            read_grants::node_read_owned(node, input.run, copy.read)
                .fetch_optional(&mut *tx)
                .await?
                .is_some_and(|l| l.stream_id == copy.stream && l.payload_revision == copy.revision),
            Error::NodeExpired
        );
        ensure!(
            row.next_batch == input.batch
                && row.next_offset == input.offset
                && input.offset < copy.size,
            s3s::s3_error!(InvalidArgument)
        );
        let end = if copy.payload == "raw" {
            let mut ranges = queries::raw_page(copy.stream, input.offset, copy.size)
                .fetch_all(&mut *tx)
                .await?;
            ranges.retain(|r| r.offset_bytes >= input.offset);
            let (start, end, _) = crate::model::raw::copy_window(copy.storage, &ranges)?;
            ensure!(
                start == input.offset && end > start && end <= copy.size,
                Error::SourceUnavailable
            );
            transactions::copy_raw_references_in(
                &mut tx,
                stream,
                copy.stream,
                copy.storage,
                row.profile.revision,
                &ranges,
            )
            .await?;
            end
        } else {
            let extents = queries::replication::extents(copy.stream, input.offset)
                .fetch_all(&mut *tx)
                .await?;
            ensure!(!extents.is_empty(), Error::SourceUnavailable);
            let mut end = input.offset;
            let mut ids = Vec::with_capacity(extents.len());
            for (offset, len, id, fragment, _) in extents {
                ensure!(
                    offset == end && fragment.is_none() && len > 0,
                    Error::SourceUnavailable
                );
                end = end
                    .checked_add(i64::from(len))
                    .ok_or(Error::SourceUnavailable)?;
                ids.push(id.ok_or(Error::SourceUnavailable)?);
            }
            ensure!(end <= copy.size, Error::SourceUnavailable);
            ids.sort_unstable();
            ids.dedup();
            transactions::copy_chunk_references_in(
                &mut tx,
                stream,
                copy.stream,
                input.offset,
                end,
                &ids,
            )
            .await?;
            end
        };
        q::add_batch(
            node.node_id,
            input.operation,
            input.batch,
            &hash,
            end,
            &[],
            true,
        )
        .execute(&mut *tx)
        .await?;
        q::advance(node.node_id, input.operation, end)
            .execute(&mut *tx)
            .await?;
        node_registry::validate_run(&mut tx, node, input.run).await?;
        tx.commit().await?;
        Ok(end)
    }
    async fn begin_write(
        &self,
        node: &NodeState,
        key: &Key,
        mut input: write::Begin,
    ) -> Result<write::Reply> {
        input.intent.validate()?;
        let issued = write::operation_time(input.operation)?;
        let hash = input.intent.hash()?;
        let mut tx = start(self, node, key, input.run).await?;
        ensure!(
            matches!(node.role.as_str(), "data" | "node"),
            Error::NodeAuthentication
        );
        if let Some(previous) = q::owned(node.node_id, input.operation)
            .fetch_optional(&mut *tx)
            .await?
        {
            ensure!(previous.request_hash == hash, Error::NodeOperationConflict);
            previous.owned(node, input.run)?;
            if previous.phase == "active" && previous.remaining > 0 {
                previous.active(&mut tx, node, input.run).await?;
            }
            return previous
                .reply(&mut tx, node, key, input.run, Some(&input.cached))
                .await;
        }
        let now = chrono::Utc::now();
        ensure!(
            issued <= now + chrono::Duration::seconds(30)
                && issued >= now - chrono::Duration::minutes(15),
            Error::NodeOperationExpired
        );
        requests::admit(self, &mut tx, node, key, input.run).await?;
        ensure!(
            q::count(node.node_id).fetch_one(&mut *tx).await? < write::MAX_ACTIVE,
            Error::NodeWriteLimit
        );
        let bucket = requests::node_read_bucket(&ReadRoute::Bucket {
            name: input.intent.bucket.clone(),
            key: input.intent.key.clone(),
            alias: None,
        })
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
        let storage = bucket
            .storage_id
            .ok_or_else(|| s3s::s3_error!(InvalidBucketState))?;
        let target = queries::work::storage(storage)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(Error::SourceUnavailable)?;
        let mut profile = queries::storage_profile(storage)
            .fetch_one(&mut *tx)
            .await?;
        profile.write_durability = None;
        let mut authority = Permit::for_action(
            Principal::Service {
                access_key: input.intent.access.clone(),
                revision: input.intent.credential_revision,
            },
            bucket.id,
            Action::Write,
        );
        let copy = if let Some(id) = input.intent.copy_read {
            let lease = read_grants::node_read_owned(node, input.run, id)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(Error::NodeExpired)?;
            let source = read_grants::node_read_stream(lease.stream_id)
                .fetch_one(&mut *tx)
                .await?;
            ensure!(
                lease.offset_bytes == 0
                    && lease.end_bytes == source.size
                    && lease.payload_revision == source.payload_revision
                    && input.intent.size == Some(source.size),
                Error::NodeOperationConflict
            );
            authority.add(source.bucket_id, Action::Read);
            let src: Uuid = sqlx::query_scalar("SELECT storage_id FROM buckets WHERE id=?")
                .bind(source.bucket_id)
                .fetch_one(&mut *tx)
                .await?;
            let references = src == storage
                && (source.payload_type == "chunks" && profile.cdc_enabled
                    || source.payload_type == "raw"
                        && source.size > 0
                        && !profile.cdc_enabled
                        && !profile.compression_enabled
                        && !profile.pack_enabled
                        && target.block_key_id.is_none()
                        && queries::raw_copyable(source.id, storage)
                            .build_query_scalar::<bool>()
                            .fetch_one(&mut *tx)
                            .await?);
            Some(write::CopySource {
                read: id,
                stream: source.id,
                revision: source.payload_revision,
                size: source.size,
                etag: source.etag,
                checksums: source.checksums,
                payload: source.payload_type,
                storage: src,
                references,
            })
        } else {
            None
        };
        transactions::lock_permit(&mut tx, &authority).await?;
        let mut metadata = input.intent.metadata.clone();
        let mut public = input.intent.public;
        let mut object_completion = None;
        if let Some(manifest) = &input.intent.completion {
            let (upload, summary, live) =
                completion(&mut tx, &input, bucket.id, &authority, manifest).await?;
            if live || upload.state == "completed" {
                let reply = write::Reply {
                    operation: input.operation,
                    bucket: Some(bucket.id),
                    stream: None,
                    local_file: None,
                    phase: if upload.state == "completed" {
                        write::Phase::Published
                    } else {
                        write::Phase::Busy
                    },
                    materials: vec![],
                    storage: None,
                    grants: vec![],
                    published: None,
                    copy_references: false,
                    checksum_algorithm: summary.algorithm.clone(),
                    completion: Some(summary),
                };
                tx.commit().await?;
                return Ok(reply);
            }
            metadata = upload.metadata;
            public = upload.public_read;
            object_completion = Some(summary);
        }
        if let Some(part) = &mut input.intent.part {
            let upload = requests::multipart_upload(
                part.upload,
                bucket.id,
                &input.intent.key,
                &input.intent.access,
            )
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchUpload))?;
            ensure!(upload.state == "active", s3s::s3_error!(NoSuchUpload));
            ensure!(
                part.algorithm
                    .as_ref()
                    .is_none_or(|a| upload.checksum_algorithm.as_ref().is_none_or(|b| a == b)),
                s3s::s3_error!(InvalidRequest)
            );
            part.algorithm = upload.checksum_algorithm.or_else(|| part.algorithm.clone());
            metadata = upload.metadata;
        }
        let stream = transactions::create_stream_in(
            &mut tx,
            StreamCreate {
                authority: authority.clone(),
                bucket: bucket.id,
                key: input.intent.key.clone(),
                purpose: if input.intent.part.is_some() {
                    StreamPurpose::Part
                } else {
                    StreamPurpose::Object
                },
                metadata,
                public,
                completion: object_completion.as_ref().map(|x| x.upload),
                owner: None,
            },
        )
        .await?;
        let part = input.intent.part.as_ref().map(|p| (p.upload, p.number));
        if let Some((upload, _)) = part
            && !profile.cdc_enabled
        {
            super::native_parts::open(
                &mut tx,
                upload,
                storage,
                profile.revision,
                stream,
                node.node_id,
                target.block_key_id,
                &profile,
            )
            .await?;
        }

        if let Some((upload, number)) = part {
            transactions::begin_upload_part_in(&mut tx, &authority, upload, number, stream).await?;
        }
        if let Some(summary) = &object_completion {
            transactions::complete_quota_in(&mut tx, summary.upload, stream, summary.size).await?;
        } else {
            transactions::reserve_quota_in(
                &mut tx,
                stream,
                input.intent.size.unwrap_or(0),
                false,
                part,
            )
            .await?;
        }
        q::insert(
            node,
            &input,
            &hash,
            stream,
            bucket.id,
            storage,
            target.block_key_id,
            &profile,
            copy.as_ref(),
        )
        .execute(&mut *tx)
        .await?;
        let observed = lookup(&mut tx, node.node_id, input.operation).await?;
        let reply = observed
            .reply(&mut tx, node, key, input.run, Some(&input.cached))
            .await?;
        node_registry::validate_run(&mut tx, node, input.run).await?;
        tx.commit().await?;
        Ok(reply)
    }
    async fn write_status(
        &self,
        node: &NodeState,
        key: &Key,
        input: write::Lookup,
        abort: bool,
    ) -> Result<write::Reply> {
        let mut tx = start(self, node, key, input.run).await?;
        let mut row = lookup(&mut tx, node.node_id, input.operation).await?;
        if abort {
            row.owned(node, input.run)?;
            if row.phase == "active" {
                end(
                    &mut tx,
                    node.node_id,
                    input.operation,
                    row.stream_id,
                    "aborted",
                )
                .await?;
                row = lookup(&mut tx, node.node_id, input.operation).await?;
            }
        }
        let cached = (!abort && row.owner_task_id.is_some()).then_some([].as_slice());
        let reply = row.reply(&mut tx, node, key, input.run, cached).await?;
        tx.commit().await?;
        Ok(reply)
    }
    async fn prepare_raw(
        &self,
        node: &NodeState,
        key: &Key,
        input: node_raw::Prepare,
    ) -> Result<node_raw::Prepared> {
        input.input.validate()?;
        let hash = node_chunk::hash(&input.input)?;
        let mut tx = start(self, node, key, input.run).await?;
        let row = lookup(&mut tx, node.node_id, input.operation).await?;
        row.owned(node, input.run)?;
        let size = input.input.size;
        let existing = q::raw(node.node_id, input.operation)
            .fetch_optional(&mut *tx)
            .await?;
        let (source, committed) = if let Some(raw) = existing {
            ensure!(raw.request_hash == hash, Error::NodeOperationConflict);
            if !raw.committed {
                row.active(&mut tx, node, input.run).await?;
            }
            (raw.location_id, raw.committed)
        } else {
            let stream = row.active(&mut tx, node, input.run).await?;
            ensure!(
                row.next_batch == 1
                    && row.next_offset == 0
                    && row.owner_task_id.is_none()
                    && (row.intent.part.is_none() || row.profile.cdc_enabled && input.input.local)
                    && (!input.input.local || row.profile.upload_cache)
                    && (row.profile.cdc_enabled
                        && (row.profile.write_processing == "background" || input.input.local)
                        || !row.profile.cdc_enabled
                            && !row.profile.pack_enabled
                            && !row.profile.compression_enabled
                            && row.block_key_id.is_none())
                    && (!row.profile.upload_cache
                        || input.input.local
                        || row.profile.upload_cache_overflow == "fallback")
                    && (!row.profile.cdc_enabled
                        && row.profile.deduplication_enabled
                        && row.profile.write_processing == "inline")
                        == input.input.hash.is_some(),
                s3s::s3_error!(InvalidRequest, "write does not accept this raw source")
            );
            ensure!(
                row.intent.size.is_none_or(|s| s == size),
                s3s::s3_error!(IncompleteBody)
            );
            transactions::reserve_quota_in(
                &mut tx,
                stream,
                size,
                false,
                row.intent.part.as_ref().map(|p| (p.upload, p.number)),
            )
            .await?;
            let duplicate = if let Some(hash) = &input.input.hash {
                queries::raw_duplicate(row.storage_id, hash, size)
                    .fetch_optional(&mut *tx)
                    .await?
                    .filter(|s| {
                        s.validate().is_ok()
                            && !s.pending_upload
                            && s.medium == "shared_s3"
                            && s.digest.as_deref() == Some(input.input.digest.as_slice())
                    })
            } else {
                None
            };
            ensure!(
                queries::raw_prepare(stream)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected()
                    == 1,
                Error::OperationAborted
            );
            let committed = duplicate.is_some();
            let (id, location) = if let Some(source) = duplicate {
                queries::raw_attach(stream, source.id, size)
                    .execute(&mut *tx)
                    .await?;
                queries::raw_reference(source.id).execute(&mut *tx).await?;
                (source.id, source.location_id)
            } else {
                let location = Uuid::now_v7();
                (
                    q::raw_source(
                        &mut tx,
                        location,
                        stream,
                        row.storage_id,
                        row.profile.revision,
                        if input.input.local {
                            "local"
                        } else {
                            "shared_s3"
                        },
                    )
                    .await?,
                    location,
                )
            };
            for (index, hashes) in input
                .input
                .page_hashes
                .chunks(crate::content::raw_pages::GROUP * 32)
                .enumerate()
            {
                super::raw_pages::put(
                    &mut tx,
                    id,
                    &crate::content::raw_pages::Group {
                        index: index as i64,
                        hashes: hashes.to_vec(),
                    },
                )
                .await?;
            }
            super::raw_pages::seal(&mut tx, id, size, &input.input.digest).await?;
            q::add_raw(
                node.node_id,
                input.operation,
                &hash,
                id,
                location,
                &input.input,
                committed,
            )
            .execute(&mut *tx)
            .await?;
            q::advance(node.node_id, input.operation, size)
                .execute(&mut *tx)
                .await?;
            (location, committed)
        };
        let grants = permits(
            &mut tx,
            &row,
            node.node_id,
            usize::from(!committed && !input.input.local),
            0,
            row.storage_id,
            row.profile.upload_concurrency,
        )
        .await?;
        node_registry::validate_run(&mut tx, node, input.run).await?;
        tx.commit().await?;
        Ok(node_raw::Prepared {
            source,
            size,
            committed,
            grants,
        })
    }
    async fn commit_raw(
        &self,
        node: &NodeState,
        key: &Key,
        input: node_raw::Commit,
    ) -> Result<node_raw::Prepared> {
        let mut tx = start(self, node, key, input.run).await?;
        let row = lookup(&mut tx, node.node_id, input.operation).await?;
        row.owned(node, input.run)?;
        let raw = q::raw(node.node_id, input.operation)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(Error::NodeOperationMissing)?;
        ensure!(
            raw.location_id == input.source,
            Error::NodeOperationConflict
        );
        if !raw.committed {
            let stream = row.active(&mut tx, node, input.run).await?;
            ensure!(
                queries::raw_finish(stream, raw.source_id, raw.size, &raw.digest, true)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected()
                    == 1,
                Error::OperationAborted
            );
            queries::raw_attach(stream, raw.source_id, raw.size)
                .execute(&mut *tx)
                .await?;
            let source = queries::raw_source(raw.source_id)
                .fetch_one(&mut *tx)
                .await?;
            if row.profile.cdc_enabled {
                let algorithm = if let Some(id) = row.block_key_id {
                    queries::work::block_key(id)
                        .fetch_one(&mut *tx)
                        .await?
                        .algorithm
                } else {
                    "none".into()
                };
                let encoding = crate::model::cache::EncodingPolicy {
                    compression: row.profile.compression_config("30s"),
                    pack_enabled: row.profile.pack_enabled,
                    pack_max_size: row.profile.pack_max_size.clone(),
                    pack_flush_timeout: row.profile.pack_upload_flush_timeout.clone(),
                    algorithm,
                    block_key_id: row.block_key_id,
                };
                transactions::enqueue_conversion(
                    &mut tx,
                    stream,
                    raw.source_id,
                    row.storage_id,
                    &encoding,
                    (source.medium == "local").then_some(node.node_id),
                )
                .await?;
            }
            if source.medium == "local" {
                ensure!(
                    queries::raw_stage(raw.source_id, row.profile.revision, None)
                        .execute(&mut *tx)
                        .await?
                        .rows_affected()
                        == 1,
                    Error::OperationAborted
                );
                queries::raw_queue(
                    raw.source_id,
                    !row.profile.cdc_enabled && row.profile.deduplication_enabled,
                    row.profile.write_processing == "background",
                )
                .execute(&mut *tx)
                .await?;
                if let Some(hash) = &raw.content_hash {
                    queries::raw_upload_hash(raw.source_id, Some(hash))
                        .execute(&mut *tx)
                        .await?;
                }
                super::node_files::queue(
                    &mut tx,
                    node.node_id,
                    stream,
                    raw.source_id,
                    row.storage_id,
                )
                .await?;
            } else if let Some(hash) = &raw.content_hash {
                let source = queries::raw_source(raw.source_id)
                    .fetch_one(&mut *tx)
                    .await?;
                transactions::share_raw(&mut tx, stream, &source, hash).await?;
            } else if !row.profile.cdc_enabled
                && row.profile.deduplication_enabled
                && row.profile.write_processing == "background"
            {
                queries::raw_enqueue_processing(stream, raw.source_id)
                    .execute(&mut *tx)
                    .await?;
            }
            q::commit_raw(node.node_id, input.operation)
                .execute(&mut *tx)
                .await?;
            q::release(node.node_id, input.operation)
                .execute(&mut *tx)
                .await?;
        }
        node_registry::validate_run(&mut tx, node, input.run).await?;
        tx.commit().await?;
        Ok(node_raw::Prepared {
            source: input.source,
            size: raw.size,
            committed: true,
            grants: vec![],
        })
    }
    async fn publish_write(
        &self,
        node: &NodeState,
        key: &Key,
        mut input: write::Publish,
    ) -> Result<write::Reply> {
        let hash = input.hash()?;
        let mut tx = start(self, node, key, input.run).await?;
        let row = lookup(&mut tx, node.node_id, input.operation).await?;
        row.owned(node, input.run)?;
        if let Some(prior) = &row.publish_hash {
            ensure!(prior == &hash, Error::NodeOperationConflict);
            return row.reply(&mut tx, node, key, input.run, None).await;
        }
        let stream = row.active(&mut tx, node, input.run).await?;
        ensure!(
            input.size == row.next_offset && row.intent.size.is_none_or(|s| s == input.size),
            s3s::s3_error!(IncompleteBody)
        );
        if let Some(copy) = &row.copy_source {
            ensure!(
                input.size == copy.size
                    && input.etag == copy.etag
                    && serde_json::to_value(&input.checksums)? == copy.checksums,
                s3s::s3_error!(InvalidRequest)
            );
        }
        if let Some(raw) = q::raw(node.node_id, input.operation)
            .fetch_optional(&mut *tx)
            .await?
        {
            ensure!(
                raw.committed
                    && raw.size == input.size
                    && (row.copy_source.is_some()
                        || row.intent.completion.is_some()
                        || hex::encode(&raw.digest) == input.etag),
                s3s::s3_error!(IncompleteBody)
            );
        }
        ensure!(
            mp::tail(node.node_id, input.operation)
                .fetch_optional(&mut *tx)
                .await?
                .is_none_or(|t| t.committed),
            s3s::s3_error!(IncompleteBody)
        );
        let mut sums = serde_json::to_value(&input.checksums)?;
        let mut finish_upload = None;
        if let Some(manifest) = &row.intent.completion {
            let record = mp::record(manifest.upload).fetch_one(&mut *tx).await?;
            ensure!(
                input.size == record.summary.size
                    && input.etag == record.summary.etag
                    && input.if_match.is_none()
                    && input.if_none.is_none(),
                s3s::s3_error!(InvalidRequest, "completion differs from its manifest")
            );
            let parts = mp::checksums(manifest.upload).fetch_all(&mut *tx).await?;
            sums = complete::checksums(
                sums,
                record.summary.algorithm.as_deref(),
                record.summary.checksum_type.as_deref(),
                &parts,
                &record.conditions.checksums,
            )?;
            input.if_match = record.conditions.if_match.clone();
            input.if_none = record.conditions.if_none.clone();
            finish_upload = Some((
                manifest.upload,
                record.manifest_hash,
                serde_json::json!({"etag":input.etag,"checksums":sums}),
                input.operation,
            ));
        }
        ensure!(
            queries::replication::finish_stream(stream, input.size, &input.etag, sums)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            Error::OperationAborted
        );
        ensure!(
            q::pending(node.node_id, input.operation)
                .fetch_one(&mut *tx)
                .await?
                == 0,
            s3s::s3_error!(IncompleteBody)
        );
        let mut phase = "published";
        if let Some(task) = row.owner_task_id {
            super::work_conversion::seal(&mut tx, &row, task, &input).await?;
        } else if let Some(part) = &row.intent.part {
            ensure!(
                input.etag.len() == 32 && input.if_match.is_none() && input.if_none.is_none(),
                s3s::s3_error!(InvalidRequest)
            );
            if let Some(algorithm) = &part.algorithm {
                ensure!(
                    input
                        .checksums
                        .contains_key(&algorithm.to_ascii_lowercase()),
                    s3s::s3_error!(InvalidRequest)
                );
            }
            let authority = queries::write_authorization(stream)
                .fetch_one(&mut *tx)
                .await?;
            transactions::commit_upload_part_in(
                &mut tx,
                &authority,
                row.bucket_id,
                part.upload,
                part.number,
                stream,
            )
            .await?;
            if !row.profile.cdc_enabled {
                super::native_parts::publish_part(
                    &mut tx,
                    &row,
                    node.node_id,
                    part,
                    stream,
                    input.size,
                )
                .await?;
            }
        } else {
            let condition = |s: Option<String>| {
                s.as_deref()
                    .map(|s| s3s::dto::ETagCondition::parse_http_header(s.as_bytes()))
                    .transpose()
            };
            match transactions::publish_in(
                &mut tx,
                stream,
                condition(input.if_match)?,
                condition(input.if_none)?,
                finish_upload,
            )
            .await
            {
                Ok(true) => {}
                Ok(false) => return Err(Error::OperationAborted.into()),
                Err(err) if err.is::<crate::model::ReplicaPending>() => {
                    let publication = err
                        .downcast_ref::<crate::model::ReplicaPending>()
                        .unwrap()
                        .0;
                    ensure!(
                        q::replica(publication, node.node_id, input.operation)
                            .execute(&mut *tx)
                            .await?
                            .rows_affected()
                            == 1,
                        Error::OperationAborted
                    );
                    phase = "preparing";
                }
                Err(err) => {
                    if let Some(reason) = crate::model::replica_rejection(&err) {
                        tx.rollback().await?;
                        let mut recovery = registry::begin(self).await?;
                        super::replica_transactions::reap_admission(
                            &mut recovery,
                            stream,
                            Some(reason),
                        )
                        .await?;
                        recovery.commit().await?;
                    }
                    return Err(err);
                }
            }
        }
        let saved = queries::quota_stream(stream).fetch_one(&mut *tx).await?;
        let result = write::Published {
            identity: Some((row.bucket_id, stream)),
            size: input.size,
            etag: input.etag,
            modified: saved.touched_at,
        };
        q::phase(
            node.node_id,
            input.operation,
            phase,
            Some(&hash),
            Some(&result),
        )
        .execute(&mut *tx)
        .await?;
        q::release(node.node_id, input.operation)
            .execute(&mut *tx)
            .await?;
        node_registry::validate_run(&mut tx, node, input.run).await?;
        let row = lookup(&mut tx, node.node_id, input.operation).await?;
        let reply = row.reply(&mut tx, node, key, input.run, None).await?;
        tx.commit().await?;
        Ok(reply)
    }
}

pub(super) async fn verify_replica(
    tx: &mut registry::Tx<'_>,
    stream: Id,
    node: Option<(Uuid, Uuid)>,
) -> Result<()> {
    if let Some((node, op)) = node {
        let row = lookup(tx, node, op).await?;
        ensure!(
            row.stream_id == Some(stream) && row.phase == "preparing",
            Error::OperationAborted
        );
        let authority = queries::write_authorization(stream)
            .fetch_one(&mut **tx)
            .await?;
        transactions::lock_permit(tx, &authority).await?;
    }
    Ok(())
}
pub(super) async fn settle_replica(
    tx: &mut registry::Tx<'_>,
    node: Option<(Uuid, Uuid)>,
    published: bool,
) -> Result<()> {
    if let Some((node, op)) = node {
        let row = lookup(tx, node, op).await?;
        ensure!(row.phase == "preparing", Error::OperationAborted);
        q::phase(
            node,
            op,
            if published { "published" } else { "aborted" },
            row.publish_hash.as_deref(),
            row.published.as_ref().map(|r| &r.0),
        )
        .execute(&mut **tx)
        .await?;
        q::release(node, op).execute(&mut **tx).await?;
    }
    Ok(())
}
