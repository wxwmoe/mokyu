use super::{
    Backend, node_registry,
    queries::{
        self, node_registry as registry, node_requests as requests, node_writes as q, read_grants,
    },
    transactions,
};
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
    operation_id: Uuid,
    lease_id: Uuid,
    boot_id: Uuid,
    fence: Uuid,
    credential_generation: i64,
    request_hash: Vec<u8>,
    stream_id: Option<Id>,
    bucket_id: Uuid,
    storage_id: Uuid,
    block_key_id: Option<Uuid>,
    profile: Json<StorageProfile>,
    intent: Json<write::Intent>,
    copy_source: Option<Json<write::CopySource>>,
    next_offset: i64,
    next_batch: i64,
    phase: String,
    publish_hash: Option<Vec<u8>>,
    published: Option<Json<write::Published>>,
    remaining: i64,
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
impl Write {
    fn owned(&self, node: &NodeState, run: Run) -> Result<()> {
        ensure!(
            self.credential_generation == node.credential_generation
                && self.boot_id == run.boot
                && self.fence == run.fence,
            Error::NodeExpired
        );
        Ok(())
    }
    async fn active(&self, tx: &mut registry::Tx<'_>, node: &NodeState, run: Run) -> Result<Id> {
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
    async fn reply(
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
            completion: None,
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
async fn start<'a>(
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
async fn lookup(tx: &mut registry::Tx<'_>, node: Uuid, op: Uuid) -> Result<Write> {
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
async fn permits(
    tx: &mut registry::Tx<'_>,
    row: &Write,
    node: Uuid,
    wanted: usize,
) -> Result<Vec<read::Grant>> {
    if wanted == 0 {
        return Ok(Vec::new());
    }
    let storage = row.storage_id;
    let current = queries::storage_profile(storage)
        .fetch_one(&mut **tx)
        .await?
        .upload_concurrency;
    let limit = match (row.profile.upload_concurrency, current) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    };
    let Some(limit) = limit else {
        return Ok(Vec::new());
    };
    read_grants::lock_storage(tx, storage, "upload").await?;
    let mut held = q::permits(node, row.operation_id, storage)
        .fetch_all(&mut **tx)
        .await?;
    let mut count = q::pool_load(storage).fetch_one(&mut **tx).await?;
    while held.len() < wanted && count < i64::from(limit) {
        let token = Uuid::new_v4();
        q::permit(token, storage, node).execute(&mut **tx).await?;
        q::assign(token, node, row.operation_id)
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
    async fn begin_write(
        &self,
        node: &NodeState,
        key: &Key,
        input: write::Begin,
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
        ensure!(
            input.intent.completion.is_none(),
            s3s::s3_error!(InvalidRequest, "completion requires an active manifest")
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
        if let Some(part) = &input.intent.part {
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
                public: input.intent.public,
                completion: None,
                owner: None,
            },
        )
        .await?;
        let part = input.intent.part.as_ref().map(|p| (p.upload, p.number));
        if let Some((upload, number)) = part {
            transactions::begin_upload_part_in(&mut tx, &authority, upload, number, stream).await?;
        }
        transactions::reserve_quota_in(
            &mut tx,
            stream,
            input.intent.size.unwrap_or(0),
            false,
            part,
        )
        .await?;
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
        let reply = row.reply(&mut tx, node, key, input.run, None).await?;
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
                    && row.intent.part.is_none()
                    && !input.input.local
                    && !row.profile.cdc_enabled
                    && !row.profile.pack_enabled
                    && !row.profile.compression_enabled
                    && row.block_key_id.is_none()
                    && (!row.profile.upload_cache
                        || row.profile.upload_cache_overflow == "fallback")
                    && (row.profile.deduplication_enabled
                        && row.profile.write_processing == "inline")
                        == input.input.hash.is_some(),
                s3s::s3_error!(InvalidRequest, "write does not accept this raw source")
            );
            ensure!(
                row.intent.size.is_none_or(|s| s == size),
                s3s::s3_error!(IncompleteBody)
            );
            transactions::reserve_quota_in(&mut tx, stream, size, false, None).await?;
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
                        "shared_s3",
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
        let grants = permits(&mut tx, &row, node.node_id, usize::from(!committed)).await?;
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
            if let Some(hash) = &raw.content_hash {
                let source = queries::raw_source(raw.source_id)
                    .fetch_one(&mut *tx)
                    .await?;
                transactions::share_raw(&mut tx, stream, &source, hash).await?;
            } else if row.profile.deduplication_enabled
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
        input: write::Publish,
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
                    && (row.copy_source.is_some() || hex::encode(&raw.digest) == input.etag),
                s3s::s3_error!(IncompleteBody)
            );
        }
        ensure!(
            queries::replication::finish_stream(
                stream,
                input.size,
                &input.etag,
                serde_json::to_value(&input.checksums)?
            )
            .execute(&mut *tx)
            .await?
            .rows_affected()
                == 1,
            Error::OperationAborted
        );
        let mut phase = "published";
        if let Some(part) = &row.intent.part {
            ensure!(
                input.etag.len() == 32
                    && input.if_match.is_none()
                    && input.if_none.is_none()
                    && row.profile.cdc_enabled,
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
                None,
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
