use super::*;

pub(in super::super) async fn begin_node_write(
    backend: &super::super::Backend,
    state: &crate::cluster::NodeState,
    key: &crate::content::codec::Key,
    input: crate::model::node_write::Begin,
) -> Result<crate::model::node_write::Reply> {
    use crate::model::{Error, node_write as write};
    input.intent.validate()?;
    let issued = write::operation_time(input.operation)?;
    let hash = input.intent.hash()?;
    let mut tx = backend.pool.begin().await?;
    backend.fence_control(&mut tx).await?;
    authenticate_rpc_node(&mut tx, state, key, false).await?;
    ensure!(
        matches!(state.role.as_str(), "data" | "node"),
        Error::NodeAuthentication
    );
    let session = require_node_run(&mut tx, state, input.run, true).await?;
    if let Some(owned) = queries::node_write(state.node_id, input.operation)
        .fetch_optional(&mut *tx)
        .await?
    {
        ensure!(owned.request_hash == hash, Error::NodeOperationConflict);
        ensure!(
            owned.credential_generation == state.credential_generation
                && owned.boot_id == input.run.boot
                && owned.fence == input.run.fence,
            Error::NodeExpired
        );
        if owned.state == "active" && owned.valid_for_ms > 0 {
            let authority: sqlx::types::Json<Permit> =
                queries::write_authorization(owned.stream_id.ok_or(Error::NodeExpired)?.into())
                    .fetch_one(&mut *tx)
                    .await?;
            authority.lock(&mut tx).await?;
        }
        let reply =
            node_write_reply(&mut tx, state, key, input.run, owned, Some(&input.cached)).await?;
        tx.commit().await?;
        return Ok(reply);
    }
    let now = queries::node_clock().fetch_one(&mut *tx).await?;
    ensure!(
        issued <= now + chrono::Duration::seconds(30)
            && issued >= now - chrono::Duration::minutes(15),
        Error::NodeOperationExpired
    );
    ensure!(
        !session.draining && queries::node_read_admission().fetch_one(&mut *tx).await?,
        Error::NodeInactive
    );
    queries::node_write_cleanup(state.node_id)
        .execute(&mut *tx)
        .await?;
    ensure!(
        queries::node_write_count(state.node_id)
            .fetch_one(&mut *tx)
            .await?
            < write::MAX_ACTIVE,
        Error::NodeWriteLimit
    );
    let revision = queries::node_write_credential(&input.intent.access)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(AccessDenied))?;
    ensure!(
        revision == input.intent.credential_revision,
        s3s::s3_error!(AccessDenied)
    );
    let route = crate::model::ReadRoute::Bucket {
        name: input.intent.bucket.clone(),
        key: input.intent.key.clone(),
        alias: None,
    };
    let bucket = queries::node_read_bucket(&route)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    let mut authority = Permit::for_action(
        Principal::Service {
            access_key: input.intent.access.clone(),
            revision,
        },
        bucket.id,
        Action::Write,
    );
    let mut copy_source = if let Some(id) = input.intent.copy_read {
        let lease = queries::node_read_owned(state, input.run, id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(Error::NodeExpired)?;
        let source = queries::node_read_stream(lease.stream_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(Error::NodeExpired)?;
        ensure!(
            lease.offset_bytes == 0
                && lease.end_bytes == source.size
                && lease.payload_revision == source.payload_revision
                && input.intent.size == Some(source.size),
            Error::NodeOperationConflict
        );
        authority.add(source.bucket_id, Action::Read);
        let storage = queries::storage_for_stream(source.id)
            .fetch_one(&mut *tx)
            .await?
            .ok_or(Error::SourceUnavailable)?;
        Some(write::CopySource {
            read: id,
            stream: source.id,
            revision: source.payload_revision,
            size: source.size,
            etag: source.etag,
            checksums: source.checksums,
            payload: source.payload_type,
            storage,
            references: false,
        })
    } else {
        None
    };
    authority.lock(&mut tx).await?;
    let mut completion = None;
    let upload = if let Some(manifest) = &input.intent.completion {
        let (upload, summary, live) =
            prepare_node_completion(&mut tx, &input, bucket.id, &authority, manifest).await?;
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
                checksum_algorithm: upload.checksum_algorithm.clone(),
                completion: Some(summary),
            };
            tx.commit().await?;
            return Ok(reply);
        }
        completion = Some(summary);
        Some(upload)
    } else if let Some(part) = &input.intent.part {
        let upload = queries::multipart_upload(
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
            part.algorithm.as_ref().is_none_or(|algorithm| upload
                .checksum_algorithm
                .as_ref()
                .is_none_or(|expected| expected == algorithm)),
            s3s::s3_error!(
                InvalidRequest,
                "part checksum algorithm differs from upload"
            )
        );
        Some(upload)
    } else {
        None
    };
    let checksum_algorithm = upload
        .as_ref()
        .and_then(|upload| upload.checksum_algorithm.clone())
        .or_else(|| {
            input
                .intent
                .part
                .as_ref()
                .and_then(|part| part.algorithm.clone())
        });
    let stream = create_stream_in(
        &mut tx,
        StreamCreate {
            authority,
            bucket: bucket.id,
            key: input.intent.key.clone(),
            purpose: if input.intent.part.is_some() {
                crate::model::StreamPurpose::Part
            } else {
                crate::model::StreamPurpose::Object
            },
            metadata: upload.as_ref().map_or_else(
                || input.intent.metadata.clone(),
                |upload| upload.metadata.clone(),
            ),
            public: completion
                .as_ref()
                .is_some_and(|_| upload.as_ref().unwrap().public_read)
                || input.intent.public,
            completion: completion.as_ref().map(|value| value.upload),
            owner: None,
        },
        Some(write::LEASE_SECONDS),
    )
    .await?;
    if let Some(part) = &input.intent.part {
        ensure!(
            queries::lock_upload_state(part.upload)
                .fetch_optional(&mut *tx)
                .await?
                .as_deref()
                == Some("active"),
            s3s::s3_error!(NoSuchUpload)
        );
        queries::begin_upload_part(part.upload, part.number, stream)
            .execute(&mut *tx)
            .await?;
    }
    let storage = queries::storage_for_stream(stream)
        .fetch_one(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(InvalidBucketState))?;
    let (r#type, block_key) = queries::node_write_storage(storage)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(Error::SourceUnavailable)?;
    ensure!(r#type == "s3", Error::SourceUnavailable);
    if let Some(id) = block_key {
        queries::lock_managed_key(id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(Error::SourceUnavailable)?;
        ensure!(
            queries::valid_storage_key(id).fetch_one(&mut *tx).await?,
            Error::SourceUnavailable
        );
    }
    let mut profile = queries::storage_profile(storage)
        .fetch_one(&mut *tx)
        .await?;
    profile.write_durability = None;
    if let Some(source) = &mut copy_source {
        source.references = source.storage == storage
            && match source.payload.as_str() {
                "chunks" => profile.cdc_enabled,
                "raw"
                    if source.size > 0
                        && !profile.cdc_enabled
                        && !profile.compression_enabled
                        && !profile.pack_enabled
                        && block_key.is_none() =>
                {
                    queries::raw_copyable(source.stream, storage)
                        .build_query_scalar::<bool>()
                        .fetch_one(&mut *tx)
                        .await?
                }
                _ => false,
            };
    }
    if let Some(upload) = input
        .intent
        .part
        .as_ref()
        .map(|part| part.upload)
        .or_else(|| completion.as_ref().map(|value| value.upload))
    {
        native::validate(&mut tx, upload, &profile, block_key, completion.is_some()).await?;
    }
    if let Some(completion) = &completion {
        queries::lock_quota(bucket.id).execute(&mut *tx).await?;
        ensure!(
            queries::complete_quota(completion.upload, stream, completion.size)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            "multipart quota reservation is missing"
        );
    } else {
        queries::reserve_quota(
            stream,
            input.intent.size.unwrap_or(0),
            false,
            input.intent.part.as_ref().map(|part| part.upload),
            input.intent.part.as_ref().map(|part| part.number),
        )
        .fetch_one(&mut *tx)
        .await?;
    }
    ensure!(
        queries::insert_node_write(
            state,
            &input,
            &hash,
            stream,
            &profile,
            block_key,
            copy_source.as_ref(),
            checksum_algorithm.as_deref()
        )
        .execute(&mut *tx)
        .await?
        .rows_affected()
            == 1,
        Error::OperationAborted
    );
    if let Some(completion) = completion {
        queries::bind_node_completion(
            state.node_id,
            input.operation,
            completion.upload,
            completion.size,
        )
        .execute(&mut *tx)
        .await?;
    }
    if let Some(part) = &input.intent.part
        && !profile.cdc_enabled
    {
        native::begin(
            &mut tx,
            state.node_id,
            stream,
            part.upload,
            &profile,
            block_key,
        )
        .await?;
    }
    let owned = queries::node_write(state.node_id, input.operation)
        .fetch_one(&mut *tx)
        .await?;
    let reply =
        node_write_reply(&mut tx, state, key, input.run, owned, Some(&input.cached)).await?;
    tx.commit().await?;
    Ok(reply)
}

pub(in super::super) async fn node_write_reply(
    db: &mut PgConnection,
    state: &crate::cluster::NodeState,
    key: &crate::content::codec::Key,
    run: crate::model::session::Run,
    owned: queries::NodeWrite,
    cached: Option<&[crate::model::node_read::Version]>,
) -> Result<crate::model::node_write::Reply> {
    use crate::model::{Error, node_read as read, node_write as write};
    if owned.state == "aborted"
        && owned.publish_hash.is_some()
        && let Some((reason,)) =
            super::super::queries::replica::node_failure(state.node_id, owned.operation_id)
                .fetch_optional(&mut *db)
                .await?
    {
        return Err(crate::model::publish::reject_replica(reason.as_deref()));
    }
    let phase = if owned.state == "published" {
        write::Phase::Published
    } else if owned.state == "aborted" {
        write::Phase::Aborted
    } else if owned.state == "active" && owned.publish_hash.is_some() {
        write::Phase::Preparing
    } else if owned.valid_for_ms > 0
        && owned.credential_generation == state.credential_generation
        && owned.boot_id == run.boot
        && owned.fence == run.fence
    {
        write::Phase::Active
    } else {
        write::Phase::Expired
    };
    let completion = if let Some(upload) = owned.completion_upload {
        queries::node_completion_summary(upload, state.node_id)
            .fetch_optional(&mut *db)
            .await?
            .map(|(mut summary, result)| {
                summary.result = result;
                summary.0
            })
    } else {
        None
    };
    let mut reply = write::Reply {
        operation: owned.operation_id,
        bucket: owned
            .bucket_id
            .or_else(|| owned.result.as_ref()?.identity.map(|(bucket, _)| bucket)),
        stream: owned
            .stream_id
            .map(Into::into)
            .or_else(|| owned.result.as_ref()?.identity.map(|(_, stream)| stream)),
        local_file: owned
            .upload_id
            .filter(|_| !owned.profile.cdc_enabled)
            .and(owned.stream_id),
        phase,
        copy_references: owned
            .copy_source
            .as_ref()
            .is_some_and(|source| source.references),
        checksum_algorithm: owned.checksum_algorithm,
        completion,
        materials: Vec::new(),
        storage: None,
        grants: Vec::new(),
        published: owned.result.map(|value| value.0),
    };
    if phase == write::Phase::Active
        && let Some(cached) = cached
    {
        let storage_id = owned.storage_id.ok_or(Error::SourceUnavailable)?;
        let record = queries::node_read_storages(&[storage_id])
            .fetch_optional(&mut *db)
            .await?
            .ok_or(Error::SourceUnavailable)?;
        let storage = read::Storage {
            connection: crate::storage::model::Connection::unprotect(&record, key)?,
            revision: owned.profile.revision,
            profile: owned.profile.0,
        };
        reply.storage = Some(read::material(
            &mut reply.materials,
            state.deployment_id,
            read::MaterialType::Storage,
            storage_id.to_string(),
            &storage,
            cached,
        )?);
        reply.grants.push(read::Grant {
            r#type: read::GrantType::Write,
            id: owned.lease_id.ok_or(Error::NodeExpired)?,
            valid_for_ms: owned.valid_for_ms as u64,
            storage: None,
        });
    }
    Ok(reply)
}

async fn abort_node_writes(
    db: &mut PgConnection,
    state: &crate::cluster::NodeState,
    run: crate::model::session::Run,
    operations: &[Uuid],
    leases: &[Uuid],
) -> Result<()> {
    if operations.is_empty() && leases.is_empty() {
        return Ok(());
    }
    let rows = queries::abort_node_writes(state, run, operations, leases)
        .fetch_all(&mut *db)
        .await?;
    let streams: Vec<_> = rows.iter().filter_map(|row| row.0).collect();
    let leases: Vec<_> = rows.iter().filter_map(|row| row.1).collect();
    if !streams.is_empty() {
        queries::release_node_completions(&streams)
            .execute(&mut *db)
            .await?;
        queries::node_write_release_permits(state, run, &streams, &[])
            .execute(&mut *db)
            .await?;
        queries::abandon_node_streams(&streams)
            .execute(&mut *db)
            .await?;
    }
    if !leases.is_empty() {
        queries::release_node_write_leases(&leases)
            .execute(db)
            .await?;
    }
    Ok(())
}

pub(in super::super) async fn node_write_status(
    backend: &super::super::Backend,
    state: &crate::cluster::NodeState,
    key: &crate::content::codec::Key,
    input: crate::model::node_write::Lookup,
    abort: bool,
) -> Result<crate::model::node_write::Reply> {
    use crate::model::Error;
    let mut tx = backend.pool.begin().await?;
    backend.fence_control(&mut tx).await?;
    authenticate_rpc_node(&mut tx, state, key, false).await?;
    ensure!(
        matches!(state.role.as_str(), "data" | "worker" | "node"),
        Error::NodeAuthentication
    );
    require_node_run(&mut tx, state, input.run, true).await?;
    let mut owned = queries::node_write(state.node_id, input.operation)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(Error::NodeOperationMissing)?;
    if abort {
        ensure!(
            owned.credential_generation == state.credential_generation
                && owned.boot_id == input.run.boot
                && owned.fence == input.run.fence,
            Error::NodeExpired
        );
        abort_node_writes(&mut tx, state, input.run, &[input.operation], &[]).await?;
        owned = queries::node_write(state.node_id, input.operation)
            .fetch_one(&mut *tx)
            .await?;
    }
    let materials = (!abort && owned.owner_task_id.is_some()).then_some([].as_slice());
    let reply = node_write_reply(&mut tx, state, key, input.run, owned, materials).await?;
    tx.commit().await?;
    Ok(reply)
}

pub(super) async fn maintain_node_reads(
    db: &mut PgConnection,
    state: &crate::cluster::NodeState,
    run: crate::model::session::Run,
    resources: crate::model::node_read::KeepAlive,
) -> Result<Vec<crate::model::node_read::GrantResult>> {
    use crate::model::node_read::{GrantResult, GrantType};
    use std::collections::{HashMap, HashSet};
    ensure!(
        resources.renew.len() + resources.release.len() <= 256,
        crate::model::Error::NodeReadLimit
    );
    let mut seen = HashSet::new();
    ensure!(
        resources
            .renew
            .iter()
            .chain(&resources.release)
            .all(|grant| !grant.id.is_nil() && seen.insert(grant.id)),
        crate::model::Error::OperationAborted
    );
    if seen.is_empty() {
        return Ok(Vec::new());
    }
    let ids = |r#type| {
        resources
            .release
            .iter()
            .filter(|grant| grant.r#type == r#type)
            .map(|grant| grant.id)
            .collect::<Vec<_>>()
    };
    let reads = ids(GrantType::Read);
    let permits = ids(GrantType::StorageRead);
    if !reads.is_empty() || !permits.is_empty() {
        queries::node_read_release_permits(state, run, &reads, &permits)
            .execute(&mut *db)
            .await?;
    }
    let windows = ids(GrantType::Window);
    if !windows.is_empty() {
        queries::node_read_release_windows(state, run, &windows)
            .execute(&mut *db)
            .await?;
    }
    if !reads.is_empty() {
        queries::node_read_release(state, run, &reads)
            .execute(&mut *db)
            .await?;
    }
    abort_node_writes(db, state, run, &[], &ids(GrantType::Write)).await?;
    let writes = ids(GrantType::StorageWrite);
    if !writes.is_empty() {
        queries::node_write_release_permits(state, run, &[], &writes)
            .execute(&mut *db)
            .await?;
    }
    if !permits.is_empty() {
        queries::native::release_reads(state, run, &permits)
            .execute(&mut *db)
            .await?;
    }
    let native = ids(GrantType::Native);
    if !native.is_empty() {
        queries::native::renew(state, run, &native, true)
            .fetch_all(&mut *db)
            .await?;
    }
    let work = ids(GrantType::Work);
    if !work.is_empty() {
        queries::work::renew(state, run, &work, true)
            .fetch_all(&mut *db)
            .await?;
    }
    let mut renewed = HashMap::new();
    for r#type in [
        GrantType::Read,
        GrantType::StorageRead,
        GrantType::Window,
        GrantType::Write,
        GrantType::StorageWrite,
        GrantType::Native,
        GrantType::Work,
    ] {
        let ids = resources
            .renew
            .iter()
            .filter(|grant| grant.r#type == r#type)
            .map(|grant| grant.id)
            .collect::<Vec<_>>();
        if !ids.is_empty() {
            renewed.extend(
                queries::node_read_renew(state, run, r#type, &ids)
                    .fetch_all(&mut *db)
                    .await?,
            );
            if r#type == GrantType::Work {
                let rejected: Vec<_> = ids
                    .iter()
                    .copied()
                    .filter(|id| !renewed.contains_key(id))
                    .collect();
                if !rejected.is_empty() {
                    queries::work::renew(state, run, &rejected, true)
                        .fetch_all(&mut *db)
                        .await?;
                }
            }
            if r#type == GrantType::Write {
                let accepted: Vec<_> = ids
                    .into_iter()
                    .filter(|id| renewed.contains_key(id))
                    .collect();
                if !accepted.is_empty() {
                    queries::renew_node_completions(state, run, &accepted)
                        .execute(&mut *db)
                        .await?;
                }
            }
        }
    }
    Ok(resources
        .renew
        .into_iter()
        .map(|grant| GrantResult {
            accepted: renewed.contains_key(&grant.id),
            valid_for_ms: renewed.get(&grant.id).copied().unwrap_or(0) as u64,
            grant,
        })
        .chain(resources.release.into_iter().map(|grant| GrantResult {
            grant,
            accepted: true,
            valid_for_ms: 0,
        }))
        .collect())
}

pub(super) async fn require_node_write(
    db: &mut Transaction<'_, Postgres>,
    row: &queries::NodeWrite,
) -> Result<Id> {
    ensure!(
        row.state == "active" && row.publish_hash.is_none() && row.valid_for_ms > 0,
        Error::NodeExpired
    );
    ensure!(
        queries::node_read_admission().fetch_one(&mut **db).await?,
        Error::NodeInactive
    );
    let stream = row.stream_id.ok_or(Error::NodeExpired)?.into();
    if let Some(id) = row.owner_task_id {
        lock_execution(
            db,
            Execution {
                id,
                token: row.owner_lease_token.ok_or(Error::NodeExpired)?,
            },
        )
        .await?;
    }
    let authority: sqlx::types::Json<Permit> = queries::write_authorization(stream)
        .fetch_one(&mut **db)
        .await?;
    authority.lock(db).await?;
    if let Some(source) = &row.copy_source {
        queries::node_copy_source(source)
            .fetch_optional(&mut **db)
            .await?
            .ok_or(Error::NodeExpired)?;
    }
    Ok(stream)
}

async fn node_chunk_permits(
    db: &mut PgConnection,
    node: Uuid,
    row: &queries::NodeWrite,
    input: &chunk::Prepare,
    plan: &[chunk::Item],
) -> Result<Vec<read::Grant>> {
    let unique: HashSet<_> = plan
        .iter()
        .filter(|item| !item.reused)
        .map(|item| item.chunk.uuid)
        .collect();
    let wanted = unique.len().min(input.parallelism as usize);
    node_write_permits(
        db,
        node,
        row,
        Some(input.batch),
        wanted,
        row.storage_id.ok_or(Error::SourceUnavailable)?,
        row.profile.upload_concurrency,
    )
    .await
}

pub(super) async fn node_write_permits(
    db: &mut PgConnection,
    node: Uuid,
    row: &queries::NodeWrite,
    batch: Option<i64>,
    wanted: usize,
    storage: Uuid,
    admitted: Option<i32>,
) -> Result<Vec<read::Grant>> {
    if wanted == 0 {
        return Ok(Vec::new());
    }
    let current = queries::node_write_limit(storage)
        .fetch_one(&mut *db)
        .await?;
    let limit = match (admitted, current) {
        (Some(admitted), Some(current)) => Some(admitted.min(current)),
        (admitted, current) => admitted.or(current),
    };
    let Some(limit) = limit else {
        return Ok(Vec::new());
    };
    queries::lock_storage_permits(format!("mokyu:store:{storage}:upload"))
        .execute(&mut *db)
        .await?;
    queries::expire_storage_permits(storage, "upload")
        .execute(&mut *db)
        .await?;
    let mut held = queries::node_write_permits(node, row.operation_id, batch)
        .fetch_all(&mut *db)
        .await?;
    let mut active = queries::storage_permit_count(storage, "upload")
        .fetch_one(&mut *db)
        .await?;
    while held.len() < wanted && active < i64::from(limit) {
        let token = Uuid::new_v4();
        queries::insert_storage_permit(token, storage, "upload", node)
            .execute(&mut *db)
            .await?;
        queries::node_write_own_permit(node, row.operation_id, batch, token)
            .execute(&mut *db)
            .await?;
        held.push((token, 60_000));
        active += 1;
    }
    ensure!(!held.is_empty(), Error::NodeWriteBusy);
    Ok(held
        .into_iter()
        .map(|(id, remaining)| read::Grant {
            r#type: read::GrantType::StorageWrite,
            id,
            valid_for_ms: remaining as u64,
            storage: Some(storage),
        })
        .collect())
}

pub(in super::super) async fn prepare_node_chunks(
    backend: &super::super::Backend,
    node: &NodeState,
    key: &Key,
    input: chunk::Prepare,
) -> Result<chunk::Assembly> {
    let end = input.end()?;
    let hash = input.hash()?;
    let mut tx = backend.pool.begin().await?;
    backend.fence_control(&mut tx).await?;
    let row = node_write_owned(&mut tx, node, key, input.run, input.operation).await?;
    if let Some(batch) = queries::node_write_batch(node.node_id, input.operation, input.batch)
        .fetch_optional(&mut *tx)
        .await?
    {
        ensure!(batch.request_hash == hash, Error::NodeOperationConflict);
        let committed = batch.commit_hash.is_some();
        if !committed {
            require_node_write(&mut tx, &row).await?;
            node_chunk_permits(&mut tx, node.node_id, &row, &input, &batch.plan).await?;
        }
        tx.commit().await?;
        return Ok(chunk::Assembly {
            items: batch.plan.0,
            ready: batch.ready,
            committed,
        });
    }
    let stream = require_node_write(&mut tx, &row).await?;
    ensure!(
        row.profile.cdc_enabled
            && queries::node_write_tail(node.node_id, input.operation)
                .fetch_optional(&mut *tx)
                .await?
                .is_none(),
        s3s::s3_error!(InvalidRequest, "write does not accept chunks")
    );
    ensure!(
        row.upload_id.is_none() || end <= write::MAX_PART_BYTES,
        s3s::s3_error!(EntityTooLarge)
    );
    ensure!(
        row.next_batch == input.batch && row.next_offset == input.offset && input.batch < i64::MAX,
        s3s::s3_error!(InvalidArgument, "chunk batch is out of order")
    );
    ensure!(
        row.expected_size.is_none_or(|size| end <= size),
        s3s::s3_error!(InvalidArgument, "write exceeds its declared length")
    );
    ensure!(
        queries::node_write_pending(node.node_id, input.operation)
            .fetch_one(&mut *tx)
            .await?
            < chunk::MAX_PENDING,
        Error::NodeWriteBusy
    );
    let storage = row.storage_id.ok_or(Error::SourceUnavailable)?;
    // Lock quota before key material, matching write admission.
    if row.completion_upload.is_none() && row.owner_task_id.is_none() {
        queries::reserve_quota(stream, end, false, row.upload_id, row.part_number)
            .fetch_one(&mut *tx)
            .await?;
    }
    let algorithm = if let Some(id) = row.block_key_id {
        queries::node_read_keys(&[id])
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(Error::SourceUnavailable)?
            .algorithm
    } else {
        "none".into()
    };
    let unique: BTreeMap<_, _> = input
        .items
        .iter()
        .map(|item| ((item.hash, item.raw_size), item))
        .collect();
    let mut allocated = HashMap::new();
    for (identity, item) in unique {
        let (mut value, reused) = allocate_chunk_in(
            &mut tx,
            crate::model::ChunkCreate {
                storage,
                stream,
                hash: item.hash,
                raw_size: item.raw_size,
                algorithm: algorithm.clone(),
                key: row.block_key_id,
            },
            false,
        )
        .await?;
        if !reused {
            value.compressed = item.compressed;
            value.stored_size = Some(item.payload_size + if algorithm == "none" { 0 } else { 16 });
        }
        allocated.insert(identity, (value, reused));
    }
    let mut offset = input.offset;
    let mut plan = Vec::with_capacity(input.items.len());
    for item in &input.items {
        let (value, reused) = &allocated[&(item.hash, item.raw_size)];
        if *reused {
            reference_chunk_in(&mut tx, stream, offset, value.id, value.raw_size, false).await?;
        }
        plan.push(chunk::Item {
            offset,
            chunk: value.clone(),
            reused: *reused,
            payload_hash: item.payload_hash,
        });
        offset += i64::from(item.raw_size);
    }
    queries::insert_node_write_batch(node.node_id, &input, &hash, end, &plan)
        .execute(&mut *tx)
        .await?;
    let packing = work::prepare_chunk_packs(&mut tx, &row, &plan).await?;
    let ready = packing.is_some();
    if let Some(packing) = packing {
        sqlx::query("UPDATE node_write_batches SET packing=$4,ready=true WHERE node_id=$1 AND operation_id=$2 AND batch_id=$3")
            .bind(node.node_id).bind(input.operation).bind(input.batch).bind(sqlx::types::Json(packing)).execute(&mut *tx).await?;
    }
    node_chunk_permits(&mut tx, node.node_id, &row, &input, &plan).await?;
    queries::advance_node_write(node.node_id, input.operation, end)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(chunk::Assembly {
        items: plan,
        ready,
        committed: false,
    })
}

pub(in super::super) async fn activate_node_chunks(
    backend: &super::super::Backend,
    node: &NodeState,
    key: &Key,
    input: chunk::Prepare,
    nonces: Vec<chunk::Nonce>,
) -> Result<chunk::Prepared> {
    let hash = input.hash()?;
    let mut tx = backend.pool.begin().await?;
    backend.fence_control(&mut tx).await?;
    let row = node_write_owned(&mut tx, node, key, input.run, input.operation).await?;
    let mut batch = queries::node_write_batch(node.node_id, input.operation, input.batch)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(Error::NodeOperationMissing)?;
    ensure!(batch.request_hash == hash, Error::NodeOperationConflict);
    if batch.commit_hash.is_some() {
        tx.commit().await?;
        return Ok(chunk::Prepared {
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
    let stream = require_node_write(&mut tx, &row).await?;
    let mut grants = node_chunk_permits(&mut tx, node.node_id, &row, &input, &batch.plan).await?;
    if !batch.ready {
        let count = nonces.len();
        let nonces: HashMap<_, _> = nonces
            .into_iter()
            .map(|value| (value.chunk, value.value))
            .collect();
        let needed: HashSet<_> = batch
            .plan
            .iter()
            .filter(|item| !item.reused && item.chunk.algorithm != "none")
            .map(|item| item.chunk.uuid)
            .collect();
        ensure!(
            count == needed.len()
                && nonces.len() == needed.len()
                && needed.iter().all(|id| nonces.contains_key(id)),
            Error::OperationAborted
        );
        let mut registered = HashSet::new();
        for item in batch.plan.iter_mut().filter(|item| !item.reused) {
            if let Some(value) = nonces.get(&item.chunk.uuid) {
                let id = crate::content::nonce::id(Some(value)).ok_or(Error::OperationAborted)?;
                ensure!(
                    value.as_slice()
                        == codec::typed_nonce(
                            item.chunk.created_at,
                            id,
                            crate::content::nonce::CHUNK
                        )?,
                    Error::OperationAborted
                );
                item.chunk.nonce = Some(value.clone());
            }
            if registered.insert(item.chunk.uuid) {
                start_chunk_upload_in(&mut tx, stream, &item.chunk, false).await?;
            }
            item.chunk.state = "uploading".into();
        }
        ensure!(
            queries::activate_node_write_batch(
                node.node_id,
                input.operation,
                input.batch,
                &batch.plan
            )
            .execute(&mut *tx)
            .await?
            .rows_affected()
                == 1,
            Error::OperationAborted
        );
    }
    let material =
        node_write_reply(&mut tx, node, key, input.run, row, Some(&input.cached)).await?;
    let mut materials = material.materials;
    grants.extend(material.grants);
    let keys: Vec<_> = batch
        .plan
        .iter()
        .filter(|item| !item.reused)
        .filter_map(|item| item.chunk.block_key_id)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let records = queries::node_read_keys(&keys).fetch_all(&mut *tx).await?;
    ensure!(records.len() == keys.len(), Error::SourceUnavailable);
    for record in records {
        let block = read::BlockKey::unprotect(record, key)?;
        read::material(
            &mut materials,
            node.deployment_id,
            read::MaterialType::Key,
            block.id.to_string(),
            &block,
            &input.cached,
        )?;
    }
    tx.commit().await?;
    Ok(chunk::Prepared {
        packing: batch.packing.map(|packing| packing.0),
        batch: input.batch,
        end: batch.end_bytes,
        committed: false,
        items: batch.plan.0,
        materials,
        storage: material.storage,
        grants,
    })
}

pub(in super::super) async fn commit_node_chunks(
    backend: &super::super::Backend,
    node: &NodeState,
    key: &Key,
    mut input: chunk::Commit,
) -> Result<i64> {
    let hash = input.hash()?;
    let mut tx = backend.pool.begin().await?;
    backend.fence_control(&mut tx).await?;
    let row = node_write_owned(&mut tx, node, key, input.run, input.operation).await?;
    let batch = queries::node_write_batch(node.node_id, input.operation, input.batch)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(Error::NodeOperationMissing)?;
    if let Some(previous) = batch.commit_hash {
        ensure!(previous == hash, Error::NodeOperationConflict);
        tx.commit().await?;
        return Ok(batch.end_bytes);
    }
    let stream = require_node_write(&mut tx, &row).await?;
    ensure!(batch.ready, Error::OperationAborted);
    let (size, _) = queries::node_write_size(stream).fetch_one(&mut *tx).await?;
    if let Some(packing) = &batch.packing {
        ensure!(input.encoded.is_empty(), Error::NodeOperationConflict);
        work::finish_chunk_packs(
            &mut tx,
            &row,
            &batch.plan,
            packing,
            input.packed.as_ref().ok_or(Error::NodeOperationConflict)?,
            key,
        )
        .await?;
    } else {
        ensure!(input.packed.is_none(), Error::NodeOperationConflict);
        let expected: HashSet<_> = batch
            .plan
            .iter()
            .filter(|item| !item.reused)
            .map(|item| item.chunk.uuid)
            .collect();
        ensure!(
            expected.len() == input.encoded.len()
                && input
                    .encoded
                    .iter()
                    .all(|value| expected.contains(&value.chunk)),
            Error::OperationAborted
        );
        let mut completed = HashSet::new();
        for item in batch.plan.iter().filter(|item| !item.reused) {
            if completed.insert(item.chunk.uuid) {
                complete_chunk_write_in(&mut tx, stream, item.offset, &item.chunk, false).await?;
            } else {
                reference_chunk_in(
                    &mut tx,
                    stream,
                    item.offset,
                    item.chunk.id,
                    item.chunk.raw_size,
                    false,
                )
                .await?;
            }
        }
    }
    require_sources(&mut tx, Some(stream), &[]).await?;
    queries::extend_writing_stream(stream, size.max(batch.end_bytes))
        .execute(&mut *tx)
        .await?;
    ensure!(
        queries::commit_node_write_batch(node.node_id, input.operation, input.batch, &hash)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        Error::OperationAborted
    );
    queries::release_write_upload_permits(node.node_id, input.operation, Some(input.batch))
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(batch.end_bytes)
}

pub(in super::super) async fn copy_node_references(
    backend: &super::super::Backend,
    node: &NodeState,
    key: &Key,
    input: write::CopyBatch,
) -> Result<i64> {
    let hash = input.hash()?;
    let mut tx = backend.pool.begin().await?;
    backend.fence_control(&mut tx).await?;
    let row = node_write_owned(&mut tx, node, key, input.run, input.operation).await?;
    if let Some(batch) = queries::node_write_batch(node.node_id, input.operation, input.batch)
        .fetch_optional(&mut *tx)
        .await?
    {
        ensure!(
            batch.request_hash == hash && batch.commit_hash.as_ref() == Some(&hash),
            Error::NodeOperationConflict
        );
        tx.commit().await?;
        return Ok(batch.end_bytes);
    }
    let stream = require_node_write(&mut tx, &row).await?;
    let source = row
        .copy_source
        .as_ref()
        .ok_or_else(|| s3s::s3_error!(InvalidRequest))?;
    ensure!(
        source.references,
        s3s::s3_error!(InvalidRequest, "write cannot copy references")
    );
    ensure!(
        row.next_batch == input.batch
            && row.next_offset == input.offset
            && input.offset < source.size,
        s3s::s3_error!(InvalidArgument, "copy batch is out of order")
    );
    let end = if source.payload == "raw" {
        let mut rows = queries::raw_page(source.stream, input.offset, source.size)
            .fetch_all(&mut *tx)
            .await?;
        rows.retain(|row| row.offset_bytes >= input.offset);
        let (offset, end, _) = crate::model::raw::copy_window(source.storage, &rows)?;
        ensure!(
            offset == input.offset && end > offset && end <= source.size,
            Error::SourceUnavailable
        );
        copy_raw_references_in(
            &mut tx,
            stream,
            source.stream,
            source.storage,
            row.profile.revision,
            &rows,
        )
        .await?;
        end
    } else {
        let extents = queries::extents_for_copy(source.stream, input.offset)
            .fetch_all(&mut *tx)
            .await?;
        ensure!(!extents.is_empty(), Error::SourceUnavailable);
        let mut end = input.offset;
        let mut chunks = Vec::with_capacity(extents.len());
        for extent in extents {
            ensure!(
                extent.offset_bytes == end && extent.fragment_id.is_none() && extent.length > 0,
                Error::SourceUnavailable
            );
            end = end
                .checked_add(i64::from(extent.length))
                .ok_or(Error::SourceUnavailable)?;
            chunks.push(extent.chunk_id.ok_or(Error::SourceUnavailable)?);
        }
        ensure!(end <= source.size, Error::SourceUnavailable);
        chunks.sort_unstable();
        chunks.dedup();
        copy_chunk_references_in(&mut tx, stream, source.stream, input.offset, end, &chunks)
            .await?;
        end
    };
    queries::save_node_copy_batch(node.node_id, &input, &hash, end)
        .execute(&mut *tx)
        .await?;
    queries::advance_node_write(node.node_id, input.operation, end)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(end)
}

pub(in super::super) async fn publish_node_write(
    backend: &super::super::Backend,
    node: &NodeState,
    key: &Key,
    mut input: write::Publish,
) -> Result<write::Reply> {
    let hash = input.hash()?;
    let mut tx: Transaction<'_, Postgres> = backend.pool.begin().await?;
    backend.fence_control(&mut tx).await?;
    let row = node_write_owned(&mut tx, node, key, input.run, input.operation).await?;
    if let Some(previous) = &row.publish_hash {
        ensure!(previous == &hash, Error::NodeOperationConflict);
        let reply = node_write_reply(&mut tx, node, key, input.run, row, None).await?;
        tx.commit().await?;
        return Ok(reply);
    }
    let stream = require_node_write(&mut tx, &row).await?;
    if let Some(source) = &row.copy_source {
        ensure!(
            input.size == source.size
                && input.etag == source.etag
                && serde_json::to_value(&input.checksums)? == source.checksums,
            s3s::s3_error!(InvalidRequest, "copy result differs from its source")
        );
    }
    if let Some(raw) = queries::node_raw_write(node.node_id, input.operation)
        .fetch_optional(&mut *tx)
        .await?
    {
        ensure!(
            raw.committed
                && raw.size == input.size
                && (row.copy_source.is_some()
                    || row.completion_upload.is_some()
                    || row.owner_task_id.is_some() && input.etag.contains('-')
                    || hex::encode(&raw.digest) == input.etag),
            s3s::s3_error!(IncompleteBody)
        );
    }
    ensure!(
        queries::node_write_tail(node.node_id, input.operation)
            .fetch_optional(&mut *tx)
            .await?
            .is_none_or(|tail| tail.committed)
            && input.size == row.next_offset
            && row.expected_size.is_none_or(|size| size == input.size)
            && queries::node_write_pending(node.node_id, input.operation)
                .fetch_one(&mut *tx)
                .await?
                == 0,
        s3s::s3_error!(IncompleteBody)
    );
    if row.upload_id.is_some() && !row.profile.cdc_enabled {
        ensure!(
            queries::native::local_part(node.node_id, input.operation)
                .fetch_optional(&mut *tx)
                .await?
                .is_some_and(|(_, size)| size == input.size),
            s3s::s3_error!(IncompleteBody)
        );
    }
    let mut sums = serde_json::to_value(&input.checksums)?;
    let completion = if let Some(upload) = row.completion_upload {
        let record = queries::node_completion(upload).fetch_one(&mut *tx).await?;
        ensure!(
            input.size == record.summary.size
                && input.etag == record.summary.etag
                && input.if_match.is_none()
                && input.if_none.is_none(),
            s3s::s3_error!(
                InvalidRequest,
                "completion result differs from its manifest"
            )
        );
        let parts = queries::node_completion_checksums(upload)
            .fetch_all(&mut *tx)
            .await?;
        sums = crate::model::node_complete::checksums(
            sums,
            record.summary.algorithm.as_deref(),
            record.summary.checksum_type.as_deref(),
            &parts,
            &record.conditions.checksums,
        )?;
        input.if_match = record.conditions.if_match.clone();
        input.if_none = record.conditions.if_none.clone();
        Some((
            upload,
            record.manifest_hash,
            json!({"etag": input.etag, "checksums": sums}),
            input.operation,
        ))
    } else {
        None
    };
    ensure!(
        queries::finish_stream(stream, input.size, &input.etag, sums)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        Error::OperationAborted
    );
    if row.owner_task_id.is_some() {
        work::seal_task_write(&mut tx, &row, &input).await?;
    } else if let (Some(upload), Some(number)) = (row.upload_id, row.part_number) {
        ensure!(
            input.etag.len() == 32 && input.if_match.is_none() && input.if_none.is_none(),
            s3s::s3_error!(InvalidRequest)
        );
        if let Some(algorithm) = &row.checksum_algorithm {
            let name = algorithm.to_ascii_lowercase();
            ensure!(
                input.checksums.contains_key(&name),
                s3s::s3_error!(InvalidRequest, "part checksum missing")
            );
        }
        let authority: sqlx::types::Json<Permit> = queries::write_authorization(stream)
            .fetch_one(&mut *tx)
            .await?;
        commit_upload_part_in(
            &mut tx,
            &authority,
            row.bucket_id.ok_or(Error::NodeExpired)?,
            upload,
            number,
            stream,
            None,
        )
        .await?;
        if !row.profile.cdc_enabled {
            ensure!(
                queries::native::queue_part(
                    upload,
                    number,
                    stream,
                    node.node_id,
                    row.profile.upload_cache
                )
                .execute(&mut *tx)
                .await?
                .rows_affected()
                    == 1,
                Error::OperationAborted
            );
        }
    } else {
        let condition = |value: Option<String>| {
            value
                .map(|value| s3s::dto::ETagCondition::parse_http_header(value.as_bytes()))
                .transpose()
        };
        match publish_in(
            &mut tx,
            stream,
            condition(input.if_match)?,
            condition(input.if_none)?,
            completion,
        )
        .await
        {
            Ok(true) => {}
            Ok(false) => return Err(Error::OperationAborted.into()),
            Err(error) if error.is::<crate::model::ReplicaPending>() => {
                let publication = error
                    .downcast_ref::<crate::model::ReplicaPending>()
                    .unwrap()
                    .0;
                ensure!(
                    super::super::queries::replica::node(
                        publication,
                        node.node_id,
                        input.operation
                    )
                    .execute(&mut *tx)
                    .await?
                    .rows_affected()
                        == 1,
                    Error::OperationAborted
                );
                ensure!(
                    queries::stage_node_write(node.node_id, input.operation, &hash)
                        .execute(&mut *tx)
                        .await?
                        .rows_affected()
                        == 1,
                    Error::OperationAborted
                );
                let row = queries::node_write(node.node_id, input.operation)
                    .fetch_one(&mut *tx)
                    .await?;
                let reply = node_write_reply(&mut tx, node, key, input.run, row, None).await?;
                tx.commit().await?;
                return Ok(reply);
            }
            Err(error) => {
                if let Some(reason) = crate::model::replica_rejection(&error) {
                    tx.rollback().await?;
                    reject_admission(&backend.pool, stream, reason).await?;
                }
                return Err(error);
            }
        }
    }
    let (_, modified) = queries::node_write_size(stream).fetch_one(&mut *tx).await?;
    queries::seal_stream_uploads(stream)
        .execute(&mut *tx)
        .await?;
    let result = write::Published {
        identity: row.bucket_id.map(|bucket| (bucket, stream)),
        size: input.size,
        etag: input.etag,
        modified,
    };
    ensure!(
        queries::publish_node_write(node.node_id, input.operation, &hash, &result)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        Error::OperationAborted
    );
    if let Some(lease) = row.lease_id {
        queries::release_node_write_leases(&[lease])
            .execute(&mut *tx)
            .await?;
    }
    let row = queries::node_write(node.node_id, input.operation)
        .fetch_one(&mut *tx)
        .await?;
    let reply = node_write_reply(&mut tx, node, key, input.run, row, None).await?;
    tx.commit().await?;
    Ok(reply)
}

pub(in super::super) async fn prepare_node_tail(
    backend: &super::super::Backend,
    node: &NodeState,
    key: &Key,
    input: crate::model::node_tail::Prepare,
) -> Result<crate::model::node_tail::Prepared> {
    let end = input.end()?;
    let hash = input.hash()?;
    let mut tx = backend.pool.begin().await?;
    backend.fence_control(&mut tx).await?;
    let row = node_write_owned(&mut tx, node, key, input.run, input.operation).await?;
    let tail = if let Some(tail) = queries::node_write_tail(node.node_id, input.operation)
        .fetch_optional(&mut *tx)
        .await?
    {
        ensure!(tail.request_hash == hash, Error::NodeOperationConflict);
        tail
    } else {
        let stream = require_node_write(&mut tx, &row).await?;
        ensure!(
            (row.upload_id.is_some()
                || row.owner_task_id.is_some()
                    && sqlx::query_scalar::<_, bool>(
                        "SELECT stream_type='part' FROM streams WHERE id=$1"
                    )
                    .bind(stream)
                    .fetch_one(&mut *tx)
                    .await?)
                && row.profile.cdc_enabled
                && row.copy_source.is_none(),
            s3s::s3_error!(InvalidRequest, "write does not accept a multipart tail")
        );
        ensure!(
            row.next_offset == input.offset
                && row.expected_size.is_none_or(|size| size == end)
                && queries::node_write_pending(node.node_id, input.operation)
                    .fetch_one(&mut *tx)
                    .await?
                    == 0,
            s3s::s3_error!(InvalidArgument, "tail is out of order")
        );
        if row.owner_task_id.is_none() {
            queries::reserve_quota(stream, end, false, row.upload_id, row.part_number)
                .fetch_one(&mut *tx)
                .await?;
        }
        let settings = queries::node_staging_settings().fetch_one(&mut *tx).await?;
        let storage = settings
            .staging_storage_id
            .ok_or(Error::SourceUnavailable)?;
        ensure!(
            queries::cluster_staging_valid(Some(storage))
                .fetch_one(&mut *tx)
                .await?,
            Error::SourceUnavailable
        );
        let limit = settings
            .staging_bytes_limit
            .as_deref()
            .map(crate::config::bytes)
            .transpose()?
            .unwrap_or(crate::storage::DEFAULT_STAGING_LIMIT);
        let fragment = Uuid::now_v7();
        let path = format!(
            "{}fragment/{}",
            crate::storage::STAGING_PREFIX,
            fragment.simple()
        );
        claim_staging_in(&mut tx, storage, &path, input.size as u64, limit).await?;
        queries::insert_fragment(&StreamTail {
            fragment,
            stream,
            offset: input.offset,
            size: input.size,
            hash: input.hash,
            staging_storage: Some(storage),
            staging_key: Some(path.clone()),
            file: None,
            file_offset: None,
        })
        .execute(&mut *tx)
        .await?;
        queries::insert_node_tail(node.node_id, &input, &hash, fragment, storage, &path)
            .execute(&mut *tx)
            .await?;
        queries::node_write_tail(node.node_id, input.operation)
            .fetch_one(&mut *tx)
            .await?
    };
    let mut reply = tail.reply();
    if !tail.committed {
        require_node_write(&mut tx, &row).await?;
        let record = queries::node_read_storages(&[tail.storage_id])
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(Error::SourceUnavailable)?;
        let profile = queries::storage_profile(tail.storage_id)
            .fetch_one(&mut *tx)
            .await?;
        reply.grants = node_write_permits(
            &mut tx,
            node.node_id,
            &row,
            None,
            1,
            tail.storage_id,
            profile.upload_concurrency,
        )
        .await?;
        let storage = read::Storage {
            connection: crate::storage::model::Connection::unprotect(&record, key)?,
            revision: profile.revision,
            profile,
        };
        reply.storage = Some(read::material(
            &mut reply.materials,
            node.deployment_id,
            read::MaterialType::Storage,
            tail.storage_id.to_string(),
            &storage,
            &input.cached,
        )?);
    }
    tx.commit().await?;
    Ok(reply)
}

pub(in super::super) async fn commit_node_tail(
    backend: &super::super::Backend,
    node: &NodeState,
    key: &Key,
    input: write::Lookup,
) -> Result<crate::model::node_tail::Prepared> {
    let mut tx = backend.pool.begin().await?;
    backend.fence_control(&mut tx).await?;
    let row = node_write_owned(&mut tx, node, key, input.run, input.operation).await?;
    let tail = queries::node_write_tail(node.node_id, input.operation)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(Error::NodeOperationMissing)?;
    if !tail.committed {
        let stream = require_node_write(&mut tx, &row).await?;
        ensure!(
            row.next_offset == tail.offset_bytes,
            Error::NodeOperationConflict
        );
        ensure!(
            queries::seal_fragment(tail.fragment_id.into())
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            Error::SourceUnavailable
        );
        queries::insert_fragment_extent(
            stream,
            tail.offset_bytes,
            tail.size,
            tail.fragment_id.into(),
        )
        .execute(&mut *tx)
        .await?;
        let end = tail.offset_bytes + i64::from(tail.size);
        queries::resize_stream(stream, end)
            .execute(&mut *tx)
            .await?;
        queries::finish_node_tail(node.node_id, input.operation, end)
            .execute(&mut *tx)
            .await?;
        queries::commit_node_tail(node.node_id, input.operation)
            .execute(&mut *tx)
            .await?;
        queries::release_write_upload_permits(node.node_id, input.operation, None)
            .execute(&mut *tx)
            .await?;
    }
    let mut reply = tail.reply();
    reply.committed = true;
    tx.commit().await?;
    Ok(reply)
}

pub(in super::super) async fn prepare_node_raw(
    backend: &super::super::Backend,
    node: &NodeState,
    key: &Key,
    input: crate::model::node_raw::Prepare,
) -> Result<crate::model::node_raw::Prepared> {
    input.input.validate()?;
    let hash = chunk::hash(&input.input)?;
    let mut tx = backend.pool.begin().await?;
    backend.fence_control(&mut tx).await?;
    let row = node_write_owned(&mut tx, node, key, input.run, input.operation).await?;
    if let Some(raw) = queries::node_raw_write(node.node_id, input.operation)
        .fetch_optional(&mut *tx)
        .await?
    {
        ensure!(raw.request_hash == hash, Error::NodeOperationConflict);
        let grants = if raw.committed || input.input.local {
            Vec::new()
        } else {
            require_node_write(&mut tx, &row).await?;
            node_write_permits(
                &mut tx,
                node.node_id,
                &row,
                None,
                1,
                row.storage_id.ok_or(Error::SourceUnavailable)?,
                row.profile.upload_concurrency,
            )
            .await?
        };
        tx.commit().await?;
        return Ok(crate::model::node_raw::Prepared {
            source: raw.source_id,
            size: raw.size,
            committed: raw.committed,
            grants,
        });
    }
    let stream = require_node_write(&mut tx, &row).await?;
    ensure!(
        (row.upload_id.is_none() || row.profile.cdc_enabled && input.input.local)
            && (!input.input.local || row.profile.upload_cache)
            && (!row.profile.upload_cache
                || input.input.local
                || row.profile.upload_cache_overflow == "fallback")
            && (((row.profile.write_processing == "background" || input.input.local)
                && row.profile.cdc_enabled)
                || (!row.profile.cdc_enabled
                    && !row.profile.compression_enabled
                    && !row.profile.pack_enabled
                    && row.block_key_id.is_none()))
            && (!row.profile.cdc_enabled
                && row.profile.deduplication_enabled
                && row.profile.write_processing == "inline")
                == input.input.hash.is_some()
            && row.next_batch == 1
            && row.next_offset == 0,
        s3s::s3_error!(InvalidRequest, "write does not accept raw data")
    );
    let size = input.input.size;
    ensure!(
        row.expected_size.is_none_or(|expected| expected == size),
        s3s::s3_error!(IncompleteBody)
    );
    let storage = row.storage_id.ok_or(Error::SourceUnavailable)?;
    if row.owner_task_id.is_none() {
        queries::reserve_quota(stream, size, false, row.upload_id, row.part_number)
            .fetch_one(&mut *tx)
            .await?;
    }
    let candidate = if let Some(hash) = &input.input.hash {
        queries::claim_chunk_hash(storage, hash)
            .execute(&mut *tx)
            .await?;
        queries::raw_duplicate(storage, hash, size)
            .fetch_optional(&mut *tx)
            .await?
            .filter(|source| {
                source.validate().is_ok()
                    && !source.pending_upload
                    && source.medium == "shared_s3"
                    && source.digest.as_deref() == Some(input.input.digest.as_slice())
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
    let committed = candidate.is_some();
    let source = if let Some(candidate) = candidate {
        queries::raw_attach(stream, candidate.id, size)
            .execute(&mut *tx)
            .await?;
        queries::raw_reference(candidate.id)
            .execute(&mut *tx)
            .await?;
        queries::extend_writing_stream(stream, size)
            .execute(&mut *tx)
            .await?;
        candidate.location_id
    } else {
        queries::raw_insert(
            stream,
            storage,
            row.profile.revision,
            if input.input.local {
                "local"
            } else {
                "shared_s3"
            },
            false,
        )
        .fetch_one(&mut *tx)
        .await?
    };
    for (index, bytes) in input
        .input
        .page_hashes
        .chunks(crate::content::raw_pages::GROUP * 32)
        .enumerate()
    {
        super::super::raw_pages::put(
            &mut tx,
            source.into(),
            &crate::content::raw_pages::Group {
                index: index as i64,
                hashes: bytes.to_vec(),
            },
        )
        .await?;
    }
    super::super::raw_pages::seal(&mut tx, source.into(), size, &input.input.digest).await?;
    queries::insert_node_raw_write(node.node_id, &input, &hash, source, committed)
        .execute(&mut *tx)
        .await?;
    queries::advance_node_write(node.node_id, input.operation, size)
        .execute(&mut *tx)
        .await?;
    let grants = node_write_permits(
        &mut tx,
        node.node_id,
        &row,
        None,
        usize::from(!committed && !input.input.local),
        row.storage_id.ok_or(Error::SourceUnavailable)?,
        row.profile.upload_concurrency,
    )
    .await?;
    tx.commit().await?;
    Ok(crate::model::node_raw::Prepared {
        source,
        size,
        committed,
        grants,
    })
}

pub(in super::super) async fn commit_node_raw(
    backend: &super::super::Backend,
    node: &NodeState,
    key: &Key,
    input: crate::model::node_raw::Commit,
) -> Result<crate::model::node_raw::Prepared> {
    let mut tx = backend.pool.begin().await?;
    backend.fence_control(&mut tx).await?;
    let row = node_write_owned(&mut tx, node, key, input.run, input.operation).await?;
    let raw = queries::node_raw_write(node.node_id, input.operation)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(Error::NodeOperationMissing)?;
    ensure!(raw.source_id == input.source, Error::NodeOperationConflict);
    if !raw.committed {
        let stream = require_node_write(&mut tx, &row).await?;
        if let Some(hash) = &raw.content_hash {
            queries::claim_chunk_hash(row.storage_id.ok_or(Error::SourceUnavailable)?, hash)
                .execute(&mut *tx)
                .await?;
        }
        let source = raw.source_id.into();
        let local = queries::raw_source(source)
            .fetch_one(&mut *tx)
            .await?
            .medium
            == "local";
        ensure!(
            queries::raw_finish(stream, source, raw.size, &raw.digest, true)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            Error::OperationAborted
        );
        queries::raw_attach(stream, source, raw.size)
            .execute(&mut *tx)
            .await?;
        if let Some(hash) = &raw.content_hash
            && !local
        {
            let current = queries::raw_source(source).fetch_one(&mut *tx).await?;
            share_raw(&mut tx, stream, &current, hash).await?;
        }
        if row.profile.write_processing == "background" || local && row.profile.cdc_enabled {
            if row.profile.cdc_enabled {
                let algorithm = if let Some(id) = row.block_key_id {
                    queries::node_read_keys(&[id])
                        .fetch_optional(&mut *tx)
                        .await?
                        .ok_or(Error::SourceUnavailable)?
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
                enqueue_conversion(&mut tx, stream, source, &encoding).await?;
            } else if row.profile.deduplication_enabled && !local {
                queries::raw_enqueue_processing(stream, source)
                    .execute(&mut *tx)
                    .await?;
            }
        }
        if local {
            ensure!(
                queries::raw_stage(source, row.profile.revision, None)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected()
                    == 1,
                Error::OperationAborted
            );
            queries::raw_queue(
                source,
                !row.profile.cdc_enabled && row.profile.deduplication_enabled,
                row.profile.write_processing == "background",
                Some(node.node_id),
            )
            .execute(&mut *tx)
            .await?;
            if raw.content_hash.is_some() {
                queries::raw_upload_hash(source, raw.content_hash.as_deref())
                    .execute(&mut *tx)
                    .await?;
            }
            raw_cache::enqueue(
                &mut tx,
                node.node_id,
                source,
                row.storage_id.ok_or(Error::SourceUnavailable)?,
            )
            .await?;
        }
        queries::extend_writing_stream(stream, raw.size)
            .execute(&mut *tx)
            .await?;
        queries::commit_node_raw_write(node.node_id, input.operation)
            .execute(&mut *tx)
            .await?;
        queries::release_write_upload_permits(node.node_id, input.operation, None)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(crate::model::node_raw::Prepared {
        source: raw.source_id,
        size: raw.size,
        committed: true,
        grants: Vec::new(),
    })
}
