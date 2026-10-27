use super::*;
use crate::model::node_native as native;
use queries::native as q;

pub(crate) async fn validate(
    db: &mut PgConnection,
    upload: Uuid,
    profile: &crate::storage::profile::StorageProfile,
    key: Option<Uuid>,
    completing: bool,
) -> Result<()> {
    let remote = queries::remote_upload(upload)
        .fetch_optional(&mut *db)
        .await?;
    if let Some(remote) = &remote {
        ensure!(
            !profile.cdc_enabled
                && !profile.compression_enabled
                && !profile.pack_enabled
                && key.is_none()
                && remote.storage == profile.storage_id,
            Error::OperationAborted
        );
    } else if !profile.cdc_enabled {
        ensure!(!completing, Error::OperationAborted);
    }
    Ok(())
}

pub(crate) async fn begin(
    db: &mut PgConnection,
    node: Uuid,
    stream: Id,
    upload: Uuid,
    profile: &crate::storage::profile::StorageProfile,
    key: Option<Uuid>,
) -> Result<()> {
    ensure!(
        !profile.compression_enabled && !profile.pack_enabled && key.is_none(),
        Error::SourceUnavailable
    );
    if queries::remote_upload(upload)
        .fetch_optional(&mut *db)
        .await?
        .is_none()
    {
        ensure!(
            queries::insert_remote(upload, profile.storage_id, profile.revision)
                .execute(&mut *db)
                .await?
                .rows_affected()
                == 1,
            Error::OperationAborted
        );
    }
    q::create_file(stream, upload, profile.storage_id, node)
        .execute(db)
        .await?;
    Ok(())
}

pub(crate) async fn prepare_local_part(
    pool: &PgPool,
    node: &NodeState,
    key: &Key,
    input: native::LocalPart,
) -> Result<write::Reply> {
    let hash = input.hash()?;
    let mut tx = pool.begin().await?;
    let row = node_write_owned(&mut tx, node, key, input.run, input.operation).await?;
    if let Some((previous, _)) = q::local_part(node.node_id, input.operation)
        .fetch_optional(&mut *tx)
        .await?
    {
        ensure!(previous == hash, Error::NodeOperationConflict);
    } else {
        let stream = require_node_write(&mut tx, &row).await?;
        ensure!(
            row.upload_id.is_some()
                && !row.profile.cdc_enabled
                && row.next_offset == 0
                && row.next_batch == 1
                && row.expected_size.is_none_or(|size| size == input.size),
            Error::NodeOperationConflict
        );
        queries::reserve_quota(stream, input.size, false, row.upload_id, row.part_number)
            .fetch_one(&mut *tx)
            .await?;
        ensure!(
            q::seal_file(stream, node.node_id)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            Error::NodeExpired
        );
        let hashes: Vec<_> = input.hashes.iter().map(|value| value.to_vec()).collect();
        q::local_fragments(stream, input.size, &hashes)
            .execute(&mut *tx)
            .await?;
        q::local_extents(stream).execute(&mut *tx).await?;
        queries::extend_writing_stream(stream, input.size)
            .execute(&mut *tx)
            .await?;
        q::record_local(node.node_id, &input, &hash)
            .execute(&mut *tx)
            .await?;
        queries::advance_node_write(node.node_id, input.operation, input.size)
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

fn owned(node: &NodeState, run: Run, row: &q::Work) -> Result<()> {
    ensure!(
        row.credential_generation == node.credential_generation
            && row.boot_id == run.boot
            && row.fence == run.fence,
        Error::NodeExpired
    );
    Ok(())
}

async fn authorize(db: &mut PgConnection, node: &NodeState, key: &Key, run: Run) -> Result<()> {
    authenticate_rpc_node(db, node, key, false).await?;
    ensure!(
        matches!(node.role.as_str(), "data" | "node"),
        Error::NodeAuthentication
    );
    require_node_run(db, node, run, true).await?;
    ensure!(
        queries::raw_upload_mode(true).fetch_one(db).await?,
        Error::NodeInactive
    );
    Ok(())
}

async fn reply(
    db: &mut PgConnection,
    node: &NodeState,
    key: &Key,
    input: &native::Claim,
    row: q::Work,
) -> Result<native::Reply> {
    owned(node, input.run, &row)?;
    ensure!(
        row.target_stream == input.stream,
        Error::NodeOperationConflict
    );
    ensure!(
        row.valid_for_ms > 0 && row.result_hash.is_none(),
        Error::NodeExpired
    );
    let record = queries::node_read_storages(&[row.storage_id])
        .fetch_one(&mut *db)
        .await?;
    let profile = queries::storage_profile(row.storage_id)
        .fetch_one(&mut *db)
        .await?;
    let storage = read::Storage {
        connection: crate::storage::model::Connection::unprotect(&record, key)?,
        revision: profile.revision,
        profile,
    };
    let mut materials = vec![];
    let storage = read::material(
        &mut materials,
        node.deployment_id,
        read::MaterialType::Storage,
        row.storage_id.to_string(),
        &storage,
        &input.cached,
    )?;
    Ok(native::Reply {
        operation: input.operation,
        work: Some(row.work.0),
        ready: false,
        materials,
        storage: Some(storage),
        grants: vec![read::Grant {
            r#type: read::GrantType::Native,
            id: row.token,
            valid_for_ms: row.valid_for_ms as u64,
            storage: Some(row.storage_id),
        }],
    })
}

pub(crate) async fn claim(
    pool: &PgPool,
    node: &NodeState,
    key: &Key,
    input: native::Claim,
) -> Result<native::Reply> {
    let issued = write::operation_time(input.operation)?;
    let mut tx = pool.begin().await?;
    authorize(&mut tx, node, key, input.run).await?;
    if let Some(row) = q::work(node.node_id, input.operation)
        .fetch_optional(&mut *tx)
        .await?
    {
        let value = reply(&mut tx, node, key, &input, row).await?;
        tx.commit().await?;
        return Ok(value);
    }
    let now = queries::node_clock().fetch_one(&mut *tx).await?;
    ensure!(
        issued <= now + chrono::Duration::seconds(30)
            && issued >= now - chrono::Duration::minutes(15),
        Error::NodeOperationExpired
    );
    if let Some(stream) = input.stream {
        let (ready, failed) = q::part_ready(node.node_id, stream)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(Error::OperationAborted)?;
        ensure!(!failed, Error::SourceUnavailable);
        if ready {
            tx.commit().await?;
            return Ok(native::Reply {
                operation: input.operation,
                work: None,
                ready: true,
                materials: vec![],
                storage: None,
                grants: vec![],
            });
        }
    }
    q::cleanup(node.node_id).execute(&mut *tx).await?;
    ensure!(
        q::work_count(node.node_id).fetch_one(&mut *tx).await? < 64,
        Error::NodeWriteLimit
    );
    let token = Uuid::new_v4();
    let mut lease = None;
    let task = if let Some(upload) = q::initialize_due(node.node_id, input.stream)
        .fetch_optional(&mut *tx)
        .await?
    {
        ensure!(
            queries::claim_remote(upload, token)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            Error::OperationAborted
        );
        Some(native::Work {
            upload: queries::remote_upload(upload).fetch_one(&mut *tx).await?,
            action: native::Action::Initialize,
        })
    } else if let Some((upload, number)) =
        queries::remote_part_due(node.node_id, input.stream.map(Into::into))
            .fetch_optional(&mut *tx)
            .await?
    {
        queries::remote_part_claim(upload, number, token)
            .execute(&mut *tx)
            .await?;
        let part = queries::remote_part(&mut tx, upload, number).await?;
        let file = q::part_file(part.stream.id, node.node_id)
            .fetch_one(&mut *tx)
            .await?;
        lease = Some(
            queries::create_read_lease(part.stream.id, 60)
                .fetch_one(&mut *tx)
                .await?,
        );
        Some(native::Work {
            upload: part.upload,
            action: native::Action::Part {
                stream: file,
                file,
                number,
                size: part.stream.size,
                etag: part.stream.etag,
            },
        })
    } else if input.stream.is_none() {
        if let Some(upload) = queries::remote_cleanup_due()
            .fetch_optional(&mut *tx)
            .await?
        {
            queries::remote_cleanup_claim(upload, token)
                .execute(&mut *tx)
                .await?;
            Some(native::Work {
                upload: queries::remote_upload(upload).fetch_one(&mut *tx).await?,
                action: native::Action::Abort,
            })
        } else {
            None
        }
    } else {
        None
    };
    let Some(task) = task else {
        tx.commit().await?;
        return Ok(native::Reply {
            operation: input.operation,
            work: None,
            ready: false,
            materials: vec![],
            storage: None,
            grants: vec![],
        });
    };
    let storage = task.upload.storage;
    queries::lock_storage_permits(format!("mokyu:store:{storage}:upload"))
        .execute(&mut *tx)
        .await?;
    queries::expire_storage_permits(storage, "upload")
        .execute(&mut *tx)
        .await?;
    if let Some(limit) = queries::node_write_limit(storage)
        .fetch_one(&mut *tx)
        .await?
    {
        ensure!(
            queries::storage_permit_count(storage, "upload")
                .fetch_one(&mut *tx)
                .await?
                < i64::from(limit),
            Error::NodeWriteBusy
        );
    }
    queries::insert_storage_permit(token, storage, "upload", node.node_id)
        .execute(&mut *tx)
        .await?;
    q::insert_work(node, &input, token, &task, lease)
        .execute(&mut *tx)
        .await?;
    let row = q::work(node.node_id, input.operation)
        .fetch_one(&mut *tx)
        .await?;
    let value = reply(&mut tx, node, key, &input, row).await?;
    tx.commit().await?;
    Ok(value)
}

pub(crate) async fn finish(
    pool: &PgPool,
    node: &NodeState,
    key: &Key,
    input: native::Finish,
) -> Result<()> {
    let hash = input.result.hash()?;
    let mut tx = pool.begin().await?;
    authorize(&mut tx, node, key, input.run).await?;
    let row = q::work(node.node_id, input.operation)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(Error::NodeOperationMissing)?;
    owned(node, input.run, &row)?;
    if let Some(previous) = &row.result_hash {
        ensure!(previous == &hash, Error::NodeOperationConflict);
        tx.commit().await?;
        return Ok(());
    }
    ensure!(row.valid_for_ms > 0, Error::NodeExpired);
    let work = &row.work;
    ensure!(
        input.result.source_error.is_none() || matches!(work.action, native::Action::Part { .. }),
        Error::NodeOperationConflict
    );
    match &work.action {
        native::Action::Initialize => {
            ensure!(
                input.result.retry == input.result.value.is_none(),
                Error::NodeOperationConflict
            );
            ensure!(
                queries::remote_initialized(
                    work.upload.upload,
                    row.token,
                    input.result.value.as_deref()
                )
                .fetch_one(&mut *tx)
                .await?,
                Error::NodeExpired
            );
        }
        native::Action::Part { .. } => {
            ensure!(
                input
                    .result
                    .value
                    .as_ref()
                    .is_none_or(|value| value.len() <= 128
                        && value.bytes().all(|byte| (33..=126).contains(&byte))),
                Error::NodeOperationConflict
            );
            ensure!(
                input.result.retry == input.result.value.is_none(),
                Error::NodeOperationConflict
            );
            ensure!(
                q::finish_part(
                    work,
                    row.token,
                    node.node_id,
                    input.result.value.as_deref(),
                    input.result.source_error.map(native::SourceError::code)
                )
                .execute(&mut *tx)
                .await?
                .rows_affected()
                    == 1,
                Error::NodeExpired
            );
        }
        native::Action::Abort => {
            ensure!(input.result.value.is_none(), Error::NodeOperationConflict);
            ensure!(
                q::clean_remote(work.upload.upload, row.token, input.result.retry)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected()
                    == 1,
                Error::NodeExpired
            );
        }
    }
    q::result(node.node_id, input.operation, &hash)
        .execute(&mut *tx)
        .await?;
    queries::release_storage_permit(row.token)
        .execute(&mut *tx)
        .await?;
    if let Some(lease) = row.read_lease {
        queries::delete_read_lease(lease).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}

pub(crate) async fn inventory(
    pool: &PgPool,
    node: &NodeState,
    key: &Key,
    input: native::Inventory,
) -> Result<Vec<crate::model::node_raw::File>> {
    ensure!(
        input.files.len() <= 128
            && input.files.iter().all(|id| !id.is_nil())
            && input.files.iter().collect::<HashSet<_>>().len() == input.files.len(),
        Error::NodeReadLimit
    );
    let mut tx = pool.begin().await?;
    authorize(&mut tx, node, key, input.run).await?;
    if input.recover {
        q::recover(node.node_id, &input.files)
            .execute(&mut *tx)
            .await?;
    }
    let ids = q::removable(node.node_id, &input.files)
        .fetch_all(&mut *tx)
        .await?;
    let pending: Vec<(Uuid, Uuid, i64)> = sqlx::query_as("SELECT m.id,m.storage_id,s.size FROM multipart_files m JOIN streams s ON s.id=m.id WHERE m.node_id=$1 AND m.id=ANY($2) AND (EXISTS(SELECT 1 FROM parts p JOIN uploads u ON u.id=p.upload_id WHERE p.stream_id=m.id AND p.remote_node_id=$1 AND p.remote_etag IS NULL AND u.state IN ('active','completing')) OR EXISTS(SELECT 1 FROM node_writes w JOIN stream_leases l ON l.id=w.lease_id WHERE w.stream_id=m.id AND w.node_id=$1 AND w.state='active' AND l.expires_at>clock_timestamp()))")
        .bind(node.node_id).bind(&input.files).fetch_all(&mut *tx).await?;
    let result = input
        .files
        .into_iter()
        .map(|id| {
            use crate::model::node_raw::{File, FileState};
            let held = pending.iter().find(|(file, _, _)| *file == id);
            File {
                id,
                storage: held.map(|(_, storage, _)| *storage),
                size: held.map_or(0, |(_, _, size)| *size as u64),
                state: if ids.contains(&id) {
                    FileState::Removable
                } else if held.is_some() {
                    FileState::Pending
                } else {
                    FileState::Protected
                },
            }
        })
        .collect();
    tx.commit().await?;
    Ok(result)
}

pub(crate) async fn complete(
    pool: &PgPool,
    node: &NodeState,
    key: &Key,
    input: native::Complete,
) -> Result<native::PreparedCompletion> {
    let mut tx = pool.begin().await?;
    let row = node_write_owned(&mut tx, node, key, input.run, input.operation).await?;
    require_node_write(&mut tx, &row).await?;
    let upload = row.completion_upload.ok_or(Error::NodeOperationConflict)?;
    ensure!(
        !q::failed(upload).fetch_one(&mut *tx).await?,
        Error::SourceUnavailable
    );
    let record = queries::node_completion(upload).fetch_one(&mut *tx).await?;
    ensure!(
        queries::freeze_remote(upload, input.operation, &record.manifest_hash)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        Error::NodeExpired
    );
    if input.resend {
        queries::release_write_upload_permits(node.node_id, input.operation, None)
            .execute(&mut *tx)
            .await?;
        q::resend(upload).execute(&mut *tx).await?;
    }
    let parts = q::completion_parts(upload).fetch_all(&mut *tx).await?;
    ensure!(parts.len() == record.summary.parts, Error::OperationAborted);
    let value = native::Completion {
        upload: queries::remote_upload(upload).fetch_one(&mut *tx).await?,
        parts,
    };
    let ready = value.upload.upstream.is_some()
        && value.parts.iter().all(|part| part.remote_etag.is_some());
    let mut grants = node_write_permits(
        &mut tx,
        node.node_id,
        &row,
        None,
        usize::from(ready),
        value.upload.storage,
        row.profile.upload_concurrency,
    )
    .await?;
    let current = queries::storage_profile(value.upload.storage)
        .fetch_one(&mut *tx)
        .await?;
    let limit = match (row.profile.read_concurrency, current.read_concurrency) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    };
    if ready && let Some(limit) = limit {
        let storage = value.upload.storage;
        queries::lock_storage_permits(format!("mokyu:store:{storage}:read"))
            .execute(&mut *tx)
            .await?;
        queries::expire_storage_permits(storage, "read")
            .execute(&mut *tx)
            .await?;
        let (token, valid_for_ms) = if let Some(permit) =
            q::read_permit(node.node_id, input.operation)
                .fetch_optional(&mut *tx)
                .await?
        {
            permit
        } else {
            ensure!(
                queries::storage_permit_count(storage, "read")
                    .fetch_one(&mut *tx)
                    .await?
                    < i64::from(limit),
                Error::NodeWriteBusy
            );
            let token = Uuid::new_v4();
            queries::insert_storage_permit(token, storage, "read", node.node_id)
                .execute(&mut *tx)
                .await?;
            queries::node_write_own_permit(node.node_id, input.operation, None, token)
                .execute(&mut *tx)
                .await?;
            (token, 60000)
        };
        grants.push(read::Grant {
            r#type: read::GrantType::StorageRead,
            id: token,
            valid_for_ms: valid_for_ms as u64,
            storage: Some(storage),
        });
    }
    tx.commit().await?;
    Ok(native::PreparedCompletion { value, grants })
}

pub(crate) async fn adopt(
    pool: &PgPool,
    node: &NodeState,
    key: &Key,
    input: native::Adopt,
) -> Result<write::Reply> {
    let mut tx = pool.begin().await?;
    let row = node_write_owned(&mut tx, node, key, input.run, input.operation).await?;
    let stream = require_node_write(&mut tx, &row).await?;
    let upload = row.completion_upload.ok_or(Error::NodeOperationConflict)?;
    let source = queries::remote_output_lock(upload, input.operation)
        .fetch_one(&mut *tx)
        .await?;
    let record = queries::node_completion(upload).fetch_one(&mut *tx).await?;
    let prepare = crate::model::node_raw::Prepare {
        run: input.run,
        operation: input.operation,
        input: crate::model::node_raw::Input {
            local: false,
            size: record.summary.size,
            digest: input.digest,
            hash: row.profile.deduplication_enabled.then_some(input.hash),
        },
    };
    let hash = chunk::hash(&prepare.input)?;
    if let Some(previous) = queries::node_raw_write(node.node_id, input.operation)
        .fetch_optional(&mut *tx)
        .await?
    {
        ensure!(
            previous.request_hash == hash && previous.committed,
            Error::NodeOperationConflict
        );
    } else {
        ensure!(
            row.next_offset == 0 && row.next_batch == 1,
            Error::NodeOperationConflict
        );
        ensure!(
            queries::raw_prepare(stream)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            Error::OperationAborted
        );
        ensure!(
            queries::remote_source_finish(
                source,
                &RemoteOutput {
                    upload,
                    token: input.operation,
                    stream,
                    size: record.summary.size,
                    digest: &input.digest,
                    hash: &input.hash,
                    background: false
                }
            )
            .execute(&mut *tx)
            .await?
            .rows_affected()
                == 1,
            Error::NodeOperationConflict
        );
        queries::raw_attach(stream, source.into(), record.summary.size)
            .execute(&mut *tx)
            .await?;
        if row.profile.deduplication_enabled {
            let current = queries::raw_source(source.into())
                .fetch_one(&mut *tx)
                .await?;
            share_raw(&mut tx, stream, &current, &input.hash).await?;
        }
        queries::extend_writing_stream(stream, record.summary.size)
            .execute(&mut *tx)
            .await?;
        queries::advance_node_write(node.node_id, input.operation, record.summary.size)
            .execute(&mut *tx)
            .await?;
        queries::insert_node_raw_write(node.node_id, &prepare, &hash, source, true)
            .execute(&mut *tx)
            .await?;
        queries::release_write_upload_permits(node.node_id, input.operation, None)
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
