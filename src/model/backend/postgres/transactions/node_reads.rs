use super::*;

async fn node_read_mapping(
    db: &mut PgConnection,
    node: Uuid,
    stream: &crate::app::StoredStream,
    start: i64,
    end: i64,
    limit: usize,
    extents: Option<Vec<crate::app::Extent>>,
) -> Result<crate::model::node_read::Mapping> {
    let (raw, extents) = if stream.payload_type == "raw" {
        (
            queries::node_read_raw(stream.id, start, end, limit as i64)
                .fetch_all(&mut *db)
                .await?
                .into_iter()
                .map(|row| (stream.id, row))
                .collect(),
            Vec::new(),
        )
    } else {
        let extents = match extents {
            Some(rows) => rows,
            None => {
                queries::node_read_extents(stream.id, start, end, limit as i64)
                    .fetch_all(&mut *db)
                    .await?
            }
        };
        (Vec::new(), extents)
    };
    super::super::queries::sources::Sources::load(db, raw, extents)
        .await?
        .mapping(node, stream, start, end)
}

async fn node_read_materials(
    db: &mut PgConnection,
    deployment: Uuid,
    key: &crate::content::codec::Key,
    mapping: &crate::model::node_read::Mapping,
    cached: &[crate::model::node_read::Version],
    reply: &mut crate::model::node_read::Reply,
) -> Result<Vec<crate::model::node_read::Storage>> {
    use crate::model::{
        Error,
        node_read::{BlockKey, MaterialType, Storage},
    };
    use std::collections::{BTreeSet, HashMap};
    let storages: Vec<_> = mapping
        .segments
        .iter()
        .map(|segment| segment.source.storage())
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let keys: Vec<_> = mapping
        .segments
        .iter()
        .filter_map(|segment| segment.source.key())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let records = queries::node_read_storages(&storages)
        .fetch_all(&mut *db)
        .await?;
    let mut profiles: HashMap<_, _> = queries::node_read_profiles(&storages)
        .fetch_all(&mut *db)
        .await?
        .into_iter()
        .map(|row| (row.storage_id, row))
        .collect();
    ensure!(records.len() == storages.len(), Error::SourceUnavailable);
    let mut materials = Vec::new();
    for record in records {
        let mut profile = profiles
            .remove(&record.id)
            .context("read storage profile is missing")?;
        profile.write_durability = None;
        let storage = Storage {
            connection: crate::storage::model::Connection::unprotect(&record, key)?,
            revision: profile.revision,
            profile,
        };
        reply.material(
            deployment,
            MaterialType::Storage,
            record.id.to_string(),
            &storage,
            cached,
        )?;
        materials.push(storage);
    }
    let records = queries::node_read_keys(&keys).fetch_all(&mut *db).await?;
    ensure!(records.len() == keys.len(), Error::SourceUnavailable);
    for record in records {
        let block = BlockKey::unprotect(record, key)?;
        reply.material(
            deployment,
            MaterialType::Key,
            block.id.to_string(),
            &block,
            cached,
        )?;
    }
    reply.mapping = Some(reply.material(
        deployment,
        MaterialType::Mapping,
        format!(
            "{}:{}:{}:{}",
            mapping.stream, mapping.revision, mapping.start, mapping.end
        ),
        mapping,
        cached,
    )?);
    Ok(materials)
}

async fn node_read_body_matches(
    db: &mut PgConnection,
    node: &crate::cluster::NodeState,
    key: &crate::content::codec::Key,
    stream: &crate::app::StoredStream,
    body: &crate::model::node_read::CachedBody,
) -> Result<bool> {
    use crate::model::node_read::{self as read, MaterialType, Outcome, Reply};
    let mapping = node_read_mapping(
        db,
        node.node_id,
        stream,
        0,
        stream.size,
        read::MAX_ITEMS,
        None,
    )
    .await?;
    if mapping.end != stream.size {
        return Ok(false);
    }
    let mut probe = Reply::new(Outcome::Ready);
    node_read_materials(db, node.deployment_id, key, &mapping, &[], &mut probe).await?;
    if !body.verified {
        for material in &probe.materials {
            if material.version.r#type == MaterialType::Storage {
                let Some(json) = &material.json else {
                    return Ok(false);
                };
                let storage: read::Storage = serde_json::from_slice(json)?;
                if storage.profile.read_integrity == "inline" {
                    return Ok(false);
                }
            }
        }
    }
    let policies: Vec<_> = probe
        .materials
        .iter()
        .filter(|material| material.version.r#type == MaterialType::Storage)
        .map(|material| &material.version)
        .collect();
    Ok(probe.mapping.as_ref() == Some(&body.mapping)
        && policies.len() == body.storages.len()
        && policies
            .iter()
            .all(|version| body.storages.contains(version)))
}

async fn node_read_permits(
    db: &mut PgConnection,
    node: Uuid,
    read: Uuid,
    storages: &[crate::model::node_read::Storage],
    shared: &mut std::collections::BTreeMap<Uuid, read::Grant>,
) -> Result<Vec<crate::model::node_read::Grant>> {
    use crate::model::{
        Error,
        node_read::{Grant, GrantType},
    };
    let mut grants = Vec::new();
    for storage in storages {
        let Some(limit) = storage.profile.read_concurrency else {
            continue;
        };
        let id = storage.connection.id;
        queries::lock_storage_permits(format!("mokyu:store:{id}:read"))
            .execute(&mut *db)
            .await?;
        let (token, remaining) = if let Some(permit) = queries::node_read_permit(read, id)
            .fetch_optional(&mut *db)
            .await?
        {
            permit
        } else if let Some(grant) = shared.get(&id) {
            queries::node_read_own_permit(read, grant.id)
                .execute(&mut *db)
                .await?;
            (grant.id, grant.valid_for_ms as i64)
        } else {
            queries::expire_storage_permits(id, "read")
                .execute(&mut *db)
                .await?;
            ensure!(
                queries::node_read_permit_count(read)
                    .fetch_one(&mut *db)
                    .await?
                    < 16,
                Error::NodeReadLimit
            );
            ensure!(
                queries::storage_permit_count(id, "read")
                    .fetch_one(&mut *db)
                    .await?
                    < i64::from(limit),
                Error::NodeReadBusy
            );
            let token = Uuid::new_v4();
            queries::insert_storage_permit(token, id, "read", node)
                .execute(&mut *db)
                .await?;
            queries::node_read_own_permit(read, token)
                .execute(&mut *db)
                .await?;
            (token, 60_000)
        };
        let grant = Grant {
            r#type: GrantType::StorageRead,
            id: token,
            valid_for_ms: remaining as u64,
            storage: Some(id),
        };
        shared.entry(id).or_insert_with(|| grant.clone());
        grants.push(grant);
    }
    Ok(grants)
}

async fn node_read_save_window(
    db: &mut PgConnection,
    read: Uuid,
    mapping: &crate::model::node_read::Mapping,
) -> Result<Uuid> {
    let id = queries::node_read_insert_window(read, mapping)
        .fetch_one(&mut *db)
        .await?;
    let pins = mapping
        .segments
        .iter()
        .map(|segment| segment.source.pin())
        .collect::<Result<Vec<_>>>()?;
    let kinds = pins
        .iter()
        .map(|(r#type, _)| (*r#type).to_owned())
        .collect::<Vec<_>>();
    let ids = pins.iter().map(|(_, id)| *id).collect::<Vec<_>>();
    ensure!(
        ids.iter().all(|id| !id.is_nil()),
        "invalid read source identity"
    );
    queries::node_read_pin(id, &kinds, &ids).execute(db).await?;
    Ok(id)
}

pub(in super::super) async fn open_node_read(
    pool: &PgPool,
    state: &crate::cluster::NodeState,
    key: &crate::content::codec::Key,
    mut input: crate::model::node_read::Query,
) -> Result<crate::model::node_read::Reply> {
    use crate::model::{
        Error,
        node_read::{self as read, MaterialType, Outcome},
    };
    let mut tx = pool.begin().await?;
    authenticate_rpc_node(&mut tx, state, key, false).await?;
    ensure!(
        matches!(state.role.as_str(), "data" | "node"),
        Error::NodeAuthentication
    );
    let session = require_node_run(&mut tx, state, input.run, false).await?;
    ensure!(
        !session.draining && queries::node_read_admission().fetch_one(&mut *tx).await?,
        Error::NodeInactive
    );
    let mut include = vec![MaterialType::Credential, MaterialType::Bucket];
    if input.policy {
        include.push(MaterialType::Policy);
    }
    let (mut reply, bucket) = node_request_materials(
        &mut tx,
        state,
        key,
        input.access.as_deref(),
        Some(&input.route),
        &include,
        &input.cached,
    )
    .await?;
    let Some(bucket) = bucket else {
        return Ok(reply);
    };
    if bucket.state != "active"
        || if let Some(access) = &input.access {
            !queries::node_read_allowed(access, bucket.project_id, bucket.id)
                .fetch_one(&mut *tx)
                .await?
        } else {
            false
        }
    {
        reply.outcome = Outcome::AccessDenied;
        return Ok(reply);
    }
    let mut object_key = input.route.key(&bucket).to_owned();
    let image = if let Some(request) = input
        .route
        .image()
        .filter(|request| bucket.image_revision > 0 || request.explicit())
    {
        let image =
            super::super::image_transactions::resolve(&mut tx, bucket.id, &object_key, request)
                .await?;
        reply.image(state.deployment_id, bucket.id, &image, &input.cached)?;
        object_key = image.key.clone();
        input.prepare_image(&image);
        Some(image)
    } else {
        None
    };
    reply.source_key = object_key.clone();
    let Some(stream) = locked_current_stream(&mut tx, bucket.id, &object_key).await? else {
        reply.outcome = Outcome::NoSuchKey;
        return Ok(reply);
    };
    if input.access.is_none() && !stream.public_read {
        reply.outcome = Outcome::AccessDenied;
        return Ok(reply);
    }
    if let Some(image) = image.filter(|image| image.rule.is_some()) {
        ensure!(
            stream.size > 0 && stream.size as u64 <= image.settings.policy.input_limit,
            s3s::s3_error!(EntityTooLarge)
        );
    }
    let object = read::Object::from_stream(&stream)?;
    reply.object = Some(reply.material(
        state.deployment_id,
        MaterialType::Object,
        object.id.to_string(),
        &object,
        &input.cached,
    )?);
    input.select(&stream, &mut reply)?;
    if reply.body_reused {
        reply.body_reused = match node_read_body_matches(
            &mut tx,
            state,
            key,
            &stream,
            input.body.as_ref().unwrap(),
        )
        .await
        {
            Ok(accepted) => accepted,
            Err(error) if crate::model::error(&error) == Some(Error::SourceUnavailable) => false,
            Err(error) => return Err(error),
        };
    }
    if reply.outcome != Outcome::Ready
        || input.head
        || (!input.mapping && !input.protection)
        || (reply.length == 0 && !input.protection)
        || (reply.body_reused && !input.protection)
    {
        ensure!(reply.bytes() <= input.max_bytes, Error::NodeReadLimit);
        require_node_run(&mut tx, state, input.run, false).await?;
        tx.commit().await?;
        return Ok(reply);
    }
    let reply = protect_node_read(
        &mut tx,
        state,
        key,
        &input,
        &stream,
        reply,
        &mut Default::default(),
    )
    .await?;
    tx.commit().await?;
    Ok(reply)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn protect_node_read(
    tx: &mut Transaction<'_, Postgres>,
    state: &NodeState,
    key: &Key,
    input: &read::Query,
    stream: &crate::app::StoredStream,
    mut reply: read::Reply,
    shared: &mut std::collections::BTreeMap<Uuid, read::Grant>,
) -> Result<read::Reply> {
    use read::{Grant, GrantType, Outcome};
    queries::lock_staging(format!("mokyu:node-read:{}", state.node_id))
        .execute(&mut **tx)
        .await?;
    let hash = input.digest()?;
    let previous = queries::node_read_existing(state, input.run, input.operation)
        .fetch_optional(&mut **tx)
        .await?;
    let start = reply.offset as i64;
    let end = (reply.offset + reply.length) as i64;
    if let Some(previous) = &previous {
        ensure!(previous.valid_for_ms > 0, Error::NodeExpired);
        ensure!(
            previous.request_hash == hash
                && previous.stream_id == stream.id
                && previous.payload_revision == stream.payload_revision
                && previous.offset_bytes == start
                && previous.end_bytes == end,
            Error::NodeConflict
        );
    } else {
        ensure!(
            queries::node_read_count(state.node_id)
                .fetch_one(&mut **tx)
                .await?
                < read::READ_LIMIT,
            Error::NodeReadBusy
        );
    }
    if !input.mapping || reply.length == 0 {
        let (id, remaining) = if let Some(previous) = previous {
            (previous.read_id, previous.valid_for_ms)
        } else {
            let id = create_read_lease(tx, stream.id, read::READ_SECONDS).await?;
            queries::node_read_insert(state, input, id, stream, start, end, &hash)
                .execute(&mut **tx)
                .await?;
            (id, read::READ_SECONDS * 1000)
        };
        reply.body_reused = false;
        reply.grants.push(Grant {
            r#type: GrantType::Read,
            id,
            valid_for_ms: remaining as u64,
            storage: None,
        });
        require_node_run(tx, state, input.run, false).await?;
        ensure!(reply.bytes() <= input.max_bytes, Error::NodeReadLimit);
        return Ok(reply);
    }
    let existing = if let Some(previous) = &previous {
        queries::node_read_window(previous.read_id, start)
            .fetch_optional(&mut **tx)
            .await?
    } else {
        None
    };
    ensure!(previous.is_none() || existing.is_some(), Error::NodeExpired);
    let mut mapping = if let Some((_, mapping)) = &existing {
        mapping.0.clone()
    } else {
        match node_read_mapping(tx, state.node_id, stream, start, end, input.max_items, None).await
        {
            Ok(mapping) => mapping,
            Err(error) if crate::model::error(&error) == Some(Error::SourceUnavailable) => {
                reply.outcome = Outcome::Pending;
                return Ok(reply);
            }
            Err(error) => return Err(error),
        }
    };
    ensure!(
        mapping.segments.len() <= input.max_items,
        Error::NodeReadLimit
    );
    let base = reply.materials.len();
    let storages = loop {
        reply.materials.truncate(base);
        let storages = node_read_materials(
            tx,
            state.deployment_id,
            key,
            &mapping,
            &input.cached,
            &mut reply,
        )
        .await?;
        if reply.bytes() + 1024 + storages.len() * 128 <= input.max_bytes {
            break storages;
        }
        ensure!(
            existing.is_none() && mapping.segments.len() > 1,
            Error::NodeReadLimit
        );
        mapping
            .segments
            .truncate(mapping.segments.len().div_ceil(2));
        let last = mapping.segments.last().unwrap();
        mapping.end = last.offset + last.length;
    };
    let (id, remaining) = if let Some(previous) = previous {
        (previous.read_id, previous.valid_for_ms)
    } else {
        let id = create_read_lease(tx, stream.id, read::READ_SECONDS).await?;
        queries::node_read_insert(state, input, id, stream, start, end, &hash)
            .execute(&mut **tx)
            .await?;
        (id, read::READ_SECONDS * 1000)
    };
    let window = if let Some((id, _)) = existing {
        id
    } else {
        node_read_save_window(tx, id, &mapping).await?
    };
    reply.grants = node_read_permits(tx, state.node_id, id, &storages, shared).await?;
    reply.grants.extend([
        Grant {
            r#type: GrantType::Read,
            id,
            valid_for_ms: remaining as u64,
            storage: None,
        },
        Grant {
            r#type: GrantType::Window,
            id: window,
            valid_for_ms: remaining as u64,
            storage: None,
        },
    ]);
    if mapping.end < end {
        reply.next = Some(read::Cursor {
            lease: id,
            stream: mapping.stream,
            revision: mapping.revision,
            offset: mapping.end,
            end,
        });
    }
    require_node_run(tx, state, input.run, false).await?;
    ensure!(
        queries::node_read_owned(state, input.run, id)
            .fetch_optional(&mut **tx)
            .await?
            .is_some(),
        Error::NodeExpired
    );
    Ok(reply)
}

pub(in super::super) async fn report_node_read(
    pool: &PgPool,
    state: &crate::cluster::NodeState,
    key: &crate::content::codec::Key,
    input: crate::model::node_read::Report,
) -> Result<Vec<Uuid>> {
    use crate::model::{
        Error,
        node_read::Source,
        source::{SourceOperation, SourceScope, SourceType},
    };
    ensure!(
        !input.segments.is_empty() && input.segments.len() <= 16,
        s3s::s3_error!(InvalidArgument)
    );
    let mut tx = pool.begin().await?;
    authenticate_rpc_node(&mut tx, state, key, false).await?;
    ensure!(
        matches!(state.role.as_str(), "data" | "worker" | "node"),
        Error::NodeAuthentication
    );
    require_node_run(&mut tx, state, input.run, false).await?;
    queries::lock_staging(format!("mokyu:node-read:{}", state.node_id))
        .execute(&mut *tx)
        .await?;
    queries::node_read_owned(state, input.run, input.read)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(Error::NodeExpired)?;
    let mapping = queries::node_read_report_window(input.read, input.window)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(Error::NodeExpired)?
        .0;
    let mut scopes = std::collections::BTreeMap::new();
    for index in input.segments {
        let segment = mapping
            .segments
            .get(index)
            .ok_or_else(|| s3s::s3_error!(InvalidArgument))?;
        let (storage_id, source, id) = match &segment.source {
            Source::Chunk { chunk } => (
                chunk.storage_id.context("chunk storage missing")?,
                SourceType::Chunk,
                chunk.encoding_id,
            ),
            Source::Pack { pack, .. } => (
                pack.storage_id.context("pack storage missing")?,
                SourceType::Pack,
                pack.id,
            ),
            Source::Raw { source } if !source.pending_upload && source.medium == "shared_s3" => {
                (source.storage_id, SourceType::Raw, source.id)
            }
            _ => return Err(s3s::s3_error!(InvalidArgument).into()),
        };
        let scope = SourceScope {
            storage_id,
            source,
            id: id.to_string(),
        };
        scopes.insert(serde_json::to_string(&scope)?, scope);
    }
    queries::lock_staging("mokyu:read-verification".into())
        .execute(&mut *tx)
        .await?;
    let mut pending = queries::node_read_verifications()
        .fetch_one(&mut *tx)
        .await?;
    let mut tasks = Vec::with_capacity(scopes.len());
    for (identity, scope) in scopes {
        let mut hash = blake3::Hasher::new_derive_key("mokyu node read report v1");
        hash.update(input.window.as_bytes());
        hash.update(identity.as_bytes());
        let task = Uuid::from_bytes(hash.finalize().as_bytes()[..16].try_into().unwrap());
        if queries::node_read_reported(task)
            .fetch_one(&mut *tx)
            .await?
        {
            tasks.push(task);
            continue;
        }
        let snapshot = queries::source_snapshot(&mut tx, &scope, true).await?;
        if snapshot.state != "ready" || snapshot.quarantine_code.is_some() {
            continue;
        }
        ensure!(pending < 64, Error::NodeReadBusy);
        let operation = SourceOperation {
            principal: crate::authorization::Principal::System,
            mode: "cluster".into(),
            // The worker replaces this suspicion only after inspecting the exact backend source.
            code: Some("hash_mismatch".into()),
            node: None,
            snapshot,
        };
        let detail = json!({"policy":"verify","operation":"verify","storage_id":scope.storage_id,
            "source":operation.snapshot.summary()?,"source_location":operation.snapshot.location(),
            "execution_mode":"cluster","source_operation":operation});
        insert_task(&mut tx, task, "integrity", None, detail, None).await?;
        pending += 1;
        tasks.push(task);
    }
    tx.commit().await?;
    Ok(tasks)
}

pub(in super::super) async fn node_read_window(
    pool: &PgPool,
    replica: Option<&super::super::connection::Replica>,
    state: &crate::cluster::NodeState,
    key: &crate::content::codec::Key,
    input: crate::model::node_read::Window,
) -> Result<crate::model::node_read::Reply> {
    use crate::model::{
        Error,
        node_read::{self as read, Grant, GrantType, Outcome, Reply},
    };
    let mut tx = pool.begin().await?;
    authenticate_rpc_node(&mut tx, state, key, false).await?;
    ensure!(
        matches!(state.role.as_str(), "data" | "worker" | "node"),
        Error::NodeAuthentication
    );
    require_node_run(&mut tx, state, input.run, false).await?;
    queries::lock_staging(format!("mokyu:node-read:{}", state.node_id))
        .execute(&mut *tx)
        .await?;
    let owned = queries::node_read_owned(state, input.run, input.cursor.lease)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(Error::NodeExpired)?;
    ensure!(
        owned.stream_id == Id::Uuid(input.cursor.stream)
            && owned.payload_revision == input.cursor.revision
            && input.cursor.end == owned.end_bytes
            && input.cursor.offset >= owned.offset_bytes
            && input.cursor.offset < owned.end_bytes,
        Error::NodeExpired
    );
    let stream = queries::node_read_stream(owned.stream_id)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(
        stream.payload_revision == owned.payload_revision,
        Error::SourceUnavailable
    );
    let existing = queries::node_read_window(owned.read_id, input.cursor.offset)
        .fetch_optional(&mut *tx)
        .await?;
    if existing.is_none() {
        ensure!(
            queries::node_read_window_count(owned.read_id)
                .fetch_one(&mut *tx)
                .await?
                < 2,
            Error::NodeReadBusy
        );
    }
    let mut mapping = if let Some((_, mapping)) = &existing {
        mapping.0.clone()
    } else {
        let extents = if let Some(replica) =
            replica.filter(|_| owned.replica_allowed && stream.payload_type == "chunks")
        {
            replica.node_extents(&input.cursor, input.max_items).await
        } else {
            None
        };
        node_read_mapping(
            &mut tx,
            state.node_id,
            &stream,
            input.cursor.offset,
            input.cursor.end,
            input.max_items,
            extents,
        )
        .await?
    };
    ensure!(
        mapping.segments.len() <= input.max_items,
        Error::NodeReadLimit
    );
    let mut reply = Reply::new(Outcome::Ready);
    let storages = loop {
        reply.materials.clear();
        let storages = node_read_materials(
            &mut tx,
            state.deployment_id,
            key,
            &mapping,
            &input.cached,
            &mut reply,
        )
        .await?;
        if reply.bytes() + 1024 + storages.len() * 128 <= input.max_bytes {
            break storages;
        }
        ensure!(
            existing.is_none() && mapping.segments.len() > 1,
            Error::NodeReadLimit
        );
        mapping
            .segments
            .truncate(mapping.segments.len().div_ceil(2));
        let last = mapping.segments.last().unwrap();
        mapping.end = last.offset + last.length;
    };
    let window = if let Some((id, _)) = existing {
        id
    } else {
        node_read_save_window(&mut tx, owned.read_id, &mapping).await?
    };
    reply.grants = node_read_permits(
        &mut tx,
        state.node_id,
        owned.read_id,
        &storages,
        &mut Default::default(),
    )
    .await?;
    reply.grants.push(Grant {
        r#type: GrantType::Window,
        id: window,
        valid_for_ms: owned.valid_for_ms as u64,
        storage: None,
    });
    reply.offset = mapping.start as u64;
    reply.length = (mapping.end - mapping.start) as u64;
    if mapping.end < input.cursor.end {
        reply.next = Some(read::Cursor {
            offset: mapping.end,
            ..input.cursor
        });
    }
    require_node_run(&mut tx, state, input.run, false).await?;
    ensure!(
        queries::node_read_owned(state, input.run, owned.read_id)
            .fetch_optional(&mut *tx)
            .await?
            .is_some(),
        Error::NodeExpired
    );
    tx.commit().await?;
    Ok(reply)
}
