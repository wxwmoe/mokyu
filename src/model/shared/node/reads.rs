use super::{
    Backend,
    queries::read_grants as q,
    read_map::{node_read_body_matches, node_read_mapping, node_read_materials},
};
use crate::{
    cluster::NodeState,
    content::codec::Key,
    model::{Error, cluster::NodeReads, node_read as read, node_refresh},
};
use anyhow::{Context, Result, ensure};
use uuid::Uuid;

async fn available_prefix(
    db: &mut q::Connection,
    node: &NodeState,
    run: crate::model::session::Run,
    mapping: &mut read::Mapping,
    storages: &[read::Storage],
) -> Result<bool> {
    use std::collections::{BTreeMap, BTreeSet};
    let limits: BTreeMap<_, _> = storages
        .iter()
        .filter_map(|s| Some((s.connection.id, s.profile.read_concurrency?)))
        .collect();
    if limits.len() < 2 {
        return Ok(false);
    }
    let mut available = BTreeSet::new();
    for (id, limit) in limits {
        q::lock_storage(db, id, "read").await?;
        let (count, local) = q::read_capacity(node, run, id).fetch_one(&mut *db).await?;
        if local || count < i64::from(limit) {
            available.insert(id);
        }
    }
    let mut included = BTreeSet::new();
    for part in &mapping.segments {
        if let Some(id) = part.source.storage()?
            && storages
                .iter()
                .any(|s| s.connection.id == id && s.profile.read_concurrency.is_some())
        {
            included.insert(id);
            if !available.contains(&id) || included.len() > read::LANES {
                ensure!(part.offset > mapping.start, Error::NodeReadBusy);
                mapping.truncate(part.offset);
                return Ok(true);
            }
        }
    }
    Ok(false)
}

async fn node_read_permits(
    db: &mut q::Connection,
    node: Uuid,
    read: Uuid,
    fresh: bool,
    storages: &[read::Storage],
    mapping: &read::Mapping,
    lanes: usize,
    held: &[Uuid],
    shared: &mut std::collections::BTreeMap<Uuid, read::Grant>,
) -> Result<Vec<read::Grant>> {
    use std::collections::{BTreeMap, BTreeSet};
    let mut demand = BTreeMap::<Uuid, usize>::new();
    let mut seen = BTreeSet::new();
    for part in &mapping.segments {
        let Some(storage) = part.source.storage()? else {
            continue;
        };
        let pin = part.source.pin()?;
        let count = match &part.source {
            read::Source::Raw { .. } if part.length > crate::content::raw_pages::PAGE as i64 => {
                (part.length as usize)
                    .div_ceil(8 * crate::content::raw_pages::PAGE)
                    .max(2)
            }
            _ if !seen.insert(pin) => 0,
            _ => 1,
        };
        *demand.entry(storage).or_default() += count.min(read::LANES);
    }
    let limits: BTreeMap<_, _> = storages
        .iter()
        .filter_map(|s| Some((s.connection.id, s.profile.read_concurrency?)))
        .filter(|(id, _)| demand.contains_key(id))
        .collect();
    ensure!(limits.len() <= read::LANES, Error::NodeReadLimit);
    let mut desired: BTreeMap<_, usize> = limits.keys().map(|id| (*id, 1)).collect();
    let mut left = lanes.saturating_sub(desired.len());
    while left > 0 {
        let before = left;
        for (id, count) in &mut desired {
            if left != 0 && *count < demand[id].min(limits[id] as usize) {
                *count += 1;
                left -= 1;
            }
        }
        if left == before {
            break;
        }
    }
    let mut grants = Vec::new();
    for (id, want) in desired {
        q::lock_storage(db, id, "read").await?;
        let (mut tokens, want) = if fresh {
            (Vec::new(), want)
        } else {
            let owned = q::node_read_permit(read, id).fetch_all(&mut *db).await?;
            let count = q::node_read_permit_count(read).fetch_one(&mut *db).await? as usize;
            let want = want.min(read::LANES.saturating_sub(count) + owned.len());
            ensure!(want > 0, Error::NodeReadLimit);
            (owned, want)
        };
        tokens.truncate(want);
        let mut borrowed = Vec::new();
        if tokens.is_empty()
            && let Some(first) = shared.get(&id)
        {
            tokens.push((first.id, first.valid_for_ms as i64));
            borrowed.push(first.id);
        }
        if tokens.len() < want {
            let allocated = q::allocate(
                db,
                read,
                node,
                id,
                i64::from(limits[&id]),
                (want - tokens.len()) as i64,
            )
            .await?;
            tokens.extend(allocated.into_iter().map(|token| (token, 60_000)));
        }
        if tokens.len() < want && lanes > 1 && !held.is_empty() {
            for item in q::read_pool(read, id, want as i64, held)
                .fetch_all(&mut *db)
                .await?
            {
                if !tokens.iter().any(|&(token, _)| token == item.0) {
                    borrowed.push(item.0);
                    tokens.push(item);
                }
                if tokens.len() == want {
                    break;
                }
            }
        }
        ensure!(!tokens.is_empty(), Error::NodeReadBusy);
        tokens.sort_unstable_by_key(|pair| pair.0);
        if !borrowed.is_empty() {
            q::own(db, read, &borrowed).await?;
        }
        for (token, remaining) in tokens {
            let grant = read::Grant {
                r#type: read::GrantType::StorageRead,
                id: token,
                valid_for_ms: remaining as u64,
                storage: Some(id),
            };
            shared.entry(id).or_insert_with(|| grant.clone());
            grants.push(grant);
        }
    }
    Ok(grants)
}

async fn node_read_save_window(
    db: &mut q::Connection,
    read: Uuid,
    mapping: &crate::model::node_read::Mapping,
) -> Result<Uuid> {
    let id = q::insert_window(db, read, mapping).await?;
    let pins: std::collections::BTreeSet<_> = mapping
        .segments
        .iter()
        .map(|segment| segment.source.pin())
        .collect::<Result<_>>()?;
    ensure!(
        pins.iter().all(|(_, id)| !id.is_nil()),
        "invalid read source identity"
    );
    q::pin(db, id, &pins).await?;
    Ok(id)
}

pub(super) async fn open_in(
    tx: &mut q::Tx<'_>,
    state: &NodeState,
    key: &Key,
    mut input: read::Query,
    refresh: bool,
) -> Result<read::Reply> {
    use read::{MaterialType, Outcome};
    let mut include = vec![MaterialType::Credential, MaterialType::Bucket];
    if input.policy {
        include.push(MaterialType::Policy);
    }
    let (mut reply, bucket, policy) = super::node_requests::materials(
        tx,
        state,
        key,
        input.access.as_deref(),
        Some(&input.route),
        &include,
        input.access.is_none(),
        &input.cached,
    )
    .await?;
    let Some(bucket) = bucket else {
        return Ok(reply);
    };
    if bucket.state != "active"
        || if let Some(access) = &input.access {
            !q::node_read_allowed(access, bucket.project_id, bucket.id)
                .fetch_one(&mut **tx)
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
        let image = super::image_transactions::resolve(tx, bucket.id, &object_key, request).await?;
        reply.image(state.deployment_id, bucket.id, &image, &input.cached)?;
        object_key = image.key.clone();
        input.prepare_image(&image);
        Some(image)
    } else {
        None
    };
    reply.source_key = object_key.clone();
    let Some(stream) = q::current_stream(tx, bucket.id, &object_key).await? else {
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
    input.select(&stream, &mut reply, policy.as_ref())?;
    if reply.body_reused {
        reply.body_reused = match node_read_body_matches(
            tx,
            state,
            key,
            &stream,
            input.body.as_ref().unwrap(),
            refresh.then_some(input.cached.as_slice()),
            &mut reply.materials,
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
        q::validate_run(tx, state, input.run).await?;
        return Ok(reply);
    }
    let reply = protect_node_read(
        tx,
        state,
        key,
        &input,
        &stream,
        reply,
        &mut Default::default(),
    )
    .await?;
    Ok(reply)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn protect_node_read(
    tx: &mut q::Tx<'_>,
    state: &NodeState,
    key: &Key,
    input: &read::Query,
    stream: &crate::app::StoredStream,
    mut reply: read::Reply,
    shared: &mut std::collections::BTreeMap<Uuid, read::Grant>,
) -> Result<read::Reply> {
    use read::{Grant, GrantType, Outcome};
    q::lock_node(tx, state.node_id).await?;
    if !input.permits.released.is_empty() {
        q::release_held(tx, state, input.run, &input.permits.released).await?;
    }
    let hash = input.digest()?;
    let previous = q::node_read_existing(state, input.run, input.operation)
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
            q::node_read_count(state.node_id)
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
            let id = q::create(tx, state, input, stream, start, end, &hash).await?;
            (id, read::READ_SECONDS * 1000)
        };
        reply.body_reused = false;
        reply.grants.push(Grant {
            r#type: GrantType::Read,
            id,
            valid_for_ms: remaining as u64,
            storage: None,
        });
        q::validate_run(tx, state, input.run).await?;
        ensure!(reply.bytes() <= input.max_bytes, Error::NodeReadLimit);
        return Ok(reply);
    }
    let existing = if let Some(previous) = &previous {
        q::node_read_window(previous.read_id, start)
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
            &mut mapping,
            input.max_bytes,
            &input.cached,
            &mut reply,
        )
        .await?;
        if existing.is_none()
            && available_prefix(tx, state, input.run, &mut mapping, &storages).await?
        {
            continue;
        }
        if reply.bytes() + 1024 + read::LANES * 128 <= input.max_bytes {
            break storages;
        }
        ensure!(existing.is_none() && mapping.shrink(), Error::NodeReadLimit);
    };
    let fresh = previous.is_none();
    let (id, remaining) = if let Some(previous) = previous {
        (previous.read_id, previous.valid_for_ms)
    } else {
        let id = q::create(tx, state, input, stream, start, end, &hash).await?;
        (id, read::READ_SECONDS * 1000)
    };
    let window = if let Some((id, _)) = existing {
        id
    } else {
        node_read_save_window(tx, id, &mapping).await?
    };
    reply.grants = node_read_permits(
        tx,
        state.node_id,
        id,
        fresh,
        &storages,
        &mapping,
        input.lanes,
        &input.permits.held,
        shared,
    )
    .await?;
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
    q::validate_run(tx, state, input.run).await?;
    ensure!(
        q::node_read_owned(state, input.run, id)
            .fetch_optional(&mut **tx)
            .await?
            .is_some(),
        Error::NodeExpired
    );
    Ok(reply)
}

#[async_trait::async_trait]
impl NodeReads for Backend {
    async fn open_read(
        &self,
        state: &NodeState,
        key: &Key,
        input: read::Query,
    ) -> Result<read::Reply> {
        let mut tx = q::begin(self).await?;
        super::queries::node_requests::admit(self, &mut tx, state, key, input.run).await?;
        let reply = open_in(&mut tx, state, key, input, false).await?;
        tx.commit().await?;
        Ok(reply)
    }
    async fn refresh_reads(
        &self,
        state: &NodeState,
        key: &Key,
        input: node_refresh::Request,
    ) -> Result<node_refresh::Reply> {
        q::refresh(self, state, key, input).await
    }
    fn refresh_status(&self) -> serde_json::Value {
        q::refresh_status(self)
    }
    async fn report_observations(
        &self,
        node: &NodeState,
        key: &Key,
        input: crate::model::node_observation::Batch,
    ) -> Result<Vec<bool>> {
        q::observations(self, node, key, input).await
    }
    async fn report_read(
        &self,
        state: &NodeState,
        key: &Key,
        input: read::Report,
    ) -> Result<Vec<Uuid>> {
        q::report(self, state, key, input).await
    }
    async fn read_window(
        &self,
        state: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        input: crate::model::node_read::Window,
    ) -> Result<crate::model::node_read::Reply> {
        use crate::model::{
            Error,
            node_read::{self as read, Grant, GrantType, Outcome, Reply},
        };
        let mut tx = q::begin(self).await?;
        q::access(self, &mut tx, state, key, input.run).await?;
        q::lock_node(&mut tx, state.node_id).await?;
        q::release_held(&mut tx, state, input.run, &input.permits.released).await?;
        let owned = q::node_read_owned(state, input.run, input.cursor.lease)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(Error::NodeExpired)?;
        ensure!(
            owned.stream_id == input.cursor.stream
                && owned.payload_revision == input.cursor.revision
                && input.cursor.end == owned.end_bytes
                && input.cursor.offset >= owned.offset_bytes
                && input.cursor.offset < owned.end_bytes,
            Error::NodeExpired
        );
        let stream = q::node_read_stream(owned.stream_id)
            .fetch_one(&mut *tx)
            .await?;
        ensure!(
            stream.payload_revision == owned.payload_revision,
            Error::SourceUnavailable
        );
        let existing = q::node_read_window(owned.read_id, input.cursor.offset)
            .fetch_optional(&mut *tx)
            .await?;
        if existing.is_none() {
            ensure!(
                q::node_read_window_count(owned.read_id)
                    .fetch_one(&mut *tx)
                    .await?
                    < 2,
                Error::NodeReadBusy
            );
        }
        let mut mapping = if let Some((_, mapping)) = &existing {
            mapping.0.clone()
        } else {
            let extents = q::replica_extents(self, &owned, &stream, &input).await;
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
                &mut mapping,
                input.max_bytes,
                &input.cached,
                &mut reply,
            )
            .await?;
            if existing.is_none()
                && available_prefix(&mut tx, state, input.run, &mut mapping, &storages).await?
            {
                continue;
            }
            if reply.bytes() + 1024 + read::LANES * 128 <= input.max_bytes {
                break storages;
            }
            ensure!(existing.is_none() && mapping.shrink(), Error::NodeReadLimit);
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
            false,
            &storages,
            &mapping,
            input.lanes,
            &input.permits.held,
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
        q::validate_run(&mut tx, state, input.run).await?;
        ensure!(
            q::node_read_owned(state, input.run, owned.read_id)
                .fetch_optional(&mut *tx)
                .await?
                .is_some(),
            Error::NodeExpired
        );
        tx.commit().await?;
        Ok(reply)
    }
}
