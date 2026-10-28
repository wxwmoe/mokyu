use super::{queries, read_sources::Sources, transactions};
use crate::{
    app::{Bucket, StoredStream},
    cluster::NodeState,
    content::codec::Key,
    model::{self, Error, ReadRoute, node_read as read, node_refresh as refresh, session::Run},
};
use anyhow::{Context, Result, ensure};
use sqlx::PgPool;
use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Semaphore, oneshot};
use uuid::Uuid;

#[path = "refresh_queries.rs"]
mod sql;

const GROUP_LIMIT: usize = 128;
const MATERIAL_LIMIT: usize = 8 * 1024 * 1024;

struct Pending {
    node: NodeState,
    input: refresh::Request,
    done: oneshot::Sender<Result<refresh::Reply>>,
}

#[derive(Default)]
struct Waiting {
    items: usize,
    requests: Vec<Pending>,
}

pub(super) struct Queue {
    waiting: Mutex<Waiting>,
    slots: Arc<Semaphore>,
    materials: Mutex<VecDeque<([u8; 32], Instant, read::Material)>>,
    groups: AtomicU64,
    requests: AtomicU64,
    items: AtomicU64,
    routes: AtomicU64,
    material_hits: AtomicU64,
}

impl Default for Queue {
    fn default() -> Self {
        Self {
            waiting: Mutex::default(),
            slots: Arc::new(Semaphore::new(2)),
            materials: Mutex::default(),
            groups: 0.into(),
            requests: 0.into(),
            items: 0.into(),
            routes: 0.into(),
            material_hits: 0.into(),
        }
    }
}

impl Queue {
    pub(super) fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({"groups":self.groups.load(Relaxed),"requests":self.requests.load(Relaxed),"items":self.items.load(Relaxed),"routes":self.routes.load(Relaxed),"material_hits":self.material_hits.load(Relaxed),"queued_items":self.waiting.lock().unwrap().items,"active_groups":2-self.slots.available_permits()})
    }

    fn storage(
        &self,
        deployment: Uuid,
        key: &Key,
        record: crate::storage::model::Record,
        mut profile: crate::storage::profile::StorageProfile,
    ) -> Result<read::Material> {
        profile.write_durability = None;
        // The lookup binds freshly read configuration, never a cached authorization decision.
        let identity = serde_json::to_vec(&(
            deployment,
            &record.id,
            &record.name,
            &record.r#type,
            &record.path,
            &record.endpoint_url,
            &record.remote_bucket_name,
            &record.region,
            &record.prefix,
            &record.access_key_encrypted,
            &record.secret_key_encrypted,
            &record.identity_marker,
            record.enabled,
            &profile,
            blake3::hash(&key.material).as_bytes(),
        ))?;
        let identity = *blake3::hash(&identity).as_bytes();
        let mut cache = self.materials.lock().unwrap();
        cache.retain(|(_, at, _)| at.elapsed() < Duration::from_secs(1));
        if let Some((_, _, material)) = cache.iter().find(|(id, _, _)| *id == identity) {
            self.material_hits.fetch_add(1, Relaxed);
            return Ok(material.clone());
        }
        let storage = read::Storage {
            connection: crate::storage::model::Connection::unprotect(&record, key)?,
            revision: profile.revision,
            profile,
        };
        let mut materials = Vec::new();
        read::material(
            &mut materials,
            deployment,
            read::MaterialType::Storage,
            record.id.to_string(),
            &storage,
            &[],
        )?;
        let material = materials.remove(0);
        let bytes = material.json.as_ref().map_or(0, Vec::len) + 512;
        if bytes <= MATERIAL_LIMIT {
            while cache.len() >= 128
                || cache
                    .iter()
                    .map(|(_, _, value)| value.json.as_ref().map_or(0, Vec::len) + 512)
                    .sum::<usize>()
                    + bytes
                    > MATERIAL_LIMIT
            {
                cache.pop_front();
            }
            cache.push_back((identity, Instant::now(), material.clone()));
        }
        Ok(material)
    }
}

async fn validate(pool: &PgPool, node: &NodeState, key: &Key, run: Run) -> Result<()> {
    ensure!(
        matches!(node.role.as_str(), "data" | "node"),
        Error::NodeAuthentication
    );
    let mut tx = pool.begin().await?;
    transactions::authenticate_rpc_node(&mut tx, node, key, false).await?;
    let state = transactions::require_node_run(&mut tx, node, run, false).await?;
    ensure!(
        !state.draining && queries::node_read_admission().fetch_one(&mut *tx).await?,
        Error::NodeInactive
    );
    tx.commit().await?;
    Ok(())
}

pub(super) async fn request(
    pool: &PgPool,
    queue: Arc<Queue>,
    node: &NodeState,
    key: &Key,
    input: refresh::Request,
) -> Result<refresh::Reply> {
    ensure!(
        !input.reads.is_empty()
            && input.reads.len() <= refresh::LIMIT
            && input.cached.len() <= read::MAX_CACHED,
        Error::NodeReadLimit
    );
    ensure!(
        input.reads.iter().all(|q| q.access.is_none()
            && !q.protection
            && !q.mapping
            && !q.head
            && q.headers.is_empty()
            && q.overrides.is_empty()),
        Error::NodeAuthentication
    );
    validate(pool, node, key, input.run).await?;
    let run = input.run;
    let result = enqueue(pool, &queue, node, key, input)?;
    let reply = result.await.context("refresh group stopped")??;
    validate(pool, node, key, run).await?;
    Ok(reply)
}

pub(super) fn enqueue(
    pool: &PgPool,
    queue: &Arc<Queue>,
    node: &NodeState,
    key: &Key,
    input: refresh::Request,
) -> Result<oneshot::Receiver<Result<refresh::Reply>>> {
    let (done, result) = oneshot::channel();
    {
        let mut waiting = queue.waiting.lock().unwrap();
        ensure!(
            waiting.items + input.reads.len() <= GROUP_LIMIT,
            Error::NodeReadBusy
        );
        if waiting.requests.is_empty() {
            let permit = queue
                .slots
                .clone()
                .try_acquire_owned()
                .map_err(|_| Error::NodeReadBusy)?;
            let worker = queue.clone();
            let pool = pool.clone();
            let key = key.clone();
            tokio::spawn(async move {
                let _permit = permit;
                tokio::time::sleep(Duration::from_millis(5)).await;
                // Closing the group before reading prevents late callers joining an old snapshot.
                let mut pending = std::mem::take(&mut *worker.waiting.lock().unwrap()).requests;
                pending.retain(|request| !request.done.is_closed());
                if pending.is_empty() {
                    return;
                }
                let replies = tokio::time::timeout(
                    Duration::from_secs(10),
                    load(&pool, &worker, &key, &pending),
                )
                .await
                .unwrap_or_else(|_| Err(Error::NodeReadBusy.into()));
                match replies {
                    Ok(replies) => {
                        for (request, reply) in pending.into_iter().zip(replies) {
                            let _ = request.done.send(Ok(reply));
                        }
                    }
                    Err(error) => {
                        for request in pending {
                            let failure = model::error(&error)
                                .map(anyhow::Error::from)
                                .unwrap_or_else(|| {
                                    anyhow::anyhow!("refresh query failed: {error}")
                                });
                            let _ = request.done.send(Err(failure));
                        }
                    }
                }
            });
        }
        waiting.items += input.reads.len();
        waiting.requests.push(Pending {
            node: node.clone(),
            input,
            done,
        });
    }
    Ok(result)
}

async fn load(
    pool: &PgPool,
    queue: &Queue,
    key: &Key,
    pending: &[Pending],
) -> Result<Vec<refresh::Reply>> {
    let deployment = pending[0].node.deployment_id;
    ensure!(
        pending.iter().all(|p| p.node.deployment_id == deployment),
        Error::NodeAuthentication
    );
    let mut routes = Vec::new();
    for query in pending.iter().flat_map(|p| &p.input.reads) {
        if !routes.contains(&query.route) {
            routes.push(query.route.clone());
        }
    }
    queue.groups.fetch_add(1, Relaxed);
    queue.requests.fetch_add(pending.len() as u64, Relaxed);
    queue.items.fetch_add(
        pending.iter().map(|p| p.input.reads.len() as u64).sum(),
        Relaxed,
    );
    queue.routes.fetch_add(routes.len() as u64, Relaxed);
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *tx)
        .await?;
    let mut buckets = HashMap::new();
    for row in sql::buckets(&routes).fetch_all(&mut *tx).await? {
        let route = &routes[row.ordinal as usize];
        // Bucket aliases take precedence, including when both names exist.
        if buckets.contains_key(route)
            && !matches!(route, ReadRoute::Bucket { alias:Some((name,_)), .. } if name == &row.bucket.name)
        {
            continue;
        }
        buckets.insert(route.clone(), row.bucket);
    }
    let wanted: BTreeSet<_> = buckets
        .iter()
        .filter(|(_, b)| b.state == "active")
        .map(|(r, b)| (b.id, r.key(b).to_owned()))
        .collect();
    let (ids, keys): (Vec<_>, Vec<_>) = wanted.into_iter().unzip();
    let streams: HashMap<_, _> = sql::streams(&ids, &keys)
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .map(|s| ((s.bucket_id, s.object_key.clone()), s))
        .collect();
    let mut raw = BTreeSet::new();
    let mut chunks = BTreeSet::new();
    for query in pending
        .iter()
        .flat_map(|p| &p.input.reads)
        .filter(|q| q.body.is_some())
    {
        if let Some(stream) = stream(&buckets, &streams, &query.route).filter(|s| s.public_read) {
            match stream.payload_type.as_str() {
                "raw" => {
                    raw.insert(stream.id);
                }
                "chunks" => {
                    chunks.insert(stream.id);
                }
                _ => {}
            }
        }
    }
    let raw: Vec<_> = raw.into_iter().collect();
    let chunks: Vec<_> = chunks.into_iter().collect();
    // One extra segment detects incomplete mappings without multiplying per-read limits.
    let limit = (8192 / (raw.len() + chunks.len()).max(1)).min(read::MAX_ITEMS) as i64 + 1;
    let raw = if raw.is_empty() {
        Vec::new()
    } else {
        sql::raw(&raw, limit)
            .fetch_all(&mut *tx)
            .await?
            .into_iter()
            .map(|row| (row.stream_id, row.source))
            .collect()
    };
    let extents = if chunks.is_empty() {
        Vec::new()
    } else {
        sql::extents(&chunks, limit).fetch_all(&mut *tx).await?
    };
    let sources = Sources::load(&mut tx, raw, extents).await?;
    let mut mappings = HashMap::new();
    let mut stores: BTreeSet<_> = buckets.values().filter_map(|b| b.storage_id).collect();
    for request in pending {
        for query in &request.input.reads {
            let Some(stream) = stream(&buckets, &streams, &query.route)
                .filter(|s| s.public_read && query.body.is_some())
            else {
                continue;
            };
            let id = (request.node.node_id, stream.id);
            if mappings.contains_key(&id) {
                continue;
            }
            let mapping = sources
                .mapping(request.node.node_id, stream, 0, stream.size)
                .ok()
                .filter(|m| m.end == stream.size && m.segments.len() <= read::MAX_ITEMS);
            if let Some(mapping) = &mapping {
                for segment in &mapping.segments {
                    if let Some(id) = segment.source.storage()? {
                        stores.insert(id);
                    }
                }
            }
            mappings.insert(id, mapping);
        }
    }
    let stores: Vec<_> = stores.into_iter().collect();
    let profiles: HashMap<_, _> = queries::node_read_profiles(&stores)
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .map(|p| (p.storage_id, p))
        .collect();
    let mut storage_materials = HashMap::new();
    for record in queries::node_read_storages(&stores)
        .fetch_all(&mut *tx)
        .await?
    {
        if let Some(profile) = profiles.get(&record.id) {
            storage_materials.insert(
                record.id,
                queue.storage(deployment, key, record, profile.clone())?,
            );
        }
    }
    let mut results = Vec::new();
    for request in pending {
        let mut response = refresh::Reply {
            reads: Vec::new(),
            materials: Vec::new(),
        };
        let mut seen = HashSet::new();
        for query in &request.input.reads {
            let mut reply = read::Reply::new(read::Outcome::Ready);
            if let Some(bucket) = buckets.get(&query.route) {
                reply.bucket = Some(reply.material(
                    deployment,
                    read::MaterialType::Bucket,
                    bucket.id.to_string(),
                    bucket,
                    &request.input.cached,
                )?);
                if let Some(profile) = bucket.storage_id.and_then(|id| profiles.get(&id)) {
                    reply.policy = Some(reply.material(
                        deployment,
                        read::MaterialType::Policy,
                        profile.storage_id.to_string(),
                        &read::Policy::try_from(profile)?,
                        &request.input.cached,
                    )?);
                }
                if bucket.state != "active" {
                    reply.outcome = read::Outcome::AccessDenied;
                } else if let Some(stream) = stream(&buckets, &streams, &query.route) {
                    if !stream.public_read {
                        reply.outcome = read::Outcome::AccessDenied;
                    } else {
                        let object = read::Object::from_stream(stream)?;
                        reply.object = Some(reply.material(
                            deployment,
                            read::MaterialType::Object,
                            object.id.to_string(),
                            &object,
                            &request.input.cached,
                        )?);
                        query.select(stream, &mut reply)?;
                        if reply.body_reused {
                            let body = query.body.as_ref().unwrap();
                            reply.body_reused = false;
                            if let Some(Some(mapping)) =
                                mappings.get(&(request.node.node_id, stream.id))
                            {
                                let json = serde_json::to_vec(mapping)?;
                                let version = read::Version::new(
                                    deployment,
                                    read::MaterialType::Mapping,
                                    format!(
                                        "{}:{}:{}:{}",
                                        mapping.stream,
                                        mapping.revision,
                                        mapping.start,
                                        mapping.end
                                    ),
                                    &json,
                                );
                                let ids: BTreeSet<_> = mapping
                                    .segments
                                    .iter()
                                    .filter_map(|s| s.source.storage().ok().flatten())
                                    .collect();
                                if version == body.mapping
                                    && ids.len() == body.storages.len()
                                    && ids.iter().all(|id| {
                                        storage_materials
                                            .get(id)
                                            .is_some_and(|m| body.storages.contains(&m.version))
                                            && (body.verified
                                                || profiles
                                                    .get(id)
                                                    .is_some_and(|p| p.read_integrity != "inline"))
                                    })
                                {
                                    reply.body_reused = true;
                                    for id in ids {
                                        let mut material = storage_materials[&id].clone();
                                        if request.input.cached.contains(&material.version) {
                                            material.json = None;
                                        }
                                        reply.materials.push(material);
                                    }
                                }
                            }
                        }
                    }
                } else {
                    reply.outcome = read::Outcome::NoSuchKey;
                }
            } else {
                reply.outcome = read::Outcome::NoSuchBucket;
            }
            for material in std::mem::take(&mut reply.materials) {
                if seen.insert(material.version.clone()) {
                    response.materials.push(material);
                }
            }
            response.reads.push(reply);
        }
        ensure!(
            response
                .materials
                .iter()
                .map(|m| m.json.as_ref().map_or(0, Vec::len) + m.version.id.len() * 2 + 128)
                .sum::<usize>()
                + response.reads.len() * 4096
                <= read::TARGET_BYTES,
            Error::NodeReadLimit
        );
        results.push(response);
    }
    tx.commit().await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("refresh-snapshot").await;
    Ok(results)
}

fn stream<'a>(
    buckets: &HashMap<ReadRoute, Bucket>,
    streams: &'a HashMap<(Uuid, String), StoredStream>,
    route: &ReadRoute,
) -> Option<&'a StoredStream> {
    let bucket = buckets.get(route)?;
    streams.get(&(bucket.id, route.key(bucket).to_owned()))
}
