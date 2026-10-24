use super::{NodeContext, RunLease, resources::Grant};
use crate::{model::node_read as read, rpc::wire};
use anyhow::{Context, Result, ensure};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};
use uuid::Uuid;

const UNIT: usize = 4096;
const MATERIAL_BYTES: usize = 1024 * 1024;
type Key = (i32, String);
type Held = HashMap<Key, Arc<Material>>;

struct Material {
    version: wire::Cached,
    json: Vec<u8>,
    referenced: AtomicBool,
    _memory: Option<OwnedSemaphorePermit>,
    backend: Mutex<Option<Arc<super::body::Backend>>>,
}

#[derive(Default)]
struct Index {
    values: Held,
    order: VecDeque<Key>,
}

struct Cache {
    index: Mutex<Index>,
    memory: Arc<Semaphore>,
}

impl Default for Cache {
    fn default() -> Self {
        Self {
            index: Mutex::new(Index::default()),
            memory: Arc::new(Semaphore::new(MATERIAL_BYTES / UNIT)),
        }
    }
}

impl Cache {
    fn snapshot(&self) -> Held {
        self.index.lock().unwrap().values.clone()
    }

    fn admit(&self, version: wire::Cached, json: Vec<u8>) -> Arc<Material> {
        let key = (version.r#type, version.id.clone());
        let units = (json.capacity() + version.id.capacity() + 2048).div_ceil(UNIT);
        let mut index = self.index.lock().unwrap();
        if let Some(value) = index
            .values
            .get(&key)
            .filter(|value| value.version == version)
        {
            value.referenced.store(true, Ordering::Relaxed);
            return value.clone();
        }
        let mut memory = None;
        if units <= MATERIAL_BYTES / UNIT {
            // Bound optional eviction work even when every entry is hot or held by a request.
            for _ in 0..16 {
                if index.values.len() < read::MAX_CACHED {
                    if let Ok(permit) = self.memory.clone().try_acquire_many_owned(units as u32) {
                        memory = Some(permit);
                        break;
                    }
                }
                let Some(old) = index.order.pop_front() else {
                    break;
                };
                if index
                    .values
                    .get(&old)
                    .is_some_and(|entry| entry.referenced.swap(false, Ordering::Relaxed))
                {
                    index.order.push_back(old);
                } else {
                    index.values.remove(&old);
                }
            }
        }
        let entry = Arc::new(Material {
            version,
            json,
            referenced: AtomicBool::new(true),
            _memory: memory,
            backend: Mutex::new(None),
        });
        if entry._memory.is_some() {
            if !index.values.contains_key(&key) {
                index.order.push_back(key.clone());
            }
            index.values.insert(key, entry.clone());
        }
        entry
    }

    fn apply(&self, deployment: Uuid, declared: Held, values: Vec<wire::Material>) -> Result<Held> {
        ensure!(
            values.len() <= read::MAX_ITEMS + 32,
            "too many read materials"
        );
        let mut seen = HashSet::new();
        let mut parsed = Vec::with_capacity(values.len());
        for value in values {
            let version = value.version.context("read material version missing")?;
            let parsed_version = crate::rpc::read::cached(vec![version.clone()])?.remove(0);
            let key = (version.r#type, version.id.clone());
            ensure!(seen.insert(key.clone()), "duplicate read material");
            match value.value.context("read material value missing")? {
                wire::material::Value::Reuse(true) => {
                    let held = declared
                        .get(&key)
                        .filter(|held| held.version == version)
                        .context("control reused an undeclared read material")?;
                    held.referenced.store(true, Ordering::Relaxed);
                    parsed.push((key, version, None, Some(held.clone())));
                }
                wire::material::Value::Json(json) => {
                    ensure!(
                        parsed_version
                            == read::Version::new(
                                deployment,
                                parsed_version.r#type,
                                parsed_version.id.clone(),
                                &json
                            ),
                        "read material revision mismatch"
                    );
                    parsed.push((key, version, Some(json), None));
                }
                _ => anyhow::bail!("invalid read material reuse marker"),
            }
        }
        Ok(parsed
            .into_iter()
            .map(|(key, version, json, held)| {
                (
                    key,
                    held.unwrap_or_else(|| self.admit(version, json.unwrap())),
                )
            })
            .collect())
    }
}

fn cached(held: &Held) -> Vec<wire::Cached> {
    held.values()
        .map(|material| material.version.clone())
        .collect()
}

fn material<T: serde::de::DeserializeOwned>(held: &Held, version: &wire::Cached) -> Result<T> {
    let value = held
        .get(&(version.r#type, version.id.clone()))
        .filter(|value| value.version == *version)
        .context("required read material missing")?;
    Ok(serde_json::from_slice(&value.json)?)
}

pub(crate) struct Reads {
    pub(crate) node: Arc<NodeContext>,
    cache: Cache,
    slots: Arc<Semaphore>,
    pub(super) reader: super::body::Reader,
}

pub(crate) struct Prepared {
    pub(crate) reply: wire::ReadReply,
    pub(crate) run: Arc<RunLease>,
    materials: Held,
    pub(super) grants: Vec<Arc<Grant>>,
    pub(super) memory: Arc<OwnedSemaphorePermit>,
    pub(super) buffers: Arc<Semaphore>,
    limits: (u32, u32),
}

impl Prepared {
    pub(super) fn backend(&self, id: Uuid) -> Result<Arc<super::body::Backend>> {
        let material = self
            .materials
            .get(&(wire::MaterialType::Storage as i32, id.to_string()))
            .context("read storage missing")?;
        let mut backend = material.backend.lock().unwrap();
        if backend.is_none() {
            let value: read::Storage = serde_json::from_slice(&material.json)?;
            ensure!(
                value.connection.id == id && value.profile.storage_id == id,
                "storage identity mismatch"
            );
            *backend = Some(Arc::new(super::body::Backend::new(value)?));
        }
        Ok(backend.as_ref().unwrap().clone())
    }

    pub(super) fn keys(
        &self,
    ) -> Result<std::collections::BTreeMap<Uuid, crate::content::codec::Key>> {
        self.materials
            .iter()
            .filter(|((r#type, _), _)| *r#type == wire::MaterialType::BlockKey as i32)
            .map(|((_, id), material)| {
                let key: read::BlockKey = serde_json::from_slice(&material.json)?;
                ensure!(id == &key.id.to_string(), "block key identity mismatch");
                Ok((
                    key.id,
                    crate::content::codec::Key::new(&key.algorithm, key.material)?,
                ))
            })
            .collect()
    }

    pub(super) async fn guard<T>(
        &self,
        node: &NodeContext,
        future: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        let mut events = node.subscribe();
        tokio::pin!(future);
        loop {
            self.check()?;
            let deadline = self
                .grants
                .iter()
                .map(|grant| grant.deadline())
                .fold(self.run.deadline(), std::cmp::min);
            tokio::select! {
                biased;
                _ = events.changed() => {},
                _ = tokio::time::sleep_until(deadline) => {},
                result = &mut future => { self.check()?; return result; }
            }
        }
    }

    pub(crate) fn material<T: serde::de::DeserializeOwned>(
        &self,
        version: &wire::Cached,
    ) -> Result<T> {
        material(&self.materials, version)
    }

    pub(crate) fn check(&self) -> Result<()> {
        self.run.check()?;
        for grant in &self.grants {
            grant.check()?;
        }
        Ok(())
    }
}

impl Reads {
    pub(crate) fn new(node: Arc<NodeContext>) -> Result<Self> {
        let budget = node
            .budget
            .get()
            .context("node has not synchronized its budget")?;
        let slots = Arc::new(Semaphore::new(budget.read_concurrency));
        let reader = super::body::Reader::new(&node)?;
        Ok(Self {
            node,
            cache: Cache::default(),
            slots,
            reader,
        })
    }

    pub(crate) async fn open(&self, mut input: wire::ReadRequest) -> Result<Prepared> {
        let memory = Arc::new(self.slots.clone().acquire_owned().await?);
        let snapshot = self.node.snapshot()?;
        ensure!(
            input.cached.is_empty(),
            "read material claims must be pinned by the caller"
        );
        let declared = self.cache.snapshot();
        input.run = Some(snapshot.lease.run.clone());
        if input.operation_id.is_empty() {
            input.operation_id = Uuid::new_v4().to_string();
        }
        input.cached = cached(&declared);
        let limits = (input.max_items, input.max_bytes);
        let started = Instant::now();
        let mut reply = self.node.client.read(input).await?;
        let grants = self
            .node
            .resources
            .adopt(&snapshot.lease, &reply.grants, started)?;
        let materials = self.cache.apply(
            self.node.client.identity.deployment_id,
            declared,
            std::mem::take(&mut reply.materials),
        )?;
        let prepared = Prepared {
            reply,
            run: snapshot.lease.clone(),
            materials,
            grants,
            memory,
            buffers: Arc::new(Semaphore::new(super::body::BUFFER_UNITS)),
            limits,
        };
        prepared.check()?;
        Ok(prepared)
    }

    pub(super) async fn window(
        &self,
        previous: Arc<Prepared>,
        cursor: wire::ReadCursor,
        retired: Option<Uuid>,
    ) -> Result<Prepared> {
        previous
            .guard(&self.node, async {
                let mut events = self.node.subscribe();
                while retired.is_some_and(|id| !self.node.resources.released(id)) {
                    events.changed().await?;
                }
                let declared = self.cache.snapshot();
                let started = Instant::now();
                let mut reply = self
                    .node
                    .client
                    .read_window(wire::ReadWindowRequest {
                        run: Some(previous.run.run.clone()),
                        cursor: Some(cursor),
                        cached: cached(&declared),
                        max_items: previous.limits.0,
                        max_bytes: previous.limits.1,
                    })
                    .await?;
                let mut grants =
                    self.node
                        .resources
                        .adopt(&previous.run, &reply.grants, started)?;
                grants.extend(
                    previous
                        .grants
                        .iter()
                        .filter(|grant| {
                            grant.reference.r#type == wire::GrantType::ReadProtection as i32
                        })
                        .cloned(),
                );
                let materials = self.cache.apply(
                    self.node.client.identity.deployment_id,
                    declared,
                    std::mem::take(&mut reply.materials),
                )?;
                let prepared = Prepared {
                    reply,
                    run: previous.run.clone(),
                    materials,
                    grants,
                    memory: previous.memory.clone(),
                    buffers: previous.buffers.clone(),
                    limits: previous.limits,
                };
                prepared.check()?;
                Ok(prepared)
            })
            .await
    }

    pub(crate) fn body(
        self: &Arc<Self>,
        prepared: Prepared,
    ) -> futures_util::stream::BoxStream<'static, Result<bytes::Bytes>> {
        super::body::stream(self.clone(), prepared)
    }
}

#[cfg(test)]
#[path = "../../tests/rpc/materials.rs"]
mod tests;
