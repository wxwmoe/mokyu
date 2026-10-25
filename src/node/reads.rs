use super::{NodeContext, RunLease, resources::Grant};
use crate::{model::node_read as read, rpc::wire};
use anyhow::{Context, Result, ensure};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};
use uuid::Uuid;

#[path = "public.rs"]
mod public;

const UNIT: usize = 4096;
const MATERIAL_BYTES: usize = 1024 * 1024;
type Key = (i32, String);
pub(super) type Held = HashMap<Key, Arc<Material>>;

pub(super) struct Material {
    version: wire::Cached,
    json: Vec<u8>,
    referenced: AtomicBool,
    _memory: Option<OwnedSemaphorePermit>,
    backend: Mutex<Option<Arc<super::body::Backend>>>,
    policy: Option<read::Policy>,
}

#[derive(Default)]
struct Index {
    values: Held,
    order: VecDeque<Key>,
}

pub(super) struct Cache {
    index: Mutex<Index>,
    memory: Arc<Semaphore>,
    public: Weak<crate::app::public_reads::Cache>,
}

impl Default for Cache {
    fn default() -> Self {
        Self {
            index: Mutex::new(Index::default()),
            memory: Arc::new(Semaphore::new(MATERIAL_BYTES / UNIT)),
            public: Weak::new(),
        }
    }
}

impl Cache {
    pub(super) fn snapshot(&self) -> Held {
        self.index.lock().unwrap().values.clone()
    }

    fn policy(&self, id: Uuid) -> Option<read::Policy> {
        let index = self.index.lock().unwrap();
        [wire::MaterialType::Storage, wire::MaterialType::ReadPolicy]
            .iter()
            .filter_map(|r#type| index.values.get(&(*r#type as i32, id.to_string())))
            .filter_map(|value| value.policy.as_ref())
            .max_by_key(|policy| policy.revision)
            .cloned()
    }

    fn admit(&self, version: wire::Cached, json: Vec<u8>) -> Result<Arc<Material>> {
        let key = (version.r#type, version.id.clone());
        let units = (json.capacity() + version.id.capacity() + 2048).div_ceil(UNIT);
        let mut index = self.index.lock().unwrap();
        if let Some(value) = index
            .values
            .get(&key)
            .filter(|value| value.version == version)
        {
            value.referenced.store(true, Ordering::Relaxed);
            return Ok(value.clone());
        }
        let policy = match wire::MaterialType::try_from(version.r#type) {
            Ok(wire::MaterialType::ReadPolicy) => {
                Some(serde_json::from_slice::<read::Policy>(&json)?)
            }
            Ok(wire::MaterialType::Storage) => Some(read::Policy::try_from(
                &serde_json::from_slice::<read::Storage>(&json)?.profile,
            )?),
            _ => None,
        };
        let mut stale = false;
        if let Some(policy) = &policy {
            ensure!(
                policy.storage_id.to_string() == version.id
                    && policy.revision > 0
                    && (1..=60).contains(&policy.refresh_seconds)
                    && (policy.refresh_seconds..=300).contains(&policy.max_age_seconds)
                    && matches!(
                        policy.read_integrity.as_str(),
                        "off" | "background" | "inline"
                    ),
                "invalid read policy"
            );
            let previous = index
                .values
                .values()
                .filter_map(|value| value.policy.as_ref())
                .filter(|value| value.storage_id == policy.storage_id)
                .map(|value| value.revision)
                .max();
            stale = previous.is_some_and(|revision| revision > policy.revision);
            if !stale
                && previous != Some(policy.revision)
                && let Some(public) = self.public.upgrade()
            {
                public.profile_changed(policy.storage_id);
            }
        }
        let mut memory = None;
        if !stale && units <= MATERIAL_BYTES / UNIT {
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
            policy,
        });
        if entry._memory.is_some() {
            if !index.values.contains_key(&key) {
                index.order.push_back(key.clone());
            }
            index.values.insert(key, entry.clone());
        }
        Ok(entry)
    }

    pub(super) fn apply(
        &self,
        deployment: Uuid,
        declared: Held,
        values: Vec<wire::Material>,
    ) -> Result<Held> {
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
        parsed
            .into_iter()
            .map(|(key, version, json, held)| {
                Ok((
                    key,
                    match held {
                        Some(value) => value,
                        None => self.admit(version, json.unwrap())?,
                    },
                ))
            })
            .collect()
    }
}

pub(super) fn cached(held: &Held) -> Vec<wire::Cached> {
    held.values()
        .map(|material| material.version.clone())
        .collect()
}

pub(super) fn material<T: serde::de::DeserializeOwned>(
    held: &Held,
    version: &wire::Cached,
) -> Result<T> {
    let value = held
        .get(&(version.r#type, version.id.clone()))
        .filter(|value| value.version == *version)
        .context("required read material missing")?;
    Ok(serde_json::from_slice(&value.json)?)
}

pub(crate) struct Reads {
    pub(crate) node: Arc<NodeContext>,
    pub(super) cache: Cache,
    slots: Arc<Semaphore>,
    pub(super) reader: super::body::Reader,
    pub(super) checks: Arc<crate::content::checks::Checks>,
    pub(super) bodies: Arc<crate::app::responses::Cache>,
    pub(crate) cors: Arc<crate::http::cors::Cache>,
    pub(super) public: Arc<crate::app::public_reads::Cache>,
    public_epoch: Mutex<u64>,
    pub(super) public_receiver:
        Mutex<Option<tokio::sync::mpsc::Receiver<crate::app::public_reads::Work>>>,
}

pub(crate) struct Prepared {
    pub(crate) reply: wire::ReadReply,
    pub(crate) run: Arc<RunLease>,
    materials: Held,
    pub(super) grants: Vec<Arc<Grant>>,
    pub(super) memory: Arc<OwnedSemaphorePermit>,
    pub(super) buffers: Arc<Semaphore>,
    limits: (u32, u32),
    pub(super) cached_body: Option<Arc<crate::app::responses::Entry>>,
    pub(super) route: [u8; 32],
    pub(super) capture: Option<Arc<Mutex<crate::app::responses::Mapping>>>,
    pub(super) public_generation: Option<u64>,
    pub(super) started: std::time::Instant,
    pub(super) verify: bool,
}

impl Prepared {
    pub(super) fn storage_versions(&self) -> Vec<wire::Cached> {
        self.materials
            .values()
            .filter(|value| value.version.r#type == wire::MaterialType::Storage as i32)
            .map(|value| value.version.clone())
            .collect()
    }

    pub(super) fn record_check(&self, evidence: Arc<crate::content::checks::Evidence>) {
        if let Some(capture) = &self.capture {
            capture.lock().unwrap().record_check(evidence);
        }
    }

    pub(super) fn finish_source(&self) {
        if let Some(capture) = &self.capture {
            capture.lock().unwrap().finish_source();
        }
    }

    pub(super) fn backend(&self, id: Uuid) -> Result<Arc<super::body::Backend>> {
        backend(&self.materials, id)
    }

    pub(super) fn keys(
        &self,
    ) -> Result<std::collections::BTreeMap<Uuid, crate::content::codec::Key>> {
        keys(&self.materials)
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
        let checks = crate::content::checks::Checks::new(reader.cpu.clone());
        let bodies = crate::app::responses::Cache::new(budget.response_cache_bytes);
        let settings = node.snapshot()?.runtime().requests.clone();
        let (cors, receiver) = crate::http::cors::Cache::new(
            crate::config::cache_bytes(&settings.cors_cache_size)? as usize,
            settings.cors_cache_entries,
        );
        tokio::spawn(crate::http::cors::run_current(
            Arc::downgrade(&cors),
            receiver,
        ));
        let (public, receiver) = crate::app::public_reads::Cache::new(cors.clone(), &bodies);
        Ok(Self {
            node,
            cache: Cache {
                public: Arc::downgrade(&public),
                ..Cache::default()
            },
            slots,
            reader,
            checks,
            bodies,
            cors,
            public,
            public_epoch: Mutex::new(0),
            public_receiver: Mutex::new(Some(receiver)),
        })
    }

    pub(crate) async fn open(&self, mut input: wire::ReadRequest) -> Result<Prepared> {
        let memory = Arc::new(self.slots.clone().acquire_owned().await?);
        let snapshot = self.node.snapshot()?;
        let generation = self.public_generation();
        ensure!(
            input.cached.is_empty() && input.body.is_none(),
            "read material claims must be pinned by the caller"
        );
        let declared = self.cache.snapshot();
        let route = super::cache::route(&input);
        let candidate = self.candidate(&input);
        input.body = candidate
            .as_ref()
            .and_then(|(_, entry)| super::cache::claim(entry));
        input.run = Some(snapshot.lease.run.clone());
        if input.operation_id.is_empty() {
            input.operation_id = Uuid::new_v4().to_string();
        }
        input.cached = cached(&declared);
        let public_route = crate::rpc::read::query(input.clone())?.route;
        let limits = (input.max_items, input.max_bytes);
        let started = Instant::now();
        let mut reply = self.node.client.read(input).await?;
        let grants = self
            .node
            .resources
            .adopt(&snapshot.lease, &reply.grants, started)?;
        let (materials, public_generation) = {
            let _epoch = self.public_epoch.lock().unwrap();
            let current = self.public.generation() == generation;
            let materials = self.cache.apply(
                self.node.client.identity.deployment_id,
                declared,
                std::mem::take(&mut reply.materials),
            )?;
            self.observe_public(&public_route, &reply)?;
            (materials, current.then(|| self.public.generation()))
        };
        let cached_body = if reply.body_reused {
            Some(candidate.context("control reused an undeclared body")?.1)
        } else {
            if reply.outcome == wire::ReadOutcome::Ready as i32
                && let Some((key, entry)) = candidate
            {
                self.bodies.remove(key, &entry);
            }
            None
        };
        let prepared = Prepared {
            reply,
            run: snapshot.lease.clone(),
            materials,
            grants,
            memory,
            buffers: Arc::new(Semaphore::new(super::body::BUFFER_UNITS)),
            limits,
            cached_body,
            route,
            capture: None,
            public_generation,
            started: started.into_std(),
            verify: false,
        };
        prepared.check()?;
        Ok(prepared)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn internal(
        &self,
        run: Arc<RunLease>,
        memory: Arc<OwnedSemaphorePermit>,
        mut reply: wire::ReadReply,
        declared: Held,
        started: Instant,
        buffers: Arc<Semaphore>,
    ) -> Result<Prepared> {
        let grants = self.node.resources.adopt(&run, &reply.grants, started)?;
        let materials = {
            let _epoch = self.public_epoch.lock().unwrap();
            self.cache.apply(
                self.node.client.identity.deployment_id,
                declared,
                std::mem::take(&mut reply.materials),
            )?
        };
        let prepared = Prepared {
            reply,
            run,
            materials,
            grants,
            memory,
            buffers,
            limits: (256, read::TARGET_BYTES as u32),
            cached_body: None,
            route: [0; 32],
            capture: None,
            public_generation: None,
            started: started.into_std(),
            verify: true,
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
                let mut materials = {
                    let _epoch = self.public_epoch.lock().unwrap();
                    self.cache.apply(
                        self.node.client.identity.deployment_id,
                        declared,
                        std::mem::take(&mut reply.materials),
                    )?
                };
                if let Some(version) = &previous.reply.object {
                    let key = (version.r#type, version.id.clone());
                    materials.insert(
                        key.clone(),
                        previous
                            .materials
                            .get(&key)
                            .context("read object missing")?
                            .clone(),
                    );
                    reply.object = Some(version.clone());
                }
                let prepared = Prepared {
                    reply,
                    run: previous.run.clone(),
                    materials,
                    grants,
                    memory: previous.memory.clone(),
                    buffers: previous.buffers.clone(),
                    limits: previous.limits,
                    cached_body: None,
                    route: previous.route,
                    capture: previous.capture.clone(),
                    public_generation: None,
                    started: started.into_std(),
                    verify: previous.verify,
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

    pub(super) async fn copy_body(
        self: &Arc<Self>,
        mut source: Prepared,
    ) -> Result<futures_util::stream::BoxStream<'static, Result<bytes::Bytes>>> {
        let object: read::Object = source.material(
            source
                .reply
                .object
                .as_ref()
                .context("copy object missing")?,
        )?;
        ensure!(object.size >= 0, "invalid copy size");
        if object.size == 0 {
            return Ok(Box::pin(futures_util::stream::empty()));
        }
        let lease = source
            .grants
            .iter()
            .find(|grant| grant.reference.r#type == wire::GrantType::ReadProtection as i32)
            .context("copy protection missing")?
            .reference
            .id
            .clone();
        source.verify = true;
        let mut window = self
            .window(
                Arc::new(source),
                wire::ReadCursor {
                    lease_id: lease,
                    stream_id: object.id.to_string(),
                    payload_revision: object.revision as u64,
                    offset: 0,
                    end: object.size as u64,
                },
                None,
            )
            .await?;
        window.reply.length = object.size as u64;
        Ok(self.body(window))
    }

    #[cfg(test)]
    pub(crate) fn backend_gets(&self) -> u64 {
        self.reader.backend_gets()
    }

    #[cfg(test)]
    pub(crate) fn read_checks(&self) -> serde_json::Value {
        self.checks.snapshot()
    }

    #[cfg(test)]
    pub(crate) fn body_cache(&self) -> serde_json::Value {
        self.bodies.snapshot()
    }

    #[cfg(test)]
    pub(crate) async fn file_cache(&self) -> serde_json::Value {
        self.reader.files.snapshot().await
    }
}

#[cfg(test)]
#[path = "../../tests/rpc/materials.rs"]
mod tests;

pub(super) fn backend(held: &Held, id: Uuid) -> Result<Arc<super::body::Backend>> {
    let material = held
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
    held: &Held,
) -> Result<std::collections::BTreeMap<Uuid, crate::content::codec::Key>> {
    held.iter()
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
