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
const DECLARATION_BYTES: usize = 8 * 1024;
type Key = (i32, String);
pub(super) type Held = HashMap<Key, Arc<Material>>;

pub(super) struct Material {
    version: wire::Cached,
    json: Vec<u8>,
    referenced: AtomicBool,
    _memory: Option<OwnedSemaphorePermit>,
    backend: Mutex<Option<Arc<super::body::Backend>>>,
    io: Arc<super::io::Io>,
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
    pub(super) io: Arc<super::io::Io>,
    epoch: Arc<std::sync::atomic::AtomicU64>,
}

impl Default for Cache {
    fn default() -> Self {
        Self {
            index: Mutex::new(Index::default()),
            memory: Arc::new(Semaphore::new(MATERIAL_BYTES / UNIT)),
            public: Weak::new(),
            io: Arc::default(),
            epoch: Arc::default(),
        }
    }
}

impl Cache {
    pub(super) fn for_node(node: &NodeContext) -> Self {
        Self {
            io: node.io.clone(),
            epoch: node.cache_epoch.clone(),
            ..Self::default()
        }
    }

    pub(super) fn snapshot(&self) -> Held {
        let index = self.index.lock().unwrap();
        let mut held = Held::new();
        let mut remaining = DECLARATION_BYTES;
        // Keep optional hints small, prioritizing shared dependencies over object history.
        for objects in [false, true] {
            for (key, material) in &index.values {
                if matches!(
                    wire::MaterialType::try_from(material.version.r#type),
                    Ok(wire::MaterialType::ObjectMetadata | wire::MaterialType::ObjectMapping)
                ) != objects
                {
                    continue;
                }
                let bytes = prost::Message::encoded_len(&material.version) + 6;
                if bytes <= remaining {
                    held.insert(key.clone(), material.clone());
                    remaining -= bytes;
                }
            }
        }
        held
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
            io: self.io.clone(),
            policy,
        });
        if !stale && entry.policy.is_some() {
            // Discard optional fills prepared with an earlier storage configuration.
            self.epoch.fetch_add(1, Ordering::AcqRel);
        }
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
            let binary = parsed_version.r#type == read::MaterialType::RawPages;
            ensure!(
                match &value.value {
                    Some(wire::material::Value::Binary(data)) =>
                        binary
                            && !data.is_empty()
                            && data.len().is_multiple_of(crate::content::raw_pages::HASH),
                    Some(wire::material::Value::Json(_)) => !binary,
                    _ => true,
                },
                "invalid read material encoding"
            );
            match value.value.context("read material value missing")? {
                wire::material::Value::Reuse(true) => {
                    let held = declared
                        .get(&key)
                        .filter(|held| held.version == version)
                        .context("control reused an undeclared read material")?;
                    held.referenced.store(true, Ordering::Relaxed);
                    parsed.push((key, version, None, Some(held.clone())));
                }
                wire::material::Value::Json(json) | wire::material::Value::Binary(json) => {
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
    pub(crate) images: Arc<crate::processing::image::job::Service>,
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
    pub(super) recovery: Arc<crate::storage::read_retry::Recovery>,
    pub(super) spare: Option<Arc<crate::storage::read_buffer::Spares>>,
    limits: (u32, u32),
    pub(super) cached_body: Option<Arc<crate::app::responses::Entry>>,
    pub(super) route: [u8; 32],
    pub(super) capture: Option<Arc<Mutex<crate::app::responses::Mapping>>>,
    pub(super) public_generation: Option<u64>,
    pub(super) started: std::time::Instant,
    pub(super) verify: bool,
}

impl Prepared {
    pub(crate) fn image_state(&self, route: &crate::model::ReadRoute) -> Result<Vec<u8>> {
        let bucket = super::delivery::bucket(self)?.context("image bucket missing")?;
        let object: read::Object =
            self.material(self.reply.object.as_ref().context("image object missing")?)?;
        crate::processing::image::request::state(
            &bucket,
            Some(&object.stream()),
            self.image(route)?.as_ref(),
        )
    }
    pub(crate) fn image_demand(&self, size: i64) -> Result<Vec<crate::app::responses::DemandSpan>> {
        let Some(version) = &self.reply.mapping else {
            return Ok(Vec::new());
        };
        let mapping: read::Mapping = self.material(version)?;
        if mapping.start != 0 || mapping.end != size || mapping.segments.len() > 256 {
            return Ok(Vec::new());
        }
        let mut offset = 0;
        let mut spans = Vec::new();
        for segment in &mapping.segments {
            let Some(source) = segment.source.cached_source() else {
                return Ok(Vec::new());
            };
            spans.push((offset, offset + segment.length, source));
            offset += segment.length;
        }
        Ok(spans)
    }
    pub(crate) fn image(
        &self,
        route: &crate::model::ReadRoute,
    ) -> Result<Option<crate::processing::image::request::Selection>> {
        let Some(request) = route.image() else {
            return Ok(None);
        };
        let Some(bucket) = super::delivery::bucket(self)? else {
            return Ok(None);
        };
        let Some(version) = &self.reply.image_policy else {
            ensure!(bucket.image_revision == 0, "image policy missing");
            return Ok(None);
        };
        let settings: crate::model::image_styles::Settings = self.material(version)?;
        ensure!(
            settings.revision == bucket.image_revision && version.id == bucket.id.to_string(),
            "image policy revision mismatch"
        );
        let mut styles = std::collections::BTreeMap::new();
        if let Some(version) = &self.reply.image_style {
            let style: crate::model::image_styles::Style = self.material(version)?;
            ensure!(
                version.id == format!("{}:{}", bucket.id, style.name),
                "image style identity mismatch"
            );
            styles.insert(style.name.clone(), style);
        }
        let selected = request.select(route.key(&bucket), settings, &styles)?;
        ensure!(
            selected.key == self.reply.source_key,
            "image source mismatch"
        );
        Ok(Some(selected))
    }
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
                .filter(|grant| !grant.returned())
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

    pub(super) async fn reserve(
        &self,
        bytes: usize,
    ) -> Result<crate::storage::read_buffer::Charge> {
        crate::storage::read_buffer::reserve(
            &self.buffers,
            &self.memory,
            self.spare.as_ref(),
            bytes,
        )
        .await
    }

    pub(crate) fn page_hashes(&self, segment: &read::Segment) -> Result<Option<&[u8]>> {
        let Some(revision) = segment.page_revision else {
            return Ok(None);
        };
        let read::Source::Raw { source } = &segment.source else {
            anyhow::bail!("page proof requires a raw source")
        };
        let page = crate::content::raw_pages::PAGE as i64;
        let first = segment.source_offset / page;
        let count = (segment.source_offset + segment.length - 1) / page - first + 1;
        let id = read::Segment::pages_id(source, first, count);
        let item = self
            .materials
            .get(&(wire::MaterialType::RawPages as i32, id))
            .filter(|value| value.version.revision == revision)
            .context("raw page material missing")?;
        ensure!(
            item.json.len() == count as usize * crate::content::raw_pages::HASH,
            "invalid raw page proof length"
        );
        Ok(Some(&item.json))
    }

    pub(crate) fn memory(&self) -> Arc<OwnedSemaphorePermit> {
        self.memory.clone()
    }
    pub(crate) fn needs_source(&self) -> bool {
        self.reply.mapping.is_none() && self.cached_body.is_none()
    }
    pub(crate) fn check(&self) -> Result<()> {
        self.run.check()?;
        for grant in &self.grants {
            if !grant.returned() {
                grant.check()?;
            }
        }
        Ok(())
    }
}

impl Reads {
    pub(crate) fn image_generation(&self) -> u64 {
        self.public_generation();
        self.public.generation()
    }
    pub(crate) fn report_foreground(self: &Arc<Self>) {
        *self.node.foreground.lock().unwrap() = Arc::downgrade(self);
    }

    pub(crate) async fn maintain(&self) -> Result<()> {
        self.reader.maintain().await
    }

    pub(crate) fn new(node: Arc<NodeContext>) -> Result<Self> {
        let budget = node
            .budget
            .get()
            .context("node has not synchronized its budget")?;
        let slots = Arc::new(Semaphore::new(budget.read_concurrency));
        let reader = super::body::Reader::new(&node)?;
        let checks = crate::content::checks::Checks::new(reader.cpu.clone());
        let bodies = crate::app::responses::Cache::new(budget.response_cache_bytes);
        let settings = node.background_snapshot()?.runtime().requests.clone();
        let (cors, receiver) = crate::http::cors::Cache::new(
            crate::config::cache_bytes(&settings.cors_cache_size)? as usize,
            settings.cors_cache_entries,
        );
        tokio::spawn(crate::http::cors::run_current(
            Arc::downgrade(&cors),
            receiver,
        ));
        let (public, receiver) = crate::app::public_reads::Cache::new(cors.clone(), &bodies);
        let images = crate::processing::image::job::Service::new(
            crate::processing::image::job::Resources {
                disk: reader.files.disk.clone(),
                cpu: reader.cpu.clone(),
                work: reader.http.clone(),
                slots: slots.clone(),
                memory: u64::from(budget.pack_memory_units) * 1024 * 1024,
                access: node.access.clone(),
            },
            crate::config::cache_bytes(&node.config.instance.image_cache_size)?,
        );
        Ok(Self {
            images,
            node: node.clone(),
            cache: Cache {
                public: Arc::downgrade(&public),
                io: node.io.clone(),
                epoch: node.cache_epoch.clone(),
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

    pub(crate) async fn open(&self, input: wire::ReadRequest) -> Result<Prepared> {
        self.open_memory(input, None).await
    }
    pub(crate) async fn open_memory(
        &self,
        mut input: wire::ReadRequest,
        memory: Option<Arc<OwnedSemaphorePermit>>,
    ) -> Result<Prepared> {
        let memory = if let Some(memory) = memory {
            memory
        } else {
            Arc::new(self.slots.clone().acquire_owned().await?)
        };
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
        input.read_lanes = self
            .node
            .budget
            .get()
            .context("node budget missing")?
            .storage_read_slots
            .min(read::LANES) as u32;
        let recovery = crate::storage::read_retry::Recovery::new(std::time::Duration::from_secs(
            crate::config::seconds(&snapshot.runtime().requests.read_idle_timeout)?,
        ));
        let limits = (input.max_items, input.max_bytes);
        let started = Instant::now();
        let mut offer = self.node.resources.read_offer(&snapshot.lease);
        input.permits = offer.permits.take();
        let mut reply = self.node.client.read(input).await?;
        let grants = self
            .node
            .resources
            .adopt(&snapshot.lease, &reply.grants, started)?;
        super::resources::Grant::classify(&grants, true);
        drop(offer);
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
            spare: Some(crate::storage::read_buffer::Spares::new(self.slots.clone())),
            recovery,
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
        super::resources::Grant::classify(&grants, false);
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
            recovery: crate::storage::read_retry::Recovery::new(std::time::Duration::from_secs(30)),
            spare: None,
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
                let mut offer = self.node.resources.read_offer(&previous.run);
                let mut reply = self
                    .node
                    .client
                    .read_window(wire::ReadWindowRequest {
                        run: Some(previous.run.run.clone()),
                        cursor: Some(cursor),
                        permits: offer.permits.take(),
                        cached: cached(&declared),
                        max_items: previous.limits.0,
                        max_bytes: previous.limits.1,
                        read_lanes: self
                            .node
                            .budget
                            .get()
                            .context("node budget missing")?
                            .storage_read_slots
                            .min(read::LANES) as u32,
                    })
                    .await?;
                reply.http_status = previous.reply.http_status;
                let mut grants =
                    self.node
                        .resources
                        .adopt(&previous.run, &reply.grants, started)?;
                super::resources::Grant::classify(&grants, true);
                drop(offer);
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
                    spare: previous.spare.clone(),
                    recovery: previous.recovery.clone(),
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
        *backend = Some(Arc::new(super::body::Backend::new(value, &material.io)?));
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
