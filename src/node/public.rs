use super::*;
use crate::{
    app::public_reads::{Confirmation, Entry, Work},
    model::{ReadRoute, ReadSnapshot},
};
use std::{sync::atomic::Ordering::Relaxed, time::Duration};

impl Reads {
    pub(crate) fn notify_public(
        &self,
        reset: bool,
        changes: &[crate::model::notification::Change],
    ) {
        let _epoch = self.public_epoch.lock().unwrap();
        self.public.notified(reset, changes);
    }

    pub(crate) fn start_public(self: &Arc<Self>) {
        if let Some(receiver) = self.public_receiver.lock().unwrap().take() {
            tokio::spawn(run(Arc::downgrade(self), receiver));
        }
    }

    pub(super) fn public_generation(&self) -> u64 {
        let mut epoch = self.public_epoch.lock().unwrap();
        let current = self.node.current.borrow().epoch;
        if *epoch != current {
            drop(self.public.changing());
            *epoch = current;
        }
        self.public.generation()
    }

    fn current_profiles(&self, profiles: &[(Uuid, i64)]) -> bool {
        profiles.iter().all(|(id, revision)| {
            self.cache
                .policy(*id)
                .is_some_and(|policy| policy.revision == *revision && policy.public_cache_enabled)
        })
    }

    pub(super) fn observe_public(&self, route: &ReadRoute, reply: &wire::ReadReply) -> Result<()> {
        self.public.fence_pending(route);
        let Some(entry) = self.public.entry(route) else {
            return Ok(());
        };
        let deployment = self.node.client.identity.deployment_id;
        let bucket = &entry.snapshot.bucket;
        let version = |r#type, id, value: Vec<u8>| {
            crate::rpc::read::version(read::Version::new(deployment, r#type, id, &value))
        };
        let bucket_version = version(
            read::MaterialType::Bucket,
            bucket.id.to_string(),
            serde_json::to_vec(bucket)?,
        );
        if reply.bucket.as_ref() != Some(&bucket_version) {
            drop(self.public.changing());
        } else if let Some(stream) = &entry.snapshot.stream {
            let object = read::Object::from_stream(stream)?;
            let object_version = version(
                read::MaterialType::Object,
                object.id.to_string(),
                serde_json::to_vec(&object)?,
            );
            if reply.object.as_ref() != Some(&object_version)
                || entry.mapping.is_some_and(|mapping| {
                    reply
                        .mapping
                        .as_ref()
                        .is_some_and(|current| current.revision.as_slice() != mapping)
                })
            {
                drop(self.public.changing_object(bucket.id, &stream.object_key));
            }
        }
        Ok(())
    }

    pub(crate) async fn open_public(
        &self,
        mut input: wire::ReadRequest,
        bucket_only: bool,
    ) -> Result<Prepared> {
        let snapshot = self.node.snapshot()?;
        self.public_generation();
        input.run = Some(snapshot.lease.run.clone());
        input.operation_id = Uuid::new_v4().to_string();
        let query = crate::rpc::read::query(input.clone())?;
        ensure!(
            query.access.is_none() && crate::http::object::reuse_public(&query.headers),
            "invalid public cache request"
        );
        if let Some(job) = self.public.refreshing(&query.route) {
            let _ = tokio::time::timeout(Duration::from_secs(2), self.refresh_notified(job)).await;
        }
        let cached = if self.node.current.borrow().phase != super::super::Phase::Ready {
            None
        } else if bucket_only {
            self.public
                .bucket_with(&query.route, |profiles| self.current_profiles(profiles))
        } else {
            self.public
                .get_with(&query.route, |profiles| self.current_profiles(profiles))
        };
        if let Some(entry) = cached {
            let memory = Arc::new(self.slots.clone().acquire_owned().await?);
            self.public_generation();
            if self.public.refresh_generation(&entry).is_some()
                && let Ok(Some(prepared)) = self.reuse_public(&input, &entry, memory, bucket_only)
            {
                return Ok(prepared);
            }
        }
        let prepared = self.open(input).await?;
        if let Some((snapshot, confirmation)) = self.public_confirmation(&prepared).ok().flatten()
            && let Some(generation) = prepared.public_generation
        {
            self.public.queue_with(
                &query.route,
                snapshot,
                prepared.started,
                generation,
                Some(confirmation),
            );
        }
        Ok(prepared)
    }

    fn public_confirmation(
        &self,
        prepared: &Prepared,
    ) -> Result<Option<(Arc<ReadSnapshot>, Confirmation)>> {
        if prepared.reply.outcome != wire::ReadOutcome::Ready as i32
            || prepared.reply.credential.is_some()
        {
            return Ok(None);
        }
        let Some(object) = &prepared.reply.object else {
            return Ok(None);
        };
        let object: read::Object = prepared.material(object)?;
        let Some(mut bucket) = super::super::delivery::bucket(prepared)? else {
            return Ok(None);
        };
        if !object.public
            || bucket.state != "active"
            || !object
                .metadata
                .get("cache_control")
                .and_then(|value| value.as_str())
                .is_none_or(crate::http::object::cache_control)
        {
            return Ok(None);
        }
        let Some(version) = &prepared.reply.policy else {
            return Ok(None);
        };
        let policy: read::Policy = prepared.material(version)?;
        ensure!(
            bucket.storage_id == Some(policy.storage_id),
            "read policy does not match the bucket"
        );
        let mut policies = vec![policy];
        let mapping = if let Some(body) = &prepared.cached_body {
            let Some(proof) = body.node_body() else {
                return Ok(None);
            };
            let index = self.cache.index.lock().unwrap();
            for version in proof.storages {
                let version = crate::rpc::read::version(version);
                let Some(material) = index
                    .values
                    .get(&(version.r#type, version.id.clone()))
                    .filter(|material| material.version == version)
                else {
                    return Ok(None);
                };
                policies.push(material.policy.clone().context("storage policy missing")?);
            }
            Some(proof.mapping.revision)
        } else if let Some(version) = &prepared.reply.mapping {
            let mapping: read::Mapping = prepared.material(version)?;
            if mapping.start == 0 && mapping.end == object.size && prepared.reply.next.is_none() {
                policies.extend(
                    prepared
                        .materials
                        .values()
                        .filter(|material| {
                            material.version.r#type == wire::MaterialType::Storage as i32
                        })
                        .filter_map(|material| material.policy.clone()),
                );
                Some(version.revision.as_slice().try_into()?)
            } else {
                None
            }
        } else {
            None
        };
        if policies.iter().any(|policy| !policy.public_cache_enabled) {
            return Ok(None);
        }
        let mut profiles: Vec<_> = policies
            .iter()
            .map(|policy| (policy.storage_id, policy.revision))
            .collect();
        profiles.sort_unstable();
        profiles.dedup();
        if !self.current_profiles(&profiles) {
            return Ok(None);
        }
        let times = (
            policies
                .iter()
                .map(|policy| policy.refresh_seconds)
                .min()
                .unwrap(),
            policies
                .iter()
                .map(|policy| policy.max_age_seconds)
                .min()
                .unwrap(),
        );
        self.cors.current(&mut bucket);
        Ok(Some((
            Arc::new(ReadSnapshot {
                bucket,
                stream: Some(object.stream()),
                raw: None,
                secret: None,
                allowed: true,
            }),
            Confirmation {
                mapping,
                profiles,
                times,
            },
        )))
    }

    async fn refresh_notified(&self, job: Arc<crate::app::public_reads::Refresh>) {
        let Some(_running) = job.claim() else {
            job.wait().await;
            return;
        };
        let result = tokio::time::timeout(Duration::from_secs(10), async {
            let mut state = self.node.subscribe();
            loop {
                let phase = state.borrow_and_update().phase;
                if phase == super::super::Phase::Ready {
                    break;
                }
                if matches!(
                    phase,
                    super::super::Phase::Stopped | super::super::Phase::Fenced
                ) {
                    return Ok(false);
                }
                state.changed().await?;
            }
            for _ in 0..3 {
                let generation = job.generation.load(Relaxed);
                #[cfg(feature = "fault-injection")]
                crate::faults::point("node-notification-refresh").await;
                let prepared = tokio::time::timeout(
                    Duration::from_secs(2),
                    self.open(request(&job.entry.route, job.entry.mapping.is_none())),
                )
                .await??;
                #[cfg(feature = "fault-injection")]
                crate::faults::point("node-notification-refreshed").await;
                self.public_generation();
                if job.generation.load(Relaxed) != generation
                    || prepared.public_generation.is_none()
                {
                    continue;
                }
                if let Some((snapshot, confirmation)) = self.public_confirmation(&prepared)? {
                    self.public.put(
                        &job.entry.route,
                        snapshot,
                        confirmation.mapping,
                        confirmation.profiles,
                        confirmation.times,
                        (prepared.started, prepared.public_generation.unwrap()),
                    );
                }
                if self
                    .public
                    .finish_refresh(&job, generation, prepared.public_generation.unwrap())
                {
                    return Ok::<_, anyhow::Error>(true);
                }
            }
            Ok(false)
        })
        .await;
        match result {
            Ok(Ok(true)) => {
                self.public.refreshed.fetch_add(1, Relaxed);
            }
            Ok(Ok(false)) => {
                self.public.skipped.fetch_add(1, Relaxed);
            }
            _ => {
                self.public.failed.fetch_add(1, Relaxed);
            }
        }
    }

    fn reuse_public(
        &self,
        input: &wire::ReadRequest,
        entry: &Arc<Entry>,
        memory: Arc<OwnedSemaphorePermit>,
        bucket_only: bool,
    ) -> Result<Option<Prepared>> {
        let epoch = self.public_epoch.lock().unwrap();
        if self.node.current.borrow().phase != super::super::Phase::Ready
            || *epoch != self.node.current.borrow().epoch
            || !self
                .public
                .reusable(entry, |profiles| self.current_profiles(profiles))
        {
            return Ok(None);
        }
        let snapshot = self.node.snapshot()?;
        let stream = entry
            .snapshot
            .stream
            .as_ref()
            .context("public object missing")?;
        let mut query = crate::rpc::read::query(input.clone())?;
        let deployment = self.node.client.identity.deployment_id;
        let mut reply = read::Reply::new(read::Outcome::Ready);
        let bucket = &entry.snapshot.bucket;
        reply.bucket = Some(reply.material(
            deployment,
            read::MaterialType::Bucket,
            bucket.id.to_string(),
            bucket,
            &[],
        )?);
        let Some(policy) = bucket.storage_id.and_then(|id| self.cache.policy(id)) else {
            return Ok(None);
        };
        reply.policy = Some(reply.material(
            deployment,
            read::MaterialType::Policy,
            policy.storage_id.to_string(),
            &policy,
            &[],
        )?);
        let candidate = if query.head || bucket_only {
            None
        } else {
            self.candidate(input)
        };
        if !query.head && !bucket_only {
            let Some((_, body)) = &candidate else {
                return Ok(None);
            };
            let Some(proof) = body.node_body() else {
                return Ok(None);
            };
            if entry.mapping != Some(proof.mapping.revision)
                || proof.policy != crate::http::object::POLICY as u32
            {
                return Ok(None);
            }
            let index = self.cache.index.lock().unwrap();
            for version in &proof.storages {
                let version = crate::rpc::read::version(version.clone());
                let Some(material) = index
                    .values
                    .get(&(version.r#type, version.id.clone()))
                    .filter(|material| material.version == version)
                else {
                    return Ok(None);
                };
                if !proof.verified
                    && material
                        .policy
                        .as_ref()
                        .is_none_or(|policy| policy.read_integrity == "inline")
                {
                    return Ok(None);
                }
            }
            query.body = Some(proof);
        }
        if !bucket_only {
            let object = read::Object::from_stream(stream)?;
            reply.object = Some(reply.material(
                deployment,
                read::MaterialType::Object,
                object.id.to_string(),
                &object,
                &[],
            )?);
            query.select(stream, &mut reply)?;
            if !query.head && !reply.body_reused {
                return Ok(None);
            }
        }
        let mut reply = crate::rpc::read::reply(reply);
        let materials = self.cache.apply(
            deployment,
            Held::new(),
            std::mem::take(&mut reply.materials),
        )?;
        let prepared = Prepared {
            reply,
            run: snapshot.lease.clone(),
            materials,
            grants: Vec::new(),
            memory,
            buffers: Arc::new(Semaphore::new(super::super::body::BUFFER_UNITS)),
            limits: (input.max_items, input.max_bytes),
            cached_body: candidate.map(|(_, body)| body),
            route: super::super::cache::route(input),
            capture: None,
            public_generation: None,
            started: std::time::Instant::now(),
            verify: false,
        };
        prepared.check()?;
        Ok(Some(prepared))
    }

    #[cfg(test)]
    pub(crate) fn public_cache(&self) -> serde_json::Value {
        self.public.snapshot()
    }
}

fn request(route: &ReadRoute, head: bool) -> wire::ReadRequest {
    let route = match route {
        ReadRoute::Host { host, key } => wire::read_route::Route::Host(wire::HostRoute {
            host: host.clone(),
            key: key.clone(),
        }),
        ReadRoute::Bucket { name, key, alias } => {
            wire::read_route::Route::Bucket(wire::BucketRoute {
                name: name.clone(),
                key: key.clone(),
                alias_name: alias
                    .as_ref()
                    .map_or_else(String::new, |(name, _)| name.clone()),
                alias_key: alias
                    .as_ref()
                    .map_or_else(String::new, |(_, key)| key.clone()),
            })
        }
    };
    wire::ReadRequest {
        surface: if matches!(route, wire::read_route::Route::Host(_)) {
            wire::ReadSurface::Web
        } else {
            wire::ReadSurface::S3
        } as i32,
        route: Some(wire::ReadRoute { route: Some(route) }),
        method: if head {
            wire::ReadMethod::Head
        } else {
            wire::ReadMethod::Get
        } as i32,
        include: if head {
            vec![
                wire::ReadInclude::Object as i32,
                wire::ReadInclude::Policy as i32,
            ]
        } else {
            vec![
                wire::ReadInclude::Object as i32,
                wire::ReadInclude::Policy as i32,
                wire::ReadInclude::Mapping as i32,
            ]
        },
        ..Default::default()
    }
}

async fn run(reads: Weak<Reads>, mut receiver: tokio::sync::mpsc::Receiver<Work>) {
    while let Some(work) = receiver.recv().await {
        let Some(reads) = reads.upgrade() else { return };
        reads.public_generation();
        match work {
            Work::Notify(job) => reads.refresh_notified(job).await,
            Work::Fill {
                route,
                snapshot,
                started,
                generation,
                confirmed,
                _memory,
            } => {
                #[cfg(feature = "fault-injection")]
                crate::faults::point("node-public-fill").await;
                reads.public_generation();
                if reads.node.snapshot().is_ok()
                    && let Some(confirmation) = confirmed
                    && reads.current_profiles(&confirmation.profiles)
                {
                    reads.public.put(
                        &route,
                        snapshot,
                        confirmation.mapping,
                        confirmation.profiles,
                        confirmation.times,
                        (started, generation),
                    );
                }
                reads.public.finished(&route);
            }
            Work::Refresh(entry) => {
                let result = tokio::time::timeout(Duration::from_secs(2), async {
                    #[cfg(feature = "fault-injection")]
                    crate::faults::point("node-public-refresh").await;
                    if reads.public.refresh_generation(&entry).is_none() {
                        return Ok(false);
                    }
                    let prepared = reads
                        .open(request(&entry.route, entry.mapping.is_none()))
                        .await?;
                    reads.public_generation();
                    if reads.public.refresh_generation(&entry).is_none() {
                        return Ok(false);
                    }
                    reads.public.remove(&entry);
                    if let Some((snapshot, confirmation)) = reads.public_confirmation(&prepared)?
                        && let Some(generation) = prepared.public_generation
                    {
                        reads.public.put(
                            &entry.route,
                            snapshot,
                            confirmation.mapping,
                            confirmation.profiles,
                            confirmation.times,
                            (prepared.started, generation),
                        );
                    }
                    Ok::<_, anyhow::Error>(true)
                })
                .await;
                reads.public.finished(&entry.route);
                match result {
                    Ok(Ok(true)) => {
                        reads.public.refreshed.fetch_add(1, Relaxed);
                    }
                    Ok(Ok(false)) => {
                        reads.public.skipped.fetch_add(1, Relaxed);
                    }
                    _ => {
                        reads.public.failed.fetch_add(1, Relaxed);
                    }
                }
            }
        }
    }
}
