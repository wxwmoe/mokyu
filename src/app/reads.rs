use super::{Active, App, StoredStream};
use crate::model::{Id, ReadRoute, ReadSnapshot, raw::RawRange};
use anyhow::Result;
use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use uuid::Uuid;

tokio::task_local! { static REQUEST: Option<Arc<Prepared>>; }

pub(crate) struct Prepared {
    pub snapshot: Arc<ReadSnapshot>,
    key: String,
    access: Option<String>,
    pin: Mutex<Option<Active>>,
    source: Mutex<Option<crate::storage::CachePin>>,
    raw_ready: AtomicBool,
    cached: Option<Arc<super::public_reads::Entry>>,
}

pub(crate) async fn scope<T>(
    prepared: Option<Arc<Prepared>>,
    future: impl Future<Output = T>,
) -> T {
    REQUEST.scope(prepared, future).await
}

pub(crate) fn metadata(bucket: Uuid, key: &str) -> Option<Result<StoredStream>> {
    REQUEST
        .try_with(|prepared| {
            let prepared = prepared
                .as_ref()
                .filter(|read| read.snapshot.bucket.id == bucket && read.key == key)?;
            Some(
                prepared
                    .snapshot
                    .stream
                    .clone()
                    .ok_or_else(|| s3s::s3_error!(NoSuchKey).into()),
            )
        })
        .ok()
        .flatten()
}

pub(crate) fn current(bucket: Uuid, key: &str) -> Option<Result<(StoredStream, Active)>> {
    REQUEST
        .try_with(|prepared| {
            let prepared = prepared
                .as_ref()
                .filter(|read| read.snapshot.bucket.id == bucket && read.key == key)?;
            let Some(stream) = &prepared.snapshot.stream else {
                return Some(Err(s3s::s3_error!(NoSuchKey).into()));
            };
            prepared
                .pin
                .lock()
                .unwrap()
                .take()
                .map(|pin| Ok((stream.clone(), pin)))
        })
        .ok()
        .flatten()
}

pub(crate) fn raw(stream: Id, revision: i64) -> Option<RawRange> {
    REQUEST
        .try_with(|prepared| {
            let prepared = prepared.as_ref().filter(|read| {
                read.raw_ready.load(Ordering::Acquire)
                    && read
                        .snapshot
                        .stream
                        .as_ref()
                        .is_some_and(|s| s.id == stream && s.payload_revision == revision)
            })?;
            prepared.snapshot.raw.clone()
        })
        .ok()
        .flatten()
}

pub(crate) fn cached(bucket: Uuid, key: &str) -> Option<(StoredStream, Option<[u8; 32]>)> {
    REQUEST
        .try_with(|prepared| {
            let prepared = prepared
                .as_ref()
                .filter(|read| read.snapshot.bucket.id == bucket && read.key == key)?;
            let entry = prepared.cached.as_ref()?;
            Some((entry.snapshot.stream.as_ref()?.clone(), entry.mapping))
        })
        .ok()
        .flatten()
}

pub(crate) fn is_raw(bucket: Uuid, key: &str) -> bool {
    REQUEST
        .try_with(|prepared| {
            prepared.as_ref().is_some_and(|read| {
                read.snapshot.bucket.id == bucket
                    && read.key == key
                    && read
                        .snapshot
                        .stream
                        .as_ref()
                        .is_some_and(|stream| stream.payload_type == "raw")
            })
        })
        .unwrap_or(false)
}

// The caller holds coord; source_gate makes selecting a cached source atomic with local GC.
pub(crate) async fn pin_cached(
    app: &App,
    bucket: Uuid,
    key: &str,
) -> Option<(StoredStream, Active)> {
    let prepared = REQUEST.try_with(|read| read.clone()).ok().flatten()?;
    if prepared.snapshot.bucket.id != bucket || prepared.key != key {
        return None;
    }
    let entry = prepared.cached.as_ref()?;
    let _sources = app.storage.source_gate.read().await;
    if !app.public_reads.raw_current(entry, &app.storage) {
        return None;
    }
    let stream = prepared.snapshot.stream.as_ref()?;
    let raw = prepared.snapshot.raw.as_ref()?;
    let pin = app.pin(stream.id);
    *prepared.source.lock().unwrap() = Some(app.storage.physical_pin(raw.source.location_id));
    prepared.raw_ready.store(true, Ordering::Release);
    Some((stream.clone(), pin))
}

pub(crate) fn secret(access: &str) -> Option<Option<Vec<u8>>> {
    REQUEST
        .try_with(|prepared| {
            prepared
                .as_ref()
                .filter(|read| read.access.as_deref() == Some(access))
                .map(|read| read.snapshot.secret.clone())
        })
        .ok()
        .flatten()
}

pub(crate) fn allowed(bucket: Uuid, access: &str) -> Option<bool> {
    REQUEST
        .try_with(|prepared| {
            prepared
                .as_ref()
                .filter(|read| {
                    read.snapshot.bucket.id == bucket && read.access.as_deref() == Some(access)
                })
                .map(|read| read.snapshot.allowed)
        })
        .ok()
        .flatten()
}

impl App {
    pub(crate) async fn prepare_read(
        self: &Arc<Self>,
        route: &ReadRoute,
        access: Option<&str>,
        public_cache: bool,
    ) -> Result<Option<Arc<Prepared>>> {
        if self.role != "single" && (!public_cache || !self.storage.public_cache_enabled()) {
            return Ok(None);
        }
        self.ensure_mode()?;
        if public_cache
            && access.is_none()
            && let Some(entry) = self.public_reads.get(route, &self.storage)
        {
            // Immutable body hits need no source pin; misses recheck and pin in App::current.
            return Ok(Some(Arc::new(Prepared {
                key: route.key(&entry.snapshot.bucket).to_owned(),
                snapshot: entry.snapshot.clone(),
                access: None,
                pin: Mutex::new(None),
                source: Mutex::new(None),
                raw_ready: AtomicBool::new(false),
                cached: Some(entry),
            })));
        }
        let _coord = self.coord.lock().await;
        let _sources = self.storage.source_gate.read().await;
        self.ensure_mode()?;
        let generation = self.public_reads.generation();
        let started = std::time::Instant::now();
        let Some(snapshot) = self.stream_store.snapshot(route, access).await? else {
            return Ok(None);
        };
        let snapshot = Arc::new(snapshot);
        if public_cache
            && access.is_none()
            && let Err(error) = self
                .cache_public_read(route, snapshot.clone(), started, generation)
                .await
        {
            tracing::debug!(%error,"public snapshot was not cached");
        }
        // Acquire both pins before GC can claim the selected stream or physical source.
        let pin = (self.role == "single")
            .then(|| snapshot.stream.as_ref().map(|stream| self.pin(stream.id)))
            .flatten();
        let source = (self.role == "single")
            .then(|| {
                snapshot
                    .raw
                    .as_ref()
                    .map(|row| self.storage.physical_pin(row.source.location_id))
            })
            .flatten();
        let key = route.key(&snapshot.bucket).to_owned();
        Ok(Some(Arc::new(Prepared {
            snapshot,
            key,
            access: access.map(str::to_owned),
            pin: Mutex::new(pin),
            source: Mutex::new(source),
            raw_ready: AtomicBool::new(true),
            cached: None,
        })))
    }
}
