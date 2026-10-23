use super::{Active, App, StoredStream};
use crate::model::{Id, ReadRoute, ReadSnapshot, raw::RawRange};
use anyhow::Result;
use std::{
    future::Future,
    sync::{Arc, Mutex},
};
use uuid::Uuid;

tokio::task_local! { static REQUEST: Option<Arc<Prepared>>; }

pub(crate) struct Prepared {
    pub snapshot: ReadSnapshot,
    key: String,
    access: Option<String>,
    pin: Mutex<Option<Active>>,
    _source: Option<crate::storage::CachePin>,
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
                read.snapshot
                    .stream
                    .as_ref()
                    .is_some_and(|s| s.id == stream && s.payload_revision == revision)
            })?;
            prepared.snapshot.raw.clone()
        })
        .ok()
        .flatten()
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
        &self,
        route: &ReadRoute,
        access: Option<&str>,
    ) -> Result<Option<Arc<Prepared>>> {
        if self.role != "single" {
            return Ok(None);
        }
        let _coord = self.coord.lock().await;
        let _sources = self.storage.source_gate.read().await;
        self.ensure_mode()?;
        let Some(snapshot) = self.stream_store.snapshot(route, access).await? else {
            return Ok(None);
        };
        // Acquire both pins before GC can claim the selected stream or physical source.
        let pin = snapshot.stream.as_ref().map(|stream| self.pin(stream.id));
        let source = snapshot
            .raw
            .as_ref()
            .map(|row| self.storage.physical_pin(row.source.location_id));
        let key = route.key(&snapshot.bucket).to_owned();
        Ok(Some(Arc::new(Prepared {
            snapshot,
            key,
            access: access.map(str::to_owned),
            pin: Mutex::new(pin),
            _source: source,
        })))
    }
}
