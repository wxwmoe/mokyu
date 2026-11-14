use super::{store::Store, writer::Sink};
use crate::{
    model::Id,
    storage::{
        Storage, StoreBackend, StorePermit, raw_request::File, raw_upload::Written, resource_gate,
    },
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures_util::{TryStreamExt, stream::BoxStream};
use md5::{Digest, Md5};
use std::sync::{Arc, atomic::Ordering};
use uuid::Uuid;

pub(crate) struct Writer {
    sink: Option<Sink>,
    file: Option<File>,
    external: bool,
    storage: Arc<Storage>,
    target: Arc<StoreBackend>,
    native: Arc<Store>,
    source: Id,
    location: Uuid,
    size: u64,
    hash: Option<blake3::Hasher>,
    fill: Option<crate::storage::raw_cache::Write>,
    _permit: StorePermit,
    _global: resource_gate::Permit,
}

impl Writer {
    pub fn new(
        storage: Arc<Storage>,
        target: Arc<StoreBackend>,
        native: Arc<Store>,
        source: Id,
        location: Uuid,
        declared: Option<i64>,
        hash: Option<blake3::Hasher>,
        fill: Option<crate::storage::raw_cache::Write>,
        permit: StorePermit,
        global: resource_gate::Permit,
    ) -> Self {
        let sink = declared
            .filter(|size| *size >= 0 && hash.is_none())
            .map(|size| {
                Sink::new(
                    native.clone(),
                    crate::storage::raw::remote_path(location),
                    size as u64,
                )
            });
        Self {
            sink,
            file: None,
            external: false,
            storage,
            target,
            native,
            source,
            location,
            size: 0,
            hash,
            fill,
            _permit: permit,
            _global: global,
        }
    }

    pub fn external(&mut self) {
        self.sink = None;
        self.external = true;
    }

    pub fn may_reuse(&self) -> bool {
        self.hash.is_some()
    }

    pub async fn reuse(&mut self, stream: Id, digest: &[u8]) -> Result<bool> {
        let Some(hash) = &self.hash else {
            return Ok(false);
        };
        self.storage
            .raw_store
            .reuse(
                stream,
                self.source,
                self.target.profile.storage_id,
                self.size as i64,
                digest,
                hash.finalize().as_bytes(),
            )
            .await
    }

    pub async fn write(&mut self, data: &[u8]) -> Result<()> {
        self.size = self
            .size
            .checked_add(data.len() as u64)
            .context("native raw length overflow")?;
        ensure!(
            self.size <= 5 * 1024_u64.pow(4),
            s3s::s3_error!(EntityTooLarge)
        );
        if let Some(hash) = &mut self.hash {
            hash.update(data);
        }
        crate::storage::raw_cache::append(&mut self.fill, data).await;
        if let Some(sink) = &mut self.sink {
            sink.write(data).await?;
        } else if !self.external {
            if self.file.is_none() {
                self.file = Some(
                    File::create(
                        &self.storage,
                        self.location,
                        self.storage
                            .local_cache(Some(self.target.profile.storage_id))?,
                    )
                    .await?,
                );
            }
            self.file
                .as_mut()
                .unwrap()
                .append(&self.storage, data)
                .await?;
        }
        Ok(())
    }

    pub async fn finish(
        mut self,
        expected: u64,
        digest: &[u8],
        input: Option<BoxStream<'_, Result<Bytes>>>,
    ) -> Result<Written> {
        ensure!(
            self.size == expected && digest.len() == 16,
            "native raw length or digest mismatch"
        );
        if let Some(sink) = self.sink.take() {
            sink.finish().await?;
        } else {
            let body: BoxStream<'_, Result<Bytes>> = if self.external {
                input.context("native raw replay source is missing")?
            } else if let Some(file) = &self.file {
                file.replay()
            } else {
                Box::pin(futures_util::stream::empty())
            };
            let expected_hash = self.hash.as_ref().map(|hash| hash.finalize());
            let result = self
                .native
                .write(
                    &crate::storage::raw::remote_path(self.location),
                    expected,
                    checked(body, digest, expected_hash),
                    object_store::PutMode::Create.into(),
                )
                .await;
            if result.as_ref().err().and_then(crate::model::error)
                == Some(crate::model::Error::StorageForwardFailed)
                && let Some(file) = &self.file
            {
                let mut direct = self.native.as_ref().clone();
                direct.region = None;
                direct
                    .write(
                        &crate::storage::raw::remote_path(self.location),
                        expected,
                        checked(file.replay(), digest, expected_hash),
                        object_store::PutMode::Create.into(),
                    )
                    .await?;
            } else {
                result?;
            }
        }
        self.storage.backend_puts.fetch_add(1, Ordering::Relaxed);
        self.storage
            .backend_write_bytes
            .fetch_add(expected, Ordering::Relaxed);
        let fill = if let Some(file) = self.file.take() {
            self.storage
                .prepare_raw_file(file, self.location, self.target.clone())
        } else {
            self.fill.take().map(|fill| {
                self.storage
                    .prepare_raw_fill(fill, self.location, expected, self.target.clone())
            })
        };
        Ok(Written::Final(
            self.hash.as_ref().map(|hash| *hash.finalize().as_bytes()),
            fill,
        ))
    }
}

fn checked<'a>(
    mut body: BoxStream<'a, Result<Bytes>>,
    digest: &'a [u8],
    expected_hash: Option<blake3::Hash>,
) -> BoxStream<'a, Result<Bytes>> {
    Box::pin(async_stream::try_stream! {
        let mut md5 = Md5::new();
        let mut hash = blake3::Hasher::new();
        while let Some(bytes) = body.try_next().await? {
            md5.update(&bytes);
            if expected_hash.is_some() { hash.update(&bytes); }
            yield bytes;
        }
        if md5.finalize().as_slice() != digest || expected_hash.is_some_and(|expected| expected != hash.finalize()) {
            Err(crate::content::codec::IntegrityError::Hash)?;
        }
    })
}
