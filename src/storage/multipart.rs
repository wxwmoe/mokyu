use super::{Storage, UploadCharge, raw::remote_path};
use crate::model::{Id, multipart::RemoteUpload};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures_util::{StreamExt, stream::BoxStream};
use object_store::ObjectStoreExt;
use std::{collections::HashSet, sync::atomic::Ordering};

impl Storage {
    pub(crate) fn remote_charge(
        &self,
        storage: uuid::Uuid,
        size: u64,
    ) -> Result<Option<UploadCharge>> {
        let policy = self.local_cache(Some(storage))?;
        if !policy.upload_cache {
            return Ok(None);
        }
        match self
            .disk
            .claim_upload_with_limit(size, storage, policy.upload_capacity)
        {
            Ok(charge) => Ok(Some(charge)),
            Err(_) => {
                crate::upload::cache::overflow(
                    self.profile(storage)?.upload_cache_overflow == "reject",
                )?;
                Ok(None)
            }
        }
    }
    pub(crate) fn hold_remote_charge(&self, stream: Id, charge: UploadCharge) {
        self.remote_charges.lock().unwrap().insert(stream, charge);
    }
    pub(crate) fn release_remote_charge(&self, stream: Id) {
        self.remote_charges.lock().unwrap().remove(&stream);
    }
    pub(crate) async fn reconcile_remote_charges(&self) -> Result<()> {
        let (Some(node), Some(cluster)) = (self.node_id.get(), &self.cluster) else {
            return Ok(());
        };
        let mut after = Id::default();
        let mut keep = HashSet::new();
        loop {
            let rows = cluster.remote_charges(*node, after).await?;
            if rows.is_empty() {
                break;
            }
            for (stream, storage, size) in rows {
                after = stream;
                keep.insert(stream);
                self.remote_charges
                    .lock()
                    .unwrap()
                    .entry(stream)
                    .or_insert_with(|| self.disk.restore_upload(storage, size as u64));
            }
        }
        self.remote_charges
            .lock()
            .unwrap()
            .retain(|stream, _| keep.contains(stream));
        Ok(())
    }
    pub(crate) async fn create_remote(&self, upload: &RemoteUpload) -> Result<String> {
        self.ensure_storage_verified(upload.storage).await?;
        let target = self.store_backend(upload.storage)?;
        ensure!(!target.is_local, "native multipart requires S3");
        let _global = self.controls.acquire().await?;
        let _permit = target.controls.acquire().await?;
        Ok(target
            .multipart
            .as_ref()
            .context("S3 multipart backend is missing")?
            .create_multipart(&remote_path(upload.source))
            .await?)
    }
    pub(crate) async fn abort_remote(&self, upload: &RemoteUpload, upstream: &str) -> Result<()> {
        let target = self.store_backend(upload.storage)?;
        let _global = self.controls.acquire().await?;
        let _permit = target.controls.acquire().await?;
        match target
            .multipart
            .as_ref()
            .context("S3 multipart backend is missing")?
            .abort_multipart(&remote_path(upload.source), &upstream.to_owned())
            .await
        {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
    pub(crate) async fn send_remote_part<'a>(
        &self,
        upload: &RemoteUpload,
        number: i32,
        size: i64,
        digest: &[u8; 16],
        input: impl FnMut() -> BoxStream<'a, Result<Bytes>>,
    ) -> Result<String> {
        ensure!(
            (1..=10000).contains(&number) && size >= 0,
            "invalid native multipart part"
        );
        self.ensure_storage_verified(upload.storage).await?;
        let target = self.store_backend(upload.storage)?;
        let _global = self.writes.acquire().await?;
        let _permit = target.writes.acquire().await?;
        let part = target
            .streaming
            .as_ref()
            .context("S3 streaming backend is missing")?
            .part(
                &remote_path(upload.source),
                upload
                    .upstream
                    .as_deref()
                    .context("S3 upload is not initialized")?,
                (number - 1) as usize,
                size as u64,
                digest,
                input,
            )
            .await?;
        self.backend_puts.fetch_add(1, Ordering::Relaxed);
        self.backend_write_bytes
            .fetch_add(size as u64, Ordering::Relaxed);
        Ok(part.content_id)
    }
    pub(crate) async fn complete_remote(
        &self,
        upload: &RemoteUpload,
        parts: &[(i32, String)],
        size: i64,
    ) -> Result<bool> {
        self.ensure_storage_verified(upload.storage).await?;
        let target = self.store_backend(upload.storage)?;
        let _global = self.controls.acquire().await?;
        let _permit = target.controls.acquire().await?;
        let path = remote_path(upload.source);
        match target.backend.head(&path).await {
            Ok(meta) => {
                ensure!(
                    meta.size == size as u64,
                    "completed S3 object length differs"
                );
                return Ok(true);
            }
            Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(error.into()),
        }
        let result = target
            .streaming
            .as_ref()
            .context("S3 streaming backend is missing")?
            .complete(
                &path,
                upload
                    .upstream
                    .as_deref()
                    .context("S3 upload is not initialized")?,
                parts,
            )
            .await;
        if result.is_err() {
            // A lost Complete response is resolved by verifying the immutable final object.
            if let Ok(meta) = target.backend.head(&path).await {
                ensure!(
                    meta.size == size as u64,
                    "completed S3 object length differs"
                );
                return Ok(true);
            }
        }
        result
    }
    pub(crate) fn remote_body<'a>(
        &'a self,
        upload: &'a RemoteUpload,
    ) -> BoxStream<'a, Result<Bytes>> {
        Box::pin(async_stream::try_stream! {
            let target=self.store_backend(upload.storage)?;
            let _global=self.reads.acquire().await?;
            let _permit=target.reads.acquire().await?;
            let result=target.backend.get(&remote_path(upload.source)).await?;
            self.backend_gets.fetch_add(1,Ordering::Relaxed);
            let mut stream=result.into_stream();
            while let Some(bytes)=stream.next().await {
                let bytes=bytes?;
                self.backend_read_bytes.fetch_add(bytes.len() as u64,Ordering::Relaxed);
                yield bytes;
            }
        })
    }
}
