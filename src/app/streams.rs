use crate::model::Id;
use crate::{authorization::Permit, config};
use anyhow::Result;
use s3s::s3_error;
use serde_json::Value;
use uuid::Uuid;

use super::{Active, App, StoredStream};

impl App {
    pub async fn metadata(&self, bucket: Uuid, key: &str) -> Result<StoredStream> {
        self.ensure_mode()?;
        let stream = if let Some(stream) = super::reads::metadata(bucket, key) {
            stream?
        } else {
            self.stream_store.current(bucket, key).await?
        };
        #[cfg(feature = "fault-injection")]
        crate::faults::point("read-metadata").await;
        Ok(stream)
    }
    pub async fn current(&self, bucket: Uuid, key: &str) -> Result<(StoredStream, Active)> {
        if let Some(current) = super::reads::current(bucket, key) {
            self.ensure_mode()?;
            #[cfg(feature = "fault-injection")]
            crate::faults::point("read-pinned").await;
            return current;
        }
        let _guard = self.coord.lock().await;
        self.ensure_mode()?;
        let (stream, pin) = if self.role == "single" {
            if let Some(current) = super::reads::pin_cached(self, bucket, key).await {
                current
            } else {
                let stream = self.stream_store.current(bucket, key).await?;
                let pin = self.pin(stream.id);
                (stream, pin)
            }
        } else {
            let ttl = self.read_lease_seconds()?;
            let started = tokio::time::Instant::now();
            let (stream, lease_id) = self.cluster()?.current_stream(bucket, key, ttl).await?;
            let pin = self.pin_with_lease(
                stream.id,
                Some(super::ReadLease {
                    id: lease_id,
                    replica: false,
                    store: self.cluster.as_ref().unwrap().clone(),
                    ttl_seconds: ttl,
                    renewed_at: tokio::sync::Mutex::new(started),
                }),
            );
            (stream, pin)
        };
        drop(_guard);
        #[cfg(feature = "fault-injection")]
        crate::faults::point("read-pinned").await;
        Ok((stream, pin))
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn new_stream(
        &self,
        authority: &Permit,
        bucket: Uuid,
        key: &str,
        stream_type: &str,
        metadata: Value,
        public: bool,
        claim_object: bool,
        completion: Option<Uuid>,
        owner: Option<(Uuid, Uuid)>,
    ) -> Result<(Id, Active)> {
        self.writable()?;
        if key.is_empty() || key.len() > 1024 {
            return Err(s3_error!(InvalidArgument, "invalid object key length").into());
        }
        let _guard = self.coord.lock().await;
        self.writable()?;
        let input = crate::model::StreamCreate {
            authority: authority.clone(),
            bucket,
            key: key.into(),
            stream_type: stream_type.into(),
            metadata,
            public,
            claim_object,
            completion,
            owner,
        };
        let id = if self.role == "single" {
            self.stream_store.create(input).await?
        } else {
            self.cluster()?
                .create_stream(input, self.write_lease_seconds()?)
                .await?
        };
        Ok((id, self.pin(id)))
    }
    pub(crate) fn write_lease_seconds(&self) -> Result<i64> {
        Ok(config::seconds(&self.config.requests.upload_idle_timeout)?
            .max(30)
            .saturating_mul(2) as i64)
    }
    pub(crate) fn read_lease_seconds(&self) -> Result<i64> {
        Ok(config::seconds(&self.config.requests.read_idle_timeout)?
            .max(30)
            .saturating_mul(2) as i64)
    }
    pub(crate) async fn renew_write_lease(&self, stream: Id) -> Result<()> {
        if self.role != "single" {
            self.cluster()?
                .renew_write(stream, self.write_lease_seconds()?)
                .await?;
        }
        Ok(())
    }
    pub async fn tail(&self, stream: Id, offset: i64, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        self.renew_write_lease(stream).await?;
        let id = Uuid::now_v7();
        let (staging_storage_id, staging_object_key, _lease) =
            self.storage.write_fragment(id, data).await.map_err(|e| {
                tracing::warn!(error=%e,"multipart fragment could not be persisted");
                s3_error!(SlowDown, "local multipart storage unavailable")
            })?;
        self.stream_store
            .append_tail(crate::model::StreamTail {
                fragment: id,
                stream,
                offset,
                size: data.len() as i32,
                hash: *blake3::hash(data).as_bytes(),
                staging_storage: staging_storage_id,
                staging_key: staging_object_key,
                file: None,
                file_offset: None,
            })
            .await
    }

    pub(crate) async fn raw_tail(
        &self,
        stream: Id,
        upload: Uuid,
        offset: i64,
        data: &[u8],
    ) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        self.renew_write_lease(stream).await?;
        let lock = self.operation_lock(upload);
        let guard = lock.lock_owned().await;
        let file = self
            .multipart
            .file(upload, self.node.get().map(|node| node.node_id))
            .await?;
        let (position, _lease, _guard) = self
            .storage
            .disk
            .append_candidate(file, data, guard)
            .await
            .map_err(|error| {
                tracing::warn!(%error,"raw multipart file could not be persisted");
                s3_error!(SlowDown, "local multipart storage unavailable")
            })?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("multipart-file-synced").await;
        self.stream_store
            .append_tail(crate::model::StreamTail {
                fragment: Uuid::now_v7(),
                stream,
                offset,
                size: data.len() as i32,
                hash: *blake3::hash(data).as_bytes(),
                staging_storage: None,
                staging_key: None,
                file: Some(file),
                file_offset: Some(position),
            })
            .await
    }
}
