use crate::{authorization::Permit, config};
use anyhow::Result;
use s3s::s3_error;
use serde_json::Value;
use uuid::Uuid;

use super::{Active, App, StoredStream};

impl App {
    pub async fn current(&self, bucket: Uuid, key: &str) -> Result<(StoredStream, Active)> {
        let _guard = self.coord.lock().await;
        self.ensure_mode()?;
        let (stream, pin) = if self.role == "single" {
            let stream = self.stream_store.current(bucket, key).await?;
            let pin = self.pin(stream.id);
            (stream, pin)
        } else {
            let ttl = self.read_lease_seconds()?;
            let (stream, lease_id) = self.cluster()?.current_stream(bucket, key, ttl).await?;
            let pin = self.pin_with_lease(
                stream.id,
                Some(super::ReadLease {
                    id: lease_id,
                    store: self.cluster.as_ref().unwrap().clone(),
                    ttl_seconds: ttl,
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
    ) -> Result<(Uuid, Active)> {
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
    pub(crate) async fn renew_write_lease(&self, stream: Uuid) -> Result<()> {
        if self.role != "single" {
            self.cluster()?
                .renew_write(stream, self.write_lease_seconds()?)
                .await?;
        }
        Ok(())
    }
    pub async fn tail(&self, stream: Uuid, offset: i64, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        self.renew_write_lease(stream).await?;
        let id = Uuid::new_v4();
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
            })
            .await
    }
}
