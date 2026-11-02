use crate::authorization::Principal;
use crate::model::Id;
use anyhow::Result;
use s3s::{dto::ETagCondition, s3_error};
use serde_json::Value;
use uuid::Uuid;

use super::App;

impl App {
    pub async fn finish_stream(
        &self,
        id: Id,
        size: i64,
        etag: &str,
        checksums: Value,
    ) -> Result<()> {
        self.sync_stream(id).await?;
        self.finish_received_stream(id, size, etag, checksums).await
    }
    pub(crate) async fn finish_received_stream(
        &self,
        id: Id,
        size: i64,
        etag: &str,
        checksums: Value,
    ) -> Result<()> {
        self.renew_write_lease(id).await?;
        if !self.stream_store.finish(id, size, etag, checksums).await? {
            return Err(s3_error!(OperationAborted).into());
        }
        Ok(())
    }
    pub async fn publish(
        &self,
        id: Id,
        object: (Uuid, &str),
        if_match: Option<&s3s::dto::ETagCondition>,
        if_none: Option<&s3s::dto::ETagCondition>,
    ) -> Result<()> {
        self.publish_complete(id, object, if_match, if_none, None)
            .await
    }
    pub async fn publish_complete(
        &self,
        id: Id,
        object: (Uuid, &str),
        if_match: Option<&s3s::dto::ETagCondition>,
        if_none: Option<&s3s::dto::ETagCondition>,
        completion: Option<(Uuid, String, Value, Uuid)>,
    ) -> Result<()> {
        self.renew_write_lease(id).await?;
        self.writable()?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("source-before-publish").await;
        let cache_change = self.public_reads.object_change(
            object.0,
            object.1,
            self.deployment_mode() == "single_control",
        );
        let result = {
            let _coord = self.coord.lock().await;
            self.writable()?;
            self.publish
                .publish(id, if_match.cloned(), if_none.cloned(), completion)
                .await
        };
        self.wait_publication(match result {
            Ok(true) => Ok(()),
            Ok(false) => Err(s3_error!(OperationAborted).into()),
            Err(error) => Err(error),
        })
        .await?;
        drop(cache_change);
        if self.deployment_mode() == "single_control" {
            self.notifications.committed();
        }
        if self
            .storage
            .buffered_writes
            .load(std::sync::atomic::Ordering::Acquire)
        {
            self.storage.wake_sync.notify_one();
        }
        self.wake_gc.notify_one();
        #[cfg(feature = "fault-injection")]
        crate::faults::point("object-before-seal").await;
        self.seal_uploads(id, true).await?;
        self.wake_raw_uploads.notify_one();
        self.work_queued();
        #[cfg(feature = "fault-injection")]
        crate::faults::point("object-published").await;
        Ok(())
    }
    pub(crate) async fn wait_publication(&self, result: Result<()>) -> Result<()> {
        let Err(error) = result else { return Ok(()) };
        let Some(pending) = error.downcast_ref::<crate::model::ReplicaPending>() else {
            return Err(error);
        };
        self.work_queued();
        tokio::time::timeout(crate::upload::multipart::completion_timeout(), async {
            let mut interval = std::time::Duration::from_millis(100);
            loop {
                self.ensure_mode()?;
                if self.publish.replicas_ready(pending.0).await? {
                    return Ok(());
                }
                tokio::time::sleep(interval).await;
                interval = (interval * 2).min(std::time::Duration::from_secs(2));
            }
        })
        .await
        .map_err(|_| s3s::s3_error!(RequestTimeout, "replica publication is still in progress"))?
    }

    pub async fn delete_object(
        &self,
        principal: &Principal,
        bucket: Uuid,
        key: &str,
    ) -> Result<()> {
        self.change_object(principal, bucket, key, None, None, None)
            .await
    }
    pub async fn change_object(
        &self,
        principal: &Principal,
        bucket: Uuid,
        key: &str,
        expected: Option<Id>,
        public: Option<bool>,
        if_match: Option<s3s::dto::ETagCondition>,
    ) -> Result<()> {
        self.writable()?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("management-before-change").await;
        let _coord = self.coord.lock().await;
        self.writable()?;
        let _cache = self.public_reads.object_change(
            bucket,
            key,
            self.deployment_mode() == "single_control",
        );
        if self
            .objects
            .change(crate::model::ObjectChange {
                principal: principal.clone(),
                bucket,
                key: key.into(),
                expected,
                public,
                if_match,
            })
            .await?
        {
            self.wake_gc.notify_one();
        }
        if self.deployment_mode() == "single_control" {
            self.notifications.committed();
        }
        Ok(())
    }
}
