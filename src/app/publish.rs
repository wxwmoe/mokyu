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
        self.renew_write_lease(id).await?;
        if !self.stream_store.finish(id, size, etag, checksums).await? {
            return Err(s3_error!(OperationAborted).into());
        }
        Ok(())
    }
    pub async fn publish(
        &self,
        id: Id,
        if_match: Option<&s3s::dto::ETagCondition>,
        if_none: Option<&s3s::dto::ETagCondition>,
    ) -> Result<()> {
        self.publish_complete(id, if_match, if_none, None).await
    }
    pub async fn publish_complete(
        &self,
        id: Id,
        if_match: Option<&s3s::dto::ETagCondition>,
        if_none: Option<&s3s::dto::ETagCondition>,
        completion: Option<(Uuid, String, Value)>,
    ) -> Result<()> {
        self.renew_write_lease(id).await?;
        self.writable()?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("source-before-publish").await;
        let _coord = self.coord.lock().await;
        self.writable()?;
        if !self
            .publish
            .publish(id, if_match.cloned(), if_none.cloned(), completion)
            .await?
        {
            return Err(s3_error!(OperationAborted).into());
        }
        self.wake_gc.notify_one();
        self.seal_uploads(id, true).await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("object-published").await;
        Ok(())
    }
    pub async fn delete_object(
        &self,
        principal: &Principal,
        bucket: Uuid,
        key: &str,
    ) -> Result<()> {
        self.change_object(principal, bucket, key, None, None).await
    }
    pub async fn change_object(
        &self,
        principal: &Principal,
        bucket: Uuid,
        key: &str,
        expected: Option<Id>,
        public: Option<bool>,
    ) -> Result<()> {
        self.writable()?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("management-before-change").await;
        let _coord = self.coord.lock().await;
        self.writable()?;
        if self
            .objects
            .change(crate::model::ObjectChange {
                principal: principal.clone(),
                bucket,
                key: key.into(),
                expected,
                public,
            })
            .await?
        {
            self.wake_gc.notify_one();
        }
        Ok(())
    }
}
