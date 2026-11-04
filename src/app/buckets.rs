use crate::authorization::{Action, Permit, Principal};
use anyhow::Result;
use s3s::s3_error;
use serde_json::Value;
use std::{sync::Arc, time::Duration};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

use super::{App, Bucket};

impl App {
    pub async fn set_cors(
        &self,
        principal: &Principal,
        bucket: Uuid,
        rules: Value,
    ) -> Result<Value> {
        self.writable()?;
        crate::http::validate_cors(&rules).map_err(|e| s3_error!(InvalidArgument, "{e}"))?;
        let _cache = self.public_reads.changing();
        self.buckets.set_cors(principal, bucket, rules).await
    }
    pub async fn admit(
        &self,
        upload: bool,
    ) -> Result<(OwnedSemaphorePermit, OwnedSemaphorePermit)> {
        let semaphore = if upload { &self.uploads } else { &self.reads };
        let permit = semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|_| s3_error!(SlowDown))?;
        Ok((permit, self.data_slot()?))
    }

    fn data_slot(&self) -> Result<OwnedSemaphorePermit> {
        self.slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| s3_error!(SlowDown).into())
    }

    pub(crate) async fn admit_put(&self) -> Result<(OwnedSemaphorePermit, OwnedSemaphorePermit)> {
        let permit = put_permit(&self.uploads, &self.upload_waiters).await?;
        Ok((permit, self.data_slot()?))
    }
    pub async fn bucket(&self, name: &str, write: bool) -> Result<Bucket> {
        if write {
            self.writable()?;
        }
        let bucket = self
            .buckets
            .by_name(name, false)
            .await?
            .ok_or_else(|| s3_error!(NoSuchBucket))?;
        if write && bucket.state != "active" {
            return Err(s3_error!(OperationAborted, "bucket is being purged").into());
        }
        Ok(bucket)
    }
    pub async fn authorize(
        &self,
        key: Option<&str>,
        bucket: Uuid,
        action: Action,
    ) -> Result<Permit> {
        let key = key.ok_or_else(|| s3_error!(AccessDenied))?;
        Principal::service(self.authorization.as_ref(), key)
            .await?
            .require(self.authorization.as_ref(), bucket, action)
            .await
    }
}

async fn put_permit(uploads: &Arc<Semaphore>, waiters: &Semaphore) -> Result<OwnedSemaphorePermit> {
    if let Ok(permit) = uploads.clone().try_acquire_owned() {
        return Ok(permit);
    }
    let _waiter = waiters.try_acquire().map_err(|_| s3_error!(SlowDown))?;
    tokio::time::timeout(Duration::from_millis(25), uploads.clone().acquire_owned())
        .await
        .map_err(|_| s3_error!(SlowDown))?
        .map_err(|_| s3_error!(SlowDown).into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn put_waiters_are_bounded_and_released() {
        let uploads = Arc::new(Semaphore::new(1));
        let waiters = Semaphore::new(1);
        let active = put_permit(&uploads, &waiters).await.unwrap();
        let mut queued = Box::pin(put_permit(&uploads, &waiters));
        assert!(futures_util::poll!(queued.as_mut()).is_pending());
        assert_eq!(waiters.available_permits(), 0);
        let error = put_permit(&uploads, &waiters).await.unwrap_err();
        assert_eq!(
            error.downcast_ref::<s3s::S3Error>().unwrap().code(),
            &s3s::S3ErrorCode::SlowDown
        );
        drop(queued);
        assert_eq!(waiters.available_permits(), 1);

        assert!(put_permit(&uploads, &waiters).await.is_err());
        assert_eq!(waiters.available_permits(), 1);
        let mut next = Box::pin(put_permit(&uploads, &waiters));
        assert!(futures_util::poll!(next.as_mut()).is_pending());
        drop(active);
        let permit = next.await.unwrap();
        assert_eq!(waiters.available_permits(), 1);
        drop(permit);
        assert_eq!(uploads.available_permits(), 1);
    }
}
