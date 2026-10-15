use crate::authorization::{Action, Permit, Principal};
use anyhow::Result;
use s3s::s3_error;
use serde_json::Value;
use tokio::sync::OwnedSemaphorePermit;
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
        let slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| s3_error!(SlowDown))?;
        Ok((permit, slot))
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
