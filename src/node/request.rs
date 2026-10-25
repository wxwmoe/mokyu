use super::{
    RunLease,
    reads::{self, Held, Reads},
};
use crate::rpc::wire;
use anyhow::{Result, ensure};
use std::sync::Arc;
use tokio::sync::OwnedSemaphorePermit;

pub(crate) struct Request {
    pub(crate) reply: wire::RequestReply,
    pub(crate) access: String,
    pub(crate) bucket: String,
    pub(crate) query: Option<wire::prepare_request_input::Query>,
    pub(super) run: Arc<RunLease>,
    pub(super) materials: Held,
    pub(super) memory: Arc<OwnedSemaphorePermit>,
}

impl Request {
    pub(crate) async fn create_multipart(
        &self,
        reads: &Reads,
        mut input: wire::CreateMultipartRequest,
    ) -> Result<wire::MultipartReply> {
        let credential = self.authorize(&input.access_key, &input.bucket)?;
        input.run = Some(self.run.run.clone());
        input.credential_revision = Some(credential.revision as u64);
        input.operation_id = uuid::Uuid::now_v7().to_string();
        let reply = reads.node.client.create_multipart(input).await?;
        self.check()?;
        Ok(reply)
    }

    pub(crate) async fn abort_multipart(
        &self,
        reads: &Reads,
        mut input: wire::AbortMultipartRequest,
    ) -> Result<wire::MultipartReply> {
        let credential = self.authorize(&input.access_key, &input.bucket)?;
        input.run = Some(self.run.run.clone());
        input.credential_revision = Some(credential.revision as u64);
        input.operation_id = uuid::Uuid::now_v7().to_string();
        let reply = reads.node.client.abort_multipart(input).await?;
        self.check()?;
        Ok(reply)
    }
    pub(crate) fn check(&self) -> Result<()> {
        Ok(self.run.check()?)
    }

    pub(crate) fn material<T: serde::de::DeserializeOwned>(
        &self,
        version: &wire::Cached,
    ) -> Result<T> {
        reads::material(&self.materials, version)
    }

    pub(crate) fn authorize(
        &self,
        access: &str,
        bucket: &str,
    ) -> Result<crate::model::node_read::Credential> {
        self.check()?;
        ensure!(
            self.access == access && self.bucket == bucket,
            s3s::s3_error!(AccessDenied)
        );
        match wire::ReadOutcome::try_from(self.reply.outcome)? {
            wire::ReadOutcome::Ready => {}
            wire::ReadOutcome::NoSuchBucket => anyhow::bail!(s3s::s3_error!(NoSuchBucket)),
            _ => anyhow::bail!(s3s::s3_error!(AccessDenied)),
        }
        let credential: crate::model::node_read::Credential = self.material(
            self.reply
                .credential
                .as_ref()
                .ok_or_else(|| s3s::s3_error!(AccessDenied))?,
        )?;
        ensure!(
            credential.access == self.access && credential.revision >= 0,
            s3s::s3_error!(AccessDenied)
        );
        Ok(credential)
    }

    pub(crate) async fn change(
        &self,
        reads: &Reads,
        access: &str,
        bucket: &str,
        keys: Vec<String>,
        public: Option<bool>,
    ) -> Result<()> {
        let credential = self.authorize(access, bucket)?;
        let resolved: crate::app::Bucket = self.material(
            self.reply
                .bucket
                .as_ref()
                .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?,
        )?;
        let _cache: Vec<_> = keys
            .iter()
            .map(|key| reads.public.changing_object(resolved.id, key))
            .collect();
        let reply = reads
            .node
            .client
            .change_objects(wire::ChangeObjectsRequest {
                run: Some(self.run.run.clone()),
                operation_id: uuid::Uuid::now_v7().to_string(),
                access_key: access.to_owned(),
                credential_revision: Some(credential.revision as u64),
                bucket: bucket.to_owned(),
                keys: keys.clone(),
                public_read: public,
            })
            .await?;
        self.check()?;
        match wire::ObjectChangeOutcome::try_from(reply.outcome)? {
            wire::ObjectChangeOutcome::Applied => Ok(()),
            wire::ObjectChangeOutcome::ObjectChangeNoSuchKey => {
                Err(s3s::s3_error!(NoSuchKey).into())
            }
            _ => anyhow::bail!("invalid object change reply"),
        }
    }
}

pub(crate) async fn prepare(
    reads: &Reads,
    memory: Arc<OwnedSemaphorePermit>,
    access: String,
    bucket: String,
    query: Option<wire::prepare_request_input::Query>,
) -> Result<Request> {
    ensure!(
        query.is_none() || !access.is_empty(),
        s3s::s3_error!(AccessDenied)
    );
    let node = &reads.node;
    let snapshot = node.snapshot()?;
    let held = reads.cache.snapshot();
    let mut include = Vec::new();
    if !access.is_empty() {
        include.push(wire::MaterialType::S3Credential as i32);
    }
    if !bucket.is_empty() {
        include.extend([
            wire::MaterialType::Bucket as i32,
            wire::MaterialType::ReadPolicy as i32,
        ]);
    }
    ensure!(!include.is_empty(), s3s::s3_error!(AccessDenied));
    let mut reply = node
        .client
        .prepare_request(wire::PrepareRequestInput {
            run: Some(snapshot.lease.run.clone()),
            access_key: access.clone(),
            bucket: bucket.clone(),
            include,
            cached: reads::cached(&held),
            query: query.clone(),
        })
        .await?;
    let materials = reads.cache.apply(
        node.client.identity.deployment_id,
        held,
        std::mem::take(&mut reply.materials),
    )?;
    #[cfg(feature = "fault-injection")]
    if query.is_some() {
        crate::faults::point("node-catalog-prepared").await;
    }
    snapshot.lease.check()?;
    Ok(Request {
        reply,
        access,
        bucket,
        query,
        run: snapshot.lease.clone(),
        materials,
        memory,
    })
}
