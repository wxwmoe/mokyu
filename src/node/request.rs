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
    pub(crate) fn check(&self) -> Result<()> {
        Ok(self.run.check()?)
    }

    pub(crate) fn material<T: serde::de::DeserializeOwned>(
        &self,
        version: &wire::Cached,
    ) -> Result<T> {
        reads::material(&self.materials, version)
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
