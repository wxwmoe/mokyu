use super::{reader::Plan, transport::Renewal};
use crate::{
    model::{
        storage_native::access::{Read, ReadRequest},
        storage_transfer::SignedGrant,
    },
    rpc::{client::Client, wire},
};
use anyhow::Result;
use std::sync::Arc;
use uuid::Uuid;

pub(crate) struct Reader {
    pub client: Arc<Client>,
    pub run: wire::Run,
    pub access: Read,
    pub region: Option<String>,
}

fn read_error(status: tonic::Status) -> anyhow::Error {
    if status.code() == tonic::Code::NotFound
        && status
            .metadata()
            .get("mokyu-storage")
            .is_some_and(|value| value == "object-missing")
    {
        anyhow::Error::from(status).context(crate::model::Error::StorageObjectMissing)
    } else {
        status.into()
    }
}

impl Reader {
    pub async fn plan(
        &self,
        version: Option<Uuid>,
        lease: Option<crate::model::storage_native::ReadLease>,
    ) -> Result<Plan> {
        let reply = self
            .client
            .storage_read(wire::NativeReadRequest {
                run: Some(self.run.clone()),
                request_json: serde_json::to_vec(&ReadRequest {
                    access: self.access.clone(),
                    version,
                    lease,
                    region: self.region.clone(),
                })?,
            })
            .await
            .map_err(read_error)?;
        let plan: Plan = serde_json::from_slice(&reply.plan_json)?;
        plan.validate()?;
        anyhow::ensure!(
            plan.object.path == self.access.path
                && plan.range == self.access.range
                && version.is_none_or(|id| id == plan.object.version),
            "native read reply differs from its request"
        );
        Ok(plan)
    }
}

#[async_trait::async_trait]
impl Renewal for Reader {
    async fn renew(&self, grant: &SignedGrant) -> Result<SignedGrant> {
        let reply = self
            .client
            .storage_read_renewal(wire::NativeReadRenewal {
                run: Some(self.run.clone()),
                access_json: serde_json::to_vec(&self.access)?,
                grant_json: serde_json::to_vec(grant)?,
            })
            .await?;
        let next: SignedGrant = serde_json::from_slice(&reply.grant_json)?;
        grant.grant.renewal(&next.grant)?;
        Ok(next)
    }
}

pub(crate) struct Writer {
    pub client: Arc<Client>,
    pub run: wire::Run,
    pub input: crate::model::storage_native::access::Upload,
}

impl Writer {
    pub async fn prepare(
        &self,
        region: Option<String>,
    ) -> Result<crate::model::storage_payload::Intent> {
        let reply = self
            .client
            .storage_upload(wire::NativeUploadRequest {
                run: Some(self.run.clone()),
                input_json: serde_json::to_vec(&self.input)?,
                region,
            })
            .await?;
        let intent: crate::model::storage_payload::Intent =
            serde_json::from_slice(&reply.result_json)?;
        intent.validate()?;
        anyhow::ensure!(
            intent.namespace == self.input.storage
                && intent.total == self.input.size
                && intent.area == crate::content::fragment::Area::Blocks,
            "native upload reply differs from its request"
        );
        Ok(intent)
    }
}

#[async_trait::async_trait]
impl super::writer::Writes for Writer {
    async fn window(&self, operation: Uuid, sequence: u32) -> Result<Vec<super::control::Piece>> {
        let reply = self
            .client
            .storage_write_window(wire::NativeWriteWindow {
                run: Some(self.run.clone()),
                input_json: serde_json::to_vec(&self.input)?,
                operation_id: operation.to_string(),
                sequence,
            })
            .await?;
        Ok(serde_json::from_slice(&reply.result_json)?)
    }

    async fn accept(
        &self,
        operation: Uuid,
        completed: &[crate::model::storage_transfer::Completion],
    ) -> Result<()> {
        self.client
            .storage_accept(wire::NativeUploadResult {
                run: Some(self.run.clone()),
                input_json: serde_json::to_vec(&self.input)?,
                operation_id: operation.to_string(),
                receipts_json: serde_json::to_vec(completed)?,
            })
            .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Renewal for Writer {
    async fn renew(&self, grant: &SignedGrant) -> Result<SignedGrant> {
        let reply = self
            .client
            .storage_upload_renewal(wire::NativeReadRenewal {
                run: Some(self.run.clone()),
                access_json: serde_json::to_vec(&self.input)?,
                grant_json: serde_json::to_vec(grant)?,
            })
            .await?;
        let next: SignedGrant = serde_json::from_slice(&reply.grant_json)?;
        grant.grant.renewal(&next.grant)?;
        Ok(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_carrier_is_distinct_from_an_expired_operation() {
        let mut status = tonic::Status::not_found("missing");
        assert_eq!(crate::model::error(&read_error(status.clone())), None);
        status
            .metadata_mut()
            .insert("mokyu-storage", "object-missing".parse().unwrap());
        assert_eq!(
            crate::model::error(&read_error(status)),
            Some(crate::model::Error::StorageObjectMissing)
        );
    }
}
