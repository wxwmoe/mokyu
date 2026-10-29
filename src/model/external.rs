use crate::{
    authorization::{Action, Principal},
    storage::external::Connection,
};
use anyhow::{Result, ensure};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

pub(crate) const ACTIONS: [Action; 4] = [Action::List, Action::Read, Action::Write, Action::Delete];

#[derive(Serialize, ToSchema)]
pub(crate) struct ExternalBucket {
    pub id: Uuid,
    pub project_id: Uuid,
    pub name: String,
    pub connection: Connection,
    pub revision: i64,
    pub enabled: bool,
    pub actions: Vec<Action>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Serialize, ToSchema)]
#[schema(as = ExternalBucketPage)]
pub(crate) struct Page {
    pub items: Vec<ExternalBucket>,
    pub next: Option<Uuid>,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = CreateExternalBucket)]
pub(crate) struct Create {
    pub project_id: Uuid,
    pub name: String,
    pub connection: Connection,
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = ExternalBucketPatch)]
pub(crate) struct Patch {
    pub revision: i64,
    pub name: Option<String>,
    pub connection: Option<Connection>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub enabled: Option<bool>,
}

pub(crate) struct Record {
    pub bucket: ExternalBucket,
    pub access_encrypted: Vec<u8>,
    pub secret_encrypted: Vec<u8>,
}

pub(crate) struct Save {
    pub id: Uuid,
    pub expected: Option<i64>,
    pub project_id: Uuid,
    pub name: String,
    pub connection: Connection,
    pub access_encrypted: Vec<u8>,
    pub secret_encrypted: Vec<u8>,
    pub enabled: bool,
}

impl Save {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            !self.id.is_nil() && !self.project_id.is_nil(),
            "external resource identity is missing"
        );
        ensure!(
            self.expected.is_none_or(|v| v > 0),
            "invalid external revision"
        );
        super::credential::label(&self.name)?;
        ensure!(
            self.name.trim() == self.name,
            "external name is not trimmed"
        );
        self.connection.validate()?;
        ensure!(
            !self.access_encrypted.is_empty() && !self.secret_encrypted.is_empty(),
            "external credentials are missing"
        );
        Ok(())
    }
}

pub(crate) fn validate_grants(grants: &[super::administration::BucketGrant]) -> Result<()> {
    super::credential::validate_grants(grants)?;
    ensure!(
        grants
            .iter()
            .all(|grant| grant.actions.iter().all(|action| ACTIONS.contains(action))),
        s3s::s3_error!(
            InvalidArgument,
            "external grants only allow listing, reading, writing and deletion"
        )
    );
    Ok(())
}

#[async_trait]
pub(crate) trait ExternalStore: Send + Sync {
    async fn page(
        &self,
        principal: &Principal,
        project: Option<Uuid>,
        after: Option<Uuid>,
        limit: usize,
    ) -> Result<Page>;
    async fn view(&self, principal: &Principal, id: Uuid) -> Result<ExternalBucket>;
    async fn record(&self, principal: &Principal, id: Uuid) -> Result<Record>;
    async fn save(&self, principal: &Principal, input: Save) -> Result<ExternalBucket>;
    async fn delete(&self, principal: &Principal, id: Uuid, revision: i64) -> Result<()>;
    async fn start_test(
        &self,
        principal: &Principal,
        input: super::external_test::Start,
    ) -> Result<Uuid>;
    async fn latest_test(
        &self,
        principal: &Principal,
        id: Uuid,
    ) -> Result<Option<super::external_test::Test>>;
    async fn test(&self, principal: &Principal, id: Uuid) -> Result<super::external_test::Test>;
    async fn stop_test(&self, principal: &Principal, external: Uuid, id: Uuid) -> Result<()>;
    async fn prepare_test(
        &self,
        task: super::task::Execution,
    ) -> Result<super::external_test::Assignment>;
    async fn finish_test(
        &self,
        task: super::task::Execution,
        plan: &super::external_test::Plan,
        report: &super::external_test::Report,
    ) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn external_grants_reject_non_transfer_actions() {
        assert!(
            validate_grants(&[super::super::administration::BucketGrant {
                bucket_id: Uuid::now_v7(),
                actions: ACTIONS.to_vec()
            }])
            .is_ok()
        );
        let error = validate_grants(&[super::super::administration::BucketGrant {
            bucket_id: Uuid::now_v7(),
            actions: vec![Action::Settings],
        }])
        .unwrap_err();
        assert!(error.downcast_ref::<s3s::S3Error>().is_some());
    }
}
