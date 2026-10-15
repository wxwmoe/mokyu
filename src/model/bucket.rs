use crate::app::{Bucket, StoredStream};
use crate::authorization::Principal;
use crate::model::task::Execution;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateBucket {
    pub name: String,
    pub project_id: Option<Uuid>,
    pub storage_id: Option<Uuid>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct BucketProject {
    pub(crate) id: Uuid,
    pub(crate) name: String,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct BucketStorage {
    pub(crate) id: Uuid,
    pub(crate) name: String,
    #[serde(rename = "type")]
    pub(crate) storage_type: String,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct Domain {
    pub(crate) host: String,
    pub(crate) bucket_id: Uuid,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub(crate) struct BucketImpact {
    pub bucket_id: Uuid,
    pub name: String,
    pub project_id: Uuid,
    #[serde(rename = "status")]
    pub state: String,
    pub revision: String,
    pub uploads_paused: bool,
    pub objects: String,
    pub logical_bytes: String,
    pub reserved_bytes: String,
    pub inflight_bytes: String,
    pub active_uploads: String,
    pub writing_streams: String,
    pub member_bucket_grants: String,
    pub service_grants: String,
    pub token_bucket_grants: String,
    pub domains: Vec<String>,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct TransferInput {
    pub(crate) target_project: Uuid,
    pub(crate) confirm_name: String,
    pub(crate) confirmation: String,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct TransferProject {
    pub(crate) id: Uuid,
    pub(crate) name: String,
    pub(crate) used_bytes: String,
    pub(crate) reserved_bytes: String,
    pub(crate) logical_bytes_limit: Option<String>,
    pub(crate) bucket_count: String,
    pub(crate) bucket_count_limit: Option<String>,
    pub(crate) members: String,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct BucketView {
    pub(crate) id: Uuid,
    pub(crate) project_id: Uuid,
    pub(crate) storage_id: Option<Uuid>,
    #[schema(value_type = Vec<crate::authorization::Action>)]
    pub(crate) actions: Vec<String>,
    pub(crate) name: String,
    #[serde(rename = "status")]
    pub(crate) state: String,
    pub(crate) revision: String,
    pub(crate) uploads_paused: bool,
    pub(crate) public_base_url: String,
    pub(crate) cors: Value,
    pub(crate) website_enabled: bool,
    pub(crate) index_document: String,
    pub(crate) error_document: String,
    pub(crate) created_at: DateTime<Utc>,
}

#[derive(Deserialize, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct Website {
    pub(crate) website_enabled: bool,
    pub(crate) index_document: String,
    pub(crate) error_document: String,
}

impl From<Bucket> for BucketView {
    fn from(bucket: Bucket) -> Self {
        Self {
            id: bucket.id,
            project_id: bucket.project_id,
            storage_id: bucket.storage_id,
            actions: crate::authorization::Action::ALL
                .iter()
                .map(|a| a.name().into())
                .collect(),
            name: bucket.name,
            state: bucket.state,
            revision: bucket.settings_revision.to_string(),
            uploads_paused: bucket.uploads_paused,
            public_base_url: bucket.public_base_url,
            cors: bucket.cors,
            website_enabled: bucket.website_enabled,
            index_document: bucket.index_document,
            error_document: bucket.error_document,
            created_at: bucket.created_at,
        }
    }
}

pub(crate) fn fingerprint(bucket: &BucketImpact, target: Option<Uuid>) -> Result<String> {
    Ok(blake3::hash(&serde_json::to_vec(&(bucket, target))?)
        .to_hex()
        .to_string())
}

pub(crate) struct BucketUpdate {
    pub revision: i64,
    pub cors: Value,
    pub website_enabled: bool,
    pub index_document: String,
    pub error_document: String,
    pub public_base_url: String,
    pub uploads_paused: bool,
    pub domains: Option<Vec<String>>,
}

#[async_trait]
pub(crate) trait BucketStore: Send + Sync {
    async fn domain_names(&self) -> Result<Vec<(String, String)>>;
    async fn purge_counts(&self, bucket: Uuid) -> Result<(i64, i64)>;
    async fn start_purge(
        &self,
        principal: &Principal,
        name: &str,
        id: Uuid,
        confirmation: Option<&str>,
    ) -> Result<Value>;
    async fn purge_uploads(&self, bucket: Uuid, default_batch: i32) -> Result<(i64, Vec<Uuid>)>;
    async fn purge_batch(
        &self,
        task: Execution,
        bucket: Uuid,
        cursor: Option<&str>,
        batch: i64,
        active: &[Uuid],
    ) -> Result<bool>;

    async fn by_name(&self, name: &str, active: bool) -> Result<Option<Bucket>>;
    async fn by_host(&self, host: &str) -> Result<Option<Bucket>>;
    async fn all(&self) -> Result<Vec<Bucket>>;
    async fn visible(
        &self,
        user: Uuid,
        admin: bool,
        token: Option<Uuid>,
    ) -> Result<Vec<BucketView>>;
    async fn list_objects(
        &self,
        bucket: Uuid,
        prefix: &str,
        key: &str,
        inclusive: bool,
    ) -> Result<Vec<StoredStream>>;
    async fn create(&self, principal: &Principal, input: CreateBucket) -> Result<Bucket>;
    async fn delete(
        &self,
        principal: &Principal,
        bucket: Uuid,
        name: &str,
        confirmation: Option<&str>,
    ) -> Result<()>;
    async fn domains(&self, principal: &Principal, bucket: Option<Uuid>) -> Result<Vec<Domain>>;
    async fn change_domain(
        &self,
        principal: &Principal,
        host: &str,
        bucket: Option<Uuid>,
    ) -> Result<()>;
    async fn settings(&self, bucket: Uuid) -> Result<(Bucket, Vec<String>)>;
    async fn save_settings(
        &self,
        principal: &Principal,
        bucket: Uuid,
        input: BucketUpdate,
    ) -> Result<()>;
    async fn creation_options(
        &self,
        admin: bool,
        user: Uuid,
        session: bool,
    ) -> Result<(Vec<BucketProject>, Vec<BucketStorage>)>;
    async fn impact(&self, principal: &Principal, bucket: Uuid) -> Result<BucketImpact>;
    async fn transfer_preview(
        &self,
        principal: &Principal,
        bucket: Uuid,
        target: Uuid,
    ) -> Result<(BucketImpact, TransferProject)>;
    async fn transfer(
        &self,
        principal: &Principal,
        bucket: Uuid,
        input: TransferInput,
    ) -> Result<Bucket>;
    async fn cors(&self, bucket: Uuid) -> Result<Value>;
    async fn set_cors(&self, principal: &Principal, bucket: Uuid, rules: Value) -> Result<Value>;
    async fn website(&self, bucket: Uuid) -> Result<Website>;
    async fn set_website(
        &self,
        principal: &Principal,
        bucket: Uuid,
        input: Website,
    ) -> Result<Website>;
}
