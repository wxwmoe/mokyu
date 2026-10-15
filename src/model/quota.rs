use anyhow::Result;
use async_trait::async_trait;
use uuid::Uuid;

/// Quota operations are a backend contract; each implementation owns its
/// reservation transaction and native SQL.
#[async_trait]
pub(crate) trait QuotaStore: Send + Sync {
    async fn read(&self, principal: &Principal, scope: &str, id: Uuid) -> Result<Quota>;
    async fn save(
        &self,
        principal: &Principal,
        scope: &str,
        id: Uuid,
        input: Limits,
    ) -> Result<Quota>;

    async fn reserve(
        &self,
        stream: Uuid,
        size: i64,
        padding: bool,
        upload: Option<(Uuid, i32)>,
    ) -> Result<i64>;

    async fn complete(&self, upload: Uuid, stream: Uuid, size: i64) -> Result<()>;
}

use crate::authorization::Principal;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Serialize, ToSchema)]
pub(crate) struct Quota {
    pub(crate) scope: String,
    pub(crate) scope_id: Uuid,
    pub(crate) used_bytes: Option<String>,
    pub(crate) reserved_bytes: Option<String>,
    pub(crate) inflight_bytes: Option<String>,
    pub(crate) object_count: Option<String>,
    pub(crate) bucket_count: Option<String>,
    pub(crate) logical_bytes_limit: Option<String>,
    pub(crate) inflight_bytes_limit: Option<String>,
    pub(crate) bucket_count_limit: Option<String>,
}
#[derive(Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct Limits {
    pub logical_bytes_limit: Option<String>,
    pub inflight_bytes_limit: Option<String>,
    pub bucket_count_limit: Option<String>,
}
pub(super) fn number(value: &Option<String>) -> Result<Option<i64>> {
    value
        .as_ref()
        .map(|value| {
            value
                .parse::<i64>()
                .ok()
                .filter(|n| *n >= 0)
                .ok_or_else(|| s3s::s3_error!(InvalidArgument).into())
        })
        .transpose()
}
pub(super) fn validate_scope(value: &str) -> Result<()> {
    if !matches!(value, "project" | "bucket") {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    Ok(())
}
