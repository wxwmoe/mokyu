use crate::{app::StoredStream, authorization::Principal, model::Id};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize, utoipa::ToSchema, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
#[schema(as=TimeRestoreScope)]
pub(crate) enum Scope {
    Object,
    Prefix,
    Bucket,
}

#[derive(Clone, Debug, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as=TimeRestoreRequest)]
pub(crate) struct Request {
    pub at: chrono::DateTime<chrono::Utc>,
    pub scope: Scope,
    #[serde(default)]
    pub key: String,
    pub target_bucket: Uuid,
    #[serde(default)]
    pub target_prefix: String,
    #[serde(default)]
    pub in_place: bool,
}
impl Request {
    pub fn validate(&self, source: Uuid) -> Result<()> {
        ensure!(
            self.key.len() <= 1024
                && self.target_prefix.len() <= 1024
                && !self.key.contains('\0')
                && !self.target_prefix.contains('\0')
                && (matches!(self.scope, Scope::Bucket) == self.key.is_empty()),
            s3s::s3_error!(InvalidArgument)
        );
        if self.in_place {
            ensure!(
                self.target_bucket == source && self.target_prefix.is_empty(),
                s3s::s3_error!(InvalidArgument)
            );
        } else {
            ensure!(
                self.target_bucket != source || !self.target_prefix.is_empty(),
                s3s::s3_error!(InvalidArgument)
            );
        }
        Ok(())
    }
    pub fn target(&self, key: &str) -> Result<String> {
        let dest = format!("{}{key}", self.target_prefix);
        ensure!(dest.len() <= 1024, s3s::s3_error!(KeyTooLongError));
        Ok(dest)
    }
}
#[derive(Default, Serialize, Deserialize, utoipa::ToSchema)]
#[schema(as=BucketRecoveryWindow)]
pub(crate) struct Window {
    pub seconds: i64,
    pub earliest_us: Option<i64>,
    pub latest_us: i64,
    pub watermark: i64,
    pub retained_bytes: i64,
    pub gaps: Vec<Gap>,
}
#[derive(Clone, Serialize, Deserialize, utoipa::ToSchema)]
#[schema(as=RecoveryGap)]
pub(crate) struct Gap {
    pub after_us: i64,
    pub before_us: i64,
}
impl Gap {
    pub fn contains(&self, t: i64) -> bool {
        self.after_us < t && t < self.before_us
    }
}

#[derive(Clone, Serialize, Deserialize, utoipa::ToSchema, sqlx::FromRow)]
#[schema(as=TimeRestoreItem)]
pub(crate) struct Item {
    pub key: String,
    pub target: String,
    pub state: String,
    #[serde(skip)]
    pub source: Option<Id>,
    #[serde(skip)]
    pub expected: Option<Id>,
    #[serde(skip)]
    pub target_changed: bool,
}
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[schema(as=TimeRestorePlan)]
pub(crate) struct Plan {
    pub id: Uuid,
    pub source_bucket: Uuid,
    pub request: Request,
    pub watermark: i64,
    pub digest: String,
    pub expires_us: i64,
    pub phase: String,
    pub counts: std::collections::BTreeMap<String, i64>,
    pub items: Vec<Item>,
    pub next: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Step {
    pub plan: Uuid,
    pub token: Uuid,
    pub key: String,
}
pub(crate) struct Work {
    pub step: Step,
    pub actor: Principal,
    pub source: Uuid,
    pub target: Uuid,
    pub item: Item,
    pub stream: Option<StoredStream>,
}
#[derive(Debug)]
pub(crate) struct PreserveSource;
impl std::fmt::Display for PreserveSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("move requires an independent stream to preserve retained history")
    }
}
impl std::error::Error for PreserveSource {}
tokio::task_local! { pub(crate) static REMOVE: Step; }

#[async_trait::async_trait]
pub(crate) trait TimeRecovery: Send + Sync {
    async fn time_window(
        &self,
        actor: &Principal,
        bucket: Uuid,
        seconds: Option<i64>,
    ) -> Result<Window>;
    async fn time_preview(
        &self,
        actor: &Principal,
        bucket: Uuid,
        request: &Request,
    ) -> Result<Plan>;
    async fn time_plan(
        &self,
        actor: &Principal,
        id: Uuid,
        digest: Option<&str>,
        after: Option<&str>,
    ) -> Result<Plan>;
    async fn time_work(&self) -> Result<Option<Work>>;
    async fn time_renew(&self, step: &Step) -> Result<bool>;
    async fn time_conflict(&self, step: &Step) -> Result<()>;
    async fn time_expire(&self) -> Result<()>;
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn backup_gap_excludes_only_unverified_instants() {
        let gap = Gap {
            after_us: 120,
            before_us: 180,
        };
        assert!(!gap.contains(120) && !gap.contains(180));
        assert!(gap.contains(121) && gap.contains(179));
    }
    #[test]
    fn separate_destination_is_the_default() {
        let bucket = Uuid::new_v4();
        let mut q = Request {
            at: chrono::Utc::now(),
            scope: Scope::Bucket,
            key: String::new(),
            target_bucket: bucket,
            target_prefix: String::new(),
            in_place: false,
        };
        assert!(q.validate(bucket).is_err());
        q.target_prefix = "restore/".into();
        assert!(q.validate(bucket).is_ok());
        assert!(q.target(&"x".repeat(1024)).is_err());
        q.in_place = true;
        assert!(q.validate(bucket).is_err());
        q.target_prefix.clear();
        assert!(q.validate(bucket).is_ok());
    }
}
