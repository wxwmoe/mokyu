use crate::model::Id;
use anyhow::Result;
use async_trait::async_trait;
use s3s::dto::{ETag, ETagCondition};
use s3s::s3_error;
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug)]
pub(crate) struct ReplicaPending(pub Uuid);

impl std::fmt::Display for ReplicaPending {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "waiting for replica publication {}", self.0)
    }
}

impl std::error::Error for ReplicaPending {}

pub(crate) fn replica_rejection(error: &anyhow::Error) -> Option<&'static str> {
    if let Some(error) = error.downcast_ref::<s3s::S3Error>() {
        return match error.code() {
            s3s::S3ErrorCode::AccessDenied => Some("access_denied"),
            s3s::S3ErrorCode::PreconditionFailed => Some("precondition_failed"),
            s3s::S3ErrorCode::OperationAborted => Some("operation_aborted"),
            _ => None,
        };
    }
    match super::error(error) {
        Some(super::Error::QuotaExceeded) => Some("quota_exceeded"),
        Some(super::Error::OperationAborted | super::Error::NodeExpired) => {
            Some("operation_aborted")
        }
        _ => None,
    }
}

pub(crate) fn reject_replica(code: Option<&str>) -> anyhow::Error {
    match code {
        Some("access_denied") => s3_error!(AccessDenied).into(),
        Some("precondition_failed") => s3_error!(PreconditionFailed).into(),
        Some("quota_exceeded") => super::Error::QuotaExceeded.into(),
        _ => super::Error::OperationAborted.into(),
    }
}

#[derive(sqlx::FromRow)]
pub(crate) struct PublishingStream {
    #[sqlx(flatten)]
    pub stream: crate::app::StoredStream,
    pub publication_id: Option<Uuid>,
}

pub(crate) fn write_intent(
    if_match: Option<&ETagCondition>,
    if_none: Option<&ETagCondition>,
    completion: &Option<(Uuid, String, Value, Uuid)>,
) -> Result<Value> {
    let header =
        |value: &ETagCondition| -> Result<String> { Ok(value.to_http_header()?.to_str()?.into()) };
    Ok(
        serde_json::json!({"operation":"write","if_match":if_match.map(header).transpose()?,
        "if_none":if_none.map(header).transpose()?,"completion":completion}),
    )
}

/// The publish transaction is owned by the backend so authorization, object
/// locks, upload completion and audit checkpoints keep one lock order.
#[async_trait]
pub(crate) trait PublishStore: Send + Sync {
    async fn replicas_ready(&self, publication: Uuid) -> Result<bool>;
    async fn publish(
        &self,
        stream: Id,
        if_match: Option<ETagCondition>,
        if_none: Option<ETagCondition>,
        completion: Option<(Uuid, String, Value, Uuid)>,
    ) -> Result<bool>;
}

pub(crate) fn check_write_conditions(
    current: Option<&str>,
    if_match: Option<&ETagCondition>,
    if_none: Option<&ETagCondition>,
) -> Result<()> {
    let matches = |condition: &ETagCondition| match condition {
        ETagCondition::Any => current.is_some(),
        ETagCondition::ETag(etag) => {
            current.is_some_and(|value| ETag::Strong(value.into()).strong_cmp(etag))
        }
    };
    if if_match.is_some_and(|condition| !matches(condition)) || if_none.is_some_and(matches) {
        return Err(s3_error!(PreconditionFailed).into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conditional_write_rules_keep_strong_and_wildcard_semantics() {
        assert!(check_write_conditions(None, None, None).is_ok());
        let matching = ETagCondition::ETag(ETag::Strong("abc".into()));
        assert!(check_write_conditions(Some("abc"), Some(&matching), None).is_ok());
        assert!(check_write_conditions(None, Some(&matching), None).is_err());
        assert!(check_write_conditions(Some("abc"), None, Some(&ETagCondition::Any)).is_err());
        assert!(check_write_conditions(None, None, Some(&ETagCondition::Any)).is_ok());
    }
}
