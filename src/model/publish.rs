use crate::model::Id;
use anyhow::Result;
use async_trait::async_trait;
use s3s::dto::{ETag, ETagCondition};
use s3s::s3_error;
use serde_json::Value;
use uuid::Uuid;

/// The publish transaction is owned by the backend so authorization, object
/// locks, upload completion and audit checkpoints keep one lock order.
#[async_trait]
pub(crate) trait PublishStore: Send + Sync {
    async fn publish(
        &self,
        stream: Id,
        if_match: Option<ETagCondition>,
        if_none: Option<ETagCondition>,
        completion: Option<(Uuid, String, Value)>,
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
