use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Error {
    QuotaExceeded,
    OperationAborted,
    QueryCancelled,
    StorageProfileConflict,
    LocalStorageFeatures,
    LastAdministrator,
    BucketAlreadyExists,
    BucketNotEmpty,
    DomainInUse,
    BucketNotDrained,
    ObjectAlreadyExists,
    SourceUnavailable,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::QuotaExceeded => "storage quota exceeded",
            Self::OperationAborted => "operation was aborted",
            Self::QueryCancelled => "database query was cancelled",
            Self::StorageProfileConflict => "storage profile changed",
            Self::LocalStorageFeatures => "local storage does not support these features",
            Self::LastAdministrator => "the last administrator must remain enabled",
            Self::BucketAlreadyExists => "bucket already exists",
            Self::BucketNotEmpty => "bucket is not empty",
            Self::DomainInUse => "domain is already assigned to another bucket",
            Self::BucketNotDrained => "bucket must be paused and drained before transfer",
            Self::ObjectAlreadyExists => "object already exists",
            Self::SourceUnavailable => "no eligible durable source is available",
        })
    }
}

impl std::error::Error for Error {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model;

    #[test]
    fn model_errors_survive_context_without_matching_unrelated_errors() {
        for classification in [
            Error::QuotaExceeded,
            Error::OperationAborted,
            Error::QueryCancelled,
            Error::StorageProfileConflict,
            Error::LocalStorageFeatures,
            Error::LastAdministrator,
            Error::BucketAlreadyExists,
            Error::BucketNotEmpty,
            Error::DomainInUse,
            Error::BucketNotDrained,
            Error::ObjectAlreadyExists,
            Error::SourceUnavailable,
        ] {
            let source = std::io::Error::other("original database failure");
            let error = anyhow::Error::from(source)
                .context(classification)
                .context("writing object");
            assert_eq!(model::error(&error), Some(classification));
            assert!(error.downcast_ref::<std::io::Error>().is_some());
        }
        assert_eq!(model::error(&anyhow::anyhow!("MKQ01")), None);
        assert_eq!(model::error(&sqlx::Error::PoolTimedOut.into()), None);
    }
}
