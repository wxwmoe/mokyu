use crate::app::App;
use crate::model::{self, Error};
use anyhow::Result;
use uuid::Uuid;

pub(crate) fn database_error(error: &anyhow::Error) -> Option<s3s::S3Error> {
    match model::error(error)? {
        Error::QuotaExceeded => {
            let mut error = s3s::S3Error::new(s3s::S3ErrorCode::Custom("QuotaExceeded".into()));
            error.set_status_code(hyper::StatusCode::FORBIDDEN);
            Some(error)
        }
        Error::LocalStorageFeatures => {
            let mut error =
                s3s::S3Error::new(s3s::S3ErrorCode::Custom("LocalStorageFeatures".into()));
            error.set_status_code(hyper::StatusCode::BAD_REQUEST);
            Some(error)
        }
        Error::OperationAborted => Some(s3s::s3_error!(OperationAborted)),
        Error::SourceUnavailable => Some(s3s::s3_error!(InternalError)),
        reason @ (Error::LastAdministrator
        | Error::BucketAlreadyExists
        | Error::BucketNotEmpty
        | Error::DomainInUse
        | Error::BucketNotDrained
        | Error::ObjectAlreadyExists
        | Error::StorageNotDrained
        | Error::StorageProfileConflict) => {
            let code = match reason {
                Error::LastAdministrator => "LastAdministrator",
                Error::BucketAlreadyExists => "BucketAlreadyExists",
                Error::BucketNotEmpty => "BucketNotEmpty",
                Error::DomainInUse => "DomainInUse",
                Error::BucketNotDrained => "BucketNotDrained",
                Error::StorageProfileConflict => "StorageProfileConflict",
                Error::StorageNotDrained => "StorageNotDrained",
                Error::ObjectAlreadyExists => "ObjectAlreadyExists",
                _ => unreachable!(),
            };
            let mut error = s3s::S3Error::new(s3s::S3ErrorCode::Custom(code.into()));
            error.set_status_code(hyper::StatusCode::CONFLICT);
            Some(error)
        }
        _ => None,
    }
}

impl App {
    pub(crate) async fn reserve_quota(
        &self,
        stream: Uuid,
        size: i64,
        padding: bool,
        upload: Option<(Uuid, i32)>,
    ) -> Result<i64> {
        self.quota.reserve(stream, size, padding, upload).await
    }

    pub(crate) async fn complete_quota(&self, upload: Uuid, stream: Uuid, size: i64) -> Result<()> {
        self.quota.complete(upload, stream, size).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn domain_errors_keep_s3_codes_and_statuses() {
        let quota = database_error(&anyhow::Error::from(Error::QuotaExceeded)).unwrap();
        assert_eq!(quota.code().as_str(), "QuotaExceeded");
        assert_eq!(quota.status_code(), Some(hyper::StatusCode::FORBIDDEN));
        let aborted = database_error(
            &anyhow::Error::from(Error::OperationAborted).context("reserving quota"),
        )
        .unwrap();
        assert_eq!(aborted.code().as_str(), "OperationAborted");
        assert_eq!(aborted.status_code(), Some(hyper::StatusCode::CONFLICT));
        assert!(database_error(&anyhow::Error::from(Error::QueryCancelled)).is_none());
        let admin = database_error(&anyhow::Error::from(Error::LastAdministrator)).unwrap();
        assert_eq!(admin.code().as_str(), "LastAdministrator");
        assert_eq!(admin.status_code(), Some(hyper::StatusCode::CONFLICT));
        for (source, code) in [
            (Error::BucketAlreadyExists, "BucketAlreadyExists"),
            (Error::BucketNotEmpty, "BucketNotEmpty"),
            (Error::DomainInUse, "DomainInUse"),
            (Error::BucketNotDrained, "BucketNotDrained"),
            (Error::ObjectAlreadyExists, "ObjectAlreadyExists"),
            (Error::StorageProfileConflict, "StorageProfileConflict"),
            (Error::StorageNotDrained, "StorageNotDrained"),
        ] {
            let error = database_error(&source.into()).unwrap();
            assert_eq!(error.code().as_str(), code);
            assert_eq!(error.status_code(), Some(hyper::StatusCode::CONFLICT));
        }
    }
}
