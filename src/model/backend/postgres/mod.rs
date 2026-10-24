pub(crate) const USER_LOCK: i64 = 734922709851002;

mod connection;
mod migration;
mod queries;
mod transactions;

#[cfg(test)]
#[path = "../../../../tests/recovery/postgres_compression.rs"]
mod compression_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/postgres_raw.rs"]
mod raw_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/task_claim.rs"]
mod task_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/task_execution.rs"]
mod execution_tests;

use crate::model::Error;
use sqlx::{PgConnection, PgPool};
use std::sync::Arc;

pub(crate) use connection::connect;

pub(crate) struct Backend {
    database_info: Option<crate::model::runtime::DatabaseInfo>,
    pool: PgPool,
    owner: tokio::sync::Mutex<PgConnection>,
    single: bool,
    replica: Option<connection::Replica>,
}

impl Backend {
    pub(crate) fn new(pool: PgPool, owner: PgConnection, single: bool) -> Self {
        Self {
            database_info: None,
            pool,
            owner: tokio::sync::Mutex::new(owner),
            single,
            replica: None,
        }
    }
}

fn sqlstate_error(code: &str) -> Option<Error> {
    match code {
        "MKQ01" => Some(Error::QuotaExceeded),
        "MKQ02" => Some(Error::OperationAborted),
        "57014" => Some(Error::QueryCancelled),
        _ => None,
    }
}

pub(crate) fn error(source: &anyhow::Error) -> Option<Error> {
    let code = source
        .downcast_ref::<sqlx::Error>()?
        .as_database_error()?
        .code()?;
    sqlstate_error(&code)
}

fn normalize_error(error: anyhow::Error) -> anyhow::Error {
    match self::error(&error) {
        Some(classification) => error.context(classification),
        None => error,
    }
}

#[cfg(test)]
mod error_tests {
    use super::*;

    #[test]
    fn postgres_sqlstates_keep_existing_error_semantics() {
        assert_eq!(sqlstate_error("MKQ01"), Some(Error::QuotaExceeded));
        assert_eq!(sqlstate_error("MKQ02"), Some(Error::OperationAborted));
        assert_eq!(sqlstate_error("57014"), Some(Error::QueryCancelled));
        assert_eq!(sqlstate_error("23505"), None);
        assert_eq!(sqlstate_error("P0001"), None);
    }
}

#[cfg(test)]
#[path = "../../../../tests/recovery/upload_cache.rs"]
mod cache_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/storage_staging.rs"]
mod staging_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/lifecycle.rs"]
mod lifecycle_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/chunk_claim.rs"]
mod chunk_claim_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/rewrite.rs"]
mod rewrite_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/postgres_bootstrap.rs"]
mod bootstrap_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/node_reports.rs"]
mod node_report_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/node_runtime.rs"]
mod node_runtime_tests;

fn stores(backend: Arc<Backend>) -> crate::model::Stores {
    crate::model::Stores {
        runtime: backend.clone(),
        cluster: Some(backend.clone()),
        metrics: backend.clone(),
        integrity: backend.clone(),
        maintenance: backend.clone(),
        packing: backend.clone(),
        physical: backend.clone(),
        upload_cache: backend.clone(),
        multipart: backend.clone(),
        buckets: backend.clone(),
        credentials: backend.clone(),
        administration: backend.clone(),
        accounts: backend.clone(),
        authorization: backend.clone(),
        audit: backend.clone(),
        objects: backend.clone(),
        publish: backend.clone(),
        quota: backend.clone(),
        streams: backend.clone(),
        raw: backend.clone(),
        task_control: backend.clone(),
        task_execution: backend.clone(),
        storage_profiles: backend.clone(),
        runtime_settings: backend.clone(),
        managed_keys: backend.clone(),
        storage_records: backend.clone(),
        chunks: backend,
    }
}
