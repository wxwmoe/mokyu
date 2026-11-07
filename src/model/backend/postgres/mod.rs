pub(crate) const USER_LOCK: i64 = 734922709851002;

mod connection;
#[path = "../../shared/image.rs"]
mod image_transactions;
#[path = "../../shared/journal.rs"]
mod journal;
#[path = "../../shared/journal_restore.rs"]
mod journal_restore;
#[path = "../../shared/journal_snapshots.rs"]
mod journal_snapshots;
mod migration;
#[path = "../../shared/node/reads.rs"]
mod node_reads;
#[path = "../../shared/node/requests.rs"]
mod node_requests;
#[path = "../../shared/node/notices.rs"]
mod notices;
mod queries;
#[path = "../../shared/raw_pages.rs"]
mod raw_pages;
#[path = "../../shared/node/read_map.rs"]
mod read_map;
mod refresh;
#[path = "../../shared/replica.rs"]
mod replica_transactions;
#[path = "../../shared/tier_access.rs"]
mod tier_access;
#[path = "../../shared/tier_moves.rs"]
mod tier_move_transactions;
#[path = "../../shared/tier.rs"]
mod tier_transactions;
mod transactions;
#[path = "../../shared/work/audit.rs"]
mod work_audit;
#[path = "../../shared/work/sources.rs"]
mod work_sources;
#[path = "../../shared/work/sweep.rs"]
mod work_sweep;

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
    control_epoch: Option<(i64, i32)>,
    database_info: Option<crate::model::runtime::DatabaseInfo>,
    pool: PgPool,
    owner: tokio::sync::Mutex<PgConnection>,
    single: bool,
    refresh: Arc<refresh::Queue>,
    replica: Option<connection::Replica>,
}

impl Backend {
    pub(crate) fn new(pool: PgPool, owner: PgConnection, single: bool) -> Self {
        Self {
            control_epoch: None,
            database_info: None,
            pool,
            owner: tokio::sync::Mutex::new(owner),
            single,
            refresh: Arc::default(),
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
    let native = source.downcast_ref::<sqlx::Error>()?.as_database_error()?;
    if native.constraint() == Some("redundancy_queue_full") {
        return Some(Error::RedundancyBacklog);
    }
    sqlstate_error(&native.code()?)
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
#[path = "../../../../tests/recovery/node_sessions.rs"]
mod node_session_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/maintenance_schedule.rs"]
mod maintenance_schedule_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/notifications.rs"]
mod notification_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/node_reads.rs"]
mod node_read_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/node_writes.rs"]
mod node_write_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/node_chunks.rs"]
mod node_chunk_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/node_raw.rs"]
mod node_raw_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/node_catalog.rs"]
mod node_catalog_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/node_objects.rs"]
mod node_object_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/node_runtime.rs"]
mod node_runtime_tests;

fn stores(backend: Arc<Backend>) -> crate::model::Stores {
    crate::model::Stores {
        runtime: backend.clone(),
        integrity_rates: Default::default(),
        notices: if backend.control_epoch.is_some() {
            Arc::new(notices::Single(backend.clone()))
        } else {
            backend.clone()
        },
        staging: backend.clone(),
        storage_permits: backend.clone(),
        node_writes: backend.clone(),
        node_reads: backend.clone(),
        node_requests: backend.clone(),
        node_work: backend.clone(),
        node_sessions: backend.clone(),
        node_inventory: backend.clone(),
        cluster: Some(backend.clone()),
        metrics: backend.clone(),
        integrity: backend.clone(),
        maintenance: backend.clone(),
        packing: backend.clone(),
        physical: backend.clone(),
        upload_cache: backend.clone(),
        multipart: backend.clone(),
        buckets: backend.clone(),
        journal: backend.clone(),
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
        external_buckets: backend.clone(),
        replications: backend.clone(),
        redundancy: backend.clone(),
        image_styles: backend.clone(),
        tiering: backend.clone(),
        chunks: backend,
    }
}

#[cfg(test)]
#[path = "../../../../tests/recovery/node_multipart.rs"]
mod node_multipart_tests;
