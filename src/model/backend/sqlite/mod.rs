mod connection;
#[path = "../shared/embedded_native.rs"]
mod embedded_native;
#[path = "../shared/image.rs"]
mod image_transactions;
mod migration;
#[path = "../shared/native_parts.rs"]
mod native_parts;
#[path = "../shared/node_files.rs"]
mod node_files;
#[path = "../shared/node_inventory.rs"]
mod node_inventory;
#[path = "../shared/node_mutations.rs"]
mod node_mutations;
#[path = "../shared/node_reads.rs"]
mod node_reads;
#[path = "../shared/node_registry.rs"]
mod node_registry;
#[path = "../shared/node_requests.rs"]
mod node_requests;
#[path = "../shared/node_work.rs"]
mod node_work;
#[path = "../shared/node_writes.rs"]
mod node_writes;
#[path = "../shared/notices.rs"]
mod notices;
mod queries;
#[path = "../shared/raw_pages.rs"]
mod raw_pages;
#[path = "../shared/read_map.rs"]
mod read_map;
#[path = "../shared/read_reports.rs"]
mod read_reports;
#[path = "../shared/replica.rs"]
mod replica_transactions;
#[path = "../shared/staging.rs"]
mod staging;
#[path = "../shared/tier_access.rs"]
mod tier_access;
#[path = "../shared/tier_moves.rs"]
mod tier_move_transactions;
#[path = "../shared/tier.rs"]
mod tier_transactions;
mod transactions;
#[path = "../shared/work_audit.rs"]
mod work_audit;
#[path = "../shared/work_collect.rs"]
mod work_collect;
#[path = "../shared/work_conversion.rs"]
mod work_conversion;
#[path = "../shared/work_export.rs"]
mod work_export;
#[path = "../shared/work_import.rs"]
mod work_import;
#[path = "../shared/work_media.rs"]
mod work_media;
#[path = "../shared/work_migration.rs"]
mod work_migration;
#[path = "../shared/work_raw.rs"]
mod work_raw;
#[path = "../shared/work_replication.rs"]
mod work_replication;
#[path = "../shared/work_rewrite.rs"]
mod work_rewrite;
#[path = "../shared/work_sources.rs"]
mod work_sources;
#[path = "../shared/work_sweep.rs"]
mod work_sweep;

use sqlx::SqlitePool;
use std::{fs::File, path::PathBuf};

pub(crate) use connection::connect;

const CONTROL_SYNC: &str =
    "SQLite single control requires io.sqlite_synchronous=full. Save full and restart the primary";

pub(crate) struct Backend {
    read_reports: tokio::sync::Mutex<()>,
    integrity_rates: std::sync::Arc<crate::storage::rate::IntegrityRates>,
    control_epoch: Option<i64>,
    database_info: crate::model::runtime::DatabaseInfo,
    authorization: std::sync::Arc<tokio::sync::RwLock<()>>,
    previews: std::sync::Arc<tokio::sync::Mutex<()>>,
    read: SqlitePool,
    shared_pool: bool,
    write: SqlitePool,
    owner: File,
    lock_path: PathBuf,
    database_path: PathBuf,
    database_metadata: std::fs::Metadata,
}

impl Backend {
    fn check_owner(&self) -> anyhow::Result<()> {
        let held = self.owner.metadata()?;
        let current = self.lock_path.metadata()?;
        let database = self.database_path.metadata()?;
        anyhow::ensure!(database.is_file(), "SQLite database file was lost");
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            anyhow::ensure!(
                (held.dev(), held.ino()) == (current.dev(), current.ino()),
                "SQLite runtime lock was replaced"
            );
            anyhow::ensure!(
                database.nlink() == 1
                    && (self.database_metadata.dev(), self.database_metadata.ino())
                        == (database.dev(), database.ino()),
                "SQLite database file was replaced or hard-linked"
            );
        }
        #[cfg(not(unix))]
        anyhow::ensure!(
            held.is_file() && current.is_file(),
            "SQLite runtime lock was lost"
        );
        Ok(())
    }
}

pub(crate) fn error(source: &anyhow::Error) -> Option<crate::model::Error> {
    let native = source
        .downcast_ref::<sqlx::Error>()?
        .as_database_error()?
        .try_downcast_ref::<sqlx::sqlite::SqliteError>()?;
    let code = sqlx::error::DatabaseError::code(native)?
        .parse::<i32>()
        .ok()?;
    if code == 275
        && sqlx::error::DatabaseError::message(native)
            == "CHECK constraint failed: redundancy_queue_full"
    {
        return Some(crate::model::Error::RedundancyBacklog);
    }
    match code & 255 {
        5 | 6 => Some(crate::model::Error::OperationAborted),
        9 => Some(crate::model::Error::QueryCancelled),
        _ => None,
    }
}

#[cfg(test)]
#[path = "../../../../tests/recovery/sqlite_runtime.rs"]
mod tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/sqlite_accounts.rs"]
mod account_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/sqlite_settings.rs"]
mod settings_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/sqlite_durability.rs"]
mod durability_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/sqlite_authorization.rs"]
mod authorization_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/sqlite_storage.rs"]
mod storage_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/sqlite_administration.rs"]
mod administration_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/sqlite_credentials.rs"]
mod credential_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/sqlite_audit.rs"]
mod audit_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/sqlite_schema.rs"]
mod schema_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/sqlite_content.rs"]
mod content_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/sqlite_quota.rs"]
mod quota_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/sqlite_multipart.rs"]
mod multipart_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/sqlite_buckets.rs"]
mod bucket_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/sqlite_objects.rs"]
mod object_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/sqlite_keys.rs"]
mod key_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/sqlite_tasks.rs"]
mod task_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/sqlite_cache.rs"]
mod cache_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/sqlite_task_execution.rs"]
mod task_execution_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/sqlite_packing.rs"]
mod packing_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/sqlite_pack_publication.rs"]
mod pack_publication_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/sqlite_pack_queues.rs"]
mod pack_queue_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/sqlite_pack_tasks.rs"]
mod pack_task_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/sqlite_rewrite.rs"]
mod rewrite_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/sqlite_integrity.rs"]
mod integrity_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/sqlite_maintenance.rs"]
mod maintenance_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/sqlite_maintenance_controls.rs"]
mod maintenance_control_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/sqlite_metrics.rs"]
mod metrics_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/sqlite_usage.rs"]
mod usage_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/sqlite_startup.rs"]
mod startup_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/sqlite_files.rs"]
mod file_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/sqlite_bootstrap.rs"]
mod bootstrap_tests;
