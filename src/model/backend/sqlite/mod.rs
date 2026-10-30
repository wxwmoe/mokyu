mod connection;
mod external_queries;
mod external_transactions;
mod migration;
mod queries;
mod replication_import_queries;
mod replication_import_transactions;
mod replication_queries;
mod replication_remote_queries;
mod replication_remote_transactions;
mod replication_transactions;
mod transactions;
mod transfer_queries;
mod transfer_transactions;

use sqlx::SqlitePool;
use std::{fs::File, path::PathBuf};

pub(crate) use connection::connect;

pub(crate) struct Backend {
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
