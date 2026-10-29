mod connection;
mod external_queries;
mod external_transactions;
mod migration;
mod queries;
mod replication_queries;
mod replication_transactions;
mod transactions;

use sqlx::{MySqlConnection, MySqlPool};

pub(crate) use connection::connect;

pub(crate) fn error(source: &anyhow::Error) -> Option<crate::model::Error> {
    let error = source
        .downcast_ref::<sqlx::Error>()?
        .as_database_error()?
        .try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>()?;
    match error.number() {
        1205 | 1213 => Some(crate::model::Error::OperationAborted),
        1317 | 1969 | 3024 => Some(crate::model::Error::QueryCancelled),
        _ => None,
    }
}

pub(crate) struct Backend {
    database_info: crate::model::runtime::DatabaseInfo,
    pool: MySqlPool,
    owner: tokio::sync::Mutex<MySqlConnection>,
    upload_admission: tokio::sync::Mutex<()>,
}

impl Backend {
    async fn check_owner(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            queries::owns_runtime()
                .fetch_one(&mut *self.owner.lock().await)
                .await?,
            "database runtime ownership was lost"
        );
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../../tests/recovery/mysql_runtime.rs"]
mod tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/mysql_accounts.rs"]
mod account_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/mysql_settings.rs"]
mod settings_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/mysql_authorization.rs"]
mod authorization_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/mysql_administration.rs"]
mod administration_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/mysql_credentials.rs"]
mod credential_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/mysql_audit.rs"]
mod audit_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/mysql_storage.rs"]
mod storage_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/mysql_keys.rs"]
mod key_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/mysql_content.rs"]
mod content_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/mysql_quota.rs"]
mod quota_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/mysql_multipart.rs"]
mod multipart_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/mysql_buckets.rs"]
mod bucket_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/mysql_objects.rs"]
mod object_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/mysql_tasks.rs"]
mod task_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/mysql_cache.rs"]
mod cache_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/mysql_task_execution.rs"]
mod task_execution_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/mysql_pack_queues.rs"]
mod pack_queue_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/mysql_pack_publication.rs"]
mod pack_publication_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/mysql_pack_tasks.rs"]
mod pack_task_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/mysql_rewrite.rs"]
mod rewrite_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/mysql_packing.rs"]
mod packing_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/mysql_integrity.rs"]
mod integrity_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/mysql_maintenance_controls.rs"]
mod maintenance_control_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/mysql_maintenance.rs"]
mod maintenance_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/mysql_metrics.rs"]
mod metrics_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/mysql_usage.rs"]
mod usage_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/mysql_inventory.rs"]
mod inventory_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/mysql_startup.rs"]
mod startup_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/mysql_compatibility.rs"]
mod compatibility_tests;

#[cfg(test)]
#[path = "../../../../tests/recovery/mysql_bootstrap.rs"]
mod bootstrap_tests;
