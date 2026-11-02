use super::*;

pub(in super::super) async fn collect_usage(
    connection: &mut SqliteConnection,
    metrics: &crate::config::MetricsSettings,
    at: chrono::DateTime<chrono::Utc>,
    rows: Vec<(String, sqlx::types::Json<StorageUsage>)>,
) -> Result<()> {
    let retention = crate::config::seconds(&metrics.storage_retention)? as f64;
    queries::save_usage(connection, at, rows).await?;
    queries::expire_usage_scopes(retention)
        .execute(&mut *connection)
        .await?;
    queries::append_usage_history(crate::config::seconds(&metrics.storage_interval)? as f64)
        .execute(&mut *connection)
        .await?;
    queries::expire_usage_history(retention)
        .execute(connection)
        .await?;
    Ok(())
}

pub(in super::super) async fn read_runtime(
    pool: &SqlitePool,
    principal: &Principal,
) -> Result<RuntimeHistory> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut connection =
        super::super::connection::DeadlineConnection::acquire(pool, deadline).await?;
    let result = tokio::time::timeout_at(deadline, async {
        let mut tx = connection.begin().await?;
        ensure!(
            queries::validate_identity(principal, &mut tx).await?,
            s3s::s3_error!(AccessDenied)
        );
        let history = queries::runtime_history().fetch_all(&mut *tx).await?;
        let work = queries::runtime_work().fetch_all(&mut *tx).await?;
        let (pending_entries, pending_bytes, oldest_pending, failed_uploads) =
            queries::runtime_pending().fetch_one(&mut *tx).await?;
        tx.commit().await?;
        Ok(RuntimeHistory {
            history: history.into_iter().map(|v| v.0).collect(),
            work,
            pending_entries,
            pending_bytes,
            oldest_pending: oldest_pending.map(queries::datetime).transpose()?,
            failed_uploads,
        })
    })
    .await;
    match result {
        Ok(result) => {
            connection.release().await?;
            result
        }
        Err(_) => Err(crate::model::Error::QueryCancelled.into()),
    }
}

pub(in super::super) async fn sample_runtime(
    pool: &SqlitePool,
    point: RuntimePoint,
    retention: f64,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    queries::runtime_sample(point).execute(&mut *tx).await?;
    queries::runtime_expire(retention).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn flush_access(
    pool: &SqlitePool,
    pending: AccessBatch,
    stale: f64,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    queries::flush_chunk_access(&mut tx, &pending).await?;
    queries::flush_pack_access(&mut tx, &pending).await?;
    queries::flush_member_access(&mut tx, &pending).await?;
    super::super::tier_access::demand(&mut tx, &pending).await?;
    queries::access_coverage(pending.dropped, stale)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn expire_access(
    pool: &SqlitePool,
    retention: f64,
    batch: i64,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut connection =
        super::super::connection::DeadlineConnection::acquire(pool, deadline).await?;
    let result = tokio::time::timeout_at(deadline, async {
        let mut tx = connection.begin_with(queries::BEGIN_WRITE).await?;
        queries::expire_access(&mut tx, retention, batch).await?;
        tx.commit().await?;
        Ok(())
    })
    .await;
    match result {
        Ok(result) => {
            connection.release().await?;
            result
        }
        Err(_) => Err(crate::model::Error::QueryCancelled.into()),
    }
}

pub(in super::super) async fn require_sources(
    db: &mut sqlx::SqliteConnection,
    stream: Option<Id>,
    chunks: &[Id],
) -> Result<()> {
    use futures_util::TryStreamExt;
    // Lock all logical rows before any physical row; shared validation locks block quarantine.
    for phase in 0..3 {
        let mut query = queries::source_locks(stream, chunks, phase);
        let mut rows = query.build_query_scalar::<i64>().fetch(&mut *db);
        while rows.try_next().await?.is_some() {}
    }
    ensure!(
        !queries::unavailable_sources(stream, chunks)
            .build_query_scalar::<bool>()
            .fetch_one(&mut *db)
            .await?,
        crate::model::Error::SourceUnavailable
    );
    Ok(())
}

pub(in super::super) async fn read_source_snapshot(
    pool: &sqlx::SqlitePool,
    scope: &crate::model::source::SourceScope,
) -> Result<crate::model::source::SourceSnapshot> {
    let mut tx = pool.begin().await?;
    let snapshot = queries::source_snapshot(&mut tx, scope, false).await?;
    tx.commit().await?;
    Ok(snapshot)
}

pub(in super::super) async fn source_update(
    pool: &sqlx::SqlitePool,
    update: crate::model::source::SourceUpdate<'_>,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    source_update_in(&mut tx, update).await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn source_update_in(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    update: crate::model::source::SourceUpdate<'_>,
) -> Result<()> {
    let operation = update.operation;
    ensure!(
        queries::validate_identity(&operation.principal, tx).await?,
        s3s::s3_error!(AccessDenied)
    );
    ensure!(
        queries::source_mode(&operation.mode)
            .fetch_one(&mut **tx)
            .await?,
        crate::model::Error::OperationAborted
    );
    lock_execution(tx, update.execution).await?;
    let snapshot = queries::source_snapshot(tx, &operation.snapshot.scope, true).await?;
    ensure!(
        snapshot.state == "ready" && snapshot.fingerprint()? == operation.snapshot.fingerprint()?,
        crate::model::Error::OperationAborted
    );
    ensure!(
        snapshot.quarantine_code.is_none() == operation.code.is_some(),
        crate::model::Error::OperationAborted
    );
    ensure!(
        operation
            .code
            .as_deref()
            .is_none_or(crate::model::source::quarantine_code),
        s3s::s3_error!(InvalidArgument)
    );
    if update.apply {
        ensure!(
            queries::change_source(&snapshot, operation.code.as_deref())?
                .execute(&mut **tx)
                .await?
                .rows_affected()
                == 1,
            crate::model::Error::OperationAborted
        );
        let action = if operation.code.is_some() {
            "source.quarantine"
        } else {
            "source.recheck"
        };
        let detail = serde_json::json!({"source":snapshot.summary()?,"revision":(snapshot.revision+1).to_string(),"code":operation.code,"requested_by":operation.principal.user_id(),"token_id":operation.principal.token_id(),"task_id":update.execution.id});
        queries::source_result(update.execution.id, serde_json::json!({"result":detail}))
            .execute(&mut **tx)
            .await?;
        queries::complete_task(update.execution.id)
            .execute(&mut **tx)
            .await?;
        queries::source_audit(
            &operation.principal,
            action,
            &snapshot.location().to_string(),
            detail,
        )?
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}
