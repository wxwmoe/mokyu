use super::*;

pub(in super::super) async fn task_page(
    pool: &PgPool,
    principal: &Principal,
    filter: crate::model::task::JobQuery,
    cursor: Option<(DateTime<Utc>, Uuid)>,
    limit: i64,
) -> Result<Vec<Value>> {
    let mut tx = pool.begin().await?;
    principal.lock_admin(&mut tx).await?;
    queries::task_page_limits().execute(&mut *tx).await?;
    let values = queries::task_page(filter, cursor, limit)
        .build_query_scalar()
        .fetch_all(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(values)
}

pub(in super::super) async fn task_view(
    pool: &PgPool,
    principal: &Principal,
    task: Uuid,
) -> Result<Value> {
    let mut tx = pool.begin().await?;
    principal.lock_admin(&mut tx).await?;
    let value = queries::task_view(task)
        .build_query_scalar()
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    tx.commit().await?;
    Ok(value)
}

pub(in super::super) async fn maintenance_mode(pool: &PgPool, enabled: bool) -> Result<()> {
    let mut tx = pool.begin().await?;
    queries::storage_mode_lock().fetch_one(&mut *tx).await?;
    if !enabled {
        ensure!(
            !queries::work::sweep_pending().fetch_one(&mut *tx).await?,
            "pause or finish destructive sweep and wait for its active batch before disabling maintenance"
        );
    }
    queries::set_maintenance_mode(enabled)
        .execute(&mut *tx)
        .await?;
    if enabled {
        queries::pause_maintenance_tasks().execute(&mut *tx).await?;
    }
    checkpoint_event(
        &mut tx,
        "maintenance.mode",
        "deployment",
        json!({"enabled":enabled}),
    )
    .await?;
    queries::notify_task("node-state".into())
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn finish_task(
    pool: &PgPool,
    input: crate::model::TaskFinish,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    let task = input.execution;
    let Some((task_type, state, parent)) = queries::lock_task_finish(task)
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(());
    };
    let failed = state == "running" && input.error.is_some();
    if failed && task_type == "storage_migration" {
        super::super::replica_transactions::failed(
            &mut tx,
            task.id,
            input.error.as_deref().unwrap(),
        )
        .await?;
    }
    if failed && matches!(task_type.as_str(), "pack" | "unpack") {
        queries::defer_pack_changes(task.id)
            .execute(&mut *tx)
            .await?;
        queries::defer_pack_maintenance(task.id)
            .execute(&mut *tx)
            .await?;
    }
    if failed && matches!(task_type.as_str(), "upload" | "cache_flush") {
        queries::backoff_pending_uploads(task.id, input.error.as_deref().unwrap())
            .execute(&mut *tx)
            .await?;
    }
    if (failed || state != "running")
        && matches!(
            task_type.as_str(),
            "pack" | "unpack" | "rewrite" | "upload" | "cache_flush"
        )
    {
        release_task_inputs(&mut tx, task.id).await?;
    }
    if failed {
        queries::fail_owned_task(task.id, input.error.as_deref().unwrap(), task.token)
            .execute(&mut *tx)
            .await?;
    } else if state == "running" {
        queries::requeue_owned_task(task.id, task.token)
            .execute(&mut *tx)
            .await?;
        // A control batch may have made the next stage executable by a node.
        queries::notify_work(task.id).execute(&mut *tx).await?;
    } else {
        queries::release_task_lease(task.id, task.token)
            .execute(&mut *tx)
            .await?;
        if state == "completed"
            && let Some(parent) = parent
        {
            queries::update_parent_progress(parent)
                .execute(&mut *tx)
                .await?;
        }
    }
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn sample_node_runtime(
    pool: &PgPool,
    state: &crate::cluster::NodeState,
    role: &str,
    key: &crate::content::codec::Key,
    sample: &crate::model::metrics::NodeSample,
) -> Result<bool> {
    sample.validate()?;
    let mut tx = pool.begin().await?;
    queries::runtime_diagnostic_limits()
        .execute(&mut *tx)
        .await?;
    validate_node(&mut tx, state, role, key).await?;
    let changed = queries::node_runtime_sample(state.node_id, sample)
        .execute(&mut *tx)
        .await?
        .rows_affected()
        == 1;
    tx.commit().await?;
    Ok(changed)
}
