use super::*;

pub(in super::super) async fn set_deployment_mode(
    pool: &PgPool,
    principal: &Principal,
    mode: &str,
) -> Result<()> {
    ensure!(
        matches!(mode, "single" | "cluster"),
        "mode must be single or cluster"
    );
    let mut tx = pool.begin().await?;
    let current: String = queries::deployment_mode_lock().fetch_one(&mut *tx).await?;
    if current == mode {
        tx.rollback().await?;
        return Ok(());
    }
    let (maintenance, tasks, uploads, sources) =
        queries::mode_transition_state().fetch_one(&mut *tx).await?;
    ensure!(
        maintenance,
        "enable maintenance before changing deployment mode"
    );
    ensure!(
        !tasks,
        "finish queued, running and paused tasks before changing deployment mode"
    );
    ensure!(
        !uploads,
        "finish or abort uploads and wait for reads to drain before changing deployment mode"
    );
    ensure!(
        !sources,
        "flush pending uploads and referenced fragments before changing deployment mode"
    );
    if mode == "cluster" {
        let local: bool = queries::cluster_local_storages()
            .fetch_one(&mut *tx)
            .await?;
        ensure!(
            !local,
            "local storages exist; migrate them to S3 and update their storage before enabling cluster mode"
        );
        let staging: Option<uuid::Uuid> = queries::cluster_staging_id().fetch_one(&mut *tx).await?;
        let valid_staging: bool = queries::cluster_staging_valid(staging)
            .fetch_one(&mut *tx)
            .await?;
        ensure!(
            valid_staging,
            "cluster mode requires an enabled S3 uploads.staging_storage_id"
        );
    } else {
        let local_node = match principal {
            Principal::Node { id, role } if role == "control" => Some(*id),
            _ => None,
        };
        let other_nodes: bool = queries::cluster_other_nodes(local_node)
            .fetch_one(&mut *tx)
            .await?;
        ensure!(
            !other_nodes,
            "disable all registered cluster nodes before returning to single mode"
        );
    }
    if mode == "single"
        && let Principal::Node { id, role } = principal
        && role == "control"
    {
        queries::release_node_id(id).execute(&mut *tx).await?;
        queries::release_node_tasks(id).execute(&mut *tx).await?;
        queries::release_node_permits(id).execute(&mut *tx).await?;
    }
    queries::preserve_failed_task_mode(&current)
        .execute(&mut *tx)
        .await?;
    queries::update_deployment_mode(&mode)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn release_node(pool: &PgPool, name: &str) -> Result<()> {
    let mut tx = pool.begin().await?;
    let id: Uuid = queries::release_node_name(&name)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("node not found or already released"))?;
    queries::release_node_tasks(&id).execute(&mut *tx).await?;
    queries::release_raw_uploads(id).execute(&mut *tx).await?;
    queries::release_node_permits(&id).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn heartbeat(
    pool: &PgPool,
    state: &crate::cluster::NodeState,
    role: &str,
    key: &crate::content::codec::Key,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    validate_node(&mut *tx, state, role, key).await?;
    queries::node_seen(state.node_id).execute(&mut *tx).await?;
    queries::notifications::renew(state.node_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn begin_report(
    pool: &PgPool,
    state: &crate::cluster::NodeState,
    role: &str,
    key: &crate::content::codec::Key,
    boot: Uuid,
    started: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    validate_node(&mut tx, state, role, key).await?;
    queries::begin_node_report(state.node_id, boot, started)
        .execute(&mut *tx)
        .await?;
    notifications::activate(
        &mut tx,
        crate::model::notification::Consumer {
            node: state.node_id,
            generation: boot,
        },
        role,
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn report_node(
    pool: &PgPool,
    state: &crate::cluster::NodeState,
    role: &str,
    key: &crate::content::codec::Key,
    report: &crate::model::node::NodeReport,
) -> Result<bool> {
    report.validate()?;
    let mut tx = pool.begin().await?;
    validate_node(&mut tx, state, role, key).await?;
    let written = queries::save_node_report(state.node_id, report)
        .execute(&mut *tx)
        .await?
        .rows_affected()
        == 1;
    tx.commit().await?;
    Ok(written)
}

pub(in super::super) async fn recover_node(pool: &PgPool, name: &str, role: &str) -> Result<()> {
    let mut tx = pool.begin().await?;
    let registered: Option<(Uuid, String)> = queries::recover_node_lock(&name)
        .fetch_optional(&mut *tx)
        .await?;
    if let Some((id, registered_role)) = registered {
        ensure!(
            registered_role == role,
            "registered node role does not match"
        );
        queries::release_node_id(&id).execute(&mut *tx).await?;
        queries::release_node_tasks(&id).execute(&mut *tx).await?;
        queries::release_raw_uploads(id).execute(&mut *tx).await?;
        queries::release_node_permits(&id).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}

struct Leadership {
    transaction: Transaction<'static, Postgres>,
}

#[async_trait::async_trait]
impl crate::model::cluster::Leader for Leadership {
    async fn finish(self: Box<Self>) -> Result<()> {
        self.transaction.commit().await?;
        Ok(())
    }
}

pub(in super::super) async fn cluster_leader(
    pool: &PgPool,
    key: i64,
) -> Result<Option<Box<dyn crate::model::cluster::Leader>>> {
    let mut transaction = pool.begin().await?;
    let locked: bool = queries::try_cluster_leader(key)
        .fetch_one(&mut *transaction)
        .await?;
    if locked {
        Ok(Some(
            Box::new(Leadership { transaction }) as Box<dyn crate::model::cluster::Leader>
        ))
    } else {
        transaction.rollback().await?;
        Ok(None)
    }
}
