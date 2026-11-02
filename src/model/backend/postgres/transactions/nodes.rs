use super::*;

pub(in super::super) async fn set_deployment_mode(
    owner: &mut PgConnection,
    principal: &Principal,
    mode: &str,
) -> Result<()> {
    use crate::model::runtime::ModeConflict;
    use sqlx::Connection as _;
    let mut tx = owner.begin().await?;
    let current: String = queries::deployment_mode_lock().fetch_one(&mut *tx).await?;
    if current == mode {
        tx.rollback().await?;
        return Ok(());
    }
    let (maintenance, tasks, uploads, sources) =
        queries::mode_transition_state().fetch_one(&mut *tx).await?;
    crate::model::runtime::check_drain(maintenance, tasks, uploads, sources)?;
    if mode != "single" {
        let local: bool = queries::cluster_local_storages()
            .fetch_one(&mut *tx)
            .await?;
        ensure!(
            !local,
            ModeConflict(
                "local storages exist; migrate or remove every local storage before changing deployment mode"
            )
        );
        let staging: Option<uuid::Uuid> = queries::cluster_staging_id().fetch_one(&mut *tx).await?;
        let valid_staging: bool = queries::cluster_staging_valid(staging)
            .fetch_one(&mut *tx)
            .await?;
        ensure!(
            valid_staging,
            ModeConflict("cluster mode requires an enabled S3 uploads.staging_storage_id")
        );
    }
    if mode == "single" || current == "cluster" && mode == "single_control" {
        let local_node = match principal {
            Principal::Node { id, role } if role == "control" => Some(*id),
            _ => None,
        };
        let other_nodes: bool = queries::cluster_other_nodes(local_node, mode == "single_control")
            .fetch_one(&mut *tx)
            .await?;
        ensure!(
            !other_nodes,
            ModeConflict(if mode == "single" {
                "disable all registered cluster nodes before returning to single mode"
            } else {
                "release all other enabled control nodes before switching to single control"
            })
        );
    }
    if mode == "single"
        && let Principal::Node { id, role } = principal
        && role == "control"
    {
        queries::park_control(*id).execute(&mut *tx).await?;
        queries::release_node_tasks(id).execute(&mut *tx).await?;
        queries::release_node_permits(id).execute(&mut *tx).await?;
    }
    if current == "single" {
        queries::restore_parked_control().execute(&mut *tx).await?;
    }
    queries::preserve_failed_task_mode(&current)
        .execute(&mut *tx)
        .await?;
    queries::invalidate_mode_sessions()
        .execute(&mut *tx)
        .await?;
    queries::update_deployment_mode(mode)
        .execute(&mut *tx)
        .await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("mode-before-commit").await;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn release_node(
    backend: &super::super::Backend,
    name: &str,
) -> Result<()> {
    let mut tx = backend.pool.begin().await?;
    backend.fence_control(&mut tx).await?;
    let id: Uuid = queries::release_node_name(&name)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("node not found or already released"))?;
    if backend.control_epoch.is_some() {
        let (role, chunks, raw, parts, writes) =
            queries::retirement(id).fetch_one(&mut *tx).await?;
        ensure!(
            role != "control",
            crate::model::runtime::ModeConflict("the embedded control cannot be released")
        );
        ensure!(
            chunks + raw + parts + writes == 0,
            crate::model::node::PendingFiles {
                chunks,
                raw,
                parts,
                writes
            }
        );
    }
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
