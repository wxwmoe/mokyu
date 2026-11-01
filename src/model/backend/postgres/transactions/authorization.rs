use super::*;

impl Principal {
    pub(in super::super) async fn lock_identity(
        &self,
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<bool> {
        lock_identity(tx, self).await
    }
    pub(in super::super) async fn lock_admin(
        &self,
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<()> {
        if lock_identity(tx, self).await? {
            Ok(())
        } else {
            Err(s3s::s3_error!(AccessDenied).into())
        }
    }
}

impl Permit {
    pub(in super::super) async fn lock(&self, tx: &mut Transaction<'_, Postgres>) -> Result<()> {
        lock_permit(tx, self).await
    }
}

pub(super) async fn lock_execution(
    tx: &mut Transaction<'_, Postgres>,
    task: Execution,
) -> Result<()> {
    ensure!(
        queries::lock_task_execution(task.id, task.token)
            .fetch_optional(&mut **tx)
            .await?
            .is_some(),
        "task execution expired"
    );
    Ok(())
}

pub(in super::super) async fn change_task_state(
    pool: &PgPool,
    id: Uuid,
    parent: bool,
    resume: bool,
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    if parent {
        // Finish locks a child before its parent; controls use the same order.
        queries::lock_task_family(id).fetch_all(&mut *tx).await?;
    }
    if resume {
        queries::refresh_task_profiles(id, parent)
            .execute(&mut *tx)
            .await?;
    }
    let changed = queries::change_task_state(id, parent, resume)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    if changed == 0 {
        return Ok(false);
    }
    if parent && resume {
        queries::update_parent_progress(id)
            .execute(&mut *tx)
            .await?;
    }
    queries::notify_work(id).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(true)
}
