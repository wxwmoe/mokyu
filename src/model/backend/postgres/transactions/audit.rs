use super::*;

pub(crate) async fn checkpoint_audit(
    db: &mut PgConnection,
    id: Id,
    action: &str,
    target: &str,
    project: Option<Uuid>,
    bucket: Option<Uuid>,
    detail: Value,
) -> Result<()> {
    let changed = queries::checkpoint_audit(id, action, target, project, bucket, detail)
        .execute(db)
        .await?
        .rows_affected();
    ensure!(changed == 1, "audit intent is missing");
    Ok(())
}

pub(crate) async fn partial_audit(db: &mut PgConnection, id: Id) -> Result<()> {
    queries::partial_audit(id).execute(db).await?;
    Ok(())
}

pub(super) async fn security_audit(
    db: &mut PgConnection,
    user: Uuid,
    action: &str,
    success: bool,
    request: &str,
) -> Result<()> {
    // Serialize rejected password events to keep the existing per-minute bound.
    queries::lock_audit_user(user).execute(&mut *db).await?;
    queries::security_audit(user, action, success, request)
        .execute(db)
        .await?;
    Ok(())
}
