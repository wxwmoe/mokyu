use super::*;

pub(in super::super) async fn create_write_lease(
    tx: &mut Transaction<'_, Postgres>,
    stream: Id,
    ttl_seconds: i64,
) -> Result<()> {
    queries::create_write_lease(stream, ttl_seconds)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub(crate) async fn lock_identity(
    db: &mut PgConnection,
    principal: &crate::authorization::Principal,
) -> Result<bool> {
    queries::validate_identity(principal, db, true).await
}

pub(crate) async fn lock_permit(
    tx: &mut PgConnection,
    permit: &crate::authorization::Permit,
) -> Result<()> {
    let admin = queries::validate_identity(&permit.principal, tx, true).await?;
    for (bucket, required) in &permit.resources {
        let exists: Option<Uuid> = queries::permission_bucket(*bucket)
            .fetch_optional(&mut *tx)
            .await?;
        if exists.is_none() {
            return Err(s3s::s3_error!(AccessDenied).into());
        }
        let available = queries::bucket_actions(&permit.principal, tx, *bucket, admin).await?;
        if required.iter().any(|a| !available.contains(a)) {
            return Err(s3s::s3_error!(AccessDenied).into());
        }
        if required.iter().any(|a| {
            matches!(
                a,
                crate::authorization::Action::Write
                    | crate::authorization::Action::Delete
                    | crate::authorization::Action::Settings
                    | crate::authorization::Action::Acl
            )
        }) {
            ensure!(
                !queries::journal_restore::native_read_only(*bucket)
                    .fetch_one(&mut *tx)
                    .await?,
                s3s::s3_error!(ServiceUnavailable, "storage takeover is not complete")
            );
        }
    }
    Ok(())
}
