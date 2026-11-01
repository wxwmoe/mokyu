use super::*;

pub(in super::super) async fn renew_read_lease(
    pool: &PgPool,
    id: uuid::Uuid,
    ttl_seconds: i64,
) -> Result<()> {
    let changed = extend_read_lease(id, ttl_seconds)
        .execute(pool)
        .await?
        .rows_affected();
    anyhow::ensure!(changed == 1, "read lease expired");
    Ok(())
}

pub(in super::super) async fn release_read_lease(pool: &PgPool, id: uuid::Uuid) -> Result<()> {
    delete_read_lease(id).execute(pool).await?;
    Ok(())
}

pub(in super::super) async fn renew_write_lease(
    pool: &PgPool,
    stream: Id,
    ttl_seconds: i64,
) -> Result<()> {
    let changed = extend_write_lease(stream, ttl_seconds)
        .execute(pool)
        .await?
        .rows_affected();
    anyhow::ensure!(changed == 1, "write lease expired");
    Ok(())
}

pub(in super::super) async fn validate_identity(
    principal: &crate::authorization::Principal,
    connection: &mut sqlx::PgConnection,
    lock: bool,
) -> Result<bool> {
    match principal {
        crate::authorization::Principal::System | crate::authorization::Principal::Bootstrap => {
            Ok(true)
        }
        crate::authorization::Principal::Local => {
            let query = if lock {
                "SELECT deployment_mode='single' FROM mokyu_meta WHERE singleton FOR SHARE"
            } else {
                "SELECT deployment_mode='single' FROM mokyu_meta WHERE singleton"
            };
            if sqlx::query_scalar(query).fetch_one(connection).await? {
                Ok(true)
            } else {
                Err(s3_error!(AccessDenied).into())
            }
        }
        crate::authorization::Principal::Node { id, role } => {
            let query = if lock {
                "SELECT role FROM nodes WHERE id=$1 AND enabled FOR SHARE"
            } else {
                "SELECT role FROM nodes WHERE id=$1 AND enabled"
            };
            let current: Option<String> = sqlx::query_scalar(query)
                .bind(id)
                .fetch_optional(&mut *connection)
                .await?;
            Ok(current.as_deref() == Some(role.as_str()) && role == "control")
        }
        crate::authorization::Principal::User {
            id,
            session,
            revision,
        } => {
            let query = if lock {
                "SELECT u.role,u.authorization_revision FROM web_users u JOIN sessions s ON s.user_id=u.id WHERE u.id=$1 AND s.id=$2 AND u.enabled AND NOT u.must_change_password AND s.expires_at>now() AND s.auth_revision=u.auth_revision FOR SHARE OF u"
            } else {
                "SELECT u.role,u.authorization_revision FROM web_users u JOIN sessions s ON s.user_id=u.id WHERE u.id=$1 AND s.id=$2 AND u.enabled AND NOT u.must_change_password AND s.expires_at>now() AND s.auth_revision=u.auth_revision"
            };
            let row: Option<(String, i64)> = sqlx::query_as(query)
                .bind(id)
                .bind(session)
                .fetch_optional(connection)
                .await?;
            match row {
                Some((role, current)) if current == *revision => Ok(role == "admin"),
                _ => Err(s3_error!(AccessDenied).into()),
            }
        }
        crate::authorization::Principal::Service {
            access_key,
            revision,
        } => {
            let query = if lock {
                "SELECT authorization_revision FROM s3_credentials WHERE access_key_id=$1 AND enabled AND (expires_at IS NULL OR expires_at>now()) FOR SHARE"
            } else {
                "SELECT authorization_revision FROM s3_credentials WHERE access_key_id=$1 AND enabled AND (expires_at IS NULL OR expires_at>now())"
            };
            let current: Option<i64> = sqlx::query_scalar(query)
                .bind(access_key)
                .fetch_optional(connection)
                .await?;
            if current == Some(*revision) {
                Ok(false)
            } else {
                Err(s3_error!(AccessDenied).into())
            }
        }
        crate::authorization::Principal::Token {
            id,
            user,
            revision,
            user_revision,
        } => {
            let user_query = if lock {
                "SELECT role,authorization_revision,auth_revision FROM web_users WHERE id=$1 AND enabled AND NOT must_change_password FOR SHARE"
            } else {
                "SELECT role,authorization_revision,auth_revision FROM web_users WHERE id=$1 AND enabled AND NOT must_change_password"
            };
            let current: Option<(String, i64, i64)> = sqlx::query_as(user_query)
                .bind(user)
                .fetch_optional(&mut *connection)
                .await?;
            let (role, current, auth) = current.ok_or_else(|| s3_error!(AccessDenied))?;
            if current != *user_revision {
                return Err(s3_error!(AccessDenied).into());
            }
            let token_query = if lock {
                "SELECT system,authorization_revision FROM api_tokens WHERE id=$1 AND user_id=$2 AND auth_revision=$3 AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at>now()) FOR SHARE"
            } else {
                "SELECT system,authorization_revision FROM api_tokens WHERE id=$1 AND user_id=$2 AND auth_revision=$3 AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at>now())"
            };
            let token: Option<(bool, i64)> = sqlx::query_as(token_query)
                .bind(id)
                .bind(user)
                .bind(auth)
                .fetch_optional(connection)
                .await?;
            match token {
                Some((system, current)) if current == *revision => Ok(system && role == "admin"),
                _ => Err(s3_error!(AccessDenied).into()),
            }
        }
    }
}
