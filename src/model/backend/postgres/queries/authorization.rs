use super::*;

pub(in super::super) async fn bucket_actions(
    principal: &crate::authorization::Principal,
    connection: &mut sqlx::PgConnection,
    bucket: Uuid,
    admin: bool,
) -> Result<Vec<Action>> {
    if admin {
        return Ok(Action::ALL.to_vec());
    }
    let actions = match principal {
        crate::authorization::Principal::Token { id, .. } => {
            let row: Option<Vec<String>> = sqlx::query_scalar(
                "SELECT actions FROM token_bucket_access WHERE token_id=$1 AND bucket_id=$2",
            )
            .bind(id)
            .bind(bucket)
            .fetch_optional(connection)
            .await?;
            Action::ALL
                .into_iter()
                .filter(|a| {
                    row.as_ref()
                        .is_some_and(|row| row.iter().any(|s| s == a.name()))
                })
                .collect()
        }
        crate::authorization::Principal::User { id, .. } => {
            let row: Option<Vec<String>> = sqlx::query_scalar(
                "SELECT actions FROM user_bucket_access WHERE bucket_id=$1 AND user_id=$2",
            )
            .bind(bucket)
            .bind(id)
            .fetch_optional(connection)
            .await?;
            Action::ALL
                .into_iter()
                .filter(|a| {
                    row.as_ref()
                        .is_some_and(|row| row.iter().any(|s| s == a.name()))
                })
                .collect()
        }
        crate::authorization::Principal::Service { access_key, .. } => {
            let row: Option<Vec<String>> = sqlx::query_scalar("SELECT g.actions FROM s3_credential_grants g JOIN buckets b ON b.id=g.bucket_id JOIN s3_credentials c ON c.access_key_id=g.access_key AND c.project_id=b.project_id WHERE g.access_key=$1 AND g.bucket_id=$2")
                .bind(access_key).bind(bucket).fetch_optional(connection).await?;
            Action::ALL
                .into_iter()
                .filter(|a| {
                    row.as_ref()
                        .is_some_and(|row| row.iter().any(|s| s == a.name()))
                })
                .collect()
        }
        crate::authorization::Principal::System
        | crate::authorization::Principal::Bootstrap
        | crate::authorization::Principal::Local
        | crate::authorization::Principal::Node { .. } => Action::ALL.to_vec(),
    };
    Ok(actions)
}

pub(in super::super) fn permission_bucket(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM buckets WHERE id=$1 FOR SHARE").bind(id)
}

#[async_trait::async_trait]
impl crate::model::AuthorizationStore for Backend {
    async fn admin(&self, principal: &Principal) -> Result<Box<dyn crate::model::AdminGuard>> {
        transactions::hold_admin(&self.pool, principal).await
    }

    async fn service(&self, access_key: &str) -> Result<crate::authorization::Principal> {
        let revision = sqlx::query_scalar(
            "SELECT authorization_revision FROM s3_credentials WHERE access_key_id=$1 AND enabled AND (expires_at IS NULL OR expires_at>now())",
        )
        .bind(access_key)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| s3_error!(AccessDenied))?;
        Ok(crate::authorization::Principal::Service {
            access_key: access_key.into(),
            revision,
        })
    }

    async fn actions(
        &self,
        principal: &crate::authorization::Principal,
        bucket: Uuid,
    ) -> Result<Vec<Action>> {
        let mut connection = self.pool.acquire().await?;
        let admin = validate_identity(principal, &mut connection, false).await?;
        bucket_actions(principal, &mut connection, bucket, admin).await
    }
}
