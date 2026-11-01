use super::*;

pub(in super::super) fn service_identity(access: &str) -> Scalar<'_, i64> {
    sqlx::query_scalar("SELECT authorization_revision FROM s3_credentials WHERE access_key_id=? AND enabled AND (expires_at IS NULL OR expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER))").bind(access)
}

pub(in super::super) fn action_bit(action: Action) -> i64 {
    match action {
        Action::List => 1,
        Action::Read => 2,
        Action::Write => 4,
        Action::Delete => 8,
        Action::Acl => 16,
        Action::Settings => 32,
        Action::Inspect => 64,
    }
}

pub(in super::super) fn action_bits(actions: &[Action]) -> i64 {
    actions
        .iter()
        .fold(0, |bits, action| bits | action_bit(*action))
}

pub(in super::super) fn actions(bits: i64) -> Vec<Action> {
    Action::ALL
        .into_iter()
        .filter(|action| bits & action_bit(*action) != 0)
        .collect()
}

pub(in super::super) async fn bucket_actions(
    principal: &Principal,
    connection: &mut sqlx::SqliteConnection,
    bucket: Uuid,
    admin: bool,
) -> Result<Vec<Action>> {
    if admin {
        return Ok(Action::ALL.to_vec());
    }
    let bits: Option<i64> = match principal {
        Principal::User { id, .. } => sqlx::query_scalar("SELECT actions FROM user_bucket_access WHERE user_id=? AND bucket_id=?").bind(id).bind(bucket).fetch_optional(connection).await?,
        Principal::Token { id, .. } => sqlx::query_scalar("SELECT actions FROM token_bucket_access WHERE token_id=? AND bucket_id=?").bind(id).bind(bucket).fetch_optional(connection).await?,
        Principal::Service { access_key, .. } => sqlx::query_scalar("SELECT g.actions FROM s3_credential_grants g JOIN buckets b ON b.id=g.bucket_id JOIN s3_credentials c ON c.access_key_id=g.access_key AND c.project_id=b.project_id WHERE g.access_key=? AND g.bucket_id=?").bind(access_key).bind(bucket).fetch_optional(connection).await?,
        Principal::Node { .. } => return Err(s3s::s3_error!(AccessDenied).into()),
        Principal::System | Principal::Bootstrap | Principal::Local => Some(127),
    };
    Ok(actions(bits.unwrap_or(0)))
}

pub(in super::super) fn permission_bucket(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM buckets WHERE id=?").bind(id)
}

#[async_trait::async_trait]
impl crate::model::AuthorizationStore for Backend {
    async fn admin(&self, principal: &Principal) -> Result<Box<dyn crate::model::AdminGuard>> {
        transactions::hold_admin(self, principal).await
    }
    async fn service(&self, access_key: &str) -> Result<Principal> {
        let revision = service_identity(access_key)
            .fetch_optional(&self.read)
            .await?
            .ok_or_else(|| s3s::s3_error!(AccessDenied))?;
        Ok(Principal::Service {
            access_key: access_key.into(),
            revision,
        })
    }
    async fn actions(&self, principal: &Principal, bucket: Uuid) -> Result<Vec<Action>> {
        let mut tx = self.read.begin().await?;
        let admin = validate_identity(principal, &mut tx).await?;
        let available = bucket_actions(principal, &mut tx, bucket, admin).await?;
        tx.commit().await?;
        Ok(available)
    }
}
