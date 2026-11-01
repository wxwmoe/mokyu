use super::*;

pub(in super::super) fn update_credential<'q>(
    access: &'q str,
    input: &'q CredentialSettings,
    expires: Option<DateTime<Utc>>,
) -> Statement<'q> {
    sqlx::query("UPDATE s3_credentials SET label=?,enabled=?,expires_at=IF(?,expires_at,?),authorization_revision=authorization_revision+1 WHERE access_key_id=?").bind(input.label.trim()).bind(input.enabled).bind(input.keep_expiry).bind(expires).bind(access)
}

pub(in super::super) fn lock_credential_project(access: &str) -> Scalar<'_, Uuid> {
    sqlx::query_scalar("SELECT project_id FROM s3_credentials WHERE access_key_id=? FOR UPDATE")
        .bind(access)
}

pub(in super::super) fn delete_credential(access: &str) -> Statement<'_> {
    sqlx::query("DELETE FROM s3_credentials WHERE access_key_id=?").bind(access)
}

pub(in super::super) fn enable_credential(access: &str, enabled: bool) -> Statement<'_> {
    sqlx::query("UPDATE s3_credentials SET enabled=?,authorization_revision=authorization_revision+1 WHERE access_key_id=?").bind(enabled).bind(access)
}

pub(in super::super) fn lock_credential(access: &str) -> Statement<'_> {
    sqlx::query("SELECT access_key_id FROM s3_credentials WHERE access_key_id=? FOR UPDATE")
        .bind(access)
}

pub(in super::super) fn expire_credential(access: &str, expires: DateTime<Utc>) -> Statement<'_> {
    sqlx::query("UPDATE s3_credentials SET expires_at=?,authorization_revision=authorization_revision+1 WHERE access_key_id=?").bind(expires).bind(access)
}

pub(in super::super) fn token_record(id: Uuid) -> Rows<'static, Token> {
    sqlx::query_as("SELECT t.*,u.enabled AND NOT u.must_change_password AND t.auth_revision=u.auth_revision AND t.revoked_at IS NULL AND (t.expires_at IS NULL OR t.expires_at>CURRENT_TIMESTAMP(6)) AS active,COALESCE((SELECT JSON_ARRAYAGG(JSON_ARRAY(HEX(g.bucket_id),g.actions)) FROM token_bucket_grants g WHERE g.token_id=t.id),JSON_ARRAY()) AS grants,COALESCE((SELECT JSON_ARRAYAGG(JSON_ARRAY(HEX(g.bucket_id),g.actions)) FROM token_external_grants g WHERE g.token_id=t.id),JSON_ARRAY()) AS external_grants FROM api_tokens t JOIN web_users u ON u.id=t.user_id WHERE t.id=?").bind(id)
}

pub(in super::super) fn token_records(
    user: Uuid,
    after: Option<Uuid>,
    limit: i64,
) -> Rows<'static, Token> {
    sqlx::query_as("SELECT t.*,u.enabled AND NOT u.must_change_password AND t.auth_revision=u.auth_revision AND t.revoked_at IS NULL AND (t.expires_at IS NULL OR t.expires_at>CURRENT_TIMESTAMP(6)) AS active,COALESCE((SELECT JSON_ARRAYAGG(JSON_ARRAY(HEX(g.bucket_id),g.actions)) FROM token_bucket_grants g WHERE g.token_id=t.id),JSON_ARRAY()) AS grants,COALESCE((SELECT JSON_ARRAYAGG(JSON_ARRAY(HEX(g.bucket_id),g.actions)) FROM token_external_grants g WHERE g.token_id=t.id),JSON_ARRAY()) AS external_grants FROM api_tokens t JOIN web_users u ON u.id=t.user_id WHERE t.user_id=? AND (? IS NULL OR t.id>?) ORDER BY t.id LIMIT ?").bind(user).bind(after).bind(after).bind(limit)
}

pub(in super::super) fn lock_token_owner(user: Uuid) -> Statement<'static> {
    sqlx::query("SELECT id FROM web_users WHERE id=? LOCK IN SHARE MODE").bind(user)
}

pub(in super::super) fn token_owner_revision(user: Uuid) -> Rows<'static, (String, i64)> {
    sqlx::query_as("SELECT role,auth_revision FROM web_users WHERE id=? AND enabled AND NOT must_change_password").bind(user)
}

pub(in super::super) fn token_owner_actions(user: Uuid, bucket: Uuid) -> Scalar<'static, u64> {
    sqlx::query_scalar("SELECT actions FROM user_bucket_access WHERE user_id=? AND bucket_id=?")
        .bind(user)
        .bind(bucket)
}

pub(in super::super) fn delete_token_grants(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM token_bucket_grants WHERE token_id=?").bind(id)
}

pub(in super::super) fn insert_token_grant(id: Uuid, grant: &BucketGrant) -> Statement<'static> {
    sqlx::query("INSERT INTO token_bucket_grants(token_id,bucket_id,actions) VALUES(?,?,?)")
        .bind(id)
        .bind(grant.bucket_id)
        .bind(action_bits(&grant.actions))
}

pub(in super::super) fn advance_token_authorization(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE api_tokens SET authorization_revision=authorization_revision+1 WHERE id=?")
        .bind(id)
}

pub(in super::super) fn insert_token<'q>(
    id: Uuid,
    user: Uuid,
    input: &'q TokenInput,
    material: &'q TokenMaterial,
    revision: i64,
    expires: Option<DateTime<Utc>>,
) -> Statement<'q> {
    sqlx::query("INSERT INTO api_tokens(id,user_id,label,prefix,token_hash,system_access,auth_revision,expires_at) VALUES(?,?,?,?,?,?,?,?)").bind(id).bind(user).bind(input.label.trim()).bind(&material.prefix).bind(material.hash.as_slice()).bind(input.system).bind(revision).bind(expires)
}

pub(in super::super) fn lock_token(id: Uuid) -> Statement<'static> {
    sqlx::query("SELECT id FROM api_tokens WHERE id=? FOR UPDATE").bind(id)
}

pub(in super::super) fn update_token<'q>(
    id: Uuid,
    input: &'q TokenInput,
    expires: Option<DateTime<Utc>>,
) -> Statement<'q> {
    sqlx::query("UPDATE api_tokens SET label=?,system_access=?,expires_at=IF(?,expires_at,?),authorization_revision=authorization_revision+1 WHERE id=?").bind(input.label.trim()).bind(input.system).bind(input.keep_expiry).bind(expires).bind(id)
}

pub(in super::super) fn token_user(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT user_id FROM api_tokens WHERE id=?").bind(id)
}

pub(in super::super) fn revoke_token(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE api_tokens SET revoked_at=COALESCE(revoked_at,CURRENT_TIMESTAMP(6)),authorization_revision=authorization_revision+1 WHERE id=?").bind(id)
}

pub(in super::super) fn user_identity(
    user: Uuid,
    session: Uuid,
    lock: bool,
) -> Rows<'static, (String, i64)> {
    let sql = if lock {
        "SELECT u.role,u.authorization_revision FROM web_users u WHERE u.id=? AND EXISTS(SELECT 1 FROM sessions s WHERE s.user_id=u.id AND s.id=? AND s.expires_at>CURRENT_TIMESTAMP(6) AND s.auth_revision=u.auth_revision) AND u.enabled AND NOT u.must_change_password LOCK IN SHARE MODE"
    } else {
        "SELECT u.role,u.authorization_revision FROM web_users u JOIN sessions s ON s.user_id=u.id WHERE u.id=? AND s.id=? AND u.enabled AND NOT u.must_change_password AND s.expires_at>CURRENT_TIMESTAMP(6) AND s.auth_revision=u.auth_revision"
    };
    sqlx::query_as(sql).bind(user).bind(session)
}

pub(in super::super) fn token_owner_identity(
    user: Uuid,
    lock: bool,
) -> Rows<'static, (String, i64, i64)> {
    let sql = if lock {
        "SELECT role,authorization_revision,auth_revision FROM web_users WHERE id=? AND enabled AND NOT must_change_password LOCK IN SHARE MODE"
    } else {
        "SELECT role,authorization_revision,auth_revision FROM web_users WHERE id=? AND enabled AND NOT must_change_password"
    };
    sqlx::query_as(sql).bind(user)
}

pub(in super::super) fn token_identity(
    token: Uuid,
    user: Uuid,
    auth: i64,
    lock: bool,
) -> Rows<'static, (bool, i64)> {
    let sql = if lock {
        "SELECT system_access,authorization_revision FROM api_tokens WHERE id=? AND user_id=? AND auth_revision=? AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at>CURRENT_TIMESTAMP(6)) LOCK IN SHARE MODE"
    } else {
        "SELECT system_access,authorization_revision FROM api_tokens WHERE id=? AND user_id=? AND auth_revision=? AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at>CURRENT_TIMESTAMP(6))"
    };
    sqlx::query_as(sql).bind(token).bind(user).bind(auth)
}

pub(in super::super) fn service_identity(access: &str, lock: bool) -> Scalar<'_, i64> {
    let sql = if lock {
        "SELECT authorization_revision FROM s3_credentials WHERE access_key_id=? AND enabled AND (expires_at IS NULL OR expires_at>CURRENT_TIMESTAMP(6)) LOCK IN SHARE MODE"
    } else {
        "SELECT authorization_revision FROM s3_credentials WHERE access_key_id=? AND enabled AND (expires_at IS NULL OR expires_at>CURRENT_TIMESTAMP(6))"
    };
    sqlx::query_scalar(sql).bind(access)
}

pub(in super::super) fn action_bit(action: Action) -> u64 {
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

pub(in super::super) fn action_bits(actions: &[Action]) -> u64 {
    actions
        .iter()
        .fold(0, |bits, action| bits | action_bit(*action))
}

pub(in super::super) fn actions(bits: u64) -> Vec<Action> {
    Action::ALL
        .into_iter()
        .filter(|action| bits & action_bit(*action) != 0)
        .collect()
}

pub(in super::super) async fn validate_identity(
    principal: &Principal,
    connection: &mut sqlx::MySqlConnection,
    lock: bool,
) -> Result<bool> {
    let denied = || s3s::s3_error!(AccessDenied);
    match principal {
        Principal::System | Principal::Bootstrap | Principal::Local => Ok(true),
        Principal::Node { .. } => Err(denied().into()),
        Principal::User {
            id,
            session,
            revision,
        } => {
            let (role, current) = user_identity(*id, *session, lock)
                .fetch_optional(connection)
                .await?
                .ok_or_else(denied)?;
            if current != *revision {
                return Err(denied().into());
            }
            Ok(role == "admin")
        }
        Principal::Service {
            access_key,
            revision,
        } => {
            let current = service_identity(access_key, lock)
                .fetch_optional(connection)
                .await?;
            if current != Some(*revision) {
                return Err(denied().into());
            }
            Ok(false)
        }
        Principal::Token {
            id,
            user,
            revision,
            user_revision,
        } => {
            let (role, current, auth) = token_owner_identity(*user, lock)
                .fetch_optional(&mut *connection)
                .await?
                .ok_or_else(denied)?;
            if current != *user_revision {
                return Err(denied().into());
            }
            let (system, current) = token_identity(*id, *user, auth, lock)
                .fetch_optional(connection)
                .await?
                .ok_or_else(denied)?;
            if current != *revision {
                return Err(denied().into());
            }
            Ok(system && role == "admin")
        }
    }
}

#[async_trait::async_trait]
impl CredentialStore for Backend {
    async fn token_user(&self, id: Uuid) -> Result<Option<Uuid>> {
        Ok(token_user(id).fetch_optional(&self.pool).await?)
    }

    async fn encrypted_secret(&self, access: &str) -> Result<Option<Vec<u8>>> {
        Ok(sqlx::query_scalar("SELECT secret_encrypted FROM s3_credentials WHERE access_key_id=? AND enabled AND (expires_at IS NULL OR expires_at>CURRENT_TIMESTAMP(6))").bind(access).fetch_optional(&self.pool).await?)
    }
    async fn touch_credential(&self, access: &str) -> Result<()> {
        sqlx::query("UPDATE s3_credentials SET last_used_at=CURRENT_TIMESTAMP(6) WHERE access_key_id=? AND (last_used_at IS NULL OR last_used_at<CURRENT_TIMESTAMP(6)-INTERVAL 5 MINUTE)").bind(access).execute(&self.pool).await?;
        Ok(())
    }
    async fn buckets(
        &self,
        access: &str,
        prefix: &str,
        after: &str,
        region: Option<&str>,
        expected_region: &str,
        limit: i64,
    ) -> Result<Vec<crate::app::Bucket>> {
        Ok(sqlx::query_as("SELECT b.* FROM buckets b JOIN s3_credential_grants g ON g.bucket_id=b.id JOIN s3_credentials c ON c.access_key_id=g.access_key WHERE g.access_key=? AND c.enabled AND (c.expires_at IS NULL OR c.expires_at>CURRENT_TIMESTAMP(6)) AND c.project_id=b.project_id AND (g.actions & 1)<>0 AND LEFT(b.name,CHAR_LENGTH(?))=? AND b.name>? AND (? IS NULL OR ?=?) ORDER BY b.name LIMIT ?").bind(access).bind(prefix).bind(prefix).bind(after).bind(region).bind(region).bind(expected_region).bind(limit).fetch_all(&self.pool).await?)
    }

    async fn credential(&self, access: &str) -> Result<Credential> {
        Ok(credential_record(access)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchKey))?)
    }
    async fn credentials(
        &self,
        project: Option<Uuid>,
        after: &str,
        limit: i64,
    ) -> Result<Vec<Credential>> {
        Ok(credential_records(project, after, limit)
            .fetch_all(&self.pool)
            .await?)
    }
    async fn create_credential(
        &self,
        principal: &Principal,
        input: CredentialInput,
        material: CredentialMaterial,
    ) -> Result<Credential> {
        transactions::create_credential(&self.pool, principal, input, material).await
    }
    async fn update_credential(
        &self,
        principal: &Principal,
        access: &str,
        input: CredentialSettings,
    ) -> Result<Credential> {
        transactions::update_credential(&self.pool, principal, access, input).await
    }
    async fn set_grants(
        &self,
        principal: &Principal,
        access: &str,
        grants: Vec<BucketGrant>,
    ) -> Result<Credential> {
        transactions::set_grants(&self.pool, principal, access, grants).await
    }
    async fn revoke_credential(&self, principal: &Principal, access: &str) -> Result<()> {
        transactions::revoke_credential(&self.pool, principal, access).await
    }
    async fn enable_credential(
        &self,
        principal: &Principal,
        access: &str,
        enabled: bool,
    ) -> Result<()> {
        transactions::enable_credential(&self.pool, principal, access, enabled).await
    }
    async fn change_grant(
        &self,
        principal: &Principal,
        access: &str,
        bucket: Uuid,
        actions: Vec<Action>,
    ) -> Result<()> {
        transactions::change_grant(&self.pool, principal, access, bucket, actions).await
    }
    async fn rotate_credential(
        &self,
        principal: &Principal,
        access: &str,
        input: RotateCredential,
        material: CredentialMaterial,
    ) -> Result<(Credential, DateTime<Utc>)> {
        transactions::rotate_credential(&self.pool, principal, access, input, material).await
    }
    async fn create_token(
        &self,
        principal: &Principal,
        user: Uuid,
        input: TokenInput,
        material: TokenMaterial,
    ) -> Result<Token> {
        transactions::create_token(&self.pool, principal, user, input, material).await
    }
    async fn update_token(
        &self,
        principal: &Principal,
        user: Uuid,
        id: Uuid,
        input: TokenInput,
    ) -> Result<Token> {
        transactions::update_token(&self.pool, principal, user, id, input).await
    }
    async fn revoke_token(&self, principal: &Principal, id: Uuid) -> Result<()> {
        transactions::revoke_token(&self.pool, principal, id).await
    }
    async fn tokens(&self, user: Uuid, after: Option<Uuid>, limit: i64) -> Result<Vec<Token>> {
        Ok(token_records(user, after, limit)
            .fetch_all(&self.pool)
            .await?)
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for crate::app::Bucket {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            project_id: row.try_get("project_id")?,
            name: row.try_get("name")?,
            storage_id: row.try_get("storage_id")?,
            state: row.try_get("state")?,
            settings_revision: row.try_get("settings_revision")?,
            image_revision: row.try_get("image_revision")?,
            uploads_paused: row.try_get("uploads_paused")?,
            public_base_url: row.try_get("public_base_url")?,
            cors: row.try_get::<serde_json::Value, _>("cors")?.into(),
            website_enabled: row.try_get("website_enabled")?,
            index_document: row.try_get("index_document")?,
            error_document: row.try_get("error_document")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for crate::model::credential::Credential {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            access_key: row.try_get("access_key_id")?,
            project_id: row.try_get("project_id")?,
            label: row.try_get("label")?,
            enabled: row.try_get("enabled")?,
            created_at: row.try_get("created_at")?,
            expires_at: row.try_get("expires_at")?,
            last_used_at: row.try_get("last_used_at")?,
            created_by: row.try_get("created_by")?,
            grants: grants(row, "grants")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for crate::model::credential::Token {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            user_id: row.try_get("user_id")?,
            label: row.try_get("label")?,
            prefix: row.try_get("prefix")?,
            system: row.try_get("system_access")?,
            active: row.try_get("active")?,
            created_at: row.try_get("created_at")?,
            expires_at: row.try_get("expires_at")?,
            last_used_at: row.try_get("last_used_at")?,
            revoked_at: row.try_get("revoked_at")?,
            grants: grants(row, "grants")?,
            external_grants: grants(row, "external_grants")?,
        })
    }
}

pub(in super::super) fn storage_mode_lock() -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT deployment_id FROM mokyu_meta WHERE id=1 FOR UPDATE")
}

pub(in super::super) fn valid_storage_key(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM managed_keys WHERE id=? AND purpose='block' AND revoked_at IS NULL LOCK IN SHARE MODE").bind(id)
}

pub(in super::super) fn insert_storage_record(
    record: &BackendRecord,
    key: Option<Uuid>,
) -> Statement<'_> {
    sqlx::query("INSERT INTO storages(id,name,type,path,endpoint_url,remote_bucket_name,region,prefix,identity_marker,block_key_id,access_key_encrypted,secret_key_encrypted) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)").bind(record.id).bind(&record.name).bind(&record.r#type).bind(&record.path).bind(&record.endpoint_url).bind(&record.remote_bucket_name).bind(&record.region).bind(&record.prefix).bind(&record.identity_marker).bind(key).bind(&record.access_key_encrypted).bind(&record.secret_key_encrypted)
}
