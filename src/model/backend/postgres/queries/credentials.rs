use super::*;

pub(in super::super) fn credential_record<'q>(access: &'q str) -> Rows<'q, Credential> {
    sqlx::query_as("SELECT c.*,COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket_id',g.bucket_id,'actions',g.actions) ORDER BY g.bucket_id) FROM s3_credential_grants g WHERE g.access_key=c.access_key_id),'[]'::jsonb) AS grants FROM s3_credentials c WHERE c.access_key_id=$1").bind(access)
}

pub(in super::super) fn delete_credential_grants<'q>(access: &'q str) -> Statement<'q> {
    sqlx::query("DELETE FROM s3_credential_grants WHERE access_key=$1").bind(access)
}

pub(in super::super) fn insert_credential_grant<'q>(
    access: &'q str,
    grant: &'q BucketGrant,
) -> Statement<'q> {
    sqlx::query("INSERT INTO s3_credential_grants(access_key,bucket_id,actions) VALUES($1,$2,$3)")
        .bind(access)
        .bind(grant.bucket_id)
        .bind(grant.actions.iter().map(|a| a.name()).collect::<Vec<_>>())
}

pub(in super::super) fn credential_project(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM projects WHERE id=$1 FOR SHARE").bind(id)
}

pub(in super::super) fn insert_credential<'q>(
    material: &'q CredentialMaterial,
    input: &'q CredentialInput,
    expires: Option<DateTime<Utc>>,
    user: Option<Uuid>,
) -> Statement<'q> {
    sqlx::query("INSERT INTO s3_credentials(access_key_id,secret_encrypted,project_id,label,expires_at,created_by) VALUES($1,$2,$3,$4,$5,$6)").bind(&material.access).bind(&material.encrypted).bind(input.project_id).bind(input.label.trim()).bind(expires).bind(user)
}

pub(in super::super) fn update_credential<'q>(
    access: &'q str,
    input: &'q CredentialSettings,
    expires: Option<DateTime<Utc>>,
) -> Statement<'q> {
    sqlx::query("UPDATE s3_credentials SET label=$2,enabled=$3,expires_at=CASE WHEN $5 THEN expires_at ELSE $4 END WHERE access_key_id=$1").bind(access).bind(input.label.trim()).bind(input.enabled).bind(expires).bind(input.keep_expiry)
}

pub(in super::super) fn lock_credential_project<'q>(access: &'q str) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("SELECT project_id FROM s3_credentials WHERE access_key_id=$1 FOR UPDATE")
        .bind(access)
}

pub(in super::super) fn delete_credential<'q>(access: &'q str) -> Statement<'q> {
    sqlx::query("DELETE FROM s3_credentials WHERE access_key_id=$1").bind(access)
}

pub(in super::super) fn enable_credential<'q>(access: &'q str, enabled: bool) -> Statement<'q> {
    sqlx::query("UPDATE s3_credentials SET enabled=$2 WHERE access_key_id=$1")
        .bind(access)
        .bind(enabled)
}

pub(in super::super) fn lock_credential<'q>(access: &'q str) -> Statement<'q> {
    sqlx::query(
        "SELECT access_key_id AS access_key FROM s3_credentials WHERE access_key_id=$1 FOR UPDATE",
    )
    .bind(access)
}

pub(in super::super) fn expire_credential<'q>(
    access: &'q str,
    expires: DateTime<Utc>,
) -> Statement<'q> {
    sqlx::query("UPDATE s3_credentials SET expires_at=$2 WHERE access_key_id=$1")
        .bind(access)
        .bind(expires)
}

pub(in super::super) fn credential_records<'q>(
    project: Option<Uuid>,
    after: &'q str,
    limit: i64,
) -> Rows<'q, Credential> {
    sqlx::query_as("SELECT c.*,COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket_id',g.bucket_id,'actions',g.actions) ORDER BY g.bucket_id) FROM s3_credential_grants g WHERE g.access_key=c.access_key_id),'[]'::jsonb) AS grants FROM s3_credentials c WHERE ($1::uuid IS NULL OR c.project_id=$1) AND c.access_key_id>$2 ORDER BY c.access_key_id LIMIT $3").bind(project).bind(after).bind(limit)
}

pub(in super::super) fn token_record(id: Uuid) -> Rows<'static, Token> {
    sqlx::query_as("SELECT t.id,t.user_id,t.label,t.prefix,t.system,t.created_at,t.expires_at,t.last_used_at,t.revoked_at,u.enabled AND NOT u.must_change_password AND t.auth_revision=u.auth_revision AND t.revoked_at IS NULL AND (t.expires_at IS NULL OR t.expires_at>now()) AS active,COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket_id',g.bucket_id,'actions',g.actions) ORDER BY g.bucket_id) FROM token_bucket_grants g WHERE g.token_id=t.id),'[]'::jsonb) AS grants,COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket_id',g.bucket_id,'actions',g.actions) ORDER BY g.bucket_id) FROM token_external_grants g WHERE g.token_id=t.id),'[]'::jsonb) AS external_grants FROM api_tokens t JOIN web_users u ON u.id=t.user_id WHERE t.id=$1").bind(id)
}

pub(in super::super) fn lock_token_owner(user: Uuid) -> Statement<'static> {
    sqlx::query("SELECT id FROM web_users WHERE id=$1 FOR SHARE").bind(user)
}

pub(in super::super) fn token_owner_revision(user: Uuid) -> Rows<'static, (String, i64)> {
    sqlx::query_as("SELECT role,auth_revision FROM web_users WHERE id=$1 AND enabled AND NOT must_change_password").bind(user)
}

pub(in super::super) fn token_owner_actions(
    user: Uuid,
    bucket: Uuid,
) -> Scalar<'static, Vec<String>> {
    sqlx::query_scalar("SELECT actions FROM user_bucket_access WHERE user_id=$1 AND bucket_id=$2")
        .bind(user)
        .bind(bucket)
}

pub(in super::super) fn delete_token_grants(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM token_bucket_grants WHERE token_id=$1").bind(id)
}

pub(in super::super) fn insert_token_grant<'q>(id: Uuid, grant: &'q BucketGrant) -> Statement<'q> {
    sqlx::query("INSERT INTO token_bucket_grants(token_id,bucket_id,actions) VALUES($1,$2,$3)")
        .bind(id)
        .bind(grant.bucket_id)
        .bind(grant.actions.iter().map(|a| a.name()).collect::<Vec<_>>())
}

pub(in super::super) fn insert_token<'q>(
    user: Uuid,
    input: &'q TokenInput,
    material: &'q TokenMaterial,
    revision: i64,
    expires: Option<DateTime<Utc>>,
) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("INSERT INTO api_tokens(user_id,label,prefix,token_hash,system,auth_revision,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING id").bind(user).bind(input.label.trim()).bind(&material.prefix).bind(material.hash.as_slice()).bind(input.system).bind(revision).bind(expires)
}

pub(in super::super) fn lock_token(id: Uuid) -> Statement<'static> {
    sqlx::query("SELECT id FROM api_tokens WHERE id=$1 FOR UPDATE").bind(id)
}

pub(in super::super) fn update_token<'q>(
    id: Uuid,
    input: &'q TokenInput,
    expires: Option<DateTime<Utc>>,
) -> Statement<'q> {
    sqlx::query("UPDATE api_tokens SET label=$2,system=$3,expires_at=CASE WHEN $5 THEN expires_at ELSE $4 END WHERE id=$1").bind(id).bind(input.label.trim()).bind(input.system).bind(expires).bind(input.keep_expiry)
}

pub(in super::super) fn token_user(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT user_id FROM api_tokens WHERE id=$1").bind(id)
}

pub(in super::super) fn revoke_token(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE api_tokens SET revoked_at=COALESCE(revoked_at,now()) WHERE id=$1").bind(id)
}

pub(in super::super) fn token_records(
    user: Uuid,
    after: Option<Uuid>,
    limit: i64,
) -> Rows<'static, Token> {
    sqlx::query_as("SELECT t.id,t.user_id,t.label,t.prefix,t.system,t.created_at,t.expires_at,t.last_used_at,t.revoked_at,u.enabled AND NOT u.must_change_password AND t.auth_revision=u.auth_revision AND t.revoked_at IS NULL AND (t.expires_at IS NULL OR t.expires_at>now()) AS active,COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket_id',g.bucket_id,'actions',g.actions) ORDER BY g.bucket_id) FROM token_bucket_grants g WHERE g.token_id=t.id),'[]'::jsonb) AS grants,COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket_id',g.bucket_id,'actions',g.actions) ORDER BY g.bucket_id) FROM token_external_grants g WHERE g.token_id=t.id),'[]'::jsonb) AS external_grants FROM api_tokens t JOIN web_users u ON u.id=t.user_id WHERE t.user_id=$1 AND ($2::uuid IS NULL OR t.id>$2) ORDER BY t.id LIMIT $3").bind(user).bind(after).bind(limit)
}

#[async_trait::async_trait]
impl CredentialStore for Backend {
    async fn token_user(&self, id: Uuid) -> Result<Option<Uuid>> {
        Ok(token_user(id).fetch_optional(&self.pool).await?)
    }

    async fn encrypted_secret(&self, access: &str) -> Result<Option<Vec<u8>>> {
        Ok(sqlx::query_scalar("SELECT secret_encrypted FROM s3_credentials WHERE access_key_id=$1 AND enabled AND (expires_at IS NULL OR expires_at>now())").bind(access).fetch_optional(&self.pool).await?)
    }
    async fn touch_credential(&self, access: &str) -> Result<()> {
        touch_credentials(&[access.to_owned()])
            .execute(&self.pool)
            .await?;
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
        Ok(
            credential_buckets(access, prefix, after, region, expected_region, limit)
                .fetch_all(&self.pool)
                .await?,
        )
    }

    async fn credential(&self, access: &str) -> Result<Credential> {
        Ok(credential_record(access)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| s3_error!(NoSuchKey))?)
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
