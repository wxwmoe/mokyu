use super::*;

pub(in super::super) async fn get_credential(
    connection: &mut MySqlConnection,
    access: &str,
) -> Result<Credential> {
    Ok(queries::credential_record(access)
        .fetch_optional(connection)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?)
}

async fn replace_credential_grants(
    tx: &mut Transaction<'_, MySql>,
    access: &str,
    project: Uuid,
    grants: Vec<BucketGrant>,
) -> Result<()> {
    credential::validate_grants(&grants)?;
    let mut ids: Vec<_> = grants.iter().map(|g| g.bucket_id).collect();
    ids.sort();
    let found: Vec<Uuid> = queries::lock_project_buckets(tx, &ids, project).await?;
    if found != ids {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    queries::delete_credential_grants(access)
        .execute(&mut **tx)
        .await?;
    for grant in grants {
        queries::insert_credential_grant(access, &grant)
            .execute(&mut **tx)
            .await?;
    }
    queries::advance_credential_authorization(access)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn insert_credential(
    tx: &mut Transaction<'_, MySql>,
    principal: &Principal,
    input: CredentialInput,
    material: CredentialMaterial,
) -> Result<Credential> {
    credential::label(&input.label)?;
    credential::validate_grants(&input.grants)?;
    let expires = credential::expiration(input.expires_in.as_deref())?;
    let project: Option<Uuid> = queries::credential_project(input.project_id)
        .fetch_optional(&mut **tx)
        .await?;
    if project.is_none() {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let access = &material.access;
    queries::insert_credential(&material, &input, expires, principal.user_id())
        .execute(&mut **tx)
        .await?;
    replace_credential_grants(tx, access, input.project_id, input.grants).await?;
    let credential = get_credential(tx, access).await?;
    Ok(credential)
}

pub(in super::super) async fn create_credential(
    pool: &MySqlPool,
    principal: &Principal,
    input: CredentialInput,
    material: CredentialMaterial,
) -> Result<Credential> {
    let mut tx = administration_transaction(pool, principal).await?;
    let row = insert_credential(&mut tx, principal, input, material).await?;
    checkpoint_event(&mut tx,"credential.create",&row.access_key,serde_json::json!({"project_id":row.project_id,"label":row.label,"grants":row.grants,"expires_at":row.expires_at})).await?;
    tx.commit().await?;
    Ok(row)
}

pub(in super::super) async fn update_credential(
    pool: &MySqlPool,
    principal: &Principal,
    access: &str,
    input: CredentialSettings,
) -> Result<Credential> {
    credential::label(&input.label)?;
    let expires = credential::expiration(input.expires_in.as_deref())?;
    if input.keep_expiry && expires.is_some() {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let mut tx = administration_transaction(pool, principal).await?;
    let changed = queries::update_credential(access, &input, expires)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    if changed == 0 {
        return Err(s3s::s3_error!(NoSuchKey).into());
    }
    if let Some(grants) = input.grants {
        let row = get_credential(&mut tx, access).await?;
        replace_credential_grants(&mut tx, access, row.project_id, grants).await?;
    }
    let row = get_credential(&mut tx, access).await?;
    checkpoint_event(&mut tx,"credential.update",access,serde_json::json!({"project_id":row.project_id,"label":row.label,"enabled":row.enabled,"grants":row.grants,"expires_at":row.expires_at})).await?;
    tx.commit().await?;
    Ok(row)
}

pub(in super::super) async fn set_grants(
    pool: &MySqlPool,
    principal: &Principal,
    access: &str,
    grants: Vec<BucketGrant>,
) -> Result<Credential> {
    let mut tx = administration_transaction(pool, principal).await?;
    let project = queries::lock_credential_project(access)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    replace_credential_grants(&mut tx, access, project, grants).await?;
    let row = get_credential(&mut tx, access).await?;
    checkpoint_event(
        &mut tx,
        "credential.permissions",
        access,
        serde_json::json!({"project_id":row.project_id,"grants":row.grants}),
    )
    .await?;
    tx.commit().await?;
    Ok(row)
}

pub(in super::super) async fn revoke_credential(
    pool: &MySqlPool,
    principal: &Principal,
    access: &str,
) -> Result<()> {
    let mut tx = administration_transaction(pool, principal).await?;
    queries::delete_credential(access).execute(&mut *tx).await?;
    checkpoint_event(&mut tx, "credential.delete", access, serde_json::json!({})).await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn enable_credential(
    pool: &MySqlPool,
    principal: &Principal,
    access: &str,
    enabled: bool,
) -> Result<()> {
    let mut tx = administration_transaction(pool, principal).await?;
    let n = queries::enable_credential(access, enabled)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    if n == 0 {
        return Err(s3s::s3_error!(NoSuchKey).into());
    }
    checkpoint_event(
        &mut tx,
        "credential.enabled",
        access,
        serde_json::json!({"enabled":enabled}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn change_grant(
    pool: &MySqlPool,
    principal: &Principal,
    access: &str,
    bucket: Uuid,
    actions: Vec<crate::authorization::Action>,
) -> Result<()> {
    let mut tx = administration_transaction(pool, principal).await?;
    queries::lock_credential(access).execute(&mut *tx).await?;
    let current = get_credential(&mut tx, access).await?;
    let mut grants = current.grants;
    grants.retain(|g| g.bucket_id != bucket);
    if !actions.is_empty() {
        grants.push(BucketGrant {
            bucket_id: bucket,
            actions,
        });
    }
    replace_credential_grants(&mut tx, access, current.project_id, grants).await?;
    let row = get_credential(&mut tx, access).await?;
    checkpoint_event(
        &mut tx,
        "credential.permissions",
        access,
        serde_json::json!({"project_id":row.project_id,"grants":row.grants}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn rotate_credential(
    pool: &MySqlPool,
    principal: &Principal,
    access: &str,
    input: RotateCredential,
    material: CredentialMaterial,
) -> Result<(Credential, chrono::DateTime<Utc>)> {
    let seconds =
        crate::config::seconds(&input.overlap).map_err(|_| s3s::s3_error!(InvalidArgument))?;
    if seconds > 30 * 86400 {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let mut tx = administration_transaction(pool, principal).await?;
    queries::lock_credential(access).execute(&mut *tx).await?;
    let current = get_credential(&mut tx, access).await?;
    if !current.enabled {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let old_expires_at = current
        .expires_at
        .unwrap_or(Utc::now() + chrono::Duration::days(30))
        .min(Utc::now() + chrono::Duration::seconds(seconds as i64));
    let replacement = insert_credential(
        &mut tx,
        principal,
        CredentialInput {
            project_id: current.project_id,
            label: current.label,
            expires_in: input.expires_in,
            grants: current.grants,
        },
        material,
    )
    .await?;
    queries::expire_credential(access, old_expires_at)
        .execute(&mut *tx)
        .await?;
    checkpoint_event(&mut tx,"credential.rotate",access,serde_json::json!({"project_id":replacement.project_id,"replacement_key":replacement.access_key,"old_expires_at":old_expires_at})).await?;
    tx.commit().await?;
    Ok((replacement, old_expires_at))
}

pub(in super::super) async fn get_token(db: &mut MySqlConnection, id: Uuid) -> Result<Token> {
    Ok(queries::token_record(id)
        .fetch_optional(db)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?)
}

async fn token_owner<'a>(
    pool: &'a MySqlPool,
    principal: &Principal,
    user: Uuid,
    other: bool,
) -> Result<Transaction<'a, MySql>> {
    let mut tx = pool.begin().await?;
    queries::lock_accounts().execute(&mut *tx).await?;
    let admin = queries::validate_identity(principal, &mut tx, true).await?;
    let control = admin && matches!(principal, Principal::System | Principal::Local);
    if !control && principal.user_id() != Some(user) && !(other && admin) {
        return Err(s3s::s3_error!(AccessDenied).into());
    }
    queries::lock_token_owner(user).execute(&mut *tx).await?;
    Ok(tx)
}

async fn token_scope(
    tx: &mut Transaction<'_, MySql>,
    user: Uuid,
    input: &TokenInput,
) -> Result<i64> {
    credential::label(&input.label)?;
    credential::validate_grants(&input.grants)?;
    let (role, revision): (String, i64) = queries::token_owner_revision(user)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(AccessDenied))?;
    if input.system
        && (role != "admin" || !input.grants.is_empty() || !input.external_grants.is_empty())
    {
        return Err(s3s::s3_error!(AccessDenied).into());
    }
    let mut grants: Vec<_> = input.grants.iter().collect();
    grants.sort_by_key(|g| g.bucket_id);
    for grant in grants {
        let exists: Option<Uuid> = queries::permission_bucket(grant.bucket_id)
            .fetch_optional(&mut **tx)
            .await?;
        if exists.is_none() {
            return Err(s3s::s3_error!(AccessDenied).into());
        }
        if role != "admin" {
            let actions = queries::token_owner_actions(user, grant.bucket_id)
                .fetch_optional(&mut **tx)
                .await?
                .unwrap_or(0);
            let requested = queries::action_bits(&grant.actions);
            if actions & requested != requested {
                return Err(s3s::s3_error!(AccessDenied).into());
            }
        }
    }
    super::super::transactions::external::token_scope(
        &mut *tx,
        user,
        &role,
        &input.external_grants,
    )
    .await?;
    Ok(revision)
}

async fn replace_token_grants(
    tx: &mut Transaction<'_, MySql>,
    id: Uuid,
    grants: Vec<BucketGrant>,
) -> Result<()> {
    queries::delete_token_grants(id).execute(&mut **tx).await?;
    for grant in grants {
        queries::insert_token_grant(id, &grant)
            .execute(&mut **tx)
            .await?;
    }
    queries::advance_token_authorization(id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub(in super::super) async fn create_token(
    pool: &MySqlPool,
    principal: &Principal,
    user: Uuid,
    input: TokenInput,
    material: TokenMaterial,
) -> Result<Token> {
    if input.keep_expiry {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let expires = credential::expiration(input.expires_in.as_deref())?;
    let mut tx = token_owner(pool, principal, user, false).await?;
    let revision = token_scope(&mut tx, user, &input).await?;
    let id = Uuid::new_v4();
    queries::insert_token(id, user, &input, &material, revision, expires)
        .execute(&mut *tx)
        .await?;
    super::super::transactions::external::replace_token(&mut tx, id, &input.external_grants)
        .await?;
    replace_token_grants(&mut tx, id, input.grants).await?;
    let token = get_token(&mut tx, id).await?;
    checkpoint_event(&mut tx,"token.create",&id.to_string(),serde_json::json!({"user_id":user,"label":token.label,"system":token.system,"grants":token.grants,"external_grants":token.external_grants,"expires_at":token.expires_at})).await?;
    tx.commit().await?;
    Ok(token)
}

pub(in super::super) async fn update_token(
    pool: &MySqlPool,
    principal: &Principal,
    user: Uuid,
    id: Uuid,
    input: TokenInput,
) -> Result<Token> {
    let expires = credential::expiration(input.expires_in.as_deref())?;
    if input.keep_expiry && expires.is_some() {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let mut tx = token_owner(pool, principal, user, false).await?;
    queries::lock_token(id).execute(&mut *tx).await?;
    let current = get_token(&mut tx, id).await?;
    if current.user_id != user || current.revoked_at.is_some() {
        return Err(s3s::s3_error!(AccessDenied).into());
    }
    token_scope(&mut tx, user, &input).await?;
    queries::update_token(id, &input, expires)
        .execute(&mut *tx)
        .await?;
    super::super::transactions::external::replace_token(&mut tx, id, &input.external_grants)
        .await?;
    replace_token_grants(&mut tx, id, input.grants).await?;
    let result = get_token(&mut tx, id).await?;
    checkpoint_event(&mut tx,"token.update",&id.to_string(),serde_json::json!({"user_id":user,"label":result.label,"system":result.system,"grants":result.grants,"external_grants":result.external_grants,"expires_at":result.expires_at})).await?;
    tx.commit().await?;
    Ok(result)
}

pub(in super::super) async fn revoke_token(
    pool: &MySqlPool,
    principal: &Principal,
    id: Uuid,
) -> Result<()> {
    let user: Uuid = queries::token_user(id)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    let mut tx = token_owner(pool, principal, user, true).await?;
    queries::revoke_token(id).execute(&mut *tx).await?;
    checkpoint_event(
        &mut tx,
        "token.revoke",
        &id.to_string(),
        serde_json::json!({"user_id":user}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}
