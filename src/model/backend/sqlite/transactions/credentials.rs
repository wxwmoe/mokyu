use super::*;

pub(in super::super) async fn enable_credential(
    pool: &SqlitePool,
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
    pool: &SqlitePool,
    principal: &Principal,
    access: &str,
    bucket: Uuid,
    actions: Vec<crate::authorization::Action>,
) -> Result<()> {
    let mut tx = administration_transaction(pool, principal).await?;
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
    pool: &SqlitePool,
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

pub(in super::super) async fn get_token(db: &mut SqliteConnection, id: Uuid) -> Result<Token> {
    Ok(queries::token_record(id)
        .fetch_optional(db)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?)
}

async fn token_owner<'a>(
    pool: &'a SqlitePool,
    principal: &Principal,
    user: Uuid,
    other: bool,
) -> Result<Transaction<'a, Sqlite>> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let admin = queries::validate_identity(principal, &mut tx).await?;
    let control = admin && matches!(principal, Principal::System | Principal::Local);
    if !control && principal.user_id() != Some(user) && !(other && admin) {
        return Err(s3s::s3_error!(AccessDenied).into());
    }
    Ok(tx)
}

async fn token_scope(
    tx: &mut Transaction<'_, Sqlite>,
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
    tx: &mut Transaction<'_, Sqlite>,
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
    pool: &SqlitePool,
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
    pool: &SqlitePool,
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
    pool: &SqlitePool,
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

pub(super) fn quota_number(value: i128) -> Result<i64> {
    ensure!(
        (0..=i128::from(i64::MAX)).contains(&value),
        crate::model::Error::QuotaExceeded
    );
    Ok(value as i64)
}

pub(in super::super) async fn lock_quota(
    connection: &mut SqliteConnection,
    bucket: uuid::Uuid,
) -> Result<[QuotaAccount; 2]> {
    let project = queries::quota_project(bucket)
        .fetch_one(&mut *connection)
        .await?;
    let project = queries::quota_account("project", project)
        .fetch_one(&mut *connection)
        .await?;
    let bucket = queries::quota_account("bucket", bucket)
        .fetch_one(connection)
        .await?;
    Ok([project, bucket])
}

pub(in super::super) async fn adjust_quota(
    connection: &mut SqliteConnection,
    accounts: &mut [QuotaAccount; 2],
    used: i64,
    reserved: i64,
    inflight: i64,
    objects: i64,
) -> Result<()> {
    for account in accounts.iter() {
        let logical = i128::from(account.used_bytes)
            + i128::from(account.reserved_bytes)
            + i128::from(used)
            + i128::from(reserved);
        let pending = i128::from(account.inflight_bytes) + i128::from(inflight);
        if i128::from(used) + i128::from(reserved) > 0
            && account
                .logical_bytes_limit
                .is_some_and(|limit| logical > i128::from(limit))
            || inflight > 0
                && account
                    .inflight_bytes_limit
                    .is_some_and(|limit| pending > i128::from(limit))
        {
            return Err(crate::model::Error::QuotaExceeded.into());
        }
        quota_number(i128::from(account.used_bytes) + i128::from(used))?;
        quota_number(i128::from(account.reserved_bytes) + i128::from(reserved))?;
        quota_number(pending)?;
        quota_number(i128::from(account.object_count) + i128::from(objects))?;
    }
    queries::adjust_quota(
        accounts[0].scope_id,
        accounts[1].scope_id,
        used,
        reserved,
        inflight,
        objects,
    )
    .execute(connection)
    .await?;
    for account in accounts.iter_mut() {
        account.used_bytes += used;
        account.reserved_bytes += reserved;
        account.inflight_bytes += inflight;
        account.object_count += objects;
    }
    Ok(())
}
