use super::*;

struct AdminAccess {
    _authorization: tokio::sync::OwnedRwLockReadGuard<()>,
    write: SqlitePool,
}

#[async_trait::async_trait]
impl crate::model::AdminGuard for AdminAccess {
    async fn checkpoint(&mut self, action: &str, target: &str, detail: Value) -> Result<()> {
        let mut tx = self.write.begin_with(queries::BEGIN_WRITE).await?;
        checkpoint_event(&mut tx, action, target, detail).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn finish(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

pub(in super::super) async fn hold_admin(
    backend: &super::super::Backend,
    principal: &Principal,
) -> Result<Box<dyn crate::model::AdminGuard>> {
    // Hold identity validity without occupying the sole writer during nested operations.
    let authorization = backend.authorization.clone().read_owned().await;
    let mut tx = backend.read.begin().await?;
    if !queries::validate_identity(principal, &mut tx).await? {
        return Err(denied());
    }
    tx.commit().await?;
    Ok(Box::new(AdminAccess {
        _authorization: authorization,
        write: backend.write.clone(),
    }))
}

pub(in super::super) async fn lock_identity(
    connection: &mut SqliteConnection,
    principal: &Principal,
) -> Result<bool> {
    queries::validate_identity(principal, connection).await
}

pub(in super::super) async fn lock_permit(
    connection: &mut SqliteConnection,
    permit: &crate::authorization::Permit,
) -> Result<()> {
    let admin = lock_identity(connection, &permit.principal).await?;
    for (bucket, required) in &permit.resources {
        if queries::permission_bucket(*bucket)
            .fetch_optional(&mut *connection)
            .await?
            .is_none()
        {
            return Err(denied());
        }
        let available =
            queries::bucket_actions(&permit.principal, connection, *bucket, admin).await?;
        if required.iter().any(|action| !available.contains(action)) {
            return Err(denied());
        }
    }
    Ok(())
}

pub(super) async fn save_storage_profile(
    tx: &mut Transaction<'_, Sqlite>,
    profile: &StorageProfile,
) -> Result<()> {
    let affected = queries::save_storage_profile(profile)?
        .execute(&mut **tx)
        .await?
        .rows_affected();
    if affected != 1 {
        return Err(crate::model::Error::StorageProfileConflict.into());
    }
    Ok(())
}

pub(super) async fn validate_block_key(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    key_id: Option<Uuid>,
) -> anyhow::Result<()> {
    let Some(key_id) = key_id else {
        return Ok(());
    };
    let valid = queries::valid_storage_key(key_id)
        .fetch_optional(&mut **tx)
        .await?;
    anyhow::ensure!(valid.is_some(), "active block key was not found");
    Ok(())
}

pub(super) async fn load_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    name: &str,
) -> anyhow::Result<crate::model::storage::StorageRow> {
    queries::storage_row_lock(name)
        .fetch_optional(&mut **tx)
        .await?
        .context("storage was not found")
}

pub(in super::super) async fn create_user(
    pool: &SqlitePool,
    principal: &Principal,
    input: UserCreate,
) -> Result<User> {
    let mut tx = administration_transaction(pool, principal).await?;
    if input.role == "member" {
        let admin: bool = queries::has_admin().fetch_one(&mut *tx).await?;
        if !admin {
            return Err(crate::model::Error::LastAdministrator.into());
        }
    }
    enable_member_mode(&mut tx, &input.role).await?;
    let id = Uuid::new_v4();
    queries::insert_user(id, &input).execute(&mut *tx).await?;
    let row = queries::user(id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(OperationAborted))?;
    queries::delete_setup_tokens().execute(&mut *tx).await?;
    checkpoint_event(
        &mut tx,
        "user.create",
        &row.id.to_string(),
        serde_json::json!({"username":row.username,"role":row.role,"enabled":row.enabled}),
    )
    .await?;
    tx.commit().await?;
    Ok(row)
}

pub(in super::super) async fn update_user(
    pool: &SqlitePool,
    principal: &Principal,
    id: Uuid,
    input: UserPatch,
) -> Result<User> {
    let mut tx = administration_transaction(pool, principal).await?;
    let current: User = queries::lock_user(id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    let role = input.role.as_deref().unwrap_or(&current.role);
    let enabled = input.enabled.unwrap_or(current.enabled);
    protect_admin(&mut tx, &current, role, enabled).await?;
    enable_member_mode(&mut tx, role).await?;
    if role == "admin" && current.role != "admin" {
        queries::delete_user_memberships(id)
            .execute(&mut *tx)
            .await?;
    }
    queries::update_user(id, role, enabled)
        .execute(&mut *tx)
        .await?;
    let row = queries::user(id).fetch_one(&mut *tx).await?;
    queries::delete_user_sessions(id).execute(&mut *tx).await?;
    checkpoint_event(&mut tx,"user.update",&id.to_string(),serde_json::json!({"username":current.username,"old_role":current.role,"role":role,"enabled":enabled})).await?;
    tx.commit().await?;
    Ok(row)
}

pub(in super::super) async fn delete_user(
    pool: &SqlitePool,
    principal: &Principal,
    id: Uuid,
) -> Result<()> {
    let mut tx = administration_transaction(pool, principal).await?;
    let current: User = queries::lock_user(id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    protect_admin(&mut tx, &current, "member", false).await?;
    queries::delete_user(id).execute(&mut *tx).await?;
    checkpoint_event(
        &mut tx,
        "user.delete",
        &id.to_string(),
        serde_json::json!({"username":current.username}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn reset_password(
    pool: &SqlitePool,
    principal: &Principal,
    id: Uuid,
    hash: &str,
    require_change: bool,
) -> Result<()> {
    let mut tx = administration_transaction(pool, principal).await?;
    let changed = queries::reset_user_password(id, hash, require_change)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    if changed == 0 {
        return Err(s3s::s3_error!(NoSuchKey).into());
    }
    queries::delete_user_sessions(id).execute(&mut *tx).await?;
    checkpoint_event(
        &mut tx,
        "user.reset_password",
        &id.to_string(),
        serde_json::json!({"must_change_password":require_change}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn save_member(
    pool: &SqlitePool,
    principal: &Principal,
    project: Uuid,
    user: Uuid,
    input: Option<Membership>,
) -> Result<()> {
    let mut tx = administration_transaction(pool, principal).await?;
    enable_member_mode(&mut tx, "member").await?;
    let target: Option<String> = queries::lock_member(user).fetch_optional(&mut *tx).await?;
    if target.as_deref() != Some("member") {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let exists: bool = queries::project_exists(project).fetch_one(&mut *tx).await?;
    if !exists {
        return Err(s3s::s3_error!(NoSuchKey).into());
    }
    let audit_detail = serde_json::json!({"project_id":project,"user_id":user,"membership":input});
    if let Some(input) = input {
        let mut buckets: Vec<Uuid> = input.grants.iter().map(|g| g.bucket_id).collect();
        buckets.sort();
        let valid = queries::lock_project_buckets(&mut tx, &buckets, project).await?;
        if valid != buckets {
            return Err(s3s::s3_error!(InvalidArgument).into());
        }
        queries::save_membership(user, project, &input.role, &input.scope)
            .execute(&mut *tx)
            .await?;
        queries::delete_member_grants(user, project)
            .execute(&mut *tx)
            .await?;
        super::super::transactions::external::replace_member(
            &mut tx,
            user,
            project,
            &input.external_grants,
        )
        .await?;
        for grant in input.grants {
            queries::insert_member_grant(user, project, &grant)
                .execute(&mut *tx)
                .await?;
        }
    } else {
        queries::delete_membership(user, project)
            .execute(&mut *tx)
            .await?;
    }
    queries::advance_user_authorization(user)
        .execute(&mut *tx)
        .await?;
    checkpoint_event(
        &mut tx,
        "project.membership",
        &user.to_string(),
        audit_detail,
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn save_project(
    pool: &SqlitePool,
    principal: &Principal,
    id: Option<Uuid>,
    input: ProjectInput,
) -> Result<Project> {
    let mut tx = administration_transaction(pool, principal).await?;
    let duplicate: bool = queries::duplicate_project(input.name.trim(), id)
        .fetch_one(&mut *tx)
        .await?;
    if duplicate {
        return Err(s3s::s3_error!(OperationAborted).into());
    }
    let row: Project = if let Some(id) = id {
        queries::update_project(id, &input)
            .execute(&mut *tx)
            .await?;
        queries::project(id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchKey))?
    } else {
        queries::enable_project_mode().execute(&mut *tx).await?;
        let id = Uuid::new_v4();
        queries::insert_project(id, &input)
            .execute(&mut *tx)
            .await?;
        queries::insert_quota_scope("project", id)
            .execute(&mut *tx)
            .await?;
        queries::project(id).fetch_one(&mut *tx).await?
    };
    checkpoint_event(&mut tx,if id.is_some(){"project.update"}else{"project.create"},&row.id.to_string(),serde_json::json!({"project_id":row.id,"name":row.name,"allow_bucket_create":row.allow_bucket_create})).await?;
    tx.commit().await?;
    Ok(row)
}

pub(in super::super) async fn delete_project(
    pool: &SqlitePool,
    principal: &Principal,
    id: Uuid,
) -> Result<()> {
    let mut tx = administration_transaction(pool, principal).await?;
    let exists: Option<Uuid> = queries::lock_project(id).fetch_optional(&mut *tx).await?;
    if exists.is_none() {
        return Err(s3s::s3_error!(NoSuchKey).into());
    }
    let dependent: bool = queries::project_dependents(id).fetch_one(&mut *tx).await?;
    if dependent {
        return Err(s3s::s3_error!(OperationAborted).into());
    }
    queries::delete_quota_scope("project", id)
        .execute(&mut *tx)
        .await?;
    queries::delete_project(id).execute(&mut *tx).await?;
    checkpoint_event(
        &mut tx,
        "project.delete",
        &id.to_string(),
        serde_json::json!({"project_id":id}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn set_project_mode(
    pool: &SqlitePool,
    principal: &Principal,
    enabled: bool,
) -> Result<()> {
    let mut tx = administration_transaction(pool, principal).await?;
    if !enabled {
        let dependent: bool = queries::project_mode_dependents()
            .fetch_one(&mut *tx)
            .await?;
        if dependent {
            return Err(s3s::s3_error!(OperationAborted).into());
        }
    }
    queries::set_project_mode(enabled).execute(&mut *tx).await?;
    checkpoint_event(
        &mut tx,
        "project.mode",
        "",
        serde_json::json!({"enabled":enabled}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn get_credential(
    connection: &mut SqliteConnection,
    access: &str,
) -> Result<Credential> {
    Ok(queries::credential_record(access)
        .fetch_optional(connection)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?)
}

pub(super) async fn replace_credential_grants(
    tx: &mut Transaction<'_, Sqlite>,
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

pub(super) async fn insert_credential(
    tx: &mut Transaction<'_, Sqlite>,
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
    pool: &SqlitePool,
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
    pool: &SqlitePool,
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
    pool: &SqlitePool,
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
    pool: &SqlitePool,
    principal: &Principal,
    access: &str,
) -> Result<()> {
    let mut tx = administration_transaction(pool, principal).await?;
    queries::delete_credential(access).execute(&mut *tx).await?;
    checkpoint_event(&mut tx, "credential.delete", access, serde_json::json!({})).await?;
    tx.commit().await?;
    Ok(())
}
