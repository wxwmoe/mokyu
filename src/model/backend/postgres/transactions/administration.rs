use super::*;

pub(crate) async fn admin_transaction<'a>(
    pool: &'a PgPool,
    principal: &Principal,
) -> Result<Transaction<'a, Postgres>> {
    let mut tx = pool.begin().await?;
    queries::lock_accounts().execute(&mut *tx).await?;
    principal.lock_admin(&mut tx).await?;
    Ok(tx)
}

async fn enable_member_mode(tx: &mut Transaction<'_, Postgres>, role: &str) -> Result<()> {
    if role == "member" {
        queries::enable_project_mode().execute(&mut **tx).await?;
    }
    Ok(())
}

async fn protect_admin(
    tx: &mut Transaction<'_, Postgres>,
    current: &User,
    role: &str,
    enabled: bool,
) -> Result<()> {
    if current.role == "admin" && current.enabled && (role != "admin" || !enabled) {
        let count: i64 = queries::admin_count().fetch_one(&mut **tx).await?;
        if count <= 1 {
            return Err(crate::model::Error::LastAdministrator.into());
        }
    }
    Ok(())
}

pub(in super::super) async fn create_user(
    pool: &PgPool,
    principal: &Principal,
    input: UserCreate,
) -> Result<User> {
    let mut tx = admin_transaction(pool, principal).await?;
    if input.role == "member" {
        let admin: bool = queries::has_admin().fetch_one(&mut *tx).await?;
        if !admin {
            return Err(crate::model::Error::LastAdministrator.into());
        }
    }
    enable_member_mode(&mut tx, &input.role).await?;
    let row: Option<User> = queries::insert_user(&input)
        .fetch_optional(&mut *tx)
        .await?;
    let row = row.ok_or_else(|| s3s::s3_error!(OperationAborted))?;
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
    pool: &PgPool,
    principal: &Principal,
    id: Uuid,
    input: UserPatch,
) -> Result<User> {
    let mut tx = admin_transaction(pool, principal).await?;
    // Mode changes lock metadata before the target user so project settings cannot race.
    queries::lock_project_mode().execute(&mut *tx).await?;
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
    let row = queries::update_user(id, role, enabled)
        .fetch_one(&mut *tx)
        .await?;
    queries::delete_user_sessions(id).execute(&mut *tx).await?;
    checkpoint_event(&mut tx,"user.update",&id.to_string(),serde_json::json!({"username":current.username,"old_role":current.role,"role":role,"enabled":enabled})).await?;
    tx.commit().await?;
    Ok(row)
}

pub(in super::super) async fn delete_user(
    pool: &PgPool,
    principal: &Principal,
    id: Uuid,
) -> Result<()> {
    let mut tx = admin_transaction(pool, principal).await?;
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
    pool: &PgPool,
    principal: &Principal,
    id: Uuid,
    hash: &str,
    require_change: bool,
) -> Result<()> {
    let mut tx = admin_transaction(pool, principal).await?;
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
    pool: &PgPool,
    principal: &Principal,
    project: Uuid,
    user: Uuid,
    input: Option<Membership>,
) -> Result<()> {
    let mut tx = admin_transaction(pool, principal).await?;
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
        let valid: Vec<Uuid> = queries::lock_project_buckets(&buckets, project)
            .fetch_all(&mut *tx)
            .await?;
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
    pool: &PgPool,
    principal: &Principal,
    id: Option<Uuid>,
    input: ProjectInput,
) -> Result<Project> {
    let mut tx = admin_transaction(pool, principal).await?;
    queries::lock_project_mode().execute(&mut *tx).await?;
    let duplicate: bool = queries::duplicate_project(input.name.trim(), id)
        .fetch_one(&mut *tx)
        .await?;
    if duplicate {
        return Err(s3s::s3_error!(OperationAborted).into());
    }
    let row: Project = if let Some(id) = id {
        queries::update_project(id, &input)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchKey))?
    } else {
        queries::enable_project_mode().execute(&mut *tx).await?;
        queries::insert_project(&input).fetch_one(&mut *tx).await?
    };
    checkpoint_event(&mut tx,if id.is_some(){"project.update"}else{"project.create"},&row.id.to_string(),serde_json::json!({"project_id":row.id,"name":row.name,"allow_bucket_create":row.allow_bucket_create})).await?;
    tx.commit().await?;
    Ok(row)
}

pub(in super::super) async fn delete_project(
    pool: &PgPool,
    principal: &Principal,
    id: Uuid,
) -> Result<()> {
    let mut tx = admin_transaction(pool, principal).await?;
    let exists: Option<Uuid> = queries::lock_project(id).fetch_optional(&mut *tx).await?;
    if exists.is_none() {
        return Err(s3s::s3_error!(NoSuchKey).into());
    }
    let dependent: bool = queries::project_dependents(id).fetch_one(&mut *tx).await?;
    if dependent {
        return Err(s3s::s3_error!(OperationAborted).into());
    }
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
    pool: &PgPool,
    principal: &Principal,
    enabled: bool,
) -> Result<()> {
    let mut tx = admin_transaction(pool, principal).await?;
    queries::lock_project_mode().execute(&mut *tx).await?;
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

struct AdminAccess {
    tx: Transaction<'static, Postgres>,
}

#[async_trait::async_trait]
impl crate::model::AdminGuard for AdminAccess {
    async fn checkpoint(&mut self, action: &str, target: &str, detail: Value) -> Result<()> {
        checkpoint_event(&mut self.tx, action, target, detail).await
    }
    async fn finish(self: Box<Self>) -> Result<()> {
        self.tx.commit().await?;
        Ok(())
    }
}

pub(in super::super) async fn hold_admin(
    pool: &PgPool,
    principal: &Principal,
) -> Result<Box<dyn crate::model::AdminGuard>> {
    let mut tx = pool.begin().await?;
    principal.lock_admin(&mut tx).await?;
    Ok(Box::new(AdminAccess { tx }))
}
