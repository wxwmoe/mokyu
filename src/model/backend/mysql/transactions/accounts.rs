use super::*;

pub(in super::super) async fn revoke_managed_key(
    pool: &MySqlPool,
    principal: &Principal,
    id: Uuid,
) -> Result<()> {
    let mut tx = administration_transaction(pool, principal).await?;
    lock_mode(&mut tx).await?;
    queries::lock_managed_key(id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    if queries::managed_key_in_use(&mut tx, id).await? {
        return Err(s3s::s3_error!(
            InvalidRequest,
            "key is still used by a storage, physical encoding, pending upload or task"
        )
        .into());
    }
    queries::revoke_managed_key(id).execute(&mut *tx).await?;
    checkpoint_event(
        &mut tx,
        "managed_key.revoke",
        &id.to_string(),
        serde_json::json!({}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) fn denied() -> anyhow::Error {
    s3s::s3_error!(AccessDenied).into()
}

pub(in super::super) async fn admin_transaction(
    pool: &MySqlPool,
    principal: &Principal,
) -> Result<Transaction<'static, MySql>> {
    let mut tx = pool.begin().await?;
    if !queries::validate_identity(principal, &mut tx, true).await? {
        return Err(denied());
    }
    Ok(tx)
}

struct AdminAccess {
    transaction: Transaction<'static, MySql>,
}

#[async_trait::async_trait]
impl crate::model::AdminGuard for AdminAccess {
    async fn checkpoint(&mut self, action: &str, target: &str, detail: Value) -> Result<()> {
        checkpoint_event(&mut self.transaction, action, target, detail).await
    }

    async fn finish(self: Box<Self>) -> Result<()> {
        self.transaction.commit().await?;
        Ok(())
    }
}

pub(in super::super) async fn hold_admin(
    pool: &MySqlPool,
    principal: &Principal,
) -> Result<Box<dyn crate::model::AdminGuard>> {
    Ok(Box::new(AdminAccess {
        transaction: admin_transaction(pool, principal).await?,
    }))
}

pub(in super::super) async fn lock_identity(
    connection: &mut MySqlConnection,
    principal: &Principal,
) -> Result<bool> {
    queries::validate_identity(principal, connection, true).await
}

pub(in super::super) async fn lock_permit(
    connection: &mut MySqlConnection,
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

async fn create_refresh(
    tx: &mut Transaction<'_, MySql>,
    session: Uuid,
    family: Uuid,
    hash: &[u8],
) -> Result<Uuid> {
    let id = Uuid::new_v4();
    queries::insert_refresh(id, session, family, hash)
        .execute(&mut **tx)
        .await?;
    Ok(id)
}

async fn replace_session_refresh(
    tx: &mut Transaction<'_, MySql>,
    session: Uuid,
    hash: &[u8],
) -> Result<()> {
    queries::revoke_refreshes(session)
        .execute(&mut **tx)
        .await?;
    create_refresh(tx, session, Uuid::new_v4(), hash).await?;
    Ok(())
}

pub(in super::super) async fn login_session(
    pool: &MySqlPool,
    input: SessionLogin<'_>,
) -> Result<()> {
    let (user, verified_hash, revision) = input.verified;
    let mut tx = pool.begin().await?;
    // Password reset and disable update this same row before revoking sessions.
    let current = queries::lock_login_user(*user)
        .fetch_optional(&mut *tx)
        .await?;
    if !current.is_some_and(|(hash, enabled, current_revision)| {
        enabled && hash == *verified_hash && current_revision == *revision
    }) {
        return Err(denied());
    }
    #[cfg(feature = "fault-injection")]
    crate::faults::point("login-before-session").await;
    let session = Uuid::new_v4();
    queries::insert_session(session, &input)
        .execute(&mut *tx)
        .await?;
    create_refresh(&mut tx, session, Uuid::new_v4(), input.refresh_hash).await?;
    security_audit(&mut tx, *user, "session.login", true, input.request_id).await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn failed_login(
    pool: &MySqlPool,
    user: Uuid,
    request: &str,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    security_audit(&mut tx, user, "session.login", false, request).await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn refresh_session(
    pool: &MySqlPool,
    input: SessionRefresh<'_>,
) -> Result<RefreshedSession> {
    let hash = blake3::hash(input.supplied.as_bytes());
    let mut tx = pool.begin().await?;
    let user = queries::refresh_user(hash.as_bytes())
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(denied)?;
    // Match password reset and session revocation lock order.
    let revision = queries::lock_refresh_user(user)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(denied)?;
    let mut credential = queries::lock_refresh(hash.as_bytes())
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(denied)?;
    let now = Utc::now();
    let mut secret = input.supplied.to_owned();
    let mut issue_refresh = false;
    if let Some(revoked) = credential.revoked_at {
        let successor = queries::lock_refresh_successor(credential.replaced_by)
            .fetch_optional(&mut *tx)
            .await?;
        if let Some(next) = successor.filter(|_| revoked + chrono::Duration::seconds(30) > now) {
            // Concurrent rotation responses return identical credentials without storing secrets.
            secret = derived_token(input.key, "mokyu-refresh-v1", input.supplied);
            credential = next;
            issue_refresh = true;
        } else {
            let active =
                queries::active_refresh_family(credential.session_id, credential.family_id)
                    .fetch_one(&mut *tx)
                    .await?;
            if active {
                queries::delete_refresh_session(credential.session_id)
                    .execute(&mut *tx)
                    .await?;
                security_audit(
                    &mut tx,
                    user,
                    "session.refresh_reuse",
                    false,
                    input.request_id,
                )
                .await?;
                tx.commit().await?;
            }
            return Err(denied());
        }
    }
    if credential.expires_at <= now {
        return Err(denied());
    }
    let expires = queries::lock_refresh_session(credential.session_id, user, revision)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(denied)?;
    if credential.expires_at <= now + chrono::Duration::days(7) {
        secret = derived_token(input.key, "mokyu-refresh-v1", &secret);
        queries::revoke_refresh(credential.id)
            .execute(&mut *tx)
            .await?;
        let new_id = create_refresh(
            &mut tx,
            credential.session_id,
            credential.family_id,
            blake3::hash(secret.as_bytes()).as_bytes(),
        )
        .await?;
        queries::replace_refresh(credential.id, new_id)
            .execute(&mut *tx)
            .await?;
        credential.id = new_id;
        issue_refresh = true;
    }
    let access = derived_token(input.key, "mokyu-session-v1", &secret);
    let csrf = csrf_token(&access);
    let issue_access = issue_refresh
        || input.access != Some(access.as_str())
        || expires <= now + chrono::Duration::seconds((input.seconds / 4).max(1) as i64);
    let expires = if issue_access {
        now + chrono::Duration::seconds(input.seconds as i64)
    } else {
        expires
    };
    queries::refresh_session(
        credential.session_id,
        blake3::hash(access.as_bytes()).as_bytes(),
        blake3::hash(csrf.as_bytes()).as_bytes(),
        expires,
    )
    .execute(&mut *tx)
    .await?;
    queries::touch_refresh(credential.id)
        .execute(&mut *tx)
        .await?;
    security_audit(&mut tx, user, "session.refresh", true, input.request_id).await?;
    tx.commit().await?;
    Ok(RefreshedSession {
        user,
        session: credential.session_id,
        access,
        csrf,
        refresh: secret,
        expires,
        refreshed_at: now,
        issue_access,
    })
}

async fn lock_account_session(
    tx: &mut Transaction<'_, MySql>,
    user: Uuid,
    current: &[u8],
) -> Result<(String, i64)> {
    let (hash, revision, enabled) = queries::lock_session_user(user)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(denied)?;
    if !enabled
        || !queries::active_session(user, current, revision)
            .fetch_one(&mut **tx)
            .await?
    {
        return Err(denied());
    }
    Ok((hash, revision))
}

async fn lock_verified_account(
    tx: &mut Transaction<'_, MySql>,
    verified: &VerifiedAccount,
    current: &[u8],
) -> Result<()> {
    let (hash, revision) = lock_account_session(tx, verified.0, current).await?;
    if hash != verified.1 || revision != verified.2 {
        return Err(denied());
    }
    Ok(())
}

pub(in super::super) async fn logout_session(
    pool: &MySqlPool,
    user: Uuid,
    current: &[u8],
) -> Result<()> {
    let mut tx = pool.begin().await?;
    lock_account_session(&mut tx, user, current).await?;
    queries::delete_access_session(current)
        .execute(&mut *tx)
        .await?;
    checkpoint_event(
        &mut tx,
        "session.logout",
        &user.to_string(),
        serde_json::json!({}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn account_preferences(
    pool: &MySqlPool,
    user: Uuid,
    current: &[u8],
    input: Preferences,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    lock_account_session(&mut tx, user, current).await?;
    queries::account_preferences(user, &input)
        .execute(&mut *tx)
        .await?;
    checkpoint_event(
        &mut tx,
        "account.preferences",
        &user.to_string(),
        serde_json::json!({}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn account_password(
    pool: &MySqlPool,
    verified: &VerifiedAccount,
    current: &[u8],
    password: &str,
    refresh: &[u8],
) -> Result<()> {
    let mut tx = pool.begin().await?;
    lock_verified_account(&mut tx, verified, current).await?;
    queries::change_password(verified.0, password)
        .execute(&mut *tx)
        .await?;
    queries::delete_other_sessions(verified.0, current)
        .execute(&mut *tx)
        .await?;
    queries::session_revision(current, verified.2 + 1)
        .execute(&mut *tx)
        .await?;
    let session = queries::session_id(current).fetch_one(&mut *tx).await?;
    replace_session_refresh(&mut tx, session, refresh).await?;
    checkpoint_event(
        &mut tx,
        "account.password",
        &verified.0.to_string(),
        serde_json::json!({}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn reauthenticate_account(
    pool: &MySqlPool,
    verified: &VerifiedAccount,
    current: &[u8],
    refresh: &[u8],
) -> Result<()> {
    let mut tx = pool.begin().await?;
    lock_verified_account(&mut tx, verified, current).await?;
    queries::reauthenticate_session(current)
        .execute(&mut *tx)
        .await?;
    let session = queries::session_id(current).fetch_one(&mut *tx).await?;
    replace_session_refresh(&mut tx, session, refresh).await?;
    checkpoint_event(
        &mut tx,
        "session.reauthenticate",
        &verified.0.to_string(),
        serde_json::json!({}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn revoke_account_sessions(
    pool: &MySqlPool,
    user: Uuid,
    current: &[u8],
    session: Option<Uuid>,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    lock_account_session(&mut tx, user, current).await?;
    if let Some(id) = session {
        queries::delete_account_session(id, user)
            .execute(&mut *tx)
            .await?;
        checkpoint_event(
            &mut tx,
            "session.revoke",
            &id.to_string(),
            serde_json::json!({}),
        )
        .await?;
    } else {
        queries::delete_other_sessions(user, current)
            .execute(&mut *tx)
            .await?;
        checkpoint_event(
            &mut tx,
            "session.revoke_others",
            &user.to_string(),
            serde_json::json!({}),
        )
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn account_setup_token(pool: &MySqlPool, hash: &[u8]) -> Result<()> {
    let mut tx = pool.begin().await?;
    queries::lock_setup_metadata().execute(&mut *tx).await?;
    ensure!(
        !queries::has_accounts().fetch_one(&mut *tx).await?,
        "an administrator already exists"
    );
    queries::save_setup_token(hash).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn initialize_admin(
    pool: &MySqlPool,
    username: &str,
    password: &str,
    require_change: bool,
) -> Result<Uuid> {
    let mut tx = pool.begin().await?;
    queries::lock_accounts().execute(&mut *tx).await?;
    ensure!(
        !queries::has_accounts().fetch_one(&mut *tx).await?,
        "an administrator already exists"
    );
    let id = Uuid::new_v4();
    queries::insert_initial_admin(id, username, password, require_change)
        .execute(&mut *tx)
        .await?;
    security_audit(&mut tx, id, "account.init", true, "cli-init").await?;
    tx.commit().await?;
    Ok(id)
}

pub(in super::super) async fn setup_account(
    pool: &MySqlPool,
    username: &str,
    password: &str,
    supplied: &[u8],
    request: &str,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    queries::lock_accounts().execute(&mut *tx).await?;
    if !queries::setup_allowed(supplied).fetch_one(&mut *tx).await? {
        return Err(denied());
    }
    let user = Uuid::new_v4();
    queries::insert_setup_admin(user, username, password)
        .execute(&mut *tx)
        .await?;
    security_audit(&mut tx, user, "account.setup", true, request).await?;
    queries::delete_setup_tokens().execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}

pub(crate) async fn checkpoint_audit(
    db: &mut MySqlConnection,
    id: Id,
    action: &str,
    target: &str,
    project: Option<Uuid>,
    bucket: Option<Uuid>,
    detail: Value,
) -> Result<()> {
    let changed = queries::checkpoint_audit(id, action, target, project, bucket, detail)
        .execute(db)
        .await?
        .rows_affected();
    ensure!(changed == 1, "audit intent is missing");
    Ok(())
}

async fn security_audit(
    db: &mut MySqlConnection,
    user: Uuid,
    action: &str,
    success: bool,
    request: &str,
) -> Result<()> {
    // Serialize rejected password events to keep the existing per-minute bound.
    queries::lock_audit_user(user).execute(&mut *db).await?;
    queries::security_audit(user, action, success, request)
        .execute(db)
        .await?;
    Ok(())
}

pub(crate) async fn checkpoint_event(
    tx: &mut Transaction<'_, MySql>,
    action: &str,
    target: &str,
    detail: Value,
) -> Result<()> {
    let Some(id) = crate::model::audit::current() else {
        return Ok(());
    };
    let project = detail
        .get("project_id")
        .and_then(Value::as_str)
        .and_then(|s| s.parse::<Uuid>().ok());
    let bucket = detail
        .get("bucket_id")
        .and_then(Value::as_str)
        .and_then(|s| s.parse::<Uuid>().ok());
    ensure!(
        serde_json::to_vec(&detail)?.len() <= 262144,
        "audit detail exceeds its bound"
    );
    checkpoint_audit(tx, id, action, target, project, bucket, detail).await
}

pub(in super::super) async fn runtime_settings_sections(
    pool: &MySqlPool,
    principal: &Principal,
) -> Result<Vec<(String, Value, i64)>> {
    let mut tx = admin_transaction(pool, principal).await?;
    let rows = queries::runtime_settings().fetch_all(&mut *tx).await?;
    tx.commit().await?;
    Ok(rows)
}
