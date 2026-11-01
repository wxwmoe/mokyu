use super::*;

pub(in super::super) async fn identify(
    connection: &mut SqliteConnection,
    fingerprint: &[u8],
) -> Result<()> {
    let mut tx = connection.begin_with(queries::BEGIN_WRITE).await?;
    if let Some(saved) = queries::fingerprint().fetch_one(&mut *tx).await? {
        ensure!(
            saved == fingerprint,
            "deployment key does not match this database"
        );
    } else {
        queries::set_fingerprint(fingerprint)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(())
}

pub(super) fn denied() -> anyhow::Error {
    s3s::s3_error!(AccessDenied).into()
}

async fn create_refresh(
    tx: &mut Transaction<'_, Sqlite>,
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
    tx: &mut Transaction<'_, Sqlite>,
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
    pool: &SqlitePool,
    input: SessionLogin<'_>,
) -> Result<()> {
    let (user, verified_hash, revision) = input.verified;
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
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
    pool: &SqlitePool,
    user: Uuid,
    request: &str,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    security_audit(&mut tx, user, "session.login", false, request).await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn refresh_session(
    pool: &SqlitePool,
    input: SessionRefresh<'_>,
) -> Result<RefreshedSession> {
    let hash = blake3::hash(input.supplied.as_bytes());
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let user = queries::refresh_user(hash.as_bytes())
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(denied)?;
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
    let expires = queries::datetime(
        queries::lock_refresh_session(credential.session_id, user, revision)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(denied)?,
    )?;
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
    tx: &mut Transaction<'_, Sqlite>,
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
    tx: &mut Transaction<'_, Sqlite>,
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
    pool: &SqlitePool,
    user: Uuid,
    current: &[u8],
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
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
    pool: &SqlitePool,
    user: Uuid,
    current: &[u8],
    input: Preferences,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
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
    pool: &SqlitePool,
    verified: &VerifiedAccount,
    current: &[u8],
    password: &str,
    refresh: &[u8],
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
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
    pool: &SqlitePool,
    verified: &VerifiedAccount,
    current: &[u8],
    refresh: &[u8],
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
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
    pool: &SqlitePool,
    user: Uuid,
    current: &[u8],
    session: Option<Uuid>,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
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

pub(in super::super) async fn account_setup_token(pool: &SqlitePool, hash: &[u8]) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    ensure!(
        !queries::has_accounts().fetch_one(&mut *tx).await?,
        "an administrator already exists"
    );
    queries::save_setup_token(hash).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}

pub(in super::super) async fn initialize_admin(
    pool: &SqlitePool,
    username: &str,
    password: &str,
    require_change: bool,
) -> Result<Uuid> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
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
    pool: &SqlitePool,
    username: &str,
    password: &str,
    supplied: &[u8],
    request: &str,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
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
    db: &mut SqliteConnection,
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
