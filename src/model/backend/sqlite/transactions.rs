//! SQLite transactions serialize writes before reading their decision state.
use super::queries;
use crate::model::Id;
use anyhow::{Context, Result, ensure};
use sqlx::{Connection, SqliteConnection};

pub(super) async fn identify(connection: &mut SqliteConnection, fingerprint: &[u8]) -> Result<()> {
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

use crate::model::account::{
    Preferences, RefreshedSession, SessionLogin, SessionRefresh, VerifiedAccount, csrf_token,
    derived_token,
};
use chrono::Utc;
use serde_json::Value;
use sqlx::{Sqlite, SqlitePool, Transaction};
use uuid::Uuid;

fn denied() -> anyhow::Error {
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

pub(super) async fn login_session(pool: &SqlitePool, input: SessionLogin<'_>) -> Result<()> {
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

pub(super) async fn failed_login(pool: &SqlitePool, user: Uuid, request: &str) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    security_audit(&mut tx, user, "session.login", false, request).await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn refresh_session(
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

pub(super) async fn logout_session(pool: &SqlitePool, user: Uuid, current: &[u8]) -> Result<()> {
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

pub(super) async fn account_preferences(
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

pub(super) async fn account_password(
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

pub(super) async fn reauthenticate_account(
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

pub(super) async fn revoke_account_sessions(
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

pub(super) async fn account_setup_token(pool: &SqlitePool, hash: &[u8]) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    ensure!(
        !queries::has_accounts().fetch_one(&mut *tx).await?,
        "an administrator already exists"
    );
    queries::save_setup_token(hash).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn initialize_admin(
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

pub(super) async fn setup_account(
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

async fn security_audit(
    db: &mut SqliteConnection,
    user: Uuid,
    action: &str,
    success: bool,
    request: &str,
) -> Result<()> {
    queries::security_audit(user, action, success, request)
        .execute(db)
        .await?;
    Ok(())
}

pub(crate) async fn checkpoint_event(
    tx: &mut Transaction<'_, Sqlite>,
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

use crate::authorization::Principal;
use crate::model::settings::{ControlSettingsPatch, RuntimeSettingsRow};

pub(super) async fn admin_transaction(
    pool: &SqlitePool,
    principal: &Principal,
) -> Result<Transaction<'static, Sqlite>> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    if !queries::validate_identity(principal, &mut tx).await? {
        return Err(denied());
    }
    Ok(tx)
}
pub(super) async fn ensure_runtime_settings(
    pool: &SqlitePool,
    defaults: &[(&str, Value)],
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    for (section, value) in defaults {
        queries::ensure_runtime_setting(section, value)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(())
}
pub(super) async fn runtime_settings_sections(
    pool: &SqlitePool,
    principal: &Principal,
) -> Result<Vec<(String, Value, i64)>> {
    let mut tx = pool.begin().await?;
    if !queries::validate_identity(principal, &mut tx).await? {
        return Err(denied());
    }
    let rows = queries::runtime_settings().fetch_all(&mut *tx).await?;
    tx.commit().await?;
    Ok(rows)
}
pub(super) async fn runtime_settings_section(
    pool: &SqlitePool,
    principal: &Principal,
    section: &str,
) -> Result<RuntimeSettingsRow> {
    let mut tx = pool.begin().await?;
    if !queries::validate_identity(principal, &mut tx).await? {
        return Err(denied());
    }
    let row = queries::runtime_settings_row(section)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(row)
}
pub(super) async fn save_runtime_settings(
    pool: &SqlitePool,
    principal: &Principal,
    section: &str,
    revision: Option<i64>,
    value: Value,
) -> Result<RuntimeSettingsRow> {
    let mut tx = admin_transaction(pool, principal).await?;
    let revision = match revision {
        Some(revision) => revision,
        None => {
            queries::lock_runtime_settings(section)
                .fetch_one(&mut *tx)
                .await?
        }
    };
    let changed =
        queries::save_runtime_settings(section, value, principal.user_id().is_none(), revision)
            .execute(&mut *tx)
            .await?
            .rows_affected();
    ensure!(
        changed == 1,
        "settings revision conflict; reload before saving"
    );
    let row = queries::runtime_settings_row(section)
        .fetch_one(&mut *tx)
        .await?;
    checkpoint_event(
        &mut tx,
        "runtime.settings",
        section,
        serde_json::json!({"revision":row.revision}),
    )
    .await?;
    tx.commit().await?;
    Ok(row)
}
pub(super) async fn reset_runtime_settings(
    pool: &SqlitePool,
    principal: &Principal,
    section: &str,
    value: Value,
) -> Result<RuntimeSettingsRow> {
    let mut tx = admin_transaction(pool, principal).await?;
    let changed = queries::reset_runtime_settings(section, value, principal.user_id().is_none())
        .execute(&mut *tx)
        .await?
        .rows_affected();
    ensure!(changed == 1, "runtime settings section is missing");
    let row = queries::runtime_settings_row(section)
        .fetch_one(&mut *tx)
        .await?;
    checkpoint_event(
        &mut tx,
        "runtime.settings",
        section,
        serde_json::json!({"revision":row.revision}),
    )
    .await?;
    tx.commit().await?;
    Ok(row)
}
pub(super) async fn update_web_settings(
    pool: &SqlitePool,
    principal: &Principal,
    input: ControlSettingsPatch,
) -> Result<(crate::config::WebSettings, i64)> {
    let mut tx = admin_transaction(pool, principal).await?;
    let current = queries::lock_web_settings().fetch_one(&mut *tx).await?;
    let mut control = current.control()?;
    if let Some(value) = input.origin {
        control.origin = value;
    }
    if let Some(value) = input.secure_cookie {
        control.secure_cookie = value;
    }
    if let Some(value) = input.session_lifetime {
        control.session_lifetime = value;
    }
    if let Some(value) = input.gravatar_base_url {
        control.gravatar_base_url = value;
    }
    control.validate()?;
    if let Some(revision) = input.revision {
        anyhow::ensure!(
            revision == current.revision,
            "settings revision conflict; reload before saving"
        );
    }
    let value = serde_json::json!({
        "origin": control.origin,
        "secure_cookie": control.secure_cookie,
        "session_lifetime": control.session_lifetime,
        "gravatar_base_url": control.gravatar_base_url,
    });
    let next_revision = current
        .revision
        .checked_add(1)
        .context("settings revision overflow")?;
    queries::save_web_settings(value, next_revision, principal.user_id(), current.revision)
        .execute(&mut *tx)
        .await?;
    checkpoint_event(
        &mut tx,
        "control.settings",
        "",
        serde_json::json!({
            "origin": control.origin,
            "secure_cookie": control.secure_cookie,
            "session_lifetime": control.session_lifetime,
            "gravatar_base_url": control.gravatar_base_url,
        }),
    )
    .await?;
    tx.commit().await?;
    Ok((control, next_revision))
}

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

pub(super) async fn hold_admin(
    backend: &super::Backend,
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
pub(super) async fn lock_permit(
    connection: &mut SqliteConnection,
    permit: &crate::authorization::Permit,
) -> Result<()> {
    let admin = queries::validate_identity(&permit.principal, connection).await?;
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

use crate::model::storage::{StorageCreation, StorageEdit, StorageInitializer, StorageUpdate};
use crate::storage::{
    model::Record as BackendRecord,
    profile::{StorageProfile, validate as validate_storage_profile},
};

async fn save_storage_profile(
    tx: &mut Transaction<'_, Sqlite>,
    profile: &StorageProfile,
) -> Result<()> {
    let affected = queries::save_storage_profile(profile)
        .execute(&mut **tx)
        .await?
        .rows_affected();
    if affected != 1 {
        return Err(crate::model::Error::StorageProfileConflict.into());
    }
    Ok(())
}
async fn validate_block_key(
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

async fn load_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    name: &str,
) -> anyhow::Result<crate::model::storage::StorageRow> {
    queries::storage_row_lock(name)
        .fetch_optional(&mut **tx)
        .await?
        .context("storage was not found")
}

pub(super) async fn update_storage_profile(
    pool: &SqlitePool,
    name: String,
    profile: StorageProfile,
) -> anyhow::Result<Uuid> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let (storage_id, storage_type, key): (Uuid, String, Option<Uuid>) =
        queries::storage_profile_target(&name)
            .fetch_optional(&mut *tx)
            .await?
            .context("storage was not found")?;
    anyhow::ensure!(
        profile.storage_id == storage_id,
        "storage profile belongs to another storage"
    );
    validate_storage_profile(&profile, &storage_type)?;
    profile.validate_key(key)?;
    save_storage_profile(&mut tx, &profile).await?;
    checkpoint_event(
        &mut tx,
        "storage.profile.update",
        &name,
        serde_json::json!({"revision": profile.revision + 1}),
    )
    .await?;
    tx.commit().await?;
    Ok(storage_id)
}

struct PreparedStorage {
    record: BackendRecord,
    key: Option<Uuid>,
    read: Option<i32>,
    upload: Option<i32>,
    control: Option<i32>,
    profile: Option<StorageProfile>,
}

impl PreparedStorage {
    async fn insert(&self, tx: &mut Transaction<'_, Sqlite>) -> Result<()> {
        validate_block_key(tx, self.key).await?;
        queries::insert_storage_record(&self.record, self.key)
            .execute(&mut **tx)
            .await?;
        queries::insert_storage_profile(
            self.record.id,
            self.read,
            self.upload,
            self.control,
            self.record.r#type == "s3",
        )
        .execute(&mut **tx)
        .await?;
        if let Some(profile) = &self.profile {
            save_storage_profile(tx, profile).await?;
        }
        Ok(())
    }
}

struct StorageInitialization {
    write: SqlitePool,
    attempted: bool,
    prepared: Option<PreparedStorage>,
}

#[async_trait::async_trait]
impl StorageInitializer for StorageInitialization {
    async fn prepare(
        &mut self,
        record: &BackendRecord,
        key: Option<Uuid>,
        read: Option<i32>,
        upload: Option<i32>,
        control: Option<i32>,
        mut profile: Option<StorageProfile>,
    ) -> Result<StorageProfile> {
        ensure!(!self.attempted, "storage preparation was already attempted");
        self.attempted = true;
        if let Some(profile) = &mut profile {
            profile.storage_id = record.id;
            profile.revision = 1;
            validate_storage_profile(profile, &record.r#type)?;
            profile.validate_key(key)?;
        }
        let prepared = PreparedStorage {
            record: record.clone(),
            key,
            read,
            upload,
            control,
            profile,
        };
        // Validate native constraints, then release the writer before external storage I/O.
        let mut tx = self.write.begin_with(queries::BEGIN_WRITE).await?;
        prepared.insert(&mut tx).await?;
        let effective = queries::storage_profile(record.id)
            .fetch_one(&mut *tx)
            .await?;
        validate_storage_profile(&effective, &record.r#type)?;
        tx.rollback().await?;
        self.prepared = Some(prepared);
        Ok(effective)
    }
    async fn finish(mut self: Box<Self>) -> Result<()> {
        let prepared = self.prepared.take().context("storage was not prepared")?;
        let mut tx = self.write.begin_with(queries::BEGIN_WRITE).await?;
        prepared.insert(&mut tx).await?;
        tx.commit().await?;
        Ok(())
    }
}

pub(super) async fn storage_creation(
    read: &SqlitePool,
    write: &SqlitePool,
) -> Result<StorageCreation> {
    Ok(StorageCreation {
        deployment: queries::storage_deployment().fetch_one(read).await?,
        mode: "single".into(),
        writer: Box::new(StorageInitialization {
            write: write.clone(),
            attempted: false,
            prepared: None,
        }),
    })
}

struct StorageModification {
    original_type: String,
    write: SqlitePool,
    id: Uuid,
    revision: i64,
}

pub(super) async fn storage_edit(
    read: &SqlitePool,
    write: &SqlitePool,
    name: &str,
) -> Result<StorageEdit> {
    let mut tx = read.begin().await?;
    let row = load_row(&mut tx, name).await?;
    let profile = queries::storage_profile_lock(row.id)
        .fetch_one(&mut *tx)
        .await?;
    let id = row.id;
    let revision = profile.revision;
    tx.commit().await?;
    let original_type = row.r#type.clone();
    Ok(StorageEdit {
        row,
        profile,
        mode: "single".into(),
        update: Box::new(StorageModification {
            original_type,
            write: write.clone(),
            id,
            revision,
        }),
    })
}

#[async_trait::async_trait]
impl StorageUpdate for StorageModification {
    async fn finish(
        self: Box<Self>,
        record: &BackendRecord,
        key: Option<Uuid>,
        profile: &StorageProfile,
    ) -> Result<()> {
        ensure!(
            record.id == self.id && profile.storage_id == self.id,
            "storage update belongs to another storage"
        );
        ensure!(
            profile.revision == self.revision,
            "storage update revision changed"
        );
        validate_storage_profile(profile, &record.r#type)?;
        profile.validate_key(key)?;
        let mut tx = self.write.begin_with(queries::BEGIN_WRITE).await?;
        if self.original_type != record.r#type {
            ensure!(
                queries::storage_relocation_ready()
                    .fetch_one(&mut *tx)
                    .await?,
                crate::model::Error::StorageNotDrained
            );
            queries::relocate_raw_sources(
                record.id,
                if record.r#type == "local" {
                    "local"
                } else {
                    "shared_s3"
                },
            )
            .execute(&mut *tx)
            .await?;
        }
        validate_block_key(&mut tx, key).await?;
        queries::update_storage_record(record, key)
            .execute(&mut *tx)
            .await?;
        save_storage_profile(&mut tx, profile).await?;
        tx.commit().await?;
        Ok(())
    }
}

pub(super) async fn storage_current_key(
    pool: &SqlitePool,
    name: &str,
    key: Option<&str>,
) -> anyhow::Result<Uuid> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let row = load_row(&mut tx, name).await?;
    let key_id = if let Some(key) = key {
        let parsed = key.parse::<Uuid>().ok();
        queries::current_storage_key(parsed, key.trim())
            .fetch_optional(&mut *tx)
            .await?
            .context("active block key was not found")
            .map(Some)?
    } else {
        None
    };
    if key_id.is_some() {
        queries::storage_profile_lock(row.id)
            .fetch_one(&mut *tx)
            .await?
            .validate_key(key_id)?;
    }
    queries::update_storage_key(row.id, key_id)
        .execute(&mut *tx)
        .await?;
    queries::bump_storage_profile(row.id)
        .execute(&mut *tx)
        .await?;
    checkpoint_event(
        &mut tx,
        "storage.current_key.update",
        name,
        serde_json::json!({"block_key_id":key_id}),
    )
    .await?;
    tx.commit().await?;
    Ok(row.id)
}

use crate::model::administration::{
    Membership, Project, ProjectInput, User, UserCreate, UserPatch,
};

async fn administration_transaction(
    pool: &SqlitePool,
    principal: &Principal,
) -> Result<Transaction<'static, Sqlite>> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    if !queries::validate_identity(principal, &mut tx).await? {
        return Err(denied());
    }
    Ok(tx)
}

async fn enable_member_mode(tx: &mut Transaction<'_, Sqlite>, role: &str) -> Result<()> {
    if role == "member" {
        queries::enable_project_mode().execute(&mut **tx).await?;
    }
    Ok(())
}

async fn protect_admin(
    tx: &mut Transaction<'_, Sqlite>,
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

pub(super) async fn create_user(
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

pub(super) async fn update_user(
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

pub(super) async fn delete_user(pool: &SqlitePool, principal: &Principal, id: Uuid) -> Result<()> {
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

pub(super) async fn reset_password(
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

pub(super) async fn save_member(
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

pub(super) async fn save_project(
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

pub(super) async fn delete_project(
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

pub(super) async fn set_project_mode(
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

use crate::model::administration::BucketGrant;
use crate::model::credential::{
    self, Credential, CredentialInput, CredentialMaterial, CredentialSettings, RotateCredential,
    Token, TokenInput, TokenMaterial,
};

pub(super) async fn get_credential(
    connection: &mut SqliteConnection,
    access: &str,
) -> Result<Credential> {
    Ok(queries::credential_record(access)
        .fetch_optional(connection)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?)
}

async fn replace_credential_grants(
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

async fn insert_credential(
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

pub(super) async fn create_credential(
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

pub(super) async fn update_credential(
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

pub(super) async fn set_grants(
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

pub(super) async fn revoke_credential(
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

pub(super) async fn enable_credential(
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

pub(super) async fn change_grant(
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

pub(super) async fn rotate_credential(
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

pub(super) async fn get_token(db: &mut SqliteConnection, id: Uuid) -> Result<Token> {
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
    if input.system && (role != "admin" || !input.grants.is_empty()) {
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

pub(super) async fn create_token(
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
    replace_token_grants(&mut tx, id, input.grants).await?;
    let token = get_token(&mut tx, id).await?;
    checkpoint_event(&mut tx,"token.create",&id.to_string(),serde_json::json!({"user_id":user,"label":token.label,"system":token.system,"grants":token.grants,"expires_at":token.expires_at})).await?;
    tx.commit().await?;
    Ok(token)
}

pub(super) async fn update_token(
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
    replace_token_grants(&mut tx, id, input.grants).await?;
    let result = get_token(&mut tx, id).await?;
    checkpoint_event(&mut tx,"token.update",&id.to_string(),serde_json::json!({"user_id":user,"label":result.label,"system":result.system,"grants":result.grants,"expires_at":result.expires_at})).await?;
    tx.commit().await?;
    Ok(result)
}

pub(super) async fn revoke_token(pool: &SqlitePool, principal: &Principal, id: Uuid) -> Result<()> {
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

use crate::content::codec::Chunk;
use crate::model::{ChunkCreate, StreamCreate, StreamTail};
use queries::{QuotaAccount, QuotaReservation};
fn quota_number(value: i128) -> Result<i64> {
    ensure!(
        (0..=i128::from(i64::MAX)).contains(&value),
        crate::model::Error::QuotaExceeded
    );
    Ok(value as i64)
}

pub(super) async fn lock_quota(
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

pub(super) async fn adjust_quota(
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

pub(super) async fn change_reservation(
    connection: &mut SqliteConnection,
    accounts: &mut [QuotaAccount; 2],
    reservation: &mut QuotaReservation,
    logical: i64,
    inflight: i64,
    credit: i64,
) -> Result<()> {
    ensure!(
        reservation.bucket_id == accounts[1].scope_id,
        "quota scope mismatch"
    );
    quota_number(i128::from(logical))?;
    quota_number(i128::from(inflight))?;
    quota_number(i128::from(credit))?;
    let reserved =
        (logical - credit).max(0) - (reservation.logical_bytes - reservation.credit_bytes).max(0);
    adjust_quota(
        connection,
        accounts,
        0,
        reserved,
        inflight - reservation.inflight_bytes,
        0,
    )
    .await?;
    queries::change_quota_reservation(reservation.id, logical, inflight, credit)
        .execute(connection)
        .await?;
    reservation.logical_bytes = logical;
    reservation.inflight_bytes = inflight;
    reservation.credit_bytes = credit;
    Ok(())
}

async fn supersede_object_quota(
    connection: &mut SqliteConnection,
    bucket: uuid::Uuid,
    key: &str,
    stream: Id,
) -> Result<()> {
    let mut accounts = lock_quota(connection, bucket).await?;
    for mut row in queries::superseded_reservations(bucket, key, stream)
        .fetch_all(&mut *connection)
        .await?
    {
        let inflight = row.inflight_bytes;
        change_reservation(connection, &mut accounts, &mut row, 0, inflight, 0).await?;
        queries::clear_quota_credit(row.id)
            .execute(&mut *connection)
            .await?;
        queries::clear_write_contributions(row.id)
            .execute(&mut *connection)
            .await?;
    }
    Ok(())
}

pub(super) async fn create_stream(pool: &sqlx::SqlitePool, mut input: StreamCreate) -> Result<Id> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    if let Some((task, token)) = input.owner {
        ensure!(
            queries::lock_owned_task(task, token)
                .fetch_optional(&mut *tx)
                .await?
                .is_some(),
            "storage migration execution lost"
        );
    }
    input
        .authority
        .add(input.bucket, crate::authorization::Action::Write);
    if input.public {
        input
            .authority
            .add(input.bucket, crate::authorization::Action::Acl);
    }
    lock_permit(&mut tx, &input.authority).await?;
    let (state, paused) = queries::lock_bucket_uploads(input.bucket)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(state == "active", crate::model::Error::OperationAborted);
    if paused && input.stream_type == "object" {
        ensure!(
            queries::completing_upload(input.completion, input.bucket, &input.key)
                .fetch_one(&mut *tx)
                .await?,
            crate::model::Error::OperationAborted
        );
    }
    let inserted = queries::insert_stream(&input).execute(&mut *tx).await?;
    let id = Id::from(inserted.last_insert_rowid());
    if input.claim_object {
        queries::claim_object(input.bucket, &input.key, id)
            .execute(&mut *tx)
            .await?;
        supersede_object_quota(&mut tx, input.bucket, &input.key, id).await?;
    }
    tx.commit().await?;
    Ok(id)
}

pub(super) async fn append_tail(pool: &sqlx::SqlitePool, tail: StreamTail) -> Result<()> {
    ensure!(
        tail.staging_storage.is_none() && tail.staging_key.is_none(),
        "shared staging is unavailable in single mode"
    );
    let fragment = Id::from(
        queries::insert_fragment(&tail)
            .execute(pool)
            .await?
            .last_insert_rowid(),
    );
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("fragment-persisted").await;
    writing_stream(&mut tx, tail.stream).await?;
    queries::seal_fragment(fragment).execute(&mut *tx).await?;
    queries::insert_fragment_extent(&tail, fragment)
        .execute(&mut *tx)
        .await?;
    queries::resize_writing_stream(tail.stream, tail.offset + i64::from(tail.size))
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

async fn writing_stream(connection: &mut SqliteConnection, stream: Id) -> Result<()> {
    ensure!(
        queries::lock_writing_stream(stream)
            .fetch_optional(connection)
            .await?
            .is_some(),
        crate::model::Error::OperationAborted
    );
    Ok(())
}

pub(super) async fn allocate_chunk(
    pool: &sqlx::SqlitePool,
    input: ChunkCreate,
) -> Result<(Chunk, bool)> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    writing_stream(&mut tx, input.stream).await?;
    if let Some(key) = input.key {
        queries::encoding_key(key, &input.algorithm)
            .fetch_one(&mut *tx)
            .await?;
    }
    queries::fail_chunk_allocations(&input)
        .execute(&mut *tx)
        .await?;
    queries::retire_failed_encodings(&input)
        .execute(&mut *tx)
        .await?;
    let (chunk, reused) = if let Some(chunk) = queries::reusable_chunk(&mut tx, &input).await? {
        (chunk, true)
    } else {
        let chunk = queries::insert_chunk(&input)
            .execute(&mut *tx)
            .await?
            .last_insert_rowid()
            .into();
        let encoding = queries::allocate_encoding(chunk)
            .execute(&mut *tx)
            .await?
            .last_insert_rowid()
            .into();
        queries::set_chunk_encoding(chunk, encoding)
            .execute(&mut *tx)
            .await?;
        queries::refresh_encoding_fingerprint(&mut tx, encoding).await?;
        (queries::allocated_chunk(&mut tx, chunk).await?, false)
    };
    if reused {
        require_sources(&mut tx, None, &[chunk.id]).await?;
    }
    tx.commit().await?;
    Ok((chunk, reused))
}

pub(super) async fn reference_chunk(
    pool: &sqlx::SqlitePool,
    stream: Id,
    offset: i64,
    chunk: Id,
    size: i32,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    writing_stream(&mut tx, stream).await?;
    ensure!(
        queries::lock_chunk(chunk).fetch_one(&mut *tx).await? == "ready",
        crate::model::Error::OperationAborted
    );
    require_sources(&mut tx, None, &[chunk]).await?;
    queries::insert_chunk_extent(stream, offset, size, chunk)
        .execute(&mut *tx)
        .await?;
    queries::changed_references(&mut tx, &[chunk], true).await?;
    queries::pin_chunk(chunk, stream).execute(&mut *tx).await?;
    queries::attach_pending_upload(chunk, stream, offset)
        .execute(&mut *tx)
        .await?;
    queries::resize_writing_stream(stream, offset + i64::from(size))
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn complete_chunk_write(
    pool: &sqlx::SqlitePool,
    stream: Id,
    offset: i64,
    chunk: &Chunk,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    writing_stream(&mut tx, stream).await?;
    ensure!(
        queries::allocated_source(stream, chunk)
            .build_query_scalar::<i64>()
            .fetch_optional(&mut *tx)
            .await?
            .is_some(),
        crate::model::Error::OperationAborted
    );
    ensure!(
        queries::complete_chunk(chunk)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    ensure!(
        queries::complete_encoding(chunk.encoding_id)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    queries::insert_chunk_extent(stream, offset, chunk.raw_size, chunk.id)
        .execute(&mut *tx)
        .await?;
    queries::resize_writing_stream(stream, offset + i64::from(chunk.raw_size))
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn copy_chunk_references(
    pool: &sqlx::SqlitePool,
    destination: Id,
    source: Id,
    offset: i64,
    end: i64,
    chunks: &[Id],
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    writing_stream(&mut tx, destination).await?;
    let locked = queries::lock_ready_chunks(&mut tx, chunks).await?;
    ensure!(locked.len() == chunks.len(), "copy source is not ready");
    require_sources(&mut tx, None, chunks).await?;
    queries::copy_extents(destination, source, offset, end)
        .execute(&mut *tx)
        .await?;
    queries::reference_chunks(&mut tx, chunks, destination).await?;
    queries::resize_writing_stream(destination, end)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn register_encoding(pool: &sqlx::SqlitePool, chunk: &Chunk) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    ensure!(
        queries::register_encoding(chunk)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    queries::refresh_encoding_fingerprint(&mut tx, chunk.encoding_id).await?;
    tx.commit().await?;
    Ok(())
}

use crate::model::quota::{Limits, Quota, number, validate_scope};

pub(super) async fn read_quota(
    pool: &sqlx::SqlitePool,
    principal: &Principal,
    scope: &str,
    id: uuid::Uuid,
) -> Result<Quota> {
    validate_scope(scope)?;
    let mut tx = pool.begin().await?;
    let admin = queries::validate_identity(principal, &mut tx).await?;
    let visible = if scope == "bucket" {
        lock_permit(
            &mut tx,
            &crate::authorization::Permit::for_action(
                principal.clone(),
                id,
                crate::authorization::Action::Inspect,
            ),
        )
        .await?;
        true
    } else if admin {
        true
    } else {
        ensure!(principal.token_id().is_none(), s3s::s3_error!(AccessDenied));
        queries::quota_membership(principal.user_id(), id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| s3s::s3_error!(AccessDenied))?
            == "all"
    };
    let row = queries::quota_account(scope, id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?
        .view(visible);
    tx.commit().await?;
    Ok(row)
}

pub(super) async fn save_quota(
    pool: &sqlx::SqlitePool,
    principal: &Principal,
    scope: &str,
    id: uuid::Uuid,
    input: Limits,
) -> Result<Quota> {
    validate_scope(scope)?;
    let (bytes, inflight, buckets) = (
        number(&input.logical_bytes_limit)?,
        number(&input.inflight_bytes_limit)?,
        number(&input.bucket_count_limit)?,
    );
    ensure!(
        scope != "bucket" || inflight.is_none() && buckets.is_none(),
        s3s::s3_error!(InvalidArgument)
    );
    let mut tx = admin_transaction(pool, principal).await?;
    queries::quota_account(scope, id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    queries::update_quota(scope, id, bytes, inflight, buckets)
        .execute(&mut *tx)
        .await?;
    let mut detail = serde_json::to_value(input)?;
    detail[format!("{scope}_id")] = serde_json::json!(id);
    checkpoint_event(&mut tx, "quota.update", &id.to_string(), detail).await?;
    let row = queries::quota_account(scope, id)
        .fetch_one(&mut *tx)
        .await?
        .view(true);
    tx.commit().await?;
    Ok(row)
}

pub(super) async fn reserve_quota(
    pool: &sqlx::SqlitePool,
    stream: Id,
    required: i64,
    padding: bool,
    upload: Option<(uuid::Uuid, i32)>,
) -> Result<i64> {
    ensure!(required >= 0, "negative quota reservation");
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let upload_bucket = if let Some((upload, _)) = upload {
        Some(
            queries::quota_upload(upload, false)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(crate::model::Error::OperationAborted)?,
        )
    } else {
        None
    };
    let info = queries::quota_stream(stream).fetch_one(&mut *tx).await?;
    ensure!(
        info.state == "writing"
            && upload_bucket
                .as_ref()
                .is_none_or(|(bucket, key)| *bucket == info.bucket_id
                    && key.as_slice() == info.object_key.as_bytes()),
        crate::model::Error::OperationAborted
    );
    let mut accounts = lock_quota(&mut tx, info.bucket_id).await?;
    let accepted = if let Some((upload, part)) = upload {
        queries::part_quota(upload, part, stream)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(crate::model::Error::OperationAborted)?
    } else {
        ensure!(
            queries::object_claim(info.bucket_id, &info.object_key, stream)
                .fetch_one(&mut *tx)
                .await?,
            crate::model::Error::OperationAborted
        );
        0
    };
    let mut reservation = if let Some(row) =
        queries::owned_quota_reservation(stream, upload.map(|v| v.0))
            .fetch_optional(&mut *tx)
            .await?
    {
        row
    } else {
        let credit = queries::quota_credit(info.bucket_id, &info.object_key)
            .fetch_optional(&mut *tx)
            .await?;
        let result = queries::insert_quota_reservation(
            info.bucket_id,
            &info.object_key,
            upload.is_none().then_some(stream),
            upload.map(|v| v.0),
            credit,
        )
        .execute(&mut *tx)
        .await?;
        queries::quota_reservation(Id::from(result.last_insert_rowid()))
            .fetch_one(&mut *tx)
            .await?
    };
    ensure!(!reservation.closed, crate::model::Error::OperationAborted);
    let owner = reservation.id;
    queries::insert_quota_write(stream, owner)
        .execute(&mut *tx)
        .await?;
    let write = queries::quota_write(stream).fetch_one(&mut *tx).await?;
    ensure!(write.reservation_id == owner, "quota owner mismatch");
    let mut target = required;
    if padding && required > write.allocated_bytes {
        target = required
            .checked_add(4194303)
            .map_or(required, |n| n / 4194304 * 4194304);
        let mut available = i128::from(target - write.allocated_bytes);
        for account in &accounts {
            if let Some(limit) = account.logical_bytes_limit {
                let room = (i128::from(limit)
                    - i128::from(account.used_bytes)
                    - i128::from(account.reserved_bytes))
                .max(0)
                    + i128::from((reservation.credit_bytes - reservation.logical_bytes).max(0))
                    + i128::from((accepted - write.allocated_bytes).max(0));
                available = available.min(room);
            }
            if let Some(limit) = account.inflight_bytes_limit {
                available =
                    available.min((i128::from(limit) - i128::from(account.inflight_bytes)).max(0));
            }
        }
        target =
            required.max(target.min(quota_number(i128::from(write.allocated_bytes) + available)?));
    }
    let contribution = (target - accepted).max(0);
    let logical = quota_number(
        i128::from(reservation.logical_bytes) + i128::from(contribution)
            - i128::from(write.contribution_bytes),
    )?;
    let inflight = quota_number(
        i128::from(reservation.inflight_bytes) + i128::from(target)
            - i128::from(write.allocated_bytes),
    )?;
    let credit = reservation.credit_bytes;
    change_reservation(
        &mut tx,
        &mut accounts,
        &mut reservation,
        logical,
        inflight,
        credit,
    )
    .await?;
    queries::change_quota_write(stream, target, contribution)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(target)
}

pub(super) async fn complete_quota(
    pool: &sqlx::SqlitePool,
    upload: uuid::Uuid,
    stream: Id,
    size: i64,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let (bucket, key) = queries::quota_upload(upload, true)
        .fetch_one(&mut *tx)
        .await?;
    let output = queries::quota_stream(stream).fetch_one(&mut *tx).await?;
    ensure!(
        size >= 0
            && output.bucket_id == bucket
            && output.object_key.as_bytes() == key
            && output.state == "writing",
        crate::model::Error::OperationAborted
    );
    lock_quota(&mut tx, bucket).await?;
    let row = queries::owned_quota_reservation(Id::default(), Some(upload))
        .fetch_one(&mut *tx)
        .await?;
    ensure!(
        row.logical_bytes >= size && row.upload_id == Some(upload) && !row.closed,
        "multipart quota reservation is missing"
    );
    queries::complete_quota(upload, stream, size)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn forget_quota_write(
    connection: &mut SqliteConnection,
    accounts: &mut [QuotaAccount; 2],
    stream: Id,
) -> Result<()> {
    let Some(write) = queries::quota_write(stream)
        .fetch_optional(&mut *connection)
        .await?
    else {
        return Ok(());
    };
    let mut row = queries::quota_reservation(write.reservation_id)
        .fetch_one(&mut *connection)
        .await?;
    if row.stream_id.is_some() {
        change_reservation(connection, accounts, &mut row, 0, 0, 0).await?;
        queries::delete_quota_reservation(row.id)
            .execute(connection)
            .await?;
    } else {
        let (logical, inflight, credit) = (
            row.logical_bytes - write.contribution_bytes,
            row.inflight_bytes - write.allocated_bytes,
            row.credit_bytes,
        );
        change_reservation(connection, accounts, &mut row, logical, inflight, credit).await?;
        queries::delete_quota_write(stream)
            .execute(&mut *connection)
            .await?;
        if row.closed
            && !queries::remaining_quota_writes(row.id)
                .fetch_one(&mut *connection)
                .await?
        {
            change_reservation(connection, accounts, &mut row, 0, 0, 0).await?;
            queries::delete_quota_reservation(row.id)
                .execute(connection)
                .await?;
        }
    }
    Ok(())
}

pub(super) async fn close_upload_quota(
    connection: &mut SqliteConnection,
    accounts: &mut [QuotaAccount; 2],
    upload: uuid::Uuid,
) -> Result<()> {
    let Some(mut row) = queries::owned_quota_reservation(Id::default(), Some(upload))
        .fetch_optional(&mut *connection)
        .await?
    else {
        return Ok(());
    };
    if row.closed {
        return Ok(());
    }
    let pending = queries::pending_quota_writes(row.id)
        .fetch_one(&mut *connection)
        .await?;
    change_reservation(connection, accounts, &mut row, 0, pending, 0).await?;
    queries::clear_write_contributions(row.id)
        .execute(&mut *connection)
        .await?;
    queries::close_quota_reservation(row.id)
        .execute(&mut *connection)
        .await?;
    if pending == 0 {
        queries::delete_quota_reservation(row.id)
            .execute(connection)
            .await?;
    }
    Ok(())
}

pub(super) async fn replace_object_quota(
    connection: &mut SqliteConnection,
    accounts: &mut [QuotaAccount; 2],
    bucket: uuid::Uuid,
    key: &str,
    old: Option<(Id, i64)>,
    new: Option<(Id, i64)>,
) -> Result<()> {
    if old.map(|v| v.0) == new.map(|v| v.0) {
        return Ok(());
    }
    let (mut freed, mut inflight) = (0, 0);
    if let Some((stream, _)) = new
        && let Some(row) = queries::publication_reservation(stream)
            .fetch_optional(&mut *connection)
            .await?
    {
        ensure!(
            row.bucket_id == bucket && !row.closed,
            "invalid publication reservation"
        );
        freed = (row.logical_bytes - row.credit_bytes).max(0);
        inflight = row.inflight_bytes;
        let pending = if row.upload_id.is_some() {
            queries::pending_quota_writes(row.id)
                .fetch_one(&mut *connection)
                .await?
        } else {
            0
        };
        if pending == 0 {
            queries::delete_quota_reservation(row.id)
                .execute(&mut *connection)
                .await?;
        } else {
            queries::clear_write_contributions(row.id)
                .execute(&mut *connection)
                .await?;
            queries::change_quota_reservation(row.id, 0, pending, 0)
                .execute(&mut *connection)
                .await?;
            queries::close_quota_reservation(row.id)
                .execute(&mut *connection)
                .await?;
            inflight -= pending;
        }
    }
    let credits = queries::displaced_credits(bucket, key, old.map(|v| v.0))
        .fetch_one(&mut *connection)
        .await?;
    queries::remove_displaced_credits(bucket, key, old.map(|v| v.0))
        .execute(&mut *connection)
        .await?;
    adjust_quota(
        connection,
        accounts,
        new.map_or(0, |v| v.1) - old.map_or(0, |v| v.1),
        credits - freed,
        -inflight,
        i64::from(new.is_some()) - i64::from(old.is_some()),
    )
    .await
}

pub(super) async fn publish(
    pool: &sqlx::SqlitePool,
    id: Id,
    if_match: Option<s3s::dto::ETagCondition>,
    if_none: Option<s3s::dto::ETagCondition>,
    completion: Option<(uuid::Uuid, String, serde_json::Value)>,
) -> Result<bool> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let authority = queries::write_authorization(id)
        .fetch_one(&mut *tx)
        .await?
        .0;
    lock_permit(&mut tx, &authority).await?;
    let upload_target = if let Some((upload, _, _)) = &completion {
        let target = queries::quota_upload(*upload, true)
            .fetch_optional(&mut *tx)
            .await?;
        if target.is_none() {
            return Ok(false);
        }
        target
    } else {
        None
    };
    let stream = queries::publish_stream_record(id)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(
        upload_target
            .as_ref()
            .is_none_or(|(bucket, key)| *bucket == stream.bucket_id
                && key.as_slice() == stream.object_key.as_bytes()),
        crate::model::Error::OperationAborted
    );
    let (state, _) = queries::lock_bucket_uploads(stream.bucket_id)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(state == "active", crate::model::Error::OperationAborted);
    let (old, token) = queries::lock_object_version(stream.bucket_id, &stream.object_key)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(
        token == id && stream.state == "writing",
        crate::model::Error::OperationAborted
    );
    if let Some((upload, _, _)) = &completion
        && let Some((version, size)) = queries::web_upload_expectation(*upload)
            .fetch_optional(&mut *tx)
            .await?
    {
        ensure!(
            old == version,
            s3s::s3_error!(PreconditionFailed, "object changed since upload started")
        );
        ensure!(stream.size == size, s3s::s3_error!(IncompleteBody));
    }
    let previous = if let Some(old) = old {
        Some(queries::stream_size_etag(old).fetch_one(&mut *tx).await?)
    } else {
        None
    };
    crate::model::check_write_conditions(
        previous.as_ref().map(|v| v.1.as_str()),
        if_match.as_ref(),
        if_none.as_ref(),
    )?;
    if stream.payload_type == "raw" {
        validate_raw(&mut tx, id, stream.size).await?;
    } else {
        ensure!(
            !queries::incomplete_extents(id).fetch_one(&mut *tx).await?,
            "cannot publish an incomplete generation"
        );
        let (end, contiguous) = queries::extent_continuity(id).fetch_one(&mut *tx).await?;
        ensure!(
            end == stream.size && contiguous,
            "cannot publish a mapping with gaps or overlaps"
        );
        require_sources(&mut tx, Some(id), &[]).await?;
    }
    let mut accounts = lock_quota(&mut tx, stream.bucket_id).await?;
    replace_object_quota(
        &mut tx,
        &mut accounts,
        stream.bucket_id,
        &stream.object_key,
        old.zip(previous.as_ref().map(|v| v.0)),
        Some((id, stream.size)),
    )
    .await?;
    queries::publish_object(&stream).execute(&mut *tx).await?;
    queries::publish_stream(id).execute(&mut *tx).await?;
    queries::ready_stream_queues(&mut tx, id).await?;
    if let Some(old) = old {
        forget_quota_write(&mut tx, &mut accounts, old).await?;
        queries::retire_stream(old).execute(&mut *tx).await?;
    }
    if let Some((upload, hash, result)) = completion {
        if queries::complete_upload(upload, id, &hash, &result)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            != 1
        {
            return Ok(false);
        }
        close_upload_quota(&mut tx, &mut accounts, upload).await?;
        for part in queries::upload_part_streams(upload)
            .fetch_all(&mut *tx)
            .await?
        {
            forget_quota_write(&mut tx, &mut accounts, part).await?;
            queries::retire_stream(part).execute(&mut *tx).await?;
        }
        queries::delete_upload_parts(upload)
            .execute(&mut *tx)
            .await?;
        checkpoint_event(&mut tx,"upload.complete",&upload.to_string(),serde_json::json!({"bucket_id":stream.bucket_id,"key":stream.object_key,"size":stream.size})).await?;
    }
    tx.commit().await?;
    Ok(true)
}

use crate::model::multipart::{
    MultipartCreate, PartStitch, SeedConsumption, Transfer, TransferFilter,
};

pub(super) async fn create_multipart(
    pool: &sqlx::SqlitePool,
    input: MultipartCreate<'_>,
) -> Result<(uuid::Uuid, bool)> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let mut authority = input.authority.clone();
    authority.add(input.bucket, crate::authorization::Action::Write);
    if input.public {
        authority.add(input.bucket, crate::authorization::Action::Acl);
    }
    lock_permit(&mut tx, &authority).await?;
    let (state, paused) = queries::lock_bucket_uploads(input.bucket)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(state == "active", crate::model::Error::OperationAborted);
    let expected = if let Some(web) = &input.web {
        if let Some((id, hash)) = queries::web_upload_identity(web.user, web.client_id)
            .fetch_optional(&mut *tx)
            .await?
        {
            ensure!(
                hash == web.request_hash,
                s3s::s3_error!(InvalidRequest, "upload identity was already used")
            );
            return Ok((id, true));
        }
        let expected = queries::web_upload_expected(input.bucket, input.key)
            .fetch_optional(&mut *tx)
            .await?
            .flatten();
        ensure!(
            expected.is_none() || web.overwrite,
            crate::model::Error::ObjectAlreadyExists
        );
        expected
    } else {
        None
    };
    ensure!(!paused, crate::model::Error::OperationAborted);
    if let Some(limit) = input.max_active {
        ensure!(
            queries::active_upload_count().fetch_one(&mut *tx).await? < limit,
            s3s::s3_error!(SlowDown)
        );
    }
    let id = uuid::Uuid::new_v4();
    queries::insert_multipart(id, &input)
        .execute(&mut *tx)
        .await?;
    if let Some(web) = input.web {
        queries::insert_web_upload(id, &web, expected)
            .execute(&mut *tx)
            .await?;
        checkpoint_event(
            &mut tx,
            "upload.create",
            &id.to_string(),
            serde_json::json!({"bucket_id":input.bucket,"key":input.key,"size":web.size}),
        )
        .await?;
    }
    tx.commit().await?;
    Ok((id, false))
}

pub(super) async fn begin_upload_part(
    pool: &sqlx::SqlitePool,
    authority: &crate::authorization::Permit,
    upload: uuid::Uuid,
    number: i32,
    stream: Id,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    lock_permit(&mut tx, authority).await?;
    let parent = queries::lock_upload(upload)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchUpload))?;
    ensure!(parent.state == "active", s3s::s3_error!(NoSuchUpload));
    let info = queries::quota_stream(stream).fetch_one(&mut *tx).await?;
    ensure!(
        info.bucket_id == parent.bucket_id
            && info.object_key == parent.object_key
            && info.stream_type == "part"
            && info.state == "writing",
        crate::model::Error::OperationAborted
    );
    if let Some((_, token, _)) = queries::lock_upload_part(upload, number)
        .fetch_optional(&mut *tx)
        .await?
        && token != stream
    {
        let mut accounts = lock_quota(&mut tx, parent.bucket_id).await?;
        if let Some(write) = queries::quota_write(token).fetch_optional(&mut *tx).await? {
            let mut row = queries::owned_quota_reservation(Id::default(), Some(upload))
                .fetch_one(&mut *tx)
                .await?;
            ensure!(
                write.reservation_id == row.id && !row.closed,
                "invalid part reservation"
            );
            let (logical, inflight, credit) = (
                row.logical_bytes - write.contribution_bytes,
                row.inflight_bytes,
                row.credit_bytes,
            );
            change_reservation(&mut tx, &mut accounts, &mut row, logical, inflight, credit).await?;
            queries::change_quota_write(token, write.allocated_bytes, 0)
                .execute(&mut *tx)
                .await?;
        }
    }
    queries::begin_upload_part(upload, number, stream)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn commit_upload_part(
    pool: &sqlx::SqlitePool,
    authority: &crate::authorization::Permit,
    bucket: uuid::Uuid,
    upload: uuid::Uuid,
    number: i32,
    stream: Id,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    lock_permit(&mut tx, authority).await?;
    let (state, _) = queries::lock_bucket_uploads(bucket)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(state == "active", crate::model::Error::OperationAborted);
    let parent = queries::lock_upload(upload)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchUpload))?;
    ensure!(parent.state == "active", s3s::s3_error!(NoSuchUpload));
    let info = queries::publish_stream_record(stream)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(
        parent.bucket_id == bucket
            && info.bucket_id == bucket
            && info.object_key == parent.object_key
            && info.stream_type == "part",
        crate::model::Error::OperationAborted
    );
    let (old, token, accepted) = queries::lock_upload_part(upload, number)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(token == stream, crate::model::Error::OperationAborted);
    if old == Some(stream) && info.state == "ready" {
        tx.commit().await?;
        return Ok(());
    }
    ensure!(
        info.state == "writing",
        crate::model::Error::OperationAborted
    );
    if info.payload_type == "raw" {
        validate_raw(&mut tx, stream, info.size).await?;
    } else {
        let (end, contiguous) = queries::extent_continuity(stream)
            .fetch_one(&mut *tx)
            .await?;
        ensure!(
            end == info.size && contiguous,
            "part mapping has gaps or overlaps"
        );
        require_sources(&mut tx, Some(stream), &[]).await?;
    }
    let mut accounts = lock_quota(&mut tx, bucket).await?;
    let mut row = queries::owned_quota_reservation(Id::default(), Some(upload))
        .fetch_one(&mut *tx)
        .await?;
    let write = queries::quota_write(stream).fetch_one(&mut *tx).await?;
    ensure!(
        write.reservation_id == row.id && write.allocated_bytes >= info.size && !row.closed,
        "part is not reserved"
    );
    let logical = quota_number(
        i128::from(row.logical_bytes) + i128::from(info.size)
            - i128::from(accepted)
            - i128::from(write.contribution_bytes),
    )?;
    let inflight = quota_number(
        i128::from(row.inflight_bytes) + i128::from(info.size)
            - i128::from(accepted)
            - i128::from(write.allocated_bytes),
    )?;
    let credit = row.credit_bytes;
    change_reservation(&mut tx, &mut accounts, &mut row, logical, inflight, credit).await?;
    queries::delete_quota_write(stream)
        .execute(&mut *tx)
        .await?;
    queries::publish_upload_part(upload, number, stream, info.size)
        .execute(&mut *tx)
        .await?;
    queries::publish_stream(stream).execute(&mut *tx).await?;
    queries::ready_stream_queues(&mut tx, stream).await?;
    if let Some(old) = old {
        forget_quota_write(&mut tx, &mut accounts, old).await?;
        queries::retire_stream(old).execute(&mut *tx).await?;
    }
    queries::touch_upload(upload).execute(&mut *tx).await?;
    checkpoint_event(
        &mut tx,
        "upload.part",
        &upload.to_string(),
        serde_json::json!({"bucket_id":bucket,"part":number}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn retire_upload_work(pool: &sqlx::SqlitePool, stream: Id) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let info = queries::publish_stream_record(stream)
        .fetch_one(&mut *tx)
        .await?;
    let mut accounts = lock_quota(&mut tx, info.bucket_id).await?;
    forget_quota_write(&mut tx, &mut accounts, stream).await?;
    queries::retire_stream(stream).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn begin_upload_completion(
    pool: &sqlx::SqlitePool,
    authority: &crate::authorization::Permit,
    upload: uuid::Uuid,
    hash: &str,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    lock_permit(&mut tx, authority).await?;
    ensure!(
        queries::begin_upload_completion(upload, hash)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    tx.commit().await?;
    Ok(())
}

pub(super) async fn abort_multipart(
    pool: &sqlx::SqlitePool,
    authority: &crate::authorization::Permit,
    upload: uuid::Uuid,
    expired: bool,
) -> Result<bool> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    lock_permit(&mut tx, authority).await?;
    let Some(parent) = queries::lock_upload(upload)
        .fetch_optional(&mut *tx)
        .await?
    else {
        if expired {
            return Ok(false);
        }
        return Err(s3s::s3_error!(NoSuchUpload).into());
    };
    if expired && parent.state != "active" {
        return Ok(false);
    }
    ensure!(
        matches!(parent.state.as_str(), "active" | "completing"),
        s3s::s3_error!(NoSuchUpload)
    );
    if queries::abort_multipart(upload, expired)
        .execute(&mut *tx)
        .await?
        .rows_affected()
        == 0
    {
        if expired {
            return Ok(false);
        }
        return Err(s3s::s3_error!(NoSuchUpload).into());
    }
    let mut accounts = lock_quota(&mut tx, parent.bucket_id).await?;
    close_upload_quota(&mut tx, &mut accounts, upload).await?;
    for stream in queries::upload_part_streams(upload)
        .fetch_all(&mut *tx)
        .await?
    {
        forget_quota_write(&mut tx, &mut accounts, stream).await?;
        queries::retire_stream(stream).execute(&mut *tx).await?;
    }
    queries::delete_upload_parts(upload)
        .execute(&mut *tx)
        .await?;
    if authority.principal.user_id().is_some() && crate::model::audit::active() {
        checkpoint_event(
            &mut tx,
            "upload.abort",
            &upload.to_string(),
            serde_json::json!({"bucket_id":parent.bucket_id}),
        )
        .await?;
    }
    tx.commit().await?;
    Ok(true)
}

pub(super) async fn upload_transfers(
    backend: &super::Backend,
    principal: &Principal,
    user: uuid::Uuid,
    filter: &TransferFilter,
    after: Option<(chrono::DateTime<chrono::Utc>, uuid::Uuid)>,
    id: Option<uuid::Uuid>,
    limit: i64,
) -> Result<(Vec<Transfer>, bool)> {
    use sqlx::Row;
    ensure!((0..i64::MAX).contains(&limit), "invalid transfer page size");
    let mut tx = backend.read.begin().await?;
    let admin = queries::validate_identity(principal, &mut tx).await?;
    let mut query =
        queries::upload_transfers(admin, user, principal.token_id(), filter, after, id, limit);
    let mut rows = query.build().fetch_all(&mut *tx).await?;
    let more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    let mut ready = Vec::new();
    let mut transfers = Vec::with_capacity(rows.len());
    for row in rows {
        let transfer: Transfer = sqlx::FromRow::from_row(&row)?;
        if transfer.remote_state == "stored"
            && row.try_get::<Option<i64>, _>("durable_at")?.is_none()
        {
            ready.push(transfer.id);
        }
        transfers.push(transfer);
    }
    tx.commit().await?;
    if !ready.is_empty() {
        let mut tx = backend.write.begin_with(queries::BEGIN_WRITE).await?;
        queries::durable_uploads(&mut tx, &ready).await?;
        tx.commit().await?;
    }
    Ok((transfers, more))
}

async fn replace_upload_range(
    connection: &mut SqliteConnection,
    stream: Id,
    offset: i64,
    length: usize,
    chunk: Id,
    source: usize,
) -> Result<()> {
    if length == 0 {
        return Ok(());
    }
    ensure!(
        queries::lock_chunk(chunk)
            .fetch_one(&mut *connection)
            .await?
            == "ready",
        crate::model::Error::OperationAborted
    );
    let end = offset + i64::try_from(length)?;
    let rows = queries::lock_upload_extents(stream, offset, end)
        .fetch_all(&mut *connection)
        .await?;
    let mut covered = offset;
    let mut previous = Vec::new();
    for row in rows {
        if let Some(chunk) = row.chunk_id {
            previous.push(chunk);
        }
        ensure!(row.offset_bytes <= covered, "extent replacement gap");
        let row_end = row.offset_bytes + i64::from(row.length);
        covered = covered.max(row_end.min(end));
        queries::remove_upload_extent(stream, row.offset_bytes)
            .execute(&mut *connection)
            .await?;
        if row.offset_bytes < offset {
            queries::insert_upload_extent(
                &row,
                row.offset_bytes,
                i32::try_from(offset - row.offset_bytes)?,
                row.source_offset,
            )
            .execute(&mut *connection)
            .await?;
        }
        if row_end > end {
            queries::insert_upload_extent(
                &row,
                end,
                i32::try_from(row_end - end)?,
                row.source_offset + i32::try_from(end - row.offset_bytes)?,
            )
            .execute(&mut *connection)
            .await?;
        }
    }
    ensure!(covered == end, "extent replacement incomplete");
    queries::insert_upload_chunk_extent(
        stream,
        offset,
        i32::try_from(length)?,
        chunk,
        i32::try_from(source)?,
    )
    .execute(&mut *connection)
    .await?;
    queries::changed_references(connection, &[chunk], true).await?;
    queries::mark_unreferenced_chunks(connection, &previous).await?;
    Ok(())
}

pub(super) async fn stitch_upload_parts(pool: &sqlx::SqlitePool, input: PartStitch) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    lock_splice_sources(&mut tx, &[input.left, input.right], input.chunk).await?;
    replace_upload_range(
        &mut tx,
        input.left,
        input.offset,
        input.left_length,
        input.chunk,
        0,
    )
    .await?;
    replace_upload_range(
        &mut tx,
        input.right,
        0,
        input.right_length,
        input.chunk,
        input.left_length,
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn consume_upload_seed(
    pool: &sqlx::SqlitePool,
    input: SeedConsumption<'_>,
) -> Result<()> {
    ensure!(input.prefix <= input.consumed, "invalid consumed prefix");
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let active = queries::lock_upload_active(input.upload)
        .fetch_one(&mut *tx)
        .await?;
    let mut streams: Vec<_> = input.spans.iter().map(|span| span.0).collect();
    streams.push(input.stream);
    streams.sort_unstable();
    streams.dedup();
    lock_splice_sources(&mut tx, &streams, input.chunk).await?;
    let mut remaining = input.prefix;
    let mut chunk_offset = 0;
    while remaining > 0 {
        let span = input.spans.first_mut().context("missing prefix span")?;
        let n = remaining.min(span.2);
        if active {
            replace_upload_range(&mut tx, span.0, span.1, n, input.chunk, chunk_offset).await?;
        }
        span.1 += n as i64;
        span.2 -= n;
        remaining -= n;
        chunk_offset += n;
        if span.2 == 0 {
            input.spans.remove(0);
        }
    }
    if input.prefix == input.consumed {
        queries::remove_upload_extent(input.stream, input.offset)
            .execute(&mut *tx)
            .await?;
        queries::mark_unreferenced_chunks(&mut tx, &[input.chunk]).await?;
    } else {
        queries::slice_upload_extent(
            input.stream,
            input.offset,
            i32::try_from(input.prefix)?,
            i32::try_from(input.consumed - input.prefix)?,
        )
        .execute(&mut *tx)
        .await?;
        queries::changed_references(&mut tx, &[input.chunk], true).await?;
    }
    queries::resize_upload_stream(
        input.stream,
        input.offset + i64::try_from(input.consumed - input.prefix)?,
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

use crate::app::Bucket;
use crate::authorization::{Action, DEFAULT_PROJECT};
use crate::model::bucket::{
    BucketImpact, BucketUpdate, CreateBucket, Domain, TransferInput, TransferProject, Website,
    fingerprint,
};
use crate::model::task::Execution;

pub(super) async fn create_bucket(
    pool: &SqlitePool,
    principal: &Principal,
    input: CreateBucket,
) -> Result<Bucket> {
    ensure!(
        s3s::path::check_bucket_name(&input.name),
        s3s::s3_error!(InvalidBucketName)
    );
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let admin = queries::validate_identity(principal, &mut tx).await?;
    let project = input.project_id.unwrap_or(DEFAULT_PROJECT);
    let allowed = queries::bucket_creation_policy(project)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    let mut selected = false;
    if !admin {
        ensure!(
            allowed && matches!(principal, Principal::User { .. }),
            s3s::s3_error!(AccessDenied)
        );
        let (role, scope) = queries::bucket_creator_membership(project, principal.user_id())
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| s3s::s3_error!(AccessDenied))?;
        ensure!(role == "maintainer", s3s::s3_error!(AccessDenied));
        selected = scope == "selected";
    }
    let available = queries::bucket_available_storages()
        .fetch_all(&mut *tx)
        .await?;
    let storage = match input.storage_id {
        Some(id) if available.contains(&id) => id,
        Some(_) => anyhow::bail!("storage is unavailable"),
        None if available.len() == 1 => available[0],
        None if available.is_empty() => anyhow::bail!("no storage configured"),
        None => anyhow::bail!("storage must be selected"),
    };
    ensure!(
        !queries::duplicate_bucket(&input.name)
            .fetch_one(&mut *tx)
            .await?,
        crate::model::Error::BucketAlreadyExists
    );
    let quota = queries::quota_account("project", project)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(
        quota
            .bucket_count_limit
            .is_none_or(|limit| quota.bucket_count < limit),
        crate::model::Error::QuotaExceeded
    );
    let id = Uuid::new_v4();
    queries::insert_bucket(id, &input.name, project, storage)
        .execute(&mut *tx)
        .await?;
    queries::change_project_bucket_quota(project, 1, 0, 0)
        .execute(&mut *tx)
        .await?;
    queries::insert_quota_scope("bucket", id)
        .execute(&mut *tx)
        .await?;
    if selected {
        let user = principal.user_id().context("bucket creator has no user")?;
        queries::insert_member_grant(
            user,
            project,
            &crate::model::administration::BucketGrant {
                bucket_id: id,
                actions: Action::ALL.to_vec(),
            },
        )
        .execute(&mut *tx)
        .await?;
        queries::advance_user_authorization(user)
            .execute(&mut *tx)
            .await?;
    }
    checkpoint_event(
        &mut tx,
        "bucket.create",
        &input.name,
        serde_json::json!({"bucket_id":id,"project_id":project}),
    )
    .await?;
    let row = queries::bucket_by_id(id).fetch_one(&mut *tx).await?;
    tx.commit().await?;
    Ok(row)
}

pub(super) async fn bucket_domains(
    pool: &SqlitePool,
    principal: &Principal,
    bucket: Option<Uuid>,
) -> Result<Vec<Domain>> {
    let mut tx = pool.begin().await?;
    ensure!(
        queries::validate_identity(principal, &mut tx).await?,
        denied()
    );
    let rows = queries::bucket_domains(bucket).fetch_all(&mut *tx).await?;
    tx.commit().await?;
    Ok(rows)
}
pub(super) async fn preview_bucket(
    pool: &SqlitePool,
    principal: &Principal,
    bucket: Uuid,
) -> Result<BucketImpact> {
    let mut tx = pool.begin().await?;
    ensure!(
        queries::validate_identity(principal, &mut tx).await?,
        denied()
    );
    let row = queries::bucket_impact(bucket)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    tx.commit().await?;
    Ok(row)
}
pub(super) async fn preview_bucket_transfer(
    pool: &SqlitePool,
    principal: &Principal,
    bucket: Uuid,
    target: Uuid,
) -> Result<(BucketImpact, TransferProject)> {
    let mut tx = pool.begin().await?;
    ensure!(
        queries::validate_identity(principal, &mut tx).await?,
        denied()
    );
    let row = queries::bucket_impact(bucket)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    let target = queries::bucket_transfer_target(target)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    ensure!(row.project_id != target.id, s3s::s3_error!(InvalidArgument));
    tx.commit().await?;
    Ok((row, target))
}

async fn bucket_members(connection: &mut SqliteConnection, bucket: Uuid) -> Result<Vec<Uuid>> {
    Ok(queries::bucket_member_ids(bucket)
        .fetch_all(connection)
        .await?)
}

async fn revoke_bucket_access(
    connection: &mut SqliteConnection,
    bucket: Uuid,
    members: &[Uuid],
) -> Result<()> {
    for user in members {
        queries::advance_user_authorization(*user)
            .execute(&mut *connection)
            .await?;
    }
    queries::advance_bucket_credentials(bucket)
        .execute(&mut *connection)
        .await?;
    queries::advance_bucket_tokens(bucket)
        .execute(&mut *connection)
        .await?;
    queries::revoke_bucket_members(bucket)
        .execute(&mut *connection)
        .await?;
    queries::revoke_bucket_credentials(bucket)
        .execute(&mut *connection)
        .await?;
    queries::revoke_bucket_tokens(bucket)
        .execute(&mut *connection)
        .await?;
    Ok(())
}

async fn remove_empty_bucket(
    connection: &mut SqliteConnection,
    row: &Bucket,
    members: &[Uuid],
) -> Result<()> {
    let accounts = lock_quota(connection, row.id).await?;
    ensure!(
        accounts[1].used_bytes == 0
            && accounts[1].reserved_bytes == 0
            && accounts[1].inflight_bytes == 0
            && accounts[1].object_count == 0,
        crate::model::Error::BucketNotEmpty
    );
    revoke_bucket_access(connection, row.id, members).await?;
    queries::delete_bucket_objects(row.id)
        .execute(&mut *connection)
        .await?;
    queries::change_project_bucket_quota(row.project_id, -1, 0, 0)
        .execute(&mut *connection)
        .await?;
    queries::delete_quota_scope("bucket", row.id)
        .execute(&mut *connection)
        .await?;
    queries::delete_bucket(row.id).execute(connection).await?;
    Ok(())
}

pub(super) async fn delete_bucket(
    pool: &SqlitePool,
    principal: &Principal,
    bucket: Uuid,
    name: &str,
    confirmation: Option<&str>,
) -> Result<()> {
    let mut tx = administration_transaction(pool, principal).await?;
    let members = bucket_members(&mut tx, bucket).await?;
    let row = queries::bucket_by_id(bucket)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    ensure!(row.name == name, s3s::s3_error!(PreconditionFailed));
    if let Some(expected) = confirmation {
        ensure!(
            fingerprint(
                &queries::bucket_impact(bucket).fetch_one(&mut *tx).await?,
                None
            )? == expected,
            s3s::s3_error!(PreconditionFailed)
        );
    }
    ensure!(
        row.state == "active"
            && !queries::bucket_has_streams(bucket)
                .fetch_one(&mut *tx)
                .await?,
        crate::model::Error::BucketNotEmpty
    );
    remove_empty_bucket(&mut tx, &row, &members).await?;
    checkpoint_event(
        &mut tx,
        "bucket.delete",
        name,
        serde_json::json!({"bucket_id":bucket,"project_id":row.project_id}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn domain_change(
    pool: &SqlitePool,
    principal: &Principal,
    host: &str,
    bucket: Option<Uuid>,
) -> Result<()> {
    let mut tx = administration_transaction(pool, principal).await?;
    let old = queries::bucket_domain_owner(host)
        .fetch_optional(&mut *tx)
        .await?;
    ensure!(
        bucket.is_none() || old.is_none() || old == bucket,
        crate::model::Error::DomainInUse
    );
    if let Some(id) = bucket.or(old) {
        let row = queries::bucket_by_id(id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(crate::model::Error::OperationAborted)?;
        ensure!(row.state == "active", crate::model::Error::OperationAborted);
        if old != bucket {
            if bucket.is_some() {
                queries::insert_bucket_domain(host, id)
                    .execute(&mut *tx)
                    .await?;
            } else {
                queries::delete_bucket_domain(host)
                    .execute(&mut *tx)
                    .await?;
            }
            queries::advance_bucket_settings(id)
                .execute(&mut *tx)
                .await?;
        }
    }
    checkpoint_event(
        &mut tx,
        "bucket.domain",
        host,
        serde_json::json!({"bucket_id":bucket.or(old),"removed":bucket.is_none()}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

async fn lock_bucket_settings(
    connection: &mut SqliteConnection,
    principal: &Principal,
    bucket: Uuid,
) -> Result<(Bucket, bool)> {
    let admin = queries::validate_identity(principal, connection).await?;
    let row = queries::bucket_by_id(bucket)
        .fetch_optional(&mut *connection)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    ensure!(
        queries::bucket_actions(principal, connection, bucket, admin)
            .await?
            .contains(&Action::Settings),
        s3s::s3_error!(AccessDenied)
    );
    ensure!(row.state == "active", crate::model::Error::OperationAborted);
    Ok((row, admin))
}

pub(super) async fn save_bucket_settings(
    pool: &SqlitePool,
    principal: &Principal,
    bucket: Uuid,
    input: BucketUpdate,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let (row, admin) = lock_bucket_settings(&mut tx, principal, bucket).await?;
    ensure!(
        row.settings_revision == input.revision,
        s3s::s3_error!(PreconditionFailed)
    );
    ensure!(
        admin
            || (input.domains.is_none()
                && input.uploads_paused == row.uploads_paused
                && input.public_base_url == row.public_base_url),
        s3s::s3_error!(AccessDenied)
    );
    if let Some(domains) = &input.domains {
        for host in domains {
            ensure!(
                queries::bucket_domain_owner(host)
                    .fetch_optional(&mut *tx)
                    .await?
                    .is_none_or(|id| id == bucket),
                crate::model::Error::DomainInUse
            );
        }
        let old = queries::bucket_domain_names(bucket)
            .fetch_all(&mut *tx)
            .await?;
        for host in &old {
            if !domains.contains(host) {
                queries::delete_bucket_domain(host)
                    .execute(&mut *tx)
                    .await?;
            }
        }
        let unique: std::collections::BTreeSet<_> = domains.iter().collect();
        for host in unique {
            if !old.contains(host) {
                queries::insert_bucket_domain(host, bucket)
                    .execute(&mut *tx)
                    .await?;
            }
        }
    }
    queries::save_bucket_settings(bucket, &input)
        .execute(&mut *tx)
        .await?;
    checkpoint_event(&mut tx,"bucket.settings",&row.name,serde_json::json!({"bucket_id":bucket,"project_id":row.project_id,"uploads_paused":input.uploads_paused})).await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn bucket_settings(
    pool: &SqlitePool,
    bucket: Uuid,
) -> Result<(Bucket, Vec<String>)> {
    let mut tx = pool.begin().await?;
    let row = queries::bucket_by_id(bucket)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    let domains = queries::bucket_domain_names(bucket)
        .fetch_all(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok((row, domains))
}

pub(super) async fn set_bucket_cors(
    pool: &SqlitePool,
    principal: &Principal,
    bucket: Uuid,
    rules: Value,
) -> Result<Value> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    lock_bucket_settings(&mut tx, principal, bucket).await?;
    queries::set_bucket_cors(bucket, &rules)
        .execute(&mut *tx)
        .await?;
    checkpoint_event(
        &mut tx,
        "bucket.cors",
        &bucket.to_string(),
        serde_json::json!({"bucket_id":bucket,"cors":rules}),
    )
    .await?;
    tx.commit().await?;
    Ok(rules)
}

pub(super) async fn set_bucket_website(
    pool: &SqlitePool,
    principal: &Principal,
    bucket: Uuid,
    input: Website,
) -> Result<Website> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    lock_bucket_settings(&mut tx, principal, bucket).await?;
    queries::set_bucket_website(bucket, &input)
        .execute(&mut *tx)
        .await?;
    checkpoint_event(
        &mut tx,
        "bucket.website",
        &bucket.to_string(),
        serde_json::json!({"bucket_id":bucket,"website":input}),
    )
    .await?;
    tx.commit().await?;
    Ok(input)
}

pub(super) async fn transfer_bucket(
    pool: &SqlitePool,
    principal: &Principal,
    bucket: Uuid,
    input: TransferInput,
) -> Result<Bucket> {
    let mut tx = administration_transaction(pool, principal).await?;
    let source = queries::quota_project(bucket)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    ensure!(
        source != input.target_project,
        s3s::s3_error!(InvalidArgument)
    );
    let users = queries::bucket_transfer_users(source, input.target_project)
        .fetch_all(&mut *tx)
        .await?;
    let members = bucket_members(&mut tx, bucket).await?;
    queries::bucket_by_id(bucket).fetch_one(&mut *tx).await?;
    let current = queries::bucket_impact(bucket).fetch_one(&mut *tx).await?;
    ensure!(
        current.name == input.confirm_name
            && fingerprint(&current, Some(input.target_project))? == input.confirmation,
        s3s::s3_error!(PreconditionFailed)
    );
    ensure!(
        current.state == "active"
            && current.uploads_paused
            && [
                &current.reserved_bytes,
                &current.inflight_bytes,
                &current.active_uploads,
                &current.writing_streams
            ]
            .iter()
            .all(|v| v.as_str() == "0"),
        crate::model::Error::BucketNotDrained
    );
    let mut projects = [source, input.target_project];
    projects.sort();
    for project in projects {
        queries::quota_account("project", project)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    }
    let total = queries::quota_account("bucket", bucket)
        .fetch_one(&mut *tx)
        .await?;
    let target = queries::quota_account("project", input.target_project)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(
        total.reserved_bytes == 0 && total.inflight_bytes == 0,
        crate::model::Error::BucketNotDrained
    );
    ensure!(
        target
            .bucket_count_limit
            .is_none_or(|limit| target.bucket_count < limit)
            && (total.used_bytes == 0
                || target
                    .logical_bytes_limit
                    .is_none_or(|limit| i128::from(target.used_bytes)
                        + i128::from(target.reserved_bytes)
                        + i128::from(total.used_bytes)
                        <= i128::from(limit))),
        crate::model::Error::QuotaExceeded
    );
    revoke_bucket_access(&mut tx, bucket, &members).await?;
    for user in users {
        if !members.contains(&user) {
            queries::advance_user_authorization(user)
                .execute(&mut *tx)
                .await?;
        }
    }
    queries::change_project_bucket_quota(source, -1, -total.used_bytes, -total.object_count)
        .execute(&mut *tx)
        .await?;
    queries::change_project_bucket_quota(
        input.target_project,
        1,
        total.used_bytes,
        total.object_count,
    )
    .execute(&mut *tx)
    .await?;
    queries::transfer_bucket(bucket, input.target_project)
        .execute(&mut *tx)
        .await?;
    let row = queries::bucket_by_id(bucket).fetch_one(&mut *tx).await?;
    checkpoint_event(&mut tx,"bucket.transfer",&row.name,serde_json::json!({"bucket_id":bucket,"project_id":input.target_project,"previous_project":source,"revoked_member_bucket_grants":current.member_bucket_grants,"revoked_service_grants":current.service_grants,"revoked_token_bucket_grants":current.token_bucket_grants})).await?;
    tx.commit().await?;
    Ok(row)
}

pub(super) async fn enqueue_task(
    connection: &mut SqliteConnection,
    task_type: &str,
    bucket: Option<Uuid>,
    detail: Value,
) -> Result<Uuid> {
    let id = crate::model::REQUEST_ID
        .try_with(|id| *id)
        .unwrap_or_else(|_| Uuid::new_v4());
    queries::insert_task(
        id,
        task_type,
        bucket,
        detail,
        crate::model::audit::current(),
    )
    .execute(connection)
    .await?;
    Ok(id)
}

pub(super) async fn start_purge(
    pool: &SqlitePool,
    principal: &Principal,
    name: &str,
    id: Uuid,
    confirmation: Option<&str>,
) -> Result<Value> {
    let mut tx = administration_transaction(pool, principal).await?;
    let row = queries::bucket_by_id(id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(PreconditionFailed))?;
    ensure!(row.name == name, s3s::s3_error!(PreconditionFailed));
    if let Some(task) = queries::existing_purge(id).fetch_optional(&mut *tx).await? {
        return Ok(serde_json::json!({"task_id":task,"existing":true}));
    }
    if let Some(expected) = confirmation {
        ensure!(
            fingerprint(&queries::bucket_impact(id).fetch_one(&mut *tx).await?, None)? == expected,
            s3s::s3_error!(PreconditionFailed)
        );
    }
    queries::seal_purge_bucket(id).execute(&mut *tx).await?;
    let task = enqueue_task(
        &mut tx,
        "purge",
        Some(id),
        serde_json::json!({"bucket_id":id,"name":name}),
    )
    .await?;
    checkpoint_event(
        &mut tx,
        "bucket.purge",
        name,
        serde_json::json!({"bucket_id":id,"task_id":task}),
    )
    .await?;
    tx.commit().await?;
    Ok(serde_json::json!({"task_id":task}))
}

pub(super) async fn lock_execution(
    connection: &mut SqliteConnection,
    task: Execution,
) -> Result<()> {
    ensure!(
        queries::lock_task_execution(task)
            .fetch_optional(connection)
            .await?
            .is_some(),
        crate::model::Error::OperationAborted
    );
    Ok(())
}

pub(super) async fn purge_batch(
    pool: &SqlitePool,
    task: Execution,
    bucket: Uuid,
    cursor: Option<&str>,
    batch: i64,
    active: &[Uuid],
) -> Result<bool> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    lock_execution(&mut tx, task).await?;
    let members = bucket_members(&mut tx, bucket).await?;
    let row = queries::bucket_by_id(bucket).fetch_one(&mut *tx).await?;
    ensure!(
        row.state == "purging",
        crate::model::Error::OperationAborted
    );
    let objects = queries::lock_purge_objects(bucket, cursor, batch)
        .fetch_all(&mut *tx)
        .await?;
    if !objects.is_empty() {
        let mut last = String::new();
        for (key, stream) in &objects {
            last = String::from_utf8(key.clone())?;
            remove_object_reference(&mut tx, bucket, &last, *stream).await?;
            if let Some(stream) = stream {
                queries::retire_stream(*stream).execute(&mut *tx).await?;
            }
        }
        queries::advance_task(task.id, &last, objects.len() as i64)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        return Ok(false);
    }
    queries::purge_upload_history(&mut tx, bucket, active, batch).await?;
    if queries::bucket_has_streams(bucket)
        .fetch_one(&mut *tx)
        .await?
    {
        tx.commit().await?;
        return Ok(false);
    }
    remove_empty_bucket(&mut tx, &row, &members).await?;
    queries::complete_task(task.id).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(true)
}

async fn remove_object_reference(
    connection: &mut SqliteConnection,
    bucket: Uuid,
    key: &str,
    old: Option<Id>,
) -> Result<()> {
    let token = Id::default();
    queries::remove_object(bucket, key, token)
        .execute(&mut *connection)
        .await?;
    supersede_object_quota(connection, bucket, key, token).await?;
    let old = if let Some(id) = old {
        Some((
            id,
            queries::stream_size_etag(id)
                .fetch_one(&mut *connection)
                .await?
                .0,
        ))
    } else {
        None
    };
    let mut accounts = lock_quota(connection, bucket).await?;
    replace_object_quota(connection, &mut accounts, bucket, key, old, None).await
}

use crate::authorization::Permit;
use crate::model::object::{
    CatalogCursor, CatalogFilter, MediaItem, ObjectChange, ObjectTransfer, Outcome, RECEIPT,
    StorageMigration,
};

async fn record_media_operation(
    connection: &mut Transaction<'_, Sqlite>,
    outcome: Outcome,
) -> Result<()> {
    if let Ok((id, bucket, key, action, count)) =
        RECEIPT.try_with(|r| (r.id, r.bucket, r.key.clone(), r.action, r.count))
    {
        ensure!(
            queries::save_media_operation(id, &outcome)
                .execute(&mut **connection)
                .await?
                .rows_affected()
                == 1,
            "media operation receipt is missing"
        );
        checkpoint_event(connection,"object.change",&key,serde_json::json!({"bucket_id":bucket,"operation":action,"version":outcome.output_version,"task_id":outcome.task_id,"operation_id":id})).await?;
        if count > 1
            && let Some(id) = crate::model::audit::current()
        {
            queries::partial_audit(id)
                .execute(&mut **connection)
                .await?;
        }
    }
    Ok(())
}
async fn complete_media_operation(
    connection: &mut Transaction<'_, Sqlite>,
    version: Option<Id>,
) -> Result<()> {
    record_media_operation(
        connection,
        Outcome {
            status: 200,
            code: None,
            output_version: version,
            task_id: None,
        },
    )
    .await
}

pub(super) async fn begin_media_operation(
    pool: &SqlitePool,
    principal: &Principal,
    user: Uuid,
    client: Uuid,
    hash: &[u8],
) -> Result<(Uuid, Vec<u8>)> {
    ensure!(hash.len() == 32, s3s::s3_error!(InvalidArgument));
    ensure!(
        principal.user_id().is_none_or(|id| id == user),
        s3s::s3_error!(AccessDenied)
    );
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    queries::validate_identity(principal, &mut tx).await?;
    queries::insert_media_operation(Uuid::new_v4(), user, principal.token_id(), client, hash)
        .execute(&mut *tx)
        .await?;
    let row = queries::find_media_operation(user, principal.token_id(), client)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(row)
}

pub(super) async fn audit_object_batch(
    pool: &SqlitePool,
    bucket: Uuid,
    detail: Value,
    partial: bool,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    checkpoint_event(&mut tx, "object.batch", &bucket.to_string(), detail).await?;
    if partial && let Some(id) = crate::model::audit::current() {
        queries::partial_audit(id).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}

async fn lock_media_scope(connection: &mut SqliteConnection, permit: &Permit) -> Result<()> {
    lock_permit(connection, permit).await?;
    for bucket in permit.resources.keys() {
        ensure!(
            queries::lock_bucket_uploads(*bucket)
                .fetch_one(&mut *connection)
                .await?
                .0
                == "active",
            crate::model::Error::OperationAborted
        );
    }
    Ok(())
}
async fn selected_object(
    connection: &mut SqliteConnection,
    bucket: Uuid,
    key: &str,
    expected: Option<Id>,
) -> Result<Option<Id>> {
    let actual = queries::lock_object_version(bucket, key)
        .fetch_optional(connection)
        .await?
        .and_then(|r| r.0);
    ensure!(actual == expected, s3s::s3_error!(PreconditionFailed));
    Ok(actual)
}
async fn selected_objects(
    connection: &mut SqliteConnection,
    input: &ObjectTransfer<'_>,
) -> Result<Option<Id>> {
    let mut objects = [
        (
            input.source_bucket,
            input.source_key,
            Some(input.source_version),
        ),
        (input.target_bucket, input.target_key, input.target_version),
    ];
    objects.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
    for (bucket, key, expected) in objects {
        selected_object(connection, bucket, key, expected).await?;
    }
    Ok(input.target_version)
}

async fn replace_object_reference(
    connection: &mut SqliteConnection,
    stream: &crate::app::StoredStream,
    old: Option<Id>,
) -> Result<()> {
    queries::claim_object(stream.bucket_id, &stream.object_key, stream.id)
        .execute(&mut *connection)
        .await?;
    supersede_object_quota(connection, stream.bucket_id, &stream.object_key, stream.id).await?;
    let previous = if let Some(id) = old {
        Some((
            id,
            queries::stream_size_etag(id)
                .fetch_one(&mut *connection)
                .await?
                .0,
        ))
    } else {
        None
    };
    let mut accounts = lock_quota(connection, stream.bucket_id).await?;
    replace_object_quota(
        connection,
        &mut accounts,
        stream.bucket_id,
        &stream.object_key,
        previous,
        Some((stream.id, stream.size)),
    )
    .await?;
    queries::publish_object(stream)
        .execute(&mut *connection)
        .await?;
    if let Some(old) = old
        && old != stream.id
    {
        queries::retire_stream(old).execute(connection).await?;
    }
    Ok(())
}

pub(super) async fn change_object(pool: &SqlitePool, input: ObjectChange) -> Result<bool> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let permit = Permit::for_action(
        input.principal,
        input.bucket,
        if input.public.is_some() {
            Action::Acl
        } else {
            Action::Delete
        },
    );
    lock_media_scope(&mut tx, &permit).await?;
    let old = queries::lock_object_version(input.bucket, &input.key)
        .fetch_optional(&mut *tx)
        .await?
        .and_then(|r| r.0);
    ensure!(
        input.expected.is_none() || input.expected == old,
        s3s::s3_error!(PreconditionFailed)
    );
    if let Some(public) = input.public {
        let id = old.ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
        queries::set_stream_public(id, public)
            .execute(&mut *tx)
            .await?;
        queries::set_catalog_public(input.bucket, &input.key, public)
            .execute(&mut *tx)
            .await?;
        complete_media_operation(&mut tx, Some(id)).await?;
        tx.commit().await?;
        return Ok(false);
    }
    remove_object_reference(&mut tx, input.bucket, &input.key, old).await?;
    if let Some(id) = old {
        queries::retire_stream(id).execute(&mut *tx).await?;
    }
    complete_media_operation(&mut tx, None).await?;
    tx.commit().await?;
    Ok(true)
}

pub(super) async fn move_object(
    pool: &SqlitePool,
    permit: &Permit,
    input: ObjectTransfer<'_>,
) -> Result<()> {
    ensure!(
        (input.source_bucket, input.source_key) != (input.target_bucket, input.target_key),
        s3s::s3_error!(InvalidArgument)
    );
    let mut authority = permit.clone();
    authority.add(input.source_bucket, Action::Read);
    authority.add(input.source_bucket, Action::Delete);
    authority.add(input.target_bucket, Action::Write);
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    lock_media_scope(&mut tx, &authority).await?;
    let old = selected_objects(&mut tx, &input).await?;
    let mut stream = queries::publish_stream_record(input.source_version)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(
        stream.state == "ready",
        crate::model::Error::OperationAborted
    );
    ensure!(
        queries::bucket_by_id(input.source_bucket)
            .fetch_one(&mut *tx)
            .await?
            .storage_id
            == queries::bucket_by_id(input.target_bucket)
                .fetch_one(&mut *tx)
                .await?
                .storage_id,
        "moving between stores requires a storage migration"
    );
    if stream.public_read {
        lock_permit(
            &mut tx,
            &Permit::for_action(permit.principal.clone(), input.target_bucket, Action::Acl),
        )
        .await?;
    }
    remove_object_reference(
        &mut tx,
        input.source_bucket,
        input.source_key,
        Some(input.source_version),
    )
    .await?;
    queries::move_stream(stream.id, input.target_bucket, input.target_key)
        .execute(&mut *tx)
        .await?;
    stream.bucket_id = input.target_bucket;
    stream.object_key = input.target_key.into();
    replace_object_reference(&mut tx, &stream, old).await?;
    complete_media_operation(&mut tx, Some(stream.id)).await?;
    tx.commit().await?;
    Ok(())
}

async fn validate_copy(
    connection: &mut SqliteConnection,
    id: Id,
    bucket: Uuid,
    key: &str,
    public: bool,
) -> Result<crate::app::StoredStream> {
    let stream = queries::publish_stream_record(id)
        .fetch_one(&mut *connection)
        .await?;
    ensure!(
        stream.state == "writing"
            && stream.stream_type == "object"
            && stream.bucket_id == bucket
            && stream.object_key == key
            && stream.public_read == public,
        crate::model::Error::OperationAborted
    );
    if stream.payload_type == "raw" {
        validate_raw(connection, id, stream.size).await?;
        return Ok(stream);
    }
    ensure!(
        !queries::incomplete_extents(id)
            .fetch_one(&mut *connection)
            .await?,
        "copy mapping is incomplete"
    );
    let (end, contiguous) = queries::extent_continuity(id).fetch_one(connection).await?;
    ensure!(
        end == stream.size && contiguous,
        "copy mapping has gaps or overlaps"
    );
    Ok(stream)
}

pub(super) async fn publish_copy(
    pool: &SqlitePool,
    permit: &Permit,
    input: ObjectTransfer<'_>,
    id: Id,
    public: bool,
    expected_public: Option<bool>,
) -> Result<()> {
    let mut authority = permit.clone();
    authority.add(input.source_bucket, Action::Read);
    authority.add(input.target_bucket, Action::Write);
    if public {
        authority.add(input.target_bucket, Action::Acl);
    }
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    lock_media_scope(&mut tx, &authority).await?;
    let old = selected_objects(&mut tx, &input).await?;
    if let Some(expected) = expected_public {
        ensure!(
            queries::quota_stream(input.source_version)
                .fetch_one(&mut *tx)
                .await?
                .public_read
                == expected,
            s3s::s3_error!(PreconditionFailed)
        );
    }
    let stream = validate_copy(&mut tx, id, input.target_bucket, input.target_key, public).await?;
    require_sources(&mut tx, Some(id), &[]).await?;
    queries::publish_stream(id).execute(&mut *tx).await?;
    queries::ready_stream_queues(&mut tx, id).await?;
    replace_object_reference(&mut tx, &stream, old).await?;
    complete_media_operation(&mut tx, Some(id)).await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn queue_storage_migration(
    pool: &SqlitePool,
    permit: &Permit,
    job: &StorageMigration,
) -> Result<()> {
    ensure!(
        matches!(job.action.as_str(), "copy" | "move"),
        s3s::s3_error!(InvalidArgument)
    );
    let mut authority = permit.clone();
    authority.add(job.source_bucket, Action::Read);
    authority.add(job.target_bucket, Action::Write);
    if job.action == "move" {
        authority.add(job.source_bucket, Action::Delete);
    }
    if job.public_read {
        authority.add(job.target_bucket, Action::Acl);
    }
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    lock_media_scope(&mut tx, &authority).await?;
    selected_objects(
        &mut tx,
        &ObjectTransfer {
            source_bucket: job.source_bucket,
            source_key: &job.source_key,
            source_version: job.source_version,
            target_bucket: job.target_bucket,
            target_key: &job.target_key,
            target_version: job.target_version,
        },
    )
    .await?;
    let task = enqueue_task(
        &mut tx,
        "storage_migration",
        Some(job.target_bucket),
        serde_json::to_value(job)?,
    )
    .await?;
    record_media_operation(
        &mut tx,
        Outcome {
            status: 202,
            code: None,
            output_version: None,
            task_id: Some(task),
        },
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn complete_storage_migration(
    pool: &SqlitePool,
    task: Execution,
    id: Id,
    job: &StorageMigration,
) -> Result<()> {
    ensure!(
        matches!(job.action.as_str(), "copy" | "move"),
        s3s::s3_error!(InvalidArgument)
    );
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    lock_execution(&mut tx, task).await?;
    let mut authority = Permit::for_action(Principal::System, job.source_bucket, Action::Read);
    authority.add(job.target_bucket, Action::Write);
    lock_media_scope(&mut tx, &authority).await?;
    let old = selected_objects(
        &mut tx,
        &ObjectTransfer {
            source_bucket: job.source_bucket,
            source_key: &job.source_key,
            source_version: job.source_version,
            target_bucket: job.target_bucket,
            target_key: &job.target_key,
            target_version: job.target_version,
        },
    )
    .await?;
    let stream = validate_copy(
        &mut tx,
        id,
        job.target_bucket,
        &job.target_key,
        job.public_read,
    )
    .await?;
    require_sources(&mut tx, Some(id), &[]).await?;
    ensure!(
        queries::publish_migration_stream(id, task)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    queries::ready_stream_queues(&mut tx, id).await?;
    if job.action == "move" {
        remove_object_reference(
            &mut tx,
            job.source_bucket,
            &job.source_key,
            Some(job.source_version),
        )
        .await?;
        queries::retire_stream(job.source_version)
            .execute(&mut *tx)
            .await?;
    }
    replace_object_reference(&mut tx, &stream, old).await?;
    ensure!(
        queries::complete_migration_task(task)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    tx.commit().await?;
    Ok(())
}

pub(super) async fn abandon_copy(pool: &SqlitePool, id: Id) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let stream = queries::publish_stream_record(id)
        .fetch_optional(&mut *tx)
        .await?;
    if let Some(stream) = stream
        && stream.state == "writing"
    {
        let mut accounts = lock_quota(&mut tx, stream.bucket_id).await?;
        forget_quota_write(&mut tx, &mut accounts, id).await?;
        queries::abandon_copy_stream(id).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}

pub(super) async fn browse_catalog(
    pool: &SqlitePool,
    authority: &Permit,
    bucket: Uuid,
    filter: &mut CatalogFilter,
    cursor: Option<CatalogCursor>,
    sizes: (Option<i64>, Option<i64>),
) -> Result<Vec<MediaItem>> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    let mut connection = super::connection::DeadlineConnection::acquire(pool, deadline).await?;
    let result = tokio::time::timeout_at(deadline, async {
        let mut tx = connection.begin().await?;
        let mut authority = authority.clone();
        authority.add(bucket, Action::List);
        lock_permit(&mut tx, &authority).await?;
        let rows = queries::browse_catalog(&mut tx, bucket, filter, cursor, sizes).await?;
        tx.commit().await?;
        Ok(rows)
    })
    .await;
    match result {
        Ok(result) => {
            connection.release().await?;
            result
        }
        Err(_) => Err(crate::model::Error::QueryCancelled.into()),
    }
}

use crate::model::key::{ManagedKeyCreate, ManagedKeyView};
pub(super) async fn list_managed_keys(
    pool: &SqlitePool,
    principal: &Principal,
) -> Result<Vec<ManagedKeyView>> {
    let mut tx = pool.begin().await?;
    ensure!(
        queries::validate_identity(principal, &mut tx).await?,
        denied()
    );
    let rows = queries::managed_key_views(&mut tx).await?;
    tx.commit().await?;
    Ok(rows)
}

pub(super) async fn create_managed_key(
    pool: &SqlitePool,
    principal: &Principal,
    input: ManagedKeyCreate,
) -> Result<ManagedKeyView> {
    let mut tx = administration_transaction(pool, principal).await?;
    queries::insert_managed_key(&input)
        .execute(&mut *tx)
        .await?;
    let result = queries::managed_key_view(&mut tx, input.id).await?;
    if input.purpose == "block" {
        queries::initialize_nonce_counters(input.id)
            .execute(&mut *tx)
            .await?;
    }
    checkpoint_event(&mut tx, "managed_key.create", &input.id.to_string(), serde_json::json!({"name":result.name,"purpose":result.purpose,"algorithm":result.algorithm,"fingerprint":result.fingerprint})).await?;
    tx.commit().await?;
    Ok(result)
}

pub(super) async fn revoke_managed_key(
    pool: &SqlitePool,
    principal: &Principal,
    id: Uuid,
) -> Result<()> {
    let mut tx = administration_transaction(pool, principal).await?;
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

pub(super) async fn task_page(
    pool: &SqlitePool,
    principal: &Principal,
    filter: crate::model::task::JobQuery,
    cursor: Option<(chrono::DateTime<chrono::Utc>, Uuid)>,
    limit: i64,
) -> Result<Vec<Value>> {
    use sqlx::Row;
    ensure!(limit >= 0, s3s::s3_error!(InvalidArgument));
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut connection = super::connection::DeadlineConnection::acquire(pool, deadline).await?;
    let result = tokio::time::timeout_at(deadline, async {
        let mut tx = connection.begin().await?;
        ensure!(
            queries::validate_identity(principal, &mut tx).await?,
            denied()
        );
        let needle = filter.actor.as_ref().map(|value| value.to_lowercase());
        let mut after = cursor;
        let mut values = Vec::new();
        while (values.len() as i64) < limit {
            let batch = if needle.is_some() { 128 } else { limit };
            let rows = queries::task_page(&filter, after, batch)
                .build()
                .fetch_all(&mut *tx)
                .await?;
            let more = rows.len() as i64 == batch;
            if let Some(last) = rows.last() {
                after = Some((
                    queries::datetime(last.try_get("created_at")?)?,
                    last.try_get("id")?,
                ));
            }
            for row in rows {
                if needle.as_ref().is_none_or(|needle| {
                    row.get::<String, _>("created_by_label")
                        .to_lowercase()
                        .contains(needle)
                }) {
                    values.push(queries::task_value(row, true)?);
                    if values.len() as i64 == limit {
                        break;
                    }
                }
            }
            if needle.is_none() || !more {
                break;
            }
        }
        tx.commit().await?;
        Ok(values)
    })
    .await;
    match result {
        Ok(result) => {
            connection.release().await?;
            result
        }
        Err(_) => Err(crate::model::Error::QueryCancelled.into()),
    }
}

pub(super) async fn task_view(pool: &SqlitePool, principal: &Principal, id: Uuid) -> Result<Value> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut connection = super::connection::DeadlineConnection::acquire(pool, deadline).await?;
    let result = tokio::time::timeout_at(deadline, async {
        let mut tx = connection.begin().await?;
        ensure!(
            queries::validate_identity(principal, &mut tx).await?,
            denied()
        );
        let row = queries::task_view(id)
            .build()
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
        let value = queries::task_value(row, true)?;
        tx.commit().await?;
        Ok(value)
    })
    .await;
    match result {
        Ok(result) => {
            connection.release().await?;
            result
        }
        Err(_) => Err(crate::model::Error::QueryCancelled.into()),
    }
}

use crate::model::cache::{CacheBatch, CacheRelease, CacheStage};

pub(super) async fn cache_stage(pool: &SqlitePool, input: CacheStage<'_>) -> Result<bool> {
    let timeout = i64::try_from(input.timeout)?
        .checked_mul(1_000_000)
        .context("cache timeout overflow")?;
    ensure!(
        crate::storage::parse_upload_cache_name(input.key).is_some_and(|id| !id.is_nil()),
        "invalid local upload cache key"
    );
    ensure!(
        input.chunk.storage_id == Some(input.storage),
        "upload cache storage changed"
    );
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    ensure!(
        queries::lock_cache_stream(input.stream, input.storage)
            .fetch_optional(&mut *tx)
            .await?
            .is_some(),
        crate::model::Error::OperationAborted
    );
    let (state, source, storage, encoding) = queries::lock_cache_source(input.chunk.id)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(
        state == if input.new { "uploading" } else { "ready" },
        "upload source changed"
    );
    ensure!(
        storage == input.storage && encoding == input.chunk.encoding_id,
        "upload source changed"
    );
    if !input.new && source.is_none() {
        return Ok(false);
    }
    if !input.new {
        require_sources(&mut tx, None, &[input.chunk.id]).await?;
    }
    queries::insert_pending_upload(&input, source, timeout)
        .execute(&mut *tx)
        .await?;
    queries::pin_chunk(input.chunk.id, input.stream)
        .execute(&mut *tx)
        .await?;
    queries::ready_cached_chunk(input.chunk.id)
        .execute(&mut *tx)
        .await?;
    queries::insert_chunk_extent(
        input.stream,
        input.offset,
        input.chunk.raw_size,
        input.chunk.id,
    )
    .execute(&mut *tx)
    .await?;
    if !input.new {
        queries::changed_references(&mut tx, &[input.chunk.id], true).await?;
    }
    queries::resize_writing_stream(
        input.stream,
        input
            .offset
            .checked_add(i64::from(input.chunk.raw_size))
            .context("cache extent overflow")?,
    )
    .execute(&mut *tx)
    .await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("upload-cache-before-commit").await;
    tx.commit().await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("upload-cache-committed").await;
    Ok(true)
}

pub(super) async fn cache_seal(
    read: &SqlitePool,
    pool: &SqlitePool,
    stream: Id,
    eof: bool,
) -> Result<bool> {
    if eof {
        queries::seal_stream_uploads(stream).execute(pool).await?;
        return Ok(true);
    }
    let rows = queries::unsealed_uploads(stream).fetch_all(read).await?;
    let mut ids = Vec::new();
    let mut total = 0u64;
    let mut end = None;
    let mut sealed = false;
    for (id, offset, size, maximum) in rows {
        let maximum = crate::config::bytes(&maximum)?;
        if !ids.is_empty() && (end != Some(offset) || total + size as u64 > maximum) {
            sealed = true;
            break;
        }
        ids.push(id);
        total += size as u64;
        end = Some(offset + i64::from(size));
        if total == maximum {
            sealed = true;
            break;
        }
    }
    if sealed {
        queries::seal_upload_chunks(&ids)
            .build()
            .execute(pool)
            .await?;
    }
    Ok(sealed)
}

pub(super) async fn cache_flush(pool: &SqlitePool) -> Result<(Uuid, bool)> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    if let Some(id) = queries::active_cache_flush()
        .fetch_optional(&mut *tx)
        .await?
    {
        return Ok((id, true));
    }
    let id = enqueue_task(
        &mut tx,
        "cache_flush",
        None,
        serde_json::json!({"upper_time":chrono::Utc::now()}),
    )
    .await?;
    tx.commit().await?;
    Ok((id, false))
}

pub(super) async fn cache_enqueue(pool: &SqlitePool) -> Result<bool> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    if let Some(id) = queries::retryable_upload().fetch_optional(&mut *tx).await? {
        queries::queue_upload_retry(id).execute(&mut *tx).await?;
        tx.commit().await?;
        return Ok(true);
    }
    let Some((first, stream, source)) = queries::due_upload().fetch_optional(&mut *tx).await?
    else {
        return Ok(false);
    };
    let id = enqueue_task(
        &mut tx,
        "upload",
        None,
        serde_json::json!({"first":first,"stream_id":stream,"source_pack_id":source}),
    )
    .await?;
    let (maximum, _) = queries::cache_policy(first).fetch_one(&mut *tx).await?;
    let maximum = crate::config::bytes(&maximum)?;
    let candidates = queries::pending_upload_segment(first, stream, source)
        .fetch_all(&mut *tx)
        .await?;
    let mut ids = Vec::new();
    let mut bytes = 0u64;
    let mut end = None;
    for (chunk, offset, size) in candidates {
        if !ids.is_empty() && (end != Some(offset) || bytes + size as u64 > maximum) {
            break;
        }
        ids.push(chunk);
        bytes += size as u64;
        end = Some(offset + i64::from(size));
    }
    if !ids.contains(&first) {
        ids = vec![first];
    }
    queries::claim_pending_uploads(&ids, id)
        .build()
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(true)
}

pub(super) async fn cache_batch(
    pool: &SqlitePool,
    task: Execution,
    detail: &Value,
    flush: bool,
) -> Result<CacheBatch> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    lock_execution(&mut tx, task).await?;
    let upper = if flush {
        Some(serde_json::from_value::<chrono::DateTime<chrono::Utc>>(
            detail["upper_time"].clone(),
        )?)
    } else {
        None
    };
    let first = if let Some(upper) = upper {
        queries::flush_candidate(upper, task.id)
            .fetch_optional(&mut *tx)
            .await?
    } else {
        Id::from_json(&detail["first"])?
    };
    let Some(first) = first else {
        return Ok(if let Some(upper) = upper {
            if queries::flush_pending(upper).fetch_one(&mut *tx).await? {
                CacheBatch::Wait
            } else {
                CacheBatch::Finished
            }
        } else {
            CacheBatch::Finished
        });
    };
    if queries::upload_is_durable(first)
        .fetch_one(&mut *tx)
        .await?
        || queries::upload_is_unused(first).fetch_one(&mut *tx).await?
    {
        return Ok(CacheBatch::Release(first));
    }
    let (maximum, timeout) = queries::cache_policy(first).fetch_one(&mut *tx).await?;
    let maximum = crate::config::bytes(&maximum)?;
    let timeout = crate::config::seconds(&timeout)?;
    let Some((stream, offset, source, expired)) = queries::pending_upload(
        first,
        i64::try_from(timeout)?
            .checked_mul(1_000_000)
            .context("cache timeout overflow")?,
    )
    .fetch_optional(&mut *tx)
    .await?
    else {
        return Ok(CacheBatch::Finished);
    };
    if let Some(pack) = source.filter(|_| !flush) {
        let ids = queries::pending_pack_uploads(pack, task.id)
            .fetch_all(&mut *tx)
            .await?;
        record_source_inputs(&mut tx, task.id, &ids).await?;
        queries::claim_pending_uploads(&ids, task.id)
            .build()
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        return Ok(CacheBatch::Rewrite { stream, pack, ids });
    }
    let all: Vec<crate::model::Mapping> =
        queries::pending_upload_mappings(first, stream, offset, task.id, flush)
            .build_query_as()
            .fetch_all(&mut *tx)
            .await?;
    let mut rows: Vec<crate::model::Mapping> = Vec::new();
    let mut raw = 0u64;
    for row in all {
        if let Some(last) = rows.last()
            && (last.offset_bytes + i64::from(last.chunk.raw_size) != row.offset_bytes
                || raw + row.chunk.raw_size as u64 > maximum
                || !queries::same_references(
                    last.chunk.id,
                    row.chunk.id,
                    row.offset_bytes - last.offset_bytes,
                )
                .fetch_one(&mut *tx)
                .await?)
        {
            break;
        }
        raw += row.chunk.raw_size as u64;
        rows.push(row);
        if flush || source.is_some() {
            break;
        }
    }
    if rows.is_empty() {
        return Ok(CacheBatch::Finished);
    }
    let mut ids: Vec<_> = rows.iter().map(|r| r.chunk.id).collect();
    ids.sort_unstable();
    ids.dedup();
    pin_inputs(&mut tx, task.id, &ids).await?;
    let current: Vec<(Id, Id, Option<Id>, Option<Id>, i64, Option<Id>)> =
        queries::lock_cache_inputs(&ids, task.id, flush)
            .build_query_as()
            .fetch_all(&mut *tx)
            .await?;
    let mut expected: Vec<_> = rows
        .iter()
        .map(|r| {
            (
                r.chunk.id,
                r.chunk.encoding_id,
                r.pack_id,
                stream,
                r.offset_bytes,
                if r.chunk.id == first { source } else { None },
            )
        })
        .collect();
    expected.sort_unstable_by_key(|r| r.0);
    ensure!(
        current == expected,
        "upload inputs changed; retry maintenance"
    );
    queries::claim_pending_uploads(&ids, task.id)
        .build()
        .execute(&mut *tx)
        .await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("upload-prepare-before-commit").await;
    tx.commit().await?;
    Ok(CacheBatch::Encode {
        stream,
        expired,
        rows,
    })
}

async fn release_task_inputs(connection: &mut SqliteConnection, task: Uuid) -> Result<()> {
    queries::retire_task_packs(task)
        .execute(&mut *connection)
        .await?;
    queries::retire_task_encodings(task)
        .execute(&mut *connection)
        .await?;
    queries::release_task_inputs(task)
        .execute(connection)
        .await?;
    Ok(())
}

pub(super) async fn cache_finish(pool: &SqlitePool, task: Execution) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    lock_execution(&mut tx, task).await?;
    queries::unclaim_uploads(task.id).execute(&mut *tx).await?;
    queries::complete_task(task.id).execute(&mut *tx).await?;
    release_task_inputs(&mut tx, task.id).await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn cache_release(
    pool: &SqlitePool,
    ids: &[Id],
    existing_pack: bool,
) -> Result<CacheRelease> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let rows: Vec<(Id, Option<String>)> = queries::lock_pending_chunks(ids)
        .build_query_as()
        .fetch_all(&mut *tx)
        .await?;
    let mut keys = Vec::new();
    for (id, key) in rows {
        if queries::can_release_pending(id, existing_pack)
            .fetch_one(&mut *tx)
            .await?
        {
            queries::schedule_uploaded_pack(id)
                .execute(&mut *tx)
                .await?;
            queries::delete_upload_pins(id).execute(&mut *tx).await?;
            queries::delete_pending_upload(id).execute(&mut *tx).await?;
            if let Some(key) = key {
                keys.push(key);
            }
        }
    }
    tx.commit().await?;
    Ok(CacheRelease {
        keys,
        staged: Vec::new(),
    })
}

pub(super) async fn cache_status(pool: &SqlitePool) -> Result<(Value, Vec<Value>)> {
    let mut tx = pool.begin().await?;
    let value = queries::cache_status(&mut tx).await?;
    tx.commit().await?;
    Ok(value)
}

pub(super) async fn claim_task(
    pool: &SqlitePool,
    input: crate::model::TaskClaim<'_>,
) -> Result<Option<Uuid>> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let Some(id) = queries::claim_task_candidate(input.class, input.active)
        .build_query_scalar::<Uuid>()
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(None);
    };
    ensure!(
        queries::claim_task(id, input.token)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        "task claim changed"
    );
    tx.commit().await?;
    Ok(Some(id))
}

pub(super) async fn finish_task(pool: &SqlitePool, input: crate::model::TaskFinish) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let task = input.execution;
    let Some((task_type, state)) = queries::lock_task_finish(task)
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(());
    };
    let failed = state == "running" && input.error.is_some();
    if failed && matches!(task_type.as_str(), "pack" | "unpack") {
        queries::defer_pack_changes(task.id)
            .execute(&mut *tx)
            .await?;
        queries::defer_pack_maintenance(task.id)
            .execute(&mut *tx)
            .await?;
    }
    if failed && matches!(task_type.as_str(), "upload" | "cache_flush") {
        queries::backoff_pending_uploads(
            task.id,
            input.error.as_deref().unwrap(),
            task_type == "upload",
        )
        .execute(&mut *tx)
        .await?;
    }
    if (failed || state != "running")
        && matches!(
            task_type.as_str(),
            "pack" | "unpack" | "rewrite" | "upload" | "cache_flush"
        )
    {
        release_task_inputs(&mut tx, task.id).await?;
    }
    let next = if failed {
        "failed"
    } else if state == "running" {
        "queued"
    } else {
        &state
    };
    queries::finish_task(
        task,
        next,
        if failed { input.error.as_deref() } else { None },
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

use crate::content::pack::Pack;
use crate::model::Mapping;
use crate::model::metrics::ScopeQuery;
use crate::model::packing::{PackMember, PackObject, PackOutput, PackSummary};

async fn usage_scope(
    connection: &mut SqliteConnection,
    principal: &Principal,
    scope: &ScopeQuery,
) -> Result<(Vec<Uuid>, bool)> {
    let admin = queries::validate_identity(principal, connection).await?;
    let ids = queries::usage_scope_buckets(
        principal.user_id(),
        admin,
        principal.token_id(),
        scope.bucket,
        scope.project,
    )
    .fetch_all(connection)
    .await?;
    ensure!(
        scope.bucket.is_none() || !ids.is_empty(),
        s3s::s3_error!(AccessDenied)
    );
    Ok((ids, admin))
}

pub(super) async fn scoped_pack_page(
    pool: &SqlitePool,
    principal: &Principal,
    scope: &ScopeQuery,
    after: Id,
    limit: i64,
    state: Option<&str>,
) -> Result<Vec<PackSummary>> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut connection = super::connection::DeadlineConnection::acquire(pool, deadline).await?;
    let result = tokio::time::timeout_at(deadline, async {
        let mut tx = connection.begin().await?;
        let (ids, admin) = usage_scope(&mut tx, principal, scope).await?;
        let all = admin && scope.bucket.is_none() && scope.project.is_none();
        let rows = queries::scoped_pack_page(&ids, admin, all, after, limit, state)
            .build_query_as()
            .fetch_all(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(rows)
    })
    .await;
    match result {
        Ok(result) => {
            connection.release().await?;
            result
        }
        Err(_) => Err(crate::model::Error::QueryCancelled.into()),
    }
}

pub(super) async fn scoped_pack_detail(
    pool: &SqlitePool,
    principal: &Principal,
    scope: &ScopeQuery,
    id: Id,
    after: Id,
    limit: i64,
) -> Result<(PackSummary, Vec<PackMember>, bool)> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut connection = super::connection::DeadlineConnection::acquire(pool, deadline).await?;
    let result = tokio::time::timeout_at(deadline, async {
        let mut tx = connection.begin().await?;
        let (ids, admin) = usage_scope(&mut tx, principal, scope).await?;
        let all = admin && scope.bucket.is_none() && scope.project.is_none();
        let pack = queries::scoped_pack(&ids, admin, all, id)
            .build_query_as()
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
        let members = queries::scoped_pack_members(id, &ids, admin, all, after, limit)
            .build_query_as()
            .fetch_all(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok((pack, members, !admin))
    })
    .await;
    match result {
        Ok(result) => {
            connection.release().await?;
            result
        }
        Err(_) => Err(crate::model::Error::QueryCancelled.into()),
    }
}

pub(super) async fn scoped_pack_objects(
    pool: &SqlitePool,
    principal: &Principal,
    scope: &ScopeQuery,
    id: Id,
    cursor: Option<(Uuid, String)>,
) -> Result<Vec<PackObject>> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut connection = super::connection::DeadlineConnection::acquire(pool, deadline).await?;
    let result = tokio::time::timeout_at(deadline, async {
        let mut tx = connection.begin().await?;
        let (ids, admin) = usage_scope(&mut tx, principal, scope).await?;
        let rows = queries::scoped_pack_objects(id, &ids, principal, admin, cursor)
            .build_query_as()
            .fetch_all(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(rows)
    })
    .await;
    match result {
        Ok(result) => {
            connection.release().await?;
            result
        }
        Err(_) => Err(crate::model::Error::QueryCancelled.into()),
    }
}

pub(super) async fn start_rewrite(
    pool: &SqlitePool,
    input: &crate::model::packing::RewriteTask,
) -> Result<(Uuid, bool)> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    queries::lock_rewrite_storage(input.storage_id)
        .fetch_optional(&mut *tx)
        .await?
        .context("enabled storage was not found")?;
    if let Some(id) = queries::active_rewrite(input.storage_id)
        .fetch_optional(&mut *tx)
        .await?
    {
        return Ok((id, true));
    }
    ensure!(
        (input.target_algorithm == "none") == input.target_key_id.is_none(),
        "invalid rewrite encryption"
    );
    if let Some(key) = input.target_key_id {
        queries::encoding_key(key, &input.target_algorithm)
            .fetch_one(&mut *tx)
            .await?;
    }
    let upper = queries::rewrite_upper(input.storage_id)
        .fetch_one(&mut *tx)
        .await?;
    let detail = serde_json::json!({"execution_mode":"single","storage_id":input.storage_id,"upper_chunk_id":upper,"profile_revision":input.profile_revision,"target_key_id":input.target_key_id,"target_key":input.target_key,"target_algorithm":input.target_algorithm});
    let id = enqueue_task(&mut tx, "rewrite", None, detail).await?;
    if input.shards == 0 || !upper.is_valid() {
        queries::complete_task(id).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok((id, false))
}

pub(super) async fn commit_rewrite_chunk(
    pool: &SqlitePool,
    task: Execution,
    source: &Chunk,
    physical: &Chunk,
) -> Result<()> {
    ensure!(
        source.id == physical.id
            && source.storage_id == physical.storage_id
            && source.raw_size == physical.raw_size
            && source.hash == physical.hash,
        "rewrite content identity changed"
    );
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    lock_execution(&mut tx, task).await?;
    let current = queries::lock_rewrite_chunk(source.id)
        .fetch_optional(&mut *tx)
        .await?;
    ensure!(
        current == Some((None, source.encoding_id)),
        "chunk source changed during rewrite"
    );
    check_source_inputs(&mut tx, task.id, &[source.id]).await?;
    let owners: Vec<Uuid> = queries::pack_input_owners(&[source.id])
        .build_query_scalar()
        .fetch_all(&mut *tx)
        .await?;
    ensure!(
        owners == [task.id],
        "rewrite input is not owned by this task"
    );
    ensure!(
        queries::rewrite_encoding_owner(physical.encoding_id, task.id)
            .fetch_one(&mut *tx)
            .await?,
        "encoding output changed"
    );
    ensure!(
        queries::register_encoding(physical)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        "encoding output changed"
    );
    queries::refresh_encoding_fingerprint(&mut tx, physical.encoding_id).await?;
    ensure!(
        queries::retire_rewrite_encoding(source.encoding_id)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        "source encoding changed during rewrite"
    );
    ensure!(
        queries::complete_encoding(physical.encoding_id)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        "encoding output changed"
    );
    ensure!(
        queries::publish_chunk_encoding(source.id, physical.encoding_id)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        "chunk output changed"
    );
    queries::advance_task(task.id, &source.id.to_string(), 1)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn commit_rewrite_pack(
    pool: &SqlitePool,
    task: Execution,
    old: &Pack,
    new: &Pack,
    current: &[Id],
    members: &[Chunk],
    first: Id,
) -> Result<()> {
    ensure!(
        old.storage_id == new.storage_id
            && old.raw_size == new.raw_size
            && old.member_count == new.member_count
            && members.len() == new.member_count as usize,
        "pack content identity changed"
    );
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    lock_execution(&mut tx, task).await?;
    let locked = queries::lock_rewrite_members(old.id)
        .fetch_all(&mut *tx)
        .await?;
    ensure!(locked == current, "pack membership changed during rewrite");
    check_source_inputs(&mut tx, task.id, current).await?;
    let owners: Vec<Uuid> = queries::pack_input_owners(current)
        .build_query_scalar()
        .fetch_all(&mut *tx)
        .await?;
    ensure!(
        owners.len() == current.len() && owners.iter().all(|id| *id == task.id),
        "rewrite inputs are not owned by this task"
    );
    let member_ids: Vec<_> = members.iter().map(|c| c.id).collect();
    ensure!(
        queries::pack_member_ids(old.id).fetch_all(&mut *tx).await? == member_ids,
        "pack content members changed during rewrite"
    );
    ensure!(
        queries::pack_metadata(new)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        "new pack changed during rewrite"
    );
    queries::refresh_pack_fingerprint(&mut tx, new.id).await?;
    queries::insert_pack_members(new.id, members.iter())
        .build()
        .execute(&mut *tx)
        .await?;
    ensure!(
        queries::publish_prepared_pack(new.id, task.id)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        "new pack changed during rewrite"
    );
    ensure!(
        queries::replace_rewrite_pack(old.id, new.id)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == current.len() as u64,
        "pack membership changed during rewrite"
    );
    ensure!(
        queries::retire_rewrite_pack(old.id)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        "source pack changed during rewrite"
    );
    // Packs can interleave chunk IDs; advance only past this pack's scan key.
    queries::advance_task(task.id, &first.to_string(), 1)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn start_pack(pool: &SqlitePool, policy: &str, automatic: bool) -> Result<Value> {
    ensure!(
        matches!(policy, "pack" | "reuse" | "reclaim" | "repack" | "range"),
        "invalid pack policy"
    );
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    queries::lock_pack_policy(policy)
        .fetch_one(&mut *tx)
        .await?;
    if let Some(id) = queries::active_pack_task(policy)
        .fetch_optional(&mut *tx)
        .await?
    {
        return Ok(serde_json::json!({"task_id":id,"existing":true}));
    }
    let storages = queries::pack_storages(Some(policy))
        .fetch_all(&mut *tx)
        .await?;
    let detail =
        serde_json::json!({"policy":policy,"upper_time":chrono::Utc::now(),"automatic":automatic});
    let mut due = Vec::new();
    for row in storages {
        let profile = queries::storage_profile(row.0).fetch_one(&mut *tx).await?;
        if automatic
            && !queries::pack_policy_due(row.0, policy, profile.maintenance_interval(policy)?)
                .fetch_one(&mut *tx)
                .await?
        {
            continue;
        }
        queries::pack_policy_scheduled(row.0, policy)
            .execute(&mut *tx)
            .await?;
        due.push(row);
    }
    let storages = due;
    if automatic && storages.is_empty() {
        tx.commit().await?;
        return Ok(serde_json::json!({"idle":true}));
    }
    let id = enqueue_storage_task(&mut tx, "pack", detail, &storages).await?;
    tx.commit().await?;
    Ok(serde_json::json!({"task_id":id}))
}

pub(super) async fn start_unpack(pool: &SqlitePool, pack: Option<Id>, all: bool) -> Result<Value> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    queries::lock_pack_policy("pack")
        .fetch_one(&mut *tx)
        .await?;
    if let Some(id) = queries::active_unpack_task(all, pack)
        .fetch_optional(&mut *tx)
        .await?
    {
        return Ok(serde_json::json!({"task_id":id,"existing":true}));
    }
    let storages = if let Some(pack) = pack {
        queries::unpack_storage(pack).fetch_all(&mut *tx).await?
    } else {
        queries::pack_storages(None).fetch_all(&mut *tx).await?
    };
    let detail = serde_json::json!({"pack_id":pack,"all":all,"upper_time":chrono::Utc::now()});
    let id = enqueue_storage_task(&mut tx, "unpack", detail, &storages).await?;
    tx.commit().await?;
    Ok(serde_json::json!({"task_id":id}))
}

async fn enqueue_storage_task(
    connection: &mut SqliteConnection,
    task_type: &str,
    mut detail: Value,
    storages: &[(Uuid, i64)],
) -> Result<Uuid> {
    detail["execution_mode"] = serde_json::json!("single");
    detail["storages"] = serde_json::json!(
        storages
            .iter()
            .map(|(id, revision)| serde_json::json!({"storage_id":id,"profile_revision":revision}))
            .collect::<Vec<_>>()
    );
    detail["storage_index"] = serde_json::json!(0);
    let history = detail["phase"] == "history";
    if !history && let Some((id, revision)) = storages.first() {
        detail["storage_id"] = serde_json::json!(id);
        detail["profile_revision"] = serde_json::json!(revision);
    }
    let id = enqueue_task(connection, task_type, None, detail).await?;
    if storages.is_empty() && !history {
        queries::complete_task(id).execute(connection).await?;
    }
    Ok(id)
}

pub(super) async fn advance_pack_storage(
    pool: &SqlitePool,
    task: Execution,
    processed: i64,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let mut detail = queries::lock_task_detail(task)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(crate::model::Error::OperationAborted)?;
    let done = advance_storage_scope(&mut detail)?;
    queries::advance_storage_task(task.id, detail, processed, done)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    #[cfg(feature = "fault-injection")]
    if !done {
        crate::faults::point("pack-storage-advanced").await;
    }
    Ok(())
}

fn advance_storage_scope(detail: &mut Value) -> Result<bool> {
    let index = if detail["phase"] == "history" {
        detail["phase"] = serde_json::json!("storage");
        0
    } else {
        detail["storage_index"]
            .as_u64()
            .context("missing task storage index")?
            .checked_add(1)
            .context("task storage index overflow")?
    };
    let next = detail["storages"]
        .as_array()
        .context("missing task storages")?
        .get(usize::try_from(index)?)
        .cloned();
    detail["storage_index"] = serde_json::json!(index);
    if let Some(next) = &next {
        detail["storage_id"] = next["storage_id"].clone();
        detail["profile_revision"] = next["profile_revision"].clone();
    }
    Ok(next.is_none())
}

pub(super) async fn finish_pack_change(
    pool: &SqlitePool,
    task: Execution,
    pack: Id,
    generation: i64,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    lock_execution(&mut tx, task).await?;
    queries::finish_pack_change(pack, generation)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}
pub(super) async fn defer_pack_queue(
    pool: &SqlitePool,
    task: Execution,
    stream: Id,
    generation: i64,
    later: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    lock_execution(&mut tx, task).await?;
    queries::defer_pack_queue(stream, generation, later)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}
pub(super) async fn finish_pack_queue(
    pool: &SqlitePool,
    task: Execution,
    stream: Id,
    generation: i64,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    lock_execution(&mut tx, task).await?;
    queries::finish_pack_queue(stream, generation)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}
pub(super) async fn advance_pack_queue(
    pool: &SqlitePool,
    task: Execution,
    stream: Id,
    generation: i64,
    cursor: i64,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    lock_execution(&mut tx, task).await?;
    queries::advance_pack_queue(stream, generation, cursor)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}
pub(super) async fn record_range(
    pool: &SqlitePool,
    task: Execution,
    pack: Id,
    processed: usize,
    decision: Value,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    lock_execution(&mut tx, task).await?;
    queries::mark_range_checked(pack).execute(&mut *tx).await?;
    queries::record_range_result(
        task.id,
        pack.to_string(),
        i64::try_from(processed)?,
        decision,
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn pin_pack_inputs(
    pool: &SqlitePool,
    task: Execution,
    rows: &[Mapping],
) -> Result<()> {
    let mut ids: Vec<_> = rows.iter().map(|r| r.chunk.id).collect();
    ids.sort_unstable();
    ids.dedup();
    if ids.is_empty() {
        return Ok(());
    }
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    lock_execution(&mut tx, task).await?;
    pin_inputs(&mut tx, task.id, &ids).await?;
    tx.commit().await?;
    Ok(())
}

async fn pin_inputs(db: &mut SqliteConnection, task: Uuid, ids: &[Id]) -> Result<()> {
    let locked: Vec<(Id, Option<Id>, Uuid)> = queries::lock_pack_layout(ids)
        .build_query_as()
        .fetch_all(&mut *db)
        .await?;
    ensure!(
        locked.len() == ids.len(),
        "pack inputs changed; retry maintenance"
    );
    let owners: Vec<Uuid> = queries::pack_input_owners(ids)
        .build_query_scalar()
        .fetch_all(&mut *db)
        .await?;
    ensure!(
        owners.iter().all(|id| *id == task),
        "pack input is already owned"
    );
    record_source_inputs(db, task, ids).await?;
    queries::insert_pack_inputs(task, ids)
        .build()
        .execute(db)
        .await?;
    Ok(())
}

pub(super) async fn prepare_encoding(
    pool: &SqlitePool,
    physical: &Chunk,
    task: Execution,
) -> Result<Id> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    lock_execution(&mut tx, task).await?;
    if let Some(key) = physical.block_key_id {
        queries::encoding_key(key, &physical.algorithm)
            .fetch_one(&mut *tx)
            .await?;
    }
    let id = queries::prepare_encoding(physical, task)
        .execute(&mut *tx)
        .await?
        .last_insert_rowid()
        .into();
    queries::refresh_encoding_fingerprint(&mut tx, id).await?;
    tx.commit().await?;
    Ok(id)
}

pub(super) async fn prepare_pack(
    pool: &SqlitePool,
    storage: Uuid,
    algorithm: &str,
    key: Option<Uuid>,
    bytes: i64,
    count: i32,
    task: Execution,
) -> Result<Pack> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    lock_execution(&mut tx, task).await?;
    if let Some(key) = key {
        queries::encoding_key(key, algorithm)
            .fetch_one(&mut *tx)
            .await?;
    }
    let id = queries::prepare_pack(storage, algorithm, key, bytes, count, task)
        .execute(&mut *tx)
        .await?
        .last_insert_rowid()
        .into();
    queries::refresh_pack_fingerprint(&mut tx, id).await?;
    let pack = queries::pack_record(id).fetch_one(&mut *tx).await?;
    tx.commit().await?;
    Ok(pack)
}

pub(super) async fn pack_metadata(pool: &SqlitePool, pack: &Pack) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    ensure!(
        queries::pack_metadata(pack)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        "pack output changed"
    );
    queries::refresh_pack_fingerprint(&mut tx, pack.id).await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn insert_pack_members(
    pool: &SqlitePool,
    pack: Id,
    rows: &[Mapping],
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let record = queries::lock_prepared_pack(pack)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(
        rows.len() == record.member_count as usize
            && rows.iter().all(|r| r.chunk.storage_id == record.storage_id)
            && rows
                .iter()
                .map(|r| i64::from(r.chunk.raw_size))
                .sum::<i64>()
                == record.raw_size,
        "pack members changed"
    );
    queries::insert_pack_members(pack, rows.iter().map(|row| &row.chunk))
        .build()
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn commit_pack_outputs(
    pool: &SqlitePool,
    task: Execution,
    rows: &[Mapping],
    outputs: &[PackOutput],
    split: bool,
    settings: &crate::content::pack::Config,
    flush_seconds: f64,
) -> Result<()> {
    let mut ids: Vec<_> = rows.iter().map(|r| r.chunk.id).collect();
    ids.sort_unstable();
    let mut retained: Vec<_> = outputs
        .iter()
        .flat_map(|o| o.chunks.iter().copied())
        .collect();
    retained.sort_unstable();
    ensure!(
        !ids.is_empty()
            && ids.windows(2).all(|p| p[0] != p[1])
            && retained.windows(2).all(|p| p[0] != p[1])
            && retained.iter().all(|id| ids.binary_search(id).is_ok()),
        "invalid pack output members"
    );
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    lock_execution(&mut tx, task).await?;
    let current: Vec<(Id, Option<Id>, Uuid)> = queries::lock_pack_layout(&ids)
        .build_query_as()
        .fetch_all(&mut *tx)
        .await?;
    ensure!(
        current.len() == rows.len()
            && rows
                .iter()
                .all(|r| current.iter().any(|(id, p, s)| *id == r.chunk.id
                    && *p == r.pack_id
                    && Some(*s) == r.chunk.storage_id)),
        "pack layout changed; retry maintenance"
    );
    let owners: Vec<Uuid> = queries::pack_input_owners(&ids)
        .build_query_scalar()
        .fetch_all(&mut *tx)
        .await?;
    ensure!(
        owners.len() == ids.len() && owners.iter().all(|id| *id == task.id),
        "pack inputs are not owned by this task"
    );
    check_source_inputs(&mut tx, task.id, &ids).await?;
    let before: i64 = queries::pack_layout_bytes(&ids)
        .build_query_scalar()
        .fetch_one(&mut *tx)
        .await?;
    let added = queries::pack_added_bytes(task.id)
        .fetch_one(&mut *tx)
        .await?;
    let after: i64 = outputs.iter().map(PackOutput::size).sum();
    let (upload, range) = queries::pack_task_policy(task.id)
        .fetch_one(&mut *tx)
        .await?;
    let cooldown = crate::config::seconds(&settings.repack_cooldown)? as f64;
    if !split && !upload {
        ensure!(
            queries::pack_exclusive(
                &ids,
                cooldown,
                crate::config::seconds(&settings.range_repack_window)? as f64,
                flush_seconds
            )
            .build_query_scalar::<bool>()
            .fetch_one(&mut *tx)
            .await?,
            "pack references changed; retry maintenance"
        );
    }
    for output in outputs {
        ensure!(
            !output.chunks.is_empty() && output.pack.is_some() != output.independent.is_some(),
            "invalid pack output"
        );
        if let Some(pack) = &output.pack {
            ensure!(
                output.chunks.len() == pack.member_count as usize
                    && output.chunks.iter().all(|id| rows
                        .iter()
                        .any(|r| r.chunk.id == *id && r.chunk.storage_id == pack.storage_id)),
                "pack members changed"
            );
            ensure!(
                queries::pack_member_ids(pack.id)
                    .fetch_all(&mut *tx)
                    .await?
                    == output.chunks,
                "pack members changed"
            );
            for pair in output.chunks.windows(2) {
                let a = rows.iter().find(|r| r.chunk.id == pair[0]).unwrap();
                let b = rows.iter().find(|r| r.chunk.id == pair[1]).unwrap();
                ensure!(
                    queries::same_references(pair[0], pair[1], b.offset_bytes - a.offset_bytes)
                        .fetch_one(&mut *tx)
                        .await?,
                    "pack reference boundaries changed; retry maintenance"
                );
            }
            ensure!(
                queries::publish_prepared_pack(pack.id, task.id)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected()
                    == 1,
                "pack output changed"
            );
            queries::retire_pack_encodings(&output.chunks, None)
                .build()
                .execute(&mut *tx)
                .await?;
        } else if let Some(chunk) = &output.independent {
            ensure!(
                output.chunks == [chunk.id],
                "independent output members changed"
            );
            let (state, owner) = queries::lock_output_encoding(chunk.encoding_id, chunk.id)
                .fetch_one(&mut *tx)
                .await?;
            if state != "ready" {
                ensure!(
                    state == "uploading" && owner == Some(task.id),
                    "encoding output changed"
                );
                queries::retire_pack_encodings(&output.chunks, Some(chunk.encoding_id))
                    .build()
                    .execute(&mut *tx)
                    .await?;
                ensure!(
                    queries::complete_encoding(chunk.encoding_id)
                        .execute(&mut *tx)
                        .await?
                        .rows_affected()
                        == 1,
                    "encoding output changed"
                );
            }
            // Reused ready encodings keep their persisted metadata, even if the input is stale.
            ensure!(
                queries::publish_chunk_encoding(chunk.id, chunk.encoding_id)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected()
                    == 1,
                "chunk output changed"
            );
        }
        queries::publish_pack_layout(
            &output.chunks,
            output.pack.as_ref().map(|p| p.id),
            split,
            if range {
                crate::config::seconds(&settings.range_repack_after)? as f64
            } else {
                cooldown
            },
            range,
        )
        .build()
        .execute(&mut *tx)
        .await?;
    }
    let dead: Vec<_> = ids
        .iter()
        .filter(|id| retained.binary_search(id).is_err())
        .copied()
        .collect();
    if !dead.is_empty() {
        ensure!(
            !queries::pack_has_references(&dead)
                .build_query_scalar::<bool>()
                .fetch_one(&mut *tx)
                .await?,
            "pack acquired a new reference; retry maintenance"
        );
        queries::fail_dead_pack_chunks(&dead)
            .build()
            .execute(&mut *tx)
            .await?;
    }
    if split {
        queries::schedule_repacked_streams(&retained, cooldown)
            .build()
            .execute(&mut *tx)
            .await?;
    }
    let old: Vec<_> = rows.iter().filter_map(|r| r.pack_id).collect();
    queries::retire_replaced_packs(&old)
        .build()
        .execute(&mut *tx)
        .await?;
    queries::record_pack_rewrite(task.id, serde_json::json!({"before_bytes":before,"output_bytes":after,"temporary_added_bytes":added,"transition_bytes":before+added})).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn pack_detail(pool: &SqlitePool, id: Id) -> Result<(Value, Vec<Value>)> {
    let mut tx = pool.begin().await?;
    let value = queries::pack_detail(&mut tx, id).await?;
    tx.commit().await?;
    Ok(value)
}
pub(super) async fn rewrite_candidates(
    pool: &SqlitePool,
    after: Id,
    job: &crate::model::packing::RewriteTask,
) -> Result<(Option<Chunk>, Option<(Id, Id)>)> {
    let mut tx = pool.begin().await?;
    let loose = queries::rewrite_loose_candidate(after, job)
        .build_query_as()
        .fetch_optional(&mut *tx)
        .await?;
    let pack = queries::rewrite_pack_candidate(after, job)
        .build_query_as()
        .fetch_optional(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok((loose, pack))
}
pub(super) async fn rewrite_pack_inputs(
    pool: &SqlitePool,
    pack: Id,
) -> Result<(Pack, Vec<Chunk>, Vec<Id>)> {
    let mut tx = pool.begin().await?;
    let old = queries::pack_record(pack).fetch_one(&mut *tx).await?;
    let members = queries::rewrite_members(pack)
        .build_query_as::<Chunk>()
        .fetch_all(&mut *tx)
        .await?;
    ensure!(
        members.len() == old.member_count as usize,
        "pack member count changed during rewrite"
    );
    let current = queries::rewrite_current_members(pack)
        .fetch_all(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok((old, members, current))
}

use crate::model::integrity::{Cursor, Detail, Finding, StorageSnapshot};
use crate::worker::integrity::Request as IntegrityRequest;

pub(super) async fn start_integrity(
    pool: &SqlitePool,
    input: IntegrityRequest,
    bucket: Option<Bucket>,
) -> Result<(Uuid, usize)> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    ensure!(
        !queries::integrity_busy().fetch_one(&mut *tx).await?,
        s3s::s3_error!(OperationAborted, "an integrity check is already active")
    );
    let storages = if let Some(storage) = bucket.as_ref().and_then(|b| b.storage_id) {
        queries::integrity_storages(Some(storage))
            .fetch_all(&mut *tx)
            .await?
    } else if bucket.is_some() {
        Vec::new()
    } else {
        queries::integrity_storages(None)
            .fetch_all(&mut *tx)
            .await?
    };
    let storages: Vec<Option<Uuid>> = if storages.is_empty() {
        vec![None]
    } else {
        storages.into_iter().map(Some).collect()
    };
    let mut detail = Detail {
        storage_id: None,
        mode: input.mode,
        phase: "metadata".into(),
        bucket_id: bucket.as_ref().map(|b| b.id),
        bucket: input.bucket,
        key: input.key,
        upper_chunk_id: "0".into(),
        upper_object: None,
        objects_checked: 0,
        chunks_checked: 0,
        bytes_checked: 0,
        issues: 0,
        skipped: 0,
        finished_at: None,
        storages: Vec::with_capacity(storages.len()),
        storage_index: 0,
    };
    for storage in storages {
        detail.storage_id = storage;
        let upper_chunk_id = queries::integrity_upper_chunk(storage)
            .fetch_one(&mut *tx)
            .await?
            .to_string();
        let upper_object = queries::integrity_upper_object(&mut tx, &detail).await?;
        detail.storages.push(StorageSnapshot {
            storage_id: storage,
            upper_chunk_id,
            upper_object,
        });
    }
    let first = &detail.storages[0];
    detail.storage_id = first.storage_id;
    detail.upper_chunk_id = first.upper_chunk_id.clone();
    detail.upper_object = first.upper_object.clone();
    let id = enqueue_task(
        &mut tx,
        "integrity",
        detail.bucket_id,
        serde_json::to_value(detail)?,
    )
    .await?;
    tx.commit().await?;
    Ok((id, 0))
}

pub(super) async fn checkpoint_integrity(
    pool: &SqlitePool,
    task: Execution,
    mut cursor: String,
    mut detail: Detail,
    issues: Vec<Finding>,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    lock_execution(&mut tx, task).await?;
    for issue in issues {
        let inserted = queries::insert_integrity_finding(task.id, &issue)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        detail.issues += inserted;
        if inserted > 0 {
            tracing::warn!(task_id=%task.id,chunk_id=?issue.chunk_id,stream_id=?issue.stream_id,code=%issue.code,"integrity finding");
        }
    }
    let mut advanced = false;
    if detail.phase == "done" && !detail.storages.is_empty() {
        detail.storage_index += 1;
        if let Some(next) = detail.storages.get(detail.storage_index) {
            detail.storage_id = next.storage_id;
            detail.upper_chunk_id = next.upper_chunk_id.clone();
            detail.upper_object = next.upper_object.clone();
            detail.phase = "metadata".into();
            detail.finished_at = None;
            cursor = serde_json::to_string(&Cursor::default())?;
            advanced = true;
        }
    }
    let processed = i64::try_from(
        detail
            .objects_checked
            .checked_add(detail.chunks_checked)
            .context("integrity count overflow")?,
    )?;
    queries::integrity_checkpoint(
        task.id,
        cursor,
        serde_json::to_value(&detail)?,
        processed,
        detail.phase == "done",
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    if advanced {
        #[cfg(feature = "fault-injection")]
        crate::faults::point("integrity-storage-advanced").await;
    }
    Ok(())
}

pub(super) async fn integrity_objects(
    pool: &SqlitePool,
    task: Uuid,
    issue: Id,
    after: Option<(Uuid, String)>,
) -> Result<Vec<Value>> {
    let mut tx = pool.begin().await?;
    let rows = queries::integrity_objects(&mut tx, task, issue, after).await?;
    tx.commit().await?;
    Ok(rows)
}

use crate::model::maintenance::{MaintenanceCompletion, MaintenanceExecution, MaintenancePreview};

use crate::model::maintenance::{FragmentCleanup, FragmentDeletion};

pub(super) async fn recover_runtime(pool: &SqlitePool, maintenance: bool) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    queries::reset_access_coverage().execute(&mut *tx).await?;
    queries::abandon_startup_streams().execute(&mut *tx).await?;
    queries::fail_startup_chunks().execute(&mut *tx).await?;
    queries::retire_startup_encodings()
        .execute(&mut *tx)
        .await?;
    queries::retire_startup_packs().execute(&mut *tx).await?;
    queries::clear_pack_inputs().execute(&mut *tx).await?;
    queries::reset_completing_uploads()
        .execute(&mut *tx)
        .await?;
    queries::reset_startup_tasks(maintenance)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}
pub(super) async fn claim_stale_stream(
    pool: &SqlitePool,
    id: Id,
    storage: Option<Uuid>,
    retention: Option<f64>,
) -> Result<bool> {
    let Some(mut tx) = try_cleanup_transaction(pool).await? else {
        return Ok(false);
    };
    if queries::lock_stale_stream(id, storage, retention)
        .build_query_scalar::<Id>()
        .fetch_optional(&mut *tx)
        .await?
        .is_none()
    {
        return Ok(false);
    }
    queries::abandon_cleanup_stream(id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(true)
}
pub(super) async fn cleanup_stream(pool: &SqlitePool, id: Id, batch: i64) -> Result<usize> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let Some(bucket) = queries::cleanup_stream_bucket(id)
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(0);
    };
    let mut accounts = lock_quota(&mut tx, bucket).await?;
    if queries::lock_cleanup_stream(id)
        .fetch_optional(&mut *tx)
        .await?
        .is_none()
    {
        return Ok(0);
    }
    forget_quota_write(&mut tx, &mut accounts, id).await?;
    let raw = queries::raw_cleanup_ranges(id, batch.clamp(1, 128))
        .fetch_all(&mut *tx)
        .await?;
    if let Some((last, _)) = raw.last() {
        let mut sources: Vec<_> = raw.iter().map(|(_, source)| *source).collect();
        sources.sort_unstable();
        sources.dedup();
        for source in sources {
            queries::raw_source(source).fetch_one(&mut *tx).await?;
            queries::raw_touch_unreferenced(source)
                .execute(&mut *tx)
                .await?;
        }
        queries::raw_detach(id, *last).execute(&mut *tx).await?;
    }
    let candidates = queries::retire_stream_encodings(id, batch)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    let detached = queries::detach_stream_chunks(&mut tx, id, batch).await?;
    let extents = queries::remove_stream_extents(&mut tx, id, batch).await?;
    let mut removed = 0;
    if queries::cleanup_stream_empty(id)
        .fetch_one(&mut *tx)
        .await?
    {
        queries::detach_upload_output(id).execute(&mut *tx).await?;
        removed = queries::remove_empty_stream(id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
    }
    tx.commit().await?;
    Ok(raw.len() + extents + (candidates + detached + removed) as usize)
}
// Keep new references fenced until the local fragment file has been removed.
struct FragmentDelete {
    tx: Transaction<'static, Sqlite>,
    id: Id,
}
#[async_trait::async_trait]
impl FragmentDeletion for FragmentDelete {
    async fn finish(self: Box<Self>) -> Result<()> {
        let Self { mut tx, id } = *self;
        queries::remove_fragment_row(id).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
}
pub(super) async fn claim_fragment(pool: &SqlitePool, id: Id) -> Result<Option<FragmentCleanup>> {
    let Some(mut tx) = try_cleanup_transaction(pool).await? else {
        return Ok(None);
    };
    let Some((location_id, sealed, owner, file)) = queries::lock_cleanup_fragment(id)
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(None);
    };
    Ok(Some(FragmentCleanup {
        location_id,
        sealed,
        file,
        owner,
        storage: None,
        key: None,
        deletion: Box::new(FragmentDelete { tx, id }),
    }))
}
pub(super) async fn cleanup_tombstones(
    pool: &SqlitePool,
    storage: Option<Uuid>,
    batch: i64,
) -> Result<u64> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let count = queries::remove_tombstones(&mut tx, storage, batch).await?;
    tx.commit().await?;
    Ok(count)
}
pub(super) async fn retire_chunk(
    pool: &SqlitePool,
    id: Id,
    storage: Uuid,
    grace: f64,
    active: &[Id],
) -> Result<Option<Vec<Uuid>>> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let Some((owner,)) = queries::lock_reclaim_chunk(id, storage, grace)
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(None);
    };
    if owner.is_some_and(|id| active.contains(&id)) {
        return Ok(None);
    }
    queries::claim_reclaim_chunk(id).execute(&mut *tx).await?;
    let locations = queries::reclaim_locations(id).fetch_all(&mut *tx).await?;
    queries::retire_chunk_encodings(id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(Some(locations))
}
pub(super) async fn mark_orphan_chunks(pool: &SqlitePool) -> Result<usize> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let changed = queries::mark_orphan_chunks()
        .execute(&mut *tx)
        .await?
        .rows_affected();
    tx.commit().await?;
    Ok(changed as usize)
}
pub(super) async fn cleanup_history(
    pool: &SqlitePool,
    section: &str,
    age: f64,
    batch: i64,
    active: &[Uuid],
    timeout_ms: u128,
) -> Result<u64> {
    let deadline = tokio::time::Instant::now()
        + std::time::Duration::from_millis(timeout_ms.clamp(1, u64::MAX as u128) as u64);
    let mut connection = super::connection::DeadlineConnection::acquire(pool, deadline).await?;
    let result = tokio::time::timeout_at(deadline, async {
        let mut tx = connection.begin_with(queries::BEGIN_WRITE).await?;
        let count = queries::cleanup_history(&mut tx, section, age, batch, active).await?;
        tx.commit().await?;
        Ok(count)
    })
    .await;
    match result {
        Ok(result) => {
            connection.release().await?;
            result
        }
        Err(_) => Err(crate::model::Error::QueryCancelled.into()),
    }
}

pub(super) async fn maintenance_mode(pool: &SqlitePool, enabled: bool) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    queries::set_maintenance_mode(enabled)
        .execute(&mut *tx)
        .await?;
    if enabled {
        queries::pause_maintenance_tasks().execute(&mut *tx).await?;
    }
    checkpoint_event(
        &mut tx,
        "maintenance.mode",
        "deployment",
        serde_json::json!({"enabled":enabled}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}
pub(super) async fn change_pack_creation(pool: &SqlitePool, paused: bool) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    queries::lock_pack_policy("pack")
        .fetch_one(&mut *tx)
        .await?;
    ensure!(
        paused || !queries::unpacking_all().fetch_one(&mut *tx).await?,
        s3s::s3_error!(
            OperationAborted,
            "finish all-pack unpacking before enabling new packs"
        )
    );
    queries::pause_pack_creation(paused)
        .execute(&mut *tx)
        .await?;
    checkpoint_event(
        &mut tx,
        "maintenance.pack_creation",
        "pack",
        serde_json::json!({"paused":paused}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}
pub(super) async fn change_maintenance_policy(
    pool: &SqlitePool,
    policy: &str,
    paused: bool,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    queries::lock_pack_policy(policy)
        .fetch_one(&mut *tx)
        .await?;
    if policy == "gc" {
        queries::pause_gc(paused).execute(&mut *tx).await?;
    }
    queries::pause_maintenance_policy(policy, paused)
        .execute(&mut *tx)
        .await?;
    checkpoint_event(
        &mut tx,
        "maintenance.policy",
        policy,
        serde_json::json!({"paused":paused}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}
pub(super) async fn start_lifecycle_policy(
    pool: &SqlitePool,
    policy: &str,
) -> Result<(Uuid, bool)> {
    ensure!(
        matches!(policy, "cleanup" | "gc"),
        "invalid lifecycle policy"
    );
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    queries::lock_pack_policy(policy)
        .fetch_one(&mut *tx)
        .await?;
    if let Some(id) = queries::active_lifecycle_task(policy)
        .fetch_optional(&mut *tx)
        .await?
    {
        return Ok((id, true));
    }
    let storages = queries::pack_storages(None).fetch_all(&mut *tx).await?;
    let id = enqueue_storage_task(
        &mut tx,
        policy,
        serde_json::json!({"phase":if policy=="cleanup" {"history"} else {"storage"}}),
        &storages,
    )
    .await?;
    queries::scheduled_maintenance(policy)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok((id, false))
}
pub(super) async fn finish_lifecycle_policy(
    pool: &SqlitePool,
    task: Execution,
    count: i64,
    again: bool,
    report: Value,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let mut detail = queries::lock_task_detail(task)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(crate::model::Error::OperationAborted)?;
    if let Some(storage) = detail["storage_id"].as_str().map(str::to_owned) {
        let saved = &mut detail["report"]["storages"][storage];
        for (key, value) in report.as_object().context("invalid maintenance report")? {
            if let Some(count) = value.as_i64() {
                saved[key] = serde_json::json!(
                    saved[key]
                        .as_i64()
                        .unwrap_or(0)
                        .checked_add(count)
                        .context("maintenance report overflow")?
                );
            }
        }
    } else {
        detail["report"]["history"] = report;
    }
    let done = !again && advance_storage_scope(&mut detail)?;
    queries::advance_storage_task(task.id, detail, count, done)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    if !again && !done {
        #[cfg(feature = "fault-injection")]
        crate::faults::point("lifecycle-storage-advanced").await;
    }
    Ok(())
}
pub(super) async fn create_maintenance_preview(
    backend: &super::Backend,
    principal: &Principal,
    action: &str,
    parameters: Value,
    impact: Value,
    confirmation: String,
) -> Result<MaintenancePreview> {
    ensure!(
        matches!(action, "unpack" | "sweep" | "quarantine" | "recheck"),
        "invalid maintenance preview action"
    );
    let _preview = backend.previews.clone().lock_owned().await;
    let pool = &backend.write;
    let actor = principal.user_id().ok_or_else(denied)?;
    let mut tx = administration_transaction(pool, principal).await?;
    queries::remove_maintenance_preview(actor, principal.token_id(), action)
        .execute(&mut *tx)
        .await?;
    let id = Uuid::new_v4();
    let fingerprint = blake3::hash(&serde_json::to_vec(&impact)?)
        .to_hex()
        .to_string();
    queries::create_maintenance_preview(
        id,
        actor,
        principal.token_id(),
        action,
        serde_json::json!({"input":parameters,"confirmation":confirmation,"impact":impact}),
        fingerprint,
    )
    .execute(&mut *tx)
    .await?;
    let expires_at = queries::maintenance_preview_expiry(id)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(MaintenancePreview {
        id,
        confirmation,
        expires_at: queries::datetime(expires_at)?,
        impact,
    })
}
struct MaintenanceReceipt {
    _preview: tokio::sync::OwnedMutexGuard<()>,
    _authorization: Box<dyn crate::model::AdminGuard>,
    write: SqlitePool,
    id: Uuid,
    action: String,
    recorded: Option<Uuid>,
}
#[async_trait::async_trait]
impl MaintenanceCompletion for MaintenanceReceipt {
    async fn finish(self: Box<Self>, task: Uuid, audit: bool) -> Result<()> {
        let mut tx = self.write.begin_with(queries::BEGIN_WRITE).await?;
        if self.recorded != Some(task) {
            queries::record_maintenance_preview(self.id, task)
                .execute(&mut *tx)
                .await?;
        }
        if audit {
            checkpoint_event(
                &mut tx,
                &format!("maintenance.{}", self.action),
                &task.to_string(),
                serde_json::json!({"preview_id":self.id}),
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }
}
pub(super) async fn maintenance_execution(
    backend: &super::Backend,
    principal: &Principal,
    id: Uuid,
    action: &str,
) -> Result<Option<MaintenanceExecution>> {
    let actor = principal.user_id().ok_or_else(denied)?;
    let preview = backend.previews.clone().lock_owned().await;
    let authorization = hold_admin(backend, principal).await?;
    let mut tx = backend.read.begin().await?;
    let Some((parameters, fingerprint, mut task)) =
        queries::lock_maintenance_preview(id, actor, principal.token_id(), action)
            .fetch_optional(&mut *tx)
            .await?
    else {
        return Ok(None);
    };
    let recorded = task;
    if task.is_none()
        && queries::maintenance_preview_task(id)
            .fetch_one(&mut *tx)
            .await?
    {
        task = Some(id);
    }
    tx.commit().await?;
    Ok(Some(MaintenanceExecution {
        parameters,
        fingerprint,
        task,
        completion: Box::new(MaintenanceReceipt {
            _preview: preview,
            _authorization: authorization,
            write: backend.write.clone(),
            id,
            action: action.to_owned(),
            recorded,
        }),
    }))
}
async fn try_cleanup_transaction(
    pool: &SqlitePool,
) -> Result<Option<Transaction<'static, Sqlite>>> {
    // Committing returns the pooled connection asynchronously; allow bounded progress.
    match tokio::time::timeout(
        std::time::Duration::from_millis(50),
        pool.begin_with(queries::BEGIN_WRITE),
    )
    .await
    {
        Ok(result) => Ok(Some(result?)),
        Err(_) => Ok(None),
    }
}

#[cfg(test)]
#[path = "../../../../tests/recovery/sqlite_cleanup.rs"]
mod cleanup_tests;

pub(super) async fn read_maintenance_controls(pool: &SqlitePool) -> Result<Vec<Value>> {
    let mut tx = pool.begin().await?;
    let rows = queries::maintenance_controls(&mut tx).await?;
    tx.commit().await?;
    Ok(rows)
}
pub(super) async fn gc_status(pool: &SqlitePool) -> Result<(Vec<(String, i64)>, bool, bool)> {
    let mut tx = pool.begin().await?;
    let value = queries::gc_status(&mut tx).await?;
    tx.commit().await?;
    Ok(value)
}

use crate::model::metrics::{CapacityPoint, StorageUsage, UsageSnapshot};
use serde_json::json;

pub(super) async fn collect_inventory(
    backend: &super::Backend,
    metrics: &crate::config::MetricsSettings,
    maintenance: &crate::config::Maintenance,
) -> Result<Value> {
    let deadline = tokio::time::Instant::now()
        + std::time::Duration::from_secs(crate::config::seconds(&metrics.query_timeout)?);
    let mut connection =
        super::connection::DeadlineConnection::acquire(&backend.read, deadline).await?;
    let result=tokio::time::timeout_at(deadline,async {

        let mut tx=connection.begin().await?;
        let as_of=chrono::Utc::now();
        let buckets=queries::inventory_buckets(&mut tx).await?;
        let mut chunks=queries::inventory_chunks().fetch_one(&mut *tx).await?;
        let physical=queries::inventory_physical(&mut tx).await?;
        for (field,states) in [("stored_bytes",&["ready","retired","deleting"][..]),("unconfirmed_bytes",&["preparing","uploading"][..])] {
            chunks[field]=json!(physical.as_array().unwrap().iter().filter(|p|states.contains(&p["state"].as_str().unwrap_or_default())).map(|p|p["stored_bytes"].as_i64().unwrap_or(0)).sum::<i64>());
        }
        let live=queries::inventory_live().fetch_one(&mut *tx).await?;
        let mut unreferenced=queries::inventory_unreferenced().fetch_one(&mut *tx).await?;
        let reclaimable=queries::inventory_reclaimable().fetch_one(&mut *tx).await?;
        unreferenced["stored_bytes"]=reclaimable["bytes"].clone();
        unreferenced["eligible_bytes"]=reclaimable["eligible"].clone();
        let tasks=queries::inventory_tasks().fetch_one(&mut *tx).await?;
        let uploads=queries::inventory_uploads().fetch_one(&mut *tx).await?;
        let cleanup=queries::inventory_cleanup(&mut tx,crate::config::seconds(&maintenance.task_retention)? as f64).await?;
        let database=queries::inventory_database().fetch_all(&mut *tx).await?;
        let usage=queries::collect_usage().fetch_all(&mut *tx).await?;
        tx.commit().await?;
        Ok::<_,anyhow::Error>((json!({"as_of":as_of,"collected_at":chrono::Utc::now(),"objects":buckets[0]["objects"],"logical_bytes":buckets[0]["logical_bytes"],"buckets":buckets.iter().skip(1).take(1000).collect::<Vec<_>>(),"buckets_truncated":buckets.len()>1001,"chunks":chunks,"live":live,"physical":physical,"unreferenced":unreferenced,"tasks":tasks,"uploads":uploads,"cleanup":cleanup,"database":database}),usage,as_of))
    }).await;
    let (value, rows, at) = match result {
        Ok(result) => {
            connection.release().await?;
            result?
        }
        Err(_) => return Err(crate::model::Error::QueryCancelled.into()),
    };
    let mut connection =
        super::connection::DeadlineConnection::acquire(&backend.write, deadline).await?;
    let result = tokio::time::timeout_at(deadline, async {
        let mut tx = connection.begin_with(queries::BEGIN_WRITE).await?;
        collect_usage(&mut tx, metrics, at, rows).await?;
        tx.commit().await?;
        anyhow::Ok(())
    })
    .await;
    match result {
        Ok(result) => {
            connection.release().await?;
            result?;
            Ok(value)
        }
        Err(_) => Err(crate::model::Error::QueryCancelled.into()),
    }
}
pub(super) async fn collect_storage(pool: &SqlitePool, storage: Uuid) -> Result<Value> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut connection = super::connection::DeadlineConnection::acquire(pool, deadline).await?;
    let result=tokio::time::timeout_at(deadline,async {

        let mut tx=connection.begin().await?;
        let as_of=chrono::Utc::now();
        let buckets=queries::storage_inventory_buckets(storage).fetch_one(&mut *tx).await?;
        let chunks=queries::storage_inventory_chunks(storage).fetch_one(&mut *tx).await?;
        let physical=queries::storage_inventory_physical(storage).fetch_one(&mut *tx).await?;
        tx.commit().await?;
        Ok(json!({"storage_id":storage,"as_of":as_of,"buckets":buckets.0,"objects":buckets.1,"logical_bytes":buckets.2,"chunks":chunks.0,"stored_bytes":chunks.1,"physical":{"objects":physical.0,"stored_bytes":physical.1}}))
    }).await;
    match result {
        Ok(result) => {
            connection.release().await?;
            result
        }
        Err(_) => Err(crate::model::Error::QueryCancelled.into()),
    }
}

pub(super) async fn read_usage(
    backend: &super::Backend,
    principal: &Principal,
    scope: &ScopeQuery,
    limit: usize,
) -> Result<UsageSnapshot> {
    ensure!((1..=256).contains(&limit), "invalid usage scope limit");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut write = false;
    loop {
        let pool = if write { &backend.write } else { &backend.read };
        let mut connection = super::connection::DeadlineConnection::acquire(pool, deadline).await?;
        let result = tokio::time::timeout_at(deadline, async {
            let mut tx = if write {
                connection.begin_with(queries::BEGIN_WRITE).await?
            } else {
                connection.begin().await?
            };
            let (ids, admin) = usage_scope(&mut tx, principal, scope).await?;
            let global = admin && scope.bucket.is_none() && scope.project.is_none();
            let key = if global {
                "global".to_owned()
            } else {
                blake3::hash(&serde_json::to_vec(&ids)?)
                    .to_hex()
                    .to_string()
            };
            let due = queries::usage_touch_due(&key)
                .fetch_optional(&mut *tx)
                .await?;
            if !write && due != Some(false) {
                tx.commit().await?;
                return anyhow::Ok(None);
            }
            let created = due.is_none();
            if created {
                queries::evict_usage_scopes(&mut tx, limit as i64 - 1).await?;
                queries::create_usage_scope(&key).execute(&mut *tx).await?;
                queries::usage_bucket_members(&mut tx, &key, &ids).await?;
            }
            if write {
                queries::touch_usage_scope(&key).execute(&mut *tx).await?;
            }
            let (as_of, data) = queries::usage_saved(&key)
                .fetch_optional(&mut *tx)
                .await?
                .unwrap_or_default();
            let history = queries::usage_history(&key).fetch_all(&mut *tx).await?;
            let history = history
                .into_iter()
                .map(|(at, data)| {
                    Ok(CapacityPoint {
                        at: queries::datetime(at)?,
                        usage: data.0,
                    })
                })
                .collect::<sqlx::Result<Vec<_>>>()?;
            tx.commit().await?;
            Ok(Some(UsageSnapshot {
                global,
                bucket_count: ids.len(),
                as_of: as_of.map(queries::datetime).transpose()?,
                usage: data.map(|v| v.0),
                history,
                created,
            }))
        })
        .await;
        let result = match result {
            Ok(result) => {
                connection.release().await?;
                result?
            }
            Err(_) => return Err(crate::model::Error::QueryCancelled.into()),
        };
        if let Some(snapshot) = result {
            return Ok(snapshot);
        }
        write = true;
    }
}

pub(super) async fn collect_usage(
    connection: &mut SqliteConnection,
    metrics: &crate::config::MetricsSettings,
    at: chrono::DateTime<chrono::Utc>,
    rows: Vec<(String, sqlx::types::Json<StorageUsage>)>,
) -> Result<()> {
    let retention = crate::config::seconds(&metrics.storage_retention)? as f64;
    queries::save_usage(connection, at, rows).await?;
    queries::expire_usage_scopes(retention)
        .execute(&mut *connection)
        .await?;
    queries::append_usage_history(crate::config::seconds(&metrics.storage_interval)? as f64)
        .execute(&mut *connection)
        .await?;
    queries::expire_usage_history(retention)
        .execute(connection)
        .await?;
    Ok(())
}

use crate::model::metrics::{AccessBatch, RuntimeHistory, RuntimePoint};

pub(super) async fn read_runtime(
    pool: &SqlitePool,
    principal: &Principal,
) -> Result<RuntimeHistory> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut connection = super::connection::DeadlineConnection::acquire(pool, deadline).await?;
    let result = tokio::time::timeout_at(deadline, async {
        let mut tx = connection.begin().await?;
        ensure!(
            queries::validate_identity(principal, &mut tx).await?,
            s3s::s3_error!(AccessDenied)
        );
        let history = queries::runtime_history().fetch_all(&mut *tx).await?;
        let work = queries::runtime_work().fetch_all(&mut *tx).await?;
        let (pending_entries, pending_bytes, oldest_pending, failed_uploads) =
            queries::runtime_pending().fetch_one(&mut *tx).await?;
        tx.commit().await?;
        Ok(RuntimeHistory {
            history: history.into_iter().map(|v| v.0).collect(),
            work,
            pending_entries,
            pending_bytes,
            oldest_pending: oldest_pending.map(queries::datetime).transpose()?,
            failed_uploads,
        })
    })
    .await;
    match result {
        Ok(result) => {
            connection.release().await?;
            result
        }
        Err(_) => Err(crate::model::Error::QueryCancelled.into()),
    }
}
pub(super) async fn sample_runtime(
    pool: &SqlitePool,
    point: RuntimePoint,
    retention: f64,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    queries::runtime_sample(point).execute(&mut *tx).await?;
    queries::runtime_expire(retention).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}
pub(super) async fn flush_access(
    pool: &SqlitePool,
    pending: AccessBatch,
    stale: f64,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    queries::flush_chunk_access(&mut tx, &pending).await?;
    queries::flush_pack_access(&mut tx, &pending).await?;
    queries::flush_member_access(&mut tx, &pending).await?;
    queries::access_coverage(pending.dropped, stale)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}
pub(super) async fn expire_access(pool: &SqlitePool, retention: f64, batch: i64) -> Result<()> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut connection = super::connection::DeadlineConnection::acquire(pool, deadline).await?;
    let result = tokio::time::timeout_at(deadline, async {
        let mut tx = connection.begin_with(queries::BEGIN_WRITE).await?;
        queries::expire_access(&mut tx, retention, batch).await?;
        tx.commit().await?;
        Ok(())
    })
    .await;
    match result {
        Ok(result) => {
            connection.release().await?;
            result
        }
        Err(_) => Err(crate::model::Error::QueryCancelled.into()),
    }
}

async fn require_sources(
    db: &mut sqlx::SqliteConnection,
    stream: Option<Id>,
    chunks: &[Id],
) -> Result<()> {
    use futures_util::TryStreamExt;
    // Lock all logical rows before any physical row; shared validation locks block quarantine.
    for phase in 0..3 {
        let mut query = queries::source_locks(stream, chunks, phase);
        let mut rows = query.build_query_scalar::<i64>().fetch(&mut *db);
        while rows.try_next().await?.is_some() {}
    }
    ensure!(
        !queries::unavailable_sources(stream, chunks)
            .build_query_scalar::<bool>()
            .fetch_one(&mut *db)
            .await?,
        crate::model::Error::SourceUnavailable
    );
    Ok(())
}

pub(super) async fn read_source_snapshot(
    pool: &sqlx::SqlitePool,
    scope: &crate::model::source::SourceScope,
) -> Result<crate::model::source::SourceSnapshot> {
    let mut tx = pool.begin().await?;
    let snapshot = queries::source_snapshot(&mut tx, scope, false).await?;
    tx.commit().await?;
    Ok(snapshot)
}

pub(super) async fn source_update(
    pool: &sqlx::SqlitePool,
    update: crate::model::source::SourceUpdate<'_>,
) -> Result<()> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let operation = update.operation;
    ensure!(
        queries::validate_identity(&operation.principal, &mut tx).await?,
        s3s::s3_error!(AccessDenied)
    );
    ensure!(
        queries::source_mode(&operation.mode)
            .fetch_one(&mut *tx)
            .await?,
        crate::model::Error::OperationAborted
    );
    lock_execution(&mut tx, update.execution).await?;
    let snapshot = queries::source_snapshot(&mut tx, &operation.snapshot.scope, true).await?;
    ensure!(
        snapshot.state == "ready" && snapshot.fingerprint()? == operation.snapshot.fingerprint()?,
        crate::model::Error::OperationAborted
    );
    ensure!(
        snapshot.quarantine_code.is_none() == operation.code.is_some(),
        crate::model::Error::OperationAborted
    );
    ensure!(
        operation
            .code
            .as_deref()
            .is_none_or(crate::model::source::quarantine_code),
        s3s::s3_error!(InvalidArgument)
    );
    if update.apply {
        ensure!(
            queries::change_source(&snapshot, operation.code.as_deref())?
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            crate::model::Error::OperationAborted
        );
        let action = if operation.code.is_some() {
            "source.quarantine"
        } else {
            "source.recheck"
        };
        let detail = serde_json::json!({"source":snapshot.summary()?,"revision":(snapshot.revision+1).to_string(),"code":operation.code,"requested_by":operation.principal.user_id(),"token_id":operation.principal.token_id(),"task_id":update.execution.id});
        queries::source_result(update.execution.id, serde_json::json!({"result":detail}))
            .execute(&mut *tx)
            .await?;
        queries::complete_task(update.execution.id)
            .execute(&mut *tx)
            .await?;
        queries::source_audit(
            &operation.principal,
            action,
            &snapshot.location().to_string(),
            detail,
        )?
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

async fn require_inputs(db: &mut sqlx::SqliteConnection, chunks: &[Id]) -> Result<()> {
    require_sources(db, None, chunks).await?;
    ensure!(
        !queries::quarantined_inputs(chunks)
            .build_query_scalar::<bool>()
            .fetch_one(db)
            .await?,
        crate::model::Error::SourceUnavailable
    );
    Ok(())
}

async fn lock_splice_sources(
    db: &mut sqlx::SqliteConnection,
    streams: &[Id],
    chunk: Id,
) -> Result<()> {
    use futures_util::TryStreamExt;
    for physical in [false, true] {
        let mut query = queries::splice_locks(streams, chunk, physical);
        let mut rows = query.build().fetch(&mut *db);
        while rows.try_next().await?.is_some() {}
    }
    require_sources(db, None, &[chunk]).await
}

async fn source_input_stamp(db: &mut sqlx::SqliteConnection, chunks: &[Id]) -> Result<Value> {
    let rows: Vec<(Id, Option<Id>, Option<i64>, Option<Id>, Option<i64>)> =
        queries::source_input_versions(chunks)
            .build_query_as()
            .fetch_all(db)
            .await?;
    Ok(Value::Object(
        rows.into_iter()
            .map(|(id, encoding, revision, pack, pack_revision)| {
                (
                    id.to_string(),
                    json!([encoding, revision, pack, pack_revision]),
                )
            })
            .collect(),
    ))
}
async fn record_source_inputs(
    db: &mut sqlx::SqliteConnection,
    task: Uuid,
    chunks: &[Id],
) -> Result<()> {
    require_inputs(db, chunks).await?;
    let stamp = source_input_stamp(db, chunks).await?;
    queries::save_source_inputs(task, &stamp)
        .execute(db)
        .await?;
    Ok(())
}
async fn check_source_inputs(
    db: &mut sqlx::SqliteConnection,
    task: Uuid,
    chunks: &[Id],
) -> Result<()> {
    require_inputs(db, chunks).await?;
    let stamp = source_input_stamp(db, chunks).await?;
    let saved = queries::saved_source_inputs(task).fetch_one(db).await?;
    ensure!(
        stamp
            .as_object()
            .unwrap()
            .iter()
            .all(|(id, value)| saved.get(id) == Some(value)),
        crate::model::Error::OperationAborted
    );
    Ok(())
}

async fn repair_writer(
    db: &mut sqlx::SqliteConnection,
    stream: Id,
    node: Option<&crate::model::recovery::RepairNode>,
) -> Result<()> {
    let authority = queries::write_authorization(stream)
        .fetch_one(&mut *db)
        .await?
        .0;
    lock_permit(&mut *db, &authority).await?;
    ensure!(
        node.is_none() && queries::repair_mode().fetch_one(&mut *db).await?,
        crate::model::Error::OperationAborted
    );
    let observed = queries::repair_owner(stream)
        .fetch_optional(&mut *db)
        .await?
        .ok_or(crate::model::Error::OperationAborted)?;
    let (bucket, stream_type, owner, token) = &observed;
    if let Some(owner) = *owner {
        let token = token.ok_or(crate::model::Error::OperationAborted)?;
        ensure!(
            queries::lock_owned_task(owner, token)
                .fetch_optional(&mut *db)
                .await?
                .is_some(),
            crate::model::Error::OperationAborted
        );
    }
    ensure!(
        queries::lock_bucket_uploads(*bucket)
            .fetch_one(&mut *db)
            .await?
            .0
            == "active",
        crate::model::Error::OperationAborted
    );
    if owner.is_none() && stream_type == "part" {
        // Upload ownership precedes stream locks, as in multipart publication.
        ensure!(
            queries::repair_upload(stream)
                .fetch_optional(&mut *db)
                .await?
                .is_some(),
            crate::model::Error::OperationAborted
        );
    }
    if owner.is_none() && stream_type == "part" {
        ensure!(
            queries::repair_part(stream)
                .fetch_optional(&mut *db)
                .await?
                == Some(stream),
            crate::model::Error::OperationAborted
        );
    }
    ensure!(
        queries::repair_stream(stream)
            .fetch_optional(&mut *db)
            .await?
            == Some(observed.clone()),
        crate::model::Error::OperationAborted
    );
    if owner.is_none() && stream_type == "object" {
        ensure!(
            queries::repair_object(stream)
                .fetch_optional(&mut *db)
                .await?
                == Some(stream),
            crate::model::Error::OperationAborted
        );
    }
    Ok(())
}

async fn repair_sources(
    db: &mut sqlx::SqliteConnection,
    chunk: &Chunk,
) -> Result<Vec<crate::model::source::SourceSnapshot>> {
    use crate::model::source::{SourceScope, SourceType};
    ensure!(
        !queries::repair_pending(chunk.id)
            .fetch_one(&mut *db)
            .await?,
        crate::model::Error::SourceUnavailable
    );
    let rows: Vec<(Id, Option<Id>, Option<i64>, Option<Id>, Option<i64>)> =
        queries::source_input_versions(&[chunk.id])
            .build_query_as()
            .fetch_all(&mut *db)
            .await?;
    ensure!(rows.len() == 1, crate::model::Error::OperationAborted);
    let mut sources = Vec::with_capacity(2);
    for (source, id) in [
        (SourceType::Chunk, rows[0].1),
        (SourceType::Pack, rows[0].3),
    ] {
        if let Some(id) = id {
            let scope = SourceScope {
                storage_id: chunk
                    .storage_id
                    .ok_or(crate::model::Error::OperationAborted)?,
                source,
                id: id.to_string(),
            };
            let snapshot = queries::source_snapshot(db, &scope, true).await?;
            ensure!(
                snapshot.state == "ready" && snapshot.quarantine_code.is_some(),
                crate::model::Error::SourceUnavailable
            );
            sources.push(snapshot);
        }
    }
    ensure!(!sources.is_empty(), crate::model::Error::SourceUnavailable);
    Ok(sources)
}

pub(super) async fn prepare_repair(
    pool: &sqlx::SqlitePool,
    input: ChunkCreate,
    node: Option<crate::model::recovery::RepairNode>,
) -> Result<crate::model::recovery::RepairAllocation> {
    use crate::model::recovery::{Repair, RepairAllocation};
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    repair_writer(&mut tx, input.stream, node.as_ref()).await?;
    let mut chunk = queries::reusable_chunk(&mut tx, &input)
        .await?
        .ok_or(crate::model::Error::OperationAborted)?;
    match require_sources(&mut tx, None, &[chunk.id]).await {
        Ok(()) => {
            tx.commit().await?;
            return Ok(RepairAllocation::Existing(chunk));
        }
        Err(error)
            if matches!(
                crate::model::error(&error),
                Some(crate::model::Error::SourceUnavailable)
            ) => {}
        Err(error) => return Err(error),
    }
    let sources = repair_sources(&mut tx, &chunk).await?;
    let revision = queries::repair_target(input.storage, input.key, &input.algorithm)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(crate::model::Error::OperationAborted)?;
    ensure!(
        sources
            .iter()
            .all(|source| source.storage_revision == revision),
        crate::model::Error::OperationAborted
    );
    chunk.location_id = Uuid::now_v7();
    chunk.algorithm = input.algorithm;
    chunk.block_key_id = input.key;
    chunk.stored_size = None;
    chunk.compressed = false;
    chunk.nonce = None;
    chunk.encoding_id = queries::insert_repair(&chunk, input.stream)
        .execute(&mut *tx)
        .await?
        .last_insert_rowid()
        .into();
    let repair = Repair {
        stream: input.stream,
        node,
        chunk,
        sources,
    };
    repair.validate()?;
    tx.commit().await?;
    Ok(RepairAllocation::Candidate(Box::new(repair)))
}

pub(super) async fn repair_metadata(
    pool: &sqlx::SqlitePool,
    repair: &crate::model::recovery::Repair,
) -> Result<()> {
    repair.validate()?;
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    repair_writer(&mut tx, repair.stream, repair.node.as_ref()).await?;
    let candidate = queries::repair_candidate(repair.chunk.encoding_id, repair.stream)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(crate::model::Error::OperationAborted)?;
    repair.check_candidate(&candidate, false)?;
    ensure!(
        queries::save_repair_metadata(&repair.chunk, repair.stream)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    queries::refresh_encoding_fingerprint(&mut tx, repair.chunk.encoding_id).await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn finish_repair(
    pool: &sqlx::SqlitePool,
    repair: &crate::model::recovery::Repair,
    offset: i64,
) -> Result<Chunk> {
    repair.validate()?;
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    repair_writer(&mut tx, repair.stream, repair.node.as_ref()).await?;
    let input = ChunkCreate {
        stream: repair.stream,
        storage: repair
            .chunk
            .storage_id
            .ok_or(crate::model::Error::OperationAborted)?,
        hash: repair.chunk.hash.as_slice().try_into()?,
        raw_size: repair.chunk.raw_size,
        algorithm: repair.chunk.algorithm.clone(),
        key: repair.chunk.block_key_id,
    };
    let current = queries::reusable_chunk(&mut tx, &input)
        .await?
        .ok_or(crate::model::Error::OperationAborted)?;
    ensure!(
        current.id == repair.chunk.id,
        crate::model::Error::OperationAborted
    );
    let healthy = match require_sources(&mut tx, None, &[current.id]).await {
        Ok(()) => true,
        Err(error)
            if matches!(
                crate::model::error(&error),
                Some(crate::model::Error::SourceUnavailable)
            ) =>
        {
            false
        }
        Err(error) => return Err(error),
    };
    let sources = if healthy {
        Vec::new()
    } else {
        repair_sources(&mut tx, &current).await?
    };
    let candidate = queries::repair_candidate(repair.chunk.encoding_id, repair.stream)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(crate::model::Error::OperationAborted)?;
    repair.check_candidate(&candidate, true)?;
    let result = if healthy {
        queries::discard_verified_repair(candidate.encoding_id, repair.stream)
            .execute(&mut *tx)
            .await?;
        current
    } else {
        ensure!(
            sources.len() == repair.sources.len(),
            crate::model::Error::OperationAborted
        );
        for (current, expected) in sources.iter().zip(&repair.sources) {
            ensure!(
                current.fingerprint()? == expected.fingerprint()?,
                crate::model::Error::OperationAborted
            );
        }
        let revision = queries::repair_target(input.storage, input.key, &input.algorithm)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(crate::model::Error::OperationAborted)?;
        ensure!(
            sources
                .iter()
                .all(|source| source.storage_revision == revision),
            crate::model::Error::OperationAborted
        );
        queries::retire_repair_sources(current.id)
            .execute(&mut *tx)
            .await?;
        ensure!(
            queries::publish_repair(candidate.encoding_id, repair.stream)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            crate::model::Error::OperationAborted
        );
        ensure!(
            queries::publish_repaired_chunk(&candidate)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            crate::model::Error::OperationAborted
        );
        let authority = queries::write_authorization(repair.stream)
            .fetch_one(&mut *tx)
            .await?
            .0;
        queries::source_audit(&authority.principal, "source.recover", &current.id.to_string(), serde_json::json!({"stream_id":repair.stream,"storage_id":input.storage,"chunk_id":current.id.to_string(),"encoding_id":candidate.encoding_id.to_string(),"sources":sources.iter().map(|source| source.summary()).collect::<Result<Vec<_>>>()?}))?.execute(&mut *tx).await?;
        candidate
    };
    queries::insert_chunk_extent(repair.stream, offset, result.raw_size, result.id)
        .execute(&mut *tx)
        .await?;
    queries::changed_references(&mut tx, &[result.id], true).await?;
    queries::pin_chunk(result.id, repair.stream)
        .execute(&mut *tx)
        .await?;
    queries::attach_pending_upload(result.id, repair.stream, offset)
        .execute(&mut *tx)
        .await?;
    queries::resize_writing_stream(repair.stream, offset + i64::from(result.raw_size))
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(result)
}

pub(super) async fn begin_raw(
    pool: &sqlx::SqlitePool,
    stream: Id,
    storage: Uuid,
    revision: i64,
) -> Result<(Id, Uuid)> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let (storage_type, unencoded, current, cdc, background) = queries::raw_target(stream, storage)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(crate::model::Error::OperationAborted)?;
    ensure!(
        matches!(storage_type.as_str(), "local" | "s3") && unencoded && !cdc && current == revision,
        crate::model::Error::OperationAborted
    );
    ensure!(
        queries::raw_prepare(stream)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    let location = Uuid::now_v7();
    let inserted = queries::raw_insert(
        location,
        stream,
        storage,
        revision,
        if storage_type == "local" {
            "local"
        } else {
            "shared_s3"
        },
    )
    .execute(&mut *tx)
    .await?;
    let id = Id::from(inserted.last_insert_rowid());
    if background {
        queries::raw_enqueue_processing(stream, id)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok((id, location))
}

pub(super) async fn finish_raw(
    pool: &sqlx::SqlitePool,
    stream: Id,
    source: Id,
    size: i64,
    digest: &[u8],
) -> Result<()> {
    ensure!(
        size >= 0 && digest.len() == 16,
        "invalid received raw source"
    );
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    ensure!(
        queries::raw_lock_stream(stream)
            .fetch_optional(&mut *tx)
            .await?
            .is_some(),
        crate::model::Error::OperationAborted
    );
    ensure!(
        queries::raw_finish(stream, source, size, digest)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    queries::raw_attach(stream, source, size)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

async fn validate_raw(db: &mut sqlx::SqliteConnection, stream: Id, size: i64) -> Result<()> {
    let sources = queries::raw_sources_lock(stream)
        .fetch_all(&mut *db)
        .await?;
    ensure!(!sources.is_empty(), "raw generation has no source");
    for source in sources {
        source.validate()?;
    }
    let (end, contiguous) = queries::raw_continuity(stream).fetch_one(db).await?;
    ensure!(
        end == size && contiguous,
        "cannot publish an incomplete raw generation"
    );
    Ok(())
}

pub(super) async fn deduplicate_raw(
    pool: &sqlx::SqlitePool,
    stream: Id,
    source: Id,
    hash: &[u8],
) -> Result<bool> {
    ensure!(hash.len() == 32, "invalid raw content hash");
    let received = queries::raw_source(source).fetch_one(pool).await?;
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    ensure!(
        queries::raw_lock_stream(stream)
            .fetch_optional(&mut *tx)
            .await?
            .is_some(),
        crate::model::Error::OperationAborted
    );
    let current = queries::raw_source(source).fetch_one(&mut *tx).await?;
    ensure!(current == received, crate::model::Error::SourceUnavailable);
    current.validate()?;
    validate_raw(&mut tx, stream, current.size).await?;
    let reused = share_raw(&mut tx, stream, &current, hash).await?;
    tx.commit().await?;
    Ok(reused)
}

async fn share_raw(
    db: &mut sqlx::SqliteConnection,
    stream: Id,
    current: &crate::model::raw::RawSource,
    hash: &[u8],
) -> Result<bool> {
    let duplicate = queries::raw_duplicate(current.storage_id, hash, current.size)
        .fetch_optional(&mut *db)
        .await?;
    let mut reused = false;
    if let Some(candidate) = duplicate {
        if candidate.id == current.id {
            return Ok(false);
        }
        if candidate.validate().is_ok() && candidate.medium == current.medium {
            ensure!(
                queries::raw_share(stream, current.id, candidate.id, current.size)
                    .execute(&mut *db)
                    .await?
                    .rows_affected()
                    == 1,
                crate::model::Error::OperationAborted
            );
            queries::raw_reference(candidate.id)
                .execute(&mut *db)
                .await?;
            queries::raw_touch_unreferenced(current.id)
                .execute(&mut *db)
                .await?;
            reused = true;
        } else {
            queries::raw_hash(candidate.id, None)
                .execute(&mut *db)
                .await?;
        }
    }
    if !reused {
        queries::raw_hash(current.id, Some(hash))
            .execute(&mut *db)
            .await?;
    }
    Ok(reused)
}

pub(super) async fn multipart_file(pool: &SqlitePool, upload: Uuid) -> Result<Uuid> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    ensure!(
        queries::lock_local_upload(upload)
            .fetch_optional(&mut *tx)
            .await?
            .as_deref()
            == Some("active"),
        crate::model::Error::OperationAborted
    );
    let id = if let Some(id) = queries::current_multipart_file(upload)
        .fetch_optional(&mut *tx)
        .await?
    {
        id
    } else {
        let id = Uuid::now_v7();
        ensure!(
            queries::insert_multipart_file(id, upload)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            crate::model::Error::OperationAborted
        );
        id
    };
    tx.commit().await?;
    Ok(id)
}

pub(super) async fn remap_multipart_file(
    pool: &SqlitePool,
    upload: Uuid,
    source: Id,
    ranges: &[crate::model::multipart::FileRemap],
) -> Result<()> {
    ensure!(
        !ranges.is_empty() && ranges.len() <= 64,
        "invalid multipart checkpoint batch"
    );
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    ensure!(
        queries::lock_local_upload(upload)
            .fetch_optional(&mut *tx)
            .await?
            .as_deref()
            == Some("completing"),
        crate::model::Error::OperationAborted
    );
    queries::insert_multipart_checkpoint(upload, source)
        .execute(&mut *tx)
        .await?;
    for range in ranges {
        ensure!(
            queries::remap_multipart_range(upload, source, range)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            crate::model::Error::OperationAborted
        );
    }
    let files: std::collections::BTreeSet<_> = ranges.iter().map(|range| range.file).collect();
    for file in files {
        queries::pending_multipart_reclaim(file)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(())
}

pub(super) async fn claim_raw_processing(
    pool: &sqlx::SqlitePool,
    single: bool,
    token: Uuid,
) -> Result<Option<crate::model::raw::RawProcessing>> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let Some(stream) = queries::raw_processing_due(single)
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(None);
    };
    queries::raw_processing_claim(stream, token, single)
        .execute(&mut *tx)
        .await?;
    let work = queries::raw_processing_record(stream)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(Some(work))
}

pub(super) async fn finish_raw_processing(
    pool: &sqlx::SqlitePool,
    work: &crate::model::raw::RawProcessing,
    source: &crate::model::raw::RawSource,
    hash: &[u8],
    lease: Option<Uuid>,
) -> Result<bool> {
    ensure!(hash.len() == 32, "invalid raw content hash");
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;

    let current_stream = queries::raw_processing_stream(work)
        .fetch_optional(&mut *tx)
        .await?;
    ensure!(
        queries::raw_processing_owned(work)
            .fetch_optional(&mut *tx)
            .await?
            == Some(source.id),
        crate::model::Error::OperationAborted
    );
    if current_stream.is_none() {
        queries::raw_processing_delete(work)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        return Ok(true);
    }
    let _ = lease;
    let current = queries::raw_source(source.id).fetch_one(&mut *tx).await?;
    ensure!(current == *source, crate::model::Error::SourceUnavailable);
    current.validate()?;
    validate_raw(&mut tx, work.stream, current.size).await?;
    if share_raw(&mut tx, work.stream, &current, hash).await? {
        queries::raw_processing_changed(work.stream)
            .execute(&mut *tx)
            .await?;
    }
    queries::raw_processing_delete(work)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(true)
}

pub(super) async fn reserve_nonce(
    pool: &SqlitePool,
    key: Uuid,
    type_tag: u8,
    units: u32,
) -> Result<(i64, i64)> {
    ensure!(matches!(type_tag, 0 | 1), "invalid nonce type");
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    let previous = queries::nonce_counter(key, type_tag)
        .fetch_optional(&mut *tx)
        .await?
        .context("active encryption key nonce counter is missing")?;
    let range = crate::model::key::nonce_reservation(previous, units)?;
    ensure!(
        queries::advance_nonce(key, type_tag, previous, range.1)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        "nonce reservation changed"
    );
    #[cfg(feature = "fault-injection")]
    crate::faults::point("nonce-before-commit").await;
    tx.commit().await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("nonce-after-commit").await;
    Ok(range)
}

use crate::model::key_replacement::{
    self, KeyReplacementInput, KeyReplacementPreview, KeyReplacementResult, ReplacementRow,
};

pub(super) async fn key_replacement_preview(
    pool: &SqlitePool,
    principal: &Principal,
    source: Uuid,
    input: KeyReplacementInput,
) -> Result<KeyReplacementPreview> {
    key_replacement::validate(&input, source)?;
    let actor = key_replacement::actor(principal)?;
    let mut tx = administration_transaction(pool, principal).await?;

    let storages = queries::replacement_storages(source)
        .fetch_all(&mut *tx)
        .await?;
    ensure!(
        !storages.is_empty(),
        s3s::s3_error!(InvalidRequest, "key has no associated storages")
    );
    ensure!(
        serde_json::to_vec(&storages)?.len() <= 131072,
        s3s::s3_error!(InvalidRequest, "associated storage list is too large")
    );
    let (source_name, mut algorithm) = queries::replacement_key(source)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    let id = Uuid::new_v4();
    let target = input.target.unwrap_or_else(Uuid::new_v4);
    let target_name = if input.target.is_some() {
        let (name, selected_algorithm) = queries::replacement_key(target)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| s3s::s3_error!(InvalidArgument, "target must be an active block key"))?;
        ensure!(
            queries::replacement_capacity(target)
                .fetch_one(&mut *tx)
                .await?,
            s3s::s3_error!(InvalidRequest, "target key cannot reserve more nonce IDs")
        );
        algorithm = selected_algorithm;
        name
    } else {
        input
            .name
            .unwrap_or_else(|| format!("key-{}", target.simple()))
            .trim()
            .to_owned()
    };
    for storage in &storages {
        queries::storage_profile_lock(storage.id)
            .fetch_one(&mut *tx)
            .await?
            .validate_key(Some(target))?;
    }
    let preview = KeyReplacementPreview {
        id,
        source,
        source_name,
        target,
        target_name,
        algorithm,
        create: input.target.is_none(),
        expires_at: Utc::now() + chrono::Duration::minutes(10),
        storages,
    };
    queries::insert_key_replacement(&actor, &preview)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(preview)
}

pub(super) async fn key_replacement(
    pool: &SqlitePool,
    principal: &Principal,
    id: Uuid,
) -> Result<ReplacementRow> {
    let actor = key_replacement::actor(principal)?;
    let mut tx = administration_transaction(pool, principal).await?;
    let row = queries::key_replacement(id, &actor)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    tx.commit().await?;
    Ok(row)
}

pub(super) async fn replace_keys(
    pool: &SqlitePool,
    principal: &Principal,
    id: Uuid,
    input: Option<ManagedKeyCreate>,
) -> Result<KeyReplacementResult> {
    let actor = key_replacement::actor(principal)?;
    let mut tx = administration_transaction(pool, principal).await?;

    let (preview, result) = queries::key_replacement(id, &actor)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    if let Some(result) = result {
        tx.commit().await?;
        return Ok(result.0);
    }
    let preview = preview.0;
    ensure!(
        preview.expires_at > Utc::now(),
        crate::model::Error::OperationAborted
    );
    let storages = queries::replacement_storages(preview.source)
        .fetch_all(&mut *tx)
        .await?;
    ensure!(
        storages == preview.storages,
        crate::model::Error::StorageProfileConflict
    );
    for storage in &storages {
        let profile = queries::storage_profile_lock(storage.id)
            .fetch_one(&mut *tx)
            .await?;
        ensure!(
            profile.revision == storage.revision,
            crate::model::Error::StorageProfileConflict
        );
        profile.validate_key(Some(preview.target))?;
    }
    ensure!(
        queries::replacement_key(preview.source)
            .fetch_optional(&mut *tx)
            .await?
            .is_some(),
        crate::model::Error::OperationAborted
    );
    if preview.create {
        let input = input.context("new key material is required")?;
        ensure!(
            input.id == preview.target
                && input.name == preview.target_name
                && input.algorithm == preview.algorithm
                && input.purpose == "block",
            s3s::s3_error!(InvalidArgument)
        );
        queries::insert_managed_key(&input)
            .execute(&mut *tx)
            .await?;
        queries::initialize_nonce_counters(input.id)
            .execute(&mut *tx)
            .await?;
    } else {
        let target = queries::replacement_key(preview.target)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(crate::model::Error::OperationAborted)?;
        ensure!(
            target == (preview.target_name.clone(), preview.algorithm.clone())
                && queries::replacement_capacity(preview.target)
                    .fetch_one(&mut *tx)
                    .await?,
            crate::model::Error::OperationAborted
        );
    }
    for storage in &storages {
        queries::update_storage_key(storage.id, Some(preview.target))
            .execute(&mut *tx)
            .await?;
        queries::bump_storage_profile(storage.id)
            .execute(&mut *tx)
            .await?;
    }
    let result = KeyReplacementResult {
        preview,
        committed_at: Utc::now(),
    };
    queries::complete_key_replacement(&result)
        .execute(&mut *tx)
        .await?;
    checkpoint_event(&mut tx, "managed_key.replace", &id.to_string(), serde_json::json!({"source":result.preview.source,"target":result.preview.target,"created":result.preview.create,"storages":result.preview.storages})).await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("key-replacement-before-commit").await;
    tx.commit().await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("key-replacement-after-commit").await;
    Ok(result)
}

pub(super) async fn reuse_raw(
    pool: &sqlx::SqlitePool,
    stream: Id,
    source: Id,
    storage: Uuid,
    size: i64,
    digest: &[u8],
    hash: &[u8],
) -> Result<bool> {
    ensure!(
        size >= 0 && digest.len() == 16 && hash.len() == 32,
        "invalid received raw identity"
    );
    if queries::raw_reusable(storage, hash, size, digest)
        .fetch_optional(pool)
        .await?
        .is_none()
    {
        return Ok(false);
    }
    #[cfg(feature = "fault-injection")]
    crate::faults::point("raw-candidate-found").await;
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;

    ensure!(
        queries::raw_lock_stream(stream)
            .fetch_optional(&mut *tx)
            .await?
            .is_some(),
        crate::model::Error::OperationAborted
    );
    ensure!(
        queries::raw_receiving(source, stream)
            .fetch_optional(&mut *tx)
            .await?
            == Some(storage),
        crate::model::Error::OperationAborted
    );
    let candidate = queries::raw_duplicate(storage, hash, size)
        .fetch_optional(&mut *tx)
        .await?;
    let Some(candidate) = candidate.filter(|candidate| {
        candidate.validate().is_ok()
            && candidate.medium == "shared_s3"
            && candidate.digest.as_deref() == Some(digest)
    }) else {
        tx.commit().await?;
        return Ok(false);
    };
    queries::raw_attach(stream, candidate.id, size)
        .execute(&mut *tx)
        .await?;
    queries::raw_reference(candidate.id)
        .execute(&mut *tx)
        .await?;
    ensure!(
        queries::raw_discard(source, stream)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    tx.commit().await?;
    Ok(true)
}
