//! SQLite statements and native row decoding.
use crate::model::Id;
use sqlx::{
    Sqlite,
    query::{Query, QueryScalar},
    sqlite::SqliteArguments,
};

type Statement<'q> = Query<'q, Sqlite, SqliteArguments>;
type Scalar<'q, T> = QueryScalar<'q, Sqlite, T, SqliteArguments>;
type Rows<'q, T> = sqlx::query::QueryAs<'q, Sqlite, T, SqliteArguments>;

pub(super) const BEGIN_WRITE: &str = "BEGIN IMMEDIATE";
pub(super) const SCHEMA: &str = include_str!("../../../../migrations/schema/sqlite.sql");

pub(super) fn schema_empty() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT NOT EXISTS(SELECT 1 FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*')",
    )
}

pub(super) fn initialize_meta(version: i64) -> Statement<'static> {
    sqlx::query("INSERT INTO mokyu_meta(id,schema_version) VALUES(1,?)").bind(version)
}

pub(super) fn version() -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT sqlite_version()")
}
pub(super) fn journal_mode() -> Scalar<'static, String> {
    sqlx::query_scalar("PRAGMA journal_mode")
}
#[cfg(test)]
pub(super) fn tables() -> Scalar<'static, String> {
    sqlx::query_scalar(
        "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT GLOB 'sqlite_*' ORDER BY name",
    )
}
pub(super) fn migrations() -> Rows<'static, (i64, bool, Vec<u8>)> {
    sqlx::query_as("SELECT version,success,checksum FROM _sqlx_migrations ORDER BY version")
}
pub(super) fn schema_version() -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT schema_version FROM mokyu_meta WHERE id=1")
}
pub(super) fn fingerprint() -> Scalar<'static, Option<Vec<u8>>> {
    sqlx::query_scalar("SELECT deployment_key_fingerprint FROM mokyu_meta WHERE id=1")
}
pub(super) fn set_fingerprint(value: &[u8]) -> Statement<'_> {
    sqlx::query("UPDATE mokyu_meta SET deployment_key_fingerprint=? WHERE id=1 AND deployment_key_fingerprint IS NULL").bind(value)
}

use super::{Backend, transactions};
use crate::model::account::{
    AccountStore, AuthSession, Preferences, Profile, RefreshedSession, Session, SessionLogin,
    SessionRefresh, SessionTimes, TokenIdentity, VerifiedAccount,
};
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{Row, sqlite::SqliteRow};
use uuid::Uuid;

pub(super) fn datetime(micros: i64) -> sqlx::Result<DateTime<Utc>> {
    DateTime::from_timestamp_micros(micros)
        .ok_or_else(|| sqlx::Error::Decode("invalid SQLite timestamp".into()))
}
fn date(row: &SqliteRow, name: &str) -> sqlx::Result<DateTime<Utc>> {
    datetime(row.try_get(name)?)
}
pub(super) fn security_audit<'q>(
    user: Uuid,
    action: &'q str,
    success: bool,
    request: &'q str,
) -> Statement<'q> {
    sqlx::query("INSERT INTO audit_events(actor_id,actor_label,source,action,target,outcome,request_id,finished_at,detail) SELECT id,username,'web',?,?,?,?,CAST(unixepoch('subsec')*1000000 AS INTEGER),? FROM web_users WHERE id=? AND (? OR NOT EXISTS(SELECT 1 FROM audit_events WHERE actor_id=? AND action=? AND outcome='failed' AND created_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)-60000000))")
        .bind(action).bind(user.to_string()).bind(if success { "succeeded" } else { "failed" }).bind(request)
        .bind(serde_json::json!({"identity_verified":success})).bind(user).bind(success).bind(user).bind(action)
}

pub(super) fn checkpoint_audit<'q>(
    id: Id,
    action: &'q str,
    target: &'q str,
    project: Option<Uuid>,
    bucket: Option<Uuid>,
    detail: Value,
) -> Statement<'q> {
    sqlx::query("UPDATE audit_events SET action=?,target=?,project_id=?,bucket_id=?,detail=?,outcome='succeeded',finished_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=?")
        .bind(action).bind(target).bind(project).bind(bucket).bind(detail).bind(id)
}

pub(super) struct RefreshToken {
    pub(super) id: Uuid,
    pub(super) session_id: Uuid,
    pub(super) family_id: Uuid,
    pub(super) expires_at: DateTime<Utc>,
    pub(super) revoked_at: Option<DateTime<Utc>>,
    pub(super) replaced_by: Option<Uuid>,
}

pub(super) fn insert_refresh<'q>(
    id: Uuid,
    session: Uuid,
    family: Uuid,
    hash: &'q [u8],
) -> Statement<'q> {
    sqlx::query("INSERT INTO session_refresh_tokens(id,session_id,family_id,token_hash,expires_at) VALUES(?,?,?,?,CAST(unixepoch('subsec')*1000000 AS INTEGER)+2592000000000)")
        .bind(id).bind(session).bind(family).bind(hash)
}

pub(super) fn revoke_refreshes(session: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE session_refresh_tokens SET revoked_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE session_id=? AND revoked_at IS NULL").bind(session)
}

pub(super) fn recent_session(session: Uuid, user: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM sessions WHERE id=? AND user_id=? AND reauthenticated_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)-300000000)").bind(session).bind(user)
}

pub(super) fn authenticate_token(hash: &[u8]) -> Rows<'_, TokenAccess> {
    sqlx::query_as("SELECT t.id,t.user_id,u.role,t.system_access,t.authorization_revision,u.authorization_revision,t.last_used_at FROM api_tokens t JOIN web_users u ON u.id=t.user_id WHERE t.token_hash=? AND t.revoked_at IS NULL AND (t.expires_at IS NULL OR t.expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)) AND t.auth_revision=u.auth_revision AND u.enabled AND NOT u.must_change_password").bind(hash)
}

pub(super) fn touch_api_token(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE api_tokens SET last_used_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND (last_used_at IS NULL OR last_used_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-300000000)").bind(id)
}

pub(super) fn authenticate_session(hash: &[u8]) -> Rows<'_, AuthSession> {
    sqlx::query_as("SELECT u.id AS user_id,s.id AS session_id,u.role,u.must_change_password,u.authorization_revision,s.csrf_hash,s.last_seen_at FROM sessions s JOIN web_users u ON u.id=s.user_id WHERE s.token_hash=? AND s.expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER) AND u.enabled AND s.auth_revision=u.auth_revision").bind(hash)
}

pub(super) fn touch_session(hash: &[u8]) -> Statement<'_> {
    sqlx::query("UPDATE sessions SET last_seen_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE token_hash=? AND last_seen_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-300000000").bind(hash)
}

pub(super) fn login_user(username: &str) -> Rows<'_, VerifiedAccount> {
    sqlx::query_as(
        "SELECT id,password_hash,auth_revision FROM web_users WHERE username=? AND enabled",
    )
    .bind(username)
}

pub(super) fn lock_login_user(user: Uuid) -> Rows<'static, (String, bool, i64)> {
    sqlx::query_as("SELECT password_hash,enabled,auth_revision FROM web_users WHERE id=?")
        .bind(user)
}

pub(super) fn insert_session<'q>(id: Uuid, input: &'q SessionLogin<'q>) -> Statement<'q> {
    sqlx::query("INSERT INTO sessions(id,token_hash,user_id,csrf_hash,expires_at,auth_revision,user_agent) VALUES(?,?,?,?,CAST(unixepoch('subsec')*1000000 AS INTEGER)+?*1000000,?,?)")
        .bind(id).bind(input.access_hash).bind(input.verified.0).bind(input.csrf_hash).bind(input.seconds as i64).bind(input.verified.2).bind(input.user_agent)
}

pub(super) fn delete_access_session(hash: &[u8]) -> Statement<'_> {
    sqlx::query("DELETE FROM sessions WHERE token_hash=?").bind(hash)
}

pub(super) fn refresh_user(hash: &[u8]) -> Scalar<'_, Uuid> {
    sqlx::query_scalar("SELECT s.user_id FROM session_refresh_tokens r JOIN sessions s ON s.id=r.session_id WHERE r.token_hash=?").bind(hash)
}

pub(super) fn lock_refresh_user(user: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT auth_revision FROM web_users WHERE id=? AND enabled").bind(user)
}

pub(super) fn lock_refresh(hash: &[u8]) -> Rows<'_, RefreshToken> {
    sqlx::query_as("SELECT id,session_id,family_id,expires_at,revoked_at,replaced_by FROM session_refresh_tokens WHERE token_hash=?").bind(hash)
}

pub(super) fn lock_refresh_successor(id: Option<Uuid>) -> Rows<'static, RefreshToken> {
    sqlx::query_as("SELECT id,session_id,family_id,expires_at,revoked_at,replaced_by FROM session_refresh_tokens WHERE id=? AND revoked_at IS NULL AND expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)").bind(id)
}

pub(super) fn active_refresh_family(session: Uuid, family: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM session_refresh_tokens WHERE session_id=? AND family_id=? AND revoked_at IS NULL)").bind(session).bind(family)
}

pub(super) fn delete_refresh_session(session: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM sessions WHERE id=?").bind(session)
}

pub(super) fn lock_refresh_session(
    session: Uuid,
    user: Uuid,
    revision: i64,
) -> Scalar<'static, i64> {
    sqlx::query_scalar(
        "SELECT expires_at FROM sessions WHERE id=? AND user_id=? AND auth_revision=?",
    )
    .bind(session)
    .bind(user)
    .bind(revision)
}

pub(super) fn revoke_refresh(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE session_refresh_tokens SET revoked_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=?")
        .bind(id)
}

pub(super) fn replace_refresh(id: Uuid, replacement: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE session_refresh_tokens SET replaced_by=? WHERE id=?")
        .bind(replacement)
        .bind(id)
}

pub(super) fn refresh_session<'q>(
    session: Uuid,
    access: &'q [u8],
    csrf: &'q [u8],
    expires: DateTime<Utc>,
) -> Statement<'q> {
    sqlx::query("UPDATE sessions SET token_hash=?,csrf_hash=?,expires_at=?,last_seen_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=?").bind(access).bind(csrf).bind(expires.timestamp_micros()).bind(session)
}

pub(super) fn touch_refresh(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE session_refresh_tokens SET last_used_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=?")
        .bind(id)
}

pub(super) fn session_times(hash: &[u8]) -> Rows<'_, (Uuid, i64, i64)> {
    sqlx::query_as("SELECT id,expires_at,reauthenticated_at FROM sessions WHERE token_hash=?")
        .bind(hash)
}

pub(super) fn account_profile(user: Uuid) -> Rows<'static, Profile> {
    sqlx::query_as("SELECT u.id,u.username,u.role,u.must_change_password,m.project_management,u.display_name,u.locale,u.theme,u.avatar_email,u.avatar_enabled FROM web_users u CROSS JOIN mokyu_meta m WHERE u.id=? AND u.enabled").bind(user)
}

pub(super) fn account_preferences(user: Uuid, input: &Preferences) -> Statement<'_> {
    sqlx::query("UPDATE web_users SET display_name=?,locale=?,theme=?,avatar_email=?,avatar_enabled=? WHERE id=? AND enabled")
        .bind(input.display_name.trim()).bind(input.locale.as_deref()).bind(input.theme.as_deref()).bind(input.avatar_email.trim().to_lowercase()).bind(input.avatar_enabled).bind(user)
}

pub(super) fn account_password(user: Uuid) -> Rows<'static, (String, i64)> {
    sqlx::query_as("SELECT password_hash,auth_revision FROM web_users WHERE id=? AND enabled")
        .bind(user)
}

pub(super) fn lock_session_user(user: Uuid) -> Rows<'static, (String, i64, bool)> {
    sqlx::query_as("SELECT password_hash,auth_revision,enabled FROM web_users WHERE id=?")
        .bind(user)
}

pub(super) fn active_session(user: Uuid, hash: &[u8], revision: i64) -> Scalar<'_, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM sessions WHERE user_id=? AND token_hash=? AND expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER) AND auth_revision=?)").bind(user).bind(hash).bind(revision)
}

pub(super) fn change_password(user: Uuid, hash: &str) -> Statement<'_> {
    sqlx::query("UPDATE web_users SET password_hash=?,auth_revision=auth_revision+1,authorization_revision=authorization_revision+1,must_change_password=FALSE WHERE id=?").bind(hash).bind(user)
}

pub(super) fn delete_other_sessions(user: Uuid, hash: &[u8]) -> Statement<'_> {
    sqlx::query("DELETE FROM sessions WHERE user_id=? AND token_hash<>?")
        .bind(user)
        .bind(hash)
}

pub(super) fn session_revision(hash: &[u8], revision: i64) -> Statement<'_> {
    sqlx::query("UPDATE sessions SET auth_revision=?,reauthenticated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE token_hash=?").bind(revision).bind(hash)
}

pub(super) fn session_id(hash: &[u8]) -> Scalar<'_, Uuid> {
    sqlx::query_scalar("SELECT id FROM sessions WHERE token_hash=?").bind(hash)
}

pub(super) fn reauthenticate_session(hash: &[u8]) -> Statement<'_> {
    sqlx::query("UPDATE sessions SET reauthenticated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE token_hash=?")
        .bind(hash)
}

pub(super) fn account_sessions(user: Uuid, hash: &[u8]) -> Rows<'_, Session> {
    sqlx::query_as("SELECT id,created_at,last_seen_at,expires_at,user_agent,(token_hash=?) AS current FROM sessions s WHERE user_id=? AND (expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER) OR EXISTS(SELECT 1 FROM session_refresh_tokens r WHERE r.session_id=s.id AND r.revoked_at IS NULL AND r.expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER))) ORDER BY created_at DESC,id DESC LIMIT 101").bind(hash).bind(user)
}

pub(super) fn delete_account_session(session: Uuid, user: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM sessions WHERE id=? AND user_id=?")
        .bind(session)
        .bind(user)
}

pub(super) fn has_accounts() -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM web_users)")
}

pub(super) fn delete_setup_tokens() -> Statement<'static> {
    sqlx::query("DELETE FROM setup_tokens")
}

pub(super) fn save_setup_token(hash: &[u8]) -> Statement<'_> {
    sqlx::query("INSERT INTO setup_tokens(id,token_hash,expires_at) VALUES(1,?,CAST(unixepoch('subsec')*1000000 AS INTEGER)+3600000000) ON CONFLICT(id) DO UPDATE SET token_hash=excluded.token_hash,created_at=CAST(unixepoch('subsec')*1000000 AS INTEGER),expires_at=excluded.expires_at").bind(hash)
}

pub(super) fn insert_initial_admin<'q>(
    id: Uuid,
    username: &'q str,
    password: &'q str,
    require_change: bool,
) -> Statement<'q> {
    sqlx::query("INSERT INTO web_users(id,username,password_hash,role,must_change_password) VALUES(?,?,?,'admin',?)").bind(id).bind(username).bind(password).bind(require_change)
}

pub(super) fn setup_required() -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM web_users)")
}

pub(super) fn setup_hash() -> Scalar<'static, Vec<u8>> {
    sqlx::query_scalar(
        "SELECT token_hash FROM setup_tokens WHERE id=1 AND expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)",
    )
}

pub(super) fn setup_allowed(hash: &[u8]) -> Scalar<'_, bool> {
    sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM web_users) AND EXISTS(SELECT 1 FROM setup_tokens WHERE id=1 AND token_hash=? AND expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER))").bind(hash)
}

pub(super) fn insert_setup_admin<'q>(
    id: Uuid,
    username: &'q str,
    password: &'q str,
) -> Statement<'q> {
    insert_initial_admin(id, username, password, false)
}

#[async_trait::async_trait]
impl AccountStore for Backend {
    async fn recent(&self, session: Uuid, user: Uuid) -> Result<bool> {
        Ok(recent_session(session, user).fetch_one(&self.read).await?)
    }
    async fn authenticate_token(&self, hash: &[u8]) -> Result<Option<TokenIdentity>> {
        let Some(row) = authenticate_token(hash).fetch_optional(&self.read).await? else {
            return Ok(None);
        };
        if row
            .last_used
            .is_none_or(|at| Utc::now().timestamp_micros().saturating_sub(at) >= 300_000_000)
        {
            touch_api_token(row.identity.0).execute(&self.write).await?;
        }
        Ok(Some(row.identity))
    }
    async fn authenticate_session(&self, hash: &[u8]) -> Result<Option<AuthSession>> {
        let row = authenticate_session(hash)
            .fetch_optional(&self.read)
            .await?;
        if row
            .as_ref()
            .is_some_and(|row| (Utc::now() - row.last_seen_at).num_seconds() >= 300)
        {
            touch_session(hash).execute(&self.write).await?;
        }
        Ok(row)
    }
    async fn login_user(&self, username: &str) -> Result<Option<VerifiedAccount>> {
        Ok(login_user(username).fetch_optional(&self.read).await?)
    }
    async fn password(&self, user: Uuid) -> Result<Option<(String, i64)>> {
        Ok(account_password(user).fetch_optional(&self.read).await?)
    }
    async fn login(&self, input: SessionLogin<'_>) -> Result<()> {
        transactions::login_session(&self.write, input).await
    }
    async fn failed_login(&self, user: Uuid, request_id: &str) -> Result<()> {
        transactions::failed_login(&self.write, user, request_id).await
    }
    async fn refresh(&self, input: SessionRefresh<'_>) -> Result<RefreshedSession> {
        let _authorization = self.authorization.write().await;
        transactions::refresh_session(&self.write, input).await
    }
    async fn session(&self, hash: &[u8]) -> Result<Option<SessionTimes>> {
        session_times(hash)
            .fetch_optional(&self.read)
            .await?
            .map(|(id, expires, recent)| Ok((id, datetime(expires)?, datetime(recent)?)))
            .transpose()
    }
    async fn logout(&self, user: Uuid, hash: &[u8]) -> Result<()> {
        let _authorization = self.authorization.write().await;
        transactions::logout_session(&self.write, user, hash).await
    }
    async fn profile(&self, user: Uuid) -> Result<Option<Profile>> {
        Ok(account_profile(user).fetch_optional(&self.read).await?)
    }
    async fn preferences(&self, user: Uuid, hash: &[u8], input: Preferences) -> Result<()> {
        transactions::account_preferences(&self.write, user, hash, input).await
    }
    async fn change_password(
        &self,
        verified: &VerifiedAccount,
        current: &[u8],
        password: &str,
        refresh: &[u8],
    ) -> Result<()> {
        let _authorization = self.authorization.write().await;
        transactions::account_password(&self.write, verified, current, password, refresh).await
    }
    async fn reauthenticate(
        &self,
        verified: &VerifiedAccount,
        current: &[u8],
        refresh: &[u8],
    ) -> Result<()> {
        let _authorization = self.authorization.write().await;
        transactions::reauthenticate_account(&self.write, verified, current, refresh).await
    }
    async fn sessions(&self, user: Uuid, current: &[u8]) -> Result<Vec<Session>> {
        Ok(account_sessions(user, current)
            .fetch_all(&self.read)
            .await?)
    }
    async fn revoke_sessions(
        &self,
        user: Uuid,
        current: &[u8],
        session: Option<Uuid>,
    ) -> Result<()> {
        let _authorization = self.authorization.write().await;
        transactions::revoke_account_sessions(&self.write, user, current, session).await
    }
    async fn initialize(&self) -> Result<()> {
        if has_accounts().fetch_one(&self.read).await? {
            delete_setup_tokens().execute(&self.write).await?;
        }
        Ok(())
    }
    async fn setup_token(&self, hash: &[u8]) -> Result<()> {
        transactions::account_setup_token(&self.write, hash).await
    }
    async fn initialize_admin(
        &self,
        username: &str,
        password: &str,
        require_change: bool,
    ) -> Result<Uuid> {
        transactions::initialize_admin(&self.write, username, password, require_change).await
    }
    async fn setup_required(&self) -> Result<bool> {
        Ok(setup_required().fetch_one(&self.read).await?)
    }
    async fn setup_hash(&self) -> Result<Option<Vec<u8>>> {
        Ok(setup_hash().fetch_optional(&self.read).await?)
    }
    async fn setup(
        &self,
        username: &str,
        password: &str,
        supplied: &[u8],
        request_id: &str,
    ) -> Result<()> {
        transactions::setup_account(&self.write, username, password, supplied, request_id).await
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::sqlite::SqliteRow> for crate::model::account::Profile {
    fn from_row(row: &'r sqlx::sqlite::SqliteRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            username: row.try_get("username")?,
            role: row.try_get("role")?,
            must_change_password: row.try_get("must_change_password")?,
            project_management: row.try_get("project_management")?,
            display_name: row.try_get("display_name")?,
            locale: row.try_get("locale")?,
            theme: row.try_get("theme")?,
            avatar_email: row.try_get("avatar_email")?,
            avatar_enabled: row.try_get("avatar_enabled")?,
            avatar_url: Default::default(),
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::sqlite::SqliteRow> for crate::model::account::Session {
    fn from_row(row: &'r sqlx::sqlite::SqliteRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            created_at: date(row, "created_at")?,
            last_seen_at: date(row, "last_seen_at")?,
            expires_at: date(row, "expires_at")?,
            user_agent: row.try_get("user_agent")?,
            current: row.try_get("current")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::sqlite::SqliteRow> for crate::model::account::AuthSession {
    fn from_row(row: &'r sqlx::sqlite::SqliteRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            user_id: row.try_get("user_id")?,
            session_id: row.try_get("session_id")?,
            role: row.try_get("role")?,
            must_change_password: row.try_get("must_change_password")?,
            authorization_revision: row.try_get("authorization_revision")?,
            csrf_hash: row.try_get("csrf_hash")?,
            last_seen_at: date(row, "last_seen_at")?,
        })
    }
}

pub(super) struct TokenAccess {
    identity: TokenIdentity,
    last_used: Option<i64>,
}
impl<'r> sqlx::FromRow<'r, SqliteRow> for TokenAccess {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            identity: (
                row.try_get(0)?,
                row.try_get(1)?,
                row.try_get(2)?,
                row.try_get(3)?,
                row.try_get(4)?,
                row.try_get(5)?,
            ),
            last_used: row.try_get(6)?,
        })
    }
}
impl<'r> sqlx::FromRow<'r, SqliteRow> for RefreshToken {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            session_id: row.try_get("session_id")?,
            family_id: row.try_get("family_id")?,
            expires_at: date(row, "expires_at")?,
            revoked_at: row
                .try_get::<Option<i64>, _>("revoked_at")?
                .map(datetime)
                .transpose()?,
            replaced_by: row.try_get("replaced_by")?,
        })
    }
}

use crate::authorization::Principal;
use crate::model::settings::{ControlSettingsPatch, RuntimeSettingsRow, RuntimeSettingsStore};
use sqlx::SqliteConnection;
pub(super) fn user_identity(user: Uuid, session: Uuid) -> Rows<'static, (String, i64)> {
    sqlx::query_as("SELECT u.role,u.authorization_revision FROM web_users u JOIN sessions s ON s.user_id=u.id WHERE u.id=? AND s.id=? AND u.enabled AND NOT u.must_change_password AND s.expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER) AND s.auth_revision=u.auth_revision").bind(user).bind(session)
}

pub(super) fn token_owner_identity(user: Uuid) -> Rows<'static, (String, i64, i64)> {
    sqlx::query_as("SELECT role,authorization_revision,auth_revision FROM web_users WHERE id=? AND enabled AND NOT must_change_password").bind(user)
}

pub(super) fn token_identity(token: Uuid, user: Uuid, auth: i64) -> Rows<'static, (bool, i64)> {
    sqlx::query_as("SELECT system_access,authorization_revision FROM api_tokens WHERE id=? AND user_id=? AND auth_revision=? AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER))").bind(token).bind(user).bind(auth)
}

pub(super) async fn validate_identity(
    principal: &Principal,
    connection: &mut SqliteConnection,
) -> Result<bool> {
    let denied = || s3s::s3_error!(AccessDenied);
    match principal {
        Principal::System | Principal::Bootstrap | Principal::Local => Ok(true),
        Principal::Node { .. } => Err(denied().into()),
        Principal::Service {
            access_key,
            revision,
        } => {
            if service_identity(access_key)
                .fetch_optional(connection)
                .await?
                != Some(*revision)
            {
                return Err(denied().into());
            }
            Ok(false)
        }
        Principal::User {
            id,
            session,
            revision,
        } => {
            let (role, current) = user_identity(*id, *session)
                .fetch_optional(connection)
                .await?
                .ok_or_else(denied)?;
            if current != *revision {
                return Err(denied().into());
            }
            Ok(role == "admin")
        }
        Principal::Token {
            id,
            user,
            revision,
            user_revision,
        } => {
            let (role, current, auth) = token_owner_identity(*user)
                .fetch_optional(&mut *connection)
                .await?
                .ok_or_else(denied)?;
            if current != *user_revision {
                return Err(denied().into());
            }
            let (system, current) = token_identity(*id, *user, auth)
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

pub(super) fn ensure_runtime_setting<'q>(section: &'q str, value: &'q Value) -> Statement<'q> {
    sqlx::query(
        "INSERT INTO runtime_settings(section,value) VALUES(?,?) ON CONFLICT(section) DO NOTHING",
    )
    .bind(section)
    .bind(value)
}

pub(super) fn runtime_settings() -> Rows<'static, (String, Value, i64)> {
    sqlx::query_as("SELECT section,value,revision FROM runtime_settings ORDER BY section")
}

pub(super) fn runtime_setting(section: &str) -> Scalar<'_, Value> {
    sqlx::query_scalar("SELECT value FROM runtime_settings WHERE section=?").bind(section)
}

pub(super) fn runtime_settings_row(section: &str) -> Rows<'_, RuntimeSettingsRow> {
    sqlx::query_as("SELECT value,revision FROM runtime_settings WHERE section=?").bind(section)
}

pub(super) fn lock_runtime_settings(section: &str) -> Scalar<'_, i64> {
    sqlx::query_scalar("SELECT revision FROM runtime_settings WHERE section=?").bind(section)
}

pub(super) fn save_runtime_settings(
    section: &str,
    value: Value,
    is_default: bool,
    revision: i64,
) -> Statement<'_> {
    sqlx::query("UPDATE runtime_settings SET value=?,revision=revision+1,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER),is_default=? WHERE section=? AND revision=?").bind(value).bind(is_default).bind(section).bind(revision)
}

pub(super) fn reset_runtime_settings(
    section: &str,
    value: Value,
    is_default: bool,
) -> Statement<'_> {
    sqlx::query("UPDATE runtime_settings SET value=?,revision=revision+1,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER),is_default=? WHERE section=?").bind(value).bind(is_default).bind(section)
}

pub(super) fn lock_web_settings() -> Rows<'static, RuntimeSettingsRow> {
    sqlx::query_as("SELECT value,revision FROM runtime_settings WHERE section='web'")
}

pub(super) fn save_web_settings(
    value: Value,
    next: i64,
    user: Option<Uuid>,
    revision: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE runtime_settings SET value=?,revision=?,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER),is_default=? WHERE section='web' AND revision=?").bind(value).bind(next).bind(user.is_none()).bind(revision)
}

#[async_trait::async_trait]
impl RuntimeSettingsStore for Backend {
    async fn revisions(&self) -> Result<Vec<(String, i64)>> {
        let revisions: Vec<(String, i64)> =
            sqlx::query_as("SELECT section,revision FROM runtime_settings")
                .fetch_all(&self.read)
                .await?;
        Ok(revisions)
    }
    async fn inspect(&self) -> Result<Vec<(String, Value, i64, bool)>> {
        let runtime: Vec<(String, serde_json::Value, i64, bool)> = sqlx::query_as(
            "SELECT section,value,revision,is_default FROM runtime_settings ORDER BY section",
        )
        .fetch_all(&self.read)
        .await?;
        Ok(runtime)
    }

    async fn row(&self, section: &str) -> Result<RuntimeSettingsRow> {
        Ok(runtime_settings_row(section).fetch_one(&self.read).await?)
    }
    async fn sections(&self, principal: &Principal) -> Result<Vec<(String, Value, i64)>> {
        transactions::runtime_settings_sections(&self.read, principal).await
    }
    async fn section(&self, principal: &Principal, section: &str) -> Result<RuntimeSettingsRow> {
        transactions::runtime_settings_section(&self.read, principal, section).await
    }
    async fn save(
        &self,
        principal: &Principal,
        section: &str,
        revision: Option<i64>,
        value: Value,
    ) -> Result<RuntimeSettingsRow> {
        transactions::save_runtime_settings(&self.write, principal, section, revision, value).await
    }
    async fn reset(
        &self,
        principal: &Principal,
        section: &str,
        value: Value,
    ) -> Result<RuntimeSettingsRow> {
        transactions::reset_runtime_settings(&self.write, principal, section, value).await
    }
    async fn update_web(
        &self,
        principal: &Principal,
        input: ControlSettingsPatch,
    ) -> Result<(crate::config::WebSettings, i64)> {
        transactions::update_web_settings(&self.write, principal, input).await
    }

    async fn ensure(&self, defaults: &[(&str, Value)]) -> Result<()> {
        transactions::ensure_runtime_settings(&self.write, defaults).await
    }

    async fn load(&self) -> Result<Vec<(String, Value, i64)>> {
        Ok(runtime_settings().fetch_all(&self.read).await?)
    }

    async fn value(&self, section: &str) -> Result<Value> {
        Ok(runtime_setting(section).fetch_one(&self.read).await?)
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for RuntimeSettingsRow {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            value: row.try_get("value")?,
            revision: row.try_get("revision")?,
        })
    }
}

use crate::authorization::Action;

pub(super) fn service_identity(access: &str) -> Scalar<'_, i64> {
    sqlx::query_scalar("SELECT authorization_revision FROM s3_credentials WHERE access_key_id=? AND enabled AND (expires_at IS NULL OR expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER))").bind(access)
}
pub(super) fn action_bit(action: Action) -> i64 {
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

pub(super) fn action_bits(actions: &[Action]) -> i64 {
    actions
        .iter()
        .fold(0, |bits, action| bits | action_bit(*action))
}

pub(super) fn actions(bits: i64) -> Vec<Action> {
    Action::ALL
        .into_iter()
        .filter(|action| bits & action_bit(*action) != 0)
        .collect()
}

pub(super) async fn bucket_actions(
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

pub(super) fn permission_bucket(id: Uuid) -> Scalar<'static, Uuid> {
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

use crate::model::storage::{StorageCreation, StorageEdit, StorageRow, StorageView};
use crate::model::{StorageProfileStore, StorageRecord, StorageRecordStore};
use crate::storage::{model::Record as BackendRecord, profile::StorageProfile};

pub(super) fn storage_deployment() -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT deployment_id FROM mokyu_meta WHERE id=1")
}

pub(super) fn valid_storage_key(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM managed_keys WHERE id=? AND purpose='block' AND revoked_at IS NULL",
    )
    .bind(id)
}

pub(super) fn insert_storage_record(record: &BackendRecord, key: Option<Uuid>) -> Statement<'_> {
    sqlx::query("INSERT INTO storages(id,name,type,path,endpoint_url,remote_bucket_name,region,prefix,identity_marker,block_key_id,access_key_encrypted,secret_key_encrypted) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)").bind(record.id).bind(&record.name).bind(&record.r#type).bind(&record.path).bind(&record.endpoint_url).bind(&record.remote_bucket_name).bind(&record.region).bind(&record.prefix).bind(&record.identity_marker).bind(key).bind(&record.access_key_encrypted).bind(&record.secret_key_encrypted)
}

pub(super) fn insert_storage_profile(
    id: Uuid,
    read: Option<i32>,
    upload: Option<i32>,
    control: Option<i32>,
    remote: bool,
) -> Statement<'static> {
    sqlx::query("INSERT INTO storage_profiles(storage_id,read_concurrency,upload_concurrency,control_concurrency,cache_enabled,upload_cache,compression_enabled,pack_enabled,pack_range_enabled,min_free_space) VALUES(?,?,?,?,?,?,?,?,?,?)").bind(id).bind(read).bind(upload).bind(control).bind(remote).bind(remote).bind(remote).bind(remote).bind(remote).bind((!remote).then_some("1GiB"))
}

pub(super) fn update_storage_record(record: &BackendRecord, key: Option<Uuid>) -> Statement<'_> {
    sqlx::query("UPDATE storages SET type=?,path=?,endpoint_url=?,remote_bucket_name=?,region=?,prefix=?,block_key_id=?,access_key_encrypted=?,secret_key_encrypted=?,enabled=?,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=?").bind(&record.r#type).bind(&record.path).bind(&record.endpoint_url).bind(&record.remote_bucket_name).bind(&record.region).bind(&record.prefix).bind(key).bind(&record.access_key_encrypted).bind(&record.secret_key_encrypted).bind(record.enabled).bind(record.id)
}

pub(super) fn current_storage_key(id: Option<Uuid>, name: &str) -> Scalar<'_, Uuid> {
    sqlx::query_scalar("SELECT id FROM managed_keys WHERE (id=? OR name=?) AND purpose='block' AND revoked_at IS NULL").bind(id).bind(name)
}

pub(super) fn update_storage_key(id: Uuid, key: Option<Uuid>) -> Statement<'static> {
    sqlx::query("UPDATE storages SET block_key_id=?,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=?")
        .bind(key)
        .bind(id)
}

pub(super) fn bump_storage_profile(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE storage_profiles SET revision=revision+1,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE storage_id=?").bind(id)
}

pub(super) fn enabled_storages() -> Rows<'static, StorageRecord> {
    sqlx::query_as(
        "SELECT s.id,s.type AS store_type,s.path,s.endpoint_url AS endpoint,s.remote_bucket_name AS bucket,s.region,s.prefix,s.access_key_encrypted AS access,s.secret_key_encrypted AS secret,s.identity_marker,p.revision FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.enabled ORDER BY s.created_at,s.id",
    )
}

pub(super) fn enabled_storage(id: Uuid) -> Rows<'static, StorageRecord> {
    sqlx::query_as("SELECT s.id,s.type AS store_type,s.path,s.endpoint_url AS endpoint,s.remote_bucket_name AS bucket,s.region,s.prefix,s.access_key_encrypted AS access,s.secret_key_encrypted AS secret,s.identity_marker,p.revision FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.id=? AND s.enabled")
        .bind(id)
}

pub(super) fn storage_profile(id: Uuid) -> Rows<'static, crate::storage::profile::StorageProfile> {
    sqlx::query_as("SELECT storage_id,revision,cdc_enabled,deduplication_enabled,write_processing,connect_timeout,request_timeout,max_retries,retry_timeout,priority_aging,min_storage_duration,unreferenced_grace,deleted_chunk_retention,upload_retention,multipart_idle_timeout,min_free_space,cache_enabled,cache_soft_limit,cache_entries,upload_cache,upload_cache_size,compression_enabled,compression_strategy,compression_level,compression_sample_level,compression_min_savings_percent,compression_min_savings_bytes,pack_enabled,pack_max_size,pack_upload_flush_timeout,pack_interval,pack_reuse_interval,pack_reclaim_interval,pack_repack_interval,pack_repack_cooldown,pack_reclaim_min_savings_bytes,pack_range_enabled,pack_range_interval,pack_range_window,pack_range_min_downloads,pack_range_min_savings_percent,pack_range_min_savings_bytes,pack_range_repack_after,pack_range_repack_window,pack_range_repack_retry_interval,pack_concurrency,integrity_concurrency,integrity_rps,integrity_bandwidth,integrity_batch_timeout,read_concurrency,upload_concurrency,control_concurrency,pack_byte_budget,integrity_byte_budget,maintenance_concurrency,maintenance_byte_budget,migration_concurrency,migration_byte_budget FROM storage_profiles WHERE storage_id=?")
        .bind(id)
}

pub(super) fn storage_relocation_ready() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT maintenance AND NOT EXISTS(SELECT 1 FROM tasks WHERE state IN ('queued','running','paused')) AND NOT EXISTS(SELECT 1 FROM uploads WHERE state IN ('active','completing')) AND NOT EXISTS(SELECT 1 FROM streams WHERE state='writing') AND NOT EXISTS(SELECT 1 FROM pending_uploads) AND NOT EXISTS(SELECT 1 FROM extents e JOIN streams s ON s.id=e.stream_id WHERE e.fragment_id IS NOT NULL AND s.state IN ('ready','writing')) AND NOT EXISTS(SELECT 1 FROM raw_sources WHERE state='writing' OR durability<>'durable' OR medium='memory' OR upload_id IS NOT NULL) FROM mokyu_meta WHERE id=1",
    )
}

pub(super) fn relocate_raw_sources(id: Uuid, medium: &'static str) -> Statement<'static> {
    sqlx::query("UPDATE raw_sources SET medium=?,revision=revision+1 WHERE storage_id=? AND medium IN ('local','shared_s3')").bind(medium).bind(id)
}

pub(super) fn storage_row_lock<'q>(name: &'q str) -> Rows<'q, crate::model::storage::StorageRow> {
    sqlx::query_as("SELECT id,name,type,path,endpoint_url,remote_bucket_name,region,prefix,access_key_encrypted,secret_key_encrypted,identity_marker,block_key_id,enabled FROM storages WHERE name=?").bind(name)
}

pub(super) fn storage_profile_target<'q>(name: &'q str) -> Rows<'q, (Uuid, String, Option<Uuid>)> {
    sqlx::query_as("SELECT id,type,block_key_id FROM storages WHERE name=?").bind(name)
}

pub(super) fn storage_profile_lock(id: Uuid) -> Rows<'static, StorageProfile> {
    sqlx::query_as("SELECT storage_id,revision,cdc_enabled,deduplication_enabled,write_processing,connect_timeout,request_timeout,max_retries,retry_timeout,priority_aging,min_storage_duration,unreferenced_grace,deleted_chunk_retention,upload_retention,multipart_idle_timeout,min_free_space,cache_enabled,cache_soft_limit,cache_entries,upload_cache,upload_cache_size,compression_enabled,compression_strategy,compression_level,compression_sample_level,compression_min_savings_percent,compression_min_savings_bytes,pack_enabled,pack_max_size,pack_upload_flush_timeout,pack_interval,pack_reuse_interval,pack_reclaim_interval,pack_repack_interval,pack_repack_cooldown,pack_reclaim_min_savings_bytes,pack_range_enabled,pack_range_interval,pack_range_window,pack_range_min_downloads,pack_range_min_savings_percent,pack_range_min_savings_bytes,pack_range_repack_after,pack_range_repack_window,pack_range_repack_retry_interval,pack_concurrency,integrity_concurrency,integrity_rps,integrity_bandwidth,integrity_batch_timeout,read_concurrency,upload_concurrency,control_concurrency,pack_byte_budget,integrity_byte_budget,maintenance_concurrency,maintenance_byte_budget,migration_concurrency,migration_byte_budget FROM storage_profiles WHERE storage_id=?").bind(id)
}

const VIEW_ORDERED: &str = "SELECT s.id,s.name,s.type AS storage_type,s.path,s.endpoint_url AS endpoint,s.remote_bucket_name AS bucket,s.region,s.prefix,s.block_key_id,p.revision AS profile_revision,p.read_concurrency,p.upload_concurrency,p.control_concurrency,s.enabled,s.created_at,s.updated_at FROM storages s JOIN storage_profiles p ON p.storage_id=s.id ORDER BY s.name";

const VIEW_BY_ID: &str = "SELECT s.id,s.name,s.type AS storage_type,s.path,s.endpoint_url AS endpoint,s.remote_bucket_name AS bucket,s.region,s.prefix,s.block_key_id,p.revision AS profile_revision,p.read_concurrency,p.upload_concurrency,p.control_concurrency,s.enabled,s.created_at,s.updated_at FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.id=?";

const VIEW_BY_NAME: &str = "SELECT s.id,s.name,s.type AS storage_type,s.path,s.endpoint_url AS endpoint,s.remote_bucket_name AS bucket,s.region,s.prefix,s.block_key_id,p.revision AS profile_revision,p.read_concurrency,p.upload_concurrency,p.control_concurrency,s.enabled,s.created_at,s.updated_at FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.name=?";

#[async_trait::async_trait]
impl StorageRecordStore for Backend {
    async fn list(&self) -> Result<Vec<StorageView>> {
        Ok(sqlx::query_as(VIEW_ORDERED).fetch_all(&self.read).await?)
    }
    async fn view(&self, id: Uuid) -> Result<StorageView> {
        Ok(sqlx::query_as(VIEW_BY_ID)
            .bind(id)
            .fetch_one(&self.read)
            .await?)
    }
    async fn named(&self, name: &str) -> Result<Option<StorageView>> {
        Ok(sqlx::query_as(VIEW_BY_NAME)
            .bind(name)
            .fetch_optional(&self.read)
            .await?)
    }
    async fn id(&self, name: &str, parsed: Option<Uuid>) -> Result<Option<Uuid>> {
        Ok(
            sqlx::query_scalar("SELECT id FROM storages WHERE name=? OR id=?")
                .bind(name)
                .bind(parsed)
                .fetch_optional(&self.read)
                .await?,
        )
    }
    async fn record(&self, name: &str) -> Result<Option<StorageRow>> {
        Ok(sqlx::query_as("SELECT id,name,type,path,endpoint_url,remote_bucket_name,region,prefix,access_key_encrypted,secret_key_encrypted,identity_marker,block_key_id,enabled FROM storages WHERE name=?").bind(name).fetch_optional(&self.read).await?)
    }
    async fn creation(&self) -> Result<StorageCreation> {
        transactions::storage_creation(&self.read, &self.write).await
    }
    async fn edit(&self, name: &str) -> Result<StorageEdit> {
        transactions::storage_edit(&self.read, &self.write, name).await
    }
    async fn current_key(&self, name: &str, key: Option<&str>) -> Result<Uuid> {
        transactions::storage_current_key(&self.write, name, key).await
    }

    async fn enabled(&self) -> Result<Vec<StorageRecord>> {
        Ok(enabled_storages().fetch_all(&self.read).await?)
    }

    async fn enabled_by_id(&self, id: Uuid) -> Result<Option<StorageRecord>> {
        Ok(enabled_storage(id).fetch_optional(&self.read).await?)
    }
}

#[async_trait::async_trait]
impl StorageProfileStore for Backend {
    async fn initialize(&self, min_free_space: &str) -> Result<()> {
        crate::config::bytes(min_free_space)?;
        sqlx::query("UPDATE storage_profiles SET min_free_space=?,revision=revision+1,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE min_free_space IS NULL AND storage_id IN (SELECT id FROM storages WHERE type='local')").bind(min_free_space).execute(&self.write).await?;
        Ok(())
    }
    async fn update(
        &self,
        name: String,
        profile: crate::storage::profile::StorageProfile,
    ) -> Result<Uuid> {
        transactions::update_storage_profile(&self.write, name, profile).await
    }

    async fn enabled_profiles(&self) -> Result<Vec<StorageProfile>> {
        Ok(sqlx::query_as("SELECT p.* FROM storage_profiles p JOIN storages s ON s.id=p.storage_id WHERE s.enabled ORDER BY s.id").fetch_all(&self.read).await?)
    }
    async fn load(
        &self,
        storage_id: uuid::Uuid,
    ) -> Result<crate::storage::profile::StorageProfile> {
        Ok(storage_profile(storage_id).fetch_one(&self.read).await?)
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for StorageView {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            storage_type: row.try_get("storage_type")?,
            path: row.try_get("path")?,
            endpoint: row.try_get("endpoint")?,
            bucket: row.try_get("bucket")?,
            region: row.try_get("region")?,
            prefix: row.try_get("prefix")?,
            block_key_id: row.try_get("block_key_id")?,
            profile_revision: row.try_get("profile_revision")?,
            read_concurrency: row.try_get("read_concurrency")?,
            upload_concurrency: row.try_get("upload_concurrency")?,
            control_concurrency: row.try_get("control_concurrency")?,
            enabled: row.try_get("enabled")?,
            created_at: date(row, "created_at")?,
            updated_at: date(row, "updated_at")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for StorageRow {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            r#type: row.try_get("type")?,
            path: row.try_get("path")?,
            endpoint_url: row.try_get("endpoint_url")?,
            remote_bucket_name: row.try_get("remote_bucket_name")?,
            region: row.try_get("region")?,
            prefix: row.try_get("prefix")?,
            access_key_encrypted: row.try_get("access_key_encrypted")?,
            secret_key_encrypted: row.try_get("secret_key_encrypted")?,
            identity_marker: row.try_get("identity_marker")?,
            block_key_id: row.try_get("block_key_id")?,
            enabled: row.try_get("enabled")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for StorageRecord {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            store_type: row.try_get("store_type")?,
            path: row.try_get("path")?,
            endpoint: row.try_get("endpoint")?,
            bucket: row.try_get("bucket")?,
            region: row.try_get("region")?,
            prefix: row.try_get("prefix")?,
            access: row.try_get("access")?,
            secret: row.try_get("secret")?,
            identity_marker: row.try_get("identity_marker")?,
            revision: row.try_get("revision")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for crate::storage::profile::StorageProfile {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            storage_id: row.try_get("storage_id")?,
            revision: row.try_get("revision")?,
            cdc_enabled: row.try_get("cdc_enabled")?,
            deduplication_enabled: row.try_get("deduplication_enabled")?,
            write_processing: row.try_get("write_processing")?,
            connect_timeout: row.try_get("connect_timeout")?,
            request_timeout: row.try_get("request_timeout")?,
            max_retries: row.try_get("max_retries")?,
            retry_timeout: row.try_get("retry_timeout")?,
            priority_aging: row.try_get("priority_aging")?,
            min_storage_duration: row.try_get("min_storage_duration")?,
            unreferenced_grace: row.try_get("unreferenced_grace")?,
            deleted_chunk_retention: row.try_get("deleted_chunk_retention")?,
            upload_retention: row.try_get("upload_retention")?,
            multipart_idle_timeout: row.try_get("multipart_idle_timeout")?,
            min_free_space: row.try_get("min_free_space")?,
            cache_enabled: row.try_get("cache_enabled")?,
            cache_soft_limit: row.try_get("cache_soft_limit")?,
            cache_entries: row.try_get("cache_entries")?,
            upload_cache: row.try_get("upload_cache")?,
            upload_cache_size: row.try_get("upload_cache_size")?,
            compression_enabled: row.try_get("compression_enabled")?,
            compression_strategy: row.try_get("compression_strategy")?,
            compression_level: row.try_get("compression_level")?,
            compression_sample_level: row.try_get("compression_sample_level")?,
            compression_min_savings_percent: row.try_get("compression_min_savings_percent")?,
            compression_min_savings_bytes: row.try_get("compression_min_savings_bytes")?,
            pack_enabled: row.try_get("pack_enabled")?,
            pack_max_size: row.try_get("pack_max_size")?,
            pack_upload_flush_timeout: row.try_get("pack_upload_flush_timeout")?,
            pack_interval: row.try_get("pack_interval")?,
            pack_reuse_interval: row.try_get("pack_reuse_interval")?,
            pack_reclaim_interval: row.try_get("pack_reclaim_interval")?,
            pack_repack_interval: row.try_get("pack_repack_interval")?,
            pack_repack_cooldown: row.try_get("pack_repack_cooldown")?,
            pack_reclaim_min_savings_bytes: row.try_get("pack_reclaim_min_savings_bytes")?,
            pack_range_enabled: row.try_get("pack_range_enabled")?,
            pack_range_interval: row.try_get("pack_range_interval")?,
            pack_range_window: row.try_get("pack_range_window")?,
            pack_range_min_downloads: row.try_get("pack_range_min_downloads")?,
            pack_range_min_savings_percent: row.try_get("pack_range_min_savings_percent")?,
            pack_range_min_savings_bytes: row.try_get("pack_range_min_savings_bytes")?,
            pack_range_repack_after: row.try_get("pack_range_repack_after")?,
            pack_range_repack_window: row.try_get("pack_range_repack_window")?,
            pack_range_repack_retry_interval: row.try_get("pack_range_repack_retry_interval")?,
            pack_concurrency: row.try_get("pack_concurrency")?,
            integrity_concurrency: row.try_get("integrity_concurrency")?,
            integrity_rps: row.try_get("integrity_rps")?,
            integrity_bandwidth: row.try_get("integrity_bandwidth")?,
            integrity_batch_timeout: row.try_get("integrity_batch_timeout")?,
            read_concurrency: row.try_get("read_concurrency")?,
            upload_concurrency: row.try_get("upload_concurrency")?,
            control_concurrency: row.try_get("control_concurrency")?,
            pack_byte_budget: row.try_get("pack_byte_budget")?,
            integrity_byte_budget: row.try_get("integrity_byte_budget")?,
            maintenance_concurrency: row.try_get("maintenance_concurrency")?,
            maintenance_byte_budget: row.try_get("maintenance_byte_budget")?,
            migration_concurrency: row.try_get("migration_concurrency")?,
            migration_byte_budget: row.try_get("migration_byte_budget")?,
        })
    }
}

pub(super) fn save_storage_profile(profile: &StorageProfile) -> Statement<'_> {
    sqlx::query("UPDATE storage_profiles SET cdc_enabled=?,deduplication_enabled=?,write_processing=?,connect_timeout=?,request_timeout=?,max_retries=?,retry_timeout=?,priority_aging=?,min_storage_duration=?,unreferenced_grace=?,deleted_chunk_retention=?,upload_retention=?,multipart_idle_timeout=?,min_free_space=?,cache_enabled=?,cache_soft_limit=?,cache_entries=?,upload_cache=?,upload_cache_size=?,compression_enabled=?,compression_strategy=?,compression_level=?,compression_sample_level=?,compression_min_savings_percent=?,compression_min_savings_bytes=?,pack_enabled=?,pack_max_size=?,pack_upload_flush_timeout=?,pack_interval=?,pack_reuse_interval=?,pack_reclaim_interval=?,pack_repack_interval=?,pack_repack_cooldown=?,pack_reclaim_min_savings_bytes=?,pack_range_enabled=?,pack_range_interval=?,pack_range_window=?,pack_range_min_downloads=?,pack_range_min_savings_percent=?,pack_range_min_savings_bytes=?,pack_range_repack_after=?,pack_range_repack_window=?,pack_range_repack_retry_interval=?,pack_concurrency=?,integrity_concurrency=?,integrity_rps=?,integrity_bandwidth=?,integrity_batch_timeout=?,read_concurrency=?,upload_concurrency=?,control_concurrency=?,pack_byte_budget=?,integrity_byte_budget=?,maintenance_concurrency=?,maintenance_byte_budget=?,migration_concurrency=?,migration_byte_budget=?,revision=revision+1,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE storage_id=? AND revision=?")
        .bind(profile.cdc_enabled)
        .bind(profile.deduplication_enabled)
        .bind(&profile.write_processing)
        .bind(&profile.connect_timeout)
        .bind(&profile.request_timeout)
        .bind(profile.max_retries)
        .bind(&profile.retry_timeout)
        .bind(&profile.priority_aging)
        .bind(&profile.min_storage_duration)
        .bind(&profile.unreferenced_grace)
        .bind(&profile.deleted_chunk_retention)
        .bind(&profile.upload_retention)
        .bind(&profile.multipart_idle_timeout)
        .bind(&profile.min_free_space)
        .bind(profile.cache_enabled)
        .bind(&profile.cache_soft_limit)
        .bind(profile.cache_entries)
        .bind(profile.upload_cache)
        .bind(&profile.upload_cache_size)
        .bind(profile.compression_enabled)
        .bind(&profile.compression_strategy)
        .bind(profile.compression_level)
        .bind(profile.compression_sample_level)
        .bind(profile.compression_min_savings_percent)
        .bind(profile.compression_min_savings_bytes)
        .bind(profile.pack_enabled)
        .bind(&profile.pack_max_size)
        .bind(&profile.pack_upload_flush_timeout)
        .bind(&profile.pack_interval)
        .bind(&profile.pack_reuse_interval)
        .bind(&profile.pack_reclaim_interval)
        .bind(&profile.pack_repack_interval)
        .bind(&profile.pack_repack_cooldown)
        .bind(&profile.pack_reclaim_min_savings_bytes)
        .bind(profile.pack_range_enabled)
        .bind(&profile.pack_range_interval)
        .bind(&profile.pack_range_window)
        .bind(profile.pack_range_min_downloads)
        .bind(profile.pack_range_min_savings_percent)
        .bind(&profile.pack_range_min_savings_bytes)
        .bind(&profile.pack_range_repack_after)
        .bind(&profile.pack_range_repack_window)
        .bind(&profile.pack_range_repack_retry_interval)
        .bind(profile.pack_concurrency)
        .bind(profile.integrity_concurrency)
        .bind(profile.integrity_rps)
        .bind(&profile.integrity_bandwidth)
        .bind(&profile.integrity_batch_timeout)
        .bind(profile.read_concurrency)
        .bind(profile.upload_concurrency)
        .bind(profile.control_concurrency)
        .bind(&profile.pack_byte_budget)
        .bind(&profile.integrity_byte_budget)
        .bind(profile.maintenance_concurrency)
        .bind(&profile.maintenance_byte_budget)
        .bind(profile.migration_concurrency)
        .bind(&profile.migration_byte_budget)
        .bind(profile.storage_id).bind(profile.revision)
}

use crate::model::administration::{
    AdministrationStore, BucketGrant, Member, Membership, Project, ProjectInput, User, UserCreate,
    UserPatch,
};
pub(super) fn find_user(name: &str) -> Rows<'_, User> {
    sqlx::query_as("SELECT * FROM web_users WHERE username=?").bind(name)
}

pub(super) fn enable_project_mode() -> Statement<'static> {
    sqlx::query("UPDATE mokyu_meta SET project_management=TRUE WHERE id=1")
}

pub(super) fn has_admin() -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM web_users WHERE enabled AND role='admin')")
}

pub(super) fn insert_user(id: Uuid, input: &UserCreate) -> Statement<'_> {
    sqlx::query("INSERT INTO web_users(id,username,password_hash,role,must_change_password) VALUES(?,?,?,?,?) ON CONFLICT(username) DO NOTHING").bind(id).bind(&input.username).bind(&input.password_hash).bind(&input.role).bind(input.must_change_password)
}

pub(super) fn admin_count() -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT COUNT(*) FROM web_users WHERE enabled AND role='admin'")
}

pub(super) fn lock_user(id: Uuid) -> Rows<'static, User> {
    sqlx::query_as("SELECT * FROM web_users WHERE id=?").bind(id)
}

pub(super) fn delete_user_memberships(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM project_members WHERE user_id=?").bind(id)
}

pub(super) fn update_user(id: Uuid, role: &str, enabled: bool) -> Statement<'_> {
    sqlx::query("UPDATE web_users SET role=?,enabled=?,auth_revision=auth_revision+1,authorization_revision=authorization_revision+1 WHERE id=?").bind(role).bind(enabled).bind(id)
}

pub(super) fn delete_user_sessions(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM sessions WHERE user_id=?").bind(id)
}

pub(super) fn delete_user(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM web_users WHERE id=?").bind(id)
}

pub(super) fn reset_user_password(id: Uuid, hash: &str, require_change: bool) -> Statement<'_> {
    sqlx::query("UPDATE web_users SET password_hash=?,auth_revision=auth_revision+1,authorization_revision=authorization_revision+1,must_change_password=? WHERE id=?").bind(hash).bind(require_change).bind(id)
}

pub(super) fn list_users<'q>(after: &'q str, limit: i64, role: Option<&'q str>) -> Rows<'q, User> {
    sqlx::query_as("SELECT * FROM web_users WHERE username>? AND (? IS NULL OR role=?) ORDER BY username LIMIT ?").bind(after).bind(role).bind(role).bind(limit)
}

pub(super) fn user(id: Uuid) -> Rows<'static, User> {
    sqlx::query_as("SELECT * FROM web_users WHERE id=?").bind(id)
}

pub(super) fn lock_member(user: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT role FROM web_users WHERE id=?").bind(user)
}

pub(super) fn project_exists(project: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM projects WHERE id=?)").bind(project)
}

pub(super) async fn lock_project_buckets(
    connection: &mut sqlx::SqliteConnection,
    buckets: &[Uuid],
    project: Uuid,
) -> Result<Vec<Uuid>> {
    if buckets.is_empty() {
        return Ok(Vec::new());
    }
    let mut query = sqlx::QueryBuilder::<Sqlite>::new("SELECT id FROM buckets WHERE project_id=");
    query.push_bind(project).push(" AND id IN (");
    let mut ids = query.separated(",");
    for id in buckets {
        ids.push_bind(*id);
    }
    query.push(") ORDER BY id");
    Ok(query.build_query_scalar().fetch_all(connection).await?)
}

pub(super) fn save_membership<'q>(
    user: Uuid,
    project: Uuid,
    role: &'q str,
    scope: &'q str,
) -> Statement<'q> {
    sqlx::query("INSERT INTO project_members(user_id,project_id,role,scope) VALUES(?,?,?,?) ON CONFLICT(user_id,project_id) DO UPDATE SET role=excluded.role,scope=excluded.scope").bind(user).bind(project).bind(role).bind(scope)
}

pub(super) fn delete_member_grants(user: Uuid, project: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM member_bucket_grants WHERE user_id=? AND project_id=?")
        .bind(user)
        .bind(project)
}

pub(super) fn insert_member_grant(
    user: Uuid,
    project: Uuid,
    grant: &BucketGrant,
) -> Statement<'static> {
    sqlx::query(
        "INSERT INTO member_bucket_grants(user_id,project_id,bucket_id,actions) VALUES(?,?,?,?)",
    )
    .bind(user)
    .bind(project)
    .bind(grant.bucket_id)
    .bind(action_bits(&grant.actions))
}

pub(super) fn delete_membership(user: Uuid, project: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM project_members WHERE user_id=? AND project_id=?")
        .bind(user)
        .bind(project)
}

pub(super) fn advance_user_authorization(user: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE web_users SET authorization_revision=authorization_revision+1 WHERE id=?")
        .bind(user)
}

fn grants(row: &SqliteRow) -> sqlx::Result<Vec<BucketGrant>> {
    use sqlx::Row;
    let sqlx::types::Json(rows): sqlx::types::Json<Vec<(Uuid, i64)>> = row.try_get("grants")?;
    let mut grants: Vec<_> = rows
        .into_iter()
        .map(|(bucket_id, bits)| BucketGrant {
            bucket_id,
            actions: actions(bits),
        })
        .collect();
    grants.sort_by_key(|grant| grant.bucket_id);
    Ok(grants)
}

pub(super) fn project_members(project: Uuid) -> Rows<'static, Member> {
    sqlx::query_as("SELECT m.user_id,u.username,u.display_name,u.enabled,m.role,m.scope,COALESCE((SELECT JSON_GROUP_ARRAY(JSON_ARRAY(lower(hex(g.bucket_id)),g.actions)) FROM member_bucket_grants g WHERE g.user_id=m.user_id AND g.project_id=m.project_id),JSON_ARRAY()) AS grants FROM project_members m JOIN web_users u ON u.id=m.user_id WHERE m.project_id=? ORDER BY u.username LIMIT 1000").bind(project)
}

pub(super) fn projects() -> Rows<'static, Project> {
    sqlx::query_as("SELECT * FROM projects ORDER BY builtin DESC,name LIMIT 1000")
}

pub(super) fn duplicate_project(name: &str, id: Option<Uuid>) -> Scalar<'_, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM projects WHERE name=? AND (? IS NULL OR id<>?))",
    )
    .bind(name)
    .bind(id)
    .bind(id)
}

pub(super) fn update_project(id: Uuid, input: &ProjectInput) -> Statement<'_> {
    sqlx::query("UPDATE projects SET name=?,description=?,allow_bucket_create=? WHERE id=?")
        .bind(input.name.trim())
        .bind(&input.description)
        .bind(input.allow_bucket_create)
        .bind(id)
}

pub(super) fn insert_project(id: Uuid, input: &ProjectInput) -> Statement<'_> {
    sqlx::query("INSERT INTO projects(id,name,description,allow_bucket_create) VALUES(?,?,?,?)")
        .bind(id)
        .bind(input.name.trim())
        .bind(&input.description)
        .bind(input.allow_bucket_create)
}

pub(super) fn project(id: Uuid) -> Rows<'static, Project> {
    sqlx::query_as("SELECT * FROM projects WHERE id=?").bind(id)
}

pub(super) fn lock_project(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM projects WHERE id=?").bind(id)
}

pub(super) fn project_dependents(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM buckets WHERE project_id=?) OR EXISTS(SELECT 1 FROM s3_credentials WHERE project_id=?) OR EXISTS(SELECT 1 FROM project_members WHERE project_id=?)").bind(id).bind(id).bind(id)
}

pub(super) fn delete_project(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM projects WHERE id=?").bind(id)
}

pub(super) fn project_mode_dependents() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM projects WHERE NOT builtin) OR EXISTS(SELECT 1 FROM web_users WHERE role='member') OR EXISTS(SELECT 1 FROM project_members)",
    )
}

pub(super) fn set_project_mode(enabled: bool) -> Statement<'static> {
    sqlx::query("UPDATE mokyu_meta SET project_management=? WHERE id=1").bind(enabled)
}

pub(super) fn visible_projects(
    user: Uuid,
    admin: bool,
    token: Option<Uuid>,
) -> Rows<'static, Project> {
    sqlx::query_as("SELECT p.* FROM projects p WHERE ? OR CASE WHEN ? IS NOT NULL THEN EXISTS(SELECT 1 FROM token_bucket_access t JOIN buckets b ON b.id=t.bucket_id WHERE t.token_id=? AND b.project_id=p.id AND (t.actions & 1)<>0) ELSE EXISTS(SELECT 1 FROM project_members m WHERE m.project_id=p.id AND m.user_id=?) END ORDER BY p.builtin DESC,p.name LIMIT 1000").bind(admin).bind(token).bind(token).bind(user)
}

pub(super) fn project_mode() -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT project_management FROM mokyu_meta WHERE id=1")
}

#[async_trait::async_trait]
impl AdministrationStore for Backend {
    async fn all_users(&self) -> Result<Vec<User>> {
        Ok(
            sqlx::query_as("SELECT * FROM web_users ORDER BY username LIMIT 1000")
                .fetch_all(&self.read)
                .await?,
        )
    }

    async fn find_user(&self, name: &str) -> Result<Option<User>> {
        Ok(find_user(name).fetch_optional(&self.read).await?)
    }
    async fn user(&self, id: Uuid) -> Result<Option<User>> {
        Ok(user(id).fetch_optional(&self.read).await?)
    }
    async fn users(
        &self,
        search: &str,
        after: &str,
        limit: i64,
        role: Option<&str>,
    ) -> Result<Vec<User>> {
        ensure!(limit >= 0, "invalid user limit");
        if search.is_empty() {
            return Ok(list_users(after, limit, role).fetch_all(&self.read).await?);
        }
        let limit = usize::try_from(limit).context("invalid user limit")?;
        let search = search.to_lowercase();
        let mut after = after.to_owned();
        let mut result = Vec::new();
        // SQLite LOWER handles ASCII only; keep Unicode search with bounded native pages.
        while result.len() < limit {
            let batch = list_users(&after, 128, role).fetch_all(&self.read).await?;
            let more = batch.len() == 128;
            for user in batch {
                after.clone_from(&user.username);
                if user.username.to_lowercase().contains(&search)
                    || user.display_name.to_lowercase().contains(&search)
                {
                    result.push(user);
                    if result.len() == limit {
                        break;
                    }
                }
            }
            if !more {
                break;
            }
        }
        Ok(result)
    }
    async fn create_user(&self, principal: &Principal, input: UserCreate) -> Result<User> {
        transactions::create_user(&self.write, principal, input).await
    }
    async fn update_user(&self, principal: &Principal, id: Uuid, input: UserPatch) -> Result<User> {
        let _authorization = self.authorization.write().await;
        transactions::update_user(&self.write, principal, id, input).await
    }
    async fn delete_user(&self, principal: &Principal, id: Uuid) -> Result<()> {
        let _authorization = self.authorization.write().await;
        transactions::delete_user(&self.write, principal, id).await
    }
    async fn reset_password(
        &self,
        principal: &Principal,
        id: Uuid,
        hash: &str,
        require_change: bool,
    ) -> Result<()> {
        let _authorization = self.authorization.write().await;
        transactions::reset_password(&self.write, principal, id, hash, require_change).await
    }
    async fn save_member(
        &self,
        principal: &Principal,
        project: Uuid,
        user: Uuid,
        input: Option<Membership>,
    ) -> Result<()> {
        let _authorization = self.authorization.write().await;
        transactions::save_member(&self.write, principal, project, user, input).await
    }
    async fn members(&self, project: Uuid) -> Result<Vec<Member>> {
        Ok(project_members(project).fetch_all(&self.read).await?)
    }
    async fn projects(&self) -> Result<Vec<Project>> {
        Ok(projects().fetch_all(&self.read).await?)
    }
    async fn visible_projects(
        &self,
        user: Uuid,
        admin: bool,
        token: Option<Uuid>,
    ) -> Result<Vec<Project>> {
        Ok(visible_projects(user, admin, token)
            .fetch_all(&self.read)
            .await?)
    }
    async fn save_project(
        &self,
        principal: &Principal,
        id: Option<Uuid>,
        input: ProjectInput,
    ) -> Result<Project> {
        transactions::save_project(&self.write, principal, id, input).await
    }
    async fn delete_project(&self, principal: &Principal, id: Uuid) -> Result<()> {
        transactions::delete_project(&self.write, principal, id).await
    }
    async fn project_mode(&self) -> Result<bool> {
        Ok(project_mode().fetch_one(&self.read).await?)
    }
    async fn set_project_mode(&self, principal: &Principal, enabled: bool) -> Result<()> {
        transactions::set_project_mode(&self.write, principal, enabled).await
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for crate::model::administration::User {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            username: row.try_get("username")?,
            display_name: row.try_get("display_name")?,
            role: row.try_get("role")?,
            enabled: row.try_get("enabled")?,
            must_change_password: row.try_get("must_change_password")?,
            created_at: date(row, "created_at")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for crate::model::administration::Member {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            user_id: row.try_get("user_id")?,
            username: row.try_get("username")?,
            display_name: row.try_get("display_name")?,
            enabled: row.try_get("enabled")?,
            role: row.try_get("role")?,
            scope: row.try_get("scope")?,
            grants: grants(row)?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for crate::model::administration::Project {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            description: row.try_get("description")?,
            builtin: row.try_get("builtin")?,
            allow_bucket_create: row.try_get("allow_bucket_create")?,
            created_at: date(row, "created_at")?,
        })
    }
}

pub(super) fn insert_quota_scope(scope: &str, id: Uuid) -> Statement<'_> {
    sqlx::query("INSERT INTO quota_scopes(scope,scope_id) VALUES(?,?)")
        .bind(scope)
        .bind(id)
}
pub(super) fn delete_quota_scope(scope: &str, id: Uuid) -> Statement<'_> {
    sqlx::query("DELETE FROM quota_scopes WHERE scope=? AND scope_id=?")
        .bind(scope)
        .bind(id)
}

use crate::model::credential::{
    Credential, CredentialInput, CredentialMaterial, CredentialSettings, CredentialStore,
    RotateCredential, Token, TokenInput, TokenMaterial,
};
pub(super) fn credential_record(access: &str) -> Rows<'_, Credential> {
    sqlx::query_as("SELECT c.*,COALESCE((SELECT JSON_GROUP_ARRAY(JSON_ARRAY(lower(hex(g.bucket_id)),g.actions)) FROM s3_credential_grants g WHERE g.access_key=c.access_key_id),JSON_ARRAY()) AS grants FROM s3_credentials c WHERE c.access_key_id=?").bind(access)
}

pub(super) fn credential_records(
    project: Option<Uuid>,
    after: &str,
    limit: i64,
) -> Rows<'_, Credential> {
    sqlx::query_as("SELECT c.*,COALESCE((SELECT JSON_GROUP_ARRAY(JSON_ARRAY(lower(hex(g.bucket_id)),g.actions)) FROM s3_credential_grants g WHERE g.access_key=c.access_key_id),JSON_ARRAY()) AS grants FROM s3_credentials c WHERE (? IS NULL OR c.project_id=?) AND c.access_key_id>? ORDER BY c.access_key_id LIMIT ?").bind(project).bind(project).bind(after).bind(limit)
}

pub(super) fn delete_credential_grants(access: &str) -> Statement<'_> {
    sqlx::query("DELETE FROM s3_credential_grants WHERE access_key=?").bind(access)
}

pub(super) fn insert_credential_grant<'q>(access: &'q str, grant: &BucketGrant) -> Statement<'q> {
    sqlx::query("INSERT INTO s3_credential_grants(access_key,bucket_id,actions) VALUES(?,?,?)")
        .bind(access)
        .bind(grant.bucket_id)
        .bind(action_bits(&grant.actions))
}

pub(super) fn advance_credential_authorization(access: &str) -> Statement<'_> {
    sqlx::query("UPDATE s3_credentials SET authorization_revision=authorization_revision+1 WHERE access_key_id=?").bind(access)
}

pub(super) fn credential_project(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM projects WHERE id=?").bind(id)
}

pub(super) fn insert_credential<'q>(
    material: &'q CredentialMaterial,
    input: &'q CredentialInput,
    expires: Option<DateTime<Utc>>,
    user: Option<Uuid>,
) -> Statement<'q> {
    sqlx::query("INSERT INTO s3_credentials(access_key_id,secret_encrypted,project_id,label,expires_at,created_by) VALUES(?,?,?,?,?,?)").bind(&material.access).bind(&material.encrypted).bind(input.project_id).bind(input.label.trim()).bind(expires.map(|v| v.timestamp_micros())).bind(user)
}

pub(super) fn update_credential<'q>(
    access: &'q str,
    input: &'q CredentialSettings,
    expires: Option<DateTime<Utc>>,
) -> Statement<'q> {
    sqlx::query("UPDATE s3_credentials SET label=?,enabled=?,expires_at=IIF(?,expires_at,?),authorization_revision=authorization_revision+1 WHERE access_key_id=?").bind(input.label.trim()).bind(input.enabled).bind(input.keep_expiry).bind(expires.map(|v| v.timestamp_micros())).bind(access)
}

pub(super) fn lock_credential_project(access: &str) -> Scalar<'_, Uuid> {
    sqlx::query_scalar("SELECT project_id FROM s3_credentials WHERE access_key_id=?").bind(access)
}

pub(super) fn delete_credential(access: &str) -> Statement<'_> {
    sqlx::query("DELETE FROM s3_credentials WHERE access_key_id=?").bind(access)
}

pub(super) fn enable_credential(access: &str, enabled: bool) -> Statement<'_> {
    sqlx::query("UPDATE s3_credentials SET enabled=?,authorization_revision=authorization_revision+1 WHERE access_key_id=?").bind(enabled).bind(access)
}

pub(super) fn expire_credential(access: &str, expires: DateTime<Utc>) -> Statement<'_> {
    sqlx::query("UPDATE s3_credentials SET expires_at=?,authorization_revision=authorization_revision+1 WHERE access_key_id=?").bind(expires.timestamp_micros()).bind(access)
}

pub(super) fn token_record(id: Uuid) -> Rows<'static, Token> {
    sqlx::query_as("SELECT t.*,u.enabled AND NOT u.must_change_password AND t.auth_revision=u.auth_revision AND t.revoked_at IS NULL AND (t.expires_at IS NULL OR t.expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)) AS active,COALESCE((SELECT JSON_GROUP_ARRAY(JSON_ARRAY(lower(hex(g.bucket_id)),g.actions)) FROM token_bucket_grants g WHERE g.token_id=t.id),JSON_ARRAY()) AS grants FROM api_tokens t JOIN web_users u ON u.id=t.user_id WHERE t.id=?").bind(id)
}

pub(super) fn token_records(user: Uuid, after: Option<Uuid>, limit: i64) -> Rows<'static, Token> {
    sqlx::query_as("SELECT t.*,u.enabled AND NOT u.must_change_password AND t.auth_revision=u.auth_revision AND t.revoked_at IS NULL AND (t.expires_at IS NULL OR t.expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)) AS active,COALESCE((SELECT JSON_GROUP_ARRAY(JSON_ARRAY(lower(hex(g.bucket_id)),g.actions)) FROM token_bucket_grants g WHERE g.token_id=t.id),JSON_ARRAY()) AS grants FROM api_tokens t JOIN web_users u ON u.id=t.user_id WHERE t.user_id=? AND (? IS NULL OR t.id>?) ORDER BY t.id LIMIT ?").bind(user).bind(after).bind(after).bind(limit)
}

pub(super) fn token_owner_revision(user: Uuid) -> Rows<'static, (String, i64)> {
    sqlx::query_as("SELECT role,auth_revision FROM web_users WHERE id=? AND enabled AND NOT must_change_password").bind(user)
}

pub(super) fn token_owner_actions(user: Uuid, bucket: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT actions FROM user_bucket_access WHERE user_id=? AND bucket_id=?")
        .bind(user)
        .bind(bucket)
}

pub(super) fn delete_token_grants(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM token_bucket_grants WHERE token_id=?").bind(id)
}

pub(super) fn insert_token_grant(id: Uuid, grant: &BucketGrant) -> Statement<'static> {
    sqlx::query("INSERT INTO token_bucket_grants(token_id,bucket_id,actions) VALUES(?,?,?)")
        .bind(id)
        .bind(grant.bucket_id)
        .bind(action_bits(&grant.actions))
}

pub(super) fn advance_token_authorization(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE api_tokens SET authorization_revision=authorization_revision+1 WHERE id=?")
        .bind(id)
}

pub(super) fn insert_token<'q>(
    id: Uuid,
    user: Uuid,
    input: &'q TokenInput,
    material: &'q TokenMaterial,
    revision: i64,
    expires: Option<DateTime<Utc>>,
) -> Statement<'q> {
    sqlx::query("INSERT INTO api_tokens(id,user_id,label,prefix,token_hash,system_access,auth_revision,expires_at) VALUES(?,?,?,?,?,?,?,?)").bind(id).bind(user).bind(input.label.trim()).bind(&material.prefix).bind(material.hash.as_slice()).bind(input.system).bind(revision).bind(expires.map(|v| v.timestamp_micros()))
}

pub(super) fn update_token<'q>(
    id: Uuid,
    input: &'q TokenInput,
    expires: Option<DateTime<Utc>>,
) -> Statement<'q> {
    sqlx::query("UPDATE api_tokens SET label=?,system_access=?,expires_at=IIF(?,expires_at,?),authorization_revision=authorization_revision+1 WHERE id=?").bind(input.label.trim()).bind(input.system).bind(input.keep_expiry).bind(expires.map(|v| v.timestamp_micros())).bind(id)
}

pub(super) fn token_user(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT user_id FROM api_tokens WHERE id=?").bind(id)
}

pub(super) fn revoke_token(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE api_tokens SET revoked_at=COALESCE(revoked_at,CAST(unixepoch('subsec')*1000000 AS INTEGER)),authorization_revision=authorization_revision+1 WHERE id=?").bind(id)
}

#[async_trait::async_trait]
impl CredentialStore for Backend {
    async fn token_user(&self, id: Uuid) -> Result<Option<Uuid>> {
        Ok(token_user(id).fetch_optional(&self.read).await?)
    }

    async fn encrypted_secret(&self, access: &str) -> Result<Option<Vec<u8>>> {
        Ok(sqlx::query_scalar("SELECT secret_encrypted FROM s3_credentials WHERE access_key_id=? AND enabled AND (expires_at IS NULL OR expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER))").bind(access).fetch_optional(&self.read).await?)
    }
    async fn touch_credential(&self, access: &str) -> Result<()> {
        let last: Option<Option<i64>> =
            sqlx::query_scalar("SELECT last_used_at FROM s3_credentials WHERE access_key_id=?")
                .bind(access)
                .fetch_optional(&self.read)
                .await?;
        let cutoff = Utc::now().timestamp_micros().saturating_sub(300_000_000);
        if last.is_some_and(|last| last.is_none_or(|last| last < cutoff)) {
            sqlx::query("UPDATE s3_credentials SET last_used_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE access_key_id=? AND (last_used_at IS NULL OR last_used_at<?)")
                .bind(access).bind(cutoff).execute(&self.write).await?;
        }
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
        Ok(sqlx::query_as("SELECT b.* FROM buckets b JOIN s3_credential_grants g ON g.bucket_id=b.id JOIN s3_credentials c ON c.access_key_id=g.access_key WHERE g.access_key=? AND c.enabled AND (c.expires_at IS NULL OR c.expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)) AND c.project_id=b.project_id AND (g.actions & 1)<>0 AND substr(b.name,1,length(?))=? AND b.name>? AND (? IS NULL OR ?=?) ORDER BY b.name LIMIT ?").bind(access).bind(prefix).bind(prefix).bind(after).bind(region).bind(region).bind(expected_region).bind(limit).fetch_all(&self.read).await?)
    }

    async fn credential(&self, access: &str) -> Result<Credential> {
        Ok(credential_record(access)
            .fetch_optional(&self.read)
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
            .fetch_all(&self.read)
            .await?)
    }
    async fn create_credential(
        &self,
        principal: &Principal,
        input: CredentialInput,
        material: CredentialMaterial,
    ) -> Result<Credential> {
        transactions::create_credential(&self.write, principal, input, material).await
    }
    async fn update_credential(
        &self,
        principal: &Principal,
        access: &str,
        input: CredentialSettings,
    ) -> Result<Credential> {
        transactions::update_credential(&self.write, principal, access, input).await
    }
    async fn set_grants(
        &self,
        principal: &Principal,
        access: &str,
        grants: Vec<BucketGrant>,
    ) -> Result<Credential> {
        transactions::set_grants(&self.write, principal, access, grants).await
    }
    async fn revoke_credential(&self, principal: &Principal, access: &str) -> Result<()> {
        transactions::revoke_credential(&self.write, principal, access).await
    }
    async fn enable_credential(
        &self,
        principal: &Principal,
        access: &str,
        enabled: bool,
    ) -> Result<()> {
        transactions::enable_credential(&self.write, principal, access, enabled).await
    }
    async fn change_grant(
        &self,
        principal: &Principal,
        access: &str,
        bucket: Uuid,
        actions: Vec<Action>,
    ) -> Result<()> {
        transactions::change_grant(&self.write, principal, access, bucket, actions).await
    }
    async fn rotate_credential(
        &self,
        principal: &Principal,
        access: &str,
        input: RotateCredential,
        material: CredentialMaterial,
    ) -> Result<(Credential, DateTime<Utc>)> {
        transactions::rotate_credential(&self.write, principal, access, input, material).await
    }
    async fn create_token(
        &self,
        principal: &Principal,
        user: Uuid,
        input: TokenInput,
        material: TokenMaterial,
    ) -> Result<Token> {
        transactions::create_token(&self.write, principal, user, input, material).await
    }
    async fn update_token(
        &self,
        principal: &Principal,
        user: Uuid,
        id: Uuid,
        input: TokenInput,
    ) -> Result<Token> {
        let _authorization = self.authorization.write().await;
        transactions::update_token(&self.write, principal, user, id, input).await
    }
    async fn revoke_token(&self, principal: &Principal, id: Uuid) -> Result<()> {
        let _authorization = self.authorization.write().await;
        transactions::revoke_token(&self.write, principal, id).await
    }
    async fn tokens(&self, user: Uuid, after: Option<Uuid>, limit: i64) -> Result<Vec<Token>> {
        Ok(token_records(user, after, limit)
            .fetch_all(&self.read)
            .await?)
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for crate::app::Bucket {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            project_id: row.try_get("project_id")?,
            name: row.try_get("name")?,
            storage_id: row.try_get("storage_id")?,
            state: row.try_get("state")?,
            settings_revision: row.try_get("settings_revision")?,
            uploads_paused: row.try_get("uploads_paused")?,
            public_base_url: row.try_get("public_base_url")?,
            cors: row.try_get("cors")?,
            website_enabled: row.try_get("website_enabled")?,
            index_document: row.try_get("index_document")?,
            error_document: row.try_get("error_document")?,
            created_at: date(row, "created_at")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for crate::model::credential::Credential {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            access_key: row.try_get("access_key_id")?,
            project_id: row.try_get("project_id")?,
            label: row.try_get("label")?,
            enabled: row.try_get("enabled")?,
            created_at: date(row, "created_at")?,
            expires_at: optional_date(row, "expires_at")?,
            last_used_at: optional_date(row, "last_used_at")?,
            created_by: row.try_get("created_by")?,
            grants: grants(row)?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for crate::model::credential::Token {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            user_id: row.try_get("user_id")?,
            label: row.try_get("label")?,
            prefix: row.try_get("prefix")?,
            system: row.try_get("system_access")?,
            active: row.try_get("active")?,
            created_at: date(row, "created_at")?,
            expires_at: optional_date(row, "expires_at")?,
            last_used_at: optional_date(row, "last_used_at")?,
            revoked_at: optional_date(row, "revoked_at")?,
            grants: grants(row)?,
        })
    }
}

fn optional_date(row: &SqliteRow, name: &str) -> sqlx::Result<Option<DateTime<Utc>>> {
    row.try_get::<Option<i64>, _>(name)?
        .map(datetime)
        .transpose()
}

const AUDIT_COLUMNS: &str = "SELECT e.id,e.created_at,e.finished_at,e.actor_id,e.actor_label,e.token_id,e.source,e.action,e.target,e.project_id,e.bucket_id,e.outcome,e.request_id,e.http_status,";

fn audit_visibility(query: &mut sqlx::QueryBuilder<Sqlite>, admin: bool, user: Option<Uuid>) {
    if !admin {
        query.push(" AND e.actor_id=").push_bind(user)
            .push(" AND ((e.bucket_id IS NULL AND (substr(e.action,1,8)='account.' OR substr(e.action,1,8)='session.' OR substr(e.action,1,6)='token.' OR (e.outcome='failed' AND e.target='' AND json_type(e.detail)='object' AND NOT EXISTS(SELECT 1 FROM json_each(e.detail))))) OR EXISTS(SELECT 1 FROM user_bucket_access a WHERE a.user_id=")
            .push_bind(user).push(" AND a.bucket_id=e.bucket_id AND (a.actions & 1)<>0))");
    }
}

#[async_trait::async_trait]
impl crate::model::AuditStore for Backend {
    async fn begin(
        &self,
        principal: &Principal,
        action: &str,
        request_id: Option<&str>,
    ) -> Result<Id> {
        let actor = principal.user_id();
        let label = if let Some(id) = actor {
            sqlx::query_scalar("SELECT username FROM web_users WHERE id=?")
                .bind(id)
                .fetch_optional(&self.read)
                .await?
                .unwrap_or_else(|| "Deleted account".to_string())
        } else {
            "Local CLI".to_string()
        };
        let source = if principal.token_id().is_some() {
            "token"
        } else if matches!(
            principal,
            Principal::System | Principal::Bootstrap | Principal::Local | Principal::Node { .. }
        ) {
            "cli"
        } else {
            "web"
        };
        let result = sqlx::query("INSERT INTO audit_events(actor_id,actor_label,token_id,source,action,request_id) VALUES(?,?,?,?,?,?)")
            .bind(actor).bind(label).bind(principal.token_id()).bind(source).bind(action).bind(request_id)
            .execute(&self.write).await?;
        Ok(result.last_insert_rowid().into())
    }

    async fn finish(&self, id: Id, status: u16, partial: bool) -> Result<()> {
        let outcome = if partial {
            "partial"
        } else if status >= 400 {
            "failed"
        } else {
            "succeeded"
        };
        sqlx::query("UPDATE audit_events SET outcome=IIF(outcome='unknown',?,outcome),finished_at=COALESCE(finished_at,CAST(unixepoch('subsec')*1000000 AS INTEGER)),http_status=? WHERE id=?")
            .bind(outcome).bind(i32::from(status)).bind(id).execute(&self.write).await?;
        Ok(())
    }

    async fn events(
        &self,
        admin: bool,
        user: Option<Uuid>,
        before: Option<Id>,
        filter: crate::model::audit::Filter,
        limit: i64,
        export: bool,
    ) -> Result<Vec<crate::model::audit::Event>> {
        ensure!(limit >= 0, "invalid audit limit");
        let limit = limit.checked_add(1).context("audit limit overflow")?;
        let Some(actor) = filter.actor.as_deref().filter(|actor| !actor.is_empty()) else {
            return Ok(audit_page(admin, user, before, &filter, limit, export)
                .build_query_as()
                .fetch_all(&self.read)
                .await?);
        };
        let actor = actor.to_lowercase();
        let wanted = usize::try_from(limit).context("invalid audit limit")?;
        let mut before = before;
        let mut result = Vec::new();
        // Native pages keep Unicode actor filtering bounded without SQLite extensions.
        while result.len() < wanted {
            let batch: Vec<crate::model::audit::Event> =
                audit_page(admin, user, before, &filter, 128, export)
                    .build_query_as()
                    .fetch_all(&self.read)
                    .await?;
            let more = batch.len() == 128;
            for event in batch {
                before = Some(event.id.parse()?);
                if event.actor_label.to_lowercase().contains(&actor) {
                    result.push(event);
                    if result.len() == wanted {
                        break;
                    }
                }
            }
            if !more {
                break;
            }
        }
        Ok(result)
    }

    async fn event(
        &self,
        id: Id,
        admin: bool,
        user: Uuid,
    ) -> Result<Option<crate::model::audit::Event>> {
        let mut query = sqlx::QueryBuilder::new(AUDIT_COLUMNS);
        query.push("e.detail AS visible_detail FROM audit_events e WHERE e.id=");
        query.push_bind(id);
        audit_visibility(&mut query, admin, Some(user));
        Ok(query.build_query_as().fetch_optional(&self.read).await?)
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for crate::model::audit::Event {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get::<i64, _>("id")?.to_string(),
            created_at: date(row, "created_at")?,
            finished_at: optional_date(row, "finished_at")?,
            actor_id: row.try_get("actor_id")?,
            actor_label: row.try_get("actor_label")?,
            token_id: row.try_get("token_id")?,
            source: row.try_get("source")?,
            action: row.try_get("action")?,
            target: row.try_get("target")?,
            project_id: row.try_get("project_id")?,
            bucket_id: row.try_get("bucket_id")?,
            outcome: row.try_get("outcome")?,
            request_id: row.try_get("request_id")?,
            status: row.try_get("http_status")?,
            detail: row.try_get("visible_detail")?,
        })
    }
}

fn audit_page(
    admin: bool,
    user: Option<Uuid>,
    before: Option<Id>,
    filter: &crate::model::audit::Filter,
    limit: i64,
    export: bool,
) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new(AUDIT_COLUMNS);
    query.push("IIF(");
    query.push_bind(export).push(" OR length(CAST(e.detail AS BLOB))<=8192,e.detail,JSON_OBJECT('details_available',json('true'))) AS visible_detail FROM audit_events e WHERE TRUE");
    audit_visibility(&mut query, admin, user);
    if let Some(before) = before {
        query.push(" AND e.id<").push_bind(before);
    }
    if let Some(action) = filter.action.as_ref().filter(|s| !s.is_empty()) {
        query
            .push(" AND substr(e.action,1,length(")
            .push_bind(action.clone())
            .push("))=")
            .push_bind(action.clone());
    }
    if let Some(source) = &filter.source {
        query.push(" AND e.source=").push_bind(source.clone());
    }
    if let Some(outcome) = &filter.outcome {
        query.push(" AND e.outcome=").push_bind(outcome.clone());
    }
    if let Some(project) = filter.project {
        query.push(" AND e.project_id=").push_bind(project);
    }
    if let Some(bucket) = filter.bucket {
        query.push(" AND e.bucket_id=").push_bind(bucket);
    }
    if let Some(since) = filter.since {
        query
            .push(" AND e.created_at>=")
            .push_bind(since.timestamp_micros());
    }
    if let Some(until) = filter.until {
        query
            .push(" AND e.created_at<=")
            .push_bind(until.timestamp_micros());
    }
    query.push(" ORDER BY e.id DESC LIMIT ").push_bind(limit);
    query
}

use crate::app::{Extent, StoredStream};
use crate::content::{codec::Chunk, pack::Pack};
use crate::model::{
    ChunkCreate, ChunkStore, FragmentSource, PhysicalSource, PhysicalStore, ReadExtent,
    StreamCreate, StreamStore, StreamTail,
};

const CHUNK_COLUMNS: &str = "c.id,c.uuid,c.current_encoding_id,COALESCE(l.location_id,zeroblob(16)) AS location_id,c.storage_id,c.hash,c.raw_size,COALESCE(l.standalone_size,c.standalone_size) AS standalone_size,COALESCE(l.encryption_algorithm,c.encryption_algorithm) AS encryption_algorithm,IIF(l.encoding_id IS NULL,c.block_key_id,l.block_key_id) AS block_key_id,COALESCE(l.standalone_compressed,c.standalone_compressed) AS standalone_compressed,IIF(l.encoding_id IS NULL,c.nonce,l.nonce) AS nonce,c.format,c.state,c.created_at";
const ENCODING_FINGERPRINT: &str = "json_array(hex(e.block_key_id),e.encryption_algorithm,e.standalone_compressed,p.compression_enabled,p.compression_strategy,p.compression_level,p.compression_sample_level,p.compression_min_savings_percent,p.compression_min_savings_bytes,'standalone')";

pub(super) fn chunk_query(ready_encoding: bool) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new("SELECT ");
    query
        .push(CHUNK_COLUMNS)
        .push(" FROM chunks c LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id");
    if ready_encoding {
        query.push(" AND l.state='ready'");
    }
    query
}

pub(super) fn lock_owned_task(task: Uuid, token: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM tasks WHERE id=? AND state='running' AND execution_token=?")
        .bind(task)
        .bind(token)
}

pub(super) fn lock_bucket_uploads(id: Uuid) -> Rows<'static, (String, bool)> {
    sqlx::query_as("SELECT state,uploads_paused FROM buckets WHERE id=?").bind(id)
}

pub(super) fn completing_upload(id: Option<Uuid>, bucket: Uuid, key: &str) -> Scalar<'_, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM uploads WHERE id=? AND bucket_id=? AND object_key=? AND state='completing')").bind(id).bind(bucket).bind(key.as_bytes())
}

pub(super) fn insert_stream(input: &StreamCreate) -> Statement<'_> {
    sqlx::query("INSERT INTO streams(bucket_id,object_key,stream_type,state,metadata,public_read,write_authorization,owner_task_id,owner_execution_token) VALUES(?,?,?,'writing',?,?,?,?,?)").bind(input.bucket).bind(input.key.as_bytes()).bind(&input.stream_type).bind(&input.metadata).bind(input.public).bind(sqlx::types::Json(&input.authority)).bind(input.owner.map(|v|v.0)).bind(input.owner.map(|v|v.1))
}

pub(super) fn claim_object(bucket: Uuid, key: &str, token: Id) -> Statement<'_> {
    sqlx::query("INSERT INTO objects(bucket_id,object_key,write_token) VALUES(?,?,?) ON CONFLICT(bucket_id,object_key) DO UPDATE SET write_token=excluded.write_token").bind(bucket).bind(key.as_bytes()).bind(token)
}

pub(super) fn insert_fragment(tail: &StreamTail) -> Statement<'_> {
    sqlx::query(
        "INSERT INTO fragments(location_id,size,hash,owner_stream,file_id,file_offset) VALUES(?,?,?,?,?,?)",
    )
    .bind(tail.fragment)
    .bind(tail.size)
    .bind(tail.hash.as_slice())
    .bind(tail.stream)
    .bind(tail.file)
    .bind(tail.file_offset)
}

pub(super) fn seal_fragment(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE fragments SET sealed=TRUE WHERE id=?").bind(id)
}

pub(super) fn insert_fragment_extent(tail: &StreamTail, fragment: Id) -> Statement<'_> {
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,fragment_id) VALUES(?,?,?,?)")
        .bind(tail.stream)
        .bind(tail.offset)
        .bind(tail.size)
        .bind(fragment)
}

pub(super) fn lock_writing_stream(id: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM streams WHERE id=? AND state='writing'").bind(id)
}

pub(super) fn resize_writing_stream(id: Id, size: i64) -> Statement<'static> {
    sqlx::query("UPDATE streams SET size=MAX(size,?),touched_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND state='writing'").bind(size).bind(id)
}

#[async_trait::async_trait]
impl StreamStore for Backend {
    async fn cached(&self, bucket: Uuid, key: &str) -> Result<Option<crate::model::CachedStream>> {
        use sqlx::FromRow;
        let rows = sqlx::query("SELECT s.*,b.settings_revision AS bucket_revision,(b.state='active') AS bucket_active,e.offset_bytes AS cache_offset,e.length AS cache_length,e.source_offset AS cache_source,e.chunk_id AS cache_chunk,c.id AS cache_ready,c.raw_size AS cache_size,c.hash AS cache_hash FROM objects o JOIN streams s ON s.id=o.stream_id JOIN buckets b ON b.id=o.bucket_id LEFT JOIN (SELECT * FROM extents WHERE stream_id=(SELECT stream_id FROM objects WHERE bucket_id=? AND object_key=?) ORDER BY offset_bytes LIMIT 65) e ON e.stream_id=s.id LEFT JOIN chunks c ON c.id=e.chunk_id AND c.state='ready' WHERE o.bucket_id=? AND o.object_key=? AND s.state='ready' ORDER BY e.offset_bytes LIMIT 65").bind(bucket).bind(key.as_bytes()).bind(bucket).bind(key.as_bytes()).fetch_all(&self.read).await?;
        let Some(first) = rows.first() else {
            return Ok(None);
        };
        Ok(Some(crate::model::CachedStream {
            stream: StoredStream::from_row(first)?,
            bucket_revision: first.try_get("bucket_revision")?,
            bucket_active: first.try_get("bucket_active")?,
            extents: rows
                .iter()
                .map(crate::model::CachedExtent::from_row)
                .collect::<Result<_, _>>()?,
        }))
    }

    async fn current(&self, bucket: Uuid, key: &str) -> Result<StoredStream> {
        sqlx::query_as("SELECT s.* FROM objects o JOIN streams s ON s.id=o.stream_id WHERE o.bucket_id=? AND o.object_key=? AND s.state='ready'").bind(bucket).bind(key.as_bytes()).fetch_optional(&self.read).await?.ok_or_else(|| s3s::s3_error!(NoSuchKey).into())
    }
    async fn create(&self, input: StreamCreate) -> Result<Id> {
        transactions::create_stream(&self.write, input).await
    }
    async fn append_tail(&self, tail: StreamTail) -> Result<()> {
        transactions::append_tail(&self.write, tail).await
    }
    async fn finish(&self, stream: Id, size: i64, etag: &str, checksums: Value) -> Result<bool> {
        Ok(sqlx::query("UPDATE streams SET size=?,etag=?,checksums=?,touched_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND state='writing'").bind(size).bind(etag).bind(checksums).bind(stream).execute(&self.write).await?.rows_affected()==1)
    }

    async fn learn_compression(
        &self,
        observation: crate::model::CompressionObservation,
        node: Option<Uuid>,
    ) -> Result<bool> {
        observation.validate()?;
        anyhow::ensure!(node.is_none(), "SQLite only supports single mode");
        self.check_owner()?;
        Ok(learn_compression(observation)
            .execute(&self.write)
            .await?
            .rows_affected()
            == 1)
    }
}

// Include the exact partial-index predicate so SQLite can use the dedup index.
pub(super) fn fail_chunk_allocations(input: &ChunkCreate) -> Statement<'_> {
    sqlx::query("UPDATE chunks SET state='failed',unreferenced_at=CAST(unixepoch('subsec')*1000000 AS INTEGER),owner_stream=NULL WHERE storage_id=? AND hash=? AND raw_size=? AND state IN ('preparing','uploading','ready') AND state<>'ready'").bind(input.storage).bind(input.hash.as_slice()).bind(input.raw_size)
}

pub(super) fn retire_failed_encodings(input: &ChunkCreate) -> Statement<'_> {
    sqlx::query("UPDATE chunk_encodings AS e SET state='retired',unreferenced_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE state='uploading' AND EXISTS(SELECT 1 FROM chunks c WHERE c.id=e.chunk_id AND c.storage_id=? AND c.hash=? AND c.raw_size=? AND c.state='failed')").bind(input.storage).bind(input.hash.as_slice()).bind(input.raw_size)
}

pub(super) async fn reusable_chunk(
    connection: &mut sqlx::SqliteConnection,
    input: &ChunkCreate,
) -> Result<Option<Chunk>> {
    let mut query = chunk_query(true);
    query
        .push(" WHERE c.storage_id=")
        .push_bind(input.storage)
        .push(" AND c.hash=")
        .push_bind(input.hash.as_slice())
        .push(" AND c.raw_size=")
        .push_bind(input.raw_size)
        .push(" AND c.state IN ('preparing','uploading','ready') AND c.state='ready'");
    Ok(query.build_query_as().fetch_optional(connection).await?)
}

pub(super) fn insert_chunk(input: &ChunkCreate) -> Statement<'_> {
    sqlx::query("INSERT INTO chunks(uuid,storage_id,hash,raw_size,encryption_algorithm,block_key_id,owner_stream,state,unreferenced_at) VALUES(?,?,?,?,?,?,?,'preparing',CAST(unixepoch('subsec')*1000000 AS INTEGER))").bind(Uuid::now_v7()).bind(input.storage).bind(input.hash.as_slice()).bind(input.raw_size).bind(&input.algorithm).bind(input.key).bind(input.stream)
}

pub(super) fn allocate_encoding(chunk: Id) -> Statement<'static> {
    sqlx::query("INSERT INTO chunk_encodings(chunk_id,location_id,storage_id,state,created_at,unreferenced_at,encryption_algorithm,block_key_id,encoding_fingerprint) SELECT id,?,storage_id,'uploading',created_at,CAST(unixepoch('subsec')*1000000 AS INTEGER),encryption_algorithm,block_key_id,'[]' FROM chunks WHERE id=?").bind(Uuid::now_v7()).bind(chunk)
}

pub(super) fn set_chunk_encoding(chunk: Id, encoding: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunks SET current_encoding_id=? WHERE id=? AND state='preparing'")
        .bind(encoding)
        .bind(chunk)
}

pub(super) async fn allocated_chunk(
    connection: &mut sqlx::SqliteConnection,
    id: Id,
) -> Result<Chunk> {
    let mut query = chunk_query(false);
    query.push(" WHERE c.id=").push_bind(id);
    Ok(query.build_query_as().fetch_one(connection).await?)
}

pub(super) async fn refresh_encoding_fingerprint(
    connection: &mut sqlx::SqliteConnection,
    encoding: Id,
) -> Result<()> {
    let mut query =
        sqlx::QueryBuilder::new("UPDATE chunk_encodings AS e SET encoding_fingerprint=");
    query
        .push(ENCODING_FINGERPRINT)
        .push(" FROM storage_profiles p WHERE p.storage_id=e.storage_id AND e.encoding_id=")
        .push_bind(encoding);
    query.build().execute(connection).await?;
    Ok(())
}

pub(super) fn lock_chunk(id: Id) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT state FROM chunks WHERE id=?").bind(id)
}

pub(super) fn insert_chunk_extent(
    stream: Id,
    offset: i64,
    length: i32,
    chunk: Id,
) -> Statement<'static> {
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,chunk_id) VALUES(?,?,?,?)")
        .bind(stream)
        .bind(offset)
        .bind(length)
        .bind(chunk)
}

pub(super) fn pin_chunk(id: Id, owner: Id) -> Statement<'static> {
    sqlx::query("INSERT INTO cache_pins(chunk_id,pin_type,owner_id) SELECT chunk_id,IIF(source_pack_id IS NULL,'upload','pack'),? FROM pending_uploads WHERE chunk_id=? ON CONFLICT(chunk_id,pin_type,owner_id) DO NOTHING").bind(owner).bind(id)
}

pub(super) fn attach_pending_upload(chunk: Id, stream: Id, offset: i64) -> Statement<'static> {
    sqlx::query("UPDATE pending_uploads SET stream_id=?,offset_bytes=? WHERE chunk_id=? AND owner_task IS NULL AND source_pack_id IS NULL AND EXISTS(SELECT 1 FROM streams WHERE id=? AND stream_type='object' AND state='writing')").bind(stream).bind(offset).bind(chunk).bind(stream)
}

pub(super) fn complete_chunk(chunk: &Chunk) -> Statement<'_> {
    sqlx::query("UPDATE chunks SET state='ready',unreferenced_at=NULL,owner_stream=NULL,reference_changed_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND current_encoding_id=? AND state='uploading'").bind(chunk.id).bind(chunk.encoding_id)
}

pub(super) fn complete_encoding(encoding: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET state='ready',unreferenced_at=NULL,stored_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE encoding_id=? AND state='uploading' AND standalone_size IS NOT NULL").bind(encoding)
}

pub(super) fn encoding_key(id: Uuid, algorithm: &str) -> Scalar<'_, Uuid> {
    sqlx::query_scalar("SELECT id FROM managed_keys WHERE id=? AND key_algorithm=? AND purpose='block' AND revoked_at IS NULL").bind(id).bind(algorithm)
}

pub(super) fn copy_extents(
    destination: Id,
    source: Id,
    offset: i64,
    end: i64,
) -> Statement<'static> {
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,chunk_id,source_offset) SELECT ?,offset_bytes,length,chunk_id,source_offset FROM extents WHERE stream_id=? AND offset_bytes>=? AND offset_bytes<?").bind(destination).bind(source).bind(offset).bind(end)
}

pub(super) fn chunk_ids(query: &mut sqlx::QueryBuilder<Sqlite>, ids: &[Id]) {
    query.push("(");
    if ids.is_empty() {
        query.push("NULL");
    } else {
        let mut values = query.separated(",");
        for id in ids {
            values.push_bind(*id);
        }
    }
    query.push(")");
}

pub(super) async fn lock_ready_chunks(
    connection: &mut sqlx::SqliteConnection,
    ids: &[Id],
) -> Result<Vec<i64>> {
    let mut query = sqlx::QueryBuilder::new("SELECT id FROM chunks WHERE state='ready' AND id IN ");
    chunk_ids(&mut query, ids);
    query.push(" ORDER BY id");
    Ok(query.build_query_scalar().fetch_all(connection).await?)
}

pub(super) async fn reference_chunks(
    connection: &mut sqlx::SqliteConnection,
    ids: &[Id],
    owner: Id,
) -> Result<()> {
    changed_references(connection, ids, true).await?;
    let mut query = sqlx::QueryBuilder::new(
        "INSERT INTO cache_pins(chunk_id,pin_type,owner_id) SELECT chunk_id,IIF(source_pack_id IS NULL,'upload','pack'),",
    );
    query
        .push_bind(owner)
        .push(" FROM pending_uploads WHERE chunk_id IN ");
    chunk_ids(&mut query, ids);
    query.push(" ON CONFLICT(chunk_id,pin_type,owner_id) DO NOTHING");
    query.build().execute(connection).await?;
    Ok(())
}

#[async_trait::async_trait]
impl ChunkStore for Backend {
    async fn prepare_repair(
        &self,
        input: ChunkCreate,
        node: Option<crate::model::recovery::RepairNode>,
    ) -> Result<crate::model::recovery::RepairAllocation> {
        transactions::prepare_repair(&self.write, input, node).await
    }
    async fn repair_metadata(&self, repair: &crate::model::recovery::Repair) -> Result<()> {
        transactions::repair_metadata(&self.write, repair).await
    }
    async fn finish_repair(
        &self,
        repair: &crate::model::recovery::Repair,
        offset: i64,
    ) -> Result<Chunk> {
        transactions::finish_repair(&self.write, repair, offset).await
    }

    async fn storage_for_stream(&self, stream: Id) -> Result<Option<Uuid>> {
        Ok(sqlx::query_scalar(
            "SELECT b.storage_id FROM streams s JOIN buckets b ON b.id=s.bucket_id WHERE s.id=?",
        )
        .bind(stream)
        .fetch_one(&self.read)
        .await?)
    }
    async fn extents_for_copy(&self, stream: Id, from: i64) -> Result<Vec<Extent>> {
        Ok(sqlx::query_as("SELECT * FROM extents WHERE stream_id=? AND offset_bytes>=? ORDER BY offset_bytes LIMIT 500").bind(stream).bind(from).fetch_all(&self.read).await?)
    }
    async fn ready_chunk(&self, id: Id) -> Result<Option<Chunk>> {
        let mut query = chunk_query(true);
        query
            .push(" WHERE c.id=")
            .push_bind(id)
            .push(" AND c.state='ready'");
        Ok(query.build_query_as().fetch_optional(&self.read).await?)
    }
    async fn fragment(&self, id: Id) -> Result<FragmentSource> {
        Ok(
            sqlx::query_as("SELECT f.location_id AS fragment_location,f.size,f.hash,f.file_id,f.file_offset,m.storage_id AS file_storage,CASE WHEN m.source_id IS NOT NULL THEN m.id END AS raw_source FROM fragments f LEFT JOIN multipart_files m ON m.id=f.file_id WHERE f.id=?")
                .bind(id)
                .fetch_one(&self.read)
                .await?,
        )
    }
    async fn extents(&self, stream: Id, from: i64, until: i64) -> Result<Vec<Extent>> {
        Ok(sqlx::query_as("SELECT * FROM extents WHERE stream_id=? AND offset_bytes>=? AND offset_bytes<? ORDER BY offset_bytes LIMIT 64").bind(stream).bind(from).bind(until).fetch_all(&self.read).await?)
    }
    async fn read_extents(&self, stream: Id, from: i64, until: i64) -> Result<Vec<ReadExtent>> {
        let mut query = sqlx::QueryBuilder::new("SELECT e.*,");
        query.push(CHUNK_COLUMNS).push(" FROM (SELECT * FROM extents WHERE stream_id=").push_bind(stream).push(" AND offset_bytes>=").push_bind(from).push(" AND offset_bytes<").push_bind(until).push(" ORDER BY offset_bytes LIMIT 64) e LEFT JOIN chunks c ON c.id=e.chunk_id AND c.state='ready' LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' ORDER BY e.offset_bytes");
        Ok(query.build_query_as().fetch_all(&self.read).await?)
    }
    async fn read_start(&self, stream: Id, position: i64) -> Result<Option<i64>> {
        Ok(sqlx::query_scalar("SELECT offset_bytes FROM extents WHERE stream_id=? AND offset_bytes<=? ORDER BY offset_bytes DESC LIMIT 1").bind(stream).bind(position).fetch_optional(&self.read).await?)
    }
    async fn allocate(&self, input: ChunkCreate) -> Result<(Chunk, bool)> {
        transactions::allocate_chunk(&self.write, input).await
    }
    async fn can_stage_reuse(&self, id: Id) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT pack_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=?) FROM chunks WHERE id=?").bind(id).bind(id).fetch_one(&self.read).await?)
    }
    async fn mark_uploading(&self, chunk: &Chunk) -> Result<()> {
        let changed=sqlx::query("UPDATE chunks SET standalone_size=?,standalone_compressed=?,nonce=?,state='uploading' WHERE id=? AND current_encoding_id=? AND state='preparing'").bind(chunk.stored_size).bind(chunk.compressed).bind(&chunk.nonce).bind(chunk.id).bind(chunk.encoding_id).execute(&self.write).await?.rows_affected();
        ensure!(changed == 1, crate::model::Error::OperationAborted);
        Ok(())
    }
    async fn reference(&self, stream: Id, offset: i64, chunk: Id, size: i32) -> Result<()> {
        transactions::reference_chunk(&self.write, stream, offset, chunk, size).await
    }
    async fn complete(&self, stream: Id, offset: i64, chunk: &Chunk) -> Result<()> {
        transactions::complete_chunk_write(&self.write, stream, offset, chunk).await
    }
    async fn copy_references(
        &self,
        destination: Id,
        source: Id,
        offset: i64,
        end: i64,
        chunks: &[Id],
    ) -> Result<()> {
        transactions::copy_chunk_references(&self.write, destination, source, offset, end, chunks)
            .await
    }
}

pub(super) fn register_encoding(chunk: &Chunk) -> Statement<'_> {
    sqlx::query("UPDATE chunk_encodings SET location_id=?,standalone_size=?,standalone_compressed=?,nonce=?,nonce_id=? WHERE encoding_id=? AND chunk_id=? AND storage_id=? AND encryption_algorithm=? AND block_key_id IS ? AND state='uploading'").bind(chunk.location_id).bind(chunk.stored_size).bind(chunk.compressed).bind(&chunk.nonce).bind(crate::content::nonce::id(chunk.nonce.as_deref())).bind(chunk.encoding_id).bind(chunk.id).bind(chunk.storage_id).bind(&chunk.algorithm).bind(chunk.block_key_id)
}

#[async_trait::async_trait]
impl PhysicalStore for Backend {
    async fn source_snapshot(
        &self,
        scope: &crate::model::source::SourceScope,
    ) -> Result<crate::model::source::SourceSnapshot> {
        transactions::read_source_snapshot(&self.read, scope).await
    }
    async fn quarantines(
        &self,
        storage: Uuid,
        packed: bool,
        after: Id,
        limit: i64,
    ) -> Result<Vec<crate::model::source::QuarantinedSource>> {
        quarantines(
            &mut *self.read.acquire().await?,
            storage,
            packed,
            after,
            limit,
        )
        .await
    }
    async fn source_update(&self, update: crate::model::source::SourceUpdate<'_>) -> Result<()> {
        transactions::source_update(&self.write, update).await
    }

    async fn has_chunks(&self) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM chunks)")
            .fetch_one(&self.read)
            .await?)
    }
    async fn indexed(&self, location: Uuid) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM chunk_encodings WHERE location_id=?) OR EXISTS(SELECT 1 FROM packs WHERE location_id=?)").bind(location).bind(location).fetch_one(&self.read).await?)
    }
    async fn upload_pins(&self, after: Id) -> Result<Vec<crate::model::physical::StagedUpload>> {
        Ok(sqlx::query_as("SELECT p.chunk_id,IIF(p.cache_key IS NULL,NULL,c.storage_id),p.cache_key,p.staged_size FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id WHERE p.chunk_id>? ORDER BY p.chunk_id LIMIT 512").bind(after).fetch_all(&self.read).await?)
    }
    async fn pending_source(&self, chunk: Id) -> Result<Option<(Uuid, String)>> {
        Ok(sqlx::query_as("SELECT c.storage_id,p.cache_key FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id WHERE p.chunk_id=? AND p.cache_key IS NOT NULL").bind(chunk).fetch_optional(&self.read).await?)
    }
    async fn pending_only(&self, chunk: Id) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=?) AND NOT EXISTS(SELECT 1 FROM chunk_encodings WHERE chunk_id=? AND state='ready') AND NOT EXISTS(SELECT 1 FROM chunks c JOIN packs p ON p.id=c.pack_id WHERE c.id=? AND p.state='ready')").bind(chunk).bind(chunk).bind(chunk).fetch_one(&self.read).await?)
    }
    async fn register_encoding(&self, chunk: &Chunk) -> Result<()> {
        transactions::register_encoding(&self.write, chunk).await
    }
    async fn source(&self, id: Id, inspection: bool) -> Result<PhysicalSource> {
        let mut tx = self.read.begin().await?;
        let mut query = chunk_query(true);
        query
            .push(" WHERE c.id=")
            .push_bind(id)
            .push(" AND c.state='ready' AND l.encoding_id IS NOT NULL");
        if !inspection {
            query.push(" AND l.quarantined_at IS NULL");
        }
        if let Some(chunk) = query.build_query_as().fetch_optional(&mut *tx).await? {
            tx.commit().await?;
            return Ok(PhysicalSource::Chunk(chunk));
        }
        let pack=sqlx::query_as("SELECT p.* FROM chunks c JOIN packs p ON p.id=c.pack_id WHERE c.id=? AND c.state='ready' AND p.state='ready' AND (? OR p.quarantined_at IS NULL)").bind(id).bind(inspection).fetch_optional(&mut *tx).await?.context(crate::model::Error::SourceUnavailable)?;
        tx.commit().await?;
        Ok(PhysicalSource::Pack(pack))
    }
    async fn pack_chunks(&self, pack: Id) -> Result<Vec<Chunk>> {
        let mut query = chunk_query(true);
        query.push(" WHERE c.state='ready' AND c.id IN (SELECT chunk_id FROM pack_members WHERE pack_id=").push_bind(pack).push(")");
        Ok(query.build_query_as().fetch_all(&self.read).await?)
    }
    async fn pack_contains(&self, pack: Id, chunk: Id) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pack_members WHERE pack_id=? AND chunk_id=?)",
        )
        .bind(pack)
        .bind(chunk)
        .fetch_one(&self.read)
        .await?)
    }
    async fn inspection(&self, task: Uuid, pack: Id) -> Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar(
            "SELECT error_code FROM integrity_packs WHERE task_id=? AND pack_id=?",
        )
        .bind(task)
        .bind(pack)
        .fetch_optional(&self.read)
        .await?)
    }
    async fn inspection_members(&self, pack: Id) -> Result<Vec<(Chunk, i64)>> {
        let mut query = sqlx::QueryBuilder::new("SELECT ");
        query.push(CHUNK_COLUMNS).push(",m.offset_bytes FROM pack_members m JOIN chunks c ON c.id=m.chunk_id LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' WHERE m.pack_id=").push_bind(pack).push(" ORDER BY m.ordinal LIMIT 4097");
        Ok(query
            .build()
            .fetch_all(&self.read)
            .await?
            .iter()
            .map(|row| Ok((sqlx::FromRow::from_row(row)?, row.try_get("offset_bytes")?)))
            .collect::<sqlx::Result<_>>()?)
    }
    async fn record_inspection(&self, task: Uuid, pack: Id, code: Option<&str>) -> Result<()> {
        sqlx::query("INSERT INTO integrity_packs(task_id,pack_id,error_code) VALUES(?,?,?) ON CONFLICT(task_id,pack_id) DO NOTHING").bind(task).bind(pack).bind(code).execute(&self.write).await?;
        Ok(())
    }
}

fn object_key(row: &SqliteRow) -> sqlx::Result<String> {
    String::from_utf8(row.try_get("object_key")?)
        .map_err(|error| sqlx::Error::Decode(Box::new(error)))
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for StoredStream {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            bucket_id: row.try_get("bucket_id")?,
            object_key: object_key(row)?,
            stream_type: row.try_get("stream_type")?,
            payload_type: row.try_get("payload_type")?,
            payload_revision: row.try_get("payload_revision")?,
            state: row.try_get("state")?,
            size: row.try_get("size")?,
            etag: row.try_get("etag")?,
            metadata: row.try_get("metadata")?,
            public_read: row.try_get("public_read")?,
            checksums: row.try_get("checksums")?,
            http_compression: row.try_get("http_compression")?,
            created_at: date(row, "created_at")?,
            touched_at: date(row, "touched_at")?,
        })
    }
}
impl<'r> sqlx::FromRow<'r, SqliteRow> for Extent {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            stream_id: row.try_get("stream_id")?,
            offset_bytes: row.try_get("offset_bytes")?,
            length: row.try_get("length")?,
            chunk_id: row.try_get("chunk_id")?,
            fragment_id: row.try_get("fragment_id")?,
            source_offset: row.try_get("source_offset")?,
        })
    }
}
impl<'r> sqlx::FromRow<'r, SqliteRow> for Chunk {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get::<Option<Id>, _>("id")?.ok_or_else(|| {
                sqlx::Error::Decode(Box::new(crate::content::codec::IntegrityError::Metadata))
            })?,
            uuid: row.try_get("uuid")?,
            encoding_id: row.try_get("current_encoding_id")?,
            location_id: row.try_get("location_id")?,
            storage_id: row.try_get("storage_id")?,
            hash: row.try_get("hash")?,
            raw_size: row.try_get("raw_size")?,
            stored_size: row.try_get("standalone_size")?,
            algorithm: row.try_get("encryption_algorithm")?,
            block_key_id: row.try_get("block_key_id")?,
            compressed: row.try_get("standalone_compressed")?,
            nonce: row.try_get("nonce")?,
            format: row.try_get("format")?,
            state: row.try_get("state")?,
            created_at: date(row, "created_at")?,
        })
    }
}
impl<'r> sqlx::FromRow<'r, SqliteRow> for ReadExtent {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            extent: sqlx::FromRow::from_row(row)?,
            chunk: sqlx::FromRow::from_row(row)?,
        })
    }
}
impl<'r> sqlx::FromRow<'r, SqliteRow> for FragmentSource {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            location_id: row.try_get("fragment_location")?,
            size: row.try_get("size")?,
            hash: row.try_get("hash")?,
            file_id: row.try_get("file_id")?,
            file_offset: row.try_get("file_offset")?,
            file_storage: row.try_get("file_storage")?,
            raw_source: row.try_get("raw_source")?,
            staging_storage_id: None,
            staging_object_key: None,
        })
    }
}
impl<'r> sqlx::FromRow<'r, SqliteRow> for Pack {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            location_id: row.try_get("location_id")?,
            storage_id: row.try_get("storage_id")?,
            algorithm: row.try_get("encryption_algorithm")?,
            block_key_id: row.try_get("block_key_id")?,
            raw_size: row.try_get("raw_size")?,
            stored_size: row.try_get("stored_size")?,
            compressed: row.try_get("pack_compressed")?,
            nonce: row.try_get("nonce")?,
            digest: row.try_get("digest")?,
            member_count: row.try_get("member_count")?,
            state: row.try_get("state")?,
            created_at: date(row, "created_at")?,
        })
    }
}

use crate::model::quota::Quota;
#[derive(sqlx::FromRow)]
pub(super) struct QuotaAccount {
    pub scope: String,
    pub scope_id: Uuid,
    pub used_bytes: i64,
    pub reserved_bytes: i64,
    pub inflight_bytes: i64,
    pub object_count: i64,
    pub bucket_count: i64,
    pub logical_bytes_limit: Option<i64>,
    pub inflight_bytes_limit: Option<i64>,
    pub bucket_count_limit: Option<i64>,
}
impl QuotaAccount {
    pub(super) fn view(self, visible: bool) -> Quota {
        Quota {
            scope: self.scope,
            scope_id: self.scope_id,
            used_bytes: visible.then(|| self.used_bytes.to_string()),
            reserved_bytes: visible.then(|| self.reserved_bytes.to_string()),
            inflight_bytes: visible.then(|| self.inflight_bytes.to_string()),
            object_count: visible.then(|| self.object_count.to_string()),
            bucket_count: visible.then(|| self.bucket_count.to_string()),
            logical_bytes_limit: self.logical_bytes_limit.map(|n| n.to_string()),
            inflight_bytes_limit: self.inflight_bytes_limit.map(|n| n.to_string()),
            bucket_count_limit: self.bucket_count_limit.map(|n| n.to_string()),
        }
    }
}

#[derive(sqlx::FromRow)]
pub(super) struct QuotaReservation {
    pub id: Id,
    pub bucket_id: Uuid,
    pub stream_id: Option<Id>,
    pub upload_id: Option<Uuid>,
    pub closed: bool,
    pub credit_bytes: i64,
    pub logical_bytes: i64,
    pub inflight_bytes: i64,
}

pub(super) fn quota_account(scope: &str, id: Uuid) -> Rows<'_, QuotaAccount> {
    sqlx::query_as("SELECT * FROM quota_scopes WHERE scope=? AND scope_id=?")
        .bind(scope)
        .bind(id)
}

pub(super) fn quota_project(bucket: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT project_id FROM buckets WHERE id=?").bind(bucket)
}

pub(super) fn adjust_quota(
    project: Uuid,
    bucket: Uuid,
    used: i64,
    reserved: i64,
    inflight: i64,
    objects: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE quota_scopes SET used_bytes=used_bytes+?,reserved_bytes=reserved_bytes+?,inflight_bytes=inflight_bytes+?,object_count=object_count+? WHERE (scope='project' AND scope_id=?) OR (scope='bucket' AND scope_id=?)").bind(used).bind(reserved).bind(inflight).bind(objects).bind(project).bind(bucket)
}

pub(super) fn superseded_reservations(
    bucket: Uuid,
    key: &str,
    stream: Id,
) -> Rows<'_, QuotaReservation> {
    sqlx::query_as("SELECT id,bucket_id,stream_id,upload_id,closed,credit_bytes,logical_bytes,inflight_bytes FROM quota_reservations WHERE bucket_id=? AND object_key=? AND stream_id<>? AND (logical_bytes>0 OR replaced_stream_id IS NOT NULL)").bind(bucket).bind(key.as_bytes()).bind(stream)
}

pub(super) fn change_quota_reservation(
    id: Id,
    logical: i64,
    inflight: i64,
    credit: i64,
) -> Statement<'static> {
    sqlx::query(
        "UPDATE quota_reservations SET logical_bytes=?,inflight_bytes=?,credit_bytes=? WHERE id=?",
    )
    .bind(logical)
    .bind(inflight)
    .bind(credit)
    .bind(id)
}

pub(super) fn clear_quota_credit(owner: Id) -> Statement<'static> {
    sqlx::query("UPDATE quota_reservations SET replaced_stream_id=NULL WHERE id=?").bind(owner)
}

pub(super) fn clear_write_contributions(owner: Id) -> Statement<'static> {
    sqlx::query("UPDATE quota_writes SET contribution_bytes=0 WHERE reservation_id=?").bind(owner)
}

pub(super) async fn changed_references(
    connection: &mut sqlx::SqliteConnection,
    chunks: &[Id],
    added: bool,
) -> Result<()> {
    if chunks.is_empty() {
        return Ok(());
    }
    let mut query = sqlx::QueryBuilder::new(
        "UPDATE chunks AS c SET unreferenced_at=IIF(EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id),NULL,COALESCE(unreferenced_at,CAST(unixepoch('subsec')*1000000 AS INTEGER))),reference_changed_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id IN ",
    );
    chunk_ids(&mut query, chunks);
    query.build().execute(&mut *connection).await?;
    let mut query = sqlx::QueryBuilder::new(
        "INSERT INTO pack_changes(pack_id,reason) SELECT DISTINCT pack_id,",
    );
    query
        .push_bind(if added { "reuse" } else { "reclaim" })
        .push(" FROM chunks WHERE pack_id IS NOT NULL AND id IN ");
    chunk_ids(&mut query, chunks);
    query.push(" ON CONFLICT(pack_id) DO UPDATE SET generation=pack_changes.generation+1,next_check_at=CAST(unixepoch('subsec')*1000000 AS INTEGER),reason=IIF(").push_bind(added).push(",'reuse',pack_changes.reason)");
    query.build().execute(&mut *connection).await?;
    let mut query = sqlx::QueryBuilder::new(
        "INSERT INTO pack_maintenance(stream_id,reason) SELECT DISTINCT e.stream_id,'repack' FROM extents e JOIN streams s ON s.id=e.stream_id WHERE s.state='ready' AND s.stream_type='object' AND e.chunk_id IN ",
    );
    chunk_ids(&mut query, chunks);
    query.push(" ON CONFLICT(stream_id) DO UPDATE SET generation=pack_maintenance.generation+1,progress_cursor=0,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER),next_check_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)");
    query.build().execute(&mut *connection).await?;
    Ok(())
}

use crate::model::quota::Limits;
use crate::model::{PublishStore, QuotaStore};

#[derive(sqlx::FromRow)]
pub(super) struct QuotaWrite {
    pub reservation_id: Id,
    pub allocated_bytes: i64,
    pub contribution_bytes: i64,
}

pub(super) fn quota_membership(user: Option<Uuid>, project: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT scope FROM project_members WHERE user_id=? AND project_id=?")
        .bind(user)
        .bind(project)
}

pub(super) fn update_quota(
    scope: &str,
    id: Uuid,
    bytes: Option<i64>,
    inflight: Option<i64>,
    buckets: Option<i64>,
) -> Statement<'_> {
    sqlx::query("UPDATE quota_scopes SET logical_bytes_limit=?,inflight_bytes_limit=?,bucket_count_limit=? WHERE scope=? AND scope_id=?").bind(bytes).bind(inflight).bind(buckets).bind(scope).bind(id)
}

pub(super) fn owned_quota_reservation(
    stream: Id,
    upload: Option<Uuid>,
) -> Rows<'static, QuotaReservation> {
    match upload {
        Some(upload) => sqlx::query_as(concat!("SELECT id,bucket_id,stream_id,upload_id,closed,credit_bytes,logical_bytes,inflight_bytes FROM quota_reservations WHERE upload_id=?", "")).bind(upload),
        None => sqlx::query_as(concat!("SELECT id,bucket_id,stream_id,upload_id,closed,credit_bytes,logical_bytes,inflight_bytes FROM quota_reservations WHERE stream_id=?", "")).bind(stream),
    }
}

pub(super) fn quota_reservation(id: Id) -> Rows<'static, QuotaReservation> {
    sqlx::query_as("SELECT id,bucket_id,stream_id,upload_id,closed,credit_bytes,logical_bytes,inflight_bytes FROM quota_reservations WHERE id=?").bind(id)
}

pub(super) fn publication_reservation(stream: Id) -> Rows<'static, QuotaReservation> {
    sqlx::query_as("SELECT id,bucket_id,stream_id,upload_id,closed,credit_bytes,logical_bytes,inflight_bytes FROM quota_reservations WHERE stream_id=? OR result_stream_id=?").bind(stream).bind(stream)
}

pub(super) fn quota_write(stream: Id) -> Rows<'static, QuotaWrite> {
    sqlx::query_as("SELECT reservation_id,allocated_bytes,contribution_bytes FROM quota_writes WHERE stream_id=?").bind(stream)
}

pub(super) fn quota_stream(stream: Id) -> Rows<'static, StoredStream> {
    sqlx::query_as("SELECT * FROM streams WHERE id=?").bind(stream)
}

pub(super) fn quota_upload(upload: Uuid, completing: bool) -> Rows<'static, (Uuid, Vec<u8>)> {
    sqlx::query_as(if completing {
        "SELECT bucket_id,object_key FROM uploads WHERE id=? AND state='completing'"
    } else {
        "SELECT bucket_id,object_key FROM uploads WHERE id=? AND state='active'"
    })
    .bind(upload)
}

pub(super) fn object_claim(bucket: Uuid, key: &str, stream: Id) -> Scalar<'_, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM objects WHERE bucket_id=? AND object_key=? AND write_token=?)",
    )
    .bind(bucket)
    .bind(key.as_bytes())
    .bind(stream)
}

pub(super) fn part_quota(upload: Uuid, part: i32, stream: Id) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT p.quota_size FROM parts p JOIN uploads u ON u.id=p.upload_id WHERE p.upload_id=? AND p.part_number=? AND p.write_token=? AND u.state='active'").bind(upload).bind(part).bind(stream)
}

pub(super) fn quota_credit(bucket: Uuid, key: &str) -> Rows<'_, (Id, i64)> {
    sqlx::query_as("SELECT o.stream_id,s.size FROM objects o JOIN streams s ON s.id=o.stream_id WHERE o.bucket_id=? AND o.object_key=? AND NOT EXISTS(SELECT 1 FROM quota_reservations r WHERE r.bucket_id=o.bucket_id AND r.object_key=o.object_key AND r.replaced_stream_id=o.stream_id)").bind(bucket).bind(key.as_bytes())
}

pub(super) fn insert_quota_reservation(
    bucket: Uuid,
    key: &str,
    stream: Option<Id>,
    upload: Option<Uuid>,
    credit: Option<(Id, i64)>,
) -> Statement<'_> {
    sqlx::query("INSERT INTO quota_reservations(bucket_id,object_key,stream_id,upload_id,replaced_stream_id,credit_bytes) VALUES(?,?,?,?,?,?)").bind(bucket).bind(key.as_bytes()).bind(stream).bind(upload).bind(credit.map(|v|v.0)).bind(credit.map_or(0,|v|v.1))
}

pub(super) fn insert_quota_write(stream: Id, owner: Id) -> Statement<'static> {
    sqlx::query("INSERT INTO quota_writes(stream_id,reservation_id) VALUES(?,?) ON CONFLICT(stream_id) DO NOTHING").bind(stream).bind(owner)
}

pub(super) fn change_quota_write(
    stream: Id,
    allocated: i64,
    contribution: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE quota_writes SET allocated_bytes=?,contribution_bytes=? WHERE stream_id=?")
        .bind(allocated)
        .bind(contribution)
        .bind(stream)
}

pub(super) fn close_quota_reservation(owner: Id) -> Statement<'static> {
    sqlx::query("UPDATE quota_reservations SET closed=TRUE,replaced_stream_id=NULL,result_stream_id=NULL WHERE id=?").bind(owner)
}

pub(super) fn delete_quota_reservation(owner: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM quota_reservations WHERE id=?").bind(owner)
}

pub(super) fn delete_quota_write(stream: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM quota_writes WHERE stream_id=?").bind(stream)
}

pub(super) fn pending_quota_writes(owner: Id) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT CAST(COALESCE(SUM(allocated_bytes),0) AS INTEGER) FROM quota_writes WHERE reservation_id=?").bind(owner)
}

pub(super) fn remaining_quota_writes(owner: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM quota_writes WHERE reservation_id=?)")
        .bind(owner)
}

pub(super) fn complete_quota(upload: Uuid, stream: Id, size: i64) -> Statement<'static> {
    sqlx::query("UPDATE quota_reservations SET result_stream_id=? WHERE upload_id=? AND logical_bytes>=? AND NOT closed").bind(stream).bind(upload).bind(size)
}

pub(super) fn displaced_credits(bucket: Uuid, key: &str, old: Option<Id>) -> Scalar<'_, i64> {
    sqlx::query_scalar("SELECT CAST(COALESCE(SUM(MIN(logical_bytes,credit_bytes)),0) AS INTEGER) FROM quota_reservations WHERE bucket_id=? AND object_key=? AND replaced_stream_id=?").bind(bucket).bind(key.as_bytes()).bind(old)
}

pub(super) fn remove_displaced_credits(bucket: Uuid, key: &str, old: Option<Id>) -> Statement<'_> {
    sqlx::query("UPDATE quota_reservations SET credit_bytes=0,replaced_stream_id=NULL WHERE bucket_id=? AND object_key=? AND replaced_stream_id=?").bind(bucket).bind(key.as_bytes()).bind(old)
}

pub(super) fn write_authorization(
    id: Id,
) -> Scalar<'static, sqlx::types::Json<crate::authorization::Permit>> {
    sqlx::query_scalar("SELECT write_authorization FROM streams WHERE id=?").bind(id)
}

pub(super) fn publish_stream_record(id: Id) -> Rows<'static, StoredStream> {
    sqlx::query_as("SELECT * FROM streams WHERE id=?").bind(id)
}

pub(super) fn lock_object_version(bucket: Uuid, key: &str) -> Rows<'_, (Option<Id>, Id)> {
    sqlx::query_as("SELECT stream_id,write_token FROM objects WHERE bucket_id=? AND object_key=?")
        .bind(bucket)
        .bind(key.as_bytes())
}

pub(super) fn web_upload_expectation(upload: Uuid) -> Rows<'static, (Option<Id>, i64)> {
    sqlx::query_as("SELECT expected_stream_id,expected_size FROM web_uploads WHERE upload_id=?")
        .bind(upload)
}

pub(super) fn stream_size_etag(id: Id) -> Rows<'static, (i64, String)> {
    sqlx::query_as("SELECT size,etag FROM streams WHERE id=?").bind(id)
}

pub(super) fn incomplete_extents(id: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM extents e LEFT JOIN chunks c ON c.id=e.chunk_id WHERE e.stream_id=? AND (e.fragment_id IS NOT NULL OR c.id IS NULL OR c.state<>'ready' OR e.source_offset+e.length>c.raw_size))").bind(id)
}

pub(super) fn extent_continuity(id: Id) -> Rows<'static, (i64, bool)> {
    sqlx::query_as("SELECT COALESCE(MAX(offset_bytes+length),0),COALESCE(MIN(offset_bytes=previous_end),1)<>0 FROM (SELECT offset_bytes,length,LAG(offset_bytes+length,1,0) OVER(ORDER BY offset_bytes) AS previous_end FROM extents WHERE stream_id=?) e").bind(id)
}

fn catalog_category(key: &str, mime: &str) -> &'static str {
    let extension = key
        .rsplit_once('.')
        .map(|(_, e)| e.to_ascii_lowercase())
        .unwrap_or_default();
    if mime.starts_with("image/")
        || matches!(
            extension.as_str(),
            "png" | "jpg" | "jpeg" | "webp" | "gif" | "avif" | "svg" | "bmp" | "heic"
        )
    {
        "image"
    } else if mime.starts_with("video/")
        || matches!(
            extension.as_str(),
            "mp4" | "webm" | "mkv" | "mov" | "avi" | "m4v"
        )
    {
        "video"
    } else if mime.starts_with("audio/")
        || matches!(
            extension.as_str(),
            "mp3" | "ogg" | "opus" | "wav" | "flac" | "aac" | "m4a"
        )
    {
        "audio"
    } else if matches!(
        mime,
        "application/zip"
            | "application/gzip"
            | "application/x-7z-compressed"
            | "application/x-tar"
    ) || matches!(
        extension.as_str(),
        "zip" | "gz" | "7z" | "rar" | "tar" | "zst"
    ) {
        "archive"
    } else if mime.starts_with("text/")
        || mime == "application/pdf"
        || matches!(
            extension.as_str(),
            "pdf"
                | "txt"
                | "md"
                | "json"
                | "csv"
                | "htm"
                | "html"
                | "xml"
                | "doc"
                | "docx"
                | "xls"
                | "xlsx"
                | "ppt"
                | "pptx"
        )
    {
        "document"
    } else {
        "other"
    }
}

pub(super) fn publish_object(stream: &StoredStream) -> Statement<'_> {
    let mime = stream
        .metadata
        .get("content_type")
        .and_then(Value::as_str)
        .unwrap_or("application/octet-stream");
    sqlx::query("UPDATE objects SET stream_id=?,catalog_size=?,catalog_modified=?,catalog_type=?,catalog_category=?,catalog_public=? WHERE bucket_id=? AND object_key=?").bind(stream.id).bind(stream.size).bind(stream.created_at.timestamp_micros()).bind(mime).bind(catalog_category(&stream.object_key,mime)).bind(stream.public_read).bind(stream.bucket_id).bind(stream.object_key.as_bytes())
}

pub(super) fn publish_stream(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams AS s SET state='ready',touched_at=CAST(unixepoch('subsec')*1000000 AS INTEGER),http_compression=CASE WHEN stream_type='object' THEN (SELECT CASE WHEN COALESCE(sum(e.length),0)=s.size THEN json_object('estimated_size',COALESCE(sum(CASE WHEN e.source_offset=0 AND e.length=c.raw_size AND c.pack_id IS NULL AND COALESCE(l.standalone_size,c.standalone_size) IS NOT NULL THEN COALESCE(l.standalone_size,c.standalone_size) ELSE e.length END),0)) ELSE '{}' END FROM extents e LEFT JOIN chunks c ON c.id=e.chunk_id LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' WHERE e.stream_id=s.id) ELSE '{}' END WHERE id=?")
        .bind(id)
}

pub(super) fn learn_compression(
    observation: crate::model::CompressionObservation,
) -> Statement<'static> {
    sqlx::query("UPDATE streams SET http_compression=json_set(CASE WHEN COALESCE(json_extract(http_compression,'$.version'),0)<? THEN CASE WHEN json_type(http_compression,'$.estimated_size')='integer' AND json_extract(http_compression,'$.estimated_size')>=0 THEN json_object('version',?,'estimated_size',json_extract(http_compression,'$.estimated_size')) ELSE json_object('version',?) END ELSE http_compression END,?,?) WHERE id=? AND state='ready' AND COALESCE(json_extract(http_compression,'$.version'),0)<=? AND (COALESCE(json_extract(http_compression,'$.version'),0)<? OR json_type(http_compression,?) IS NULL) AND EXISTS(SELECT 1 FROM mokyu_meta WHERE id=1 AND NOT maintenance)")
        .bind(observation.version).bind(observation.version).bind(observation.version).bind(format!("$.{}",observation.encoding)).bind(observation.size).bind(observation.stream).bind(observation.version).bind(observation.version).bind(format!("$.{}",observation.encoding))
}

pub(super) fn retire_stream(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET state='retired',touched_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=?")
        .bind(id)
}

pub(super) fn complete_upload<'a>(
    upload: Uuid,
    stream: Id,
    hash: &'a str,
    result: &'a Value,
) -> Statement<'a> {
    sqlx::query("UPDATE uploads SET state='completed',result=?,output_stream=?,touched_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND state='completing' AND manifest_hash=?").bind(result).bind(stream).bind(upload).bind(hash)
}

pub(super) fn upload_part_streams(upload: Uuid) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT stream_id FROM parts WHERE upload_id=? AND stream_id IS NOT NULL ORDER BY part_number").bind(upload)
}

pub(super) fn delete_upload_parts(upload: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM parts WHERE upload_id=?").bind(upload)
}

pub(super) async fn ready_stream_queues(
    connection: &mut sqlx::SqliteConnection,
    stream: Id,
) -> Result<()> {
    sqlx::query("INSERT INTO pack_maintenance(stream_id) SELECT id FROM streams WHERE id=? AND state='ready' AND stream_type='object' AND EXISTS(SELECT 1 FROM extents WHERE stream_id=? AND chunk_id IS NOT NULL LIMIT 1 OFFSET 1) ON CONFLICT(stream_id) DO UPDATE SET generation=pack_maintenance.generation+1,progress_cursor=0,reason='pack',updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER),next_check_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)").bind(stream).bind(stream).execute(&mut *connection).await?;
    sqlx::query("INSERT INTO pack_changes(pack_id) SELECT DISTINCT c.pack_id FROM extents e JOIN chunks c ON c.id=e.chunk_id WHERE e.stream_id=? AND c.pack_id IS NOT NULL ON CONFLICT(pack_id) DO UPDATE SET generation=pack_changes.generation+1,next_check_at=CAST(unixepoch('subsec')*1000000 AS INTEGER),reason='reuse'").bind(stream).execute(connection).await?;
    Ok(())
}

#[async_trait::async_trait]
impl QuotaStore for Backend {
    async fn read(&self, principal: &Principal, scope: &str, id: Uuid) -> Result<Quota> {
        transactions::read_quota(&self.read, principal, scope, id).await
    }
    async fn save(
        &self,
        principal: &Principal,
        scope: &str,
        id: Uuid,
        input: Limits,
    ) -> Result<Quota> {
        transactions::save_quota(&self.write, principal, scope, id, input).await
    }
    async fn reserve(
        &self,
        stream: Id,
        size: i64,
        padding: bool,
        upload: Option<(Uuid, i32)>,
    ) -> Result<i64> {
        transactions::reserve_quota(&self.write, stream, size, padding, upload).await
    }
    async fn complete(&self, upload: Uuid, stream: Id, size: i64) -> Result<()> {
        transactions::complete_quota(&self.write, upload, stream, size).await
    }
}

#[async_trait::async_trait]
impl PublishStore for Backend {
    async fn publish(
        &self,
        stream: Id,
        if_match: Option<s3s::dto::ETagCondition>,
        if_none: Option<s3s::dto::ETagCondition>,
        completion: Option<(Uuid, String, Value)>,
    ) -> Result<bool> {
        transactions::publish(&self.write, stream, if_match, if_none, completion).await
    }
}

use crate::model::multipart::{
    MultipartCreate, MultipartStore, PartStitch, ReceivedPart, SeedConsumption, Transfer,
    TransferFilter, Upload, UploadPart, WebUpload,
};

pub(super) fn web_upload_identity(user: Uuid, client: Uuid) -> Rows<'static, (Uuid, Vec<u8>)> {
    sqlx::query_as("SELECT upload_id,request_hash FROM web_uploads WHERE user_id=? AND client_id=?")
        .bind(user)
        .bind(client)
}
pub(super) fn web_upload_expected(bucket: Uuid, key: &str) -> Scalar<'_, Option<Id>> {
    sqlx::query_scalar("SELECT stream_id FROM objects WHERE bucket_id=? AND object_key=?")
        .bind(bucket)
        .bind(key.as_bytes())
}
pub(super) fn active_upload_count() -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT COUNT(*) FROM uploads WHERE state IN ('active','completing')")
}
pub(super) fn insert_multipart<'a>(id: Uuid, input: &'a MultipartCreate<'_>) -> Statement<'a> {
    sqlx::query("INSERT INTO uploads(id,bucket_id,object_key,access_key,metadata,public_read,checksum_algorithm,checksum_type) VALUES(?,?,?,?,?,?,?,?)").bind(id).bind(input.bucket).bind(input.key.as_bytes()).bind(input.access).bind(&input.metadata).bind(input.public).bind(&input.algorithm).bind(&input.checksum_type)
}
pub(super) fn insert_web_upload(id: Uuid, web: &WebUpload, expected: Option<Id>) -> Statement<'_> {
    sqlx::query("INSERT INTO web_uploads(upload_id,user_id,client_id,request_hash,file_name,expected_size,part_size,modified_at,expected_stream_id) VALUES(?,?,?,?,?,?,?,?,?)").bind(id).bind(web.user).bind(web.client_id).bind(&web.request_hash).bind(&web.file_name).bind(web.size).bind(web.part_size).bind(web.modified_at).bind(expected)
}
pub(super) fn lock_upload(upload: Uuid) -> Rows<'static, Upload> {
    sqlx::query_as("SELECT * FROM uploads WHERE id=?").bind(upload)
}
pub(super) fn lock_upload_part(upload: Uuid, number: i32) -> Rows<'static, (Option<Id>, Id, i64)> {
    sqlx::query_as(
        "SELECT stream_id,write_token,quota_size FROM parts WHERE upload_id=? AND part_number=?",
    )
    .bind(upload)
    .bind(number)
}
pub(super) fn begin_upload_part(upload: Uuid, number: i32, stream: Id) -> Statement<'static> {
    sqlx::query("INSERT INTO parts(upload_id,part_number,write_token) VALUES(?,?,?) ON CONFLICT(upload_id,part_number) DO UPDATE SET write_token=excluded.write_token").bind(upload).bind(number).bind(stream)
}
pub(super) fn publish_upload_part(
    upload: Uuid,
    number: i32,
    stream: Id,
    size: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE parts SET stream_id=?,quota_size=? WHERE upload_id=? AND part_number=? AND write_token=?").bind(stream).bind(size).bind(upload).bind(number).bind(stream)
}
pub(super) fn touch_upload(upload: Uuid) -> Statement<'static> {
    sqlx::query(
        "UPDATE uploads SET touched_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=?",
    )
    .bind(upload)
}
pub(super) fn begin_upload_completion(upload: Uuid, hash: &str) -> Statement<'_> {
    sqlx::query("UPDATE uploads SET state='completing',manifest_hash=?,touched_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND state='active'").bind(hash).bind(upload)
}
pub(super) fn abort_multipart(id: Uuid, expired: bool) -> Statement<'static> {
    sqlx::query("UPDATE uploads SET state='aborted',touched_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND state IN ('active','completing') AND (NOT ? OR (state='active' AND EXISTS(SELECT 1 FROM buckets b JOIN storage_profiles p ON p.storage_id=b.storage_id WHERE b.id=uploads.bucket_id AND uploads.touched_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-(CAST(substr(p.multipart_idle_timeout,1,length(p.multipart_idle_timeout)-1) AS INTEGER)*CASE substr(p.multipart_idle_timeout,-1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END)*1000000)))").bind(id).bind(expired)
}
pub(super) fn lock_upload_active(upload: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT state='active' FROM uploads WHERE id=?").bind(upload)
}
pub(super) fn remove_upload_extent(stream: Id, offset: i64) -> Statement<'static> {
    sqlx::query("DELETE FROM extents WHERE stream_id=? AND offset_bytes=?")
        .bind(stream)
        .bind(offset)
}
pub(super) fn slice_upload_extent(
    stream: Id,
    offset: i64,
    prefix: i32,
    length: i32,
) -> Statement<'static> {
    sqlx::query("UPDATE extents SET source_offset=?,length=? WHERE stream_id=? AND offset_bytes=?")
        .bind(prefix)
        .bind(length)
        .bind(stream)
        .bind(offset)
}
pub(super) fn resize_upload_stream(stream: Id, size: i64) -> Statement<'static> {
    sqlx::query("UPDATE streams SET size=? WHERE id=?")
        .bind(size)
        .bind(stream)
}
pub(super) fn lock_upload_extents(stream: Id, offset: i64, end: i64) -> Rows<'static, Extent> {
    sqlx::query_as("SELECT * FROM extents WHERE stream_id=? AND offset_bytes>=MAX(0,?-4194304) AND offset_bytes<? AND offset_bytes+length>? ORDER BY offset_bytes").bind(stream).bind(offset).bind(end).bind(offset)
}
pub(super) fn insert_upload_chunk_extent(
    stream: Id,
    offset: i64,
    length: i32,
    chunk: Id,
    source: i32,
) -> Statement<'static> {
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,chunk_id,source_offset) VALUES(?,?,?,?,?)").bind(stream).bind(offset).bind(length).bind(chunk).bind(source)
}
pub(super) fn insert_upload_extent(
    extent: &Extent,
    offset: i64,
    length: i32,
    source: i32,
) -> Statement<'_> {
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,chunk_id,fragment_id,source_offset) VALUES(?,?,?,?,?,?)").bind(extent.stream_id).bind(offset).bind(length).bind(extent.chunk_id).bind(extent.fragment_id).bind(source)
}
pub(super) async fn mark_unreferenced_chunks(
    connection: &mut sqlx::SqliteConnection,
    chunks: &[Id],
) -> Result<()> {
    changed_references(connection, chunks, false).await
}

pub(super) async fn durable_uploads(
    connection: &mut sqlx::SqliteConnection,
    ids: &[Uuid],
) -> Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    let mut query = sqlx::QueryBuilder::new(
        "UPDATE uploads SET durable_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE durable_at IS NULL AND id IN (",
    );
    let mut values = query.separated(",");
    for id in ids {
        values.push_bind(*id);
    }
    query.push(")");
    query.build().execute(connection).await?;
    Ok(())
}

const TRANSFER_VIEW: &str = "SELECT u.id,u.bucket_id,b.name bucket_name,u.object_key,u.state,IIF(w.upload_id IS NULL,'s3','web') source,w.user_id,COALESCE(NULLIF(a.display_name,''),NULLIF(a.username,''),c.label,'S3 application') owner,w.file_name,CAST(w.expected_size AS TEXT) expected_size,CAST(w.part_size AS TEXT) part_size,w.modified_at,CAST(IIF(u.state='completed',COALESCE(s.size,w.expected_size,0),COALESCE((SELECT SUM(quota_size) FROM parts WHERE upload_id=u.id),0)) AS TEXT) received_bytes,(SELECT COUNT(*) FROM parts WHERE upload_id=u.id) parts,CASE WHEN u.state<>'completed' THEN 'pending' WHEN u.durable_at IS NOT NULL THEN 'stored' WHEN u.output_stream IS NULL THEN 'unavailable' WHEN EXISTS(SELECT 1 FROM extents e JOIN chunks ch ON ch.id=e.chunk_id WHERE e.stream_id=u.output_stream AND NOT EXISTS(SELECT 1 FROM chunk_encodings l WHERE l.chunk_id=ch.id AND l.state='ready') AND NOT EXISTS(SELECT 1 FROM packs p WHERE p.id=ch.pack_id AND p.state='ready')) THEN 'pending' ELSE 'stored' END remote_state,u.created_at,u.touched_at,p.multipart_idle_timeout,u.durable_at FROM uploads u JOIN buckets b ON b.id=u.bucket_id JOIN storage_profiles p ON p.storage_id=b.storage_id LEFT JOIN web_uploads w ON w.upload_id=u.id LEFT JOIN web_users a ON a.id=w.user_id LEFT JOIN s3_credentials c ON c.access_key_id=u.access_key LEFT JOIN streams s ON s.id=u.output_stream WHERE ";

pub(super) fn upload_transfers(
    admin: bool,
    user: Uuid,
    token: Option<Uuid>,
    filter: &TransferFilter,
    after: Option<(DateTime<Utc>, Uuid)>,
    id: Option<Uuid>,
    limit: i64,
) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new(TRANSFER_VIEW);
    if admin {
        query.push("TRUE");
    } else {
        if let Some(token) = token {
            query
                .push("EXISTS(SELECT 1 FROM token_bucket_access g WHERE g.token_id=")
                .push_bind(token);
        } else {
            query
                .push("EXISTS(SELECT 1 FROM user_bucket_access g WHERE g.user_id=")
                .push_bind(user);
        }
        query
            .push(" AND g.bucket_id=u.bucket_id AND (g.actions&4)<>0 AND (w.user_id=")
            .push_bind(user)
            .push(" OR (g.actions&32)<>0))");
    }
    if let Some(id) = id {
        query.push(" AND u.id=").push_bind(id);
    } else {
        match filter.state.as_deref() {
            Some("all") => {}
            Some(state) => {
                query.push(" AND u.state=").push_bind(state);
            }
            None => {
                query.push(" AND u.state IN ('active','completing')");
            }
        }
    }
    if let Some(bucket) = filter.bucket {
        query.push(" AND u.bucket_id=").push_bind(bucket);
    }
    if let Some(source) = &filter.source {
        query.push(if source == "web" {
            " AND w.upload_id IS NOT NULL"
        } else {
            " AND w.upload_id IS NULL"
        });
    }
    if filter.own == Some(true) {
        query.push(" AND w.user_id=").push_bind(user);
    }
    if let Some((time, id)) = after {
        query
            .push(" AND (u.created_at,u.id)<(")
            .push_bind(time.timestamp_micros())
            .push(",")
            .push_bind(id)
            .push(")");
    }
    query
        .push(" ORDER BY u.created_at DESC,u.id DESC LIMIT ")
        .push_bind(limit + 1);
    query
}

#[async_trait::async_trait]
impl MultipartStore for Backend {
    async fn pending_files(
        &self,
        storage: Option<Uuid>,
        limit: i64,
    ) -> Result<Vec<crate::model::multipart::MultipartFile>> {
        Ok(sqlx::query_as("SELECT id,upload_id,storage_id,source_id,state FROM multipart_files WHERE reclaim_pending AND state='sealed' AND (? IS NULL OR storage_id=?) ORDER BY id LIMIT ?").bind(storage).bind(storage).bind(limit).fetch_all(&self.read).await?)
    }
    async fn clear_reclaim(&self, id: Uuid) -> Result<()> {
        sqlx::query(
            "UPDATE multipart_files SET reclaim_pending=false WHERE id=? AND state='sealed'",
        )
        .bind(id)
        .execute(&self.write)
        .await?;
        Ok(())
    }
    async fn file_location(
        &self,
        id: Uuid,
    ) -> Result<Option<crate::model::multipart::MultipartFile>> {
        Ok(sqlx::query_as(
            "SELECT id,upload_id,storage_id,source_id,state FROM multipart_files WHERE id=?",
        )
        .bind(id)
        .fetch_optional(&self.read)
        .await?)
    }
    async fn file_ranges(&self, id: Uuid) -> Result<Vec<(i64, i32)>> {
        Ok(sqlx::query_as("SELECT file_offset,size FROM fragments WHERE file_id=? ORDER BY file_offset LIMIT 4097").bind(id).fetch_all(&self.read).await?)
    }
    async fn remap_file(
        &self,
        upload: Uuid,
        source: Id,
        ranges: &[crate::model::multipart::FileRemap],
    ) -> Result<()> {
        transactions::remap_multipart_file(&self.write, upload, source, ranges).await
    }
    async fn file(&self, upload: Uuid) -> Result<Uuid> {
        transactions::multipart_file(&self.write, upload).await
    }
    async fn seal_files(&self, upload: Uuid) -> Result<()> {
        sqlx::query("UPDATE multipart_files SET state='sealed' WHERE upload_id=? AND state='open'")
            .bind(upload)
            .execute(&self.write)
            .await?;
        Ok(())
    }
    async fn local_ranges(
        &self,
        stream: Id,
        after: i64,
    ) -> Result<Vec<crate::model::multipart::LocalPartRange>> {
        Ok(sqlx::query_as("SELECT e.offset_bytes,e.length,e.source_offset,e.fragment_id,f.location_id AS fragment_location,f.size,f.hash,f.file_id,f.file_offset,m.storage_id AS file_storage,CASE WHEN m.source_id IS NOT NULL THEN m.id END AS raw_source FROM extents e LEFT JOIN fragments f ON f.id=e.fragment_id LEFT JOIN multipart_files m ON m.id=f.file_id WHERE e.stream_id=? AND e.offset_bytes>=? ORDER BY e.offset_bytes LIMIT 64").bind(stream).bind(after).fetch_all(&self.read).await?)
    }
    async fn unused_files(
        &self,
        storage: Option<Uuid>,
        limit: i64,
    ) -> Result<Vec<crate::model::multipart::MultipartFile>> {
        Ok(sqlx::query_as("SELECT f.id,f.upload_id,f.storage_id,f.source_id,f.state FROM multipart_files f WHERE (? IS NULL OR storage_id=?) AND NOT EXISTS(SELECT 1 FROM fragments WHERE file_id=f.id) AND (f.state<>'open' OR NOT EXISTS(SELECT 1 FROM uploads WHERE id=f.upload_id AND state IN ('active','completing'))) ORDER BY f.id LIMIT ?").bind(storage).bind(storage).bind(limit).fetch_all(&self.read).await?)
    }
    async fn claim_file(&self, id: Uuid) -> Result<bool> {
        Ok(sqlx::query("UPDATE multipart_files AS f SET state='deleting' WHERE id=? AND NOT EXISTS(SELECT 1 FROM fragments WHERE file_id=f.id) AND (f.state<>'open' OR NOT EXISTS(SELECT 1 FROM uploads WHERE id=f.upload_id AND state IN ('active','completing')))").bind(id).execute(&self.write).await?.rows_affected()>0)
    }
    async fn remove_file(&self, id: Uuid) -> Result<()> {
        sqlx::query("DELETE FROM multipart_files WHERE id=? AND state='deleting'")
            .bind(id)
            .execute(&self.write)
            .await?;
        Ok(())
    }

    async fn upload(&self, id: Uuid, bucket: Uuid, key: &str, access: &str) -> Result<Upload> {
        sqlx::query_as("SELECT * FROM uploads WHERE id=? AND bucket_id=? AND object_key=? AND access_key=? AND state<>'aborted'").bind(id).bind(bucket).bind(key.as_bytes()).bind(access).fetch_optional(&self.read).await?.ok_or_else(||s3s::s3_error!(NoSuchUpload).into())
    }
    async fn create(&self, input: MultipartCreate<'_>) -> Result<(Uuid, bool)> {
        transactions::create_multipart(&self.write, input).await
    }
    async fn begin_part(
        &self,
        authority: &crate::authorization::Permit,
        upload: Uuid,
        number: i32,
        stream: Id,
    ) -> Result<()> {
        transactions::begin_upload_part(&self.write, authority, upload, number, stream).await
    }
    async fn commit_part(
        &self,
        authority: &crate::authorization::Permit,
        bucket: Uuid,
        upload: Uuid,
        number: i32,
        stream: Id,
    ) -> Result<()> {
        transactions::commit_upload_part(&self.write, authority, bucket, upload, number, stream)
            .await
    }
    async fn active(&self, upload: Uuid) -> Result<bool> {
        Ok(
            sqlx::query_scalar("SELECT state='active' FROM uploads WHERE id=?")
                .bind(upload)
                .fetch_one(&self.read)
                .await?,
        )
    }
    async fn part(&self, upload: Uuid, number: i32) -> Result<Option<StoredStream>> {
        Ok(sqlx::query_as("SELECT s.* FROM parts p JOIN streams s ON s.id=p.stream_id WHERE p.upload_id=? AND p.part_number=? AND s.state='ready'").bind(upload).bind(number).fetch_optional(&self.read).await?)
    }
    async fn tail(&self, stream: Id) -> Result<Option<Extent>> {
        Ok(sqlx::query_as(
            "SELECT * FROM extents WHERE stream_id=? ORDER BY offset_bytes DESC LIMIT 1",
        )
        .bind(stream)
        .fetch_optional(&self.read)
        .await?)
    }
    async fn pair(&self, upload: Uuid, number: i32) -> Result<Option<(Value, Vec<StoredStream>)>> {
        let mut tx = self.read.begin().await?;
        let Some(metadata) =
            sqlx::query_scalar("SELECT metadata FROM uploads WHERE id=? AND state='active'")
                .bind(upload)
                .fetch_optional(&mut *tx)
                .await?
        else {
            return Ok(None);
        };
        let parts=sqlx::query_as("SELECT s.* FROM parts p JOIN streams s ON s.id=p.stream_id WHERE p.upload_id=? AND p.part_number IN (?,?) AND s.state='ready' ORDER BY p.part_number").bind(upload).bind(number).bind(number+1).fetch_all(&mut *tx).await?;
        tx.commit().await?;
        Ok(Some((metadata, parts)))
    }
    async fn consume_seed(&self, input: SeedConsumption<'_>) -> Result<()> {
        transactions::consume_upload_seed(&self.write, input).await
    }
    async fn stitch(&self, input: PartStitch) -> Result<()> {
        transactions::stitch_upload_parts(&self.write, input).await
    }
    async fn retire_work(&self, stream: Id) -> Result<()> {
        transactions::retire_upload_work(&self.write, stream).await
    }
    async fn begin_complete(
        &self,
        authority: &crate::authorization::Permit,
        upload: Uuid,
        hash: &str,
    ) -> Result<()> {
        transactions::begin_upload_completion(&self.write, authority, upload, hash).await
    }
    async fn retry_complete(&self, upload: Uuid) -> Result<()> {
        sqlx::query("UPDATE uploads SET state='active',touched_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND state='completing'").bind(upload).execute(&self.write).await?;
        Ok(())
    }
    async fn abort(&self, authority: &crate::authorization::Permit, upload: Uuid) -> Result<()> {
        transactions::abort_multipart(&self.write, authority, upload, false)
            .await
            .map(|_| ())
    }
    async fn expire(&self, upload: Uuid) -> Result<bool> {
        transactions::abort_multipart(
            &self.write,
            &crate::authorization::Permit::system(),
            upload,
            true,
        )
        .await
    }
    async fn parts(&self, upload: Uuid, after: i32, limit: i64) -> Result<Vec<UploadPart>> {
        sqlx::query("SELECT p.part_number,s.size,s.etag,s.touched_at,s.checksums FROM parts p JOIN streams s ON s.id=p.stream_id WHERE p.upload_id=? AND p.part_number>? ORDER BY p.part_number LIMIT ?").bind(upload).bind(after).bind(limit).fetch_all(&self.read).await?.iter().map(|row| Ok((row.try_get("part_number")?,row.try_get("size")?,row.try_get("etag")?,date(row,"touched_at")?,row.try_get("checksums")?))).collect()
    }
    async fn uploads(
        &self,
        bucket: Uuid,
        access: &str,
        prefix: &str,
        after: &str,
        inclusive: bool,
        id: Option<Uuid>,
    ) -> Result<Vec<Upload>> {
        Ok(sqlx::query_as("SELECT * FROM uploads WHERE bucket_id=? AND access_key=? AND state IN ('active','completing') AND substr(object_key,1,length(?))=? AND (object_key>? OR (object_key=? AND (? OR id>?))) ORDER BY object_key,id LIMIT 64").bind(bucket).bind(access).bind(prefix.as_bytes()).bind(prefix.as_bytes()).bind(after.as_bytes()).bind(after.as_bytes()).bind(inclusive).bind(id).fetch_all(&self.read).await?)
    }
    async fn touch(&self, upload: Uuid) -> Result<()> {
        ensure!(
            sqlx::query(
                "UPDATE uploads SET touched_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND state='active'"
            )
            .bind(upload)
            .execute(&self.write)
            .await?
            .rows_affected()
                == 1,
            s3s::s3_error!(NoSuchUpload)
        );
        Ok(())
    }
    async fn transfers(
        &self,
        principal: &Principal,
        user: Uuid,
        filter: &TransferFilter,
        after: Option<(DateTime<Utc>, Uuid)>,
        id: Option<Uuid>,
        limit: i64,
    ) -> Result<(Vec<Transfer>, bool)> {
        transactions::upload_transfers(self, principal, user, filter, after, id, limit).await
    }
    async fn bucket_name(&self, bucket: Uuid) -> Result<String> {
        Ok(sqlx::query_scalar("SELECT name FROM buckets WHERE id=?")
            .bind(bucket)
            .fetch_one(&self.read)
            .await?)
    }
    async fn web_parts(&self, upload: Uuid, after: i32) -> Result<Vec<ReceivedPart>> {
        Ok(sqlx::query_as("SELECT p.part_number AS number,CAST(s.size AS TEXT) AS size,s.etag,json_extract(s.checksums,'$.sha256') AS sha256 FROM parts p JOIN streams s ON s.id=p.stream_id WHERE p.upload_id=? AND p.part_number>? ORDER BY p.part_number LIMIT 1001").bind(upload).bind(after).fetch_all(&self.read).await?)
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for Upload {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            bucket_id: row.try_get("bucket_id")?,
            object_key: object_key(row)?,
            state: row.try_get("state")?,
            metadata: row.try_get("metadata")?,
            public_read: row.try_get("public_read")?,
            checksum_algorithm: row.try_get("checksum_algorithm")?,
            checksum_type: row.try_get("checksum_type")?,
            manifest_hash: row.try_get("manifest_hash")?,
            result: row.try_get("result")?,
            created_at: date(row, "created_at")?,
        })
    }
}
impl<'r> sqlx::FromRow<'r, SqliteRow> for Transfer {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            bucket_id: row.try_get("bucket_id")?,
            bucket_name: row.try_get("bucket_name")?,
            object_key: object_key(row)?,
            state: row.try_get("state")?,
            source: row.try_get("source")?,
            user_id: row.try_get("user_id")?,
            owner: row.try_get("owner")?,
            file_name: row.try_get("file_name")?,
            expected_size: row.try_get("expected_size")?,
            part_size: row.try_get("part_size")?,
            modified_at: row.try_get("modified_at")?,
            received_bytes: row.try_get("received_bytes")?,
            parts: row.try_get("parts")?,
            remote_state: row.try_get("remote_state")?,
            created_at: date(row, "created_at")?,
            touched_at: date(row, "touched_at")?,
            multipart_idle_timeout: row.try_get("multipart_idle_timeout")?,
            expires_at: None,
            can_resume: false,
        })
    }
}
impl<'r> sqlx::FromRow<'r, SqliteRow> for ReceivedPart {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            number: row.try_get("number")?,
            size: row.try_get("size")?,
            etag: row.try_get("etag")?,
            sha256: row.try_get("sha256")?,
        })
    }
}

use crate::app::Bucket;
use crate::model::bucket::{
    BucketImpact, BucketProject, BucketStorage, BucketStore, BucketUpdate, BucketView,
    CreateBucket, Domain, TransferInput, TransferProject, Website,
};
use crate::model::task::Execution;

pub(super) fn bucket_by_id(id: Uuid) -> Rows<'static, Bucket> {
    sqlx::query_as("SELECT * FROM buckets WHERE id=?").bind(id)
}
pub(super) fn bucket_creation_policy(project: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT allow_bucket_create FROM projects WHERE id=?").bind(project)
}
pub(super) fn bucket_creator_membership(
    project: Uuid,
    user: Option<Uuid>,
) -> Rows<'static, (String, String)> {
    sqlx::query_as("SELECT role,scope FROM project_members WHERE project_id=? AND user_id=?")
        .bind(project)
        .bind(user)
}
pub(super) fn bucket_available_storages() -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM storages WHERE enabled ORDER BY created_at,id")
}
pub(super) fn insert_bucket(id: Uuid, name: &str, project: Uuid, storage: Uuid) -> Statement<'_> {
    sqlx::query("INSERT INTO buckets(id,name,project_id,storage_id) VALUES(?,?,?,?)")
        .bind(id)
        .bind(name)
        .bind(project)
        .bind(storage)
}
pub(super) fn duplicate_bucket(name: &str) -> Scalar<'_, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM buckets WHERE name=?)").bind(name)
}
pub(super) fn change_project_bucket_quota(
    id: Uuid,
    count: i64,
    bytes: i64,
    objects: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE quota_scopes SET bucket_count=bucket_count+?,used_bytes=used_bytes+?,object_count=object_count+? WHERE scope='project' AND scope_id=?").bind(count).bind(bytes).bind(objects).bind(id)
}
pub(super) fn bucket_has_streams(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM streams WHERE bucket_id=?) OR EXISTS(SELECT 1 FROM uploads WHERE bucket_id=?)").bind(id).bind(id)
}
pub(super) fn delete_bucket_objects(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM objects WHERE bucket_id=?").bind(id)
}
pub(super) fn delete_bucket(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM buckets WHERE id=?").bind(id)
}
pub(super) fn bucket_member_ids(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM web_users WHERE id IN (SELECT user_id FROM member_bucket_grants WHERE bucket_id=?) ORDER BY id").bind(id)
}
pub(super) fn advance_bucket_credentials(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE s3_credentials SET authorization_revision=authorization_revision+1 WHERE access_key_id IN (SELECT access_key FROM s3_credential_grants WHERE bucket_id=?)").bind(id)
}
pub(super) fn advance_bucket_tokens(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE api_tokens SET auth_revision=auth_revision+1 WHERE id IN (SELECT token_id FROM token_bucket_grants WHERE bucket_id=?)").bind(id)
}
pub(super) fn revoke_bucket_members(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM member_bucket_grants WHERE bucket_id=?").bind(id)
}
pub(super) fn revoke_bucket_credentials(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM s3_credential_grants WHERE bucket_id=?").bind(id)
}
pub(super) fn revoke_bucket_tokens(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM token_bucket_grants WHERE bucket_id=?").bind(id)
}
pub(super) fn bucket_transfer_users(source: Uuid, target: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM web_users WHERE id IN (SELECT user_id FROM project_members WHERE project_id IN (?,?)) ORDER BY id").bind(source).bind(target)
}
pub(super) fn transfer_bucket(id: Uuid, project: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE buckets SET project_id=?,uploads_paused=FALSE,settings_revision=settings_revision+1 WHERE id=?").bind(project).bind(id)
}
pub(super) fn bucket_domain_owner(host: &str) -> Scalar<'_, Uuid> {
    sqlx::query_scalar("SELECT bucket_id FROM bucket_domains WHERE host=?").bind(host)
}
pub(super) fn insert_bucket_domain(host: &str, id: Uuid) -> Statement<'_> {
    sqlx::query("INSERT INTO bucket_domains(host,bucket_id) VALUES(?,?)")
        .bind(host)
        .bind(id)
}
pub(super) fn delete_bucket_domain(host: &str) -> Statement<'_> {
    sqlx::query("DELETE FROM bucket_domains WHERE host=?").bind(host)
}
pub(super) fn bucket_domain_names(id: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT host FROM bucket_domains WHERE bucket_id=? ORDER BY host").bind(id)
}
pub(super) fn bucket_domains(id: Option<Uuid>) -> Rows<'static, Domain> {
    sqlx::query_as(
        "SELECT host,bucket_id FROM bucket_domains WHERE ? IS NULL OR bucket_id=? ORDER BY host",
    )
    .bind(id)
    .bind(id)
}
pub(super) fn advance_bucket_settings(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE buckets SET settings_revision=settings_revision+1 WHERE id=?").bind(id)
}
pub(super) fn save_bucket_settings(id: Uuid, input: &BucketUpdate) -> Statement<'_> {
    sqlx::query("UPDATE buckets SET cors=?,website_enabled=?,index_document=?,error_document=?,public_base_url=?,uploads_paused=?,settings_revision=settings_revision+1 WHERE id=?").bind(&input.cors).bind(input.website_enabled).bind(&input.index_document).bind(&input.error_document).bind(&input.public_base_url).bind(input.uploads_paused).bind(id)
}
pub(super) fn set_bucket_cors(id: Uuid, rules: &Value) -> Statement<'_> {
    sqlx::query("UPDATE buckets SET cors=?,settings_revision=settings_revision+1 WHERE id=? AND state='active'").bind(rules).bind(id)
}
pub(super) fn set_bucket_website(id: Uuid, input: &Website) -> Statement<'_> {
    sqlx::query("UPDATE buckets SET website_enabled=?,index_document=?,error_document=?,settings_revision=settings_revision+1 WHERE id=? AND state='active'").bind(input.website_enabled).bind(&input.index_document).bind(&input.error_document).bind(id)
}
pub(super) fn bucket_impact(id: Uuid) -> Rows<'static, BucketImpact> {
    sqlx::query_as("SELECT b.id AS bucket_id,b.name,b.project_id,b.state,CAST(b.settings_revision AS TEXT) revision,b.uploads_paused,CAST(q.object_count AS TEXT) objects,CAST(q.used_bytes AS TEXT) logical_bytes,CAST(q.reserved_bytes AS TEXT) reserved_bytes,CAST(q.inflight_bytes AS TEXT) inflight_bytes,(SELECT CAST(COUNT(*) AS TEXT) FROM uploads WHERE bucket_id=b.id AND state IN ('active','completing')) active_uploads,(SELECT CAST(COUNT(*) AS TEXT) FROM streams WHERE bucket_id=b.id AND state='writing') writing_streams,(SELECT CAST(COUNT(*) AS TEXT) FROM member_bucket_grants WHERE bucket_id=b.id) member_bucket_grants,(SELECT CAST(COUNT(*) AS TEXT) FROM s3_credential_grants WHERE bucket_id=b.id) service_grants,(SELECT CAST(COUNT(*) AS TEXT) FROM token_bucket_grants WHERE bucket_id=b.id) token_bucket_grants,COALESCE((SELECT json_group_array(host) FROM bucket_domains WHERE bucket_id=b.id),JSON_ARRAY()) domains FROM buckets b JOIN quota_scopes q ON q.scope='bucket' AND q.scope_id=b.id WHERE b.id=?").bind(id)
}
pub(super) fn bucket_transfer_target(id: Uuid) -> Rows<'static, TransferProject> {
    sqlx::query_as("SELECT p.id,p.name,CAST(q.used_bytes AS TEXT) used_bytes,CAST(q.reserved_bytes AS TEXT) reserved_bytes,CAST(q.logical_bytes_limit AS TEXT) logical_bytes_limit,CAST(q.bucket_count AS TEXT) bucket_count,CAST(q.bucket_count_limit AS TEXT) bucket_count_limit,(SELECT CAST(COUNT(*) AS TEXT) FROM project_members WHERE project_id=p.id) members FROM projects p JOIN quota_scopes q ON q.scope='project' AND q.scope_id=p.id WHERE p.id=?").bind(id)
}
pub(super) fn existing_purge(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM tasks WHERE task_type='purge' AND bucket_id=? AND state<>'completed' LIMIT 1").bind(id)
}
pub(super) fn seal_purge_bucket(id: Uuid) -> Statement<'static> {
    sqlx::query(
        "UPDATE buckets SET state='purging',settings_revision=settings_revision+1 WHERE id=?",
    )
    .bind(id)
}
pub(super) fn insert_task(
    id: Uuid,
    task_type: &str,
    bucket: Option<Uuid>,
    detail: Value,
    audit: Option<Id>,
) -> Statement<'_> {
    sqlx::query("INSERT INTO tasks(id,task_type,bucket_id,state,detail,created_by_label,source) VALUES(?,?,?,'queued',?,COALESCE((SELECT actor_label FROM audit_events WHERE id=?),'Scheduler'),COALESCE((SELECT source FROM audit_events WHERE id=?),'system'))").bind(id).bind(task_type).bind(bucket).bind(detail).bind(audit).bind(audit)
}
pub(super) fn lock_task_execution(id: Execution) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM tasks WHERE id=? AND state='running' AND execution_token=?")
        .bind(id.id)
        .bind(id.token)
}
pub(super) fn lock_purge_objects(
    bucket: Uuid,
    cursor: Option<&str>,
    batch: i64,
) -> Rows<'_, (Vec<u8>, Option<Id>)> {
    sqlx::query_as("SELECT object_key,stream_id FROM objects WHERE bucket_id=? AND (? IS NULL OR object_key>?) ORDER BY object_key LIMIT ?").bind(bucket).bind(cursor.map(str::as_bytes)).bind(cursor.map(str::as_bytes)).bind(batch)
}
pub(super) fn remove_object(bucket: Uuid, key: &str, token: Id) -> Statement<'_> {
    sqlx::query("UPDATE objects SET stream_id=NULL,write_token=?,catalog_size=NULL,catalog_modified=NULL,catalog_type=NULL,catalog_category=NULL,catalog_public=NULL WHERE bucket_id=? AND object_key=?").bind(token).bind(bucket).bind(key.as_bytes())
}
pub(super) fn advance_task(id: Uuid, cursor: &str, processed: i64) -> Statement<'_> {
    sqlx::query("UPDATE tasks SET progress_cursor=?,processed=processed+?,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=?").bind(cursor).bind(processed).bind(id)
}
pub(super) fn complete_task(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='completed',updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=?")
        .bind(id)
}
pub(super) async fn purge_upload_history(
    connection: &mut sqlx::SqliteConnection,
    bucket: Uuid,
    active: &[Uuid],
    batch: i64,
) -> Result<()> {
    let mut query = sqlx::QueryBuilder::<Sqlite>::new(
        "DELETE FROM uploads WHERE id IN (SELECT id FROM uploads WHERE bucket_id=",
    );
    query.push_bind(bucket).push(" AND state IN ('completed','aborted') AND NOT EXISTS(SELECT 1 FROM quota_reservations q WHERE q.upload_id=uploads.id)");
    if !active.is_empty() {
        query.push(" AND id NOT IN (");
        let mut values = query.separated(",");
        for id in active {
            values.push_bind(*id);
        }
        query.push(")");
    }
    query.push(" ORDER BY id LIMIT ").push_bind(batch).push(")");
    query.build().execute(connection).await?;
    Ok(())
}

#[async_trait::async_trait]
impl BucketStore for Backend {
    async fn domain_names(&self) -> Result<Vec<(String, String)>> {
        Ok(sqlx::query_as("SELECT d.host,b.name FROM bucket_domains d JOIN buckets b ON b.id=d.bucket_id ORDER BY d.host").fetch_all(&self.read).await?)
    }
    async fn purge_counts(&self, bucket: Uuid) -> Result<(i64, i64)> {
        Ok(sqlx::query_as("SELECT (SELECT COUNT(*) FROM objects WHERE bucket_id=? AND stream_id IS NOT NULL),(SELECT COUNT(*) FROM uploads WHERE bucket_id=? AND state IN ('active','completing'))").bind(bucket).bind(bucket).fetch_one(&self.read).await?)
    }
    async fn start_purge(
        &self,
        principal: &Principal,
        name: &str,
        id: Uuid,
        confirmation: Option<&str>,
    ) -> Result<Value> {
        transactions::start_purge(&self.write, principal, name, id, confirmation).await
    }
    async fn purge_uploads(&self, bucket: Uuid, default_batch: i32) -> Result<(i64, Vec<Uuid>)> {
        let batch: Option<i32>=sqlx::query_scalar("SELECT maintenance_concurrency FROM storage_profiles WHERE storage_id=(SELECT storage_id FROM buckets WHERE id=?)").bind(bucket).fetch_one(&self.read).await?;
        let batch = i64::from(batch.unwrap_or(default_batch).max(1));
        Ok((batch,sqlx::query_scalar("SELECT id FROM uploads WHERE bucket_id=? AND state IN ('active','completing') ORDER BY id LIMIT ?").bind(bucket).bind(batch).fetch_all(&self.read).await?))
    }
    async fn purge_batch(
        &self,
        task: Execution,
        bucket: Uuid,
        cursor: Option<&str>,
        batch: i64,
        active: &[Uuid],
    ) -> Result<bool> {
        let _authorization = self.authorization.write().await;
        transactions::purge_batch(&self.write, task, bucket, cursor, batch, active).await
    }
    async fn by_name(&self, name: &str, active: bool) -> Result<Option<Bucket>> {
        Ok(
            sqlx::query_as("SELECT * FROM buckets WHERE name=? AND (NOT ? OR state='active')")
                .bind(name)
                .bind(active)
                .fetch_optional(&self.read)
                .await?,
        )
    }
    async fn by_host(&self, host: &str) -> Result<Option<Bucket>> {
        Ok(sqlx::query_as(
            "SELECT b.* FROM buckets b JOIN bucket_domains d ON d.bucket_id=b.id WHERE d.host=?",
        )
        .bind(host)
        .fetch_optional(&self.read)
        .await?)
    }
    async fn all(&self) -> Result<Vec<Bucket>> {
        Ok(sqlx::query_as("SELECT * FROM buckets ORDER BY name")
            .fetch_all(&self.read)
            .await?)
    }
    async fn visible(
        &self,
        user: Uuid,
        admin: bool,
        token: Option<Uuid>,
    ) -> Result<Vec<BucketView>> {
        Ok(sqlx::query_as("SELECT b.*,CAST(IIF(?,127,IIF(? IS NOT NULL,COALESCE(t.actions,0),COALESCE(a.actions,0))) AS INTEGER) actions FROM buckets b LEFT JOIN user_bucket_access a ON a.bucket_id=b.id AND a.user_id=? LEFT JOIN token_bucket_access t ON t.bucket_id=b.id AND t.token_id=? WHERE ? OR (IIF(? IS NOT NULL,t.actions,a.actions)&1)<>0 ORDER BY b.name LIMIT 1000").bind(admin).bind(token).bind(user).bind(token).bind(admin).bind(token).fetch_all(&self.read).await?)
    }
    async fn list_objects(
        &self,
        bucket: Uuid,
        prefix: &str,
        key: &str,
        inclusive: bool,
    ) -> Result<Vec<StoredStream>> {
        let mut query = sqlx::QueryBuilder::new(
            "SELECT s.* FROM objects o JOIN streams s ON s.id=o.stream_id WHERE o.bucket_id=",
        );
        query
            .push_bind(bucket)
            .push(" AND o.object_key>=")
            .push_bind(prefix.as_bytes());
        if let Some(end) = crate::app::listing::successor(prefix) {
            query.push(" AND o.object_key<").push_bind(end.into_bytes());
        }
        query
            .push(" AND o.object_key")
            .push(if inclusive { ">=" } else { ">" })
            .push_bind(key.as_bytes())
            .push(" ORDER BY o.object_key LIMIT 64");
        Ok(query.build_query_as().fetch_all(&self.read).await?)
    }
    async fn create(&self, principal: &Principal, input: CreateBucket) -> Result<Bucket> {
        let _authorization = self.authorization.write().await;
        transactions::create_bucket(&self.write, principal, input).await
    }
    async fn delete(
        &self,
        principal: &Principal,
        bucket: Uuid,
        name: &str,
        confirmation: Option<&str>,
    ) -> Result<()> {
        let _authorization = self.authorization.write().await;
        transactions::delete_bucket(&self.write, principal, bucket, name, confirmation).await
    }
    async fn domains(&self, principal: &Principal, bucket: Option<Uuid>) -> Result<Vec<Domain>> {
        transactions::bucket_domains(&self.read, principal, bucket).await
    }
    async fn change_domain(
        &self,
        principal: &Principal,
        host: &str,
        bucket: Option<Uuid>,
    ) -> Result<()> {
        transactions::domain_change(&self.write, principal, host, bucket).await
    }
    async fn settings(&self, bucket: Uuid) -> Result<(Bucket, Vec<String>)> {
        transactions::bucket_settings(&self.read, bucket).await
    }
    async fn save_settings(
        &self,
        principal: &Principal,
        bucket: Uuid,
        input: BucketUpdate,
    ) -> Result<()> {
        transactions::save_bucket_settings(&self.write, principal, bucket, input).await
    }
    async fn creation_options(
        &self,
        admin: bool,
        user: Uuid,
        session: bool,
    ) -> Result<(Vec<BucketProject>, Vec<BucketStorage>)> {
        Ok((sqlx::query_as("SELECT id,name FROM projects p WHERE ? OR (? AND allow_bucket_create AND EXISTS(SELECT 1 FROM project_members WHERE project_id=p.id AND user_id=? AND role='maintainer')) ORDER BY builtin DESC,name LIMIT 1000").bind(admin).bind(session).bind(user).fetch_all(&self.read).await?,sqlx::query_as("SELECT id,name,type AS storage_type FROM storages WHERE enabled ORDER BY name").fetch_all(&self.read).await?))
    }
    async fn impact(&self, principal: &Principal, bucket: Uuid) -> Result<BucketImpact> {
        transactions::preview_bucket(&self.read, principal, bucket).await
    }
    async fn transfer_preview(
        &self,
        principal: &Principal,
        bucket: Uuid,
        target: Uuid,
    ) -> Result<(BucketImpact, TransferProject)> {
        transactions::preview_bucket_transfer(&self.read, principal, bucket, target).await
    }
    async fn transfer(
        &self,
        principal: &Principal,
        bucket: Uuid,
        input: TransferInput,
    ) -> Result<Bucket> {
        let _authorization = self.authorization.write().await;
        transactions::transfer_bucket(&self.write, principal, bucket, input).await
    }
    async fn cors(&self, bucket: Uuid) -> Result<Value> {
        Ok(sqlx::query_scalar("SELECT cors FROM buckets WHERE id=?")
            .bind(bucket)
            .fetch_optional(&self.read)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?)
    }
    async fn set_cors(&self, principal: &Principal, bucket: Uuid, rules: Value) -> Result<Value> {
        transactions::set_bucket_cors(&self.write, principal, bucket, rules).await
    }
    async fn website(&self, bucket: Uuid) -> Result<Website> {
        Ok(sqlx::query_as(
            "SELECT website_enabled,index_document,error_document FROM buckets WHERE id=?",
        )
        .bind(bucket)
        .fetch_optional(&self.read)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?)
    }
    async fn set_website(
        &self,
        principal: &Principal,
        bucket: Uuid,
        input: Website,
    ) -> Result<Website> {
        transactions::set_bucket_website(&self.write, principal, bucket, input).await
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for BucketView {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        let mut result = BucketView::from(Bucket::from_row(row)?);
        result.actions = actions(row.try_get("actions")?)
            .into_iter()
            .map(|a| a.name().into())
            .collect();
        Ok(result)
    }
}
impl<'r> sqlx::FromRow<'r, SqliteRow> for BucketProject {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
        })
    }
}
impl<'r> sqlx::FromRow<'r, SqliteRow> for BucketStorage {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            storage_type: row.try_get("storage_type")?,
        })
    }
}
impl<'r> sqlx::FromRow<'r, SqliteRow> for Domain {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            host: row.try_get("host")?,
            bucket_id: row.try_get("bucket_id")?,
        })
    }
}
impl<'r> sqlx::FromRow<'r, SqliteRow> for Website {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            website_enabled: row.try_get("website_enabled")?,
            index_document: row.try_get("index_document")?,
            error_document: row.try_get("error_document")?,
        })
    }
}
impl<'r> sqlx::FromRow<'r, SqliteRow> for TransferProject {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            used_bytes: row.try_get("used_bytes")?,
            reserved_bytes: row.try_get("reserved_bytes")?,
            logical_bytes_limit: row.try_get("logical_bytes_limit")?,
            bucket_count: row.try_get("bucket_count")?,
            bucket_count_limit: row.try_get("bucket_count_limit")?,
            members: row.try_get("members")?,
        })
    }
}
impl<'r> sqlx::FromRow<'r, SqliteRow> for BucketImpact {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        let sqlx::types::Json(mut domains): sqlx::types::Json<Vec<String>> =
            row.try_get("domains")?;
        domains.sort();
        Ok(Self {
            bucket_id: row.try_get("bucket_id")?,
            name: row.try_get("name")?,
            project_id: row.try_get("project_id")?,
            state: row.try_get("state")?,
            revision: row.try_get("revision")?,
            uploads_paused: row.try_get("uploads_paused")?,
            objects: row.try_get("objects")?,
            logical_bytes: row.try_get("logical_bytes")?,
            reserved_bytes: row.try_get("reserved_bytes")?,
            inflight_bytes: row.try_get("inflight_bytes")?,
            active_uploads: row.try_get("active_uploads")?,
            writing_streams: row.try_get("writing_streams")?,
            member_bucket_grants: row.try_get("member_bucket_grants")?,
            service_grants: row.try_get("service_grants")?,
            token_bucket_grants: row.try_get("token_bucket_grants")?,
            domains,
        })
    }
}

use crate::model::object::{
    CatalogCursor, CatalogFilter, MediaItem, ObjectChange, ObjectStore, ObjectTransfer, Outcome,
    StorageMigration,
};

pub(super) fn insert_media_operation(
    id: Uuid,
    user: Uuid,
    token: Option<Uuid>,
    client: Uuid,
    hash: &[u8],
) -> Statement<'_> {
    sqlx::query("INSERT INTO media_operations(id,user_id,token_id,client_id,request_hash) VALUES(?,?,?,?,?) ON CONFLICT DO NOTHING").bind(id).bind(user).bind(token).bind(client).bind(hash)
}
pub(super) fn find_media_operation(
    user: Uuid,
    token: Option<Uuid>,
    client: Uuid,
) -> Rows<'static, (Uuid, Vec<u8>)> {
    sqlx::query_as("SELECT id,request_hash FROM media_operations WHERE user_id=? AND token_id IS ? AND client_id=?").bind(user).bind(token).bind(client)
}
pub(super) fn save_media_operation(id: Uuid, outcome: &Outcome) -> Statement<'_> {
    sqlx::query("UPDATE media_operations SET result=? WHERE id=? AND result IS NULL")
        .bind(sqlx::types::Json(outcome))
        .bind(id)
}
pub(super) fn partial_audit(id: Id) -> Statement<'static> {
    sqlx::query(
        "UPDATE audit_events SET outcome='partial',finished_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=?",
    )
    .bind(id)
}
pub(super) fn set_stream_public(id: Id, public: bool) -> Statement<'static> {
    sqlx::query("UPDATE streams SET public_read=? WHERE id=?")
        .bind(public)
        .bind(id)
}
pub(super) fn set_catalog_public(bucket: Uuid, key: &str, public: bool) -> Statement<'_> {
    sqlx::query("UPDATE objects SET catalog_public=? WHERE bucket_id=? AND object_key=?")
        .bind(public)
        .bind(bucket)
        .bind(key.as_bytes())
}
pub(super) fn move_stream(id: Id, bucket: Uuid, key: &str) -> Statement<'_> {
    sqlx::query("UPDATE streams SET bucket_id=?,object_key=? WHERE id=?")
        .bind(bucket)
        .bind(key.as_bytes())
        .bind(id)
}
pub(super) fn abandon_copy_stream(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET state='abandoned',owner_task_id=NULL,owner_execution_token=NULL,touched_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND state='writing'").bind(id)
}
pub(super) fn publish_migration_stream(id: Id, task: Execution) -> Statement<'static> {
    sqlx::query("UPDATE streams SET state='ready',owner_task_id=NULL,owner_execution_token=NULL,touched_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND state='writing' AND owner_task_id=? AND owner_execution_token=?").bind(id).bind(task.id).bind(task.token)
}
pub(super) fn complete_migration_task(task: Execution) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='completed',processed=processed+1,execution_token=NULL,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND state='running' AND execution_token=?").bind(task.id).bind(task.token)
}
pub(super) async fn browse_catalog(
    connection: &mut sqlx::SqliteConnection,
    bucket: Uuid,
    filter: &mut CatalogFilter,
    cursor: Option<CatalogCursor>,
    sizes: (Option<i64>, Option<i64>),
) -> Result<Vec<MediaItem>> {
    if filter.mode == "contains" && !filter.q.is_empty() {
        let mut run = 0;
        let usable = filter.q.chars().any(|c| {
            run = if c.is_alphanumeric() { run + 1 } else { 0 };
            run >= 3
        });
        if !usable {
            filter.mode = "prefix".into();
        }
    }
    let target = i64::try_from(filter.limit)
        .ok()
        .and_then(|limit| limit.checked_add(1))
        .ok_or_else(|| s3s::s3_error!(InvalidArgument))?;
    if filter.mode != "contains" || filter.q.is_empty() {
        return Ok(
            catalog_page(bucket, filter, cursor.as_ref(), sizes, target)?
                .build_query_as()
                .fetch_all(connection)
                .await?,
        );
    }
    let needle = filter.q.to_lowercase();
    let mut after = cursor;
    let mut result = Vec::new();
    loop {
        let rows: Vec<MediaItem> = catalog_page(bucket, filter, after.as_ref(), sizes, 128)?
            .build_query_as()
            .fetch_all(&mut *connection)
            .await?;
        let more = rows.len() == 128;
        if let Some(last) = rows.last() {
            after = Some(CatalogCursor {
                scope: String::new(),
                key: last.object_key.clone(),
                size: last.size.clone(),
                modified: last.modified_at,
                folder: None,
            });
        }
        for row in rows {
            let text = if filter.search_in == "name" {
                row.object_key.rsplit('/').next().unwrap_or("")
            } else {
                &row.object_key
            };
            if text.to_lowercase().contains(&needle) {
                result.push(row);
                if result.len() as i64 == target {
                    return Ok(result);
                }
            }
        }
        if !more {
            return Ok(result);
        }
    }
}

fn catalog_page(
    bucket: Uuid,
    filter: &CatalogFilter,
    cursor: Option<&CatalogCursor>,
    (min, max): (Option<i64>, Option<i64>),
    limit: i64,
) -> Result<sqlx::QueryBuilder<Sqlite>> {
    let mut query = sqlx::QueryBuilder::<Sqlite>::new(
        "SELECT stream_id AS id,object_key,CAST(catalog_size AS TEXT) size,catalog_type AS content_type,catalog_public AS public_read,catalog_modified AS modified_at FROM objects WHERE stream_id IS NOT NULL AND bucket_id=",
    );
    query.push_bind(bucket);
    if !filter.prefix.is_empty() {
        query
            .push(" AND object_key>=")
            .push_bind(filter.prefix.as_bytes());
        if let Some(end) = crate::app::listing::successor(&filter.prefix) {
            query.push(" AND object_key<").push_bind(end.into_bytes());
        }
    }
    if !filter.q.is_empty() {
        let prefix = format!("{}{}", filter.prefix, filter.q);
        match filter.mode.as_str() {
            "exact" => {
                query
                    .push(" AND object_key=")
                    .push_bind(prefix.into_bytes());
            }
            "prefix" => {
                query.push(" AND object_key>=").push_bind(prefix.as_bytes());
                if let Some(end) = crate::app::listing::successor(&prefix) {
                    query.push(" AND object_key<").push_bind(end.into_bytes());
                }
            }
            _ => {}
        }
    }
    if let Some(category) = &filter.category {
        query.push(" AND catalog_category=").push_bind(category);
    }
    if let Some(public) = filter.public {
        query.push(" AND catalog_public=").push_bind(public);
    }
    if let Some(min) = min {
        query.push(" AND catalog_size>=").push_bind(min);
    }
    if let Some(max) = max {
        query.push(" AND catalog_size<=").push_bind(max);
    }
    if let Some(since) = filter.since {
        query
            .push(" AND catalog_modified>=")
            .push_bind(since.timestamp_micros());
    }
    if let Some(until) = filter.until {
        query
            .push(" AND catalog_modified<=")
            .push_bind(until.timestamp_micros());
    }
    let column = match filter.sort.as_str() {
        "size" => "catalog_size",
        "modified" => "catalog_modified",
        _ => "object_key",
    };
    let comparison = if filter.order == "asc" { " > " } else { " < " };
    if let Some(cursor) = cursor {
        if column == "object_key" {
            query
                .push(" AND object_key")
                .push(comparison)
                .push_bind(cursor.key.as_bytes());
        } else {
            query
                .push(" AND (")
                .push(column)
                .push(",object_key)")
                .push(comparison)
                .push("(");
            if column == "catalog_size" {
                query.push_bind(
                    cursor
                        .size
                        .parse::<i64>()
                        .ok()
                        .filter(|n| *n >= 0)
                        .ok_or_else(|| s3s::s3_error!(InvalidArgument))?,
                );
            } else {
                query.push_bind(cursor.modified.timestamp_micros());
            }
            query.push(",").push_bind(cursor.key.as_bytes()).push(")");
        }
    }
    let direction = if filter.order == "asc" {
        " ASC"
    } else {
        " DESC"
    };
    query.push(" ORDER BY ").push(column).push(direction);
    if column != "object_key" {
        query.push(",object_key").push(direction);
    }
    query.push(" LIMIT ").push_bind(limit);
    Ok(query)
}

#[async_trait::async_trait]
impl ObjectStore for Backend {
    async fn browse(
        &self,
        authority: &crate::authorization::Permit,
        bucket: Uuid,
        filter: &mut CatalogFilter,
        cursor: Option<CatalogCursor>,
        sizes: (Option<i64>, Option<i64>),
    ) -> Result<Vec<MediaItem>> {
        transactions::browse_catalog(&self.read, authority, bucket, filter, cursor, sizes).await
    }
    async fn chunks(&self, stream: Id, after: i64, limit: i64) -> Result<Vec<Value>> {
        let rows: Vec<(Value,Option<Uuid>)> = sqlx::query_as("SELECT JSON_OBJECT('id',CAST(c.id AS TEXT),'offset_bytes',CAST(e.offset_bytes AS TEXT),'length',e.length,'source_offset',e.source_offset,'raw_size',c.raw_size,'stored_size',l.standalone_size,'independent_size_hint',c.standalone_size,'payload_size',c.standalone_size-IIF(c.encryption_algorithm='none',0,16),'compression',IIF(c.standalone_compressed,'zstd','none'),'algorithm',c.encryption_algorithm,'pack_id',CAST(c.pack_id AS TEXT),'source',CASE WHEN l.encoding_id IS NOT NULL THEN 'chunk' WHEN c.pack_id IS NOT NULL THEN 'pack' ELSE 'pending' END,'reads',CAST(COALESCE(a.read_count,0) AS TEXT),'range_reads',CAST(COALESCE(a.range_read_count,0) AS TEXT)),c.block_key_id FROM extents e JOIN chunks c ON c.id=e.chunk_id LEFT JOIN chunk_encodings l ON l.chunk_id=c.id AND l.state='ready' LEFT JOIN chunk_access_stats a ON a.chunk_id=c.id WHERE e.stream_id=? AND e.offset_bytes>? ORDER BY e.offset_bytes LIMIT ?").bind(stream).bind(after).bind(limit).fetch_all(&self.read).await?;
        rows.into_iter()
            .map(|(mut row, key)| {
                row["block_key_id"] = serde_json::to_value(key)?;
                Ok(row)
            })
            .collect()
    }
    async fn change(&self, input: ObjectChange) -> Result<bool> {
        transactions::change_object(&self.write, input).await
    }
    async fn begin_operation(
        &self,
        principal: &Principal,
        user: Uuid,
        client: Uuid,
        hash: &[u8],
    ) -> Result<(Uuid, Vec<u8>)> {
        transactions::begin_media_operation(&self.write, principal, user, client, hash).await
    }
    async fn operation(&self, id: Uuid) -> Result<Option<Outcome>> {
        let result: Option<sqlx::types::Json<Outcome>> =
            sqlx::query_scalar("SELECT result FROM media_operations WHERE id=?")
                .bind(id)
                .fetch_one(&self.read)
                .await?;
        Ok(result.map(|r| r.0))
    }
    async fn fail_operation(&self, id: Uuid, outcome: &Outcome) -> Result<()> {
        save_media_operation(id, outcome)
            .execute(&self.write)
            .await?;
        Ok(())
    }
    async fn audit_batch(&self, bucket: Uuid, detail: Value, partial: bool) -> Result<()> {
        transactions::audit_object_batch(&self.write, bucket, detail, partial).await
    }
    async fn storage(&self, bucket: Uuid) -> Result<Option<Uuid>> {
        Ok(
            sqlx::query_scalar("SELECT storage_id FROM buckets WHERE id=?")
                .bind(bucket)
                .fetch_optional(&self.read)
                .await?,
        )
    }
    async fn move_object(
        &self,
        permit: &crate::authorization::Permit,
        input: ObjectTransfer<'_>,
    ) -> Result<()> {
        transactions::move_object(&self.write, permit, input).await
    }
    async fn publish_copy(
        &self,
        permit: &crate::authorization::Permit,
        input: ObjectTransfer<'_>,
        stream: Id,
        public: bool,
        expected_public: Option<bool>,
    ) -> Result<()> {
        transactions::publish_copy(&self.write, permit, input, stream, public, expected_public)
            .await
    }
    async fn queue_migration(
        &self,
        permit: &crate::authorization::Permit,
        job: &StorageMigration,
    ) -> Result<()> {
        transactions::queue_storage_migration(&self.write, permit, job).await
    }
    async fn complete_migration(
        &self,
        task: Uuid,
        token: Uuid,
        stream: Id,
        job: &StorageMigration,
    ) -> Result<()> {
        transactions::complete_storage_migration(
            &self.write,
            Execution { id: task, token },
            stream,
            job,
        )
        .await
    }
    async fn abandon_copy(&self, stream: Id) -> Result<()> {
        transactions::abandon_copy(&self.write, stream).await
    }
    async fn abandon_migration(&self, stream: Id) -> Result<()> {
        transactions::abandon_copy(&self.write, stream).await
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for MediaItem {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            object_key: object_key(row)?,
            size: row.try_get("size")?,
            content_type: row.try_get("content_type")?,
            public_read: row.try_get("public_read")?,
            modified_at: date(row, "modified_at")?,
        })
    }
}

use crate::model::key::{ManagedKeyCreate, ManagedKeyMaterial, ManagedKeyView};

pub(super) fn managed_keys() -> Rows<'static, ManagedKeyMaterial> {
    sqlx::query_as(
        "SELECT id,name,purpose,key_algorithm,secret_encrypted,fingerprint FROM managed_keys WHERE secret_encrypted IS NOT NULL ORDER BY id",
    )
}
pub(super) fn required_keys() -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT DISTINCT block_key_id FROM (SELECT block_key_id FROM chunk_encodings WHERE encryption_algorithm<>'none' AND state<>'deleted' UNION SELECT block_key_id FROM packs WHERE encryption_algorithm<>'none' AND state<>'deleted' UNION SELECT block_key_id FROM chunks WHERE encryption_algorithm<>'none' AND EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=chunks.id)) sources WHERE block_key_id IS NOT NULL",
    )
}

use crate::model::runtime::{RuntimeState, RuntimeStore};
use crate::model::{ManagedKeyRecord, ManagedKeyStore};

#[async_trait::async_trait]
impl RuntimeStore for Backend {
    fn database_info(&self) -> Option<&crate::model::runtime::DatabaseInfo> {
        Some(&self.database_info)
    }
    async fn initialize(&self, maintenance: bool) -> Result<RuntimeState> {
        if maintenance {
            sqlx::query("UPDATE mokyu_meta SET maintenance=TRUE WHERE id=1")
                .execute(&self.write)
                .await?;
        }
        self.state().await
    }
    async fn state(&self) -> Result<RuntimeState> {
        let (gc_paused, maintenance, pack_creation_paused) = sqlx::query_as(
            "SELECT gc_paused,maintenance,pack_creation_paused FROM mokyu_meta WHERE id=1",
        )
        .fetch_one(&self.read)
        .await?;
        Ok(RuntimeState {
            gc_paused,
            maintenance,
            pack_creation_paused,
            mode: "single".into(),
            mode_revision: 0,
        })
    }
    async fn pending_uploads(&self) -> Result<bool> {
        Ok(
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_uploads)")
                .fetch_one(&self.read)
                .await?,
        )
    }
    async fn health(&self) -> Result<()> {
        sqlx::query("SELECT 1").execute(&self.read).await?;
        Ok(())
    }
    fn pool(&self) -> (u32, usize) {
        let writer = (self.write.size(), self.write.num_idle());
        if self.shared_pool {
            writer
        } else {
            (writer.0 + self.read.size(), writer.1 + self.read.num_idle())
        }
    }
}

const KEY_REFERENCES: &str = "EXISTS(SELECT 1 FROM storages s WHERE s.block_key_id=k.id) OR EXISTS(SELECT 1 FROM chunk_encodings e WHERE e.block_key_id=k.id AND e.state<>'deleted') OR EXISTS(SELECT 1 FROM packs p WHERE p.block_key_id=k.id AND p.state<>'deleted') OR EXISTS(SELECT 1 FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE c.block_key_id=k.id) OR EXISTS(SELECT 1 FROM tasks t WHERE replace(lower(json_extract(t.detail,'$.target_key_id')),'-','')=lower(hex(k.id)) AND t.state IN ('queued','running','paused','failed'))";

pub(super) async fn managed_key_views(
    connection: &mut sqlx::SqliteConnection,
) -> Result<Vec<ManagedKeyView>> {
    let mut query = managed_key_query();
    query.push(" ORDER BY k.name");
    Ok(query.build_query_as().fetch_all(connection).await?)
}

fn managed_key_query() -> sqlx::QueryBuilder<Sqlite> {
    let mut query =
        sqlx::QueryBuilder::new("SELECT k.id,k.name,k.purpose,k.key_algorithm AS algorithm,");
    query.push(KEY_REFERENCES).push(" AS in_use,LOWER(HEX(k.fingerprint)) AS fingerprint,k.created_at,k.revoked_at FROM managed_keys k");
    query
}

pub(super) async fn managed_key_view(
    connection: &mut sqlx::SqliteConnection,
    id: Uuid,
) -> Result<ManagedKeyView> {
    let mut query = managed_key_query();
    query.push(" WHERE k.id=").push_bind(id);
    Ok(query.build_query_as().fetch_one(connection).await?)
}

pub(super) async fn managed_key_in_use(
    connection: &mut sqlx::SqliteConnection,
    id: Uuid,
) -> Result<bool> {
    let mut query = sqlx::QueryBuilder::new("SELECT ");
    query
        .push(KEY_REFERENCES)
        .push(" FROM managed_keys k WHERE k.id=")
        .push_bind(id);
    Ok(query.build_query_scalar().fetch_one(connection).await?)
}

pub(super) fn insert_managed_key(input: &ManagedKeyCreate) -> Statement<'_> {
    sqlx::query("INSERT INTO managed_keys(id,name,purpose,key_algorithm,secret_encrypted,fingerprint) VALUES(?,?,?,?,?,?)").bind(input.id).bind(input.name.trim()).bind(&input.purpose).bind(&input.algorithm).bind(&input.protected).bind(input.fingerprint.as_slice())
}

pub(super) fn lock_managed_key(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM managed_keys WHERE id=?").bind(id)
}

pub(super) fn revoke_managed_key(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE managed_keys SET revoked_at=COALESCE(revoked_at,CAST(unixepoch('subsec')*1000000 AS INTEGER)),secret_encrypted=NULL WHERE id=?").bind(id)
}

#[async_trait::async_trait]
impl ManagedKeyStore for Backend {
    async fn replacement_preview(
        &self,
        principal: &Principal,
        source: Uuid,
        input: KeyReplacementInput,
    ) -> Result<KeyReplacementPreview> {
        transactions::key_replacement_preview(&self.write, principal, source, input).await
    }
    async fn replacement(&self, principal: &Principal, id: Uuid) -> Result<ReplacementRow> {
        transactions::key_replacement(&self.write, principal, id).await
    }
    async fn replace(
        &self,
        principal: &Principal,
        id: Uuid,
        input: Option<ManagedKeyCreate>,
    ) -> Result<KeyReplacementResult> {
        transactions::replace_keys(&self.write, principal, id, input).await
    }

    async fn nonce_observations(&self) -> Result<Vec<crate::model::key_usage::NonceRow>> {
        Ok(sqlx::query_as("SELECT k.id,k.name,c.reserved_id,p.reserved_id,(SELECT count(*) FROM storages s WHERE s.block_key_id=k.id),EXISTS(SELECT 1 FROM chunk_encodings e WHERE e.block_key_id=k.id AND e.state='uploading') OR EXISTS(SELECT 1 FROM packs p WHERE p.block_key_id=k.id AND p.state='preparing') OR EXISTS(SELECT 1 FROM chunks c WHERE c.block_key_id=k.id AND c.state IN ('preparing','uploading')) OR EXISTS(SELECT 1 FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE c.block_key_id=k.id) OR EXISTS(SELECT 1 FROM tasks t WHERE replace(lower(json_extract(t.detail,'$.target_key_id')),'-','')=lower(hex(k.id)) AND t.state IN ('queued','running','paused','failed')),o.data,o.revision FROM managed_keys k LEFT JOIN nonce_counters c ON c.block_key_id=k.id AND c.type_tag=0 LEFT JOIN nonce_counters p ON p.block_key_id=k.id AND p.type_tag=1 LEFT JOIN nonce_observations o ON o.block_key_id=k.id WHERE k.purpose='block' AND k.revoked_at IS NULL ORDER BY k.id").fetch_all(&self.read).await?)
    }
    async fn save_nonce_history(
        &self,
        key: Uuid,
        previous: Option<i64>,
        history: crate::model::key_usage::NonceHistory,
    ) -> Result<()> {
        sqlx::query("INSERT INTO nonce_observations(block_key_id,data,revision) VALUES(?,?,1) ON CONFLICT(block_key_id) DO UPDATE SET data=excluded.data,revision=nonce_observations.revision+1 WHERE nonce_observations.revision=?").bind(key).bind(sqlx::types::Json(history)).bind(previous.unwrap_or(0)).execute(&self.write).await?;
        Ok(())
    }

    async fn reserve_nonce(&self, key: Uuid, type_tag: u8, units: u32) -> Result<(i64, i64)> {
        transactions::reserve_nonce(&self.write, key, type_tag, units).await
    }

    async fn resolve_block(&self, name: &str) -> Result<Option<ManagedKeyRecord>> {
        Ok(sqlx::query_as("SELECT id,name,key_algorithm AS algorithm FROM managed_keys WHERE name=? AND purpose='block' AND revoked_at IS NULL").bind(name).fetch_optional(&self.read).await?)
    }
    async fn active_block(&self, id: Uuid) -> Result<Option<(String, String)>> {
        Ok(sqlx::query_as("SELECT name,key_algorithm FROM managed_keys WHERE id=? AND purpose='block' AND revoked_at IS NULL").bind(id).fetch_optional(&self.read).await?)
    }
    async fn for_storage(&self, storage: Uuid) -> Result<Option<ManagedKeyRecord>> {
        Ok(sqlx::query_as("SELECT k.id,k.name,k.key_algorithm AS algorithm FROM storages s JOIN managed_keys k ON k.id=s.block_key_id WHERE s.id=? AND k.purpose='block' AND k.revoked_at IS NULL").bind(storage).fetch_optional(&self.read).await?)
    }
    async fn material(&self, name: &str) -> Result<Option<ManagedKeyMaterial>> {
        Ok(sqlx::query_as("SELECT id,name,purpose,key_algorithm,secret_encrypted,fingerprint FROM managed_keys WHERE name=? AND secret_encrypted IS NOT NULL").bind(name).fetch_optional(&self.read).await?)
    }
    async fn list(&self, principal: &Principal) -> Result<Vec<ManagedKeyView>> {
        transactions::list_managed_keys(&self.read, principal).await
    }
    async fn create(
        &self,
        principal: &Principal,
        input: ManagedKeyCreate,
    ) -> Result<ManagedKeyView> {
        transactions::create_managed_key(&self.write, principal, input).await
    }
    async fn revoke(&self, principal: &Principal, id: Uuid) -> Result<()> {
        transactions::revoke_managed_key(&self.write, principal, id).await
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for ManagedKeyView {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            nonce_capacity: None,
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            purpose: row.try_get("purpose")?,
            algorithm: row.try_get("algorithm")?,
            in_use: row.try_get("in_use")?,
            fingerprint: row.try_get("fingerprint")?,
            created_at: date(row, "created_at")?,
            revoked_at: optional_date(row, "revoked_at")?,
        })
    }
}
impl<'r> sqlx::FromRow<'r, SqliteRow> for ManagedKeyRecord {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            algorithm: row.try_get("algorithm")?,
        })
    }
}
impl<'r> sqlx::FromRow<'r, SqliteRow> for ManagedKeyMaterial {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            purpose: row.try_get("purpose")?,
            algorithm: row.try_get("key_algorithm")?,
            encrypted: row.try_get("secret_encrypted")?,
            fingerprint: row.try_get("fingerprint")?,
        })
    }
}

use crate::model::task::{JobQuery, TaskControlStore};

pub(super) fn task_value(row: SqliteRow, detailed: bool) -> Result<Value> {
    let mut value = serde_json::json!({
        "id": row.try_get::<Uuid,_>("id")?,
        "task_type": row.try_get::<String,_>("task_type")?,
        "bucket_id": row.try_get::<Option<Uuid>,_>("bucket_id")?,
        "state": row.try_get::<String,_>("state")?,
        "cursor": row.try_get::<Option<String>,_>("progress_cursor")?,
        "processed": row.try_get::<i64,_>("processed")?,
        "detail": row.try_get::<Value,_>("detail")?,
        "error": row.try_get::<Option<String>,_>("error")?,
        "created_by_label": row.try_get::<String,_>("created_by_label")?,
        "source": row.try_get::<String,_>("source")?,
        "created_at": date(&row,"created_at")?,
        "updated_at": date(&row,"updated_at")?,
        "started_at": optional_date(&row,"started_at")?,
    });
    if detailed {
        value["bucket_name"] = row.try_get::<Option<String>, _>("bucket_name")?.into();
        value["blocked"] = row.try_get::<Option<String>, _>("blocked")?.into();
    }
    Ok(value)
}

const JOB_SQL: &str = "SELECT t.*,b.name AS bucket_name,CASE WHEN c.paused OR (c.policy='gc' AND m.gc_paused) THEN 'PolicyPaused' WHEN m.maintenance AND t.task_type IN ('pack','unpack','purge','gc','upload','storage_migration','rewrite','usage') THEN 'MaintenanceMode' WHEN m.pack_creation_paused AND t.task_type='pack' AND json_extract(t.detail,'$.policy') IN ('pack','repack') THEN 'PackCreationStopped' END AS blocked FROM tasks t LEFT JOIN buckets b ON b.id=t.bucket_id CROSS JOIN mokyu_meta m LEFT JOIN maintenance_controls c ON c.policy=CASE WHEN t.task_type='pack' THEN json_extract(t.detail,'$.policy') ELSE t.task_type END";

pub(super) fn task_page(
    q: &JobQuery,
    cursor: Option<(DateTime<Utc>, Uuid)>,
    limit: i64,
) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new(JOB_SQL);
    query.push(" WHERE TRUE");
    if let Some(state) = &q.state {
        query.push(" AND t.state=").push_bind(state);
    }
    if let Some(policy) = &q.policy {
        query.push(" AND t.policy=").push_bind(policy);
    }
    if let Some(bucket) = q.bucket {
        query.push(" AND t.bucket_id=").push_bind(bucket);
    }
    if let Some(storage) = q.storage {
        query.push(" AND (b.storage_id=").push_bind(storage)
            .push(" OR json_extract(t.detail,'$.storage_id')=").push_bind(storage.to_string())
            .push(" OR EXISTS(SELECT 1 FROM json_each(t.detail,'$.storages') item WHERE json_extract(IIF(item.type='object',item.value,'{}'),'$.storage_id')=").push_bind(storage.to_string()).push("))");
    }
    if let Some((time, id)) = cursor {
        query
            .push(" AND (t.created_at<")
            .push_bind(time.timestamp_micros())
            .push(" OR (t.created_at=")
            .push_bind(time.timestamp_micros())
            .push(" AND t.id<")
            .push_bind(id)
            .push("))");
    }
    query
        .push(" ORDER BY t.created_at DESC,t.id DESC LIMIT ")
        .push_bind(limit);
    query
}

pub(super) fn task_view(id: Uuid) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new(JOB_SQL);
    query.push(" WHERE t.id=").push_bind(id);
    query
}

#[async_trait::async_trait]
impl TaskControlStore for Backend {
    async fn unpack_status(&self, pack: Id) -> Result<Option<Value>> {
        sqlx::query("SELECT * FROM tasks WHERE task_type='unpack' AND detail->>'$.pack_id'=? ORDER BY created_at DESC,id DESC LIMIT 1")
            .bind(pack).fetch_optional(&self.read).await?.map(|row| task_value(row,false)).transpose()
    }
    async fn rewrite_status(&self, storage: Uuid) -> Result<Option<Value>> {
        sqlx::query("SELECT * FROM tasks WHERE task_type='rewrite' AND detail->>'$.storage_id'=? ORDER BY created_at DESC,id DESC LIMIT 1")
            .bind(storage.to_string()).fetch_optional(&self.read).await?.map(|row| task_value(row,false)).transpose()
    }
    async fn rewrite_task(&self, storage: Uuid) -> Result<Option<Uuid>> {
        Ok(sqlx::query_scalar("SELECT id FROM tasks WHERE task_type='rewrite' AND detail->>'$.storage_id'=? ORDER BY created_at DESC,id DESC LIMIT 1")
            .bind(storage.to_string()).fetch_optional(&self.read).await?)
    }
    async fn page(
        &self,
        principal: &Principal,
        filter: JobQuery,
        cursor: Option<(DateTime<Utc>, Uuid)>,
        limit: i64,
    ) -> Result<Vec<Value>> {
        transactions::task_page(&self.read, principal, filter, cursor, limit).await
    }
    async fn view(&self, principal: &Principal, id: Uuid) -> Result<Value> {
        transactions::task_view(&self.read, principal, id).await
    }
    async fn all(&self) -> Result<Vec<Value>> {
        sqlx::query("SELECT * FROM tasks ORDER BY created_at DESC,id DESC LIMIT 100")
            .fetch_all(&self.read)
            .await?
            .into_iter()
            .map(|row| task_value(row, false))
            .collect()
    }
    async fn raw(&self, id: Uuid) -> Result<Option<Value>> {
        sqlx::query("SELECT * FROM tasks WHERE id=?")
            .bind(id)
            .fetch_optional(&self.read)
            .await?
            .map(|row| task_value(row, false))
            .transpose()
    }
    async fn other_integrity(&self, id: Uuid) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE task_type='integrity' AND json_extract(detail,'$.source_operation') IS NULL AND state IN ('queued','running') AND id<>?)").bind(id).fetch_one(&self.read).await?)
    }
    async fn enqueue(&self, task_type: &str, bucket: Option<Uuid>, detail: Value) -> Result<Uuid> {
        transactions::enqueue_task(&mut *self.write.acquire().await?, task_type, bucket, detail)
            .await
    }
    async fn sweep_preview(&self, id: Uuid) -> Result<Option<Value>> {
        Ok(sqlx::query_scalar(
            "SELECT detail FROM tasks WHERE id=? AND task_type='sweep' AND state='completed'",
        )
        .bind(id)
        .fetch_optional(&self.read)
        .await?)
    }
    async fn pause(&self, task: Execution) -> Result<()> {
        sqlx::query("UPDATE tasks SET state='paused',updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND execution_token=? AND state IN ('queued','running')").bind(task.id).bind(task.token).execute(&self.write).await?;
        Ok(())
    }
    async fn running(&self, task: Execution) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE id=? AND state='running' AND execution_token=?)").bind(task.id).bind(task.token).fetch_one(&self.read).await?)
    }
    async fn complete(&self, task: Execution) -> Result<()> {
        sqlx::query("UPDATE tasks SET state='completed',updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND execution_token=? AND state='running'").bind(task.id).bind(task.token).execute(&self.write).await?;
        Ok(())
    }
    async fn progress(
        &self,
        task: Execution,
        cursor: &str,
        processed: i64,
        detail: Value,
    ) -> Result<()> {
        sqlx::query("UPDATE tasks SET progress_cursor=?,processed=processed+?,detail=?,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND execution_token=? AND state='running'").bind(cursor).bind(processed).bind(detail).bind(task.id).bind(task.token).execute(&self.write).await?;
        Ok(())
    }
    async fn load(&self, id: Uuid) -> Result<Option<(bool, String, Value)>> {
        Ok(
            sqlx::query_as::<_, (String, Value)>("SELECT task_type,detail FROM tasks WHERE id=?")
                .bind(id)
                .fetch_optional(&self.read)
                .await?
                .map(|(task_type, detail)| (false, task_type, detail)),
        )
    }
    async fn change_state(&self, id: Uuid, parent: bool, resume: bool) -> Result<bool> {
        ensure!(!parent, "SQLite does not support child tasks");
        let query = if resume {
            "UPDATE tasks SET state='queued',error=NULL,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER),detail=CASE WHEN json_type(detail,'$.profile_revision') IS NOT NULL THEN COALESCE((SELECT json_set(tasks.detail,'$.profile_revision',p.revision) FROM storage_profiles p WHERE p.storage_id=unhex(replace(json_extract(tasks.detail,'$.storage_id'),'-',''))),detail) ELSE detail END WHERE id=? AND state IN ('paused','failed')"
        } else {
            "UPDATE tasks SET state='paused',updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND state IN ('queued','running')"
        };
        Ok(sqlx::query(query)
            .bind(id)
            .execute(&self.write)
            .await?
            .rows_affected()
            == 1)
    }
}

use crate::model::Mapping;
use crate::model::cache::{CacheBatch, CacheRelease, CacheStage, UploadCacheStore};

pub(super) fn lock_cache_stream(stream: Id, storage: Uuid) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT s.id FROM streams s JOIN buckets b ON b.id=s.bucket_id WHERE s.id=? AND s.state='writing' AND b.storage_id=?").bind(stream).bind(storage)
}

pub(super) fn lock_cache_source(chunk: Id) -> Rows<'static, (String, Option<Id>, Uuid, Id)> {
    sqlx::query_as("SELECT state,pack_id,storage_id,current_encoding_id FROM chunks WHERE id=?")
        .bind(chunk)
}
pub(super) fn insert_pending_upload<'q>(
    input: &'q CacheStage<'q>,
    source: Option<Id>,
    timeout: i64,
) -> Statement<'q> {
    sqlx::query("INSERT INTO pending_uploads(chunk_id,stream_id,offset_bytes,staged_size,cache_key,source_pack_id,next_retry_at) VALUES(?,?,?,?,?,?,CAST(unixepoch('subsec')*1000000 AS INTEGER)+?)")
        .bind(input.chunk.id).bind(input.stream).bind(input.offset).bind(input.size).bind(input.key).bind(source).bind(timeout)
}
pub(super) fn ready_cached_chunk(chunk: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunks SET state='ready',unreferenced_at=NULL,owner_stream=NULL,reference_changed_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=?").bind(chunk)
}
pub(super) fn seal_stream_uploads(stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE pending_uploads SET next_retry_at=MIN(next_retry_at,CAST(unixepoch('subsec')*1000000 AS INTEGER)) WHERE owner_task IS NULL AND chunk_id IN (SELECT chunk_id FROM extents WHERE stream_id=?)").bind(stream)
}
pub(super) fn unsealed_uploads(stream: Id) -> Rows<'static, (Id, i64, i32, String)> {
    sqlx::query_as("SELECT p.chunk_id,p.offset_bytes,c.raw_size,sp.pack_max_size FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id JOIN storage_profiles sp ON sp.storage_id=c.storage_id WHERE p.stream_id=? AND p.owner_task IS NULL ORDER BY p.offset_bytes LIMIT 1026").bind(stream)
}
pub(super) fn seal_upload_chunks(ids: &[Id]) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new(
        "UPDATE pending_uploads SET next_retry_at=MIN(next_retry_at,CAST(unixepoch('subsec')*1000000 AS INTEGER)) WHERE owner_task IS NULL AND chunk_id IN ",
    );
    chunk_ids(&mut query, ids);
    query
}
pub(super) fn active_cache_flush() -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM tasks WHERE task_type='cache_flush' AND state IN ('queued','running') LIMIT 1",
    )
}
pub(super) fn retryable_upload() -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT p.owner_task FROM pending_uploads p JOIN tasks t ON t.id=p.owner_task WHERE p.next_retry_at<=CAST(unixepoch('subsec')*1000000 AS INTEGER) AND t.task_type='upload' AND t.state='failed' ORDER BY p.next_retry_at,p.chunk_id LIMIT 1",
    )
}
pub(super) fn queue_upload_retry(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='queued',error=NULL,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND state='failed'").bind(id)
}
pub(super) fn due_upload() -> Rows<'static, (Id, Option<Id>, Option<Id>)> {
    sqlx::query_as(
        "SELECT chunk_id,stream_id,source_pack_id FROM pending_uploads WHERE owner_task IS NULL AND next_retry_at<=CAST(unixepoch('subsec')*1000000 AS INTEGER) ORDER BY next_retry_at,chunk_id LIMIT 1",
    )
}
pub(super) fn pending_upload_segment(
    first: Id,
    stream: Option<Id>,
    source: Option<Id>,
) -> Rows<'static, (Id, i64, i32)> {
    sqlx::query_as("SELECT p.chunk_id,p.offset_bytes,c.raw_size FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id WHERE p.owner_task IS NULL AND (p.chunk_id=? OR (p.stream_id=? AND p.source_pack_id IS ?)) ORDER BY p.offset_bytes LIMIT 1026")
        .bind(first).bind(stream).bind(source)
}
pub(super) fn claim_pending_uploads(ids: &[Id], task: Uuid) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new("UPDATE pending_uploads SET owner_task=");
    query.push_bind(task).push(" WHERE chunk_id IN ");
    chunk_ids(&mut query, ids);
    query
}
pub(super) fn flush_candidate(upper: DateTime<Utc>, task: Uuid) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT chunk_id FROM pending_uploads u WHERE created_at<=? AND NOT EXISTS(SELECT 1 FROM pack_inputs WHERE chunk_id=u.chunk_id) AND (owner_task IS NULL OR owner_task=? OR owner_task IN (SELECT id FROM tasks WHERE state IN ('paused','failed'))) ORDER BY created_at,chunk_id LIMIT 1")
        .bind(upper.timestamp_micros()).bind(task)
}
pub(super) fn flush_pending(upper: DateTime<Utc>) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_uploads WHERE created_at<=?)")
        .bind(upper.timestamp_micros())
}
pub(super) fn upload_is_durable(id: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE chunk_id=? AND (EXISTS(SELECT 1 FROM chunk_encodings WHERE chunk_id=c.id AND state='ready') OR (c.pack_id IS NOT NULL AND NOT(c.pack_id IS u.source_pack_id))))").bind(id)
}
pub(super) fn upload_is_unused(id: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=?)").bind(id)
}
pub(super) fn pending_upload(
    id: Id,
    timeout: i64,
) -> Rows<'static, (Option<Id>, i64, Option<Id>, bool)> {
    sqlx::query_as("SELECT stream_id,offset_bytes,source_pack_id,created_at<=CAST(unixepoch('subsec')*1000000 AS INTEGER)-? FROM pending_uploads WHERE chunk_id=?").bind(timeout).bind(id)
}
pub(super) fn pending_pack_uploads(pack: Id, task: Uuid) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT chunk_id FROM pending_uploads WHERE source_pack_id=? AND (owner_task IS NULL OR owner_task=?) ORDER BY chunk_id").bind(pack).bind(task)
}
pub(super) fn pending_upload_mappings(
    first: Id,
    stream: Option<Id>,
    offset: i64,
    task: Uuid,
    flush: bool,
) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new("SELECT ");
    query.push(CHUNK_COLUMNS).push(",p.offset_bytes,c.pack_id FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' WHERE (p.chunk_id=").push_bind(first)
        .push(" OR (p.stream_id=").push_bind(stream).push(" AND p.offset_bytes>").push_bind(offset).push(" AND p.source_pack_id IS NULL)) AND (p.owner_task IS NULL OR p.owner_task=").push_bind(task)
        .push(" OR (").push_bind(flush).push(" AND p.owner_task IN (SELECT id FROM tasks WHERE state IN ('paused','failed')))) ORDER BY p.offset_bytes LIMIT 1026");
    query
}
pub(super) fn same_references(a: Id, b: Id, distance: i64) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM extents a JOIN chunks c ON c.id=a.chunk_id WHERE a.chunk_id=? AND (a.source_offset<>0 OR a.length<>c.raw_size OR NOT EXISTS(SELECT 1 FROM extents b JOIN chunks d ON d.id=b.chunk_id WHERE b.chunk_id=? AND b.stream_id=a.stream_id AND b.offset_bytes=a.offset_bytes+? AND b.source_offset=0 AND b.length=d.raw_size))) AND NOT EXISTS(SELECT 1 FROM extents b JOIN chunks c ON c.id=b.chunk_id WHERE b.chunk_id=? AND (b.source_offset<>0 OR b.length<>c.raw_size OR NOT EXISTS(SELECT 1 FROM extents a JOIN chunks d ON d.id=a.chunk_id WHERE a.chunk_id=? AND a.stream_id=b.stream_id AND a.offset_bytes=b.offset_bytes-? AND a.source_offset=0 AND a.length=d.raw_size)))")
        .bind(a).bind(b).bind(distance).bind(b).bind(a).bind(distance)
}
pub(super) fn unclaim_uploads(task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE pending_uploads SET owner_task=NULL WHERE owner_task=?").bind(task)
}
pub(super) fn retire_task_packs(task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE packs SET state='retired',unreferenced_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE owner_task=? AND state='preparing'").bind(task)
}
pub(super) fn retire_task_encodings(task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET state='retired',unreferenced_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE owner_task=? AND state='uploading'").bind(task)
}
pub(super) fn release_task_inputs(task: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM pack_inputs WHERE task_id=?").bind(task)
}
pub(super) fn lock_pending_chunks(ids: &[Id]) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT c.id,u.cache_key FROM chunks c LEFT JOIN pending_uploads u ON u.chunk_id=c.id WHERE c.id IN ",
    );
    chunk_ids(&mut query, ids);
    query.push(" ORDER BY c.id");
    query
}
pub(super) fn can_release_pending(id: Id, existing_pack: bool) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM chunk_encodings WHERE chunk_id=? AND state='ready') OR EXISTS(SELECT 1 FROM chunks c JOIN packs p ON p.id=c.pack_id WHERE c.id=? AND p.state='ready' AND (? OR NOT(c.pack_id IS (SELECT source_pack_id FROM pending_uploads WHERE chunk_id=c.id)))) OR NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=?)")
        .bind(id).bind(id).bind(existing_pack).bind(id)
}
pub(super) fn schedule_uploaded_pack(id: Id) -> Statement<'static> {
    sqlx::query("INSERT INTO pack_maintenance(stream_id,reason) SELECT DISTINCT e.stream_id,'pack' FROM extents e JOIN streams s ON s.id=e.stream_id JOIN chunks c ON c.id=e.chunk_id WHERE e.chunk_id=? AND s.state='ready' AND c.pack_id IS NULL ON CONFLICT(stream_id) DO UPDATE SET progress_cursor=0,generation=pack_maintenance.generation+1,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)").bind(id)
}
pub(super) fn delete_upload_pins(id: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM cache_pins WHERE chunk_id=?").bind(id)
}
pub(super) fn delete_pending_upload(id: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM pending_uploads WHERE chunk_id=?").bind(id)
}
pub(super) fn owned_upload_pins(
    names: &[String],
    storage: Option<Uuid>,
) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT u.cache_key FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE u.cache_key IN (",
    );
    let mut list = query.separated(",");
    if names.is_empty() {
        list.push("NULL");
    }
    for name in names {
        list.push_bind(name.clone());
    }
    query.push(")");
    if let Some(storage) = storage {
        query.push(" AND c.storage_id=").push_bind(storage);
    }
    query.push(" ORDER BY u.chunk_id");
    query
}

#[async_trait::async_trait]
impl UploadCacheStore for Backend {
    async fn bypass(&self, stream: Id) -> Result<bool> {
        Ok(
            sqlx::query_scalar("SELECT upload_cache_bypass FROM streams WHERE id=?")
                .bind(stream)
                .fetch_one(&self.read)
                .await?,
        )
    }
    async fn disable(&self, stream: Id) -> Result<()> {
        sqlx::query("UPDATE streams SET upload_cache_bypass=TRUE WHERE id=?")
            .bind(stream)
            .execute(&self.write)
            .await?;
        Ok(())
    }
    async fn stage(&self, input: CacheStage<'_>) -> Result<bool> {
        transactions::cache_stage(&self.write, input).await
    }
    async fn seal(&self, stream: Id, eof: bool) -> Result<bool> {
        transactions::cache_seal(&self.read, &self.write, stream, eof).await
    }
    async fn status(&self) -> Result<(Value, Vec<Value>)> {
        transactions::cache_status(&self.read).await
    }
    async fn flush(&self) -> Result<(Uuid, bool)> {
        transactions::cache_flush(&self.write).await
    }
    async fn enqueue(&self) -> Result<bool> {
        if retryable_upload()
            .fetch_optional(&self.read)
            .await?
            .is_none()
            && due_upload().fetch_optional(&self.read).await?.is_none()
        {
            return Ok(false);
        }
        transactions::cache_enqueue(&self.write).await
    }
    async fn next_attempt(&self) -> Result<Option<DateTime<Utc>>> {
        let micros: Option<i64> = sqlx::query_scalar("SELECT MIN(p.next_retry_at) FROM pending_uploads p LEFT JOIN tasks t ON t.id=p.owner_task WHERE p.owner_task IS NULL OR (t.task_type='upload' AND t.state='failed')").fetch_one(&self.read).await?;
        Ok(micros.map(datetime).transpose()?)
    }
    async fn pause(&self, task: Execution) -> Result<()> {
        TaskControlStore::pause(self, task).await
    }
    async fn batch(&self, task: Execution, detail: &Value, flush: bool) -> Result<CacheBatch> {
        transactions::cache_batch(&self.write, task, detail, flush).await
    }
    async fn metadata(&self, stream: Id) -> Result<Option<(Value, String)>> {
        sqlx::query("SELECT metadata,object_key FROM streams WHERE id=?")
            .bind(stream)
            .fetch_optional(&self.read)
            .await?
            .map(|r| Ok((r.try_get("metadata")?, object_key(&r)?)))
            .transpose()
    }
    async fn processed(&self, task: Execution) -> Result<()> {
        sqlx::query("UPDATE tasks SET processed=processed+1,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND state='running' AND execution_token=?").bind(task.id).bind(task.token).execute(&self.write).await?;
        Ok(())
    }
    async fn finish(&self, task: Execution) -> Result<()> {
        transactions::cache_finish(&self.write, task).await
    }
    async fn release(&self, ids: &[Id], existing_pack: bool) -> Result<CacheRelease> {
        transactions::cache_release(&self.write, ids, existing_pack).await
    }
    async fn pins(&self, names: &[String], storage: Option<Uuid>) -> Result<Vec<String>> {
        Ok(owned_upload_pins(names, storage)
            .build_query_scalar()
            .fetch_all(&self.read)
            .await?)
    }
    async fn unused(&self, storage: Option<Uuid>) -> Result<Vec<Id>> {
        Ok(sqlx::query_scalar("SELECT u.chunk_id FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE (? IS NULL OR c.storage_id=?) AND u.owner_task IS NULL AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=u.chunk_id) ORDER BY u.chunk_id LIMIT 128").bind(storage).bind(storage).fetch_all(&self.read).await?)
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for Mapping {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            chunk: sqlx::FromRow::from_row(row)?,
            offset_bytes: row.try_get("offset_bytes")?,
            pack_id: row.try_get("pack_id")?,
        })
    }
}

pub(super) async fn cache_status(
    connection: &mut sqlx::SqliteConnection,
) -> Result<(Value, Vec<Value>)> {
    let (entries,bytes,oldest,failed): (i64,i64,Option<i64>,i64)=sqlx::query_as("SELECT COUNT(*),CAST(COALESCE(SUM(staged_size),0) AS INTEGER),MIN(created_at),CAST(COALESCE(SUM(last_error IS NOT NULL),0) AS INTEGER) FROM pending_uploads").fetch_one(&mut *connection).await?;
    let rows=sqlx::query("SELECT p.chunk_id,p.pin_type,p.owner_id,p.created_at,u.last_error,u.attempts,u.next_retry_at FROM cache_pins p JOIN pending_uploads u USING(chunk_id) ORDER BY p.created_at,p.chunk_id,p.owner_id LIMIT 100").fetch_all(&mut *connection).await?;
    let pins=rows.into_iter().map(|r| Ok(serde_json::json!({
            "chunk_id":r.try_get::<i64,_>("chunk_id")?.to_string(),"pin_type":r.try_get::<String,_>("pin_type")?,
            "owner_id":r.try_get::<Id,_>("owner_id")?,"created_at":date(&r,"created_at")?,
            "last_error":r.try_get::<Option<String>,_>("last_error")?,"attempts":r.try_get::<i32,_>("attempts")?,
            "next_retry_at":date(&r,"next_retry_at")?
        }))).collect::<sqlx::Result<Vec<_>>>()?;
    Ok((
        serde_json::json!({"entries":entries,"bytes":bytes,"oldest_at":oldest.map(datetime).transpose()?,"failed_entries":failed}),
        pins,
    ))
}

use crate::model::task::{TaskClaim, TaskExecutionStore, TaskFinish, TaskRecord};

pub(super) fn claim_task_candidate(class: i32, active: &[Uuid]) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT t.id FROM tasks t CROSS JOIN mokyu_meta m WHERE t.state='queued' AND CASE WHEN t.task_type IN ('pack','unpack') THEN 1 WHEN t.task_type IN ('upload','cache_flush','storage_migration','rewrite') THEN 2 ELSE 0 END=",
    );
    query.push_bind(class);
    if !active.is_empty() {
        query.push(" AND t.id NOT IN (");
        let mut ids = query.separated(",");
        for id in active {
            ids.push_bind(*id);
        }
        query.push(")");
    }
    query.push(" AND NOT(m.maintenance AND t.task_type IN ('pack','unpack','purge','gc','storage_migration','rewrite','usage')) AND NOT EXISTS(SELECT 1 FROM maintenance_controls c WHERE c.policy=CASE WHEN t.task_type='pack' THEN json_extract(t.detail,'$.policy') ELSE t.task_type END AND IIF(c.policy='gc',m.gc_paused,c.paused)) AND NOT(t.task_type='pack' AND json_extract(t.detail,'$.policy') IN ('pack','repack') AND m.pack_creation_paused) ORDER BY t.updated_at,t.id LIMIT 1");
    query
}
pub(super) fn claim_task(id: Uuid, token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='running',started_at=COALESCE(started_at,CAST(unixepoch('subsec')*1000000 AS INTEGER)),execution_token=?,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND state='queued'").bind(token).bind(id)
}
pub(super) fn task_record(id: Uuid) -> Rows<'static, TaskRecord> {
    sqlx::query_as("SELECT task_type,state,bucket_id,progress_cursor,detail,execution_token FROM tasks WHERE id=?").bind(id)
}
pub(super) fn pause_owned_task(task: Execution, error: Option<&str>) -> Statement<'_> {
    sqlx::query("UPDATE tasks SET state='paused',error=COALESCE(?,error),updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND state='running' AND execution_token=?").bind(error).bind(task.id).bind(task.token)
}
pub(super) fn storage_profile_revision(id: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT revision FROM storage_profiles WHERE storage_id=?").bind(id)
}
pub(super) fn lock_task_finish(task: Execution) -> Rows<'static, (String, String)> {
    sqlx::query_as("SELECT task_type,state FROM tasks WHERE id=? AND execution_token=?")
        .bind(task.id)
        .bind(task.token)
}
pub(super) fn defer_pack_changes(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE pack_changes SET next_check_at=(SELECT CAST(unixepoch('subsec')*1000000 AS INTEGER)+(CAST(substr(sp.pack_reuse_interval,1,length(sp.pack_reuse_interval)-1) AS INTEGER)*CASE substr(sp.pack_reuse_interval,-1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END)*1000000 FROM packs p JOIN storage_profiles sp ON sp.storage_id=p.storage_id WHERE p.id=pack_changes.pack_id) WHERE pack_id IN (SELECT c.pack_id FROM pack_inputs i JOIN chunks c ON c.id=i.chunk_id WHERE i.task_id=?)").bind(id)
}

pub(super) fn defer_pack_maintenance(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE pack_maintenance SET next_check_at=(SELECT CAST(unixepoch('subsec')*1000000 AS INTEGER)+(CAST(substr(sp.pack_reuse_interval,1,length(sp.pack_reuse_interval)-1) AS INTEGER)*CASE substr(sp.pack_reuse_interval,-1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END)*1000000 FROM streams s JOIN buckets b ON b.id=s.bucket_id JOIN storage_profiles sp ON sp.storage_id=b.storage_id WHERE s.id=pack_maintenance.stream_id) WHERE stream_id IN (SELECT e.stream_id FROM extents e JOIN pack_inputs i ON i.chunk_id=e.chunk_id WHERE i.task_id=?)").bind(id)
}

pub(super) fn backoff_pending_uploads(task: Uuid, error: &str, retain: bool) -> Statement<'_> {
    sqlx::query("UPDATE pending_uploads SET next_retry_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)+MIN(300,1 << MIN(attempts,8))*1000000,attempts=MIN(attempts+1,30),last_error=?,owner_task=IIF(?,owner_task,NULL) WHERE owner_task=?").bind(error).bind(retain).bind(task)
}
pub(super) fn finish_task<'q>(
    task: Execution,
    state: &'q str,
    error: Option<&'q str>,
) -> Statement<'q> {
    sqlx::query("UPDATE tasks SET state=?,error=COALESCE(?,error),execution_token=NULL,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND execution_token=?").bind(state).bind(error).bind(task.id).bind(task.token)
}

#[async_trait::async_trait]
impl TaskExecutionStore for Backend {
    async fn claim(&self, input: TaskClaim<'_>) -> Result<Option<Uuid>> {
        if claim_task_candidate(input.class, input.active)
            .build_query_scalar::<Uuid>()
            .fetch_optional(&self.read)
            .await?
            .is_none()
        {
            return Ok(None);
        }
        transactions::claim_task(&self.write, input).await
    }
    async fn load(&self, id: Uuid) -> Result<TaskRecord> {
        Ok(task_record(id).fetch_one(&self.read).await?)
    }
    async fn fence(&self, expected_mode: &str, id: Uuid, token: Uuid) -> Result<bool> {
        let Some(row) = task_record(id).fetch_optional(&self.read).await? else {
            return Ok(false);
        };
        if row.state != "running" || row.execution_token != Some(token) {
            return Ok(false);
        }
        let execution = Execution { id, token };
        if expected_mode != "single" {
            pause_owned_task(execution, None)
                .execute(&self.write)
                .await?;
            return Ok(false);
        }
        if let (Some(storage), Some(revision)) = (
            row.detail["storage_id"]
                .as_str()
                .and_then(|s| s.parse::<Uuid>().ok()),
            row.detail["profile_revision"].as_i64(),
        ) && storage_profile_revision(storage)
            .fetch_optional(&self.read)
            .await?
            != Some(revision)
        {
            pause_owned_task(
                execution,
                Some("storage profile changed; resume to refresh"),
            )
            .execute(&self.write)
            .await?;
            return Ok(false);
        }
        Ok(true)
    }
    async fn confirm(&self, id: Uuid, token: Uuid) -> Result<bool> {
        TaskControlStore::running(self, Execution { id, token }).await
    }
    async fn finish(&self, input: TaskFinish) -> Result<()> {
        transactions::finish_task(&self.write, input).await
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for TaskRecord {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            task_type: row.try_get("task_type")?,
            state: row.try_get("state")?,
            bucket_id: row.try_get("bucket_id")?,
            cursor: row.try_get("progress_cursor")?,
            detail: row.try_get("detail")?,
            execution_token: row.try_get("execution_token")?,
        })
    }
}

use crate::model::metrics::ScopeQuery;
use crate::model::packing::{
    PackCandidate, PackMember, PackObject, PackOutput, PackSummary, PackingStore,
};

fn pack_value(row: SqliteRow) -> Result<Value> {
    let mut value = serde_json::json!({
        "id": row.try_get::<i64,_>("id")?.to_string(),
        "location_id": row.try_get::<Uuid,_>("location_id")?,
        "storage_id": row.try_get::<Uuid,_>("storage_id")?,
        "encryption_algorithm": row.try_get::<String,_>("encryption_algorithm")?,
        "block_key_id": row.try_get::<Option<Uuid>,_>("block_key_id")?,
        "raw_size": row.try_get::<i64,_>("raw_size")?,
        "stored_size": row.try_get::<Option<i64>,_>("stored_size")?,
        "pack_compressed": row.try_get::<bool,_>("pack_compressed")?,
        "member_count": row.try_get::<i32,_>("member_count")?,
        "state": row.try_get::<String,_>("state")?,
        "owner_task": row.try_get::<Option<Uuid>,_>("owner_task")?,
        "created_at": date(&row,"created_at")?,
    });
    value["pack_fingerprint"] = row.try_get::<String, _>("pack_fingerprint")?.into();
    for field in ["nonce", "digest"] {
        value[field] = row
            .try_get::<Option<Vec<u8>>, _>(field)?
            .map(|bytes| format!("\\x{}", hex::encode(bytes)))
            .into();
    }
    for field in [
        "unreferenced_at",
        "deleted_at",
        "range_checked_at",
        "stored_at",
    ] {
        value[field] = serde_json::to_value(optional_date(&row, field)?)?;
    }
    Ok(value)
}

fn pack_layout(pack: Id, referenced: bool) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new("SELECT ");
    query.push(CHUNK_COLUMNS).push(",m.offset_bytes,c.pack_id FROM pack_members m JOIN chunks c ON c.id=m.chunk_id AND c.pack_id=m.pack_id LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' WHERE m.pack_id=").push_bind(pack).push(" AND c.state='ready'");
    if referenced {
        query.push(" AND EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id)");
    }
    query.push(" ORDER BY m.ordinal LIMIT 4096");
    query
}

#[derive(sqlx::FromRow)]
struct PackCandidateRow {
    #[sqlx(flatten)]
    row: Mapping,
    eligible: bool,
    unit_count: i64,
}

#[async_trait::async_trait]
impl PackingStore for Backend {
    async fn in_storage(&self, pack: Id, storage: Uuid) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM packs WHERE id=? AND storage_id=? AND state='ready')",
        )
        .bind(pack)
        .bind(storage)
        .fetch_one(&self.read)
        .await?)
    }
    async fn scoped_page(
        &self,
        principal: &Principal,
        scope: &ScopeQuery,
        after: Id,
        limit: i64,
        state: Option<&str>,
    ) -> Result<Vec<PackSummary>> {
        transactions::scoped_pack_page(&self.read, principal, scope, after, limit, state).await
    }
    async fn scoped_detail(
        &self,
        principal: &Principal,
        scope: &ScopeQuery,
        id: Id,
        after: Id,
        limit: i64,
    ) -> Result<(PackSummary, Vec<PackMember>, bool)> {
        transactions::scoped_pack_detail(&self.read, principal, scope, id, after, limit).await
    }
    async fn scoped_objects(
        &self,
        principal: &Principal,
        scope: &ScopeQuery,
        id: Id,
        cursor: Option<(Uuid, String)>,
    ) -> Result<Vec<PackObject>> {
        transactions::scoped_pack_objects(&self.read, principal, scope, id, cursor).await
    }
    async fn page(&self, after: Id, limit: i64) -> Result<Vec<Value>> {
        sqlx::query("SELECT * FROM packs WHERE id>? ORDER BY id LIMIT ?")
            .bind(after)
            .bind(limit)
            .fetch_all(&self.read)
            .await?
            .into_iter()
            .map(pack_value)
            .collect()
    }
    async fn detail(&self, id: Id) -> Result<(Value, Vec<Value>)> {
        transactions::pack_detail(&self.read, id).await
    }

    async fn rewrite_counts(
        &self,
        storage: Uuid,
        algorithm: &str,
        key: Option<Uuid>,
    ) -> Result<(i64, i64)> {
        Ok(rewrite_counts(storage, algorithm, key)
            .build_query_as()
            .fetch_one(&self.read)
            .await?)
    }
    async fn start_rewrite(&self, job: &RewriteTask, node: Option<Uuid>) -> Result<(Uuid, bool)> {
        ensure!(node.is_none(), "SQLite does not support cluster nodes");
        transactions::start_rewrite(&self.write, job).await
    }
    async fn rewrite_candidates(
        &self,
        after: Id,
        job: &RewriteTask,
    ) -> Result<(Option<Chunk>, Option<(Id, Id)>)> {
        transactions::rewrite_candidates(&self.read, after, job).await
    }
    async fn rewrite_pack_inputs(&self, pack: Id) -> Result<(Pack, Vec<Chunk>, Vec<Id>)> {
        transactions::rewrite_pack_inputs(&self.read, pack).await
    }

    async fn commit_rewrite_chunk(
        &self,
        task: Uuid,
        token: Uuid,
        source: &Chunk,
        physical: &Chunk,
    ) -> Result<()> {
        transactions::commit_rewrite_chunk(
            &self.write,
            Execution { id: task, token },
            source,
            physical,
        )
        .await
    }
    async fn commit_rewrite_pack(
        &self,
        task: Uuid,
        token: Uuid,
        old: &Pack,
        new: &Pack,
        current: &[Id],
        members: &[Chunk],
        first: Id,
    ) -> Result<()> {
        transactions::commit_rewrite_pack(
            &self.write,
            Execution { id: task, token },
            old,
            new,
            current,
            members,
            first,
        )
        .await
    }
    async fn complete_rewrite(&self, task: Uuid, token: Uuid, after: Id) -> Result<()> {
        sqlx::query("UPDATE tasks SET state='completed',progress_cursor=?,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND execution_token=? AND state='running'").bind(after.to_string()).bind(task).bind(token).execute(&self.write).await?;
        Ok(())
    }
    async fn pack_totals(&self) -> Result<Vec<Value>> {
        Ok(sqlx::query_scalar("SELECT JSON_OBJECT('state',state,'count',COUNT(*),'raw_bytes',CAST(COALESCE(SUM(raw_size),0) AS INTEGER),'stored_bytes',CAST(COALESCE(SUM(stored_size),0) AS INTEGER)) FROM packs GROUP BY state").fetch_all(&self.read).await?)
    }
    async fn unpack_totals(&self, pack: Option<Id>) -> Result<(i64, i64)> {
        Ok(sqlx::query_as("SELECT COUNT(*),CAST(COALESCE(SUM(raw_size),0) AS INTEGER) FROM packs WHERE state='ready' AND (? IS NULL OR id=?)").bind(pack).bind(pack).fetch_one(&self.read).await?)
    }
    async fn unpack_identity(&self, pack: Option<Id>) -> Result<(String, String)> {
        Ok(sqlx::query_as("SELECT CAST(COALESCE(MAX(id),0) AS TEXT),CAST(COALESCE(SUM(id),0) AS TEXT) FROM packs WHERE state='ready' AND (? IS NULL OR id=?)").bind(pack).bind(pack).fetch_one(&self.read).await?)
    }
    async fn preparing_packs(&self) -> Result<bool> {
        Ok(
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM packs WHERE state='preparing')")
                .fetch_one(&self.read)
                .await?,
        )
    }
    async fn ready_encoding(&self, chunk: Id) -> Result<Option<(Id, i32)>> {
        Ok(sqlx::query_as("SELECT encoding_id,standalone_size FROM chunk_encodings WHERE chunk_id=? AND state='ready'").bind(chunk).fetch_optional(&self.read).await?)
    }
    async fn prepare_encoding(&self, physical: &Chunk, task: Execution) -> Result<Id> {
        transactions::prepare_encoding(&self.write, physical, task).await
    }
    async fn encoding_metadata(&self, physical: &Chunk) -> Result<()> {
        transactions::register_encoding(&self.write, physical).await
    }
    async fn prepare_pack(
        &self,
        storage: Uuid,
        algorithm: &str,
        key: Option<Uuid>,
        bytes: i64,
        count: i32,
        task: Execution,
    ) -> Result<Pack> {
        transactions::prepare_pack(&self.write, storage, algorithm, key, bytes, count, task).await
    }
    async fn discard_pack(&self, pack: Id) -> Result<()> {
        sqlx::query("UPDATE packs SET state='retired',unreferenced_at=COALESCE(unreferenced_at,CAST(unixepoch('subsec')*1000000 AS INTEGER)) WHERE id=? AND state='preparing'").bind(pack).execute(&self.write).await?;
        Ok(())
    }
    async fn pack_metadata(&self, pack: &Pack) -> Result<()> {
        transactions::pack_metadata(&self.write, pack).await
    }
    async fn pack_has_pending(&self, pack: Id) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pending_uploads WHERE source_pack_id=?)",
        )
        .bind(pack)
        .fetch_one(&self.read)
        .await?)
    }
    async fn pack_layout(&self, pack: Id) -> Result<Vec<Mapping>> {
        Ok(pack_layout(pack, false)
            .build_query_as()
            .fetch_all(&self.read)
            .await?)
    }
    async fn pack_has_writers(&self, pack: Id) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM chunks c JOIN extents e ON e.chunk_id=c.id JOIN streams s ON s.id=e.stream_id WHERE c.pack_id=? AND s.state='writing')").bind(pack).fetch_one(&self.read).await?)
    }
    async fn pack_raw_size(&self, pack: Id) -> Result<i64> {
        Ok(
            sqlx::query_scalar("SELECT raw_size FROM packs WHERE id=? AND state='ready'")
                .bind(pack)
                .fetch_one(&self.read)
                .await?,
        )
    }
    async fn chunk_has_references(&self, chunk: Id) -> Result<bool> {
        Ok(
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM extents WHERE chunk_id=?)")
                .bind(chunk)
                .fetch_one(&self.read)
                .await?,
        )
    }
    async fn pack_stored_size(&self, pack: Id) -> Result<i64> {
        Ok(
            sqlx::query_scalar("SELECT stored_size FROM packs WHERE id=?")
                .bind(pack)
                .fetch_one(&self.read)
                .await?,
        )
    }
    async fn pack_candidates(
        &self,
        stream: Id,
        offset: i64,
        cooldown: f64,
    ) -> Result<Vec<PackCandidate>> {
        let mut query = sqlx::QueryBuilder::<Sqlite>::new("SELECT ");
        query.push(CHUNK_COLUMNS).push(",e.offset_bytes,c.pack_id,(NOT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=c.id) AND NOT EXISTS(SELECT 1 FROM chunk_encodings q WHERE q.chunk_id=c.id AND q.state='ready' AND q.quarantined_at IS NOT NULL) AND NOT EXISTS(SELECT 1 FROM packs q WHERE q.id=c.pack_id AND q.quarantined_at IS NOT NULL) AND e.source_offset=0 AND e.length=c.raw_size AND (SELECT COUNT(*) FROM extents WHERE chunk_id=c.id)=1 AND COALESCE(c.repack_after<=CAST(unixepoch('subsec')*1000000 AS INTEGER),TRUE) AND (c.split_at IS NULL OR c.reference_changed_at<=CAST(unixepoch('subsec')*1000000 AS INTEGER)-").push_bind((cooldown * 1_000_000.) as i64).push(")) AS eligible,CAST(IIF(c.pack_id IS NULL,1,(SELECT COUNT(*) FROM chunks WHERE pack_id=c.pack_id AND state='ready')) AS INTEGER) AS unit_count FROM extents e JOIN chunks c ON c.id=e.chunk_id LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' JOIN streams s ON s.id=e.stream_id WHERE e.stream_id=").push_bind(stream).push(" AND e.offset_bytes>=").push_bind(offset).push(" AND s.state='ready' AND c.state='ready' AND EXISTS(SELECT 1 FROM objects WHERE stream_id=s.id) ORDER BY e.offset_bytes LIMIT 1026");
        Ok(query
            .build_query_as::<PackCandidateRow>()
            .fetch_all(&self.read)
            .await?
            .into_iter()
            .map(|r| PackCandidate {
                row: r.row,
                eligible: r.eligible,
                unit_count: r.unit_count,
            })
            .collect())
    }
    async fn pack_member_count(&self, pack: Id) -> Result<i64> {
        Ok(
            sqlx::query_scalar("SELECT COUNT(*) FROM chunks WHERE pack_id=? AND state='ready'")
                .bind(pack)
                .fetch_one(&self.read)
                .await?,
        )
    }
    async fn pack_stream_metadata(&self, stream: Id) -> Result<(Value, String)> {
        let (metadata, key): (Value, Vec<u8>) =
            sqlx::query_as("SELECT metadata,object_key FROM streams WHERE id=?")
                .bind(stream)
                .fetch_one(&self.read)
                .await?;
        Ok((metadata, String::from_utf8(key)?))
    }
    async fn physical_bytes(&self, ids: &[Id]) -> Result<i64> {
        Ok(pack_layout_bytes(ids)
            .build_query_scalar()
            .fetch_one(&self.read)
            .await?)
    }
    async fn pause_pack_task(&self, task: Execution) -> Result<()> {
        pause_owned_task(task, None).execute(&self.write).await?;
        Ok(())
    }
    async fn next_pack(
        &self,
        after: Id,
        pack: Option<Id>,
        upper: Option<DateTime<Utc>>,
        storage: Option<Uuid>,
    ) -> Result<Option<Id>> {
        Ok(sqlx::query_scalar("SELECT id FROM packs WHERE state='ready' AND id>? AND (? IS NULL OR id=?) AND (? IS NULL OR storage_id=?) AND created_at<=COALESCE(?,CAST(unixepoch('subsec')*1000000 AS INTEGER)) ORDER BY id LIMIT 1").bind(after).bind(pack).bind(pack).bind(storage).bind(storage).bind(upper.map(|time|time.timestamp_micros())).fetch_optional(&self.read).await?)
    }
    async fn next_pack_stream(
        &self,
        after: Id,
        offset: i64,
        upper: DateTime<Utc>,
        storage: Option<Uuid>,
    ) -> Result<Option<Id>> {
        Ok(sqlx::query_scalar("SELECT s.id FROM streams s JOIN objects o ON o.stream_id=s.id JOIN buckets b ON b.id=o.bucket_id WHERE s.state='ready' AND (s.id>? OR(s.id=? AND ?>=0)) AND (? IS NULL OR b.storage_id=?) AND s.created_at<=? ORDER BY s.id LIMIT 1").bind(after).bind(after).bind(offset).bind(storage).bind(storage).bind(upper.timestamp_micros()).fetch_optional(&self.read).await?)
    }
    async fn advance_pack_task(
        &self,
        task: Execution,
        next: &Option<String>,
        processed: i64,
    ) -> Result<()> {
        if let Some(next) = next {
            sqlx::query("UPDATE tasks SET progress_cursor=?,processed=processed+?,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND execution_token=? AND state='running'").bind(next).bind(processed).bind(task.id).bind(task.token).execute(&self.write).await?;
            Ok(())
        } else {
            transactions::advance_pack_storage(&self.write, task, processed).await
        }
    }
    async fn next_pack_change(
        &self,
        cutoff: DateTime<Utc>,
        policy: &str,
        storage: Option<Uuid>,
    ) -> Result<Option<(Id, i64)>> {
        Ok(sqlx::query_as("SELECT pc.pack_id,pc.generation FROM pack_changes pc JOIN packs p ON p.id=pc.pack_id WHERE pc.next_check_at<=? AND pc.reason=? AND (? IS NULL OR p.storage_id=?) ORDER BY pc.next_check_at,pc.pack_id LIMIT 1").bind(cutoff.timestamp_micros()).bind(policy).bind(storage).bind(storage).fetch_optional(&self.read).await?)
    }
    async fn finish_pack_change(&self, task: Execution, pack: Id, generation: i64) -> Result<()> {
        transactions::finish_pack_change(&self.write, task, pack, generation).await
    }
    async fn next_pack_queue(
        &self,
        cutoff: DateTime<Utc>,
        policy: &str,
        storage: Option<Uuid>,
    ) -> Result<Option<(Id, i64, i64)>> {
        Ok(sqlx::query_as("SELECT pm.stream_id,pm.progress_cursor,pm.generation FROM pack_maintenance pm JOIN streams s ON s.id=pm.stream_id JOIN objects o ON o.stream_id=s.id JOIN buckets b ON b.id=o.bucket_id WHERE pm.next_check_at<=? AND pm.reason=? AND (? IS NULL OR b.storage_id=?) ORDER BY pm.next_check_at,pm.stream_id LIMIT 1").bind(cutoff.timestamp_micros()).bind(policy).bind(storage).bind(storage).fetch_optional(&self.read).await?)
    }
    async fn stream_repack_after(
        &self,
        stream: Id,
        cooldown: f64,
    ) -> Result<Option<DateTime<Utc>>> {
        let micros:Option<i64>=sqlx::query_scalar("SELECT MAX(later) FROM (SELECT MAX(COALESCE(c.repack_after,c.reference_changed_at),c.reference_changed_at+?) AS later FROM extents e JOIN chunks c ON c.id=e.chunk_id WHERE e.stream_id=? AND c.split_at IS NOT NULL AND (SELECT COUNT(*) FROM extents WHERE chunk_id=c.id)=1) deferred WHERE later>CAST(unixepoch('subsec')*1000000 AS INTEGER)").bind((cooldown*1_000_000.) as i64).bind(stream).fetch_one(&self.read).await?;
        Ok(micros.map(datetime).transpose()?)
    }

    async fn defer_pack_queue(
        &self,
        task: Execution,
        stream: Id,
        generation: i64,
        later: DateTime<Utc>,
    ) -> Result<()> {
        transactions::defer_pack_queue(&self.write, task, stream, generation, later).await
    }
    async fn finish_pack_queue(&self, task: Execution, stream: Id, generation: i64) -> Result<()> {
        transactions::finish_pack_queue(&self.write, task, stream, generation).await
    }
    async fn advance_pack_queue(
        &self,
        task: Execution,
        stream: Id,
        generation: i64,
        cursor: i64,
    ) -> Result<()> {
        transactions::advance_pack_queue(&self.write, task, stream, generation, cursor).await
    }
    async fn pin_pack_inputs(&self, task: Execution, rows: &[Mapping]) -> Result<()> {
        transactions::pin_pack_inputs(&self.write, task, rows).await
    }
    async fn start_pack(&self, policy: &str, automatic: bool, node: Option<Uuid>) -> Result<Value> {
        ensure!(node.is_none(), "SQLite does not support cluster nodes");
        transactions::start_pack(&self.write, policy, automatic).await
    }
    async fn start_unpack(&self, pack: Option<Id>, all: bool, node: Option<Uuid>) -> Result<Value> {
        ensure!(node.is_none(), "SQLite does not support cluster nodes");
        transactions::start_unpack(&self.write, pack, all).await
    }
    async fn insert_pack_members(&self, pack: Id, rows: &[Mapping]) -> Result<()> {
        transactions::insert_pack_members(&self.write, pack, rows).await
    }
    async fn commit_pack_outputs(
        &self,
        task: Execution,
        rows: &[Mapping],
        outputs: &[PackOutput],
        split: bool,
        settings: &crate::content::pack::Config,
        flush_seconds: f64,
    ) -> Result<()> {
        transactions::commit_pack_outputs(
            &self.write,
            task,
            rows,
            outputs,
            split,
            settings,
            flush_seconds,
        )
        .await
    }
    async fn same_references(&self, a: Id, b: Id, distance: i64) -> Result<bool> {
        Ok(same_references(a, b, distance)
            .fetch_one(&self.read)
            .await?)
    }
    async fn next_range_pack(
        &self,
        storage: Uuid,
        cutoff: DateTime<Utc>,
        window: f64,
        minimum: i64,
    ) -> Result<Option<Id>> {
        Ok(sqlx::query_scalar("SELECT p.id FROM packs p JOIN pack_access_windows w ON w.pack_id=p.id WHERE p.storage_id=? AND p.state='ready' AND p.created_at<=? AND (p.range_checked_at IS NULL OR p.range_checked_at<?) AND w.window_start>=CAST((CAST(unixepoch('subsec')*1000000 AS INTEGER)-?)/3600000000 AS INTEGER)*3600000000 GROUP BY p.id,p.range_checked_at HAVING SUM(w.partial_downloads)>=? AND (p.range_checked_at IS NULL OR MAX(w.updated_at)>p.range_checked_at) ORDER BY SUM(w.partial_bytes) DESC,p.id LIMIT 1").bind(storage).bind(cutoff.timestamp_micros()).bind(cutoff.timestamp_micros()).bind((window * 1_000_000.) as i64).bind(minimum).fetch_optional(&self.read).await?)
    }
    async fn complete_range_task(&self, task: Execution) -> Result<()> {
        transactions::advance_pack_storage(&self.write, task, 0).await
    }
    async fn range_pack_blocked(&self, pack: Id) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_uploads WHERE source_pack_id=?) OR EXISTS(SELECT 1 FROM chunks c JOIN extents e ON e.chunk_id=c.id JOIN streams s ON s.id=e.stream_id WHERE c.pack_id=? AND s.state='writing')").bind(pack).bind(pack).fetch_one(&self.read).await?)
    }
    async fn pack_range_totals(&self, pack: Id, window: f64) -> Result<(i64, i64, i64)> {
        Ok(sqlx::query_as("SELECT CAST(COALESCE(SUM(partial_downloads),0) AS INTEGER),CAST(COALESCE(SUM(partial_bytes),0) AS INTEGER),CAST(COALESCE(SUM(downloads-partial_downloads),0) AS INTEGER) FROM pack_access_windows WHERE pack_id=? AND window_start>=CAST((CAST(unixepoch('subsec')*1000000 AS INTEGER)-?)/3600000000 AS INTEGER)*3600000000").bind(pack).bind((window * 1_000_000.) as i64).fetch_one(&self.read).await?)
    }
    async fn pack_member_hits(&self, pack: Id, window: f64) -> Result<Vec<(Id, i64)>> {
        Ok(sqlx::query_as("SELECT chunk_id,CAST(SUM(downloads) AS INTEGER) FROM pack_member_access_windows WHERE pack_id=? AND window_start>=CAST((CAST(unixepoch('subsec')*1000000 AS INTEGER)-?)/3600000000 AS INTEGER)*3600000000 GROUP BY chunk_id").bind(pack).bind((window * 1_000_000.) as i64).fetch_all(&self.read).await?)
    }
    async fn pack_dimensions(&self, pack: Id) -> Result<(i64, i64, i32)> {
        Ok(
            sqlx::query_as("SELECT stored_size,raw_size,member_count FROM packs WHERE id=?")
                .bind(pack)
                .fetch_one(&self.read)
                .await?,
        )
    }
    async fn range_pack_layout(&self, pack: Id) -> Result<Vec<Mapping>> {
        Ok(pack_layout(pack, true)
            .build_query_as()
            .fetch_all(&self.read)
            .await?)
    }
    async fn defer_range_repack(
        &self,
        stream: Id,
        window: f64,
        flush: f64,
        retry: f64,
        offset: i64,
    ) -> Result<()> {
        sqlx::query("UPDATE chunks AS c SET repack_after=CAST(unixepoch('subsec')*1000000 AS INTEGER)+? FROM access_state g WHERE c.id IN (SELECT chunk_id FROM extents WHERE stream_id=? AND offset_bytes>=? ORDER BY offset_bytes LIMIT 1026) AND c.range_split_at IS NOT NULL AND c.repack_after<=CAST(unixepoch('subsec')*1000000 AS INTEGER) AND (g.coverage_since IS NULL OR g.coverage_since>CAST((CAST(unixepoch('subsec')*1000000 AS INTEGER)-?)/3600000000 AS INTEGER)*3600000000 OR g.flushed_at IS NULL OR g.flushed_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-? OR EXISTS(SELECT 1 FROM chunk_access_windows w WHERE w.chunk_id=c.id AND w.range_read_count>0 AND w.window_start>=CAST((CAST(unixepoch('subsec')*1000000 AS INTEGER)-?)/3600000000 AS INTEGER)*3600000000))").bind((retry*1_000_000.) as i64).bind(stream).bind(offset).bind((window*1_000_000.) as i64).bind((flush*1_000_000.) as i64).bind((window*1_000_000.) as i64).execute(&self.write).await?;
        Ok(())
    }

    async fn record_range(
        &self,
        task: Execution,
        id: Id,
        processed: usize,
        decision: Value,
    ) -> Result<()> {
        transactions::record_range(&self.write, task, id, processed, decision).await
    }
}

pub(super) fn usage_scope_buckets(
    user: Option<Uuid>,
    admin: bool,
    token: Option<Uuid>,
    bucket: Option<Uuid>,
    project: Option<Uuid>,
) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT b.id FROM buckets b LEFT JOIN user_bucket_access a ON a.bucket_id=b.id AND a.user_id=? LEFT JOIN token_bucket_access t ON t.bucket_id=b.id AND t.token_id=? WHERE (? OR (IIF(? IS NULL,a.actions,t.actions)&64)<>0) AND (? IS NULL OR b.id=?) AND (? IS NULL OR b.project_id=?) ORDER BY b.id").bind(user).bind(token).bind(admin).bind(token).bind(bucket).bind(bucket).bind(project).bind(project)
}

fn uuid_ids(query: &mut sqlx::QueryBuilder<Sqlite>, ids: &[Uuid]) {
    query.push("(");
    if ids.is_empty() {
        query.push("NULL");
    } else {
        let mut values = query.separated(",");
        for id in ids {
            values.push_bind(*id);
        }
    }
    query.push(")");
}

fn scoped_packs(ids: &[Uuid], admin: bool, all: bool) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT CAST(p.id AS TEXT) AS id,p.state,p.pack_compressed AS compressed,",
    );
    query.push(if admin { "CAST(p.raw_size AS TEXT) AS raw_size,CAST(p.stored_size AS TEXT) AS stored_size,p.member_count" } else { "NULL AS raw_size,NULL AS stored_size,NULL AS member_count" });
    query.push(",p.created_at FROM packs p WHERE TRUE");
    if !admin {
        query.push(" AND p.state='ready'");
    }
    if !all {
        query.push(" AND EXISTS(SELECT 1 FROM pack_members m JOIN chunks c ON c.id=m.chunk_id AND c.pack_id=m.pack_id JOIN extents e ON e.chunk_id=c.id JOIN streams s ON s.id=e.stream_id JOIN objects o ON o.bucket_id=s.bucket_id AND o.object_key=s.object_key AND o.stream_id=s.id WHERE m.pack_id=p.id AND o.bucket_id IN ");
        uuid_ids(&mut query, ids);
        query.push(")");
    }
    query
}

pub(super) fn scoped_pack_page(
    ids: &[Uuid],
    admin: bool,
    all: bool,
    after: Id,
    limit: i64,
    state: Option<&str>,
) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = scoped_packs(ids, admin, all);
    query.push(" AND p.id>").push_bind(after);
    if let Some(state) = state {
        query.push(" AND p.state=").push_bind(state.to_owned());
    }
    query.push(" ORDER BY p.id LIMIT ").push_bind(limit);
    query
}

pub(super) fn scoped_pack(
    ids: &[Uuid],
    admin: bool,
    all: bool,
    id: Id,
) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = scoped_packs(ids, admin, all);
    query.push(" AND p.id=").push_bind(id);
    query
}

pub(super) fn scoped_pack_members(
    id: Id,
    ids: &[Uuid],
    admin: bool,
    all: bool,
    after: Id,
    limit: i64,
) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new(
        "WITH selected_members AS (SELECT c.id,c.raw_size,c.pack_id,m.ordinal FROM pack_members m JOIN chunks c ON c.id=m.chunk_id WHERE m.pack_id=",
    );
    query.push_bind(id).push(" AND c.id>").push_bind(after);
    if !all {
        query.push(" AND c.pack_id=m.pack_id AND EXISTS(SELECT 1 FROM extents e JOIN streams s ON s.id=e.stream_id JOIN objects o ON o.bucket_id=s.bucket_id AND o.object_key=s.object_key AND o.stream_id=s.id WHERE e.chunk_id=c.id AND o.bucket_id IN ");
        uuid_ids(&mut query, ids);
        query.push(")");
    }
    query.push(" ORDER BY c.id LIMIT ").push_bind(limit).push("),visible_ranges AS (SELECT m.id,e.source_offset AS lo,e.source_offset+e.length AS hi FROM selected_members m JOIN extents e ON e.chunk_id=m.id JOIN streams s ON s.id=e.stream_id JOIN objects o ON o.bucket_id=s.bucket_id AND o.object_key=s.object_key AND o.stream_id=s.id WHERE o.bucket_id IN ");
    uuid_ids(&mut query, ids);
    // Running maximum, rather than the preceding endpoint, also merges nested intervals.
    query.push("),range_order AS (SELECT id,lo,hi,MAX(hi) OVER(PARTITION BY id ORDER BY lo,hi ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS previous_end FROM visible_ranges),range_groups AS (SELECT id,lo,hi,SUM(previous_end IS NULL OR lo>previous_end) OVER(PARTITION BY id ORDER BY lo,hi ROWS UNBOUNDED PRECEDING) AS span_id FROM range_order),range_spans AS (SELECT id,span_id,MAX(hi)-MIN(lo) AS bytes FROM range_groups GROUP BY id,span_id),visible AS (SELECT id,SUM(bytes) AS bytes FROM range_spans GROUP BY id) SELECT CAST(m.id AS TEXT) AS chunk_id,");
    query.push(if admin {
        "m.ordinal,m.raw_size"
    } else {
        "NULL AS ordinal,NULL AS raw_size"
    });
    query.push(",CAST(COALESCE(v.bytes,0) AS TEXT) AS visible_bytes,COALESCE(m.pack_id=").push_bind(id).push(" AND NOT EXISTS(SELECT 1 FROM chunk_encodings WHERE chunk_id=m.id AND state='ready'),FALSE) AS current_source FROM selected_members m LEFT JOIN visible v ON v.id=m.id ORDER BY m.id");
    query
}

pub(super) fn scoped_pack_objects(
    id: Id,
    ids: &[Uuid],
    principal: &Principal,
    admin: bool,
    cursor: Option<(Uuid, String)>,
) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT DISTINCT o.bucket_id,b.name AS bucket_name,o.object_key AS object_key,s.id AS version,CAST(s.size AS TEXT) AS size,COALESCE(s.metadata->>'$.content_type','application/octet-stream') AS content_type,s.public_read FROM pack_members m JOIN chunks c ON c.id=m.chunk_id JOIN extents e ON e.chunk_id=c.id JOIN streams s ON s.id=e.stream_id JOIN objects o ON o.bucket_id=s.bucket_id AND o.object_key=s.object_key AND o.stream_id=s.id JOIN buckets b ON b.id=o.bucket_id LEFT JOIN user_bucket_access a ON a.bucket_id=b.id AND a.user_id=",
    );
    query
        .push_bind(principal.user_id())
        .push(" LEFT JOIN token_bucket_access t ON t.bucket_id=b.id AND t.token_id=")
        .push_bind(principal.token_id())
        .push(" WHERE m.pack_id=")
        .push_bind(id)
        .push(" AND o.bucket_id IN ");
    uuid_ids(&mut query, ids);
    if !admin {
        query
            .push(" AND c.pack_id=m.pack_id AND (IIF(")
            .push_bind(principal.token_id())
            .push(" IS NULL,a.actions,t.actions)&1)<>0");
    }
    if let Some((bucket, key)) = cursor {
        query
            .push(" AND (o.bucket_id>")
            .push_bind(bucket)
            .push(" OR(o.bucket_id=")
            .push_bind(bucket)
            .push(" AND o.object_key>")
            .push_bind(key.into_bytes())
            .push("))");
    }
    query.push(" ORDER BY o.bucket_id,o.object_key LIMIT 101");
    query
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for PackSummary {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            state: row.try_get("state")?,
            compressed: row.try_get("compressed")?,
            raw_size: row.try_get("raw_size")?,
            stored_size: row.try_get("stored_size")?,
            member_count: row.try_get("member_count")?,
            created_at: date(row, "created_at")?,
        })
    }
}
impl<'r> sqlx::FromRow<'r, SqliteRow> for PackMember {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            chunk_id: row.try_get("chunk_id")?,
            ordinal: row.try_get("ordinal")?,
            raw_size: row.try_get("raw_size")?,
            visible_bytes: row.try_get("visible_bytes")?,
            current_source: row.try_get("current_source")?,
        })
    }
}
impl<'r> sqlx::FromRow<'r, SqliteRow> for PackObject {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        let key = String::from_utf8(row.try_get("object_key")?).map_err(|e| {
            sqlx::Error::ColumnDecode {
                index: "object_key".into(),
                source: Box::new(e),
            }
        })?;
        Ok(Self {
            bucket_id: row.try_get("bucket_id")?,
            bucket_name: row.try_get("bucket_name")?,
            key,
            version: row.try_get("version")?,
            size: row.try_get("size")?,
            content_type: row.try_get("content_type")?,
            public_read: row.try_get("public_read")?,
        })
    }
}

const PACK_FINGERPRINT: &str = "json_array(hex(k.block_key_id),k.encryption_algorithm,k.pack_compressed,k.member_count,p.compression_enabled,p.compression_strategy,p.compression_level,p.compression_sample_level,p.compression_min_savings_percent,p.compression_min_savings_bytes,p.pack_max_size,p.pack_range_enabled,p.pack_range_min_downloads,p.pack_range_min_savings_percent,p.pack_range_min_savings_bytes,'pack')";

pub(super) fn lock_pack_layout(ids: &[Id]) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT id,pack_id,storage_id FROM chunks WHERE state='ready' AND id IN ",
    );
    chunk_ids(&mut query, ids);
    query.push(" ORDER BY id");
    query
}
pub(super) fn pack_input_owners(ids: &[Id]) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new("SELECT task_id FROM pack_inputs WHERE chunk_id IN ");
    chunk_ids(&mut query, ids);
    query
}
pub(super) fn lock_cache_inputs(ids: &[Id], task: Uuid, flush: bool) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT c.id,c.current_encoding_id,c.pack_id,p.stream_id,p.offset_bytes,p.source_pack_id FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id WHERE c.id IN ",
    );
    chunk_ids(&mut query, ids);
    query.push(" AND (p.owner_task IS NULL OR p.owner_task=").push_bind(task)
        .push(" OR (").push_bind(flush)
        .push(" AND p.owner_task IN (SELECT id FROM tasks WHERE state IN ('paused','failed')))) ORDER BY c.id");
    query
}
pub(super) fn insert_pack_inputs(task: Uuid, ids: &[Id]) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new("INSERT INTO pack_inputs(task_id,chunk_id) SELECT ");
    query
        .push_bind(task)
        .push(",c.id FROM chunks c WHERE c.id IN ");
    chunk_ids(&mut query, ids);
    query.push(" AND NOT EXISTS(SELECT 1 FROM pack_inputs WHERE chunk_id=c.id)");
    query
}
pub(super) fn prepare_encoding(physical: &Chunk, task: Execution) -> Statement<'_> {
    sqlx::query("INSERT INTO chunk_encodings(chunk_id,location_id,storage_id,state,created_at,unreferenced_at,owner_task,encryption_algorithm,block_key_id,encoding_fingerprint) VALUES(?,?,?,'uploading',?,CAST(unixepoch('subsec')*1000000 AS INTEGER),?,?,?,'[]')")
        .bind(physical.id).bind(physical.location_id).bind(physical.storage_id).bind(physical.created_at.timestamp_micros()).bind(task.id).bind(&physical.algorithm).bind(physical.block_key_id)
}
pub(super) fn prepare_pack(
    storage: Uuid,
    algorithm: &str,
    key: Option<Uuid>,
    bytes: i64,
    count: i32,
    task: Execution,
) -> Statement<'_> {
    sqlx::query("INSERT INTO packs(location_id,storage_id,encryption_algorithm,block_key_id,raw_size,member_count,state,unreferenced_at,owner_task,pack_fingerprint) VALUES(?,?,?,?,?,?,'preparing',CAST(unixepoch('subsec')*1000000 AS INTEGER),?,'[]')")
        .bind(Uuid::now_v7()).bind(storage).bind(algorithm).bind(key).bind(bytes).bind(count).bind(task.id)
}
pub(super) async fn refresh_pack_fingerprint(
    connection: &mut sqlx::SqliteConnection,
    pack: Id,
) -> Result<()> {
    let mut query = sqlx::QueryBuilder::new("UPDATE packs AS k SET pack_fingerprint=");
    query
        .push(PACK_FINGERPRINT)
        .push(" FROM storage_profiles p WHERE p.storage_id=k.storage_id AND k.id=")
        .push_bind(pack);
    query.build().execute(connection).await?;
    Ok(())
}
pub(super) fn pack_record(id: Id) -> Rows<'static, Pack> {
    sqlx::query_as("SELECT * FROM packs WHERE id=?").bind(id)
}
pub(super) fn lock_prepared_pack(id: Id) -> Rows<'static, Pack> {
    sqlx::query_as("SELECT * FROM packs WHERE id=? AND state='preparing'").bind(id)
}
pub(super) fn pack_metadata(pack: &Pack) -> Statement<'_> {
    sqlx::query("UPDATE packs SET stored_size=?,pack_compressed=?,nonce=?,nonce_id=?,digest=? WHERE id=? AND location_id=? AND storage_id=? AND encryption_algorithm=? AND block_key_id IS ? AND state='preparing'")
        .bind(pack.stored_size).bind(pack.compressed).bind(&pack.nonce).bind(crate::content::nonce::id(pack.nonce.as_deref())).bind(&pack.digest).bind(pack.id).bind(pack.location_id).bind(pack.storage_id).bind(&pack.algorithm).bind(pack.block_key_id)
}
pub(super) fn insert_pack_members<'a>(
    pack: Id,
    chunks: impl Iterator<Item = &'a Chunk>,
) -> sqlx::QueryBuilder<Sqlite> {
    let mut query =
        sqlx::QueryBuilder::new("INSERT INTO pack_members(pack_id,ordinal,chunk_id,offset_bytes) ");
    let mut offset = 0i64;
    query.push_values(chunks.enumerate(), |mut row, (ordinal, chunk)| {
        row.push_bind(pack)
            .push_bind(ordinal as i32)
            .push_bind(chunk.id)
            .push_bind(offset);
        offset += i64::from(chunk.raw_size);
    });
    query
}
pub(super) fn pack_member_ids(pack: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT chunk_id FROM pack_members WHERE pack_id=? ORDER BY ordinal")
        .bind(pack)
}
pub(super) fn pack_layout_bytes(ids: &[Id]) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT CAST(COALESCE(SUM(bytes),0) AS INTEGER) FROM (SELECT standalone_size bytes FROM chunk_encodings WHERE state='ready' AND chunk_id IN ",
    );
    chunk_ids(&mut query, ids);
    query.push(" UNION ALL SELECT stored_size FROM packs WHERE id IN (SELECT pack_id FROM chunks WHERE id IN ");
    chunk_ids(&mut query, ids);
    query.push(")) physical");
    query
}
pub(super) fn pack_added_bytes(task: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT CAST(COALESCE(SUM(bytes),0) AS INTEGER) FROM (SELECT stored_size bytes FROM packs WHERE owner_task=? AND state='preparing' UNION ALL SELECT standalone_size FROM chunk_encodings WHERE owner_task=? AND state='uploading') physical").bind(task).bind(task)
}
pub(super) fn pack_task_policy(task: Uuid) -> Rows<'static, (bool, bool)> {
    sqlx::query_as("SELECT task_type IN ('upload','cache_flush'),COALESCE(task_type='pack' AND json_extract(detail,'$.policy')='range',FALSE) FROM tasks WHERE id=?").bind(task)
}
pub(super) fn pack_exclusive(
    ids: &[Id],
    cooldown: f64,
    window: f64,
    flush: f64,
) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT NOT EXISTS(SELECT 1 FROM chunks c CROSS JOIN access_state a WHERE c.id IN ",
    );
    chunk_ids(&mut query, ids);
    query.push(" AND ((SELECT COUNT(*) FROM extents WHERE chunk_id=c.id)<>1 OR NOT COALESCE(c.repack_after<=CAST(unixepoch('subsec')*1000000 AS INTEGER),TRUE) OR (c.split_at IS NOT NULL AND c.reference_changed_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)-").push_bind((cooldown*1_000_000.) as i64)
        .push(") OR (c.range_split_at IS NOT NULL AND (a.coverage_since IS NULL OR a.coverage_since>CAST((CAST(unixepoch('subsec')*1000000 AS INTEGER)-").push_bind((window*1_000_000.) as i64)
        .push(")/3600000000 AS INTEGER)*3600000000 OR a.flushed_at IS NULL OR a.flushed_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-").push_bind((flush*1_000_000.) as i64)
        .push(" OR EXISTS(SELECT 1 FROM chunk_access_windows w WHERE w.chunk_id=c.id AND w.range_read_count>0 AND w.window_start>=CAST((CAST(unixepoch('subsec')*1000000 AS INTEGER)-").push_bind((window*1_000_000.) as i64)
        .push(")/3600000000 AS INTEGER)*3600000000)))))");
    query
}

pub(super) fn publish_prepared_pack(pack: Id, task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE packs SET state='ready',unreferenced_at=NULL,stored_at=CAST(unixepoch('subsec')*1000000 AS INTEGER),owner_task=NULL WHERE id=? AND owner_task=? AND state='preparing' AND stored_size IS NOT NULL AND digest IS NOT NULL").bind(pack).bind(task)
}
pub(super) fn retire_pack_encodings(ids: &[Id], keep: Option<Id>) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new(
        "UPDATE chunk_encodings SET state='retired',unreferenced_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE state='ready' AND chunk_id IN ",
    );
    chunk_ids(&mut query, ids);
    if let Some(keep) = keep {
        query.push(" AND encoding_id<>").push_bind(keep);
    }
    query
}
pub(super) fn lock_output_encoding(
    encoding: Id,
    chunk: Id,
) -> Rows<'static, (String, Option<Uuid>)> {
    sqlx::query_as("SELECT state,owner_task FROM chunk_encodings WHERE encoding_id=? AND chunk_id=? AND standalone_size IS NOT NULL").bind(encoding).bind(chunk)
}
pub(super) fn publish_chunk_encoding(chunk: Id, encoding: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunks AS c SET current_encoding_id=e.encoding_id,standalone_size=e.standalone_size,standalone_compressed=e.standalone_compressed,nonce=e.nonce,encryption_algorithm=e.encryption_algorithm,block_key_id=e.block_key_id FROM chunk_encodings e WHERE e.chunk_id=c.id AND e.encoding_id=? AND e.state='ready' AND c.id=? AND c.state='ready'").bind(encoding).bind(chunk)
}

pub(super) fn publish_pack_layout(
    ids: &[Id],
    pack: Option<Id>,
    split: bool,
    cooldown: f64,
    range: bool,
) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new("UPDATE chunks SET pack_id=");
    query.push_bind(pack).push(",split_at=IIF(").push_bind(split).push(",CAST(unixepoch('subsec')*1000000 AS INTEGER),split_at),repack_after=IIF(").push_bind(split)
        .push(",MAX(COALESCE(repack_after,CAST(unixepoch('subsec')*1000000 AS INTEGER)),CAST(unixepoch('subsec')*1000000 AS INTEGER)+").push_bind((cooldown*1_000_000.) as i64)
        .push("),repack_after),range_split_at=CASE WHEN ").push_bind(range).push(" THEN CAST(unixepoch('subsec')*1000000 AS INTEGER) WHEN ").push_bind(!split && pack.is_some()).push(" THEN NULL ELSE range_split_at END WHERE id IN ");
    chunk_ids(&mut query, ids);
    query
}

pub(super) fn pack_has_references(ids: &[Id]) -> sqlx::QueryBuilder<Sqlite> {
    let mut query =
        sqlx::QueryBuilder::new("SELECT EXISTS(SELECT 1 FROM extents WHERE chunk_id IN ");
    chunk_ids(&mut query, ids);
    query.push(")");
    query
}
pub(super) fn fail_dead_pack_chunks(ids: &[Id]) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new(
        "UPDATE chunks SET state='failed',pack_id=NULL,unreferenced_at=COALESCE(unreferenced_at,CAST(unixepoch('subsec')*1000000 AS INTEGER)) WHERE id IN ",
    );
    chunk_ids(&mut query, ids);
    query
}
pub(super) fn schedule_repacked_streams(ids: &[Id], cooldown: f64) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new(
        "INSERT INTO pack_maintenance(stream_id,reason,next_check_at) SELECT DISTINCT e.stream_id,'repack',CAST(unixepoch('subsec')*1000000 AS INTEGER)+",
    );
    query.push_bind((cooldown*1_000_000.) as i64).push(" FROM extents e JOIN streams s ON s.id=e.stream_id WHERE s.state='ready' AND s.stream_type='object' AND e.chunk_id IN ");
    chunk_ids(&mut query, ids);
    query.push(" ON CONFLICT(stream_id) DO UPDATE SET generation=pack_maintenance.generation+1,progress_cursor=0,reason='repack',updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER),next_check_at=excluded.next_check_at");
    query
}

pub(super) fn retire_replaced_packs(ids: &[Id]) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new(
        "UPDATE packs AS p SET state='retired',unreferenced_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE state='ready' AND id IN ",
    );
    chunk_ids(&mut query, ids);
    query.push(" AND NOT EXISTS(SELECT 1 FROM chunks WHERE pack_id=p.id AND state='ready')");
    query
}
pub(super) fn record_pack_rewrite(task: Uuid, detail: Value) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET detail=JSON_SET(detail,'$.last_rewrite',json(?)) WHERE id=?")
        .bind(detail)
        .bind(task)
}

pub(super) fn lock_pack_policy(policy: &str) -> Scalar<'_, String> {
    sqlx::query_scalar("SELECT policy FROM maintenance_controls WHERE policy=?").bind(policy)
}
pub(super) fn active_pack_task(policy: &str) -> Scalar<'_, Uuid> {
    sqlx::query_scalar("SELECT id FROM tasks WHERE task_type='pack' AND json_extract(detail,'$.policy')=? AND state IN ('queued','running') LIMIT 1").bind(policy)
}
pub(super) fn pack_storages(policy: Option<&str>) -> Rows<'_, (Uuid, i64)> {
    sqlx::query_as("SELECT s.id,p.revision FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.enabled AND CASE WHEN ? IN ('pack','repack') THEN p.pack_enabled WHEN ?='range' THEN p.pack_range_enabled ELSE TRUE END ORDER BY s.created_at,s.id").bind(policy).bind(policy)
}
pub(super) fn unpack_storage(pack: Id) -> Rows<'static, (Uuid, i64)> {
    sqlx::query_as("SELECT s.id,p.revision FROM packs k JOIN storages s ON s.id=k.storage_id JOIN storage_profiles p ON p.storage_id=s.id WHERE k.id=?").bind(pack)
}
pub(super) fn active_unpack_task(all: bool, pack: Option<Id>) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM tasks WHERE task_type='unpack' AND state IN ('queued','running','paused','failed') AND json_extract(detail,'$.all')=? AND json_extract(detail,'$.pack_id') IS ? LIMIT 1").bind(all).bind(pack)
}

pub(super) fn lock_task_detail(task: Execution) -> Scalar<'static, Value> {
    sqlx::query_scalar(
        "SELECT detail FROM tasks WHERE id=? AND state='running' AND execution_token=?",
    )
    .bind(task.id)
    .bind(task.token)
}
pub(super) fn advance_storage_task(
    task: Uuid,
    detail: Value,
    processed: i64,
    done: bool,
) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET progress_cursor=NULL,detail=?,processed=processed+?,state=IIF(?,'completed',state),updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=?").bind(detail).bind(processed).bind(done).bind(task)
}
pub(super) fn finish_pack_change(pack: Id, generation: i64) -> Statement<'static> {
    sqlx::query("DELETE FROM pack_changes WHERE pack_id=? AND generation=?")
        .bind(pack)
        .bind(generation)
}
pub(super) fn defer_pack_queue(
    stream: Id,
    generation: i64,
    later: DateTime<Utc>,
) -> Statement<'static> {
    sqlx::query("UPDATE pack_maintenance SET progress_cursor=0,next_check_at=?,reason='repack' WHERE stream_id=? AND generation=?").bind(later.timestamp_micros()).bind(stream).bind(generation)
}
pub(super) fn finish_pack_queue(stream: Id, generation: i64) -> Statement<'static> {
    sqlx::query("DELETE FROM pack_maintenance WHERE stream_id=? AND generation=?")
        .bind(stream)
        .bind(generation)
}
pub(super) fn advance_pack_queue(stream: Id, generation: i64, cursor: i64) -> Statement<'static> {
    sqlx::query("UPDATE pack_maintenance SET progress_cursor=? WHERE stream_id=? AND generation=?")
        .bind(cursor)
        .bind(stream)
        .bind(generation)
}
pub(super) fn mark_range_checked(pack: Id) -> Statement<'static> {
    sqlx::query(
        "UPDATE packs SET range_checked_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=?",
    )
    .bind(pack)
}
pub(super) fn record_range_result(
    task: Uuid,
    cursor: String,
    processed: i64,
    decision: Value,
) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET progress_cursor=?,processed=processed+?,detail=JSON_SET(detail,'$.last_range',json(?),'$.evaluated',COALESCE(CAST(detail->>'$.evaluated' AS INTEGER),0)+1),updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=?").bind(cursor).bind(processed).bind(decision).bind(task)
}

use crate::model::packing::RewriteTask;

fn rewrite_needed(
    query: &mut sqlx::QueryBuilder<Sqlite>,
    pack: bool,
    algorithm: &str,
    key: Option<Uuid>,
) {
    query.push(if pack {
        "(NOT(k.pack_fingerprint IS "
    } else {
        "(l.encoding_id IS NULL OR NOT(l.encoding_fingerprint IS "
    });
    query
        .push("json_array(hex(")
        .push_bind(key)
        .push("),")
        .push_bind(algorithm.to_owned());
    query.push(if pack {
        ",k.pack_compressed,k.member_count"
    } else {
        ",l.standalone_compressed"
    });
    query.push(",p.compression_enabled,p.compression_strategy,p.compression_level,p.compression_sample_level,p.compression_min_savings_percent,p.compression_min_savings_bytes");
    if pack {
        query.push(",p.pack_max_size,p.pack_range_enabled,p.pack_range_min_downloads,p.pack_range_min_savings_percent,p.pack_range_min_savings_bytes,'pack'");
    } else {
        query.push(",'standalone'");
    }
    query
        .push(")) OR ((NOT p.compression_enabled OR p.compression_strategy='never') AND ")
        .push(if pack {
            "k.pack_compressed))"
        } else {
            "l.standalone_compressed))"
        });
}
pub(super) fn rewrite_counts(
    storage: Uuid,
    algorithm: &str,
    key: Option<Uuid>,
) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new("SELECT CAST(COALESCE(SUM(c.pack_id IS NULL AND ");
    rewrite_needed(&mut query, false, algorithm, key);
    query.push("),0) AS INTEGER),CAST(COALESCE(SUM(c.pack_id IS NOT NULL AND ");
    rewrite_needed(&mut query, true, algorithm, key);
    query.push("),0) AS INTEGER) FROM chunks c JOIN storage_profiles p ON p.storage_id=c.storage_id LEFT JOIN chunk_encodings l ON l.chunk_id=c.id AND l.state='ready' LEFT JOIN packs k ON k.id=c.pack_id AND k.state='ready' WHERE c.state='ready' AND c.storage_id=").push_bind(storage);
    query
}
pub(super) fn rewrite_loose_candidate(after: Id, job: &RewriteTask) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = chunk_query(true);
    query.push(" JOIN storage_profiles p ON p.storage_id=c.storage_id WHERE c.state='ready' AND c.pack_id IS NULL AND l.encoding_id IS NOT NULL AND c.id>").push_bind(after).push(" AND c.id<=").push_bind(job.upper_chunk_id).push(" AND c.storage_id=").push_bind(job.storage_id).push(" AND ");
    rewrite_needed(&mut query, false, &job.target_algorithm, job.target_key_id);
    query.push(" ORDER BY c.id LIMIT 1");
    query
}
pub(super) fn rewrite_pack_candidate(after: Id, job: &RewriteTask) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT MIN(c.id),k.id FROM chunks c JOIN packs k ON k.id=c.pack_id AND k.state='ready' JOIN storage_profiles p ON p.storage_id=c.storage_id WHERE c.state='ready' AND c.storage_id=",
    );
    query.push_bind(job.storage_id).push(" AND ");
    rewrite_needed(&mut query, true, &job.target_algorithm, job.target_key_id);
    query
        .push(" GROUP BY k.id HAVING MIN(c.id)>")
        .push_bind(after)
        .push(" AND MIN(c.id)<=")
        .push_bind(job.upper_chunk_id)
        .push(" ORDER BY MIN(c.id) LIMIT 1");
    query
}
pub(super) fn lock_rewrite_storage(storage: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM storages WHERE id=? AND enabled").bind(storage)
}
pub(super) fn active_rewrite(storage: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM tasks WHERE task_type='rewrite' AND json_extract(detail,'$.storage_id')=? AND state IN ('queued','running','paused') LIMIT 1").bind(storage.to_string())
}

pub(super) fn rewrite_upper(storage: Uuid) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT COALESCE(MAX(id),0) FROM chunks WHERE storage_id=?").bind(storage)
}
pub(super) fn lock_rewrite_chunk(chunk: Id) -> Rows<'static, (Option<Id>, Id)> {
    sqlx::query_as("SELECT pack_id,current_encoding_id FROM chunks WHERE id=? AND state='ready'")
        .bind(chunk)
}
pub(super) fn retire_rewrite_encoding(encoding: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET state='retired',unreferenced_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE encoding_id=? AND state='ready'").bind(encoding)
}
pub(super) fn rewrite_members(pack: Id) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new("SELECT ");
    query.push(CHUNK_COLUMNS).push(" FROM pack_members m JOIN chunks c ON c.id=m.chunk_id LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' WHERE m.pack_id=").push_bind(pack).push(" ORDER BY m.ordinal LIMIT 4097");
    query
}
pub(super) fn rewrite_current_members(pack: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM chunks WHERE pack_id=? AND state='ready' ORDER BY id")
        .bind(pack)
}
pub(super) fn lock_rewrite_members(pack: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM chunks WHERE pack_id=? AND state='ready' ORDER BY id")
        .bind(pack)
}
pub(super) fn replace_rewrite_pack(old: Id, new: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunks SET pack_id=? WHERE pack_id=? AND state='ready'")
        .bind(new)
        .bind(old)
}
pub(super) fn retire_rewrite_pack(pack: Id) -> Statement<'static> {
    sqlx::query("UPDATE packs SET state='retired',unreferenced_at=CAST(unixepoch('subsec')*1000000 AS INTEGER),owner_task=NULL WHERE id=? AND state='ready'").bind(pack)
}
pub(super) fn rewrite_encoding_owner(encoding: Id, task: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT state='uploading' AND owner_task=? FROM chunk_encodings WHERE encoding_id=?",
    )
    .bind(task)
    .bind(encoding)
}

pub(super) async fn pack_detail(
    connection: &mut sqlx::SqliteConnection,
    id: Id,
) -> Result<(Value, Vec<Value>)> {
    let pack = sqlx::query("SELECT * FROM packs WHERE id=?")
        .bind(id)
        .fetch_optional(&mut *connection)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    let members=sqlx::query_scalar("SELECT json_object('ordinal',m.ordinal,'chunk_id',CAST(m.chunk_id AS TEXT),'offset',m.offset_bytes,'raw_size',c.raw_size,'current',json(IIF(c.pack_id=m.pack_id,'true','false'))) FROM pack_members m JOIN chunks c ON c.id=m.chunk_id WHERE m.pack_id=? ORDER BY m.ordinal LIMIT 4096").bind(id).fetch_all(connection).await?;
    Ok((pack_value(pack)?, members))
}

use crate::model::integrity::{Cursor, Detail, Finding, IntegrityStore, MappingRange};
use crate::worker::integrity::Request as IntegrityRequest;

pub(super) fn integrity_busy() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM tasks WHERE task_type='integrity' AND json_extract(detail,'$.source_operation') IS NULL AND state IN ('queued','running'))",
    )
}
pub(super) fn integrity_storages(storage: Option<Uuid>) -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM storages WHERE enabled AND (? IS NULL OR id=?) ORDER BY created_at,id",
    )
    .bind(storage)
    .bind(storage)
}
pub(super) fn integrity_upper_chunk(storage: Option<Uuid>) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT COALESCE(MAX(id),0) FROM chunks WHERE (? IS NULL OR storage_id=?)")
        .bind(storage)
        .bind(storage)
}
fn integrity_scope(query: &mut sqlx::QueryBuilder<Sqlite>, detail: &Detail) {
    if let Some(storage) = detail.storage_id {
        query
            .push(" AND EXISTS(SELECT 1 FROM buckets b WHERE b.id=o.bucket_id AND b.storage_id=")
            .push_bind(storage)
            .push(")");
    }
    if let Some(bucket) = detail.bucket_id {
        query.push(" AND o.bucket_id=").push_bind(bucket);
    }
    if let Some(key) = &detail.key {
        query
            .push(" AND o.object_key=")
            .push_bind(key.as_bytes().to_vec());
    }
    if let Some((bucket, key)) = &detail.upper_object {
        query
            .push(" AND (o.bucket_id<")
            .push_bind(*bucket)
            .push(" OR(o.bucket_id=")
            .push_bind(*bucket)
            .push(" AND o.object_key<=")
            .push_bind(key.as_bytes().to_vec())
            .push("))");
    }
}
pub(super) async fn integrity_upper_object(
    connection: &mut sqlx::SqliteConnection,
    detail: &Detail,
) -> Result<Option<(Uuid, String)>> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT o.bucket_id,o.object_key FROM objects o WHERE o.stream_id IS NOT NULL",
    );
    integrity_scope(&mut query, detail);
    query.push(" ORDER BY o.bucket_id DESC,o.object_key DESC LIMIT 1");
    let row: Option<(Uuid, Vec<u8>)> = query.build_query_as().fetch_optional(connection).await?;
    row.map(|(bucket, key)| Ok((bucket, String::from_utf8(key)?)))
        .transpose()
}
pub(super) fn insert_integrity_finding(task: Uuid, issue: &Finding) -> Statement<'_> {
    // The write transaction serializes checkpoints; INSERT SELECT preserves an exact inserted count.
    sqlx::query("INSERT INTO integrity_issues(task_id,subject,code,chunk_id,location_id,stream_id,bucket_id,object_key,detail) SELECT ?,?,?,?,?,?,?,?,? WHERE NOT EXISTS(SELECT 1 FROM integrity_issues WHERE task_id=? AND subject=? AND code=?)").bind(task).bind(&issue.subject).bind(&issue.code).bind(issue.chunk_id).bind(issue.location_id).bind(issue.stream_id).bind(issue.bucket_id).bind(issue.key.as_deref().map(str::as_bytes)).bind(&issue.detail).bind(task).bind(&issue.subject).bind(&issue.code)
}
pub(super) fn integrity_checkpoint(
    task: Uuid,
    cursor: String,
    detail: Value,
    processed: i64,
    done: bool,
) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET progress_cursor=?,detail=?,processed=?,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER),state=IIF(?,'completed',state) WHERE id=?").bind(cursor).bind(detail).bind(processed).bind(done).bind(task)
}
fn integrity_finding_value(row: SqliteRow) -> Result<Value> {
    Ok(serde_json::json!({
        "id": row.try_get::<i64,_>("id")?.to_string(),
        "task_id": row.try_get::<Uuid,_>("task_id")?,
        "subject": row.try_get::<String,_>("subject")?,
        "code": row.try_get::<String,_>("code")?,
        "chunk_id": row.try_get::<Option<i64>,_>("chunk_id")?.map(|id| id.to_string()),
        "location_id": row.try_get::<Option<Uuid>,_>("location_id")?,
        "stream_id": row.try_get::<Option<Id>,_>("stream_id")?,
        "bucket_id": row.try_get::<Option<Uuid>,_>("bucket_id")?,
        "object_key": row.try_get::<Option<Vec<u8>>,_>("object_key")?.map(String::from_utf8).transpose()?,
        "detail": row.try_get::<Value,_>("detail")?,
        "created_at": date(&row,"created_at")?,
    }))
}

#[async_trait::async_trait]
impl IntegrityStore for Backend {
    async fn start(
        &self,
        input: IntegrityRequest,
        bucket: Option<Bucket>,
        node: Option<Uuid>,
    ) -> Result<(Uuid, usize)> {
        ensure!(node.is_none(), "SQLite does not support cluster nodes");
        transactions::start_integrity(&self.write, input, bucket).await
    }
    async fn chunks(&self, detail: &Detail, after: Id, limit: i64) -> Result<Vec<Chunk>> {
        let mut query = chunk_query(true);
        query.push(" WHERE c.state='ready' AND c.id>").push_bind(after).push(" AND c.id<=").push_bind(detail.upper_chunk_id.parse::<i64>()?).push(" AND EXISTS(SELECT 1 FROM extents e JOIN objects o ON o.stream_id=e.stream_id WHERE e.chunk_id=c.id");
        integrity_scope(&mut query, detail);
        query.push(") ORDER BY c.id LIMIT ").push_bind(limit);
        Ok(query.build_query_as().fetch_all(&self.read).await?)
    }
    async fn mapping(
        &self,
        detail: &Detail,
        cursor: &Cursor,
    ) -> Result<Option<(StoredStream, Uuid, String)>> {
        let mut query = sqlx::QueryBuilder::<Sqlite>::new(
            "SELECT s.*,o.bucket_id AS visible_bucket,o.object_key AS visible_key FROM objects o JOIN streams s ON s.id=o.stream_id WHERE TRUE",
        );
        integrity_scope(&mut query, detail);
        if let Some(stream) = cursor.stream {
            query.push(" AND o.stream_id=").push_bind(stream);
        } else if let (Some(bucket), Some(key)) = (cursor.bucket, &cursor.key) {
            query
                .push(" AND (o.bucket_id>")
                .push_bind(bucket)
                .push(" OR(o.bucket_id=")
                .push_bind(bucket)
                .push(" AND o.object_key>")
                .push_bind(key.as_bytes().to_vec())
                .push("))");
        }
        query.push(" ORDER BY o.bucket_id,o.object_key LIMIT 1");
        let row = query.build().fetch_optional(&self.read).await?;
        row.map(|r| {
            Ok((
                sqlx::FromRow::from_row(&r)?,
                r.try_get("visible_bucket")?,
                String::from_utf8(r.try_get("visible_key")?)?,
            ))
        })
        .transpose()
    }
    async fn ranges(&self, stream: Id, after: i64) -> Result<Vec<MappingRange>> {
        Ok(sqlx::query_as("SELECT e.offset_bytes,e.length,e.source_offset,e.fragment_id,e.chunk_id,c.state,c.raw_size FROM extents e LEFT JOIN chunks c ON c.id=e.chunk_id WHERE e.stream_id=? AND e.offset_bytes>? ORDER BY e.offset_bytes LIMIT 65").bind(stream).bind(after).fetch_all(&self.read).await?)
    }
    async fn current(&self, bucket: Uuid, key: &str, stream: Id) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM objects WHERE bucket_id=? AND object_key=? AND stream_id=?)").bind(bucket).bind(key.as_bytes()).bind(stream).fetch_one(&self.read).await?)
    }
    async fn chunk_stream(&self, chunk: Id, detail: &Detail) -> Result<Option<Id>> {
        let mut query = sqlx::QueryBuilder::<Sqlite>::new(
            "SELECT o.stream_id FROM extents e JOIN objects o ON o.stream_id=e.stream_id JOIN chunks c ON c.id=e.chunk_id WHERE c.state='ready' AND c.id=",
        );
        query.push_bind(chunk);
        integrity_scope(&mut query, detail);
        query.push(" ORDER BY o.bucket_id,o.object_key LIMIT 1");
        Ok(query
            .build_query_scalar()
            .fetch_optional(&self.read)
            .await?)
    }
    async fn checkpoint(
        &self,
        task: Execution,
        cursor: String,
        detail: Detail,
        issues: Vec<Finding>,
    ) -> Result<()> {
        transactions::checkpoint_integrity(&self.write, task, cursor, detail, issues).await
    }
    async fn exists(&self, task: Uuid) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM tasks WHERE id=? AND task_type='integrity')",
        )
        .bind(task)
        .fetch_one(&self.read)
        .await?)
    }
    async fn report_task(&self, task: Uuid) -> Result<Option<Value>> {
        sqlx::query("SELECT * FROM tasks WHERE id=? AND task_type='integrity'")
            .bind(task)
            .fetch_optional(&self.read)
            .await?
            .map(|row| task_value(row, false))
            .transpose()
    }
    async fn report_upper(&self, task: Uuid) -> Result<Id> {
        Ok(
            sqlx::query_scalar("SELECT COALESCE(MAX(id),0) FROM integrity_issues WHERE task_id=?")
                .bind(task)
                .fetch_one(&self.read)
                .await?,
        )
    }
    async fn report_page(&self, task: Uuid, after: Id, upper: Id) -> Result<Vec<Value>> {
        sqlx::query("SELECT * FROM integrity_issues WHERE task_id=? AND id>? AND id<=? ORDER BY id LIMIT 100").bind(task).bind(after).bind(upper).fetch_all(&self.read).await?.into_iter().map(integrity_finding_value).collect()
    }
    async fn findings(
        &self,
        task: Uuid,
        after: Id,
        limit: i64,
        code: Option<&str>,
    ) -> Result<Vec<Value>> {
        sqlx::query("SELECT * FROM integrity_issues WHERE task_id=? AND id>? AND (? IS NULL OR code=?) ORDER BY id LIMIT ?").bind(task).bind(after).bind(code).bind(code).bind(limit).fetch_all(&self.read).await?.into_iter().map(integrity_finding_value).collect()
    }
    async fn groups(&self, task: Uuid) -> Result<Vec<(String, String)>> {
        Ok(sqlx::query_as("SELECT code,CAST(COUNT(*) AS TEXT) FROM integrity_issues WHERE task_id=? GROUP BY code ORDER BY code").bind(task).fetch_all(&self.read).await?)
    }
    async fn objects(
        &self,
        task: Uuid,
        issue: Id,
        after: Option<(Uuid, String)>,
    ) -> Result<Vec<Value>> {
        transactions::integrity_objects(&self.read, task, issue, after).await
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for MappingRange {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            offset_bytes: row.try_get("offset_bytes")?,
            length: row.try_get("length")?,
            source_offset: row.try_get("source_offset")?,
            fragment_id: row.try_get("fragment_id")?,
            chunk_id: row.try_get("chunk_id")?,
            state: row.try_get("state")?,
            raw_size: row.try_get("raw_size")?,
        })
    }
}

pub(super) async fn integrity_objects(
    connection: &mut sqlx::SqliteConnection,
    task: Uuid,
    issue: Id,
    after: Option<(Uuid, String)>,
) -> Result<Vec<Value>> {
    let (chunk, stream): (Option<i64>, Option<Id>) =
        sqlx::query_as("SELECT chunk_id,stream_id FROM integrity_issues WHERE task_id=? AND id=?")
            .bind(task)
            .bind(issue)
            .fetch_optional(&mut *connection)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    let mut query = sqlx::QueryBuilder::<Sqlite>::new(
        "SELECT o.bucket_id,b.name AS bucket_name,o.object_key,o.stream_id FROM objects o JOIN buckets b ON b.id=o.bucket_id WHERE ",
    );
    if let Some(chunk) = chunk {
        query
            .push("EXISTS(SELECT 1 FROM extents WHERE stream_id=o.stream_id AND chunk_id=")
            .push_bind(chunk)
            .push(")");
    } else {
        query.push("o.stream_id=").push_bind(stream);
    }
    if let Some((bucket, key)) = after {
        query
            .push(" AND (o.bucket_id>")
            .push_bind(bucket)
            .push(" OR(o.bucket_id=")
            .push_bind(bucket)
            .push(" AND o.object_key>")
            .push_bind(key.into_bytes())
            .push("))");
    }
    query.push(" ORDER BY o.bucket_id,o.object_key LIMIT 101");
    query
        .build()
        .fetch_all(connection)
        .await?
        .into_iter()
        .map(|row| {
            Ok(serde_json::json!({
                "bucket_id": row.try_get::<Uuid,_>("bucket_id")?,
                "bucket": row.try_get::<String,_>("bucket_name")?,
                "key": object_key(&row)?,
                "version": row.try_get::<Id,_>("stream_id")?,
            }))
        })
        .collect()
}

use crate::model::maintenance::{
    FragmentCleanup, MaintenanceExecution, MaintenancePreview, MaintenanceStore, RecoveryFragment,
};

pub(super) fn reset_access_coverage() -> Statement<'static> {
    sqlx::query(
        "UPDATE access_state SET coverage_since=CAST(unixepoch('subsec')*1000000 AS INTEGER),flushed_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=1",
    )
}
pub(super) fn abandon_startup_streams() -> Statement<'static> {
    sqlx::query(
        "UPDATE streams SET state='abandoned',owner_task_id=NULL,owner_execution_token=NULL,touched_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE state='writing'",
    )
}
pub(super) fn fail_startup_chunks() -> Statement<'static> {
    sqlx::query(
        "UPDATE chunks SET state='failed',unreferenced_at=COALESCE(unreferenced_at,CAST(unixepoch('subsec')*1000000 AS INTEGER)) WHERE state IN ('preparing','uploading')",
    )
}
pub(super) fn retire_startup_encodings() -> Statement<'static> {
    sqlx::query(
        "UPDATE chunk_encodings SET owner_stream=NULL,state='retired',unreferenced_at=COALESCE(unreferenced_at,CAST(unixepoch('subsec')*1000000 AS INTEGER)) WHERE state='uploading'",
    )
}
pub(super) fn retire_startup_packs() -> Statement<'static> {
    sqlx::query(
        "UPDATE packs SET state='retired',unreferenced_at=COALESCE(unreferenced_at,CAST(unixepoch('subsec')*1000000 AS INTEGER)) WHERE state='preparing'",
    )
}
pub(super) fn clear_pack_inputs() -> Statement<'static> {
    sqlx::query("DELETE FROM pack_inputs")
}
pub(super) fn reset_completing_uploads() -> Statement<'static> {
    sqlx::query("UPDATE uploads SET state='active' WHERE state='completing'")
}
pub(super) fn reset_startup_tasks(maintenance: bool) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state=IIF(? AND task_type IN ('purge','pack','unpack','upload','cache_flush','storage_migration','rewrite'),'paused','queued'),execution_token=NULL,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE state='running' OR (? AND state='queued' AND task_type IN ('purge','pack','unpack','upload','cache_flush','storage_migration','rewrite'))").bind(maintenance).bind(maintenance)
}
fn stale_stream_filter(query: &mut sqlx::QueryBuilder<Sqlite>, retention: Option<f64>) {
    query.push(" AND ((s.state IN ('retired','abandoned') AND (").push_bind(retention).push(" IS NULL OR s.touched_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-").push_bind(retention.map(|value|(value*1_000_000.) as i64))
        .push(")) OR s.state='writing') AND NOT EXISTS(SELECT 1 FROM objects WHERE stream_id=s.id) AND NOT EXISTS(SELECT 1 FROM parts WHERE stream_id=s.id) AND NOT EXISTS(SELECT 1 FROM uploads WHERE output_stream=s.id AND state IN ('active','completing')) AND NOT EXISTS(SELECT 1 FROM tasks t WHERE t.id=s.owner_task_id AND t.state='running' AND t.execution_token=s.owner_execution_token)");
}

pub(super) fn stale_streams(
    active: &[Id],
    batch: i64,
    storage: Option<Uuid>,
    retention: Option<f64>,
) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new("SELECT s.id FROM streams s WHERE TRUE");
    if !active.is_empty() {
        query.push(" AND s.id NOT IN ");
        chunk_ids(&mut query, active);
    }
    if let Some(storage) = storage {
        query
            .push(" AND EXISTS(SELECT 1 FROM buckets b WHERE b.id=s.bucket_id AND b.storage_id=")
            .push_bind(storage)
            .push(")");
    }
    stale_stream_filter(&mut query, retention);
    query
        .push(" ORDER BY s.touched_at,s.id LIMIT ")
        .push_bind(batch);
    query
}
pub(super) fn lock_stale_stream(
    id: Id,
    storage: Option<Uuid>,
    retention: Option<f64>,
) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new("SELECT s.id FROM streams s WHERE s.id=");
    query.push_bind(id);
    if let Some(storage) = storage {
        query
            .push(" AND EXISTS(SELECT 1 FROM buckets b WHERE b.id=s.bucket_id AND b.storage_id=")
            .push_bind(storage)
            .push(")");
    }
    stale_stream_filter(&mut query, retention);
    query.push("");
    query
}
pub(super) fn abandon_cleanup_stream(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET state='abandoned',owner_task_id=NULL,owner_execution_token=NULL WHERE id=?").bind(id)
}
pub(super) fn cleanup_stream_bucket(id: Id) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT bucket_id FROM streams WHERE id=?").bind(id)
}
pub(super) fn lock_cleanup_stream(id: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM streams s WHERE id=? AND state='abandoned' AND NOT EXISTS(SELECT 1 FROM objects WHERE stream_id=s.id) AND NOT EXISTS(SELECT 1 FROM parts WHERE stream_id=s.id) AND NOT EXISTS(SELECT 1 FROM uploads WHERE output_stream=s.id AND state IN ('active','completing'))").bind(id)
}
pub(super) async fn detach_stream_chunks(
    connection: &mut sqlx::SqliteConnection,
    id: Id,
    batch: i64,
) -> Result<u64> {
    Ok(sqlx::query("UPDATE chunks AS c SET owner_stream=NULL,state=IIF(state IN ('preparing','uploading'),'failed',state),unreferenced_at=IIF(NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id),COALESCE(unreferenced_at,CAST(unixepoch('subsec')*1000000 AS INTEGER)),unreferenced_at) WHERE id IN (SELECT id FROM chunks WHERE owner_stream=? ORDER BY id LIMIT ?)").bind(id).bind(batch).execute(connection).await?.rows_affected())
}

pub(super) async fn remove_stream_extents(
    connection: &mut sqlx::SqliteConnection,
    id: Id,
    batch: i64,
) -> Result<usize> {
    let rows: Vec<(Id, Option<Id>)> = sqlx::query_as(
        "SELECT offset_bytes,chunk_id FROM extents WHERE stream_id=? ORDER BY offset_bytes LIMIT ?",
    )
    .bind(id)
    .bind(batch)
    .fetch_all(&mut *connection)
    .await?;
    let Some((last, _)) = rows.last() else {
        return Ok(0);
    };
    sqlx::query("DELETE FROM extents WHERE stream_id=? AND offset_bytes<=?")
        .bind(id)
        .bind(last)
        .execute(&mut *connection)
        .await?;
    let chunks: Vec<Id> = rows.iter().filter_map(|r| r.1).collect();
    mark_unreferenced_chunks(connection, &chunks).await?;
    Ok(rows.len())
}
pub(super) fn cleanup_stream_empty(id: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM raw_ranges WHERE stream_id=?) AND NOT EXISTS(SELECT 1 FROM extents WHERE stream_id=?) AND NOT EXISTS(SELECT 1 FROM chunks WHERE owner_stream=?) AND NOT EXISTS(SELECT 1 FROM fragments WHERE owner_stream=?) AND NOT EXISTS(SELECT 1 FROM chunk_encodings WHERE owner_stream=?)").bind(id).bind(id).bind(id).bind(id).bind(id)
}
pub(super) fn detach_upload_output(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE uploads SET output_stream=NULL WHERE output_stream=? AND state IN ('completed','aborted')").bind(id)
}
pub(super) fn remove_empty_stream(id: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM streams WHERE id=?").bind(id)
}
pub(super) fn stale_fragments(
    batch: i64,
    active: &[Id],
    storage: Option<Uuid>,
) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT f.id FROM fragments f WHERE NOT EXISTS(SELECT 1 FROM extents WHERE fragment_id=f.id)",
    );
    if let Some(storage) = storage {
        query.push(" AND EXISTS(SELECT 1 FROM streams s JOIN buckets b ON b.id=s.bucket_id WHERE s.id=f.owner_stream AND b.storage_id=").push_bind(storage).push(")");
    }
    query.push(" AND (f.sealed OR f.owner_stream IS NULL OR (");
    if !active.is_empty() {
        query.push("f.owner_stream NOT IN ");
        chunk_ids(&mut query, active);
        query.push(" AND ");
    }
    query.push("NOT EXISTS(SELECT 1 FROM streams s WHERE s.id=f.owner_stream AND s.state='writing'))) ORDER BY f.created_at,f.id LIMIT ").push_bind(batch);
    query
}
pub(super) fn lock_cleanup_fragment(
    id: Id,
) -> Rows<'static, (Uuid, bool, Option<Id>, Option<Uuid>)> {
    sqlx::query_as("SELECT location_id,sealed,owner_stream,file_id FROM fragments f WHERE id=? AND NOT EXISTS(SELECT 1 FROM extents WHERE fragment_id=f.id)").bind(id)
}
pub(super) fn remove_fragment_row(id: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM fragments WHERE id=?").bind(id)
}
pub(super) async fn remove_tombstones(
    connection: &mut sqlx::SqliteConnection,
    storage: Option<Uuid>,
    batch: i64,
) -> Result<u64> {
    Ok(sqlx::query("DELETE FROM objects WHERE (bucket_id,object_key) IN (SELECT o.bucket_id,o.object_key FROM objects o WHERE (? IS NULL OR EXISTS(SELECT 1 FROM buckets b WHERE b.id=o.bucket_id AND b.storage_id=?)) AND o.stream_id IS NULL AND NOT EXISTS(SELECT 1 FROM streams s WHERE s.bucket_id=o.bucket_id AND s.object_key=o.object_key AND s.state='writing') ORDER BY o.bucket_id,o.object_key LIMIT ?)").bind(storage).bind(storage).bind(batch).execute(connection).await?.rows_affected())
}

pub(super) fn lock_reclaim_chunk(
    id: Id,
    storage: Uuid,
    grace: f64,
) -> Rows<'static, (Option<Id>,)> {
    sqlx::query_as("SELECT owner_stream FROM chunks c WHERE id=? AND storage_id=? AND state IN ('ready','failed','deleting') AND unreferenced_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-? AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id) AND NOT EXISTS(SELECT 1 FROM pack_inputs WHERE chunk_id=c.id) AND NOT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=c.id) AND NOT EXISTS(SELECT 1 FROM chunk_encodings WHERE chunk_id=c.id AND owner_stream IS NOT NULL)").bind(id).bind(storage).bind((grace*1_000_000.) as i64)
}
pub(super) fn claim_reclaim_chunk(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunks SET state='deleting',pack_id=NULL WHERE id=?").bind(id)
}
pub(super) fn reclaim_locations(id: Id) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT location_id FROM chunk_encodings WHERE chunk_id=? AND state IN ('ready','uploading','retired','deleting')").bind(id)
}
pub(super) fn retire_chunk_encodings(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings AS e SET state='retired',unreferenced_at=COALESCE(e.unreferenced_at,c.unreferenced_at,CAST(unixepoch('subsec')*1000000 AS INTEGER)) FROM chunks c WHERE c.id=e.chunk_id AND c.id=? AND e.state IN ('ready','uploading')").bind(id)
}
pub(super) fn mark_orphan_chunks() -> Statement<'static> {
    sqlx::query(
        "UPDATE chunks AS c SET unreferenced_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE c.state='ready' AND c.owner_stream IS NULL AND c.unreferenced_at IS NULL AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id)",
    )
}
fn physical_table(packed: bool) -> (&'static str, &'static str) {
    if packed {
        ("packs", "id")
    } else {
        ("chunk_encodings", "encoding_id")
    }
}
pub(super) async fn cleanup_history(
    connection: &mut sqlx::SqliteConnection,
    section: &str,
    age: f64,
    batch: i64,
    active: &[Uuid],
) -> Result<u64> {
    let (table, id, time, guard) = match section {
        "key_replacements" => ("key_replacements", "id", "expires_at", "TRUE"),
        "maintenance_previews" => ("maintenance_previews", "id", "expires_at", "TRUE"),
        "media_operations" => ("media_operations", "id", "created_at", "TRUE"),
        "audit_events" => ("audit_events", "id", "created_at", "TRUE"),
        "chunks" => (
            "chunks",
            "id",
            "deleted_at",
            "h.state='deleted' AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=h.id) AND NOT EXISTS(SELECT 1 FROM chunk_encodings WHERE chunk_id=h.id AND (state<>'deleted' OR recovery_pending OR owner_stream IS NOT NULL)) AND NOT EXISTS(SELECT 1 FROM pack_members WHERE chunk_id=h.id) AND NOT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=h.id) AND NOT EXISTS(SELECT 1 FROM pack_inputs WHERE chunk_id=h.id) AND NOT EXISTS(SELECT 1 FROM cache_pins WHERE chunk_id=h.id)",
        ),
        "packs" => (
            "packs",
            "id",
            "deleted_at",
            "h.state='deleted' AND NOT EXISTS(SELECT 1 FROM chunks WHERE pack_id=h.id) AND NOT EXISTS(SELECT 1 FROM pending_uploads WHERE source_pack_id=h.id)",
        ),
        "chunk_encodings" => (
            "chunk_encodings",
            "encoding_id",
            "deleted_at",
            "h.state='deleted' AND NOT h.recovery_pending AND h.owner_stream IS NULL",
        ),
        "uploads" => (
            "uploads",
            "id",
            "touched_at",
            "h.state IN ('completed','aborted') AND NOT EXISTS(SELECT 1 FROM parts WHERE upload_id=h.id) AND NOT EXISTS(SELECT 1 FROM quota_reservations WHERE upload_id=h.id)",
        ),
        "integrity_issues" => (
            "integrity_issues",
            "id",
            "(SELECT t.updated_at FROM tasks t WHERE t.id=h.task_id AND t.state='completed')",
            "TRUE",
        ),
        "tasks" => (
            "tasks",
            "id",
            "updated_at",
            "h.state='completed' AND NOT EXISTS(SELECT 1 FROM integrity_issues WHERE task_id=h.id)",
        ),
        "sessions" => (
            "sessions",
            "id",
            "expires_at",
            "NOT EXISTS(SELECT 1 FROM session_refresh_tokens r WHERE r.session_id=h.id AND r.revoked_at IS NULL AND r.expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER))",
        ),
        "refresh_tokens" => (
            "session_refresh_tokens",
            "id",
            "revoked_at",
            "NOT EXISTS(SELECT 1 FROM session_refresh_tokens live WHERE live.session_id=h.session_id AND live.family_id=h.family_id AND live.revoked_at IS NULL AND live.expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER))",
        ),
        _ => anyhow::bail!("unknown cleanup section"),
    };
    let duration = match section {
        "chunks" | "packs" | "chunk_encodings" => Some(
            "(SELECT (CAST(substr(sp.deleted_chunk_retention,1,length(sp.deleted_chunk_retention)-1) AS INTEGER)*CASE substr(sp.deleted_chunk_retention,-1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=h.storage_id)",
        ),
        "uploads" => Some(
            "(SELECT (CAST(substr(sp.upload_retention,1,length(sp.upload_retention)-1) AS INTEGER)*CASE substr(sp.upload_retention,-1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM buckets b JOIN storage_profiles sp ON sp.storage_id=b.storage_id WHERE b.id=h.bucket_id)",
        ),
        _ => None,
    };
    let mut query = sqlx::QueryBuilder::new(format!(
        "DELETE FROM {table} WHERE {id} IN (SELECT h.{id} FROM {table} h WHERE {guard} AND {time}<"
    ));
    if let Some(duration) = duration {
        query.push(format!(
            "CAST(unixepoch('subsec')*1000000 AS INTEGER)-({duration})*1000000"
        ));
    } else {
        query
            .push("CAST(unixepoch('subsec')*1000000 AS INTEGER)-")
            .push_bind((age * 1_000_000.) as i64);
    }
    if section == "uploads" && !active.is_empty() {
        query.push(" AND h.id NOT IN ");
        uuid_ids(&mut query, active);
    }
    query
        .push(format!(" ORDER BY {time},h.{id} LIMIT "))
        .push_bind(batch)
        .push(")");
    Ok(query.build().execute(connection).await?.rows_affected())
}

#[async_trait::async_trait]
impl MaintenanceStore for Backend {
    async fn preview(
        &self,
        principal: &Principal,
        action: &str,
        parameters: Value,
        impact: Value,
        confirmation: String,
    ) -> Result<MaintenancePreview> {
        transactions::create_maintenance_preview(
            self,
            principal,
            action,
            parameters,
            impact,
            confirmation,
        )
        .await
    }
    async fn execution(
        &self,
        principal: &Principal,
        id: Uuid,
        action: &str,
    ) -> Result<Option<MaintenanceExecution>> {
        transactions::maintenance_execution(self, principal, id, action).await
    }
    async fn destructive_sweep(&self) -> Result<bool> {
        Ok(destructive_sweep().fetch_one(&self.read).await?)
    }
    async fn mode(&self, enabled: bool) -> Result<()> {
        transactions::maintenance_mode(&self.write, enabled).await
    }
    async fn pack_creation(&self, paused: bool) -> Result<()> {
        transactions::change_pack_creation(&self.write, paused).await
    }
    async fn policy_paused(&self, policy: &str) -> Result<bool> {
        Ok(maintenance_policy_paused(policy)
            .fetch_one(&self.read)
            .await?)
    }
    async fn controls(&self) -> Result<Vec<Value>> {
        transactions::read_maintenance_controls(&self.read).await
    }
    async fn preparing_packs(&self) -> Result<String> {
        Ok(preparing_pack_count().fetch_one(&self.read).await?)
    }
    async fn change_policy(&self, policy: &str, paused: bool) -> Result<()> {
        transactions::change_maintenance_policy(&self.write, policy, paused).await
    }
    async fn start_policy(&self, policy: &str, node: Option<Uuid>) -> Result<(Uuid, bool)> {
        ensure!(node.is_none(), "SQLite does not support cluster nodes");
        transactions::start_lifecycle_policy(&self.write, policy).await
    }
    async fn finish_policy(
        &self,
        id: Execution,
        count: i64,
        again: bool,
        report: Value,
    ) -> Result<()> {
        transactions::finish_lifecycle_policy(&self.write, id, count, again, report).await
    }
    async fn schedule(&self) -> Result<Vec<(String, bool, Option<f64>)>> {
        Ok(maintenance_schedule().fetch_all(&self.read).await?)
    }
    async fn reuse_due(&self) -> Result<bool> {
        Ok(due_pack_reuse().fetch_one(&self.read).await?)
    }
    async fn scheduled(&self, policy: &str) -> Result<()> {
        scheduled_maintenance(policy).execute(&self.write).await?;
        Ok(())
    }
    async fn chunk_candidates(
        &self,
        storage: Uuid,
        grace: f64,
        batch: i64,
    ) -> Result<Vec<(Id, i32)>> {
        Ok(sqlx::query_as("SELECT id,raw_size FROM chunks c WHERE storage_id=? AND state IN ('ready','failed','deleting') AND unreferenced_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-? AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id) AND NOT EXISTS(SELECT 1 FROM pack_inputs WHERE chunk_id=c.id) AND NOT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=c.id) ORDER BY unreferenced_at,id LIMIT ?").bind(storage).bind((grace*1_000_000.) as i64).bind(batch).fetch_all(&self.read).await?)
    }
    async fn cleanup_history(
        &self,
        section: &str,
        age: f64,
        batch: i64,
        active: &[Uuid],
        timeout_ms: u128,
    ) -> Result<u64> {
        let _preview = if section == "maintenance_previews" {
            let Ok(guard) = self.previews.clone().try_lock_owned() else {
                return Ok(0);
            };
            Some(guard)
        } else {
            None
        };
        transactions::cleanup_history(&self.write, section, age, batch, active, timeout_ms).await
    }
    async fn recover(&self, maintenance: bool) -> Result<()> {
        transactions::recover_runtime(&self.write, maintenance).await
    }
    async fn fragment_exists(&self, id: Uuid) -> Result<bool> {
        Ok(
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM fragments WHERE location_id=?) OR EXISTS(SELECT 1 FROM multipart_files WHERE id=?)")
                .bind(id).bind(id)
                .fetch_one(&self.read)
                .await?,
        )
    }
    async fn recovery_fragments(&self, after: Id) -> Result<Vec<RecoveryFragment>> {
        Ok(sqlx::query_as("SELECT f.id,f.location_id AS fragment_location,f.size,f.hash,f.file_id,f.file_offset,m.storage_id AS file_storage,CASE WHEN m.source_id IS NOT NULL THEN m.id END AS raw_source FROM fragments f LEFT JOIN multipart_files m ON m.id=f.file_id WHERE f.id>? AND EXISTS(SELECT 1 FROM extents WHERE fragment_id=f.id) ORDER BY f.id LIMIT 128").bind(after).fetch_all(&self.read).await?)
    }
    async fn fragment_uploads(&self, id: Id) -> Result<Vec<Uuid>> {
        Ok(sqlx::query_scalar("SELECT DISTINCT p.upload_id FROM parts p JOIN extents e ON e.stream_id=p.stream_id WHERE e.fragment_id=?").bind(id).fetch_all(&self.read).await?)
    }
    async fn expired_uploads(&self, batch: i64, storage: Option<Uuid>) -> Result<Vec<Uuid>> {
        Ok(sqlx::query_scalar("SELECT u.id FROM uploads u JOIN buckets b ON b.id=u.bucket_id JOIN storage_profiles p ON p.storage_id=b.storage_id WHERE (? IS NULL OR b.storage_id=?) AND u.state='active' AND u.touched_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-(CAST(substr(p.multipart_idle_timeout,1,length(p.multipart_idle_timeout)-1) AS INTEGER)*CASE substr(p.multipart_idle_timeout,-1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END)*1000000 ORDER BY u.touched_at,u.id LIMIT ?").bind(storage).bind(storage).bind(batch).fetch_all(&self.read).await?)
    }

    async fn stale_streams(
        &self,
        active: &[Id],
        batch: i64,
        storage: Option<Uuid>,
        retention: Option<f64>,
    ) -> Result<Vec<Id>> {
        Ok(stale_streams(active, batch, storage, retention)
            .build_query_scalar()
            .fetch_all(&self.read)
            .await?)
    }
    async fn claim_stream(
        &self,
        id: Id,
        storage: Option<Uuid>,
        retention: Option<f64>,
    ) -> Result<bool> {
        transactions::claim_stale_stream(&self.write, id, storage, retention).await
    }
    async fn cleanup_stream(&self, id: Id, batch: i64) -> Result<usize> {
        transactions::cleanup_stream(&self.write, id, batch).await
    }
    async fn stale_fragments(
        &self,
        batch: i64,
        active: &[Id],
        storage: Option<Uuid>,
    ) -> Result<Vec<Id>> {
        Ok(stale_fragments(batch, active, storage)
            .build_query_scalar()
            .fetch_all(&self.read)
            .await?)
    }
    async fn claim_fragment(&self, id: Id) -> Result<Option<FragmentCleanup>> {
        transactions::claim_fragment(&self.write, id).await
    }
    async fn cleanup_tombstones(&self, storage: Option<Uuid>, batch: i64) -> Result<u64> {
        transactions::cleanup_tombstones(&self.write, storage, batch).await
    }
    async fn gc_paused(&self) -> Result<bool> {
        Ok(
            sqlx::query_scalar("SELECT gc_paused OR maintenance FROM mokyu_meta WHERE id=1")
                .fetch_one(&self.read)
                .await?,
        )
    }
    async fn storages(&self) -> Result<Vec<Uuid>> {
        Ok(
            sqlx::query_scalar("SELECT storage_id FROM storage_profiles ORDER BY storage_id")
                .fetch_all(&self.read)
                .await?,
        )
    }
    async fn retire_chunk(
        &self,
        id: Id,
        storage: Uuid,
        grace: f64,
        active: &[Id],
    ) -> Result<Option<Vec<Uuid>>> {
        transactions::retire_chunk(&self.write, id, storage, grace, active).await
    }
    async fn delete_chunk(&self, id: Id, storage: Uuid) -> Result<()> {
        sqlx::query("UPDATE chunks SET state='deleted',deleted_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND storage_id=? AND state='deleting'").bind(id).bind(storage).execute(&self.write).await?;
        Ok(())
    }
    async fn retire_packs(&self, storage: Uuid) -> Result<()> {
        sqlx::query("UPDATE packs AS p SET state='retired',unreferenced_at=COALESCE(unreferenced_at,(SELECT MAX(c.unreferenced_at) FROM pack_members m JOIN chunks c ON c.id=m.chunk_id WHERE m.pack_id=p.id),CAST(unixepoch('subsec')*1000000 AS INTEGER)) WHERE state='ready' AND NOT EXISTS(SELECT 1 FROM chunks WHERE pack_id=p.id AND state='ready') AND p.storage_id=?").bind(storage).execute(&self.write).await?;
        Ok(())
    }
    async fn physical_candidates(
        &self,
        packed: bool,
        grace: f64,
        batch: i64,
        minimum: f64,
        storage: Uuid,
    ) -> Result<Vec<(Id, Uuid, bool)>> {
        let state_guard = if packed {
            "state IN ('retired','deleting')"
        } else {
            "(state IN ('retired','deleting') OR (state='deleted' AND recovery_pending)) AND owner_stream IS NULL"
        };
        let (table, id) = physical_table(packed);
        let unknown = if packed {
            "stored_at IS NULL"
        } else {
            "stored_at IS NULL OR recovery_pending"
        };
        Ok(sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT {id},location_id,({unknown}) FROM {table} WHERE {state_guard} AND unreferenced_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-? AND (?=0 OR stored_at IS NULL OR stored_at<=CAST(unixepoch('subsec')*1000000 AS INTEGER)-?) AND storage_id=? ORDER BY unreferenced_at,{id} LIMIT ?"))).bind((grace*1_000_000.) as i64).bind(minimum).bind((minimum*1_000_000.) as i64).bind(storage).bind(batch).fetch_all(&self.read).await?)
    }
    async fn physical_age(&self, packed: bool, id: Id, stored: DateTime<Utc>) -> Result<()> {
        if !packed {
            sqlx::query("UPDATE chunk_encodings SET stored_at=CASE WHEN recovery_pending THEN ? ELSE COALESCE(stored_at,?) END WHERE encoding_id=?").bind(stored.timestamp_micros()).bind(stored.timestamp_micros()).bind(id).execute(&self.write).await?;
            return Ok(());
        }
        let (table, column) = physical_table(packed);
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE {table} SET stored_at=COALESCE(stored_at,?) WHERE {column}=?"
        )))
        .bind(stored.timestamp_micros())
        .bind(id)
        .execute(&self.write)
        .await?;
        Ok(())
    }
    async fn claim_physical(&self, packed: bool, id: Id, grace: f64, minimum: f64) -> Result<bool> {
        let state_guard = if packed {
            "state IN ('retired','deleting')"
        } else {
            "(state IN ('retired','deleting') OR (state='deleted' AND recovery_pending)) AND owner_stream IS NULL"
        };
        let (table, column) = physical_table(packed);
        Ok(sqlx::query(sqlx::AssertSqlSafe(format!("UPDATE {table} SET state='deleting' WHERE {column}=? AND {state_guard} AND unreferenced_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-? AND (?=0 OR stored_at IS NULL OR stored_at<=CAST(unixepoch('subsec')*1000000 AS INTEGER)-?)"))).bind(id).bind((grace*1_000_000.) as i64).bind(minimum).bind((minimum*1_000_000.) as i64).execute(&self.write).await?.rows_affected()!=0)
    }
    async fn delete_physical(&self, packed: bool, id: Id, retention: f64) -> Result<()> {
        if !packed {
            sqlx::query("UPDATE chunk_encodings SET recovery_pending=recovery_pending AND (deleted_at IS NULL OR deleted_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)-?),state='deleted',unreferenced_at=CAST(unixepoch('subsec')*1000000 AS INTEGER),deleted_at=COALESCE(deleted_at,CAST(unixepoch('subsec')*1000000 AS INTEGER)) WHERE encoding_id=? AND state='deleting'").bind((retention*1_000_000.) as i64).bind(id).execute(&self.write).await?;
            return Ok(());
        }
        let (table, column) = physical_table(packed);
        sqlx::query(sqlx::AssertSqlSafe(format!("UPDATE {table} SET state='deleted',deleted_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE {column}=? AND state='deleting'"))).bind(id).execute(&self.write).await?;
        Ok(())
    }
    async fn gc_status(&self) -> Result<(Vec<(String, i64)>, bool, bool)> {
        transactions::gc_status(&self.read).await
    }
    async fn mark_orphans(&self) -> Result<usize> {
        transactions::mark_orphan_chunks(&self.write).await
    }
}

pub(super) fn destructive_sweep() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM tasks WHERE task_type='sweep' AND json_extract(detail,'$.dry_run')=FALSE AND state IN ('queued','running'))",
    )
}
pub(super) fn set_maintenance_mode(enabled: bool) -> Statement<'static> {
    sqlx::query("UPDATE mokyu_meta SET maintenance=? WHERE id=1").bind(enabled)
}
pub(super) fn pause_maintenance_tasks() -> Statement<'static> {
    sqlx::query(
        "UPDATE tasks SET state='paused',updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE task_type IN ('purge','pack','unpack','storage_migration','rewrite') AND state IN ('queued','running')",
    )
}
pub(super) fn unpacking_all() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM tasks WHERE task_type='unpack' AND json_extract(detail,'$.all')=TRUE AND state<>'completed')",
    )
}
pub(super) fn pause_pack_creation(paused: bool) -> Statement<'static> {
    sqlx::query("UPDATE mokyu_meta SET pack_creation_paused=? WHERE id=1").bind(paused)
}
pub(super) fn maintenance_policy_paused(policy: &str) -> Scalar<'_, bool> {
    sqlx::query_scalar("SELECT IIF(c.policy='gc',m.gc_paused,c.paused) FROM maintenance_controls c CROSS JOIN mokyu_meta m WHERE c.policy=?").bind(policy)
}
pub(super) fn pause_gc(paused: bool) -> Statement<'static> {
    sqlx::query("UPDATE mokyu_meta SET gc_paused=? WHERE id=1").bind(paused)
}
pub(super) fn pause_maintenance_policy(policy: &str, paused: bool) -> Statement<'_> {
    sqlx::query(
        "UPDATE maintenance_controls SET paused=?,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE policy=?",
    )
    .bind(paused)
    .bind(policy)
}
pub(super) fn active_lifecycle_task(policy: &str) -> Scalar<'_, Uuid> {
    sqlx::query_scalar("SELECT id FROM tasks WHERE policy=? AND task_type=? AND state IN ('queued','running','paused') ORDER BY created_at,id LIMIT 1").bind(policy).bind(policy)
}
pub(super) fn scheduled_maintenance(policy: &str) -> Statement<'_> {
    sqlx::query(
        "UPDATE maintenance_controls SET last_scheduled_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE policy=?",
    )
    .bind(policy)
}
pub(super) fn maintenance_schedule() -> Rows<'static, (String, bool, Option<f64>)> {
    sqlx::query_as(
        "SELECT c.policy,IIF(c.policy='gc',m.gc_paused,c.paused),(CAST(unixepoch('subsec')*1000000 AS INTEGER)-c.last_scheduled_at)/1000000.0 FROM maintenance_controls c CROSS JOIN mokyu_meta m",
    )
}
pub(super) fn due_pack_reuse() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pack_changes WHERE reason='reuse' AND next_check_at<=CAST(unixepoch('subsec')*1000000 AS INTEGER))",
    )
}
pub(super) fn preparing_pack_count() -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT CAST(COUNT(*) AS TEXT) FROM packs WHERE state='preparing'")
}
pub(super) async fn maintenance_controls(
    connection: &mut sqlx::SqliteConnection,
) -> Result<Vec<Value>> {
    let rows = sqlx::query("SELECT c.*,IIF(c.policy='gc',m.gc_paused,c.paused) AS policy_paused,(SELECT id FROM tasks t WHERE t.policy=c.policy ORDER BY created_at DESC,id DESC LIMIT 1) AS latest_id,(SELECT id FROM tasks t WHERE t.policy=c.policy AND state IN ('queued','running','paused') ORDER BY created_at,id LIMIT 1) AS active_id FROM maintenance_controls c CROSS JOIN mokyu_meta m ORDER BY c.policy").fetch_all(&mut *connection).await?;
    let mut ids = Vec::new();
    for row in &rows {
        for column in ["latest_id", "active_id"] {
            if let Some(id) = row.try_get::<Option<Uuid>, _>(column)? {
                ids.push(id);
            }
        }
    }
    ids.sort_unstable();
    ids.dedup();
    let mut tasks = std::collections::HashMap::new();
    if !ids.is_empty() {
        let mut query = sqlx::QueryBuilder::<Sqlite>::new("SELECT * FROM tasks WHERE id IN ");
        uuid_ids(&mut query, &ids);
        for row in query.build().fetch_all(connection).await? {
            tasks.insert(row.try_get::<Uuid, _>("id")?, task_value(row, false)?);
        }
    }
    rows.into_iter().map(|row| {
        let latest = row.try_get::<Option<Uuid>,_>("latest_id")?;
        let active = row.try_get::<Option<Uuid>,_>("active_id")?;
        Ok(serde_json::json!({"policy":row.try_get::<String,_>("policy")?,"paused":row.try_get::<bool,_>("policy_paused")?,"last_scheduled_at":date(&row,"last_scheduled_at")?,"updated_at":date(&row,"updated_at")?,"latest":latest.and_then(|id|tasks.get(&id)),"active":active.and_then(|id|tasks.get(&id))}))
    }).collect()
}
pub(super) fn remove_maintenance_preview(
    actor: Uuid,
    token: Option<Uuid>,
    action: &str,
) -> Statement<'_> {
    sqlx::query("DELETE FROM maintenance_previews WHERE actor_id=? AND token_id IS ? AND action=? AND task_id IS NULL").bind(actor).bind(token).bind(action)
}
pub(super) fn create_maintenance_preview(
    id: Uuid,
    actor: Uuid,
    token: Option<Uuid>,
    action: &str,
    parameters: Value,
    fingerprint: String,
) -> Statement<'_> {
    sqlx::query("INSERT INTO maintenance_previews(id,actor_id,token_id,action,parameters,fingerprint) VALUES(?,?,?,?,?,?)").bind(id).bind(actor).bind(token).bind(action).bind(parameters).bind(fingerprint)
}
pub(super) fn maintenance_preview_expiry(id: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT expires_at FROM maintenance_previews WHERE id=?").bind(id)
}
pub(super) fn lock_maintenance_preview(
    id: Uuid,
    actor: Uuid,
    token: Option<Uuid>,
    action: &str,
) -> Rows<'_, (Value, String, Option<Uuid>)> {
    sqlx::query_as("SELECT parameters,fingerprint,task_id FROM maintenance_previews WHERE id=? AND actor_id=? AND token_id IS ? AND action=? AND expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)").bind(id).bind(actor).bind(token).bind(action)
}
pub(super) fn maintenance_preview_task(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE id=?)").bind(id)
}
pub(super) fn record_maintenance_preview(id: Uuid, task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE maintenance_previews SET task_id=? WHERE id=?")
        .bind(task)
        .bind(id)
}

pub(super) async fn gc_status(
    connection: &mut sqlx::SqliteConnection,
) -> Result<(Vec<(String, i64)>, bool, bool)> {
    let counts = sqlx::query_as("SELECT state,COUNT(*) FROM chunks GROUP BY state")
        .fetch_all(&mut *connection)
        .await?;
    let (paused, maintenance) =
        sqlx::query_as("SELECT gc_paused,maintenance FROM mokyu_meta WHERE id=1")
            .fetch_one(connection)
            .await?;
    Ok((counts, paused, maintenance))
}

use crate::model::metrics::StorageUsage;

pub(super) async fn inventory_buckets(
    connection: &mut sqlx::SqliteConnection,
) -> Result<Vec<Value>> {
    let rows:Vec<(Option<Uuid>,Option<String>,i64,i64)>=sqlx::query_as("WITH totals AS (SELECT b.id,b.name,COUNT(o.stream_id) AS objects,CAST(COALESCE(SUM(s.size),0) AS INTEGER) AS logical_bytes FROM buckets b LEFT JOIN objects o ON o.bucket_id=b.id AND o.stream_id IS NOT NULL LEFT JOIN streams s ON s.id=o.stream_id GROUP BY b.id,b.name) SELECT NULL AS id,NULL AS name,CAST(COALESCE(SUM(objects),0) AS INTEGER),CAST(COALESCE(SUM(logical_bytes),0) AS INTEGER) FROM totals UNION ALL SELECT id,name,objects,logical_bytes FROM totals ORDER BY id LIMIT 1002").fetch_all(connection).await?;
    Ok(rows.into_iter().map(|(id,name,objects,logical_bytes)|serde_json::json!({"id":id,"name":name,"objects":objects,"logical_bytes":logical_bytes})).collect())
}
pub(super) fn inventory_chunks() -> Scalar<'static, Value> {
    sqlx::query_scalar(
        "SELECT JSON_OBJECT('states',COALESCE(json_group_object(state,n),JSON_OBJECT())) FROM (SELECT state,COUNT(*) n FROM chunks GROUP BY state) c",
    )
}
pub(super) async fn inventory_physical(connection: &mut sqlx::SqliteConnection) -> Result<Value> {
    let rows:Vec<(String,String,i64,i64)>=sqlx::query_as("SELECT source_type,state,COUNT(*),CAST(COALESCE(SUM(stored_size),0) AS INTEGER) FROM (SELECT 'chunk' AS source_type,state,standalone_size AS stored_size FROM chunk_encodings UNION ALL SELECT 'pack',state,stored_size FROM packs UNION ALL SELECT 'raw',state,size FROM raw_sources) sources GROUP BY source_type,state ORDER BY source_type,state").fetch_all(connection).await?;
    Ok(Value::Array(rows.into_iter().map(|(source_type,state,objects,stored_bytes)|serde_json::json!({"source_type":source_type,"state":state,"objects":objects,"stored_bytes":stored_bytes})).collect()))
}
pub(super) fn inventory_live() -> Scalar<'static, Value> {
    sqlx::query_scalar(
        "WITH refs AS (SELECT e.chunk_id,SUM(e.length) AS bytes FROM objects o JOIN extents e ON e.stream_id=o.stream_id WHERE e.chunk_id IS NOT NULL GROUP BY e.chunk_id), raw_live AS (SELECT id,size FROM raw_sources r WHERE EXISTS(SELECT 1 FROM raw_ranges e JOIN objects o ON o.stream_id=e.stream_id WHERE e.source_id=r.id)), physical AS (SELECT l.standalone_size AS bytes,l.standalone_size-IIF(l.encryption_algorithm='none',0,16) AS payload FROM refs r JOIN chunks c ON c.id=r.chunk_id JOIN chunk_encodings l ON l.chunk_id=c.id AND l.state='ready' UNION ALL SELECT p.stored_size,p.stored_size-IIF(p.encryption_algorithm='none',0,16) FROM packs p WHERE state='ready' AND EXISTS(SELECT 1 FROM refs r JOIN chunks c ON c.id=r.chunk_id WHERE c.pack_id=p.id) UNION ALL SELECT size,size FROM raw_live) SELECT JSON_OBJECT('raw_objects',(SELECT COUNT(*) FROM raw_live),'chunks',COUNT(*),'reference_bytes',CAST((COALESCE(SUM(r.bytes),0)+(SELECT COALESCE(SUM(e.length),0) FROM raw_ranges e JOIN objects o ON o.stream_id=e.stream_id)) AS INTEGER),'raw_bytes',CAST((COALESCE(SUM(c.raw_size),0)+(SELECT COALESCE(SUM(size),0) FROM raw_live)) AS INTEGER),'pending_raw_bytes',CAST(COALESCE(SUM(IIF(c.pack_id IS NULL AND NOT EXISTS(SELECT 1 FROM chunk_encodings WHERE chunk_id=c.id AND state='ready'),c.raw_size,0)),0) AS INTEGER),'stored_bytes',(SELECT CAST(COALESCE(SUM(bytes),0) AS INTEGER) FROM physical),'payload_bytes',(SELECT CAST(COALESCE(SUM(payload),0) AS INTEGER) FROM physical)) FROM refs r JOIN chunks c ON c.id=r.chunk_id",
    )
}
pub(super) fn inventory_unreferenced() -> Scalar<'static, Value> {
    sqlx::query_scalar(
        "SELECT JSON_OBJECT('chunks',COUNT(*),'eligible_chunks',CAST(COALESCE(SUM(unreferenced_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-((SELECT (CAST(substr(sp.unreferenced_grace,1,length(sp.unreferenced_grace)-1) AS INTEGER)*CASE substr(sp.unreferenced_grace,-1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=c.storage_id))*1000000 AND owner_stream IS NULL AND NOT EXISTS(SELECT 1 FROM pack_inputs WHERE chunk_id=c.id) AND NOT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=c.id)),0) AS INTEGER)) FROM chunks c WHERE state IN ('ready','failed','deleting') AND unreferenced_at IS NOT NULL AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id)",
    )
}
pub(super) fn inventory_reclaimable() -> Scalar<'static, Value> {
    sqlx::query_scalar(
        "SELECT JSON_OBJECT('bytes',CAST(COALESCE(SUM(bytes),0) AS INTEGER),'eligible',CAST(COALESCE(SUM(IIF(since<CAST(unixepoch('subsec')*1000000 AS INTEGER)-((SELECT (CAST(substr(sp.unreferenced_grace,1,length(sp.unreferenced_grace)-1) AS INTEGER)*CASE substr(sp.unreferenced_grace,-1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=p.storage_id))*1000000,bytes,0)),0) AS INTEGER)) FROM (SELECT l.standalone_size AS bytes,COALESCE(l.unreferenced_at,c.unreferenced_at) AS since,l.storage_id FROM chunk_encodings l JOIN chunks c ON c.id=l.chunk_id WHERE l.state IN ('retired','deleting') OR (l.state='ready' AND c.unreferenced_at IS NOT NULL AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id)) UNION ALL SELECT stored_size,unreferenced_at,storage_id FROM packs WHERE state IN ('retired','deleting') UNION ALL SELECT size,unreferenced_at,storage_id FROM raw_sources r WHERE r.owner_stream IS NULL AND NOT EXISTS(SELECT 1 FROM raw_ranges WHERE source_id=r.id) AND NOT EXISTS(SELECT 1 FROM multipart_files WHERE source_id=r.id)) p",
    )
}
pub(super) fn inventory_tasks() -> Scalar<'static, Value> {
    sqlx::query_scalar(
        "SELECT COALESCE(json_group_object(state,n),JSON_OBJECT()) FROM (SELECT state,COUNT(*) n FROM tasks GROUP BY state) t",
    )
}
pub(super) fn inventory_uploads() -> Scalar<'static, Value> {
    sqlx::query_scalar(
        "SELECT COALESCE(json_group_object(state,n),JSON_OBJECT()) FROM (SELECT state,COUNT(*) n FROM uploads WHERE state IN ('active','completing') GROUP BY state) u",
    )
}
pub(super) async fn inventory_cleanup(
    connection: &mut sqlx::SqliteConnection,
    tasks: f64,
) -> Result<Value> {
    let rows:Vec<(String,i64,Option<i64>)>=sqlx::query_as("WITH cutoffs AS (SELECT CAST(unixepoch('subsec')*1000000 AS INTEGER)-? AS tasks) SELECT 'chunks',COUNT(*),MIN(deleted_at) FROM chunks c WHERE state='deleted' AND deleted_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-((SELECT (CAST(substr(sp.deleted_chunk_retention,1,length(sp.deleted_chunk_retention)-1) AS INTEGER)*CASE substr(sp.deleted_chunk_retention,-1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=c.storage_id))*1000000 AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id) AND NOT EXISTS(SELECT 1 FROM chunk_encodings WHERE chunk_id=c.id AND (state<>'deleted' OR recovery_pending OR owner_stream IS NOT NULL)) AND NOT EXISTS(SELECT 1 FROM pack_members WHERE chunk_id=c.id) AND NOT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=c.id) AND NOT EXISTS(SELECT 1 FROM pack_inputs WHERE chunk_id=c.id) AND NOT EXISTS(SELECT 1 FROM cache_pins WHERE chunk_id=c.id)
        UNION ALL SELECT 'uploads',COUNT(*),MIN(touched_at) FROM uploads u WHERE state IN ('completed','aborted') AND touched_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-((SELECT (CAST(substr(sp.upload_retention,1,length(sp.upload_retention)-1) AS INTEGER)*CASE substr(sp.upload_retention,-1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=(SELECT storage_id FROM buckets WHERE id=u.bucket_id)))*1000000 AND NOT EXISTS(SELECT 1 FROM parts WHERE upload_id=u.id) AND NOT EXISTS(SELECT 1 FROM quota_reservations WHERE upload_id=u.id)
        UNION ALL SELECT 'tasks',COUNT(*),MIN(updated_at) FROM tasks t WHERE state='completed' AND updated_at<(SELECT tasks FROM cutoffs) AND NOT EXISTS(SELECT 1 FROM integrity_issues WHERE task_id=t.id)
        UNION ALL SELECT 'integrity_issues',COUNT(*),MIN(t.updated_at) FROM integrity_issues i JOIN tasks t ON t.id=i.task_id WHERE t.state='completed' AND t.updated_at<(SELECT tasks FROM cutoffs)
        UNION ALL SELECT 'sessions',COUNT(*),MIN(expires_at) FROM sessions s WHERE expires_at<CAST(unixepoch('subsec')*1000000 AS INTEGER) AND NOT EXISTS(SELECT 1 FROM session_refresh_tokens r WHERE r.session_id=s.id AND r.revoked_at IS NULL AND r.expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER))
        UNION ALL SELECT 'packs',COUNT(*),MIN(deleted_at) FROM packs p WHERE state='deleted' AND deleted_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-((SELECT (CAST(substr(sp.deleted_chunk_retention,1,length(sp.deleted_chunk_retention)-1) AS INTEGER)*CASE substr(sp.deleted_chunk_retention,-1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=p.storage_id))*1000000 AND NOT EXISTS(SELECT 1 FROM chunks WHERE pack_id=p.id) AND NOT EXISTS(SELECT 1 FROM pending_uploads WHERE source_pack_id=p.id)
        UNION ALL SELECT 'chunk_encodings',COUNT(*),MIN(deleted_at) FROM chunk_encodings ce WHERE state='deleted' AND deleted_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-((SELECT (CAST(substr(sp.deleted_chunk_retention,1,length(sp.deleted_chunk_retention)-1) AS INTEGER)*CASE substr(sp.deleted_chunk_retention,-1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=ce.storage_id))*1000000").bind((tasks*1_000_000.) as i64).fetch_all(connection).await?;
    let mut result = serde_json::Map::new();
    for (name, eligible, oldest) in rows {
        result.insert(
            name,
            serde_json::json!({"eligible":eligible,"oldest_at":oldest.map(datetime).transpose()?}),
        );
    }
    Ok(Value::Object(result))
}

pub(super) fn inventory_database() -> Scalar<'static, Value> {
    sqlx::query_scalar(
        "SELECT json_object('table',s.tbl_name,'total_bytes',SUM(d.pgsize),'index_bytes',SUM(IIF(s.type='index',d.pgsize,0)),'live_rows_estimate',NULL,'dead_rows_estimate',NULL,'last_autovacuum',NULL,'last_autoanalyze',NULL) FROM dbstat('main',TRUE) d JOIN sqlite_schema s ON s.name=d.name WHERE s.tbl_name IN ('chunks','extents','raw_sources','raw_ranges','streams','objects','uploads','parts','fragments','multipart_files','tasks','sessions','session_refresh_tokens','integrity_issues','chunk_encodings','packs','pack_members','pack_maintenance','pack_changes','pack_inputs','chunk_access_stats','chunk_access_windows','pack_access_windows','pack_member_access_windows','pending_uploads','cache_pins') GROUP BY s.tbl_name ORDER BY s.tbl_name",
    )
}

pub(super) fn storage_inventory_buckets(storage: Uuid) -> Rows<'static, (i64, i64, i64)> {
    sqlx::query_as("SELECT COUNT(DISTINCT b.id),COUNT(o.stream_id),CAST(COALESCE(SUM(s.size),0) AS INTEGER) FROM buckets b LEFT JOIN objects o ON o.bucket_id=b.id AND o.stream_id IS NOT NULL LEFT JOIN streams s ON s.id=o.stream_id WHERE b.storage_id=?").bind(storage)
}
pub(super) fn storage_inventory_chunks(storage: Uuid) -> Rows<'static, (i64, i64)> {
    sqlx::query_as("SELECT COUNT(*),CAST(COALESCE(SUM(standalone_size),0) AS INTEGER) FROM chunks WHERE storage_id=? AND state IN ('ready','deleting')").bind(storage)
}
pub(super) fn storage_inventory_physical(storage: Uuid) -> Rows<'static, (i64, i64)> {
    sqlx::query_as("SELECT COUNT(*),CAST(COALESCE(SUM(bytes),0) AS INTEGER) FROM (SELECT standalone_size AS bytes FROM chunk_encodings WHERE storage_id=? AND state IN ('ready','retired','deleting') UNION ALL SELECT stored_size FROM packs WHERE storage_id=? AND state IN ('ready','retired','deleting') UNION ALL SELECT size FROM raw_sources WHERE storage_id=? AND state<>'writing') p").bind(storage).bind(storage).bind(storage)
}

#[async_trait::async_trait]
impl crate::model::metrics::MetricsStore for Backend {
    async fn inventory(
        &self,
        metrics: &crate::config::MetricsSettings,
        maintenance: &crate::config::Maintenance,
    ) -> Result<Value> {
        transactions::collect_inventory(self, metrics, maintenance).await
    }
    async fn storage_inventory(&self, storage: Uuid) -> Result<Value> {
        transactions::collect_storage(&self.read, storage).await
    }
    async fn usage(
        &self,
        principal: &Principal,
        scope: &crate::model::metrics::ScopeQuery,
        limit: usize,
    ) -> Result<crate::model::metrics::UsageSnapshot> {
        transactions::read_usage(self, principal, scope, limit).await
    }
    async fn runtime(
        &self,
        principal: &Principal,
    ) -> Result<crate::model::metrics::RuntimeHistory> {
        transactions::read_runtime(&self.read, principal).await
    }
    async fn sample_runtime(&self, point: RuntimePoint, retention: f64) -> Result<()> {
        transactions::sample_runtime(&self.write, point, retention).await
    }
    async fn flush_access(&self, pending: AccessBatch, stale: f64) -> Result<()> {
        transactions::flush_access(&self.write, pending, stale).await
    }
    async fn expire_access(&self, retention: f64, batch: i64) -> Result<()> {
        if let Err(error) = transactions::expire_access(&self.write, retention, batch).await {
            tracing::warn!(%error,"access statistics cleanup failed");
        }
        Ok(())
    }
}

pub(super) fn usage_touch_due(key: &str) -> Scalar<'_, bool> {
    sqlx::query_scalar("SELECT requested_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-3600000000 FROM usage_summary WHERE id=?").bind(key)
}

pub(super) async fn evict_usage_scopes(
    connection: &mut sqlx::SqliteConnection,
    keep: i64,
) -> Result<()> {
    sqlx::query("DELETE FROM usage_summary WHERE id IN (SELECT id FROM usage_summary WHERE id<>'global' ORDER BY requested_at DESC,id LIMIT -1 OFFSET ?)").bind(keep).execute(connection).await?;
    Ok(())
}

pub(super) fn create_usage_scope(key: &str) -> Statement<'_> {
    sqlx::query("INSERT INTO usage_summary(id) VALUES(?)").bind(key)
}
pub(super) async fn usage_bucket_members(
    connection: &mut sqlx::SqliteConnection,
    key: &str,
    ids: &[Uuid],
) -> Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    let mut query = sqlx::QueryBuilder::new("INSERT INTO usage_buckets(scope_id,bucket_id) ");
    query.push_values(ids, |mut row, id| {
        row.push_bind(key).push_bind(id);
    });
    query.build().execute(connection).await?;
    Ok(())
}
pub(super) fn touch_usage_scope(key: &str) -> Statement<'_> {
    sqlx::query("UPDATE usage_summary SET requested_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND requested_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-3600000000").bind(key)
}
pub(super) fn usage_saved(
    key: &str,
) -> Rows<'_, (Option<i64>, Option<sqlx::types::Json<StorageUsage>>)> {
    sqlx::query_as("SELECT as_of,data FROM usage_summary WHERE id=?").bind(key)
}
pub(super) fn usage_history(key: &str) -> Rows<'_, (i64, sqlx::types::Json<StorageUsage>)> {
    sqlx::query_as("SELECT snapshot_at,data FROM (SELECT snapshot_at,data FROM usage_history WHERE scope_id=? ORDER BY snapshot_at DESC LIMIT 2048) h ORDER BY snapshot_at").bind(key)
}
pub(super) fn expire_usage_scopes(retention: f64) -> Statement<'static> {
    sqlx::query("DELETE FROM usage_summary WHERE id<>'global' AND requested_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-?").bind((retention*1_000_000.) as i64)
}
pub(super) fn append_usage_history(interval: f64) -> Statement<'static> {
    sqlx::query("INSERT INTO usage_history(scope_id,snapshot_at,data) SELECT s.id,s.as_of,s.data FROM usage_summary s WHERE s.as_of IS NOT NULL AND NOT EXISTS(SELECT 1 FROM usage_history h WHERE h.scope_id=s.id AND h.snapshot_at>s.as_of-?) ON CONFLICT(scope_id,snapshot_at) DO NOTHING").bind((interval*1_000_000.) as i64)
}
pub(super) fn expire_usage_history(retention: f64) -> Statement<'static> {
    sqlx::query("DELETE FROM usage_history WHERE snapshot_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-?").bind((retention*1_000_000.) as i64)
}
pub(super) async fn save_usage(
    connection: &mut sqlx::SqliteConnection,
    at: DateTime<Utc>,
    rows: Vec<(String, sqlx::types::Json<StorageUsage>)>,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut query = sqlx::QueryBuilder::new("UPDATE usage_summary SET as_of=");
    query.push_bind(at.timestamp_micros()).push(",data=CASE id");
    let mut ids = Vec::new();
    for (id, data) in rows {
        query
            .push(" WHEN ")
            .push_bind(id.clone())
            .push(" THEN ")
            .push_bind(data);
        ids.push(id);
    }
    query.push(" ELSE data END WHERE id IN (");
    let mut values = query.separated(",");
    for id in ids {
        values.push_bind(id);
    }
    values.push_unseparated(")");
    query
        .push(" AND (as_of IS NULL OR as_of<")
        .push_bind(at.timestamp_micros())
        .push(")");
    query.build().execute(connection).await?;
    Ok(())
}

// Merge intervals before allocating each covered segment across projects and their buckets.
pub(super) const USAGE_SUMMARY: &str = r#"WITH chunk_ranges AS (
    SELECT DISTINCT o.bucket_id,b.project_id,e.chunk_id,e.source_offset AS lo,e.source_offset+e.length AS hi
    FROM objects o JOIN buckets b ON b.id=o.bucket_id JOIN extents e ON e.stream_id=o.stream_id WHERE e.chunk_id IS NOT NULL
), ordered_ranges AS (
    SELECT r.*,MAX(hi) OVER(PARTITION BY bucket_id,chunk_id ORDER BY lo,hi ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS previous_hi FROM chunk_ranges r
), grouped_ranges AS (
    SELECT r.*,SUM(IIF(previous_hi IS NULL OR lo>previous_hi,1,0)) OVER(PARTITION BY bucket_id,chunk_id ORDER BY lo,hi ROWS UNBOUNDED PRECEDING) AS group_id FROM ordered_ranges r
), ranges AS (
    SELECT bucket_id,project_id,chunk_id,MIN(lo) AS lo,MAX(hi) AS hi FROM grouped_ranges GROUP BY bucket_id,project_id,chunk_id,group_id
), boundaries AS (
    SELECT chunk_id,lo AS point FROM ranges UNION SELECT chunk_id,hi FROM ranges
), segments AS (
    SELECT chunk_id,point AS lo,LEAD(point) OVER(PARTITION BY chunk_id ORDER BY point) AS hi FROM boundaries
), coverage AS (
    SELECT s.chunk_id,s.lo,s.hi,r.bucket_id,r.project_id FROM segments s JOIN ranges r ON r.chunk_id=s.chunk_id AND r.lo<=s.lo AND r.hi>=s.hi WHERE s.hi IS NOT NULL
), project_counts AS (
    SELECT chunk_id,lo,COUNT(DISTINCT project_id) AS n FROM coverage GROUP BY chunk_id,lo
), bucket_counts AS (
    SELECT chunk_id,lo,project_id,COUNT(*) AS n FROM coverage GROUP BY chunk_id,lo,project_id
), allocated AS (
    SELECT c.bucket_id,c.chunk_id,SUM(CAST(c.hi-c.lo AS REAL)/p.n/b.n) AS raw_bytes FROM coverage c JOIN project_counts p USING(chunk_id,lo) JOIN bucket_counts b USING(chunk_id,lo,project_id) GROUP BY c.bucket_id,c.chunk_id
), chunk_unique AS (
    SELECT chunk_id,SUM(hi-lo) AS bytes FROM (SELECT DISTINCT chunk_id,lo,hi FROM coverage) u GROUP BY chunk_id
), sources AS (
    SELECT c.id AS chunk_id,CASE WHEN e.encoding_id IS NOT NULL THEN 'chunk' WHEN p.id IS NOT NULL THEN 'pack' ELSE 'pending' END AS source_type,
        COALESCE(e.encoding_id,p.id,c.id) AS source_id,COALESCE(e.standalone_size,p.stored_size) AS stored_size,u.bytes
    FROM chunk_unique u JOIN chunks c ON c.id=u.chunk_id LEFT JOIN chunk_encodings e ON e.chunk_id=c.id AND e.state='ready' LEFT JOIN packs p ON p.id=c.pack_id AND p.state='ready'
), source_totals AS (
    SELECT source_type,source_id,SUM(bytes) AS raw_bytes,MAX(stored_size) AS stored_size FROM sources GROUP BY source_type,source_id
), attribution AS (
    SELECT a.bucket_id,a.chunk_id,a.raw_bytes,s.source_type,s.source_id,a.raw_bytes/t.raw_bytes*t.stored_size AS encoded_bytes FROM allocated a JOIN sources s USING(chunk_id) JOIN source_totals t USING(source_type,source_id)
), scope_buckets AS (
    SELECT 'global' AS scope_id,id AS bucket_id FROM buckets UNION ALL SELECT scope_id,bucket_id FROM usage_buckets
), scope_segments AS (
    SELECT DISTINCT b.scope_id,c.chunk_id,c.lo,c.hi FROM scope_buckets b JOIN coverage c ON c.bucket_id=b.bucket_id
), scope_unique AS (
    SELECT u.scope_id,COUNT(DISTINCT u.chunk_id) AS chunks,COUNT(DISTINCT IIF(s.source_type='pack',s.source_id,NULL)) AS packs,SUM(u.hi-u.lo) AS bytes,
        SUM(IIF(s.source_type='pack',u.hi-u.lo,0)) AS packed_bytes,SUM(IIF(s.source_type='pending',u.hi-u.lo,0)) AS pending_bytes
    FROM scope_segments u JOIN sources s USING(chunk_id) GROUP BY u.scope_id
), scope_allocated AS (
    SELECT b.scope_id,SUM(a.raw_bytes) AS raw_bytes,COALESCE(SUM(a.encoded_bytes),0) AS encoded_bytes,MIN(a.encoded_bytes IS NOT NULL) AS complete
    FROM scope_buckets b JOIN attribution a ON a.bucket_id=b.bucket_id GROUP BY b.scope_id
), logical_totals AS (
    SELECT 'global' AS id,COUNT(b.id) AS buckets,COALESCE(SUM(q.used_bytes),0) AS bytes,COALESCE(SUM(q.object_count),0) AS objects FROM buckets b LEFT JOIN quota_scopes q ON q.scope='bucket' AND q.scope_id=b.id
    UNION ALL SELECT m.scope_id,COUNT(b.id),COALESCE(SUM(q.used_bytes),0),COALESCE(SUM(q.object_count),0) FROM usage_buckets m JOIN buckets b ON b.id=m.bucket_id LEFT JOIN quota_scopes q ON q.scope='bucket' AND q.scope_id=b.id GROUP BY m.scope_id
), scope_logical AS (
    SELECT s.id,COALESCE(l.buckets,0) AS buckets,COALESCE(l.bytes,0) AS bytes,COALESCE(l.objects,0) AS objects FROM usage_summary s LEFT JOIN logical_totals l ON l.id=s.id
), raw_references AS (
    SELECT DISTINCT o.bucket_id,b.project_id,r.id AS source_id,r.size
    FROM objects o JOIN buckets b ON b.id=o.bucket_id JOIN raw_ranges e ON e.stream_id=o.stream_id JOIN raw_sources r ON r.id=e.source_id
), raw_project_counts AS (
    SELECT source_id,COUNT(DISTINCT project_id) AS n FROM raw_references GROUP BY source_id
), raw_allocated AS (
    SELECT r.*,CAST(r.size AS REAL)/p.n/COUNT(*) OVER(PARTITION BY r.source_id,r.project_id) AS attributed
    FROM raw_references r JOIN raw_project_counts p USING(source_id)
), raw_scopes AS (
    SELECT id,SUM(size) AS bytes,SUM(attributed) AS attributed FROM (
        SELECT s.scope_id AS id,r.source_id,MAX(r.size) AS size,SUM(r.attributed) AS attributed
        FROM scope_buckets s JOIN raw_allocated r ON r.bucket_id=s.bucket_id GROUP BY s.scope_id,r.source_id
    ) scoped_raw GROUP BY id
), raw_inventory AS (
    SELECT r.*,
        EXISTS(SELECT 1 FROM raw_ranges e JOIN objects o ON o.stream_id=e.stream_id WHERE e.source_id=r.id) AS selected,
        r.owner_stream IS NULL AND NOT EXISTS(SELECT 1 FROM raw_ranges e WHERE e.source_id=r.id) AS garbage
    FROM raw_sources r
), raw_physical AS (
    SELECT COALESCE(SUM(CASE WHEN state<>'writing' THEN size ELSE 0 END),0) AS indexed_bytes,
        COALESCE(SUM(CASE WHEN selected THEN size ELSE 0 END),0) AS selected_bytes,
        COALESCE(SUM(CASE WHEN NOT selected AND NOT garbage AND state<>'writing' THEN size ELSE 0 END),0) AS retained_bytes,
        COALESCE(SUM(CASE WHEN NOT selected AND garbage THEN size ELSE 0 END),0) AS gc_bytes,
        COALESCE(SUM(CASE WHEN state='writing' THEN size ELSE 0 END),0) AS unconfirmed_bytes,
        COUNT(*) AS raw_objects FROM raw_inventory
), physical AS (
    SELECT 'chunk' AS source_type,e.encoding_id AS source_id,e.state,e.standalone_size AS stored_size,
        e.state IN ('retired','deleting') OR (c.unreferenced_at IS NOT NULL AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id)) AS garbage
    FROM chunk_encodings e JOIN chunks c ON c.id=e.chunk_id WHERE e.state IN ('ready','retired','deleting','uploading')
    UNION ALL SELECT 'pack',id,state,stored_size,state IN ('retired','deleting') FROM packs WHERE state<>'deleted'
), bridge AS (
    SELECT JSON_OBJECT(
        'indexed_bytes',CAST((COALESCE(SUM(IIF(p.state IN ('ready','retired','deleting'),p.stored_size,0)),0))+(SELECT indexed_bytes FROM raw_physical) AS TEXT),
        'selected_bytes',CAST((COALESCE(SUM(IIF(t.source_id IS NOT NULL,p.stored_size,0)),0))+(SELECT selected_bytes FROM raw_physical) AS TEXT),
        'retained_bytes',CAST((COALESCE(SUM(IIF(t.source_id IS NULL AND p.state IN ('ready','retired','deleting') AND NOT p.garbage,p.stored_size,0)),0))+(SELECT retained_bytes FROM raw_physical) AS TEXT),
        'gc_bytes',CAST((COALESCE(SUM(IIF(t.source_id IS NULL AND p.garbage,p.stored_size,0)),0))+(SELECT gc_bytes FROM raw_physical) AS TEXT),
        'unconfirmed_bytes',CAST((COALESCE(SUM(IIF(p.state IN ('preparing','uploading'),p.stored_size,0)),0))+(SELECT unconfirmed_bytes FROM raw_physical) AS TEXT),
        'chunk_objects',CAST(COALESCE(SUM(p.source_type='chunk' AND p.state IN ('ready','retired','deleting')),0) AS TEXT),
        'raw_objects',CAST((SELECT raw_objects FROM raw_physical) AS TEXT),
        'pack_objects',CAST(COALESCE(SUM(p.source_type='pack' AND p.state IN ('ready','retired','deleting')),0) AS TEXT)
    ) AS data FROM physical p LEFT JOIN source_totals t ON t.source_type=p.source_type AND t.source_id=p.source_id AND t.source_type<>'pending'
)
SELECT l.id,JSON_OBJECT(
    'buckets',CAST(l.buckets AS TEXT),'objects',CAST(l.objects AS TEXT),'logical_bytes',CAST(l.bytes AS TEXT),
    'unique_bytes',CAST((COALESCE(u.bytes,0)+COALESCE(r.bytes,0)) AS TEXT),'attributed_raw_bytes',CAST(ROUND((COALESCE(a.raw_bytes,0)+COALESCE(r.attributed,0)),6) AS TEXT),
    'encoded_bytes',IIF(COALESCE(a.complete,TRUE),CAST(ROUND((COALESCE(a.encoded_bytes,0)+COALESCE(r.attributed,0)),6) AS TEXT),NULL),
    'encoded_known_bytes',CAST(ROUND((COALESCE(a.encoded_bytes,0)+COALESCE(r.attributed,0)),6) AS TEXT),
    'local_savings',CAST(l.bytes-(COALESCE(u.bytes,0)+COALESCE(r.bytes,0)) AS TEXT),'shared_savings',CAST(ROUND((COALESCE(u.bytes,0)+COALESCE(r.bytes,0))-(COALESCE(a.raw_bytes,0)+COALESCE(r.attributed,0)),6) AS TEXT),
    'encoding_savings',IIF(COALESCE(a.complete,TRUE),CAST(ROUND((COALESCE(a.raw_bytes,0)+COALESCE(r.attributed,0))-(COALESCE(a.encoded_bytes,0)+COALESCE(r.attributed,0)),6) AS TEXT),NULL),
    'chunks',CAST(COALESCE(u.chunks,0) AS TEXT),'packs',CAST(COALESCE(u.packs,0) AS TEXT),
    'packed_bytes',CAST(COALESCE(u.packed_bytes,0) AS TEXT),'pending_bytes',CAST(COALESCE(u.pending_bytes,0) AS TEXT),
    'physical',IIF(l.id='global',json((SELECT data FROM bridge)),NULL)
) AS data FROM scope_logical l LEFT JOIN scope_unique u ON u.scope_id=l.id LEFT JOIN scope_allocated a ON a.scope_id=l.id LEFT JOIN raw_scopes r ON r.id=l.id
"#;
pub(super) fn collect_usage() -> Rows<'static, (String, sqlx::types::Json<StorageUsage>)> {
    sqlx::query_as(USAGE_SUMMARY)
}

use crate::model::metrics::{AccessBatch, RuntimePoint, WorkSummary};

impl<'r> sqlx::FromRow<'r, SqliteRow> for WorkSummary {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            task_type: row.try_get("task_type")?,
            state: row.try_get("state")?,
            processed: row.try_get("processed")?,
            updated_at: date(row, "updated_at")?,
        })
    }
}
pub(super) fn runtime_history() -> Scalar<'static, sqlx::types::Json<RuntimePoint>> {
    sqlx::query_scalar(
        "SELECT data FROM (SELECT sampled_at,data FROM runtime_samples ORDER BY sampled_at DESC LIMIT 2048) h ORDER BY sampled_at",
    )
}
pub(super) fn runtime_work() -> Rows<'static, WorkSummary> {
    sqlx::query_as(
        "SELECT id,policy AS task_type,state,CAST(processed AS TEXT) AS processed,updated_at FROM tasks WHERE state IN ('queued','running','paused') ORDER BY CASE state WHEN 'running' THEN 0 WHEN 'queued' THEN 1 ELSE 2 END,updated_at DESC,id LIMIT 5",
    )
}
pub(super) fn runtime_pending() -> Rows<'static, (String, String, Option<i64>, String)> {
    sqlx::query_as(
        "SELECT CAST(COUNT(*) AS TEXT),CAST(COALESCE(SUM(staged_size),0) AS TEXT),MIN(created_at),CAST(COALESCE(SUM(last_error IS NOT NULL),0) AS TEXT) FROM pending_uploads",
    )
}
pub(super) fn runtime_sample(point: RuntimePoint) -> Statement<'static> {
    sqlx::query("INSERT INTO runtime_samples(sampled_at,data) VALUES(?,?)")
        .bind(point.at.timestamp_micros())
        .bind(sqlx::types::Json(point))
}
pub(super) fn runtime_expire(retention: f64) -> Statement<'static> {
    sqlx::query("DELETE FROM runtime_samples WHERE sampled_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-?").bind((retention*1_000_000.) as i64)
}
pub(super) async fn flush_chunk_access(
    connection: &mut sqlx::SqliteConnection,
    pending: &AccessBatch,
) -> Result<()> {
    if pending.chunks.is_empty() {
        return Ok(());
    }
    let data = sqlx::types::Json(
        pending
            .chunks
            .iter()
            .map(|((id, at), c)| {
                (
                    *id,
                    at.timestamp_micros(),
                    c.reads,
                    c.ranges,
                    c.bytes,
                    c.origins,
                )
            })
            .collect::<Vec<_>>(),
    );
    sqlx::query("INSERT INTO chunk_access_stats(chunk_id,read_count,range_read_count,read_bytes,last_read_at) SELECT c.id,SUM(json_extract(j.value,'$[2]')),SUM(json_extract(j.value,'$[3]')),SUM(json_extract(j.value,'$[4]')),CAST(unixepoch('subsec')*1000000 AS INTEGER) FROM json_each(?) j JOIN chunks c ON c.id=json_extract(j.value,'$[0]') GROUP BY c.id ORDER BY c.id ON CONFLICT(chunk_id) DO UPDATE SET read_count=chunk_access_stats.read_count+excluded.read_count,range_read_count=chunk_access_stats.range_read_count+excluded.range_read_count,read_bytes=chunk_access_stats.read_bytes+excluded.read_bytes,last_read_at=excluded.last_read_at").bind(&data).execute(&mut *connection).await?;
    sqlx::query("INSERT INTO chunk_access_windows(chunk_id,window_start,read_count,range_read_count,read_bytes,range_origin_reads) SELECT c.id,json_extract(j.value,'$[1]'),json_extract(j.value,'$[2]'),json_extract(j.value,'$[3]'),json_extract(j.value,'$[4]'),json_extract(j.value,'$[5]') FROM json_each(?) j JOIN chunks c ON c.id=json_extract(j.value,'$[0]') ORDER BY c.id,json_extract(j.value,'$[1]') ON CONFLICT(chunk_id,window_start) DO UPDATE SET read_count=chunk_access_windows.read_count+excluded.read_count,range_read_count=chunk_access_windows.range_read_count+excluded.range_read_count,read_bytes=chunk_access_windows.read_bytes+excluded.read_bytes,range_origin_reads=chunk_access_windows.range_origin_reads+excluded.range_origin_reads").bind(data).execute(connection).await?;
    Ok(())
}
pub(super) async fn flush_pack_access(
    connection: &mut sqlx::SqliteConnection,
    pending: &AccessBatch,
) -> Result<()> {
    if pending.packs.is_empty() {
        return Ok(());
    }
    let data = sqlx::types::Json(
        pending
            .packs
            .iter()
            .map(|((id, at), c)| {
                (
                    *id,
                    at.timestamp_micros(),
                    c.downloads,
                    c.bytes,
                    c.partial,
                    c.useful,
                    c.partial_bytes,
                )
            })
            .collect::<Vec<_>>(),
    );
    sqlx::query("INSERT INTO pack_access_windows(pack_id,window_start,downloads,downloaded_bytes,partial_downloads,useful_bytes,partial_bytes) SELECT p.id,json_extract(j.value,'$[1]'),json_extract(j.value,'$[2]'),json_extract(j.value,'$[3]'),json_extract(j.value,'$[4]'),json_extract(j.value,'$[5]'),json_extract(j.value,'$[6]') FROM json_each(?) j JOIN packs p ON p.id=json_extract(j.value,'$[0]') ORDER BY p.id,json_extract(j.value,'$[1]') ON CONFLICT(pack_id,window_start) DO UPDATE SET downloads=pack_access_windows.downloads+excluded.downloads,downloaded_bytes=pack_access_windows.downloaded_bytes+excluded.downloaded_bytes,partial_downloads=pack_access_windows.partial_downloads+excluded.partial_downloads,useful_bytes=pack_access_windows.useful_bytes+excluded.useful_bytes,partial_bytes=pack_access_windows.partial_bytes+excluded.partial_bytes,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)").bind(data).execute(connection).await?;
    Ok(())
}
pub(super) async fn flush_member_access(
    connection: &mut sqlx::SqliteConnection,
    pending: &AccessBatch,
) -> Result<()> {
    if pending.members.is_empty() {
        return Ok(());
    }
    let data = sqlx::types::Json(
        pending
            .members
            .iter()
            .map(|((pack, chunk, at), n)| (*pack, *chunk, at.timestamp_micros(), *n))
            .collect::<Vec<_>>(),
    );
    sqlx::query("INSERT INTO pack_member_access_windows(pack_id,chunk_id,window_start,downloads) SELECT p.id,c.id,json_extract(j.value,'$[2]'),json_extract(j.value,'$[3]') FROM json_each(?) j JOIN packs p ON p.id=json_extract(j.value,'$[0]') JOIN chunks c ON c.id=json_extract(j.value,'$[1]') ORDER BY p.id,json_extract(j.value,'$[2]'),c.id ON CONFLICT(pack_id,window_start,chunk_id) DO UPDATE SET downloads=pack_member_access_windows.downloads+excluded.downloads").bind(data).execute(connection).await?;
    Ok(())
}
pub(super) fn access_coverage(dropped: bool, stale: f64) -> Statement<'static> {
    sqlx::query("UPDATE access_state SET coverage_since=IIF(? OR coverage_since IS NULL OR flushed_at IS NULL OR flushed_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-?,CAST(unixepoch('subsec')*1000000 AS INTEGER),coverage_since),flushed_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=1").bind(dropped).bind((stale*1_000_000.) as i64)
}
pub(super) async fn expire_access(
    connection: &mut sqlx::SqliteConnection,
    retention: f64,
    batch: i64,
) -> Result<()> {
    for (table, columns) in [
        ("chunk_access_windows", "chunk_id,window_start"),
        ("pack_access_windows", "pack_id,window_start"),
        (
            "pack_member_access_windows",
            "pack_id,window_start,chunk_id",
        ),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(format!("DELETE FROM {table} WHERE ({columns}) IN (SELECT {columns} FROM {table} WHERE window_start<CAST(unixepoch('subsec')*1000000 AS INTEGER)-? ORDER BY window_start,{columns} LIMIT ?)"))).bind((retention*1_000_000.) as i64).bind(batch).execute(&mut *connection).await?;
    }
    Ok(())
}

fn source_scope(query: &mut sqlx::QueryBuilder<Sqlite>, stream: Option<Id>, chunks: &[Id]) {
    query.push("c.id IN (");
    if let Some(stream) = stream {
        query
            .push("SELECT chunk_id FROM extents WHERE stream_id=")
            .push_bind(stream);
    } else if chunks.is_empty() {
        query.push("NULL");
    } else {
        let mut values = query.separated(",");
        for id in chunks {
            values.push_bind(*id);
        }
    }
    query.push(")");
}

pub(super) fn source_locks(
    stream: Option<Id>,
    chunks: &[Id],
    phase: u8,
) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new(match phase {
        0 => "SELECT c.id FROM chunks c WHERE ",
        1 => {
            "SELECT l.encoding_id FROM chunk_encodings l WHERE l.state='ready' AND l.chunk_id IN (SELECT c.id FROM chunks c WHERE "
        }
        2 => {
            "SELECT p.id FROM packs p WHERE p.state='ready' AND p.id IN (SELECT c.pack_id FROM chunks c WHERE "
        }
        _ => unreachable!(),
    });
    source_scope(&mut query, stream, chunks);
    query.push(match phase {
        0 => " ORDER BY c.id",
        1 => ") ORDER BY l.encoding_id",
        _ => ") ORDER BY p.id",
    });
    query
}

pub(super) fn unavailable_sources(stream: Option<Id>, chunks: &[Id]) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new("SELECT EXISTS(SELECT 1 FROM chunks c WHERE ");
    source_scope(&mut query, stream, chunks);
    query.push(" AND (c.state<>'ready' OR (NOT EXISTS(SELECT 1 FROM chunk_encodings l WHERE l.chunk_id=c.id AND l.state='ready' AND l.quarantined_at IS NULL) AND NOT EXISTS(SELECT 1 FROM packs p WHERE p.id=c.pack_id AND p.state='ready' AND p.quarantined_at IS NULL) AND NOT (EXISTS(SELECT 1 FROM pending_uploads u WHERE u.chunk_id=c.id) AND NOT EXISTS(SELECT 1 FROM chunk_encodings l WHERE l.chunk_id=c.id AND l.quarantined_at IS NOT NULL) AND NOT EXISTS(SELECT 1 FROM packs p WHERE p.id=c.pack_id AND p.quarantined_at IS NOT NULL)))))");
    query
}

pub(super) async fn source_snapshot(
    db: &mut sqlx::SqliteConnection,
    scope: &crate::model::source::SourceScope,
    lock: bool,
) -> Result<crate::model::source::SourceSnapshot> {
    use crate::model::source::{SourceSnapshot, manifest_digest};
    let suffix = if lock { "" } else { "" };
    let (storage_revision, marker): (i64, sqlx::types::Json<Value>) = sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT p.revision,s.identity_marker FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.id=? AND s.enabled{suffix}")))
        .bind(scope.storage_id).fetch_one(&mut *db).await?;
    let suffix = if lock { "" } else { "" };
    if scope.source == crate::model::source::SourceType::Raw {
        let raw: RawSource = sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT r.id AS raw_id,r.location_id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine FROM raw_sources r WHERE r.id=? AND r.storage_id=?{suffix}")))
            .bind(scope.identity()?).bind(scope.storage_id).fetch_one(&mut *db).await?;
        return Ok(SourceSnapshot {
            scope: scope.clone(),
            state: raw.state.clone(),
            revision: raw.revision,
            quarantine_code: raw.quarantine_code.clone(),
            physical: PhysicalSource::Raw(raw),
            storage_revision,
            identity_marker: marker.0,
            manifest_digest: None,
        });
    }
    let id = scope.identity()?;
    let (table, column) = if scope.packed() {
        ("packs", "id")
    } else {
        ("chunk_encodings", "encoding_id")
    };
    let (state, revision, quarantine_code): (String, i64, Option<String>) = sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT state,quarantine_revision,quarantine_code FROM {table} WHERE {column}=? AND storage_id=?{suffix}")))
        .bind(id).bind(scope.storage_id).fetch_optional(&mut *db).await?.context(crate::model::Error::OperationAborted)?;
    let physical = if scope.packed() {
        PhysicalSource::Pack(
            sqlx::query_as("SELECT * FROM packs WHERE id=? AND storage_id=?")
                .bind(id)
                .bind(scope.storage_id)
                .fetch_one(&mut *db)
                .await?,
        )
    } else {
        PhysicalSource::Chunk(sqlx::query_as("SELECT c.id,c.uuid,l.encoding_id AS current_encoding_id,l.location_id,c.storage_id,c.hash,c.raw_size,l.standalone_size,l.encryption_algorithm,l.block_key_id,l.standalone_compressed,l.nonce,c.format,c.state,c.created_at FROM chunk_encodings l JOIN chunks c ON c.id=l.chunk_id WHERE l.encoding_id=? AND l.storage_id=?").bind(id).bind(scope.storage_id).fetch_one(&mut *db).await?)
    };
    let manifest_digest = if let PhysicalSource::Pack(pack) = &physical {
        let members: Vec<(Id, i64, i32, Vec<u8>)> = sqlx::query_as("SELECT m.chunk_id,m.offset_bytes,c.raw_size,c.hash FROM pack_members m JOIN chunks c ON c.id=m.chunk_id WHERE m.pack_id=? ORDER BY m.ordinal LIMIT 4097").bind(id).fetch_all(&mut *db).await?;
        Some(manifest_digest(pack, &members)?)
    } else {
        None
    };
    Ok(SourceSnapshot {
        scope: scope.clone(),
        physical,
        state,
        revision,
        quarantine_code,
        storage_revision,
        identity_marker: marker.0,
        manifest_digest,
    })
}

pub(super) async fn quarantines(
    db: &mut sqlx::SqliteConnection,
    storage: Uuid,
    packed: bool,
    after: Id,
    limit: i64,
) -> Result<Vec<crate::model::source::QuarantinedSource>> {
    use crate::model::source::{QuarantinedSource, SourceScope, SourceType};
    let (table, id) = if packed {
        ("packs", "id")
    } else {
        ("chunk_encodings", "encoding_id")
    };
    let rows: Vec<(i64, Uuid, String, String, String, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT {id},location_id,state,strftime('%Y-%m-%dT%H:%M:%fZ',quarantined_at/1000000.0,'unixepoch'),quarantine_code,quarantine_revision FROM {table} WHERE storage_id=? AND quarantined_at IS NOT NULL AND {id}>? ORDER BY {id} LIMIT ?")))
        .bind(storage).bind(after).bind(limit).fetch_all(db).await?;
    Ok(rows
        .into_iter()
        .map(
            |(id, location_id, state, quarantined_at, code, revision)| QuarantinedSource {
                scope: SourceScope {
                    storage_id: storage,
                    source: if packed {
                        SourceType::Pack
                    } else {
                        SourceType::Chunk
                    },
                    id: id.to_string(),
                },
                location_id,
                state,
                quarantined_at,
                code,
                revision: revision.to_string(),
            },
        )
        .collect())
}

pub(super) fn change_source<'a>(
    snapshot: &crate::model::source::SourceSnapshot,
    code: Option<&'a str>,
) -> Result<Statement<'a>> {
    if snapshot.scope.source == crate::model::source::SourceType::Raw {
        return Ok(sqlx::query("UPDATE raw_sources SET quarantined_at=CASE WHEN ? IS NULL THEN NULL ELSE CAST(unixepoch('subsec')*1000000 AS INTEGER) END,quarantine_code=?,revision=revision+1 WHERE id=? AND storage_id=? AND revision=? AND state='ready'")
            .bind(code).bind(code).bind(snapshot.scope.identity()?).bind(snapshot.scope.storage_id).bind(snapshot.revision));
    }
    let (table, id) = if snapshot.scope.packed() {
        ("packs", "id")
    } else {
        ("chunk_encodings", "encoding_id")
    };
    Ok(sqlx::query(sqlx::AssertSqlSafe(format!("UPDATE {table} SET quarantined_at=CASE WHEN ? IS NULL THEN NULL ELSE CAST(unixepoch('subsec')*1000000 AS INTEGER) END,quarantine_code=?,quarantine_revision=quarantine_revision+1 WHERE {id}=? AND storage_id=? AND quarantine_revision=? AND state='ready'")))
        .bind(code).bind(code).bind(snapshot.scope.identity()?).bind(snapshot.scope.storage_id).bind(snapshot.revision))
}

pub(super) fn source_mode(mode: &str) -> Scalar<'_, bool> {
    sqlx::query_scalar("SELECT ?='single'").bind(mode)
}

pub(super) fn source_result(task: Uuid, result: Value) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET detail=json_patch(detail,?),processed=processed+1 WHERE id=?")
        .bind(result)
        .bind(task)
}

pub(super) fn quarantined_inputs(chunks: &[Id]) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new("SELECT EXISTS(SELECT 1 FROM chunks c WHERE ");
    source_scope(&mut query, None, chunks);
    query.push(" AND (EXISTS(SELECT 1 FROM chunk_encodings l WHERE l.chunk_id=c.id AND l.state='ready' AND l.quarantined_at IS NOT NULL) OR EXISTS(SELECT 1 FROM packs p WHERE p.id=c.pack_id AND p.state='ready' AND p.quarantined_at IS NOT NULL)))");
    query
}

pub(super) fn splice_locks(
    streams: &[Id],
    chunk: Id,
    physical: bool,
) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new(if physical {
        "SELECT c.id FROM chunks c WHERE c.id="
    } else {
        "SELECT id FROM streams WHERE id IN ("
    });
    if physical {
        query
            .push_bind(chunk)
            .push(" OR c.id IN (SELECT chunk_id FROM extents WHERE stream_id IN (");
    }
    let mut values = query.separated(",");
    for stream in streams {
        values.push_bind(*stream);
    }
    query.push(if physical {
        ")) ORDER BY c.id"
    } else {
        ") ORDER BY id"
    });
    query
}

pub(super) fn allocated_source(stream: Id, chunk: &Chunk) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT l.encoding_id FROM chunks c JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.chunk_id=c.id WHERE c.state='uploading' AND l.state='uploading' AND l.quarantined_at IS NULL AND l.owner_task IS NULL AND c.id=",
    );
    query
        .push_bind(chunk.id)
        .push(" AND c.owner_stream=")
        .push_bind(stream)
        .push(" AND l.encoding_id=")
        .push_bind(chunk.encoding_id)
        .push(" AND l.location_id=")
        .push_bind(chunk.location_id)
        .push("");
    query
}
pub(super) fn source_input_versions(chunks: &[Id]) -> sqlx::QueryBuilder<Sqlite> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT c.id,l.encoding_id,l.quarantine_revision,p.id,p.quarantine_revision FROM chunks c LEFT JOIN chunk_encodings l ON l.chunk_id=c.id AND l.state='ready' LEFT JOIN packs p ON p.id=c.pack_id AND p.state='ready' WHERE ",
    );
    source_scope(&mut query, None, chunks);
    query.push(" ORDER BY c.id,l.encoding_id,p.id");
    query
}
pub(super) fn save_source_inputs(task: Uuid, stamp: &Value) -> Statement<'_> {
    sqlx::query("UPDATE tasks SET detail=json_set(detail,'$.source_inputs',json(?)) WHERE id=?")
        .bind(stamp)
        .bind(task)
}
pub(super) fn saved_source_inputs(task: Uuid) -> Scalar<'static, Value> {
    sqlx::query_scalar("SELECT json_extract(detail,'$.source_inputs') FROM tasks WHERE id=?")
        .bind(task)
}

// Recovery candidates are private to their writing stream until full verification.
pub(super) fn repair_mode() -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT NOT maintenance FROM mokyu_meta")
}
pub(super) fn repair_stream(
    stream: Id,
) -> Rows<'static, (Uuid, String, Option<Uuid>, Option<Uuid>)> {
    sqlx::query_as("SELECT bucket_id,stream_type,owner_task_id,owner_execution_token FROM streams WHERE id=? AND state='writing'").bind(stream)
}
pub(super) fn repair_object(stream: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT o.write_token FROM objects o JOIN streams s ON s.bucket_id=o.bucket_id AND s.object_key=o.object_key WHERE s.id=?").bind(stream)
}
pub(super) fn repair_part(stream: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT write_token FROM parts WHERE write_token=?").bind(stream)
}
pub(super) fn repair_target(storage: Uuid, key: Option<Uuid>, algorithm: &str) -> Scalar<'_, i64> {
    sqlx::query_scalar("SELECT p.revision FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.id=? AND s.enabled AND s.block_key_id IS ? AND ((s.block_key_id IS NULL AND ?='none') OR EXISTS(SELECT 1 FROM managed_keys k WHERE k.id=s.block_key_id AND k.key_algorithm=? AND k.revoked_at IS NULL))").bind(storage).bind(key).bind(algorithm).bind(algorithm)
}
pub(super) fn repair_pending(chunk: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=?)").bind(chunk)
}
pub(super) fn insert_repair(chunk: &Chunk, stream: Id) -> Statement<'_> {
    sqlx::query("INSERT INTO chunk_encodings(chunk_id,location_id,storage_id,state,created_at,unreferenced_at,owner_stream,recovery_pending,encryption_algorithm,block_key_id,standalone_compressed,encoding_fingerprint) VALUES(?,?,?,'uploading',?,CAST(unixepoch('subsec')*1000000 AS INTEGER),?,TRUE,?,?,FALSE,'{}')").bind(chunk.id).bind(chunk.location_id).bind(chunk.storage_id).bind(chunk.created_at.timestamp_micros()).bind(stream).bind(&chunk.algorithm).bind(chunk.block_key_id)
}
pub(super) fn repair_candidate(encoding: Id, stream: Id) -> Rows<'static, Chunk> {
    sqlx::query_as("SELECT c.id,c.uuid,l.encoding_id AS current_encoding_id,l.location_id,c.storage_id,c.hash,c.raw_size,l.standalone_size,l.encryption_algorithm,l.block_key_id,l.standalone_compressed,l.nonce,c.format,c.state,c.created_at FROM chunk_encodings l JOIN chunks c ON c.id=l.chunk_id WHERE l.encoding_id=? AND l.owner_stream=? AND l.owner_task IS NULL AND l.state='uploading' AND l.recovery_pending AND l.quarantined_at IS NULL").bind(encoding).bind(stream)
}
pub(super) fn save_repair_metadata(chunk: &Chunk, stream: Id) -> Statement<'_> {
    sqlx::query("UPDATE chunk_encodings SET standalone_size=?,standalone_compressed=?,nonce=?,nonce_id=? WHERE encoding_id=? AND owner_stream=? AND state='uploading'").bind(chunk.stored_size).bind(chunk.compressed).bind(&chunk.nonce).bind(crate::content::nonce::id(chunk.nonce.as_deref())).bind(chunk.encoding_id).bind(stream)
}
pub(super) fn retire_repair_sources(chunk: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET state='retired',unreferenced_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE chunk_id=? AND state='ready' AND quarantined_at IS NOT NULL").bind(chunk)
}
pub(super) fn publish_repair(encoding: Id, stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET state='ready',owner_stream=NULL,recovery_pending=FALSE,unreferenced_at=NULL,stored_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE encoding_id=? AND owner_stream=? AND state='uploading' AND quarantined_at IS NULL").bind(encoding).bind(stream)
}
pub(super) fn discard_verified_repair(encoding: Id, stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET state='retired',owner_stream=NULL,recovery_pending=FALSE,unreferenced_at=CAST(unixepoch('subsec')*1000000 AS INTEGER),stored_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE encoding_id=? AND owner_stream=? AND state='uploading'").bind(encoding).bind(stream)
}
pub(super) fn publish_repaired_chunk(chunk: &Chunk) -> Statement<'_> {
    sqlx::query("UPDATE chunks SET current_encoding_id=?,pack_id=NULL,standalone_size=?,standalone_compressed=?,nonce=?,encryption_algorithm=?,block_key_id=?,unreferenced_at=NULL WHERE id=? AND state='ready'").bind(chunk.encoding_id).bind(chunk.stored_size).bind(chunk.compressed).bind(&chunk.nonce).bind(&chunk.algorithm).bind(chunk.block_key_id).bind(chunk.id)
}

pub(super) fn retire_stream_encodings(stream: Id, batch: i64) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET owner_stream=NULL,state='retired',unreferenced_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE encoding_id IN (SELECT encoding_id FROM chunk_encodings WHERE owner_stream=? AND state='uploading' ORDER BY encoding_id LIMIT ?)").bind(stream).bind(batch)
}

pub(super) fn repair_owner(
    stream: Id,
) -> Rows<'static, (Uuid, String, Option<Uuid>, Option<Uuid>)> {
    sqlx::query_as("SELECT bucket_id,stream_type,owner_task_id,owner_execution_token FROM streams WHERE id=? AND state='writing'").bind(stream)
}
pub(super) fn repair_upload(stream: Id) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM uploads WHERE id=(SELECT r.upload_id FROM quota_writes w JOIN quota_reservations r ON r.id=w.reservation_id WHERE w.stream_id=?) AND state='active'").bind(stream)
}

pub(super) fn source_audit<'a>(
    principal: &'a Principal,
    action: &'a str,
    target: &'a str,
    mut detail: Value,
) -> Result<Statement<'a>> {
    let (source, fallback, access_key) = match principal {
        Principal::Service { access_key, .. } => {
            detail["access_key_id"] = serde_json::json!(access_key);
            ("s3", "S3 credential", Some(access_key.as_str()))
        }
        Principal::User { .. } => ("web", "Deleted account", None),
        Principal::Token { .. } => ("token", "Deleted account", None),
        Principal::Local => ("cli", "Local CLI", None),
        _ => ("cli", "Internal operation", None),
    };
    if let Some(id) = crate::model::audit::current() {
        detail["request_audit_id"] = serde_json::json!(id.to_string());
    }
    ensure!(
        serde_json::to_vec(&detail)?.len() <= 262144,
        "audit detail exceeds its bound"
    );
    Ok(sqlx::query("INSERT INTO audit_events(actor_id,actor_label,token_id,source,action,target,outcome,finished_at,detail) VALUES(?,COALESCE((SELECT username FROM web_users WHERE id=?),NULLIF(substr((SELECT label FROM s3_credentials WHERE access_key_id=?),1,64),''),?),?,?,?,?, 'succeeded',CAST(unixepoch('subsec')*1000000 AS INTEGER),?)")
        .bind(principal.user_id()).bind(principal.user_id()).bind(access_key).bind(fallback).bind(principal.token_id()).bind(source).bind(action).bind(target).bind(detail))
}

use crate::model::raw::{RawProcessing, RawRange, RawSource, RawStore};
pub(super) fn raw_target(
    stream: Id,
    storage: Uuid,
) -> Rows<'static, (String, bool, i64, bool, bool)> {
    sqlx::query_as("SELECT t.type,(t.block_key_id IS NULL AND NOT p.compression_enabled AND NOT p.pack_enabled),p.revision,p.cdc_enabled,(p.deduplication_enabled AND p.write_processing='background') FROM streams s JOIN buckets b ON b.id=s.bucket_id JOIN storages t ON t.id=b.storage_id JOIN storage_profiles p ON p.storage_id=t.id WHERE s.id=? AND t.id=? AND t.enabled AND s.state='writing'").bind(stream).bind(storage)
}
pub(super) fn raw_prepare(stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET payload_type='raw' WHERE id=? AND state='writing' AND payload_type='chunks' AND NOT EXISTS(SELECT 1 FROM extents WHERE stream_id=?) AND NOT EXISTS(SELECT 1 FROM raw_ranges WHERE stream_id=?)").bind(stream).bind(stream).bind(stream)
}
pub(super) fn raw_insert(
    id: Uuid,
    stream: Id,
    storage: Uuid,
    revision: i64,
    medium: &'static str,
) -> Statement<'static> {
    sqlx::query(
        "INSERT INTO raw_sources(location_id,owner_stream,storage_id,profile_revision,medium) VALUES(?,?,?,?,?)",
    )
    .bind(id)
    .bind(stream)
    .bind(storage)
    .bind(revision).bind(medium)
}
pub(super) fn raw_finish(stream: Id, source: Id, size: i64, digest: &[u8]) -> Statement<'_> {
    sqlx::query("UPDATE raw_sources SET size=?,digest=?,state='ready',owner_stream=NULL,upload_id=NULL WHERE id=? AND owner_stream=? AND state='writing'").bind(size).bind(digest).bind(source).bind(stream)
}
pub(super) fn raw_attach(stream: Id, source: Id, size: i64) -> Statement<'static> {
    sqlx::query("INSERT INTO raw_ranges(stream_id,offset_bytes,length,source_id) VALUES(?,0,?,?)")
        .bind(stream)
        .bind(size)
        .bind(source)
}
pub(super) fn raw_sources_lock(stream: Id) -> Rows<'static, RawSource> {
    sqlx::query_as("SELECT r.id AS raw_id,r.location_id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine FROM raw_sources r WHERE r.id IN (SELECT source_id FROM raw_ranges WHERE stream_id=?) ORDER BY r.id").bind(stream)
}
pub(super) fn raw_continuity(stream: Id) -> Rows<'static, (i64, bool)> {
    sqlx::query_as("SELECT COALESCE(MAX(e.offset_bytes+e.length),0),COALESCE(MIN(e.offset_bytes=e.previous_end AND e.source_offset+e.length<=r.size),1)<>0 AND NOT EXISTS(SELECT 1 FROM extents WHERE stream_id=?) FROM (SELECT *,LAG(offset_bytes+length,1,0) OVER(ORDER BY offset_bytes) AS previous_end FROM raw_ranges WHERE stream_id=?) e JOIN raw_sources r ON r.id=e.source_id").bind(stream).bind(stream)
}
pub(super) fn raw_cleanup_ranges(stream: Id, batch: i64) -> Rows<'static, (i64, Id)> {
    sqlx::query_as("SELECT offset_bytes,source_id FROM raw_ranges WHERE stream_id=? ORDER BY offset_bytes LIMIT ?").bind(stream).bind(batch)
}
pub(super) fn raw_touch_unreferenced(source: Id) -> Statement<'static> {
    sqlx::query("UPDATE raw_sources SET unreferenced_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=?").bind(source)
}
pub(super) fn raw_detach(stream: Id, end: i64) -> Statement<'static> {
    sqlx::query("DELETE FROM raw_ranges WHERE stream_id=? AND offset_bytes<=?")
        .bind(stream)
        .bind(end)
}

#[async_trait::async_trait]
impl RawStore for Backend {
    async fn claim_processing(
        &self,
        token: Uuid,
        node: crate::model::raw::Processor<'_>,
    ) -> Result<Option<RawProcessing>> {
        transactions::claim_raw_processing(&self.write, node.is_none(), token).await
    }
    async fn renew_processing(
        &self,
        work: &RawProcessing,
        _node: crate::model::raw::Processor<'_>,
    ) -> Result<bool> {
        Ok(sqlx::query("UPDATE raw_processing SET lease_expires_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)+120000000 WHERE stream_id=? AND token=? AND (lease_expires_at IS NULL OR lease_expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER))")
            .bind(work.stream).bind(work.token).execute(&self.write).await?.rows_affected() == 1)
    }
    async fn save_processing_hash(
        &self,
        work: &RawProcessing,
        revision: i64,
        hash: &[u8],
    ) -> Result<()> {
        anyhow::ensure!(hash.len() == 32, "invalid raw processing hash");
        anyhow::ensure!(sqlx::query("UPDATE raw_processing SET content_hash=?,source_revision=? WHERE stream_id=? AND token=? AND (lease_expires_at IS NULL OR lease_expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER))")
            .bind(hash).bind(revision).bind(work.stream).bind(work.token).execute(&self.write).await?.rows_affected()==1,
            crate::model::Error::OperationAborted);
        Ok(())
    }
    async fn retry_processing(&self, work: &RawProcessing, error: Option<&str>) -> Result<()> {
        let error = error.map(|message| message.chars().take(1024).collect::<String>());
        sqlx::query("UPDATE raw_processing SET token=NULL,lease_expires_at=NULL,last_error=?,attempts=attempts+?,next_attempt_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)+?*1000000 WHERE stream_id=? AND token=?")
            .bind(&error).bind(i32::from(error.is_some())).bind(if error.is_some() {30i64} else {1}).bind(work.stream).bind(work.token).execute(&self.write).await?;
        Ok(())
    }
    async fn cancel_processing(&self, work: &RawProcessing) -> Result<()> {
        raw_processing_delete(work).execute(&self.write).await?;
        Ok(())
    }
    async fn finish_processing(
        &self,
        work: &RawProcessing,
        source: &RawSource,
        hash: &[u8],
        lease: Option<Uuid>,
        _node: crate::model::raw::Processor<'_>,
    ) -> Result<bool> {
        transactions::finish_raw_processing(&self.write, work, source, hash, lease).await
    }
    async fn processing_status(&self) -> Result<Value> {
        let (pending,failed,bytes): (i64,i64,i64)=sqlx::query_as("SELECT COUNT(*),CAST(COALESCE(SUM(w.last_error IS NOT NULL),0) AS INTEGER),CAST(COALESCE(SUM(r.size),0) AS INTEGER) FROM raw_processing w JOIN raw_sources r ON r.id=w.source_id")
            .fetch_one(&self.write).await?;
        Ok(serde_json::json!({"pending":pending,"failed":failed,"bytes":bytes}))
    }

    async fn set_upload(&self, source: Id, upload: &str) -> Result<()> {
        anyhow::ensure!(
            !upload.is_empty() && upload.len() <= 4096,
            "invalid raw multipart upload id"
        );
        let changed = sqlx::query("UPDATE raw_sources SET upload_id=? WHERE id=? AND state='writing' AND medium='shared_s3' AND upload_id IS NULL")
            .bind(upload).bind(source).execute(&self.write).await?.rows_affected();
        anyhow::ensure!(changed == 1, crate::model::Error::OperationAborted);
        Ok(())
    }
    async fn upload_id(&self, source: Id) -> Result<Option<String>> {
        Ok(
            sqlx::query_scalar("SELECT upload_id FROM raw_sources WHERE id=?")
                .bind(source)
                .fetch_optional(&self.read)
                .await?
                .flatten(),
        )
    }
    async fn deduplicate(&self, stream: Id, source: Id, hash: &[u8]) -> Result<bool> {
        transactions::deduplicate_raw(&self.write, stream, source, hash).await
    }

    async fn quarantines(
        &self,
        storage: Uuid,
        after: Id,
        limit: i64,
    ) -> Result<Vec<crate::model::source::QuarantinedSource>> {
        use crate::model::source::{QuarantinedSource, SourceScope, SourceType};
        let rows: Vec<(Id,Uuid,String,String,String,i64)> = sqlx::query_as("SELECT id,location_id,state,strftime('%Y-%m-%dT%H:%M:%fZ',quarantined_at/1000000.0,'unixepoch'),quarantine_code,revision FROM raw_sources WHERE storage_id=? AND id>? AND quarantined_at IS NOT NULL ORDER BY id LIMIT ?")
            .bind(storage).bind(after).bind(limit).fetch_all(&self.read).await?;
        Ok(rows
            .into_iter()
            .map(
                |(id, location_id, state, quarantined_at, code, revision)| QuarantinedSource {
                    scope: SourceScope {
                        storage_id: storage,
                        source: SourceType::Raw,
                        id: id.to_string(),
                    },
                    location_id,
                    state,
                    quarantined_at,
                    code,
                    revision: revision.to_string(),
                },
            )
            .collect())
    }
    async fn begin(&self, stream: Id, storage: Uuid, profile_revision: i64) -> Result<(Id, Uuid)> {
        transactions::begin_raw(&self.write, stream, storage, profile_revision).await
    }
    async fn finish(&self, stream: Id, source: Id, size: i64, digest: &[u8]) -> Result<()> {
        transactions::finish_raw(&self.write, stream, source, size, digest).await
    }
    async fn page(&self, stream: Id, start: i64, end: i64) -> Result<Vec<RawRange>> {
        Ok(sqlx::query_as("SELECT r.id AS raw_id,r.location_id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine,e.offset_bytes,e.length,e.source_offset FROM raw_ranges e JOIN raw_sources r ON r.id=e.source_id WHERE e.stream_id=? AND e.offset_bytes+e.length>=? AND e.offset_bytes<=? ORDER BY e.offset_bytes LIMIT 128")
            .bind(stream).bind(start).bind(end).fetch_all(&self.read).await?)
    }
    async fn candidates(
        &self,
        storage: Uuid,
        grace: i64,
        minimum: i64,
        limit: i64,
    ) -> Result<Vec<RawSource>> {
        anyhow::ensure!(
            (1..=128).contains(&limit) && grace >= 0 && minimum >= 0,
            "invalid raw cleanup bounds"
        );
        Ok(sqlx::query_as("SELECT r.id AS raw_id,r.location_id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine FROM raw_sources r WHERE r.storage_id=? AND r.owner_stream IS NULL AND r.unreferenced_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-?*1000000 AND r.created_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-?*1000000 AND NOT EXISTS(SELECT 1 FROM raw_ranges WHERE source_id=r.id) AND NOT EXISTS(SELECT 1 FROM multipart_files WHERE source_id=r.id) ORDER BY r.unreferenced_at,r.id LIMIT ?")
            .bind(storage).bind(grace).bind(minimum).bind(limit).fetch_all(&self.read).await?)
    }
    async fn claim(&self, source: Id, revision: i64, grace: i64, minimum: i64) -> Result<bool> {
        Ok(sqlx::query("UPDATE raw_sources SET state='deleting',revision=revision+1 WHERE id=? AND revision=? AND owner_stream IS NULL AND unreferenced_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-?*1000000 AND created_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-?*1000000 AND NOT EXISTS(SELECT 1 FROM raw_ranges WHERE source_id=raw_sources.id) AND NOT EXISTS(SELECT 1 FROM multipart_files WHERE source_id=raw_sources.id)")
            .bind(source).bind(revision).bind(grace).bind(minimum).execute(&self.write).await?.rows_affected()==1)
    }
    async fn removed(&self, source: Id) -> Result<()> {
        sqlx::query("DELETE FROM raw_sources WHERE id=? AND state='deleting' AND NOT EXISTS(SELECT 1 FROM raw_ranges WHERE source_id=raw_sources.id) AND NOT EXISTS(SELECT 1 FROM multipart_files WHERE source_id=raw_sources.id)")
            .bind(source).execute(&self.write).await?;
        Ok(())
    }
}

pub(super) fn raw_lock_stream(stream: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar(
        "SELECT id FROM streams WHERE id=? AND state='writing' AND payload_type='raw'  ",
    )
    .bind(stream)
}

pub(super) fn raw_source(id: Id) -> Rows<'static, RawSource> {
    sqlx::query_as("SELECT r.id AS raw_id,r.location_id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine FROM raw_sources r WHERE r.id=?").bind(id)
}

pub(super) fn raw_duplicate(storage: Uuid, hash: &[u8], size: i64) -> Rows<'_, RawSource> {
    sqlx::query_as("SELECT r.id AS raw_id,r.location_id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine FROM raw_sources r WHERE r.storage_id=? AND r.content_hash=? AND r.size=?")
        .bind(storage).bind(hash).bind(size)
}
pub(super) fn raw_hash(id: Id, hash: Option<&[u8]>) -> Statement<'_> {
    sqlx::query("UPDATE raw_sources SET content_hash=? WHERE id=?")
        .bind(hash)
        .bind(id)
}
pub(super) fn raw_share(
    stream: Id,
    previous: Id,
    replacement: Id,
    size: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE raw_ranges SET source_id=? WHERE stream_id=? AND source_id=? AND offset_bytes=0 AND source_offset=0 AND length=?")
        .bind(replacement).bind(stream).bind(previous).bind(size)
}

pub(super) fn raw_reference(source: Id) -> Statement<'static> {
    sqlx::query("UPDATE raw_sources SET revision=revision+1 WHERE id=?").bind(source)
}

pub(super) fn lock_local_upload(upload: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT state FROM uploads WHERE id=?").bind(upload)
}
pub(super) fn current_multipart_file(upload: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM multipart_files WHERE upload_id=? AND state='open' ORDER BY id LIMIT 1",
    )
    .bind(upload)
}
pub(super) fn insert_multipart_file(id: Uuid, upload: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO multipart_files(id,upload_id,storage_id) SELECT ?,u.id,b.storage_id FROM uploads u JOIN buckets b ON b.id=u.bucket_id JOIN storages s ON s.id=b.storage_id WHERE u.id=? AND u.state='active' AND s.type='local'").bind(id).bind(upload)
}

pub(super) fn insert_multipart_checkpoint(upload: Uuid, source: Id) -> Statement<'static> {
    sqlx::query("INSERT INTO multipart_files(id,upload_id,storage_id,source_id,state) SELECT r.location_id,?,r.storage_id,r.id,'sealed' FROM raw_sources r WHERE r.id=? AND r.state='writing' AND r.medium='local' AND r.owner_stream IS NOT NULL ON CONFLICT(id) DO NOTHING").bind(upload).bind(source)
}
pub(super) fn remap_multipart_range<'a>(
    upload: Uuid,
    source: Id,
    range: &'a crate::model::multipart::FileRemap,
) -> Statement<'a> {
    sqlx::query("UPDATE fragments SET file_id=(SELECT location_id FROM raw_sources WHERE id=?),file_offset=? WHERE id=? AND file_id=? AND file_offset=? AND size=? AND hash=? AND sealed AND EXISTS(SELECT 1 FROM extents e JOIN parts p ON p.stream_id=e.stream_id WHERE e.fragment_id=fragments.id AND p.upload_id=?)").bind(source).bind(range.destination).bind(range.fragment).bind(range.file).bind(range.offset).bind(range.size).bind(range.hash.as_slice()).bind(upload)
}

pub(super) fn pending_multipart_reclaim(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE multipart_files SET reclaim_pending=true WHERE id=? AND state='sealed'")
        .bind(id)
}

pub(super) fn cache_policy(chunk: Id) -> Rows<'static, (String, String)> {
    sqlx::query_as("SELECT p.pack_max_size,p.pack_upload_flush_timeout FROM chunks c JOIN storage_profiles p ON p.storage_id=c.storage_id WHERE c.id=?").bind(chunk)
}

pub(super) fn pack_policy_due(storage: Uuid, policy: &str, seconds: u64) -> Scalar<'_, bool> {
    sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM storage_maintenance WHERE storage_id=? AND policy=? AND last_scheduled_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)-?*1000000)").bind(storage).bind(policy).bind(seconds as i64)
}
pub(super) fn pack_policy_scheduled(storage: Uuid, policy: &str) -> Statement<'_> {
    sqlx::query("INSERT INTO storage_maintenance(storage_id,policy,last_scheduled_at) VALUES(?,?,CAST(unixepoch('subsec')*1000000 AS INTEGER)) ON CONFLICT(storage_id,policy) DO UPDATE SET last_scheduled_at=excluded.last_scheduled_at").bind(storage).bind(policy)
}

pub(super) fn raw_enqueue_processing(stream: Id, source: Id) -> Statement<'static> {
    sqlx::query("INSERT INTO raw_processing(stream_id,source_id,payload_revision) SELECT id,?,payload_revision FROM streams WHERE id=?").bind(source).bind(stream)
}

pub(super) fn raw_processing_due(single: bool) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT w.stream_id FROM raw_processing w JOIN streams s ON s.id=w.stream_id WHERE s.state<>'writing' AND (s.state<>'ready' OR EXISTS(SELECT 1 FROM objects o WHERE o.stream_id=s.id)) AND w.next_attempt_at<=CAST(unixepoch('subsec')*1000000 AS INTEGER) AND (? OR w.token IS NULL OR w.lease_expires_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)) ORDER BY w.next_attempt_at,w.stream_id LIMIT 1").bind(single)
}

pub(super) fn raw_processing_claim(stream: Id, token: Uuid, single: bool) -> Statement<'static> {
    sqlx::query("UPDATE raw_processing SET token=?,lease_expires_at=CASE WHEN ? THEN NULL ELSE CAST(unixepoch('subsec')*1000000 AS INTEGER)+120000000 END WHERE stream_id=?").bind(token).bind(single).bind(stream)
}

pub(super) fn raw_processing_record(stream: Id) -> Rows<'static, RawProcessing> {
    sqlx::query_as("SELECT w.stream_id AS stream,w.source_id AS source,s.bucket_id AS bucket,CAST(s.object_key AS TEXT) AS object_key,w.payload_revision,w.content_hash,w.source_revision,w.token FROM raw_processing w JOIN streams s ON s.id=w.stream_id WHERE w.stream_id=?").bind(stream)
}

pub(super) fn raw_processing_owned(work: &RawProcessing) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT source_id FROM raw_processing WHERE stream_id=? AND token=? AND (lease_expires_at IS NULL OR lease_expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER))").bind(work.stream).bind(work.token)
}

pub(super) fn raw_processing_delete(work: &RawProcessing) -> Statement<'static> {
    sqlx::query("DELETE FROM raw_processing WHERE stream_id=? AND token=?")
        .bind(work.stream)
        .bind(work.token)
}

pub(super) fn raw_processing_stream(work: &RawProcessing) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM streams WHERE id=? AND state='ready' AND payload_type='raw' AND payload_revision=? AND EXISTS(SELECT 1 FROM objects WHERE stream_id=streams.id)").bind(work.stream).bind(work.payload_revision)
}

pub(super) fn raw_processing_changed(stream: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET payload_revision=payload_revision+1 WHERE id=?").bind(stream)
}

pub(super) fn nonce_counter(key: Uuid, type_tag: u8) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT n.reserved_id FROM nonce_counters n JOIN managed_keys k ON k.id=n.block_key_id WHERE n.block_key_id=? AND n.type_tag=? AND k.purpose='block' AND k.revoked_at IS NULL").bind(key).bind(i64::from(type_tag))
}

pub(super) fn advance_nonce(
    key: Uuid,
    type_tag: u8,
    previous: i64,
    last: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE nonce_counters SET reserved_id=? WHERE block_key_id=? AND type_tag=? AND reserved_id=?").bind(last).bind(key).bind(i64::from(type_tag)).bind(previous)
}

pub(super) fn initialize_nonce_counters(key: Uuid) -> Statement<'static> {
    sqlx::query(
        "INSERT INTO nonce_counters(block_key_id,type_tag,reserved_id) VALUES(?,0,0),(?,1,0)",
    )
    .bind(key)
    .bind(key)
}

pub(super) fn incompatible_payload(version: i64) -> Scalar<'static, bool> {
    if version < 6 {
        return sqlx::query_scalar("SELECT FALSE");
    }
    if version == 6 {
        return sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM chunks) OR EXISTS(SELECT 1 FROM packs) OR EXISTS(SELECT 1 FROM streams)",
        );
    }
    if version >= 22 {
        return sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM chunks) OR EXISTS(SELECT 1 FROM packs) OR EXISTS(SELECT 1 FROM fragments) OR EXISTS(SELECT 1 FROM streams) OR EXISTS(SELECT 1 FROM objects) OR EXISTS(SELECT 1 FROM uploads) OR EXISTS(SELECT 1 FROM raw_sources) OR EXISTS(SELECT 1 FROM multipart_files)",
        );
    }
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM chunks) OR EXISTS(SELECT 1 FROM packs) OR EXISTS(SELECT 1 FROM fragments) OR EXISTS(SELECT 1 FROM streams) OR EXISTS(SELECT 1 FROM objects) OR EXISTS(SELECT 1 FROM uploads)",
    )
}

use crate::model::key_replacement::{
    KeyReplacementInput, KeyReplacementPreview, KeyReplacementResult, KeyReplacementStorage,
    ReplacementRow,
};

pub(super) fn replacement_storages(source: Uuid) -> Rows<'static, KeyReplacementStorage> {
    sqlx::query_as("SELECT s.id,s.name,s.enabled,p.revision FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.block_key_id=? ORDER BY s.id ").bind(source)
}
pub(super) fn replacement_key(id: Uuid) -> Rows<'static, (String, String)> {
    sqlx::query_as("SELECT name,key_algorithm FROM managed_keys WHERE id=? AND purpose='block' AND revoked_at IS NULL ").bind(id)
}
pub(super) fn replacement_capacity(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT count(*)=2 AND min(reserved_id)>=0 AND max(reserved_id)<=9223372036854775743 FROM nonce_counters WHERE block_key_id=?").bind(id)
}
pub(super) fn insert_key_replacement<'q>(
    actor: &'q str,
    plan: &'q KeyReplacementPreview,
) -> Statement<'q> {
    sqlx::query("INSERT INTO key_replacements(id,actor,plan,expires_at) VALUES(?,?,?,?)")
        .bind(plan.id)
        .bind(actor)
        .bind(sqlx::types::Json(plan))
        .bind(plan.expires_at.timestamp_micros())
}
pub(super) fn key_replacement<'q>(id: Uuid, actor: &'q str) -> Rows<'q, ReplacementRow> {
    sqlx::query_as("SELECT plan,result FROM key_replacements WHERE id=? AND actor=? ")
        .bind(id)
        .bind(actor)
}
pub(super) fn complete_key_replacement(result: &KeyReplacementResult) -> Statement<'_> {
    sqlx::query("UPDATE key_replacements SET result=?,expires_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)+604800000000 WHERE id=? AND result IS NULL").bind(sqlx::types::Json(result)).bind(result.preview.id)
}
