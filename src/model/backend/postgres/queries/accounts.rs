use super::*;

#[derive(sqlx::FromRow)]
pub(in super::super) struct RefreshToken {
    pub(in super::super) id: Uuid,
    pub(in super::super) session_id: Uuid,
    pub(in super::super) family_id: Uuid,
    pub(in super::super) expires_at: DateTime<Utc>,
    pub(in super::super) revoked_at: Option<DateTime<Utc>>,
    pub(in super::super) replaced_by: Option<Uuid>,
}

pub(in super::super) fn insert_refresh<'q>(
    session: Uuid,
    family: Uuid,
    hash: &'q [u8],
) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("INSERT INTO session_refresh_tokens(session_id,family_id,token_hash,expires_at) VALUES($1,$2,$3,now()+$4*interval '1 day') RETURNING id").bind(session).bind(family).bind(hash).bind(30_f64)
}

pub(in super::super) fn revoke_refreshes(session: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE session_refresh_tokens SET revoked_at=now() WHERE session_id=$1 AND revoked_at IS NULL").bind(session)
}

pub(in super::super) fn recent_session(session: Uuid, user: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM sessions WHERE id=$1 AND user_id=$2 AND reauthenticated_at>now()-interval '5 minutes')").bind(session).bind(user)
}

pub(in super::super) fn authenticate_token<'q>(hash: &'q [u8]) -> Rows<'q, TokenIdentity> {
    sqlx::query_as("SELECT t.id,t.user_id,u.role,t.system,t.authorization_revision,u.authorization_revision FROM api_tokens t JOIN web_users u ON u.id=t.user_id WHERE t.token_hash=$1 AND t.revoked_at IS NULL AND (t.expires_at IS NULL OR t.expires_at>now()) AND t.auth_revision=u.auth_revision AND u.enabled AND NOT u.must_change_password").bind(hash)
}

pub(in super::super) fn touch_api_token(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE api_tokens SET last_used_at=now() WHERE id=$1 AND (last_used_at IS NULL OR last_used_at<now()-interval '5 minutes')").bind(id)
}

pub(in super::super) fn authenticate_session<'q>(hash: &'q [u8]) -> Rows<'q, AuthSession> {
    sqlx::query_as("SELECT u.id AS user_id,s.id AS session_id,u.role,u.must_change_password,u.authorization_revision,s.csrf_hash,s.last_seen_at FROM sessions s JOIN web_users u ON u.id=s.user_id WHERE s.token_hash=$1 AND s.expires_at>now() AND u.enabled AND s.auth_revision=u.auth_revision").bind(hash)
}

pub(in super::super) fn touch_session<'q>(hash: &'q [u8]) -> Statement<'q> {
    sqlx::query("UPDATE sessions SET last_seen_at=now() WHERE token_hash=$1 AND last_seen_at<now()-interval '5 minutes'").bind(hash)
}

pub(in super::super) fn login_user<'q>(username: &'q str) -> Rows<'q, VerifiedAccount> {
    sqlx::query_as(
        "SELECT id,password_hash,auth_revision FROM web_users WHERE username=$1 AND enabled",
    )
    .bind(username)
}

pub(in super::super) fn lock_login_user(user: Uuid) -> Rows<'static, (String, bool, i64)> {
    sqlx::query_as(
        "SELECT password_hash,enabled,auth_revision FROM web_users WHERE id=$1 FOR UPDATE",
    )
    .bind(user)
}

pub(in super::super) fn insert_session<'q>(input: &'q SessionLogin<'q>) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("INSERT INTO sessions(token_hash,user_id,csrf_hash,expires_at,auth_revision,user_agent) VALUES($1,$2,$3,now()+$4*interval '1 second',$5,$6) RETURNING id").bind(input.access_hash).bind(input.verified.0).bind(input.csrf_hash).bind(input.seconds as f64).bind(input.verified.2).bind(input.user_agent)
}

pub(in super::super) fn delete_access_session<'q>(hash: &'q [u8]) -> Statement<'q> {
    sqlx::query("DELETE FROM sessions WHERE token_hash=$1").bind(hash)
}

pub(in super::super) fn refresh_user<'q>(hash: &'q [u8]) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("SELECT s.user_id FROM session_refresh_tokens r JOIN sessions s ON s.id=r.session_id WHERE r.token_hash=$1").bind(hash)
}

pub(in super::super) fn lock_refresh_user(user: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT auth_revision FROM web_users WHERE id=$1 AND enabled FOR UPDATE")
        .bind(user)
}

pub(in super::super) fn lock_refresh<'q>(hash: &'q [u8]) -> Rows<'q, RefreshToken> {
    sqlx::query_as("SELECT id,session_id,family_id,expires_at,revoked_at,replaced_by FROM session_refresh_tokens WHERE token_hash=$1 FOR UPDATE").bind(hash)
}

pub(in super::super) fn lock_refresh_successor(id: Option<Uuid>) -> Rows<'static, RefreshToken> {
    sqlx::query_as("SELECT id,session_id,family_id,expires_at,revoked_at,replaced_by FROM session_refresh_tokens WHERE id=$1 AND revoked_at IS NULL AND expires_at>now() FOR UPDATE").bind(id)
}

pub(in super::super) fn active_refresh_family(
    session: Uuid,
    family: Uuid,
) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM session_refresh_tokens WHERE session_id=$1 AND family_id=$2 AND revoked_at IS NULL)").bind(session).bind(family)
}

pub(in super::super) fn delete_refresh_session(session: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM sessions WHERE id=$1").bind(session)
}

pub(in super::super) fn lock_refresh_session(
    session: Uuid,
    user: Uuid,
    revision: i64,
) -> Scalar<'static, DateTime<Utc>> {
    sqlx::query_scalar("SELECT expires_at FROM sessions WHERE id=$1 AND user_id=$2 AND auth_revision=$3 FOR UPDATE").bind(session).bind(user).bind(revision)
}

pub(in super::super) fn revoke_refresh(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE session_refresh_tokens SET revoked_at=now() WHERE id=$1").bind(id)
}

pub(in super::super) fn replace_refresh(id: Uuid, replacement: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE session_refresh_tokens SET replaced_by=$2 WHERE id=$1")
        .bind(id)
        .bind(replacement)
}

pub(in super::super) fn refresh_session<'q>(
    session: Uuid,
    access: &'q [u8],
    csrf: &'q [u8],
    expires: DateTime<Utc>,
) -> Statement<'q> {
    sqlx::query("UPDATE sessions SET token_hash=$2,csrf_hash=$3,expires_at=$4,last_seen_at=now() WHERE id=$1").bind(session).bind(access).bind(csrf).bind(expires)
}

pub(in super::super) fn touch_refresh(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE session_refresh_tokens SET last_used_at=now() WHERE id=$1").bind(id)
}

pub(in super::super) fn session_times<'q>(hash: &'q [u8]) -> Rows<'q, SessionTimes> {
    sqlx::query_as("SELECT id,expires_at,reauthenticated_at FROM sessions WHERE token_hash=$1")
        .bind(hash)
}

pub(in super::super) fn account_profile(user: Uuid) -> Rows<'static, Profile> {
    sqlx::query_as("SELECT u.id,u.username,u.role,u.must_change_password,m.project_management,u.display_name,u.locale,u.theme,u.avatar_email,u.avatar_enabled FROM web_users u CROSS JOIN mokyu_meta m WHERE u.id=$1 AND u.enabled").bind(user)
}

pub(in super::super) fn account_preferences<'q>(
    user: Uuid,
    input: &'q Preferences,
) -> Statement<'q> {
    sqlx::query("UPDATE web_users SET display_name=$2,locale=$3,theme=$4,avatar_email=$5,avatar_enabled=$6 WHERE id=$1 AND enabled").bind(user).bind(input.display_name.trim()).bind(input.locale.as_deref()).bind(input.theme.as_deref()).bind(input.avatar_email.trim().to_lowercase()).bind(input.avatar_enabled)
}

pub(in super::super) fn account_password(user: Uuid) -> Rows<'static, (String, i64)> {
    sqlx::query_as("SELECT password_hash,auth_revision FROM web_users WHERE id=$1 AND enabled")
        .bind(user)
}

pub(in super::super) fn lock_session_user(user: Uuid) -> Rows<'static, (String, i64, bool)> {
    sqlx::query_as(
        "SELECT password_hash,auth_revision,enabled FROM web_users WHERE id=$1 FOR UPDATE",
    )
    .bind(user)
}

pub(in super::super) fn active_session<'q>(
    user: Uuid,
    hash: &'q [u8],
    revision: i64,
) -> Scalar<'q, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM sessions WHERE user_id=$1 AND token_hash=$2 AND expires_at>now() AND auth_revision=$3)").bind(user).bind(hash).bind(revision)
}

pub(in super::super) fn change_password<'q>(user: Uuid, hash: &'q str) -> Statement<'q> {
    sqlx::query("UPDATE web_users SET password_hash=$2,auth_revision=auth_revision+1,must_change_password=false WHERE id=$1").bind(user).bind(hash)
}

pub(in super::super) fn delete_other_sessions<'q>(user: Uuid, hash: &'q [u8]) -> Statement<'q> {
    sqlx::query("DELETE FROM sessions WHERE user_id=$1 AND token_hash<>$2")
        .bind(user)
        .bind(hash)
}

pub(in super::super) fn session_revision<'q>(hash: &'q [u8], revision: i64) -> Statement<'q> {
    sqlx::query("UPDATE sessions SET auth_revision=$2,reauthenticated_at=now() WHERE token_hash=$1")
        .bind(hash)
        .bind(revision)
}

pub(in super::super) fn session_id<'q>(hash: &'q [u8]) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("SELECT id FROM sessions WHERE token_hash=$1").bind(hash)
}

pub(in super::super) fn reauthenticate_session<'q>(hash: &'q [u8]) -> Statement<'q> {
    sqlx::query("UPDATE sessions SET reauthenticated_at=now() WHERE token_hash=$1").bind(hash)
}

pub(in super::super) fn account_sessions<'q>(user: Uuid, hash: &'q [u8]) -> Rows<'q, Session> {
    sqlx::query_as("SELECT id,created_at,last_seen_at,expires_at,user_agent,(token_hash=$2) AS current FROM sessions s WHERE user_id=$1 AND (expires_at>now() OR EXISTS(SELECT 1 FROM session_refresh_tokens r WHERE r.session_id=s.id AND r.revoked_at IS NULL AND r.expires_at>now())) ORDER BY created_at DESC,id DESC LIMIT 101").bind(user).bind(hash)
}

pub(in super::super) fn delete_account_session(session: Uuid, user: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM sessions WHERE id=$1 AND user_id=$2")
        .bind(session)
        .bind(user)
}

pub(in super::super) fn has_accounts() -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM web_users)")
}

pub(in super::super) fn delete_setup_tokens() -> Statement<'static> {
    sqlx::query("DELETE FROM setup_tokens")
}

pub(in super::super) fn lock_setup_metadata() -> Statement<'static> {
    sqlx::query("SELECT singleton FROM mokyu_meta WHERE singleton FOR UPDATE")
}

pub(in super::super) fn save_setup_token<'q>(hash: &'q [u8]) -> Statement<'q> {
    sqlx::query("INSERT INTO setup_tokens(singleton,token_hash,expires_at) VALUES(true,$1,now()+interval '1 hour') ON CONFLICT(singleton) DO UPDATE SET token_hash=excluded.token_hash,created_at=now(),expires_at=excluded.expires_at").bind(hash)
}

pub(in super::super) fn lock_accounts() -> Statement<'static> {
    sqlx::query("SELECT pg_advisory_xact_lock($1)").bind(super::super::USER_LOCK)
}

pub(in super::super) fn insert_initial_admin<'q>(
    username: &'q str,
    password: &'q str,
    require_change: bool,
) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("INSERT INTO web_users(username,password_hash,role,must_change_password) VALUES($1,$2,'admin',$3) RETURNING id").bind(username).bind(password).bind(require_change)
}

pub(in super::super) fn setup_required() -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM web_users)")
}

pub(in super::super) fn setup_hash() -> Scalar<'static, Vec<u8>> {
    sqlx::query_scalar("SELECT token_hash FROM setup_tokens WHERE singleton AND expires_at>now()")
}

pub(in super::super) fn setup_allowed<'q>(hash: &'q [u8]) -> Scalar<'q, bool> {
    sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM web_users) AND EXISTS(SELECT 1 FROM setup_tokens WHERE token_hash=$1 AND expires_at>clock_timestamp())").bind(hash)
}

pub(in super::super) fn insert_setup_admin<'q>(
    username: &'q str,
    password: &'q str,
) -> Scalar<'q, Uuid> {
    sqlx::query_scalar(
        "INSERT INTO web_users(username,password_hash,role) VALUES($1,$2,'admin') RETURNING id",
    )
    .bind(username)
    .bind(password)
}

#[async_trait::async_trait]
impl AccountStore for Backend {
    async fn recent(&self, session: Uuid, user: Uuid) -> Result<bool> {
        Ok(recent_session(session, user).fetch_one(&self.pool).await?)
    }
    async fn authenticate_token(&self, hash: &[u8]) -> Result<Option<TokenIdentity>> {
        let row = authenticate_token(hash).fetch_optional(&self.pool).await?;
        if let Some(row) = &row {
            touch_api_token(row.0).execute(&self.pool).await?;
        }
        Ok(row)
    }
    async fn authenticate_session(&self, hash: &[u8]) -> Result<Option<AuthSession>> {
        let row = authenticate_session(hash)
            .fetch_optional(&self.pool)
            .await?;
        if row
            .as_ref()
            .is_some_and(|row| (Utc::now() - row.last_seen_at).num_seconds() >= 300)
        {
            touch_session(hash).execute(&self.pool).await?;
        }
        Ok(row)
    }
    async fn login_user(&self, username: &str) -> Result<Option<VerifiedAccount>> {
        Ok(login_user(username).fetch_optional(&self.pool).await?)
    }
    async fn password(&self, user: Uuid) -> Result<Option<(String, i64)>> {
        Ok(account_password(user).fetch_optional(&self.pool).await?)
    }
    async fn login(&self, input: SessionLogin<'_>) -> Result<()> {
        transactions::login_session(&self.pool, input).await
    }
    async fn failed_login(&self, user: Uuid, request_id: &str) -> Result<()> {
        transactions::failed_login(&self.pool, user, request_id).await
    }
    async fn refresh(&self, input: SessionRefresh<'_>) -> Result<RefreshedSession> {
        transactions::refresh_session(&self.pool, input).await
    }
    async fn session(&self, hash: &[u8]) -> Result<Option<SessionTimes>> {
        Ok(session_times(hash).fetch_optional(&self.pool).await?)
    }
    async fn logout(&self, user: Uuid, hash: &[u8]) -> Result<()> {
        transactions::logout_session(&self.pool, user, hash).await
    }
    async fn profile(&self, user: Uuid) -> Result<Option<Profile>> {
        Ok(account_profile(user).fetch_optional(&self.pool).await?)
    }
    async fn preferences(&self, user: Uuid, hash: &[u8], input: Preferences) -> Result<()> {
        transactions::account_preferences(&self.pool, user, hash, input).await
    }
    async fn change_password(
        &self,
        verified: &VerifiedAccount,
        current: &[u8],
        password: &str,
        refresh: &[u8],
    ) -> Result<()> {
        transactions::account_password(&self.pool, verified, current, password, refresh).await
    }
    async fn reauthenticate(
        &self,
        verified: &VerifiedAccount,
        current: &[u8],
        refresh: &[u8],
    ) -> Result<()> {
        transactions::reauthenticate_account(&self.pool, verified, current, refresh).await
    }
    async fn sessions(&self, user: Uuid, current: &[u8]) -> Result<Vec<Session>> {
        Ok(account_sessions(user, current)
            .fetch_all(&self.pool)
            .await?)
    }
    async fn revoke_sessions(
        &self,
        user: Uuid,
        current: &[u8],
        session: Option<Uuid>,
    ) -> Result<()> {
        transactions::revoke_account_sessions(&self.pool, user, current, session).await
    }
    async fn initialize(&self) -> Result<()> {
        if has_accounts().fetch_one(&self.pool).await? {
            delete_setup_tokens().execute(&self.pool).await?;
        }
        Ok(())
    }
    async fn setup_token(&self, hash: &[u8]) -> Result<()> {
        transactions::account_setup_token(&self.pool, hash).await
    }
    async fn initialize_admin(
        &self,
        username: &str,
        password: &str,
        require_change: bool,
    ) -> Result<Uuid> {
        transactions::initialize_admin(&self.pool, username, password, require_change).await
    }
    async fn setup_required(&self) -> Result<bool> {
        Ok(setup_required().fetch_one(&self.pool).await?)
    }
    async fn setup_hash(&self) -> Result<Option<Vec<u8>>> {
        Ok(setup_hash().fetch_optional(&self.pool).await?)
    }
    async fn setup(
        &self,
        username: &str,
        password: &str,
        supplied: &[u8],
        request_id: &str,
    ) -> Result<()> {
        transactions::setup_account(&self.pool, username, password, supplied, request_id).await
    }
}
