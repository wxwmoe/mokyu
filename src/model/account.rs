use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Serialize, ToSchema)]
pub(crate) struct Profile {
    pub id: Uuid,
    pub username: String,
    pub role: String,
    pub must_change_password: bool,
    pub project_management: bool,
    pub display_name: String,
    pub locale: Option<String>,
    pub theme: Option<String>,
    pub avatar_email: String,
    pub avatar_enabled: bool,
    pub avatar_url: Option<String>,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct Preferences {
    pub display_name: String,
    pub locale: Option<String>,
    pub theme: Option<String>,
    pub avatar_email: String,
    pub avatar_enabled: bool,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct Session {
    pub(crate) id: Uuid,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) last_seen_at: DateTime<Utc>,
    pub(crate) expires_at: DateTime<Utc>,
    pub(crate) user_agent: String,
    pub(crate) current: bool,
}

pub(crate) struct AuthSession {
    pub user_id: Uuid,
    pub session_id: Uuid,
    pub role: String,
    pub must_change_password: bool,
    pub authorization_revision: i64,
    pub csrf_hash: Vec<u8>,
    pub last_seen_at: DateTime<Utc>,
}

pub(crate) type TokenIdentity = (Uuid, Uuid, String, bool, i64, i64);
pub(crate) type VerifiedAccount = (Uuid, String, i64);
pub(crate) type SessionTimes = (Uuid, DateTime<Utc>, DateTime<Utc>);

pub(crate) struct SessionLogin<'a> {
    pub verified: &'a VerifiedAccount,
    pub access_hash: &'a [u8],
    pub csrf_hash: &'a [u8],
    pub refresh_hash: &'a [u8],
    pub seconds: u64,
    pub user_agent: &'a str,
    pub request_id: &'a str,
}

pub(crate) struct SessionRefresh<'a> {
    pub supplied: &'a str,
    pub access: Option<&'a str>,
    pub key: &'a [u8; 32],
    pub seconds: u64,
    pub request_id: &'a str,
}

pub(crate) struct RefreshedSession {
    pub user: Uuid,
    pub session: Uuid,
    pub access: String,
    pub csrf: String,
    pub refresh: String,
    pub expires: DateTime<Utc>,
    pub refreshed_at: DateTime<Utc>,
    pub issue_access: bool,
}

pub(crate) fn derived_token(key: &[u8; 32], purpose: &str, secret: &str) -> String {
    let mut hash = blake3::Hasher::new_keyed(key);
    hash.update(purpose.as_bytes());
    hash.update(b"\0");
    hash.update(secret.as_bytes());
    hash.finalize().to_hex().to_string()
}

pub(crate) fn csrf_token(token: &str) -> String {
    let mut hash = blake3::Hasher::new();
    hash.update(b"mokyu-web-csrf-v1\0");
    hash.update(token.as_bytes());
    hash.finalize().to_hex().to_string()
}

#[async_trait]
pub(crate) trait AccountStore: Send + Sync {
    async fn recent(&self, session: Uuid, user: Uuid) -> Result<bool>;
    async fn authenticate_token(&self, hash: &[u8]) -> Result<Option<TokenIdentity>>;
    async fn authenticate_session(&self, hash: &[u8]) -> Result<Option<AuthSession>>;
    async fn login_user(&self, username: &str) -> Result<Option<VerifiedAccount>>;
    async fn password(&self, user: Uuid) -> Result<Option<(String, i64)>>;
    async fn login(&self, input: SessionLogin<'_>) -> Result<()>;
    async fn failed_login(&self, user: Uuid, request_id: &str) -> Result<()>;
    async fn refresh(&self, input: SessionRefresh<'_>) -> Result<RefreshedSession>;
    async fn session(&self, hash: &[u8]) -> Result<Option<SessionTimes>>;
    async fn logout(&self, user: Uuid, hash: &[u8]) -> Result<()>;
    async fn profile(&self, user: Uuid) -> Result<Option<Profile>>;
    async fn preferences(&self, user: Uuid, hash: &[u8], input: Preferences) -> Result<()>;
    async fn change_password(
        &self,
        verified: &VerifiedAccount,
        current: &[u8],
        password: &str,
        refresh: &[u8],
    ) -> Result<()>;
    async fn reauthenticate(
        &self,
        verified: &VerifiedAccount,
        current: &[u8],
        refresh: &[u8],
    ) -> Result<()>;
    async fn sessions(&self, user: Uuid, current: &[u8]) -> Result<Vec<Session>>;
    async fn revoke_sessions(
        &self,
        user: Uuid,
        current: &[u8],
        session: Option<Uuid>,
    ) -> Result<()>;
    async fn initialize(&self) -> Result<()>;
    async fn setup_token(&self, hash: &[u8]) -> Result<()>;
    async fn initialize_admin(
        &self,
        username: &str,
        password: &str,
        require_change: bool,
    ) -> Result<Uuid>;
    async fn setup_required(&self) -> Result<bool>;
    async fn setup_hash(&self) -> Result<Option<Vec<u8>>>;
    async fn setup(
        &self,
        username: &str,
        password: &str,
        supplied: &[u8],
        request_id: &str,
    ) -> Result<()>;
}
