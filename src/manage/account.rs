use super::{Identity, operations, origin, token};
use crate::{
    app::App,
    http::{HttpError, unauthorized},
};
use anyhow::{Context, Result};
use axum::{
    Json,
    extract::{Extension, Path, State},
    http::{HeaderMap, StatusCode},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{path::PathBuf, sync::Arc};
use tokio::io::AsyncWriteExt;
use utoipa::ToSchema;
use uuid::Uuid;

pub(crate) const USER_LOCK: i64 = 734922709851002;

#[derive(Serialize, sqlx::FromRow, ToSchema)]
pub(super) struct Profile {
    pub id: Uuid,
    pub username: String,
    pub role: String,
    pub project_management: bool,
    pub display_name: String,
    pub locale: Option<String>,
    pub theme: Option<String>,
    pub avatar_email: String,
    pub avatar_enabled: bool,
    #[sqlx(skip)]
    pub avatar_url: Option<String>,
}

pub(super) async fn profile(app: &App, id: Uuid) -> Result<Profile, HttpError> {
    let mut row: Profile = sqlx::query_as("SELECT u.id,u.username,u.role,m.project_management,u.display_name,u.locale,u.theme,u.avatar_email,u.avatar_enabled FROM web_users u CROSS JOIN mokyu_meta m WHERE u.id=$1 AND u.enabled")
        .bind(id).fetch_optional(&app.db).await?.ok_or_else(unauthorized)?;
    if row.avatar_enabled && !row.avatar_email.is_empty() {
        let hash = hex::encode(Sha256::digest(
            row.avatar_email.trim().to_lowercase().as_bytes(),
        ));
        row.avatar_url = Some(format!(
            "{}/{hash}?s=128&r=g&d=404",
            app.config.manage.gravatar_base_url.trim_end_matches('/')
        ));
    }
    Ok(row)
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct Preferences {
    display_name: String,
    locale: Option<String>,
    theme: Option<String>,
    avatar_email: String,
    avatar_enabled: bool,
}
impl Preferences {
    fn validate(&self) -> Result<(), HttpError> {
        let email = self.avatar_email.trim();
        if self.display_name.len() > 240
            || self.display_name.chars().any(char::is_control)
            || !matches!(self.locale.as_deref(), None | Some("en" | "zh-CN" | "ja"))
            || !matches!(
                self.theme.as_deref(),
                None | Some("auto" | "light" | "dark")
            )
            || email.len() > 320
            || email.chars().any(|c| c.is_whitespace() || c.is_control())
            || (!email.is_empty()
                && (email.matches('@').count() != 1
                    || email.starts_with('@')
                    || email.ends_with('@')))
            || (self.avatar_enabled && email.is_empty())
        {
            return Err(s3s::s3_error!(InvalidArgument).into());
        }
        Ok(())
    }
}

#[utoipa::path(get, path="/api/me", responses((status=200, body=Profile)))]
pub(super) async fn me(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<Profile>, HttpError> {
    let id = actor.id;
    Ok(Json(profile(&app, id).await?))
}
#[utoipa::path(put, path="/api/me", request_body=Preferences, responses((status=200, body=Profile)))]
pub(super) async fn save(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    headers: HeaderMap,
    Json(input): Json<Preferences>,
) -> Result<Json<Profile>, HttpError> {
    let id = actor.id;
    input.validate()?;
    let mut tx = app.db.begin().await?;
    lock_session(&mut tx, &headers, id).await?;
    sqlx::query("UPDATE web_users SET display_name=$2,locale=$3,theme=$4,avatar_email=$5,avatar_enabled=$6 WHERE id=$1 AND enabled")
        .bind(id).bind(input.display_name.trim()).bind(input.locale).bind(input.theme)
        .bind(input.avatar_email.trim().to_lowercase()).bind(input.avatar_enabled).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(Json(profile(&app, id).await?))
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct Password {
    current_password: String,
    new_password: String,
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct Reauthenticate {
    password: String,
}

async fn verify(app: &App, id: Uuid, password: String) -> Result<(Uuid, String, i64), HttpError> {
    if password.len() > 1024 {
        return Err(unauthorized());
    }
    let (hash, revision): (String, i64) =
        sqlx::query_as("SELECT password_hash,auth_revision FROM web_users WHERE id=$1 AND enabled")
            .bind(id)
            .fetch_optional(&app.db)
            .await?
            .ok_or_else(unauthorized)?;
    if !operations::password_matches(hash.clone(), password).await? {
        return Err(unauthorized());
    }
    Ok((id, hash, revision))
}
pub(super) async fn lock_session(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    headers: &HeaderMap,
    id: Uuid,
) -> Result<(String, i64), HttpError> {
    let row: Option<(String, i64, bool)> = sqlx::query_as(
        "SELECT password_hash,auth_revision,enabled FROM web_users WHERE id=$1 FOR UPDATE",
    )
    .bind(id)
    .fetch_optional(&mut **tx)
    .await?;
    let (hash, revision, enabled) = row.ok_or_else(unauthorized)?;
    if !enabled {
        return Err(unauthorized());
    }
    let active: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM sessions WHERE user_id=$1 AND token_hash=$2 AND expires_at>now() AND auth_revision=$3)")
        .bind(id).bind(blake3::hash(token(headers)?.as_bytes()).as_bytes().as_slice()).bind(revision).fetch_one(&mut **tx).await?;
    if !active {
        return Err(unauthorized());
    }
    Ok((hash, revision))
}
async fn lock_verified(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    headers: &HeaderMap,
    verified: &(Uuid, String, i64),
) -> Result<(), HttpError> {
    let (hash, revision) = lock_session(tx, headers, verified.0).await?;
    if hash != verified.1 || revision != verified.2 {
        return Err(unauthorized());
    }
    Ok(())
}

#[utoipa::path(post, path="/api/me/password", request_body=Password, responses((status=204)))]
pub(super) async fn password(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    headers: HeaderMap,
    Json(input): Json<Password>,
) -> Result<StatusCode, HttpError> {
    if !(12..=1024).contains(&input.new_password.len()) {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let verified = verify(&app, actor.id, input.current_password).await?;
    let hash = operations::password_hash(input.new_password).await?;
    let mut tx = app.db.begin().await?;
    lock_verified(&mut tx, &headers, &verified).await?;
    sqlx::query("UPDATE web_users SET password_hash=$2,auth_revision=auth_revision+1 WHERE id=$1")
        .bind(verified.0)
        .bind(hash)
        .execute(&mut *tx)
        .await?;
    let current = blake3::hash(token(&headers)?.as_bytes());
    sqlx::query("DELETE FROM sessions WHERE user_id=$1 AND token_hash<>$2")
        .bind(verified.0)
        .bind(current.as_bytes().as_slice())
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "UPDATE sessions SET auth_revision=$2,reauthenticated_at=now() WHERE token_hash=$1",
    )
    .bind(current.as_bytes().as_slice())
    .bind(verified.2 + 1)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(post, path="/api/me/reauth", request_body=Reauthenticate, responses((status=204)))]
pub(super) async fn reauthenticate(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    headers: HeaderMap,
    Json(input): Json<Reauthenticate>,
) -> Result<StatusCode, HttpError> {
    let verified = verify(&app, actor.id, input.password).await?;
    let mut tx = app.db.begin().await?;
    lock_verified(&mut tx, &headers, &verified).await?;
    sqlx::query("UPDATE sessions SET reauthenticated_at=now() WHERE token_hash=$1")
        .bind(
            blake3::hash(token(&headers)?.as_bytes())
                .as_bytes()
                .as_slice(),
        )
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Serialize, sqlx::FromRow, ToSchema)]
pub(super) struct Session {
    id: Uuid,
    created_at: DateTime<Utc>,
    last_seen_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    user_agent: String,
    current: bool,
}
#[derive(Serialize, ToSchema)]
pub(super) struct SessionList {
    sessions: Vec<Session>,
    more: bool,
}

#[utoipa::path(get, path="/api/me/sessions", responses((status=200, body=SessionList)))]
pub(super) async fn sessions(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    headers: HeaderMap,
) -> Result<Json<SessionList>, HttpError> {
    let id = actor.id;
    let mut rows = sqlx::query_as("SELECT id,created_at,last_seen_at,expires_at,user_agent,(token_hash=$2) AS current FROM sessions WHERE user_id=$1 AND expires_at>now() ORDER BY created_at DESC,id DESC LIMIT 101")
        .bind(id).bind(blake3::hash(token(&headers)?.as_bytes()).as_bytes().as_slice()).fetch_all(&app.db).await?;
    let more = rows.len() > 100;
    rows.truncate(100);
    Ok(Json(SessionList {
        sessions: rows,
        more,
    }))
}
#[utoipa::path(delete, path="/api/me/sessions/{id}", params(("id"=Uuid, Path)), responses((status=204)))]
pub(super) async fn revoke_session(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, HttpError> {
    let user = actor.id;
    let mut tx = app.db.begin().await?;
    lock_session(&mut tx, &headers, user).await?;
    sqlx::query("DELETE FROM sessions WHERE id=$1 AND user_id=$2")
        .bind(id)
        .bind(user)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}
#[utoipa::path(delete, path="/api/me/sessions", responses((status=204)))]
pub(super) async fn revoke_others(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    headers: HeaderMap,
) -> Result<StatusCode, HttpError> {
    let id = actor.id;
    let mut tx = app.db.begin().await?;
    lock_session(&mut tx, &headers, id).await?;
    sqlx::query("DELETE FROM sessions WHERE user_id=$1 AND token_hash<>$2")
        .bind(id)
        .bind(
            blake3::hash(token(&headers)?.as_bytes())
                .as_bytes()
                .as_slice(),
        )
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}

fn setup_path(app: &App) -> Result<PathBuf> {
    Ok(app
        .config
        .listen
        .admin_socket
        .parent()
        .context("admin socket directory is required")?
        .join("setup-token"))
}

pub(crate) async fn initialize(app: &App) -> Result<()> {
    let has_users: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM web_users)")
        .fetch_one(&app.db)
        .await?;
    let path = setup_path(app)?;
    if has_users {
        sqlx::query("DELETE FROM manage_setup")
            .execute(&app.db)
            .await?;
        let _ = tokio::fs::remove_file(path).await;
        return Ok(());
    }
    tokio::fs::create_dir_all(path.parent().unwrap()).await?;
    let secret = match tokio::fs::read_to_string(&path).await {
        Ok(value) => {
            anyhow::ensure!(
                value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit()),
                "invalid setup token file; remove it to generate a new token"
            );
            value
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let value = crate::admin::random_secret()?;
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&path)
                .await?;
            file.write_all(value.as_bytes()).await?;
            file.sync_all().await?;
            value
        }
        Err(error) => return Err(error.into()),
    };
    sqlx::query("INSERT INTO manage_setup(singleton,token_hash) VALUES(true,$1) ON CONFLICT(singleton) DO UPDATE SET token_hash=excluded.token_hash")
        .bind(blake3::hash(secret.as_bytes()).as_bytes().as_slice()).execute(&app.db).await?;
    tracing::info!(path=%path.display(), "first administrator setup token is available in the private local file");
    Ok(())
}

#[derive(Serialize, ToSchema)]
pub(super) struct Bootstrap {
    setup_required: bool,
}
#[utoipa::path(get, path="/api/bootstrap", responses((status=200, body=Bootstrap)))]
pub(super) async fn bootstrap(State(app): State<Arc<App>>) -> Result<Json<Bootstrap>, HttpError> {
    let setup_required = sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM web_users)")
        .fetch_one(&app.db)
        .await?;
    Ok(Json(Bootstrap { setup_required }))
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct Setup {
    token: String,
    username: String,
    password: String,
}
#[utoipa::path(post, path="/api/setup", request_body=Setup, responses((status=201)))]
pub(super) async fn setup(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(input): Json<Setup>,
) -> Result<StatusCode, HttpError> {
    origin(&app, &headers)?;
    if input.token.len() != 64
        || input.username.is_empty()
        || input.username.len() > 64
        || input.username.chars().any(char::is_control)
        || !(12..=1024).contains(&input.password.len())
    {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let expected: Option<Vec<u8>> =
        sqlx::query_scalar("SELECT token_hash FROM manage_setup WHERE singleton")
            .fetch_optional(&app.db)
            .await?;
    let supplied = blake3::hash(input.token.as_bytes());
    use subtle::ConstantTimeEq;
    if !expected.is_some_and(|expected| bool::from(expected.ct_eq(supplied.as_bytes()))) {
        return Err(unauthorized());
    }
    let hash = operations::password_hash(input.password).await?;
    let mut tx = app.db.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(USER_LOCK)
        .execute(&mut *tx)
        .await?;
    let allowed: bool = sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM web_users) AND EXISTS(SELECT 1 FROM manage_setup WHERE token_hash=$1)").bind(supplied.as_bytes().as_slice()).fetch_one(&mut *tx).await?;
    if !allowed {
        return Err(unauthorized());
    }
    sqlx::query("INSERT INTO web_users(id,username,password_hash,role) VALUES($1,$2,$3,'admin')")
        .bind(Uuid::new_v4())
        .bind(input.username)
        .bind(hash)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM manage_setup")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    let _ = tokio::fs::remove_file(setup_path(&app)?).await;
    Ok(StatusCode::CREATED)
}
