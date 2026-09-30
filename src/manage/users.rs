use super::{Identity, account::USER_LOCK, operations};
use crate::{
    app::App,
    authorization::{Action, Principal},
    http::HttpError,
};
use anyhow::Result;
use axum::{
    Json,
    extract::{Extension, Path, Query, State},
    http::StatusCode,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Postgres, Transaction};
use std::{collections::HashSet, sync::Arc};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

#[derive(Serialize, sqlx::FromRow, ToSchema)]
pub(crate) struct User {
    pub id: Uuid,
    pub username: String,
    display_name: String,
    pub role: String,
    pub enabled: bool,
    must_change_password: bool,
    created_at: DateTime<Utc>,
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateUser {
    pub username: String,
    pub password: String,
    pub role: String,
    #[serde(default = "yes")]
    pub must_change_password: bool,
}
fn yes() -> bool {
    true
}
fn valid_role(role: &str) -> bool {
    matches!(role, "admin" | "member")
}
pub(crate) async fn find(app: &App, name: &str) -> Result<User> {
    Ok(sqlx::query_as("SELECT * FROM web_users WHERE username=$1")
        .bind(name)
        .fetch_optional(&app.db)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?)
}
pub(super) async fn admin_transaction<'a>(
    app: &'a App,
    principal: &Principal,
) -> Result<Transaction<'a, Postgres>> {
    let mut tx = app.db.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(USER_LOCK)
        .execute(&mut *tx)
        .await?;
    principal.lock_admin(&mut tx).await?;
    Ok(tx)
}
async fn project_mode(tx: &mut Transaction<'_, Postgres>, role: &str) -> Result<()> {
    if role == "member" {
        sqlx::query("UPDATE mokyu_meta SET project_management=true")
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}
pub(crate) async fn create_user(
    app: &App,
    principal: &Principal,
    input: CreateUser,
) -> Result<User> {
    if input.username.is_empty()
        || input.username.len() > 64
        || input.username.trim() != input.username
        || input.username.chars().any(char::is_control)
        || !valid_role(&input.role)
        || !(12..=1024).contains(&input.password.len())
    {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let hash = operations::password_hash(input.password).await?;
    let mut tx = admin_transaction(app, principal).await?;
    if input.role == "member" {
        let admin: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM web_users WHERE enabled AND role='admin')",
        )
        .fetch_one(&mut *tx)
        .await?;
        if !admin {
            return Err(crate::http::problem(StatusCode::CONFLICT, "LastAdministrator").0);
        }
    }
    project_mode(&mut tx, &input.role).await?;
    let row: Option<User> = sqlx::query_as("INSERT INTO web_users(id,username,password_hash,role,must_change_password) VALUES($1,$2,$3,$4,$5) ON CONFLICT(username) DO NOTHING RETURNING *")
        .bind(Uuid::new_v4()).bind(input.username).bind(hash).bind(input.role).bind(input.must_change_password).fetch_optional(&mut *tx).await?;
    let row = row.ok_or_else(|| s3s::s3_error!(OperationAborted))?;
    sqlx::query("DELETE FROM manage_setup")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(row)
}
#[derive(Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct UserPatch {
    pub role: Option<String>,
    pub enabled: Option<bool>,
}
async fn protect_admin(
    tx: &mut Transaction<'_, Postgres>,
    current: &User,
    role: &str,
    enabled: bool,
) -> Result<()> {
    if current.role == "admin" && current.enabled && (role != "admin" || !enabled) {
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM web_users WHERE enabled AND role='admin'")
                .fetch_one(&mut **tx)
                .await?;
        if count <= 1 {
            return Err(crate::http::problem(StatusCode::CONFLICT, "LastAdministrator").0);
        }
    }
    Ok(())
}
pub(crate) async fn update_user(
    app: &App,
    principal: &Principal,
    id: Uuid,
    input: UserPatch,
) -> Result<User> {
    if input.role.as_deref().is_some_and(|role| !valid_role(role))
        || (input.role.is_none() && input.enabled.is_none())
    {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let mut tx = admin_transaction(app, principal).await?;
    // Mode changes lock metadata before the target user so project settings cannot race.
    sqlx::query("SELECT singleton FROM mokyu_meta FOR UPDATE")
        .execute(&mut *tx)
        .await?;
    let current: User = sqlx::query_as("SELECT * FROM web_users WHERE id=$1 FOR UPDATE")
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    let role = input.role.as_deref().unwrap_or(&current.role);
    let enabled = input.enabled.unwrap_or(current.enabled);
    protect_admin(&mut tx, &current, role, enabled).await?;
    project_mode(&mut tx, role).await?;
    if role == "admin" && current.role != "admin" {
        sqlx::query("DELETE FROM project_members WHERE user_id=$1")
            .bind(id)
            .execute(&mut *tx)
            .await?;
    }
    let row = sqlx::query_as("UPDATE web_users SET role=$2,enabled=$3,auth_revision=auth_revision+1 WHERE id=$1 RETURNING *").bind(id).bind(role).bind(enabled).fetch_one(&mut *tx).await?;
    sqlx::query("DELETE FROM sessions WHERE user_id=$1")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(row)
}
pub(crate) async fn delete_user(app: &App, principal: &Principal, id: Uuid) -> Result<()> {
    let mut tx = admin_transaction(app, principal).await?;
    let current: User = sqlx::query_as("SELECT * FROM web_users WHERE id=$1 FOR UPDATE")
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    protect_admin(&mut tx, &current, "member", false).await?;
    sqlx::query("DELETE FROM web_users WHERE id=$1")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResetPassword {
    pub password: String,
    #[serde(default = "yes")]
    pub must_change_password: bool,
}
pub(crate) async fn reset_password(
    app: &App,
    principal: &Principal,
    id: Uuid,
    input: ResetPassword,
) -> Result<()> {
    if !(12..=1024).contains(&input.password.len()) {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let hash = operations::password_hash(input.password).await?;
    let mut tx = admin_transaction(app, principal).await?;
    let changed = sqlx::query("UPDATE web_users SET password_hash=$2,auth_revision=auth_revision+1,must_change_password=$3 WHERE id=$1")
        .bind(id).bind(hash).bind(input.must_change_password).execute(&mut *tx).await?.rows_affected();
    if changed == 0 {
        return Err(s3s::s3_error!(NoSuchKey).into());
    }
    sqlx::query("DELETE FROM sessions WHERE user_id=$1")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}
#[derive(Deserialize, IntoParams)]
pub(super) struct UserQuery {
    q: Option<String>,
    role: Option<String>,
    after: Option<String>,
    limit: Option<i64>,
}
#[derive(Serialize, ToSchema)]
pub(super) struct UserPage {
    users: Vec<User>,
    next: Option<String>,
}
#[utoipa::path(get,path="/api/users",operation_id="users_list",params(UserQuery),responses((status=200,body=UserPage)))]
pub(super) async fn list(
    State(app): State<Arc<App>>,
    Query(q): Query<UserQuery>,
) -> Result<Json<UserPage>, HttpError> {
    let limit = q.limit.unwrap_or(50);
    if !(1..=100).contains(&limit)
        || q.role.as_deref().is_some_and(|role| !valid_role(role))
        || q.q.as_ref().is_some_and(|s| s.len() > 240)
        || q.after.as_ref().is_some_and(|s| s.len() > 64)
    {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let mut users: Vec<User> = sqlx::query_as("SELECT * FROM web_users WHERE (position(lower($1) in lower(username))>0 OR position(lower($1) in lower(display_name))>0) AND username COLLATE \"C\">$2 AND ($4::text IS NULL OR role=$4) ORDER BY username COLLATE \"C\" LIMIT $3")
        .bind(q.q.unwrap_or_default()).bind(q.after.unwrap_or_default()).bind(limit+1).bind(q.role).fetch_all(&app.db).await?;
    let more = users.len() > limit as usize;
    users.truncate(limit as usize);
    let next = more.then(|| users.last().unwrap().username.clone());
    Ok(Json(UserPage { users, next }))
}
#[utoipa::path(get,path="/api/users/{id}",operation_id="users_get",params(("id"=Uuid,Path)),responses((status=200,body=User)))]
pub(super) async fn get(
    State(app): State<Arc<App>>,
    Path(id): Path<Uuid>,
) -> Result<Json<User>, HttpError> {
    Ok(Json(
        sqlx::query_as("SELECT * FROM web_users WHERE id=$1")
            .bind(id)
            .fetch_optional(&app.db)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchKey))?,
    ))
}
#[utoipa::path(post,path="/api/users",operation_id="users_create",request_body=CreateUser,responses((status=201,body=User)))]
pub(super) async fn create(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<CreateUser>,
) -> Result<(StatusCode, Json<User>), HttpError> {
    actor.recent(&app).await?;
    Ok((
        StatusCode::CREATED,
        Json(create_user(&app, &actor.principal, input).await?),
    ))
}
#[utoipa::path(patch,path="/api/users/{id}",operation_id="users_update",params(("id"=Uuid,Path)),request_body=UserPatch,responses((status=200,body=User)))]
pub(super) async fn update(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
    Json(input): Json<UserPatch>,
) -> Result<Json<User>, HttpError> {
    actor.recent(&app).await?;
    Ok(Json(update_user(&app, &actor.principal, id, input).await?))
}
#[utoipa::path(delete,path="/api/users/{id}",operation_id="users_delete",params(("id"=Uuid,Path)),responses((status=204)))]
pub(super) async fn delete(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, HttpError> {
    actor.recent(&app).await?;
    delete_user(&app, &actor.principal, id).await?;
    Ok(StatusCode::NO_CONTENT)
}
#[utoipa::path(post,path="/api/users/{id}/reset-password",params(("id"=Uuid,Path)),request_body=ResetPassword,responses((status=204)))]
pub(super) async fn reset(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
    Json(input): Json<ResetPassword>,
) -> Result<StatusCode, HttpError> {
    actor.recent(&app).await?;
    reset_password(&app, &actor.principal, id, input).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct BucketGrant {
    pub bucket_id: Uuid,
    pub actions: Vec<Action>,
}
#[derive(Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct Membership {
    pub role: String,
    pub scope: String,
    #[serde(default)]
    pub grants: Vec<BucketGrant>,
}
#[derive(Serialize, sqlx::FromRow, ToSchema)]
pub(super) struct Member {
    user_id: Uuid,
    username: String,
    display_name: String,
    enabled: bool,
    role: String,
    scope: String,
    #[schema(value_type=Vec<BucketGrant>)]
    grants: sqlx::types::Json<Vec<BucketGrant>>,
}
pub(crate) async fn save_member(
    app: &App,
    principal: &Principal,
    project: Uuid,
    user: Uuid,
    input: Option<Membership>,
) -> Result<()> {
    if let Some(input) = &input {
        let ceiling = Action::role(&input.role);
        let mut ids = HashSet::new();
        if ceiling.is_empty()
            || !matches!(input.scope.as_str(), "all" | "selected")
            || input.grants.len() > 1000
            || (input.scope == "all" && !input.grants.is_empty())
            || input.grants.iter().any(|g| {
                !ids.insert(g.bucket_id)
                    || g.actions.is_empty()
                    || g.actions.iter().any(|a| !ceiling.contains(a))
            })
        {
            return Err(s3s::s3_error!(InvalidArgument).into());
        }
    }
    let mut tx = admin_transaction(app, principal).await?;
    project_mode(&mut tx, "member").await?;
    let target: Option<String> =
        sqlx::query_scalar("SELECT role FROM web_users WHERE id=$1 FOR UPDATE")
            .bind(user)
            .fetch_optional(&mut *tx)
            .await?;
    if target.as_deref() != Some("member") {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM projects WHERE id=$1)")
        .bind(project)
        .fetch_one(&mut *tx)
        .await?;
    if !exists {
        return Err(s3s::s3_error!(NoSuchKey).into());
    }
    if let Some(input) = input {
        let mut buckets: Vec<Uuid> = input.grants.iter().map(|g| g.bucket_id).collect();
        buckets.sort();
        let valid: Vec<Uuid> = sqlx::query_scalar(
            "SELECT id FROM buckets WHERE id=ANY($1) AND project_id=$2 ORDER BY id FOR SHARE",
        )
        .bind(&buckets)
        .bind(project)
        .fetch_all(&mut *tx)
        .await?;
        if valid != buckets {
            return Err(s3s::s3_error!(InvalidArgument).into());
        }
        sqlx::query("INSERT INTO project_members(user_id,project_id,role,scope) VALUES($1,$2,$3,$4) ON CONFLICT(user_id,project_id) DO UPDATE SET role=excluded.role,scope=excluded.scope")
            .bind(user).bind(project).bind(input.role).bind(input.scope).execute(&mut *tx).await?;
        sqlx::query("DELETE FROM member_grants WHERE user_id=$1 AND project_id=$2")
            .bind(user)
            .bind(project)
            .execute(&mut *tx)
            .await?;
        for grant in input.grants {
            sqlx::query("INSERT INTO member_grants(user_id,project_id,bucket_id,actions) VALUES($1,$2,$3,$4)").bind(user).bind(project).bind(grant.bucket_id).bind(grant.actions.iter().map(|a|a.name()).collect::<Vec<_>>()).execute(&mut *tx).await?;
        }
    } else {
        sqlx::query("DELETE FROM project_members WHERE user_id=$1 AND project_id=$2")
            .bind(user)
            .bind(project)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(())
}
#[utoipa::path(get,path="/api/projects/{id}/members",params(("id"=Uuid,Path)),responses((status=200,body=Vec<Member>)))]
pub(super) async fn members(
    State(app): State<Arc<App>>,
    Path(id): Path<Uuid>,
) -> Result<Json<Vec<Member>>, HttpError> {
    Ok(Json(sqlx::query_as("SELECT m.user_id,u.username,u.display_name,u.enabled,m.role,m.scope,COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket_id',g.bucket_id,'actions',g.actions) ORDER BY g.bucket_id) FROM member_grants g WHERE g.user_id=m.user_id AND g.project_id=m.project_id),'[]'::jsonb) AS grants FROM project_members m JOIN web_users u ON u.id=m.user_id WHERE m.project_id=$1 ORDER BY u.username LIMIT 1000").bind(id).fetch_all(&app.db).await?))
}
#[utoipa::path(put,path="/api/projects/{id}/members/{user}",params(("id"=Uuid,Path),("user"=Uuid,Path)),request_body=Membership,responses((status=204)))]
pub(super) async fn put_member(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path((id, user)): Path<(Uuid, Uuid)>,
    Json(input): Json<Membership>,
) -> Result<StatusCode, HttpError> {
    actor.recent(&app).await?;
    save_member(&app, &actor.principal, id, user, Some(input)).await?;
    Ok(StatusCode::NO_CONTENT)
}
#[utoipa::path(delete,path="/api/projects/{id}/members/{user}",params(("id"=Uuid,Path),("user"=Uuid,Path)),responses((status=204)))]
pub(super) async fn remove_member(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path((id, user)): Path<(Uuid, Uuid)>,
) -> Result<StatusCode, HttpError> {
    actor.recent(&app).await?;
    save_member(&app, &actor.principal, id, user, None).await?;
    Ok(StatusCode::NO_CONTENT)
}
