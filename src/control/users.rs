use super::{Identity, operations};
pub(crate) use crate::model::administration::{BucketGrant, Member, Membership, User, UserPatch};
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
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, sync::Arc};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

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
    app.administration
        .find_user(name)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey).into())
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
    app.administration
        .create_user(
            principal,
            crate::model::administration::UserCreate {
                username: input.username,
                password_hash: hash,
                role: input.role,
                must_change_password: input.must_change_password,
            },
        )
        .await
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
    app.administration.update_user(principal, id, input).await
}
pub(crate) async fn delete_user(app: &App, principal: &Principal, id: Uuid) -> Result<()> {
    app.administration.delete_user(principal, id).await
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
    app.administration
        .reset_password(principal, id, &hash, input.must_change_password)
        .await
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
    #[serde(rename = "items")]
    users: Vec<User>,
    #[serde(rename = "next_cursor")]
    next: Option<String>,
}
#[utoipa::path(get,path="/api/v1/users",operation_id="users_list",params(UserQuery),responses((status=200,body=UserPage)))]
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
    let mut users = app
        .administration
        .users(
            &q.q.unwrap_or_default(),
            &q.after.unwrap_or_default(),
            limit + 1,
            q.role.as_deref(),
        )
        .await?;
    let more = users.len() > limit as usize;
    users.truncate(limit as usize);
    let next = more.then(|| users.last().unwrap().username.clone());
    Ok(Json(UserPage { users, next }))
}
#[utoipa::path(get,path="/api/v1/users/{id}",operation_id="users_get",params(("id"=Uuid,Path)),responses((status=200,body=User)))]
pub(super) async fn get(
    State(app): State<Arc<App>>,
    Path(id): Path<Uuid>,
) -> Result<Json<User>, HttpError> {
    Ok(Json(
        app.administration
            .user(id)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchKey))?,
    ))
}
#[utoipa::path(post,path="/api/v1/users",operation_id="users_create",request_body=CreateUser,responses((status=201,body=User)))]
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
#[utoipa::path(patch,path="/api/v1/users/{id}",operation_id="users_update",params(("id"=Uuid,Path)),request_body=UserPatch,responses((status=200,body=User)))]
pub(super) async fn update(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
    Json(input): Json<UserPatch>,
) -> Result<Json<User>, HttpError> {
    actor.recent(&app).await?;
    Ok(Json(update_user(&app, &actor.principal, id, input).await?))
}
#[utoipa::path(delete,path="/api/v1/users/{id}",operation_id="users_delete",params(("id"=Uuid,Path)),responses((status=204)))]
pub(super) async fn delete(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, HttpError> {
    actor.recent(&app).await?;
    delete_user(&app, &actor.principal, id).await?;
    Ok(StatusCode::NO_CONTENT)
}
#[utoipa::path(post,path="/api/v1/users/{id}/reset-password",params(("id"=Uuid,Path)),request_body=ResetPassword,responses((status=204)))]
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

pub(crate) async fn save_member(
    app: &App,
    principal: &Principal,
    project: Uuid,
    user: Uuid,
    input: Option<Membership>,
) -> Result<()> {
    if let Some(input) = &input {
        crate::model::external::validate_grants(&input.external_grants)?;
        let ceiling = Action::role(&input.role);
        let mut ids = HashSet::new();
        if ceiling.is_empty()
            || !matches!(input.scope.as_str(), "all" | "selected")
            || input.grants.len() > 1000
            || (input.scope == "all"
                && (!input.grants.is_empty() || !input.external_grants.is_empty()))
            || input
                .external_grants
                .iter()
                .any(|g| g.actions.iter().any(|a| !ceiling.contains(a)))
            || input.grants.iter().any(|g| {
                !ids.insert(g.bucket_id)
                    || g.actions.is_empty()
                    || g.actions.iter().any(|a| !ceiling.contains(a))
            })
        {
            return Err(s3s::s3_error!(InvalidArgument).into());
        }
    }
    app.administration
        .save_member(principal, project, user, input)
        .await
}
#[utoipa::path(get,path="/api/v1/projects/{id}/members",params(("id"=Uuid,Path)),responses((status=200,body=Vec<Member>)))]
pub(super) async fn members(
    State(app): State<Arc<App>>,
    Path(id): Path<Uuid>,
) -> Result<Json<Vec<Member>>, HttpError> {
    Ok(Json(app.administration.members(id).await?))
}
#[utoipa::path(put,path="/api/v1/projects/{id}/members/{user}",params(("id"=Uuid,Path),("user"=Uuid,Path)),request_body=Membership,responses((status=204)))]
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
#[utoipa::path(delete,path="/api/v1/projects/{id}/members/{user}",params(("id"=Uuid,Path),("user"=Uuid,Path)),responses((status=204)))]
pub(super) async fn remove_member(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path((id, user)): Path<(Uuid, Uuid)>,
) -> Result<StatusCode, HttpError> {
    actor.recent(&app).await?;
    save_member(&app, &actor.principal, id, user, None).await?;
    Ok(StatusCode::NO_CONTENT)
}
