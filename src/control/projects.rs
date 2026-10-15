use super::Identity;
use crate::{
    app::App,
    authorization::{DEFAULT_PROJECT, Principal},
    http::HttpError,
};
use anyhow::Result;
use axum::{
    Json,
    extract::{Extension, Path, State},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::ToSchema;
use uuid::Uuid;

pub(crate) use crate::model::administration::{Project, ProjectInput};

impl ProjectInput {
    fn validate(&self) -> Result<()> {
        if self.name.trim().is_empty()
            || self.name.len() > 128
            || self.name.chars().any(char::is_control)
            || self.description.len() > 2000
        {
            return Err(s3s::s3_error!(InvalidArgument).into());
        }
        Ok(())
    }
}
pub(crate) async fn list_all(
    store: &dyn crate::model::AdministrationStore,
) -> Result<Vec<Project>> {
    store.projects().await
}
pub(crate) async fn save(
    app: &App,
    principal: &Principal,
    id: Option<Uuid>,
    input: ProjectInput,
) -> Result<Project> {
    input.validate()?;
    app.administration.save_project(principal, id, input).await
}
pub(crate) async fn remove(app: &App, principal: &Principal, id: Uuid) -> Result<()> {
    if id == DEFAULT_PROJECT {
        return Err(s3s::s3_error!(OperationAborted).into());
    }
    app.administration.delete_project(principal, id).await
}
pub(crate) async fn mode(app: &App, principal: &Principal, enabled: bool) -> Result<()> {
    app.administration
        .set_project_mode(principal, enabled)
        .await
}

#[utoipa::path(get, path="/api/v1/projects", responses((status=200, body=Vec<Project>)))]
pub(super) async fn list(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<Vec<Project>>, HttpError> {
    Ok(Json(
        app.administration
            .visible_projects(actor.id, actor.admin, actor.principal.token_id())
            .await?,
    ))
}
#[utoipa::path(post, path="/api/v1/projects", request_body=ProjectInput, responses((status=201, body=Project)))]
pub(super) async fn create(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<ProjectInput>,
) -> Result<(StatusCode, Json<Project>), HttpError> {
    actor.recent(&app).await?;
    Ok((
        StatusCode::CREATED,
        Json(save(&app, &actor.principal, None, input).await?),
    ))
}
#[utoipa::path(put, path="/api/v1/projects/{id}", params(("id"=Uuid, Path)), request_body=ProjectInput, responses((status=200, body=Project)))]
pub(super) async fn update(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
    Json(input): Json<ProjectInput>,
) -> Result<Json<Project>, HttpError> {
    actor.recent(&app).await?;
    Ok(Json(save(&app, &actor.principal, Some(id), input).await?))
}
#[utoipa::path(delete, path="/api/v1/projects/{id}", params(("id"=Uuid, Path)), responses((status=204)))]
pub(super) async fn delete(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, HttpError> {
    actor.recent(&app).await?;
    remove(&app, &actor.principal, id).await?;
    Ok(StatusCode::NO_CONTENT)
}
#[derive(Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProjectMode {
    enabled: bool,
}
#[utoipa::path(get, path="/api/v1/projects/settings", responses((status=200, body=ProjectMode)))]
pub(super) async fn get_mode(State(app): State<Arc<App>>) -> Result<Json<ProjectMode>, HttpError> {
    Ok(Json(ProjectMode {
        enabled: app.administration.project_mode().await?,
    }))
}
#[utoipa::path(put, path="/api/v1/projects/settings", request_body=ProjectMode, responses((status=200, body=ProjectMode)))]
pub(super) async fn set_mode(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<ProjectMode>,
) -> Result<Json<ProjectMode>, HttpError> {
    actor.recent(&app).await?;
    mode(&app, &actor.principal, input.enabled).await?;
    Ok(Json(input))
}
