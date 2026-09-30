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
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::Arc;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Serialize, sqlx::FromRow, ToSchema)]
pub(crate) struct Project {
    id: Uuid,
    name: String,
    description: String,
    builtin: bool,
    allow_bucket_create: bool,
    created_at: DateTime<Utc>,
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProjectInput {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub allow_bucket_create: bool,
}
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
pub(crate) async fn list_all(db: &PgPool) -> Result<Vec<Project>> {
    Ok(
        sqlx::query_as("SELECT * FROM projects ORDER BY builtin DESC,name LIMIT 1000")
            .fetch_all(db)
            .await?,
    )
}
pub(crate) async fn save(
    app: &App,
    principal: &Principal,
    id: Option<Uuid>,
    input: ProjectInput,
) -> Result<Project> {
    input.validate()?;
    let mut tx = super::users::admin_transaction(app, principal).await?;
    sqlx::query("SELECT singleton FROM mokyu_meta FOR UPDATE")
        .execute(&mut *tx)
        .await?;
    let duplicate: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM projects WHERE name=$1 AND ($2::uuid IS NULL OR id<>$2))",
    )
    .bind(input.name.trim())
    .bind(id)
    .fetch_one(&mut *tx)
    .await?;
    if duplicate {
        return Err(s3s::s3_error!(OperationAborted).into());
    }
    let row = if let Some(id) = id {
        sqlx::query_as("UPDATE projects SET name=$2,description=$3,allow_bucket_create=$4 WHERE id=$1 RETURNING *")
            .bind(id).bind(input.name.trim()).bind(input.description).bind(input.allow_bucket_create).fetch_optional(&mut *tx).await?.ok_or_else(|| s3s::s3_error!(NoSuchKey))?
    } else {
        sqlx::query("UPDATE mokyu_meta SET project_management=true")
            .execute(&mut *tx)
            .await?;
        sqlx::query_as("INSERT INTO projects(id,name,description,allow_bucket_create) VALUES($1,$2,$3,$4) RETURNING *")
            .bind(Uuid::new_v4()).bind(input.name.trim()).bind(input.description).bind(input.allow_bucket_create).fetch_one(&mut *tx).await?
    };
    tx.commit().await?;
    Ok(row)
}
pub(crate) async fn remove(app: &App, principal: &Principal, id: Uuid) -> Result<()> {
    if id == DEFAULT_PROJECT {
        return Err(s3s::s3_error!(OperationAborted).into());
    }
    let mut tx = super::users::admin_transaction(app, principal).await?;
    let exists: Option<Uuid> = sqlx::query_scalar("SELECT id FROM projects WHERE id=$1 FOR UPDATE")
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?;
    if exists.is_none() {
        return Err(s3s::s3_error!(NoSuchKey).into());
    }
    let dependent: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM buckets WHERE project_id=$1) OR EXISTS(SELECT 1 FROM credentials WHERE project_id=$1) OR EXISTS(SELECT 1 FROM project_members WHERE project_id=$1)").bind(id).fetch_one(&mut *tx).await?;
    if dependent {
        return Err(s3s::s3_error!(OperationAborted).into());
    }
    sqlx::query("DELETE FROM projects WHERE id=$1")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}
pub(crate) async fn mode(app: &App, principal: &Principal, enabled: bool) -> Result<()> {
    let mut tx = super::users::admin_transaction(app, principal).await?;
    sqlx::query("SELECT singleton FROM mokyu_meta FOR UPDATE")
        .execute(&mut *tx)
        .await?;
    if !enabled {
        let dependent: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM projects WHERE NOT builtin) OR EXISTS(SELECT 1 FROM web_users WHERE role='member') OR EXISTS(SELECT 1 FROM project_members)").fetch_one(&mut *tx).await?;
        if dependent {
            return Err(s3s::s3_error!(OperationAborted).into());
        }
    }
    sqlx::query("UPDATE mokyu_meta SET project_management=$1")
        .bind(enabled)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

#[utoipa::path(get, path="/api/projects", responses((status=200, body=Vec<Project>)))]
pub(super) async fn list(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<Vec<Project>>, HttpError> {
    let rows = sqlx::query_as("SELECT p.* FROM projects p WHERE $2 OR CASE WHEN $3::uuid IS NOT NULL THEN EXISTS(SELECT 1 FROM token_bucket_access t JOIN buckets b ON b.id=t.bucket_id WHERE t.token_id=$3 AND b.project_id=p.id AND 'bucket.list'=ANY(t.actions)) ELSE EXISTS(SELECT 1 FROM project_members m WHERE m.project_id=p.id AND m.user_id=$1) END ORDER BY p.builtin DESC,p.name LIMIT 1000").bind(actor.id).bind(actor.admin).bind(actor.principal.token_id()).fetch_all(&app.db).await?;
    Ok(Json(rows))
}
#[utoipa::path(post, path="/api/projects", request_body=ProjectInput, responses((status=201, body=Project)))]
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
#[utoipa::path(put, path="/api/projects/{id}", params(("id"=Uuid, Path)), request_body=ProjectInput, responses((status=200, body=Project)))]
pub(super) async fn update(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
    Json(input): Json<ProjectInput>,
) -> Result<Json<Project>, HttpError> {
    actor.recent(&app).await?;
    Ok(Json(save(&app, &actor.principal, Some(id), input).await?))
}
#[utoipa::path(delete, path="/api/projects/{id}", params(("id"=Uuid, Path)), responses((status=204)))]
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
#[utoipa::path(get, path="/api/settings/projects", responses((status=200, body=ProjectMode)))]
pub(super) async fn get_mode(State(app): State<Arc<App>>) -> Result<Json<ProjectMode>, HttpError> {
    Ok(Json(ProjectMode {
        enabled: sqlx::query_scalar("SELECT project_management FROM mokyu_meta")
            .fetch_one(&app.db)
            .await?,
    }))
}
#[utoipa::path(put, path="/api/settings/projects", request_body=ProjectMode, responses((status=200, body=ProjectMode)))]
pub(super) async fn set_mode(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<ProjectMode>,
) -> Result<Json<ProjectMode>, HttpError> {
    actor.recent(&app).await?;
    mode(&app, &actor.principal, input.enabled).await?;
    Ok(Json(input))
}
