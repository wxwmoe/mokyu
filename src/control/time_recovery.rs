use super::Identity;
use crate::{
    app::App,
    http::HttpError,
    model::time_recovery::{Plan, Request, Window},
};
use axum::{
    Json,
    extract::{Extension, Path, Query, State},
};
use serde::Deserialize;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Deserialize, utoipa::ToSchema)]
#[schema(as=RecoveryWindowChange)]
#[serde(deny_unknown_fields)]
pub(super) struct Change {
    seconds: i64,
}
#[derive(Deserialize, utoipa::ToSchema)]
#[schema(as=TimeRestoreConfirmation)]
#[serde(deny_unknown_fields)]
pub(super) struct Confirmation {
    digest: String,
}
#[derive(Deserialize, utoipa::IntoParams)]
pub(super) struct Cursor {
    after: Option<String>,
}
#[utoipa::path(get, operation_id="recovery_window",path="/api/v1/buckets/{bucket}/time-window",params(("bucket"=Uuid,Path)),responses((status=200,body=Window)))]
pub(super) async fn window(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<Json<Window>, HttpError> {
    Ok(Json(
        app.objects.time_window(&actor.principal, id, None).await?,
    ))
}
#[utoipa::path(put, operation_id="set_recovery_window",path="/api/v1/buckets/{bucket}/time-window",params(("bucket"=Uuid,Path)),request_body=Change,responses((status=200,body=Window)))]
pub(super) async fn set(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
    Json(c): Json<Change>,
) -> Result<Json<Window>, HttpError> {
    actor.recent(&app).await?;
    app.writable()?;
    Ok(Json(
        app.objects
            .time_window(&actor.principal, id, Some(c.seconds))
            .await?,
    ))
}
#[utoipa::path(post,operation_id="preview_time_restore",path="/api/v1/buckets/{bucket}/time-restore",params(("bucket"=Uuid,Path)),request_body=Request,responses((status=200,body=Plan)))]
pub(super) async fn preview(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
    Json(req): Json<Request>,
) -> Result<Json<Plan>, HttpError> {
    actor.recent(&app).await?;
    app.writable()?;
    Ok(Json(
        app.objects.time_preview(&actor.principal, id, &req).await?,
    ))
}
#[utoipa::path(get,operation_id="time_restore_plan",path="/api/v1/time-restores/{id}",params(("id"=Uuid,Path),Cursor),responses((status=200,body=Plan)))]
pub(super) async fn status(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
    Query(c): Query<Cursor>,
) -> Result<Json<Plan>, HttpError> {
    Ok(Json(
        app.objects
            .time_plan(&actor.principal, id, None, c.after.as_deref())
            .await?,
    ))
}
#[utoipa::path(post,operation_id="execute_time_restore",path="/api/v1/time-restores/{id}",params(("id"=Uuid,Path)),request_body=Confirmation,responses((status=200,body=Plan)))]
pub(super) async fn run(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
    Json(c): Json<Confirmation>,
) -> Result<Json<Plan>, HttpError> {
    actor.recent(&app).await?;
    app.writable()?;
    let p = app
        .objects
        .time_plan(&actor.principal, id, Some(&c.digest), None)
        .await?;
    app.wake_maintenance.notify_waiters();
    Ok(Json(p))
}
