use super::{Identity, maintenance::TaskStarted, replication};
use crate::{
    app::App,
    http::HttpError,
    model::redundancy::{Action, Command, Link, Preview, Settings},
};
use axum::{
    Json,
    extract::{Extension, Path, Query, State},
    http::{HeaderMap, StatusCode},
};
use serde::Serialize;
use std::sync::Arc;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Serialize, ToSchema)]
#[schema(as = RedundancyPage)]
pub(super) struct Page {
    links: Vec<Link>,
    next_after: Option<Uuid>,
}

#[utoipa::path(get,path="/api/v1/buckets/{bucket}/redundancy",operation_id="redundancy_list",params(("bucket"=Uuid,Path),replication::Filter),responses((status=200,body=Page)))]
pub(super) async fn list(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Query(filter): Query<replication::Filter>,
) -> Result<Json<Page>, HttpError> {
    let (links, next_after) = app
        .redundancy
        .list(
            &actor.principal,
            bucket,
            filter.after,
            replication::limit(filter.limit)?,
        )
        .await?;
    Ok(Json(Page { links, next_after }))
}

#[utoipa::path(post,path="/api/v1/buckets/{bucket}/redundancy/preview",operation_id="redundancy_preview",params(("bucket"=Uuid,Path)),request_body=Settings,responses((status=200,body=Preview)))]
pub(super) async fn preview(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Json(input): Json<Settings>,
) -> Result<Json<Preview>, HttpError> {
    Ok(Json(
        app.redundancy
            .preview(&actor.principal, bucket, &input)
            .await?,
    ))
}

#[utoipa::path(post,path="/api/v1/buckets/{bucket}/redundancy",operation_id="redundancy_create",params(("bucket"=Uuid,Path),("Idempotency-Key"=Option<Uuid>,Header)),request_body=Settings,responses((status=202,body=TaskStarted)))]
pub(super) async fn create(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    headers: HeaderMap,
    Json(input): Json<Settings>,
) -> Result<(StatusCode, Json<TaskStarted>), HttpError> {
    let id = replication::request_id(&headers)?;
    let options = input.replication(bucket)?;
    let connections = replication::connections(&app, &actor.principal, id, &options).await?;
    let task_id = crate::model::REQUEST_ID
        .scope(
            id,
            app.redundancy
                .create(&actor.principal, bucket, &input, &connections),
        )
        .await?;
    app.wake_tasks.notify_one();
    Ok((StatusCode::ACCEPTED, Json(TaskStarted { task_id })))
}

#[utoipa::path(get,path="/api/v1/buckets/{bucket}/redundancy/{id}",operation_id="redundancy_get",params(("bucket"=Uuid,Path),("id"=Uuid,Path)),responses((status=200,body=Link)))]
pub(super) async fn get(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path((bucket, id)): Path<(Uuid, Uuid)>,
) -> Result<Json<Link>, HttpError> {
    Ok(Json(
        app.redundancy.get(&actor.principal, bucket, id).await?,
    ))
}

#[utoipa::path(post,path="/api/v1/buckets/{bucket}/redundancy/{id}/command",operation_id="redundancy_command",params(("bucket"=Uuid,Path),("id"=Uuid,Path)),request_body=Command,responses((status=202,body=TaskStarted)))]
pub(super) async fn command(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path((bucket, id)): Path<(Uuid, Uuid)>,
    Json(input): Json<Command>,
) -> Result<(StatusCode, Json<TaskStarted>), HttpError> {
    input.validate()?;
    let connections = if matches!(
        input.action,
        Action::Resume | Action::Retry | Action::Reconcile
    ) {
        let link = app.redundancy.get(&actor.principal, bucket, id).await?;
        let options = link.settings.replication(link.source_bucket)?;
        replication::connections(&app, &actor.principal, id, &options).await?
    } else {
        Vec::new()
    };
    app.redundancy
        .command(&actor.principal, bucket, id, &input, &connections)
        .await?;
    app.wake_tasks.notify_one();
    Ok((StatusCode::ACCEPTED, Json(TaskStarted { task_id: id })))
}
