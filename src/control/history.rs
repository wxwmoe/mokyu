use super::Identity;
use crate::{
    app::App,
    authorization::{Action, Permit},
    http::{HttpError, ReadOptions},
    model::history::{Page, RetentionFilter, RetentionPreview, Selection, Versioning},
};
use axum::{
    Json,
    extract::{Extension, Path, Query, State},
    http::{HeaderMap, Method, StatusCode},
    response::Response,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

#[derive(Deserialize, IntoParams, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as=ObjectVersionSelection)]
pub(super) struct Entry {
    key: String,
    version_id: String,
}
#[derive(Serialize, ToSchema)]
pub(super) struct Restored {
    version_id: Option<String>,
}

#[utoipa::path(get,operation_id="bucket_versioning",path="/api/v1/buckets/{bucket}/versioning",params(("bucket"=Uuid,Path)),responses((status=200,body=Versioning)))]
pub(super) async fn get(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
) -> Result<Json<Versioning>, HttpError> {
    Ok(Json(
        app.objects
            .versioning(&actor.principal, bucket, None)
            .await?,
    ))
}
#[utoipa::path(put,operation_id="set_bucket_versioning",path="/api/v1/buckets/{bucket}/versioning",params(("bucket"=Uuid,Path)),request_body=Versioning,responses((status=200,body=Versioning)))]
pub(super) async fn set(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Json(mode): Json<Versioning>,
) -> Result<Json<Versioning>, HttpError> {
    actor.recent(&app).await?;
    app.writable()?;
    Ok(Json(
        app.objects
            .versioning(&actor.principal, bucket, Some(mode))
            .await?,
    ))
}
#[utoipa::path(get,operation_id="list_versions",path="/api/v1/buckets/{bucket}/versions",params(("bucket"=Uuid,Path),Selection),responses((status=200,body=Page)))]
pub(super) async fn list(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Query(filter): Query<Selection>,
) -> Result<Json<Page>, HttpError> {
    let permit = Permit::for_action(actor.principal, bucket, Action::ListVersions);
    Ok(Json(app.objects.versions(&permit, bucket, &filter).await?))
}
#[utoipa::path(get,operation_id="preview_version_retention",path="/api/v1/buckets/{bucket}/versions/retention-preview",params(("bucket"=Uuid,Path),RetentionFilter),responses((status=200,body=RetentionPreview)))]
pub(super) async fn retention_preview(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Query(filter): Query<RetentionFilter>,
) -> Result<Json<RetentionPreview>, HttpError> {
    Ok(Json(
        app.objects
            .retention_preview(&actor.principal, bucket, filter)
            .await?,
    ))
}
#[utoipa::path(get,operation_id="version_content",path="/api/v1/buckets/{bucket}/versions/content",params(("bucket"=Uuid,Path),Entry),responses((status=200,content_type="application/octet-stream"),(status=206)))]
pub(super) async fn content(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Query(q): Query<Entry>,
    headers: HeaderMap,
    method: Method,
) -> Result<Response, HttpError> {
    let permit = Permit::for_action(actor.principal, bucket, Action::ReadVersion);
    let (stream, pin, _) = app
        .history_read(bucket, &q.key, Some(&q.version_id), &permit)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchVersion))?;
    let mut options = ReadOptions::managed(true, Some(stream.id));
    options.selected = Some((stream, pin));
    crate::http::respond(app, bucket, &q.key, headers, method, options).await
}
#[utoipa::path(post,operation_id="restore_version",path="/api/v1/buckets/{bucket}/versions/restore",params(("bucket"=Uuid,Path)),request_body=Entry,responses((status=200,body=Restored)))]
pub(super) async fn restore(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Json(q): Json<Entry>,
) -> Result<Json<Restored>, HttpError> {
    actor.recent(&app).await?;
    Ok(Json(Restored {
        version_id: app
            .restore_version(&actor.principal, bucket, &q.key, &q.version_id)
            .await?,
    }))
}
#[utoipa::path(delete,operation_id="delete_version",path="/api/v1/buckets/{bucket}/versions",params(("bucket"=Uuid,Path),Entry),responses((status=204)))]
pub(super) async fn remove(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Query(q): Query<Entry>,
) -> Result<StatusCode, HttpError> {
    actor.recent(&app).await?;
    let p = Permit::for_action(actor.principal, bucket, Action::DeleteVersion);
    app.history_delete(&p, bucket, &q.key, Some(&q.version_id), None)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
