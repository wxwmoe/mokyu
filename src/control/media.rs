use super::{Identity, catalog::MediaItem};
use crate::model::Id;
use crate::{
    app::{Active, App, Metadata, StoredStream},
    authorization::Action,
    http::{HttpError, ReadOptions, problem},
};
use anyhow::Result;
use axum::{
    Json,
    body::Body,
    extract::{Extension, Path, Query, State},
    http::{HeaderMap, Method, StatusCode},
    response::Response,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

const THUMB_LIMIT: usize = 640 * 1024;
const TEXT_LIMIT: usize = 64 * 1024;
#[derive(Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
pub(super) struct ObjectQuery {
    key: String,
    version: Id,
}
#[derive(Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
pub(super) struct ContentQuery {
    key: String,
    version: Id,
    #[serde(default)]
    preview: bool,
}
#[derive(Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct ObjectMetadata {
    pub content_type: Option<String>,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,
    pub content_language: Option<String>,
    pub expires: Option<String>,
    #[serde(default)]
    pub user: std::collections::BTreeMap<String, String>,
}
#[derive(Serialize, ToSchema)]
pub(super) struct ObjectDetail {
    item: MediaItem,
    etag: String,
    metadata: ObjectMetadata,
    preview: &'static str,
    thumbnail: bool,
    touched_at: DateTime<Utc>,
}
fn preview_type(metadata: &Metadata) -> &'static str {
    if metadata
        .content_encoding
        .as_deref()
        .is_some_and(|v| !v.is_empty() && v != "identity")
    {
        return "none";
    }
    match metadata
        .content_type
        .as_deref()
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "image/jpeg" | "image/png" | "image/webp" | "image/gif" | "image/avif" => "image",
        "video/mp4" | "video/webm" | "video/ogg" => "video",
        "audio/mpeg" | "audio/ogg" | "audio/mp4" | "audio/wav" | "audio/webm" | "audio/flac" => {
            "audio"
        }
        "text/plain" | "text/markdown" | "text/csv" | "application/json" | "application/xml"
        | "text/xml" => "text",
        _ => "none",
    }
}
fn thumbnail_type(metadata: &Metadata) -> bool {
    metadata
        .content_encoding
        .as_ref()
        .is_none_or(|v| v.is_empty() || v == "identity")
        && matches!(
            metadata
                .content_type
                .as_deref()
                .unwrap_or("")
                .split(';')
                .next()
                .unwrap_or("")
                .trim()
                .to_ascii_lowercase()
                .as_str(),
            "image/jpeg"
                | "image/png"
                | "image/webp"
                | "image/gif"
                | "image/avif"
                | "image/jxl"
                | "image/heic"
                | "image/heif"
        )
}
async fn current(
    app: &App,
    actor: &Identity,
    bucket: Uuid,
    q: &ObjectQuery,
    action: Action,
) -> Result<(StoredStream, Active)> {
    q.version.validate(app.config.database.backend)?;
    if q.key.is_empty() || q.key.len() > 1024 || q.key.contains('\0') {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    actor
        .principal
        .require(app.authorization.as_ref(), bucket, action)
        .await?;
    let (stream, pin) = app.current(bucket, &q.key).await?;
    if stream.id != q.version {
        return Err(s3s::s3_error!(PreconditionFailed).into());
    }
    Ok((stream, pin))
}
#[utoipa::path(get, operation_id="media_detail", path="/api/v1/buckets/{bucket}/object", params(("bucket"=Uuid,Path),ObjectQuery), responses((status=200,body=ObjectDetail)))]
pub(super) async fn detail(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Query(q): Query<ObjectQuery>,
) -> Result<Json<ObjectDetail>, HttpError> {
    let (stream, _pin) = current(&app, &actor, bucket, &q, Action::List).await?;
    let metadata: Metadata = serde_json::from_value(stream.metadata.clone())?;
    let preview = preview_type(&metadata);
    let thumbnail = thumbnail_type(&metadata) && stream.size <= 32 * 1024 * 1024;
    Ok(Json(ObjectDetail {
        etag: stream.etag.clone(),
        preview,
        thumbnail,
        touched_at: stream.touched_at,
        item: stream.into(),
        metadata: ObjectMetadata {
            content_type: metadata.content_type,
            cache_control: metadata.cache_control,
            content_disposition: metadata.content_disposition,
            content_encoding: metadata.content_encoding,
            content_language: metadata.content_language,
            expires: metadata.expires,
            user: metadata.user.unwrap_or_default().into_iter().collect(),
        },
    }))
}
#[utoipa::path(get, operation_id="media_content", path="/api/v1/buckets/{bucket}/object/content", params(("bucket"=Uuid,Path),ContentQuery), responses((status=200,content_type="application/octet-stream"),(status=206)))]
pub(super) async fn content(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    headers: HeaderMap,
    method: Method,
    Query(q): Query<ContentQuery>,
) -> Result<Response, HttpError> {
    actor
        .principal
        .require(app.authorization.as_ref(), bucket, Action::Read)
        .await?;
    crate::http::respond(
        app,
        bucket,
        &q.key,
        headers,
        method,
        ReadOptions::managed(q.preview, Some(q.version)),
    )
    .await
}
#[derive(Serialize, ToSchema)]
pub(super) struct TextPreview {
    text: String,
    truncated: bool,
}
#[utoipa::path(get, operation_id="media_text", path="/api/v1/buckets/{bucket}/object/text", params(("bucket"=Uuid,Path),ObjectQuery), responses((status=200,body=TextPreview)))]
pub(super) async fn text(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Query(q): Query<ObjectQuery>,
) -> Result<Json<TextPreview>, HttpError> {
    let (stream, pin) = current(&app, &actor, bucket, &q, Action::Read).await?;
    let metadata: Metadata = serde_json::from_value(stream.metadata.clone())?;
    if preview_type(&metadata) != "text" {
        return Err(problem(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "PreviewUnavailable",
        ));
    }
    let end = stream.size.min(TEXT_LIMIT as i64);
    let truncated = stream.size > end;
    let permits = app.admit(false).await?;
    let body = Body::new(s3s::Body::from(
        app.body(stream, pin, 0..end, permits, true, None).await?,
    ));
    let bytes = axum::body::to_bytes(body, TEXT_LIMIT).await?;
    Ok(Json(TextPreview {
        text: String::from_utf8_lossy(&bytes).into_owned(),
        truncated,
    }))
}
#[utoipa::path(get, operation_id="media_thumbnail", path="/api/v1/buckets/{bucket}/object/thumbnail", params(("bucket"=Uuid,Path),ObjectQuery), responses((status=200,content_type="image/png"),(status=415),(status=503)))]
pub(super) async fn thumbnail(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Query(q): Query<ObjectQuery>,
) -> Result<Response, HttpError> {
    let (stream, pin) = current(&app, &actor, bucket, &q, Action::Read).await?;
    let metadata: Metadata = serde_json::from_value(stream.metadata.clone())?;
    if !thumbnail_type(&metadata) || stream.size > 32 * 1024 * 1024 {
        return Err(problem(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "PreviewUnavailable",
        ));
    }
    let product = crate::processing::image::job::execute(
        app.clone(), stream, pin,
        crate::processing::image::Pipeline::parse_native(
            "mokyu/v1/frame,index=0/orient/resize,mode=contain,width=384,height=384,upscale=false/encode,format=png"
        )?,
        None, crate::processing::image::InputLimits::new(32 * 1024 * 1024), THUMB_LIMIT as u64,
    ).await.map_err(|error| {
        if error.downcast_ref::<s3s::S3Error>().is_some() {
            HttpError(error)
        } else if error.is::<std::io::Error>() || error.is::<tokio::time::error::Elapsed>() {
            problem(StatusCode::SERVICE_UNAVAILABLE, "PreviewBusy")
        } else {
            tracing::debug!(%error, "management preview unavailable");
            problem(StatusCode::UNSUPPORTED_MEDIA_TYPE, "PreviewUnavailable")
        }
    })?;
    current(&app, &actor, bucket, &q, Action::Read).await?;
    Ok(Response::builder()
        .header("content-type", "image/png")
        .header("content-length", product.output.bytes)
        .header("cache-control", "private, no-store")
        .header("x-content-type-options", "nosniff")
        .body(product.body().await?)?)
}
