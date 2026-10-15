use super::Identity;
pub(crate) use crate::model::multipart::{ReceivedPart, Transfer, TransferFilter as Filter};
use crate::{
    app::App,
    authorization::{Action, Permit},
    config,
    http::HttpError,
    upload::multipart::WebUpload,
};
use anyhow::{Context, Result};
use axum::{
    Json,
    body::Body,
    extract::{Extension, Path, Query, State},
    http::{HeaderMap, StatusCode},
};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use chrono::{DateTime, Utc};
use s3s::{S3Request, dto::*};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct UploadInput {
    client_id: Uuid,
    key: String,
    file_name: String,
    size: String,
    modified_at: Option<i64>,
    #[serde(default)]
    content_type: String,
    #[serde(default)]
    public_read: bool,
    #[serde(default)]
    overwrite: bool,
}

#[derive(Serialize, ToSchema)]
pub(super) struct TransferPage {
    #[serde(rename = "items")]
    uploads: Vec<Transfer>,
    #[serde(rename = "next_cursor")]
    next: Option<String>,
}

#[derive(Deserialize, Serialize)]
struct Cursor {
    created: DateTime<Utc>,
    id: Uuid,
    bucket: Option<Uuid>,
    state: Option<String>,
    source: Option<String>,
    own: Option<bool>,
}

async fn page(app: &App, actor: &Identity, q: Filter, id: Option<Uuid>) -> Result<TransferPage> {
    let limit = q.limit.unwrap_or(50);
    if !(1..=100).contains(&limit)
        || q.state
            .as_deref()
            .is_some_and(|v| !["active", "completing", "completed", "aborted", "all"].contains(&v))
        || q.source
            .as_deref()
            .is_some_and(|v| !["web", "s3"].contains(&v))
    {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let cursor = q
        .after
        .as_ref()
        .map(|s| -> Result<Cursor> { Ok(serde_json::from_slice(&URL_SAFE_NO_PAD.decode(s)?)?) })
        .transpose()
        .map_err(|_| s3s::s3_error!(InvalidArgument))?;
    if cursor.as_ref().is_some_and(|c| {
        c.bucket != q.bucket || c.state != q.state || c.source != q.source || c.own != q.own
    }) {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let (mut rows, more) = app
        .multipart
        .transfers(
            &actor.principal,
            actor.id,
            &q,
            cursor.map(|c| (c.created, c.id)),
            id,
            limit,
        )
        .await?;
    let idle = config::seconds(&app.config.uploads.idle_timeout)?;
    for row in &mut rows {
        row.expires_at = matches!(row.state.as_str(), "active" | "completing")
            .then(|| row.touched_at + chrono::Duration::seconds(idle as i64));
        row.can_resume = row.state == "active" && row.user_id == Some(actor.id);
    }
    let next = if more {
        let last = rows.last().unwrap();
        Some(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&Cursor {
            created: last.created_at,
            id: last.id,
            bucket: q.bucket,
            state: q.state,
            source: q.source,
            own: q.own,
        })?))
    } else {
        None
    };
    Ok(TransferPage {
        uploads: rows,
        next,
    })
}
async fn detail(app: &App, actor: &Identity, id: Uuid) -> Result<Transfer> {
    page(app, actor, Filter::default(), Some(id))
        .await?
        .uploads
        .pop()
        .ok_or_else(|| s3s::s3_error!(NoSuchUpload).into())
}
async fn owned(app: &App, actor: &Identity, id: Uuid) -> Result<Transfer> {
    let row = detail(app, actor, id).await?;
    if row.user_id != Some(actor.id) {
        return Err(s3s::s3_error!(AccessDenied).into());
    }
    actor
        .principal
        .require(app.authorization.as_ref(), row.bucket_id, Action::Write)
        .await?;
    Ok(row)
}
fn request<T>(input: T) -> S3Request<T> {
    S3Request {
        input,
        method: hyper::Method::POST,
        uri: hyper::Uri::default(),
        headers: HeaderMap::new(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

#[utoipa::path(post,operation_id="create_web_upload",path="/api/v1/buckets/{bucket}/uploads",params(("bucket"=Uuid,Path)),request_body=UploadInput,responses((status=201,body=Transfer)))]
pub(super) async fn create(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Json(input): Json<UploadInput>,
) -> Result<(StatusCode, Json<Transfer>), HttpError> {
    let size = input
        .size
        .parse::<i64>()
        .ok()
        .filter(|s| (0..=53687091200000).contains(s))
        .ok_or_else(|| s3s::s3_error!(InvalidArgument))?;
    if input.file_name.is_empty()
        || input.file_name.len() > 1024
        || input.modified_at.is_some_and(|n| n < 0)
        || input.content_type.len() > 255
        || input.content_type.chars().any(char::is_control)
    {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    actor
        .principal
        .require(app.authorization.as_ref(), bucket, Action::Write)
        .await?;
    let name: String = app.multipart.bucket_name(bucket).await?;
    let part_size = (((size + 9999) / 10000 + 1048575) / 1048576).max(16) * 1048576;
    let spec = WebUpload {
        user: actor.id,
        client_id: input.client_id,
        request_hash: blake3::hash(&serde_json::to_vec(&(bucket, &input))?)
            .as_bytes()
            .to_vec(),
        file_name: input.file_name,
        size,
        part_size,
        modified_at: input.modified_at,
        overwrite: input.overwrite,
    };
    let response = app
        .create_multipart(
            request(CreateMultipartUploadInput {
                bucket: name,
                key: input.key,
                content_type: Some(if input.content_type.is_empty() {
                    "application/octet-stream".into()
                } else {
                    input.content_type
                }),
                acl: Some(ObjectCannedACL::from(if input.public_read {
                    "public-read".to_owned()
                } else {
                    "private".to_owned()
                })),
                checksum_algorithm: Some(ChecksumAlgorithm::from("SHA256".to_owned())),
                checksum_type: Some(ChecksumType::from("COMPOSITE".to_owned())),
                ..Default::default()
            }),
            Some(&actor.principal),
            Some(spec),
        )
        .await?;
    let id = Uuid::parse_str(
        response
            .output
            .upload_id
            .as_deref()
            .context("missing upload id")?,
    )?;
    Ok((StatusCode::CREATED, Json(detail(&app, &actor, id).await?)))
}
#[utoipa::path(get,operation_id="list_uploads",path="/api/v1/uploads",params(Filter),responses((status=200,body=TransferPage)))]
pub(super) async fn list(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Query(q): Query<Filter>,
) -> Result<Json<TransferPage>, HttpError> {
    Ok(Json(page(&app, &actor, q, None).await?))
}
#[utoipa::path(get,operation_id="get_upload",path="/api/v1/uploads/{id}",params(("id"=Uuid,Path)),responses((status=200,body=Transfer)))]
pub(super) async fn get(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<Json<Transfer>, HttpError> {
    Ok(Json(detail(&app, &actor, id).await?))
}

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct PartReceipt {
    number: i32,
    etag: String,
    sha256: String,
}

#[derive(Serialize, ToSchema)]
pub(super) struct PartsPage {
    parts: Vec<ReceivedPart>,
    next: Option<i32>,
}
#[derive(Deserialize, IntoParams)]
pub(super) struct PartsFilter {
    after: Option<i32>,
}
#[utoipa::path(get,operation_id="list_web_parts",path="/api/v1/uploads/{id}/parts",params(("id"=Uuid,Path),PartsFilter),responses((status=200,body=PartsPage)))]
pub(super) async fn parts(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
    Query(q): Query<PartsFilter>,
) -> Result<Json<PartsPage>, HttpError> {
    owned(&app, &actor, id).await?;
    let after = q.after.unwrap_or(0);
    if !(0..=10000).contains(&after) {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let mut parts: Vec<ReceivedPart> = app.multipart.web_parts(id, after).await?;
    let more = parts.len() > 1000;
    parts.truncate(1000);
    let next = more.then(|| parts.last().unwrap().number);
    Ok(Json(PartsPage { parts, next }))
}
#[utoipa::path(put,operation_id="put_web_part",path="/api/v1/uploads/{id}/parts/{number}",params(("id"=Uuid,Path),("number"=i32,Path)),request_body(content_type="application/octet-stream",content=String),responses((status=200,body=PartReceipt)))]
pub(super) async fn put_part(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path((id, number)): Path<(Uuid, i32)>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<PartReceipt>, HttpError> {
    let row = owned(&app, &actor, id).await?;
    let size = row
        .expected_size
        .as_ref()
        .context("missing file size")?
        .parse::<i64>()?;
    let step = row
        .part_size
        .as_ref()
        .context("missing part size")?
        .parse::<i64>()?;
    if number < 1 || i64::from(number) > ((size + step - 1) / step).max(1) {
        return Err(s3s::s3_error!(InvalidPart).into());
    }
    let length = step.min(size - (i64::from(number) - 1) * step);
    let hash = headers
        .get("x-content-sha256")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| s3s::s3_error!(InvalidArgument))?;
    if STANDARD.decode(hash).ok().is_none_or(|h| h.len() != 32) {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let body = std::sync::Mutex::new(Box::pin(body.into_data_stream()));
    let stream = futures_util::stream::poll_fn(move |cx| {
        futures_util::Stream::poll_next(body.lock().unwrap().as_mut(), cx)
    });
    let mut req = request(UploadPartInput {
        bucket: row.bucket_name,
        key: row.object_key,
        upload_id: id.to_string(),
        part_number: number,
        body: Some(StreamingBlob::wrap(stream)),
        content_length: Some(length),
        checksum_algorithm: Some(ChecksumAlgorithm::from("SHA256".to_owned())),
        ..Default::default()
    });
    req.headers
        .insert("x-amz-checksum-sha256", headers["x-content-sha256"].clone());
    let response = app.upload_part(req, Some(&actor.principal)).await?;
    Ok(Json(PartReceipt {
        number,
        etag: response
            .output
            .e_tag
            .and_then(|e| e.as_strong().map(str::to_owned))
            .context("missing part etag")?,
        sha256: hash.to_owned(),
    }))
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct Completion {
    parts: Vec<PartReceipt>,
}
#[utoipa::path(post,operation_id="complete_web_upload",path="/api/v1/uploads/{id}/complete",params(("id"=Uuid,Path)),request_body=Completion,responses((status=200,body=Transfer)))]
pub(super) async fn complete(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
    Json(input): Json<Completion>,
) -> Result<Json<Transfer>, HttpError> {
    let row = owned(&app, &actor, id).await?;
    let size = row
        .expected_size
        .as_ref()
        .context("missing size")?
        .parse::<i64>()?;
    let part_size = row
        .part_size
        .as_ref()
        .context("missing part size")?
        .parse::<i64>()?;
    if input.parts.len() != ((size + part_size - 1) / part_size).max(1) as usize
        || input.parts.iter().enumerate().any(|(i, p)| {
            p.number != i as i32 + 1
                || p.etag.len() != 32
                || STANDARD
                    .decode(&p.sha256)
                    .ok()
                    .is_none_or(|h| h.len() != 32)
        })
    {
        return Err(s3s::s3_error!(InvalidPart).into());
    }
    let parts = input
        .parts
        .into_iter()
        .map(|p| CompletedPart {
            part_number: Some(p.number),
            e_tag: Some(ETag::Strong(p.etag)),
            checksum_sha256: Some(p.sha256),
            ..Default::default()
        })
        .collect();
    let response = app
        .complete_multipart(
            request(CompleteMultipartUploadInput {
                bucket: row.bucket_name,
                key: row.object_key,
                upload_id: id.to_string(),
                multipart_upload: Some(CompletedMultipartUpload { parts: Some(parts) }),
                mpu_object_size: Some(size),
                checksum_type: Some(ChecksumType::from("COMPOSITE".to_owned())),
                ..Default::default()
            }),
            Some(&actor.principal),
        )
        .await?;
    if let Some(future) = response.output.future {
        future.await?;
    }
    Ok(Json(detail(&app, &actor, id).await?))
}
#[utoipa::path(delete,operation_id="abort_upload",path="/api/v1/uploads/{id}",params(("id"=Uuid,Path)),responses((status=204)))]
pub(super) async fn abort(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, HttpError> {
    let row = detail(&app, &actor, id).await?;
    let mut permit = Permit::for_action(actor.principal.clone(), row.bucket_id, Action::Write);
    if row.user_id != Some(actor.id) {
        permit.add(row.bucket_id, Action::Settings);
    }
    let lock = app.operation_lock(id);
    let _guard = lock.lock().await;
    let coord = app.coord.lock().await;
    app.abort_upload_locked(id, &coord, &permit).await?;
    Ok(StatusCode::NO_CONTENT)
}
