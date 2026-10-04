use super::Identity;
use crate::{
    app::App,
    authorization::{Action, Permit},
    config,
    http::HttpError,
    multipart::WebUpload,
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
use sqlx::{FromRow, Postgres, QueryBuilder};
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
#[derive(Serialize, FromRow, ToSchema)]
pub(super) struct Transfer {
    id: Uuid,
    bucket_id: Uuid,
    bucket_name: String,
    object_key: String,
    state: String,
    source: String,
    user_id: Option<Uuid>,
    owner: String,
    file_name: Option<String>,
    expected_size: Option<String>,
    part_size: Option<String>,
    modified_at: Option<i64>,
    received_bytes: String,
    parts: i64,
    remote_state: String,
    created_at: DateTime<Utc>,
    touched_at: DateTime<Utc>,
    #[sqlx(default)]
    expires_at: Option<DateTime<Utc>>,
    #[sqlx(default)]
    can_resume: bool,
}
#[derive(Serialize, ToSchema)]
pub(super) struct TransferPage {
    uploads: Vec<Transfer>,
    next: Option<String>,
}
#[derive(Default, Deserialize, Serialize, IntoParams)]
pub(super) struct Filter {
    bucket: Option<Uuid>,
    state: Option<String>,
    source: Option<String>,
    own: Option<bool>,
    after: Option<String>,
    limit: Option<i64>,
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

const VIEW: &str = "SELECT u.id,u.bucket_id,b.name bucket_name,u.object_key,u.state,CASE WHEN w.upload_id IS NULL THEN 's3' ELSE 'web' END source,w.user_id,COALESCE(NULLIF(a.display_name,''),NULLIF(a.username,''),c.label,'S3 application') owner,w.file_name,w.expected_size::text,w.part_size::text,w.modified_at,CASE WHEN u.state='completed' THEN COALESCE(s.size,w.expected_size,0) ELSE COALESCE((SELECT sum(quota_size) FROM parts WHERE upload_id=u.id),0) END::text received_bytes,(SELECT count(*) FROM parts WHERE upload_id=u.id) parts,CASE WHEN u.state<>'completed' THEN 'pending' WHEN u.durable_at IS NOT NULL THEN 'stored' WHEN u.output_stream IS NULL THEN 'unavailable' WHEN EXISTS(SELECT 1 FROM extents e JOIN chunks ch ON ch.id=e.chunk_id WHERE e.stream_id=u.output_stream AND NOT EXISTS(SELECT 1 FROM chunk_locations l WHERE l.chunk_id=ch.id AND l.state='ready') AND NOT EXISTS(SELECT 1 FROM packs p WHERE p.id=ch.pack_id AND p.state='ready')) THEN 'pending' ELSE 'stored' END remote_state,u.created_at,u.touched_at FROM uploads u JOIN buckets b ON b.id=u.bucket_id LEFT JOIN web_uploads w ON w.upload_id=u.id LEFT JOIN web_users a ON a.id=w.user_id LEFT JOIN credentials c ON c.access_key=u.access_key LEFT JOIN streams s ON s.id=u.output_stream WHERE ";

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
    let mut tx = app.db.begin().await?;
    let admin = actor.principal.lock_identity(&mut tx).await?;
    let mut sql = QueryBuilder::<Postgres>::new(VIEW);
    if admin {
        sql.push("true");
    } else {
        if let Some(token) = actor.principal.token_id() {
            sql.push("EXISTS(SELECT 1 FROM token_bucket_access g WHERE g.token_id=")
                .push_bind(token);
        } else {
            sql.push("EXISTS(SELECT 1 FROM user_bucket_access g WHERE g.user_id=")
                .push_bind(actor.id);
        }
        sql.push(" AND g.bucket_id=u.bucket_id AND 'object.write'=ANY(g.actions) AND (w.user_id=")
            .push_bind(actor.id)
            .push(" OR 'bucket.settings'=ANY(g.actions)))");
    }
    if let Some(id) = id {
        sql.push(" AND u.id=").push_bind(id);
    } else {
        match q.state.as_deref() {
            Some("all") => {}
            Some(state) => {
                sql.push(" AND u.state=").push_bind(state);
            }
            None => {
                sql.push(" AND u.state IN ('active','completing')");
            }
        }
    }
    if let Some(bucket) = q.bucket {
        sql.push(" AND u.bucket_id=").push_bind(bucket);
    }
    if let Some(source) = &q.source {
        sql.push(if source == "web" {
            " AND w.upload_id IS NOT NULL"
        } else {
            " AND w.upload_id IS NULL"
        });
    }
    if q.own == Some(true) {
        sql.push(" AND w.user_id=").push_bind(actor.id);
    }
    if let Some(cursor) = cursor {
        sql.push(" AND (u.created_at,u.id)<(")
            .push_bind(cursor.created)
            .push(",")
            .push_bind(cursor.id)
            .push(")");
    }
    sql.push(" ORDER BY u.created_at DESC,u.id DESC LIMIT ")
        .push_bind(limit + 1);
    let mut rows: Vec<Transfer> = sql.build_query_as().fetch_all(&mut *tx).await?;
    let more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    let ready: Vec<Uuid> = rows
        .iter()
        .filter(|r| r.remote_state == "stored")
        .map(|r| r.id)
        .collect();
    if !ready.is_empty() {
        sqlx::query("UPDATE uploads SET durable_at=now() WHERE id=ANY($1) AND durable_at IS NULL")
            .bind(ready)
            .execute(&mut *tx)
            .await?;
    }
    let idle = config::seconds(&app.config.multipart.idle_timeout)?;
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
    tx.commit().await?;
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
        .require(&app.db, row.bucket_id, Action::Write)
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

#[utoipa::path(post,operation_id="create_web_upload",path="/api/buckets/{bucket}/uploads",params(("bucket"=Uuid,Path)),request_body=UploadInput,responses((status=201,body=Transfer)))]
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
        .require(&app.db, bucket, Action::Write)
        .await?;
    let name: String = sqlx::query_scalar("SELECT name FROM buckets WHERE id=$1")
        .bind(bucket)
        .fetch_one(&app.db)
        .await?;
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
#[utoipa::path(get,operation_id="list_uploads",path="/api/uploads",params(Filter),responses((status=200,body=TransferPage)))]
pub(super) async fn list(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Query(q): Query<Filter>,
) -> Result<Json<TransferPage>, HttpError> {
    Ok(Json(page(&app, &actor, q, None).await?))
}
#[utoipa::path(get,operation_id="get_upload",path="/api/uploads/{id}",params(("id"=Uuid,Path)),responses((status=200,body=Transfer)))]
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
#[derive(Serialize, ToSchema, FromRow)]
pub(super) struct ReceivedPart {
    number: i32,
    size: String,
    etag: String,
    sha256: Option<String>,
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
#[utoipa::path(get,operation_id="list_web_parts",path="/api/uploads/{id}/parts",params(("id"=Uuid,Path),PartsFilter),responses((status=200,body=PartsPage)))]
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
    let mut parts:Vec<ReceivedPart>=sqlx::query_as("SELECT p.part_number number,s.size::text,s.etag,s.checksums->>'sha256' sha256 FROM parts p JOIN streams s ON s.id=p.stream_id WHERE p.upload_id=$1 AND p.part_number>$2 ORDER BY p.part_number LIMIT 1001").bind(id).bind(after).fetch_all(&app.db).await?;
    let more = parts.len() > 1000;
    parts.truncate(1000);
    let next = more.then(|| parts.last().unwrap().number);
    Ok(Json(PartsPage { parts, next }))
}
#[utoipa::path(put,operation_id="put_web_part",path="/api/uploads/{id}/parts/{number}",params(("id"=Uuid,Path),("number"=i32,Path)),request_body(content_type="application/octet-stream",content=String),responses((status=200,body=PartReceipt)))]
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
#[utoipa::path(post,operation_id="complete_web_upload",path="/api/uploads/{id}/complete",params(("id"=Uuid,Path)),request_body=Completion,responses((status=200,body=Transfer)))]
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
#[utoipa::path(delete,operation_id="abort_upload",path="/api/uploads/{id}",params(("id"=Uuid,Path)),responses((status=204)))]
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
