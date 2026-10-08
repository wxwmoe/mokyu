use super::{auth::Identity, contract, insights, maintenance};
use crate::{app::App, http::HttpError};
use anyhow::Result;
use axum::{
    Json,
    body::Body,
    extract::{Extension, Path, Query, State},
    http::HeaderValue,
    response::{IntoResponse, Response},
};
use chrono::{DateTime, Utc};
use s3s::s3_error;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use uuid::Uuid;

#[utoipa::path(post,path="/api/v1/integrity",request_body=crate::worker::integrity::Request,responses((status=200,body=maintenance::TaskStarted)))]
pub(super) async fn start_integrity(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<crate::worker::integrity::Request>,
) -> Result<Json<maintenance::TaskStarted>, HttpError> {
    let mut guard = app.db.begin().await?;
    actor.principal.lock_admin(&mut guard).await?;
    let user_id = actor.id;
    let result = app.integrity_start(input).await;
    tracing::info!(%user_id,success=result.is_ok(),task_id=?result.as_ref().ok().and_then(|r|r.get("task_id")),"management integrity check");
    guard.commit().await?;
    Ok(Json(serde_json::from_value(result?)?))
}
#[utoipa::path(
    get,
    operation_id = "packs_list",
    path = "/api/v1/packs",
    params(IssueQuery),
    responses((status = 200, body = Value), (status = 403, body = contract::ErrorBody))
)]
pub(super) async fn packs(
    State(app): State<Arc<App>>,
    Extension(_actor): Extension<Identity>,
    Query(q): Query<IssueQuery>,
) -> Result<Json<Value>, HttpError> {
    let limit = q.limit.unwrap_or(100);
    if !(1..=200).contains(&limit) || q.after.is_some_and(|id| id < 0) {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let rows:Vec<Value>=sqlx::query_scalar("SELECT to_jsonb(p)||jsonb_build_object('id',p.id::text) FROM packs p WHERE id>$1 ORDER BY id LIMIT $2")
        .bind(q.after.unwrap_or(0)).bind(limit+1).fetch_all(&app.db).await?;
    let next = if rows.len() > limit as usize {
        rows[limit as usize - 1]["id"].clone()
    } else {
        Value::Null
    };
    Ok(Json(
        json!({"status":app.pack_status().await?,"packs":rows.into_iter().take(limit as usize).collect::<Vec<_>>(),"next_after":next}),
    ))
}
#[utoipa::path(
    get,
    operation_id = "pack_detail",
    path = "/api/v1/packs/{id}",
    params(("id" = i64, Path)),
    responses((status = 200, body = Value), (status = 403, body = contract::ErrorBody))
)]
pub(super) async fn pack_detail(
    State(app): State<Arc<App>>,
    Extension(_actor): Extension<Identity>,
    Path(id): Path<i64>,
) -> Result<Json<Value>, HttpError> {
    let pack: Value = sqlx::query_scalar(
        "SELECT to_jsonb(p)||jsonb_build_object('id',p.id::text) FROM packs p WHERE id=$1",
    )
    .bind(id)
    .fetch_optional(&app.db)
    .await?
    .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    let members:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('ordinal',m.ordinal,'chunk_id',m.chunk_id::text,'offset',m.offset_bytes,'raw_size',c.raw_size,'current',c.pack_id=m.pack_id) FROM pack_members m JOIN chunks c ON c.id=m.chunk_id WHERE m.pack_id=$1 ORDER BY ordinal LIMIT 4096").bind(id).fetch_all(&app.db).await?;
    Ok(Json(json!({"pack":pack,"members":members})))
}
#[utoipa::path(
    post,
    operation_id = "cache_flush",
    path = "/api/v1/cache/flush",
    responses((status = 200, body = Value), (status = 403, body = contract::ErrorBody))
)]
pub(super) async fn cache_flush(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<Value>, HttpError> {
    let user_id = actor.id;
    let result = app.cache_flush_start().await;
    tracing::info!(%user_id,success=result.is_ok(),"management upload cache flush");
    Ok(Json(result?))
}
#[derive(Deserialize, utoipa::IntoParams)]
pub(crate) struct IssueQuery {
    after: Option<i64>,
    limit: Option<i64>,
    code: Option<String>,
}
#[derive(Deserialize, Serialize, utoipa::ToSchema)]
struct IntegrityIssue {
    id: String,
    task_id: Uuid,
    code: String,
    chunk_id: Option<String>,
    location_id: Option<Uuid>,
    stream_id: Option<Uuid>,
    bucket_id: Option<Uuid>,
    object_key: Option<String>,
    detail: Value,
    created_at: DateTime<Utc>,
}
#[derive(Deserialize, Serialize, utoipa::ToSchema)]
pub(crate) struct IssuePage {
    #[serde(rename = "items")]
    issues: Vec<IntegrityIssue>,
    #[serde(rename = "next_cursor")]
    next_after: Option<String>,
    groups: std::collections::BTreeMap<String, String>,
}
#[utoipa::path(get,path="/api/v1/tasks/{id}/issues",params(("id"=Uuid,Path),IssueQuery),responses((status=200,body=IssuePage)))]
pub(super) async fn integrity_issues(
    State(app): State<Arc<App>>,
    Extension(_actor): Extension<Identity>,
    Path(id): Path<Uuid>,
    Query(q): Query<IssueQuery>,
) -> Result<Json<IssuePage>, HttpError> {
    let value = app
        .integrity_issues_filtered(
            id,
            q.after.unwrap_or(0),
            q.limit.unwrap_or(100),
            q.code.as_deref(),
        )
        .await?;
    Ok(Json(serde_json::from_value(value)?))
}
#[derive(Deserialize, utoipa::IntoParams)]
pub(crate) struct IssueObjectsQuery {
    after: Option<String>,
}
#[derive(Deserialize, Serialize, utoipa::ToSchema)]
struct IssueObject {
    bucket_id: Uuid,
    bucket: String,
    key: String,
    version: Uuid,
}
#[derive(Deserialize, Serialize, utoipa::ToSchema)]
pub(crate) struct IssueObjects {
    #[serde(rename = "items")]
    objects: Vec<IssueObject>,
    #[serde(rename = "next_cursor")]
    next: Option<(Uuid, String)>,
}
#[utoipa::path(get,path="/api/v1/tasks/{id}/issues/{issue}/objects",params(("id"=Uuid,Path),("issue"=i64,Path),IssueObjectsQuery),responses((status=200,body=IssueObjects)))]
pub(super) async fn integrity_objects(
    State(app): State<Arc<App>>,
    Extension(_actor): Extension<Identity>,
    Path((id, issue)): Path<(Uuid, i64)>,
    Query(q): Query<IssueObjectsQuery>,
) -> Result<Json<IssueObjects>, HttpError> {
    let after = q
        .after
        .map(|s| serde_json::from_str::<(Uuid, String)>(&s))
        .transpose()
        .map_err(|_| s3s::s3_error!(InvalidArgument))?;
    Ok(Json(serde_json::from_value(
        app.integrity_objects(id, issue, after).await?,
    )?))
}
#[utoipa::path(
    get,
    operation_id = "integrity_report",
    path = "/api/v1/tasks/{id}/report",
    params(("id" = Uuid, Path)),
    responses((status = 200, description = "JSON Lines report", content_type = "application/x-ndjson"), (status = 403, body = contract::ErrorBody))
)]
pub(super) async fn integrity_report(
    State(app): State<Arc<App>>,
    Extension(_actor): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<Response, HttpError> {
    let task: Value =
        sqlx::query_scalar("SELECT to_jsonb(t) FROM tasks t WHERE id=$1 AND task_type='integrity'")
            .bind(id)
            .fetch_optional(&app.db)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    if task["state"] != "completed" {
        return Err(s3s::s3_error!(
            OperationAborted,
            "finish the inspection before exporting its report"
        )
        .into());
    }
    // Fix the boundary so report size stays finite; pages do not retain a database connection.
    let upper: i64 =
        sqlx::query_scalar("SELECT COALESCE(max(id),0) FROM integrity_issues WHERE task_id=$1")
            .bind(id)
            .fetch_one(&app.db)
            .await?;
    let body = async_stream::try_stream! {
        yield bytes::Bytes::from(format!("{}\n",json!({"type":"task","task":task})));
        let mut after = 0;
        let mut count = 0u64;
        loop {
            let rows: Vec<Value> = sqlx::query_scalar("SELECT to_jsonb(i)||jsonb_build_object('id',i.id::text,'chunk_id',i.chunk_id::text) FROM integrity_issues i WHERE task_id=$1 AND id>$2 AND id<=$3 ORDER BY id LIMIT 100").bind(id).bind(after).bind(upper).fetch_all(&app.db).await.map_err(std::io::Error::other)?;
            if rows.is_empty() { break; }
            for row in rows {
                after = row["id"].as_str().and_then(|v|v.parse().ok()).ok_or_else(||std::io::Error::other("invalid report ID"))?;
                count += 1;
                yield bytes::Bytes::from(format!("{}\n",json!({"type":"issue","issue":row})));
            }
        }
        if count != task["detail"]["issues"].as_u64().unwrap_or(0) { Err(std::io::Error::other("report expired during export"))?; }
        yield bytes::Bytes::from(format!("{}\n",json!({"type":"end","issues":count})));
    };
    let body: std::pin::Pin<
        Box<dyn futures_util::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send>,
    > = Box::pin(body);
    let mut response = Body::from_stream(body).into_response();
    response.headers_mut().insert(
        "content-type",
        HeaderValue::from_static("application/x-ndjson"),
    );
    response.headers_mut().insert(
        "content-disposition",
        HeaderValue::from_str(&format!("attachment; filename=\"integrity-{id}.jsonl\""))?,
    );
    Ok(response)
}
