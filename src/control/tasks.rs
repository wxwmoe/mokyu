use super::{auth::Identity, contract, maintenance};
use crate::model::Id;
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
    let guard = app.authorization.admin(&actor.principal).await?;
    let user_id = actor.id;
    let result = app.integrity_start(input).await;
    tracing::info!(%user_id,success=result.is_ok(),task_id=?result.as_ref().ok().and_then(|r|r.get("task_id")),"management integrity check");
    guard.finish().await?;
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
    if !(1..=200).contains(&limit) {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let rows = app
        .packing
        .page(
            Id::parse(
                q.after.as_deref().unwrap_or("0"),
                app.config.database.backend,
            )?,
            limit + 1,
        )
        .await?;
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
    params(("id" = String, Path)),
    responses((status = 200, body = Value), (status = 403, body = contract::ErrorBody))
)]
pub(super) async fn pack_detail(
    State(app): State<Arc<App>>,
    Extension(_actor): Extension<Identity>,
    Path(id): Path<String>,
) -> Result<Json<Value>, HttpError> {
    let id = Id::parse(&id, app.config.database.backend)?;
    let (pack, members) = app.packing.detail(id).await?;
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
    after: Option<String>,
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
    stream_id: Option<Id>,
    bucket_id: Option<Uuid>,
    object_key: Option<String>,
    detail: Value,
    created_at: DateTime<Utc>,
}
#[derive(Deserialize, Serialize, utoipa::ToSchema)]
pub(crate) struct IssuePage {
    #[serde(rename = "items", alias = "issues")]
    issues: Vec<IntegrityIssue>,
    #[serde(rename = "next_cursor", alias = "next_after")]
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
            Id::parse(
                q.after.as_deref().unwrap_or("0"),
                app.config.database.backend,
            )?,
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
    version: Id,
}
#[derive(Deserialize, Serialize, utoipa::ToSchema)]
pub(crate) struct IssueObjects {
    #[serde(rename = "items", alias = "objects")]
    objects: Vec<IssueObject>,
    #[serde(rename = "next_cursor", alias = "next")]
    next: Option<(Uuid, String)>,
}
#[utoipa::path(get,path="/api/v1/tasks/{id}/issues/{issue}/objects",params(("id"=Uuid,Path),("issue"=String,Path),IssueObjectsQuery),responses((status=200,body=IssueObjects)))]
pub(super) async fn integrity_objects(
    State(app): State<Arc<App>>,
    Extension(_actor): Extension<Identity>,
    Path((id, issue)): Path<(Uuid, String)>,
    Query(q): Query<IssueObjectsQuery>,
) -> Result<Json<IssueObjects>, HttpError> {
    let after = q
        .after
        .map(|s| serde_json::from_str::<(Uuid, String)>(&s))
        .transpose()
        .map_err(|_| s3s::s3_error!(InvalidArgument))?;
    Ok(Json(serde_json::from_value(
        app.integrity_objects(id, Id::parse(&issue, app.config.database.backend)?, after)
            .await?,
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
    let task = app
        .integrity_store
        .report_task(id)
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
    let upper = app.integrity_store.report_upper(id).await?;
    let body = async_stream::try_stream! {
        yield bytes::Bytes::from(format!("{}\n",json!({"type":"task","task":task})));
        let mut after = Id::default();
        let mut count = 0u64;
        loop {
            let rows = app.integrity_store.report_page(id,after,upper).await.map_err(std::io::Error::other)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integrity_pages_keep_public_names() {
        let issues: IssuePage = serde_json::from_value(json!({
            "issues": [], "next_after": null, "groups": {}
        }))
        .unwrap();
        assert_eq!(
            serde_json::to_value(issues).unwrap(),
            json!({
                "items": [], "next_cursor": null, "groups": {}
            })
        );
        let objects: IssueObjects = serde_json::from_value(json!({
            "objects": [], "next": null
        }))
        .unwrap();
        assert_eq!(
            serde_json::to_value(objects).unwrap(),
            json!({
                "items": [], "next_cursor": null
            })
        );
        let schema = serde_json::to_value(crate::control::schema()).unwrap();
        for name in ["IssuePage", "IssueObjects"] {
            let properties = &schema["components"]["schemas"][name]["properties"];
            assert!(properties.get("items").is_some());
            assert!(properties.get("next_cursor").is_some());
            assert!(properties.get("issues").is_none());
            assert!(properties.get("objects").is_none());
        }
    }
}
