use super::Identity;
use crate::{app::App, authorization::Principal, http::HttpError};
use anyhow::Result;
use axum::{
    Json,
    extract::{Extension, Path, Query, State},
    http::HeaderValue,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use serde_json::Value;
use std::sync::Arc;
use utoipa::ToSchema;
use uuid::Uuid;

pub(crate) use crate::model::audit::{Event, Filter, active, current, scope};

pub(crate) async fn begin(
    store: &dyn crate::model::AuditStore,
    principal: &Principal,
    action: &str,
    request_id: Option<&str>,
) -> Result<i64> {
    store.begin(principal, action, request_id).await
}

pub(crate) async fn finish(
    store: &dyn crate::model::AuditStore,
    id: i64,
    status: u16,
    partial: bool,
) {
    if let Err(error) = store.finish(id, status, partial).await {
        tracing::warn!(audit_id=id,%error,"audit result could not be saved; durable intent remains");
    }
}

#[derive(Serialize, ToSchema)]
pub(crate) struct Page {
    #[serde(rename = "items")]
    events: Vec<Event>,
    #[serde(rename = "next_cursor")]
    next: Option<String>,
}
pub(crate) async fn page(
    app: &App,
    admin: bool,
    user: Option<Uuid>,
    filter: Filter,
    export: bool,
) -> Result<Page> {
    let limit = filter.limit.unwrap_or(50);
    let before = filter
        .after
        .as_ref()
        .map(|s| s.parse::<i64>())
        .transpose()
        .map_err(|_| s3s::s3_error!(InvalidArgument))?;
    if !(1..=if export { 100 } else { 200 }).contains(&limit)
        || before.is_some_and(|v| v <= 0)
        || filter.actor.as_ref().is_some_and(|v| v.len() > 128)
        || filter.action.as_ref().is_some_and(|v| v.len() > 128)
        || filter
            .source
            .as_deref()
            .is_some_and(|v| !["web", "token", "cli", "s3"].contains(&v))
        || filter
            .outcome
            .as_deref()
            .is_some_and(|v| !["unknown", "succeeded", "failed", "partial"].contains(&v))
        || filter.since.zip(filter.until).is_some_and(|(a, b)| a > b)
    {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let mut events = app
        .audit
        .events(admin, user, before, filter, limit, export)
        .await?;
    let more = events.len() > limit as usize;
    events.truncate(limit as usize);
    let next = more.then(|| events.last().unwrap().id.clone());
    Ok(Page { events, next })
}
#[utoipa::path(get,path="/api/v1/audit",operation_id="audit_list",params(Filter),responses((status=200,body=Page)))]
pub(super) async fn list(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Query(filter): Query<Filter>,
) -> Result<Json<Page>, HttpError> {
    Ok(Json(
        page(&app, actor.admin, Some(actor.id), filter, false).await?,
    ))
}
#[utoipa::path(get,path="/api/v1/audit/{id}",operation_id="audit_detail",params(("id"=String,Path)),responses((status=200,body=Event)))]
pub(super) async fn get(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<String>,
) -> Result<Json<Event>, HttpError> {
    let id = id
        .parse::<i64>()
        .map_err(|_| s3s::s3_error!(InvalidArgument))?;
    let event = app
        .audit
        .event(id, actor.admin, actor.id)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    Ok(Json(event))
}
#[utoipa::path(get,path="/api/v1/audit/export",operation_id="audit_export",params(Filter),responses((status=200,description="Up to 100 JSON Lines; X-Next-Cursor continues the export.")))]
pub(super) async fn export(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Query(filter): Query<Filter>,
) -> Result<Response, HttpError> {
    let page = page(&app, actor.admin, Some(actor.id), filter, true).await?;
    let mut body = String::new();
    for event in page.events {
        body.push_str(&serde_json::to_string(&event)?);
        body.push('\n');
    }
    let mut response = (
        [
            ("content-type", "application/x-ndjson"),
            (
                "content-disposition",
                "attachment; filename=\"mokyu-audit.jsonl\"",
            ),
        ],
        body,
    )
        .into_response();
    if let Some(next) = page.next {
        response.headers_mut().insert(
            "x-next-cursor",
            HeaderValue::from_str(&next)
                .map_err(|_| HttpError(s3s::s3_error!(InternalError).into()))?,
        );
    }
    Ok(response)
}
