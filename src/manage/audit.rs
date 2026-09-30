use super::Identity;
use crate::{app::App, authorization::Principal, http::HttpError};
use anyhow::Result;
use axum::{
    Json,
    extract::{Extension, Path, Query, State},
    http::HeaderValue,
    response::{IntoResponse, Response},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{PgPool, Postgres, Transaction};
use std::{future::Future, sync::Arc};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

tokio::task_local! { static EVENT: i64; }
pub(crate) fn active() -> bool {
    EVENT.try_with(|_| ()).is_ok()
}

pub(crate) async fn begin(
    db: &PgPool,
    principal: &Principal,
    action: &str,
    request_id: Option<&str>,
) -> Result<i64> {
    let actor = principal.user_id();
    let label = if let Some(id) = actor {
        sqlx::query_scalar("SELECT username FROM web_users WHERE id=$1")
            .bind(id)
            .fetch_optional(db)
            .await?
            .unwrap_or_else(|| "Deleted account".to_string())
    } else {
        "Local CLI".to_string()
    };
    let source = if principal.token_id().is_some() {
        "token"
    } else if matches!(principal, Principal::Local) {
        "cli"
    } else {
        "web"
    };
    Ok(sqlx::query_scalar("INSERT INTO audit_events(actor_id,actor_label,token_id,source,action,request_id) VALUES($1,$2,$3,$4,$5,$6) RETURNING id")
        .bind(actor).bind(label).bind(principal.token_id()).bind(source).bind(action).bind(request_id).fetch_one(db).await?)
}
pub(crate) async fn scope<T>(id: i64, future: impl Future<Output = T>) -> T {
    EVENT.scope(id, future).await
}
pub(crate) async fn checkpoint(
    tx: &mut Transaction<'_, Postgres>,
    action: &str,
    target: &str,
    detail: Value,
) -> Result<()> {
    let Ok(id) = EVENT.try_with(|id| *id) else {
        return Ok(());
    };
    let project = detail
        .get("project_id")
        .and_then(Value::as_str)
        .and_then(|s| s.parse::<Uuid>().ok());
    let bucket = detail
        .get("bucket_id")
        .and_then(Value::as_str)
        .and_then(|s| s.parse::<Uuid>().ok());
    anyhow::ensure!(
        serde_json::to_vec(&detail)?.len() <= 262144,
        "audit detail exceeds its bound"
    );
    let changed=sqlx::query("UPDATE audit_events SET action=$2,target=$3,project_id=$4,bucket_id=$5,detail=$6,outcome='succeeded',finished_at=now() WHERE id=$1")
        .bind(id).bind(action).bind(target).bind(project).bind(bucket).bind(detail).execute(&mut **tx).await?.rows_affected();
    anyhow::ensure!(changed == 1, "audit intent is missing");
    Ok(())
}
pub(crate) async fn finish(db: &PgPool, id: i64, status: u16, partial: bool) {
    let outcome = if partial {
        "partial"
    } else if status >= 400 {
        "failed"
    } else {
        "succeeded"
    };
    if let Err(error)=sqlx::query("UPDATE audit_events SET outcome=CASE WHEN outcome='unknown' THEN $2 ELSE outcome END,finished_at=COALESCE(finished_at,now()),status=$3 WHERE id=$1")
        .bind(id).bind(outcome).bind(i32::from(status)).execute(db).await {
        tracing::warn!(audit_id=id,%error,"audit result could not be saved; durable intent remains");
    }
}
pub(crate) async fn partial(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    if let Ok(id) = EVENT.try_with(|id| *id) {
        sqlx::query("UPDATE audit_events SET outcome='partial' WHERE id=$1")
            .bind(id)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}
pub(super) async fn security(
    db: &mut sqlx::PgConnection,
    user: Uuid,
    action: &str,
    success: bool,
    request: &str,
) -> Result<()> {
    // Keep at most one rejected password event per account per minute.
    sqlx::query("SELECT id FROM web_users WHERE id=$1 FOR UPDATE")
        .bind(user)
        .execute(&mut *db)
        .await?;
    sqlx::query("INSERT INTO audit_events(actor_id,actor_label,source,action,target,outcome,request_id,finished_at,detail) SELECT id,username,'web',$2,id::text,$3,$4,now(),jsonb_build_object('identity_verified',$5::boolean) FROM web_users WHERE id=$1 AND ($5 OR NOT EXISTS(SELECT 1 FROM audit_events WHERE actor_id=$1 AND action=$2 AND outcome='failed' AND created_at>now()-interval '1 minute'))")
        .bind(user).bind(action).bind(if success {"succeeded"} else {"failed"}).bind(request).bind(success).execute(db).await?;
    Ok(())
}

#[derive(Serialize, sqlx::FromRow, ToSchema)]
pub(crate) struct Event {
    id: String,
    created_at: DateTime<Utc>,
    finished_at: Option<DateTime<Utc>>,
    actor_id: Option<Uuid>,
    actor_label: String,
    token_id: Option<Uuid>,
    source: String,
    action: String,
    target: String,
    project_id: Option<Uuid>,
    bucket_id: Option<Uuid>,
    outcome: String,
    request_id: Option<String>,
    status: Option<i32>,
    detail: Value,
}
#[derive(Deserialize, IntoParams, Default)]
pub(crate) struct Filter {
    pub after: Option<String>,
    pub limit: Option<i64>,
    pub actor: Option<String>,
    pub action: Option<String>,
    pub source: Option<String>,
    pub outcome: Option<String>,
    pub project: Option<Uuid>,
    pub bucket: Option<Uuid>,
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
}
#[derive(Serialize, ToSchema)]
pub(crate) struct Page {
    events: Vec<Event>,
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
            .is_some_and(|v| !["web", "token", "cli"].contains(&v))
        || filter
            .outcome
            .as_deref()
            .is_some_and(|v| !["unknown", "succeeded", "failed", "partial"].contains(&v))
        || filter.since.zip(filter.until).is_some_and(|(a, b)| a > b)
    {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let mut events: Vec<Event>=sqlx::query_as("SELECT e.id::text,e.created_at,e.finished_at,e.actor_id,e.actor_label,e.token_id,e.source,e.action,e.target,e.project_id,e.bucket_id,e.outcome,e.request_id,e.status,CASE WHEN $13 OR octet_length(e.detail::text)<=8192 THEN e.detail ELSE '{\"details_available\":true}'::jsonb END AS detail FROM audit_events e WHERE ($1 OR (actor_id=$2 AND ((e.bucket_id IS NULL AND (starts_with(e.action,'account.') OR starts_with(e.action,'session.') OR starts_with(e.action,'token.') OR (e.outcome='failed' AND e.target='' AND e.detail='{}'::jsonb))) OR EXISTS(SELECT 1 FROM user_bucket_access a WHERE a.user_id=$2 AND a.bucket_id=e.bucket_id AND 'bucket.list'=ANY(a.actions))))) AND ($3::bigint IS NULL OR e.id<$3) AND position(lower($4) in lower(e.actor_label))>0 AND starts_with(e.action,$5) AND ($6::text IS NULL OR e.source=$6) AND ($7::text IS NULL OR e.outcome=$7) AND ($8::uuid IS NULL OR e.project_id=$8) AND ($9::uuid IS NULL OR e.bucket_id=$9) AND ($10::timestamptz IS NULL OR e.created_at>=$10) AND ($11::timestamptz IS NULL OR e.created_at<=$11) ORDER BY e.id DESC LIMIT $12")
        .bind(admin).bind(user).bind(before).bind(filter.actor.unwrap_or_default()).bind(filter.action.unwrap_or_default()).bind(filter.source).bind(filter.outcome).bind(filter.project).bind(filter.bucket).bind(filter.since).bind(filter.until).bind(limit+1).bind(export).fetch_all(&app.db).await?;
    let more = events.len() > limit as usize;
    events.truncate(limit as usize);
    let next = more.then(|| events.last().unwrap().id.clone());
    Ok(Page { events, next })
}
#[utoipa::path(get,path="/api/audit",operation_id="audit_list",params(Filter),responses((status=200,body=Page)))]
pub(super) async fn list(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Query(filter): Query<Filter>,
) -> Result<Json<Page>, HttpError> {
    Ok(Json(
        page(&app, actor.admin, Some(actor.id), filter, false).await?,
    ))
}
#[utoipa::path(get,path="/api/audit/{id}",operation_id="audit_detail",params(("id"=String,Path)),responses((status=200,body=Event)))]
pub(super) async fn get(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<String>,
) -> Result<Json<Event>, HttpError> {
    let id = id
        .parse::<i64>()
        .map_err(|_| s3s::s3_error!(InvalidArgument))?;
    let event=sqlx::query_as("SELECT e.id::text,e.created_at,e.finished_at,e.actor_id,e.actor_label,e.token_id,e.source,e.action,e.target,e.project_id,e.bucket_id,e.outcome,e.request_id,e.status,e.detail FROM audit_events e WHERE e.id=$1 AND ($2 OR (e.actor_id=$3 AND ((e.bucket_id IS NULL AND (starts_with(e.action,'account.') OR starts_with(e.action,'session.') OR starts_with(e.action,'token.') OR (e.outcome='failed' AND e.target='' AND e.detail='{}'::jsonb))) OR EXISTS(SELECT 1 FROM user_bucket_access a WHERE a.user_id=$3 AND a.bucket_id=e.bucket_id AND 'bucket.list'=ANY(a.actions)))))")
        .bind(id).bind(actor.admin).bind(actor.id).fetch_optional(&app.db).await?.ok_or_else(||s3s::s3_error!(NoSuchKey))?;
    Ok(Json(event))
}
#[utoipa::path(get,path="/api/audit/export",operation_id="audit_export",params(Filter),responses((status=200,description="Up to 100 JSON Lines; X-Next-Cursor continues the export.")))]
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
