use super::Identity;
use crate::{
    app::App,
    http::HttpError,
    model::{
        Id,
        replication::{Command, Item, Options, Replication},
    },
};
use axum::{
    Json,
    extract::{Extension, Path, Query, State},
    http::{HeaderMap, StatusCode},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

#[derive(Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
pub(super) struct Filter {
    after: Option<Uuid>,
    limit: Option<i64>,
}

#[derive(Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
pub(super) struct ItemFilter {
    after: Option<String>,
    limit: Option<i64>,
}

#[derive(Serialize, ToSchema)]
#[schema(as = ReplicationPage)]
pub(super) struct Page {
    replications: Vec<Replication>,
    next_after: Option<Uuid>,
}

#[derive(Serialize, ToSchema)]
#[schema(as = ReplicationItems)]
pub(super) struct Items {
    items: Vec<Item>,
    next_after: Option<Id>,
}

fn limit(value: Option<i64>) -> Result<i64, HttpError> {
    let value = value.unwrap_or(50);
    if !(1..=100).contains(&value) {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    Ok(value)
}

#[utoipa::path(post,path="/api/v1/replications",operation_id="replication_create",params(("Idempotency-Key"=Option<Uuid>, Header, description="Reuse this UUID when retrying the same task creation")),request_body=Options,responses((status=202,body=super::maintenance::TaskStarted)))]
pub(super) async fn create(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    headers: HeaderMap,
    Json(input): Json<Options>,
) -> Result<(StatusCode, Json<super::maintenance::TaskStarted>), HttpError> {
    input.validate()?;
    let id = headers
        .get("Idempotency-Key")
        .map(|value| {
            value
                .to_str()
                .ok()
                .and_then(|value| value.parse::<Uuid>().ok())
                .filter(|id| !id.is_nil())
                .ok_or_else(|| s3s::s3_error!(InvalidArgument))
        })
        .transpose()?
        .unwrap_or_else(Uuid::now_v7);
    let task_id = crate::model::REQUEST_ID
        .scope(id, app.replications.create(&actor.principal, &input))
        .await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("replication-created").await;
    app.wake_tasks.notify_one();
    Ok((
        StatusCode::ACCEPTED,
        Json(super::maintenance::TaskStarted { task_id }),
    ))
}

#[utoipa::path(get,path="/api/v1/replications",operation_id="replication_list",params(Filter),responses((status=200,body=Page)))]
pub(super) async fn list(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Query(input): Query<Filter>,
) -> Result<Json<Page>, HttpError> {
    let limit = limit(input.limit)?;
    let (rows, next_after) = app
        .replications
        .list(&actor.principal, input.after, limit)
        .await?;
    Ok(Json(Page {
        replications: rows,
        next_after,
    }))
}

#[utoipa::path(get,path="/api/v1/replications/{id}",operation_id="replication_get",params(("id"=Uuid,Path)),responses((status=200,body=Replication)))]
pub(super) async fn get(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<Json<Replication>, HttpError> {
    Ok(Json(app.replications.view(&actor.principal, id).await?))
}

#[utoipa::path(get,path="/api/v1/replications/{id}/items",operation_id="replication_items",params(("id"=Uuid,Path),ItemFilter),responses((status=200,body=Items)))]
pub(super) async fn items(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
    Query(input): Query<ItemFilter>,
) -> Result<Json<Items>, HttpError> {
    let limit = limit(input.limit)?;
    let after = input
        .after
        .as_deref()
        .map(|value| Id::parse(value, app.config.database.backend))
        .transpose()?;
    let mut rows = app
        .replications
        .items(&actor.principal, id, after, limit + 1)
        .await?;
    let next_after = (rows.len() > limit as usize).then(|| rows[limit as usize - 1].id);
    rows.truncate(limit as usize);
    Ok(Json(Items {
        items: rows,
        next_after,
    }))
}

#[utoipa::path(post,path="/api/v1/replications/{id}/command",operation_id="replication_command",params(("id"=Uuid,Path)),request_body=Command,responses((status=202,body=super::maintenance::TaskStarted)))]
pub(super) async fn command(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
    Json(input): Json<Command>,
) -> Result<(StatusCode, Json<super::maintenance::TaskStarted>), HttpError> {
    app.replications
        .command(&actor.principal, id, input)
        .await?;
    app.wake_tasks.notify_one();
    Ok((
        StatusCode::ACCEPTED,
        Json(super::maintenance::TaskStarted { task_id: id }),
    ))
}
