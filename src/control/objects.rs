use super::{
    auth::Identity,
    contract::{self, BucketView},
    insights,
};
use crate::{app::App, authorization::Action, http::HttpError};
use anyhow::Result;
use axum::{
    Json,
    extract::{Extension, Path, Query, State},
};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use uuid::Uuid;

#[utoipa::path(get, path = "/api/v1/buckets", responses((status = 200, body = Vec<BucketView>), (status = 403, body = contract::ErrorBody)))]
pub(super) async fn buckets(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<Vec<BucketView>>, HttpError> {
    let rows = app
        .buckets
        .visible(actor.id, actor.admin, actor.principal.token_id())
        .await?;
    Ok(Json(rows))
}
pub(crate) use crate::model::bucket::Website;
#[utoipa::path(
    get,
    operation_id = "bucket_cors",
    path = "/api/v1/buckets/{bucket}/cors",
    params(("bucket" = Uuid, Path)),
    responses((status = 200, body = Value), (status = 403, body = contract::ErrorBody))
)]
pub(super) async fn cors(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
) -> Result<Json<Value>, HttpError> {
    actor
        .principal
        .require(app.authorization.as_ref(), bucket, Action::Settings)
        .await?;
    let rules = app.buckets.cors(bucket).await?;
    Ok(Json(rules))
}
#[utoipa::path(
    put,
    operation_id = "bucket_cors_save",
    path = "/api/v1/buckets/{bucket}/cors",
    params(("bucket" = Uuid, Path)),
    request_body = Value,
    responses((status = 200, body = Value), (status = 400, body = contract::ErrorBody))
)]
pub(super) async fn save_cors(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Json(rules): Json<Value>,
) -> Result<Json<Value>, HttpError> {
    Ok(Json(app.set_cors(&actor.principal, bucket, rules).await?))
}
impl Website {
    pub(crate) fn validate(&self) -> Result<(), HttpError> {
        let valid_key = |key: &str| {
            !key.starts_with('/')
                && !key.contains('\\')
                && !key.chars().any(char::is_control)
                && !key.split('/').any(|part| part == "." || part == "..")
        };
        if self.index_document.is_empty()
            || self.index_document.len() > 255
            || self.index_document.contains('/')
            || !valid_key(&self.index_document)
            || self.error_document.len() > 1024
            || !valid_key(&self.error_document)
        {
            return Err(s3s::s3_error!(InvalidArgument, "invalid website document key").into());
        }
        Ok(())
    }
}
#[utoipa::path(get, path = "/api/v1/buckets/{bucket}/website", params(("bucket" = Uuid, Path)), responses((status = 200, body = Website), (status = 403, body = contract::ErrorBody)))]
pub(super) async fn website(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
) -> Result<Json<Website>, HttpError> {
    actor
        .principal
        .require(app.authorization.as_ref(), bucket, Action::Settings)
        .await?;
    let row = app.buckets.website(bucket).await?;
    Ok(Json(row))
}
#[utoipa::path(put, path = "/api/v1/buckets/{bucket}/website", params(("bucket" = Uuid, Path)), request_body = Website, responses((status = 200, body = Website), (status = 400, body = contract::ErrorBody)))]
pub(super) async fn save_website(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Json(input): Json<Website>,
) -> Result<Json<Website>, HttpError> {
    app.writable()?;
    input.validate()?;
    Ok(Json(
        app.buckets
            .set_website(&actor.principal, bucket, input)
            .await?,
    ))
}
#[derive(Deserialize, utoipa::IntoParams)]
pub(crate) struct ChunkQuery {
    key: String,
    version: Uuid,
    after: Option<i64>,
    limit: Option<i64>,
}
#[utoipa::path(get,operation_id="object_chunks",path="/api/v1/buckets/{bucket}/object/chunks",params(("bucket"=Uuid,Path),ChunkQuery),responses((status=200,body=insights::ChunkPage)))]
pub(super) async fn object_chunks(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Query(q): Query<ChunkQuery>,
) -> Result<Json<insights::ChunkPage>, HttpError> {
    actor
        .principal
        .require(app.authorization.as_ref(), bucket, Action::Inspect)
        .await?;
    let limit = q.limit.unwrap_or(100);
    if !(1..=200).contains(&limit) || q.after.is_some_and(|a| a < 0) {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let (object, _pin) = app.current(bucket, &q.key).await?;
    if object.id != q.version {
        return Err(s3s::s3_error!(PreconditionFailed).into());
    }
    let mut rows = app
        .objects
        .chunks(object.id, q.after.unwrap_or(-1), limit + 1)
        .await?;
    let more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    if !actor.admin {
        for row in &mut rows {
            if let Some(row) = row.as_object_mut() {
                for key in ["reads", "range_reads", "block_key_id"] {
                    row.remove(key);
                }
            }
        }
    }
    let next_offset = more.then(|| {
        rows.last().unwrap()["offset_bytes"]
            .as_str()
            .unwrap()
            .to_owned()
    });
    let chunks = rows
        .into_iter()
        .map(serde_json::from_value)
        .collect::<Result<_, _>>()?;
    Ok(Json(insights::ChunkPage {
        chunks,
        next_offset,
    }))
}
