use super::Identity;
use crate::{
    app::App,
    http::HttpError,
    model::{
        redundancy::{Location, Placement},
        tiering::{Candidate, Unit, Update, View},
    },
};
use anyhow::{Result, ensure};
use axum::{
    Json,
    extract::{Extension, Path, Query, State},
};
use std::sync::Arc;
use uuid::Uuid;

#[derive(serde::Deserialize, utoipa::IntoParams)]
#[serde(deny_unknown_fields)]
pub(super) struct Filter {
    unit: Unit,
    after: Option<Uuid>,
    limit: Option<u16>,
}

#[utoipa::path(get,path="/api/v1/buckets/{bucket}/tiering/units",operation_id="bucket_tiering_units",params(("bucket"=Uuid,Path),Filter),responses((status=200,body=Vec<Candidate>)))]
pub(super) async fn units(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Query(filter): Query<Filter>,
) -> Result<Json<Vec<Candidate>>, HttpError> {
    let stale =
        crate::config::seconds(&app.config.metrics.access_flush_interval)?.saturating_mul(2);
    Ok(Json(
        app.tiering
            .candidates(
                &actor.principal,
                bucket,
                filter.unit,
                filter.after.unwrap_or_default(),
                filter.limit.unwrap_or(32),
                i64::try_from(stale).map_err(anyhow::Error::from)?,
            )
            .await?,
    ))
}

#[utoipa::path(get,path="/api/v1/buckets/{bucket}/tiering",operation_id="bucket_tiering",params(("bucket"=Uuid,Path)),responses((status=200,body=View)))]
pub(super) async fn get(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
) -> Result<Json<View>, HttpError> {
    Ok(Json(app.tiering.tiering(&actor.principal, bucket).await?))
}

async fn change(
    app: &App,
    actor: &Identity,
    bucket: Uuid,
    input: &Update,
    preview: bool,
) -> Result<View> {
    if !preview {
        app.writable()?;
    }
    input.policy.validate()?;
    ensure!(
        u64::from(input.policy.observation_seconds)
            <= crate::config::seconds(&app.config.metrics.access_retention)?,
        s3s::s3_error!(InvalidArgument, "promotion window exceeds access retention")
    );
    let pair = app
        .tiering
        .locations(&actor.principal, bucket, input.policy.cold_storage)
        .await?;
    ensure!(
        app.role == "single"
            || matches!(pair.cold.2, Location::Remote { .. })
                && matches!(pair.hot.2, Location::Remote { .. }),
        s3s::s3_error!(InvalidArgument, "cluster tiers require shared S3 storage")
    );
    let (source, target) = tokio::try_join!(pair.hot.2.resolve(), pair.cold.2.resolve())?;
    let placement = Placement { source, target };
    placement.validate()?;
    app.storage.check_tier(pair.cold.0, pair.cold.1).await?;
    let result = app
        .tiering
        .update(&actor.principal, bucket, input, &pair, &placement, preview)
        .await?;
    if !preview {
        app.storage.access.collect(true);
        app.storage.access.wake.notify_one();
    }
    Ok(result)
}

#[utoipa::path(put,path="/api/v1/buckets/{bucket}/tiering",operation_id="bucket_tiering_save",params(("bucket"=Uuid,Path)),request_body=Update,responses((status=200,body=View)))]
pub(super) async fn save(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Json(input): Json<Update>,
) -> Result<Json<View>, HttpError> {
    Ok(Json(change(&app, &actor, bucket, &input, false).await?))
}
#[utoipa::path(post,path="/api/v1/buckets/{bucket}/tiering/preview",operation_id="bucket_tiering_preview",params(("bucket"=Uuid,Path)),request_body=Update,responses((status=200,body=View)))]
pub(super) async fn preview(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Json(input): Json<Update>,
) -> Result<Json<View>, HttpError> {
    Ok(Json(change(&app, &actor, bucket, &input, true).await?))
}

#[utoipa::path(post,path="/api/v1/buckets/{bucket}/tiering/state",operation_id="bucket_tiering_transition",params(("bucket"=Uuid,Path)),request_body=crate::model::tiering::Change,responses((status=200,body=View)))]
pub(super) async fn transition(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Json(input): Json<crate::model::tiering::Change>,
) -> Result<Json<View>, HttpError> {
    let view = app
        .tiering
        .transition(&actor.principal, bucket, &input)
        .await?;
    app.storage.access.wake.notify_one();
    Ok(Json(view))
}
