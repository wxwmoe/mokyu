use super::Identity;
use crate::{
    app::App,
    authorization::Principal,
    http::HttpError,
    model::journal::{Status, policy::Policy},
};
use anyhow::Result;
use axum::{
    Json,
    extract::{Extension, Path, State},
};
use serde::Deserialize;
use std::sync::Arc;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = MetadataJournalUpdate)]
pub(crate) struct Update {
    revision: String,
    policy: Policy,
}

async fn id(app: &App, name: &str) -> Result<Uuid> {
    Ok(app
        .storage_records
        .id(name, None)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?)
}

pub(super) async fn inspect(app: &App, principal: &Principal, name: &str) -> Result<Status> {
    app.authorization.admin(principal).await?.finish().await?;
    app.journal.journal_status(id(app, name).await?).await
}

pub(super) async fn configure(
    app: &App,
    principal: &Principal,
    name: &str,
    input: Update,
) -> Result<Status> {
    app.writable()?;
    let storage = id(app, name).await?;
    let revision = input
        .revision
        .parse::<i64>()
        .ok()
        .filter(|n| *n > 0)
        .ok_or_else(|| s3s::s3_error!(InvalidArgument, "invalid journal policy revision"))?;
    input
        .policy
        .validate()
        .map_err(|e| s3s::s3_error!(InvalidArgument, "{e}"))?;
    app.journal
        .policy(principal, storage, revision, &input.policy)
        .await?;
    app.wake_maintenance.notify_waiters();
    app.journal.journal_status(storage).await
}

pub(super) async fn force(app: &App, principal: &Principal, name: &str) -> Result<Status> {
    app.writable()?;
    let storage = id(app, name).await?;
    app.journal.flush(principal, storage).await?;
    app.wake_maintenance.notify_waiters();
    app.journal.journal_status(storage).await
}

#[utoipa::path(get,operation_id="metadata_journal_status",path="/api/v1/storages/{name}/journal",
    params(("name"=String,Path)),responses((status=200,body=Status)))]
pub(crate) async fn get(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(name): Path<String>,
) -> Result<Json<Status>, HttpError> {
    Ok(Json(inspect(&app, &actor.principal, &name).await?))
}

#[utoipa::path(put,operation_id="metadata_journal_update",path="/api/v1/storages/{name}/journal",
    params(("name"=String,Path)),request_body=Update,responses((status=200,body=Status)))]
pub(crate) async fn set(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(name): Path<String>,
    Json(input): Json<Update>,
) -> Result<Json<Status>, HttpError> {
    actor.recent(&app).await?;
    Ok(Json(configure(&app, &actor.principal, &name, input).await?))
}

#[utoipa::path(post,operation_id="metadata_journal_flush",path="/api/v1/storages/{name}/journal/flush",
    params(("name"=String,Path)),responses((status=200,body=Status)))]
pub(crate) async fn flush(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(name): Path<String>,
) -> Result<Json<Status>, HttpError> {
    actor.recent(&app).await?;
    Ok(Json(force(&app, &actor.principal, &name).await?))
}
use crate::model::journal::snapshots::{Action as SnapshotAction, Status as SnapshotStatus};
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as=MetadataSnapshotRequest)]
pub(crate) struct SnapshotRequest {
    action: SnapshotAction,
}

pub(super) async fn snapshots(
    app: &App,
    principal: &Principal,
    name: &str,
    action: Option<SnapshotAction>,
) -> Result<SnapshotStatus> {
    app.authorization.admin(principal).await?.finish().await?;
    let storage = id(app, name).await?;
    if let Some(action) = action {
        app.writable()?;
        app.journal
            .snapshot_request(principal, storage, action)
            .await?;
        app.wake_maintenance.notify_waiters();
    }
    app.journal.snapshot_status(storage).await
}
#[utoipa::path(get,operation_id="metadata_snapshots_status",path="/api/v1/storages/{name}/snapshots",params(("name"=String,Path)),responses((status=200,body=SnapshotStatus)))]
pub(crate) async fn snapshot_get(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(name): Path<String>,
) -> Result<Json<SnapshotStatus>, HttpError> {
    Ok(Json(snapshots(&app, &actor.principal, &name, None).await?))
}
#[utoipa::path(post,operation_id="metadata_snapshot_create",path="/api/v1/storages/{name}/snapshots",params(("name"=String,Path)),request_body=SnapshotRequest,responses((status=200,body=SnapshotStatus)))]
pub(crate) async fn snapshot_post(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(name): Path<String>,
    Json(input): Json<SnapshotRequest>,
) -> Result<Json<SnapshotStatus>, HttpError> {
    actor.recent(&app).await?;
    Ok(Json(
        snapshots(&app, &actor.principal, &name, Some(input.action)).await?,
    ))
}
