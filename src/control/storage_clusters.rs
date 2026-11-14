use super::Identity;
use crate::{
    app::App,
    http::HttpError,
    model::storage_cluster::{Cluster, Create, MemberState, Members, Overview, Update},
};
use axum::{
    Json,
    extract::{Extension, Path, State},
};
use std::sync::Arc;
use uuid::Uuid;

#[utoipa::path(post, operation_id="storage_cluster_repair", path="/api/v1/storage-clusters/{id}/fragments/{fragment}/repair", params(("id"=Uuid,Path),("fragment"=Uuid,Path)), responses((status=200,body=super::maintenance::TaskStarted)))]
pub(super) async fn repair(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path((id, fragment)): Path<(Uuid, Uuid)>,
) -> Result<Json<super::maintenance::TaskStarted>, HttpError> {
    actor.recent(&app).await?;
    Ok(Json(super::maintenance::TaskStarted {
        task_id: app
            .native_storage
            .request_repair(&actor.principal, id, fragment)
            .await?,
    }))
}

#[utoipa::path(get, operation_id="storage_cluster_recovery_sources", path="/api/v1/storage-clusters/{id}/recovery-sources", params(("id"=Uuid,Path), super::recovery::native::Query), responses((status=200,body=super::recovery::native::Page)))]
pub(super) async fn sources(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
    axum::extract::Query(query): axum::extract::Query<super::recovery::native::Query>,
) -> Result<Json<super::recovery::native::Page>, HttpError> {
    Ok(Json(
        super::recovery::native::list(&app, &actor.principal, id, query).await?,
    ))
}

#[utoipa::path(post, operation_id="storage_cluster_takeover", path="/api/v1/storage-clusters/takeover", request_body=crate::model::storage_cluster::Takeover, responses((status=200,body=Cluster)))]
pub(super) async fn takeover(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<crate::model::storage_cluster::Takeover>,
) -> Result<Json<Cluster>, HttpError> {
    actor.recent(&app).await?;
    Ok(Json(
        crate::storage::native::recovery::takeover(
            app.storage_clusters.as_ref(),
            &actor.principal,
            &app.secrets.credential_key,
            input,
        )
        .await?,
    ))
}

#[utoipa::path(get, operation_id="storage_clusters", path="/api/v1/storage-clusters", responses((status=200,body=Overview)))]
pub(super) async fn list(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<Overview>, HttpError> {
    app.authorization
        .admin(&actor.principal)
        .await?
        .finish()
        .await?;
    Ok(Json(app.storage_clusters.overview().await?))
}

#[utoipa::path(post, operation_id="storage_cluster_create", path="/api/v1/storage-clusters", request_body=Create, responses((status=200,body=Cluster)))]
pub(super) async fn create(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<Create>,
) -> Result<Json<Cluster>, HttpError> {
    actor.recent(&app).await?;
    Ok(Json(
        app.storage_clusters
            .create(&actor.principal, &app.secrets.credential_key, input)
            .await?,
    ))
}

#[utoipa::path(put, operation_id="storage_cluster_update", path="/api/v1/storage-clusters/{id}", params(("id"=Uuid,Path)), request_body=Update, responses((status=200,body=Cluster)))]
pub(super) async fn update(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
    Json(input): Json<Update>,
) -> Result<Json<Cluster>, HttpError> {
    actor.recent(&app).await?;
    Ok(Json(
        app.storage_clusters
            .update(&actor.principal, id, input)
            .await?,
    ))
}

#[utoipa::path(post, operation_id="storage_cluster_members", path="/api/v1/storage-clusters/{id}/members", params(("id"=Uuid,Path)), request_body=Members, responses((status=200,body=Cluster)))]
pub(super) async fn members(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
    Json(input): Json<Members>,
) -> Result<Json<Cluster>, HttpError> {
    actor.recent(&app).await?;
    Ok(Json(
        app.storage_clusters
            .add_members(&actor.principal, id, input)
            .await?,
    ))
}

#[utoipa::path(post, operation_id="storage_cluster_enable", path="/api/v1/storage-clusters/{id}/groups", params(("id"=Uuid,Path)), request_body=Members, responses((status=200,body=Cluster)))]
pub(super) async fn groups(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
    Json(input): Json<Members>,
) -> Result<Json<Cluster>, HttpError> {
    actor.recent(&app).await?;
    Ok(Json(
        app.storage_clusters
            .enable_groups(&actor.principal, id, input)
            .await?,
    ))
}

#[utoipa::path(put, operation_id="storage_cluster_member_state", path="/api/v1/storage-clusters/{id}/members/{media}", params(("id"=Uuid,Path),("media"=Uuid,Path)), request_body=MemberState, responses((status=200,body=Cluster)))]
pub(super) async fn state(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path((id, media)): Path<(Uuid, Uuid)>,
    Json(input): Json<MemberState>,
) -> Result<Json<Cluster>, HttpError> {
    actor.recent(&app).await?;
    Ok(Json(
        app.storage_clusters
            .member_state(&actor.principal, id, media, input)
            .await?,
    ))
}
