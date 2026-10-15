use super::Identity;
use crate::{app::App, authorization::Principal, http::HttpError};
use anyhow::Result;
use axum::{
    Json,
    extract::{Extension, Path, State},
};
use std::sync::Arc;
use uuid::Uuid;

pub(crate) use crate::model::quota::{Limits, Quota};
pub(crate) async fn read(app: &App, principal: &Principal, scope: &str, id: Uuid) -> Result<Quota> {
    app.quota.read(principal, scope, id).await
}
pub(crate) async fn save(
    app: &App,
    principal: &Principal,
    scope: &str,
    id: Uuid,
    input: Limits,
) -> Result<Quota> {
    app.quota.save(principal, scope, id, input).await
}
#[utoipa::path(get, path="/api/v1/quotas/{scope_type}/{scope_id}",params(("scope_type"=String,Path),("scope_id"=Uuid,Path)),responses((status=200,body=Quota)))]
pub(super) async fn get(
    State(app): State<Arc<App>>,
    Extension(identity): Extension<Identity>,
    Path((scope_type, scope_id)): Path<(String, Uuid)>,
) -> Result<Json<Quota>, HttpError> {
    Ok(Json(
        read(&app, &identity.principal, &scope_type, scope_id).await?,
    ))
}
#[utoipa::path(put, path="/api/v1/quotas/{scope_type}/{scope_id}",params(("scope_type"=String,Path),("scope_id"=Uuid,Path)),request_body=Limits,responses((status=200,body=Quota)))]
pub(super) async fn put(
    State(app): State<Arc<App>>,
    Extension(identity): Extension<Identity>,
    Path((scope_type, scope_id)): Path<(String, Uuid)>,
    Json(input): Json<Limits>,
) -> Result<Json<Quota>, HttpError> {
    identity.recent(&app).await?;
    Ok(Json(
        save(&app, &identity.principal, &scope_type, scope_id, input).await?,
    ))
}
