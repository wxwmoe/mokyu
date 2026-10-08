use super::Identity;
use crate::{
    app::App,
    authorization::{Action, Permit, Principal},
    http::HttpError,
};
use anyhow::Result;
use axum::{
    Json,
    extract::{Extension, Path, State},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Serialize, sqlx::FromRow, ToSchema)]
pub(crate) struct Quota {
    scope: String,
    scope_id: Uuid,
    used_bytes: Option<String>,
    reserved_bytes: Option<String>,
    inflight_bytes: Option<String>,
    object_count: Option<String>,
    bucket_count: Option<String>,
    logical_bytes_limit: Option<String>,
    inflight_bytes_limit: Option<String>,
    bucket_count_limit: Option<String>,
}
#[derive(Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct Limits {
    pub logical_bytes_limit: Option<String>,
    pub inflight_bytes_limit: Option<String>,
    pub bucket_count_limit: Option<String>,
}
fn number(value: &Option<String>) -> Result<Option<i64>> {
    value
        .as_ref()
        .map(|value| {
            value
                .parse::<i64>()
                .ok()
                .filter(|n| *n >= 0)
                .ok_or_else(|| s3s::s3_error!(InvalidArgument).into())
        })
        .transpose()
}
fn validate_scope(value: &str) -> Result<()> {
    if !matches!(value, "project" | "bucket") {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    Ok(())
}
pub(crate) async fn read(app: &App, principal: &Principal, scope: &str, id: Uuid) -> Result<Quota> {
    validate_scope(scope)?;
    let mut tx = app.db.begin().await?;
    let admin = principal.lock_identity(&mut tx).await?;
    let visible = if scope == "bucket" {
        Permit::for_action(principal.clone(), id, Action::Inspect)
            .lock(&mut tx)
            .await?;
        true
    } else if admin {
        true
    } else {
        if principal.token_id().is_some() {
            return Err(s3s::s3_error!(AccessDenied).into());
        }
        let scope: Option<String> = sqlx::query_scalar(
            "SELECT scope FROM project_members WHERE user_id=$1 AND project_id=$2",
        )
        .bind(principal.user_id())
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?;
        scope.ok_or_else(|| s3s::s3_error!(AccessDenied))? == "all"
    };
    let result=sqlx::query_as("SELECT scope,scope_id,CASE WHEN $3 THEN used_bytes::text END used_bytes,CASE WHEN $3 THEN reserved_bytes::text END reserved_bytes,CASE WHEN $3 THEN inflight_bytes::text END inflight_bytes,CASE WHEN $3 THEN object_count::text END object_count,CASE WHEN $3 THEN bucket_count::text END bucket_count,logical_bytes_limit::text logical_bytes_limit,inflight_bytes_limit::text inflight_bytes_limit,bucket_count_limit::text bucket_count_limit FROM quota_scopes WHERE scope=$1 AND scope_id=$2")
        .bind(scope).bind(id).bind(visible).fetch_optional(&mut *tx).await?.ok_or_else(||s3s::s3_error!(NoSuchKey))?;
    tx.commit().await?;
    Ok(result)
}
pub(crate) async fn save(
    app: &App,
    principal: &Principal,
    scope: &str,
    id: Uuid,
    input: Limits,
) -> Result<Quota> {
    validate_scope(scope)?;
    let (bytes, inflight, buckets) = (
        number(&input.logical_bytes_limit)?,
        number(&input.inflight_bytes_limit)?,
        number(&input.bucket_count_limit)?,
    );
    if scope == "bucket" && (inflight.is_some() || buckets.is_some()) {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let mut tx = super::users::admin_transaction(app, principal).await?;
    let changed=sqlx::query("UPDATE quota_scopes SET logical_bytes_limit=$3,inflight_bytes_limit=$4,bucket_count_limit=$5 WHERE scope=$1 AND scope_id=$2")
        .bind(scope).bind(id).bind(bytes).bind(inflight).bind(buckets).execute(&mut *tx).await?.rows_affected();
    if changed != 1 {
        return Err(s3s::s3_error!(NoSuchKey).into());
    }
    let mut detail = serde_json::to_value(input)?;
    detail[format!("{scope}_id")] = serde_json::json!(id);
    super::audit::checkpoint(&mut tx, "quota.update", &id.to_string(), detail).await?;
    tx.commit().await?;
    read(app, principal, scope, id).await
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
