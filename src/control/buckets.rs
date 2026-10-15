use super::{Identity, contract::BucketView};
use crate::model::bucket::BucketUpdate;
pub(crate) use crate::model::bucket::{
    BucketImpact, BucketProject, BucketStorage, CreateBucket, Domain, TransferInput,
    TransferProject, fingerprint,
};
use crate::{
    app::{App, Bucket},
    authorization::{Action, Principal},
    http::{CorsRule, HttpError},
};
use anyhow::Result;
use axum::{
    Json,
    extract::{Extension, Path, Query, State},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, sync::Arc};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

#[derive(Serialize, ToSchema)]
pub(super) struct BucketSettings {
    bucket: BucketView,
    domains: Vec<String>,
}

#[derive(Serialize, ToSchema)]
pub(super) struct BucketCreationOptions {
    projects: Vec<BucketProject>,
    storages: Vec<BucketStorage>,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct DomainInput {
    host: String,
    bucket_id: Uuid,
}

#[derive(Deserialize, IntoParams)]
pub(super) struct DomainQuery {
    bucket_id: Option<Uuid>,
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct SettingsInput {
    revision: String,
    cors: Vec<CorsRule>,
    website_enabled: bool,
    index_document: String,
    error_document: String,
    public_base_url: String,
    uploads_paused: bool,
    domains: Option<Vec<String>>,
}

#[derive(Serialize, ToSchema)]
pub(super) struct BucketPreview {
    bucket: BucketImpact,
    confirmation: String,
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct ConfirmBucket {
    confirm_name: String,
    confirmation: String,
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct TransferTarget {
    target_project: Uuid,
}

#[derive(Serialize, ToSchema)]
pub(super) struct TransferPreview {
    bucket: BucketImpact,
    target: TransferProject,
    confirmation: String,
}
#[derive(Serialize, Deserialize, ToSchema)]
pub(super) struct BucketTask {
    task_id: Uuid,
    #[serde(default)]
    existing: bool,
}

fn revision(value: &str) -> Result<i64> {
    value
        .parse::<i64>()
        .ok()
        .filter(|v| *v >= 0)
        .ok_or_else(|| s3s::s3_error!(InvalidArgument).into())
}
pub(crate) fn host(value: &str) -> Result<String> {
    let value = value.to_ascii_lowercase();
    let authority = value
        .parse::<axum::http::uri::Authority>()
        .map_err(|_| s3s::s3_error!(InvalidArgument))?;
    let name = authority.host();
    if value.len() > 253
        || name.is_empty()
        || name.ends_with('.')
        || name.split('.').any(|s| {
            s.is_empty()
                || s.len() > 63
                || s.starts_with('-')
                || s.ends_with('-')
                || !s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
        })
        || value.contains(':') && authority.port_u16().is_none()
        || value.contains('@')
    {
        return Err(s3s::s3_error!(
            InvalidArgument,
            "use an ASCII hostname with an optional port"
        )
        .into());
    }
    Ok(value)
}
fn public_url(value: &str) -> Result<String> {
    if value.is_empty() {
        return Ok(String::new());
    }
    let url: hyper::Uri = value.parse().map_err(|_| s3s::s3_error!(InvalidArgument))?;
    if value.len() > 2048
        || !matches!(url.scheme_str(), Some("http" | "https"))
        || url.host().is_none()
        || url.authority().is_some_and(|a| a.as_str().contains('@'))
        || url.query().is_some()
        || value.contains('#')
    {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    Ok(format!("{}/", value.trim_end_matches('/')))
}

pub(crate) async fn create_bucket(
    app: &App,
    principal: &Principal,
    input: CreateBucket,
) -> Result<Bucket> {
    app.writable()?;
    app.buckets.create(principal, input).await
}

pub(crate) async fn delete_bucket(
    app: &App,
    principal: &Principal,
    bucket: Uuid,
    name: &str,
    confirmation: Option<&str>,
) -> Result<()> {
    app.writable()?;
    let _coord = app.coord.lock().await;
    app.writable()?;
    app.buckets
        .delete(principal, bucket, name, confirmation)
        .await
}
pub(crate) async fn domain_change(
    app: &App,
    principal: &Principal,
    value: &str,
    bucket: Option<Uuid>,
) -> Result<()> {
    app.writable()?;
    app.buckets
        .change_domain(principal, &host(value)?, bucket)
        .await
}

#[utoipa::path(
    get,
    operation_id = "domains_list",
    path = "/api/v1/domains",
    params(DomainQuery),
    responses((status = 200, body = Vec<Domain>))
)]
pub(super) async fn domains(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Query(query): Query<DomainQuery>,
) -> Result<Json<Vec<Domain>>, HttpError> {
    actor.recent(&app).await?;
    Ok(Json(
        app.buckets
            .domains(&actor.principal, query.bucket_id)
            .await?,
    ))
}

#[utoipa::path(
    post,
    operation_id = "domain_create",
    path = "/api/v1/domains",
    request_body = DomainInput,
    responses((status = 201, body = Domain))
)]
pub(super) async fn add_domain(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<DomainInput>,
) -> Result<(StatusCode, Json<Domain>), HttpError> {
    actor.recent(&app).await?;
    domain_change(&app, &actor.principal, &input.host, Some(input.bucket_id)).await?;
    Ok((
        StatusCode::CREATED,
        Json(Domain {
            host: input.host,
            bucket_id: input.bucket_id,
        }),
    ))
}

#[utoipa::path(
    delete,
    operation_id = "domain_delete",
    path = "/api/v1/domains/{host}",
    params(("host" = String, Path)),
    responses((status = 204))
)]
pub(super) async fn remove_domain(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(host): Path<String>,
) -> Result<StatusCode, HttpError> {
    actor.recent(&app).await?;
    domain_change(&app, &actor.principal, &host, None).await?;
    Ok(StatusCode::NO_CONTENT)
}
async fn settings(app: &App, principal: &Principal, bucket: Uuid) -> Result<BucketSettings> {
    principal
        .require(app.authorization.as_ref(), bucket, Action::Settings)
        .await?;
    let (row, domains) = app.buckets.settings(bucket).await?;
    let mut view = BucketView::from(row);
    view.actions = principal
        .actions(app.authorization.as_ref(), bucket)
        .await?
        .iter()
        .map(|a| a.name().into())
        .collect();
    Ok(BucketSettings {
        bucket: view,
        domains,
    })
}
#[utoipa::path(post,operation_id="bucket_create",path="/api/v1/buckets",request_body=CreateBucket,responses((status=201,body=BucketView)))]
pub(super) async fn create(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<CreateBucket>,
) -> Result<(StatusCode, Json<BucketView>), HttpError> {
    Ok((
        StatusCode::CREATED,
        Json(create_bucket(&app, &actor.principal, input).await?.into()),
    ))
}
#[utoipa::path(get,operation_id="bucket_creation_options",path="/api/v1/bucket-projects",responses((status=200,body=BucketCreationOptions)))]
pub(super) async fn options(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<BucketCreationOptions>, HttpError> {
    let (projects, storages) = app
        .buckets
        .creation_options(
            actor.admin,
            actor.id,
            matches!(actor.principal, Principal::User { .. }),
        )
        .await?;
    Ok(Json(BucketCreationOptions { projects, storages }))
}
#[utoipa::path(get,operation_id="bucket_settings",path="/api/v1/buckets/{bucket}/settings",params(("bucket"=Uuid,Path)),responses((status=200,body=BucketSettings)))]
pub(super) async fn get(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
) -> Result<Json<BucketSettings>, HttpError> {
    Ok(Json(settings(&app, &actor.principal, bucket).await?))
}
#[utoipa::path(put,operation_id="bucket_settings_save",path="/api/v1/buckets/{bucket}/settings",params(("bucket"=Uuid,Path)),request_body=SettingsInput,responses((status=200,body=BucketSettings)))]
pub(super) async fn save(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Json(input): Json<SettingsInput>,
) -> Result<Json<BucketSettings>, HttpError> {
    app.writable()?;
    let rules = serde_json::to_value(&input.cors)?;
    crate::http::validate_cors(&rules).map_err(|_| s3s::s3_error!(InvalidArgument))?;
    super::Website {
        website_enabled: input.website_enabled,
        index_document: input.index_document.clone(),
        error_document: input.error_document.clone(),
    }
    .validate()?;
    let url = public_url(&input.public_base_url)?;
    let domains = input
        .domains
        .map(|values| {
            if values.len() > 100 {
                return Err(s3s::s3_error!(InvalidArgument).into());
            }
            let mut set = HashSet::new();
            values
                .into_iter()
                .map(|v| {
                    let value = host(&v)?;
                    if !set.insert(value.clone()) {
                        return Err(s3s::s3_error!(InvalidArgument).into());
                    }
                    Ok(value)
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?;
    app.buckets
        .save_settings(
            &actor.principal,
            bucket,
            BucketUpdate {
                revision: revision(&input.revision)?,
                cors: rules,
                website_enabled: input.website_enabled,
                index_document: input.index_document,
                error_document: input.error_document,
                public_base_url: url,
                uploads_paused: input.uploads_paused,
                domains,
            },
        )
        .await?;
    Ok(Json(settings(&app, &actor.principal, bucket).await?))
}
#[utoipa::path(delete,operation_id="bucket_delete",path="/api/v1/buckets/{bucket}",params(("bucket"=Uuid,Path)),request_body=ConfirmBucket,responses((status=204)))]
pub(super) async fn delete(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Json(input): Json<ConfirmBucket>,
) -> Result<StatusCode, HttpError> {
    actor.recent(&app).await?;
    delete_bucket(
        &app,
        &actor.principal,
        bucket,
        &input.confirm_name,
        Some(&input.confirmation),
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}
#[utoipa::path(post,operation_id="bucket_purge_preview",path="/api/v1/buckets/{bucket}/purge/preview",params(("bucket"=Uuid,Path)),responses((status=200,body=BucketPreview)))]
pub(super) async fn purge_preview(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
) -> Result<Json<BucketPreview>, HttpError> {
    let bucket = app.buckets.impact(&actor.principal, bucket).await?;
    let confirmation = fingerprint(&bucket, None)?;
    Ok(Json(BucketPreview {
        bucket,
        confirmation,
    }))
}
#[utoipa::path(post,operation_id="bucket_purge",path="/api/v1/buckets/{bucket}/purge",params(("bucket"=Uuid,Path)),request_body=ConfirmBucket,responses((status=200,body=BucketTask)))]
pub(super) async fn purge(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Json(input): Json<ConfirmBucket>,
) -> Result<Json<BucketTask>, HttpError> {
    actor.recent(&app).await?;
    Ok(Json(serde_json::from_value(
        app.purge_start(
            &actor.principal,
            &input.confirm_name,
            bucket,
            Some(&input.confirmation),
        )
        .await?,
    )?))
}

#[utoipa::path(post,operation_id="bucket_transfer_preview",path="/api/v1/buckets/{bucket}/transfer/preview",params(("bucket"=Uuid,Path)),request_body=TransferTarget,responses((status=200,body=TransferPreview)))]
pub(super) async fn transfer_preview(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Json(input): Json<TransferTarget>,
) -> Result<Json<TransferPreview>, HttpError> {
    let (bucket, target) = app
        .buckets
        .transfer_preview(&actor.principal, bucket, input.target_project)
        .await?;
    let confirmation = fingerprint(&bucket, Some(target.id))?;
    Ok(Json(TransferPreview {
        bucket,
        target,
        confirmation,
    }))
}
#[utoipa::path(post,operation_id="bucket_transfer",path="/api/v1/buckets/{bucket}/transfer",params(("bucket"=Uuid,Path)),request_body=TransferInput,responses((status=200,body=BucketView)))]
pub(super) async fn transfer(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Json(input): Json<TransferInput>,
) -> Result<Json<BucketView>, HttpError> {
    actor.recent(&app).await?;
    app.writable()?;
    let _coord = app.coord.lock().await;
    app.writable()?;
    Ok(Json(
        app.buckets
            .transfer(&actor.principal, bucket, input)
            .await?
            .into(),
    ))
}
