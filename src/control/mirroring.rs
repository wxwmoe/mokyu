use super::Identity;
use crate::{
    app::App,
    authorization::{Action, Principal},
    content::codec,
    http::HttpError,
    model::mirroring::{Preview, PreviewInput, Source, Update, View},
};
use anyhow::{Result, ensure};
use axum::{
    Json,
    extract::{Extension, Path, State},
};
use std::sync::Arc;
use uuid::Uuid;

pub(crate) async fn view(app: &App, principal: &Principal, bucket: Uuid) -> Result<View> {
    principal
        .require(app.authorization.as_ref(), bucket, Action::Settings)
        .await?;
    let mut view = app.mirroring_config(bucket).await?;
    view.config.redact();
    Ok(view)
}

pub(crate) async fn configure(
    app: &App,
    principal: &Principal,
    admin: bool,
    bucket: Uuid,
    mut input: Update,
) -> Result<View> {
    app.writable()?;
    principal
        .require(app.authorization.as_ref(), bucket, Action::Settings)
        .await?;
    ensure!(input.revision >= 0, s3s::s3_error!(InvalidArgument));
    let current = app.mirroring_config(bucket).await?;
    ensure!(
        input.revision == current.revision,
        s3s::s3_error!(PreconditionFailed)
    );
    input
        .config
        .retain_headers(&current.config)
        .and_then(|_| input.config.validate())
        .map_err(|_| s3s::s3_error!(InvalidArgument, "invalid mirroring configuration"))?;
    let (target, _, _) = app.buckets.settings(bucket).await?;
    for rule in &input.config.rules {
        if rule.public_read {
            principal
                .require(app.authorization.as_ref(), bucket, Action::Acl)
                .await?;
        }
        for source in std::iter::once(&rule.source).chain(rule.backup.iter()) {
            match source {
                Source::Http { allow_private, .. } => {
                    ensure!(!allow_private || admin, s3s::s3_error!(AccessDenied))
                }
                Source::S3 { external_id, .. } => {
                    let source = app.external_buckets.view(principal, *external_id).await?;
                    ensure!(
                        source.enabled
                            && source.project_id == target.project_id
                            && source.actions.contains(&Action::Read),
                        s3s::s3_error!(AccessDenied)
                    );
                }
            }
        }
    }
    let revision = input
        .revision
        .checked_add(1)
        .ok_or_else(|| s3s::s3_error!(InvalidArgument))?;
    let encrypted = codec::protect(
        &serde_json::to_vec(&input.config)?,
        &app.secrets.credential_key,
        format!("bucket-mirroring-{bucket}-{revision}").as_bytes(),
    )?;
    let _cache = app.public_reads.changing();
    app.mirroring
        .save(principal, bucket, input.revision, &input.config, encrypted)
        .await?;
    input.config.redact();
    Ok(View {
        revision,
        config: input.config,
    })
}

#[utoipa::path(get, path="/api/v1/buckets/{bucket}/mirroring", operation_id="bucket_mirroring", params(("bucket"=Uuid, Path)), responses((status=200, body=View)))]
pub(super) async fn get(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
) -> Result<Json<View>, HttpError> {
    Ok(Json(view(&app, &actor.principal, bucket).await?))
}

#[utoipa::path(put, path="/api/v1/buckets/{bucket}/mirroring", operation_id="bucket_mirroring_save", params(("bucket"=Uuid, Path)), request_body=Update, responses((status=200, body=View)))]
pub(super) async fn save(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Json(input): Json<Update>,
) -> Result<Json<View>, HttpError> {
    actor.recent(&app).await?;
    Ok(Json(
        configure(&app, &actor.principal, actor.admin, bucket, input).await?,
    ))
}

#[utoipa::path(post, path="/api/v1/buckets/{bucket}/mirroring/preview", operation_id="bucket_mirroring_preview", params(("bucket"=Uuid, Path)), request_body=PreviewInput, responses((status=200, body=Preview)))]
pub(super) async fn preview(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Json(input): Json<PreviewInput>,
) -> Result<Json<Preview>, HttpError> {
    actor
        .principal
        .require(app.authorization.as_ref(), bucket, Action::Settings)
        .await?;
    Ok(Json(input.config.preview(&input.key).map_err(|_| {
        s3s::s3_error!(InvalidArgument, "invalid mirroring rule or object key")
    })?))
}
