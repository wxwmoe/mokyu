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
    extract::{Extension, Path, Query, State},
};
use std::sync::Arc;
use uuid::Uuid;

#[derive(serde::Deserialize, utoipa::IntoParams)]
pub(crate) struct BlockQuery {
    after: Option<String>,
    limit: Option<usize>,
}

#[derive(serde::Deserialize, utoipa::IntoParams)]
pub(crate) struct StatusQuery {
    token: Option<String>,
    limit: Option<i64>,
}

pub(super) async fn status(
    app: &App,
    principal: &Principal,
    bucket: Uuid,
    token: Option<String>,
    limit: Option<i64>,
) -> Result<super::maintenance::JobPage> {
    principal
        .require(app.authorization.as_ref(), bucket, Action::Settings)
        .await?;
    // The global task view is restricted here to this bucket and this operation.
    super::maintenance::job_page(
        app,
        &Principal::System,
        crate::model::task::JobQuery {
            bucket: Some(bucket),
            policy: Some("mirroring".into()),
            token,
            limit,
            state: None,
            storage: None,
            actor: None,
        },
    )
    .await
}

#[utoipa::path(get, path="/api/v1/buckets/{bucket}/mirroring/tasks", operation_id="bucket_mirroring_tasks", params(("bucket"=Uuid, Path), StatusQuery), responses((status=200, body=super::maintenance::JobPage)))]
pub(super) async fn tasks(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Query(q): Query<StatusQuery>,
) -> Result<Json<super::maintenance::JobPage>, HttpError> {
    Ok(Json(
        status(&app, &actor.principal, bucket, q.token, q.limit).await?,
    ))
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = MirroringTaskAction)]
pub(crate) struct TaskAction {
    pub task: Uuid,
    pub resume: bool,
}

pub(crate) async fn change_task(
    app: &App,
    principal: &Principal,
    bucket: Uuid,
    input: TaskAction,
) -> Result<()> {
    principal
        .require(app.authorization.as_ref(), bucket, Action::Settings)
        .await?;
    let task = app
        .task_control
        .raw(input.task)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    ensure!(
        task["bucket_id"].as_str() == Some(bucket.to_string().as_str())
            && task["detail"]["operation"] == "mirroring",
        s3s::s3_error!(AccessDenied)
    );
    app.task_change(principal, input.task, input.resume).await?;
    Ok(())
}

#[utoipa::path(post, path="/api/v1/buckets/{bucket}/mirroring/tasks", operation_id="bucket_mirroring_task_action", params(("bucket"=Uuid, Path)), request_body=TaskAction, responses((status=204)))]
pub(super) async fn task_action(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Json(input): Json<TaskAction>,
) -> Result<axum::http::StatusCode, HttpError> {
    actor.recent(&app).await?;
    change_task(&app, &actor.principal, bucket, input).await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}

#[utoipa::path(get, path="/api/v1/buckets/{bucket}/mirroring/blocks", operation_id="bucket_mirroring_blocks", params(("bucket"=Uuid, Path), BlockQuery), responses((status=200, body=crate::model::mirroring::BlockPage)))]
pub(super) async fn blocks(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Query(query): Query<BlockQuery>,
) -> Result<Json<crate::model::mirroring::BlockPage>, HttpError> {
    let limit = query.limit.unwrap_or(50);
    if !(1..=100).contains(&limit) || query.after.as_ref().is_some_and(|key| key.len() > 1024) {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    Ok(Json(
        app.mirroring
            .blocks(&actor.principal, bucket, query.after.as_deref(), limit)
            .await?,
    ))
}

#[utoipa::path(post, path="/api/v1/buckets/{bucket}/mirroring/unblock", operation_id="bucket_mirroring_unblock", params(("bucket"=Uuid, Path)), request_body=crate::model::mirroring::Unblock, responses((status=204)))]
pub(super) async fn unblock(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Json(input): Json<crate::model::mirroring::Unblock>,
) -> Result<axum::http::StatusCode, HttpError> {
    actor.recent(&app).await?;
    app.writable()?;
    app.mirroring
        .unblock(&actor.principal, bucket, &input.key)
        .await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}

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
    validate(app, principal, admin, bucket, &mut input).await?;
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

async fn validate(
    app: &App,
    principal: &Principal,
    admin: bool,
    bucket: Uuid,
    input: &mut Update,
) -> Result<()> {
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
    Ok(())
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = MirroringProbeInput)]
pub(crate) struct ProbeInput {
    pub revision: i64,
    pub config: crate::model::mirroring::Config,
    pub key: String,
    #[serde(default)]
    pub backup: bool,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
#[schema(as = MirroringProbe)]
pub(crate) struct Probe {
    rule_id: Uuid,
    backup: bool,
    source: String,
    source_key: String,
    size: Option<u64>,
    content_type: Option<String>,
    elapsed_ms: u64,
}

pub(crate) async fn probe(
    app: &App,
    principal: &Principal,
    admin: bool,
    bucket: Uuid,
    input: ProbeInput,
) -> Result<Probe> {
    let mut update = Update {
        revision: input.revision,
        config: input.config,
    };
    validate(app, principal, admin, bucket, &mut update).await?;
    crate::model::mirroring::valid_key(&input.key).map_err(|_| s3s::s3_error!(InvalidArgument))?;
    let rule = update
        .config
        .select(&input.key)
        .ok_or_else(|| s3s::s3_error!(InvalidArgument, "no matching mirroring rule"))?;
    let source = if input.backup {
        rule.backup
            .as_ref()
            .ok_or_else(|| s3s::s3_error!(InvalidArgument, "backup origin is not configured"))?
    } else {
        &rule.source
    };
    let snapshot = if let Source::S3 { external_id, .. } = source {
        let permit =
            crate::authorization::Permit::for_action(principal.clone(), *external_id, Action::Read);
        let record = app
            .external_buckets
            .authorized(&permit)
            .await?
            .pop()
            .ok_or_else(|| s3s::s3_error!(AccessDenied))?;
        Some(crate::model::external::Snapshot::from_record(
            record,
            &app.secrets.credential_key,
        )?)
    } else {
        None
    };
    let key = rule.key(&input.key)?;
    let _network = app.storage.reads.acquire().await?;
    let started = std::time::Instant::now();
    let opened = crate::storage::origin::open(
        source,
        &update.config,
        &key,
        snapshot.as_ref(),
        axum::http::Method::HEAD,
        axum::http::HeaderMap::new(),
    )
    .await
    .map_err(
        |error| match error.downcast_ref::<s3s::S3Error>().map(|e| e.code()) {
            Some(s3s::S3ErrorCode::NoSuchKey) => {
                s3s::s3_error!(NoSuchKey, "origin object not found")
            }
            Some(s3s::S3ErrorCode::AccessDenied) => {
                s3s::s3_error!(AccessDenied, "origin authentication failed")
            }
            _ => s3s::s3_error!(ServiceUnavailable, "origin metadata test failed"),
        },
    )?;
    // A probe never consumes a body or falls back to a GET.
    Ok(Probe {
        rule_id: rule.id,
        backup: input.backup,
        source: source.address(&key)?,
        source_key: source.key(&key)?,
        size: opened.length,
        content_type: opened
            .headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned),
        elapsed_ms: started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
    })
}

#[utoipa::path(post, path="/api/v1/buckets/{bucket}/mirroring/test", operation_id="bucket_mirroring_test", params(("bucket"=Uuid, Path)), request_body=ProbeInput, responses((status=200, body=Probe)))]
pub(super) async fn test(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Json(input): Json<ProbeInput>,
) -> Result<Json<Probe>, HttpError> {
    actor.recent(&app).await?;
    Ok(Json(
        probe(&app, &actor.principal, actor.admin, bucket, input).await?,
    ))
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
