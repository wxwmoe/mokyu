use super::Identity;
use crate::model::Id;
use crate::{
    app::App,
    http::{HttpError, problem},
};
use anyhow::Result;
use axum::{
    Json,
    extract::{Extension, Path, Query, State},
    http::StatusCode,
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Deserialize, Serialize, ToSchema)]
pub(super) struct Job {
    id: Uuid,
    task_type: String,
    policy: String,
    #[serde(rename = "status")]
    state: String,
    processed: String,
    created_by: String,
    source: String,
    bucket_id: Option<Uuid>,
    bucket_name: Option<String>,
    created_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    updated_at: DateTime<Utc>,
    error: Option<String>,
    detail: Value,
    blocked: Option<String>,
}
fn job(mut value: Value) -> Result<Job> {
    if let Some(detail) = value["detail"].as_object_mut() {
        detail.remove("source_operation");
        detail.remove("source_inputs");
        detail.remove("sweep_page");
        detail.remove("node_pack");
    }
    let task_type = value["task_type"].as_str().unwrap_or("unknown").to_owned();
    value["task_type"] = json!(task_type);
    value["status"] = value["state"].clone();
    let created_by = value["created_by_label"]
        .as_str()
        .or_else(|| value["source"].as_str())
        .unwrap_or("system");
    value["created_by"] = json!(created_by);
    value["processed"] = json!(
        value["processed"]
            .as_i64()
            .map(|n| n.to_string())
            .unwrap_or_else(|| value["processed"].as_str().unwrap_or("0").into())
    );
    if !matches!(
        value["state"].as_str(),
        Some("queued" | "running" | "paused" | "failed")
    ) {
        value["blocked"] = Value::Null;
    }
    value["policy"] = value["detail"]["policy"]
        .as_str()
        .map(|s| json!(s))
        .unwrap_or_else(|| json!(task_type));
    Ok(serde_json::from_value(value)?)
}
#[derive(Deserialize, Serialize, ToSchema)]
pub(super) struct Policy {
    policy: String,
    paused: bool,
    enabled: bool,
    interval_seconds: u64,
    next_run_at: Option<DateTime<Utc>>,
    active: Option<Job>,
    latest: Option<Job>,
}
#[derive(Deserialize, Serialize, ToSchema)]
pub(super) struct MaintenanceStatus {
    maintenance: bool,
    controls: Vec<Policy>,
    concurrency: usize,
    pack_configured: bool,
    pack_creation_paused: bool,
    pack_creation_enabled: bool,
    preparing_packs: String,
    active_operations: usize,
    backend_prefixes: Vec<BackendPrefix>,
    profiles: Vec<crate::storage::profile::StorageProfile>,
}
#[derive(Deserialize, Serialize, ToSchema)]
struct BackendPrefix {
    id: Uuid,
    prefix: String,
}
#[cfg(test)]
mod tests {
    use super::job;
    use serde_json::json;

    #[test]
    fn job_normalizes_current_task_fields() {
        let job = job(json!({
            "id": "00000000-0000-0000-0000-000000000001",
            "task_type": "cleanup",
            "state": "completed",
            "created_by_label": "Scheduler",
            "source": "system",
            "processed": 0,
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
            "started_at": null,
            "bucket_id": null,
            "bucket_name": null,
            "error": null,
            "detail": {"role": "child", "source_operation": {"principal":"private"}, "source_inputs": {"1":[1,0,null,null]}, "sweep_page": {"items":["private"]}},
            "blocked": null,
        }))
        .unwrap();
        assert_eq!(job.task_type, "cleanup");
        assert_eq!(job.policy, "cleanup");
        assert_eq!(job.created_by, "Scheduler");
        assert_eq!(job.processed, "0");
        assert_eq!(job.detail, json!({"role":"child"}));
    }
}
async fn guard(app: &App, actor: &Identity) -> Result<Box<dyn crate::model::AdminGuard>> {
    app.authorization.admin(&actor.principal).await
}
#[utoipa::path(get,operation_id="maintenance_status",path="/api/v1/maintenance",responses((status=200,body=MaintenanceStatus)))]
pub(super) async fn status(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<MaintenanceStatus>, HttpError> {
    let access = guard(&app, &actor).await?;
    let mut value = app.maintenance_status().await?;
    for control in value["controls"].as_array_mut().unwrap() {
        for key in ["active", "latest"] {
            if !control[key].is_null() {
                control[key] = serde_json::to_value(job(control[key].take())?)?;
            }
        }
    }
    access.finish().await?;
    Ok(Json(serde_json::from_value(value)?))
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct PolicyAction {
    action: String,
}
#[derive(Deserialize, Serialize, ToSchema)]
pub(super) struct TaskStarted {
    pub task_id: Uuid,
}
#[derive(Serialize, ToSchema)]
pub(super) struct MaintenanceResult {
    task_id: Option<Uuid>,
}
#[utoipa::path(post,path="/api/v1/maintenance/{policy}/actions",params(("policy"=String,Path)),request_body=PolicyAction,responses((status=200,body=MaintenanceResult)))]
pub(super) async fn policy(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(policy): Path<String>,
    Json(input): Json<PolicyAction>,
) -> Result<Json<MaintenanceResult>, HttpError> {
    let mut access = guard(&app, &actor).await?;
    let result = match input.action.as_str() {
        "pause" => app.maintenance_change(&policy, true).await?,
        "resume" => app.maintenance_change(&policy, false).await?,
        "run" => app.maintenance_start(&policy).await?,
        _ => return Err(s3s::s3_error!(InvalidArgument).into()),
    };
    access
        .checkpoint(
            "maintenance.action",
            &policy,
            json!({"action":input.action,"task_id":result.get("task_id")}),
        )
        .await?;
    access.finish().await?;
    Ok(Json(MaintenanceResult {
        task_id: result
            .get("task_id")
            .map(|v| serde_json::from_value(v.clone()))
            .transpose()?,
    }))
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct Toggle {
    enabled: bool,
}
#[utoipa::path(post,path="/api/v1/maintenance/mode",request_body=Toggle,responses((status=204)))]
pub(super) async fn mode(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<Toggle>,
) -> Result<StatusCode, HttpError> {
    actor.recent(&app).await?;
    let access = guard(&app, &actor).await?;
    super::operations::execute_as(
        &app,
        &actor.principal,
        crate::cli::Command::Maintenance(if input.enabled {
            crate::cli::Maintenance::Enable
        } else {
            crate::cli::Maintenance::Disable
        }),
    )
    .await?;
    access.finish().await?;
    Ok(StatusCode::NO_CONTENT)
}
#[utoipa::path(post,path="/api/v1/maintenance/pack-creation",request_body=Toggle,responses((status=204)))]
pub(super) async fn creation(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<Toggle>,
) -> Result<StatusCode, HttpError> {
    actor.recent(&app).await?;
    let access = guard(&app, &actor).await?;
    app.pack_creation_change(!input.enabled).await?;
    access.finish().await?;
    Ok(StatusCode::NO_CONTENT)
}
#[utoipa::path(post,path="/api/v1/maintenance/flush",responses((status=200,body=TaskStarted)))]
pub(super) async fn flush(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<TaskStarted>, HttpError> {
    let access = guard(&app, &actor).await?;
    let value = app.cache_flush_start().await?;
    access.finish().await?;
    Ok(Json(serde_json::from_value(value)?))
}
#[derive(Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct PackScope {
    pack_id: Option<String>,
    #[serde(default)]
    all: bool,
}
impl PackScope {
    fn id(&self, backend: crate::config::DatabaseType) -> Result<Option<Id>> {
        let id = self
            .pack_id
            .as_ref()
            .map(|v| Id::parse(v, backend))
            .transpose()
            .map_err(|_| s3s::s3_error!(InvalidArgument))?;
        if self.all == id.is_some() || id.is_some_and(|v| !v.is_valid()) {
            return Err(s3s::s3_error!(InvalidArgument).into());
        }
        Ok(id)
    }
}
pub(super) use crate::model::maintenance::MaintenancePreview;
use crate::model::source::{SourceOperation, SourcePage, SourceQuery, SourceScope};

#[utoipa::path(get,path="/api/v1/maintenance/quarantines",params(SourceQuery),responses((status=200,body=SourcePage)))]
pub(super) async fn quarantines(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Query(query): Query<SourceQuery>,
) -> Result<Json<SourcePage>, HttpError> {
    Ok(Json(app.source_list(&actor.principal, query).await?))
}

async fn source_preview(
    app: &App,
    actor: &Identity,
    scope: SourceScope,
    recheck: bool,
) -> Result<MaintenancePreview> {
    let operation = app.source_prepare(&actor.principal, scope, recheck).await?;
    let action = if recheck { "recheck" } else { "quarantine" };
    let impact = operation.snapshot.summary()?;
    let confirmation = format!(
        "{} {}",
        action.to_ascii_uppercase(),
        operation.snapshot.scope.id
    );
    let access = guard(app, actor).await?;
    let preview = app
        .maintenance_store
        .preview(
            &actor.principal,
            action,
            serde_json::to_value(operation)?,
            impact,
            confirmation,
        )
        .await?;
    access.finish().await?;
    Ok(preview)
}

#[utoipa::path(post,path="/api/v1/maintenance/quarantine/preview",request_body=SourceScope,responses((status=200,body=MaintenancePreview)))]
pub(super) async fn quarantine_preview(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(scope): Json<SourceScope>,
) -> Result<Json<MaintenancePreview>, HttpError> {
    Ok(Json(source_preview(&app, &actor, scope, false).await?))
}

#[utoipa::path(post,path="/api/v1/maintenance/recheck/preview",request_body=SourceScope,responses((status=200,body=MaintenancePreview)))]
pub(super) async fn recheck_preview(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(scope): Json<SourceScope>,
) -> Result<Json<MaintenancePreview>, HttpError> {
    Ok(Json(source_preview(&app, &actor, scope, true).await?))
}
async fn unpack_impact(app: &App, input: &PackScope) -> Result<Value> {
    let id = input.id(app.config.database.backend)?;
    app.unpack_start(id, input.all, false).await
}
#[utoipa::path(post,path="/api/v1/maintenance/unpack/preview",request_body=PackScope,responses((status=200,body=MaintenancePreview)))]
pub(super) async fn unpack_preview(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<PackScope>,
) -> Result<Json<MaintenancePreview>, HttpError> {
    let access = guard(&app, &actor).await?;
    let impact = unpack_impact(&app, &input).await?;
    let phrase = if input.all {
        "UNPACK ALL".into()
    } else {
        format!("UNPACK {}", input.id(app.config.database.backend)?.unwrap())
    };
    let result = app
        .maintenance_store
        .preview(
            &actor.principal,
            "unpack",
            serde_json::to_value(input)?,
            impact,
            phrase,
        )
        .await?;
    access.finish().await?;
    Ok(Json(result))
}
#[derive(Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct SweepRequest {
    older_than: String,
    storage_id: Option<Uuid>,
}
#[utoipa::path(post,path="/api/v1/maintenance/sweep",request_body=SweepRequest,responses((status=200,body=TaskStarted)))]
pub(super) async fn sweep(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<SweepRequest>,
) -> Result<Json<TaskStarted>, HttpError> {
    let access = guard(&app, &actor).await?;
    let result = app
        .sweep_start(false, None, None, &input.older_than, input.storage_id)
        .await?;
    access.finish().await?;
    Ok(Json(serde_json::from_value(result)?))
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct SweepTask {
    task_id: Uuid,
}
async fn sweep_impact(app: &App, id: Uuid) -> Result<Value> {
    let detail = app
        .task_control
        .sweep_preview(id)
        .await?
        .ok_or_else(|| s3s::s3_error!(InvalidArgument))?;
    let storage_id: Uuid = serde_json::from_value(detail["storage_id"].clone())?;
    let minimum = app
        .storage_profiles
        .load(storage_id)
        .await?
        .min_storage_seconds()?;
    Ok(
        json!({"task_id":id,"summary":detail,"backend_prefix":detail["prefix"],"minimum_storage_seconds":minimum}),
    )
}
#[utoipa::path(post,path="/api/v1/maintenance/sweep/preview",request_body=SweepTask,responses((status=200,body=MaintenancePreview)))]
pub(super) async fn sweep_preview(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<SweepTask>,
) -> Result<Json<MaintenancePreview>, HttpError> {
    let access = guard(&app, &actor).await?;
    let impact = sweep_impact(&app, input.task_id).await?;
    let prefix = impact["backend_prefix"].as_str().unwrap_or("").to_owned();
    let phrase = if prefix.is_empty() {
        "/".into()
    } else {
        prefix
    };
    let result = app
        .maintenance_store
        .preview(
            &actor.principal,
            "sweep",
            json!({"task_id":input.task_id}),
            impact,
            phrase,
        )
        .await?;
    access.finish().await?;
    Ok(Json(result))
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct ExecutePreview {
    preview_id: Uuid,
    confirmation: String,
}
#[utoipa::path(post,path="/api/v1/maintenance/{operation}/execute",params(("operation"=String,Path)),request_body=ExecutePreview,responses((status=200,body=TaskStarted)))]
pub(super) async fn execute(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(operation): Path<String>,
    Json(input): Json<ExecutePreview>,
) -> Result<Json<TaskStarted>, HttpError> {
    actor.recent(&app).await?;
    let execution = app
        .maintenance_store
        .execution(&actor.principal, input.preview_id, &operation)
        .await?
        .ok_or_else(|| problem(StatusCode::CONFLICT, "PreviewExpired").0)?;
    let crate::model::maintenance::MaintenanceExecution {
        parameters,
        fingerprint,
        task,
        completion,
    } = execution;
    if parameters["confirmation"] != input.confirmation {
        return Err(problem(
            StatusCode::PRECONDITION_FAILED,
            "ConfirmationMismatch",
        ));
    }
    if let Some(task_id) = task {
        completion.finish(task_id, false).await?;
        return Ok(Json(TaskStarted { task_id }));
    }
    let result = if matches!(operation.as_str(), "quarantine" | "recheck") {
        let bound: SourceOperation = serde_json::from_value(parameters["input"].clone())?;
        if bound.code.is_some() != (operation == "quarantine") {
            return Err(s3s::s3_error!(InvalidArgument).into());
        }
        crate::worker::REQUEST_ID
            .scope(input.preview_id, app.source_start(bound))
            .await?
    } else if operation == "unpack" {
        let scope: PackScope = serde_json::from_value(parameters["input"].clone())?;
        let impact = unpack_impact(&app, &scope).await?;
        if fingerprint
            != blake3::hash(&serde_json::to_vec(&impact)?)
                .to_hex()
                .as_str()
        {
            return Err(s3s::s3_error!(PreconditionFailed).into());
        }
        crate::worker::REQUEST_ID
            .scope(
                input.preview_id,
                app.unpack_checked(
                    scope.id(app.config.database.backend)?,
                    scope.all,
                    true,
                    Some(&fingerprint),
                ),
            )
            .await?
    } else if operation == "sweep" {
        let id: Uuid = serde_json::from_value(parameters["input"]["task_id"].clone())?;
        let impact = sweep_impact(&app, id).await?;
        if fingerprint
            != blake3::hash(&serde_json::to_vec(&impact)?)
                .to_hex()
                .as_str()
        {
            return Err(s3s::s3_error!(PreconditionFailed).into());
        }
        let seconds = impact["summary"]["older_than_seconds"]
            .as_u64()
            .ok_or_else(|| s3s::s3_error!(InvalidArgument))?;
        let storage_id: Uuid = impact["summary"]["storage_id"]
            .as_str()
            .ok_or_else(|| s3s::s3_error!(InvalidArgument))?
            .parse()?;
        let prefix = impact["backend_prefix"]
            .as_str()
            .ok_or_else(|| s3s::s3_error!(InvalidArgument))?;
        crate::worker::REQUEST_ID
            .scope(
                input.preview_id,
                app.sweep_start(
                    true,
                    Some(id),
                    Some(prefix),
                    &format!("{seconds}s"),
                    Some(storage_id),
                ),
            )
            .await?
    } else {
        return Err(s3s::s3_error!(InvalidArgument).into());
    };
    let result: TaskStarted = serde_json::from_value(result)?;
    completion.finish(result.task_id, true).await?;
    Ok(Json(result))
}

pub(super) use crate::model::task::JobQuery;
#[derive(Deserialize, Serialize)]
struct Cursor {
    filter: String,
    at: DateTime<Utc>,
    id: Uuid,
}
#[derive(Serialize, ToSchema)]
pub(super) struct JobPage {
    #[serde(rename = "items")]
    tasks: Vec<Job>,
    #[serde(rename = "next_cursor")]
    next_token: Option<String>,
}
#[utoipa::path(get,path="/api/v1/tasks",params(JobQuery),responses((status=200,body=JobPage)))]
pub(super) async fn jobs(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Query(q): Query<JobQuery>,
) -> Result<Json<JobPage>, HttpError> {
    let limit = q.limit.unwrap_or(50);
    if !(1..=200).contains(&limit)
        || q.state
            .as_deref()
            .is_some_and(|s| !["queued", "running", "paused", "completed", "failed"].contains(&s))
        || q.policy.as_ref().is_some_and(|s| s.len() > 32)
        || q.actor.as_ref().is_some_and(|s| s.len() > 128)
        || q.token.as_ref().is_some_and(|s| s.len() > 4096)
    {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let filter = serde_json::to_string(&json!([q.state, q.policy, q.bucket, q.storage, q.actor]))?;
    let cursor = q
        .token
        .as_ref()
        .map(|s| -> Result<Cursor> { Ok(serde_json::from_slice(&URL_SAFE_NO_PAD.decode(s)?)?) })
        .transpose()
        .map_err(|_| s3s::s3_error!(InvalidArgument))?;
    if cursor.as_ref().is_some_and(|c| c.filter != filter) {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let values = app
        .task_control
        .page(
            &actor.principal,
            q,
            cursor.map(|value| (value.at, value.id)),
            limit + 1,
        )
        .await?;
    let mut tasks = values.into_iter().map(job).collect::<Result<Vec<_>>>()?;
    let next_token = if tasks.len() > limit as usize {
        let last = &tasks[limit as usize - 1];
        Some(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&Cursor {
            filter,
            at: last.created_at,
            id: last.id,
        })?))
    } else {
        None
    };
    tasks.truncate(limit as usize);
    Ok(Json(JobPage { tasks, next_token }))
}
#[utoipa::path(get,path="/api/v1/tasks/{id}",params(("id"=Uuid,Path)),responses((status=200,body=Job)))]
pub(super) async fn task(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<Json<Job>, HttpError> {
    let value = app.task_control.view(&actor.principal, id).await?;
    Ok(Json(job(value)?))
}
#[utoipa::path(post,path="/api/v1/tasks/{id}/actions",params(("id"=Uuid,Path)),request_body=PolicyAction,responses((status=204)))]
pub(super) async fn task_action(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
    Json(input): Json<PolicyAction>,
) -> Result<StatusCode, HttpError> {
    if !["pause", "resume"].contains(&input.action.as_str()) {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let mut access = guard(&app, &actor).await?;
    app.task_change(id, input.action == "resume").await?;
    access
        .checkpoint(
            "task.action",
            &id.to_string(),
            json!({"action":input.action}),
        )
        .await?;
    access.finish().await?;
    Ok(StatusCode::NO_CONTENT)
}
