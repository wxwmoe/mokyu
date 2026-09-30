use super::{Identity, audit};
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
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

#[derive(Deserialize, Serialize, ToSchema)]
pub(super) struct Job {
    id: Uuid,
    kind: String,
    policy: String,
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
    value["policy"] = value["detail"]["kind"]
        .as_str()
        .map(|s| json!(s))
        .unwrap_or_else(|| value["kind"].clone());
    Ok(serde_json::from_value(value)?)
}
#[derive(Deserialize, Serialize, ToSchema)]
pub(super) struct Policy {
    kind: String,
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
    backend_prefix: String,
    min_storage_duration: String,
}
async fn guard<'a>(
    app: &'a App,
    actor: &Identity,
) -> Result<sqlx::Transaction<'a, sqlx::Postgres>> {
    let mut tx = app.db.begin().await?;
    actor.principal.lock_admin(&mut tx).await?;
    Ok(tx)
}
#[utoipa::path(get,operation_id="maintenance_status",path="/api/maintenance",responses((status=200,body=MaintenanceStatus)))]
pub(super) async fn status(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<MaintenanceStatus>, HttpError> {
    let tx = guard(&app, &actor).await?;
    let mut value = app.maintenance_status().await?;
    for control in value["controls"].as_array_mut().unwrap() {
        for key in ["active", "latest"] {
            if !control[key].is_null() {
                control[key] = serde_json::to_value(job(control[key].take())?)?;
            }
        }
    }
    tx.commit().await?;
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
#[utoipa::path(post,path="/api/maintenance/{kind}/actions",params(("kind"=String,Path)),request_body=PolicyAction,responses((status=200,body=MaintenanceResult)))]
pub(super) async fn policy(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(kind): Path<String>,
    Json(input): Json<PolicyAction>,
) -> Result<Json<MaintenanceResult>, HttpError> {
    let mut tx = guard(&app, &actor).await?;
    let result = match input.action.as_str() {
        "pause" => app.maintenance_change(&kind, true).await?,
        "resume" => app.maintenance_change(&kind, false).await?,
        "run" => app.maintenance_start(&kind).await?,
        _ => return Err(s3s::s3_error!(InvalidArgument).into()),
    };
    audit::checkpoint(
        &mut tx,
        "maintenance.action",
        &kind,
        json!({"action":input.action,"task_id":result.get("task_id")}),
    )
    .await?;
    tx.commit().await?;
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
#[utoipa::path(post,path="/api/maintenance/mode",request_body=Toggle,responses((status=204)))]
pub(super) async fn mode(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<Toggle>,
) -> Result<StatusCode, HttpError> {
    actor.recent(&app).await?;
    let tx = guard(&app, &actor).await?;
    super::operations::execute(
        &app,
        crate::admin::Command::Maintenance(if input.enabled {
            crate::admin::Maintenance::Enable
        } else {
            crate::admin::Maintenance::Disable
        }),
    )
    .await?;
    tx.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}
#[utoipa::path(post,path="/api/maintenance/pack-creation",request_body=Toggle,responses((status=204)))]
pub(super) async fn creation(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<Toggle>,
) -> Result<StatusCode, HttpError> {
    actor.recent(&app).await?;
    let tx = guard(&app, &actor).await?;
    app.pack_creation_change(!input.enabled).await?;
    tx.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}
#[utoipa::path(post,path="/api/maintenance/flush",responses((status=200,body=TaskStarted)))]
pub(super) async fn flush(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<TaskStarted>, HttpError> {
    let tx = guard(&app, &actor).await?;
    let value = app.cache_flush_start().await?;
    tx.commit().await?;
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
    fn id(&self) -> Result<Option<i64>> {
        let id = self
            .pack_id
            .as_ref()
            .map(|v| v.parse::<i64>())
            .transpose()
            .map_err(|_| s3s::s3_error!(InvalidArgument))?;
        if self.all == id.is_some() || id.is_some_and(|v| v < 1) {
            return Err(s3s::s3_error!(InvalidArgument).into());
        }
        Ok(id)
    }
}
#[derive(Serialize, ToSchema)]
pub(super) struct MaintenancePreview {
    id: Uuid,
    confirmation: String,
    expires_at: DateTime<Utc>,
    impact: Value,
}
async fn preview(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    actor: &Identity,
    action: &str,
    parameters: Value,
    impact: Value,
    confirmation: String,
) -> Result<MaintenancePreview> {
    // One bounded, expiring row replaces a caller's previous unexecuted preview of this action.
    sqlx::query("DELETE FROM maintenance_previews WHERE actor_id=$1 AND token_id IS NOT DISTINCT FROM $2 AND action=$3 AND task_id IS NULL").bind(actor.id).bind(actor.principal.token_id()).bind(action).execute(&mut **tx).await?;
    let id = Uuid::new_v4();
    let fingerprint = blake3::hash(&serde_json::to_vec(&impact)?)
        .to_hex()
        .to_string();
    let expires_at=sqlx::query_scalar("INSERT INTO maintenance_previews(id,actor_id,token_id,action,parameters,fingerprint) VALUES($1,$2,$3,$4,$5,$6) RETURNING expires_at")
        .bind(id).bind(actor.id).bind(actor.principal.token_id()).bind(action).bind(json!({"input":parameters,"confirmation":confirmation,"impact":impact})).bind(fingerprint).fetch_one(&mut **tx).await?;
    Ok(MaintenancePreview {
        id,
        confirmation,
        expires_at,
        impact,
    })
}
async fn unpack_impact(app: &App, input: &PackScope) -> Result<Value> {
    let id = input.id()?;
    app.unpack_start(id, input.all, false).await
}
#[utoipa::path(post,path="/api/maintenance/unpack/preview",request_body=PackScope,responses((status=200,body=MaintenancePreview)))]
pub(super) async fn unpack_preview(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<PackScope>,
) -> Result<Json<MaintenancePreview>, HttpError> {
    let mut tx = guard(&app, &actor).await?;
    let impact = unpack_impact(&app, &input).await?;
    let phrase = if input.all {
        "UNPACK ALL".into()
    } else {
        format!("UNPACK {}", input.id()?.unwrap())
    };
    let result = preview(
        &mut tx,
        &actor,
        "unpack",
        serde_json::to_value(input)?,
        impact,
        phrase,
    )
    .await?;
    tx.commit().await?;
    Ok(Json(result))
}
#[derive(Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct SweepRequest {
    older_than: String,
}
#[utoipa::path(post,path="/api/maintenance/sweep",request_body=SweepRequest,responses((status=200,body=TaskStarted)))]
pub(super) async fn sweep(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<SweepRequest>,
) -> Result<Json<TaskStarted>, HttpError> {
    let tx = guard(&app, &actor).await?;
    let result = app
        .sweep_start(false, None, None, &input.older_than)
        .await?;
    tx.commit().await?;
    Ok(Json(serde_json::from_value(result)?))
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct SweepTask {
    task_id: Uuid,
}
async fn sweep_impact(app: &App, id: Uuid) -> Result<Value> {
    let detail:Value=sqlx::query_scalar("SELECT detail FROM tasks WHERE id=$1 AND kind='sweep' AND state='completed' AND detail->>'dry_run'='true'").bind(id).fetch_optional(&app.db).await?.ok_or_else(||s3s::s3_error!(InvalidArgument))?;
    Ok(
        json!({"task_id":id,"summary":detail,"backend_prefix":app.storage.namespace(),"minimum_storage_seconds":app.config.backend.min_storage_seconds()?}),
    )
}
#[utoipa::path(post,path="/api/maintenance/sweep/preview",request_body=SweepTask,responses((status=200,body=MaintenancePreview)))]
pub(super) async fn sweep_preview(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<SweepTask>,
) -> Result<Json<MaintenancePreview>, HttpError> {
    let mut tx = guard(&app, &actor).await?;
    let impact = sweep_impact(&app, input.task_id).await?;
    let prefix = app.storage.namespace();
    let phrase = if prefix.is_empty() {
        "/".into()
    } else {
        prefix
    };
    let result = preview(
        &mut tx,
        &actor,
        "sweep",
        json!({"task_id":input.task_id}),
        impact,
        phrase,
    )
    .await?;
    tx.commit().await?;
    Ok(Json(result))
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct ExecutePreview {
    preview_id: Uuid,
    confirmation: String,
}
#[utoipa::path(post,path="/api/maintenance/{operation}/execute",params(("operation"=String,Path)),request_body=ExecutePreview,responses((status=200,body=TaskStarted)))]
pub(super) async fn execute(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(operation): Path<String>,
    Json(input): Json<ExecutePreview>,
) -> Result<Json<TaskStarted>, HttpError> {
    actor.recent(&app).await?;
    let mut tx = guard(&app, &actor).await?;
    let saved:Option<(Value,String,Option<Uuid>)>=sqlx::query_as("SELECT parameters,fingerprint,task_id FROM maintenance_previews WHERE id=$1 AND actor_id=$2 AND token_id IS NOT DISTINCT FROM $3 AND action=$4 AND expires_at>now() FOR UPDATE")
        .bind(input.preview_id).bind(actor.id).bind(actor.principal.token_id()).bind(&operation).fetch_optional(&mut *tx).await?;
    let (parameters, fingerprint, task) =
        saved.ok_or_else(|| problem(StatusCode::CONFLICT, "PreviewExpired").0)?;
    if parameters["confirmation"] != input.confirmation {
        return Err(problem(
            StatusCode::PRECONDITION_FAILED,
            "ConfirmationMismatch",
        ));
    }
    if let Some(task_id) = task {
        return Ok(Json(TaskStarted { task_id }));
    }
    if sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM tasks WHERE id=$1)")
        .bind(input.preview_id)
        .fetch_one(&mut *tx)
        .await?
    {
        sqlx::query("UPDATE maintenance_previews SET task_id=$1 WHERE id=$1")
            .bind(input.preview_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        return Ok(Json(TaskStarted {
            task_id: input.preview_id,
        }));
    }
    let result = if operation == "unpack" {
        let scope: PackScope = serde_json::from_value(parameters["input"].clone())?;
        let impact = unpack_impact(&app, &scope).await?;
        if fingerprint
            != blake3::hash(&serde_json::to_vec(&impact)?)
                .to_hex()
                .as_str()
        {
            return Err(s3s::s3_error!(PreconditionFailed).into());
        }
        crate::tasks::REQUEST_ID
            .scope(
                input.preview_id,
                app.unpack_checked(scope.id()?, scope.all, true, Some(&fingerprint)),
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
        crate::tasks::REQUEST_ID
            .scope(
                input.preview_id,
                app.sweep_start(
                    true,
                    Some(id),
                    Some(&app.storage.namespace()),
                    &format!("{seconds}s"),
                ),
            )
            .await?
    } else {
        return Err(s3s::s3_error!(InvalidArgument).into());
    };
    let result: TaskStarted = serde_json::from_value(result)?;
    sqlx::query("UPDATE maintenance_previews SET task_id=$2 WHERE id=$1")
        .bind(input.preview_id)
        .bind(result.task_id)
        .execute(&mut *tx)
        .await?;
    audit::checkpoint(
        &mut tx,
        &format!("maintenance.{operation}"),
        &result.task_id.to_string(),
        json!({"preview_id":input.preview_id}),
    )
    .await?;
    tx.commit().await?;
    Ok(Json(result))
}

#[derive(Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
pub(super) struct JobQuery {
    state: Option<String>,
    kind: Option<String>,
    bucket: Option<Uuid>,
    actor: Option<String>,
    token: Option<String>,
    limit: Option<i64>,
}
#[derive(Deserialize, Serialize)]
struct Cursor {
    filter: String,
    at: DateTime<Utc>,
    id: Uuid,
}
#[derive(Serialize, ToSchema)]
pub(super) struct JobPage {
    tasks: Vec<Job>,
    next_token: Option<String>,
}
const JOB_SQL: &str = "SELECT to_jsonb(t)||jsonb_build_object('bucket_name',b.name,'blocked',CASE WHEN c.paused OR (coalesce(c.kind,'')='gc' AND m.gc_paused) THEN 'PolicyPaused' WHEN m.maintenance AND t.kind IN ('pack','unpack','purge','gc','upload') THEN 'MaintenanceMode' WHEN m.pack_creation_paused AND t.kind='pack' AND t.detail->>'kind' IN ('pack','repack') THEN 'PackCreationStopped' END) FROM tasks t LEFT JOIN buckets b ON b.id=t.bucket_id CROSS JOIN mokyu_meta m LEFT JOIN maintenance_controls c ON c.kind=CASE WHEN t.kind='pack' THEN t.detail->>'kind' ELSE t.kind END";
#[utoipa::path(get,path="/api/tasks",params(JobQuery),responses((status=200,body=JobPage)))]
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
        || q.kind.as_ref().is_some_and(|s| s.len() > 32)
        || q.actor.as_ref().is_some_and(|s| s.len() > 128)
        || q.token.as_ref().is_some_and(|s| s.len() > 4096)
    {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let filter = serde_json::to_string(&json!([q.state, q.kind, q.bucket, q.actor]))?;
    let cursor = q
        .token
        .as_ref()
        .map(|s| -> Result<Cursor> { Ok(serde_json::from_slice(&URL_SAFE_NO_PAD.decode(s)?)?) })
        .transpose()
        .map_err(|_| s3s::s3_error!(InvalidArgument))?;
    if cursor.as_ref().is_some_and(|c| c.filter != filter) {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let mut tx = guard(&app, &actor).await?;
    sqlx::raw_sql("SET LOCAL statement_timeout='5s';SET LOCAL work_mem='4MB'")
        .execute(&mut *tx)
        .await?;
    let mut sql = sqlx::QueryBuilder::new(JOB_SQL);
    sql.push(" WHERE true");
    if let Some(value) = q.state {
        sql.push(" AND t.state=").push_bind(value);
    }
    if let Some(value) = q.kind {
        sql.push(" AND coalesce(t.detail->>'kind',t.kind)=")
            .push_bind(value);
    }
    if let Some(value) = q.bucket {
        sql.push(" AND t.bucket_id=").push_bind(value);
    }
    if let Some(value) = q.actor {
        sql.push(" AND position(lower(")
            .push_bind(value)
            .push(") in lower(t.created_by))>0");
    }
    if let Some(cursor) = cursor {
        sql.push(" AND (t.created_at,t.id)<(")
            .push_bind(cursor.at)
            .push(",")
            .push_bind(cursor.id)
            .push(")");
    }
    sql.push(" ORDER BY t.created_at DESC,t.id DESC LIMIT ")
        .push_bind(limit + 1);
    let values: Vec<Value> = sql.build_query_scalar().fetch_all(&mut *tx).await?;
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
    tx.commit().await?;
    Ok(Json(JobPage { tasks, next_token }))
}
#[utoipa::path(get,path="/api/tasks/{id}",params(("id"=Uuid,Path)),responses((status=200,body=Job)))]
pub(super) async fn task(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<Json<Job>, HttpError> {
    let mut tx = guard(&app, &actor).await?;
    let mut query = sqlx::QueryBuilder::new(JOB_SQL);
    query.push(" WHERE t.id=").push_bind(id);
    let value = query
        .build_query_scalar()
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    tx.commit().await?;
    Ok(Json(job(value)?))
}
#[utoipa::path(post,path="/api/tasks/{id}/actions",params(("id"=Uuid,Path)),request_body=PolicyAction,responses((status=204)))]
pub(super) async fn task_action(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
    Json(input): Json<PolicyAction>,
) -> Result<StatusCode, HttpError> {
    if !["pause", "resume"].contains(&input.action.as_str()) {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let mut tx = guard(&app, &actor).await?;
    app.task_change(id, input.action == "resume").await?;
    audit::checkpoint(
        &mut tx,
        "task.action",
        &id.to_string(),
        json!({"action":input.action}),
    )
    .await?;
    tx.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}
#[derive(Serialize, ToSchema)]
pub(super) struct KeyMaterial {
    secret: String,
}
#[utoipa::path(post,path="/api/service/key-material",responses((status=200,body=KeyMaterial)))]
pub(super) async fn key_material(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<KeyMaterial>, HttpError> {
    actor.recent(&app).await?;
    let mut tx = guard(&app, &actor).await?;
    let secret = crate::admin::random_secret()?;
    audit::checkpoint(
        &mut tx,
        "service.key_material",
        "new key",
        json!({"generated":true}),
    )
    .await?;
    tx.commit().await?;
    Ok(Json(KeyMaterial { secret }))
}
