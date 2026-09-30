use super::{Identity, contract::BucketView};
use crate::{
    app::{App, Bucket},
    authorization::{Action, DEFAULT_PROJECT, Permit, Principal},
    http::{CorsRule, HttpError, problem},
};
use anyhow::Result;
use axum::{
    Json,
    extract::{Extension, Path, State},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{Postgres, Transaction};
use std::{collections::HashSet, sync::Arc};
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateBucket {
    pub name: String,
    pub project_id: Option<Uuid>,
}
#[derive(Serialize, ToSchema)]
pub(super) struct BucketSettings {
    bucket: BucketView,
    domains: Vec<String>,
}
#[derive(Serialize, sqlx::FromRow, ToSchema)]
pub(super) struct BucketProject {
    id: Uuid,
    name: String,
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
#[derive(Serialize, Deserialize, sqlx::FromRow, ToSchema)]
pub(crate) struct BucketImpact {
    pub bucket_id: Uuid,
    pub name: String,
    pub project_id: Uuid,
    pub state: String,
    pub revision: String,
    pub uploads_paused: bool,
    pub objects: String,
    pub logical_bytes: String,
    pub reserved_bytes: String,
    pub inflight_bytes: String,
    pub active_uploads: String,
    pub writing_streams: String,
    pub member_grants: String,
    pub service_grants: String,
    pub token_grants: String,
    pub domains: Vec<String>,
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
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct TransferInput {
    target_project: Uuid,
    confirm_name: String,
    confirmation: String,
}
#[derive(Serialize, sqlx::FromRow, ToSchema)]
pub(super) struct TransferProject {
    id: Uuid,
    name: String,
    used_bytes: String,
    reserved_bytes: String,
    byte_limit: Option<String>,
    bucket_count: String,
    bucket_limit: Option<String>,
    members: String,
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

fn conflict(code: &'static str) -> anyhow::Error {
    problem(StatusCode::CONFLICT, code).0
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
pub(crate) async fn lock_settings(
    tx: &mut Transaction<'_, Postgres>,
    principal: &Principal,
    bucket: Uuid,
) -> Result<()> {
    principal.lock_identity(tx).await?;
    sqlx::query("SELECT id FROM buckets WHERE id=$1 FOR UPDATE")
        .bind(bucket)
        .execute(&mut **tx)
        .await?;
    Permit::for_action(principal.clone(), bucket, Action::Settings)
        .lock(tx)
        .await
}
pub(crate) async fn create_bucket(
    app: &App,
    principal: &Principal,
    input: CreateBucket,
) -> Result<Bucket> {
    app.writable()?;
    if !s3s::path::check_bucket_name(&input.name) {
        return Err(s3s::s3_error!(InvalidBucketName).into());
    }
    let mut tx = app.db.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(super::account::USER_LOCK)
        .execute(&mut *tx)
        .await?;
    let admin = principal.lock_identity(&mut tx).await?;
    let project = input.project_id.unwrap_or(DEFAULT_PROJECT);
    let allowed: Option<bool> =
        sqlx::query_scalar("SELECT allow_bucket_create FROM projects WHERE id=$1 FOR SHARE")
            .bind(project)
            .fetch_optional(&mut *tx)
            .await?;
    let allowed = allowed.ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    let mut selected = false;
    if !admin {
        if !allowed || !matches!(principal, Principal::User { .. }) {
            return Err(s3s::s3_error!(AccessDenied).into());
        }
        let member: Option<(String, String)> = sqlx::query_as(
            "SELECT role,scope FROM project_members WHERE project_id=$1 AND user_id=$2",
        )
        .bind(project)
        .bind(principal.user_id())
        .fetch_optional(&mut *tx)
        .await?;
        let Some((role, scope)) = member else {
            return Err(s3s::s3_error!(AccessDenied).into());
        };
        if role != "maintainer" {
            return Err(s3s::s3_error!(AccessDenied).into());
        }
        selected = scope == "selected";
    }
    let row:Option<Bucket>=sqlx::query_as("INSERT INTO buckets(id,name,project_id) VALUES($1,$2,$3) ON CONFLICT(name) DO NOTHING RETURNING *").bind(Uuid::new_v4()).bind(&input.name).bind(project).fetch_optional(&mut *tx).await?;
    let row = row.ok_or_else(|| conflict("BucketAlreadyExists"))?;
    if selected {
        sqlx::query(
            "INSERT INTO member_grants(user_id,project_id,bucket_id,actions) VALUES($1,$2,$3,$4)",
        )
        .bind(principal.user_id())
        .bind(project)
        .bind(row.id)
        .bind(Action::ALL.map(|a| a.name()).as_slice())
        .execute(&mut *tx)
        .await?;
    }
    super::audit::checkpoint(
        &mut tx,
        "bucket.create",
        &row.name,
        json!({"bucket_id":row.id,"project_id":project}),
    )
    .await?;
    tx.commit().await?;
    Ok(row)
}
pub(crate) async fn lock_dependents(
    tx: &mut Transaction<'_, Postgres>,
    bucket: Uuid,
) -> Result<()> {
    // Identity rows precede the bucket, matching admitted requests and grant mutations.
    sqlx::query("SELECT id FROM web_users WHERE id IN (SELECT user_id FROM member_grants WHERE bucket_id=$1) ORDER BY id FOR UPDATE").bind(bucket).execute(&mut **tx).await?;
    sqlx::query("SELECT access_key FROM credentials WHERE access_key IN (SELECT access_key FROM grants WHERE bucket_id=$1) ORDER BY access_key FOR UPDATE").bind(bucket).execute(&mut **tx).await?;
    sqlx::query("SELECT id FROM api_tokens WHERE id IN (SELECT token_id FROM token_grants WHERE bucket_id=$1) ORDER BY id FOR UPDATE").bind(bucket).execute(&mut **tx).await?;
    Ok(())
}
pub(crate) async fn impact(
    tx: &mut Transaction<'_, Postgres>,
    bucket: Uuid,
) -> Result<BucketImpact> {
    Ok(sqlx::query_as("SELECT b.id AS bucket_id,b.name,b.project_id,b.state,b.settings_revision::text AS revision,b.uploads_paused,q.object_count::text AS objects,q.used_bytes::text AS logical_bytes,q.reserved_bytes::text,q.inflight_bytes::text,(SELECT count(*)::text FROM uploads WHERE bucket_id=b.id AND state IN ('active','completing')) AS active_uploads,(SELECT count(*)::text FROM streams WHERE bucket_id=b.id AND state='writing') AS writing_streams,(SELECT count(*)::text FROM member_grants WHERE bucket_id=b.id) AS member_grants,(SELECT count(*)::text FROM grants WHERE bucket_id=b.id) AS service_grants,(SELECT count(*)::text FROM token_grants WHERE bucket_id=b.id) AS token_grants,ARRAY(SELECT host FROM domains WHERE bucket_id=b.id ORDER BY host) AS domains FROM buckets b JOIN quota_accounts q ON q.kind='bucket' AND q.id=b.id WHERE b.id=$1").bind(bucket).fetch_optional(&mut **tx).await?.ok_or_else(||s3s::s3_error!(NoSuchBucket))?)
}
pub(crate) fn fingerprint(bucket: &BucketImpact, target: Option<Uuid>) -> Result<String> {
    Ok(blake3::hash(&serde_json::to_vec(&(bucket, target))?)
        .to_hex()
        .to_string())
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
    let mut tx = super::users::admin_transaction(app, principal).await?;
    lock_dependents(&mut tx, bucket).await?;
    let row: Bucket = sqlx::query_as("SELECT * FROM buckets WHERE id=$1 FOR UPDATE")
        .bind(bucket)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    if row.name != name {
        return Err(s3s::s3_error!(PreconditionFailed).into());
    }
    if let Some(expected) = confirmation
        && fingerprint(&impact(&mut tx, bucket).await?, None)? != expected
    {
        return Err(s3s::s3_error!(PreconditionFailed).into());
    }
    let busy:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM streams WHERE bucket_id=$1) OR EXISTS(SELECT 1 FROM uploads WHERE bucket_id=$1)").bind(bucket).fetch_one(&mut *tx).await?;
    if busy || row.state != "active" {
        return Err(conflict("BucketNotEmpty"));
    }
    sqlx::query("DELETE FROM objects WHERE bucket_id=$1")
        .bind(bucket)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM buckets WHERE id=$1")
        .bind(bucket)
        .execute(&mut *tx)
        .await?;
    super::audit::checkpoint(
        &mut tx,
        "bucket.delete",
        name,
        json!({"bucket_id":bucket,"project_id":row.project_id}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}
pub(crate) async fn domain_change(
    app: &App,
    principal: &Principal,
    value: &str,
    bucket: Option<Uuid>,
) -> Result<()> {
    app.writable()?;
    let value = host(value)?;
    let mut tx = super::users::admin_transaction(app, principal).await?;
    let old: Option<Uuid> = sqlx::query_scalar("SELECT bucket_id FROM domains WHERE host=$1")
        .bind(&value)
        .fetch_optional(&mut *tx)
        .await?;
    if bucket.is_some() && old.is_some() && old != bucket {
        return Err(conflict("DomainInUse"));
    }
    if let Some(id) = bucket.or(old) {
        let state: Option<String> =
            sqlx::query_scalar("SELECT state FROM buckets WHERE id=$1 FOR UPDATE")
                .bind(id)
                .fetch_optional(&mut *tx)
                .await?;
        if state.as_deref() != Some("active") {
            return Err(s3s::s3_error!(OperationAborted).into());
        }
    }
    if let Some(id) = bucket {
        sqlx::query(
            "INSERT INTO domains(host,bucket_id) VALUES($1,$2) ON CONFLICT(host) DO NOTHING",
        )
        .bind(&value)
        .bind(id)
        .execute(&mut *tx)
        .await?;
    } else {
        sqlx::query("DELETE FROM domains WHERE host=$1")
            .bind(&value)
            .execute(&mut *tx)
            .await?;
    }
    super::audit::checkpoint(
        &mut tx,
        "bucket.domain",
        &value,
        json!({"bucket_id":bucket.or(old),"removed":bucket.is_none()}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}
async fn settings(app: &App, principal: &Principal, bucket: Uuid) -> Result<BucketSettings> {
    principal.require(&app.db, bucket, Action::Settings).await?;
    let mut tx = app.db.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *tx)
        .await?;
    let row: Bucket = sqlx::query_as("SELECT * FROM buckets WHERE id=$1")
        .bind(bucket)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    let domains = sqlx::query_scalar("SELECT host FROM domains WHERE bucket_id=$1 ORDER BY host")
        .bind(bucket)
        .fetch_all(&mut *tx)
        .await?;
    tx.commit().await?;
    let mut view = BucketView::from(row);
    view.actions = principal
        .actions(&app.db, bucket)
        .await?
        .iter()
        .map(|a| a.name().into())
        .collect();
    Ok(BucketSettings {
        bucket: view,
        domains,
    })
}
#[utoipa::path(post,operation_id="bucket_create",path="/api/buckets",request_body=CreateBucket,responses((status=201,body=BucketView)))]
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
#[utoipa::path(get,operation_id="bucket_creation_options",path="/api/bucket-projects",responses((status=200,body=Vec<BucketProject>)))]
pub(super) async fn options(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<Vec<BucketProject>>, HttpError> {
    Ok(Json(sqlx::query_as("SELECT id,name FROM projects p WHERE $1 OR ($3 AND allow_bucket_create AND EXISTS(SELECT 1 FROM project_members WHERE project_id=p.id AND user_id=$2 AND role='maintainer')) ORDER BY builtin DESC,name LIMIT 1000")
        .bind(actor.admin).bind(actor.id).bind(matches!(actor.principal,Principal::User{..})).fetch_all(&app.db).await?))
}
#[utoipa::path(get,operation_id="bucket_settings",path="/api/buckets/{bucket}/settings",params(("bucket"=Uuid,Path)),responses((status=200,body=BucketSettings)))]
pub(super) async fn get(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
) -> Result<Json<BucketSettings>, HttpError> {
    Ok(Json(settings(&app, &actor.principal, bucket).await?))
}
#[utoipa::path(put,operation_id="bucket_settings_save",path="/api/buckets/{bucket}/settings",params(("bucket"=Uuid,Path)),request_body=SettingsInput,responses((status=200,body=BucketSettings)))]
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
    let mut tx = app.db.begin().await?;
    if domains.is_some() {
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(super::account::USER_LOCK)
            .execute(&mut *tx)
            .await?;
    }
    let admin = actor.principal.lock_identity(&mut tx).await?;
    lock_settings(&mut tx, &actor.principal, bucket).await?;
    let row: Bucket = sqlx::query_as("SELECT * FROM buckets WHERE id=$1 FOR UPDATE")
        .bind(bucket)
        .fetch_one(&mut *tx)
        .await?;
    if row.settings_revision != revision(&input.revision)? {
        return Err(s3s::s3_error!(PreconditionFailed).into());
    }
    if row.state != "active" {
        return Err(s3s::s3_error!(OperationAborted).into());
    }
    if !admin
        && (domains.is_some()
            || row.uploads_paused != input.uploads_paused
            || row.public_base_url != url)
    {
        return Err(s3s::s3_error!(AccessDenied).into());
    }
    if let Some(domains) = domains {
        let used: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM domains WHERE host=ANY($1) AND bucket_id<>$2)",
        )
        .bind(&domains)
        .bind(bucket)
        .fetch_one(&mut *tx)
        .await?;
        if used {
            return Err(conflict("DomainInUse").into());
        }
        sqlx::query("DELETE FROM domains WHERE bucket_id=$1 AND NOT(host=ANY($2))")
            .bind(bucket)
            .bind(&domains)
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO domains(host,bucket_id) SELECT unnest($1::text[]),$2 ON CONFLICT DO NOTHING").bind(domains).bind(bucket).execute(&mut *tx).await?;
    }
    sqlx::query("UPDATE buckets SET cors=$2,website_enabled=$3,index_document=$4,error_document=$5,public_base_url=$6,uploads_paused=$7 WHERE id=$1")
        .bind(bucket).bind(rules).bind(input.website_enabled).bind(input.index_document).bind(input.error_document).bind(url).bind(input.uploads_paused).execute(&mut *tx).await?;
    super::audit::checkpoint(&mut tx,"bucket.settings",&row.name,json!({"bucket_id":bucket,"project_id":row.project_id,"uploads_paused":input.uploads_paused})).await?;
    tx.commit().await?;
    Ok(Json(settings(&app, &actor.principal, bucket).await?))
}
#[utoipa::path(delete,operation_id="bucket_delete",path="/api/buckets/{bucket}",params(("bucket"=Uuid,Path)),request_body=ConfirmBucket,responses((status=204)))]
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
#[utoipa::path(post,operation_id="bucket_purge_preview",path="/api/buckets/{bucket}/purge/preview",params(("bucket"=Uuid,Path)),responses((status=200,body=BucketPreview)))]
pub(super) async fn purge_preview(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
) -> Result<Json<BucketPreview>, HttpError> {
    let mut tx = app.db.begin().await?;
    actor.principal.lock_admin(&mut tx).await?;
    let bucket = impact(&mut tx, bucket).await?;
    let confirmation = fingerprint(&bucket, None)?;
    Ok(Json(BucketPreview {
        bucket,
        confirmation,
    }))
}
#[utoipa::path(post,operation_id="bucket_purge",path="/api/buckets/{bucket}/purge",params(("bucket"=Uuid,Path)),request_body=ConfirmBucket,responses((status=200,body=BucketTask)))]
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
async fn target(tx: &mut Transaction<'_, Postgres>, id: Uuid) -> Result<TransferProject> {
    Ok(sqlx::query_as("SELECT p.id,p.name,q.used_bytes::text,q.reserved_bytes::text,q.byte_limit::text,q.bucket_count::text,q.bucket_limit::text,(SELECT count(*)::text FROM project_members WHERE project_id=p.id) AS members FROM projects p JOIN quota_accounts q ON q.kind='project' AND q.id=p.id WHERE p.id=$1").bind(id).fetch_optional(&mut **tx).await?.ok_or_else(||s3s::s3_error!(NoSuchKey))?)
}
#[utoipa::path(post,operation_id="bucket_transfer_preview",path="/api/buckets/{bucket}/transfer/preview",params(("bucket"=Uuid,Path)),request_body=TransferTarget,responses((status=200,body=TransferPreview)))]
pub(super) async fn transfer_preview(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Json(input): Json<TransferTarget>,
) -> Result<Json<TransferPreview>, HttpError> {
    let mut tx = app.db.begin().await?;
    actor.principal.lock_admin(&mut tx).await?;
    let bucket = impact(&mut tx, bucket).await?;
    let target = target(&mut tx, input.target_project).await?;
    if bucket.project_id == target.id {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let confirmation = fingerprint(&bucket, Some(target.id))?;
    Ok(Json(TransferPreview {
        bucket,
        target,
        confirmation,
    }))
}
#[utoipa::path(post,operation_id="bucket_transfer",path="/api/buckets/{bucket}/transfer",params(("bucket"=Uuid,Path)),request_body=TransferInput,responses((status=200,body=BucketView)))]
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
    let mut tx = super::users::admin_transaction(&app, &actor.principal).await?;
    let source: Uuid = sqlx::query_scalar("SELECT project_id FROM buckets WHERE id=$1")
        .bind(bucket)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    if source == input.target_project {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    sqlx::query("SELECT id FROM web_users WHERE id IN (SELECT user_id FROM project_members WHERE project_id=ANY($1)) ORDER BY id FOR UPDATE").bind([source,input.target_project].as_slice()).execute(&mut *tx).await?;
    lock_dependents(&mut tx, bucket).await?;
    sqlx::query("SELECT id FROM buckets WHERE id=$1 FOR UPDATE")
        .bind(bucket)
        .execute(&mut *tx)
        .await?;
    let current = impact(&mut tx, bucket).await?;
    if current.name != input.confirm_name
        || fingerprint(&current, Some(input.target_project))? != input.confirmation
    {
        return Err(s3s::s3_error!(PreconditionFailed).into());
    }
    if current.state != "active"
        || !current.uploads_paused
        || [
            &current.reserved_bytes,
            &current.inflight_bytes,
            &current.active_uploads,
            &current.writing_streams,
        ]
        .iter()
        .any(|s| s.as_str() != "0")
    {
        return Err(conflict("BucketNotDrained").into());
    }
    target(&mut tx, input.target_project).await?;
    sqlx::query("DELETE FROM member_grants WHERE bucket_id=$1")
        .bind(bucket)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM grants WHERE bucket_id=$1")
        .bind(bucket)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM token_grants WHERE bucket_id=$1")
        .bind(bucket)
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE web_users SET authorization_revision=authorization_revision+1 WHERE id IN (SELECT user_id FROM project_members WHERE project_id=ANY($1))").bind([source,input.target_project].as_slice()).execute(&mut *tx).await?;
    let row: Bucket = sqlx::query_as(
        "UPDATE buckets SET project_id=$2,uploads_paused=false WHERE id=$1 RETURNING *",
    )
    .bind(bucket)
    .bind(input.target_project)
    .fetch_one(&mut *tx)
    .await?;
    super::audit::checkpoint(&mut tx,"bucket.transfer",&row.name,json!({"bucket_id":bucket,"project_id":input.target_project,"previous_project":source,"revoked_member_grants":current.member_grants,"revoked_service_grants":current.service_grants,"revoked_token_grants":current.token_grants})).await?;
    tx.commit().await?;
    Ok(Json(row.into()))
}
