use super::{
    Identity,
    storage::{self, StorageInput},
};
use crate::{
    app::App,
    authorization::Principal,
    content::codec::Key,
    http::HttpError,
    model::journal::restore::{Progress, Selection, Unlocked, View},
    storage::{
        journal::{
            segment_path,
            source::{self, Source},
        },
        marker, model,
    },
};
use anyhow::{Context, Result, ensure};
use axum::{
    Json,
    extract::{Extension, Path, State},
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, sync::Arc};
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as=StorageRecoveryProbe)]
pub(crate) struct Probe {
    pub storage: StorageInput,
    pub deployment_key: Option<String>,
}
#[derive(Serialize, ToSchema)]
#[schema(as=StorageRecoveryProbeResult)]
pub(crate) struct ProbeResult {
    pub state: String,
    pub recovery: Option<View>,
    pub storage: Option<Uuid>,
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as=StorageRecoveryStart)]
pub(crate) struct Start {
    pub revision: String,
    pub selection: Selection,
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as=StorageRecoveryAction)]
pub(crate) struct Action {
    pub revision: String,
    pub action: String,
}
fn probe_error(error: anyhow::Error, fallback: &'static str) -> anyhow::Error {
    use axum::http::StatusCode;
    let code = if error.chain().any(|e| {
        matches!(
            e.downcast_ref::<object_store::Error>(),
            Some(
                object_store::Error::PermissionDenied { .. }
                    | object_store::Error::Unauthenticated { .. }
            )
        )
    }) {
        "RecoverySourceAccess"
    } else if error.chain().any(|e| {
        e.downcast_ref::<reqwest::Error>()
            .is_some_and(|e| e.is_timeout() || e.is_connect())
    }) {
        "RecoverySourceConnection"
    } else if error.to_string().starts_with("unsupported") {
        "RecoveryUnsupported"
    } else {
        fallback
    };
    crate::http::problem(StatusCode::CONFLICT, code).0
}

fn response(state: &str, id: Option<Uuid>, recovery: Option<View>) -> ProbeResult {
    ProbeResult {
        state: state.to_owned(),
        storage: id,
        recovery,
    }
}
fn revision(value: &str) -> Result<i64> {
    value
        .parse::<i64>()
        .ok()
        .filter(|v| *v > 0)
        .context("invalid recovery revision")
}

pub(crate) async fn probe(app: &App, principal: &Principal, input: Probe) -> Result<ProbeResult> {
    app.authorization.admin(principal).await?.finish().await?;
    let info = input.storage;
    ensure!(
        info.storage_type == "s3"
            || info.storage_type == "local" && app.deployment_mode() == "single",
        "unsupported metadata recovery storage"
    );
    let name = storage::validate_name(&info.name)?;
    let tmp = Uuid::new_v4();
    let row = storage::build_record(
        app,
        tmp,
        name.clone(),
        info.storage_type,
        info.path,
        info.endpoint,
        info.bucket,
        info.region,
        storage::normalize_prefix(info.prefix)?,
        info.access_key,
        info.secret_key,
        marker::create(tmp, tmp)?,
    )?;
    let mut conn = model::Connection::unprotect(&row, &app.secrets.credential_key)?;
    if conn.r#type == "local"
        && !std::path::Path::new(conn.path.as_deref().context("missing local storage path")?)
            .try_exists()?
    {
        return Ok(response("empty", None, None));
    }
    let store = model::Store::connect(&conn, source::options())?;
    let Some(doc) = marker::Document::probe(store.backend.as_ref())
        .await
        .map_err(|e| probe_error(e, "RecoveryMarkerInvalid"))?
    else {
        return Ok(response("empty", None, None));
    };
    let identity = marker::identity(&doc.value)?;
    let sid = identity.storage_id;
    if let Some(existing) = app.storage_records.enabled_by_id(sid).await? {
        marker::verify(&doc.value, &existing.identity_marker)?;
        return Ok(response("registered", Some(sid), None));
    }
    let Some(journal) = marker::journal(&doc.value)? else {
        return Ok(response("no_journal", Some(sid), None));
    };
    let key = match journal.unlock(sid, &app.secrets.credential_key) {
        Ok(key) => key,
        Err(_) => match input.deployment_key {
            None => return Ok(response("locked", Some(sid), None)),
            Some(value) => journal
                .unlock(
                    sid,
                    &Key::new("aes-256-gcm", crate::config::file::deployment_key(&value)?)?,
                )
                .map_err(|_| {
                    crate::http::problem(axum::http::StatusCode::CONFLICT, "RecoveryUnlockFailed").0
                })?,
        },
    };
    conn.id = sid;
    conn.identity_marker = doc.value;
    let source = Source {
        connection: conn,
        journal,
        material: key.material,
    };
    let (catalog, cut) = source
        .read(store.backend.as_ref(), None)
        .await
        .map_err(|e| probe_error(e, "RecoveryIncomplete"))?;
    let mut progress = Progress::default();
    for b in catalog.buckets.values() {
        let summary = b.summary()?;
        if app.buckets.by_name(&b.name, false).await?.is_some() {
            progress.conflicts.push(b.name.clone())
        }
        progress.buckets.push(summary);
    }
    progress
        .buckets
        .sort_by(|a, b| a.name.cmp(&b.name).then(a.id.cmp(&b.id)));
    let mut required = BTreeSet::new();
    for b in catalog.buckets.values() {
        required.extend(catalog.required_keys(b)?);
    }
    progress.required_keys = u32::try_from(required.len())?;
    for id in required {
        let key = &catalog.keys[&id];
        if app.journal.recovery_key_conflict(key).await? {
            progress.conflicts.push(id.to_string())
        }
    }
    if source.connection.r#type == "s3" {
        let next = cut
            .point
            .sequence
            .checked_add(1)
            .context("journal sequence exhausted")?;
        progress.pending_uploads = source
            .client()?
            .uploads(&segment_path(cut.point.epoch, next))
            .await
            .ok()
            .and_then(|(v, more)| (!more).then_some(v.len() as u32));
    }
    let id = Uuid::new_v4();
    let unlocked = Unlocked {
        source,
        cut,
        actor: principal.user_id(),
        expires_us: (chrono::Utc::now() + chrono::TimeDelta::hours(24)).timestamp_micros(),
        storage_name: name,
        claim: None,
    };
    app.journal
        .recovery_create(
            principal,
            id,
            sid,
            &unlocked.seal(id, &app.secrets.credential_key)?,
            &progress,
        )
        .await?;
    Ok(response(
        "preview",
        Some(sid),
        Some(app.journal.recovery(id).await?.view()),
    ))
}

pub(crate) async fn status(app: &App, p: &Principal, id: Uuid) -> Result<View> {
    app.authorization.admin(p).await?.finish().await?;
    Ok(app.journal.recovery(id).await?.view())
}
pub(crate) async fn start(app: &App, p: &Principal, id: Uuid, input: Start) -> Result<View> {
    app.writable()?;
    app.authorization.admin(p).await?.finish().await?;
    let job = app.journal.recovery(id).await?;
    ensure!(
        job.state == "preview" && job.revision == revision(&input.revision)?,
        "recovery preview changed"
    );
    let unlocked = Unlocked::open(&job.source, id, &app.secrets.credential_key)?;
    ensure!(
        unlocked.actor == p.user_id()
            && chrono::Utc::now().timestamp_micros() < unlocked.expires_us,
        "recovery preview expired"
    );
    let selection = &input.selection;
    ensure!(
        !selection.buckets.is_empty()
            && selection.exclusive_takeover == selection.destination.is_none()
            && (!selection.complete_journal_upload
                || selection.exclusive_takeover && unlocked.source.connection.r#type == "s3"),
        "choose a recovery destination or confirm exclusive source ownership"
    );
    let mut names = BTreeSet::new();
    for (source, name) in &selection.buckets {
        ensure!(
            job.progress.buckets.iter().any(|b| b.id == *source)
                && s3s::path::check_bucket_name(name)
                && names.insert(name),
            "invalid recovery bucket selection"
        );
        ensure!(
            app.buckets.by_name(name, false).await?.is_none(),
            crate::model::Error::BucketAlreadyExists
        );
    }
    let target = if let Some(target) = selection.destination {
        ensure!(
            target != job.source_storage,
            "recovery copy requires an independent destination"
        );
        let target_row = app
            .storage_records
            .enabled_by_id(target)
            .await?
            .context("destination storage unavailable")?;
        ensure!(
            target_row.store_type == "s3"
                || target_row.store_type == "local" && app.deployment_mode() == "single",
            "unsupported recovery destination"
        );
        let origin = &unlocked.source.connection;
        if origin.r#type == "local" && target_row.store_type == "local" {
            let from = crate::storage::journal::local::Local::new(origin)?
                .context("missing local source")?
                .root
                .canonicalize()?;
            let backend = app.storage.store_backend(target)?;
            let to = backend
                .raw_root
                .as_deref()
                .and_then(std::path::Path::parent)
                .context("missing local destination")?
                .canonicalize()?;
            ensure!(
                !to.starts_with(&from) && !from.starts_with(&to),
                "recovery storage namespaces overlap"
            );
        }
        if origin.r#type == "s3"
            && target_row.store_type == "s3"
            && target_row.endpoint == origin.endpoint_url
            && target_row.bucket == origin.remote_bucket_name
        {
            let a = format!("{}/", target_row.prefix.trim_matches('/'));
            let b = format!("{}/", origin.prefix.trim_matches('/'));
            ensure!(
                a != "/" && b != "/" && !a.starts_with(&b) && !b.starts_with(&a),
                "recovery storage namespaces overlap"
            );
        }
        let target_backend = app.storage.store_backend(target)?;
        marker::verify_store(target_backend.backend.as_ref(), &target_row.identity_marker).await?;
        target
    } else {
        ensure!(
            app.storage_records
                .named(&unlocked.storage_name)
                .await?
                .is_none(),
            "destination storage name exists"
        );
        job.source_storage
    };
    app.journal
        .recovery_queue(p, id, job.revision, selection, &job.source, target)
        .await?;
    app.wake_maintenance.notify_waiters();
    status(app, p, id).await
}
pub(crate) async fn control(app: &App, p: &Principal, id: Uuid, input: Action) -> Result<View> {
    app.writable()?;
    ensure!(
        matches!(input.action.as_str(), "cancel" | "retry"),
        "invalid recovery action"
    );
    app.authorization.admin(p).await?.finish().await?;
    let job = app.journal.recovery(id).await?;
    if input.action == "cancel" && !job.source.is_empty() {
        let unlocked = Unlocked::open(&job.source, id, &app.secrets.credential_key)?;
        ensure!(
            unlocked.claim.is_none(),
            "resume the recorded storage takeover"
        );
    }
    app.journal
        .recovery_control(p, id, revision(&input.revision)?, input.action == "retry")
        .await?;
    app.wake_maintenance.notify_waiters();
    status(app, p, id).await
}

pub(crate) async fn register(
    app: &App,
    unlocked: &Unlocked,
    value: serde_json::Value,
    block: Option<Uuid>,
) -> Result<()> {
    let c = &unlocked.source.connection;
    if let Some(row) = app.storage_records.enabled_by_id(c.id).await? {
        marker::verify(&value, &row.identity_marker)?;
        return Ok(());
    }
    let record = storage::build_record(
        app,
        c.id,
        unlocked.storage_name.clone(),
        c.r#type.clone(),
        c.path.clone(),
        c.endpoint_url.clone(),
        c.remote_bucket_name.clone(),
        c.region.clone(),
        c.prefix.clone(),
        c.access.clone(),
        c.secret.clone(),
        value,
    )?;
    let mut profiles = app.storage_profiles.enabled_profiles().await?;
    let mut writer = app.storage_records.creation().await?.writer;
    let profile = writer
        .prepare(&record, block, None, None, None, None)
        .await?;
    profiles.push(profile.clone());
    profile.validate_budget(&app.config, &app.config.budget_for(&profiles)?)?;
    app.storage
        .verify_store_with_profile(record, &profile)
        .await?;
    writer.finish().await?;
    app.storage.refresh_store(c.id).await
}

#[utoipa::path(post,operation_id="storage_recovery_probe",path="/api/v1/storage-recoveries/probe",request_body=Probe,responses((status=200,body=ProbeResult)))]
pub(crate) async fn probe_route(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<Probe>,
) -> Result<Json<ProbeResult>, HttpError> {
    actor.recent(&app).await?;
    Ok(Json(probe(&app, &actor.principal, input).await?))
}
#[utoipa::path(get,operation_id="storage_recovery_status",path="/api/v1/storage-recoveries/{id}",params(("id"=Uuid,Path)),responses((status=200,body=View)))]
pub(crate) async fn status_route(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<Json<View>, HttpError> {
    Ok(Json(status(&app, &actor.principal, id).await?))
}
#[utoipa::path(post,operation_id="storage_recovery_start",path="/api/v1/storage-recoveries/{id}/start",params(("id"=Uuid,Path)),request_body=Start,responses((status=200,body=View)))]
pub(crate) async fn start_route(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
    Json(input): Json<Start>,
) -> Result<Json<View>, HttpError> {
    actor.recent(&app).await?;
    Ok(Json(start(&app, &actor.principal, id, input).await?))
}
#[utoipa::path(post,operation_id="storage_recovery_action",path="/api/v1/storage-recoveries/{id}/action",params(("id"=Uuid,Path)),request_body=Action,responses((status=200,body=View)))]
pub(crate) async fn control_route(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
    Json(input): Json<Action>,
) -> Result<Json<View>, HttpError> {
    actor.recent(&app).await?;
    Ok(Json(control(&app, &actor.principal, id, input).await?))
}
#[utoipa::path(get,operation_id="storage_recovery_list",path="/api/v1/storage-recoveries",responses((status=200,body=Vec<View>)))]
pub(crate) async fn list_route(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<Vec<View>>, HttpError> {
    app.authorization
        .admin(&actor.principal)
        .await?
        .finish()
        .await?;
    let mut jobs = Vec::new();
    for id in app.journal.recovery_recent().await? {
        jobs.push(app.journal.recovery(id).await?.view());
    }
    Ok(Json(jobs))
}
