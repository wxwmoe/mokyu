use super::{Identity, media::ObjectMetadata};
use crate::model::object::{
    MediaAction, ObjectTransfer, Outcome, RECEIPT, Receipt, StorageMigration,
};
use crate::{
    app::App,
    authorization::{Action, Permit},
    http::{HttpError, problem},
};
use anyhow::{Context, Result};
use axum::{
    Json,
    extract::{Extension, State},
    http::{HeaderValue, StatusCode},
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::HashSet, sync::Arc};
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct Item {
    client_id: Uuid,
    key: String,
    version: Uuid,
    target_key: Option<String>,
    target_version: Option<Uuid>,
}
#[derive(Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct Batch {
    bucket: Uuid,
    action: MediaAction,
    target_bucket: Option<Uuid>,
    #[serde(default)]
    confirm_storage_migration: bool,
    public_read: Option<bool>,
    metadata: Option<ObjectMetadata>,
    objects: Vec<Item>,
}
#[derive(Serialize, ToSchema)]
pub(super) struct ItemResult {
    client_id: Uuid,
    key: String,
    #[serde(flatten)]
    outcome: Outcome,
    replayed: bool,
}
#[derive(Serialize, ToSchema)]
pub(super) struct BatchResult {
    results: Vec<ItemResult>,
}
fn invalid() -> anyhow::Error {
    s3s::s3_error!(InvalidArgument).into()
}
fn key(value: &str) -> bool {
    !value.is_empty() && value.len() <= 1024 && !value.contains('\0')
}
fn metadata(input: &ObjectMetadata) -> Result<Value> {
    for value in [
        &input.content_type,
        &input.cache_control,
        &input.content_disposition,
        &input.content_encoding,
        &input.content_language,
        &input.expires,
    ]
    .into_iter()
    .flatten()
    {
        if value.len() > 2048 || HeaderValue::from_str(value).is_err() {
            return Err(invalid());
        }
    }
    for (name, value) in &input.user {
        if name.is_empty()
            || name.len() > 256
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
            || HeaderValue::from_str(value).is_err()
        {
            return Err(invalid());
        }
    }
    if serde_json::to_vec(input)?.len() > 8192 {
        return Err(invalid());
    }
    Ok(serde_json::to_value(input)?)
}
fn authority(actor: &Identity, input: &Batch) -> Permit {
    let mut permit = Permit::for_action(
        actor.principal.clone(),
        input.bucket,
        match input.action {
            MediaAction::Delete => Action::Delete,
            MediaAction::Private | MediaAction::PublicRead => Action::Acl,
            _ => Action::Read,
        },
    );
    match input.action {
        MediaAction::Copy | MediaAction::Move => {
            permit.add(input.target_bucket.unwrap(), Action::Write);
            if input.public_read == Some(true) {
                permit.add(input.target_bucket.unwrap(), Action::Acl);
            }
            if input.action == MediaAction::Move {
                permit.add(input.bucket, Action::Delete);
            }
        }
        MediaAction::Metadata => permit.add(input.bucket, Action::Write),
        _ => (),
    }
    permit
}
async fn check(permit: &Permit, app: &App) -> Result<()> {
    for (bucket, actions) in &permit.resources {
        for action in actions {
            permit
                .principal
                .require(app.authorization.as_ref(), *bucket, *action)
                .await?;
        }
    }
    Ok(())
}

#[utoipa::path(post,operation_id="media_actions",path="/api/v1/media/actions",request_body=Batch,responses((status=200,body=BatchResult)))]
pub(super) async fn batch(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Extension(context): Extension<crate::status::stats::RequestContext>,
    Json(input): Json<Batch>,
) -> Result<Json<BatchResult>, HttpError> {
    let copy = matches!(input.action, MediaAction::Copy | MediaAction::Move);
    if input.objects.is_empty()
        || input.objects.len() > 1000
        || copy != input.target_bucket.is_some()
        || (input.action == MediaAction::Metadata) != input.metadata.is_some()
        || input.public_read.is_some() && input.action != MediaAction::Copy
    {
        return Err(invalid().into());
    }
    if let Some(meta) = &input.metadata {
        metadata(meta)?;
    }
    let mut keys = HashSet::new();
    let mut ids = HashSet::new();
    let mut targets = HashSet::new();
    if input.objects.iter().any(|o| {
        !key(&o.key)
            || !keys.insert(&o.key)
            || !ids.insert(o.client_id)
            || copy != o.target_key.is_some()
            || !copy && o.target_version.is_some()
            || o.target_key.as_deref().is_some_and(|k| {
                !key(k)
                    || !targets.insert(k)
                    || input.target_bucket == Some(input.bucket) && k == o.key
            })
    }) {
        return Err(invalid().into());
    }
    let permit = authority(&actor, &input);
    let mut results = Vec::with_capacity(input.objects.len());
    for item in &input.objects {
        let result = apply(&app, &actor, &input, item, &permit).await;
        let (outcome, replayed) = match result {
            Ok(value) => value,
            Err(error) => (failure(error), false),
        };
        if outcome.status != 200 {
            context
                .failed
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        results.push(ItemResult {
            client_id: item.client_id,
            key: item.key.clone(),
            outcome,
            replayed,
        });
    }
    app.objects.audit_batch(input.bucket, json!({"bucket_id":input.bucket,"target_bucket_id":input.target_bucket,"operation":input.action,"count":results.len(),"failed":results.iter().filter(|r|r.outcome.status!=200).count(),"sample":results.iter().take(20).collect::<Vec<_>>()}), context.failed.load(std::sync::atomic::Ordering::Relaxed)).await?;
    Ok(Json(BatchResult { results }))
}
fn failure(error: anyhow::Error) -> Outcome {
    let response = HttpError(error).into_response();
    Outcome {
        status: response.status().as_u16(),
        code: response
            .extensions()
            .get::<crate::http::ErrorCode>()
            .map(|c| c.0.clone()),
        output_version: None,
        task_id: None,
    }
}
async fn apply(
    app: &Arc<App>,
    actor: &Identity,
    input: &Batch,
    item: &Item,
    permit: &Permit,
) -> Result<(Outcome, bool)> {
    check(permit, app).await?;
    let hash = blake3::hash(&serde_json::to_vec(&(
        input.bucket,
        input.action,
        input.target_bucket,
        input.confirm_storage_migration,
        input.public_read,
        &input.metadata,
        item,
    ))?);
    let (id, expected) = app
        .objects
        .begin_operation(&actor.principal, actor.id, item.client_id, hash.as_bytes())
        .await?;
    if expected != hash.as_bytes() {
        return Err(problem(StatusCode::CONFLICT, "IdempotencyConflict").0);
    }
    let lock = app.operation_lock(id);
    let _guard = lock.lock().await;
    let previous = app.objects.operation(id).await?;
    if let Some(previous) = previous {
        check(permit, app).await?;
        return Ok((previous, true));
    }
    let result = RECEIPT
        .scope(
            Receipt {
                id,
                bucket: input.bucket,
                key: item.key.clone(),
                action: input.action,
                count: input.objects.len(),
            },
            async {
                match input.action {
                    MediaAction::Delete | MediaAction::Private | MediaAction::PublicRead => {
                        app.change_object(
                            &actor.principal,
                            input.bucket,
                            &item.key,
                            Some(item.version),
                            match input.action {
                                MediaAction::Delete => None,
                                MediaAction::Private => Some(false),
                                _ => Some(true),
                            },
                        )
                        .await
                    }
                    MediaAction::Move => move_object(app, input, item, permit).await,
                    MediaAction::Copy | MediaAction::Metadata => {
                        copy_object(app, input, item, permit).await
                    }
                }
            },
        )
        .await;
    if let Err(error) = result {
        let outcome = failure(error);
        app.objects.fail_operation(id, &outcome).await?;
    }
    #[cfg(feature = "fault-injection")]
    crate::faults::point("media-after-operation").await;
    let result = app
        .objects
        .operation(id)
        .await?
        .context("media operation receipt is missing")?;
    Ok((result, false))
}
async fn move_object(app: &Arc<App>, input: &Batch, item: &Item, permit: &Permit) -> Result<()> {
    let target = input.target_bucket.unwrap();
    let source_target = app.objects.storage(input.bucket).await?;
    let target_target = app.objects.storage(target).await?;
    if source_target.is_none() || target_target.is_none() {
        return Err(s3s::s3_error!(NoSuchBucket).into());
    }
    if source_target != target_target {
        return copy_object(app, input, item, permit).await;
    }
    app.writable()?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("media-before-publish").await;
    let _coord = app.coord.lock().await;
    app.writable()?;
    app.objects
        .move_object(
            permit,
            ObjectTransfer {
                source_bucket: input.bucket,
                source_key: &item.key,
                source_version: item.version,
                target_bucket: target,
                target_key: item.target_key.as_deref().unwrap(),
                target_version: item.target_version,
            },
        )
        .await?;
    app.wake_gc.notify_one();
    Ok(())
}
async fn copy_object(app: &Arc<App>, input: &Batch, item: &Item, permit: &Permit) -> Result<()> {
    let _admission = app.admit(true).await?;
    let (source, _pin) = app.current(input.bucket, &item.key).await?;
    if source.id != item.version {
        return Err(s3s::s3_error!(PreconditionFailed).into());
    }
    let replace = input.action == MediaAction::Metadata;
    let (target, key, expected, public, meta) = if replace {
        (
            input.bucket,
            item.key.as_str(),
            Some(item.version),
            source.public_read,
            metadata(input.metadata.as_ref().unwrap())?,
        )
    } else {
        (
            input.target_bucket.unwrap(),
            item.target_key.as_deref().unwrap(),
            item.target_version,
            if input.action == MediaAction::Move {
                source.public_read
            } else {
                input.public_read.unwrap_or(false)
            },
            source.metadata.clone(),
        )
    };
    let source_target = app
        .objects
        .storage(input.bucket)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    let target_target = app
        .objects
        .storage(target)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    if source_target != target_target {
        if !input.confirm_storage_migration {
            return Err(problem(StatusCode::CONFLICT, "StorageMigrationConfirmationRequired").0);
        }
        let _coord = app.coord.lock().await;
        app.writable()?;
        app.objects
            .queue_migration(
                permit,
                &StorageMigration {
                    source_bucket: input.bucket,
                    source_key: item.key.clone(),
                    source_version: item.version,
                    target_bucket: target,
                    target_key: key.to_owned(),
                    target_version: expected,
                    action: if input.action == MediaAction::Move {
                        "move"
                    } else {
                        "copy"
                    }
                    .to_owned(),
                    public_read: public,
                },
            )
            .await?;
        app.wake_tasks.notify_one();
        return Ok(());
    }
    let (id, _pin) = app
        .new_stream(
            permit, target, key, "object", meta, public, false, None, None,
        )
        .await?;
    let result = async {
        app.copy_extents(id, &source).await?;
        app.finish_stream(id, source.size, &source.etag, source.checksums.clone())
            .await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("media-before-publish").await;
        let _coord = app.coord.lock().await;
        app.writable()?;
        app.objects
            .publish_copy(
                permit,
                ObjectTransfer {
                    source_bucket: input.bucket,
                    source_key: &item.key,
                    source_version: item.version,
                    target_bucket: target,
                    target_key: key,
                    target_version: expected,
                },
                id,
                public,
                replace.then_some(source.public_read),
            )
            .await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    if result.is_err() {
        let _ = app.objects.abandon_copy(id).await;
    }
    app.wake_gc.notify_one();
    result
}
