use super::{Identity, media::ObjectMetadata};
use crate::{
    app::App,
    authorization::{Action, Permit},
    http::{HttpError, problem},
};
use anyhow::{Result, ensure};
use axum::{
    Json,
    extract::{Extension, State},
    http::{HeaderValue, StatusCode},
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};
use std::{collections::HashSet, sync::Arc};
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Clone, Copy, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub(super) enum Kind {
    Delete,
    Private,
    PublicRead,
    Copy,
    Move,
    Metadata,
}
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
    action: Kind,
    target_bucket: Option<Uuid>,
    public_read: Option<bool>,
    metadata: Option<ObjectMetadata>,
    objects: Vec<Item>,
}
#[derive(Clone, Serialize, Deserialize, ToSchema)]
pub(super) struct Outcome {
    status: u16,
    code: Option<String>,
    output_version: Option<Uuid>,
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
struct Receipt {
    id: Uuid,
    bucket: Uuid,
    key: String,
    action: Kind,
    count: usize,
}
tokio::task_local! { static RECEIPT:Receipt; }

pub(crate) async fn complete(
    tx: &mut Transaction<'_, Postgres>,
    version: Option<Uuid>,
) -> Result<()> {
    if let Ok((id, bucket, key, action, count)) =
        RECEIPT.try_with(|r| (r.id, r.bucket, r.key.clone(), r.action, r.count))
    {
        let result = Outcome {
            status: 200,
            code: None,
            output_version: version,
        };
        let changed =
            sqlx::query("UPDATE media_operations SET result=$2 WHERE id=$1 AND result IS NULL")
                .bind(id)
                .bind(sqlx::types::Json(&result))
                .execute(&mut **tx)
                .await?
                .rows_affected();
        ensure!(changed == 1, "media operation receipt is missing");
        super::audit::checkpoint(
            tx,
            "object.change",
            &key,
            json!({"bucket_id":bucket,"operation":action,"version":version,"operation_id":id}),
        )
        .await?;
        if count > 1 {
            super::audit::partial(tx).await?;
        }
    }
    Ok(())
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
            Kind::Delete => Action::Delete,
            Kind::Private | Kind::PublicRead => Action::Acl,
            _ => Action::Read,
        },
    );
    match input.action {
        Kind::Copy | Kind::Move => {
            permit.add(input.target_bucket.unwrap(), Action::Write);
            if input.public_read == Some(true) {
                permit.add(input.target_bucket.unwrap(), Action::Acl);
            }
            if input.action == Kind::Move {
                permit.add(input.bucket, Action::Delete);
            }
        }
        Kind::Metadata => permit.add(input.bucket, Action::Write),
        _ => (),
    }
    permit
}
async fn check(permit: &Permit, app: &App) -> Result<()> {
    for (bucket, actions) in &permit.resources {
        for action in actions {
            permit.principal.require(&app.db, *bucket, *action).await?;
        }
    }
    Ok(())
}

#[utoipa::path(post,operation_id="media_actions",path="/api/media/actions",request_body=Batch,responses((status=200,body=BatchResult)))]
pub(super) async fn batch(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Extension(context): Extension<crate::stats::RequestContext>,
    Json(input): Json<Batch>,
) -> Result<Json<BatchResult>, HttpError> {
    let copy = matches!(input.action, Kind::Copy | Kind::Move);
    if input.objects.is_empty()
        || input.objects.len() > 1000
        || copy != input.target_bucket.is_some()
        || (input.action == Kind::Metadata) != input.metadata.is_some()
        || input.public_read.is_some() && input.action != Kind::Copy
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
    let mut tx = app.db.begin().await?;
    super::audit::checkpoint(&mut tx,"object.batch",&input.bucket.to_string(),json!({"bucket_id":input.bucket,"target_bucket_id":input.target_bucket,"operation":input.action,"count":results.len(),"failed":results.iter().filter(|r|r.outcome.status!=200).count(),"sample":results.iter().take(20).collect::<Vec<_>>()})).await?;
    if context.failed.load(std::sync::atomic::Ordering::Relaxed) {
        super::audit::partial(&mut tx).await?;
    }
    tx.commit().await?;
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
        input.public_read,
        &input.metadata,
        item,
    ))?);
    let receipt = Uuid::new_v4();
    let mut tx = app.db.begin().await?;
    actor.principal.lock_identity(&mut tx).await?;
    sqlx::query("INSERT INTO media_operations(id,user_id,token_id,client_id,request_hash) VALUES($1,$2,$3,$4,$5) ON CONFLICT(user_id,token_id,client_id) DO NOTHING")
        .bind(receipt).bind(actor.id).bind(actor.principal.token_id()).bind(item.client_id).bind(hash.as_bytes().as_slice()).execute(&mut *tx).await?;
    let (id,expected):(Uuid,Vec<u8>)=sqlx::query_as("SELECT id,request_hash FROM media_operations WHERE user_id=$1 AND token_id IS NOT DISTINCT FROM $2 AND client_id=$3")
        .bind(actor.id).bind(actor.principal.token_id()).bind(item.client_id).fetch_one(&mut *tx).await?;
    tx.commit().await?;
    if expected != hash.as_bytes() {
        return Err(problem(StatusCode::CONFLICT, "IdempotencyConflict").0);
    }
    let lock = app.operation_lock(id);
    let _guard = lock.lock().await;
    let previous: Option<Value> =
        sqlx::query_scalar("SELECT result FROM media_operations WHERE id=$1")
            .bind(id)
            .fetch_one(&app.db)
            .await?;
    if let Some(previous) = previous {
        check(permit, app).await?;
        return Ok((serde_json::from_value(previous)?, true));
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
                    Kind::Delete | Kind::Private | Kind::PublicRead => {
                        app.change_object(
                            &actor.principal,
                            input.bucket,
                            &item.key,
                            Some(item.version),
                            match input.action {
                                Kind::Delete => None,
                                Kind::Private => Some(false),
                                _ => Some(true),
                            },
                        )
                        .await
                    }
                    Kind::Move => move_object(app, input, item, permit).await,
                    Kind::Copy | Kind::Metadata => copy_object(app, input, item, permit).await,
                }
            },
        )
        .await;
    if let Err(error) = result {
        let outcome = failure(error);
        sqlx::query("UPDATE media_operations SET result=$2 WHERE id=$1 AND result IS NULL")
            .bind(id)
            .bind(sqlx::types::Json(&outcome))
            .execute(&app.db)
            .await?;
    }
    #[cfg(feature = "fault-injection")]
    crate::faults::point("media-after-operation").await;
    let result: Value = sqlx::query_scalar("SELECT result FROM media_operations WHERE id=$1")
        .bind(id)
        .fetch_one(&app.db)
        .await?;
    Ok((serde_json::from_value(result)?, false))
}
async fn scope(tx: &mut Transaction<'_, Postgres>, permit: &Permit) -> Result<()> {
    permit.lock(tx).await?;
    let buckets: Vec<Uuid> = permit.resources.keys().copied().collect();
    let blocked: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM buckets WHERE id=ANY($1) AND state<>'active')",
    )
    .bind(&buckets)
    .fetch_one(&mut **tx)
    .await?;
    if blocked {
        return Err(s3s::s3_error!(OperationAborted).into());
    }
    sqlx::query("SELECT id FROM quota_accounts WHERE kind='project' AND id IN (SELECT project_id FROM buckets WHERE id=ANY($1)) ORDER BY id FOR UPDATE").bind(&buckets).execute(&mut **tx).await?;
    sqlx::query(
        "SELECT id FROM quota_accounts WHERE kind='bucket' AND id=ANY($1) ORDER BY id FOR UPDATE",
    )
    .bind(&buckets)
    .execute(&mut **tx)
    .await?;
    Ok(())
}
async fn selected(
    tx: &mut Transaction<'_, Postgres>,
    bucket: Uuid,
    key: &str,
    expected: Option<Uuid>,
) -> Result<Option<Uuid>> {
    let actual: Option<Uuid> = sqlx::query_scalar(
        "SELECT stream_id FROM objects WHERE bucket_id=$1 AND key=$2 FOR UPDATE",
    )
    .bind(bucket)
    .bind(key)
    .fetch_optional(&mut **tx)
    .await?
    .flatten();
    if actual != expected {
        return Err(s3s::s3_error!(PreconditionFailed).into());
    }
    Ok(actual)
}
async fn move_object(app: &Arc<App>, input: &Batch, item: &Item, permit: &Permit) -> Result<()> {
    app.writable()?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("media-before-publish").await;
    let _coord = app.coord.lock().await;
    app.writable()?;
    let mut tx = app.db.begin().await?;
    scope(&mut tx, permit).await?;
    selected(&mut tx, input.bucket, &item.key, Some(item.version)).await?;
    let target = input.target_bucket.unwrap();
    let key = item.target_key.as_deref().unwrap();
    let old = selected(&mut tx, target, key, item.target_version).await?;
    let public: bool = sqlx::query_scalar("SELECT public_read FROM streams WHERE id=$1")
        .bind(item.version)
        .fetch_one(&mut *tx)
        .await?;
    if public {
        Permit::for_action(permit.principal.clone(), target, Action::Acl)
            .lock(&mut tx)
            .await?;
    }
    sqlx::query("UPDATE objects SET stream_id=NULL,write_epoch=$3 WHERE bucket_id=$1 AND key=$2")
        .bind(input.bucket)
        .bind(&item.key)
        .bind(Uuid::new_v4())
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE streams SET bucket_id=$2,object_key=$3 WHERE id=$1")
        .bind(item.version)
        .bind(target)
        .bind(key)
        .execute(&mut *tx)
        .await?;
    sqlx::query("INSERT INTO objects(bucket_id,key,stream_id,write_epoch) VALUES($1,$2,$3,$3) ON CONFLICT(bucket_id,key) DO UPDATE SET stream_id=excluded.stream_id,write_epoch=excluded.write_epoch")
        .bind(target).bind(key).bind(item.version).execute(&mut *tx).await?;
    if let Some(old) = old {
        sqlx::query("UPDATE streams SET state='retired',touched_at=now() WHERE id=$1")
            .bind(old)
            .execute(&mut *tx)
            .await?;
    }
    complete(&mut tx, Some(item.version)).await?;
    tx.commit().await?;
    app.wake_gc.notify_one();
    Ok(())
}
async fn copy_object(app: &Arc<App>, input: &Batch, item: &Item, permit: &Permit) -> Result<()> {
    let _admission = app.admit(true).await?;
    let (source, _pin) = app.current(input.bucket, &item.key).await?;
    if source.id != item.version {
        return Err(s3s::s3_error!(PreconditionFailed).into());
    }
    let replace = input.action == Kind::Metadata;
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
            input.public_read.unwrap_or(false),
            source.metadata.clone(),
        )
    };
    let (id, _pin) = app
        .new_stream(permit, target, key, "object", meta, public, false)
        .await?;
    let result=async {
        app.copy_extents(id,&source).await?;
        app.finish_stream(id,source.size,&source.etag,source.checksums.clone()).await?;
        #[cfg(feature="fault-injection")]
        crate::faults::point("media-before-publish").await;
        let _coord=app.coord.lock().await;app.writable()?;
        let mut tx=app.db.begin().await?;
        let mut authority=permit.clone();if public {authority.add(target,Action::Acl);}
        scope(&mut tx,&authority).await?;
        selected(&mut tx,input.bucket,&item.key,Some(item.version)).await?;
        let old=selected(&mut tx,target,key,expected).await?;
        if replace {
            let current:bool=sqlx::query_scalar("SELECT public_read FROM streams WHERE id=$1").bind(source.id).fetch_one(&mut *tx).await?;
            if current!=source.public_read {return Err(s3s::s3_error!(PreconditionFailed).into());}
        }
        sqlx::query("UPDATE streams SET state='ready',touched_at=now() WHERE id=$1 AND state='writing'").bind(id).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO objects(bucket_id,key,stream_id,write_epoch) VALUES($1,$2,$3,$3) ON CONFLICT(bucket_id,key) DO UPDATE SET stream_id=excluded.stream_id,write_epoch=excluded.write_epoch")
            .bind(target).bind(key).bind(id).execute(&mut *tx).await?;
        if let Some(old)=old {sqlx::query("UPDATE streams SET state='retired',touched_at=now() WHERE id=$1").bind(old).execute(&mut *tx).await?;}
        complete(&mut tx,Some(id)).await?;tx.commit().await?;
        Ok::<_,anyhow::Error>(())
    }.await;
    if result.is_err() {
        let _ = sqlx::query(
            "UPDATE streams SET state='abandoned',touched_at=now() WHERE id=$1 AND state='writing'",
        )
        .bind(id)
        .execute(&app.db)
        .await;
    }
    app.wake_gc.notify_one();
    result
}
