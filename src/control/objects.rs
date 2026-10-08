use super::{
    audit,
    auth::Identity,
    buckets,
    contract::{self, BucketView},
    insights,
};
use crate::{app::App, authorization::Action, http::HttpError};
use anyhow::Result;
use axum::{
    Json,
    extract::{Extension, Path, Query, State},
};
use s3s::s3_error;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use uuid::Uuid;

#[utoipa::path(get, path = "/api/v1/buckets", responses((status = 200, body = Vec<BucketView>), (status = 403, body = contract::ErrorBody)))]
pub(super) async fn buckets(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
) -> Result<Json<Vec<BucketView>>, HttpError> {
    let rows: Vec<BucketView> = sqlx::query_as("SELECT b.*,b.settings_revision::text AS revision,CASE WHEN $2 THEN ARRAY['bucket.list','object.read','object.write','object.delete','object.acl','bucket.settings','storage.inspect'] WHEN $3::uuid IS NOT NULL THEN t.actions ELSE a.actions END AS actions FROM buckets b LEFT JOIN user_bucket_access a ON a.bucket_id=b.id AND a.user_id=$1 LEFT JOIN token_bucket_access t ON t.bucket_id=b.id AND t.token_id=$3 WHERE $2 OR 'bucket.list'=ANY(CASE WHEN $3::uuid IS NOT NULL THEN t.actions ELSE a.actions END) ORDER BY b.name LIMIT 1000")
        .bind(actor.id).bind(actor.admin).bind(actor.principal.token_id())
        .fetch_all(&app.db)
        .await?;
    Ok(Json(rows))
}
#[derive(Deserialize, Serialize, sqlx::FromRow, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct Website {
    pub(crate) website_enabled: bool,
    pub(crate) index_document: String,
    pub(crate) error_document: String,
}
#[utoipa::path(
    get,
    operation_id = "bucket_cors",
    path = "/api/v1/buckets/{bucket}/cors",
    params(("bucket" = Uuid, Path)),
    responses((status = 200, body = Value), (status = 403, body = contract::ErrorBody))
)]
pub(super) async fn cors(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
) -> Result<Json<Value>, HttpError> {
    actor
        .principal
        .require(&app.db, bucket, Action::Settings)
        .await?;
    let rules = sqlx::query_scalar("SELECT cors FROM buckets WHERE id=$1")
        .bind(bucket)
        .fetch_optional(&app.db)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    Ok(Json(rules))
}
#[utoipa::path(
    put,
    operation_id = "bucket_cors_save",
    path = "/api/v1/buckets/{bucket}/cors",
    params(("bucket" = Uuid, Path)),
    request_body = Value,
    responses((status = 200, body = Value), (status = 400, body = contract::ErrorBody))
)]
pub(super) async fn save_cors(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Json(rules): Json<Value>,
) -> Result<Json<Value>, HttpError> {
    Ok(Json(app.set_cors(&actor.principal, bucket, rules).await?))
}
impl Website {
    pub(crate) fn validate(&self) -> Result<(), HttpError> {
        let valid_key = |key: &str| {
            !key.starts_with('/')
                && !key.contains('\\')
                && !key.chars().any(char::is_control)
                && !key.split('/').any(|part| part == "." || part == "..")
        };
        if self.index_document.is_empty()
            || self.index_document.len() > 255
            || self.index_document.contains('/')
            || !valid_key(&self.index_document)
            || self.error_document.len() > 1024
            || !valid_key(&self.error_document)
        {
            return Err(s3s::s3_error!(InvalidArgument, "invalid website document key").into());
        }
        Ok(())
    }
}
#[utoipa::path(get, path = "/api/v1/buckets/{bucket}/website", params(("bucket" = Uuid, Path)), responses((status = 200, body = Website), (status = 403, body = contract::ErrorBody)))]
pub(super) async fn website(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
) -> Result<Json<Website>, HttpError> {
    actor
        .principal
        .require(&app.db, bucket, Action::Settings)
        .await?;
    let row = sqlx::query_as(
        "SELECT website_enabled,index_document,error_document FROM buckets WHERE id=$1",
    )
    .bind(bucket)
    .fetch_optional(&app.db)
    .await?
    .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    Ok(Json(row))
}
#[utoipa::path(put, path = "/api/v1/buckets/{bucket}/website", params(("bucket" = Uuid, Path)), request_body = Website, responses((status = 200, body = Website), (status = 400, body = contract::ErrorBody)))]
pub(super) async fn save_website(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Json(input): Json<Website>,
) -> Result<Json<Website>, HttpError> {
    app.writable()?;
    input.validate()?;
    let mut tx = app.db.begin().await?;
    buckets::lock_settings(&mut tx, &actor.principal, bucket).await?;
    let row = sqlx::query_as("UPDATE buckets SET website_enabled=$2,index_document=$3,error_document=$4 WHERE id=$1 AND state='active' RETURNING website_enabled,index_document,error_document")
        .bind(bucket).bind(input.website_enabled).bind(&input.index_document).bind(&input.error_document)
        .fetch_optional(&mut *tx).await?.ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    audit::checkpoint(&mut tx,"bucket.website",&bucket.to_string(),json!({"bucket_id":bucket,"website_enabled":input.website_enabled,"index_document":input.index_document,"error_document":input.error_document})).await?;
    tx.commit().await?;
    Ok(Json(row))
}
#[derive(Deserialize, utoipa::IntoParams)]
pub(crate) struct ChunkQuery {
    key: String,
    version: Uuid,
    after: Option<i64>,
    limit: Option<i64>,
}
#[utoipa::path(get,operation_id="object_chunks",path="/api/v1/buckets/{bucket}/object/chunks",params(("bucket"=Uuid,Path),ChunkQuery),responses((status=200,body=insights::ChunkPage)))]
pub(super) async fn object_chunks(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Query(q): Query<ChunkQuery>,
) -> Result<Json<insights::ChunkPage>, HttpError> {
    actor
        .principal
        .require(&app.db, bucket, Action::Inspect)
        .await?;
    let limit = q.limit.unwrap_or(100);
    if !(1..=200).contains(&limit) || q.after.is_some_and(|a| a < 0) {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let (object, _pin) = app.current(bucket, &q.key).await?;
    if object.id != q.version {
        return Err(s3s::s3_error!(PreconditionFailed).into());
    }
    let mut rows: Vec<Value> = sqlx::query_scalar("SELECT jsonb_build_object('id',c.id::text,'offset_bytes',e.offset_bytes::text,'length',e.length,'source_offset',e.source_offset,'raw_size',c.raw_size,'stored_size',l.standalone_size,'independent_size_hint',c.standalone_size,'payload_size',c.standalone_size-CASE WHEN c.encryption_algorithm='none' THEN 0 ELSE 16 END,'compression',CASE WHEN c.standalone_compressed THEN 'zstd' ELSE 'none' END,'algorithm',c.encryption_algorithm,'block_key_id',c.block_key_id,'pack_id',c.pack_id::text,'source',CASE WHEN l.encoding_id IS NOT NULL THEN 'chunk' WHEN c.pack_id IS NOT NULL THEN 'pack' ELSE 'pending' END,'reads',COALESCE(a.reads,0)::text,'range_reads',COALESCE(a.range_reads,0)::text) FROM extents e JOIN chunks c ON c.id=e.chunk_id LEFT JOIN chunk_encodings l ON l.chunk_id=c.id AND l.state='ready' LEFT JOIN chunk_access_stats a ON a.chunk_id=c.id WHERE e.stream_id=$1 AND e.offset_bytes>$2 ORDER BY e.offset_bytes LIMIT $3")
        .bind(object.id).bind(q.after.unwrap_or(-1)).bind(limit + 1).fetch_all(&app.db).await?;
    let more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    if !actor.admin {
        for row in &mut rows {
            if let Some(row) = row.as_object_mut() {
                for key in ["reads", "range_reads", "block_key_id"] {
                    row.remove(key);
                }
            }
        }
    }
    let next_offset = more.then(|| {
        rows.last().unwrap()["offset_bytes"]
            .as_str()
            .unwrap()
            .to_owned()
    });
    let chunks = rows
        .into_iter()
        .map(serde_json::from_value)
        .collect::<Result<_, _>>()?;
    Ok(Json(insights::ChunkPage {
        chunks,
        next_offset,
    }))
}
