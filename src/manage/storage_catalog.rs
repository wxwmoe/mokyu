use super::{
    Identity,
    insights::{ScopeQuery, scope},
};
use crate::{app::App, http::HttpError};
use anyhow::Result;
use axum::{
    Json,
    extract::{Extension, Path, Query, State},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

#[derive(Deserialize, IntoParams)]
pub(super) struct PackQuery {
    bucket: Option<Uuid>,
    project: Option<Uuid>,
    after: Option<i64>,
    limit: Option<i64>,
    state: Option<String>,
}
#[derive(Serialize, Deserialize, sqlx::FromRow, ToSchema)]
pub(super) struct PackSummary {
    id: String,
    state: String,
    compressed: bool,
    raw_size: Option<String>,
    stored_size: Option<String>,
    member_count: Option<i32>,
    created_at: DateTime<Utc>,
}
#[derive(Serialize, ToSchema)]
pub(super) struct PackPage {
    packs: Vec<PackSummary>,
    next: Option<String>,
}
#[derive(Serialize, sqlx::FromRow, ToSchema)]
pub(super) struct PackMember {
    chunk_id: String,
    ordinal: Option<i32>,
    raw_size: Option<i32>,
    visible_bytes: String,
    current_source: bool,
}
#[derive(Serialize, ToSchema)]
pub(super) struct PackDetail {
    pack: PackSummary,
    members: Vec<PackMember>,
    next: Option<String>,
    scoped: bool,
}
#[derive(Deserialize, IntoParams)]
pub(super) struct MemberQuery {
    bucket: Option<Uuid>,
    project: Option<Uuid>,
    after: Option<i64>,
    limit: Option<i64>,
}
#[derive(Deserialize, IntoParams)]
pub(super) struct ObjectsQuery {
    bucket: Option<Uuid>,
    project: Option<Uuid>,
    cursor: Option<String>,
}
#[derive(Serialize, Deserialize)]
struct Cursor {
    pack: i64,
    bucket: Option<Uuid>,
    project: Option<Uuid>,
    last_bucket: Uuid,
    last_key: String,
}
#[derive(Serialize, sqlx::FromRow, ToSchema)]
pub(super) struct PackObject {
    bucket_id: Uuid,
    bucket_name: String,
    key: String,
    version: Uuid,
    size: String,
    content_type: String,
    public_read: bool,
}
#[derive(Serialize, ToSchema)]
pub(super) struct PackObjects {
    objects: Vec<PackObject>,
    next: Option<String>,
}
fn limit(value: Option<i64>, after: Option<i64>) -> Result<i64> {
    let value = value.unwrap_or(50);
    if !(1..=200).contains(&value) || after.is_some_and(|v| v < 0) {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    Ok(value)
}
async fn bounds(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> Result<()> {
    sqlx::raw_sql(
        "SET LOCAL statement_timeout='5s';SET LOCAL lock_timeout='1s';SET LOCAL work_mem='4MB'",
    )
    .execute(&mut **tx)
    .await?;
    Ok(())
}
#[utoipa::path(get,operation_id="storage_packs",path="/api/storage/packs",params(PackQuery),responses((status=200,body=PackPage)))]
pub(super) async fn packs(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Query(q): Query<PackQuery>,
) -> Result<Json<PackPage>, HttpError> {
    let limit = limit(q.limit, q.after)?;
    if q.state
        .as_deref()
        .is_some_and(|s| !["preparing", "ready", "retired", "deleting", "deleted"].contains(&s))
    {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let mut tx = app.db.begin().await?;
    bounds(&mut tx).await?;
    let ids = scope(
        &mut tx,
        &actor,
        &ScopeQuery {
            bucket: q.bucket,
            project: q.project,
        },
    )
    .await?;
    let all = actor.admin && q.bucket.is_none() && q.project.is_none();
    let mut rows:Vec<PackSummary>=sqlx::query_as("SELECT p.id::text,p.state,p.compressed,CASE WHEN $3 THEN p.raw_size::text END raw_size,CASE WHEN $3 THEN p.stored_size::text END stored_size,CASE WHEN $3 THEN p.member_count END member_count,p.created_at FROM packs p WHERE p.id>$1 AND ($5::text IS NULL OR p.state=$5) AND ($3 OR p.state='ready') AND ($4 OR EXISTS(SELECT 1 FROM pack_members m JOIN chunks c ON c.id=m.chunk_id JOIN extents e ON e.chunk_id=c.id JOIN streams s ON s.id=e.stream_id JOIN objects o ON o.bucket_id=s.bucket_id AND o.key=s.object_key AND o.stream_id=s.id WHERE m.pack_id=p.id AND c.pack_id=p.id AND o.bucket_id=ANY($2))) ORDER BY p.id LIMIT $6")
        .bind(q.after.unwrap_or(0)).bind(&ids).bind(actor.admin).bind(all).bind(q.state).bind(limit+1).fetch_all(&mut *tx).await?;
    let next = (rows.len() > limit as usize).then(|| rows[limit as usize - 1].id.clone());
    rows.truncate(limit as usize);
    tx.commit().await?;
    Ok(Json(PackPage { packs: rows, next }))
}
#[utoipa::path(get,operation_id="storage_pack",path="/api/storage/packs/{id}",params(("id"=i64,Path),MemberQuery),responses((status=200,body=PackDetail)))]
pub(super) async fn detail(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<i64>,
    Query(q): Query<MemberQuery>,
) -> Result<Json<PackDetail>, HttpError> {
    let limit = limit(q.limit, q.after)?;
    if id < 1 {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let mut tx = app.db.begin().await?;
    bounds(&mut tx).await?;
    let ids = scope(
        &mut tx,
        &actor,
        &ScopeQuery {
            bucket: q.bucket,
            project: q.project,
        },
    )
    .await?;
    let all = actor.admin && q.bucket.is_none() && q.project.is_none();
    let pack:PackSummary=sqlx::query_as("SELECT p.id::text,p.state,p.compressed,CASE WHEN $3 THEN p.raw_size::text END raw_size,CASE WHEN $3 THEN p.stored_size::text END stored_size,CASE WHEN $3 THEN p.member_count END member_count,p.created_at FROM packs p WHERE p.id=$1 AND ($3 OR p.state='ready') AND ($4 OR EXISTS(SELECT 1 FROM pack_members m JOIN chunks c ON c.id=m.chunk_id JOIN extents e ON e.chunk_id=c.id JOIN streams s ON s.id=e.stream_id JOIN objects o ON o.bucket_id=s.bucket_id AND o.key=s.object_key AND o.stream_id=s.id WHERE m.pack_id=p.id AND c.pack_id=p.id AND o.bucket_id=ANY($2)))")
        .bind(id).bind(&ids).bind(actor.admin).bind(all).fetch_optional(&mut *tx).await?.ok_or_else(||s3s::s3_error!(NoSuchKey))?;
    let mut members:Vec<PackMember>=sqlx::query_as("SELECT c.id::text chunk_id,CASE WHEN $3 THEN m.ordinal END ordinal,CASE WHEN $3 THEN c.raw_size END raw_size,coalesce((SELECT sum(upper(span)-lower(span))::text FROM unnest(v.spans) span),'0') AS visible_bytes,coalesce(c.pack_id=m.pack_id AND NOT EXISTS(SELECT 1 FROM chunk_locations WHERE chunk_id=c.id AND state='ready'),false) AS current_source FROM pack_members m JOIN chunks c ON c.id=m.chunk_id LEFT JOIN LATERAL (SELECT range_agg(int8range(e.source_offset::bigint,e.source_offset::bigint+e.length)) spans FROM extents e JOIN streams s ON s.id=e.stream_id JOIN objects o ON o.bucket_id=s.bucket_id AND o.key=s.object_key AND o.stream_id=s.id WHERE e.chunk_id=c.id AND o.bucket_id=ANY($2)) v ON true WHERE m.pack_id=$1 AND c.id>$5 AND ($4 OR (v.spans IS NOT NULL AND c.pack_id=m.pack_id)) ORDER BY c.id LIMIT $6")
        .bind(id).bind(&ids).bind(actor.admin).bind(all).bind(q.after.unwrap_or(0)).bind(limit+1).fetch_all(&mut *tx).await?;
    let next =
        (members.len() > limit as usize).then(|| members[limit as usize - 1].chunk_id.clone());
    members.truncate(limit as usize);
    tx.commit().await?;
    Ok(Json(PackDetail {
        pack,
        members,
        next,
        scoped: !actor.admin,
    }))
}
#[utoipa::path(get,operation_id="storage_pack_objects",path="/api/storage/packs/{id}/objects",params(("id"=i64,Path),ObjectsQuery),responses((status=200,body=PackObjects)))]
pub(super) async fn objects(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<i64>,
    Query(q): Query<ObjectsQuery>,
) -> Result<Json<PackObjects>, HttpError> {
    if id < 1 || q.cursor.as_ref().is_some_and(|v| v.len() > 4096) {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let cursor = q
        .cursor
        .as_deref()
        .map(|v| -> Result<Cursor> {
            let cursor: Cursor = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(v)?)?;
            anyhow::ensure!(
                cursor.pack == id && cursor.bucket == q.bucket && cursor.project == q.project,
                "cursor belongs to another scope"
            );
            Ok(cursor)
        })
        .transpose()
        .map_err(|_| s3s::s3_error!(InvalidArgument))?;
    let mut tx = app.db.begin().await?;
    bounds(&mut tx).await?;
    let ids = scope(
        &mut tx,
        &actor,
        &ScopeQuery {
            bucket: q.bucket,
            project: q.project,
        },
    )
    .await?;
    let mut rows:Vec<PackObject>=sqlx::query_as("SELECT DISTINCT o.bucket_id,b.name AS bucket_name,o.key,s.id AS version,s.size::text,coalesce(s.metadata->>'content_type','application/octet-stream') AS content_type,s.public_read FROM pack_members m JOIN chunks c ON c.id=m.chunk_id JOIN extents e ON e.chunk_id=c.id JOIN streams s ON s.id=e.stream_id JOIN objects o ON o.bucket_id=s.bucket_id AND o.key=s.object_key AND o.stream_id=s.id JOIN buckets b ON b.id=o.bucket_id LEFT JOIN user_bucket_access a ON a.bucket_id=b.id AND a.user_id=$3 LEFT JOIN token_bucket_access t ON t.bucket_id=b.id AND t.token_id=$4 WHERE m.pack_id=$1 AND o.bucket_id=ANY($2) AND ($5 OR (c.pack_id=$1 AND 'bucket.list'=ANY(CASE WHEN $4::uuid IS NOT NULL THEN t.actions ELSE a.actions END))) AND ($6::uuid IS NULL OR (o.bucket_id,o.key)>($6,$7)) ORDER BY o.bucket_id,o.key LIMIT 101")
        .bind(id).bind(ids).bind(actor.id).bind(actor.principal.token_id()).bind(actor.admin).bind(cursor.as_ref().map(|v|v.last_bucket)).bind(cursor.as_ref().map(|v|&v.last_key)).fetch_all(&mut *tx).await?;
    let next = if rows.len() > 100 {
        let row = &rows[99];
        Some(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&Cursor {
            pack: id,
            bucket: q.bucket,
            project: q.project,
            last_bucket: row.bucket_id,
            last_key: row.key.clone(),
        })?))
    } else {
        None
    };
    rows.truncate(100);
    tx.commit().await?;
    Ok(Json(PackObjects {
        objects: rows,
        next,
    }))
}
