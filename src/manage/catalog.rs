use super::Identity;
use crate::{
    app::{App, StoredStream},
    authorization::Action,
    http::HttpError,
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
use sqlx::{FromRow, Postgres, QueryBuilder};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

const INDEXES: [(&str, &str); 4] = [
    (
        "objects_catalog_size",
        "CREATE INDEX CONCURRENTLY objects_catalog_size ON objects(bucket_id,catalog_size,key) WHERE stream_id IS NOT NULL",
    ),
    (
        "objects_catalog_modified",
        "CREATE INDEX CONCURRENTLY objects_catalog_modified ON objects(bucket_id,catalog_modified,key) WHERE stream_id IS NOT NULL",
    ),
    (
        "objects_catalog_kind",
        "CREATE INDEX CONCURRENTLY objects_catalog_kind ON objects(bucket_id,catalog_kind,key) WHERE stream_id IS NOT NULL",
    ),
    (
        "objects_catalog_search",
        "CREATE INDEX CONCURRENTLY objects_catalog_search ON objects USING gin ((key COLLATE \"default\") gin_trgm_ops) WHERE stream_id IS NOT NULL",
    ),
];

pub(crate) async fn run(app: Arc<App>) -> Result<()> {
    loop {
        if app.maintenance.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }
        match build(&app).await {
            Ok(true) => return std::future::pending().await,
            Ok(false) => tokio::time::sleep(Duration::from_millis(100)).await,
            Err(error) => {
                tracing::warn!(%error,"media catalog build will retry");
                let _=sqlx::query("UPDATE catalog_build SET last_error='catalog_build_failed',updated_at=now() WHERE singleton").execute(&app.db).await;
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        }
    }
}
async fn build(app: &App) -> Result<bool> {
    let phase: String = sqlx::query_scalar("SELECT phase FROM catalog_build WHERE singleton")
        .fetch_one(&app.db)
        .await?;
    if phase == "ready" {
        return Ok(true);
    }
    if phase == "indexes" {
        for (name, sql) in INDEXES {
            let valid: Option<bool> = sqlx::query_scalar(
                "SELECT indisvalid FROM pg_index WHERE indexrelid=to_regclass($1)",
            )
            .bind(name)
            .fetch_optional(&app.db)
            .await?;
            if valid == Some(true) {
                continue;
            }
            sqlx::query("UPDATE catalog_build SET current_index=$1,last_error=NULL,updated_at=now() WHERE singleton").bind(name).execute(&app.db).await?;
            let mut connection = app.db.acquire().await?;
            connection.close_on_drop();
            sqlx::raw_sql("SET statement_timeout=0; SET lock_timeout='1s'; SET maintenance_work_mem='32MB'; SET max_parallel_maintenance_workers=0").execute(&mut *connection).await?;
            if valid == Some(false) {
                QueryBuilder::<Postgres>::new("DROP INDEX CONCURRENTLY ")
                    .push(name)
                    .build()
                    .execute(&mut *connection)
                    .await?;
            }
            sqlx::query(sql).execute(&mut *connection).await?;
            return Ok(false);
        }
        sqlx::query("UPDATE catalog_build SET phase='backfill',current_index=NULL,last_error=NULL,updated_at=now() WHERE singleton").execute(&app.db).await?;
        return Ok(false);
    }
    let mut tx = app.db.begin().await?;
    sqlx::raw_sql("SET LOCAL statement_timeout='2s'; SET LOCAL lock_timeout='200ms'; SET LOCAL work_mem='4MB'").execute(&mut *tx).await?;
    let cursor: (Option<Uuid>, Option<String>) = sqlx::query_as(
        "SELECT cursor_bucket,cursor_key FROM catalog_build WHERE singleton FOR UPDATE",
    )
    .fetch_one(&mut *tx)
    .await?;
    let mut scan = QueryBuilder::<Postgres>::new("SELECT bucket_id,key FROM objects");
    if let Some(bucket) = cursor.0 {
        scan.push(" WHERE (bucket_id,key)>(")
            .push_bind(bucket)
            .push(",")
            .push_bind(&cursor.1)
            .push(")");
    }
    scan.push(" ORDER BY bucket_id,key LIMIT 500 FOR UPDATE");
    let rows: Vec<(Uuid, String)> = scan.build_query_as().fetch_all(&mut *tx).await?;
    if let Some((bucket, key)) = rows.last() {
        let mut update = QueryBuilder::<Postgres>::new(
            "UPDATE objects o SET catalog_size=s.size,catalog_modified=s.created_at,catalog_type=coalesce(s.metadata->>'content_type','application/octet-stream'),catalog_kind=catalog_kind(o.key,coalesce(s.metadata->>'content_type','application/octet-stream')),catalog_public=s.public_read FROM streams s WHERE o.stream_id=s.id AND o.catalog_size IS NULL",
        );
        if let Some(bucket) = cursor.0 {
            update
                .push(" AND (o.bucket_id,o.key)>(")
                .push_bind(bucket)
                .push(",")
                .push_bind(&cursor.1)
                .push(")");
        }
        update
            .push(" AND (o.bucket_id,o.key)<=(")
            .push_bind(bucket)
            .push(",")
            .push_bind(key)
            .push(")")
            .build()
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE catalog_build SET cursor_bucket=$1,cursor_key=$2,scanned=scanned+$3,last_error=NULL,updated_at=now() WHERE singleton").bind(bucket).bind(key).bind(rows.len() as i64).execute(&mut *tx).await?;
    } else {
        sqlx::query("UPDATE catalog_build SET phase='ready',last_error=NULL,updated_at=now() WHERE singleton").execute(&mut *tx).await?;
    }
    tx.commit().await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("catalog-after-batch").await;
    Ok(false)
}

#[derive(Serialize, FromRow, ToSchema)]
pub(crate) struct CatalogStatus {
    phase: String,
    scanned: Option<String>,
    current_index: Option<String>,
    last_error: Option<String>,
    updated_at: DateTime<Utc>,
}
pub(crate) async fn progress(app: &App) -> Result<CatalogStatus> {
    Ok(sqlx::query_as("SELECT phase,scanned::text,current_index,last_error,updated_at FROM catalog_build WHERE singleton").fetch_one(&app.db).await?)
}
#[utoipa::path(get,operation_id="catalog_status",path="/api/buckets/{bucket}/catalog",params(("bucket"=Uuid,Path)),responses((status=200,body=CatalogStatus)))]
pub(super) async fn status(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
) -> Result<Json<CatalogStatus>, HttpError> {
    actor
        .principal
        .require(&app.db, bucket, Action::List)
        .await?;
    let mut row = progress(&app).await?;
    if !actor.admin {
        row.scanned = None;
        row.current_index = None;
        row.last_error = None;
    }
    Ok(Json(row))
}

#[derive(Serialize, Deserialize, IntoParams)]
#[serde(default, deny_unknown_fields)]
pub(super) struct Filter {
    prefix: String,
    q: String,
    mode: String,
    search_in: String,
    kind: Option<String>,
    public: Option<bool>,
    min_size: Option<String>,
    max_size: Option<String>,
    since: Option<DateTime<Utc>>,
    until: Option<DateTime<Utc>>,
    sort: String,
    order: String,
    recursive: bool,
    after: Option<String>,
    limit: usize,
}
impl Default for Filter {
    fn default() -> Self {
        Self {
            prefix: String::new(),
            q: String::new(),
            mode: "contains".into(),
            search_in: "name".into(),
            kind: None,
            public: None,
            min_size: None,
            max_size: None,
            since: None,
            until: None,
            sort: "name".into(),
            order: "asc".into(),
            recursive: false,
            after: None,
            limit: 100,
        }
    }
}
#[derive(Serialize, Deserialize)]
struct Cursor {
    scope: String,
    key: String,
    size: String,
    modified: DateTime<Utc>,
    folder: Option<String>,
}
#[derive(Serialize, FromRow, ToSchema)]
pub(super) struct MediaItem {
    pub id: Uuid,
    pub object_key: String,
    pub size: String,
    pub content_type: String,
    pub public_read: bool,
    pub modified_at: DateTime<Utc>,
}
impl From<StoredStream> for MediaItem {
    fn from(s: StoredStream) -> Self {
        Self {
            id: s.id,
            object_key: s.object_key,
            size: s.size.to_string(),
            content_type: s.metadata["content_type"]
                .as_str()
                .unwrap_or("application/octet-stream")
                .into(),
            public_read: s.public_read,
            modified_at: s.created_at,
        }
    }
}
#[derive(Serialize, ToSchema)]
pub(super) struct MediaPage {
    objects: Vec<MediaItem>,
    prefixes: Vec<String>,
    next: Option<String>,
    layout: &'static str,
    search_mode: String,
    index_ready: bool,
}
fn escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}
fn invalid() -> anyhow::Error {
    s3s::s3_error!(InvalidArgument).into()
}
fn size(value: &Option<String>) -> Result<Option<i64>> {
    value
        .as_ref()
        .map(|s| {
            s.parse::<i64>()
                .ok()
                .filter(|n| *n >= 0)
                .ok_or_else(invalid)
        })
        .transpose()
}

#[utoipa::path(get,operation_id="browse_media",path="/api/buckets/{bucket}/objects",params(("bucket"=Uuid,Path),Filter),responses((status=200,body=MediaPage)))]
pub(super) async fn list(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Query(mut q): Query<Filter>,
) -> Result<Json<MediaPage>, HttpError> {
    if q.prefix.len() > 1024
        || q.q.len() > 256
        || q.prefix.contains('\0')
        || q.q.contains('\0')
        || !(1..=200).contains(&q.limit)
        || !["contains", "prefix", "exact"].contains(&q.mode.as_str())
        || !["name", "path"].contains(&q.search_in.as_str())
        || !["asc", "desc"].contains(&q.order.as_str())
        || q.kind.as_deref().is_some_and(|k| {
            !["image", "video", "audio", "document", "archive", "other"].contains(&k)
        })
        || q.since.zip(q.until).is_some_and(|(a, b)| a > b)
        || !["name", "size", "modified"].contains(&q.sort.as_str())
    {
        return Err(invalid().into());
    }
    let (min, max) = (size(&q.min_size)?, size(&q.max_size)?);
    if min.zip(max).is_some_and(|(a, b)| a > b) {
        return Err(invalid().into());
    }
    let token = q.after.take();
    let scope = blake3::hash(&serde_json::to_vec(&(bucket, &q))?)
        .to_hex()
        .to_string();
    let cursor = token
        .map(|v| -> Result<Cursor> { Ok(serde_json::from_slice(&URL_SAFE_NO_PAD.decode(v)?)?) })
        .transpose()
        .map_err(|_| invalid())?;
    if cursor.as_ref().is_some_and(|c| c.scope != scope) {
        return Err(invalid().into());
    }
    let authority = actor
        .principal
        .require(&app.db, bucket, Action::List)
        .await?;
    let ready: bool = sqlx::query_scalar("SELECT phase='ready' FROM catalog_build WHERE singleton")
        .fetch_one(&app.db)
        .await?;
    let folders = !q.recursive
        && q.q.is_empty()
        && q.kind.is_none()
        && q.public.is_none()
        && min.is_none()
        && max.is_none()
        && q.since.is_none()
        && q.until.is_none()
        && q.sort == "name"
        && q.order == "asc";
    if folders {
        let page = app
            .list(
                bucket,
                &q.prefix,
                "/",
                None,
                cursor.as_ref().and_then(|c| c.folder.as_deref()),
                q.limit,
            )
            .await?;
        let next = page
            .next
            .map(|next| {
                serde_json::to_vec(&Cursor {
                    scope,
                    key: String::new(),
                    size: "0".into(),
                    modified: Utc::now(),
                    folder: Some(next),
                })
            })
            .transpose()?
            .map(|v| URL_SAFE_NO_PAD.encode(v));
        return Ok(Json(MediaPage {
            objects: page.objects.into_iter().map(Into::into).collect(),
            prefixes: page.prefixes,
            next,
            layout: "folders",
            search_mode: q.mode,
            index_ready: ready,
        }));
    }
    if cursor.as_ref().is_some_and(|c| c.folder.is_some()) {
        return Err(invalid().into());
    }
    let mut tx = app.db.begin().await?;
    sqlx::raw_sql("SET LOCAL statement_timeout='2s'; SET LOCAL work_mem='4MB'")
        .execute(&mut *tx)
        .await?;
    authority.lock(&mut tx).await?;
    if q.mode == "contains" && !q.q.is_empty() {
        let usable: bool = sqlx::query_scalar("SELECT $1 COLLATE \"default\" ~ '[[:alnum:]]{3}'")
            .bind(&q.q)
            .fetch_one(&mut *tx)
            .await?;
        if !usable {
            q.mode = "prefix".into();
        }
    }
    let indexed = q.kind.is_some()
        || q.public.is_some()
        || min.is_some()
        || max.is_some()
        || q.since.is_some()
        || q.until.is_some()
        || q.sort != "name"
        || !q.q.is_empty() && q.mode == "contains";
    if !ready && indexed {
        return Err(crate::http::problem(
            StatusCode::SERVICE_UNAVAILABLE,
            "CatalogBuilding",
        ));
    }
    let mut sql = QueryBuilder::<Postgres>::new(
        "SELECT stream_id id,key object_key,catalog_size::text size,catalog_type content_type,catalog_public public_read,catalog_modified modified_at FROM ",
    );
    sql.push(if ready {"objects"} else {"(SELECT o.bucket_id,o.key,o.stream_id,s.size catalog_size,s.created_at catalog_modified,coalesce(s.metadata->>'content_type','application/octet-stream') catalog_type,s.public_read catalog_public FROM objects o JOIN streams s ON s.id=o.stream_id) objects"});
    sql.push(" WHERE stream_id IS NOT NULL AND bucket_id=");
    sql.push_bind(bucket);
    if !q.prefix.is_empty() {
        sql.push(" AND key>=").push_bind(&q.prefix);
        if let Some(end) = crate::listing::successor(&q.prefix) {
            sql.push(" AND key<").push_bind(end);
        }
    }
    if !q.q.is_empty() {
        match q.mode.as_str() {
            "exact" => {
                sql.push(" AND key=")
                    .push_bind(format!("{}{}", q.prefix, q.q));
            }
            "prefix" => {
                let prefix = format!("{}{}", q.prefix, q.q);
                sql.push(" AND key>=").push_bind(prefix.clone());
                if let Some(end) = crate::listing::successor(&prefix) {
                    sql.push(" AND key<").push_bind(end);
                }
            }
            _ => {
                let pattern = format!("%{}%", escape(&q.q));
                sql.push(" AND key COLLATE \"default\" ILIKE ")
                    .push_bind(pattern.clone());
                if q.search_in == "name" {
                    sql.push(" AND regexp_replace(key,'^.*/','') COLLATE \"default\" ILIKE ")
                        .push_bind(pattern);
                }
            }
        }
    }
    if let Some(kind) = &q.kind {
        sql.push(" AND catalog_kind=").push_bind(kind);
    }
    if let Some(public) = q.public {
        sql.push(" AND catalog_public=").push_bind(public);
    }
    if let Some(min) = min {
        sql.push(" AND catalog_size>=").push_bind(min);
    }
    if let Some(max) = max {
        sql.push(" AND catalog_size<=").push_bind(max);
    }
    if let Some(since) = q.since {
        sql.push(" AND catalog_modified>=").push_bind(since);
    }
    if let Some(until) = q.until {
        sql.push(" AND catalog_modified<=").push_bind(until);
    }
    let column = match q.sort.as_str() {
        "size" => "catalog_size",
        "modified" => "catalog_modified",
        _ => "key",
    };
    let comparison = if q.order == "asc" { " > " } else { " < " };
    if let Some(c) = cursor {
        if column == "key" {
            sql.push(" AND key").push(comparison).push_bind(c.key);
        } else {
            sql.push(" AND (")
                .push(column)
                .push(",key)")
                .push(comparison)
                .push("(");
            if column == "catalog_size" {
                sql.push_bind(
                    c.size
                        .parse::<i64>()
                        .ok()
                        .filter(|n| *n >= 0)
                        .ok_or_else(invalid)?,
                );
            } else {
                sql.push_bind(c.modified);
            }
            sql.push(",").push_bind(c.key).push(")");
        }
    }
    let direction = if q.order == "asc" { " ASC" } else { " DESC" };
    sql.push(" ORDER BY ").push(column).push(direction);
    if column != "key" {
        sql.push(",key").push(direction);
    }
    sql.push(" LIMIT ").push_bind(q.limit as i64 + 1);
    let mut rows: Vec<MediaItem> =
        sql.build_query_as()
            .fetch_all(&mut *tx)
            .await
            .map_err(|error| {
                if error
                    .as_database_error()
                    .and_then(|e| e.code())
                    .is_some_and(|code| code == "57014")
                {
                    crate::http::problem(StatusCode::UNPROCESSABLE_ENTITY, "SearchTooBroad").0
                } else {
                    error.into()
                }
            })?;
    let more = rows.len() > q.limit;
    rows.truncate(q.limit);
    let next = if more {
        let r = rows.last().unwrap();
        Some(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&Cursor {
            scope,
            key: r.object_key.clone(),
            size: r.size.clone(),
            modified: r.modified_at,
            folder: None,
        })?))
    } else {
        None
    };
    tx.commit().await?;
    Ok(Json(MediaPage {
        objects: rows,
        prefixes: vec![],
        next,
        layout: "flat",
        search_mode: q.mode,
        index_ready: ready,
    }))
}
