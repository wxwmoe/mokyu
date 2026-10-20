use super::{Identity, insights::ScopeQuery};
use crate::model::Id;
use crate::{app::App, http::HttpError};
use anyhow::Result;
use axum::{
    Json,
    extract::{Extension, Path, Query, State},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

#[derive(Deserialize, IntoParams)]
pub(super) struct PackQuery {
    bucket: Option<Uuid>,
    project: Option<Uuid>,
    after: Option<String>,
    limit: Option<i64>,
    state: Option<String>,
}
#[derive(Serialize, ToSchema)]
pub(super) struct PackPage {
    #[serde(rename = "items")]
    packs: Vec<PackSummary>,
    #[serde(rename = "next_cursor")]
    next: Option<String>,
}
#[derive(Serialize, ToSchema)]
pub(super) struct PackDetail {
    pack: PackSummary,
    #[serde(rename = "items")]
    members: Vec<PackMember>,
    #[serde(rename = "next_cursor")]
    next: Option<String>,
    scoped: bool,
}
#[derive(Deserialize, IntoParams)]
pub(super) struct MemberQuery {
    bucket: Option<Uuid>,
    project: Option<Uuid>,
    after: Option<String>,
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
    pack: Id,
    bucket: Option<Uuid>,
    project: Option<Uuid>,
    last_bucket: Uuid,
    last_key: String,
}
#[derive(Serialize, ToSchema)]
pub(super) struct PackObjects {
    #[serde(rename = "items")]
    objects: Vec<PackObject>,
    #[serde(rename = "next_cursor")]
    next: Option<String>,
}
fn limit(value: Option<i64>) -> Result<i64> {
    let value = value.unwrap_or(50);
    if !(1..=200).contains(&value) {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    Ok(value)
}
#[utoipa::path(get,operation_id="storage_packs",path="/api/v1/storage/packs",params(PackQuery),responses((status=200,body=PackPage)))]
pub(super) async fn packs(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Query(q): Query<PackQuery>,
) -> Result<Json<PackPage>, HttpError> {
    let limit = limit(q.limit)?;
    if q.state
        .as_deref()
        .is_some_and(|s| !["preparing", "ready", "retired", "deleting", "deleted"].contains(&s))
    {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let mut rows = app
        .packing
        .scoped_page(
            &actor.principal,
            &ScopeQuery {
                bucket: q.bucket,
                project: q.project,
            },
            Id::parse(
                q.after.as_deref().unwrap_or("0"),
                app.config.database.backend,
            )?,
            limit + 1,
            q.state.as_deref(),
        )
        .await?;
    let next = (rows.len() > limit as usize).then(|| rows[limit as usize - 1].id.clone());
    rows.truncate(limit as usize);
    Ok(Json(PackPage { packs: rows, next }))
}
#[utoipa::path(get,operation_id="storage_pack",path="/api/v1/storage/packs/{id}",params(("id"=String,Path),MemberQuery),responses((status=200,body=PackDetail)))]
pub(super) async fn detail(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<String>,
    Query(q): Query<MemberQuery>,
) -> Result<Json<PackDetail>, HttpError> {
    let limit = limit(q.limit)?;
    let id = Id::parse(&id, app.config.database.backend)?;
    if !id.is_valid() {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let (pack, mut members, scoped) = app
        .packing
        .scoped_detail(
            &actor.principal,
            &ScopeQuery {
                bucket: q.bucket,
                project: q.project,
            },
            id,
            Id::parse(
                q.after.as_deref().unwrap_or("0"),
                app.config.database.backend,
            )?,
            limit + 1,
        )
        .await?;
    let next =
        (members.len() > limit as usize).then(|| members[limit as usize - 1].chunk_id.clone());
    members.truncate(limit as usize);
    Ok(Json(PackDetail {
        pack,
        members,
        next,
        scoped,
    }))
}
#[utoipa::path(get,operation_id="storage_pack_objects",path="/api/v1/storage/packs/{id}/objects",params(("id"=String,Path),ObjectsQuery),responses((status=200,body=PackObjects)))]
pub(super) async fn objects(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<String>,
    Query(q): Query<ObjectsQuery>,
) -> Result<Json<PackObjects>, HttpError> {
    let id = Id::parse(&id, app.config.database.backend)?;
    if !id.is_valid() || q.cursor.as_ref().is_some_and(|v| v.len() > 4096) {
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
    let mut rows = app
        .packing
        .scoped_objects(
            &actor.principal,
            &ScopeQuery {
                bucket: q.bucket,
                project: q.project,
            },
            id,
            cursor.map(|value| (value.last_bucket, value.last_key)),
        )
        .await?;
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
    Ok(Json(PackObjects {
        objects: rows,
        next,
    }))
}

pub(super) use crate::model::packing::{PackMember, PackObject, PackSummary};
