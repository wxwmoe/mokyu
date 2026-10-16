use super::Identity;
pub(super) use crate::model::object::MediaItem;
use crate::model::object::{CatalogCursor as Cursor, CatalogFilter as Filter};
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
use chrono::Utc;
use serde::Serialize;
use std::sync::Arc;
use utoipa::ToSchema;
use uuid::Uuid;

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
    #[serde(rename = "items")]
    objects: Vec<MediaItem>,
    prefixes: Vec<String>,
    #[serde(rename = "next_cursor")]
    next: Option<String>,
    layout: &'static str,
    search_mode: String,
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

#[utoipa::path(get,operation_id="browse_media",path="/api/v1/buckets/{bucket}/objects",params(("bucket"=Uuid,Path),Filter),responses((status=200,body=MediaPage)))]
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
        || q.category.as_deref().is_some_and(|k| {
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
        .require(app.authorization.as_ref(), bucket, Action::List)
        .await?;
    let folders = !q.recursive
        && q.q.is_empty()
        && q.category.is_none()
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
        }));
    }
    if cursor.as_ref().is_some_and(|c| c.folder.is_some()) {
        return Err(invalid().into());
    }
    let mut rows = app
        .objects
        .browse(&authority, bucket, &mut q, cursor, (min, max))
        .await
        .map_err(|error| match crate::model::error(&error) {
            Some(crate::model::Error::QueryCancelled) => {
                crate::http::problem(StatusCode::UNPROCESSABLE_ENTITY, "SearchTooBroad").0
            }
            _ => error,
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
    Ok(Json(MediaPage {
        objects: rows,
        prefixes: vec![],
        next,
        layout: "flat",
        search_mode: q.mode,
    }))
}
