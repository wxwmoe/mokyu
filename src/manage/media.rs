use super::{Identity, catalog::MediaItem};
use crate::{
    app::{Active, App, Metadata, StoredStream},
    authorization::Action,
    config,
    http::{HttpError, ReadOptions, problem},
    storage::{Area, Disk},
};
use anyhow::{Result, ensure};
use axum::{
    Json,
    body::Body,
    extract::{Extension, Path, Query, State},
    http::{HeaderMap, Method, StatusCode},
    response::Response,
};
use chrono::{DateTime, Utc};
use image::{DynamicImage, ImageDecoder, ImageFormat, ImageReader};
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    io::Cursor,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Semaphore;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

const THUMB_LIMIT: usize = 640 * 1024;
const TEXT_LIMIT: usize = 64 * 1024;
const CACHE_ENTRIES: usize = 4096;

pub struct Thumbnails {
    root: PathBuf,
    disk: Arc<Disk>,
    capacity: u64,
    entries: Mutex<VecDeque<(Uuid, u64)>>,
    job: Arc<Semaphore>,
}
impl Thumbnails {
    pub async fn new(config: &config::Config, disk: Arc<Disk>) -> Result<Self> {
        let cache = Self {
            root: config.storage.data.join("thumbnails"),
            disk,
            capacity: config::cache_bytes(&config.manage.thumbnail_cache_size)?,
            entries: Mutex::new(VecDeque::new()),
            job: Arc::new(Semaphore::new(1)),
        };
        if let Err(error) = cache.scan().await {
            tracing::warn!(%error,"thumbnail cache scan incomplete; previews remain available");
        }
        Ok(cache)
    }
    async fn scan(&self) -> Result<()> {
        tokio::fs::create_dir_all(&self.root).await?;
        let mut files = tokio::fs::read_dir(&self.root).await?;
        while let Some(file) = files.next_entry().await? {
            if !file.file_type().await?.is_file() {
                continue;
            }
            let name = file.file_name();
            let id = name
                .to_str()
                .and_then(|s| s.strip_prefix("v1-"))
                .and_then(|s| Uuid::parse_str(s).ok());
            if let Some(id) = id {
                if self.entries.lock().unwrap().len() == CACHE_ENTRIES {
                    tokio::fs::remove_file(file.path()).await?;
                    continue;
                }
                let size = file.metadata().await?.len();
                self.disk.account_existing(Area::Thumbnails, size);
                self.entries.lock().unwrap().push_back((id, size));
            } else {
                tokio::fs::remove_file(file.path()).await?;
            }
        }
        self.trim(0)
    }
    fn path(&self, id: Uuid) -> PathBuf {
        self.root.join(format!("v1-{id}"))
    }
    fn trim(&self, incoming: u64) -> Result<()> {
        let mut entries = self.entries.lock().unwrap();
        let attempts = entries.len();
        for _ in 0..attempts {
            if self.disk.used()[2].saturating_add(incoming) <= self.capacity
                && entries.len() < CACHE_ENTRIES
            {
                break;
            }
            let (id, size) = entries.pop_front().unwrap();
            match std::fs::remove_file(self.path(id)) {
                Ok(()) => self.disk.release(Area::Thumbnails, size),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    self.disk.release(Area::Thumbnails, size)
                }
                Err(_) => entries.push_back((id, size)),
            }
        }
        ensure!(
            entries.len() < CACHE_ENTRIES
                && self.disk.used()[2].saturating_add(incoming) <= self.capacity,
            "thumbnail cache is full"
        );
        Ok(())
    }
    async fn get(&self, id: Uuid) -> Option<Vec<u8>> {
        let data = crate::storage::read_bounded(self.path(id), THUMB_LIMIT + 32)
            .await
            .ok()?;
        (data.len() > 32 && blake3::hash(&data[32..]).as_bytes() == &data[..32])
            .then(|| data[32..].to_vec())
    }
    fn put(&self, id: Uuid, data: &[u8]) -> Result<()> {
        if self.capacity == 0 {
            return Ok(());
        }
        std::fs::create_dir_all(&self.root)?;
        ensure!(data.len() <= THUMB_LIMIT, "thumbnail exceeds bound");
        // A damaged entry keeps its charge until its file is actually removed.
        let mut entries = self.entries.lock().unwrap();
        if let Some(index) = entries.iter().position(|(key, _)| *key == id) {
            match std::fs::remove_file(self.path(id)) {
                Ok(()) => (),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
                Err(e) => return Err(e.into()),
            }
            let (_, size) = entries.remove(index).unwrap();
            self.disk.release(Area::Thumbnails, size);
        }
        drop(entries);
        let size = data.len() as u64 + 32;
        self.trim(size)?;
        let ticket = self.disk.reserve(Area::Thumbnails, size)?;
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.path(id))?;
        // Reads verify the digest; interrupted writes remain charged and can be evicted.
        ticket.commit();
        self.entries.lock().unwrap().push_back((id, size));
        file.write_all(blake3::hash(data).as_bytes())?;
        file.write_all(data)?;
        Ok(())
    }
}

#[derive(Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
pub(super) struct ObjectQuery {
    key: String,
    version: Uuid,
}
#[derive(Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
pub(super) struct ContentQuery {
    key: String,
    version: Uuid,
    #[serde(default)]
    preview: bool,
}
#[derive(Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct ObjectMetadata {
    pub content_type: Option<String>,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,
    pub content_language: Option<String>,
    pub expires: Option<String>,
    #[serde(default)]
    pub user: std::collections::BTreeMap<String, String>,
}
#[derive(Serialize, ToSchema)]
pub(super) struct ObjectDetail {
    item: MediaItem,
    etag: String,
    metadata: ObjectMetadata,
    preview: &'static str,
    thumbnail: bool,
    touched_at: DateTime<Utc>,
}
fn kind(metadata: &Metadata) -> &'static str {
    if metadata
        .content_encoding
        .as_deref()
        .is_some_and(|v| !v.is_empty() && v != "identity")
    {
        return "none";
    }
    match metadata
        .content_type
        .as_deref()
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "image/jpeg" | "image/png" | "image/webp" | "image/gif" | "image/avif" => "image",
        "video/mp4" | "video/webm" | "video/ogg" => "video",
        "audio/mpeg" | "audio/ogg" | "audio/mp4" | "audio/wav" | "audio/webm" | "audio/flac" => {
            "audio"
        }
        "text/plain" | "text/markdown" | "text/csv" | "application/json" | "application/xml"
        | "text/xml" => "text",
        _ => "none",
    }
}
async fn current(
    app: &App,
    actor: &Identity,
    bucket: Uuid,
    q: &ObjectQuery,
    action: Action,
) -> Result<(StoredStream, Active)> {
    if q.key.is_empty() || q.key.len() > 1024 || q.key.contains('\0') {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    actor.principal.require(&app.db, bucket, action).await?;
    let (stream, pin) = app.current(bucket, &q.key).await?;
    if stream.id != q.version {
        return Err(s3s::s3_error!(PreconditionFailed).into());
    }
    Ok((stream, pin))
}
#[utoipa::path(get, operation_id="media_detail", path="/api/buckets/{bucket}/object", params(("bucket"=Uuid,Path),ObjectQuery), responses((status=200,body=ObjectDetail)))]
pub(super) async fn detail(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Query(q): Query<ObjectQuery>,
) -> Result<Json<ObjectDetail>, HttpError> {
    let (stream, _pin) = current(&app, &actor, bucket, &q, Action::List).await?;
    let metadata: Metadata = serde_json::from_value(stream.metadata.clone())?;
    let preview = kind(&metadata);
    let thumbnail = preview == "image"
        && !metadata
            .content_type
            .as_deref()
            .unwrap_or("")
            .starts_with("image/avif")
        && stream.size <= 32 * 1024 * 1024;
    Ok(Json(ObjectDetail {
        etag: stream.etag.clone(),
        preview,
        thumbnail,
        touched_at: stream.touched_at,
        item: stream.into(),
        metadata: ObjectMetadata {
            content_type: metadata.content_type,
            cache_control: metadata.cache_control,
            content_disposition: metadata.content_disposition,
            content_encoding: metadata.content_encoding,
            content_language: metadata.content_language,
            expires: metadata.expires,
            user: metadata.user.unwrap_or_default().into_iter().collect(),
        },
    }))
}
#[utoipa::path(get, operation_id="media_content", path="/api/buckets/{bucket}/object/content", params(("bucket"=Uuid,Path),ContentQuery), responses((status=200,content_type="application/octet-stream"),(status=206)))]
pub(super) async fn content(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    headers: HeaderMap,
    method: Method,
    Query(q): Query<ContentQuery>,
) -> Result<Response, HttpError> {
    actor
        .principal
        .require(&app.db, bucket, Action::Read)
        .await?;
    crate::http::respond(
        app,
        bucket,
        &q.key,
        headers,
        method,
        ReadOptions::managed(q.preview, Some(q.version)),
    )
    .await
}
#[derive(Serialize, ToSchema)]
pub(super) struct TextPreview {
    text: String,
    truncated: bool,
}
#[utoipa::path(get, operation_id="media_text", path="/api/buckets/{bucket}/object/text", params(("bucket"=Uuid,Path),ObjectQuery), responses((status=200,body=TextPreview)))]
pub(super) async fn text(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Query(q): Query<ObjectQuery>,
) -> Result<Json<TextPreview>, HttpError> {
    let (stream, pin) = current(&app, &actor, bucket, &q, Action::Read).await?;
    let metadata: Metadata = serde_json::from_value(stream.metadata.clone())?;
    if kind(&metadata) != "text" {
        return Err(problem(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "PreviewUnavailable",
        ));
    }
    let end = stream.size.min(TEXT_LIMIT as i64);
    let truncated = stream.size > end;
    let permits = app.admit(false).await?;
    let body = Body::new(s3s::Body::from(
        app.body(stream, pin, 0, end, permits, true),
    ));
    let bytes = axum::body::to_bytes(body, TEXT_LIMIT).await?;
    Ok(Json(TextPreview {
        text: String::from_utf8_lossy(&bytes).into_owned(),
        truncated,
    }))
}
#[utoipa::path(get, operation_id="media_thumbnail", path="/api/buckets/{bucket}/object/thumbnail", params(("bucket"=Uuid,Path),ObjectQuery), responses((status=200,content_type="image/png"),(status=415),(status=503)))]
pub(super) async fn thumbnail(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Query(q): Query<ObjectQuery>,
) -> Result<Response, HttpError> {
    let (stream, pin) = current(&app, &actor, bucket, &q, Action::Read).await?;
    let data = if let Some(data) = app.thumbnails.get(stream.id).await {
        data
    } else {
        let metadata: Metadata = serde_json::from_value(stream.metadata.clone())?;
        if kind(&metadata) != "image" {
            return Err(problem(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "PreviewUnavailable",
            ));
        }
        let job = app
            .thumbnails
            .job
            .clone()
            .try_acquire_owned()
            .map_err(|_| problem(StatusCode::SERVICE_UNAVAILABLE, "PreviewBusy"))?;
        let allowance =
            (app.budget.data_slots as u64 * app.budget.slot_bytes).min(192 * 1024 * 1024);
        let source_limit = (allowance / 8).min(32 * 1024 * 1024) as usize;
        if stream.size > source_limit as i64 {
            return Err(problem(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "PreviewTooLarge",
            ));
        }
        let permits = app.admit(false).await?;
        let extra = app
            .slots
            .clone()
            .try_acquire_many_owned(
                (allowance.div_ceil(app.budget.slot_bytes) as u32).saturating_sub(1),
            )
            .map_err(|_| problem(StatusCode::SERVICE_UNAVAILABLE, "PreviewBusy"))?;
        let id = stream.id;
        let end = stream.size;
        let body = Body::new(s3s::Body::from(
            app.body(stream, pin, 0, end, permits, false),
        ));
        let source = tokio::time::timeout(
            Duration::from_secs(15),
            axum::body::to_bytes(body, source_limit),
        )
        .await
        .map_err(|_| problem(StatusCode::GATEWAY_TIMEOUT, "PreviewTimeout"))??;
        let retained = app
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| problem(StatusCode::SERVICE_UNAVAILABLE, "PreviewBusy"))?;
        let cpu = app.storage.cpu.clone().acquire_owned().await?;
        let cache = app.thumbnails.clone();
        let task = tokio::task::spawn_blocking(move || {
            let (_job, _memory, _retained, _cpu) = (job, extra, retained, cpu);
            let data = make_thumbnail(&source, allowance)?;
            if let Err(error) = cache.put(id, &data) {
                tracing::warn!(%error,"thumbnail cache write skipped");
            }
            Ok::<_, anyhow::Error>(data)
        });
        tokio::time::timeout(Duration::from_secs(15), task)
            .await
            .map_err(|_| problem(StatusCode::GATEWAY_TIMEOUT, "PreviewTimeout"))??
            .map_err(|error| {
                tracing::debug!(%error,"thumbnail unavailable");
                problem(StatusCode::UNSUPPORTED_MEDIA_TYPE, "PreviewUnavailable")
            })?
    };
    Ok(Response::builder()
        .header("content-type", "image/png")
        .header("content-length", data.len())
        .header("cache-control", "private, no-store")
        .header("x-content-type-options", "nosniff")
        .body(Body::from(data))?)
}
fn make_thumbnail(source: &[u8], allowance: u64) -> Result<Vec<u8>> {
    let format = image::guess_format(source)?;
    ensure!(
        matches!(
            format,
            ImageFormat::Jpeg | ImageFormat::Png | ImageFormat::Gif | ImageFormat::WebP
        ),
        "unsupported image format"
    );
    let mut limits = image::Limits::default();
    limits.max_image_width = Some(16384);
    limits.max_image_height = Some(16384);
    limits.max_alloc = Some(allowance / 3);
    let mut reader = ImageReader::with_format(Cursor::new(source), format);
    reader.limits(limits);
    let mut decoder = reader.into_decoder()?;
    let (w, h) = decoder.dimensions();
    ensure!(
        w > 0 && h > 0 && u64::from(w) * u64::from(h) <= (allowance / 12).min(16_000_000),
        "image pixel limit exceeded"
    );
    let orientation = decoder.orientation()?;
    let decoded = DynamicImage::from_decoder(decoder)?;
    let mut thumbnail = decoded.thumbnail(w.min(384), h.min(384));
    thumbnail.apply_orientation(orientation);
    let mut result = Cursor::new(Vec::new());
    thumbnail.write_to(&mut result, ImageFormat::Png)?;
    let data = result.into_inner();
    ensure!(data.len() <= THUMB_LIMIT, "encoded thumbnail exceeds bound");
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_still_thumbnail() {
        let source = DynamicImage::new_rgb8(800, 400);
        for format in [
            ImageFormat::Jpeg,
            ImageFormat::Png,
            ImageFormat::Gif,
            ImageFormat::WebP,
        ] {
            let mut bytes = Cursor::new(Vec::new());
            source.write_to(&mut bytes, format).unwrap();
            let result = make_thumbnail(bytes.get_ref(), 192 * 1024 * 1024).unwrap();
            let image = image::load_from_memory(&result).unwrap();
            assert_eq!((image.width(), image.height()), (384, 192));
            assert!(make_thumbnail(bytes.get_ref(), 1024).is_err());
        }
        assert!(make_thumbnail(b"<svg onload='alert(1)'/>", 192 * 1024 * 1024).is_err());
    }
}
