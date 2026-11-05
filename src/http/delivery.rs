use crate::{
    app::{App, Metadata, StoredStream},
    model::raw::RawRange,
};
use anyhow::Result;
use axum::http::{HeaderMap, header};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering::Relaxed},
};

#[derive(Default)]
pub(crate) struct Counters {
    accel: AtomicU64,
    sendfile: AtomicU64,
    bytes: AtomicU64,
    fallback: AtomicU64,
}

impl Counters {
    pub(crate) fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({"x_accel_redirect":self.accel.load(Relaxed), "x_sendfile":self.sendfile.load(Relaxed),
            "planned_bytes":self.bytes.load(Relaxed), "application_fallback":self.fallback.load(Relaxed)})
    }
}

pub(crate) fn body_cache(
    app: &App,
    bucket: uuid::Uuid,
    key: &str,
    fallback: Option<uuid::Uuid>,
) -> bool {
    let id = crate::app::reads::storage(bucket, key).or(fallback);
    id.is_none_or(|id| {
        app.storage
            .profile(id)
            .is_ok_and(|profile| profile.file_delivery == "application")
    })
}

pub(crate) async fn prepare(
    app: &Arc<App>,
    stream: &StoredStream,
    metadata: &Metadata,
    request: &HeaderMap,
    head: bool,
    checksum: bool,
    headers: &mut HeaderMap,
) -> Result<bool> {
    if stream.payload_type != "raw" {
        return Ok(false);
    }
    let Some(row) = crate::app::reads::raw(stream.id, stream.payload_revision) else {
        return Ok(false);
    };
    let profile = app.storage.profile(row.source.storage_id)?;
    if profile.file_delivery == "application" {
        return Ok(false);
    }
    let eligible = !head
        && !checksum
        && proxy_request(request, &profile.file_delivery)
        && proxy_response(metadata, headers)
        && profile.read_integrity != "inline"
        && !row.source.pending_upload
        && complete(&row, stream.size);
    if !eligible {
        app.file_delivery.fallback.fetch_add(1, Relaxed);
        return Ok(false);
    }
    row.validate()?;
    let address = match row.source.medium.as_str() {
        "local" => {
            let reader = app.storage.raw_reader(&row.source, false).await?;
            if matches!(reader.file, crate::storage::raw::RawFile::Memory { .. }) {
                app.file_delivery.fallback.fetch_add(1, Relaxed);
                return Ok(false);
            }
            app.storage.check_delivery_directory(&row.source).await?;
            row.source.address()
        }
        "shared_s3"
            if profile.file_delivery == "x_accel_redirect"
                && profile.cache_enabled
                && app.storage.file_cache().delivery_raw(&row.source).await? =>
        {
            row.source.location_id
        }
        _ => {
            app.file_delivery.fallback.fetch_add(1, Relaxed);
            return Ok(false);
        }
    };
    if app.storage.profile(row.source.storage_id)?.revision != profile.revision {
        return Err(
            s3s::s3_error!(ServiceUnavailable, "file delivery settings changed; retry").into(),
        );
    }
    handoff(&profile, address, headers)?;
    if row.source.medium == "shared_s3" {
        app.storage.access.demand(
            &mut Default::default(),
            crate::model::tiering::Source::Raw(row.source.location_id),
            stream.size as usize,
            false,
        );
    }
    if row.source.medium == "local" && profile.read_integrity == "background" {
        app.read_checks.raw_source(row.source.clone());
    }
    if profile.file_delivery == "x_accel_redirect" {
        &app.file_delivery.accel
    } else {
        &app.file_delivery.sendfile
    }
    .fetch_add(1, Relaxed);
    app.file_delivery
        .bytes
        .fetch_add(stream.size as u64, Relaxed);
    Ok(true)
}

fn complete(row: &RawRange, size: i64) -> bool {
    row.offset_bytes == 0 && row.source_offset == 0 && row.length == size && row.source.size == size
}

pub(crate) fn proxy_request(headers: &HeaderMap, mode: &str) -> bool {
    headers
        .get("x-mokyu-file-delivery")
        .is_some_and(|value| value.as_bytes() == mode.as_bytes())
        && !super::object::conditional(headers)
        && !headers.contains_key(header::RANGE)
        && !headers.contains_key(header::IF_RANGE)
        && !headers.contains_key("x-amz-checksum-mode")
        && !super::compression::no_transform(headers)
}

pub(crate) fn proxy_response(metadata: &Metadata, headers: &HeaderMap) -> bool {
    metadata.user.as_ref().is_none_or(|user| user.is_empty())
        && !metadata
            .cache_control
            .as_deref()
            .is_some_and(super::compression::no_transform_value)
        && metadata.content_encoding.is_none()
        && !headers.contains_key(header::CONTENT_ENCODING)
        && !super::compression::no_transform(headers)
}

pub(crate) fn handoff(
    profile: &crate::storage::profile::StorageProfile,
    address: uuid::Uuid,
    headers: &mut HeaderMap,
) -> Result<()> {
    let value = format!(
        "{}{}",
        profile
            .file_delivery_prefix
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("missing delivery prefix"))?,
        crate::storage::chunk_name(address)
    );
    let name = if profile.file_delivery == "x_accel_redirect" {
        "x-accel-redirect"
    } else {
        "x-sendfile"
    };
    headers.insert(name, value.parse()?);
    // The upstream response carries metadata only; the proxy supplies the file length.
    headers.insert(header::CONTENT_LENGTH, "0".parse()?);
    if name == "x-accel-redirect" {
        headers.remove(header::ACCEPT_RANGES);
        // Nginx otherwise retains these across a failed file open and an application retry.
        for (field, internal) in [
            (header::CACHE_CONTROL, "x-mokyu-cache-control"),
            (header::CONTENT_DISPOSITION, "x-mokyu-content-disposition"),
            (header::EXPIRES, "x-mokyu-expires"),
        ] {
            if let Some(value) = headers.remove(field) {
                headers.insert(internal, value);
            }
        }
    }
    Ok(())
}
