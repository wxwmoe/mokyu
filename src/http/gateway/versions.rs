use super::*;
use crate::model::history::{Page, Selection};
pub(crate) fn selection(input: &ListObjectVersionsInput) -> Selection {
    Selection {
        prefix: input.prefix.clone().unwrap_or_default(),
        delimiter: input.delimiter.clone(),
        key_marker: input.key_marker.clone(),
        version_marker: input.version_id_marker.clone(),
        max_keys: input.max_keys,
    }
}
pub(crate) fn listing(
    input: ListObjectVersionsInput,
    page: Page,
) -> Result<S3Response<ListObjectVersionsOutput>> {
    let limit = selection(&input).limit()?;
    let encode = |k: String| {
        if input.encoding_type.is_some() {
            encode_key(&k)
        } else {
            k
        }
    };
    let mut versions = Vec::new();
    let mut markers = Vec::new();
    for row in page.rows {
        let date = chrono::DateTime::from_timestamp_micros(row.modified_us)
            .context("invalid version timestamp")?;
        if let Some(size) = row.size {
            versions.push(ObjectVersion {
                key: Some(encode(row.key)),
                version_id: Some(row.version_id),
                is_latest: Some(row.latest),
                last_modified: Some(stamp(date)),
                e_tag: row.etag.map(ETag::Strong),
                size: Some(size),
                storage_class: Some(ObjectVersionStorageClass::from("STANDARD".to_owned())),
                ..Default::default()
            });
        } else {
            markers.push(DeleteMarkerEntry {
                key: Some(encode(row.key)),
                version_id: Some(row.version_id),
                is_latest: Some(row.latest),
                last_modified: Some(stamp(date)),
                ..Default::default()
            });
        }
    }
    let prefixes = page
        .prefixes
        .into_iter()
        .map(|key| CommonPrefix {
            prefix: Some(encode(key)),
        })
        .collect();
    Ok(S3Response::new(ListObjectVersionsOutput {
        name: Some(input.bucket),
        prefix: input.prefix.map(&encode),
        delimiter: input.delimiter.map(&encode),
        max_keys: Some(limit as i32),
        encoding_type: input.encoding_type.clone(),
        is_truncated: Some(page.next_key.is_some()),
        key_marker: input.key_marker.map(&encode),
        version_id_marker: input.version_id_marker,
        next_key_marker: page.next_key.map(&encode),
        next_version_id_marker: page.next_version,
        versions: Some(versions),
        delete_markers: Some(markers),
        common_prefixes: Some(prefixes),
        ..Default::default()
    }))
}
