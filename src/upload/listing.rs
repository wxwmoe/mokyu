use crate::model::multipart::{UploadListing, UploadSummary};
use anyhow::{Result, ensure};
use s3s::{S3Response, dto::*};
use uuid::Uuid;

pub(crate) struct Page {
    prefix: String,
    delimiter: String,
    marker: Option<String>,
    cursor: String,
    inclusive: bool,
    id: Option<Uuid>,
    limit: usize,
    done: bool,
    result: UploadListing,
}

impl Page {
    pub(crate) fn new(
        prefix: String,
        delimiter: String,
        marker: Option<String>,
        id: Option<Uuid>,
        limit: usize,
    ) -> Result<Self> {
        ensure!(limit <= 1000, s3s::s3_error!(InvalidArgument));
        Ok(Self {
            cursor: marker.clone().unwrap_or_else(|| prefix.clone()),
            inclusive: marker.is_none(),
            id: if marker.is_some() { id } else { None },
            prefix,
            delimiter,
            marker,
            limit,
            done: limit == 0,
            result: UploadListing::default(),
        })
    }

    pub(crate) fn position(&self) -> Option<(&str, bool, Option<Uuid>)> {
        (!self.done).then_some((&self.cursor, self.inclusive, self.id))
    }

    pub(crate) fn append(&mut self, rows: Vec<UploadSummary>) -> Result<()> {
        if rows.is_empty() {
            self.done = true;
        }
        for row in rows {
            ensure!(
                row.object_key.starts_with(&self.prefix),
                "upload outside listing prefix"
            );
            if row.object_key < self.cursor
                || (row.object_key == self.cursor
                    && !self.inclusive
                    && self.id.is_none_or(|id| row.id <= id))
            {
                continue;
            }
            let group = (!self.delimiter.is_empty())
                .then(|| row.object_key[self.prefix.len()..].find(&self.delimiter))
                .flatten()
                .map(|n| row.object_key[..self.prefix.len() + n + self.delimiter.len()].to_owned());
            if let Some(group) = group {
                let after = crate::app::listing::successor(&group);
                self.cursor = after.clone().unwrap_or_default();
                self.inclusive = true;
                self.id = None;
                if self.marker.as_ref().is_none_or(|marker| group > *marker) {
                    if self.full() {
                        break;
                    }
                    self.result.next_key = Some(group.clone());
                    self.result.next_id = None;
                    self.result.prefixes.push(group);
                }
                if after.is_none() {
                    self.done = true;
                    break;
                }
            } else {
                if self.full() {
                    break;
                }
                self.cursor = row.object_key.clone();
                self.inclusive = false;
                self.id = Some(row.id);
                self.result.next_key = Some(row.object_key.clone());
                self.result.next_id = Some(row.id);
                self.result.uploads.push(row);
            }
        }
        Ok(())
    }

    fn full(&mut self) -> bool {
        if self.result.uploads.len() + self.result.prefixes.len() != self.limit {
            return false;
        }
        self.result.truncated = true;
        self.done = true;
        true
    }

    pub(crate) fn finish(mut self) -> UploadListing {
        if !self.result.truncated {
            self.result.next_key = None;
            self.result.next_id = None;
        }
        self.result
    }
}

pub(crate) fn output(
    i: ListMultipartUploadsInput,
    result: UploadListing,
) -> Result<S3Response<ListMultipartUploadsOutput>> {
    ensure!(
        i.encoding_type
            .as_ref()
            .is_none_or(|value| value.as_str() == "url"),
        s3s::s3_error!(InvalidArgument, "encoding-type must be url")
    );
    let encode = |s: String| {
        if i.encoding_type.is_some() {
            crate::app::listing::encode_key(&s)
        } else {
            s
        }
    };
    let uploads = result
        .uploads
        .into_iter()
        .map(|u| MultipartUpload {
            key: Some(encode(u.object_key)),
            upload_id: Some(u.id.to_string()),
            initiated: Some(crate::http::gateway::stamp(u.created_at)),
            storage_class: Some(StorageClass::from_static("STANDARD")),
            checksum_algorithm: u.checksum_algorithm.map(ChecksumAlgorithm::from),
            checksum_type: u.checksum_type.map(ChecksumType::from),
            ..Default::default()
        })
        .collect();
    Ok(S3Response::new(ListMultipartUploadsOutput {
        bucket: Some(i.bucket),
        uploads: Some(uploads),
        common_prefixes: Some(
            result
                .prefixes
                .into_iter()
                .map(|prefix| CommonPrefix {
                    prefix: Some(encode(prefix)),
                })
                .collect(),
        ),
        delimiter: i.delimiter.map(encode),
        is_truncated: Some(result.truncated),
        max_uploads: Some(i.max_uploads.unwrap_or(1000)),
        next_key_marker: result.next_key.map(encode),
        next_upload_id_marker: result.next_id.map(|id| id.to_string()),
        key_marker: i.key_marker.map(encode),
        upload_id_marker: i.upload_id_marker,
        prefix: i.prefix.map(encode),
        encoding_type: i.encoding_type,
        ..Default::default()
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn row(key: &str, n: u128) -> UploadSummary {
        UploadSummary {
            id: Uuid::from_u128(n),
            object_key: key.into(),
            created_at: chrono::Utc::now(),
            checksum_algorithm: None,
            checksum_type: None,
        }
    }
    #[test]
    fn upload_pages_preserve_duplicate_keys_and_skip_delimiter_groups() -> Result<()> {
        let mut page = Page::new("a".into(), "/".into(), None, None, 2)?;
        page.append(vec![
            row("a/x", 1),
            row("a/y", 2),
            row("ab", 3),
            row("ab", 4),
        ])?;
        let result = page.finish();
        assert_eq!(result.prefixes, ["a/"]);
        assert!(result.truncated);
        assert_eq!(result.next_key.as_deref(), Some("ab"));
        assert_eq!(result.next_id, Some(Uuid::from_u128(3)));
        let mut page = Page::new("a".into(), "/".into(), result.next_key, result.next_id, 2)?;
        page.append(vec![row("ab", 3), row("ab", 4)])?;
        page.append(vec![])?;
        let result = page.finish();
        assert_eq!(result.uploads.len(), 1);
        assert_eq!(result.uploads[0].id, Uuid::from_u128(4));
        assert!(!result.truncated && result.next_key.is_none());
        assert!(
            Page::new("".into(), "".into(), None, None, 0)?
                .position()
                .is_none()
        );
        assert!(Page::new("".into(), "".into(), None, None, 1001).is_err());
        Ok(())
    }
}
