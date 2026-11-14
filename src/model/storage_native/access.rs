use crate::{
    content::{codec, pack, raw_pages},
    model::{
        Error,
        node_read::{Mapping, Source},
    },
    storage::location::Layout,
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::ops::Range;
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Read {
    pub windows: Vec<Uuid>,
    #[serde(default)]
    pub upload: Option<Uuid>,
    #[serde(default)]
    pub work: Option<Uuid>,
    pub storage: Uuid,
    pub path: String,
    pub range: Option<Range<u64>>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReadRequest {
    pub access: Read,
    pub version: Option<Uuid>,
    #[serde(default)]
    pub lease: Option<super::ReadLease>,
    pub region: Option<String>,
}

impl ReadRequest {
    pub fn validate(&self) -> Result<()> {
        self.access.validate()?;
        ensure!(
            self.version.is_none_or(|id| !id.is_nil())
                && self
                    .lease
                    .as_ref()
                    .is_none_or(|lease| self.version == Some(lease.operation))
                && self.region.as_ref().is_none_or(|value| !value.is_empty()
                    && value.len() <= 128
                    && !value.chars().any(char::is_control)),
            "invalid native read request"
        );
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Upload {
    pub operation: Uuid,
    #[serde(default)]
    pub work: bool,
    pub batch: Option<i64>,
    pub storage: Uuid,
    pub path: String,
    pub size: u64,
}

pub(crate) struct UploadAccess {
    pub owner: Uuid,
    pub cluster: Uuid,
    pub expires: i64,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Maintenance {
    pub operation: Uuid,
    pub storage: Uuid,
    pub action: MaintenanceAction,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum MaintenanceAction {
    List { after: Option<String> },
    Delete { path: String },
    Collect { payload: Uuid },
}

impl Maintenance {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.operation.is_nil() && !self.storage.is_nil(),
            "invalid native maintenance identity"
        );
        match &self.action {
            MaintenanceAction::List { after } => {
                if let Some(path) = after {
                    super::path(path)?;
                }
            }
            MaintenanceAction::Delete { path } => {
                super::path(path)?;
            }
            MaintenanceAction::Collect { payload } => {
                ensure!(!payload.is_nil(), "invalid native cleanup payload");
            }
        }
        Ok(())
    }
}

impl Upload {
    pub fn multipart(&self, prefix: &str, plan: &crate::model::node_native::Work) -> Result<Uuid> {
        use crate::model::node_native::Action;
        let (file, size) = match &plan.action {
            Action::Part { file, size, .. } => (*file, *size),
            Action::Tail { size, .. } => (plan.upload.source, i64::from(*size)),
            _ => return Err(s3s::s3_error!(AccessDenied).into()),
        };
        ensure!(
            self.work
                && self.batch.is_none()
                && self.storage == plan.upload.storage
                && size >= 0
                && self.size == size as u64
                && self.matches(
                    prefix,
                    object_store::path::Path::from(crate::storage::fragment_key(file))
                )?,
            s3s::s3_error!(AccessDenied)
        );
        Ok(file)
    }

    pub fn validate(&self) -> Result<()> {
        super::path(&self.path)?;
        ensure!(
            !self.operation.is_nil()
                && !self.storage.is_nil()
                && self.batch.is_none_or(|batch| batch > 0)
                && (!self.work || self.batch.is_none())
                && self.size <= 5 * 1024_u64.pow(4),
            "invalid native upload request"
        );
        Ok(())
    }

    pub fn matches(&self, prefix: &str, path: object_store::path::Path) -> Result<bool> {
        let prefix = object_store::path::Path::parse(prefix)?;
        Ok(self.path
            == if prefix.as_ref().is_empty() {
                path.to_string()
            } else {
                format!("{prefix}/{path}")
            })
    }

    pub fn raw(&self, prefix: &str, location: Uuid, size: i64) -> Result<Uuid> {
        ensure!(
            size >= 0
                && self.size == size as u64
                && self.matches(prefix, Layout::S3.object(location))?,
            s3s::s3_error!(AccessDenied)
        );
        Ok(location)
    }

    pub fn tail(&self, prefix: &str, fragment: Uuid, size: i32) -> Result<Uuid> {
        ensure!(
            size > 0
                && size as usize <= codec::MAX
                && self.size == size as u64
                && self.matches(
                    prefix,
                    object_store::path::Path::from(crate::storage::fragment_key(fragment))
                )?,
            s3s::s3_error!(AccessDenied)
        );
        Ok(fragment)
    }

    pub fn chunks(
        &self,
        prefix: &str,
        items: &[crate::model::node_chunk::Item],
        packing: Option<&crate::model::node_pack::Batch>,
    ) -> Result<Uuid> {
        if let Some(packing) = packing {
            for index in 0..packing.packs.len() {
                let pack = packing.pack(self.storage, index)?;
                if self.matches(prefix, Layout::S3.pack(pack.address()))? {
                    ensure!(
                        (pack::frame::HEADER.len() as u64..=pack::MAX_PAYLOAD as u64)
                            .contains(&self.size),
                        s3s::s3_error!(AccessDenied)
                    );
                    return Ok(pack.address());
                }
            }
            let packed = packing.packed_chunks()?;
            for (index, target) in packing.chunks.iter().enumerate() {
                if packed[index] || target.ready.is_some() {
                    continue;
                }
                let chunk = packing.chunk(self.storage, index)?;
                if self.matches(prefix, Layout::S3.chunk(chunk.address()))? {
                    let maximum =
                        u64::try_from(chunk.raw_size)? + 16 + codec::FILE_HEADER.len() as u64;
                    ensure!(
                        self.size > codec::FILE_HEADER.len() as u64 && self.size <= maximum,
                        s3s::s3_error!(AccessDenied)
                    );
                    return Ok(chunk.address());
                }
            }
        } else {
            for item in items
                .iter()
                .filter(|item| !item.reused && item.chunk.storage_id == Some(self.storage))
            {
                if self.matches(prefix, Layout::S3.chunk(item.chunk.address()))? {
                    ensure!(
                        self.size == item.chunk.file_size()? as u64,
                        s3s::s3_error!(AccessDenied)
                    );
                    return Ok(item.chunk.address());
                }
            }
        }
        Err(s3s::s3_error!(AccessDenied).into())
    }
}

impl Read {
    pub fn validate(&self) -> Result<()> {
        super::path(&self.path)?;
        ensure!(
            !self.storage.is_nil()
                && [
                    self.upload.is_some(),
                    self.work.is_some(),
                    !self.windows.is_empty()
                ]
                .into_iter()
                .filter(|value| *value)
                .count()
                    == 1
                && self.windows.len() <= 16
                && self.upload.is_none_or(|id| !id.is_nil())
                && self.work.is_none_or(|id| !id.is_nil())
                && self.windows.iter().all(|id| !id.is_nil())
                && self
                    .range
                    .as_ref()
                    .is_none_or(|span| span.start <= span.end && span.end <= i64::MAX as u64),
            Error::NodeReadLimit
        );
        Ok(())
    }

    pub fn include(
        &self,
        prefix: &str,
        mapping: &Mapping,
        ranges: &mut Vec<Range<u64>>,
    ) -> Result<()> {
        ensure!(
            mapping.segments.len() <= crate::model::node_read::MAX_ITEMS,
            Error::NodeReadLimit
        );
        for segment in &mapping.segments {
            if segment.source.storage()? != Some(self.storage) {
                continue;
            }
            if self.marker(prefix)? {
                ranges.push(0..crate::storage::marker::MAX_BYTES as u64);
                continue;
            }
            let (path, range) = match &segment.source {
                Source::Chunk { chunk } => (
                    Layout::S3.chunk(chunk.address()),
                    0..chunk.file_size()? as u64,
                ),
                Source::Pack {
                    pack,
                    chunk,
                    ordinal,
                    offset,
                } => {
                    let size =
                        u64::try_from(pack.stored_size.context(codec::IntegrityError::Metadata)?)?;
                    let position = pack::frame::Position {
                        ordinal: *ordinal,
                        offset: *offset,
                        encoding: pack::frame::Encoding::capture(chunk)?,
                    };
                    (
                        Layout::S3.pack(pack.address()),
                        position.range(chunk.raw_size, size, pack.member_count)?,
                    )
                }
                Source::Raw { source } => {
                    let start = u64::try_from(segment.source_offset)?;
                    let end = start
                        .checked_add(u64::try_from(segment.length)?)
                        .context(codec::IntegrityError::Length)?;
                    let size = u64::try_from(source.size)?;
                    ensure!(end <= size, codec::IntegrityError::Length);
                    // Range integrity reads the complete intersecting pages, including their boundaries.
                    let page = raw_pages::PAGE as u64;
                    (
                        Layout::S3.object(source.address()),
                        start / page * page..end.div_ceil(page).saturating_mul(page).min(size),
                    )
                }
                Source::Staged { key, size, .. } => (
                    object_store::path::Path::parse(key)?,
                    0..u64::try_from(*size)?,
                ),
                Source::Local { .. } | Source::Fragment { .. } => continue,
            };
            let prefix = object_store::path::Path::parse(prefix)?;
            let path = if prefix.as_ref().is_empty() {
                path.to_string()
            } else {
                format!("{prefix}/{path}")
            };
            if path == self.path {
                ranges.push(range);
            }
        }
        Ok(())
    }

    pub fn include_completion(
        &self,
        prefix: &str,
        plan: &crate::model::node_native::Completion,
        ranges: &mut Vec<Range<u64>>,
    ) -> Result<()> {
        ensure!(
            self.upload.is_some() && self.storage == plan.upload.storage,
            s3s::s3_error!(AccessDenied)
        );
        let total = plan.size()?;
        let prefix = object_store::path::Path::parse(prefix)?;
        let matches = |path: object_store::path::Path| {
            self.path
                == if prefix.as_ref().is_empty() {
                    path.to_string()
                } else {
                    format!("{prefix}/{path}")
                }
        };
        if matches(Layout::S3.object(plan.upload.source)) {
            ranges.push(0..total);
        }
        for part in &plan.parts {
            let file = Uuid::parse_str(
                part.remote_etag
                    .as_deref()
                    .ok_or(Error::SourceUnavailable)?,
            )?;
            if matches(object_store::path::Path::from(
                crate::storage::fragment_key(file),
            )) {
                ranges.push(0..part.size as u64);
            }
        }
        Ok(())
    }

    pub fn marker(&self, prefix: &str) -> Result<bool> {
        let prefix = object_store::path::Path::parse(prefix)?;
        Ok(self.path
            == if prefix.as_ref().is_empty() {
                "mokyu.json".into()
            } else {
                format!("{prefix}/mokyu.json")
            })
    }

    pub fn check(&self, ranges: &mut [Range<u64>]) -> Result<()> {
        ensure!(!ranges.is_empty(), s3s::s3_error!(AccessDenied));
        let Some(requested) = &self.range else {
            return Ok(());
        };
        ranges.sort_unstable_by_key(|range| (range.start, range.end));
        let mut covered = requested.start;
        for range in ranges {
            if range.start <= covered && range.end >= covered {
                covered = range.end;
                if covered >= requested.end {
                    return Ok(());
                }
            }
        }
        Err(s3s::s3_error!(AccessDenied).into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Id, node_read::Segment, raw::RawSource};

    #[test]
    fn completion_reads_only_frozen_carriers() -> Result<()> {
        let file = Uuid::now_v7();
        let storage = Uuid::now_v7();
        let mut plan = crate::model::node_native::Completion {
            upload: crate::model::multipart::RemoteUpload {
                upload: Uuid::now_v7(),
                state: "completing".into(),
                source: Uuid::now_v7(),
                storage,
                profile: 1,
                upstream: Some("native".into()),
                manifest: Some("frozen".into()),
            },
            parts: vec![crate::model::node_native::Part {
                number: 1,
                size: 17,
                etag: "a".repeat(32),
                remote_etag: Some(file.to_string()),
            }],
        };
        let mut request = Read {
            windows: vec![],
            upload: Some(Uuid::now_v7()),
            work: None,
            storage,
            path: format!("prefix/{}", crate::storage::fragment_key(file)),
            range: Some(0..17),
        };
        let mut ranges = vec![];
        request.include_completion("prefix", &plan, &mut ranges)?;
        request.check(&mut ranges)?;
        request.range = Some(0..18);
        assert!(request.check(&mut ranges).is_err());
        request.range = None;
        request.path = format!("prefix/{}", crate::storage::fragment_key(Uuid::now_v7()));
        ranges.clear();
        request.include_completion("prefix", &plan, &mut ranges)?;
        assert!(request.check(&mut ranges).is_err());
        request.path = format!("prefix/{}", Layout::S3.object(plan.upload.source));
        request.include_completion("prefix", &plan, &mut ranges)?;
        request.check(&mut ranges)?;
        request.storage = Uuid::now_v7();
        assert!(
            request
                .include_completion("prefix", &plan, &mut vec![])
                .is_err()
        );
        request.storage = storage;
        plan.parts[0].remote_etag = None;
        assert!(
            request
                .include_completion("prefix", &plan, &mut vec![])
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn native_ranges_preserve_gaps_and_bind_storage_and_prefix() -> Result<()> {
        let storage = Uuid::now_v7();
        let mut request = Read {
            windows: vec![Uuid::now_v7()],
            upload: None,
            work: None,
            storage,
            path: "prefix/objects/test".into(),
            range: Some(10..30),
        };
        request.validate()?;
        assert!(request.check(&mut [10..19, 20..30]).is_err());
        request.check(&mut [20..30, 10..20])?;
        request.range = Some(10..31);
        assert!(request.check(&mut [10..30]).is_err());
        request.range = None;
        assert!(request.check(&mut []).is_err());
        let source = RawSource {
            id: Id::Integer(1),
            location_id: Uuid::now_v7(),
            address: None,
            home_storage_id: None,
            storage_id: storage,
            size: 3 * raw_pages::PAGE as i64,
            digest: Some(vec![0; 16]),
            state: "ready".into(),
            medium: "shared".into(),
            pending_upload: false,
            revision: 1,
            quarantine_code: None,
        };
        request.path = format!("prefix/{}", Layout::S3.object(source.address()));
        let mapping = Mapping {
            stream: Id::Integer(1),
            revision: 1,
            start: 0,
            end: 1,
            segments: vec![Segment {
                page_revision: None,
                offset: 0,
                length: 1,
                source_offset: raw_pages::PAGE as i64 + 31,
                source: Source::Raw { source },
            }],
        };
        let mut ranges = Vec::new();
        request.include("prefix", &mapping, &mut ranges)?;
        assert_eq!(ranges, [raw_pages::PAGE as u64..2 * raw_pages::PAGE as u64]);
        request.range = Some(0..1);
        assert!(request.check(&mut ranges).is_err());
        ranges.clear();
        request.include("another", &mapping, &mut ranges)?;
        assert!(ranges.is_empty());
        request.storage = Uuid::now_v7();
        request.include("prefix", &mapping, &mut ranges)?;
        assert!(ranges.is_empty());
        Ok(())
    }
}
