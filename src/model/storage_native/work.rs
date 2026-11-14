use super::access::{Read, Upload};
use crate::{
    content::{codec, pack},
    model::{
        PhysicalSource, node_pack,
        node_work::{Plan, Rewrite},
        transfer::Input,
    },
    storage::location::Layout,
};
use anyhow::{Context, Result, ensure};
use object_store::path::Path;
use std::ops::Range;
use uuid::Uuid;

pub(crate) fn deletion(
    plan: &Plan,
    namespace: Uuid,
    prefix: &str,
    name: &str,
) -> Result<Option<i64>> {
    use crate::model::node_work::Physical;
    ensure!(
        plan.storage() == Some(namespace)
            || matches!(plan, Plan::Tier { work, .. } if matches!(work.as_ref(), crate::model::tiering::Work::Remove { source } if source.storage().ok() == Some(namespace))),
        s3s::s3_error!(AccessDenied)
    );
    let matches = |path: Path| {
        name == if prefix.is_empty() {
            path.to_string()
        } else {
            format!("{prefix}/{path}")
        }
    };
    match plan {
        Plan::Collect {
            items,
            delete_before,
            ..
        } => {
            for item in items {
                let path = match &item.source {
                    Physical::Chunk => Layout::S3.chunk(item.location),
                    Physical::Pack => Layout::S3.pack(item.location),
                    Physical::Raw { .. } => Layout::S3.object(item.location),
                    Physical::Native { .. } => continue,
                    Physical::Staging { key } => {
                        ensure!(
                            crate::storage::is_fragment_key(key),
                            s3s::s3_error!(AccessDenied)
                        );
                        Path::parse(key)?
                    }
                };
                if matches(path) {
                    return Ok(item.inspect_age.then_some(delete_before.timestamp_micros()));
                }
            }
        }
        Plan::SweepDelete { items, before, .. } => {
            for item in items {
                if matches(Path::parse(&item.path)?) {
                    ensure!(
                        Layout::S3.parse_chunk_or_pack(&item.path).is_some(),
                        s3s::s3_error!(AccessDenied)
                    );
                    return Ok(Some(
                        before
                            .timestamp_micros()
                            .min(item.modified.timestamp_micros()),
                    ));
                }
            }
        }
        Plan::Tier { work, .. } => {
            if let crate::model::tiering::Work::Remove { source } = work.as_ref() {
                ensure!(
                    source.storage()? == namespace && matches(source.shared_path()),
                    s3s::s3_error!(AccessDenied)
                );
                return Ok(None);
            }
        }
        _ => {}
    }
    Err(s3s::s3_error!(AccessDenied).into())
}

impl Read {
    fn carrier(
        &self,
        prefix: &str,
        storage: Option<Uuid>,
        path: Path,
        size: u64,
        ranges: &mut Vec<Range<u64>>,
    ) -> Result<()> {
        if storage != Some(self.storage) {
            return Ok(());
        }
        if self.marker(prefix)? {
            ranges.push(0..crate::storage::marker::MAX_BYTES as u64);
        }
        let full = if prefix.is_empty() {
            path.to_string()
        } else {
            format!("{prefix}/{path}")
        };
        if self.path == full {
            ranges.push(0..size);
        }
        Ok(())
    }

    fn physical(
        &self,
        prefix: &str,
        source: &PhysicalSource,
        ranges: &mut Vec<Range<u64>>,
    ) -> Result<()> {
        self.carrier(
            prefix,
            Some(source.storage()?),
            source.shared_path(),
            source.bytes()?.try_into()?,
            ranges,
        )
    }

    fn input(&self, prefix: &str, source: &Input, ranges: &mut Vec<Range<u64>>) -> Result<()> {
        match source {
            Input::Chunk { chunk } => self.carrier(
                prefix,
                chunk.storage_id,
                Layout::S3.chunk(chunk.address()),
                chunk.file_size()? as u64,
                ranges,
            ),
            Input::Pack { pack, .. } => self.carrier(
                prefix,
                pack.storage_id,
                Layout::S3.pack(pack.address()),
                pack.stored_size
                    .context("missing pack length")?
                    .try_into()?,
                ranges,
            ),
            Input::Raw { source } => self.carrier(
                prefix,
                Some(source.storage_id),
                Layout::S3.object(source.address()),
                source.size.try_into()?,
                ranges,
            ),
        }
    }

    fn packing(
        &self,
        prefix: &str,
        batch: &node_pack::Batch,
        ranges: &mut Vec<Range<u64>>,
    ) -> Result<()> {
        for source in &batch.sources {
            match source {
                node_pack::Source::Chunk { chunk } => self.carrier(
                    prefix,
                    chunk.storage_id,
                    Layout::S3.chunk(chunk.address()),
                    chunk.file_size()? as u64,
                    ranges,
                )?,
                node_pack::Source::Pack { pack, .. } => self.carrier(
                    prefix,
                    pack.storage_id,
                    Layout::S3.pack(pack.address()),
                    pack.stored_size
                        .context("missing pack length")?
                        .try_into()?,
                    ranges,
                )?,
            }
        }
        Ok(())
    }

    pub fn include_work(
        &self,
        prefix: &str,
        plan: &Plan,
        ranges: &mut Vec<Range<u64>>,
    ) -> Result<()> {
        if plan.storage() == Some(self.storage) && self.marker(prefix)? {
            ranges.push(0..crate::storage::marker::MAX_BYTES as u64);
        }
        match plan {
            Plan::Tier { work, .. } => {
                self.physical(prefix, work.source(), ranges)?;
                if let Some(source) = work.target() {
                    self.physical(prefix, source, ranges)?;
                }
            }
            Plan::HashRaw { items, .. } => {
                for item in items {
                    self.carrier(
                        prefix,
                        Some(item.source.storage_id),
                        Layout::S3.object(item.source.address()),
                        item.source.size.try_into()?,
                        ranges,
                    )?;
                }
            }
            Plan::ConvertRaw { source, .. }
            | Plan::ScanTransfer(crate::model::transfer::Scan { source, .. }) => {
                self.carrier(
                    prefix,
                    Some(source.storage_id),
                    Layout::S3.object(source.address()),
                    source.size.try_into()?,
                    ranges,
                )?;
            }
            Plan::Inspect {
                snapshot,
                inspect: true,
                ..
            } => self.physical(prefix, &snapshot.physical, ranges)?,
            Plan::Audit { items, .. } => {
                for item in items.iter().filter(|item| item.previous.is_none()) {
                    if let Some(snapshot) = &item.snapshot {
                        self.physical(prefix, &snapshot.physical, ranges)?;
                    }
                }
            }
            Plan::Pack { batch, .. } => self.packing(prefix, batch, ranges)?,
            Plan::CopyTransfer {
                transfer, batch, ..
            } => {
                self.input(prefix, &transfer.input, ranges)?;
                self.packing(prefix, batch, ranges)?;
            }
            Plan::RawTransfer { transfer, .. } => {
                self.input(prefix, &transfer.input, ranges)?;
                self.carrier(
                    prefix,
                    Some(transfer.target.storage_id),
                    Layout::S3.object(transfer.target.address()),
                    transfer.target.size.try_into()?,
                    ranges,
                )?;
            }
            Plan::Rewrite { items, .. } => {
                for item in items {
                    match item {
                        Rewrite::Chunk { source, .. } => self.carrier(
                            prefix,
                            source.storage_id,
                            Layout::S3.chunk(source.address()),
                            source.file_size()? as u64,
                            ranges,
                        )?,
                        Rewrite::Pack { source, .. } => self.carrier(
                            prefix,
                            source.storage_id,
                            Layout::S3.pack(source.address()),
                            source
                                .stored_size
                                .context("missing pack length")?
                                .try_into()?,
                            ranges,
                        )?,
                    }
                }
            }
            Plan::Collect { storage, items, .. } => {
                for item in items.iter().filter(|item| item.inspect_age) {
                    let path = match &item.source {
                        crate::model::node_work::Physical::Chunk => Layout::S3.chunk(item.location),
                        crate::model::node_work::Physical::Pack => Layout::S3.pack(item.location),
                        crate::model::node_work::Physical::Raw { .. } => {
                            Layout::S3.object(item.location)
                        }
                        crate::model::node_work::Physical::Staging { key } => Path::parse(key)?,
                        crate::model::node_work::Physical::Native { .. } => continue,
                    };
                    // Maintenance may inspect metadata without gaining access to the body.
                    self.carrier(prefix, Some(*storage), path, 0, ranges)?;
                }
            }
            Plan::SweepDelete { storage, items, .. } => {
                for item in items {
                    self.carrier(prefix, Some(*storage), Path::parse(&item.path)?, 0, ranges)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn work_scope_separates_sources_outputs_and_metadata() -> Result<()> {
        let storage = Uuid::now_v7();
        let source = crate::model::raw::RawSource {
            id: crate::model::Id::Integer(1),
            location_id: Uuid::now_v7(),
            address: None,
            home_storage_id: None,
            storage_id: storage,
            size: 1024,
            digest: Some(vec![0; 16]),
            state: "ready".into(),
            medium: "shared".into(),
            pending_upload: false,
            revision: 1,
            quarantine_code: None,
        };
        let operation = Uuid::now_v7();
        let mut input = Upload {
            operation,
            work: true,
            batch: None,
            storage,
            path: format!("scope/{}", Layout::S3.object(source.address())),
            size: 1024,
        };
        input.validate()?;
        let upload = Plan::UploadRaw {
            storage,
            items: vec![crate::model::node_work::UploadFile {
                task: Uuid::now_v7(),
                source: source.clone(),
                hashing: false,
            }],
        };
        assert_eq!(input.for_work("scope", &upload)?, source.address());
        input.size += 1;
        assert!(input.for_work("scope", &upload).is_err());
        input.size -= 1;
        assert!(input.for_work("other", &upload).is_err());
        let mut target = source.clone();
        target.storage_id = Uuid::now_v7();
        target.address = Some(Uuid::now_v7());
        let to = PhysicalSource::Raw(target.clone());
        let copy = Plan::Tier {
            storage: target.storage_id,
            work: Box::new(crate::model::tiering::Work::Copy {
                source: PhysicalSource::Raw(source.clone()),
                target: to.clone(),
                rate: 1024,
                recode: false,
                progress: Default::default(),
            }),
            source_permits: [storage, target.storage_id],
        };
        assert!(input.for_work("scope", &copy).is_err());
        let mut output = input.clone();
        output.storage = target.storage_id;
        output.path = format!("scope/{}", to.shared_path());
        assert_eq!(output.for_work("scope", &copy)?, target.address());
        output.size += 1;
        assert!(output.for_work("scope", &copy).is_err());
        assert!(deletion(&copy, storage, "scope", &input.path).is_err());
        let retire = Plan::Tier {
            storage: target.storage_id,
            work: Box::new(crate::model::tiering::Work::Remove {
                source: PhysicalSource::Raw(source.clone()),
            }),
            source_permits: [storage, target.storage_id],
        };
        assert_eq!(deletion(&retire, storage, "scope", &input.path)?, None);
        assert!(deletion(&retire, target.storage_id, "scope", &input.path).is_err());
        assert!(deletion(&retire, storage, "scope", &output.path).is_err());
        let hash = Plan::HashRaw {
            storage,
            items: vec![crate::model::node_work::RawHash { source, hash: None }],
        };
        assert!(input.for_work("scope", &hash).is_err());
        let mut read = Read {
            windows: Vec::new(),
            upload: None,
            work: Some(operation),
            storage,
            path: input.path.clone(),
            range: Some(0..1024),
        };
        read.validate()?;
        let mut spans = Vec::new();
        read.include_work("scope", &upload, &mut spans)?;
        assert!(read.check(&mut spans).is_err());
        read.include_work("scope", &hash, &mut spans)?;
        read.check(&mut spans)?;
        read.range = Some(0..1025);
        assert!(read.check(&mut spans).is_err());
        read.path = "scope/mokyu.json".into();
        read.range = None;
        spans.clear();
        read.include_work("scope", &upload, &mut spans)?;
        read.check(&mut spans)?;
        read.storage = Uuid::now_v7();
        spans.clear();
        read.include_work("scope", &hash, &mut spans)?;
        assert!(read.check(&mut spans).is_err());
        read.upload = Some(operation);
        assert!(read.validate().is_err());
        assert!(super::super::path(&"a".repeat(1024)).is_ok());
        assert!(super::super::path(&"a".repeat(1025)).is_err());
        Ok(())
    }
}

impl Upload {
    pub fn for_work(&self, prefix: &str, plan: &Plan) -> Result<Uuid> {
        ensure!(
            plan.storage() == Some(self.storage),
            s3s::s3_error!(AccessDenied)
        );
        match plan {
            Plan::Pack { batch, .. } | Plan::CopyTransfer { batch, .. } => {
                return self.chunks(prefix, &[], Some(batch));
            }
            Plan::UploadRaw { items, .. } => {
                for item in items {
                    if self.matches(prefix, Layout::S3.object(item.source.address()))? {
                        return self.raw(prefix, item.source.address(), item.source.size);
                    }
                }
            }
            Plan::RawTransfer { transfer, .. } => {
                return self.raw(prefix, transfer.target.address(), transfer.target.size);
            }
            Plan::Rewrite { items, .. } => {
                for item in items {
                    let (path, owner, minimum, maximum) = match item {
                        Rewrite::Chunk { target, .. } => (
                            Layout::S3.chunk(target.location_id),
                            target.location_id,
                            codec::FILE_HEADER.len() as u64 + 1,
                            u64::try_from(target.raw_size)? + 16 + codec::FILE_HEADER.len() as u64,
                        ),
                        Rewrite::Pack {
                            target,
                            replacements,
                            ..
                        } => {
                            let last = replacements.last().context("missing pack replacement")?;
                            let size = last
                                .position
                                .offset
                                .checked_add(u64::try_from(last.position.encoding.stored_size)?)
                                .context("pack replacement length overflow")?;
                            (
                                Layout::S3.pack(target.location_id),
                                target.location_id,
                                size,
                                size,
                            )
                        }
                    };
                    if self.matches(prefix, path)? {
                        ensure!(
                            (minimum..=maximum).contains(&self.size)
                                && self.size <= pack::MAX_PAYLOAD as u64,
                            s3s::s3_error!(AccessDenied)
                        );
                        return Ok(owner);
                    }
                }
            }
            Plan::Tier { work, .. } => {
                if let Some(target) = work.target() {
                    ensure!(
                        target.storage()? == self.storage
                            && self.matches(prefix, target.shared_path())?
                            && self.size == u64::try_from(target.bytes()?)?,
                        s3s::s3_error!(AccessDenied)
                    );
                    return Ok(target.address());
                }
            }
            _ => {}
        }
        Err(s3s::s3_error!(AccessDenied).into())
    }
}
