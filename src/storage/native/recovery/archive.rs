use super::scan::Scanner;
use crate::{
    model::{Error, storage_payload::Intent, storage_transfer::Action},
    storage::native::{
        reader,
        store::{failure, metadata},
    },
};
use anyhow::{Context, Result, ensure};
use futures_util::TryStreamExt;
use object_store::{GetOptions, GetResult, GetResultPayload, path::Path};
use std::sync::Arc;
use uuid::Uuid;

// Only a confirmed recovery job constructs this reader. Its catalog is separate from live storage heads.
pub(crate) struct Archive {
    scan: Arc<Scanner>,
    catalog: Uuid,
    namespace: Uuid,
    media: Vec<Uuid>,
}

impl Archive {
    pub fn new(scan: Arc<Scanner>, catalog: Uuid, namespace: Uuid, group: &str) -> Result<Self> {
        let media: Vec<_> = scan
            .anchor
            .members
            .iter()
            .filter(|m| m.topology.purpose == "backup" && m.topology.storage_group == group)
            .map(|m| m.media_id)
            .collect();
        ensure!(
            scan.backup_group.as_deref() == Some(group)
                && !catalog.is_nil()
                && !namespace.is_nil()
                && !media.is_empty(),
            "unknown recovery backup group"
        );
        Ok(Self {
            scan,
            catalog,
            namespace,
            media,
        })
    }

    async fn targets(
        &self,
        intent: &Intent,
        sequence: u32,
        start: u64,
        length: u64,
    ) -> Result<Vec<crate::model::storage_transfer::Destination>> {
        let receipts = self
            .scan
            .clusters
            .catalog_shards(self.catalog, intent.operation, sequence)
            .await?;
        let mut candidates = Vec::new();
        for receipt in receipts {
            let h = &receipt.header;
            intent.accepts(h)?;
            let offset = start
                .checked_sub(h.offset)
                .context("invalid backup read offset")?;
            ensure!(
                offset
                    .checked_add(length)
                    .is_some_and(|end| end <= h.shard_bytes),
                "backup read exceeds verified fragment"
            );
            for media in &self.media {
                let action = Action::BackupRead {
                    receipt: receipt.clone(),
                    offset,
                    length,
                };
                if let Ok(target) = self.scan.target(*media, action).await {
                    candidates.push(target);
                }
            }
        }
        ensure!(
            !candidates.is_empty(),
            "no backup location for the recovered stripe"
        );
        Ok(candidates)
    }

    pub async fn get(
        self: &Arc<Self>,
        path: &Path,
        name: &str,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let intent = self
            .scan
            .clusters
            .catalog_image(self.catalog, self.namespace, name)
            .await
            .map_err(|e| failure(path, e))?
            .ok_or_else(|| failure(path, Error::StorageObjectMissing.into()))?;
        if intent.cluster != self.scan.anchor.cluster_id
            || intent.layout.data_shards != 1
            || intent.layout.parity_shards != 0
        {
            return Err(failure(path, anyhow::anyhow!("unsupported backup layout")));
        }
        if options
            .version
            .as_deref()
            .is_some_and(|v| Uuid::parse_str(v).ok() != Some(intent.operation))
        {
            return Err(failure(path, Error::StorageObjectChanged.into()));
        }
        let meta = metadata(
            path.clone(),
            &reader::Metadata {
                path: name.into(),
                version: intent.operation,
                size: intent.total,
                modified: 0,
            },
        )
        .map_err(|e| failure(path, e))?;
        options.check_preconditions(&meta)?;
        let range = options
            .range
            .map(|r| r.as_range(meta.size))
            .transpose()
            .map_err(|e| failure(path, e.into()))?
            .unwrap_or(0..meta.size);
        let source = self.clone();
        let file = path.clone();
        let requested = range.clone();
        let body = async_stream::try_stream! {
            let mut start=requested.start;
            while !options.head && start<requested.end {
                let seq=start/intent.span();
                let end=requested.end.min((seq+1)*intent.span()).min(start+reader::PART);
                let copies=source.targets(&intent,seq as u32,start,end-start).await.map_err(|e|failure(&file,e))?;
                let mut part=reader::part(source.scan.pool.clone(),source.scan.clone(),reader::Part{offset:start,length:end-start,copies}).await.map_err(|e|failure(&file,e))?;
                while let Some(bytes)=part.try_next().await.map_err(|e|failure(&file,e))? { yield bytes; }
                start=end;
            }
        };
        Ok(GetResult {
            payload: GetResultPayload::Stream(Box::pin(body)),
            meta,
            range,
            attributes: Default::default(),
            extensions: Default::default(),
        })
    }
}
