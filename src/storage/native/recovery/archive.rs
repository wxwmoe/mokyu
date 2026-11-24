use super::scan::Scanner;
use crate::{
    model::{Error, storage_payload::Intent},
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

    pub fn frozen(scan: Arc<Scanner>, catalog: Uuid, namespace: Uuid) -> Result<Self> {
        let media = scan
            .offline
            .as_ref()
            .context("offline media required")?
            .sources
            .keys()
            .copied()
            .collect();
        Ok(Self {
            scan,
            catalog,
            namespace,
            media,
        })
    }

    async fn targets(&self, intent: &Intent, sequence: u32) -> Result<Vec<super::part::Candidate>> {
        let receipts = self
            .scan
            .clusters
            .catalog_shards(self.catalog, intent.operation, sequence)
            .await?;
        let mut candidates = Vec::new();
        for receipt in receipts {
            intent.accepts(&receipt.header)?;
            if self.scan.backup_group.is_none() {
                let media = receipt.header.media;
                candidates.push((receipt, media, false));
                continue;
            }
            for media in &self.media {
                candidates.push((receipt.clone(), *media, true));
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
        if intent.cluster != self.scan.anchor.cluster_id || !intent.layout.supported() {
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
                let candidates=source.targets(&intent,seq as u32).await.map_err(|e|failure(&file,e))?;
                let (len, mut part)=source.scan.range(&intent,&candidates,start,requested.end).await.map_err(|e|failure(&file,e))?;
                while let Some(bytes)=part.try_next().await.map_err(|e|failure(&file,e))? { yield bytes; }
                start+=len;
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
