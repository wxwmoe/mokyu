use crate::{
    authorization::Principal,
    content::codec::{self, Key},
    model::{
        storage_cluster::{Cluster, StorageClusters, Takeover},
        storage_recovery::{CatalogEntry, Cursor},
    },
};
use anyhow::{Context, Result, ensure};
use futures_util::TryStreamExt;
use std::time::Duration;
use uuid::Uuid;

pub(crate) mod archive;
mod part;
mod replay;
pub(crate) mod scan;

pub(crate) async fn takeover(
    clusters: &dyn StorageClusters,
    principal: &Principal,
    key: &Key,
    input: Takeover,
) -> Result<Cluster> {
    let targets = clusters
        .takeover_targets(principal, &input.media_ids)
        .await?;
    if let Some(assigned) = targets.first().and_then(|t| t.device.cluster_id) {
        ensure!(
            targets
                .iter()
                .all(|t| t.device.cluster_id == Some(assigned)),
            "selected storage includes already assigned members"
        );
        let (value, _) = clusters.placement_snapshot(assigned).await?;
        ensure!(
            value.name == input.name,
            "storage cluster is already registered under another name"
        );
        return Ok(value);
    }
    ensure!(
        targets.iter().all(|t| t.device.cluster_id.is_none()),
        "selected storage includes already assigned members"
    );
    let anchor = targets
        .iter()
        .filter_map(|t| t.device.announcement.manifest.as_ref())
        .max_by_key(|m| m.revision)
        .context("selected storage has no committed layout")?
        .clone();
    let mut hash = blake3::Hasher::new_derive_key("mokyu/storage/layout-recovery/v1");
    hash.update(&key.material);
    hash.update(anchor.digest()?.as_bytes());
    let catalog = Uuid::from_bytes(hash.finalize().as_bytes()[..16].try_into()?);
    clusters
        .catalog_open(catalog, anchor.cluster_id, &anchor)
        .await?;
    let pool = super::transport::Pool::new(Duration::from_secs(90));
    for target in targets {
        let head = target
            .device
            .announcement
            .manifest
            .as_ref()
            .context("storage has no committed layout")?;
        ensure!(
            head.cluster_id == anchor.cluster_id,
            "selected storage belongs to different clusters"
        );
        let run = target
            .run
            .filter(|_| target.expires > chrono::Utc::now().timestamp_micros())
            .context("selected storage is offline")?;
        let credential = codec::unprotect(
            target
                .credential
                .as_deref()
                .context("storage credential is missing")?,
            key,
            crate::cluster::credential_binding(target.device.node_id).as_bytes(),
        )?;
        let mut revision = 0;
        let mut after = None;
        loop {
            let page = clusters.catalog_cursors(catalog, after).await?;
            if page.is_empty() {
                break;
            }
            for (saved, cursor) in &page {
                if cursor.media == target.device.media_id && cursor.scope == "layouts" {
                    revision = *saved;
                }
            }
            let last = &page.last().unwrap().1;
            after = Some((last.media, last.scope.clone()));
        }
        let mut pages = pool
            .layouts(
                &target.device.announcement.topology.url,
                &credential,
                run,
                head,
            )
            .await?;
        let scan = Uuid::now_v7();
        while let Some(page) = pages.try_next().await? {
            let mut entries = Vec::new();
            for entry in page.entries {
                if entry.pending {
                    continue;
                }
                ensure!(
                    entry.error.is_none() && entry.proof.is_none(),
                    "storage layout history is damaged"
                );
                entries.push(CatalogEntry::Manifest(
                    entry.manifest.context("layout record is missing")?,
                ));
            }
            revision = clusters
                .catalog_checkpoint(
                    catalog,
                    revision,
                    &Cursor {
                        media: target.device.media_id,
                        boot: run.boot,
                        fence: run.fence,
                        manifest: anchor.digest()?,
                        scan,
                        scope: "layouts".into(),
                        page: page.number,
                        prefix: 0,
                        complete: page.complete,
                    },
                    &entries,
                )
                .await?;
        }
    }
    clusters
        .takeover(principal, key, catalog, &anchor, input)
        .await
}
