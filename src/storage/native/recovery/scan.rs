use super::*;
use crate::{
    content::fragment::Area,
    model::{
        storage_cluster::Manifest,
        storage_recovery::{Link, Proof, RecoveryCatalog, verify},
        storage_transfer::{Action, Destination, SignedGrant},
    },
    storage::native::{
        control::Control,
        transport::{Inventory, Pool, Renewal},
    },
};
use futures_util::StreamExt;
use std::sync::Arc;

#[derive(Debug)]
struct InterruptedInventory;
impl std::fmt::Display for InterruptedInventory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("frozen source inventory interrupted")
    }
}
impl std::error::Error for InterruptedInventory {}

pub(crate) struct Scanner {
    pub(super) clusters: Arc<dyn RecoveryCatalog>,
    control: Option<Arc<Control>>,
    pub(super) offline: Option<Arc<super::media::Media>>,
    pub(super) pool: Arc<Pool>,
    pub anchor: Manifest,
    pub(super) backup_group: Option<String>,
}

#[async_trait::async_trait]
impl Renewal for Scanner {
    async fn renew(&self, signed: &SignedGrant) -> Result<SignedGrant> {
        Ok(self
            .control
            .as_ref()
            .context("control is unavailable during offline recovery")?
            .recovery_target(
                &self.anchor,
                signed.grant.media,
                signed.grant.action.clone(),
                Some(signed),
            )
            .await?
            .authorization)
    }
}

impl Scanner {
    pub async fn open(
        clusters: Arc<dyn StorageClusters>,
        control: Arc<Control>,
        cluster: Uuid,
        backup_group: Option<String>,
    ) -> Result<Arc<Self>> {
        let (value, _) = clusters.placement_snapshot(cluster).await?;
        Ok(Arc::new(Self {
            clusters,
            control: Some(control),
            offline: None,
            pool: Arc::new(Pool::new(Duration::from_secs(90))),
            anchor: value.manifest,
            backup_group,
        }))
    }

    pub fn frozen(
        clusters: Arc<dyn RecoveryCatalog>,
        anchor: Manifest,
        media: Arc<super::media::Media>,
        backup_group: Option<String>,
    ) -> Arc<Self> {
        Arc::new(Self {
            clusters,
            control: None,
            pool: Arc::new(Pool::new(Duration::from_secs(60))),
            offline: Some(media),
            anchor,
            backup_group,
        })
    }

    pub(super) async fn target(&self, media: Uuid, action: Action) -> Result<Destination> {
        if let Some(group) = &self.backup_group {
            ensure!(
                self.anchor.members.iter().any(|m| m.media_id == media
                    && m.topology.purpose == "backup"
                    && &m.topology.storage_group == group),
                "media is outside the selected recovery group"
            );
        }
        self.control
            .as_ref()
            .context("control is unavailable during offline recovery")?
            .recovery_target(&self.anchor, media, action, None)
            .await
    }

    pub async fn namespaces(
        self: &Arc<Self>,
        media: Uuid,
    ) -> Result<
        futures_util::stream::BoxStream<
            'static,
            Result<crate::model::storage_recovery::EvidenceEntry>,
        >,
    > {
        let target = self.target(media, Action::Namespaces).await?;
        let mut pages = self.pool.inventory(target, self.clone()).await?;
        Ok(Box::pin(async_stream::try_stream! {
            while let Some(page) = pages.try_next().await? {
                let page = match page { Inventory::Evidence(page) => page,
                    _ => Err(anyhow::anyhow!("expected namespace evidence"))?, };
                for entry in page.entries { yield entry; }
            }
        }))
    }

    async fn cursor(&self, id: Uuid, media: Uuid, scope: &str) -> Result<(i64, u16, Option<Uuid>)> {
        let mut after = None;
        loop {
            let page = self.clusters.catalog_cursors(id, after).await?;
            if page.is_empty() {
                return Ok((0, 0, None));
            }
            for (revision, cursor) in &page {
                if cursor.media == media && cursor.scope == scope {
                    return Ok((
                        *revision,
                        if cursor.complete { 0 } else { cursor.prefix },
                        Some(cursor.boot),
                    ));
                }
            }
            let last = &page.last().unwrap().1;
            after = Some((last.media, last.scope.clone()));
        }
    }

    async fn scan(
        self: &Arc<Self>,
        id: Uuid,
        namespace: Uuid,
        media: Uuid,
        scope: &str,
        alias: usize,
        heads: &mut Vec<Link>,
    ) -> Result<()> {
        let (mut revision, prefix, previous_view) = self.cursor(id, media, scope).await?;
        let action = match scope {
            "layouts" => Action::Layouts,
            "history" => Action::History { namespace },
            "recovery" => Action::Scan {
                area: Area::Recovery,
                prefix,
            },
            _ => anyhow::bail!("invalid recovery scan scope"),
        };
        let (boot, fence, mut pages, prefix) = if let Some(disks) = &self.offline {
            let source = &disks
                .sources
                .get(&media)
                .context("unknown recovery medium")?[alias]
                .1;
            let view = source.view();
            let prefix = if previous_view == Some(view) {
                prefix
            } else {
                0
            };
            let pages = source
                .pages(media, namespace, scope, prefix)
                .await
                .context(InterruptedInventory)?;
            use crate::node::storage::frozen::Page;
            (
                view,
                self.anchor.record_id,
                pages
                    .map_err(|e| e.context(InterruptedInventory))
                    .map_ok(|p| match p {
                        Page::Evidence(p) => Inventory::Evidence(p),
                        Page::Fragments(p) => Inventory::Fragments(p),
                    })
                    .boxed(),
                prefix,
            )
        } else {
            let target = self.target(media, action).await?;
            (
                target.authorization.grant.boot,
                target.authorization.grant.fence,
                self.pool.inventory(target, self.clone()).await?,
                prefix,
            )
        };
        let mut cursor = Cursor {
            media,
            boot,
            fence,
            manifest: self.anchor.digest()?,
            scan: Uuid::now_v7(),
            scope: scope.into(),
            page: 0,
            prefix,
            complete: false,
        };
        let mut head = None;
        while let Some(page) = pages.try_next().await? {
            let mut entries = Vec::new();
            match page {
                Inventory::Evidence(page) => {
                    cursor.page = page.number;
                    cursor.complete = page.complete;
                    if cursor.page > 0 {
                        ensure!(page.head == head, "recovery head changed during scan");
                    }
                    head = page.head;
                    if scope == "history"
                        && let Some(link) = &head
                        && !heads.contains(link)
                    {
                        heads.push(link.clone());
                    }
                    for entry in page.entries {
                        if entry.pending || entry.error.is_some() {
                            continue;
                        }
                        if let Some(proof) = entry.proof {
                            entries.push(CatalogEntry::Proof(proof));
                        }
                        if let Some(manifest) = entry.manifest {
                            entries.push(CatalogEntry::Manifest(manifest));
                        }
                    }
                }
                Inventory::Fragments(page) => {
                    cursor.page = page.number;
                    cursor.prefix = if page.number == 0 {
                        prefix
                    } else {
                        page.restart_prefix
                    };
                    cursor.complete = page.complete;
                    for entry in page.entries {
                        if entry.pending || entry.error.is_some() {
                            continue;
                        }
                        if let Some(receipt) = entry.receipt
                            && receipt.header.namespace == namespace
                        {
                            ensure!(
                                (receipt.header.media == media
                                    || self
                                        .anchor
                                        .members
                                        .iter()
                                        .any(|m| m.media_id == media
                                            && m.topology.purpose == "backup"))
                                    && entry.header.as_ref() == Some(&receipt.header),
                                "recovery fragment belongs to another source"
                            );
                            if receipt.header.media != media {
                                entries.push(CatalogEntry::BackupLocation {
                                    fragment: receipt.header.fragment,
                                    media,
                                    namespace,
                                    cluster: self.anchor.cluster_id,
                                });
                            }
                            entries.push(CatalogEntry::Fragment(receipt));
                        }
                    }
                }
            }
            revision = self
                .clusters
                .catalog_checkpoint(id, revision, &cursor, &entries)
                .await?;
        }
        if !cursor.complete {
            let err = anyhow::anyhow!("source inventory ended before its final page");
            return Err(if self.offline.is_some() {
                err.context(InterruptedInventory)
            } else {
                err
            });
        }
        Ok(())
    }

    pub async fn collect(
        self: &Arc<Self>,
        id: Uuid,
        namespace: Uuid,
        media: &[Uuid],
    ) -> Result<Proof> {
        self.collect_at(id, namespace, media, None).await
    }
    pub async fn collect_at(
        self: &Arc<Self>,
        id: Uuid,
        namespace: Uuid,
        media: &[Uuid],
        digest: Option<&str>,
    ) -> Result<Proof> {
        ensure!(
            !media.is_empty()
                && media.len() <= 256
                && media.iter().collect::<std::collections::HashSet<_>>().len() == media.len(),
            "select distinct recovery members"
        );
        self.clusters
            .catalog_open(id, namespace, &self.anchor)
            .await?;
        let mut heads = Vec::new();
        for member in media {
            let count = self
                .offline
                .as_ref()
                .and_then(|m| m.sources.get(member))
                .map_or(1, Vec::len);
            for alias in 0..count {
                for scope in ["layouts", "history", "recovery"] {
                    if let Err(e) = self
                        .scan(id, namespace, *member, scope, alias, &mut heads)
                        .await
                    {
                        if !e.is::<InterruptedInventory>() {
                            return Err(e);
                        }
                        eprintln!(
                            "Recovery medium {member}, source {}, {scope}: {e:#}",
                            alias + 1
                        );
                        break;
                    }
                }
            }
        }
        let head = if let Some(digest) = digest {
            let mut after = None;
            let link = loop {
                let page = self.clusters.catalog_page(id, 1, None, after).await?;
                let mut found = None;
                for entry in page.entries {
                    if let CatalogEntry::Proof(p) = entry {
                        let link = p.link()?;
                        if hex::encode(link.digest) == digest {
                            found = Some(link);
                            break;
                        }
                    }
                }
                if let Some(link) = found {
                    break link;
                }
                after = Some(
                    page.after
                        .context("requested recovery publication was not found")?,
                );
            };
            verify::rooted_chain(self.clusters.as_ref(), id, &self.anchor, &[link]).await?
        } else {
            verify::rooted_chain(self.clusters.as_ref(), id, &self.anchor, &heads).await?
        };
        self.index_proof(id, &head).await?;
        Ok(head)
    }

    pub async fn index_proof(&self, id: Uuid, head: &Proof) -> Result<()> {
        self.clusters.catalog_bind(id, head).await?;
        let mut after = 0;
        let mut previous = None;
        loop {
            let proofs = self
                .clusters
                .catalog_proofs(id, after, head.publication.revision)
                .await?;
            if proofs.is_empty() {
                break;
            }
            for proof in proofs {
                ensure!(
                    proof.publication.revision == after + 1,
                    "competing or missing recovery publication"
                );
                proof.follows(previous.as_ref())?;
                self.clusters.catalog_index(id, &proof).await?;
                after = proof.publication.revision;
                previous = Some(proof);
            }
        }
        ensure!(
            previous.as_ref() == Some(head),
            "recovery file index did not reach its head"
        );
        Ok(())
    }
}
