use super::{
    fragments,
    inventory::{EvidenceKind, EvidenceScan, Scan},
    recovery,
};
use crate::{
    content::fragment::Area,
    model::{
        storage_cluster::{Manifest, Media},
        storage_recovery::{EvidencePage, InventoryPage},
    },
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Identity {
    pub view: Uuid,
    pub media: Media,
    pub manifest: Manifest,
}

pub(crate) const MAX_RANGE: u64 = 1024 * 1024;

pub(crate) struct Medium {
    root: PathBuf,
    pub identity: Identity,
    backup: bool,
    #[cfg(unix)]
    markers: [std::fs::Metadata; 2],
}

fn record<T: DeserializeOwned>(root: &Path, path: &Path) -> Result<T> {
    fragments::check_directories(root, path)?;
    let bytes = recovery::read(path)?.with_context(|| format!("missing {}", path.display()))?;
    Ok(serde_json::from_slice(&bytes)?)
}

impl Medium {
    pub fn open(root: PathBuf) -> Result<Arc<Self>> {
        let root = root.canonicalize()?;
        #[cfg(unix)]
        let markers = [
            std::fs::symlink_metadata(root.join("media.json"))?,
            std::fs::symlink_metadata(root.join("cluster.json"))?,
        ];
        let media: Media = record(&root, &root.join("media.json"))?;
        media.validate()?;
        let manifest: Manifest = record(&root, &root.join("cluster.json"))?;
        manifest.validate()?;
        let history = root
            .join("cluster/history")
            .join(manifest.cluster_id.simple().to_string())
            .join(format!("{}.json", manifest.record_id.simple()));
        let fixed: Manifest = record(&root, &history)?;
        ensure!(
            fixed == manifest,
            "cluster history differs from its committed manifest"
        );
        let member = manifest
            .members
            .iter()
            .find(|m| m.media_id == media.media_id)
            .context("media does not belong to its committed cluster")?;
        let backup = member.topology.purpose == "backup";
        let medium = Arc::new(Self {
            #[cfg(unix)]
            markers,
            root,
            identity: Identity {
                media,
                manifest,
                view: Uuid::new_v4(),
            },
            backup,
        });
        medium.check()?;
        Ok(medium)
    }

    pub fn range(
        &self,
        receipt: &crate::model::storage_payload::Receipt,
        offset: u64,
        length: u64,
    ) -> Result<crate::rpc::wire::RecoveryBytes> {
        use crate::{
            content::fragment::UNIT,
            rpc::wire::{RecoveryBytes, RecoveryUnit},
        };
        self.check()?;
        receipt.validate()?;
        let header = &receipt.header;
        ensure!(
            header.cluster == self.identity.manifest.cluster_id
                && (self.backup || header.media == self.identity.media.media_id),
            "fragment belongs to a different medium"
        );
        let end = offset.checked_add(length).context("range overflow")?;
        ensure!(
            length > 0 && length <= MAX_RANGE && end <= header.shard_bytes,
            "invalid recovery range"
        );
        let path = self.root.join(header.area.path(header.fragment));
        fragments::check_directories(&self.root, &path)?;
        let mut file = fragments::open(&path, false)?;
        ensure!(
            fragments::inspect_file(&mut file)? == *receipt,
            "fragment differs from its verified receipt"
        );
        let mut proof = fragments::ReadProof::default();
        let mut units = Vec::new();
        for i in offset / UNIT as u64..=(end - 1) / UNIT as u64 {
            let data = fragments::unit(&mut file, receipt, i as usize, &mut proof)?;
            units.push(RecoveryUnit {
                data,
                siblings: proof.hashes.iter().map(|v| v.to_vec()).collect(),
            });
        }
        self.check()?;
        Ok(RecoveryBytes { units })
    }

    pub(super) fn check(&self) -> Result<()> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            for (name, before) in ["media.json", "cluster.json"]
                .into_iter()
                .zip(&self.markers)
            {
                let now = std::fs::symlink_metadata(self.root.join(name))?;
                ensure!(
                    now.is_file()
                        && (
                            now.dev(),
                            now.ino(),
                            now.ctime(),
                            now.ctime_nsec(),
                            now.size()
                        ) == (
                            before.dev(),
                            before.ino(),
                            before.ctime(),
                            before.ctime_nsec(),
                            before.size()
                        ),
                    "frozen source view changed"
                );
            }
        }
        #[cfg(not(unix))]
        {
            let media: Media = record(&self.root, &self.root.join("media.json"))?;
            let manifest: Manifest = record(&self.root, &self.root.join("cluster.json"))?;
            ensure!(
                media == self.identity.media && manifest == self.identity.manifest,
                "frozen source view changed"
            );
        }
        Ok(())
    }

    pub fn pages(
        self: &Arc<Self>,
        namespace: Uuid,
        scope: Scope,
        prefix: u16,
    ) -> futures_util::stream::BoxStream<'static, Result<Page>> {
        use futures_util::StreamExt;
        let medium = self.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let task = tokio::task::spawn_blocking(move || -> Result<()> {
            let mut pages = medium.scan(namespace, scope, prefix)?;
            for index in 0.. {
                let (page, done) = pages.next(index)?;
                if tx.blocking_send(page).is_err() || done {
                    break;
                }
            }
            Ok(())
        });
        async_stream::try_stream! {
            let mut rx=rx;
            while let Some(page)=rx.recv().await { yield page; }
            task.await??;
        }
        .boxed()
    }

    pub fn scan(self: &Arc<Self>, namespace: Uuid, scope: Scope, prefix: u16) -> Result<Pages> {
        self.check()?;
        let check: fragments::Check = Arc::new(|| Ok(()));
        let reader = match scope {
            Scope::Layouts => Reader::Evidence(EvidenceScan::frozen(
                &self.root,
                EvidenceKind::Layouts,
                self.identity.manifest.cluster_id,
                check,
            )?),
            Scope::History => Reader::Evidence(EvidenceScan::frozen(
                &self.root,
                EvidenceKind::History,
                namespace,
                check,
            )?),
            Scope::Recovery => Reader::Fragments(Scan::frozen(
                &self.root,
                self.identity.media.media_id,
                self.backup,
                Area::Recovery,
                prefix,
                check,
            )?),
        };
        Ok(Pages {
            medium: self.clone(),
            reader,
        })
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Scope {
    Layouts,
    History,
    Recovery,
}

pub(crate) struct Pages {
    medium: Arc<Medium>,
    reader: Reader,
}

enum Reader {
    Evidence(EvidenceScan),
    Fragments(Scan),
}

#[derive(Serialize, Deserialize)]
pub(crate) enum Page {
    Evidence(EvidencePage),
    Fragments(InventoryPage),
}

impl Pages {
    pub fn next(&mut self, index: u64) -> Result<(Page, bool)> {
        let result = match &mut self.reader {
            Reader::Evidence(scan) => {
                let page = scan.page(index)?;
                let done = page.complete;
                (Page::Evidence(page), done)
            }
            Reader::Fragments(scan) => {
                let page = scan.page(index)?;
                let done = page.complete;
                (Page::Fragments(page), done)
            }
        };
        self.medium.check()?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn an_empty_mount_is_not_initialized() -> Result<()> {
        let dir = tempfile::tempdir()?;
        assert!(Medium::open(dir.path().to_owned()).is_err());
        assert_eq!(dir.path().read_dir()?.count(), 0);
        Ok(())
    }

    #[test]
    #[ignore = "requires a frozen storage member and an online baseline"]
    fn frozen_publication_chain() -> Result<()> {
        let root =
            PathBuf::from(std::env::var_os("MOKYU_TEST_FROZEN_MEDIA").context("missing media")?);
        let namespace = Uuid::parse_str(&std::env::var("MOKYU_TEST_FROZEN_NAMESPACE")?)?;
        let disk = Medium::open(root)?;
        let mut manifests = BTreeMap::new();
        let mut proofs = BTreeMap::new();
        let mut head = None;
        let mut receipts = 0;
        for scope in [Scope::Layouts, Scope::History, Scope::Recovery] {
            let mut pages = disk.scan(namespace, scope, 0)?;
            for i in 0.. {
                let (page, done) = pages.next(i)?;
                match page {
                    Page::Evidence(page) => {
                        if page.head.is_some() {
                            head = page.head;
                        }
                        for entry in page.entries {
                            ensure!(
                                entry.error.is_none() && !entry.pending,
                                "unexpected fixture candidate"
                            );
                            if let Some(manifest) = entry.manifest {
                                manifests.insert(manifest.digest()?, manifest);
                            }
                            if let Some(proof) = entry.proof {
                                proofs.insert(proof.publication.revision, proof);
                            }
                        }
                    }
                    Page::Fragments(page) => {
                        receipts += page
                            .entries
                            .iter()
                            .filter(|e| {
                                !e.pending
                                    && e.receipt
                                        .as_ref()
                                        .is_some_and(|r| r.header.namespace == namespace)
                            })
                            .count();
                    }
                }
                if done {
                    break;
                }
            }
        }
        let mut previous = None;
        for p in proofs.values() {
            p.verify(
                manifests
                    .get(&p.publication.manifest)
                    .context("missing historical manifest")?,
            )?;
            p.follows(previous)?;
            previous = Some(p);
        }
        let proof = previous.context("missing proof")?;
        ensure!(
            Some(proof.link()?) == head && receipts > 0,
            "unrooted fixture"
        );
        println!(
            "media={} publication={} through={} receipts={}",
            disk.identity.media.media_id,
            hex::encode(proof.link()?.digest),
            proof.publication.through,
            receipts
        );
        Ok(())
    }
}
