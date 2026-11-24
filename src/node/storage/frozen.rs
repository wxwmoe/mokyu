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
    pub media: Media,
    pub manifest: Manifest,
}

pub(crate) struct Medium {
    root: PathBuf,
    pub identity: Identity,
    backup: bool,
}

fn record<T: DeserializeOwned>(root: &Path, path: &Path) -> Result<T> {
    fragments::check_directories(root, path)?;
    let bytes = recovery::read(path)?.with_context(|| format!("missing {}", path.display()))?;
    Ok(serde_json::from_slice(&bytes)?)
}

impl Medium {
    pub fn open(root: PathBuf) -> Result<Arc<Self>> {
        let root = root.canonicalize()?;
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
        Ok(Arc::new(Self {
            root,
            identity: Identity { media, manifest },
            backup,
        }))
    }

    fn check(&self) -> Result<()> {
        let media: Media = record(&self.root, &self.root.join("media.json"))?;
        ensure!(media == self.identity.media, "source media changed");
        Ok(())
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
