use super::{files::Files, fragments};
use crate::{
    content::fragment::{Area, HEADER_SIZE, Header},
    model::storage_recovery::{EvidenceEntry, EvidencePage, Link},
    storage::Disk,
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{fs::ReadDir, io::Read, sync::Arc};
use tokio::sync::OwnedRwLockWriteGuard;
use uuid::Uuid;

pub(crate) use crate::model::storage_recovery::{InventoryEntry as Entry, InventoryPage as Page};

const PAGE_LIMIT: usize = 128;

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EvidenceKind {
    History,
    Layouts,
    Namespaces,
}

pub(super) struct EvidenceScan {
    directory: Option<ReadDir>,
    owner: Uuid,
    kind: EvidenceKind,
    root: std::path::PathBuf,
    head: Option<Link>,
    last: Option<EvidencePage>,
    check: fragments::Check,
    _freeze: OwnedRwLockWriteGuard<()>,
    _lock: Arc<std::fs::File>,
}

impl EvidenceScan {
    pub fn open(
        files: &Files,
        kind: EvidenceKind,
        owner: Uuid,
        check: fragments::Check,
    ) -> Result<Self> {
        ensure!(!owner.is_nil(), "invalid evidence owner");
        check()?;
        let freeze = files
            .mutations
            .clone()
            .try_write_owned()
            .context("storage still has active mutations")?;
        let mut path = files.disk.root.join(match kind {
            EvidenceKind::History | EvidenceKind::Namespaces => "recovery/history",
            EvidenceKind::Layouts => "cluster/history",
        });
        if !matches!(kind, EvidenceKind::Namespaces) {
            path = path.join(owner.simple().to_string());
        }
        let head = if matches!(kind, EvidenceKind::History) {
            let path = files
                .disk
                .root
                .join("recovery/heads")
                .join(format!("{}.json", owner.simple()));
            fragments::check_directories(&files.disk.root, &path)?;
            super::recovery::read(&path)?
                .map(|bytes| serde_json::from_slice(&bytes))
                .transpose()?
        } else {
            None
        };
        fragments::check_directories(&files.disk.root, &path.join("entry"))?;
        let directory = match std::fs::read_dir(path) {
            Ok(value) => Some(value),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            directory,
            owner,
            kind,
            root: files.disk.root.clone(),
            head,
            last: None,
            check,
            _freeze: freeze,
            _lock: files.lock.clone(),
        })
    }

    pub fn page(&mut self, number: u64) -> Result<EvidencePage> {
        (self.check)()?;
        if let Some(last) = &self.last {
            if last.number == number {
                return Ok(last.clone());
            }
            ensure!(
                !last.complete && last.number.checked_add(1) == Some(number),
                "evidence page is out of sequence"
            );
        } else {
            ensure!(number == 0, "evidence scan must begin with page zero");
        }
        let mut page = EvidencePage {
            number,
            entries: Vec::new(),
            head: self.head.clone(),
            complete: false,
        };
        while page.entries.len() < 4 {
            (self.check)()?;
            let Some(item) = self.directory.as_mut().and_then(Iterator::next) else {
                page.complete = true;
                break;
            };
            let item = item?;
            let name = item
                .file_name()
                .into_string()
                .map_err(|_| anyhow::anyhow!("evidence contains a non-UTF-8 filename"))?;
            let pending = name.ends_with(".part");
            let mut entry = EvidenceEntry {
                name,
                pending,
                proof: None,
                manifest: None,
                error: None,
            };
            let result = (|| -> Result<()> {
                if matches!(self.kind, EvidenceKind::Namespaces) {
                    let namespace = Uuid::parse_str(&entry.name)?;
                    ensure!(
                        !namespace.is_nil()
                            && entry.name == namespace.simple().to_string()
                            && item.file_type()?.is_dir(),
                        "invalid recovery namespace directory"
                    );
                    let head = self
                        .root
                        .join("recovery/heads")
                        .join(format!("{}.json", namespace.simple()));
                    fragments::check_directories(&self.root, &head)?;
                    let link: Link = serde_json::from_slice(
                        &super::recovery::read(&head)?.context("recovery head is missing")?,
                    )?;
                    let path = super::recovery::history(&self.root, namespace, link.id);
                    fragments::check_directories(&self.root, &path)?;
                    let proof: crate::model::storage_recovery::Proof = serde_json::from_slice(
                        &super::recovery::read(&path)?.context("recovery head proof is missing")?,
                    )?;
                    ensure!(
                        proof.publication.namespace == namespace && proof.link()? == link,
                        "recovery head differs from its proof"
                    );
                    entry.proof = Some(proof);
                    return Ok(());
                }
                let name = entry.name.strip_suffix(".part").unwrap_or(&entry.name);
                let text = name
                    .strip_suffix(".json")
                    .context("invalid evidence filename")?;
                let id = Uuid::parse_str(text)?;
                ensure!(
                    !id.is_nil() && text == id.simple().to_string(),
                    "invalid evidence identity"
                );
                let bytes = super::recovery::read(&item.path())?
                    .context("evidence disappeared during its frozen scan")?;
                match self.kind {
                    EvidenceKind::History => {
                        let proof: crate::model::storage_recovery::Proof =
                            serde_json::from_slice(&bytes)?;
                        proof.publication.validate()?;
                        ensure!(
                            proof.publication.id == id
                                && proof.publication.namespace == self.owner
                                && proof.signature.len() == 64,
                            "evidence belongs to another path"
                        );
                        entry.proof = Some(proof);
                    }
                    EvidenceKind::Layouts => {
                        let manifest: crate::model::storage_cluster::Manifest =
                            serde_json::from_slice(&bytes)?;
                        manifest.validate()?;
                        ensure!(
                            manifest.record_id == id && manifest.cluster_id == self.owner,
                            "layout belongs to another path"
                        );
                        entry.manifest = Some(manifest);
                    }
                    EvidenceKind::Namespaces => unreachable!(),
                }
                Ok(())
            })();
            if let Err(error) = result {
                entry.error = Some(error.to_string());
            }
            page.entries.push(entry);
        }
        (self.check)()?;
        self.last = Some(page.clone());
        Ok(page)
    }
}

pub(super) struct Scan {
    disk: Arc<Disk>,
    media: Uuid,
    backup: bool,
    area: Area,
    prefix: u16,
    directory: Option<ReadDir>,
    last: Option<Page>,
    check: fragments::Check,
    _freeze: OwnedRwLockWriteGuard<()>,
    _lock: Arc<std::fs::File>,
}

impl Scan {
    pub fn open(files: &Files, area: Area, prefix: u16, check: fragments::Check) -> Result<Self> {
        ensure!(prefix < 4096, "invalid inventory restart prefix");
        check()?;
        let freeze = files
            .mutations
            .clone()
            .try_write_owned()
            .context("storage still has active mutations")?;
        Ok(Self {
            disk: files.disk.clone(),
            media: files.media.media_id,
            backup: files.backup,
            area,
            prefix,
            directory: None,
            last: None,
            check,
            _freeze: freeze,
            _lock: files.lock.clone(),
        })
    }

    pub fn page(&mut self, number: u64) -> Result<Page> {
        (self.check)()?;
        if let Some(last) = &self.last {
            if last.number == number {
                return Ok(last.clone());
            }
            ensure!(
                !last.complete && last.number.checked_add(1) == Some(number),
                "inventory page is out of sequence"
            );
        } else {
            ensure!(number == 0, "inventory must begin with page zero");
        }
        let mut page = Page {
            number,
            restart_prefix: self.prefix,
            entries: Vec::new(),
            complete: false,
        };
        let mut opened = 0;
        while self.prefix < 4096 && page.entries.len() < PAGE_LIMIT && opened < 256 {
            (self.check)()?;
            let path = self
                .disk
                .root
                .join(self.area.directory())
                .join(format!("{:03x}", self.prefix));
            if self.directory.is_none() {
                opened += 1;
                fragments::check_directories(&self.disk.root, &path.join("entry"))?;
                match std::fs::read_dir(&path) {
                    Ok(directory) => self.directory = Some(directory),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        self.prefix += 1;
                        continue;
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            let Some(item) = self.directory.as_mut().unwrap().next() else {
                self.directory = None;
                self.prefix += 1;
                continue;
            };
            let item = item?;
            let name = item
                .file_name()
                .into_string()
                .map_err(|_| anyhow::anyhow!("inventory contains a non-UTF-8 filename"))?;
            let pending = name.ends_with(".part");
            let mut entry = Entry {
                prefix: self.prefix,
                name,
                pending,
                header: None,
                receipt: None,
                error: None,
            };
            let result = (|| -> Result<()> {
                let text = if pending {
                    entry.name.strip_suffix(".part").unwrap()
                } else {
                    &entry.name
                };
                let id = Uuid::parse_str(text)?;
                ensure!(
                    id.simple().to_string() == text && !id.is_nil(),
                    "invalid fragment filename"
                );
                let expected = self.disk.root.join(self.area.path(id));
                ensure!(
                    expected.parent() == Some(path.as_path()),
                    "fragment is in another directory"
                );
                let mut file = fragments::open(&item.path(), false)?;
                let mut bytes = [0; HEADER_SIZE];
                file.read_exact(&mut bytes)?;
                let header = Header::decode(&bytes)?;
                ensure!(
                    header.fragment == id
                        && (header.media == self.media || self.backup)
                        && header.area == self.area,
                    "fragment header differs from its path or media"
                );
                entry.header = Some(header);
                match fragments::inspect_file(&mut file) {
                    Ok(receipt) => entry.receipt = Some(receipt),
                    Err(error) => return Err(error),
                }
                Ok(())
            })();
            if let Err(error) = result {
                entry.error = Some(error.to_string());
            }
            page.entries.push(entry);
        }
        page.complete = self.prefix == 4096;
        (self.check)()?;
        self.last = Some(page.clone());
        Ok(page)
    }
}

#[cfg(test)]
mod tests {
    use super::super::fragments::tests::{allowed, fixture, header, input};
    use super::*;
    use bytes::Bytes;
    use std::collections::HashSet;

    #[tokio::test]
    async fn inventory_retries_and_restarts_without_a_directory_index() -> Result<()> {
        let (root, _, _, files) = fixture("8MiB").await?;
        let mut expected = HashSet::new();
        let mut first = None;
        for index in 1..=135u128 {
            let mut planned = header(&files, 3);
            planned.area = Area::Recovery;
            planned.fragment = Uuid::from_u128(index * 4096 + 0x321);
            let receipt = fragments::write(
                &files,
                planned.clone(),
                input(Bytes::from_static(b"abc"), true),
                allowed(),
            )
            .await?;
            expected.insert(planned.fragment);
            first.get_or_insert(receipt);
        }
        let receipt = first.unwrap();
        let mut scan = Scan::open(&files, Area::Recovery, 0x123, allowed())?;
        let page = scan.page(0)?;
        assert_eq!(page.entries.len(), PAGE_LIMIT);
        assert_eq!(
            serde_json::to_vec(&page)?,
            serde_json::to_vec(&scan.page(0)?)?
        );
        assert!(scan.page(2).is_err());
        assert!(
            fragments::remove(&files, receipt.clone(), allowed())
                .await
                .is_err()
        );
        assert!(
            fragments::write(
                &files,
                header(&files, 0),
                input(Bytes::new(), true),
                allowed()
            )
            .await
            .is_err()
        );
        let restart = page.restart_prefix;
        let mut found: HashSet<_> = page
            .entries
            .iter()
            .map(|e| e.receipt.as_ref().unwrap().header.fragment)
            .collect();
        drop(scan);
        let mut scan = Scan::open(&files, Area::Recovery, restart, allowed())?;
        let mut number = 0;
        loop {
            let page = scan.page(number)?;
            assert!(page.entries.len() <= PAGE_LIMIT);
            for entry in page.entries {
                assert!(!entry.pending && entry.error.is_none());
                found.insert(entry.receipt.unwrap().header.fragment);
            }
            if page.complete {
                break;
            }
            number += 1;
        }
        assert_eq!(found, expected);
        drop(scan);
        assert!(fragments::remove(&files, receipt, allowed()).await?);
        assert!(!root.path().join("catalog").exists());
        assert!(!root.path().join("index.db").exists());
        Ok(())
    }

    #[tokio::test]
    async fn inventory_keeps_partial_and_wrong_identity_files_diagnostic() -> Result<()> {
        let (_directory, _, _, files) = fixture("8MiB").await?;
        let mut planned = header(&files, 3);
        planned.area = Area::Recovery;
        planned.fragment = Uuid::from_u128(0x321);
        assert!(
            fragments::write(
                &files,
                planned.clone(),
                input(Bytes::from_static(b"a"), false),
                allowed()
            )
            .await
            .is_err()
        );
        let pending = files
            .disk
            .root
            .join("recovery/123/00000000000000000000000000000321.part");
        assert!(pending.exists());
        let other = files
            .disk
            .root
            .join("recovery/123/00000000000000000000000000001321");
        std::fs::copy(&pending, &other)?;
        let mut scan = Scan::open(&files, Area::Recovery, 0x123, allowed())?;
        let page = scan.page(0)?;
        assert_eq!(page.entries.len(), 2);
        assert!(
            page.entries
                .iter()
                .all(|entry| entry.receipt.is_none() && entry.error.is_some())
        );
        assert!(
            page.entries
                .iter()
                .any(|entry| entry.pending && entry.header.is_some())
        );
        assert!(
            page.entries
                .iter()
                .any(|entry| !entry.pending && entry.header.is_none())
        );
        Ok(())
    }
}
