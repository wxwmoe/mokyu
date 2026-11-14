use crate::{
    config::{Budget, Instance},
    model::storage_cluster::{Capacity, FORMAT, Manifest, Media, Registration},
    storage::Disk,
};
use anyhow::{Context, Result, ensure};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};
use uuid::Uuid;

pub(super) struct Files {
    pub disk: Arc<Disk>,
    pub media: Media,
    pub manifest: RwLock<Option<Manifest>>,
    pub pending: RwLock<Option<Manifest>>,
    pub history: std::sync::Mutex<Option<Manifest>>,
    pub mutations: Arc<tokio::sync::RwLock<()>>,
    limit: Option<u64>,
    pub(super) lock: Arc<std::fs::File>,
}

async fn read<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    ensure!(
        tokio::fs::symlink_metadata(path).await?.is_file(),
        "storage record is not a regular file"
    );
    serde_json::from_slice(&crate::storage::local::read_bounded(path, 128 * 1024).await?)
        .context("invalid storage record; check the media before recovery")
}

fn history_path(root: &Path, manifest: &Manifest) -> PathBuf {
    root.join("cluster/history")
        .join(manifest.cluster_id.simple().to_string())
        .join(format!("{}.json", manifest.record_id.simple()))
}

async fn verify_history(root: &Path, manifest: &Manifest) -> Result<()> {
    let history: Manifest = read(&history_path(root, manifest))
        .await
        .context("cluster history is missing or damaged")?;
    ensure!(
        history.digest()? == manifest.digest()?,
        "cluster history differs from its head"
    );
    Ok(())
}

fn replacement(old: &Manifest, candidate: &Manifest) -> bool {
    old.cluster_id == candidate.cluster_id
        && candidate.revision >= old.revision
        && (candidate == old
            || candidate.revision > old.revision
                && (candidate.takeover_epoch == old.takeover_epoch
                    && candidate.deployment_id == old.deployment_id
                    && candidate.publication_key == old.publication_key
                    || candidate.takeover_epoch > old.takeover_epoch))
}

impl Files {
    pub async fn open(
        instance: &Instance,
        budget: &Budget,
        lock: Arc<std::fs::File>,
    ) -> Result<Self> {
        let root = &instance.data_path;
        let media_path = root.join("media.json");
        let pending = root.join("media.json.part");
        let media: Media = if tokio::fs::try_exists(&media_path).await? {
            read(&media_path).await?
        } else {
            let mut entries = tokio::fs::read_dir(root).await?;
            while let Some(entry) = entries.next_entry().await? {
                ensure!(
                    matches!(
                        entry.file_name().to_str(),
                        Some("node.json" | "gateway.lock" | "media.json.part")
                    ),
                    "media.json is missing from a nonempty storage directory"
                );
            }
            let media = if tokio::fs::try_exists(&pending).await? {
                read::<Media>(&pending).await?
            } else {
                Media {
                    format_version: FORMAT,
                    media_id: Uuid::now_v7(),
                }
            };
            media.validate()?;
            let bytes = serde_json::to_vec(&media)?;
            let destination = media_path.clone();
            let guard = lock.clone();
            tokio::task::spawn_blocking(move || {
                let _guard = guard;
                write_record(&destination, &bytes)
            })
            .await??;
            media
        };
        media.validate()?;
        let head = root.join("cluster.json");
        let pending_head = root.join("cluster.json.part");
        let manifest: Option<Manifest> = if tokio::fs::try_exists(&head).await? {
            Some(read(&head).await?)
        } else {
            None
        };
        let mut receiving = None;
        if let Some(current) = &manifest {
            verify_history(root, current).await?;
        }
        if tokio::fs::try_exists(&pending_head).await? {
            match read::<Manifest>(&pending_head).await {
                Ok(candidate) => {
                    candidate.validate()?;
                    ensure!(
                        candidate
                            .members
                            .iter()
                            .any(|m| m.media_id == media.media_id),
                        "pending cluster head belongs to another media"
                    );
                    ensure!(
                        manifest
                            .as_ref()
                            .is_none_or(|old| replacement(old, &candidate)),
                        "pending cluster head has another owner or older revision"
                    );
                    receiving = Some(candidate);
                }
                Err(error) if manifest.is_none() => return Err(error),
                Err(_) => (),
            }
        }
        if let Some(current) = &manifest {
            ensure!(
                current
                    .members
                    .iter()
                    .any(|member| member.media_id == media.media_id),
                "cluster head belongs to another media"
            );
        } else if receiving.is_none() && tokio::fs::try_exists(root.join("cluster/history")).await?
        {
            anyhow::bail!("cluster.json is missing; retain history and recover the committed head");
        }
        let history = std::sync::Mutex::new(manifest.clone());
        Ok(Self {
            disk: Disk::storage_member(instance, budget)?,
            media,
            manifest: RwLock::new(manifest),
            pending: RwLock::new(receiving),
            history,
            mutations: Arc::new(tokio::sync::RwLock::new(())),
            limit: instance
                .data_limit
                .as_deref()
                .map(crate::config::bytes)
                .transpose()?,
            lock,
        })
    }

    pub fn capacity(&self, boot: Uuid) -> Result<Capacity> {
        let measurement = self
            .disk
            .data
            .as_ref()
            .context("storage accounting is missing")?
            .measurement();
        let (used_bytes, reserved_bytes, revision) = measurement.unwrap_or_default();
        Ok(Capacity {
            accounting_ready: measurement.is_some(),
            accounting: measurement
                .map(|_| crate::model::storage_cluster::Accounting { boot, revision }),
            limit: self.limit,
            used_bytes,
            reserved_bytes,
            available_bytes: fs2::available_space(&self.disk.root)?,
            min_free_space: self.disk.floor,
        })
    }

    pub async fn apply(&self, reply: Registration, deployment: Uuid) -> Result<()> {
        if let Some(candidate) = &reply.manifest {
            candidate.validate()?;
            ensure!(
                candidate.deployment_id == deployment
                    && candidate
                        .members
                        .iter()
                        .any(|m| m.media_id == self.media.media_id),
                "candidate belongs to another deployment or media"
            );
            for old in [
                self.manifest.read().unwrap().clone(),
                self.pending.read().unwrap().clone(),
            ]
            .iter()
            .flatten()
            {
                ensure!(
                    replacement(old, candidate),
                    "candidate would replace another owner or newer revision"
                );
            }
            if self.manifest.read().unwrap().as_ref() != Some(candidate)
                && self.pending.read().unwrap().as_ref() != Some(candidate)
            {
                self.publish(
                    self.disk.root.join("cluster.json.part"),
                    serde_json::to_vec(candidate)?,
                )
                .await?;
                *self.pending.write().unwrap() = Some(candidate.clone());
            }
        }
        let mut previous = self.history.lock().unwrap().clone();
        let mut after = previous.as_ref().map_or(0, |record| record.revision);
        for record in &reply.history {
            record.validate()?;
            ensure!(
                record.revision == after + 1,
                "cluster history is out of sequence"
            );
            if let Some(old) = &previous {
                record.follows(old)?;
            } else {
                ensure!(
                    record.previous.is_none(),
                    "initial cluster history is missing"
                );
            }
            let path = history_path(&self.disk.root, record);
            if tokio::fs::try_exists(&path).await? {
                let saved: Manifest = read(&path).await?;
                ensure!(
                    saved.digest()? == record.digest()?,
                    "immutable cluster history differs"
                );
            } else {
                self.publish(path, serde_json::to_vec(record)?).await?;
            }
            after = record.revision;
            *self.history.lock().unwrap() = Some(record.clone());
            previous = Some(record.clone());
        }
        if let Some(manifest) = reply.manifest {
            manifest.validate()?;
            if manifest.revision > after {
                return Ok(());
            }
            ensure!(
                manifest.deployment_id == deployment
                    && manifest.revision == after
                    && manifest
                        .members
                        .iter()
                        .any(|member| member.media_id == self.media.media_id),
                "cluster head does not match registered media or history"
            );
            verify_history(&self.disk.root, &manifest).await?;
            if self.manifest.read().unwrap().as_ref() != Some(&manifest) {
                let disk = self.disk.clone();
                let guard = self.lock.clone();
                let mutation = self
                    .mutations
                    .clone()
                    .try_read_owned()
                    .context("storage is frozen for inventory")?;
                tokio::task::spawn_blocking(move || -> Result<()> {
                    let (_guard, _mutation) = (guard, mutation);
                    let head = disk.root.join("cluster.json");
                    let pending = temporary(&head);
                    let mut space = disk.reserve_space(&disk.root, disk.floor, 0)?;
                    space.observe(vec![head.clone(), pending.clone()])?;
                    std::fs::rename(pending, head)?;
                    std::fs::File::open(&disk.root)?.sync_all()?;
                    Ok(())
                })
                .await??;
                *self.manifest.write().unwrap() = Some(manifest);
            }
            *self.pending.write().unwrap() = None;
        }
        Ok(())
    }

    async fn publish(&self, path: PathBuf, bytes: Vec<u8>) -> Result<()> {
        let disk = self.disk.clone();
        let guard = self.lock.clone();
        let mutation = self
            .mutations
            .clone()
            .try_read_owned()
            .context("storage is frozen for inventory")?;
        tokio::task::spawn_blocking(move || {
            let (_guard, _mutation) = (guard, mutation);
            let mut space = disk.reserve_space(&disk.root, disk.floor, bytes.len() as u64)?;
            space.observe(vec![path.clone(), temporary(&path)])?;
            write_record(&path, &bytes)
        })
        .await?
    }
}

fn temporary(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(".part");
    value.into()
}

fn write_record(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    let parent = path.parent().context("storage record has no parent")?;
    let mut missing = Vec::new();
    let mut directory = parent;
    while !directory.exists() {
        missing.push(directory.to_owned());
        directory = directory.parent().context("storage parent is missing")?;
    }
    for directory in missing.iter().rev() {
        std::fs::create_dir(directory)?;
        std::fs::File::open(directory.parent().unwrap())?.sync_all()?;
    }
    let pending = temporary(path);
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&pending)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(pending, path)?;
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::storage_cluster::{Member, Topology};

    #[tokio::test]
    async fn incomplete_membership_resumes_without_resetting_media() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let instance = Instance {
            data_path: directory.path().into(),
            data_limit: Some("16MiB".into()),
            ..Default::default()
        };
        let budget =
            Budget::for_runtime(&instance, &crate::config::RuntimeSettings::default(), &[])?;
        let lock = Arc::new(crate::runtime::lock_directory(directory.path())?);
        let files = Files::open(&instance, &budget, lock.clone()).await?;
        files.disk.data.as_ref().unwrap().reconcile()?;
        let media = files.media.media_id;
        let initial = Manifest {
            record_id: Uuid::now_v7(),
            format_version: FORMAT,
            cluster_id: Uuid::now_v7(),
            deployment_id: Uuid::now_v7(),
            publication_key: [1; 32],
            takeover_epoch: 1,
            revision: 1,
            previous: None,
            members: vec![],
            groups: vec![],
        };
        let mut head = Manifest {
            record_id: Uuid::now_v7(),
            revision: 2,
            previous: Some(initial.digest()?),
            ..initial.clone()
        };
        head.members.push(Member {
            media_id: media,
            node_id: Uuid::now_v7(),
            state: "available".into(),
            topology: Topology {
                url: "https://storage.example".into(),
                region: None,
                storage_group: "host".into(),
                purpose: "primary".into(),
            },
        });
        files
            .apply(
                Registration {
                    manifest: Some(head.clone()),
                    history: vec![initial.clone()],
                },
                head.deployment_id,
            )
            .await?;
        assert!(files.manifest.read().unwrap().is_none());
        drop(files);
        let restarted = Files::open(&instance, &budget, lock.clone()).await?;
        restarted.disk.data.as_ref().unwrap().reconcile()?;
        assert_eq!(restarted.media.media_id, media);
        assert_eq!(*restarted.pending.read().unwrap(), Some(head.clone()));
        restarted
            .apply(
                Registration {
                    manifest: Some(head.clone()),
                    history: vec![initial, head.clone()],
                },
                head.deployment_id,
            )
            .await?;
        assert_eq!(*restarted.manifest.read().unwrap(), Some(head.clone()));
        assert!(!directory.path().join("cluster.json.part").exists());
        drop(restarted);
        let complete = Files::open(&instance, &budget, lock.clone()).await?;
        complete.disk.data.as_ref().unwrap().reconcile()?;
        assert_eq!(*complete.manifest.read().unwrap(), Some(head.clone()));
        let mut takeover = Manifest {
            record_id: Uuid::now_v7(),
            deployment_id: Uuid::now_v7(),
            publication_key: [2; 32],
            takeover_epoch: head.takeover_epoch + 1,
            revision: head.revision + 1,
            previous: Some(head.digest()?),
            ..head.clone()
        };
        takeover.members[0].node_id = Uuid::now_v7();
        let mut invalid = takeover.clone();
        invalid.takeover_epoch = head.takeover_epoch;
        assert!(invalid.follows(&head).is_err());
        assert!(
            complete
                .apply(
                    Registration {
                        manifest: Some(invalid.clone()),
                        history: vec![invalid],
                    },
                    takeover.deployment_id
                )
                .await
                .is_err()
        );
        complete
            .apply(
                Registration {
                    manifest: Some(takeover.clone()),
                    history: vec![],
                },
                takeover.deployment_id,
            )
            .await?;
        assert_eq!(*complete.manifest.read().unwrap(), Some(head.clone()));
        drop(complete);
        let adopted = Files::open(&instance, &budget, lock.clone()).await?;
        adopted.disk.data.as_ref().unwrap().reconcile()?;
        assert_eq!(*adopted.pending.read().unwrap(), Some(takeover.clone()));
        adopted
            .apply(
                Registration {
                    manifest: Some(takeover.clone()),
                    history: vec![takeover.clone()],
                },
                takeover.deployment_id,
            )
            .await?;
        assert_eq!(adopted.media.media_id, media);
        assert_eq!(*adopted.manifest.read().unwrap(), Some(takeover));
        assert!(
            adopted
                .apply(
                    Registration {
                        manifest: Some(head.clone()),
                        history: vec![],
                    },
                    head.deployment_id
                )
                .await
                .is_err()
        );
        drop(adopted);
        std::fs::remove_file(directory.path().join("cluster.json"))?;
        assert!(Files::open(&instance, &budget, lock.clone()).await.is_err());
        std::fs::remove_file(directory.path().join("media.json"))?;
        assert!(Files::open(&instance, &budget, lock).await.is_err());
        Ok(())
    }
}
