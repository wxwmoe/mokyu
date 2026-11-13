use super::{
    files::Files,
    fragments::{self, Check},
};
use crate::{
    model::{
        storage_cluster::Manifest,
        storage_recovery::{Link, PROOF_LIMIT, Proof},
    },
    storage::local,
};
use anyhow::{Context, Result, ensure};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};
use uuid::Uuid;

pub(super) fn history(root: &Path, namespace: Uuid, id: Uuid) -> PathBuf {
    root.join("recovery/history")
        .join(namespace.simple().to_string())
        .join(format!("{}.json", id.simple()))
}

pub(super) fn read(path: &Path) -> Result<Option<Vec<u8>>> {
    let file = match fragments::open(path, false) {
        Ok(file) => file,
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
        {
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    ensure!(
        file.metadata()?.len() <= PROOF_LIMIT as u64,
        "recovery evidence exceeds its limit"
    );
    let mut bytes = Vec::new();
    file.take(PROOF_LIMIT as u64 + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() <= PROOF_LIMIT,
        "recovery evidence exceeds its limit"
    );
    Ok(Some(bytes))
}

fn publish(root: &Path, path: &Path, bytes: &[u8], immutable: bool, check: &Check) -> Result<()> {
    fragments::directories(root, path)?;
    if immutable {
        if let Some(existing) = read(path)? {
            ensure!(existing == bytes, "immutable recovery evidence differs");
            fragments::open(path, false)?.sync_all()?;
            File::open(path.parent().unwrap())?.sync_all()?;
            return Ok(());
        }
    }
    let pending = fragments::temporary(path);
    if immutable {
        if let Some(existing) = read(&pending)? {
            ensure!(
                bytes.starts_with(&existing),
                "partial recovery evidence belongs to another publication"
            );
        }
    }
    let mut file = match fragments::open(&pending, true) {
        Ok(file) => file,
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
        {
            OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&pending)?
        }
        Err(error) => return Err(error),
    };
    file.set_len(0)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    #[cfg(feature = "fault-injection")]
    crate::faults::blocking(if immutable {
        "storage-evidence-synced"
    } else {
        "storage-head-synced"
    });
    check()?;
    std::fs::rename(&pending, path)?;
    File::open(path.parent().unwrap())?.sync_all()?;
    Ok(())
}

pub(super) async fn commit(
    files: &Files,
    proof: Proof,
    manifest: Manifest,
    check: Check,
) -> Result<Link> {
    proof.verify(&manifest)?;
    ensure!(
        manifest
            .members
            .iter()
            .any(|member| member.media_id == files.media.media_id),
        "recovery evidence targets another media"
    );
    let bytes = serde_json::to_vec(&proof)?;
    ensure!(
        bytes.len() <= PROOF_LIMIT,
        "recovery evidence exceeds its limit"
    );
    let disk = files.disk.clone();
    let lock = files.lock.clone();
    let permit = fragments::checked(disk.io.clone().acquire_owned(), &check).await??;
    let mutation = files
        .mutations
        .clone()
        .try_read_owned()
        .context("storage is frozen for inventory")?;
    tokio::task::spawn_blocking(move || {
        let (_lock, _permit, _mutation) = (lock, permit, mutation);
        check()?;
        let namespace = proof.publication.namespace;
        ensure!(
            disk.pending.lock().unwrap().insert(namespace),
            "namespace has an active publication"
        );
        let _exclusive = local::Fragment {
            disk: disk.clone(),
            id: namespace,
        };
        let destination = history(&disk.root, namespace, proof.publication.id);
        let head = disk
            .root
            .join("recovery/heads")
            .join(format!("{}.json", namespace.simple()));
        fragments::check_directories(&disk.root, &destination)?;
        fragments::check_directories(&disk.root, &head)?;
        let current: Option<Link> = read(&head)?
            .map(|bytes| serde_json::from_slice(&bytes))
            .transpose()?;
        let link = proof.link()?;
        ensure!(
            current == proof.publication.previous || current.as_ref() == Some(&link),
            "recovery head changed"
        );
        if current.as_ref() == Some(&link) {
            ensure!(
                read(&destination)?.as_deref() == Some(bytes.as_slice()),
                "committed recovery evidence is missing or differs"
            );
            fragments::open(&destination, false)?.sync_all()?;
            fragments::open(&head, false)?.sync_all()?;
            File::open(destination.parent().unwrap())?.sync_all()?;
            File::open(head.parent().unwrap())?.sync_all()?;
            return Ok(link);
        }
        if let Some(previous) = &proof.publication.previous {
            let bytes = read(&history(&disk.root, namespace, previous.id))?
                .context("recovery predecessor is missing")?;
            let previous: Proof = serde_json::from_slice(&bytes)?;
            proof.follows(Some(&previous))?;
        } else {
            proof.follows(None)?;
        }
        let mut space = disk.reserve_space(&disk.root, disk.floor, bytes.len() as u64 + 65536)?;
        space.observe(vec![
            destination.clone(),
            fragments::temporary(&destination),
            head.clone(),
            fragments::temporary(&head),
            destination.parent().unwrap().to_owned(),
            head.parent().unwrap().to_owned(),
            disk.root.join("recovery/history"),
            disk.root.join("recovery"),
        ])?;
        publish(&disk.root, &destination, &bytes, true, &check)?;
        #[cfg(feature = "fault-injection")]
        crate::faults::blocking("storage-evidence-published");
        publish(
            &disk.root,
            &head,
            &serde_json::to_vec(&link)?,
            false,
            &check,
        )?;
        Ok(link)
    })
    .await?
}

#[cfg(test)]
mod tests {
    use super::super::fragments::tests::{allowed, fixture};
    use super::*;
    use crate::{
        content::codec::Key,
        model::{
            storage_cluster::{Member, Topology},
            storage_recovery::{Publication, public_key},
        },
    };

    #[tokio::test]
    async fn recovery_evidence_is_immutable_and_head_updates_are_conditional() -> Result<()> {
        let (_directory, _, _, files) = fixture("8MiB").await?;
        let key = Key::new("aes-256-gcm", [3; 32])?;
        let cluster = Uuid::now_v7();
        let manifest = Manifest {
            format_version: 1,
            record_id: Uuid::now_v7(),
            cluster_id: cluster,
            deployment_id: Uuid::now_v7(),
            publication_key: public_key(&key, cluster)?,
            takeover_epoch: 1,
            revision: 1,
            previous: None,
            members: vec![Member {
                media_id: files.media.media_id,
                node_id: Uuid::now_v7(),
                state: "available".into(),
                topology: Topology {
                    url: "https://storage.example".into(),
                    region: None,
                    storage_group: "host".into(),
                    purpose: "primary".into(),
                },
            }],
            groups: vec![],
        };
        let publication = Publication {
            format_version: 1,
            id: Uuid::now_v7(),
            namespace: Uuid::now_v7(),
            cluster,
            manifest: manifest.digest()?,
            takeover_epoch: 1,
            revision: 1,
            previous: None,
            through: 1,
            parts: vec![],
            retired: vec![],
        };
        let first = Proof::sign(publication, &key, &manifest)?;
        let link = commit(&files, first.clone(), manifest.clone(), allowed()).await?;
        assert_eq!(
            commit(&files, first.clone(), manifest.clone(), allowed()).await?,
            link
        );
        let mut next = first.publication.clone();
        next.id = Uuid::now_v7();
        next.revision = 2;
        next.previous = Some(link.clone());
        next.through = 4;
        let second = Proof::sign(next.clone(), &key, &manifest)?;
        commit(&files, second.clone(), manifest.clone(), allowed()).await?;
        next.id = Uuid::now_v7();
        let conflict = Proof::sign(next, &key, &manifest)?;
        assert!(
            commit(&files, conflict, manifest.clone(), allowed())
                .await
                .is_err()
        );
        let evidence = history(
            &files.disk.root,
            second.publication.namespace,
            second.publication.id,
        );
        assert_eq!(
            serde_json::from_slice::<Proof>(&read(&evidence)?.unwrap())?,
            second
        );
        let head = files
            .disk
            .root
            .join("recovery/heads")
            .join(format!("{}.json", second.publication.namespace.simple()));
        std::fs::remove_file(head)?;
        assert!(read(&evidence)?.is_some());
        assert!(commit(&files, second, manifest, allowed()).await.is_err());
        assert_eq!(files.disk.data.as_ref().unwrap().snapshot().2, 0);
        Ok(())
    }
}
