use super::{
    Backend, Location, setting,
    sources::{Entry, Sources},
};
use crate::{
    model::{
        OfflineCatalog,
        storage_cluster::Manifest,
        storage_recovery::{CatalogEntry, Proof, RecoveryCatalog, verify},
    },
    storage::{
        Disk,
        journal::{catalog::Catalog, source::Cut},
        native::{
            recovery::{
                archive::Archive,
                media::{Media, Source},
                scan::Scanner,
            },
            store::{Authority, Store},
            transport::Pool,
        },
    },
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub(super) struct Fixed {
    anchor: Manifest,
    proof: Proof,
    catalog: Uuid,
    group: Option<String>,
}

impl Fixed {
    pub fn check(&self, list: &Sources) -> Result<()> {
        ensure!(
            self.proof.publication.namespace == list.namespace
                && list.cluster.is_none_or(|x| x == self.anchor.cluster_id)
                && list.backup_group == self.group
                && list
                    .manifest_digest
                    .as_ref()
                    .is_none_or(|d| self.anchor.digest().as_ref().ok() == Some(d))
                && list
                    .publication_digest
                    .as_ref()
                    .is_none_or(|d| self.proof.link().is_ok_and(|l| hex::encode(l.digest) == *d)),
            "a different recovery point or group requires a new MOKYU_DUMP_DB"
        );
        Ok(())
    }
}

fn roots(list: &Sources) -> Result<Vec<PathBuf>> {
    list.sources
        .iter()
        .filter_map(|s| match s {
            Entry::Directory { path } => Some(match path.canonicalize() {
                Ok(p) => Ok(p),
                Err(_) => super::selection::absolute(path),
            }),
            _ => None,
        })
        .collect()
}

pub(super) fn guard(list: &Sources, dir: &Path) -> Result<()> {
    ensure!(
        !roots(list)?.iter().any(|p| dir.starts_with(p)),
        "dump workspace overlaps a source medium"
    );
    Ok(())
}

async fn discover(list: &Sources, fixed: Option<&Fixed>) -> Result<(Manifest, Arc<Media>)> {
    let mut media = Media::default();
    let mut anchor = fixed.map(|f| f.anchor.clone());
    let mut cluster = list.cluster.or(anchor.as_ref().map(|a| a.cluster_id));
    for entry in &list.sources {
        let (name, result) = match entry {
            Entry::Directory { path } => (
                path.display().to_string(),
                Source::directory(path.clone()).await,
            ),
            Entry::Remote {
                url,
                credential_file,
            } => (url.clone(), Source::remote(url, credential_file).await),
        };
        let (identity, source) = match result {
            Ok(v) => v,
            Err(e) => {
                eprintln!("Recovery source {name} unavailable: {e:#}");
                continue;
            }
        };
        let m = identity.manifest;
        ensure!(
            cluster.get_or_insert(m.cluster_id) == &m.cluster_id,
            "source {name} belongs to another storage cluster"
        );
        let id = identity.media.media_id;
        let member = m
            .members
            .iter()
            .find(|x| x.media_id == id)
            .context("unknown medium")?;
        let selected = match list.backup_group.as_deref() {
            Some(g) => member.topology.purpose == "backup" && member.topology.storage_group == g,
            None => member.topology.purpose != "backup",
        };
        if !selected {
            continue;
        }
        if fixed.is_none()
            && list
                .manifest_digest
                .as_ref()
                .is_none_or(|d| m.digest().as_ref().ok() == Some(d))
            && anchor.as_ref().is_none_or(|a| m.revision > a.revision)
        {
            anchor = Some(m.clone());
        }
        media.sources.entry(id).or_default().push((m, source));
    }
    ensure!(
        !media.sources.is_empty(),
        "no readable media in the selected recovery group"
    );
    let anchor = anchor.context("trusted cluster manifest is unavailable")?;
    for (id, sources) in &media.sources {
        ensure!(
            anchor.members.iter().any(|m| m.media_id == *id),
            "medium {id} is outside the trusted manifest"
        );
        for (manifest, _) in sources {
            ensure!(
                manifest.revision != anchor.revision || *manifest == anchor,
                "conflicting manifests for medium {id}"
            );
        }
    }
    Ok((anchor, Arc::new(media)))
}

fn backend(location: Location, fixed: &Fixed, scan: Arc<Scanner>) -> Result<Backend> {
    let publication = &fixed.proof.publication;
    let archive = Archive::frozen(scan, fixed.catalog, publication.namespace)?;
    let store = Arc::new(Store {
        authority: Authority::Recovery(Arc::new(archive)),
        pool: Arc::new(Pool::new(Duration::from_secs(60))),
        namespace: publication.namespace,
        cluster: fixed.anchor.cluster_id,
        prefix: object_store::path::Path::parse(&publication.prefix)?,
        region: None,
    });
    let mut c = Backend::connection();
    c.r#type = "mokyu".into();
    c.cluster_id = Some(fixed.anchor.cluster_id);
    c.prefix = publication.prefix.clone();
    let value = publication
        .identity
        .clone()
        .context("published backend marker is missing")?;
    let backend = Backend::from_marker(location, store, c, value)?;
    ensure!(
        backend.source.connection.id == publication.namespace,
        "published marker refers to another namespace"
    );
    Ok(backend)
}

async fn earlier(
    scan: &Arc<Scanner>,
    sql: &OfflineCatalog,
    location: &Location,
    fixed: &Fixed,
    disk: &Arc<Disk>,
) -> Result<Option<(String, u64)>> {
    let mut proof = fixed.proof.clone();
    while let Some(link) = proof.publication.previous.clone() {
        let revision = proof.publication.revision - 1;
        let Some(CatalogEntry::Proof(previous)) = sql
            .catalog_entry(fixed.catalog, 1, link.id, revision)
            .await?
        else {
            break;
        };
        proof.follows(Some(&previous))?;
        let candidate = Uuid::now_v7();
        sql.copy_evidence(fixed.catalog, candidate).await?;
        let trial = async {
            let head = verify::rooted_chain(sql, candidate, &fixed.anchor, &[link]).await?;
            scan.index_proof(candidate, &head).await?;
            let trial = Fixed {
                anchor: fixed.anchor.clone(),
                group: fixed.group.clone(),
                proof: head,
                catalog: candidate,
            };
            let backend = backend(location.clone(), &trial, scan.clone())?;
            let mut rows = Catalog::temporary(None)?;
            let cut = scan
                .dump(candidate, &backend.source, &trial.proof, disk, &mut rows)
                .await?;
            Ok::<_, anyhow::Error>((hex::encode(trial.proof.link()?.digest), cut.point.sequence))
        }
        .await;
        sql.forget(candidate).await?;
        if let Ok(point) = trial {
            return Ok(Some(point));
        }
        proof = previous;
    }
    Ok(None)
}

pub(super) async fn build(
    uri: &str,
    path: &Path,
    dir: &Path,
    catalog: &mut Catalog,
) -> Result<(Backend, Cut, Option<Fixed>)> {
    let list = Sources::read(Path::new(uri))?;
    let (anchor, media) = discover(&list, None).await?;
    let ids = media.sources.keys().copied().collect::<Vec<_>>();
    let digest = anchor.digest()?;
    eprintln!(
        "Recovery cluster {} · manifest {digest} · {} distinct media",
        anchor.cluster_id,
        ids.len()
    );
    let staging = path.with_added_extension("scan");
    let mut options = fs::OpenOptions::new();
    options.create(true).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let lock = options.open(&staging)?;
    lock.try_lock()
        .context("another process is scanning this dump database")?;
    let sql = OfflineCatalog::open(&staging, false).await?;
    let hash = blake3::hash(&serde_json::to_vec(&(
        &digest,
        list.namespace,
        &list.backup_group,
        &list.publication_digest,
    ))?);
    let id = Uuid::from_bytes(hash.as_bytes()[..16].try_into()?);
    let scan = Scanner::frozen(
        sql.clone(),
        anchor.clone(),
        media.clone(),
        list.backup_group.clone(),
    );
    let proof = scan
        .collect_at(id, list.namespace, &ids, list.publication_digest.as_deref())
        .await?;
    for rev in 1..=anchor.revision {
        let m = verify::layout(sql.as_ref(), id, &anchor, rev.try_into()?).await?;
        for aliases in media.sources.values() {
            ensure!(
                aliases.iter().all(|(x, _)| x.revision != rev || *x == m),
                "media report conflicting historical layouts at revision {rev}"
            );
        }
        sql.layout(&m, rev == anchor.revision).await?;
    }
    let fixed = Fixed {
        anchor,
        proof,
        catalog: id,
        group: list.backup_group.clone(),
    };
    fixed.check(&list)?;
    let location = Location {
        uri: Location::normalize(uri)?,
        endpoint: None,
        region: None,
    };
    let backend = backend(location.clone(), &fixed, scan.clone())?;
    let disk = Disk::workspace(dir.to_owned(), None, 0, 4);
    let cut = match scan
        .dump(id, &backend.source, &fixed.proof, &disk, catalog)
        .await
    {
        Ok(cut) => cut,
        Err(err) => {
            let message = format!(
                "publication {} at sequence {} is incomplete: {err:#}",
                hex::encode(fixed.proof.link()?.digest),
                fixed.proof.publication.through
            );
            if list.publication_digest.is_none()
                && let Some((digest, seq)) = earlier(&scan, &sql, &location, &fixed, &disk).await?
            {
                anyhow::bail!(
                    "{message}. Earlier verified recovery point at sequence {seq}: explicitly set publication_digest=\"{digest}\" to select it"
                );
            }
            anyhow::bail!("{message}");
        }
    };
    ensure!(
        cut.point.sequence == fixed.proof.publication.through,
        "dump watermark differs from publication"
    );
    catalog.db.execute(
        "ATTACH DATABASE ? AS scan",
        [staging.to_str().context("invalid work path")?],
    )?;
    catalog.db.execute_batch("CREATE TABLE storage_recovery_objects AS SELECT * FROM scan.storage_recovery_objects WHERE 0;
        CREATE TABLE storage_recovery_shards AS SELECT * FROM scan.storage_recovery_shards WHERE 0;")?;
    for name in ["storage_recovery_objects", "storage_recovery_shards"] {
        catalog.db.execute(
            &format!("INSERT INTO main.{name} SELECT * FROM scan.{name} WHERE catalog=?"),
            [id.as_bytes().as_slice()],
        )?;
    }
    catalog.db.execute_batch("CREATE UNIQUE INDEX dump_native_path ON storage_recovery_objects(catalog,path_hash);
        CREATE UNIQUE INDEX dump_native_shard ON storage_recovery_shards(catalog,operation,sequence,shard_index,copy_index);")?;
    eprintln!(
        "Verified publication {} at sequence {}",
        hex::encode(fixed.proof.link()?.digest),
        cut.point.sequence
    );
    Ok((backend, cut, Some(fixed)))
}

pub(super) async fn reopen(
    path: &Path,
    location: &Location,
    fixed: &Fixed,
) -> Result<(Backend, Vec<PathBuf>)> {
    let uri = setting("MOKYU_DUMP_SOURCE")?.unwrap_or_else(|| location.uri.clone());
    let list = Sources::read(Path::new(&uri))?;
    fixed.check(&list)?;
    let (anchor, media) = discover(&list, Some(fixed)).await?;
    let sql = OfflineCatalog::open(path, true).await?;
    let scan = Scanner::frozen(sql, anchor, media, list.backup_group.clone());
    Ok((backend(location.clone(), fixed, scan)?, roots(&list)?))
}

#[cfg(test)]
#[path = "../../tests/storage/offline_epochs.rs"]
mod tests;
