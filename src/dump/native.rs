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
    layouts: Vec<String>,
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

async fn returning_source(
    source: &Source,
    media: Uuid,
    layout: &Manifest,
    fixed: &Fixed,
    db: &OfflineCatalog,
) -> Result<()> {
    use crate::node::storage::frozen::Page;
    use futures_util::TryStreamExt;
    let namespace = fixed.proof.publication.namespace;
    let mut layouts = std::collections::BTreeMap::new();
    let mut observed_history = None;
    if layout.revision > fixed.anchor.revision {
        let history = source.layout_history(media, namespace).await?;
        let mut current = layout;
        while current.revision > fixed.anchor.revision {
            layouts.insert(current.digest()?, current.clone());
            let parent = current
                .previous
                .as_ref()
                .and_then(|d| history.get(d))
                .context("missing layout history on reconnected medium")?;
            current.follows(parent)?;
            current = parent;
        }
        ensure!(
            current == &fixed.anchor,
            "medium {media} belongs to a conflicting layout"
        );
        observed_history = Some(history);
    } else {
        ensure!(
            fixed.layouts.get(usize::try_from(layout.revision - 1)?) == Some(&layout.digest()?),
            "medium {media} reports a conflicting historical layout"
        );
    }
    let mut stream = source.pages(media, namespace, "history", 0).await?;
    let Some(Page::Evidence(first)) = stream.try_next().await? else {
        anyhow::bail!("missing publication inventory on medium {media}");
    };
    let Some(head) = first.head.clone() else {
        return Ok(());
    };
    if db.known_link(&head).await? {
        return Ok(());
    }
    let history = match observed_history {
        Some(history) => history,
        None => source.layout_history(media, namespace).await?,
    };
    for (digest, candidate) in history {
        if fixed.layouts.get(usize::try_from(candidate.revision - 1)?) == Some(&digest) {
            layouts.insert(digest, candidate);
        }
    }
    layouts.insert(fixed.anchor.digest()?, fixed.anchor.clone());
    // A later snapshot may contain unrelated candidates. Walk only its committed head back to the pinned point.
    let staged = rusqlite::Connection::open("")?;
    staged.execute_batch(
        "CREATE TABLE proofs(digest BLOB PRIMARY KEY,body BLOB NOT NULL) WITHOUT ROWID; BEGIN",
    )?;
    let mut page = first;
    loop {
        ensure!(
            page.head.as_ref() == Some(&head),
            "publication changed while reconnecting medium {media}"
        );
        for entry in page.entries {
            if let Some(p) = entry.proof
                && !entry.pending
                && entry.error.is_none()
                && p.publication.revision > fixed.proof.publication.revision
            {
                staged.execute(
                    "INSERT OR IGNORE INTO proofs VALUES(?,?)",
                    rusqlite::params![p.link()?.digest.as_slice(), serde_json::to_vec(&p)?],
                )?;
            }
        }
        if page.complete {
            break;
        }
        let Some(Page::Evidence(next)) = stream.try_next().await? else {
            anyhow::bail!("publication history interrupted on medium {media}");
        };
        page = next;
    }
    let mut link = head;
    let mut child: Option<Proof> = None;
    let pinned = fixed.proof.link()?;
    while link != pinned {
        let body: Vec<u8> = staged
            .query_row(
                "SELECT body FROM proofs WHERE digest=?",
                [link.digest.as_slice()],
                |r| r.get(0),
            )
            .with_context(|| {
                format!("medium {media} has a conflicting or incomplete publication history")
            })?;
        let p: Proof = serde_json::from_slice(&body)?;
        ensure!(
            p.link()? == link && p.publication.namespace == namespace,
            "reconnected publication identity differs"
        );
        p.verify(
            layouts
                .get(&p.publication.manifest)
                .context("untrusted publication layout")?,
        )?;
        if let Some(child) = child {
            child.follows(Some(&p))?;
        }
        link = p
            .publication
            .previous
            .clone()
            .context("publication does not descend from the pinned recovery point")?;
        child = Some(p);
    }
    if let Some(child) = child {
        child.follows(Some(&fixed.proof))?;
    }
    Ok(())
}

async fn discover(
    list: &Sources,
    saved: Option<(&Fixed, &OfflineCatalog)>,
) -> Result<(Manifest, Arc<Media>)> {
    let fixed = saved.map(|(f, _)| f);
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
        if fixed.is_none() {
            match &list.manifest_digest {
                Some(digest) if anchor.is_none() => {
                    anchor = if m.digest()? == *digest {
                        Some(m.clone())
                    } else {
                        source
                            .layout_history(id, list.namespace)
                            .await?
                            .remove(digest)
                    };
                }
                None if anchor.as_ref().is_none_or(|a| m.revision > a.revision) => {
                    anchor = Some(m.clone())
                }
                _ => (),
            }
        }
        if let Some((fixed, db)) = saved {
            if let Err(e) = returning_source(&source, id, &m, fixed, db).await {
                if e.is::<std::io::Error>()
                    || e.downcast_ref::<tonic::Status>().is_some_and(|s| {
                        matches!(
                            s.code(),
                            tonic::Code::Unavailable
                                | tonic::Code::DeadlineExceeded
                                | tonic::Code::Cancelled
                        )
                    })
                {
                    eprintln!("Recovery medium {id}, source {name} unavailable: {e:#}");
                    continue;
                }
                return Err(e);
            }
        }
        media.sources.entry(id).or_default().push((m, source));
    }
    ensure!(
        !media.sources.is_empty(),
        "no readable media in the selected recovery group"
    );
    let anchor = anchor.context("trusted cluster manifest is unavailable")?;
    ensure!(
        Some(anchor.cluster_id) == cluster,
        "trusted manifest belongs to another storage cluster"
    );
    for (id, sources) in &media.sources {
        let member = anchor
            .members
            .iter()
            .find(|m| m.media_id == *id)
            .with_context(|| format!("medium {id} is outside the trusted manifest"))?;
        ensure!(
            match list.backup_group.as_deref() {
                Some(group) =>
                    member.topology.purpose == "backup" && member.topology.storage_group == group,
                None => member.topology.purpose != "backup",
            },
            "medium {id} is outside the trusted recovery group"
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
                layouts: fixed.layouts.clone(),
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

pub(super) fn lock_scan(staging: &Path) -> Result<fs::File> {
    let mut options = fs::OpenOptions::new();
    options.create(true).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let lock = options.open(staging)?;
    lock.try_lock()
        .context("another process is scanning this dump database")?;
    Ok(lock)
}

pub(super) async fn build(
    uri: &str,
    staging: &Path,
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
    let sql = OfflineCatalog::open(staging, false).await?;
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
    let mut layouts = Vec::new();
    for rev in 1..=anchor.revision {
        let m = verify::layout(sql.as_ref(), id, &anchor, rev.try_into()?).await?;
        for aliases in media.sources.values() {
            ensure!(
                aliases.iter().all(|(x, _)| x.revision != rev || *x == m),
                "media report conflicting historical layouts at revision {rev}"
            );
        }
        layouts.push(m.digest()?);
        sql.layout(&m, rev == anchor.revision).await?;
    }
    let fixed = Fixed {
        anchor,
        layouts,
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
    catalog.db.execute_batch("CREATE TABLE dump_native_publications(id BLOB PRIMARY KEY, digest BLOB NOT NULL) WITHOUT ROWID")?;
    let mut after = 0;
    while after < fixed.proof.publication.revision {
        let proofs = sql
            .catalog_proofs(id, after, fixed.proof.publication.revision)
            .await?;
        ensure!(!proofs.is_empty(), "missing verified publication");
        for proof in proofs {
            let link = proof.link()?;
            catalog.db.execute(
                "INSERT INTO dump_native_publications VALUES(?,?)",
                rusqlite::params![link.id, link.digest.as_slice()],
            )?;
            after = proof.publication.revision;
        }
    }
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
    sql.close().await;
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
    let sql = OfflineCatalog::open(path, true).await?;
    let (anchor, media) = discover(&list, Some((fixed, &sql))).await?;
    let scan = Scanner::frozen(sql, anchor, media, list.backup_group.clone());
    Ok((backend(location.clone(), fixed, scan)?, roots(&list)?))
}

#[cfg(test)]
#[path = "../../tests/storage/offline_epochs.rs"]
mod tests;
