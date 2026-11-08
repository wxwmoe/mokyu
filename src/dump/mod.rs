mod catalog;
mod origin;

use self::{
    catalog::Catalog,
    origin::{Backend, Location, setting},
};
use crate::storage::journal::{local::Local, source::Cut};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::ErrorKind,
    path::{Path, PathBuf},
};
use uuid::Uuid;

#[derive(clap::Args)]
pub(crate) struct Args {
    /// List the buckets in the selected recovery point.
    #[arg(value_name="COMMAND",num_args=0..=2)]
    operands: Vec<String>,
}

#[derive(Deserialize, Serialize, PartialEq, Debug)]
struct Identity {
    location: Location,
    deployment: Uuid,
    storage: Uuid,
    journal: Uuid,
    cut: Cut,
}

fn parent(path: &Path) -> &Path {
    path.parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
}

fn existing(path: &Path) -> Result<Option<(Connection, Identity)>> {
    match fs::symlink_metadata(path) {
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
        Ok(m) => ensure!(
            m.file_type().is_file(),
            "MOKYU_DUMP_DB must name a regular file"
        ),
        Err(e) => return Err(e.into()),
    }
    let db = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    db.execute_batch("PRAGMA trusted_schema=OFF; PRAGMA query_only=ON;")?;
    let version: i64 = db.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    let app: i64 = db.query_row("PRAGMA application_id", [], |r| r.get(0))?;
    ensure!(
        version == 1 && app == 1296778320,
        "unrecognized MOKYU_DUMP_DB"
    );
    let check: String = db.query_row("PRAGMA quick_check(1)", [], |r| r.get(0))?;
    ensure!(check == "ok", "damaged MOKYU_DUMP_DB");
    let bytes: Vec<u8> = db.query_row("SELECT value FROM identity", [], |r| r.get(0))?;
    let origin: Identity =
        serde_json::from_slice(&bytes).context("invalid dump catalog identity")?;
    if let Some(uri) = setting("MOKYU_DUMP_SOURCE")? {
        ensure!(
            Location::normalize(&uri)? == origin.location.uri,
            "MOKYU_DUMP_SOURCE differs from the catalog; select a new MOKYU_DUMP_DB"
        );
    }
    for (name, expected) in [
        ("MOKYU_DUMP_SOURCE_ENDPOINT", &origin.location.endpoint),
        ("MOKYU_DUMP_SOURCE_REGION", &origin.location.region),
    ] {
        if expected.is_some()
            && let Some(value) = setting(name)?
        {
            ensure!(
                Some(&value) == expected.as_ref(),
                "{name} differs from the catalog"
            );
        }
    }
    Ok(Some((db, origin)))
}

async fn database(path: &Path) -> Result<(Connection, Identity)> {
    if let Some(ready) = existing(path)? {
        return Ok(ready);
    }
    let uri = setting("MOKYU_DUMP_SOURCE")?
        .context("set MOKYU_DUMP_SOURCE to build the missing metadata catalog")?;
    let budget = crate::config::bytes(
        &setting("MOKYU_DUMP_MEMORY_LIMIT")?.unwrap_or_else(|| "256MiB".into()),
    )
    .context("invalid MOKYU_DUMP_MEMORY_LIMIT")?;
    ensure!(
        budget >= 64 * 1024 * 1024,
        "MOKYU_DUMP_MEMORY_LIMIT must allow at least 64MiB for journal replay"
    );
    let backend = Backend::open(&uri).await?;
    let dir = parent(path)
        .canonicalize()
        .context("missing MOKYU_DUMP_DB parent directory")?;
    if let Some(root) = &backend.source.connection.path {
        ensure!(
            !dir.starts_with(fs::canonicalize(root)?),
            "MOKYU_DUMP_DB must be outside the read-only source"
        );
    }
    let local = Local::new(&backend.source.connection)?;
    let _guard = match &local {
        Some(l) => Some(l.lock(false).await?),
        None => None,
    };
    backend.verify().await?;
    let file = tempfile::Builder::new()
        .prefix(".mokyu-catalog-")
        .tempfile_in(&dir)?;
    let mut catalog = Catalog(Connection::open(file.path())?);
    catalog.0.execute_batch("PRAGMA trusted_schema=OFF; PRAGMA synchronous=FULL; PRAGMA temp_store=FILE; PRAGMA cache_size=-2048; PRAGMA temp.cache_size=-2048; BEGIN IMMEDIATE;")?;
    catalog.0.execute_batch(include_str!("schema.sql"))?;
    let cut = backend
        .source
        .walk(&*backend.store, None, None, local.as_ref(), &mut catalog)
        .await?;
    ensure!(
        cut.point.sequence != 0 || cut.checkpoint.is_some(),
        "metadata journal has no published recovery point"
    );
    backend.verify().await?;
    let identity = Identity {
        location: backend.location,
        deployment: backend.deployment,
        storage: backend.source.connection.id,
        journal: backend.source.journal.journal_id,
        cut,
    };
    catalog.0.execute(
        "INSERT INTO identity VALUES(?)",
        [serde_json::to_vec(&identity)?],
    )?;
    catalog.0.execute_batch("COMMIT")?;
    catalog.0.close().map_err(|(_, e)| e)?;
    file.as_file().sync_all()?;
    match file.persist_noclobber(path) {
        Ok(_) => fs::File::open(&dir)?.sync_all()?,
        Err(e) if e.error.kind() == ErrorKind::AlreadyExists => (),
        Err(e) => return Err(e.error.into()),
    }
    let (db, actual) = existing(path)?.context("published metadata catalog disappeared")?;
    ensure!(
        actual.location == identity.location
            && actual.deployment == identity.deployment
            && actual.storage == identity.storage
            && actual.journal == identity.journal,
        "concurrent catalog build used a different source"
    );
    Ok((db, actual))
}

pub(crate) async fn run(args: Args) -> Result<()> {
    if args.operands.is_empty() {
        println!("Usage: mokyu dump ls");
        return Ok(());
    }
    ensure!(args.operands == ["ls"], "expected: mokyu dump ls");
    let path = PathBuf::from(setting("MOKYU_DUMP_DB")?.unwrap_or_else(|| "./metadata.db".into()));
    let (db, _) = database(&path).await?;
    let mut stmt = db.prepare("SELECT name FROM buckets ORDER BY name")?;
    for name in stmt.query_map([], |r| r.get::<_, String>(0))? {
        println!("{}", name?);
    }
    Ok(())
}
