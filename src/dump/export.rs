use super::{
    Identity,
    body::Reader,
    origin::{Backend, S3, physical, setting},
    output::{Conflict, Outcome, Policy, Transfer},
    selection::{Selection, Target},
};
use crate::storage::{external::Client, journal::source::options};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OpenFlags};
use std::{fs::OpenOptions, path::Path};

pub(super) fn size(name: &str, default: &str) -> Result<u64> {
    crate::config::bytes(&setting(name)?.unwrap_or_else(|| default.into()))
        .with_context(|| format!("invalid {name}"))
}
struct Destination {
    client: Client,
    endpoint: String,
    threshold: u64,
    part: u64,
}
fn endpoint(url: &str) -> Result<String> {
    let url = reqwest::Url::parse(url)?;
    Ok(format!(
        "{}{}",
        url.origin().ascii_serialization(),
        url.path().trim_end_matches('/')
    ))
}
fn destination(target: &Target) -> Result<Option<Destination>> {
    let Target::S3 { bucket, .. } = target else {
        return Ok(None);
    };
    let s3 = S3::load("DEST", bucket, "", None)?;
    let threshold = size("MOKYU_DUMP_MULTIPART_THRESHOLD", "100MiB")?;
    let part = size("MOKYU_DUMP_PART_SIZE", "16MiB")?;
    ensure!(
        (5 * 1024 * 1024..=5 * 1024u64.pow(3)).contains(&part),
        "MOKYU_DUMP_PART_SIZE must be between 5MiB and 5GiB"
    );
    let client = s3
        .connection
        .connect(&s3.access, &s3.secret, options())?
        .with_session_token(s3.token.as_deref())?;
    Ok(Some(Destination {
        client,
        endpoint: endpoint(&s3.connection.endpoint_url)?,
        threshold,
        part,
    }))
}
pub(super) struct Schedule {
    pub slots: tokio::sync::Semaphore,
    pub count: usize,
    pub buffer: u64,
}
pub(super) async fn run(path: &Path, id: &Identity, sel: Selection, target: Target) -> Result<()> {
    let policy = match setting("MOKYU_DUMP_IF_EXISTS")?
        .as_deref()
        .unwrap_or("verify")
    {
        "verify" => Policy::Verify,
        "overwrite" => Policy::Overwrite,
        "skip" => Policy::Skip,
        _ => anyhow::bail!("MOKYU_DUMP_IF_EXISTS must be verify, skip or overwrite"),
    };
    let memory = size("MOKYU_DUMP_MEMORY_LIMIT", "256MiB")?;
    ensure!(
        memory >= 32 * 1024 * 1024,
        "MOKYU_DUMP_MEMORY_LIMIT must allow at least 32MiB for an export"
    );
    let requested = setting("MOKYU_DUMP_CONCURRENCY")?
        .unwrap_or_else(|| "4".into())
        .parse::<usize>()
        .context("invalid MOKYU_DUMP_CONCURRENCY")?;
    ensure!(requested > 0, "MOKYU_DUMP_CONCURRENCY must be positive");
    let dest = destination(&target)?;
    let lock = OpenOptions::new().read(true).write(true).open(path)?;
    lock.try_lock()
        .context("another process is exporting with this MOKYU_DUMP_DB")?;
    let db = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )?;
    let v: Vec<u8> = db.query_row("SELECT value FROM identity", [], |r| r.get(0))?;
    ensure!(
        serde_json::from_slice::<Identity>(&v)? == *id,
        "MOKYU_DUMP_DB changed"
    );
    let backend = Backend::open(&id.location.uri, Some(&id.location)).await?;
    ensure!(
        backend.location == id.location
            && backend.deployment == id.deployment
            && backend.source.connection.id == id.storage
            && backend.source.journal.journal_id == id.journal,
        "source identity differs from MOKYU_DUMP_DB"
    );
    let db_path = path.canonicalize()?;
    let local_root = backend
        .source
        .connection
        .path
        .as_deref()
        .map(std::fs::canonicalize)
        .transpose()?;
    if let Some(root) = &local_root {
        ensure!(
            !db_path.starts_with(root),
            "MOKYU_DUMP_DB must be outside the read-only source"
        );
    }
    let (total, bytes) = sel.visit(&db, |item| {
        if let Some(d) = &dest {
            super::multipart::part_width(item.size.try_into()?, d.part)?;
            if item.size as u64 <= d.threshold {
                ensure!(
                    item.size as u64 <= crate::storage::external::objects::MAX_PART,
                    "MOKYU_DUMP_MULTIPART_THRESHOLD exceeds the S3 single PUT limit for {}",
                    item.key
                );
            }
        }
        let mapped = target.mapped(&sel, &item.key)?;
        target.protect_local(&mapped, id, &db_path)?;
        if matches!(target, Target::Local(_))
            && let Some(root) = &local_root
        {
            ensure!(
                !super::selection::absolute(Path::new(&mapped))?.starts_with(root),
                "target overlaps the read-only source"
            );
        }
        if matches!(target, Target::Local(_)) {
            sel.check_collision(&db, &item.key)?;
        }
        if let (Some(d), Some(src)) = (&dest, &id.location.endpoint)
            && endpoint(src)? == d.endpoint
        {
            let (a, p) = physical(&id.location.uri)?;
            let (b, k) = physical(&mapped)?;
            ensure!(
                a != b
                    || !(p.is_empty()
                        || k == p
                        || k.strip_prefix(p).is_some_and(|rest| rest.starts_with('/'))),
                "S3 destination overlaps the read-only source"
            );
        }
        Ok(())
    })?;
    db.execute_batch("PRAGMA trusted_schema=OFF; PRAGMA synchronous=FULL; PRAGMA temp_store=FILE;
      CREATE TABLE IF NOT EXISTS dump_sums(bucket BLOB,seq INTEGER,alg TEXT,value TEXT,PRIMARY KEY(bucket,seq,alg));
      CREATE TABLE IF NOT EXISTS dump_versions(bucket BLOB,seq INTEGER,dest TEXT,version TEXT,PRIMARY KEY(bucket,seq,dest));")?;
    db.execute_batch(super::resume::SCHEMA)?;
    let recovery = *blake3::hash(&serde_json::to_vec(id)?).as_bytes();
    let mut resumed = 0;
    let mut reused = 0u64;
    let reader = Reader::new(&db, &backend)?;
    use futures_util::{StreamExt, stream};
    const M: u64 = 1024 * 1024;
    let available = memory - 8 * M;
    let count = if total <= 1 && dest.as_ref().is_none_or(|d| bytes <= d.threshold) {
        1
    } else {
        requested.min((available / (48 * M)).clamp(1, 10000) as usize)
    };
    let slot = available / count as u64;
    let buffer = slot / 3;
    let window = ((slot - buffer).saturating_sub(12 * M) / 3).clamp(M, 32 * M);
    let schedule = Schedule {
        slots: tokio::sync::Semaphore::new(count),
        count,
        buffer,
    };
    let progress = super::progress::Progress::new(total, bytes);
    progress.print(true);
    let mut cursor = None;
    let (mut published, mut verified, mut skipped, mut conflicts, mut failed, mut written) =
        (0, 0, 0, 0, 0, 0u64);
    loop {
        let items = sel.page(&db, cursor.as_deref(), 64)?;
        let Some(last) = items.last() else { break };
        cursor = Some(last.key.clone());
        let jobs = stream::iter(&items)
            .map(|item| {
                let (reader, sel, target, dest, progress, schedule) =
                    (&reader, &sel, &target, &dest, &progress, &schedule);
                async move {
                    let object_progress = progress.object();
                    let result = async {
                        let attrs = reader.attributes(sel.bucket, item)?;
                        let integrity = super::verify::LogicalDigest::new(&attrs)?.into();
                        let transfer = Transfer {
                            reader,
                            bucket: sel.bucket,
                            attrs,
                            integrity,
                            item,
                            policy,
                            generation: sel.generation,
                            recovery,
                            window,
                            schedule,
                            progress: &object_progress,
                        };
                        let mapped = target.mapped(sel, &item.key)?;
                        match dest {
                            Some(d) => {
                                let (_, key) = physical(&mapped)?;
                                transfer
                                    .remote(
                                        &d.client,
                                        key,
                                        &format!("{} {mapped}", d.endpoint),
                                        d.threshold,
                                        d.part,
                                    )
                                    .await
                            }
                            None => {
                                let _slot = schedule.slots.acquire().await?;
                                transfer.local(Path::new(&mapped)).await
                            }
                        }
                    }
                    .await;
                    if matches!(result, Ok(Outcome::Verified | Outcome::Skipped)) {
                        object_progress.finish(item.size as u64);
                    }
                    (item, result)
                }
            })
            .buffer_unordered(count);
        futures_util::pin_mut!(jobs);
        while let Some((item, outcome)) = jobs.next().await {
            match outcome {
                Ok(Outcome::Published) => {
                    published += 1;
                    written += item.size as u64;
                }
                Ok(Outcome::Resumed(n)) => {
                    resumed += 1;
                    written += item.size as u64;
                    reused += n;
                }
                Ok(Outcome::Verified) => {
                    verified += 1;
                }
                Ok(Outcome::Skipped) => {
                    skipped += 1;
                }
                Err(e) => {
                    if e.is::<Conflict>() {
                        conflicts += 1;
                    } else {
                        failed += 1;
                    }
                    eprintln!("mokyu://{}/{}: {e:#}", sel.name, item.key.escape_debug());
                }
            }
            progress.done();
        }
    }
    progress.print(true);
    let transferred = progress.transferred();
    eprintln!(
        "{total} objects, {bytes} B total: {published} published, {resumed} resumed ({written} B published, {reused} B reused, {transferred} B transferred), {verified} verified, {skipped} skipped, {conflicts} conflicts, {failed} failed"
    );
    ensure!(
        conflicts + failed == 0,
        "some objects could not be exported"
    );
    Ok(())
}
