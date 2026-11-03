use super::{
    Command, StyleExport, StyleImport, Styles,
    transport::{CliTarget, cli_target, rpc},
};
use crate::model::image_styles::{Import, Imported, PAGE};
use anyhow::{Context, Result, ensure};
use serde_json::json;
use sqlx::Connection;
use std::{
    fs::File,
    io::{self, BufRead, BufReader, Read, Seek, Write},
    path::{Path, PathBuf},
};
use uuid::Uuid;

struct Spool {
    file: File,
    path: PathBuf,
}
impl Spool {
    fn new(directory: &Path) -> Result<Self> {
        let path = directory.join(format!(".mokyu-styles-{}", Uuid::new_v4()));
        let mut options = File::options();
        options.create_new(true).write(true).read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(&path)?;
        Ok(Self { file, path })
    }
}
impl Drop for Spool {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}
pub(super) async fn execute(config: Option<&Path>, cmd: Styles) -> Result<()> {
    let target = cli_target(config)?;
    match cmd {
        Styles::Import(args) => import(&target, args).await,
        Styles::Export(args) => export(&target, args).await,
    }
}
async fn send(target: &CliTarget, bucket: &str, body: &Import) -> Result<Imported> {
    let cmd = Command::Styles(Styles::Import(StyleImport {
        bucket: bucket.into(),
        text: None,
        file: None,
        name_prefix: String::new(),
        on_conflict: Default::default(),
        dry_run: false,
        lines: vec![],
        document: Some(serde_json::to_value(body)?),
    }));
    Ok(serde_json::from_value(rpc(target, &cmd).await?)?)
}
async fn import(target: &CliTarget, args: StyleImport) -> Result<()> {
    let mut input: Box<dyn BufRead> = match (args.text, args.file) {
        (Some(text), None) => Box::new(io::Cursor::new(text)),
        (None, Some(path)) if path == Path::new("-") => Box::new(BufReader::new(io::stdin())),
        (None, Some(path)) => Box::new(BufReader::new(File::open(path)?)),
        _ => anyhow::bail!("choose exactly one of --text and --file"),
    };
    let mut spool = Spool::new(&std::env::temp_dir())?;
    let mut seen = sqlx::SqliteConnection::connect_with(
        &sqlx::sqlite::SqliteConnectOptions::new()
            .in_memory(true)
            .pragma("temp_store", "FILE")
            .pragma("temp.cache_size", "-512"),
    )
    .await?;
    sqlx::raw_sql("CREATE TEMP TABLE names (name TEXT NOT NULL PRIMARY KEY)")
        .execute(&mut seen)
        .await?;
    let mut offset = 0u64;
    let mut initial = None;
    let mut errors = false;
    let mut any = false;
    let lines = args
        .lines
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    ensure!(!lines.contains(&0), "line numbers start at one");
    loop {
        let mut text = String::new();
        let mut count = 0u16;
        for _ in 0..PAGE {
            let mut line = String::new();
            let limit = crate::processing::image::Pipeline::MAX_BODY + 160;
            let bytes = input.by_ref().take(limit as u64 + 1).read_line(&mut line)?;
            if bytes == 0 {
                break;
            }
            ensure!(
                bytes <= limit,
                "image style line {} is too long",
                offset + u64::from(count) + 1
            );
            text.push_str(&line);
            count += 1;
        }
        if count == 0 {
            break;
        }
        let start = offset;
        offset += u64::from(count);
        let selected = (!lines.is_empty()).then(|| {
            lines
                .range((start + 1)..=offset)
                .map(|n| (n - start) as u16)
                .collect::<Vec<_>>()
        });
        if selected.as_ref().is_some_and(Vec::is_empty) || text.trim().is_empty() {
            continue;
        }
        any = true;
        let body = Import {
            text,
            prefix: args.name_prefix.clone(),
            conflict: args.on_conflict,
            selected,
            revision: None,
            apply: false,
        };
        let result = send(target, &args.bucket, &body).await?;
        ensure!(
            initial.is_none_or(|v| v == result.revision),
            "styles changed during import preflight"
        );
        initial = Some(result.revision);
        for row in &result.rows {
            let number = start + u64::from(row.line);
            let duplicate = row.outcome != "ignored"
                && !row.name.is_empty()
                && sqlx::query("INSERT OR IGNORE INTO names(name) VALUES(?)")
                    .bind(&row.name)
                    .execute(&mut seen)
                    .await?
                    .rows_affected()
                    == 0;
            let problem = if duplicate {
                Some("duplicate name in import file")
            } else {
                row.error.as_deref()
            };
            if row.outcome != "ignored"
                && let Some(e) = problem
            {
                eprintln!("line {number}: {e}");
                errors = true;
            }
            if args.dry_run {
                println!(
                    "{}",
                    serde_json::to_string(
                        &json!({"line":number,"name":row.name,"outcome":if duplicate {"error"}else{&row.outcome},"error":problem})
                    )?
                );
            }
        }
        serde_json::to_writer(&mut spool.file, &(start, body))?;
        spool.file.write_all(b"\n")?;
    }
    seen.close().await?;
    ensure!(
        any && lines.last().is_none_or(|n| *n <= offset),
        "no matching image style lines"
    );
    ensure!(!errors, "image preflight failed; no styles were applied");
    if args.dry_run {
        return Ok(());
    }
    spool.file.rewind()?;
    let mut revision = initial.context("missing image revision")?;
    for chunk in BufReader::new(&spool.file).lines() {
        let (start, mut body): (u64, Import) = serde_json::from_str(&chunk?)?;
        body.revision = Some(revision);
        body.apply = true;
        let result = match send(target, &args.bucket, &body).await {
            Ok(r) => r,
            Err(_) => send(target, &args.bucket, &body).await.with_context(|| {
                format!(
                    "batch starting at line {} was not confirmed; check prior batches",
                    start + 1
                )
            })?,
        };
        ensure!(
            result.applied,
            "batch at line {} was not applied: {}",
            start + 1,
            serde_json::to_string(&result.rows)?
        );
        revision = result.revision;
        eprintln!(
            "confirmed {} lines at revision {revision}",
            result.rows.len()
        );
    }
    Ok(())
}
async fn export(target: &CliTarget, args: StyleExport) -> Result<()> {
    let path = args.output.filter(|p| p != Path::new("-"));
    let tempdir = std::env::temp_dir();
    let mut spool = Spool::new(
        path.as_ref()
            .map(|p| {
                p.parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or(Path::new("."))
            })
            .unwrap_or(&tempdir),
    )?;
    let mut revision = None;
    let mut after = None;
    loop {
        let doc = crate::control::image_styles::Export {
            revision,
            after,
            prefix: args.name_prefix.clone(),
            names: (!args.names.is_empty()).then(|| args.names.clone()),
        };
        let cmd = Command::Styles(Styles::Export(StyleExport {
            bucket: args.bucket.clone(),
            output: None,
            name_prefix: None,
            names: vec![],
            document: Some(serde_json::to_value(&doc)?),
        }));
        let page: crate::control::image_styles::ExportPage =
            serde_json::from_value(rpc(target, &cmd).await?)?;
        ensure!(
            revision.is_none_or(|r| r == page.revision),
            "styles changed during export"
        );
        revision = Some(page.revision);
        spool.file.write_all(page.text.as_bytes())?;
        if page.next.is_none() {
            break;
        }
        after = page.next;
    }
    spool.file.flush()?;
    if let Some(path) = path {
        spool.file.sync_all()?;
        std::fs::rename(&spool.path, path)?;
    } else {
        spool.file.rewind()?;
        io::copy(&mut spool.file, &mut io::stdout().lock())?;
    }
    Ok(())
}
