use super::{Identity, origin::physical};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use std::path::{Component, Path, PathBuf};
use uuid::Uuid;

pub(super) struct Selection {
    pub bucket: Uuid,
    pub generation: i64,
    pub name: String,
    pub prefix: String,
    pub recursive: bool,
}
pub(super) enum Target {
    Local(PathBuf),
    S3 { bucket: String, key: String },
}
pub(super) struct Item {
    pub key: String,
    pub size: i64,
    pub seq: i64,
}
const QUERY: &str = "SELECT key,size,seq FROM objects WHERE bucket=?1 AND generation=?2 AND key>=?3 AND (?4 IS NULL OR key<?4) AND (?5 OR key=?3) AND (?6 IS NULL OR key>?6) ORDER BY key LIMIT ?7";

impl Selection {
    pub fn parse(db: &Connection, uri: &str) -> Result<Self> {
        let value = uri
            .strip_prefix("mokyu://")
            .context("source must be a mokyu:// URI")?;
        let (name, prefix) = value.split_once('/').unwrap_or((value, ""));
        ensure!(
            s3s::path::check_bucket_name(name) && prefix.len() <= 1024 && !prefix.contains('\0'),
            "invalid logical source"
        );
        let (bucket, generation) = db
            .query_row(
                "SELECT id,generation FROM buckets WHERE name=? AND visible=1",
                [name],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?
            .with_context(|| format!("bucket {name} has no recovery point"))?;
        Ok(Self {
            bucket,
            generation,
            name: name.into(),
            prefix: prefix.into(),
            recursive: prefix.is_empty() || prefix.ends_with('/'),
        })
    }
    pub fn page(&self, db: &Connection, after: Option<&str>, limit: i64) -> Result<Vec<Item>> {
        let upper = self.prefix.strip_suffix('/').map(|p| format!("{p}0"));
        let mut stmt = db.prepare_cached(QUERY)?;
        Ok(stmt
            .query_map(
                params![
                    self.bucket,
                    self.generation,
                    self.prefix,
                    upper,
                    self.recursive,
                    after,
                    limit
                ],
                |r| {
                    Ok(Item {
                        key: r.get(0)?,
                        size: r.get(1)?,
                        seq: r.get(2)?,
                    })
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }
    pub fn visit(
        &self,
        db: &Connection,
        mut handle: impl FnMut(Item) -> Result<()>,
    ) -> Result<(u64, u64)> {
        let mut after = None;
        let mut n = 0u64;
        let mut bytes = 0u64;
        loop {
            let page = self.page(db, after.as_deref(), 128)?;
            let Some(last) = page.last() else { break };
            after = Some(last.key.clone());
            for item in page {
                bytes = bytes
                    .checked_add(u64::try_from(item.size)?)
                    .context("logical byte count overflow")?;
                n += 1;
                handle(item)?;
            }
        }
        ensure!(
            self.recursive || n == 1,
            "object mokyu://{}/{} was not found",
            self.name,
            self.prefix
        );
        Ok((n, bytes))
    }
    pub fn list(&self, db: &Connection) -> Result<()> {
        let mut previous = String::new();
        self.visit(db, |item| {
            let relative = if self.recursive {
                &item.key[self.prefix.len()..]
            } else {
                &item.key
            };
            if let Some((dir, _)) = relative.split_once('/') {
                let dir = format!("{dir}/");
                if previous != dir {
                    println!("{}", dir.escape_debug());
                    previous = dir;
                }
            } else {
                println!("{}\t{} B", relative.escape_debug(), item.size);
            }
            Ok(())
        })?;
        Ok(())
    }
    pub fn check_collision(&self, db: &Connection, key: &str) -> Result<()> {
        if self.recursive {
            let relative = key
                .strip_prefix(&self.prefix)
                .context("invalid selection")?;
            let mut stmt = db.prepare_cached(
                "SELECT EXISTS(SELECT 1 FROM objects WHERE bucket=? AND generation=? AND key=?)",
            )?;
            for (i, _) in relative.match_indices('/') {
                let parent = &key[..self.prefix.len() + i];
                ensure!(
                    !stmt.query_row(params![self.bucket, self.generation, parent], |r| r
                        .get::<_, bool>(0))?,
                    "local file and directory collide at {parent}"
                );
            }
        }
        Ok(())
    }
}

pub(super) fn absolute(path: &Path) -> Result<PathBuf> {
    let mut result = PathBuf::new();
    for c in std::path::absolute(path)?.components() {
        match c {
            Component::ParentDir => {
                ensure!(result.pop(), "path escapes its root");
            }
            Component::CurDir => (),
            _ => result.push(c.as_os_str()),
        }
    }
    Ok(result)
}
impl Target {
    pub fn parse(value: &str, selection: &Selection) -> Result<Self> {
        if value.starts_with("s3://") {
            let (bucket, key) = physical(value)?;
            ensure!(
                !key.contains('\0') && key.len() <= 1024,
                "invalid S3 target key"
            );
            if !selection.recursive {
                ensure!(
                    !key.is_empty() && !key.ends_with('/'),
                    "provide an exact S3 target key"
                );
            }
            Ok(Self::S3 {
                bucket: bucket.into(),
                key: key.into(),
            })
        } else {
            ensure!(
                !value.is_empty() && !value.contains("://"),
                "target must be a local path or s3:// URI"
            );
            Ok(Self::Local(value.into()))
        }
    }
    pub fn mapped(&self, selection: &Selection, key: &str) -> Result<String> {
        let relative = if selection.recursive {
            key.strip_prefix(&selection.prefix)
                .context("invalid source prefix")?
        } else {
            ""
        };
        match self {
            Self::Local(root) => {
                let mut path = root.clone();
                if selection.recursive {
                    for part in relative.split('/') {
                        ensure!(
                            !part.is_empty()
                                && part != "."
                                && part != ".."
                                && !part.contains(['\\', '\0'])
                                && Path::new(part).components().count() == 1,
                            "object {key:?} cannot be represented by a local file"
                        );
                        path.push(part);
                    }
                }
                Ok(path.to_str().context("target path must be UTF-8")?.into())
            }
            Self::S3 {
                bucket,
                key: prefix,
            } => {
                let path = if !selection.recursive {
                    prefix.clone()
                } else if prefix.is_empty() {
                    relative.into()
                } else {
                    format!("{}/{relative}", prefix.trim_end_matches('/'))
                };
                ensure!(
                    !path.is_empty() && path.len() <= 1024,
                    "invalid mapped S3 target key"
                );
                Ok(format!("s3://{bucket}/{path}"))
            }
        }
    }
    pub fn protect_local(&self, mapped: &str, source: &Identity, db_path: &Path) -> Result<()> {
        if matches!(self, Self::Local(_)) {
            let path = absolute(Path::new(mapped))?;
            if source.location.endpoint.is_none() {
                ensure!(
                    !path.starts_with(&source.location.uri),
                    "target overlaps the read-only source"
                );
            }
            let db = absolute(db_path)?;
            for suffix in ["", "-wal", "-shm", "-journal"] {
                let mut reserved = db.as_os_str().to_owned();
                reserved.push(suffix);
                ensure!(
                    !path.starts_with(Path::new(&reserved)),
                    "target overlaps MOKYU_DUMP_DB"
                );
            }
        }
        Ok(())
    }
}
pub(super) fn preview(
    db: &Connection,
    identity: &Identity,
    db_path: &Path,
    sel: &Selection,
    target: &Target,
) -> Result<()> {
    let mut sample = Vec::new();
    let (count, bytes) = sel.visit(db, |item| {
        let out = target.mapped(sel, &item.key)?;
        if matches!(target, Target::Local(_)) {
            sel.check_collision(db, &item.key)?;
            target.protect_local(&out, identity, db_path)?;
        }
        if sample.len() < 10 {
            sample.push(format!(
                "mokyu://{}/{} -> {}  {} B",
                sel.name,
                item.key.escape_debug(),
                out.escape_debug(),
                item.size
            ));
        }
        Ok(())
    })?;
    for line in sample {
        println!("{line}");
    }
    println!("{count} objects, {bytes} B total");
    Ok(())
}
