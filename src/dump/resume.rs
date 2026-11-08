use super::output::{Outcome, Policy, Transfer, anchored, directory};
use anyhow::{Context, Result};
use rusqlite::{OptionalExtension, params};
use std::{
    fs::{self, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
};
use uuid::Uuid;

pub(super) const SCHEMA:&str="CREATE TABLE IF NOT EXISTS dump_files(task BLOB PRIMARY KEY,token BLOB,dev BLOB,ino BLOB,offset INTEGER,digest BLOB);
    CREATE TABLE IF NOT EXISTS dump_uploads(task BLOB PRIMARY KEY,upload TEXT,owner BLOB NOT NULL,part_size INTEGER,size INTEGER,checksums INTEGER NOT NULL,completing INTEGER DEFAULT 0,aborting INTEGER DEFAULT 0);
    CREATE TABLE IF NOT EXISTS dump_parts(task BLOB,number INTEGER,size INTEGER,etag TEXT,checksum TEXT,PRIMARY KEY(task,number));";
impl Transfer<'_, '_> {
    pub fn task(&self, dest: &str) -> Result<[u8; 32]> {
        let mut h = blake3::Hasher::new();
        h.update(&self.recovery);
        h.update(&serde_json::to_vec(&(
            self.bucket,
            self.generation,
            &self.item.key,
            self.item.seq,
            dest,
        ))?);
        Ok(*h.finalize().as_bytes())
    }
    pub async fn local(&self, path: &Path) -> Result<Outcome> {
        let dest = super::selection::absolute(path)?;
        let task = self.task(dest.to_str().context("local filename must be UTF-8")?)?;
        let db = self.reader.db;
        let dir = directory(super::parent(&dest))?;
        let base = anchored(&dir);
        let final_path = base.join(dest.file_name().context("missing target filename")?);
        let record = db
            .query_row(
                "SELECT token,dev,ino,offset,digest FROM dump_files WHERE task=?",
                [task],
                |r| {
                    Ok((
                        r.get::<_, Uuid>(0)?,
                        r.get::<_, [u8; 8]>(1)?,
                        r.get::<_, [u8; 8]>(2)?,
                        r.get::<_, i64>(3)?,
                        r.get::<_, [u8; 32]>(4)?,
                    ))
                },
            )
            .optional()?;
        if let Some(out) = self.check_local(&final_path).await? {
            if let Some((token, dev, ino, ..)) = record {
                let part = base.join(format!(".mokyu-dump-{}.part", token.simple()));
                if let Ok(meta) = fs::symlink_metadata(&part)
                    && meta.dev().to_be_bytes() == dev
                    && meta.ino().to_be_bytes() == ino
                {
                    fs::remove_file(part)?;
                    dir.sync_all()?;
                }
                db.execute("DELETE FROM dump_files WHERE task=?", [task])?;
            }
            return Ok(out);
        }
        let mut candidate = None;
        if let Some((token, dev, ino, confirmed, digest)) = record {
            let part = base.join(format!(".mokyu-dump-{}.part", token.simple()));
            let mut options = OpenOptions::new();
            options
                .read(true)
                .write(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
            let file = match options.open(&part) {
                Ok(f) => Some(f),
                Err(e) if matches!(e.kind(), std::io::ErrorKind::NotFound) => None,
                Err(e) => return Err(e.into()),
            };
            if let Some(mut file) = file {
                let m = file.metadata()?;
                if m.is_file()
                    && m.dev().to_be_bytes() == dev
                    && m.ino().to_be_bytes() == ino
                    && m.nlink() == 1
                {
                    let confirmed = u64::try_from(confirmed)?;
                    let mut h = blake3::Hasher::new();
                    let valid = confirmed <= m.len()
                        && confirmed <= self.size()
                        && std::io::copy(&mut (&mut file).take(confirmed), &mut h)? == confirmed
                        && h.finalize().as_bytes() == &digest;
                    if valid {
                        file.set_len(confirmed)?;
                        file.seek(SeekFrom::Start(confirmed))?;
                        candidate = Some((file, part, confirmed, h));
                    } else {
                        fs::remove_file(part)?;
                    }
                }
            }
        }
        let (mut file, part, mut at, mut h) = if let Some(v) = candidate {
            v
        } else {
            let token = Uuid::new_v4();
            let part = base.join(format!(".mokyu-dump-{}.part", token.simple()));
            let file = OpenOptions::new()
                .write(true)
                .read(true)
                .create_new(true)
                .mode(0o600)
                .open(&part)?;
            file.sync_all()?;
            dir.sync_all()?;
            let m = file.metadata()?;
            let h = blake3::Hasher::new();
            db.execute(
                "INSERT OR REPLACE INTO dump_files VALUES(?,?,?,?,0,?)",
                params![
                    task,
                    token,
                    m.dev().to_be_bytes(),
                    m.ino().to_be_bytes(),
                    h.finalize().as_bytes()
                ],
            )?;
            (file, part, 0, h)
        };
        let reused = at;
        while at < self.size() {
            let data = self.read(at, self.size()).await?;
            file.write_all(&data)?;
            h.update(&data);
            at += data.len() as u64;
            file.sync_data()?;
            db.execute(
                "UPDATE dump_files SET offset=?2,digest=?3 WHERE task=?1",
                params![task, i64::try_from(at)?, h.finalize().as_bytes()],
            )?;
        }
        file.sync_all()?;
        if self.policy == Policy::Overwrite {
            fs::rename(&part, &final_path)?;
        } else {
            match fs::hard_link(&part, &final_path) {
                Ok(()) => (),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    return self
                        .check_local(&final_path)
                        .await?
                        .context("concurrent target disappeared");
                }
                Err(e) => return Err(e.into()),
            }
            fs::remove_file(&part)?;
        }
        dir.sync_all()?;
        db.execute("DELETE FROM dump_files WHERE task=?", [task])?;
        Ok(if reused == 0 {
            Outcome::Published
        } else {
            Outcome::Resumed(reused)
        })
    }
}
