use super::{body::Reader, selection::Item, verify};
use crate::{
    model::journal::record::Attributes,
    storage::external::{
        Client,
        objects::{Condition, Written},
    },
};
use anyhow::{Context, Result, ensure};
use axum::http::Method;
use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::{Bytes, BytesMut};
use rusqlite::params;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Read,
    path::{Path, PathBuf},
};
use uuid::Uuid;

#[derive(Clone, Copy, PartialEq)]
pub(super) enum Policy {
    Verify,
    Skip,
    Overwrite,
}
pub(super) enum Outcome {
    Published,
    Resumed(u64),
    Verified,
    Skipped,
}
#[derive(Debug)]
pub(super) struct Conflict(pub String);
impl std::fmt::Display for Conflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for Conflict {}
fn conflict(s: impl ToString) -> anyhow::Error {
    Conflict(s.to_string()).into()
}

pub(super) struct Transfer<'a, 'r> {
    pub reader: &'a Reader<'r>,
    pub bucket: Uuid,
    pub item: &'a Item,
    pub attrs: Attributes,
    pub policy: Policy,
    pub window: u64,
    pub generation: i64,
    pub recovery: [u8; 32],
}
pub(super) fn directory(path: &Path) -> Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    let path = super::selection::absolute(path)?;
    let mut dir = File::open("/")?;
    for c in path.components() {
        let std::path::Component::Normal(name) = c else {
            continue;
        };
        let next = anchored(&dir).join(name);
        let open = || {
            OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
                .open(&next)
        };
        dir = match open() {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                match fs::create_dir(&next) {
                    Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => (),
                    v => v?,
                };
                open()?
            }
            v => v?,
        };
    }
    Ok(dir)
}
pub(super) fn anchored(dir: &File) -> PathBuf {
    use std::os::fd::AsRawFd;
    PathBuf::from(format!("/proc/self/fd/{}", dir.as_raw_fd()))
}
fn existing_file(path: &Path) -> Result<Option<File>> {
    use std::os::unix::fs::OpenOptionsExt;
    match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
    {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        v => {
            let f = v?;
            ensure!(
                f.metadata()?.is_file(),
                "local target is not a regular file"
            );
            Ok(Some(f))
        }
    }
}
impl Transfer<'_, '_> {
    pub fn size(&self) -> u64 {
        self.item.size as u64
    }
    pub async fn read(&self, start: u64, end: u64) -> Result<Bytes> {
        self.reader
            .read(self.bucket, self.item, start, end, self.window)
            .await
    }
    async fn verify_local(&self, mut f: File) -> Result<()> {
        ensure!(f.metadata()?.len() == self.size(), "local length differs");
        let mut hash = Sha256::new();
        let mut buf = vec![0; 128 * 1024];
        loop {
            let n = f.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hash.update(&buf[..n]);
        }
        let expected = verify::full(
            self.reader,
            self.bucket,
            self.item,
            &self.attrs,
            "sha256",
            self.window,
        )
        .await?;
        ensure!(
            hash.finalize().as_slice() == STANDARD.decode(expected)?,
            "local checksum differs"
        );
        Ok(())
    }
    pub(super) async fn check_local(&self, path: &Path) -> Result<Option<Outcome>> {
        let Some(file) = existing_file(path)? else {
            return Ok(None);
        };
        Ok(match self.policy {
            Policy::Skip => Some(Outcome::Skipped),
            Policy::Overwrite => None,
            Policy::Verify => {
                self.verify_local(file).await.map_err(conflict)?;
                Some(Outcome::Verified)
            }
        })
    }
    async fn remote_state(
        &self,
        c: &Client,
        key: &str,
        dest: &str,
    ) -> Result<Result<Condition, Outcome>> {
        let Some(head) = verify::head(c, key, self.policy == Policy::Verify)
            .await
            .context("reading MOKYU_DUMP_DEST target metadata")?
        else {
            return Ok(Ok(Condition::Absent));
        };
        if self.policy == Policy::Skip {
            return Ok(Err(Outcome::Skipped));
        }
        if self.policy == Policy::Overwrite {
            return Ok(Ok(Condition::Match(head.0.etag)));
        }
        if let Some(version) = &head.0.version {
            let ours=self.reader.db.query_row("SELECT EXISTS(SELECT 1 FROM dump_versions WHERE bucket=? AND seq=? AND dest=? AND version=?)",params![self.bucket,self.item.seq,dest,version],|r|r.get::<_,bool>(0))?;
            if ours
                && head.0.metadata == verify::headers(&self.attrs)?
                && head.0.size == self.size()
            {
                return Ok(Err(Outcome::Verified));
            }
        }
        if verify::same(
            self.reader,
            self.bucket,
            self.item,
            &self.attrs,
            c,
            key,
            &head,
            self.window,
        )
        .await
        .map_err(conflict)?
        {
            Ok(Err(Outcome::Verified))
        } else {
            Err(conflict("S3 target content or metadata differs"))
        }
    }
    pub(super) async fn buffer(&self, from: u64, end: u64) -> Result<Bytes> {
        let mut out = BytesMut::with_capacity(usize::try_from(end - from)?);
        let mut at = from;
        while at < end {
            let b = self.read(at, end).await?;
            at += b.len() as u64;
            out.extend_from_slice(&b);
        }
        Ok(out.freeze())
    }
    async fn put(
        &self,
        c: &Client,
        key: &str,
        condition: &Condition,
        metadata: &BTreeMap<String, String>,
        budget: u64,
    ) -> Result<Written> {
        let mut headers = crate::storage::external::objects::write_headers(metadata, None)?;
        condition.apply(&mut headers)?;
        let size = self.size();
        if size <= budget {
            let body = self.buffer(0, size).await?;
            headers.insert(
                "x-amz-checksum-sha256",
                STANDARD.encode(Sha256::digest(&body)).parse()?,
            );
            let reply = c.bytes(Method::PUT, Some(key), &[], headers, body).await?;
            return Written::headers(&c.empty(reply).await?, None);
        }
        let digest = verify::full(
            self.reader,
            self.bucket,
            self.item,
            &self.attrs,
            "sha256",
            self.window,
        )
        .await?;
        headers.insert("x-amz-checksum-sha256", digest.parse()?);
        headers.insert("content-length", size.to_string().parse()?);
        let (tx, mut rx) = tokio::sync::mpsc::channel::<std::io::Result<Bytes>>(1);
        let body = axum::body::Body::from_stream(
            async_stream::stream! {while let Some(chunk)=rx.recv().await{yield chunk;}},
        );
        let producer = async {
            let mut pos = 0;
            while pos < size {
                let data = self.read(pos, size).await?;
                pos += data.len() as u64;
                tx.send(Ok(data)).await.context("S3 closed the upload")?;
            }
            drop(tx);
            Ok::<_, anyhow::Error>(())
        };
        let digest = hex::encode(STANDARD.decode(digest)?);
        let send = async {
            let reply = c
                .request(Method::PUT, Some(key), &[], headers, body, &digest)
                .await?;
            Written::headers(&c.empty(reply).await?, None)
        };
        Ok(tokio::try_join!(producer, send)?.1)
    }
    pub async fn remote(
        &self,
        c: &Client,
        key: &str,
        dest: &str,
        threshold: u64,
        part: u64,
        buffer: u64,
    ) -> Result<Outcome> {
        let task = self.task(dest)?;
        let completing = self.reader.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM dump_uploads WHERE task=? AND completing=1)",
            [task],
            |r| r.get::<_, bool>(0),
        )?;
        if completing && let Some(head) = verify::head(c, key, true).await? {
            if verify::same(
                self.reader,
                self.bucket,
                self.item,
                &self.attrs,
                c,
                key,
                &head,
                self.window,
            )
            .await
            .unwrap_or(false)
            {
                self.forget_upload(&task)?;
                return Ok(Outcome::Verified);
            }
        }
        let condition = match self.remote_state(c, key, dest).await? {
            Ok(v) => v,
            Err(out) => return Ok(out),
        };
        let metadata = verify::headers(&self.attrs)?;
        let active = self.reader.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM dump_uploads WHERE task=?)",
            [task],
            |r| r.get::<_, bool>(0),
        )?;
        let written = if !active && self.size() <= threshold {
            ensure!(
                self.size() <= crate::storage::external::objects::MAX_PART,
                "MOKYU_DUMP_MULTIPART_THRESHOLD exceeds the S3 single PUT limit"
            );
            self.put(c, key, &condition, &metadata, buffer)
                .await
                .map(|w| (w, 0))
        } else {
            self.upload(c, key, task, &condition, &metadata, part, buffer)
                .await
        };
        let (written, reused) = match written {
            Ok(w) => w,
            Err(e) => {
                if !matches!(self.policy, Policy::Overwrite)
                    && let Err(out) = self.remote_state(c, key, dest).await?
                {
                    return Ok(out);
                }
                return Err(e);
            }
        };
        if let Some(version) = written.version {
            self.reader.db.execute(
                "INSERT OR REPLACE INTO dump_versions VALUES(?,?,?,?)",
                params![self.bucket, self.item.seq, dest, version],
            )?;
        }
        self.forget_upload(&task)?;
        Ok(if reused > 0 {
            Outcome::Resumed(reused)
        } else {
            Outcome::Published
        })
    }
}
