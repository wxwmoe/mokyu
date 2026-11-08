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
    pub integrity: std::cell::RefCell<Option<verify::LogicalDigest>>,
    pub policy: Policy,
    pub window: u64,
    pub generation: i64,
    pub recovery: [u8; 32],
    pub schedule: &'a super::export::Schedule,
    pub progress: &'a super::progress::ObjectProgress<'a>,
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
        let data = self
            .reader
            .read(self.bucket, self.item, start, end, self.window)
            .await?;
        if let Some(check) = self.integrity.borrow_mut().as_mut()
            && let Some(offset) = check.next.checked_sub(start)
            && offset < data.len() as u64
        {
            check.digest.update(&data[offset as usize..]);
            check.next = start + data.len() as u64;
        }
        Ok(data)
    }
    pub(super) async fn finish_digest(&self) -> Result<()> {
        let Some(mut check) = self.integrity.take() else {
            return Ok(());
        };
        // Resumed or out-of-order parts may leave a suffix outside the running digest.
        while check.next < self.size() {
            let data = self
                .reader
                .read(self.bucket, self.item, check.next, self.size(), self.window)
                .await?;
            check.digest.update(&data);
            check.next += data.len() as u64;
        }
        check
            .digest
            .finish(None)
            .context("restored object checksum mismatch")?;
        Ok(())
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
    pub(super) async fn send_range(
        &self,
        c: &Client,
        key: &str,
        query: &[(&str, &str)],
        mut headers: axum::http::HeaderMap,
        from: u64,
        end: u64,
        checksum: bool,
    ) -> Result<(Written, String)> {
        let length = end - from;
        if length <= self.schedule.buffer {
            let body = self.buffer(from, end).await?;
            let hash = STANDARD.encode(Sha256::digest(&body));
            if query.is_empty() {
                self.finish_digest().await?;
            }
            if checksum {
                headers.insert("x-amz-checksum-sha256", hash.parse()?);
            }
            let reply = c
                .bytes(Method::PUT, Some(key), query, headers, body)
                .await?;
            let headers = c.empty(reply).await?;
            ensure!(
                headers
                    .get("x-amz-checksum-sha256")
                    .is_none_or(|v| v.as_bytes() == hash.as_bytes()),
                "S3 upload checksum mismatch"
            );
            self.progress.advance(length, true);
            return Ok((Written::headers(&headers, None)?, hash));
        }
        let hash = if from == 0 && end == self.size() {
            verify::full(
                self.reader,
                self.bucket,
                self.item,
                &self.attrs,
                "sha256",
                self.window,
            )
            .await?
        } else {
            verify::calculate(
                self.reader,
                self.bucket,
                self.item,
                from,
                end,
                "sha256",
                self.window,
            )
            .await?
        };
        if checksum {
            headers.insert("x-amz-checksum-sha256", hash.parse()?);
        }
        headers.insert("content-length", length.to_string().parse()?);
        let (tx, mut rx) = tokio::sync::mpsc::channel::<std::io::Result<Bytes>>(1);
        let body = axum::body::Body::from_stream(
            async_stream::stream! {while let Some(chunk)=rx.recv().await{yield chunk;}},
        );
        let producer = async {
            let mut pos = from;
            let mut sha = Sha256::new();
            while pos < end {
                let data = self.read(pos, end).await?;
                let n = data.len() as u64;
                sha.update(&data);
                pos += n;
                if pos == end {
                    if query.is_empty() {
                        self.finish_digest().await?;
                    }
                    ensure!(
                        STANDARD.encode(sha.clone().finalize()) == hash,
                        "source changed before upload"
                    );
                }
                tx.send(Ok(data)).await.context("S3 closed the upload")?;
                self.progress.advance(n, true);
            }
            drop(tx);
            Ok::<_, anyhow::Error>(())
        };
        let digest = hex::encode(STANDARD.decode(&hash)?);
        let send = async {
            let reply = c
                .request(Method::PUT, Some(key), query, headers, body, &digest)
                .await?;
            Written::headers(&c.empty(reply).await?, None)
        };
        Ok((tokio::try_join!(producer, send)?.1, hash))
    }
    pub async fn remote(
        &self,
        c: &Client,
        key: &str,
        dest: &str,
        threshold: u64,
        part: u64,
    ) -> Result<Outcome> {
        let slot = self.schedule.slots.acquire().await?;
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
            let mut headers = crate::storage::external::objects::write_headers(&metadata, None)?;
            condition.apply(&mut headers)?;
            self.send_range(c, key, &[], headers, 0, self.size(), true)
                .await
                .map(|(w, _)| (w, 0))
        } else {
            drop(slot);
            self.upload(c, key, task, &condition, &metadata, part).await
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
