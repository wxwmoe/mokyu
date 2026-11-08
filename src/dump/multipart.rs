use super::output::Transfer;
use crate::storage::{
    external::objects::UploadedPart,
    external::{
        Client,
        objects::{self, Condition, Written},
    },
};
use anyhow::{Result, ensure};
use axum::http::Method;
use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::Bytes;
use rusqlite::{OptionalExtension, params};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

impl Transfer<'_, '_> {
    fn owner(&self, task: &[u8; 32], id: &str) -> [u8; 32] {
        let key = blake3::derive_key(
            "mokyu/dump/upload-owner",
            &self.reader.backend.source.material,
        );
        *blake3::Hasher::new_keyed(&key)
            .update(task)
            .update(id.as_bytes())
            .finalize()
            .as_bytes()
    }
    pub(super) fn forget_upload(&self, task: &[u8; 32]) -> Result<()> {
        let tx = self.reader.db.unchecked_transaction()?;
        tx.execute("DELETE FROM dump_parts WHERE task=?", [task])?;
        tx.execute("DELETE FROM dump_uploads WHERE task=?", [task])?;
        tx.commit()?;
        Ok(())
    }
    async fn abort_owned(
        &self,
        c: &Client,
        key: &str,
        task: &[u8; 32],
        upload: &str,
    ) -> Result<()> {
        self.reader.db.execute(
            "UPDATE dump_uploads SET aborting=1 WHERE task=?1 AND upload=?2",
            params![task, upload],
        )?;
        c.abort_upload(key, upload).await?;
        self.forget_upload(task)
    }
    async fn start(
        &self,
        c: &Client,
        key: &str,
        meta: &BTreeMap<String, String>,
    ) -> Result<(String, bool)> {
        let mut h = objects::write_headers(meta, None)?;
        h.insert("x-amz-checksum-algorithm", "SHA256".parse()?);
        h.insert("x-amz-checksum-type", "COMPOSITE".parse()?);
        let resp = c
            .bytes(Method::POST, Some(key), &[("uploads", "")], h, Bytes::new())
            .await?;
        if matches!(resp.status().as_u16(), 400 | 501) {
            return Ok((c.start_upload(key, meta, None).await?, false));
        }
        let resp = c.checked(resp).await?;
        Ok((
            objects::created_upload(&c.bounded(resp.into_body(), 64 * 1024).await?, key)?,
            true,
        ))
    }
    async fn remote_parts(
        &self,
        c: &Client,
        key: &str,
        upload: &str,
    ) -> Result<Option<BTreeMap<u16, UploadedPart>>> {
        let mut parts = BTreeMap::new();
        let mut pos = 0;
        loop {
            let Some(page) = c.parts(key, upload, pos).await? else {
                return Ok(None);
            };
            for p in page.items {
                ensure!(p.number > pos && p.number <= 10000, "invalid part sequence");
                pos = p.number;
                parts.insert(p.number, p);
            }
            match page.next {
                None => break,
                Some(next) => ensure!(next == pos && pos < 10000, "invalid parts cursor"),
            }
        }
        Ok(Some(parts))
    }
    pub(super) async fn upload(
        &self,
        c: &Client,
        key: &str,
        task: [u8; 32],
        condition: &Condition,
        meta: &BTreeMap<String, String>,
        configured: u64,
        buffer: u64,
    ) -> Result<(Written, u64)> {
        let db = self.reader.db;
        let previous = db
            .query_row(
                "SELECT upload,part_size,size,checksums,aborting,owner FROM dump_uploads WHERE task=?",
                [task],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, bool>(3)?,
                        r.get::<_, bool>(4)?, r.get::<_,[u8;32]>(5)?,
                    ))
                },
            )
            .optional()?;
        let mut saved = None;
        if let Some((id, part, size, algo, aborting, owner)) = previous {
            ensure!(
                owner == self.owner(&task, &id),
                "cannot establish ownership of saved Upload ID"
            );
            let width = u64::try_from(part).unwrap_or(0);
            if aborting
                || size != self.item.size
                || !(5 * 1024 * 1024..=objects::MAX_PART).contains(&width)
                || self.size().div_ceil(width) > 10000
            {
                self.abort_owned(c, key, &task, &id).await?;
            } else if let Some(parts) = self.remote_parts(c, key, &id).await? {
                if parts
                    .keys()
                    .any(|n| u64::from(*n) > self.size().div_ceil(width))
                {
                    self.abort_owned(c, key, &task, &id).await?;
                } else {
                    saved = Some((id, width, algo, parts));
                }
            } else {
                self.forget_upload(&task)?;
            }
        }
        let (id, width, algo, mut remote) = if let Some(v) = saved {
            v
        } else {
            let width = configured.max(self.size().div_ceil(10000));
            ensure!(
                width <= objects::MAX_PART,
                "object exceeds S3 Multipart size limit"
            );
            ensure!(
                width <= buffer,
                "MOKYU_DUMP_MEMORY_LIMIT is too small for the required S3 part"
            );
            let (id, algo) = self.start(c, key, meta).await?;
            db.execute(
                "INSERT INTO dump_uploads(task,upload,part_size,size,checksums,owner) VALUES(?,?,?,?,?,?)",
                params![task, id, i64::try_from(width)?, self.item.size, algo,self.owner(&task,&id)],
            )?;
            (id, width, algo, BTreeMap::new())
        };
        let mut parts = Vec::new();
        let mut checksums = Vec::new();
        let mut reused = 0;
        for number in 1..=self.size().div_ceil(width) {
            let number = u16::try_from(number)?;
            let start = u64::from(number - 1) * width;
            let end = self.size().min(start + width);
            let length = end - start;
            let mut receipt = None;
            if let Some(remote) = remote.remove(&number)
                && remote.size == length
                && let Some((size, etag, hash)) = db
                    .query_row(
                        "SELECT size,etag,checksum FROM dump_parts WHERE task=? AND number=?",
                        params![task, number],
                        |r| {
                            Ok((
                                r.get::<_, i64>(0)?,
                                r.get::<_, String>(1)?,
                                r.get::<_, String>(2)?,
                            ))
                        },
                    )
                    .optional()?
                && u64::try_from(size).ok() == Some(length)
                && etag == remote.etag
                && STANDARD.decode(&hash).is_ok_and(|v| v.len() == 32)
            {
                receipt = Some((remote, hash));
                reused += length;
            }
            let (part, hash) = if let Some(v) = receipt {
                v
            } else {
                ensure!(
                    length <= buffer,
                    "MOKYU_DUMP_MEMORY_LIMIT is too small for the saved part size"
                );
                let body = self.buffer(start, end).await?;
                let hash = STANDARD.encode(Sha256::digest(&body));
                let part = if algo {
                    let mut h = axum::http::HeaderMap::new();
                    h.insert("x-amz-checksum-sha256", hash.parse()?);
                    let n = number.to_string();
                    let reply = c
                        .bytes(
                            Method::PUT,
                            Some(key),
                            &[("partNumber", &n), ("uploadId", &id)],
                            h,
                            body,
                        )
                        .await?;
                    let headers = c.empty(reply).await?;
                    ensure!(
                        headers
                            .get("x-amz-checksum-sha256")
                            .is_none_or(|v| v.as_bytes() == hash.as_bytes()),
                        "S3 part checksum mismatch"
                    );
                    UploadedPart {
                        number,
                        size: length,
                        etag: Written::headers(&headers, None)?.etag,
                    }
                } else {
                    c.upload_part(key, &id, number, body).await?
                };
                db.execute(
                    "INSERT OR REPLACE INTO dump_parts VALUES(?,?,?,?,?)",
                    params![task, number, i64::try_from(length)?, part.etag, hash],
                )?;
                (part, hash)
            };
            parts.push(part);
            checksums.push(hash);
        }
        db.execute("UPDATE dump_uploads SET completing=1 WHERE task=?", [task])?;
        let written = c
            .complete_with_checksums(
                key,
                &id,
                &parts,
                condition,
                algo.then_some(checksums.as_slice()),
            )
            .await?;
        Ok((written, reused))
    }
}
