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
use futures_util::{StreamExt, stream};
use rusqlite::{OptionalExtension, params};
use std::collections::BTreeMap;

pub(super) fn part_width(size: u64, configured: u64) -> Result<u64> {
    ensure!(
        size <= objects::MAX_SIZE,
        "object exceeds the external S3 client's 5TiB limit"
    );
    let width = configured.max(size.div_ceil(10000));
    ensure!(
        width <= objects::MAX_PART,
        "object exceeds the S3 Multipart part limit"
    );
    Ok(width)
}

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
    ) -> Result<Option<BTreeMap<u16, (UploadedPart, Option<String>)>>> {
        let mut parts = BTreeMap::new();
        let mut pos = 0;
        loop {
            let Some(mut page) = c.parts(key, upload, pos).await? else {
                return Ok(None);
            };
            for p in page.items {
                ensure!(p.number > pos && p.number <= 10000, "invalid part sequence");
                pos = p.number;
                let checksum = page.sha256.remove(&p.number);
                parts.insert(p.number, (p, checksum));
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
        let (id, width, algo, remote) = if let Some(v) = saved {
            v
        } else {
            let width = part_width(self.size(), configured)?;
            let (id, algo) = self.start(c, key, meta).await?;
            db.execute(
                "INSERT INTO dump_uploads(task,upload,part_size,size,checksums,owner) VALUES(?,?,?,?,?,?)",
                params![task, id, i64::try_from(width)?, self.item.size, algo,self.owner(&task,&id)],
            )?;
            (id, width, algo, BTreeMap::new())
        };
        let count = u16::try_from(self.size().div_ceil(width))?;
        let work = stream::iter(1..=count).map(|number| {
            let old = remote.get(&number);
            let id = &id;
            async move {
                let start = u64::from(number - 1) * width;
                let end = self.size().min(start + width);
                let length = end - start;
                if let Some((p, remote_hash)) = old
                    && p.size == length
                    && let Some((size, etag, hash)) = db.query_row(
                        "SELECT size,etag,checksum FROM dump_parts WHERE task=? AND number=?",
                        params![task, number],
                        |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?)),
                    ).optional()?
                    && u64::try_from(size).ok() == Some(length)
                    && etag == p.etag
                    && remote_hash.as_ref().is_none_or(|v| v == &hash)
                    && STANDARD.decode(&hash).is_ok_and(|v| v.len() == 32)
                {
                    self.progress.advance(length, false);
                    return Ok::<_, anyhow::Error>((p.clone(), hash, length));
                }
                let _slot = self.schedule.slots.acquire().await?;
                let n = number.to_string();
                let (receipt, hash) = self.send_range(
                    c, key, &[("partNumber", &n), ("uploadId", id)],
                    axum::http::HeaderMap::new(), start, end, algo
                ).await?;
                let part = UploadedPart { number, size: length, etag: receipt.etag };
                db.execute("INSERT OR REPLACE INTO dump_parts VALUES(?,?,?,?,?)",
                    params![task, number, i64::try_from(length)?, part.etag, hash])?;
                Ok((part, hash, 0))
            }
        }).buffered(self.schedule.count);
        futures_util::pin_mut!(work);
        let mut parts = Vec::with_capacity(count.into());
        let mut checksums = Vec::with_capacity(count.into());
        let mut reused = 0;
        while let Some(result) = work.next().await {
            let (part, hash, n) = result?;
            parts.push(part);
            checksums.push(hash);
            reused += n;
        }
        let _slot = self.schedule.slots.acquire().await?;
        self.finish_digest().await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn raises_part_size_at_ten_thousand_parts() {
        let width = 16 * 1024 * 1024;
        for size in [10000 * width - 1, 10000 * width] {
            assert_eq!(part_width(size, width).unwrap(), width);
        }
        assert_eq!(part_width(10000 * width + 1, width).unwrap(), width + 1);
        assert!(part_width(5 * 1024u64.pow(4) + 1, width).is_err());
    }
}
