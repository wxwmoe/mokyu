use crate::{
    config,
    content::codec::{Chunk, MAX},
    model::{ChunkClaim, ChunkCreate, ManagedKeyRecord},
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use s3s::s3_error;
use std::sync::{Arc, Mutex, Weak};
use uuid::Uuid;

use super::App;

impl App {
    pub(crate) async fn reference(&self, stream: Uuid, offset: i64, c: &Chunk) -> Result<()> {
        self.chunks
            .reference(stream, offset, c.id, c.raw_size)
            .await?;
        self.seal_uploads(stream, false).await?;
        Ok(())
    }
    pub async fn put_chunk(&self, stream: Uuid, offset: i64, raw: Vec<u8>) -> Result<Chunk> {
        tokio::time::timeout(
            std::time::Duration::from_secs(config::seconds(
                &self.config.requests.upload_idle_timeout,
            )?),
            self.put_chunk_inner(stream, offset, raw),
        )
        .await
        .map_err(|_| s3_error!(RequestTimeout, "upload processing made no progress"))?
    }
    pub(crate) async fn store_encoding(&self, storage_id: Uuid) -> Result<(String, Option<Uuid>)> {
        let Some(key) = self.managed_keys.for_storage(storage_id).await? else {
            return Ok(("none".into(), None));
        };
        let ManagedKeyRecord {
            id,
            name,
            algorithm,
        } = key;
        if self.secrets.key(&name).is_none() {
            self.managed_keys
                .ensure_loaded(&self.secrets, &name)
                .await?;
        }
        let loaded = self
            .secrets
            .key(&name)
            .with_context(|| format!("missing store block key {name}"))?;
        ensure!(
            loaded.algorithm == algorithm,
            "store block key algorithm mismatch"
        );
        Ok((algorithm, Some(id)))
    }

    async fn put_chunk_inner(&self, stream: Uuid, offset: i64, raw: Vec<u8>) -> Result<Chunk> {
        ensure!(!raw.is_empty() && raw.len() <= MAX, "invalid chunk length");
        self.renew_write_lease(stream).await?;
        let hash = *blake3::hash(&raw).as_bytes();
        let storage_id = self
            .chunks
            .storage_for_stream(stream)
            .await?
            .context("stream has no store")?;
        let mutex = {
            let mut m = self.hashes.lock().unwrap();
            m.retain(|_, v| v.strong_count() > 0);
            if let Some(l) = m.get(&(storage_id, hash)).and_then(Weak::upgrade) {
                l
            } else {
                let l = Arc::new(tokio::sync::Mutex::new(()));
                m.insert((storage_id, hash), Arc::downgrade(&l));
                l
            }
        };
        let _guard = mutex.lock().await;
        let (algorithm, key_id) = self.store_encoding(storage_id).await?;
        let ChunkClaim {
            chunk: c,
            reused,
            guard: _claim,
        } = self
            .chunks
            .allocate(ChunkCreate {
                storage: storage_id,
                stream,
                hash,
                raw_size: raw.len() as i32,
                algorithm,
                key: key_id,
            })
            .await?;
        if reused {
            if self.config.local_cache.upload_cache && self.chunks.can_stage_reuse(c.id).await? {
                let (data, compressed) = self
                    .storage
                    .cache_payload(&c, Bytes::copy_from_slice(&raw))
                    .await?;
                if self
                    .stage_chunk(stream, offset, &c, data, compressed, false)
                    .await?
                {
                    return Ok(c);
                }
            }
            self.reference(stream, offset, &c).await?;
            return Ok(c);
        }
        #[cfg(feature = "fault-injection")]
        crate::faults::point("chunk-allocated").await;
        let mut c = c;
        c.location_id = Uuid::new_v4();
        let (c, encoded, cache) = self.storage.encode(c, raw, true).await?;
        self.chunks.mark_uploading(&c).await?;
        if self
            .stage_chunk(
                stream,
                offset,
                &c,
                cache.clone(),
                crate::content::codec::cache_compressed(
                    &c,
                    self.config.local_cache.min_compression_savings_percent,
                ),
                true,
            )
            .await?
        {
            return Ok(c);
        }
        self.storage.register_location(&c).await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("chunk-uploading").await;
        self.storage.put(&c, encoded, cache).await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("chunk-stored").await;
        self.chunks.complete(stream, offset, &c).await?;
        Ok(c)
    }
}
