use crate::model::Id;
use crate::{
    config,
    content::codec::{Chunk, MAX},
    model::{ChunkCreate, ManagedKeyRecord},
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use s3s::s3_error;
use std::sync::Arc;
use uuid::Uuid;

use super::App;

impl App {
    pub(crate) fn content_lock(
        &self,
        storage_id: Uuid,
        hash: [u8; 32],
    ) -> Arc<tokio::sync::Mutex<()>> {
        self.hashes.get((storage_id, hash))
    }
    pub(crate) async fn reference(&self, stream: Id, offset: i64, c: &Chunk) -> Result<()> {
        self.chunks
            .reference(stream, offset, c.id, c.raw_size)
            .await?;
        self.seal_uploads(stream, false).await?;
        Ok(())
    }
    pub async fn put_chunk(
        &self,
        stream: Id,
        offset: i64,
        raw: Vec<u8>,
        retransmitted: bool,
    ) -> Result<Chunk> {
        self.put_chunk_with(stream, offset, raw, retransmitted, None)
            .await
    }
    pub(crate) async fn put_chunk_with(
        &self,
        stream: Id,
        offset: i64,
        raw: Vec<u8>,
        retransmitted: bool,
        accepted: Option<&crate::model::cache::EncodingPolicy>,
    ) -> Result<Chunk> {
        tokio::time::timeout(
            std::time::Duration::from_secs(config::seconds(
                &self.config.requests.upload_idle_timeout,
            )?),
            self.put_chunk_inner(stream, offset, raw, retransmitted, accepted),
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

    async fn put_chunk_inner(
        &self,
        stream: Id,
        offset: i64,
        raw: Vec<u8>,
        retransmitted: bool,
        accepted: Option<&crate::model::cache::EncodingPolicy>,
    ) -> Result<Chunk> {
        ensure!(!raw.is_empty() && raw.len() <= MAX, "invalid chunk length");
        self.renew_write_lease(stream).await?;
        let hash = *blake3::hash(&raw).as_bytes();
        let storage_id = self
            .chunks
            .storage_for_stream(stream)
            .await?
            .context("stream has no store")?;
        let profile = self.storage.profile(storage_id)?;
        let mutex = self.content_lock(storage_id, hash);
        let _guard = mutex.lock().await;
        let (algorithm, key_id) = if let Some(policy) = accepted {
            self.storage.ensure_key_loaded(policy.block_key_id).await?;
            (policy.algorithm.clone(), policy.block_key_id)
        } else {
            self.store_encoding(storage_id).await?
        };
        let _claim = if self.role == "single" {
            None
        } else {
            Some(self.cluster()?.claim_chunk(storage_id, &hash).await?)
        };
        let input = ChunkCreate {
            storage: storage_id,
            stream,
            hash,
            raw_size: raw.len() as i32,
            algorithm,
            key: key_id,
        };
        let (c, reused) = match self.chunks.allocate(input.clone()).await {
            Ok(value) => value,
            Err(error)
                if retransmitted
                    && matches!(
                        crate::model::error(&error),
                        Some(crate::model::Error::SourceUnavailable)
                    ) =>
            {
                return self.repair_chunk(input, offset, raw, accepted).await;
            }
            Err(error) => return Err(error),
        };
        if reused {
            #[cfg(feature = "fault-injection")]
            crate::faults::point("source-before-reference").await;
            if accepted.is_none()
                && profile.upload_cache
                && self.chunks.can_stage_reuse(c.id).await?
            {
                let (data, compressed) = self
                    .storage
                    .cache_payload(&c, Bytes::copy_from_slice(&raw))
                    .await?;
                if self
                    .stage_chunk(stream, offset, &c, data, compressed, false, &profile)
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
        if accepted.is_none()
            && profile.upload_cache
            && self
                .stage_chunk(
                    stream,
                    offset,
                    &c,
                    Bytes::copy_from_slice(&raw),
                    false,
                    true,
                    &profile,
                )
                .await?
        {
            return Ok(c);
        }
        let (c, encoded, cache) = self
            .storage
            .encode(c, raw, true, None, accepted.map(|p| &p.compression))
            .await?;
        self.chunks.mark_uploading(&c).await?;
        self.storage.register_location(&c).await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("chunk-uploading").await;
        self.storage.put(&c, encoded, cache).await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("chunk-stored").await;
        self.chunks.complete(stream, offset, &c).await?;
        Ok(c)
    }

    async fn repair_chunk(
        &self,
        input: ChunkCreate,
        offset: i64,
        raw: Vec<u8>,
        accepted: Option<&crate::model::cache::EncodingPolicy>,
    ) -> Result<Chunk> {
        use crate::model::recovery::{RepairAllocation, RepairNode};
        let stream = input.stream;
        let allocation = {
            let _coord = self.coord.lock().await;
            self.writable()?;
            ensure!(
                !self.draining.load(std::sync::atomic::Ordering::Acquire),
                crate::model::Error::OperationAborted
            );
            let node = self.node.get().map(|state| RepairNode {
                state: state.clone(),
                role: self.role.clone(),
                key: self.secrets.credential_key.clone(),
            });
            self.chunks.prepare_repair(input, node).await?
        };
        let mut repair = match allocation {
            RepairAllocation::Existing(chunk) => {
                self.reference(stream, offset, &chunk).await?;
                return Ok(chunk);
            }
            RepairAllocation::Candidate(repair) => repair,
        };
        #[cfg(feature = "fault-injection")]
        crate::faults::point("repair-prepared").await;
        self.storage
            .verify_source_namespace(&repair.sources[0])
            .await?;
        let (chunk, encoded, cache) = self
            .storage
            .encode(
                repair.chunk.clone(),
                raw,
                true,
                None,
                accepted.map(|p| &p.compression),
            )
            .await?;
        repair.chunk = chunk;
        self.chunks.repair_metadata(&repair).await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("repair-uploading").await;
        self.storage.put(&repair.chunk, encoded, cache).await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("repair-stored").await;
        self.storage.verify_chunk(&repair.chunk).await?;
        self.storage
            .verify_source_namespace(&repair.sources[0])
            .await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("repair-before-commit").await;
        let chunk = {
            let _coord = self.coord.lock().await;
            self.writable()?;
            ensure!(
                !self.draining.load(std::sync::atomic::Ordering::Acquire),
                crate::model::Error::OperationAborted
            );
            self.chunks.finish_repair(&repair, offset).await?
        };
        #[cfg(feature = "fault-injection")]
        crate::faults::point("repair-committed").await;
        self.seal_uploads(repair.stream, false).await?;
        Ok(chunk)
    }
}
