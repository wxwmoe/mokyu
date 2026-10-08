use crate::{
    codec::{Chunk, MAX},
    config,
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use s3s::s3_error;
use std::sync::{Arc, Mutex, Weak};
use uuid::Uuid;

use super::App;

impl App {
    pub(crate) async fn reference(&self, stream: Uuid, offset: i64, c: &Chunk) -> Result<()> {
        let mut tx = self.db.begin().await?;
        let state: String = sqlx::query_scalar("SELECT state FROM chunks WHERE id=$1 FOR UPDATE")
            .bind(c.id)
            .fetch_one(&mut *tx)
            .await?;
        if state != "ready" {
            return Err(s3_error!(OperationAborted, "chunk is being reclaimed").into());
        }
        sqlx::query(
            "INSERT INTO extents(stream_id,offset_bytes,length,chunk_id) VALUES($1,$2,$3,$4)",
        )
        .bind(stream)
        .bind(offset)
        .bind(c.raw_size)
        .bind(c.id)
        .execute(&mut *tx)
        .await?;
        sqlx::query("UPDATE chunks SET unreferenced_at=NULL WHERE id=$1")
            .bind(c.id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO cache_pins(chunk_id,pin_type,owner_id) SELECT chunk_id,CASE WHEN source_pack_id IS NULL THEN 'upload' ELSE 'pack' END,$2 FROM pending_uploads WHERE chunk_id=$1 ON CONFLICT DO NOTHING")
            .bind(c.id).bind(stream).execute(&mut *tx).await?;
        sqlx::query("UPDATE pending_uploads SET stream_id=$2,offset_bytes=$3 WHERE chunk_id=$1 AND owner_task IS NULL AND source_pack_id IS NULL AND EXISTS(SELECT 1 FROM streams WHERE id=$2 AND stream_type='object' AND state='writing')")
            .bind(c.id).bind(stream).bind(offset).execute(&mut *tx).await?;
        sqlx::query("UPDATE streams SET size=GREATEST(size,$2),touched_at=now() WHERE id=$1 AND state='writing'").bind(stream).bind(offset+c.raw_size as i64).execute(&mut *tx).await?;
        tx.commit().await?;
        self.seal_uploads(stream, false).await?;
        Ok(())
    }
    pub async fn put_chunk(
        &self,
        stream: Uuid,
        offset: i64,
        raw: Vec<u8>,
        should_compress: bool,
    ) -> Result<Chunk> {
        tokio::time::timeout(
            std::time::Duration::from_secs(config::seconds(
                &self.config.requests.upload_idle_timeout,
            )?),
            self.put_chunk_inner(stream, offset, raw, should_compress),
        )
        .await
        .map_err(|_| s3_error!(RequestTimeout, "upload processing made no progress"))?
    }
    pub(crate) async fn store_encoding(&self, storage_id: Uuid) -> Result<(String, Option<Uuid>)> {
        let key: Option<(Uuid, String, String)> = sqlx::query_as(
            "SELECT k.id,k.name,k.key_algorithm FROM storages s JOIN managed_keys k ON k.id=s.block_key_id WHERE s.id=$1 AND k.purpose='block' AND k.revoked_at IS NULL",
        )
        .bind(storage_id)
        .fetch_optional(&self.db)
        .await?;
        let Some((_id, name, algorithm)) = key else {
            return Ok(("none".into(), None));
        };
        if self.secrets.key(&name).is_none() {
            crate::db::load_runtime_key(&self.db, &self.secrets, &name).await?;
        }
        let loaded = self
            .secrets
            .key(&name)
            .with_context(|| format!("missing store block key {name}"))?;
        ensure!(
            loaded.algorithm == algorithm,
            "store block key algorithm mismatch"
        );
        Ok((algorithm, Some(_id)))
    }

    async fn put_chunk_inner(
        &self,
        stream: Uuid,
        offset: i64,
        raw: Vec<u8>,
        should_compress: bool,
    ) -> Result<Chunk> {
        ensure!(!raw.is_empty() && raw.len() <= MAX, "invalid chunk length");
        let hash = *blake3::hash(&raw).as_bytes();
        let storage_id: Uuid = sqlx::query_scalar::<_, Option<Uuid>>(
            "SELECT b.storage_id FROM streams s JOIN buckets b ON b.id=s.bucket_id WHERE s.id=$1",
        )
        .bind(stream)
        .fetch_one(&self.db)
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
        // Rollback releases the claim on every return and cancellation path.
        let mut hash_lock = self.db.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1::uuid::text || ':' || encode($2,'hex'), 734922709851001))")
            .bind(storage_id)
            .bind(hash.as_slice())
            .execute(&mut *hash_lock)
            .await?;
        sqlx::query("UPDATE chunks SET state='failed',unreferenced_at=now(),owner_stream=NULL WHERE storage_id=$1 AND hash=$2 AND raw_size=$3 AND state IN ('preparing','uploading')")
            .bind(storage_id).bind(hash.as_slice()).bind(raw.len() as i32).execute(&self.db).await?;
        let c: Chunk = loop {
            let mut tx = self.db.begin().await?;
            let old: Option<Chunk> = sqlx::query_as("SELECT * FROM chunks WHERE storage_id=$1 AND hash=$2 AND raw_size=$3 AND state='ready' ORDER BY id LIMIT 1")
                .bind(storage_id).bind(hash.as_slice()).bind(raw.len() as i32).fetch_optional(&mut *tx).await?;
            if let Some(c) = old {
                tx.commit().await?;
                if self.config.local_cache.upload_cache {
                    let packed: bool = sqlx::query_scalar("SELECT pack_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=$1) FROM chunks WHERE id=$1")
                        .bind(c.id).fetch_one(&self.db).await?;
                    if packed {
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
                }
                match self.reference(stream, offset, &c).await {
                    Ok(()) => return Ok(c),
                    Err(e) => return Err(e),
                }
            }
            let inserted: Option<Chunk> = sqlx::query_as("INSERT INTO chunks(storage_id,hash,raw_size,encryption_algorithm,block_key_id,owner_stream,state,unreferenced_at) VALUES($1,$2,$3,$4,$5,$6,'preparing',now()) ON CONFLICT (storage_id,hash,raw_size) WHERE state IN ('preparing','uploading','ready') DO NOTHING RETURNING *")
                .bind(storage_id).bind(hash.as_slice()).bind(raw.len() as i32).bind(&algorithm).bind(key_id).bind(stream).fetch_optional(&mut *tx).await?;
            tx.commit().await?;
            if let Some(c) = inserted {
                break c;
            }
            return Err(
                s3_error!(OperationAborted, "chunk allocation is already in progress").into(),
            );
        };
        #[cfg(feature = "fault-injection")]
        crate::faults::point("chunk-allocated").await;
        let mut c = c;
        c.location_id = Uuid::new_v4();
        let (c, encoded, cache) = self.storage.encode(c, raw, should_compress).await?;
        sqlx::query("UPDATE chunks SET standalone_size=$2,standalone_compressed=$3,nonce=$4,state='uploading' WHERE id=$1 AND state='preparing'")
            .bind(c.id).bind(c.stored_size).bind(c.compressed).bind(&c.nonce).execute(&self.db).await?;
        if self
            .stage_chunk(
                stream,
                offset,
                &c,
                cache.clone(),
                crate::codec::cache_compressed(
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
        let mut tx = self.db.begin().await?;
        let changed=sqlx::query("UPDATE chunks SET state='ready',unreferenced_at=NULL,owner_stream=NULL WHERE id=$1 AND state='uploading'").bind(c.id).execute(&mut *tx).await?.rows_affected();
        sqlx::query("UPDATE chunk_encodings SET state='ready',unreferenced_at=NULL,stored_at=clock_timestamp() WHERE encoding_id=$1 AND state='uploading'").bind(c.encoding_id).execute(&mut *tx).await?;
        if changed != 1 {
            return Err(s3_error!(OperationAborted).into());
        }
        sqlx::query(
            "INSERT INTO extents(stream_id,offset_bytes,length,chunk_id) VALUES($1,$2,$3,$4)",
        )
        .bind(stream)
        .bind(offset)
        .bind(c.raw_size)
        .bind(c.id)
        .execute(&mut *tx)
        .await?;
        sqlx::query("UPDATE streams SET size=$2,touched_at=now() WHERE id=$1 AND state='writing'")
            .bind(stream)
            .bind(offset + c.raw_size as i64)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(c)
    }
}
