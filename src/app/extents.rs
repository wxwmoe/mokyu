use crate::{
    codec::{Chunk, MAX},
    config,
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures_util::StreamExt;
use s3s::dto::StreamingBlob;
use std::sync::{Arc, Mutex};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

use super::{Active, App, Extent, ReadExtent, StoredStream};

impl App {
    pub async fn extent_bytes(&self, e: &Extent) -> Result<Bytes> {
        self.extent_bytes_with(e, &crate::storage::ReadContext::default())
            .await
    }
    pub(crate) async fn copy_extents(
        &self,
        destination: Uuid,
        source: &StoredStream,
    ) -> Result<()> {
        let source_target: Uuid = sqlx::query_scalar::<_, Option<Uuid>>(
            "SELECT b.storage_id FROM streams s JOIN buckets b ON b.id=s.bucket_id WHERE s.id=$1",
        )
        .bind(source.id)
        .fetch_one(&self.db)
        .await?
        .context("source stream has no store")?;
        let destination_target: Uuid = sqlx::query_scalar::<_, Option<Uuid>>(
            "SELECT b.storage_id FROM streams s JOIN buckets b ON b.id=s.bucket_id WHERE s.id=$1",
        )
        .bind(destination)
        .fetch_one(&self.db)
        .await?
        .context("destination stream has no store")?;
        if source_target != destination_target {
            let mut offset = 0;
            while offset < source.size {
                let rows: Vec<Extent> = sqlx::query_as(
                    "SELECT * FROM extents WHERE stream_id=$1 AND offset_bytes>=$2 ORDER BY offset_bytes LIMIT 500",
                )
                .bind(source.id)
                .bind(offset)
                .fetch_all(&self.db)
                .await?;
                ensure!(!rows.is_empty(), "source mapping is incomplete");
                for row in rows {
                    ensure!(row.offset_bytes == offset, "invalid copy mapping");
                    let data = self.extent_bytes(&row).await?;
                    self.put_chunk(destination, offset, data.to_vec(), true)
                        .await?;
                    offset += i64::from(row.length);
                }
            }
            return Ok(());
        }
        let mut offset = 0;
        while offset < source.size {
            let rows: Vec<Extent> = sqlx::query_as("SELECT * FROM extents WHERE stream_id=$1 AND offset_bytes>=$2 ORDER BY offset_bytes LIMIT 500")
                .bind(source.id).bind(offset).fetch_all(&self.db).await?;
            ensure!(!rows.is_empty(), "source mapping is incomplete");
            let mut end = offset;
            let mut chunks = Vec::with_capacity(rows.len());
            for row in &rows {
                ensure!(
                    row.offset_bytes == end && row.fragment_id.is_none(),
                    "invalid copy mapping"
                );
                chunks.push(row.chunk_id.context("copy source has no chunk")?);
                end += i64::from(row.length);
            }
            ensure!(end <= source.size, "copy mapping exceeds source");
            chunks.sort_unstable();
            chunks.dedup();
            let mut tx = self.db.begin().await?;
            let locked: Vec<i64> = sqlx::query_scalar(
                "SELECT id FROM chunks WHERE id=ANY($1) AND state='ready' ORDER BY id FOR UPDATE",
            )
            .bind(&chunks)
            .fetch_all(&mut *tx)
            .await?;
            ensure!(locked.len() == chunks.len(), "copy source is not ready");
            sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,chunk_id,source_offset) SELECT $1,offset_bytes,length,chunk_id,source_offset FROM extents WHERE stream_id=$2 AND offset_bytes>=$3 AND offset_bytes<$4")
                .bind(destination).bind(source.id).bind(offset).bind(end).execute(&mut *tx).await?;
            sqlx::query("UPDATE chunks SET unreferenced_at=NULL WHERE id=ANY($1)")
                .bind(&chunks)
                .execute(&mut *tx)
                .await?;
            sqlx::query("INSERT INTO cache_pins(chunk_id,pin_type,owner_id) SELECT chunk_id,CASE WHEN source_pack_id IS NULL THEN 'upload' ELSE 'pack' END,$2 FROM pending_uploads WHERE chunk_id=ANY($1) ON CONFLICT DO NOTHING")
                .bind(&chunks).bind(destination).execute(&mut *tx).await?;
            sqlx::query(
                "UPDATE streams SET size=$2,touched_at=now() WHERE id=$1 AND state='writing'",
            )
            .bind(destination)
            .bind(end)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            offset = end;
        }
        Ok(())
    }
    pub async fn extent_bytes_with(
        &self,
        e: &Extent,
        context: &crate::storage::ReadContext,
    ) -> Result<Bytes> {
        tokio::time::timeout(
            std::time::Duration::from_secs(config::seconds(
                &self.config.requests.upload_idle_timeout,
            )?),
            self.extent_bytes_inner(e, context),
        )
        .await
        .context("multipart reconstruction made no progress before timeout")?
    }
    async fn extent_bytes_inner(
        &self,
        e: &Extent,
        context: &crate::storage::ReadContext,
    ) -> Result<Bytes> {
        let data = if let Some(id) = e.chunk_id {
            let c: Chunk = sqlx::query_as("SELECT c.id,c.current_encoding_id AS current_encoding_id,COALESCE(l.location_id,'00000000-0000-0000-0000-000000000000'::uuid) AS location_id,c.storage_id,c.hash,c.raw_size,COALESCE(l.standalone_size,c.standalone_size) AS standalone_size,COALESCE(l.encryption_algorithm,c.encryption_algorithm) AS encryption_algorithm,COALESCE(l.block_key_id,c.block_key_id) AS block_key_id,COALESCE(l.standalone_compressed,c.standalone_compressed) AS standalone_compressed,COALESCE(l.nonce,c.nonce) AS nonce,c.format,c.state,c.created_at FROM chunks c LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' WHERE c.id=$1 AND c.state='ready'")
                .bind(id)
                .fetch_optional(&self.db)
                .await?
                .context("referenced chunk not ready")?;
            self.storage.get_with(&c, Some(context)).await?
        } else {
            let id = e.fragment_id.context("extent has no source")?;
            let (size, hash, staging_storage_id, staging_object_key): (
                i32,
                Vec<u8>,
                Option<Uuid>,
                Option<String>,
            ) = sqlx::query_as(
                "SELECT size,hash,staging_storage_id,staging_object_key FROM fragments WHERE id=$1",
            )
            .bind(id)
            .fetch_one(&self.db)
            .await?;
            let data = self
                .storage
                .read_fragment(id, staging_storage_id, staging_object_key.as_deref())
                .await
                .context("multipart fragment missing")?;
            ensure!(
                data.len() == size as usize
                    && data.len() <= MAX
                    && blake3::hash(&data).as_bytes() == hash.as_slice(),
                "local fragment integrity mismatch"
            );
            Bytes::from(data)
        };
        e.slice(data)
    }
    pub async fn extents(&self, stream: Uuid, from: i64, until: i64) -> Result<Vec<Extent>> {
        Ok(sqlx::query_as("SELECT * FROM extents WHERE stream_id=$1 AND offset_bytes>=$2 AND offset_bytes<$3 ORDER BY offset_bytes LIMIT 64").bind(stream).bind(from).bind(until).fetch_all(&self.db).await?)
    }
    async fn read_extents(&self, stream: Uuid, from: i64, until: i64) -> Result<Vec<ReadExtent>> {
        // Bound the mapping page before joining; a missing ready chunk must fail decoding the row.
        Ok(sqlx::query_as("SELECT e.*,c.id,c.current_encoding_id AS current_encoding_id,COALESCE(l.location_id,'00000000-0000-0000-0000-000000000000'::uuid) AS location_id,c.storage_id,c.hash,c.raw_size,COALESCE(l.standalone_size,c.standalone_size) AS standalone_size,COALESCE(l.encryption_algorithm,c.encryption_algorithm) AS encryption_algorithm,COALESCE(l.block_key_id,c.block_key_id) AS block_key_id,COALESCE(l.standalone_compressed,c.standalone_compressed) AS standalone_compressed,COALESCE(l.nonce,c.nonce) AS nonce,c.format,c.state,c.created_at FROM (SELECT * FROM extents WHERE stream_id=$1 AND offset_bytes>=$2 AND offset_bytes<$3 ORDER BY offset_bytes LIMIT 64) e LEFT JOIN chunks c ON c.id=e.chunk_id AND c.state='ready' LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' ORDER BY e.offset_bytes")
            .bind(stream).bind(from).bind(until).fetch_all(&self.db).await?)
    }
    pub fn body(
        self: &Arc<Self>,
        stream: StoredStream,
        pin: Active,
        start: i64,
        end: i64,
        permits: (OwnedSemaphorePermit, OwnedSemaphorePermit),
        range: bool,
    ) -> StreamingBlob {
        let app = self.clone();
        let body = async_stream::try_stream! {
            let _pin=pin;let _permits=permits;let read_context=Arc::new(crate::storage::ReadContext::user(range));
            let mut cursor:Option<i64>=sqlx::query_scalar("SELECT offset_bytes FROM extents WHERE stream_id=$1 AND offset_bytes<=$2 ORDER BY offset_bytes DESC LIMIT 1").bind(stream.id).bind(start).fetch_optional(&app.db).await?;
            let mut position=start;
            while position<end {
                let rows=app.read_extents(stream.id,cursor.context("object mapping is incomplete")?,end).await?;
                (!rows.is_empty()).then_some(()).context("object mapping is incomplete")?;
                let mut loaded=futures_util::stream::iter(rows.into_iter().map(|row| {let app=app.clone();let context=read_context.clone();async move{let data=app.storage.get_with(&row.chunk,Some(&context)).await?;let data=row.extent.slice(data)?;Ok::<_,anyhow::Error>((row.extent,data))}})).buffered(2);
                while let Some(result)=loaded.next().await {
                    let (row,data)=result?;
                    (row.offset_bytes<=position && row.offset_bytes+row.length as i64>position).then_some(()).context("object mapping has a gap")?;
                    let skip=(position-row.offset_bytes) as usize;let count=((end-position) as usize).min(data.len()-skip);
                    position+=count as i64;cursor=Some(row.offset_bytes+row.length as i64);
                    if let Some(id)=row.chunk_id {app.storage.access.chunk(id,range,count,false);}
                    yield data.slice(skip..skip+count);
                    if position==end {break;}
                }
            }
        };
        let mut body = Box::pin(body.map(|r: Result<Bytes>| r.map_err(std::io::Error::other)));
        let idle = std::time::Duration::from_secs(
            config::seconds(&self.config.requests.read_idle_timeout)
                .expect("validated read timeout"),
        );
        let limited = async_stream::stream! {
            loop {match tokio::time::timeout(idle,body.next()).await {
                Ok(Some(result))=>{let failed=result.is_err();yield result;if failed {break;}},
                Ok(None)=>break,
                Err(_)=>{yield Err(std::io::Error::new(std::io::ErrorKind::TimedOut,"read made no progress"));break;}
            }}
        };
        let body = Mutex::new(Box::pin(limited));
        StreamingBlob::wrap(futures_util::stream::poll_fn(move |cx| {
            futures_util::Stream::poll_next(body.lock().unwrap().as_mut(), cx)
        }))
    }
}
