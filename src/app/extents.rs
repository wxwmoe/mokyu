use crate::{config, content::codec::MAX};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures_util::StreamExt;
use s3s::dto::StreamingBlob;
use std::sync::{Arc, Mutex};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

use super::{Active, App, Extent, StoredStream};

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
        self.renew_write_lease(destination).await?;
        let source_target = self
            .chunks
            .storage_for_stream(source.id)
            .await?
            .context("source stream has no store")?;
        let destination_target = self
            .chunks
            .storage_for_stream(destination)
            .await?
            .context("destination stream has no store")?;
        if source_target != destination_target {
            let mut offset = 0;
            while offset < source.size {
                self.renew_write_lease(destination).await?;
                let rows = self.chunks.extents_for_copy(source.id, offset).await?;
                ensure!(!rows.is_empty(), "source mapping is incomplete");
                for row in rows {
                    ensure!(row.offset_bytes == offset, "invalid copy mapping");
                    let data = self.extent_bytes(&row).await?;
                    self.put_chunk(destination, offset, data.to_vec(), false)
                        .await?;
                    offset += i64::from(row.length);
                }
            }
            return Ok(());
        }
        let mut offset = 0;
        while offset < source.size {
            self.renew_write_lease(destination).await?;
            let rows = self.chunks.extents_for_copy(source.id, offset).await?;
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
            self.chunks
                .copy_references(destination, source.id, offset, end, &chunks)
                .await?;
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
            let c = self
                .chunks
                .ready_chunk(id)
                .await?
                .context("referenced chunk not ready")?;
            self.storage.get_with(&c, Some(context)).await?
        } else {
            let id = e.fragment_id.context("extent has no source")?;
            let source = self.chunks.fragment(id).await?;
            let data = self
                .storage
                .read_fragment(
                    id,
                    source.staging_storage_id,
                    source.staging_object_key.as_deref(),
                )
                .await
                .context("multipart fragment missing")?;
            ensure!(
                data.len() == source.size as usize
                    && data.len() <= MAX
                    && blake3::hash(&data).as_bytes() == source.hash.as_slice(),
                "local fragment integrity mismatch"
            );
            Bytes::from(data)
        };
        e.slice(data)
    }
    pub fn body(
        self: &Arc<Self>,
        stream: StoredStream,
        pin: Active,
        span: std::ops::Range<i64>,
        permits: (OwnedSemaphorePermit, OwnedSemaphorePermit),
        range: bool,
        capture: Option<Arc<Mutex<super::responses::Mapping>>>,
    ) -> StreamingBlob {
        let app = self.clone();
        let (start, end) = (span.start, span.end);
        let body = async_stream::try_stream! {
            let _pin=pin;let _permits=permits;let read_context=Arc::new(crate::storage::ReadContext::user(range));
            #[cfg(feature="fault-injection")]
            crate::faults::point("read-body").await;
            _pin.renew_read_lease().await?;
            let mut cursor=if start==0 {Some(0)} else {app.chunks.read_start(stream.id,start).await?};
            let mut position=start;
            while position<end {
                app.ensure_mode()?;
                #[cfg(feature="fault-injection")]
                if position != start && _pin.read_lease.as_ref().is_some_and(|lease| lease.replica) {
                    crate::faults::point("read-replica-page").await;
                }
                _pin.renew_read_lease().await?;
                let from=cursor.context("object mapping is incomplete")?;
                let rows=if position != start && let Some(lease)=_pin.read_lease.as_ref().filter(|lease|lease.replica) {
                    lease.store.read_page(stream.id,lease.id,from,end).await?
                } else {app.chunks.read_extents(stream.id,from,end).await?};
                (!rows.is_empty()).then_some(()).context("object mapping is incomplete")?;
                if let Some(capture)=&capture { let mut mapping=capture.lock().unwrap(); for row in &rows { mapping.record(row); } }
                let mut loaded=futures_util::stream::iter(rows.into_iter().map(|row| {let app=app.clone();let context=read_context.clone();async move{let data=app.storage.get_with(&row.chunk,Some(&context)).await?;let data=row.extent.slice(data)?;Ok::<_,anyhow::Error>((row.extent,data))}})).buffered(2);
                while let Some(result)=loaded.next().await {
                    _pin.renew_read_lease().await?;
                    let (row,data)=result?;
                    (row.offset_bytes<=position && row.offset_bytes+row.length as i64>position).then_some(()).context("object mapping has a gap")?;
                    let skip=(position-row.offset_bytes) as usize;let count=((end-position) as usize).min(data.len()-skip);
                    position+=count as i64;cursor=Some(row.offset_bytes+row.length as i64);
                    if position==end && let Some(capture)=&capture { capture.lock().unwrap().finish_source(); }
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
