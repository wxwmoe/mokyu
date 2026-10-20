use crate::model::Id;
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
    pub(crate) async fn copy_extents(&self, destination: Id, source: &StoredStream) -> Result<()> {
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
        let raw_revision = self.storage.raw_policy(destination_target)?;
        if source.payload_type == "raw" || raw_revision.is_some() {
            return self
                .copy_payload(destination, destination_target, raw_revision, source)
                .await;
        }
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

    pub(crate) fn payload_bytes<'a>(
        &'a self,
        source: &'a StoredStream,
    ) -> impl futures_util::Stream<Item = Result<Bytes>> + Send + 'a {
        async_stream::try_stream! {
            let mut offset = 0;
            if source.payload_type=="raw" {
                use md5::{Digest, Md5};
                loop {
                    let rows = self.raw.page(source.id,offset,source.size).await?;
                    (!rows.is_empty()).then_some(()).context("raw mapping is incomplete")?;
                    let before = offset;
                    for row in rows {
                        if row.offset_bytes+row.length<=offset && source.size!=0 { continue; }
                        row.validate()?;
                        (row.offset_bytes==offset).then_some(()).context("raw mapping has gaps or overlaps")?;
                        let whole_source = row.source_offset == 0 && row.length == row.source.size;
                        let mut digest = Md5::new();
                        let mut reader = self.storage.raw_reader(&row.source,!whole_source).await?;
                        reader.file.seek(std::io::SeekFrom::Start(row.source_offset as u64)).await?;
                        let mut remaining = row.length;
                        while remaining>0 {
                            self.ensure_mode()?;
                            let mut buffer = vec![0; remaining.min(256*1024) as usize];
                            let count = reader.file.read(&mut buffer).await?;
                            (count>0).then_some(()).context("raw source was truncated")?;
                            buffer.truncate(count);
                            if whole_source { digest.update(&buffer); }
                            remaining -= count as i64;
                            offset += count as i64;
                            reader.file.account_read(&self.storage, count as u64);
                            yield Bytes::from(buffer);
                        }
                        if whole_source {
                            (Some(digest.finalize().as_slice()) == row.source.digest.as_deref())
                                .then_some(()).context(crate::content::codec::IntegrityError::Hash)?;
                        }
                    }
                    if offset==source.size { break; }
                    (offset>before && offset<source.size).then_some(()).context("raw mapping length mismatch")?;
                }
            } else {
                while offset<source.size {
                    let rows = self.chunks.extents(source.id,offset,source.size).await?;
                    (!rows.is_empty()).then_some(()).context("object mapping is incomplete")?;
                    for row in rows {
                        (row.offset_bytes==offset).then_some(()).context("object mapping has gaps or overlaps")?;
                        let data = self.extent_bytes(&row).await?;
                        offset += i64::from(row.length);
                        yield data;
                    }
                }
                (offset==source.size).then_some(()).context("object mapping length mismatch")?;
            }
        }
    }

    pub(crate) fn replay_payload<'a>(
        &'a self,
        destination: Id,
        sources: &'a [StoredStream],
        range: std::ops::Range<u64>,
    ) -> futures_util::stream::BoxStream<'a, Result<Bytes>> {
        Box::pin(async_stream::try_stream! {
            let mut base = 0u64;
            let mut emitted = 0u64;
            let mut renewed = tokio::time::Instant::now();
            self.renew_write_lease(destination).await?;
            for source in sources {
                let end = base.checked_add(source.size as u64).context("replay size overflow")?;
                if range.start >= end { base = end; continue; }
                if range.end <= base { break; }
                let mut position = range.start.saturating_sub(base) as i64;
                let until = (range.end.min(end) - base) as i64;
                let mut cursor = if source.payload_type == "raw" || position == 0 { position }
                    else { self.chunks.read_start(source.id, position).await?.context("replay source mapping is missing")? };
                while position < until {
                    self.ensure_mode()?;
                    if renewed.elapsed() >= std::time::Duration::from_secs(5) {
                        self.renew_write_lease(destination).await?;
                        renewed = tokio::time::Instant::now();
                    }
                    let before = position;
                    if source.payload_type == "raw" {
                        for row in self.raw.page(source.id, position, until).await? {
                            if row.offset_bytes + row.length <= position { continue; }
                            row.validate()?;
                            (row.offset_bytes <= position).then_some(()).context("replay source mapping has a gap")?;
                            let end = (row.offset_bytes + row.length).min(until);
                            let mut reader = self.storage.raw_reader(&row.source, false).await?;
                            reader.file.seek(std::io::SeekFrom::Start((row.source_offset + position - row.offset_bytes) as u64)).await?;
                            while position < end {
                                let mut bytes = vec![0; (end - position).min(256 * 1024) as usize];
                                let count = reader.file.read(&mut bytes).await?;
                                (count > 0).then_some(()).context("replay raw source was truncated")?;
                                bytes.truncate(count);
                                position += count as i64;
                                emitted += count as u64;
                                reader.file.account_read(&self.storage, count as u64);
                                if renewed.elapsed() >= std::time::Duration::from_secs(5) {
                                    self.renew_write_lease(destination).await?;
                                    renewed = tokio::time::Instant::now();
                                }
                                yield Bytes::from(bytes);
                            }
                            if position == until { break; }
                        }
                    } else {
                        for row in self.chunks.extents(source.id, cursor, until).await? {
                            (row.offset_bytes <= position && row.offset_bytes + i64::from(row.length) > position)
                                .then_some(()).context("replay source mapping has a gap")?;
                            let bytes = self.extent_bytes(&row).await?;
                            let skip = (position - row.offset_bytes) as usize;
                            let count = (bytes.len() - skip).min((until - position) as usize);
                            position += count as i64;
                            emitted += count as u64;
                            cursor = row.offset_bytes + i64::from(row.length);
                            yield bytes.slice(skip..skip + count);
                            if position == until { break; }
                        }
                    }
                    (position > before).then_some(()).context("replay source is incomplete")?;
                }
                base = end;
            }
            (emitted == range.end - range.start).then_some(()).context("replay length mismatch")?;
        })
    }

    async fn copy_payload(
        &self,
        destination: Id,
        storage: Uuid,
        revision: Option<i64>,
        source: &StoredStream,
    ) -> Result<()> {
        use md5::{Digest, Md5};
        let mut input = Box::pin(self.payload_bytes(source));
        let mut offset = 0;
        if let Some(revision) = revision {
            let (id, location) = self.raw.begin(destination, storage, revision).await?;
            let mut writer = self
                .storage
                .raw_replay_writer(storage, id, location, source.size, &[])
                .await?;
            let mut digest = Md5::new();
            while let Some(data) = input.next().await {
                let data = data?;
                self.renew_write_lease(destination).await?;
                digest.update(&data);
                writer.write(&data).await?;
                offset += data.len() as i64;
            }
            ensure!(offset == source.size, "copy length mismatch");
            let hash = writer
                .finish_replay(offset as u64, |range| {
                    self.replay_payload(destination, std::slice::from_ref(source), range)
                })
                .await?;
            self.raw
                .finish(destination, id, offset, &digest.finalize())
                .await?;
            self.deduplicate_raw(destination, storage, id, hash).await?;
        } else {
            let mut window = Vec::with_capacity(MAX);
            while let Some(data) = input.next().await {
                let data = data?;
                let mut rest = data.as_ref();
                while !rest.is_empty() {
                    let count = (MAX - window.len()).min(rest.len());
                    window.extend_from_slice(&rest[..count]);
                    rest = &rest[count..];
                    if window.len() == MAX {
                        let count = crate::content::codec::cut(&window);
                        self.put_chunk(destination, offset, window[..count].to_vec(), false)
                            .await?;
                        offset += count as i64;
                        window.drain(..count);
                    }
                }
            }
            while !window.is_empty() {
                let count = crate::content::codec::cut(&window);
                self.put_chunk(destination, offset, window[..count].to_vec(), false)
                    .await?;
                offset += count as i64;
                window.drain(..count);
            }
            ensure!(offset == source.size, "copy length mismatch");
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
            let _gate = self.storage.source_gate.read().await;
            let source = self.chunks.fragment(id).await?;
            let data = self
                .storage
                .read_fragment(source.location_id, &source)
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
    pub async fn body(
        self: &Arc<Self>,
        stream: StoredStream,
        pin: Active,
        span: std::ops::Range<i64>,
        permits: (OwnedSemaphorePermit, OwnedSemaphorePermit),
        range: bool,
        capture: Option<Arc<Mutex<super::responses::Mapping>>>,
    ) -> Result<StreamingBlob> {
        if stream.payload_type == "raw" {
            return self.raw_body(stream, pin, span, permits).await;
        }
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
        Ok(StreamingBlob::wrap(futures_util::stream::poll_fn(
            move |cx| futures_util::Stream::poll_next(body.lock().unwrap().as_mut(), cx),
        )))
    }
}
