use super::{Active, App, StoredStream};
use crate::model::Id;
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures_util::StreamExt;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::OwnedSemaphorePermit;
use uuid::Uuid;

impl App {
    pub(crate) async fn deduplicate_raw(
        &self,
        stream: Id,
        storage: Uuid,
        source: Id,
        hash: Option<[u8; 32]>,
    ) -> Result<()> {
        if let Some(hash) = hash {
            let lock = self.content_lock(storage, hash);
            let _guard = lock.lock().await;
            self.raw.deduplicate(stream, source, &hash).await?;
        }
        Ok(())
    }
    pub(crate) async fn raw_body(
        self: &Arc<Self>,
        stream: StoredStream,
        pin: Active,
        span: std::ops::Range<i64>,
        permits: (OwnedSemaphorePermit, OwnedSemaphorePermit),
    ) -> Result<s3s::dto::StreamingBlob> {
        let mut ranges = Vec::new();
        let mut cursor = 0;
        let mut first = None;
        loop {
            let rows = self.raw.page(stream.id, cursor, stream.size).await?;
            ensure!(!rows.is_empty(), "raw mapping is incomplete");
            let before = cursor;
            for row in rows {
                if row.offset_bytes + row.length <= cursor && stream.size != 0 {
                    continue;
                }
                row.validate()?;
                ensure!(
                    row.offset_bytes == cursor,
                    "raw mapping has gaps or overlaps"
                );
                ensure!(
                    ranges.len() < 10000,
                    "raw mapping exceeds the multipart limit"
                );
                let reader = pin
                    .with_read_lease(self.storage.raw_reader(&row.source, true))
                    .await?;
                if row.offset_bytes == 0 && row.length == stream.size {
                    first = Some(reader);
                }
                cursor += row.length;
                ranges.push(row);
            }
            if cursor == stream.size {
                break;
            }
            ensure!(
                cursor > before && cursor < stream.size,
                "raw mapping length mismatch"
            );
        }
        let app = self.clone();
        let idle = Duration::from_secs(crate::config::seconds(
            &self.config.requests.read_idle_timeout,
        )?);
        let body = async_stream::try_stream! {
            let _pin = pin;
            let _permits = permits;
            #[cfg(feature="fault-injection")]
            crate::faults::point("read-body").await;
            _pin.renew_read_lease().await?;
            for row in ranges {
                let start = span.start.max(row.offset_bytes);
                let end = span.end.min(row.offset_bytes+row.length);
                if start>=end { continue; }
                app.ensure_mode()?;
                _pin.renew_read_lease().await?;
                let mut reader = if let Some(reader)=first.take() {reader} else {app.storage.raw_reader(&row.source,false).await?};
                reader.file.seek(std::io::SeekFrom::Start((row.source_offset+start-row.offset_bytes) as u64)).await?;
                let mut remaining = end-start;
                while remaining>0 {
                    let mut buffer = vec![0;remaining.min(256*1024) as usize];
                    let count = tokio::time::timeout(idle,_pin.with_read_lease(reader.file.read(&mut buffer))).await??;
                    (count>0).then_some(()).context("raw source was truncated")?;
                    buffer.truncate(count);
                    remaining -= count as i64;
                    reader.file.account_read(&app.storage, count as u64);
                    yield Bytes::from(buffer);
                }
            }
        };
        let body =
            Mutex::new(Box::pin(body.map(|result: Result<Bytes>| {
                result.map_err(std::io::Error::other)
            })));
        Ok(s3s::dto::StreamingBlob::wrap(
            futures_util::stream::poll_fn(move |cx| {
                futures_util::Stream::poll_next(body.lock().unwrap().as_mut(), cx)
            }),
        ))
    }
}
