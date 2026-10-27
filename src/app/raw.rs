use super::{Active, App, StoredStream};
use crate::model::Id;
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures_util::StreamExt;
use md5::{Digest, Md5};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::OwnedSemaphorePermit;
use uuid::Uuid;

impl App {
    pub(crate) async fn finish_raw_received(
        &self,
        stream: Id,
        source: Id,
        storage: Uuid,
        size: i64,
        digest: &[u8],
        written: crate::storage::raw_upload::Written,
    ) -> Result<()> {
        use crate::storage::raw_upload::Written;
        match written {
            Written::Chunks(mut file) => {
                use crate::content::codec::{self, MAX};
                use tokio::io::AsyncReadExt;
                let encoding = self.raw.inline_conversion(stream, source).await?;
                self.upload_cache.disable(stream).await?;
                #[cfg(feature = "fault-injection")]
                crate::faults::point("cdc-inline-fallback").await;
                file.rewind().await?;
                let mut window = vec![0; MAX];
                let mut filled = 0usize;
                let mut offset = 0i64;
                let mut eof = false;
                while !eof || filled != 0 {
                    if !eof && filled < MAX {
                        let count = file
                            .file
                            .as_mut()
                            .unwrap()
                            .read(&mut window[filled..])
                            .await?;
                        eof = count == 0;
                        filled += count;
                        if !eof && filled < MAX {
                            continue;
                        }
                    }
                    if filled == 0 {
                        break;
                    }
                    let length = codec::cut(&window[..filled]);
                    self.put_chunk_with(
                        stream,
                        offset,
                        window[..length].to_vec(),
                        true,
                        Some(&encoding),
                    )
                    .await?;
                    offset += length as i64;
                    window.copy_within(length..filled, 0);
                    filled -= length;
                }
                ensure!(
                    offset == size,
                    crate::content::codec::IntegrityError::Length
                );
            }
            Written::Final(hash, fill) => {
                self.raw.finish(stream, source, size, digest).await?;
                if let Some(fill) = fill {
                    let _ = fill.send(());
                }
                self.deduplicate_raw(stream, storage, source, hash).await?;
            }
            Written::Cached {
                revision,
                deduplicate,
                background,
                boot,
            } => {
                let _coord = self.coord.lock().await;
                self.raw
                    .stage_upload(
                        stream,
                        source,
                        size,
                        digest,
                        revision,
                        deduplicate,
                        background,
                        boot,
                        self.node
                            .get()
                            .map(|node| (node, self.role.as_str(), &self.secrets.credential_key)),
                    )
                    .await?;
            }
            Written::Reused => {}
        }
        Ok(())
    }

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
        capture: Option<Arc<Mutex<super::responses::Mapping>>>,
    ) -> Result<s3s::dto::StreamingBlob> {
        let mut ranges = Vec::new();
        let mut cursor = 0;
        let mut first = None;
        let mut prefetched = super::reads::raw(stream.id, stream.payload_revision);
        loop {
            let rows = if let Some(row) = prefetched.take() {
                vec![row]
            } else {
                self.raw.page(stream.id, cursor, stream.size).await?
            };
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
                let mode = self
                    .storage
                    .profile(row.source.storage_id)?
                    .read_integrity
                    .clone();
                let reader = pin
                    .with_read_lease(self.storage.raw_reader(&row.source, mode == "inline"))
                    .await?;
                if row.offset_bytes == 0 && row.length == stream.size {
                    first = Some(reader);
                }
                cursor += row.length;
                ranges.push((row, mode));
            }
            if cursor == stream.size {
                break;
            }
            ensure!(
                cursor > before && cursor < stream.size,
                "raw mapping length mismatch"
            );
        }
        let capture = capture.filter(|capture| {
            let mut mapping = capture.lock().unwrap();
            for (row, _) in &ranges {
                mapping.record_raw(row, stream.payload_revision);
            }
            mapping.signature().is_some() && span == (0..stream.size)
        });
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
            for (row, mode) in ranges {
                let start = span.start.max(row.offset_bytes);
                let end = span.end.min(row.offset_bytes+row.length);
                if start>=end { continue; }
                app.ensure_mode()?;
                _pin.renew_read_lease().await?;
                let mut reader = if let Some(reader)=first.take() {reader} else {app.storage.raw_reader(&row.source,false).await?};
                reader.file.seek(std::io::SeekFrom::Start((row.source_offset+start-row.offset_bytes) as u64)).await?;
                let mut remaining = end-start;
                let complete = row.source_offset == 0 && start == row.offset_bytes && end-start == row.source.size;
                let mut check = if mode == "background" && complete { Some(app.read_checks.raw(&row.source, matches!(&reader.file, crate::storage::raw::RawFile::Remote(_)))) } else { None };
                if mode == "background" && !complete { app.read_checks.raw_source(row.source.clone()); }
                let evidence = if mode == "off" { Some(app.read_checks.unchecked()) } else { check.as_ref().map(|check| check.evidence.clone()) };
                if let Some(capture) = &capture && let Some(evidence) = evidence { capture.lock().unwrap().record_check(evidence); }
                let mut digest = capture.as_ref().filter(|_| mode == "inline").map(|_| Md5::new());
                while remaining>0 {
                    let mut buffer = vec![0;remaining.min(256*1024) as usize];
                    let count = tokio::time::timeout(idle,_pin.with_read_lease(reader.file.read(&mut buffer))).await??;
                    (count>0).then_some(()).context("raw source was truncated")?;
                    buffer.truncate(count);
                    buffer.shrink_to_fit();
                    remaining -= count as i64;
                    reader.file.account_read(&app.storage, count as u64);
                    if let Some(digest)=&mut digest { digest.update(&buffer); }
                    if remaining==0 && let Some(digest)=digest.take() {
                        (digest.finalize().as_slice()==row.source.digest.as_deref().unwrap()).then_some(()).context(crate::content::codec::IntegrityError::Hash)?;
                    }
                    let retained = buffer.capacity();
                    let bytes = Bytes::from(buffer);
                    if let Some(check) = &mut check { check.feed(bytes.clone(), retained); }
                    if remaining == 0 && let Some(capture) = &capture { capture.lock().unwrap().finish_source(); }
                    yield bytes;
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
