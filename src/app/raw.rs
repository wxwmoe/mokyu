use super::{Active, App, StoredStream};
use crate::model::Id;
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt};
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
                        self.processor_node()
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
        use crate::{
            content::raw_pages::PAGE,
            storage::{raw_segments as segments, read_buffer},
        };
        let mut ranges = Vec::new();
        let mut cursor = span.start;
        let mut prefetched = super::reads::raw(stream.id, stream.payload_revision);
        while cursor < span.end {
            let rows = if let Some(row) = prefetched.take() {
                vec![row]
            } else {
                self.raw.page(stream.id, cursor, span.end).await?
            };
            let previous = cursor;
            for row in rows {
                row.validate()?;
                let end = row
                    .offset_bytes
                    .checked_add(row.length)
                    .context("raw mapping overflow")?;
                if end <= cursor || row.offset_bytes >= span.end {
                    continue;
                }
                ensure!(
                    row.offset_bytes <= cursor && ranges.len() < 10000,
                    "invalid raw extent sequence"
                );
                let mode = self
                    .storage
                    .profile(row.source.storage_id)?
                    .read_integrity
                    .clone();
                cursor = end.min(span.end);
                ranges.push((
                    row,
                    crate::storage::ReadContext::integrity(&mode).to_owned(),
                ));
            }
            ensure!(cursor > previous, "raw mapping is incomplete");
        }
        let capture = capture.filter(|capture| {
            let mut mapping = capture.lock().unwrap();
            for (row, _) in &ranges {
                mapping.record_raw(row, stream.payload_revision);
            }
            span == (0..stream.size) && mapping.signature().is_some()
        });
        let idle = Duration::from_secs(crate::config::seconds(
            &self.config.requests.read_idle_timeout,
        )?);
        let recovery = crate::storage::read_retry::Recovery::new(idle);
        let first = if let Some((first, _)) = ranges.first() {
            Some(
                pin.with_read_lease(self.storage.raw_reader(&first.source, false))
                    .await?,
            )
        } else {
            None
        };
        let app = self.clone();
        let body = async_stream::try_stream! {
            let _pin=pin;let (_reader,slot)=permits;let spare=read_buffer::Spares::new(app.slots.clone());
            let slot=Arc::new(slot);let primary=Arc::new(tokio::sync::Semaphore::new(read_buffer::UNITS));
            let retain:segments::Retain=Arc::new(move |n| {
                let primary=primary.clone();let slot=slot.clone();let spare=spare.clone();
                Box::pin(async move {Ok(Box::new(read_buffer::reserve(&primary,&slot,Some(&spare),n).await?) as segments::Permit)})
            });
            let mut first=first;
            #[cfg(feature="fault-injection")]
            crate::faults::point("read-body").await;
            let mut demand=crate::status::access::DemandRead::default();
            for (row,mode) in ranges {
                app.ensure_mode()?;_pin.renew_read_lease().await?;
                let physical=(row.source_offset+span.start.max(row.offset_bytes)-row.offset_bytes) as u64
                    ..(row.source_offset+span.end.min(row.offset_bytes+row.length)-row.offset_bytes) as u64;
                let complete=physical==(0..row.source.size as u64);
                let recovery=recovery.configured(app.storage.profile(row.source.storage_id)?.max_retries);
                let mut reader=if let Some(first)=first.take(){first}else{
                    _pin.with_read_lease(app.storage.raw_reader(&row.source,false)).await?
                };
                let mut remote=reader.range_source();let mut partial=remote.as_ref().and_then(|_| app.storage.partial(&row.source));
                let from_backend=remote.is_some() || (!row.source.pending_upload && matches!(&reader.file,crate::storage::raw::RawFile::Local(_)));
                let mut check=if mode=="background" && complete {Some(app.read_checks.raw(&row.source,remote.is_some()))} else{None};
                if let Some(capture)=&capture {
                    let evidence=if mode=="off" || (mode=="background" && !complete) {Some(app.read_checks.unchecked())} else {check.as_ref().map(|v|v.evidence.clone())};
                    if let Some(e)=evidence {capture.lock().unwrap().record_check(e);}
                }
                let mut digest=capture.as_ref().filter(|_|mode=="inline" && complete).map(|_|Md5::new());
                let mut offset=physical.start;
                while offset<physical.end {
                    let end=if (mode=="inline" || partial.is_some()) && physical.end-offset > 128*1024*1024 {
                        ((offset/(PAGE as u64))+128)*(PAGE as u64)
                    }else{physical.end};
                    let span=offset..end;
                    let strict=mode=="inline";
                    let proof=if strict || partial.is_some() {
                        let first=(offset/PAGE as u64) as i64;let count=((end-1)/PAGE as u64-first as u64+1) as usize;
                        let hashes=app.raw.page_hashes(row.source.id,row.source.size,first,count).await?;
                        hashes.map(|bytes|segments::Proof::new(&span,bytes,app.storage.cpu.clone()).map(Arc::new)).transpose()?
                    }else{None};
                    if strict && proof.is_none(){Err(crate::model::Error::SourceUnavailable)?;}
                    let cached=matches!(&reader.file,crate::storage::raw::RawFile::Cached{..});let mut rejected=false;
                    let mut output=if let Some(remote)=&remote {
                        let io=app.storage.raw_range_io(row.source.storage_id);
                        if let Some(cache)=&partial {cache.clone().stream(remote.clone(),span,strict,proof,app.budget.storage_read_slots,io,retain.clone(),recovery.clone())}
                        else {remote.clone().stream(span,proof,app.budget.storage_read_slots,io,retain.clone(),recovery.clone()).map_ok(|v|(v,true)).boxed()}
                    }else{
                        segments::local(&mut reader.file,span,row.source.size as u64,proof,retain.clone()).map_ok(move |v|(v,from_backend)).boxed()
                    };
                    while let Some(bytes)=tokio::time::timeout(idle,_pin.with_read_lease(async {Ok(output.next().await)})).await?? {
                        let (bytes,from_backend)=match bytes {Err(_) if cached=>{rejected=true;break},other=>other?};let count=bytes.len();offset+=count as u64;
                        if offset>end { Err(crate::content::codec::IntegrityError::Length)?; }
                        if from_backend { app.storage.backend_read_bytes.fetch_add(count as u64,std::sync::atomic::Ordering::Relaxed); }
                        else { app.storage.cache_hit_bytes.fetch_add(count as u64,std::sync::atomic::Ordering::Relaxed); }
                        if let Some(digest)=&mut digest {digest.update(&bytes);}
                        if offset==physical.end && let Some(digest)=digest.take() {
                            if Some(digest.finalize().as_slice())!=row.source.digest.as_deref() { Err(crate::content::codec::IntegrityError::Hash)?; }
                        }
                        if let Some(check)=&mut check {check.feed(bytes.clone(),count);}
                        if offset==physical.end && let Some(capture)=&capture {capture.lock().unwrap().finish_source();}
                        app.storage.access.demand(&mut demand,crate::model::tiering::Source::Raw(row.source.location_id),count,from_backend);
                        yield bytes;
                    }
                    drop(output);
                    if rejected {
                        reader=_pin.with_read_lease(app.storage.raw_reader_uncached(&row.source,false)).await?;
                        app.storage.reject_raw_cache(row.source.location_id).await;
                        remote=reader.range_source();partial=remote.as_ref().and_then(|_|app.storage.partial(&row.source));
                        continue;
                    }
                    if offset!=end { Err(crate::content::codec::IntegrityError::Length)?; }
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
