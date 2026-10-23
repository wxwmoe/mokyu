use crate::{
    app::App,
    authorization::Permit,
    content::codec::{self, Chunk, IntegrityError, MAX},
    model::{ChunkCreate, Id, Mapping, raw::Conversion, task::Execution},
};
use anyhow::{Result, ensure};
use bytes::Bytes;
use md5::{Digest, Md5};
use serde_json::Value;
use std::{sync::Arc, time::Duration};

impl App {
    pub(crate) async fn convert_raw(&self, task: Execution, detail: Value) -> Result<bool> {
        let job: Conversion = serde_json::from_value(detail)?;
        job.encoding.validate()?;
        let Ok(_slot) = self.slots.try_acquire() else {
            tokio::time::sleep(Duration::from_millis(100)).await;
            return Ok(false);
        };
        let (stream, pin) = match self.current(job.bucket, &job.object_key).await {
            Ok(value) => value,
            Err(error)
                if error
                    .downcast_ref::<s3s::S3Error>()
                    .is_some_and(|e| e.code() == &s3s::S3ErrorCode::NoSuchKey) =>
            {
                self.task_control.complete(task).await?;
                self.wake_raw_uploads.notify_one();
                return Ok(true);
            }
            Err(error) => return Err(error),
        };
        if stream.id != job.stream
            || stream.payload_revision != job.payload_revision
            || stream.payload_type != "raw"
        {
            self.task_control.complete(task).await?;
            self.wake_raw_uploads.notify_one();
            return Ok(true);
        }
        let ranges = self.raw.page(stream.id, 0, stream.size).await?;
        ensure!(ranges.len() == 1, IntegrityError::RawMetadata);
        let range = &ranges[0];
        range.validate()?;
        ensure!(
            range.source.id == job.source
                && range.offset_bytes == 0
                && range.source_offset == 0
                && range.length == stream.size
                && range.source.size == stream.size,
            IntegrityError::RawMetadata
        );
        let source = &range.source;
        let (candidate, _candidate_pin) = self
            .new_stream(
                &Permit::system(),
                job.bucket,
                &job.object_key,
                "object",
                stream.metadata.clone(),
                stream.public_read,
                false,
                None,
                Some((task.id, task.token)),
            )
            .await?;
        let result = pin
            .with_read_lease(async {
                #[cfg(feature = "fault-injection")]
                crate::faults::point("cdc-before-read").await;
                let maximum = if job.encoding.pack_enabled {
                    crate::config::bytes(&job.encoding.pack_max_size)? as usize
                } else {
                    MAX
                };
                let memory = self
                    .storage
                    .pack_work(
                        maximum
                            + 12
                            + (maximum / codec::MIN + 1) * crate::content::pack::MEMBER_SIZE,
                    )
                    .await?;
                let read = self.reads.acquire().await?;
                let mut reader = self.storage.raw_reader_uncached(source, false).await?;
                let mut window = vec![0; MAX];
                let mut filled = 0usize;
                let mut eof = false;
                let mut offset = 0i64;
                let mut digest = Md5::new();
                let mut rows = Vec::new();
                let mut data = Vec::new();
                let mut bytes = 0usize;
                let content_type = stream.metadata.get("content-type").and_then(Value::as_str);
                while !eof || filled != 0 {
                    self.storage_writable()?;
                    if !eof && filled < MAX {
                        let count = reader.file.read(&mut window[filled..]).await?;
                        eof = count == 0;
                        digest.update(&window[filled..filled + count]);
                        filled += count;
                        ensure!(
                            offset + filled as i64 <= source.size,
                            IntegrityError::Length
                        );
                        if !eof && filled < MAX {
                            continue;
                        }
                    }
                    if filled == 0 {
                        break;
                    }
                    let (length, hash) = {
                        let _cpu = self.storage.cpu.acquire().await?;
                        let length = codec::cut(&window[..filled]);
                        (length, *blake3::hash(&window[..length]).as_bytes())
                    };
                    if bytes + length > maximum
                        || rows.iter().any(|r: &Mapping| r.chunk.hash == hash)
                    {
                        self.convert_batch(
                            task,
                            candidate,
                            &mut rows,
                            &mut data,
                            &memory,
                            content_type,
                            &job,
                        )
                        .await?;
                        bytes = 0;
                    }
                    self.renew_write_lease(candidate).await?;
                    let mutex = self.content_lock(source.storage_id, hash);
                    let (chunk, reused) = {
                        let _hash = mutex.lock().await;
                        let _claim = if self.role == "single" {
                            None
                        } else {
                            Some(
                                self.cluster()?
                                    .claim_chunk(source.storage_id, &hash)
                                    .await?,
                            )
                        };
                        self.chunks
                            .allocate(ChunkCreate {
                                storage: source.storage_id,
                                stream: candidate,
                                hash,
                                raw_size: length as i32,
                                algorithm: job.encoding.algorithm.clone(),
                                key: job.encoding.block_key_id,
                            })
                            .await?
                    };
                    if reused {
                        self.convert_batch(
                            task,
                            candidate,
                            &mut rows,
                            &mut data,
                            &memory,
                            content_type,
                            &job,
                        )
                        .await?;
                        bytes = 0;
                        self.reference(candidate, offset, &chunk).await?;
                    } else {
                        bytes += length;
                        rows.push(Mapping {
                            chunk: chunk.clone(),
                            offset_bytes: offset,
                            pack_id: None,
                        });
                        data.push((chunk, Bytes::copy_from_slice(&window[..length])));
                    }
                    offset += length as i64;
                    window.copy_within(length..filled, 0);
                    filled -= length;
                }
                ensure!(offset == source.size, IntegrityError::Length);
                ensure!(
                    Some(digest.finalize().as_slice()) == source.digest.as_deref(),
                    IntegrityError::Hash
                );
                self.convert_batch(
                    task,
                    candidate,
                    &mut rows,
                    &mut data,
                    &memory,
                    content_type,
                    &job,
                )
                .await?;
                drop(reader);
                drop(read);
                drop(memory);
                self.finish_stream(
                    candidate,
                    stream.size,
                    &stream.etag,
                    stream.checksums.clone(),
                )
                .await?;
                #[cfg(feature = "fault-injection")]
                crate::faults::point("cdc-before-switch").await;
                loop {
                    self.storage_writable()?;
                    ensure!(
                        super::task::fence(self, task.id, task.token).await?,
                        crate::model::Error::OperationAborted
                    );
                    self.renew_write_lease(candidate).await?;
                    let done = {
                        let _coord = self.coord.lock().await;
                        let reading = self.role == "single"
                            && self
                                .active
                                .lock()
                                .unwrap()
                                .get(&stream.id)
                                .copied()
                                .unwrap_or(0)
                                > 1;
                        if reading {
                            false
                        } else {
                            let _cache = self.public_reads.changing();
                            self.raw
                                .finish_conversion(
                                    task,
                                    &job,
                                    source,
                                    candidate,
                                    pin.read_lease_id(),
                                )
                                .await?
                        }
                    };
                    if done {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                #[cfg(feature = "fault-injection")]
                crate::faults::point("cdc-after-switch").await;
                Ok::<_, anyhow::Error>(())
            })
            .await;
        if result.is_err() {
            self.objects.abandon_migration(candidate).await?;
        }
        result?;
        self.wake_raw_uploads.notify_one();
        self.wake_gc.notify_one();
        Ok(true)
    }

    async fn convert_batch(
        &self,
        task: Execution,
        candidate: Id,
        rows: &mut Vec<Mapping>,
        data: &mut Vec<(Chunk, Bytes)>,
        memory: &Arc<tokio::sync::OwnedSemaphorePermit>,
        content_type: Option<&str>,
        job: &Conversion,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        ensure!(
            super::task::fence(self, task.id, task.token).await?,
            crate::model::Error::OperationAborted
        );
        let outputs = self
            .pack_data(
                task,
                rows,
                data,
                memory,
                content_type,
                &job.object_key,
                job.encoding.pack_enabled,
                Some(&job.encoding),
            )
            .await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("cdc-batch-stored").await;
        let _coord = self.coord.lock().await;
        self.storage_writable()?;
        self.raw
            .commit_conversion_batch(task, candidate, rows, &outputs)
            .await?;
        rows.clear();
        data.clear();
        Ok(())
    }
}
