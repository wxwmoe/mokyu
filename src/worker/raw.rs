use crate::{app::App, content::codec::IntegrityError, model::raw::RawProcessing};
use anyhow::{Result, ensure};
use md5::{Digest, Md5};
use std::{sync::Arc, sync::atomic::Ordering, time::Duration};
use uuid::Uuid;

pub(crate) async fn run(app: Arc<App>) -> Result<()> {
    loop {
        if app.ensure_mode().is_ok()
            && !app.maintenance.load(Ordering::Acquire)
            && !app.draining.load(Ordering::Acquire)
        {
            match app
                .raw
                .claim_processing(Uuid::new_v4(), processor(&app))
                .await
            {
                Ok(Some(work)) => {
                    let result = crate::storage::resource_gate::PRIORITY
                        .scope(
                            crate::storage::resource_gate::MAINTENANCE,
                            execute(&app, &work),
                        )
                        .await;
                    let retry = match result {
                        Ok(true) => {
                            app.wake_gc.notify_one();
                            Ok(())
                        }
                        Ok(false) => app.raw.retry_processing(&work, None).await,
                        Err(error) => {
                            tracing::warn!(stream=%work.stream, source=%work.source, %error, "raw processing will retry");
                            app.raw
                                .retry_processing(&work, Some(&error.to_string()))
                                .await
                        }
                    };
                    if let Err(error) = retry {
                        tracing::warn!(stream=%work.stream, %error, "raw processing progress will retry");
                    }
                    continue;
                }
                Ok(None) => {}
                Err(error) => tracing::warn!(%error, "raw processing scan will retry"),
            }
        }
        // Task notifications belong to the dispatcher; raw work has its own queue.
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn execute(app: &App, work: &RawProcessing) -> Result<bool> {
    let processing = process(app, work);
    if app.role == "single" {
        return processing.await;
    }
    let mut tick = tokio::time::interval(Duration::from_secs(30));
    tick.tick().await;
    tokio::select! {
        result = processing => result,
        result = async {
            loop {
                tick.tick().await;
                ensure!(app.raw.renew_processing(work, processor(app)).await?, crate::model::Error::OperationAborted);
            }
        } => result,
    }
}

fn processor(app: &App) -> crate::model::raw::Processor<'_> {
    app.node
        .get()
        .map(|node| (node, app.role.as_str(), &app.secrets.credential_key))
}

async fn process(app: &App, work: &RawProcessing) -> Result<bool> {
    let Ok(_slot) = app.slots.try_acquire() else {
        return Ok(false);
    };
    let Ok(_read) = app.reads.try_acquire() else {
        return Ok(false);
    };
    let (stream, pin) = match app.current(work.bucket, &work.object_key).await {
        Ok(value) => value,
        Err(error)
            if error
                .downcast_ref::<s3s::S3Error>()
                .is_some_and(|error| error.code() == &s3s::S3ErrorCode::NoSuchKey) =>
        {
            app.raw.cancel_processing(work).await?;
            return Ok(true);
        }
        Err(error) => return Err(error),
    };
    if stream.id != work.stream || stream.payload_revision != work.payload_revision {
        app.raw.cancel_processing(work).await?;
        return Ok(true);
    }
    pin.with_read_lease(async {
        let ranges = app.raw.page(stream.id, 0, stream.size).await?;
        ensure!(ranges.len() == 1, IntegrityError::RawMetadata);
        let range = &ranges[0];
        range.validate()?;
        ensure!(
            range.source.id == work.source
                && range.offset_bytes == 0
                && range.source_offset == 0
                && range.length == stream.size
                && range.source.size == stream.size,
            IntegrityError::RawMetadata
        );
        let source = &range.source;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("raw-processing-before-hash").await;
        let hash: [u8; 32] = if let Some(hash) = work
            .content_hash
            .as_ref()
            .filter(|_| work.source_revision == Some(source.revision))
        {
            hash.as_slice()
                .try_into()
                .map_err(|_| IntegrityError::RawMetadata)?
        } else {
            let Ok(_cpu) = app.storage.cpu.try_acquire() else {
                return Ok(false);
            };
            let hash = async {
                let mut reader = app.storage.raw_reader_uncached(source, false).await?;
                let mut hash = blake3::Hasher::new();
                let mut digest = Md5::new();
                let mut buffer = vec![0; 256 * 1024];
                let mut size = 0i64;
                loop {
                    app.ensure_mode()?;
                    let count = reader.file.read(&mut buffer).await?;
                    if count == 0 {
                        break;
                    }
                    size += count as i64;
                    ensure!(size <= source.size, IntegrityError::Length);
                    hash.update(&buffer[..count]);
                    digest.update(&buffer[..count]);
                }
                ensure!(size == source.size, IntegrityError::Length);
                ensure!(
                    Some(digest.finalize().as_slice()) == source.digest.as_deref(),
                    IntegrityError::Hash
                );
                app.storage
                    .backend_read_bytes
                    .fetch_add(size as u64, Ordering::Relaxed);
                Ok::<_, anyhow::Error>(*hash.finalize().as_bytes())
            }
            .await?;
            app.raw
                .save_processing_hash(work, source.revision, &hash)
                .await?;
            hash
        };
        #[cfg(feature = "fault-injection")]
        crate::faults::point("raw-processing-before-switch").await;
        let mutex = app.content_lock(source.storage_id, hash);
        let _hash = mutex.lock().await;
        let _coord = app.coord.lock().await;
        app.storage_writable()?;
        if app.role == "single"
            && app
                .active
                .lock()
                .unwrap()
                .get(&stream.id)
                .copied()
                .unwrap_or(0)
                > 1
        {
            return Ok(false);
        }
        app.raw
            .finish_processing(work, source, &hash, pin.read_lease_id(), processor(app))
            .await
    })
    .await
}
