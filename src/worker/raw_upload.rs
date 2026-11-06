use super::raw::processor;
use crate::{app::App, model::raw::RawUpload};
use anyhow::{Context, Result, ensure};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use uuid::Uuid;

pub(crate) async fn run(app: Arc<App>) -> Result<()> {
    if !matches!(app.role.as_str(), "single" | "data" | "worker" | "node") {
        return std::future::pending().await;
    }
    uploads(app).await
}

async fn uploads(app: Arc<App>) -> Result<()> {
    let mut reconciled = tokio::time::Instant::now();
    let mut after = None;
    loop {
        if app.ensure_mode().is_ok()
            && !app.maintenance.load(Ordering::Acquire)
            && !app.draining.load(Ordering::Acquire)
        {
            if reconciled.elapsed() >= Duration::from_secs(5) {
                if let Err(error) = reconcile(&app, &mut after).await {
                    tracing::warn!(%error,"raw upload pins will be reconciled again");
                }
                if let Err(error) = app.remote_housekeeping().await {
                    tracing::warn!(%error,"native multipart cleanup will retry");
                }
                reconciled = tokio::time::Instant::now();
            }
            let native = match app.transfer_remote_part(None).await {
                Ok(value) => value,
                Err(error) => {
                    tracing::warn!(%error,"native multipart transfer will retry");
                    false
                }
            };
            match tick(&app, None).await {
                Ok(true) => continue,
                Ok(false) if native => continue,
                Ok(false) => {}
                Err(error) => tracing::warn!(%error,"raw upload scan will retry"),
            }
        }
        tokio::select! {
            _=app.wake_raw_uploads.notified()=>{},
            _=tokio::time::sleep(Duration::from_secs(1))=>{},
        }
    }
}

async fn reconcile(app: &App, after: &mut Option<(Uuid, Uuid)>) -> Result<()> {
    let Some((end, pins)) = app.storage.raw_pin_batch(*after) else {
        *after = None;
        return Ok(());
    };
    let ids: Vec<_> = pins.iter().map(|(_, source)| *source).collect();
    #[cfg(feature = "fault-injection")]
    crate::faults::point("raw-pin-scan").await;
    let pending: std::collections::HashSet<_> =
        app.raw.pending_uploads(&ids).await?.into_iter().collect();
    let mut checked = 0;
    for (location, source) in pins {
        *after = (location < end).then_some((location, end));
        if !pending.contains(&source) {
            app.storage
                .release_finished_raw(source, location, Some(app.coord.lock().await))
                .await?;
            checked += 1;
            if checked == 8 {
                break;
            }
        }
    }
    Ok(())
}

pub(crate) async fn tick(app: &App, before: Option<chrono::DateTime<chrono::Utc>>) -> Result<bool> {
    let Ok(_worker) = app.raw_upload_worker.try_lock() else {
        return Ok(false);
    };
    let (work, _pin) = {
        let _coord = if app.role == "single" {
            Some(app.coord.lock().await)
        } else {
            None
        };
        app.storage_writable()?;
        let Some(work) = app
            .raw
            .claim_upload(
                Uuid::new_v4(),
                before,
                processor(app),
                &app.storage.urgent_writes(),
            )
            .await?
        else {
            return Ok(false);
        };
        let _sources = app.storage.source_gate.read().await;
        let pin = app.storage.physical_pin(work.source.location_id);
        (work, pin)
    };
    let priority = if work.background {
        crate::storage::resource_gate::MAINTENANCE
    } else {
        crate::storage::resource_gate::UPLOAD
    };
    if let Err(error) = crate::storage::resource_gate::PRIORITY
        .scope(priority, execute(app, &work))
        .await
    {
        tracing::warn!(source=%work.source.id,%error,"raw upload will retry");
        app.raw
            .retry_upload(&work, Some(&error.to_string()))
            .await?;
    }
    app.storage
        .release_finished_raw(work.source.id, work.source.location_id, None)
        .await?;
    Ok(true)
}

async fn execute(app: &App, work: &RawUpload) -> Result<()> {
    if app.role == "single" {
        return process(app, work).await;
    }
    let mut tick = tokio::time::interval(Duration::from_secs(30));
    tick.tick().await;
    tokio::select! {
        result=process(app,work)=>result,
        result=async {
            loop {
                tick.tick().await;
                ensure!(app.raw.renew_upload(work,processor(app)).await?,crate::model::Error::OperationAborted);
            }
        }=>result,
    }
}

async fn process(app: &App, work: &RawUpload) -> Result<()> {
    {
        let _coord = if app.role == "single" {
            Some(app.coord.lock().await)
        } else {
            None
        };
        if app.raw.abandon_upload(work, processor(app)).await? {
            app.wake_gc.notify_one();
            return Ok(());
        }
    }
    work.source.validate()?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("raw-upload-before-process").await;
    let hash: Option<[u8; 32]> = if let Some(hash) = &work.content_hash {
        Some(
            hash.as_slice()
                .try_into()
                .context("invalid pending raw content hash")?,
        )
    } else if work.deduplicate {
        let mut reader = app.storage.raw_reader_uncached(&work.source, false).await?;
        let _cpu = app.storage.cpu.acquire().await?;
        crate::storage::raw_upload::hash_file(
            &mut reader.file,
            work.source.size as u64,
            work.source.digest.as_deref().unwrap(),
            true,
        )
        .await?
    } else {
        None
    };
    if let Some(hash) = hash {
        let streams = app.raw.upload_streams(work.source.id).await?;
        let lock = app.content_lock(work.source.storage_id, hash);
        let _hash = lock.lock().await;
        let _coord = if app.role == "single" {
            Some(app.coord.lock().await)
        } else {
            None
        };
        app.storage_writable()?;
        let streams: Vec<_> = streams
            .into_iter()
            .filter(|stream| !app.is_active(*stream))
            .collect();
        let _cache = app.public_reads.changing_raw(work.source.id, &streams);
        if app
            .raw
            .reuse_upload(work, &hash, &streams, processor(app))
            .await?
        {
            app.wake_gc.notify_one();
            return Ok(());
        }
    }
    if work.source.pending_upload {
        app.ensure_mode()?;
        app.storage
            .restart_raw_upload(
                &work.source,
                work.token.context("raw upload token is missing")?,
            )
            .await?;
        let mut reader = app.storage.raw_reader_uncached(&work.source, false).await?;
        app.storage
            .send_raw(
                work.source.storage_id,
                work.source.id,
                work.source.location_id,
                &mut reader.file,
                work.source.size as u64,
                work.source.digest.as_deref().unwrap(),
                hash.as_ref(),
                work.token,
            )
            .await?;
    }
    #[cfg(feature = "fault-injection")]
    crate::faults::point("raw-upload-before-commit").await;
    let _coord = if app.role == "single" {
        Some(app.coord.lock().await)
    } else {
        None
    };
    let _cache = app.public_reads.changing_raw(work.source.id, &[]);
    app.raw
        .complete_upload(
            work,
            hash.as_ref().map(|hash| hash.as_slice()),
            processor(app),
        )
        .await?;
    app.wake_gc.notify_one();
    Ok(())
}
