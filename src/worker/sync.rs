use crate::{
    app::App,
    content::codec::IntegrityError,
    model::{Id, raw::RawSource},
};
use anyhow::Result;
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

pub(crate) async fn run(app: Arc<App>) -> Result<()> {
    if app.role != "single" {
        return std::future::pending().await;
    }
    let mut after = Id::default();
    let mut chunks_after = Id::default();
    loop {
        let mut progressed = false;
        if app.ensure_mode().is_ok() && app.storage.buffered_writes.load(Ordering::Acquire) {
            match crate::storage::resource_gate::PRIORITY
                .scope(
                    crate::storage::resource_gate::MAINTENANCE,
                    app.sync_raw_batch(None, false, &mut after),
                )
                .await
            {
                Ok(true) => progressed = true,
                Ok(false) => after = Id::default(),
                Err(error) => tracing::warn!(%error, "buffered file sync will retry"),
            }
            match crate::storage::resource_gate::PRIORITY
                .scope(
                    crate::storage::resource_gate::MAINTENANCE,
                    app.sync_chunk_batch(None, false, &mut chunks_after),
                )
                .await
            {
                Ok(true) => progressed = true,
                Ok(false) => chunks_after = Id::default(),
                Err(error) => tracing::warn!(%error, "buffered chunk sync will retry"),
            }
        }
        if progressed {
            continue;
        }
        tokio::select! {
            _ = app.storage.wake_sync.notified() => {}
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }
}

impl App {
    pub(crate) async fn sync_stream(&self, stream: Id) -> Result<()> {
        self.sync_raw_stream(stream).await?;
        if !self.storage.buffered_writes.load(Ordering::Acquire) {
            return Ok(());
        }
        self.seal_uploads(stream, true).await?;
        let mut after = Id::default();
        loop {
            let rows = self
                .physical
                .buffered_chunks(after, None, Some(stream), None, false, &[])
                .await?;
            if rows.is_empty() {
                return Ok(());
            }
            for chunk in rows {
                after = chunk.id;
                self.sync_chunk_source(&chunk, false).await?;
            }
        }
    }
    pub(crate) async fn sync_chunk_batch(
        &self,
        before: Option<chrono::DateTime<chrono::Utc>>,
        recovery: bool,
        after: &mut Id,
    ) -> Result<bool> {
        let rows = self
            .physical
            .buffered_chunks(
                *after,
                before,
                None,
                None,
                !recovery && before.is_none(),
                &self.storage.urgent_writes(),
            )
            .await?;
        let found = !rows.is_empty();
        for chunk in rows {
            *after = chunk.id;
            if let Err(error) = self.sync_chunk_source(&chunk, recovery).await {
                if recovery && let Some(code) = sync_failure(&error) {
                    self.physical.synced_chunk(&chunk, Some(code)).await?;
                    tracing::error!(chunk=%chunk.id, code, "buffered chunk is unavailable after restart");
                    continue;
                }
                return Err(error);
            }
        }
        Ok(found)
    }
    async fn sync_chunk_source(
        &self,
        chunk: &crate::content::codec::Chunk,
        verify: bool,
    ) -> Result<()> {
        let wait = async {
            loop {
                let lock = self.operation_locks.get(chunk.location_id);
                let sync = lock.lock().await;
                let pin = {
                    let _gate = self.storage.source_gate.read().await;
                    self.storage.physical_pin(chunk.location_id)
                };
                let rows = self
                    .physical
                    .buffered_chunks(Id::default(), None, None, Some(chunk.id), false, &[])
                    .await?;
                let Some(current) = rows
                    .first()
                    .filter(|current| current.location_id == chunk.location_id)
                else {
                    return Ok(());
                };
                if self.storage.sync_chunk(current, verify).await? {
                    #[cfg(feature = "fault-injection")]
                    crate::faults::point("buffered-chunk-synced").await;
                    self.physical.synced_chunk(current, None).await?;
                    return Ok(());
                }
                // A full cache cannot hold a second copy; let the final uploader drain RAM.
                drop(pin);
                drop(sync);
                self.storage.expedite_writes(chunk.storage_id.unwrap());
                self.wake_tasks.notify_waiters();
                #[cfg(feature = "fault-injection")]
                crate::faults::point("buffered-chunk-before-final").await;
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        };
        tokio::time::timeout(
            Duration::from_secs(crate::config::seconds(
                &self.config.requests.upload_idle_timeout,
            )?),
            wait,
        )
        .await
        .map_err(|_| {
            s3s::s3_error!(
                SlowDown,
                "write memory is waiting for durable storage; retry later"
            )
        })?
    }
    pub(crate) async fn sync_raw_stream(&self, stream: Id) -> Result<()> {
        if !self.storage.buffered_writes.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut after = Id::default();
        loop {
            let rows = self
                .raw
                .buffered(after, None, Some(stream), false, &[])
                .await?;
            if rows.is_empty() {
                return Ok(());
            }
            for source in rows {
                after = source.id;
                self.sync_raw_source(&source, false).await?;
            }
        }
    }

    pub(crate) async fn sync_raw_batch(
        &self,
        before: Option<chrono::DateTime<chrono::Utc>>,
        recovery: bool,
        after: &mut Id,
    ) -> Result<bool> {
        let rows = self
            .raw
            .buffered(
                *after,
                before,
                None,
                !recovery && before.is_none(),
                &self.storage.urgent_writes(),
            )
            .await?;
        let found = !rows.is_empty();
        for source in rows {
            // Retry failures on the next scan without blocking later files.
            *after = source.id;
            if !recovery && before.is_none() && source.medium == "memory" {
                continue;
            }
            if let Err(error) = self.sync_raw_source(&source, recovery).await {
                if recovery {
                    let code = sync_failure(&error);
                    if let Some(code) = code {
                        self.raw.synced(&source, Some(code)).await?;
                        tracing::error!(source=%source.id, storage=%source.storage_id, code, "buffered source is unavailable after restart");
                        continue;
                    }
                }
                return Err(error);
            }
        }
        Ok(found)
    }

    pub(crate) async fn sync_raw_source(&self, source: &RawSource, verify: bool) -> Result<()> {
        let lock = self.operation_locks.get(source.location_id);
        let _sync = lock.lock().await;
        let _pin = {
            let _gate = self.storage.source_gate.read().await;
            self.storage.physical_pin(source.location_id)
        };
        let Some(current) = self.raw.buffered_source(source.id).await? else {
            return Ok(());
        };
        self.storage.sync_raw(&current, verify).await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("buffered-file-synced").await;
        self.raw.synced(&current, None).await?;
        Ok(())
    }
}

fn sync_failure(error: &anyhow::Error) -> Option<&'static str> {
    match error.downcast_ref::<IntegrityError>() {
        Some(value) if crate::model::source::quarantine_code(value.code()) => Some(value.code()),
        _ => error
            .downcast_ref::<std::io::Error>()
            .filter(|e| e.kind() == std::io::ErrorKind::NotFound)
            .map(|_| "remote_missing"),
    }
}
