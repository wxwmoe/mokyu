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
    loop {
        if app.ensure_mode().is_ok() && app.storage.buffered_writes.load(Ordering::Acquire) {
            match crate::storage::resource_gate::PRIORITY
                .scope(
                    crate::storage::resource_gate::MAINTENANCE,
                    app.sync_raw_batch(None, false, &mut after),
                )
                .await
            {
                Ok(true) => continue,
                Ok(false) => after = Id::default(),
                Err(error) => tracing::warn!(%error, "buffered file sync will retry"),
            }
        }
        tokio::select! {
            _ = app.storage.wake_sync.notified() => {}
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }
}

impl App {
    pub(crate) async fn sync_raw_stream(&self, stream: Id) -> Result<()> {
        if !self.storage.buffered_writes.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut after = Id::default();
        loop {
            let rows = self.raw.buffered(after, None, Some(stream)).await?;
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
        let rows = self.raw.buffered(*after, before, None).await?;
        let found = !rows.is_empty();
        for source in rows {
            // Retry failures on the next scan without blocking later files.
            *after = source.id;
            if let Err(error) = self.sync_raw_source(&source, recovery).await {
                if recovery {
                    let code = match error.downcast_ref::<IntegrityError>() {
                        Some(IntegrityError::RemoteMissing) => Some("remote_missing"),
                        Some(IntegrityError::Length) => Some("length_mismatch"),
                        Some(IntegrityError::Hash) => Some("hash_mismatch"),
                        _ => error
                            .downcast_ref::<std::io::Error>()
                            .filter(|e| e.kind() == std::io::ErrorKind::NotFound)
                            .map(|_| "remote_missing"),
                    };
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
