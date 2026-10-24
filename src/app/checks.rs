use super::App;
use crate::{
    content::{
        checks::{self, FAILED, SKIPPED, VERIFIED},
        codec::Chunk,
    },
    model::raw::RawSource,
    storage::Storage,
};
use anyhow::Context;
use bytes::Bytes;
pub(crate) use checks::{Evidence, RawCheck};
use std::sync::{Arc, OnceLock, Weak};

pub(crate) struct Checks {
    storage: Arc<Storage>,
    app: OnceLock<Weak<App>>,
    work: Arc<checks::Checks>,
}
enum Target {
    Chunk(Chunk),
    Raw(RawSource),
}
impl Checks {
    pub fn new(storage: Arc<Storage>) -> Arc<Self> {
        Arc::new(Self {
            work: checks::Checks::new(storage.cpu.clone()),
            storage,
            app: OnceLock::new(),
        })
    }
    pub fn bind(&self, app: Weak<App>) {
        let _ = self.app.set(app);
    }
    pub fn snapshot(&self) -> serde_json::Value {
        self.work.snapshot()
    }
    pub fn unchecked(&self) -> Arc<Evidence> {
        self.work.unchecked()
    }
    pub fn chunk(self: &Arc<Self>, chunk: &Chunk, bytes: Bytes, retained: usize) -> Arc<Evidence> {
        let checks = self.clone();
        let chunk = chunk.clone();
        self.work.chunk(
            chunk.hash.clone(),
            bytes,
            retained,
            move |valid, bytes| async move {
                if valid {
                    checks.storage.fill_read(&chunk, bytes, None);
                } else {
                    checks.failure(Target::Chunk(chunk)).await;
                }
            },
        )
    }
    pub fn raw(self: &Arc<Self>, source: &RawSource) -> RawCheck {
        let checks = self.clone();
        let source = source.clone();
        self.work.raw(
            source.size as u64,
            source.digest.clone(),
            move |valid| async move {
                if !valid {
                    checks.failure(Target::Raw(source)).await;
                }
            },
        )
    }
    pub fn raw_source(self: &Arc<Self>, source: RawSource) {
        let (evidence, job) = self.work.begin();
        let Some(job) = job else {
            return;
        };
        let Some(app) = self.app.get().and_then(Weak::upgrade) else {
            self.work.finish(&evidence, SKIPPED);
            return;
        };
        // Leave a request slot free instead of waiting behind foreground readers.
        let Ok(mut slots) = app.slots.clone().try_acquire_many_owned(2) else {
            self.work.finish(&evidence, SKIPPED);
            return;
        };
        let slot = slots.split(1).unwrap();
        drop(slots);
        let checks = self.clone();
        tokio::spawn(crate::storage::resource_gate::PRIORITY.scope(
            crate::storage::resource_gate::MAINTENANCE,
            async move {
                let _job = job;
                let _slot = slot;
                #[cfg(feature = "fault-injection")]
                crate::faults::point("read-integrity-check").await;
                let result = tokio::time::timeout(std::time::Duration::from_secs(30), async {
                    let _profile = checks
                        .storage
                        .profile_permit(source.storage_id, "integrity")
                        .await?;
                    let profile = checks.storage.profile(source.storage_id)?;
                    anyhow::ensure!(
                        app.acquire_profile_rate(source.storage_id, &profile, 2, source.size)
                            .await?,
                        "optional check rate exhausted"
                    );
                    checks.storage.raw_reader_uncached(&source, true).await?;
                    Ok::<_, anyhow::Error>(())
                })
                .await;
                drop(_slot);
                match result {
                    Ok(Ok(())) => checks.work.finish(&evidence, VERIFIED),
                    Ok(Err(error))
                        if error
                            .downcast_ref::<crate::content::codec::IntegrityError>()
                            .is_some() =>
                    {
                        checks.work.finish(&evidence, FAILED);
                        checks.failure(Target::Raw(source)).await;
                    }
                    _ => checks.work.finish(&evidence, SKIPPED),
                }
            },
        ));
    }
    async fn failure(&self, target: Target) {
        self.storage.invalidate_reads();
        match &target {
            Target::Chunk(chunk) => self.storage.reject_chunk_cache(chunk).await,
            Target::Raw(raw) => self.storage.reject_cache(raw.location_id).await,
        }
        let Some(app) = self.app.get().and_then(Weak::upgrade) else {
            return;
        };
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            crate::storage::resource_gate::PRIORITY.scope(
                crate::storage::resource_gate::MAINTENANCE,
                async {
                    use crate::model::{
                        PhysicalSource,
                        source::{SourceScope, SourceType},
                    };
                    let scope = match target {
                        Target::Raw(raw) => SourceScope {
                            storage_id: raw.storage_id,
                            source: SourceType::Raw,
                            id: raw.id.to_string(),
                        },
                        Target::Chunk(chunk) => match app.physical.source(chunk.id, true).await? {
                            PhysicalSource::Chunk(c) => SourceScope {
                                storage_id: c.storage_id.context("chunk has no store")?,
                                source: SourceType::Chunk,
                                id: c.encoding_id.to_string(),
                            },
                            PhysicalSource::Pack(p) => SourceScope {
                                storage_id: p.storage_id.context("pack has no store")?,
                                source: SourceType::Pack,
                                id: p.id.to_string(),
                            },
                            PhysicalSource::Raw(_) => anyhow::bail!("chunk resolved to raw source"),
                        },
                    };
                    // Re-read the exact backend namespace and content before any durable isolation.
                    let operation = app
                        .source_prepare(&crate::authorization::Principal::System, scope, false)
                        .await?;
                    app.source_start(operation).await?;
                    Ok::<_, anyhow::Error>(())
                },
            ),
        )
        .await;
        if !matches!(result, Ok(Ok(()))) {
            tracing::debug!(
                "read digest failed; current backend did not confirm a quarantinable source"
            );
        }
    }
}
