use super::App;
use crate::{content::codec::Chunk, model::raw::RawSource, storage::Storage};
use anyhow::Context;
use bytes::Bytes;
use md5::{Digest, Md5};
use std::sync::{
    Arc, OnceLock, Weak,
    atomic::{AtomicU8, AtomicU64, Ordering},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

const PENDING: u8 = 0;
const VERIFIED: u8 = 1;
const SKIPPED: u8 = 2;
const FAILED: u8 = 3;
const UNCHECKED: u8 = 4;
const UNIT: usize = 4096;
const MEMORY: usize = 8 * 1024 * 1024;
const JOBS: usize = 4;

pub(crate) struct Evidence(AtomicU8);
impl Evidence {
    pub fn verified(&self) -> bool {
        self.0.load(Ordering::Acquire) == VERIFIED
    }
    pub fn cacheable(&self) -> bool {
        !matches!(self.0.load(Ordering::Acquire), SKIPPED | FAILED)
    }
    fn pending(&self) -> bool {
        self.0.load(Ordering::Acquire) == PENDING
    }
}

pub(crate) struct Checks {
    storage: Arc<Storage>,
    app: OnceLock<Weak<App>>,
    memory: Arc<Semaphore>,
    jobs: Arc<Semaphore>,
    verified: AtomicU64,
    failed: AtomicU64,
    skipped: AtomicU64,
    unchecked: AtomicU64,
    off: Arc<Evidence>,
}

enum Target {
    Chunk(Chunk),
    Raw(RawSource),
}
struct Payload {
    bytes: Bytes,
    _memory: OwnedSemaphorePermit,
}

pub(crate) struct RawCheck {
    pub evidence: Arc<Evidence>,
    checks: Arc<Checks>,
    sender: Option<mpsc::Sender<Payload>>,
}

impl RawCheck {
    pub fn feed(&mut self, bytes: Bytes, retained: usize) {
        let Some(sender) = &self.sender else {
            return;
        };
        let Some(memory) = self.checks.reserve(retained) else {
            self.checks.finish(&self.evidence, SKIPPED);
            self.sender = None;
            return;
        };
        if sender
            .try_send(Payload {
                bytes,
                _memory: memory,
            })
            .is_err()
        {
            self.checks.finish(&self.evidence, SKIPPED);
            self.sender = None;
        }
    }
}

impl Checks {
    pub fn new(storage: Arc<Storage>) -> Arc<Self> {
        Arc::new(Self {
            storage,
            app: OnceLock::new(),
            memory: Arc::new(Semaphore::new(MEMORY / UNIT)),
            jobs: Arc::new(Semaphore::new(JOBS)),
            verified: 0.into(),
            failed: 0.into(),
            skipped: 0.into(),
            unchecked: 0.into(),
            off: Arc::new(Evidence(UNCHECKED.into())),
        })
    }
    pub fn bind(&self, app: Weak<App>) {
        let _ = self.app.set(app);
    }
    fn reserve(&self, size: usize) -> Option<OwnedSemaphorePermit> {
        self.memory
            .clone()
            .try_acquire_many_owned(size.max(1).div_ceil(UNIT).try_into().ok()?)
            .ok()
    }
    fn evidence() -> Arc<Evidence> {
        Arc::new(Evidence(PENDING.into()))
    }
    pub fn unchecked(&self) -> Arc<Evidence> {
        self.unchecked.fetch_add(1, Ordering::Relaxed);
        self.off.clone()
    }
    fn finish(&self, evidence: &Evidence, state: u8) {
        if evidence
            .0
            .compare_exchange(PENDING, state, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let counter = match state {
            VERIFIED => &self.verified,
            FAILED => &self.failed,
            _ => &self.skipped,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
    pub fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({"limit_bytes": MEMORY,"used_bytes":(MEMORY / UNIT-self.memory.available_permits())*UNIT,
            "active":JOBS-self.jobs.available_permits(),"verified":self.verified.load(Ordering::Relaxed),
            "failed":self.failed.load(Ordering::Relaxed),"skipped":self.skipped.load(Ordering::Relaxed),
            "unchecked":self.unchecked.load(Ordering::Relaxed)})
    }
    pub fn chunk(self: &Arc<Self>, chunk: &Chunk, bytes: Bytes, retained: usize) -> Arc<Evidence> {
        let evidence = Self::evidence();
        let Some(memory) = self.reserve(retained) else {
            self.finish(&evidence, SKIPPED);
            return evidence;
        };
        let Ok(job) = self.jobs.clone().try_acquire_owned() else {
            self.finish(&evidence, SKIPPED);
            return evidence;
        };
        let checks = self.clone();
        let result = evidence.clone();
        let chunk = chunk.clone();
        tokio::spawn(async move {
            let _job = job;
            #[cfg(feature = "fault-injection")]
            crate::faults::point("read-integrity-check").await;
            let Ok(cpu) = checks.storage.cpu.clone().try_acquire_owned() else {
                checks.finish(&result, SKIPPED);
                return;
            };
            let expected = chunk.hash.clone();
            let verified = tokio::task::spawn_blocking(move || {
                let _cpu = cpu;
                let valid = blake3::hash(&bytes).as_bytes().as_slice() == expected;
                (valid, bytes, memory)
            })
            .await;
            match verified {
                Ok((true, bytes, _memory)) => {
                    checks.finish(&result, VERIFIED);
                    checks.storage.fill_read(&chunk, bytes, None);
                }
                Ok((false, _, _)) => {
                    checks.finish(&result, FAILED);
                    checks.failure(Target::Chunk(chunk)).await;
                }
                Err(_) => checks.finish(&result, SKIPPED),
            }
        });
        evidence
    }
    pub fn raw(self: &Arc<Self>, source: &RawSource) -> RawCheck {
        let evidence = Self::evidence();
        let mut audit = RawCheck {
            evidence: evidence.clone(),
            checks: self.clone(),
            sender: None,
        };
        let Ok(job) = self.jobs.clone().try_acquire_owned() else {
            self.finish(&evidence, SKIPPED);
            return audit;
        };
        let (sender, mut receiver) = mpsc::channel::<Payload>(32);
        audit.sender = Some(sender);
        let checks = self.clone();
        let source = source.clone();
        tokio::spawn(async move {
            let _job = job;
            #[cfg(feature = "fault-injection")]
            crate::faults::point("read-integrity-check").await;
            let mut digest = Md5::new();
            let mut size = 0u64;
            loop {
                let payload =
                    match tokio::time::timeout(std::time::Duration::from_secs(30), receiver.recv())
                        .await
                    {
                        Ok(Some(payload)) => payload,
                        Ok(None) => break,
                        Err(_) => {
                            checks.finish(&evidence, SKIPPED);
                            return;
                        }
                    };
                if !evidence.pending() {
                    return;
                }
                let Ok(cpu) = checks.storage.cpu.clone().try_acquire_owned() else {
                    checks.finish(&evidence, SKIPPED);
                    return;
                };
                size = size.saturating_add(payload.bytes.len() as u64);
                let result = tokio::task::spawn_blocking(move || {
                    let _cpu = cpu;
                    let _memory = payload._memory;
                    digest.update(&payload.bytes);
                    digest
                })
                .await;
                let Ok(current) = result else {
                    checks.finish(&evidence, SKIPPED);
                    return;
                };
                digest = current;
            }
            if !evidence.pending() {
                return;
            }
            if size != source.size as u64 {
                checks.finish(&evidence, SKIPPED);
                return;
            }
            if source.digest.as_deref() == Some(digest.finalize().as_slice()) {
                checks.finish(&evidence, VERIFIED);
            } else {
                checks.finish(&evidence, FAILED);
                checks.failure(Target::Raw(source)).await;
            }
        });
        audit
    }
    pub fn raw_source(self: &Arc<Self>, source: RawSource) {
        let evidence = Self::evidence();
        let Ok(job) = self.jobs.clone().try_acquire_owned() else {
            self.finish(&evidence, SKIPPED);
            return;
        };
        let Some(app) = self.app.get().and_then(Weak::upgrade) else {
            self.finish(&evidence, SKIPPED);
            return;
        };
        // Leave a request slot free instead of waiting behind foreground readers.
        let Ok(mut slots) = app.slots.clone().try_acquire_many_owned(2) else {
            self.finish(&evidence, SKIPPED);
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
                    Ok(Ok(())) => checks.finish(&evidence, VERIFIED),
                    Ok(Err(error))
                        if error
                            .downcast_ref::<crate::content::codec::IntegrityError>()
                            .is_some() =>
                    {
                        checks.finish(&evidence, FAILED);
                        checks.failure(Target::Raw(source)).await;
                    }
                    _ => checks.finish(&evidence, SKIPPED),
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
