use bytes::Bytes;
use md5::{Digest, Md5};
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
};
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
};

#[cfg(test)]
#[path = "../../tests/storage/read_checks.rs"]
mod tests;

const PENDING: u8 = 0;
pub(crate) const VERIFIED: u8 = 1;
pub(crate) const SKIPPED: u8 = 2;
pub(crate) const FAILED: u8 = 3;
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
    cpu: Arc<Semaphore>,
    memory: Arc<Semaphore>,
    jobs: Arc<Semaphore>,
    verified: AtomicU64,
    failed: AtomicU64,
    skipped: AtomicU64,
    unchecked: AtomicU64,
    off: Arc<Evidence>,
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
        let Some(sender) = &self.sender else { return };
        let Some(memory) = self.checks.reserve(retained) else {
            tracing::debug!(retained, "background raw check memory exhausted");
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
            tracing::debug!("background raw check queue exhausted");
            self.checks.finish(&self.evidence, SKIPPED);
            self.sender = None;
        }
    }
}

impl Checks {
    pub fn new(cpu: Arc<Semaphore>) -> Arc<Self> {
        Arc::new(Self {
            cpu,
            memory: Arc::new(Semaphore::new(MEMORY / UNIT)),
            jobs: Arc::new(Semaphore::new(JOBS)),
            verified: 0.into(),
            failed: 0.into(),
            skipped: 0.into(),
            unchecked: 0.into(),
            off: Arc::new(Evidence(UNCHECKED.into())),
        })
    }
    fn reserve(&self, size: usize) -> Option<OwnedSemaphorePermit> {
        self.memory
            .clone()
            .try_acquire_many_owned(size.max(1).div_ceil(UNIT).try_into().ok()?)
            .ok()
    }
    pub fn begin(&self) -> (Arc<Evidence>, Option<OwnedSemaphorePermit>) {
        let evidence = Arc::new(Evidence(PENDING.into()));
        let job = self.jobs.clone().try_acquire_owned().ok();
        if job.is_none() {
            self.finish(&evidence, SKIPPED);
        }
        (evidence, job)
    }
    pub fn unchecked(&self) -> Arc<Evidence> {
        self.unchecked.fetch_add(1, Ordering::Relaxed);
        self.off.clone()
    }
    pub fn finish(&self, evidence: &Evidence, state: u8) {
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
    pub fn chunk<F, R>(
        self: &Arc<Self>,
        expected: Vec<u8>,
        bytes: Bytes,
        retained: usize,
        complete: F,
    ) -> Arc<Evidence>
    where
        F: FnOnce(bool, Bytes) -> R + Send + 'static,
        R: Future<Output = ()> + Send,
    {
        let (evidence, job) = self.begin();
        let Some(job) = job else { return evidence };
        let Some(memory) = self.reserve(retained) else {
            self.finish(&evidence, SKIPPED);
            return evidence;
        };
        let checks = self.clone();
        let result = evidence.clone();
        tokio::spawn(async move {
            let _job = job;
            #[cfg(feature = "fault-injection")]
            crate::faults::point("read-integrity-check").await;
            let Ok(cpu) = checks.cpu.clone().try_acquire_owned() else {
                checks.finish(&result, SKIPPED);
                return;
            };
            let verified = tokio::task::spawn_blocking(move || {
                let _cpu = cpu;
                let valid = blake3::hash(&bytes).as_bytes().as_slice() == expected;
                (valid, bytes, memory)
            })
            .await;
            match verified {
                Ok((valid, bytes, _memory)) => {
                    checks.finish(&result, if valid { VERIFIED } else { FAILED });
                    complete(valid, bytes).await;
                }
                Err(_) => checks.finish(&result, SKIPPED),
            }
        });
        evidence
    }
    pub fn raw<W, P, F, R>(
        self: &Arc<Self>,
        size: u64,
        expected: Option<Vec<u8>>,
        prepare: P,
        complete: F,
    ) -> RawCheck
    where
        W: AsyncWrite + Unpin + Send + 'static,
        P: Future<Output = Option<W>> + Send + 'static,
        F: FnOnce(bool, Option<W>) -> R + Send + 'static,
        R: Future<Output = ()> + Send,
    {
        let (evidence, job) = self.begin();
        let mut audit = RawCheck {
            evidence: evidence.clone(),
            checks: self.clone(),
            sender: None,
        };
        let Some(job) = job else { return audit };
        let (sender, mut receiver) = mpsc::channel::<Payload>(MEMORY / UNIT);
        audit.sender = Some(sender);
        let checks = self.clone();
        tokio::spawn(async move {
            let _job = job;
            #[cfg(feature = "fault-injection")]
            crate::faults::point("read-integrity-check").await;
            let (cache_sender, mut cache_receiver) = mpsc::channel::<Payload>(MEMORY / UNIT);
            let mut cache_sender = Some(cache_sender);
            let mut cache = tokio::task::JoinSet::new();
            cache.spawn(async move {
                let mut writer =
                    tokio::time::timeout(std::time::Duration::from_millis(100), prepare)
                        .await
                        .ok()
                        .flatten()?;
                while let Some(payload) = cache_receiver.recv().await {
                    tokio::time::timeout(std::time::Duration::from_millis(100), async {
                        #[cfg(feature = "fault-injection")]
                        crate::faults::point("raw-cache-stream").await;
                        writer.write_all(&payload.bytes).await
                    })
                    .await
                    .ok()?
                    .ok()?;
                }
                Some(writer)
            });
            let mut digest = Md5::new();
            let mut received = 0u64;
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
                let Ok(cpu) = checks.cpu.clone().try_acquire_owned() else {
                    tracing::debug!("background raw check CPU busy");
                    checks.finish(&evidence, SKIPPED);
                    return;
                };
                received = received.saturating_add(payload.bytes.len() as u64);
                let result = tokio::task::spawn_blocking(move || {
                    let _cpu = cpu;
                    digest.update(&payload.bytes);
                    (digest, payload)
                })
                .await;
                let Ok((current, payload)) = result else {
                    checks.finish(&evidence, SKIPPED);
                    return;
                };
                digest = current;
                if let Some(sender) = &cache_sender
                    && sender.try_send(payload).is_err()
                {
                    // A slow optional writer must not stall digest verification.
                    cache_sender = None;
                    cache.abort_all();
                }
            }
            if !evidence.pending() {
                return;
            }
            if received != size {
                checks.finish(&evidence, SKIPPED);
                return;
            }
            let valid = expected.as_deref() == Some(digest.finalize().as_slice());
            checks.finish(&evidence, if valid { VERIFIED } else { FAILED });
            drop(cache_sender);
            let writer = if valid {
                tokio::time::timeout(std::time::Duration::from_secs(1), cache.join_next())
                    .await
                    .ok()
                    .flatten()
                    .and_then(Result::ok)
                    .flatten()
            } else {
                None
            };
            drop(cache);
            complete(valid, writer).await;
        });
        audit
    }
}
