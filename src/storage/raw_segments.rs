use crate::content::{
    codec::IntegrityError,
    raw_pages::{HASH, PAGE},
};
use anyhow::{Context, Result, ensure};
use bytes::{Bytes, BytesMut};
use futures_util::{StreamExt, future::BoxFuture, stream::BoxStream};
use object_store::{GetOptions, GetRange, ObjectStore, path::Path};
use std::{
    collections::VecDeque,
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::mpsc;

pub(crate) const SEGMENT: u64 = 8 * PAGE as u64;

#[derive(Clone)]
pub(crate) struct Remote {
    pub(crate) backend: Arc<dyn ObjectStore>,
    pub(crate) key: Path,
    pub(crate) size: u64,
    pub(crate) tag: Option<String>,
    pub(crate) version: Option<String>,
    pub(crate) gets: Arc<AtomicU64>,
    pub(crate) diagnostics: Arc<super::diagnostics::Diagnostics>,
}

pub(crate) struct Proof {
    pub(crate) first: u64,
    pub(crate) hashes: Vec<u8>,
    cpu: Arc<tokio::sync::Semaphore>,
}

impl Proof {
    pub(crate) fn new(
        range: &Range<u64>,
        hashes: Vec<u8>,
        cpu: Arc<tokio::sync::Semaphore>,
    ) -> Result<Self> {
        let page = PAGE as u64;
        let first = range.start / page;
        ensure!(
            range.end > range.start
                && hashes.len() as u64 == ((range.end - 1) / page - first + 1) * HASH as u64,
            IntegrityError::RawMetadata
        );
        Ok(Self { first, hashes, cpu })
    }

    async fn verify(self: &Arc<Self>, start: u64, bytes: Bytes) -> Result<Bytes> {
        let cpu = self.cpu.clone().acquire_owned().await?;
        let proof = self.clone();
        tokio::task::spawn_blocking(move || {
            let _cpu = cpu;
            proof.check(start, &bytes)?;
            Ok(bytes)
        })
        .await?
    }

    pub(super) fn contains(&self, page: u64) -> bool {
        page >= self.first && page - self.first < (self.hashes.len() / HASH) as u64
    }
    pub(super) fn check(&self, start: u64, bytes: &[u8]) -> Result<()> {
        let index = (start / PAGE as u64)
            .checked_sub(self.first)
            .context(IntegrityError::RawMetadata)?;
        let offset = usize::try_from(index)?
            .checked_mul(HASH)
            .context(IntegrityError::RawMetadata)?;
        ensure!(
            self.hashes.get(offset..offset + HASH)
                == Some(blake3::hash(bytes).as_bytes().as_slice()),
            IntegrityError::Hash
        );
        Ok(())
    }
}

pub(crate) type Permit = Box<dyn Send + Sync>;
pub(crate) type Acquire = Arc<dyn Fn() -> BoxFuture<'static, Result<Permit>> + Send + Sync>;
pub(crate) type Retain = Arc<dyn Fn(usize) -> BoxFuture<'static, Result<Permit>> + Send + Sync>;

struct Held(Bytes, #[allow(dead_code)] Permit);
impl AsRef<[u8]> for Held {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

pub(super) fn held(bytes: Bytes, permit: Permit) -> Bytes {
    Bytes::from_owner(Held(bytes, permit))
}

struct Job {
    incoming: mpsc::Receiver<Result<Bytes>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Job {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn bounds(request: &Range<u64>, strict: bool, size: u64) -> (Range<u64>, u64) {
    let page = PAGE as u64;
    let span = if strict {
        request.start / page * page..request.end.div_ceil(page).saturating_mul(page).min(size)
    } else {
        request.clone()
    };
    let length = span.end - span.start;
    let stride = if request.end - request.start <= page {
        length
    } else if length <= SEGMENT {
        length.div_ceil(page).div_ceil(2) * page
    } else {
        SEGMENT
    };
    (span, stride)
}

impl Remote {
    pub(crate) fn stream(
        self,
        requested: Range<u64>,
        proof: Option<Arc<Proof>>,
        width: usize,
        acquire: Acquire,
        retain: Retain,
    ) -> BoxStream<'static, Result<Bytes>> {
        Box::pin(async_stream::try_stream! {
            ensure_range(&self, &requested)?;
            let (span,stride) = bounds(&requested, proof.is_some(),self.size);
            let width = width.clamp(1,crate::model::node_read::LANES);
            let mut next = span.start;
            let mut jobs = VecDeque::new();
            let (first,mut turn)=tokio::sync::oneshot::channel();
            let _=first.send(());
            let remote = Arc::new(self);
            while next < span.end || !jobs.is_empty() {
                while next < span.end && jobs.len() < width {
                    let end = next.saturating_add(stride).min(span.end);
                    let range = next..end;
                    let remote=remote.clone();let proof=proof.clone();let requested=requested.clone();
                    let acquire=acquire.clone();let retain=retain.clone();
                    let (tx,incoming)=mpsc::channel(1);
                    let (admitted,after)=tokio::sync::oneshot::channel();
                    let before=std::mem::replace(&mut turn,after);
                    let task=tokio::spawn(async move {
                        let result=async {
                            before.await?;
                            let buffer_size=(range.end-range.start).min(if proof.is_some(){PAGE as u64}else{256*1024}) as usize;
                            let initial=retain(buffer_size).await?;
                            let io=acquire().await?;
                            let _=admitted.send(());
                            remote.download(range,&requested,proof.as_ref(),&tx,io,retain,initial).await
                        }.await;
                        if let Err(error)=result { let _=tx.send(Err(error)).await; }
                    });
                    jobs.push_back(Job { incoming,task });
                    next=end;
                }
                let mut head=jobs.pop_front().context("empty raw read window")?;
                while let Some(bytes)=head.incoming.recv().await { yield bytes?; }
                (&mut head.task).await?;
            }
        })
    }

    async fn download(
        &self,
        span: Range<u64>,
        requested: &Range<u64>,
        proof: Option<&Arc<Proof>>,
        tx: &mpsc::Sender<Result<Bytes>>,
        _io: Permit,
        retain: Retain,
        initial: Permit,
    ) -> Result<()> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        let reply = self
            .backend
            .get_opts(
                &self.key,
                GetOptions {
                    if_match: self.tag.clone(),
                    version: self.version.clone(),
                    range: Some(GetRange::Bounded(span.clone())),
                    ..Default::default()
                },
            )
            .await
            .inspect_err(|error| self.diagnostics.backend(error))
            .map_err(super::raw::remote_error)?;
        ensure!(
            reply.range == span
                && reply.meta.size == self.size
                && self
                    .tag
                    .as_ref()
                    .is_none_or(|t| reply.meta.e_tag.as_ref() == Some(t))
                && self
                    .version
                    .as_ref()
                    .is_none_or(|v| reply.meta.version.as_ref() == Some(v)),
            IntegrityError::Length
        );
        let mut stream = reply.into_stream();
        let mut initial = Some(initial);
        let mut leftover = Bytes::new();
        let mut pos = span.start;
        while pos < span.end {
            let output = tx.reserve().await.context("raw reader closed")?;
            let n = (span.end - pos).min(if proof.is_some() {
                PAGE as u64
            } else {
                256 * 1024
            }) as usize;
            let memory = if let Some(initial) = initial.take() {
                initial
            } else {
                retain(n).await?
            };
            let mut page = BytesMut::with_capacity(n);
            while page.len() < n {
                if leftover.is_empty() {
                    leftover = stream
                        .next()
                        .await
                        .context(IntegrityError::Length)?
                        .inspect_err(|error| self.diagnostics.backend(error))
                        .map_err(super::raw::remote_error)?;
                    ensure!(
                        leftover.len() as u64 <= span.end - pos - page.len() as u64,
                        IntegrityError::Length
                    );
                }
                let count = leftover.len().min(n - page.len());
                page.extend_from_slice(&leftover.split_to(count));
                if proof.is_none() && !page.is_empty() {
                    break;
                }
            }
            let page = Bytes::from_owner(Held(page.freeze(), memory));
            let page = if let Some(proof) = proof {
                proof.verify(pos, page).await?
            } else {
                page
            };
            let end = pos + page.len() as u64;
            let from = requested.start.saturating_sub(pos).min(page.len() as u64) as usize;
            let to = requested.end.min(end).saturating_sub(pos) as usize;
            ensure!(from < to, IntegrityError::Length);
            output.send(Ok(page.slice(from..to)));
            pos = end;
        }
        Ok(())
    }
}

pub(crate) fn local<'a>(
    file: &'a mut super::raw::RawFile,
    requested: Range<u64>,
    size: u64,
    proof: Option<Arc<Proof>>,
    retain: Retain,
) -> BoxStream<'a, Result<Bytes>> {
    Box::pin(async_stream::try_stream! {
        if !(requested.start<requested.end && requested.end<=size){ Err(IntegrityError::Length)?; }
        let (span,_)=bounds(&requested,proof.is_some(),size);
        file.seek(std::io::SeekFrom::Start(span.start)).await?;
        let mut pos=span.start;
        while pos<span.end {
            let n=(span.end-pos).min(if proof.is_some(){PAGE as u64}else{256*1024}) as usize;
            let memory=retain(n).await?;
            let mut data=vec![0;n];
            file.read_exact(&mut data).await?;
            let data=Bytes::from_owner(Held(Bytes::from(data),memory));
            let output=if let Some(proof)=&proof {proof.verify(pos,data).await?}else{data};
            let end=pos+n as u64;
            let from=requested.start.saturating_sub(pos) as usize;
            let to=requested.end.min(end).saturating_sub(pos) as usize;
            yield output.slice(from..to);
            pos=end;
        }
    })
}

fn ensure_range(source: &Remote, range: &Range<u64>) -> Result<()> {
    ensure!(
        range.start < range.end && range.end <= source.size,
        IntegrityError::Length
    );
    ensure!(
        source.version.is_some()
            || source
                .tag
                .as_ref()
                .is_some_and(|tag| !tag.is_empty() && !tag.starts_with("W/")),
        IntegrityError::RawMetadata
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_small_reads_and_adjacent_parallel_ranges() {
        let p = PAGE as u64;
        assert_eq!(bounds(&(p - 1..p + 1), false, 9 * p), (p - 1..p + 1, 2));
        assert_eq!(bounds(&(p - 1..p + 1), true, 9 * p), (0..2 * p, 2 * p));
        assert_eq!(bounds(&(11..p + 12), false, 9 * p), (11..p + 12, p));
        assert_eq!(
            bounds(&(p..9 * p + 3), true, 9 * p + 3),
            (p..9 * p + 3, 8 * p)
        );
    }
}
