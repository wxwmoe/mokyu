use anyhow::{Result, ensure};
use futures_util::StreamExt;
use object_store::{GetOptions, GetRange, ObjectStore, path::Path};
use std::{
    future::Future,
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::time::Instant;

pub(crate) struct Recovery {
    spent: Arc<AtomicBool>,
    enabled: bool,
    deadline: Instant,
    stall: Duration,
}

impl Recovery {
    pub(crate) fn configured(self: &Arc<Self>, max: i32) -> Arc<Self> {
        if max > 0 {
            return self.clone();
        }
        Arc::new(Self {
            spent: self.spent.clone(),
            deadline: self.deadline,
            stall: self.stall,
            enabled: false,
        })
    }
    pub(crate) fn new(limit: Duration) -> Arc<Self> {
        Arc::new(Self {
            spent: Arc::new(AtomicBool::new(false)),
            enabled: true,
            deadline: Instant::now() + limit,
            stall: (limit / 3).clamp(Duration::from_millis(500), Duration::from_secs(2)),
        })
    }
}

pub(crate) struct Attempt {
    budget: Arc<Recovery>,
    sent: AtomicUsize,
    head: Option<(tokio::sync::watch::Receiver<u64>, u64)>,
}
tokio::task_local! {
    pub(crate) static OP:Arc<Attempt>;
    pub(crate) static REQUEST:Arc<Recovery>;
    pub(crate) static READ_AHEAD:bool;
}

impl Attempt {
    pub(crate) fn new(budget: Arc<Recovery>) -> Arc<Self> {
        Arc::new(Self {
            budget,
            sent: AtomicUsize::new(0),
            head: None,
        })
    }
    pub(crate) fn segment(
        budget: Arc<Recovery>,
        head: tokio::sync::watch::Receiver<u64>,
        start: u64,
    ) -> Arc<Self> {
        Arc::new(Self {
            budget,
            sent: AtomicUsize::new(0),
            head: Some((head, start)),
        })
    }
    async fn prefix(&self) -> io::Result<()> {
        if let Some((rx, start)) = &self.head {
            let mut rx = rx.clone();
            rx.wait_for(|v| *v >= *start)
                .await
                .map_err(|_| io::Error::other("read ended"))?;
        }
        Ok(())
    }
    pub(super) async fn before_wire(&self) -> io::Result<()> {
        if self.sent.load(Ordering::Relaxed) > 0 {
            self.prefix().await?;
        }
        self.sending()
    }
    pub(crate) fn sending(&self) -> io::Result<()> {
        if self.sent.fetch_add(1, Ordering::Relaxed) > 0 {
            if !self.budget.enabled
                || Instant::now() >= self.budget.deadline
                || self.budget.spent.swap(true, Ordering::AcqRel)
            {
                return Err(io::Error::other("read recovery budget exhausted"));
            }
            tracing::debug!("retrying stalled or interrupted origin read");
        }
        Ok(())
    }
    pub(crate) fn may_retry(&self, error: &anyhow::Error) -> bool {
        self.budget.enabled
            && !self.budget.spent.load(Ordering::Acquire)
            && Instant::now() < self.budget.deadline
            && (error.downcast_ref::<io::Error>().is_some_and(|e| {
                matches!(
                    e.kind(),
                    io::ErrorKind::TimedOut
                        | io::ErrorKind::UnexpectedEof
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::ConnectionAborted
                        | io::ErrorKind::BrokenPipe
                )
            }) || error
                .chain()
                .filter_map(|e| e.downcast_ref::<object_store::client::HttpError>())
                .any(|e| {
                    matches!(
                        e.kind(),
                        object_store::client::HttpErrorKind::Timeout
                            | object_store::client::HttpErrorKind::Connect
                            | object_store::client::HttpErrorKind::Request
                            | object_store::client::HttpErrorKind::Interrupted
                    )
                }))
    }
    pub(crate) async fn progress<T>(&self, f: impl Future<Output = Result<T>>) -> Result<T> {
        if !self.budget.enabled {
            return f.await;
        }
        tokio::pin!(f);
        if let Some((rx, start)) = &self.head
            && *rx.borrow() < *start
        {
            tokio::select! {r=&mut f=>return r,r=self.prefix()=>r?}
        }
        let now = Instant::now();
        let due = if self.sent.load(Ordering::Relaxed) > 1 {
            self.budget.deadline
        } else if self.budget.spent.load(Ordering::Acquire) {
            self.budget.deadline.max(now + self.budget.stall)
        } else {
            now + self.budget.stall
        };
        tokio::time::timeout_at(due, f)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "origin read made no progress"))?
    }
}

// A complete authenticated chunk or pack is the smallest safe retry unit.
pub(crate) async fn bounded(
    store: &dyn ObjectStore,
    key: &Path,
    bound: usize,
    span: Option<(std::ops::Range<u64>, u64)>,
    budget: Option<Arc<Recovery>>,
) -> Result<Vec<u8>> {
    async fn read(
        store: &dyn ObjectStore,
        key: &Path,
        bound: usize,
        span: Option<&(std::ops::Range<u64>, u64)>,
        op: Option<&Attempt>,
    ) -> Result<Vec<u8>> {
        let options = GetOptions {
            range: span.map(|(range, _)| GetRange::Bounded(range.clone())),
            ..Default::default()
        };
        let open = async { Ok(store.get_opts(key, options).await?) };
        let result = if let Some(op) = op {
            op.progress(open).await?
        } else {
            open.await?
        };
        let size = if let Some((range, total)) = span {
            ensure!(
                result.meta.size == *total && result.range == *range,
                crate::content::codec::IntegrityError::Length
            );
            usize::try_from(
                range
                    .end
                    .checked_sub(range.start)
                    .ok_or(crate::content::codec::IntegrityError::Length)?,
            )?
        } else {
            usize::try_from(result.meta.size)?
        };
        ensure!(size <= bound, crate::content::codec::IntegrityError::Length);
        let mut bytes = Vec::with_capacity(size);
        let mut stream = result.into_stream();
        loop {
            let get = async { Ok(stream.next().await.transpose()?) };
            let next = if let Some(op) = op {
                op.progress(get).await?
            } else {
                get.await?
            };
            let Some(part) = next else { break };
            ensure!(
                part.len() <= size - bytes.len(),
                crate::content::codec::IntegrityError::Length
            );
            bytes.extend_from_slice(&part);
        }
        if bytes.len() != size {
            return Err(
                io::Error::new(io::ErrorKind::UnexpectedEof, "incomplete origin response").into(),
            );
        }
        Ok(bytes)
    }
    let Some(budget) = budget else {
        return read(store, key, bound, span.as_ref(), None).await;
    };
    let op = Attempt::new(budget);
    OP.scope(op.clone(), async {
        match read(store, key, bound, span.as_ref(), Some(&op)).await {
            Err(e) if op.may_retry(&e) => read(store, key, bound, span.as_ref(), Some(&op)).await,
            other => other,
        }
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sdk_and_foreground_share_one_extra_wire_attempt() {
        let budget = Recovery::new(Duration::from_secs(30));
        let a = Attempt::new(budget.clone());
        let b = Attempt::new(budget);
        a.sending().unwrap();
        b.sending().unwrap();
        a.sending().unwrap();
        assert!(a.sending().is_err());
        assert!(b.sending().is_err());
    }
}
