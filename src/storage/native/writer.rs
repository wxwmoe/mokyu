use super::{
    control::{Control, Piece},
    transport::{Pool, Renewal},
};
use crate::{
    content::fragment::UNIT,
    model::{storage_payload::Intent, storage_transfer::Completion},
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures_util::{StreamExt, stream::BoxStream};
use tokio::sync::mpsc;
use uuid::Uuid;

pub(crate) struct Sink {
    sender: Option<mpsc::Sender<Bytes>>,
    future: Option<futures_util::future::BoxFuture<'static, Result<Intent>>>,
    size: u64,
    received: u64,
}

impl Sink {
    pub fn new(
        store: std::sync::Arc<super::store::Store>,
        path: object_store::path::Path,
        size: u64,
    ) -> Self {
        let (sender, receiver) = mpsc::channel::<Bytes>(1);
        let future = Box::pin(async move {
            let body = futures_util::stream::unfold(receiver, |mut receiver| async move {
                receiver.recv().await.map(|bytes| (Ok(bytes), receiver))
            })
            .boxed();
            store
                .write(&path, size, body, object_store::PutMode::Create.into())
                .await
        });
        Self {
            sender: Some(sender),
            future: Some(future),
            size,
            received: 0,
        }
    }

    pub async fn write(&mut self, data: &[u8]) -> Result<()> {
        ensure!(
            self.received
                .checked_add(data.len() as u64)
                .is_some_and(|end| end <= self.size),
            "native stream exceeds its declared size"
        );
        for bytes in data.chunks(UNIT) {
            let sender = self.sender.as_ref().context("native stream is closed")?;
            let future = self.future.as_mut().context("native stream has stopped")?;
            let finished = tokio::select! {
                result = &mut *future => Some(result),
                result = sender.send(Bytes::copy_from_slice(bytes)) => {
                    if result.is_err() { Some(future.await) } else { None }
                }
            };
            if let Some(result) = finished {
                self.future = None;
                self.sender = None;
                return Err(result.err().unwrap_or_else(|| {
                    anyhow::anyhow!("native stream completed before its input")
                }));
            }
        }
        self.received += data.len() as u64;
        Ok(())
    }

    pub async fn finish(mut self) -> Result<Intent> {
        ensure!(self.received == self.size, "native stream is truncated");
        self.sender.take();
        self.future
            .take()
            .context("native stream has stopped")?
            .await
    }
}

#[async_trait::async_trait]
pub(crate) trait Writes: Renewal {
    async fn window(&self, operation: Uuid, sequence: u32) -> Result<Vec<Piece>>;
    async fn accept(&self, operation: Uuid, completed: &[Completion]) -> Result<()>;
}

#[async_trait::async_trait]
impl Writes for Control {
    async fn window(&self, operation: Uuid, sequence: u32) -> Result<Vec<Piece>> {
        self.write_window(operation, sequence, 1).await
    }
    async fn accept(&self, operation: Uuid, completed: &[Completion]) -> Result<()> {
        Control::accept(self, operation, completed).await
    }
}

// Only required copies participate in the foreground stream; missing copies remain durable SQL work.
pub(crate) async fn write(
    control: &dyn Writes,
    pool: &Pool,
    intent: &Intent,
    mut body: BoxStream<'_, Result<Bytes>>,
    region: Option<&str>,
) -> Result<()> {
    intent.validate()?;
    ensure!(
        intent.layout.data_shards == 1 && intent.layout.parity_shards == 0,
        "native streaming writes require a replicated layout"
    );
    let mut pending = Bytes::new();
    let mut final_receipts = None;
    for sequence in 0..intent.sequences() as u32 {
        let pieces = control.window(intent.operation, sequence).await?;
        ensure!(
            pieces.len() == intent.layout.width(),
            "native write plan is incomplete"
        );
        let forwarding = region.is_some()
            && pieces.len() == 2
            && pieces[0].region.is_some()
            && pieces[0].region == pieces[1].region
            && pieces[0].region.as_deref() != region
            && pieces.iter().all(|piece| {
                intent.required(piece.mapping.header.copy, piece.mapping.header.shard)
            });
        let mut targets = Vec::new();
        let mut senders = Vec::new();
        let mut writes = Vec::new();
        let length = intent
            .total
            .saturating_sub(u64::from(sequence) * intent.span())
            .min(intent.span());
        for piece in pieces {
            let header = &piece.mapping.header;
            intent.accepts(header)?;
            if !intent.required(header.copy, header.shard) {
                continue;
            }
            let target = piece
                .destination
                .context("required storage write target is unavailable")?;
            ensure!(
                matches!(&target.authorization.grant.action,
                crate::model::storage_transfer::Action::Write { header: authorized } if authorized == header),
                "native destination differs from its mapping"
            );
            targets.push(target);
        }
        let groups = if forwarding {
            vec![targets]
        } else {
            targets.into_iter().map(|target| vec![target]).collect()
        };
        for mut targets in groups {
            let (send, receive) = mpsc::channel::<Bytes>(2);
            senders.push(send);
            writes.push(async move {
                let body = futures_util::stream::unfold(receive, |mut receive| async move {
                    receive.recv().await.map(|bytes| (Ok(bytes), receive))
                })
                .boxed();
                if targets.len() == 1 {
                    Ok(vec![
                        pool.write(targets.pop().unwrap(), body, control).await?,
                    ])
                } else {
                    pool.forward(targets, body, control)
                        .await
                        .map_err(|error| error.context(crate::model::Error::StorageForwardFailed))
                }
            });
        }
        ensure!(!writes.is_empty(), "native write has no required fragments");
        let feed = async {
            let mut left = length;
            while left > 0 {
                if pending.is_empty() {
                    pending = body
                        .next()
                        .await
                        .context("native object ended before its planned length")??;
                    continue;
                }
                let count = pending.len().min(UNIT).min(left as usize);
                let part = pending.split_to(count);
                for send in &senders {
                    send.send(part.clone())
                        .await
                        .context("native storage writer stopped")?;
                }
                left -= count as u64;
            }
            drop(senders);
            Ok::<_, anyhow::Error>(())
        };
        let writing = futures_util::future::try_join_all(writes);
        tokio::pin!(feed, writing);
        let completed = tokio::select! {
            result = &mut writing => {
                let result = result?;
                feed.await?;
                result
            }
            fed = &mut feed => {
                let result = writing.await?;
                fed?;
                result
            }
        };
        let completed = completed.into_iter().flatten().collect::<Vec<_>>();
        if u64::from(sequence) + 1 == intent.sequences() {
            final_receipts = Some(completed);
        } else {
            control.accept(intent.operation, &completed).await?;
        }
    }
    ensure!(
        pending.is_empty(),
        "native object exceeds its planned length"
    );
    while let Some(bytes) = body.next().await {
        ensure!(
            bytes?.is_empty(),
            "native object exceeds its planned length"
        );
    }
    control
        .accept(
            intent.operation,
            &final_receipts.context("native stream has no final receipts")?,
        )
        .await?;
    Ok(())
}
