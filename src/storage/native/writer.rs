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
    async fn advance(
        &self,
        operation: Uuid,
        sequence: u32,
        previous: &[Completion],
        _sequence_ms: u32,
    ) -> Result<Vec<Piece>> {
        self.accept(operation, previous).await?;
        self.window(operation, sequence).await
    }
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

// Unconfirmed slots remain durable repair work after the foreground write.
pub(crate) async fn write(
    control: &dyn Writes,
    pool: &Pool,
    intent: &Intent,
    body: BoxStream<'_, Result<Bytes>>,
    region: Option<&str>,
) -> Result<()> {
    write_prepared(control, pool, intent, body, region, Vec::new()).await
}

pub(crate) async fn write_prepared(
    control: &dyn Writes,
    pool: &Pool,
    intent: &Intent,
    mut body: BoxStream<'_, Result<Bytes>>,
    region: Option<&str>,
    first_window: Vec<Piece>,
) -> Result<()> {
    intent.validate()?;
    ensure!(intent.layout.supported(), "unsupported native write layout");
    let rs = (intent.layout.parity_shards > 0)
        .then(|| crate::content::erasure::codec(&intent.layout))
        .transpose()?;
    let mut pending = Bytes::new();
    let mut window = std::collections::VecDeque::from(first_window);
    let mut receipts: Vec<Completion> = Vec::new();
    let mut sequence_ms = 0;
    for sequence in 0..intent.sequences() as u32 {
        if window.is_empty() {
            let next = if receipts.is_empty() {
                control.window(intent.operation, sequence).await?
            } else {
                control
                    .advance(intent.operation, sequence, &receipts, sequence_ms)
                    .await?
            };
            receipts.clear();
            ensure!(
                next.len() <= crate::model::storage_payload::BATCH_LIMIT,
                "native window exceeds fragment limit"
            );
            window = next.into();
        }
        let started = tokio::time::Instant::now();
        let pieces: Vec<_> = window
            .drain(..window.len().min(intent.layout.width()))
            .collect();
        ensure!(
            pieces.len() == intent.layout.width()
                && pieces.iter().enumerate().all(|(slot, p)| {
                    let h = &p.mapping.header;
                    h.sequence == sequence
                        && usize::from(h.copy)
                            * usize::from(intent.layout.data_shards + intent.layout.parity_shards)
                            + usize::from(h.shard)
                            == slot
                }),
            "native write plan is incomplete"
        );
        let mut groups: Vec<Vec<_>> = Vec::new();
        let mut routes = std::collections::HashMap::new();
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
            let route = piece
                .region
                .filter(|r| region.is_some() && Some(r.as_str()) != region);
            let slot = route
                .map(|r| {
                    *routes.entry((header.shard, r)).or_insert_with(|| {
                        groups.push(Vec::new());
                        groups.len() - 1
                    })
                })
                .unwrap_or_else(|| {
                    groups.push(Vec::new());
                    groups.len() - 1
                });
            groups[slot].push(target);
        }
        for mut targets in groups {
            if targets.len() > 1 {
                let seed = (intent.payload.as_u128() ^ u128::from(sequence)) as usize;
                let start = seed % targets.len();
                targets.rotate_left(start);
            }
            let (send, receive) = mpsc::channel::<Bytes>(2);
            let shard = targets[0]
                .authorization
                .grant
                .action
                .write_header()
                .context("native write grant required")?
                .shard;
            senders.push((shard, send));
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
            let data = usize::from(intent.layout.data_shards);
            let stripe = intent.layout.stripe_bytes as usize;
            let mut left = length;
            let parity_needed = senders.iter().any(|(shard, _)| usize::from(*shard) >= data);
            let mut parity = if parity_needed {
                vec![vec![0u8; stripe]; usize::from(intent.layout.parity_shards)]
            } else {
                Vec::new()
            };
            let zeros = Bytes::from_static(&[0; UNIT]);
            while left > 0 {
                for shard in 0..data {
                    let used = left.min(stripe as u64) as usize;
                    let stored = if rs.is_some() { stripe } else { used };
                    let mut pos = 0usize;
                    while pos < stored {
                        let part = if pos < used {
                            while pending.is_empty() {
                                pending = body
                                    .next()
                                    .await
                                    .context("native object ended before its planned length")??;
                            }
                            pending.split_to((used - pos).min(pending.len()).min(UNIT))
                        } else {
                            zeros.slice(..(stored - pos).min(UNIT))
                        };
                        if parity_needed {
                            let mut slices: Vec<_> = parity
                                .iter_mut()
                                .map(|p| &mut p[pos..pos + part.len()])
                                .collect();
                            rs.as_ref()
                                .unwrap()
                                .encode_single_sep(shard, &part, &mut slices)?;
                        }
                        for (_, send) in senders
                            .iter()
                            .filter(|(index, _)| usize::from(*index) == shard)
                        {
                            send.send(part.clone())
                                .await
                                .context("native storage writer stopped")?;
                        }
                        pos += part.len();
                    }
                    left -= used as u64;
                }
                for (i, buffer) in parity.iter().enumerate() {
                    for chunk in buffer.chunks(UNIT) {
                        let bytes = Bytes::copy_from_slice(chunk);
                        for (_, send) in senders
                            .iter()
                            .filter(|(index, _)| usize::from(*index) == data + i)
                        {
                            send.send(bytes.clone())
                                .await
                                .context("native parity writer stopped")?;
                        }
                    }
                }
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
        receipts.extend(completed.into_iter().flatten());
        sequence_ms = started.elapsed().as_millis().clamp(1, u128::from(u32::MAX)) as u32;
        if !window.is_empty()
            && receipts.iter().any(|r| {
                r.authorization.grant.expires
                    <= chrono::Utc::now().timestamp() + i64::from(sequence_ms).div_euclid(1000) + 5
            })
        {
            control.accept(intent.operation, &receipts).await?;
            receipts.clear();
        }
    }
    ensure!(
        pending.is_empty() && window.is_empty(),
        "native object exceeds its planned length"
    );
    while let Some(bytes) = body.next().await {
        ensure!(
            bytes?.is_empty(),
            "native object exceeds its planned length"
        );
    }
    control.accept(intent.operation, &receipts).await?;
    Ok(())
}
