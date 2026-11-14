use super::transport::{Pool, Renewal};
use crate::model::storage_transfer::{Action, Destination};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt, stream::BoxStream};
use serde::{Deserialize, Serialize};
use std::{ops::Range, sync::Arc};
use uuid::Uuid;

pub(crate) const PART: u64 = 512 * 1024;
pub(crate) const WINDOW: u64 = 8 * 1024 * 1024;

pub(crate) use crate::model::storage_native::Metadata;

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Part {
    pub offset: u64,
    pub length: u64,
    pub copies: Vec<Destination>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Plan {
    pub object: Metadata,
    pub lease: crate::model::storage_native::ReadLease,
    pub range: Option<Range<u64>>,
    pub parts: Vec<Part>,
    pub parallelism: u16,
}

impl Plan {
    pub fn validate(&self) -> Result<()> {
        crate::model::storage_native::path(&self.object.path)?;
        ensure!(
            self.lease.operation == self.object.version
                && self.lease.modified == self.object.modified,
            "native read lease differs from its metadata"
        );
        ensure!(
            !self.object.version.is_nil()
                && self.parts.len() <= 32
                && (1..=2).contains(&self.parallelism),
            "invalid native read plan"
        );
        let Some(range) = &self.range else {
            ensure!(self.parts.is_empty(), "metadata request returned data");
            return Ok(());
        };
        ensure!(
            range.start <= range.end
                && range.end <= self.object.size
                && range.end - range.start <= WINDOW,
            "invalid native read range"
        );
        let mut offset = range.start;
        for part in &self.parts {
            ensure!(
                part.offset == offset
                    && part.length > 0
                    && part.length <= PART
                    && !part.copies.is_empty()
                    && part.copies.len() <= 2,
                "invalid native read part"
            );
            for destination in &part.copies {
                let Action::Read {
                    receipt,
                    offset,
                    length,
                } = &destination.authorization.grant.action
                else {
                    anyhow::bail!("native read plan contains another action");
                };
                receipt.validate()?;
                ensure!(
                    receipt.header.operation == self.object.version
                        && receipt.header.total == self.object.size
                        && receipt.header.layout.data_shards == 1
                        && receipt.header.layout.parity_shards == 0
                        && receipt.header.offset.checked_add(*offset) == Some(part.offset)
                        && *length == part.length,
                    "native read plan changed its payload or range"
                );
            }
            offset = offset
                .checked_add(part.length)
                .context("native read offset overflow")?;
        }
        ensure!(offset == range.end, "native read plan has a gap");
        Ok(())
    }
}

async fn part(
    pool: Arc<Pool>,
    renewal: Arc<dyn Renewal>,
    part: Part,
) -> Result<BoxStream<'static, Result<Bytes>>> {
    let mut copies = part.copies.into_iter();
    let mut failure = None;
    let first = loop {
        let Some(target) = copies.next() else {
            return Err(failure.unwrap_or_else(|| anyhow::anyhow!("no readable storage copy")));
        };
        match pool.read(target, renewal.clone()).await {
            Ok(stream) => break stream,
            Err(error) => failure = Some(error),
        }
    };
    Ok(Box::pin(async_stream::try_stream! {
        let mut stream = first;
        let mut received = 0u64;
        let mut skip = 0u64;
        loop {
            let error = loop {
                match stream.try_next().await {
                    Ok(Some(mut bytes)) => {
                        let discarded = skip.min(bytes.len() as u64);
                        skip -= discarded;
                        bytes = bytes.slice(discarded as usize..);
                        received = received.checked_add(bytes.len() as u64).context("native read length overflow")?;
                        (received <= part.length).then_some(()).context("native read exceeds its part")?;
                        if !bytes.is_empty() { yield bytes; }
                    }
                    Ok(None) if received == part.length => break None,
                    Ok(None) => break Some(anyhow::anyhow!("native read copy is truncated")),
                    Err(error) => break Some(error),
                }
            };
            let Some(mut error) = error else { break; };
            // A replacement copy is independently verified; discard only bytes already delivered.
            stream = loop {
                let Some(target) = copies.next() else { Err(error)?; unreachable!(); };
                match pool.read(target, renewal.clone()).await {
                    Ok(stream) => break stream,
                    Err(next) => error = next,
                }
            };
            skip = received;
        }
    }))
}

pub(crate) fn read(
    pool: Arc<Pool>,
    renewal: Arc<dyn Renewal>,
    plan: Plan,
    lanes: usize,
) -> Result<BoxStream<'static, Result<Bytes>>> {
    plan.validate()?;
    ensure!((1..=2).contains(&lanes), "invalid native read parallelism");
    let lanes = lanes.min(plan.parallelism as usize);
    Ok(futures_util::stream::iter(
        plan.parts
            .into_iter()
            .map(move |value| prefetch(pool.clone(), renewal.clone(), value)),
    )
    .buffered(lanes)
    .map_ok(|bytes| futures_util::stream::iter(bytes.into_iter().map(Ok)))
    .try_flatten()
    .boxed())
}

async fn prefetch(pool: Arc<Pool>, renewal: Arc<dyn Renewal>, value: Part) -> Result<Vec<Bytes>> {
    part(pool, renewal, value).await?.try_collect().await
}
