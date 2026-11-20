use super::transport::{Pool, Renewal};
use crate::{
    content::erasure,
    model::{
        storage_cluster::{Layout, MEMBER_LIMIT},
        storage_transfer::{Action, Destination},
    },
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt, stream::BoxStream};
use serde::{Deserialize, Serialize};
use std::{ops::Range, sync::Arc};

pub(crate) const PART: u64 = 512 * 1024;
pub(crate) const WINDOW: u64 = 8 * 1024 * 1024;

pub(crate) fn part_length(layout: &Layout) -> u64 {
    if layout.data_shards == 1 {
        PART
    } else {
        u64::from(layout.stripe_bytes).min(if layout.parity_shards == 0 {
            2 * 1024 * 1024
        } else {
            (WINDOW / u64::from(layout.data_shards)).min(2 * 1024 * 1024)
                / crate::content::fragment::UNIT as u64
                * crate::content::fragment::UNIT as u64
        })
    }
}

// Leave room for an unaligned first part and the largest signed destination URL.
pub(crate) fn window(layout: &Layout) -> u64 {
    let slots = if layout.parity_shards == 0 {
        usize::from(layout.extra_copies) + 1
    } else {
        layout.width()
    };
    WINDOW.min(part_length(layout) * (256usize / slots.max(1)).saturating_sub(1).max(1) as u64)
}

pub(crate) use crate::model::storage_native::Metadata;

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Part {
    pub offset: u64,
    pub length: u64,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub prefer_local: bool,
    pub copies: Vec<Destination>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recovery: Vec<Destination>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Plan {
    pub object: Metadata,
    pub lease: crate::model::storage_native::ReadLease,
    pub range: Option<Range<u64>>,
    pub parts: Vec<Part>,
    pub parallelism: u16,
    pub window: u64,
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
            (crate::content::fragment::UNIT as u64..=WINDOW).contains(&self.window)
                && !self.object.version.is_nil()
                && self.parts.len() <= 1 + WINDOW as usize / crate::content::fragment::UNIT
                && (1..=MEMBER_LIMIT).contains(&usize::from(self.parallelism)),
            "invalid native read plan"
        );
        let Some(range) = &self.range else {
            ensure!(self.parts.is_empty(), "metadata request returned data");
            return Ok(());
        };
        ensure!(
            range.start <= range.end
                && range.end <= self.object.size
                && range.end - range.start <= self.window,
            "invalid native read range"
        );
        let mut offset = range.start;
        for part in &self.parts {
            ensure!(
                part.offset == offset
                    && part.length > 0
                    && (!part.copies.is_empty() || !part.recovery.is_empty())
                    && part.copies.len() + part.recovery.len() <= MEMBER_LIMIT,
                "invalid native read part"
            );
            let mut generation = None;
            let mut independent = std::collections::HashSet::new();
            for (target, direct) in part
                .copies
                .iter()
                .map(|t| (t, true))
                .chain(part.recovery.iter().map(|t| (t, false)))
            {
                let Action::Read {
                    receipt,
                    offset,
                    length,
                } = &target.authorization.grant.action
                else {
                    anyhow::bail!("native read plan contains another action");
                };
                receipt.validate()?;
                let h = &receipt.header;
                let relative = part
                    .offset
                    .checked_sub(h.offset)
                    .context("invalid shard range")?;
                let (shard, start, contiguous) = erasure::locate(&h.layout, relative);
                let identity = (h.namespace, h.payload, h.revision, h.sequence, &h.layout);
                ensure!(
                    h.operation == self.object.version
                        && h.total == self.object.size
                        && h.layout.supported()
                        && part.length <= part_length(&h.layout)
                        && generation
                            .replace(identity)
                            .is_none_or(|old| old == identity)
                        && (h.shard == shard) == direct
                        && (direct || h.layout.parity_shards > 0)
                        && relative
                            .checked_add(part.length)
                            .is_some_and(|end| end <= h.length)
                        && *offset == start
                        && *length == part.length
                        && *length <= contiguous
                        && start
                            .checked_add(*length)
                            .is_some_and(|end| end <= h.shard_bytes),
                    "native read plan changed its payload or range"
                );
                independent.insert(h.shard);
            }
            ensure!(
                !part.copies.is_empty()
                    || independent.len() >= usize::from(generation.unwrap().4.data_shards),
                "native range has insufficient distinct shards"
            );
            offset = offset
                .checked_add(part.length)
                .context("native read offset overflow")?;
        }
        ensure!(offset == range.end, "native read plan has a gap");
        Ok(())
    }
}

pub(super) async fn part(
    pool: Arc<Pool>,
    renewal: Arc<dyn Renewal>,
    part: Part,
) -> Result<BoxStream<'static, Result<Bytes>>> {
    if !part.recovery.is_empty() || part.copies.is_empty() {
        let slots = Arc::new(tokio::sync::Semaphore::new(4));
        let bytes = coded(pool, renewal, part, slots).await?;
        return Ok(futures_util::stream::once(async { Ok(bytes) }).boxed());
    }
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
    ensure!(
        (1..=MEMBER_LIMIT).contains(&lanes),
        "invalid native read parallelism"
    );
    let lanes = lanes.min(plan.parallelism as usize);
    let slots = Arc::new(tokio::sync::Semaphore::new(lanes));
    let stalled = Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
    Ok(
        futures_util::stream::iter(plan.parts.into_iter().map(move |mut value| {
            let (pool, renewal, slots) = (pool.clone(), renewal.clone(), slots.clone());
            let stalled = stalled.clone();
            async move {
                if !value.recovery.is_empty() || value.copies.is_empty() {
                    return Ok(vec![coded(pool, renewal, value, slots).await?]);
                }
                let _permit = slots.acquire().await?;
                if value.prefer_local && value.copies.len() > 1 {
                    for target in &value.copies[..value.copies.len() - 1] {
                        let media = target.authorization.grant.media;
                        if stalled.lock().unwrap().contains(&media) {
                            continue;
                        }
                        if let Ok(chunks) = responsive(&pool, renewal.clone(), target).await {
                            return Ok(chunks);
                        }
                        stalled.lock().unwrap().insert(media);
                    }
                    // Retry slower copies without a stall deadline if the last alternative fails.
                    value.copies.rotate_right(1);
                }
                part(pool, renewal, value)
                    .await?
                    .try_collect::<Vec<_>>()
                    .await
            }
        }))
        .buffered(lanes)
        .map_ok(|bytes| futures_util::stream::iter(bytes.into_iter().map(Ok)))
        .try_flatten()
        .boxed(),
    )
}

async fn responsive(
    pool: &Pool,
    renewal: Arc<dyn Renewal>,
    target: &Destination,
) -> Result<Vec<Bytes>> {
    let stall = std::time::Duration::from_millis(250);
    let mut stream = tokio::time::timeout(stall, pool.read(target.clone(), renewal)).await??;
    let mut chunks = Vec::new();
    while let Some(bytes) = tokio::time::timeout(stall, stream.try_next()).await?? {
        chunks.push(bytes);
    }
    Ok(chunks)
}

async fn coded(
    pool: Arc<Pool>,
    renewal: Arc<dyn Renewal>,
    part: Part,
    slots: Arc<tokio::sync::Semaphore>,
) -> Result<Bytes> {
    let (first, _, _) = part
        .copies
        .first()
        .or(part.recovery.first())
        .context("missing fragment")?
        .authorization
        .grant
        .action
        .read_range()
        .context("fragment is not readable")?;
    let layout = first.header.layout.clone();
    let (wanted, _, _) = erasure::locate(
        &layout,
        part.offset
            .checked_sub(first.header.offset)
            .context("invalid shard range")?,
    );
    let fetch = |target: Destination| {
        let (pool, renewal, slots) = (pool.clone(), renewal.clone(), slots.clone());
        async move {
            let _permit = slots.acquire().await?;
            let (r, _, size) = target
                .authorization
                .grant
                .action
                .read_range()
                .context("read grant required")?;
            ensure!(
                size == part.length && size <= 2 * 1024 * 1024,
                "coded range exceeds budget"
            );
            let index = r.header.shard;
            let mut out = Vec::with_capacity(size as usize);
            let mut stream = pool.read(target, renewal).await?;
            while let Some(bytes) = stream.try_next().await? {
                out.extend_from_slice(&bytes);
            }
            ensure!(
                out.len() as u64 == size,
                "incomplete verified fragment range"
            );
            Ok::<_, anyhow::Error>((index, out))
        }
    };
    let read_direct = || async {
        let mut error = anyhow::anyhow!("data fragment is offline");
        for target in part.copies.iter().cloned() {
            match fetch(target).await {
                Ok((_, bytes)) => return Ok(Bytes::from(bytes)),
                Err(e) => error = e,
            }
        }
        Err(error)
    };
    let mut direct = Box::pin(read_direct());
    let enough = part
        .recovery
        .iter()
        .filter_map(|t| {
            t.authorization
                .grant
                .action
                .read_range()
                .map(|(r, _, _)| r.header.shard)
        })
        .collect::<std::collections::HashSet<_>>()
        .len()
        >= usize::from(layout.data_shards);
    if !enough {
        return direct.await;
    }
    let mut direct_done = false;
    tokio::select! {
        result = &mut direct => { if let Ok(bytes)=result { return Ok(bytes); } direct_done=true; },
        _ = tokio::time::sleep(std::time::Duration::from_millis(250)) => (),
    }
    let fallback = async {
        let mut pending = futures_util::stream::FuturesUnordered::new();
        let mut deferred = Vec::new();
        let mut preferred = std::collections::HashSet::new();
        let needed = usize::from(layout.data_shards);
        for target in part.recovery {
            let index = target
                .authorization
                .grant
                .action
                .read_range()
                .context("read grant required")?
                .0
                .header
                .shard;
            if part.prefer_local && (preferred.len() == needed || !preferred.insert(index)) {
                deferred.push(target);
            } else {
                pending.push(fetch(target));
            }
        }
        let hedge = tokio::time::sleep(std::time::Duration::from_millis(20));
        tokio::pin!(hedge);
        let mut shards = vec![None; usize::from(layout.data_shards + layout.parity_shards)];
        let mut good = 0;
        let mut last = None;
        while !pending.is_empty() {
            let result = tokio::select! {
                Some(result) = pending.next() => result,
                _ = &mut hedge, if !deferred.is_empty() => {
                    pending.extend(deferred.drain(..).map(&fetch));
                    continue;
                }
            };
            let (index, bytes) = match result {
                Ok(v) => v,
                Err(e) => {
                    last = Some(e);
                    pending.extend(deferred.drain(..).map(&fetch));
                    continue;
                }
            };
            if shards[usize::from(index)].is_none() {
                shards[usize::from(index)] = Some(bytes);
                good += 1;
                if good == usize::from(layout.data_shards) {
                    return Ok(Bytes::from(erasure::restore(&layout, shards, wanted)?));
                }
            }
        }
        Err(last
            .unwrap_or_else(|| anyhow::anyhow!("too few distinct indices"))
            .context(format!(
                "only {good} verified fragments for {}-piece reconstruction",
                layout.data_shards
            )))
    };
    tokio::pin!(fallback);
    if direct_done || slots.available_permits() == 0 {
        // A stalled preferred read cannot hold the last lane needed to reconstruct it.
        drop(direct);
        return match fallback.await {
            Err(_) if !direct_done => read_direct().await,
            result => result,
        };
    }
    tokio::select! {
        result=&mut direct => match result { Ok(bytes)=>Ok(bytes), Err(_)=>fallback.await },
        result=&mut fallback => match result { Ok(bytes)=>Ok(bytes), Err(_)=>direct.await },
    }
}
