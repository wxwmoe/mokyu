use anyhow::{Result, ensure};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    ops::Range,
    sync::Arc,
};
use tokio::sync::{OnceCell, OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

pub(crate) const MAX_RANGE: u64 = 32 * 1024 * 1024;

struct Buffered {
    address: Uuid,
    storage: Uuid,
    file_size: u64,
    bytes: Vec<u8>,
    _memory: OwnedSemaphorePermit,
}

struct Group {
    range: Range<u64>,
    data: OnceCell<Option<Buffered>>,
}

#[derive(Default)]
pub(crate) struct Window {
    pub(crate) savings: i64,
    pub(crate) multiple: bool,
    groups: BTreeMap<(Uuid, u64, u64), Arc<Group>>,
}

pub(crate) fn runs<K: Copy + Ord>(
    members: impl IntoIterator<Item = (K, Range<u64>)>,
    mut visit: impl FnMut(&[(K, u64, u64)], Range<u64>),
) {
    let members: BTreeSet<_> = members
        .into_iter()
        .filter(|(_, span)| span.start < span.end && span.end - span.start <= MAX_RANGE)
        .map(|(pack, span)| (pack, span.start, span.end))
        .collect();
    let mut members = members.into_iter().peekable();
    while let Some(first) = members.next() {
        let (pack, start, mut end) = first;
        let mut adjacent = vec![first];
        while let Some(&(id, next, stop)) = members.peek() {
            if id != pack || next != end || stop - start > MAX_RANGE {
                break;
            }
            adjacent.push(members.next().unwrap());
            end = stop;
        }
        visit(&adjacent, start..end);
    }
}

impl Window {
    pub fn new(members: impl IntoIterator<Item = (Uuid, Range<u64>)>) -> Self {
        let mut groups = BTreeMap::new();
        let (mut gets, mut bytes) = (0u64, 0u64);
        let mut count = 0;
        runs(members, |adjacent, range| {
            count += adjacent.len();
            gets += 1;
            bytes += range.end - range.start;
            if adjacent.len() > 1 {
                let group = Arc::new(Group {
                    range,
                    data: OnceCell::new(),
                });
                groups.extend(adjacent.iter().map(|&member| (member, group.clone())));
            }
        });
        Self {
            groups,
            multiple: count > 1,
            savings: gets.saturating_sub(bytes.div_ceil(MAX_RANGE)) as i64,
        }
    }

    pub async fn read<F, Fut>(
        &self,
        pack: &crate::content::pack::Pack,
        span: Range<u64>,
        file_size: u64,
        memory: &Arc<Semaphore>,
        fetch: F,
    ) -> Result<Vec<u8>>
    where
        F: Fn(Range<u64>) -> Fut,
        Fut: Future<Output = Result<Vec<u8>>>,
    {
        if let Some(group) = self.groups.get(&(pack.location_id, span.start, span.end))
            && group.range.end <= file_size
            && let Some(storage) = pack.storage_id
        {
            let length = (group.range.end - group.range.start) as usize;
            let data = group
                .data
                .get_or_try_init(|| async {
                    // Other readers and writers may already own the budget. A single member can always progress.
                    let Ok(permit) = memory
                        .clone()
                        .try_acquire_many_owned(length.div_ceil(1024 * 1024) as u32)
                    else {
                        return Ok::<_, anyhow::Error>(None);
                    };
                    let bytes = fetch(group.range.clone()).await?;
                    ensure!(
                        bytes.len() == length,
                        crate::content::codec::IntegrityError::Length
                    );
                    Ok(Some(Buffered {
                        address: pack.address(),
                        storage,
                        file_size,
                        bytes,
                        _memory: permit,
                    }))
                })
                .await?;
            if let Some(data) = data
                && data.address == pack.address()
                && data.storage == storage
                && data.file_size == file_size
            {
                let from = (span.start - group.range.start) as usize;
                let to = (span.end - group.range.start) as usize;
                return Ok(data.bytes[from..to].to_vec());
            }
        }
        fetch(span).await
    }
}
