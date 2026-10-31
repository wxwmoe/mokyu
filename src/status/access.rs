use crate::model::Id;
use crate::{app::App, config};
use anyhow::Result;
use chrono::{DateTime, Timelike, Utc};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::Duration,
};

use crate::model::metrics::AccessBatch;

tokio::task_local! { pub(crate) static DERIVING: Arc<Mutex<DerivedRead>>; }

#[derive(Default)]
pub(crate) struct DerivedRead {
    sources: std::collections::HashMap<crate::model::tiering::Source, (usize, bool)>,
    cached: std::collections::BTreeMap<uuid::Uuid, i64>,
    bytes: usize,
}

impl DerivedRead {
    fn record_cached(&mut self, chunks: &std::collections::BTreeMap<uuid::Uuid, i64>) {
        for (&id, &bytes) in chunks {
            if bytes <= 0 || self.cached.len() >= 256 && !self.cached.contains_key(&id) {
                continue;
            }
            *self.cached.entry(id).or_default() += bytes;
            self.bytes += bytes as usize;
        }
    }
    fn record(&mut self, source: crate::model::tiering::Source, bytes: usize, origin: bool) {
        if self.sources.len() >= 256 && !self.sources.contains_key(&source) {
            return;
        }
        let entry = self.sources.entry(source).or_default();
        entry.0 += bytes;
        entry.1 |= origin;
        self.bytes += bytes;
    }
    pub(crate) fn served(&self, access: &Access, expected: i64) {
        if !access.collecting() {
            return;
        }
        if i64::try_from(self.bytes).ok() != Some(expected) {
            access.lost();
            return;
        }
        let mut req = DemandRead::default();
        for (&source, &(bytes, origin)) in &self.sources {
            access.demand(&mut req, source, bytes, origin);
        }
        access.cached(self.cached.clone());
    }
}

pub struct Access {
    buffer: Mutex<AccessBatch>,
    limit: usize,
    pub wake: tokio::sync::Notify,
    gaps: AtomicU64,
    collecting: std::sync::atomic::AtomicBool,
}
#[derive(Default)]
pub(crate) struct DemandRead(
    std::collections::HashMap<(crate::model::tiering::Unit, uuid::Uuid), bool>,
);
impl DemandRead {
    fn count(
        &mut self,
        key: (crate::model::tiering::Unit, uuid::Uuid),
        origin: bool,
    ) -> (i64, i64) {
        if let Some(seen) = self.0.get_mut(&key) {
            let new_origin = origin && !*seen;
            *seen |= origin;
            return (0, i64::from(new_origin));
        }
        // Untracked units still receive bytes and a new last-read time, never extra promotion votes.
        if self.0.len() >= 256 {
            return (0, 0);
        }
        self.0.insert(key, origin);
        (1, i64::from(origin))
    }
}
#[cfg(test)]
mod demand_tests {
    use super::*;
    use crate::model::tiering::{Source, Unit};
    #[tokio::test]
    async fn derived_reads_defer_cached_and_origin_demand_until_output() {
        let access = Access::new(32);
        access.collect(true);
        access.take();
        let state = Arc::new(Mutex::new(DerivedRead::default()));
        let chunk = uuid::Uuid::from_u128(100);
        let raw = uuid::Uuid::from_u128(101);
        DERIVING
            .scope(state.clone(), async {
                access.cached([(chunk, 7)].into_iter().collect());
                access.demand(&mut DemandRead::default(), Source::Raw(raw), 11, true);
            })
            .await;
        let batch = access.take();
        assert!(batch.cached.is_empty() && batch.demand.is_empty());
        state.lock().unwrap().served(&access, 18);
        let batch = access.take();
        assert_eq!(batch.cached.len(), 1);
        assert_eq!(batch.demand.len(), 1);
        assert!(!batch.dropped);
    }
    #[tokio::test]
    async fn served_spans_vote_once_and_only_for_the_member_read() {
        let access = Access::new(4);
        let pack = uuid::Uuid::from_u128(12);
        let member = uuid::Uuid::from_u128(13);
        let src = Source::Pack { pack, member };
        let mut req = DemandRead::default();
        access.demand(&mut req, src, 7, true);
        assert!(access.take().demand.is_empty());
        access.collect(true);
        DERIVING
            .scope(Default::default(), async {
                access.demand(&mut req, src, 60, true);
            })
            .await;
        access.demand(&mut req, src, 1, false);
        access.demand(&mut req, src, 2, true);
        access.demand(&mut req, src, 3, true);
        let batch = access.take();
        assert_eq!(batch.demand.len(), 2);
        for ((unit, id, _), d) in batch.demand {
            assert!((unit, id) == (Unit::Pack, pack) || (unit, id) == (Unit::Member, member));
            assert_eq!((d.bytes, d.reads, d.origins), (6, 1, 1));
        }
        let access = Access::new(1);
        access.collect(true);
        access.take();
        access.demand(&mut req, src, 5, false);
        let dropped = access.take();
        assert!(dropped.dropped && dropped.demand.is_empty());
    }
}
impl Access {
    pub fn new(limit: usize) -> Self {
        Self {
            buffer: Mutex::new(AccessBatch::default()),
            limit,
            wake: tokio::sync::Notify::new(),
            gaps: AtomicU64::new(0),
            collecting: false.into(),
        }
    }
    fn hour() -> DateTime<Utc> {
        Utc::now()
            .with_minute(0)
            .unwrap()
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap()
    }
    pub fn chunk(&self, id: Id, range: bool, bytes: usize, origin: bool) {
        let key = (id, Self::hour());
        let mut buffer = self.buffer.lock().unwrap();
        if buffer.len() >= self.limit && !buffer.chunks.contains_key(&key) {
            buffer.dropped = true;
            self.gaps.fetch_add(1, Relaxed);
            self.wake.notify_one();
            return;
        }
        let counts = buffer.chunks.entry(key).or_default();
        counts.reads += 1;
        counts.ranges += i64::from(range);
        counts.bytes += bytes as i64;
        counts.origins += i64::from(origin && range);
    }
    pub fn pack(
        &self,
        id: Id,
        bytes: usize,
        partial: bool,
        useful: usize,
        members: impl Iterator<Item = Id>,
    ) {
        let key = (id, Self::hour());
        let members: Vec<Id> = if partial {
            members.collect()
        } else {
            Vec::new()
        };
        let mut buffer = self.buffer.lock().unwrap();
        let added = usize::from(!buffer.packs.contains_key(&key))
            + members
                .iter()
                .map(|c| {
                    usize::from(!buffer.chunks.contains_key(&(*c, key.1)))
                        + usize::from(!buffer.members.contains_key(&(id, *c, key.1)))
                })
                .sum::<usize>();
        if buffer.len() + added > self.limit {
            buffer.dropped = true;
            self.gaps.fetch_add(1, Relaxed);
            self.wake.notify_one();
            return;
        }
        let counts = buffer.packs.entry(key).or_default();
        counts.downloads += 1;
        counts.bytes += bytes as i64;
        counts.partial += i64::from(partial);
        counts.partial_bytes += if partial { bytes as i64 } else { 0 };
        counts.useful += useful as i64;
        for c in members {
            *buffer.members.entry((id, c, key.1)).or_default() += 1;
            buffer.chunks.entry((c, key.1)).or_default().origins += 1;
        }
    }
    pub(crate) fn cached(&self, chunks: std::collections::BTreeMap<uuid::Uuid, i64>) {
        if chunks.is_empty() || !self.collecting() {
            return;
        }
        if DERIVING
            .try_with(|deferred| deferred.lock().unwrap().record_cached(&chunks))
            .is_ok()
        {
            return;
        }
        let (ids, bytes): (Vec<_>, Vec<_>) = chunks.into_iter().unzip();
        let key = (ids, Self::hour());
        let mut buffer = self.buffer.lock().unwrap();
        if let Some(d) = buffer.cached.get_mut(&key) {
            d.reads += 1;
            for (v, n) in d.bytes.iter_mut().zip(bytes) {
                *v += n;
            }
        } else if buffer.len() + key.0.len() + 1 <= self.limit {
            buffer.cached_slots += key.0.len() + 1;
            buffer
                .cached
                .insert(key, crate::model::tiering::CachedDemand { reads: 1, bytes });
        } else {
            buffer.dropped = true;
            self.gaps.fetch_add(1, Relaxed);
            self.wake.notify_one();
        }
    }
    pub(crate) fn demand(
        &self,
        request: &mut DemandRead,
        source: crate::model::tiering::Source,
        bytes: usize,
        origin: bool,
    ) {
        use crate::model::tiering::{Source, Unit};
        if bytes == 0
            || !self.collecting()
            || DERIVING
                .try_with(|deferred| deferred.lock().unwrap().record(source, bytes, origin))
                .is_ok()
        {
            return;
        }
        let at = Self::hour();
        let (unit, id, member) = match source {
            Source::Chunk(id) => (Unit::Chunk, id, None),
            Source::Raw(id) => (Unit::Raw, id, None),
            Source::Pack { pack, member } => (Unit::Pack, pack, Some(member)),
        };
        let keys = [
            Some((unit, id, at)),
            member.map(|id| (Unit::Member, id, at)),
        ];
        let mut buffer = self.buffer.lock().unwrap();
        let extra = keys
            .iter()
            .flatten()
            .filter(|key| !buffer.demand.contains_key(*key))
            .count();
        if buffer.len() + extra > self.limit {
            buffer.dropped = true;
            self.gaps.fetch_add(1, Relaxed);
            self.wake.notify_one();
            return;
        }
        for key in keys.into_iter().flatten() {
            let (reads, origins) = request.count((key.0, key.1), origin);
            let count = buffer.demand.entry(key).or_default();
            count.reads += reads;
            count.bytes += bytes as i64;
            count.origins += origins;
        }
    }
    async fn flush(&self, app: &App) -> Result<()> {
        self.collect(app.tiering.active().await?);
        let pending = self.take();
        app.metrics
            .flush_access(
                pending,
                (config::seconds(&app.config.metrics.access_flush_interval)? * 2) as f64,
            )
            .await?;
        if self.collecting()
            && !app.draining.load(Relaxed)
            && !app.maintenance.load(Relaxed)
            && app
                .tiering
                .schedule(i64::try_from(
                    config::seconds(&app.config.metrics.access_flush_interval)? * 2,
                )?)
                .await?
                > 0
        {
            app.wake_tasks.notify_waiters();
        }
        Ok(())
    }
    pub(crate) fn take(&self) -> AccessBatch {
        let mut batch = std::mem::take(&mut *self.buffer.lock().unwrap());
        batch.collecting = self.collecting();
        batch
    }
    pub(crate) fn collecting(&self) -> bool {
        self.collecting.load(Relaxed)
    }
    pub(crate) fn collect(&self, enabled: bool) {
        if !self.collecting.swap(enabled, Relaxed) && enabled {
            self.lost();
        }
    }
    pub(crate) fn lost(&self) {
        self.buffer.lock().unwrap().dropped = true;
        self.gaps.fetch_add(1, Relaxed);
    }
    pub(crate) fn snapshot(&self) -> serde_json::Value {
        let buffer = self.buffer.lock().unwrap();
        serde_json::json!({"pending":buffer.len(),
            "gaps":self.gaps.load(Relaxed)})
    }
}
pub async fn run(app: Arc<App>) -> Result<()> {
    let period = Duration::from_secs(config::seconds(&app.config.metrics.access_flush_interval)?);
    loop {
        tokio::select! {_=tokio::time::sleep(period)=>{},_=app.storage.access.wake.notified()=>{}}
        if let Err(e) = app.storage.access.flush(&app).await {
            app.storage.access.lost();
            tracing::warn!(error=%e,"access statistics flush failed; observation coverage has a gap");
        }
        app.metrics
            .expire_access(
                config::seconds(&app.config.metrics.access_retention)? as f64,
                app.config.maintenance.batch_size as i64,
            )
            .await?;
    }
}
