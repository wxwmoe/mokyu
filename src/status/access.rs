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

pub struct Access {
    buffer: Mutex<AccessBatch>,
    limit: usize,
    pub wake: tokio::sync::Notify,
    gaps: AtomicU64,
}
impl Access {
    pub fn new(limit: usize) -> Self {
        Self {
            buffer: Mutex::new(AccessBatch::default()),
            limit,
            wake: tokio::sync::Notify::new(),
            gaps: AtomicU64::new(0),
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
        if buffer.chunks.len() + buffer.packs.len() + buffer.members.len() >= self.limit
            && !buffer.chunks.contains_key(&key)
        {
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
        if buffer.chunks.len() + buffer.packs.len() + buffer.members.len() + added > self.limit {
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
    async fn flush(&self, app: &App) -> Result<()> {
        let pending = self.take();
        app.metrics
            .flush_access(
                pending,
                (config::seconds(&app.config.metrics.access_flush_interval)? * 2) as f64,
            )
            .await?;
        Ok(())
    }
    pub(crate) fn take(&self) -> AccessBatch {
        std::mem::take(&mut *self.buffer.lock().unwrap())
    }
    pub(crate) fn lost(&self) {
        self.buffer.lock().unwrap().dropped = true;
        self.gaps.fetch_add(1, Relaxed);
    }
    pub(crate) fn snapshot(&self) -> serde_json::Value {
        let buffer = self.buffer.lock().unwrap();
        serde_json::json!({"pending":buffer.chunks.len() + buffer.packs.len() + buffer.members.len(),
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
