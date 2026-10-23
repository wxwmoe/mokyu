use super::{Storage, profile::StorageProfile};
use chrono::{DateTime, Utc};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use uuid::Uuid;

pub(super) struct Backlog {
    oldest: HashMap<Uuid, DateTime<Utc>>,
    pressure: HashMap<Uuid, Instant>,
    refreshed: Instant,
}
impl Backlog {
    pub(super) fn new(rows: Vec<(Uuid, DateTime<Utc>)>) -> Self {
        Self {
            oldest: rows.into_iter().collect(),
            pressure: HashMap::new(),
            refreshed: Instant::now(),
        }
    }
}
impl Storage {
    pub(crate) async fn watch_write_backlog(&self) -> anyhow::Result<()> {
        loop {
            match self.physical.write_backlog().await {
                Ok(rows) => {
                    let mut state = self.write_backlog.lock().unwrap();
                    state.oldest = rows.into_iter().collect();
                    state.refreshed = Instant::now();
                    state.pressure.retain(|_, until| *until > Instant::now());
                }
                Err(error) => tracing::warn!(%error, "write backlog refresh will retry"),
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    pub(crate) fn write_overdue(&self, storage: Uuid, profile: &StorageProfile) -> bool {
        let state = self.write_backlog.lock().unwrap();
        state.refreshed.elapsed() > Duration::from_secs(5)
            || state.oldest.get(&storage).is_some_and(|oldest| {
                (Utc::now() - oldest).num_seconds()
                    >= profile.write_backlog_seconds().unwrap_or(300) as i64
            })
    }
    pub(crate) fn expedite_writes(&self, storage: Uuid) {
        self.write_backlog
            .lock()
            .unwrap()
            .pressure
            .insert(storage, Instant::now() + Duration::from_secs(60));
        self.wake_sync.notify_one();
    }
    pub(crate) fn urgent_writes(&self) -> Vec<Uuid> {
        self.write_backlog
            .lock()
            .unwrap()
            .pressure
            .iter()
            .filter_map(|(id, until)| (*until > Instant::now()).then_some(*id))
            .collect()
    }
    pub(crate) fn write_backlog_status(&self) -> serde_json::Value {
        let state = self.write_backlog.lock().unwrap();
        serde_json::json!({"refreshed_seconds_ago":state.refreshed.elapsed().as_secs(),"oldest":state.oldest})
    }
}
