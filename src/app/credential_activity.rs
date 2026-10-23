use super::App;
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::{Duration, Instant},
};
use tokio::sync::mpsc;

struct Entry {
    pending: bool,
    next: Instant,
}

pub(crate) struct Activity {
    sender: mpsc::Sender<String>,
    entries: Mutex<HashMap<String, Entry>>,
    completed: AtomicU64,
    dropped: AtomicU64,
    failed: AtomicU64,
}

impl Activity {
    pub fn new() -> (Arc<Self>, mpsc::Receiver<String>) {
        // Verified identifiers and queue metadata stay within the runtime reserve, below 128 KiB.
        let (sender, receiver) = mpsc::channel(256);
        (
            Arc::new(Self {
                sender,
                entries: Mutex::new(HashMap::with_capacity(256)),
                completed: 0.into(),
                dropped: 0.into(),
                failed: 0.into(),
            }),
            receiver,
        )
    }

    pub fn record(&self, access: &str) {
        let now = Instant::now();
        let mut entries = self.entries.lock().unwrap();
        if entries
            .get(access)
            .is_some_and(|entry| entry.pending || entry.next > now)
        {
            return;
        }
        if entries.len() == 256 {
            entries.retain(|_, entry| entry.pending || entry.next > now);
        }
        if entries.len() == 256 || access.len() > 128 {
            self.dropped.fetch_add(1, Relaxed);
            return;
        }
        if self.sender.try_send(access.to_owned()).is_err() {
            self.dropped.fetch_add(1, Relaxed);
            return;
        }
        entries.insert(
            access.to_owned(),
            Entry {
                pending: true,
                next: now,
            },
        );
    }

    fn finish(&self, access: &str, success: bool) {
        if let Some(entry) = self.entries.lock().unwrap().get_mut(access) {
            entry.pending = false;
            entry.next = Instant::now() + Duration::from_secs(if success { 300 } else { 1 });
        }
    }

    pub fn start(app: Weak<App>, mut receiver: mpsc::Receiver<String>) {
        tokio::spawn(async move {
            while let Some(access) = receiver.recv().await {
                let Some(app) = app.upgrade() else { break };
                let queue = &app.credential_activity;
                if app.maintenance.load(Relaxed)
                    || app.draining.load(Relaxed)
                    || app.ensure_mode().is_err()
                {
                    queue.dropped.fetch_add(1, Relaxed);
                    queue.finish(&access, false);
                    continue;
                }
                let result = tokio::time::timeout(Duration::from_secs(2), async {
                    #[cfg(feature = "fault-injection")]
                    crate::faults::point("credential-activity-before-write").await;
                    app.credentials.touch_credential(&access).await
                })
                .await;
                let success = matches!(result, Ok(Ok(())));
                if success {
                    queue.completed.fetch_add(1, Relaxed);
                } else {
                    queue.failed.fetch_add(1, Relaxed);
                }
                queue.finish(&access, success);
            }
        });
    }

    pub fn snapshot(&self) -> serde_json::Value {
        let entries = self.entries.lock().unwrap();
        serde_json::json!({"entries":entries.len(),"pending":entries.values().filter(|entry|entry.pending).count(),"completed":self.completed.load(Relaxed),"dropped":self.dropped.load(Relaxed),"failed":self.failed.load(Relaxed)})
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activity_is_bounded_coalesced_and_retried_after_failure() {
        let (queue, mut receiver) = Activity::new();
        for _ in 0..1000 {
            queue.record("first");
        }
        assert_eq!(queue.snapshot()["pending"], 1);
        assert_eq!(receiver.try_recv().unwrap(), "first");
        queue.finish("first", true);
        queue.record("first");
        assert!(receiver.try_recv().is_err());
        assert_eq!(queue.snapshot()["pending"], 0);
        queue.finish("first", false);
        queue.record("first");
        assert!(receiver.try_recv().is_err());
        queue.entries.lock().unwrap().get_mut("first").unwrap().next = Instant::now();
        queue.record("first");
        assert_eq!(receiver.try_recv().unwrap(), "first");
        for n in 0..1000 {
            queue.record(&format!("other-{n}"));
        }
        assert_eq!(queue.snapshot()["pending"], 256);
        assert_eq!(queue.snapshot()["entries"], 256);
        assert_eq!(queue.snapshot()["dropped"], 745);

        let (queue, receiver) = Activity::new();
        queue.record(&"x".repeat(129));
        assert_eq!(queue.snapshot()["entries"], 0);
        drop(receiver);
        queue.record("first");
        assert_eq!(queue.snapshot()["pending"], 0);
        assert_eq!(queue.snapshot()["dropped"], 2);
    }
}
