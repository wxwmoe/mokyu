use super::App;
use crate::model::CompressionObservation;
use std::{
    collections::HashSet,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
};
use tokio::sync::mpsc;
use uuid::Uuid;

type Key = (Uuid, i32, &'static str);

pub(crate) struct Observations {
    sender: mpsc::Sender<CompressionObservation>,
    pending: Mutex<HashSet<Key>>,
    written: AtomicU64,
    dropped: AtomicU64,
    failed: AtomicU64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observations_are_coalesced_and_bounded() {
        let (queue, receiver) = Observations::new();
        let first = CompressionObservation {
            stream: Uuid::new_v4(),
            version: 1,
            encoding: "zstd",
            size: 1,
        };
        for _ in 0..1000 {
            queue.submit(first);
        }
        assert_eq!(queue.snapshot()["pending"], 1);
        for _ in 0..1000 {
            queue.submit(CompressionObservation {
                stream: Uuid::new_v4(),
                ..first
            });
        }
        assert_eq!(queue.snapshot()["pending"], 256);
        assert_eq!(queue.snapshot()["dropped"], 745);
        drop(receiver);
        let (queue, receiver) = Observations::new();
        drop(receiver);
        queue.submit(first);
        assert_eq!(queue.snapshot()["pending"], 0);
        assert_eq!(queue.snapshot()["dropped"], 1);
    }
}

impl Observations {
    pub fn new() -> (Arc<Self>, mpsc::Receiver<CompressionObservation>) {
        // Fixed metadata stays below 128 KiB of the runtime reserve; no object buffers enter this queue.
        let (sender, receiver) = mpsc::channel(256);
        (
            Arc::new(Self {
                sender,
                pending: Mutex::new(HashSet::with_capacity(256)),
                written: 0.into(),
                dropped: 0.into(),
                failed: 0.into(),
            }),
            receiver,
        )
    }

    pub fn submit(&self, observation: CompressionObservation) {
        let key = (
            observation.stream,
            observation.version,
            observation.encoding,
        );
        let mut pending = self.pending.lock().unwrap();
        if pending.contains(&key) {
            return;
        }
        if pending.len() == 256 {
            self.dropped.fetch_add(1, Relaxed);
            return;
        }
        pending.insert(key);
        if self.sender.try_send(observation).is_err() {
            pending.remove(&key);
            self.dropped.fetch_add(1, Relaxed);
        }
    }

    pub fn start(app: Weak<App>, mut receiver: mpsc::Receiver<CompressionObservation>) {
        tokio::spawn(async move {
            while let Some(observation) = receiver.recv().await {
                let Some(app) = app.upgrade() else { break };
                let queue = &app.compression_observations;
                if app.maintenance.load(Relaxed) || app.restart_required.load(Relaxed) {
                    queue.dropped.fetch_add(1, Relaxed);
                } else {
                    // Native database barriers serialize this with maintenance and owner loss, without holding coord.
                    match tokio::time::timeout(
                        std::time::Duration::from_secs(2),
                        app.stream_store.learn_compression(
                            observation,
                            app.node.get().map(|node| node.node_id),
                        ),
                    )
                    .await
                    {
                        Ok(Ok(true)) => {
                            queue.written.fetch_add(1, Relaxed);
                        }
                        Ok(Ok(false)) => {}
                        _ => {
                            queue.failed.fetch_add(1, Relaxed);
                        }
                    }
                }
                queue.pending.lock().unwrap().remove(&(
                    observation.stream,
                    observation.version,
                    observation.encoding,
                ));
            }
        });
    }

    pub fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({"pending":self.pending.lock().unwrap().len(),"written":self.written.load(Relaxed),"dropped":self.dropped.load(Relaxed),"failed":self.failed.load(Relaxed)})
    }
}
