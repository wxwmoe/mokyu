use crate::{
    app::App,
    model::notification::{Change, Consumer, Delivery, LIMIT},
};
use anyhow::{Context, Result, ensure};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
    },
    time::Duration,
};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio::time::Instant;
use uuid::Uuid;

const BATCH: usize = 32;
const BYTES: usize = 8 * 1024 * 1024;

pub(crate) struct Batch {
    pub(crate) deliveries: Vec<Delivery>,
    _memory: OwnedSemaphorePermit,
}

struct Outbox {
    id: Uuid,
    sender: mpsc::Sender<Batch>,
}

pub(crate) struct Subscription {
    hub: Arc<Hub>,
    consumer: Consumer,
    id: Uuid,
    pub(crate) receiver: mpsc::Receiver<Batch>,
    pub(crate) resets: watch::Receiver<u64>,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        let mut subscribers = self.hub.subscribers.lock().unwrap();
        if subscribers
            .get(&self.consumer)
            .is_some_and(|value| value.id == self.id)
        {
            subscribers.remove(&self.consumer);
        }
    }
}

pub(crate) struct Hub {
    subscribers: Mutex<HashMap<Consumer, Outbox>>,
    memory: Arc<Semaphore>,
    wake: Notify,
    resets: watch::Sender<u64>,
    connected: AtomicBool,
    sent: AtomicU64,
    applied: AtomicU64,
    backpressure: AtomicU64,
}

impl Default for Hub {
    fn default() -> Self {
        Self {
            subscribers: Mutex::default(),
            memory: Arc::new(Semaphore::new(BYTES)),
            wake: Notify::new(),
            resets: watch::channel(0).0,
            connected: AtomicBool::new(false),
            sent: AtomicU64::new(0),
            applied: AtomicU64::new(0),
            backpressure: AtomicU64::new(0),
        }
    }
}

impl Hub {
    pub(crate) fn subscribe(self: &Arc<Self>, consumer: Consumer) -> Result<Subscription> {
        let mut subscribers = self.subscribers.lock().unwrap();
        ensure!(
            subscribers.contains_key(&consumer) || subscribers.len() < 4095,
            "notification session limit reached"
        );
        let id = Uuid::new_v4();
        let (sender, receiver) = mpsc::channel(4);
        subscribers.insert(consumer, Outbox { id, sender });
        self.wake.notify_one();
        Ok(Subscription {
            hub: self.clone(),
            consumer,
            id,
            receiver,
            resets: self.resets.subscribe(),
        })
    }

    fn reset(&self, connected: bool) {
        self.connected.store(connected, Relaxed);
        self.resets
            .send_modify(|value| *value = value.wrapping_add(1));
        self.wake.notify_one();
    }

    fn consumers(&self) -> Vec<Consumer> {
        if self.memory.available_permits() < 64 * 1024 {
            return Vec::new();
        }
        self.subscribers
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, outbox)| outbox.sender.capacity() > 0)
            .map(|(consumer, _)| *consumer)
            .collect()
    }

    fn deliver(&self, consumer: Consumer, deliveries: Vec<Delivery>) {
        let subscribers = self.subscribers.lock().unwrap();
        let Some(outbox) = subscribers.get(&consumer) else {
            return;
        };
        for chunk in deliveries.chunks(BATCH) {
            let bytes: usize = chunk
                .iter()
                .map(|delivery| {
                    512 + match &delivery.change {
                        Change::Object { key, .. } => key.len(),
                        _ => 0,
                    }
                })
                .sum();
            let Ok(memory) = self.memory.clone().try_acquire_many_owned(bytes as u32) else {
                self.backpressure.fetch_add(chunk.len() as u64, Relaxed);
                continue;
            };
            let batch = Batch {
                deliveries: chunk.to_vec(),
                _memory: memory,
            };
            if outbox.sender.try_send(batch).is_ok() {
                self.sent.fetch_add(chunk.len() as u64, Relaxed);
            } else {
                self.backpressure.fetch_add(chunk.len() as u64, Relaxed);
            }
        }
    }

    pub(crate) fn applied(&self, count: u64) {
        self.applied.fetch_add(count, Relaxed);
    }

    pub(crate) fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({"listening":self.connected.load(Relaxed), "sessions":self.subscribers.lock().unwrap().len(),
            "queued_bytes":BYTES-self.memory.available_permits(), "sent":self.sent.load(Relaxed), "applied":self.applied.load(Relaxed), "backpressure":self.backpressure.load(Relaxed)})
    }
}

pub(crate) async fn run(app: Arc<App>) -> Result<()> {
    let listener = async {
        loop {
            let result: Result<()> = async {
                let mut listener = app.cluster()?.notification_changes().await?;
                app.notifications.reset(true);
                loop {
                    if !listener.next().await? {
                        app.notifications.reset(true);
                    }
                    app.notifications.wake.notify_one();
                }
            }
            .await;
            app.notifications.reset(false);
            if let Err(error) = result {
                tracing::warn!(%error, "notification subscription will retry");
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        #[allow(unreachable_code)]
        Ok::<(), anyhow::Error>(())
    };
    tokio::select! { result = listener => result, result = dispatch(&app) => result }
}

async fn dispatch(app: &App) -> Result<()> {
    let consumer = Consumer {
        node: app
            .node
            .get()
            .context("control identity is missing")?
            .node_id,
        generation: *app.node_boot.get().context("control boot is missing")?,
    };
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut maintenance = Instant::now();
    loop {
        tokio::select! { _ = tick.tick() => {}, _ = app.notifications.wake.notified() => {} }
        // The deadline is anchored at the first wake, even during sustained input.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let result: Result<()> = async {
            for _ in 0..8 {
                let mut consumers = app.notifications.consumers();
                consumers.push(consumer);
                let deliveries = app.cluster()?.claim_notifications(&consumers).await?;
                #[cfg(feature = "fault-injection")]
                if !deliveries.is_empty() {
                    crate::faults::point("notifications-claimed").await;
                }
                let full = deliveries.len() == LIMIT;
                let mut groups: HashMap<Consumer, Vec<Delivery>> = HashMap::new();
                for delivery in deliveries {
                    groups.entry(delivery.consumer).or_default().push(delivery);
                }
                let local = groups.remove(&consumer).unwrap_or_default();
                for (consumer, group) in groups {
                    app.notifications.deliver(consumer, group);
                }
                let mut applied = Vec::new();
                for delivery in local {
                    match &delivery.change {
                        Change::Object { bucket, key } => {
                            drop(app.public_reads.changing_object(*bucket, key))
                        }
                        Change::Bucket { .. } => drop(app.public_reads.changing()),
                        Change::Storage { storage } => {
                            app.public_reads.profile_changed(*storage);
                            app.storage.refresh_store(*storage).await?;
                        }
                    }
                    applied.push(delivery.id);
                }
                if !applied.is_empty() {
                    app.notifications.applied(
                        app.cluster()?
                            .acknowledge_notifications(consumer, &applied)
                            .await?,
                    );
                }
                if !full {
                    break;
                }
            }
            if maintenance.elapsed() >= Duration::from_secs(5) {
                if app.cluster()?.maintain_notifications().await? {
                    app.notifications.wake.notify_one();
                } else {
                    maintenance = Instant::now();
                }
            }
            Ok(())
        }
        .await;
        if let Err(error) = result {
            tracing::warn!(%error, "notification dispatch will retry");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn outboxes_bound_slow_consumers_and_keep_replacement_subscriptions() {
        let hub = Arc::new(Hub::default());
        let consumer = Consumer {
            node: Uuid::new_v4(),
            generation: Uuid::new_v4(),
        };
        let previous = hub.subscribe(consumer).unwrap();
        let mut current = hub.subscribe(consumer).unwrap();
        drop(previous);
        let deliveries = (0..LIMIT)
            .map(|_| Delivery {
                consumer,
                id: Uuid::new_v4(),
                change: Change::Object {
                    bucket: Uuid::new_v4(),
                    key: "x".repeat(1024),
                },
            })
            .collect::<Vec<_>>();
        hub.deliver(consumer, deliveries.clone());
        hub.deliver(consumer, deliveries);
        assert!(hub.consumers().is_empty());
        assert_eq!(hub.snapshot()["backpressure"], 128);
        let batch = current.receiver.recv().await.unwrap();
        assert_eq!(batch.deliveries.len(), BATCH);
        drop(batch);
        assert_eq!(hub.consumers(), vec![consumer]);
        drop(current);
        assert!(hub.consumers().is_empty());
        assert_eq!(hub.memory.available_permits(), BYTES);
    }
}
