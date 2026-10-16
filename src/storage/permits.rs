use anyhow::Result;
use std::collections::HashMap;
use std::sync::{
    Arc, Weak,
    atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering},
};
use std::time::Duration;
use uuid::Uuid;

pub(crate) struct SharedStorePermit {
    pub(crate) store: Arc<dyn crate::model::cluster::ClusterStore>,
    pub(crate) token: Uuid,
    pub(crate) alive: Arc<AtomicBool>,
}

impl Drop for SharedStorePermit {
    fn drop(&mut self) {
        self.alive.store(false, Ordering::Release);
        let store = self.store.clone();
        let token = self.token;
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let _ = store.release_storage_permit(token).await;
            });
        }
    }
}

pub(crate) struct StorePermit {
    _gate: Arc<StoreGate>,
    pub(crate) _local: crate::storage::resource_gate::Permit,
    pub(crate) _shared: Option<SharedStorePermit>,
}

pub(crate) struct StoreGate {
    pub(crate) local: Arc<crate::storage::resource_gate::Gate>,
    pub(crate) store: Option<Arc<dyn crate::model::cluster::ClusterStore>>,
    pub(crate) storage_id: Uuid,
    pub(crate) direction: &'static str,
    cluster_limit: AtomicI32,
    pub(crate) holder: Uuid,
}

pub(crate) type GateRegistry = HashMap<(Uuid, &'static str), Weak<StoreGate>>;

impl StoreGate {
    pub(crate) fn new(
        store: &Option<Arc<dyn crate::model::cluster::ClusterStore>>,
        storage_id: Uuid,
        direction: &'static str,
        local_limit: usize,
        cluster_limit: Option<i32>,
        aging: Duration,
        holder: Uuid,
    ) -> Arc<Self> {
        Arc::new(Self {
            local: crate::storage::resource_gate::Gate::new(local_limit, aging),
            store: store.clone(),
            storage_id,
            direction,
            cluster_limit: AtomicI32::new(cluster_limit.unwrap_or(0)),
            holder,
        })
    }

    pub(crate) fn reuse(self: &Arc<Self>, registry: &mut GateRegistry) -> Arc<Self> {
        let key = (self.storage_id, self.direction);
        if let Some(gate) = registry.get(&key).and_then(Weak::upgrade) {
            gate.cluster_limit.store(
                self.cluster_limit.load(Ordering::Acquire),
                Ordering::Release,
            );
            let (limit, aging) = self.local.settings();
            gate.local.configure(limit, aging);
            gate
        } else {
            registry.insert(key, Arc::downgrade(self));
            self.clone()
        }
    }

    async fn acquire_shared(&self) -> Result<Option<SharedStorePermit>> {
        let Some(store) = &self.store else {
            return Ok(None);
        };
        loop {
            let limit = self.cluster_limit.load(Ordering::Acquire);
            if limit == 0 {
                return Ok(None);
            }
            if let Some(token) = store
                .acquire_storage_permit(self.storage_id, self.direction, limit, self.holder)
                .await?
            {
                let alive = Arc::new(AtomicBool::new(true));
                let renew_alive = alive.clone();
                let renew_store = store.clone();
                tokio::spawn(async move {
                    let mut failures = 0u8;
                    while renew_alive.load(Ordering::Acquire) {
                        tokio::time::sleep(Duration::from_secs(20)).await;
                        if !renew_alive.load(Ordering::Acquire) {
                            break;
                        }
                        match renew_store.renew_storage_permit(token).await {
                            Ok(true) => failures = 0,
                            Ok(_) => {
                                renew_alive.store(false, Ordering::Release);
                                break;
                            }
                            Err(error) => {
                                failures = failures.saturating_add(1);
                                if failures >= 3 {
                                    tracing::warn!(%error, %token, "storage permit renewal stopped");
                                    let _ = renew_store.release_storage_permit(token).await;
                                    renew_alive.store(false, Ordering::Release);
                                    break;
                                }
                            }
                        }
                    }
                });
                return Ok(Some(SharedStorePermit {
                    store: store.clone(),
                    token,
                    alive,
                }));
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    pub(crate) async fn acquire(self: &Arc<Self>) -> Result<StorePermit> {
        self.acquire_with_priority(None).await
    }
    pub(crate) async fn acquire_with_priority(
        self: &Arc<Self>,
        priority: Option<Arc<AtomicUsize>>,
    ) -> Result<StorePermit> {
        let local = self.local.acquire_shared(priority).await?;
        let shared = self.acquire_shared().await?;
        Ok(StorePermit {
            _gate: self.clone(),
            _local: local,
            _shared: shared,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reload_retains_active_counts_and_disabled_targets_keep_their_gate() {
        let id = Uuid::new_v4();
        let holder = Uuid::new_v4();
        let mut registry = GateRegistry::new();
        let gate = StoreGate::new(
            &None,
            id,
            "upload",
            2,
            Some(2),
            Duration::from_secs(60),
            holder,
        )
        .reuse(&mut registry);
        let first = gate.acquire().await.unwrap();
        let second = gate.acquire().await.unwrap();
        assert!(first._shared.is_none() && second._shared.is_none());
        drop(gate);

        let reloaded = StoreGate::new(&None, id, "upload", 1, Some(1), Duration::ZERO, holder)
            .reuse(&mut registry);
        assert!(Arc::ptr_eq(&reloaded, &first._gate));
        assert_eq!(reloaded.local.settings(), (1, Duration::ZERO));
        let mut waiting = Box::pin(reloaded.acquire());
        assert!(futures_util::poll!(&mut waiting).is_pending());
        drop(first);
        assert!(futures_util::poll!(&mut waiting).is_pending());
        drop(second);
        let held = waiting.await.unwrap();
        let mut canceled = Box::pin(reloaded.acquire());
        assert!(futures_util::poll!(&mut canceled).is_pending());
        drop(canceled);
        let mut waiting = Box::pin(reloaded.acquire());
        assert!(futures_util::poll!(&mut waiting).is_pending());
        let raised = StoreGate::new(&None, id, "upload", 2, Some(2), Duration::ZERO, holder)
            .reuse(&mut registry);
        assert!(Arc::ptr_eq(&raised, &reloaded));
        let other = waiting.await.unwrap();
        assert_eq!(raised.local.snapshot()["running"], 2);
        let read = StoreGate::new(&None, id, "read", 1, Some(1), Duration::ZERO, holder)
            .reuse(&mut registry);
        assert!(read.acquire().await.unwrap()._shared.is_none());
        drop((held, other));
        assert_eq!(raised.local.snapshot()["running"], 0);
    }
}
