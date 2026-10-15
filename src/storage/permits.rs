use anyhow::Result;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use uuid::Uuid;

pub(crate) struct SharedStorePermit {
    pub(crate) store: Arc<dyn crate::model::StoragePermitStore>,
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
                let _ = store.release(token).await;
            });
        }
    }
}

pub(crate) struct StorePermit {
    pub(crate) _local: crate::storage::resource_gate::Permit,
    pub(crate) _shared: Option<SharedStorePermit>,
}

pub(crate) struct StoreGate {
    pub(crate) local: Arc<crate::storage::resource_gate::Gate>,
    pub(crate) store: Arc<dyn crate::model::StoragePermitStore>,
    pub(crate) storage_id: Uuid,
    pub(crate) direction: &'static str,
    pub(crate) cluster_limit: Option<i32>,
    pub(crate) holder: Uuid,
}

impl StoreGate {
    pub(crate) fn new(
        store: &Arc<dyn crate::model::StoragePermitStore>,
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
            cluster_limit,
            holder,
        })
    }

    pub(crate) async fn acquire_shared(&self, limit: i32) -> Result<SharedStorePermit> {
        loop {
            if let Some(token) = self
                .store
                .acquire(self.storage_id, self.direction, limit, self.holder)
                .await?
            {
                let alive = Arc::new(AtomicBool::new(true));
                let renew_alive = alive.clone();
                let store = self.store.clone();
                tokio::spawn(async move {
                    let mut failures = 0u8;
                    while renew_alive.load(Ordering::Acquire) {
                        tokio::time::sleep(Duration::from_secs(20)).await;
                        if !renew_alive.load(Ordering::Acquire) {
                            break;
                        }
                        match store.renew(token).await {
                            Ok(true) => failures = 0,
                            Ok(_) => {
                                renew_alive.store(false, Ordering::Release);
                                break;
                            }
                            Err(error) => {
                                failures = failures.saturating_add(1);
                                if failures >= 3 {
                                    tracing::warn!(%error, %token, "storage permit renewal stopped");
                                    let _ = store.release(token).await;
                                    renew_alive.store(false, Ordering::Release);
                                    break;
                                }
                            }
                        }
                    }
                });
                return Ok(SharedStorePermit {
                    store: self.store.clone(),
                    token,
                    alive,
                });
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    pub(crate) async fn acquire(self: &Arc<Self>) -> Result<StorePermit> {
        let local = self.local.acquire().await?;
        let shared = match self.cluster_limit {
            Some(limit) => Some(self.acquire_shared(limit).await?),
            None => None,
        };
        Ok(StorePermit {
            _local: local,
            _shared: shared,
        })
    }
}
