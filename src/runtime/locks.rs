use std::{
    collections::HashMap,
    hash::Hash,
    sync::{Arc, Mutex, Weak},
};

pub(crate) struct Locks<K>(Mutex<Entries<K>>);

struct Entries<K> {
    locks: HashMap<K, Weak<tokio::sync::Mutex<()>>>,
    remaining: usize,
}

impl<K> Default for Locks<K> {
    fn default() -> Self {
        Self(Mutex::new(Entries {
            locks: HashMap::new(),
            remaining: 0,
        }))
    }
}

impl<K: Eq + Hash> Locks<K> {
    pub(crate) fn get(&self, key: K) -> Arc<tokio::sync::Mutex<()>> {
        let mut entries = self.0.lock().unwrap();
        if entries.remaining == 0 {
            entries.locks.retain(|_, lock| lock.strong_count() > 0);
            let live = entries.locks.len().max(64);
            if entries.locks.capacity() > live.saturating_mul(4) {
                entries.locks.shrink_to(live);
            }
            entries.remaining = live;
        }
        // Count hits too, so a quiet hot key eventually reclaims expired peers.
        entries.remaining -= 1;
        if let Some(lock) = entries.locks.get(&key).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        entries.locks.insert(key, Arc::downgrade(&lock));
        lock
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn identity_survives_sweeps_and_cancelled_waiters() {
        let locks = Locks::default();
        let first = locks.get(1);
        let guard = first.lock().await;
        let waiting = tokio::spawn(locks.get(1).lock_owned());
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        for key in 2..2048 {
            assert!(locks.get(key).try_lock_owned().is_ok());
            assert!(Arc::ptr_eq(&first, &locks.get(1)));
        }
        waiting.abort();
        assert!(waiting.await.unwrap_err().is_cancelled());
        assert!(locks.get(1).try_lock_owned().is_err());
        drop(guard);
        assert!(locks.get(1).try_lock_owned().is_ok());
    }

    #[test]
    fn hot_hits_reclaim_idle_entries_and_capacity() {
        let locks = Locks::default();
        let live: Vec<_> = (0..4096).map(|key| locks.get(key)).collect();
        let first = live[0].clone();
        drop(live);
        for _ in 0..8192 {
            assert!(Arc::ptr_eq(&first, &locks.get(0)));
        }
        let entries = locks.0.lock().unwrap();
        assert_eq!(entries.locks.len(), 1);
        assert!(entries.locks.capacity() <= 256);
    }
}
