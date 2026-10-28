use crate::model::notification::Change;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::Notify;
use uuid::Uuid;

#[derive(Default)]
pub(super) struct Waiters(Mutex<HashMap<(Uuid, String), Weak<Notify>>>);

impl Waiters {
    pub(super) fn register(&self, bucket: Uuid, key: &str) -> Arc<Notify> {
        let mut waiters = self.0.lock().unwrap();
        waiters.retain(|_, waiter| waiter.strong_count() > 0);
        let waiter = waiters.entry((bucket, key.to_owned())).or_default();
        if let Some(notify) = waiter.upgrade() {
            return notify;
        }
        let notify = Arc::new(Notify::new());
        *waiter = Arc::downgrade(&notify);
        notify
    }

    pub(super) fn notify(&self, reset: bool, changes: &[Change]) {
        self.0.lock().unwrap().retain(|(bucket, key), waiter| {
            let Some(waiter) = waiter.upgrade() else {
                return false;
            };
            if reset
                || changes.iter().any(|change| match change {
                    Change::Object {
                        bucket: id,
                        key: name,
                    } => id == bucket && name == key,
                    Change::Bucket { bucket: id } => id == bucket,
                    Change::Storage { .. } => false,
                })
            {
                waiter.notify_waiters();
            }
            true
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn notices_target_registered_waiters_and_discard_canceled_entries() {
        let waiters = Waiters::default();
        let bucket = Uuid::now_v7();
        let other = Uuid::now_v7();
        let first = waiters.register(bucket, "first");
        let second = waiters.register(bucket, "second");
        let foreign = waiters.register(other, "first");
        assert!(Arc::ptr_eq(&first, &waiters.register(bucket, "first")));
        {
            let a = first.notified();
            let b = second.notified();
            let c = foreign.notified();
            tokio::pin!(a, b, c);
            a.as_mut().enable();
            b.as_mut().enable();
            c.as_mut().enable();
            waiters.notify(
                false,
                &[Change::Object {
                    bucket,
                    key: "first".into(),
                }],
            );
            assert!(futures_util::poll!(a.as_mut()).is_ready());
            assert!(futures_util::poll!(b.as_mut()).is_pending());
            assert!(futures_util::poll!(c.as_mut()).is_pending());
            waiters.notify(false, &[Change::Bucket { bucket }]);
            assert!(futures_util::poll!(b.as_mut()).is_ready());
            assert!(futures_util::poll!(c.as_mut()).is_pending());
            waiters.notify(true, &[]);
            assert!(futures_util::poll!(c.as_mut()).is_ready());
        }
        drop(first);
        drop(second);
        waiters.notify(false, &[]);
        assert_eq!(waiters.0.lock().unwrap().len(), 1);
        drop(foreign);
        let _next = waiters.register(bucket, "next");
        assert_eq!(waiters.0.lock().unwrap().len(), 1);
    }
}
