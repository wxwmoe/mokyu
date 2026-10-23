use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

tokio::task_local! { pub static PRIORITY: usize; }
pub const FOREGROUND: usize = 0;
pub const UPLOAD: usize = 1;
pub const MAINTENANCE: usize = 2;

struct Waiting {
    since: Instant,
    send: oneshot::Sender<Permit>,
    priority: Option<Arc<AtomicUsize>>,
}
struct State {
    limit: usize,
    aging: Duration,
    foreground: bool,
    active: usize,
    background: usize,
    queues: [VecDeque<Waiting>; 3],
}
pub struct Gate {
    state: Mutex<State>,
    waiting: Arc<crate::status::stats::Counters>,
}
pub struct Permit {
    gate: Option<Arc<Gate>>,
    background: bool,
}
impl Gate {
    pub fn new(limit: usize, aging: Duration) -> Arc<Self> {
        assert!(limit > 0);
        Arc::new(Self {
            waiting: Arc::default(),
            state: Mutex::new(State {
                limit,
                aging,
                foreground: false,
                active: 0,
                background: 0,
                queues: std::array::from_fn(|_| VecDeque::new()),
            }),
        })
    }
    fn dispatch(self: &Arc<Self>, state: &mut State) {
        for queue in &mut state.queues {
            queue.retain(|w| !w.send.is_closed());
        }
        for i in 1..3 {
            let mut kept = VecDeque::new();
            while let Some(waiting) = state.queues[i].pop_front() {
                let priority = waiting
                    .priority
                    .as_ref()
                    .map_or(i, |p| p.load(Ordering::Relaxed).min(i));
                if priority < i {
                    state.queues[priority].push_back(waiting);
                } else {
                    kept.push_back(waiting);
                }
            }
            state.queues[i] = kept;
        }
        while state.active < state.limit {
            let background_limit = if state.foreground {
                (state.limit / 2).max(1)
            } else {
                state.limit
            };
            let reserve = state.foreground
                && state.limit > 1
                && state.background == 0
                && state.active + 1 == state.limit
                && state.queues[1..].iter().any(|queue| !queue.is_empty());
            let eligible = |i: usize| {
                !state.queues[i].is_empty()
                    && if i == FOREGROUND {
                        !reserve
                    } else {
                        state.background < background_limit
                    }
            };
            let aged = (0..3)
                .filter(|&i| {
                    eligible(i)
                        && state.queues[i]
                            .front()
                            .is_some_and(|w| w.since.elapsed() >= state.aging)
                })
                .min_by_key(|&i| state.queues[i].front().unwrap().since);
            let Some(i) = aged.or_else(|| (0..3).find(|&i| eligible(i))) else {
                break;
            };
            let waiting = state.queues[i].pop_front().unwrap();
            state.active += 1;
            let background = i != FOREGROUND;
            state.background += usize::from(background);
            if let Err(mut permit) = waiting.send.send(Permit {
                gate: Some(self.clone()),
                background,
            }) {
                permit.gate = None;
                state.active -= 1;
                state.background -= usize::from(background);
            }
        }
    }
    pub async fn acquire(self: &Arc<Self>) -> anyhow::Result<Permit> {
        self.acquire_shared(None).await
    }
    pub fn refresh(self: &Arc<Self>) {
        self.dispatch(&mut self.state.lock().unwrap());
    }
    pub(crate) fn settings(&self) -> (usize, Duration, bool) {
        let state = self.state.lock().unwrap();
        (state.limit, state.aging, state.foreground)
    }
    pub(crate) fn configure(self: &Arc<Self>, limit: usize, aging: Duration, foreground: bool) {
        assert!(limit > 0);
        let mut state = self.state.lock().unwrap();
        state.limit = limit;
        state.aging = aging;
        state.foreground = foreground;
        self.dispatch(&mut state);
    }
    pub async fn acquire_shared(
        self: &Arc<Self>,
        shared: Option<Arc<AtomicUsize>>,
    ) -> anyhow::Result<Permit> {
        let mut waiting = self.waiting.begin();
        let (send, receive) = oneshot::channel();
        {
            let mut state = self.state.lock().unwrap();
            let priority = shared
                .as_ref()
                .map_or_else(
                    || PRIORITY.try_with(|p| *p).unwrap_or(FOREGROUND),
                    |p| p.load(Ordering::Relaxed),
                )
                .min(MAINTENANCE);
            state.queues[priority].push_back(Waiting {
                since: Instant::now(),
                send,
                priority: shared,
            });
            self.dispatch(&mut state);
        }
        let permit = receive.await?;
        waiting.finish(false);
        Ok(permit)
    }
    pub fn snapshot(&self) -> serde_json::Value {
        let state = self.state.lock().unwrap();
        serde_json::json!({"limit":state.limit,"running":state.active,"running_background":state.background,"foreground":state.foreground,"waiting":self.waiting.snapshot(),
            "queued":{"foreground":state.queues[FOREGROUND].iter().filter(|w|!w.send.is_closed()).count(),"upload":state.queues[UPLOAD].iter().filter(|w|!w.send.is_closed()).count(),"maintenance":state.queues[MAINTENANCE].iter().filter(|w|!w.send.is_closed()).count()},
            "oldest_wait_seconds":state.queues.iter().flat_map(|q|q.iter()).filter(|w|!w.send.is_closed()).map(|w|w.since.elapsed().as_secs()).max().unwrap_or(0)})
    }
}
impl Drop for Permit {
    fn drop(&mut self) {
        if let Some(gate) = self.gate.take() {
            let mut state = gate.state.lock().unwrap();
            state.active -= 1;
            state.background -= usize::from(self.background);
            gate.dispatch(&mut state);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn foreground_policy_reserves_both_sides_and_survives_reload() {
        let gate = Gate::new(4, Duration::from_secs(60));
        gate.configure(4, Duration::from_secs(60), true);
        let first = PRIORITY.scope(UPLOAD, gate.acquire()).await.unwrap();
        let second = PRIORITY.scope(MAINTENANCE, gate.acquire()).await.unwrap();
        let mut third = Box::pin(PRIORITY.scope(UPLOAD, gate.acquire()));
        assert!(futures_util::poll!(&mut third).is_pending());
        let front = gate.acquire().await.unwrap();
        assert_eq!(gate.snapshot()["running_background"], 2);
        gate.configure(1, Duration::ZERO, true);
        drop((first, second));
        assert!(futures_util::poll!(&mut third).is_pending());
        drop(front);
        drop(third.await.unwrap());
        assert_eq!(gate.snapshot()["running"], 0);

        gate.configure(2, Duration::from_secs(60), true);
        let one = gate.acquire().await.unwrap();
        let two = gate.acquire().await.unwrap();
        let mut foreground = Box::pin(gate.acquire());
        let mut background = Box::pin(PRIORITY.scope(UPLOAD, gate.acquire()));
        assert!(futures_util::poll!(&mut foreground).is_pending());
        assert!(futures_util::poll!(&mut background).is_pending());
        drop(one);
        let reserved = background.await.unwrap();
        assert!(futures_util::poll!(&mut foreground).is_pending());
        drop(two);
        drop(foreground.await.unwrap());
        drop(reserved);
    }
    #[tokio::test]
    async fn prioritizes_waiters_without_bypassing_limit_and_discards_cancelled() {
        let gate = Gate::new(1, Duration::from_secs(60));
        let held = gate.acquire().await.unwrap();
        let mut background = Box::pin(PRIORITY.scope(MAINTENANCE, gate.acquire()));
        assert!(futures_util::poll!(&mut background).is_pending());
        let mut foreground = Box::pin(gate.acquire());
        assert!(futures_util::poll!(&mut foreground).is_pending());
        let mut cancelled = Box::pin(gate.acquire());
        assert!(futures_util::poll!(&mut cancelled).is_pending());
        drop(held);
        let first = foreground.await.unwrap();
        assert!(futures_util::poll!(&mut background).is_pending());
        drop(cancelled);
        assert_eq!(gate.snapshot()["waiting"]["canceled"], 1);
        drop(first);
        assert!(background.await.is_ok());
        let gate = Gate::new(1, Duration::ZERO);
        let held = gate.acquire().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(1), gate.acquire())
                .await
                .is_err()
        );
        drop(held);
        assert!(gate.acquire().await.is_ok());
        assert_eq!(gate.snapshot()["waiting"]["canceled"], 1);
    }
    #[tokio::test]
    async fn promotes_shared_downloads_and_ages_old_waiters() {
        let gate = Gate::new(1, Duration::from_secs(60));
        let held = gate.acquire().await.unwrap();
        let priority = Arc::new(AtomicUsize::new(MAINTENANCE));
        let mut shared = Box::pin(gate.acquire_shared(Some(priority.clone())));
        assert!(futures_util::poll!(&mut shared).is_pending());
        let mut upload = Box::pin(PRIORITY.scope(UPLOAD, gate.acquire()));
        assert!(futures_util::poll!(&mut upload).is_pending());
        priority.store(FOREGROUND, Ordering::Relaxed);
        gate.refresh();
        drop(held);
        let first = shared.await.unwrap();
        assert!(futures_util::poll!(&mut upload).is_pending());
        drop(first);
        drop(upload.await.unwrap());

        let held = gate.acquire().await.unwrap();
        let mut background = Box::pin(PRIORITY.scope(MAINTENANCE, gate.acquire()));
        assert!(futures_util::poll!(&mut background).is_pending());
        gate.state.lock().unwrap().queues[MAINTENANCE][0].since -= Duration::from_secs(61);
        let mut foreground = Box::pin(gate.acquire());
        assert!(futures_util::poll!(&mut foreground).is_pending());
        drop(held);
        let aged = background.await.unwrap();
        assert!(futures_util::poll!(&mut foreground).is_pending());
        drop(aged);
        assert!(foreground.await.is_ok());
    }
}
