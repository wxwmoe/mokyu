use anyhow::{Context, Result, ensure};
use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use uuid::Uuid;

pub(crate) const CHUNK: u8 = 0;
pub(crate) const PACK: u8 = 1;
pub(crate) const UNIT: i64 = 64;
pub(crate) const MAX_UNITS: u32 = 1024;
const MAX_DOMAINS: usize = 1024;
const COVER_SECONDS: f64 = 10.0;

struct Range {
    next: AtomicU64,
    last: u64,
}
impl Range {
    fn take(&self) -> Option<i64> {
        self.next
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
                (next <= self.last).then_some(next + 1)
            })
            .ok()
            .map(|value| value as i64)
    }
}

#[derive(Default)]
struct Forecast {
    sample: Option<(Instant, u64)>,
    rate: f64,
}
impl Forecast {
    fn units(&mut self, now: Instant, issued: u64) -> u32 {
        let previous = self.sample.replace((now, issued));
        let Some((at, count)) = previous else {
            return 1;
        };
        let elapsed = now.saturating_duration_since(at);
        if elapsed >= Duration::from_secs(60) {
            self.rate = 0.0;
            return 1;
        }
        if elapsed.is_zero() || issued < count {
            return 1;
        }
        let observed = (issued - count) as f64 / elapsed.as_secs_f64();
        self.rate = if self.rate == 0.0 {
            observed
        } else {
            self.rate * 0.5 + observed * 0.5
        };
        units(self.rate * COVER_SECONDS)
    }
}

fn units(demand: f64) -> u32 {
    if !demand.is_finite() || demand <= 0.0 {
        return 1;
    }
    (demand / UNIT as f64).ceil().clamp(1.0, MAX_UNITS as f64) as u32
}

#[derive(Default)]
struct Domain {
    range: RwLock<Option<Arc<Range>>>,
    issued: AtomicU64,
    refill: tokio::sync::Mutex<Forecast>,
}
impl Domain {
    fn take(&self) -> Option<i64> {
        let range = self.range.read().unwrap().clone()?;
        let value = range.take()?;
        self.issued.fetch_add(1, Ordering::Relaxed);
        Some(value)
    }
}

#[derive(Default)]
struct Domains {
    entries: HashMap<(Uuid, u8), Arc<Domain>>,
    order: VecDeque<(Uuid, u8)>,
}

#[derive(Default)]
pub(crate) struct Allocator {
    domains: Mutex<Domains>,
}
impl Allocator {
    pub(crate) async fn next<F, Fut>(&self, key: Uuid, type_tag: u8, reserve: F) -> Result<i64>
    where
        F: Fn(u32) -> Fut,
        Fut: Future<Output = Result<(i64, i64)>>,
    {
        ensure!(matches!(type_tag, CHUNK | PACK), "invalid nonce type");
        let domain = {
            let mut cache = self.domains.lock().unwrap();
            if let Some(domain) = cache.entries.get(&(key, type_tag)) {
                domain.clone()
            } else {
                if cache.entries.len() == MAX_DOMAINS
                    && let Some(oldest) = cache.order.pop_front()
                {
                    // Eviction burns unused reservations; outstanding users keep their own range.
                    cache.entries.remove(&oldest);
                }
                let domain = Arc::new(Domain::default());
                cache.entries.insert((key, type_tag), domain.clone());
                cache.order.push_back((key, type_tag));
                domain
            }
        };
        if let Some(value) = domain.take() {
            return Ok(value);
        }
        let mut forecast = domain.refill.lock().await;
        if let Some(value) = domain.take() {
            return Ok(value);
        }
        let requested = forecast.units(Instant::now(), domain.issued.load(Ordering::Relaxed));
        // The reservation is durable before any ID can reach an encryption operation.
        let (first, last) = reserve(requested).await?;
        ensure!(
            first > 0
                && last >= first
                && last - first < i64::from(MAX_UNITS) * UNIT
                && (last - first + 1) % UNIT == 0,
            "invalid nonce reservation"
        );
        *domain.range.write().unwrap() = Some(Arc::new(Range {
            next: AtomicU64::new(first as u64),
            last: last as u64,
        }));
        domain.take().context("nonce reservation is empty")
    }
}

pub(crate) fn id(nonce: Option<&[u8]>) -> Option<i64> {
    let nonce: &[u8; 12] = nonce?.try_into().ok()?;
    let value = i64::from_be_bytes(nonce[4..].try_into().ok()?);
    (value > 0).then_some(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicI64;

    #[test]
    fn forecast_has_bounded_units_and_decays_after_idle() {
        for (demand, expected) in [
            (1.0, 1),
            (64.0, 1),
            (65.0, 2),
            (5000.0, 79),
            (65536.0, 1024),
            (65537.0, 1024),
            (f64::NAN, 1),
        ] {
            assert_eq!(units(demand), expected);
        }
        let start = Instant::now();
        let mut forecast = Forecast::default();
        assert_eq!(forecast.units(start, 0), 1);
        assert_eq!(forecast.units(start + Duration::from_secs(1), 64), 10);
        assert_eq!(forecast.units(start + Duration::from_secs(61), 64), 1);
    }

    #[tokio::test]
    async fn concurrent_ranges_never_overlap_and_restart_burns_unused_ids() -> Result<()> {
        let allocator = Arc::new(Allocator::default());
        let high = Arc::new(AtomicI64::new(0));
        let key = Uuid::new_v4();
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..16 {
            let allocator = allocator.clone();
            let high = high.clone();
            tasks.spawn(async move {
                let mut ids = Vec::new();
                for _ in 0..1000 {
                    ids.push(
                        allocator
                            .next(key, CHUNK, |count| {
                                let high = high.clone();
                                async move {
                                    tokio::task::yield_now().await;
                                    let size = i64::from(count) * UNIT;
                                    let old = high.fetch_add(size, Ordering::SeqCst);
                                    Ok((old + 1, old + size))
                                }
                            })
                            .await?,
                    );
                }
                Ok::<_, anyhow::Error>(ids)
            });
        }
        let mut ids = std::collections::HashSet::new();
        while let Some(result) = tasks.join_next().await {
            for id in result?? {
                assert!(ids.insert(id));
            }
        }
        assert_eq!(ids.len(), 16000);
        let previous = high.load(Ordering::SeqCst);
        let next = Allocator::default()
            .next(key, CHUNK, |count| {
                let high = high.clone();
                async move {
                    let size = i64::from(count) * UNIT;
                    let old = high.fetch_add(size, Ordering::SeqCst);
                    Ok((old + 1, old + size))
                }
            })
            .await?;
        assert_eq!(next, previous + 1);
        Ok(())
    }

    #[tokio::test]
    async fn failed_reservations_are_never_published() -> Result<()> {
        let allocator = Allocator::default();
        let key = Uuid::new_v4();
        assert!(
            allocator
                .next(key, PACK, |_| async {
                    anyhow::bail!("commit outcome unknown")
                })
                .await
                .is_err()
        );
        assert_eq!(
            allocator
                .next(key, PACK, |_| async { Ok((65, 128)) })
                .await?,
            65
        );
        assert_eq!(
            allocator
                .next(key, CHUNK, |_| async { Ok((1, 64)) })
                .await?,
            1
        );
        assert_eq!(
            allocator
                .next(Uuid::new_v4(), PACK, |_| async { Ok((1, 64)) })
                .await?,
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn final_range_stops_at_the_signed_limit() -> Result<()> {
        let allocator = Allocator::default();
        let key = Uuid::new_v4();
        for expected in (i64::MAX - UNIT + 1)..=i64::MAX {
            let value = allocator
                .next(key, CHUNK, |_| async {
                    Ok((i64::MAX - UNIT + 1, i64::MAX))
                })
                .await?;
            assert_eq!(value, expected);
        }
        assert!(
            allocator
                .next(key, CHUNK, |_| async {
                    anyhow::bail!("capacity exhausted")
                })
                .await
                .is_err()
        );
        assert_eq!(
            id(Some(&crate::content::codec::typed_nonce(
                chrono::Utc::now(),
                i64::MAX,
                CHUNK
            )?)),
            Some(i64::MAX)
        );
        assert_eq!(id(Some(&[0; 12])), None);
        Ok(())
    }
}
