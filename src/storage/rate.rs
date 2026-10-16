use std::time::Instant;

struct Bucket {
    tokens: f64,
    rate: f64,
    updated: Instant,
}

#[derive(Default)]
pub(crate) struct IntegrityRate {
    requests: Option<Bucket>,
    bytes: Option<Bucket>,
}

fn available(bucket: &mut Option<Bucket>, rate: Option<f64>, amount: f64, now: Instant) -> f64 {
    let Some(rate) = rate else {
        return f64::INFINITY;
    };
    // A batch larger than one second's budget must eventually make progress.
    let capacity = rate.max(amount).max(1.0);
    let bucket = bucket.get_or_insert(Bucket {
        tokens: capacity,
        rate,
        updated: now,
    });
    bucket.tokens = (bucket.tokens
        + now.duration_since(bucket.updated).as_secs_f64() * bucket.rate)
        .min(capacity);
    bucket.updated = now;
    bucket.rate = rate;
    bucket.tokens
}

impl IntegrityRate {
    pub(crate) fn acquire(
        &mut self,
        request_rate: Option<f64>,
        byte_rate: Option<f64>,
        requests: f64,
        bytes: f64,
        now: Instant,
    ) -> bool {
        let request_tokens = available(&mut self.requests, request_rate, requests, now);
        let byte_tokens = available(&mut self.bytes, byte_rate, bytes, now);
        if request_tokens < requests || byte_tokens < bytes {
            return false;
        }
        if request_rate.is_some() {
            self.requests.as_mut().unwrap().tokens -= requests;
        }
        if byte_rate.is_some() {
            self.bytes.as_mut().unwrap().tokens -= bytes;
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn request_and_byte_admission_is_atomic_and_rejected_waits_charge_nothing() {
        let now = Instant::now();
        let mut state = IntegrityRate::default();
        assert!(state.acquire(Some(10.0), Some(1.0), 1.0, 1.0, now));
        for _ in 0..20 {
            assert!(!state.acquire(Some(10.0), Some(1.0), 1.0, 1.0, now));
        }
        assert!(state.acquire(Some(10.0), Some(1.0), 9.0, 0.0, now));
        assert!(!state.acquire(Some(10.0), Some(1.0), 1.0, 0.0, now));
        assert!(state.acquire(
            Some(10.0),
            Some(1.0),
            10.0,
            1.0,
            now + Duration::from_secs(1)
        ));
    }

    #[test]
    fn oversized_batches_progress_without_resetting_on_rate_changes() {
        let now = Instant::now();
        let mut state = IntegrityRate::default();
        assert!(state.acquire(Some(1.0), Some(64.0), 2.0, 256.0, now));
        assert!(!state.acquire(
            Some(1.0),
            Some(64.0),
            2.0,
            256.0,
            now + Duration::from_secs(3)
        ));
        assert!(state.acquire(
            Some(1.0),
            Some(64.0),
            2.0,
            256.0,
            now + Duration::from_secs(4)
        ));
        assert!(!state.acquire(
            Some(100.0),
            Some(1024.0),
            2.0,
            256.0,
            now + Duration::from_secs(4)
        ));
        assert!(state.acquire(
            Some(100.0),
            Some(1024.0),
            2.0,
            256.0,
            now + Duration::from_millis(4250)
        ));
        assert!(!state.acquire(
            Some(1.0),
            Some(64.0),
            2.0,
            256.0,
            now + Duration::from_millis(4250)
        ));
        assert!(state.acquire(
            None,
            None,
            1000.0,
            1000000.0,
            now + Duration::from_millis(4250)
        ));
        assert!(!state.acquire(
            Some(1.0),
            Some(64.0),
            2.0,
            256.0,
            now + Duration::from_millis(4250)
        ));
        assert!(state.acquire(
            Some(1.0),
            Some(64.0),
            2.0,
            256.0,
            now + Duration::from_millis(8250)
        ));
    }
}
