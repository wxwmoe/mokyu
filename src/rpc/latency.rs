use serde::Serialize;
use std::{collections::VecDeque, time::Duration};
use tokio::time::Instant;

pub(super) const INTERVAL: Duration = Duration::from_secs(10);
const FRESH: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Policy {
    #[default]
    Baseline,
    Low,
    Coalesced,
}

impl Policy {
    pub(super) fn initial(local: Option<&str>, remote: Option<&str>) -> Self {
        match local.zip(remote) {
            Some((a, b)) if a == b => Self::Low,
            Some(_) => Self::Coalesced,
            None => Self::Baseline,
        }
    }

    pub(crate) fn code(self) -> u32 {
        match self {
            Self::Baseline => 0,
            Self::Low => 1,
            Self::Coalesced => 2,
        }
    }
    pub(crate) fn from_code(code: u32) -> Self {
        match code {
            1 => Self::Low,
            2 => Self::Coalesced,
            _ => Self::Baseline,
        }
    }
    fn classify(samples: &[u64]) -> Self {
        let mut group: [u64; 5] = samples.try_into().unwrap();
        group.sort_unstable();
        match group[2] {
            0..=30_000 => Self::Low,
            60_000.. => Self::Coalesced,
            _ => Self::Baseline,
        }
    }
}

pub(super) struct Path {
    initial: Policy,
    policy: Policy,
    measured: bool,
    seen: Instant,
    samples: VecDeque<u64>,
    total: u64,
    changes: u64,
}

impl Path {
    pub(super) fn new(initial: Policy, now: Instant) -> Self {
        Self {
            initial,
            policy: initial,
            measured: false,
            seen: now,
            samples: VecDeque::with_capacity(10),
            total: 0,
            changes: 0,
        }
    }
    pub(super) fn policy(&self, now: Instant) -> Policy {
        if now.duration_since(self.seen) >= FRESH {
            Policy::Baseline
        } else {
            self.policy
        }
    }
    pub(super) fn record(&mut self, now: Instant, elapsed: Duration) {
        if now.duration_since(self.seen) >= FRESH {
            self.samples.clear();
            self.measured = false;
            self.policy = Policy::Baseline;
        }
        self.seen = now;
        self.total += 1;
        if self.samples.len() == 10 {
            self.samples.pop_front();
        }
        self.samples
            .push_back(elapsed.as_micros().min(u64::MAX as u128) as u64);
        let values = self.samples.make_contiguous();
        let next = if !self.measured && values.len() >= 5 {
            self.measured = true;
            Some(Policy::classify(&values[values.len() - 5..]))
        } else if values.len() == 10 {
            let first = Policy::classify(&values[..5]);
            let second = Policy::classify(&values[5..]);
            (first == second && first != Policy::Baseline).then_some(first)
        } else {
            None
        };
        if let Some(next) = next
            && self.policy != next
        {
            self.changes += 1;
            self.policy = next;
        }
    }
    pub(super) fn measured(&self, now: Instant) -> bool {
        self.measured && now.duration_since(self.seen) < FRESH
    }
    pub(super) fn snapshot(&self) -> serde_json::Value {
        let now = Instant::now();
        let source = if now.duration_since(self.seen) >= FRESH {
            "expired"
        } else if self.measured {
            "rtt"
        } else if self.total == 0 || self.policy != Policy::Baseline {
            "initial"
        } else {
            "sampling"
        };
        serde_json::json!({"initial":self.initial,"policy":self.policy(now),"source":source,
            "samples_us":self.samples,"sample_count":self.total,"changes":self.changes,"age_ms":now.duration_since(self.seen).as_millis() as u64})
    }
}

// Only authorizes an empty latency probe. Business methods still verify the live run and credentials.
pub(super) fn probe_token(
    key: &[u8; 32],
    node: &crate::cluster::NodeState,
    period: i64,
) -> blake3::Hash {
    let mut hash = blake3::Hasher::new_keyed(key);
    hash.update(b"mokyu-node-rtt-v1\0");
    hash.update(node.deployment_id.as_bytes());
    hash.update(node.node_id.as_bytes());
    hash.update(&node.credential_generation.to_be_bytes());
    hash.update(node.credential.as_bytes());
    hash.update(node.role.as_bytes());
    hash.update(&period.to_be_bytes());
    hash.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn measured_paths_override_labels_and_expire() {
        let mut now = Instant::now();
        for (local, remote, expected) in [
            (None, None, Policy::Baseline),
            (Some("a"), None, Policy::Baseline),
            (None, Some("b"), Policy::Baseline),
            (Some("a"), Some("a"), Policy::Low),
            (Some("a"), Some("b"), Policy::Coalesced),
        ] {
            assert_eq!(Policy::initial(local, remote), expected);
        }
        let mut path = Path::new(Policy::Low, now);
        for (count, ms, expected) in [
            (4, 180, Policy::Low),
            (1, 180, Policy::Coalesced),
            (1, 2, Policy::Coalesced),
            (9, 2, Policy::Low),
            (1, 180, Policy::Low),
            (10, 45, Policy::Low),
            (10, 180, Policy::Coalesced),
        ] {
            for _ in 0..count {
                now += INTERVAL;
                path.record(now, Duration::from_millis(ms));
            }
            assert_eq!(path.policy(now), expected);
        }
        now += FRESH;
        assert_eq!(path.policy(now), Policy::Baseline);
        path.record(now, Duration::from_millis(2));
        assert_eq!(path.policy(now), Policy::Baseline);
        assert_eq!(path.samples.len(), 1);
    }
}
