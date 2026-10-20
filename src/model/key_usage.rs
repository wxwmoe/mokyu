use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

const PERIOD: i64 = 300;
const DAY: i64 = 86_400;
const STALE: i64 = PERIOD * 3;
const CAPACITY: i128 = (i64::MAX / 64 * 64) as i128;

#[derive(Clone, Default, Deserialize, Serialize)]
pub(crate) struct NonceHistory {
    samples: Vec<Sample>,
    warnings: [Option<String>; 2],
    regressed: bool,
}

#[derive(Clone, Deserialize, Serialize)]
struct Sample {
    at: i64,
    reserved: [i64; 2],
}

pub(crate) type NonceRow = (
    Uuid,
    String,
    Option<i64>,
    Option<i64>,
    i64,
    bool,
    Option<sqlx::types::Json<NonceHistory>>,
    Option<i64>,
);

#[derive(Clone, Serialize, ToSchema)]
pub(crate) struct NonceCapacity {
    pub key_id: Uuid,
    pub name: String,
    pub storage_count: i64,
    pub pending: bool,
    pub remaining: Option<String>,
    pub state: String,
    pub sampled_at: Option<DateTime<Utc>>,
    pub stale: bool,
    pub counters: Vec<NonceCounter>,
}

#[derive(Clone, Serialize, ToSchema)]
pub(crate) struct NonceCounter {
    pub type_tag: u8,
    pub reserved_id: Option<String>,
    pub available: Option<String>,
    pub remaining: Option<String>,
    pub estimated_days: Option<u64>,
    pub observed_seconds: i64,
    pub reason: Option<String>,
}

fn available(reserved: i64) -> i128 {
    i128::from((i64::MAX - reserved) / 64 * 64)
}

fn percent(reserved: i64) -> String {
    ((available(reserved) * 100 + CAPACITY / 2) / CAPACITY).to_string()
}

impl NonceHistory {
    pub(crate) fn sample(&mut self, reserved: [i64; 2], at: i64) -> bool {
        if reserved.iter().any(|n| *n < 0) {
            return false;
        }
        if let Some(last) = self.samples.last() {
            if reserved.iter().zip(last.reserved).any(|(a, b)| *a < b) {
                self.regressed = true;
            }
            if at < last.at || at - last.at > STALE {
                self.samples.clear();
            } else if at - last.at < PERIOD {
                return false;
            }
        }
        self.samples.push(Sample { at, reserved });
        // Keep one boundary point for the long window, with a fixed memory bound.
        while self.samples.len() > 289 || (self.samples.len() > 2 && self.samples[1].at <= at - DAY)
        {
            self.samples.remove(0);
        }
        for tag in 0..2 {
            let reason = if i128::from(reserved[tag]) * 10 >= i128::from(i64::MAX) * 9 {
                Some("reserved")
            } else if self
                .forecast(tag, reserved[tag], at)
                .0
                .is_some_and(|s| s <= 60 * DAY)
            {
                Some("forecast")
            } else {
                None
            };
            if let Some(reason) = reason {
                self.warnings[tag] = Some(reason.into());
            }
        }
        true
    }

    fn forecast(&self, tag: usize, reserved: i64, at: i64) -> (Option<i64>, i64) {
        let Some(last) = self.samples.last() else {
            return (None, 0);
        };
        if self.regressed || at < last.at || at - last.at > STALE {
            return (None, 0);
        }
        let mut result = None;
        let mut coverage = 0;
        for window in [3600, DAY] {
            let Some(first) = self
                .samples
                .iter()
                .find(|sample| sample.at >= last.at - window)
            else {
                continue;
            };
            let elapsed = last.at - first.at;
            let consumed = i128::from(last.reserved[tag]) - i128::from(first.reserved[tag]);
            coverage = coverage.max(elapsed);
            if elapsed >= PERIOD && consumed > 0 {
                let seconds = available(reserved) * i128::from(elapsed) / consumed;
                let seconds = i64::try_from(seconds).unwrap_or(i64::MAX);
                result = Some(result.map_or(seconds, |previous: i64| previous.min(seconds)));
            }
        }
        (result, coverage)
    }
}

pub(crate) fn capacity(row: &NonceRow, now: DateTime<Utc>) -> NonceCapacity {
    let mut history = row.6.as_ref().map(|h| h.0.clone()).unwrap_or_default();
    if let Some(last) = history.samples.last() {
        history.regressed |= [row.2, row.3]
            .into_iter()
            .zip(last.reserved)
            .any(|(current, old)| current.is_some_and(|n| n < old));
    }
    let sampled_at = history
        .samples
        .last()
        .and_then(|s| DateTime::from_timestamp(s.at, 0));
    let stale = sampled_at.is_none_or(|at| now < at || (now - at).num_seconds() > STALE);
    let counters: Vec<_> = [row.2, row.3]
        .into_iter()
        .enumerate()
        .map(|(tag, reserved)| {
            let reserved = reserved.filter(|r| *r >= 0);
            let (seconds, observed_seconds) = reserved
                .map(|r| history.forecast(tag, r, now.timestamp()))
                .unwrap_or_default();
            let reason = if reserved.is_none() {
                Some("counter".into())
            } else if history.regressed {
                Some("rollback".into())
            } else if reserved.is_some_and(|r| available(r) == 0) {
                Some("exhausted".into())
            } else if reserved.is_some_and(|r| i128::from(r) * 10 >= i128::from(i64::MAX) * 9) {
                Some("reserved".into())
            } else {
                history.warnings[tag].clone()
            };
            NonceCounter {
                type_tag: tag as u8,
                reserved_id: reserved.map(|r| r.to_string()),
                available: reserved.map(|r| available(r).to_string()),
                remaining: reserved.map(percent),
                estimated_days: seconds
                    .filter(|s| *s <= 365_000 * DAY)
                    .map(|s| ((s + DAY - 1) / DAY) as u64),
                observed_seconds,
                reason,
            }
        })
        .collect();
    let unknown = counters
        .iter()
        .any(|c| c.remaining.is_none() || c.reason.as_deref() == Some("rollback"));
    let exhausted = counters
        .iter()
        .any(|c| c.reason.as_deref() == Some("exhausted"));
    let warning = counters.iter().any(|c| c.reason.is_some());
    NonceCapacity {
        key_id: row.0,
        name: row.1.clone(),
        storage_count: row.4,
        pending: row.5,
        remaining: if unknown {
            None
        } else {
            [row.2, row.3].into_iter().flatten().max().map(percent)
        },
        state: if exhausted {
            "exhausted"
        } else if unknown {
            "unknown"
        } else if warning {
            "warning"
        } else {
            "normal"
        }
        .into(),
        sampled_at,
        stale,
        counters,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_boundaries_and_integer_precision() {
        assert_eq!(percent(0), "100");
        assert_eq!(percent(64), "100");
        for tail in [0, 63] {
            assert_eq!(percent(i64::MAX - tail), "0")
        }
        for tail in [64, 65] {
            assert_eq!(percent(i64::MAX - tail), "0")
        }
        for reserved in [0, 64, i64::MAX / 100, i64::MAX / 2, i64::MAX - 64, i64::MAX] {
            assert!(percent(reserved).parse::<u8>().unwrap() <= 100);
        }
        assert_eq!(percent(i64::MAX / 2), "50");
        assert_eq!(available(i64::MAX - 65), 64);
        let threshold = ((i128::from(i64::MAX) * 9 + 9) / 10) as i64;
        let mut history = NonceHistory::default();
        history.sample([threshold - 1, 0], 300);
        assert!(history.warnings[0].is_none());
        history.sample([threshold, 0], 600);
        assert_eq!(history.warnings[0].as_deref(), Some("reserved"));
        assert!(history.warnings[1].is_none());
    }

    #[test]
    fn forecast_survives_restart_and_warning_survives_idle() {
        let mut history = NonceHistory::default();
        let spent = i64::MAX / 10_000;
        history.sample([0, 0], 300);
        history.sample([spent, 0], 600);
        assert_eq!(history.warnings[0].as_deref(), Some("forecast"));
        let mut history: NonceHistory =
            serde_json::from_value(serde_json::to_value(history).unwrap()).unwrap();
        assert!(history.forecast(0, spent, 600).0.unwrap() < 60 * DAY);
        assert!(history.forecast(1, 0, 600).0.is_none());
        assert!(history.forecast(0, spent, 600 + STALE + 1).0.is_none());
        history.sample([spent, 0], DAY * 2);
        assert_eq!(history.samples.len(), 1);
        assert!(history.forecast(0, spent, DAY * 2).0.is_none());
        assert_eq!(history.warnings[0].as_deref(), Some("forecast"));
        history.sample([spent - 1, 0], DAY * 2 + PERIOD);
        assert!(history.regressed);
        assert!(history.forecast(0, spent, DAY * 2 + PERIOD).0.is_none());
    }

    #[test]
    fn forecast_threshold_and_zero_rate_are_independent_of_percentage() {
        let mut history = NonceHistory::default();
        history.sample([0, 0], 0);
        // Two keys with similar remaining percentages can have different warning states.
        history.sample([i64::MAX / 20_000, i64::MAX / 10_000], PERIOD);
        assert!(history.warnings[0].is_none());
        assert_eq!(history.warnings[1].as_deref(), Some("forecast"));
        assert_eq!(percent(i64::MAX / 10_000), "100");
        for n in 2..=300 {
            history.sample([i64::MAX / 20_000, i64::MAX / 10_000], n * PERIOD);
        }
        assert!(
            history
                .forecast(1, i64::MAX / 10_000, 300 * PERIOD)
                .0
                .is_none()
        );
        assert_eq!(history.warnings[1].as_deref(), Some("forecast"));
    }

    #[test]
    fn sampling_is_bounded_and_clock_errors_do_not_invent_rates() {
        let mut history = NonceHistory::default();
        for n in 0..1000 {
            history.sample([n * 64, 0], n * PERIOD);
        }
        assert!(history.samples.len() <= 289);
        let at = 999 * PERIOD;
        assert!(!history.sample([999 * 64, 0], at + 1));
        assert!(history.forecast(0, 999 * 64, at - 1).0.is_none());
        history.sample([999 * 64, 0], at - 1);
        assert_eq!(history.samples.len(), 1);
        assert!(history.forecast(0, 999 * 64, at - 1).0.is_none());
        let row = (
            Uuid::nil(),
            "key".into(),
            Some(64),
            None,
            0,
            false,
            None,
            None,
        );
        let view = capacity(&row, Utc::now());
        assert_eq!(view.state, "unknown");
        assert!(view.remaining.is_none());
        assert!(view.stale);
    }
}
