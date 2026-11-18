use super::wire;
use crate::model::notification::{Change, Delivery, LIMIT};
use std::collections::{HashSet, VecDeque};
use tonic::Status;
use uuid::Uuid;

#[derive(Default)]
pub(super) struct HintSchedule {
    sent: crate::cluster::notifications::Hints,
    work: Option<tokio::time::Instant>,
    settings: Option<tokio::time::Instant>,
}

impl HintSchedule {
    fn work_due(
        &self,
        policy: super::latency::Policy,
        now: tokio::time::Instant,
    ) -> tokio::time::Instant {
        use std::time::Duration;
        self.work.map_or(now, |last| {
            last + match policy {
                super::latency::Policy::Low => Duration::from_millis(100),
                _ => Duration::from_secs(1),
            }
        })
    }

    pub(super) fn deadline(
        &self,
        hints: &crate::cluster::notifications::Hints,
        policy: super::latency::Policy,
        now: tokio::time::Instant,
    ) -> Option<tokio::time::Instant> {
        if hints.state != self.sent.state || hints.urgent_work != self.sent.urgent_work {
            return Some(now);
        }
        let work = (hints.work != self.sent.work).then(|| self.work_due(policy, now));
        let settings = (hints.settings != self.sent.settings).then(|| self.settings.unwrap_or(now));
        work.into_iter().chain(settings).min()
    }

    pub(super) fn take(
        &mut self,
        hints: &crate::cluster::notifications::Hints,
        policy: super::latency::Policy,
        now: tokio::time::Instant,
    ) -> wire::Notice {
        let state_changed = hints.state != self.sent.state;
        self.sent.state = hints.state;
        let work_available = hints.urgent_work != self.sent.urgent_work
            || hints.work != self.sent.work && self.work_due(policy, now) <= now;
        if work_available {
            self.work = Some(now);
            self.sent.work = hints.work;
            self.sent.urgent_work = hints.urgent_work;
        }
        let mut settings = Vec::new();
        if hints.settings != self.sent.settings && self.settings.is_none_or(|due| due <= now) {
            settings = hints
                .settings
                .iter()
                .filter(|(section, revision)| self.sent.settings.get(*section) != Some(*revision))
                .filter_map(|(name, revision)| {
                    Some(wire::Revision {
                        section: super::section(name)? as i32,
                        revision: *revision as u64,
                    })
                })
                .collect();
            self.sent.settings.clone_from(&hints.settings);
            self.settings = Some(now + std::time::Duration::from_secs(1));
        }
        wire::Notice {
            state_changed,
            work_available,
            settings,
            ..Default::default()
        }
    }
}

pub(super) fn notice(deliveries: &[Delivery]) -> wire::Notice {
    wire::Notice {
        changes: deliveries
            .iter()
            .map(|delivery| wire::Invalidation {
                event_id: delivery.id.to_string(),
                target: Some(match &delivery.change {
                    Change::Reset => wire::invalidation::Target::Reset(true),
                    Change::Object { bucket, key } => {
                        wire::invalidation::Target::Object(wire::ObjectChange {
                            bucket_id: bucket.to_string(),
                            key: key.clone(),
                        })
                    }
                    Change::Bucket { bucket } => {
                        wire::invalidation::Target::BucketId(bucket.to_string())
                    }
                    Change::Storage { storage } => {
                        wire::invalidation::Target::StorageId(storage.to_string())
                    }
                }),
            })
            .collect(),
        ..Default::default()
    }
}

pub(super) fn receipt(receipt: wire::Receipt) -> Result<Vec<Uuid>, Status> {
    if receipt.applied.is_empty() || receipt.applied.len() > LIMIT {
        return Err(Status::invalid_argument("invalid notification receipt"));
    }
    let ids = receipt
        .applied
        .iter()
        .map(|id| super::uuid(id))
        .collect::<Result<Vec<_>, _>>()?;
    if ids.iter().collect::<HashSet<_>>().len() != ids.len() {
        return Err(Status::invalid_argument("duplicate notification receipt"));
    }
    Ok(ids)
}

#[derive(Default)]
pub(super) struct Applied {
    ids: HashSet<Uuid>,
    order: VecDeque<Uuid>,
}

impl Applied {
    pub(super) fn apply(
        &mut self,
        notice: wire::Notice,
        apply: impl FnOnce(bool, &[Change]),
    ) -> Result<wire::Receipt, Status> {
        if notice.changes.len() > LIMIT
            || prost::Message::encoded_len(&notice) > super::SESSION_LIMIT - 1024
        {
            return Err(Status::data_loss("notification batch exceeds limit"));
        }
        let mut ids = Vec::new();
        let mut changes = Vec::new();
        for item in notice.changes {
            let id = super::uuid(&item.event_id)
                .map_err(|_| Status::data_loss("invalid notification identity"))?;
            let change = match item.target {
                Some(wire::invalidation::Target::Object(object)) => Change::Object {
                    bucket: super::uuid(&object.bucket_id)?,
                    key: object.key,
                },
                Some(wire::invalidation::Target::BucketId(bucket)) => Change::Bucket {
                    bucket: super::uuid(&bucket)?,
                },
                Some(wire::invalidation::Target::StorageId(storage)) => Change::Storage {
                    storage: super::uuid(&storage)?,
                },
                Some(wire::invalidation::Target::Reset(true)) => Change::Reset,
                _ => return Err(Status::data_loss("notification target is missing")),
            };
            change
                .validate()
                .map_err(|_| Status::data_loss("invalid notification target"))?;
            if !self.ids.contains(&id) && !ids.contains(&id) {
                changes.push(change);
            }
            if !ids.contains(&id) {
                ids.push(id);
            }
        }
        apply(notice.reset || changes.contains(&Change::Reset), &changes);
        for id in &ids {
            if self.ids.insert(*id) {
                self.order.push_back(*id);
            }
        }
        while self.order.len() > 4096 {
            self.ids.remove(&self.order.pop_front().unwrap());
        }
        Ok(wire::Receipt {
            applied: ids.into_iter().map(|id| id.to_string()).collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::notification::Consumer;

    #[test]
    fn work_and_settings_have_independent_anchored_deadlines() {
        use super::super::latency::Policy;
        use std::time::Duration;
        let start = tokio::time::Instant::now();
        for (policy, ms) in [
            (Policy::Low, 100),
            (Policy::Coalesced, 1000),
            (Policy::Baseline, 1000),
        ] {
            let mut schedule = HintSchedule::default();
            let mut hints = crate::cluster::notifications::Hints::default();
            hints.settings.insert("io".into(), 1);
            assert_eq!(schedule.take(&hints, policy, start).settings.len(), 1);
            let first = start + Duration::from_millis(1);
            hints.work += 1;
            assert_eq!(schedule.deadline(&hints, policy, first), Some(first));
            assert!(schedule.take(&hints, policy, first).work_available);
            let due = first + Duration::from_millis(ms);
            for i in 1..6 {
                hints.work += 1;
                assert_eq!(
                    schedule.deadline(&hints, policy, first + Duration::from_millis(i)),
                    Some(due)
                );
            }
            assert!(schedule.take(&hints, policy, due).work_available);
            assert_eq!(schedule.deadline(&hints, policy, due), None);
            hints.urgent_work += 1;
            hints.state += 1;
            let notice = schedule.take(&hints, policy, due);
            assert!(notice.state_changed && notice.work_available);
        }
    }

    #[test]
    fn a_coalesced_reset_is_confirmed_after_invalidating_once() {
        let id = Uuid::new_v4();
        let batch = notice(&[Delivery {
            consumer: Consumer {
                node: Uuid::new_v4(),
                generation: Uuid::new_v4(),
            },
            id,
            change: Change::Reset,
        }]);
        let mut applied = Applied::default();
        let first = applied
            .apply(batch.clone(), |reset, changes| {
                assert!(reset);
                assert_eq!(changes, &[Change::Reset]);
            })
            .unwrap();
        let retry = applied
            .apply(batch, |reset, changes| {
                assert!(!reset);
                assert!(changes.is_empty());
            })
            .unwrap();
        assert_eq!(first, retry);
        assert_eq!(receipt(first).unwrap(), vec![id]);
    }

    #[test]
    fn receipts_follow_application_and_duplicate_batches_do_not_invalidate_again() {
        let batch = notice(&vec![
            Delivery {
                consumer: Consumer {
                    node: Uuid::new_v4(),
                    generation: Uuid::new_v4()
                },
                id: Uuid::new_v4(),
                change: Change::Object {
                    bucket: Uuid::new_v4(),
                    key: "x".repeat(1024)
                }
            };
            32
        ]);
        assert!(prost::Message::encoded_len(&batch) < super::super::SESSION_LIMIT);
        let mut applied = Applied::default();
        let first = applied
            .apply(batch.clone(), |reset, changes| {
                assert!(!reset);
                assert_eq!(changes.len(), 1);
            })
            .unwrap();
        assert_eq!(receipt(first.clone()).unwrap().len(), 1);
        let second = applied
            .apply(batch, |_, changes| assert!(changes.is_empty()))
            .unwrap();
        assert_eq!(first, second);
        let mut invalid = wire::Notice::default();
        invalid.changes.push(wire::Invalidation::default());
        assert!(
            applied
                .apply(invalid, |_, _| panic!("invalid batches cannot be applied"))
                .is_err()
        );
    }
}
