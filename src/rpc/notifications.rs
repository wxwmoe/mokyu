use super::wire;
use crate::model::notification::{Change, Delivery, LIMIT};
use std::collections::{HashSet, VecDeque};
use tonic::Status;
use uuid::Uuid;

pub(super) fn notice(deliveries: &[Delivery]) -> wire::Notice {
    wire::Notice {
        changes: deliveries
            .iter()
            .map(|delivery| wire::Invalidation {
                event_id: delivery.id.to_string(),
                target: Some(match &delivery.change {
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
                None => return Err(Status::data_loss("notification target is missing")),
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
        apply(notice.reset, &changes);
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
