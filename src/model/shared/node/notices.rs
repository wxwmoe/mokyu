use super::{Backend, queries::notices as q};
use crate::model::{
    cluster::NodeNotices,
    notification::{Change, Consumer, Delivery, LIMIT},
};
use anyhow::{Result, ensure};
use std::{collections::HashSet, sync::Arc};
use uuid::Uuid;

pub(super) struct Single(pub(super) Arc<Backend>);

fn ids(values: impl Iterator<Item = Uuid>) -> Result<String> {
    Ok(serde_json::to_string(
        &values.map(|id| id.simple().to_string()).collect::<Vec<_>>(),
    )?)
}

#[async_trait::async_trait]
impl NodeNotices for Single {
    async fn control_consumer(&self, c: Consumer, activate: bool) -> Result<()> {
        let mut tx = q::begin(&self.0).await?;
        if activate {
            q::membership(&mut tx).await?;
            q::activate(c, "control").execute(&mut *tx).await?;
            q::retire_previous(c).execute(&mut *tx).await?;
        } else {
            ensure!(
                q::renew(c).execute(&mut *tx).await?.rows_affected() == 1,
                "control notification registration expired"
            );
        }
        tx.commit().await?;
        Ok(())
    }

    async fn claim_notifications(&self, consumers: &[Consumer]) -> Result<Vec<Delivery>> {
        ensure!(consumers.len() <= 4096, "too many notification consumers");
        if consumers.is_empty() {
            return Ok(Vec::new());
        }
        let active: HashSet<_> = consumers.iter().copied().collect();
        let nodes = ids(active.iter().map(|c| c.node))?;
        let rows = q::pending(&nodes).fetch_all(q::reader(&self.0)).await?;
        let mut deliveries = Vec::with_capacity(rows.len());
        for (node, generation, id, scope, resource, key) in rows {
            let consumer = Consumer { node, generation };
            if !active.contains(&consumer) {
                continue;
            }
            let change = match scope.as_str() {
                "object" => Change::Object {
                    bucket: resource,
                    key,
                },
                "bucket" => Change::Bucket { bucket: resource },
                "storage" => Change::Storage { storage: resource },
                "reset" => Change::Reset,
                _ => anyhow::bail!("invalid notification scope"),
            };
            change.validate()?;
            deliveries.push(Delivery {
                consumer,
                id,
                change,
            });
        }
        if !deliveries.is_empty() {
            let events = ids(deliveries.iter().map(|d| d.id))?;
            let mut tx = q::begin(&self.0).await?;
            q::claimed(&events).execute(&mut *tx).await?;
            tx.commit().await?;
        }
        Ok(deliveries)
    }

    async fn acknowledge_notifications(&self, consumer: Consumer, events: &[Uuid]) -> Result<u64> {
        ensure!(
            events.len() <= LIMIT && events.iter().all(|id| !id.is_nil()),
            "invalid notification receipt"
        );
        let events = ids(events.iter().copied())?;
        let mut tx = q::begin(&self.0).await?;
        let count = q::acknowledge(&mut tx, consumer, &events).await?;
        tx.commit().await?;
        Ok(count)
    }

    async fn maintain_notifications(&self) -> Result<bool> {
        if !q::has_expired().fetch_one(q::reader(&self.0)).await? {
            return Ok(false);
        }
        let mut tx = q::begin(&self.0).await?;
        let count = q::prune().execute(&mut *tx).await?.rows_affected();
        tx.commit().await?;
        Ok(count == 32)
    }
}
