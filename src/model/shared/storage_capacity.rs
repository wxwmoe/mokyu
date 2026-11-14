use super::{
    Backend,
    queries::{storage_capacity as q, storage_cluster, storage_payload},
};
use crate::model::{
    Error,
    storage_cluster::{
        Cluster, Target,
        placement::{Status, Table},
    },
    storage_payload::Intent,
};
use anyhow::{Context, Result, ensure};
use std::collections::HashMap;
use uuid::Uuid;

pub(super) struct Allocation {
    pub cluster: Cluster,
    pub targets: Vec<Target>,
    pub(super) status: HashMap<Uuid, Status>,
    cursor: Option<Uuid>,
}

pub(super) async fn lock(
    backend: &Backend,
    tx: &mut storage_payload::Tx<'_>,
    id: Uuid,
) -> Result<Allocation> {
    // Identity locks precede the cluster allocation lock, then object/payload locks.
    let targets = storage_cluster::targets(backend, tx, Some(id), None, true).await?;
    let (policy, manifest, cursor) = q::allocation(id).fetch_one(&mut **tx).await?;
    let cluster = Cluster {
        id,
        name: String::new(),
        revision: manifest.revision,
        policy: policy.0,
        manifest: manifest.0,
        state: String::new(),
    };
    let now = chrono::Utc::now().timestamp_micros();
    q::abandoned(id, now).execute(&mut **tx).await?;
    for target in &targets {
        let capacity = &target.device.announcement.capacity;
        if capacity.accounting_ready
            && let Some(stamp) = capacity.accounting
        {
            if target.run.is_some_and(|run| run.boot == stamp.boot) {
                q::settle(target.device.media_id, stamp)
                    .execute(&mut **tx)
                    .await?;
            }
        }
    }
    let reserved: HashMap<_, _> = q::totals(id)
        .fetch_all(&mut **tx)
        .await?
        .into_iter()
        .collect();
    let status = targets
        .iter()
        .map(|target| {
            let announcement = &target.device.announcement;
            let current = target.run.is_some_and(|run| {
                announcement
                    .capacity
                    .accounting
                    .is_some_and(|stamp| stamp.boot == run.boot)
            });
            (
                target.device.media_id,
                Status {
                    online: current
                        && target.expires > now
                        && now - target.device.observed_at < 15_000_000
                        && announcement.manifest.as_ref().is_some_and(|head| {
                            head.cluster_id == id && head.revision == cluster.revision
                        }),
                    free: announcement.capacity.writable().saturating_sub(
                        reserved
                            .get(&target.device.media_id)
                            .copied()
                            .unwrap_or(0)
                            .max(0) as u64,
                    ),
                    requests: announcement.load.as_ref().map(|load| {
                        (
                            u64::from(load.admitting) + load.active.len() as u64,
                            load.slots,
                        )
                    }),
                },
            )
        })
        .collect();
    Ok(Allocation {
        cluster,
        targets,
        status,
        cursor,
    })
}

// Includes file headers, proofs, footers and the writer's allocation allowance.
pub(super) fn bytes(intent: &Intent, copy: u16, shard: u16) -> Result<u64> {
    let media = Uuid::from_u128(1);
    let first = intent.header(0, copy, shard, media, media)?.file_bytes() + 65536;
    let last = intent
        .header((intent.sequences() - 1) as u32, copy, shard, media, media)?
        .file_bytes()
        + 65536;
    let bytes = first
        .checked_mul(intent.sequences() - 1)
        .and_then(|value| value.checked_add(last))
        .context("storage reservation overflow")?;
    ensure!(bytes <= i64::MAX as u64, "storage reservation is too large");
    Ok(bytes)
}

impl Allocation {
    pub async fn reserve_repair(
        &self,
        tx: &mut storage_payload::Tx<'_>,
        header: &crate::content::fragment::Header,
    ) -> Result<()> {
        let amount = header
            .file_bytes()
            .checked_add(65536)
            .context("repair reservation overflow")?;
        ensure!(
            self.cluster
                .manifest
                .members
                .iter()
                .any(|member| member.media_id == header.media && member.state == "available")
                && self
                    .status
                    .get(&header.media)
                    .is_some_and(|status| status.online && status.free >= amount),
            s3s::s3_error!(
                SlowDown,
                "repair target has insufficient available capacity"
            )
        );
        super::queries::storage_repair::reserve(
            header.operation,
            header.media,
            header.cluster,
            amount as i64,
        )
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
    pub fn select(&self, intent: &mut Intent, region: Option<&str>) -> Result<()> {
        ensure!(
            self.cluster.manifest.revision as u64 == intent.layout_revision
                && self.cluster.manifest.takeover_epoch == intent.takeover_epoch
                && self.cluster.policy.layout == intent.layout,
            Error::OperationAborted
        );
        let recovery;
        let cluster = if intent.area == crate::content::fragment::Area::Recovery {
            recovery = Cluster {
                policy: crate::model::storage_cluster::Policy {
                    acknowledgement: crate::model::storage_cluster::Acknowledgement::All,
                    degraded_acknowledgement: None,
                    ..self.cluster.policy.clone()
                },
                ..self.cluster.clone()
            };
            &recovery
        } else {
            &self.cluster
        };
        let selected = Table::new(cluster)?.select(
            intent.payload,
            region,
            bytes(intent, 0, 0)?,
            &self.status,
            self.cursor,
        )?;
        intent.group = selected.group;
        intent.members = selected.members;
        intent.acknowledgement = selected.acknowledgement;
        Ok(())
    }

    pub async fn reserve(
        &self,
        tx: &mut storage_payload::Tx<'_>,
        intent: &Intent,
        initial: bool,
    ) -> Result<Vec<Uuid>> {
        let mut result = Vec::new();
        for (index, media) in intent.members.iter().copied().enumerate() {
            let width = usize::from(intent.layout.data_shards + intent.layout.parity_shards);
            let (copy, shard) = ((index / width) as u16, (index % width) as u16);
            let active = self
                .cluster
                .manifest
                .members
                .iter()
                .any(|m| m.media_id == media && m.state == "available")
                && self.status.get(&media).is_some_and(|status| status.online);
            if !active {
                ensure!(
                    !initial || !intent.required(copy, shard),
                    Error::OperationAborted
                );
                continue;
            }
            if q::reserved(intent.operation, media)
                .fetch_optional(&mut **tx)
                .await?
                .is_some()
                || q::received(intent.operation, media)
                    .fetch_one(&mut **tx)
                    .await? as u64
                    == intent.sequences()
            {
                result.push(media);
                continue;
            }
            let amount = bytes(intent, copy, shard)?;
            if self.status[&media].free < amount {
                ensure!(
                    !intent.required(copy, shard),
                    s3s::s3_error!(SlowDown, "storage capacity is reserved by other writes")
                );
                continue;
            }
            q::insert(intent.operation, media, intent.cluster, amount as i64)
                .execute(&mut **tx)
                .await?;
            result.push(media);
        }
        if initial {
            q::cursor(intent.cluster, intent.group)
                .execute(&mut **tx)
                .await?;
        }
        Ok(result)
    }

    pub async fn received(
        &self,
        tx: &mut storage_payload::Tx<'_>,
        intent: &Intent,
        proofs: &[crate::model::storage_transfer::Completion],
    ) -> Result<()> {
        for proof in proofs {
            let media = proof.receipt.receipt.header.media;
            let complete = q::received(intent.operation, media)
                .fetch_one(&mut **tx)
                .await? as u64
                == intent.sequences()
                && !super::queries::storage_repair::incomplete(intent.operation, media)
                    .fetch_one(&mut **tx)
                    .await?;
            q::stamp(intent.operation, media, proof.receipt.accounting, complete)
                .execute(&mut **tx)
                .await?;
        }
        for target in &self.targets {
            let capacity = &target.device.announcement.capacity;
            if capacity.accounting_ready
                && let Some(stamp) = capacity.accounting
            {
                if target.run.is_some_and(|run| run.boot == stamp.boot) {
                    q::settle(target.device.media_id, stamp)
                        .execute(&mut **tx)
                        .await?;
                }
            }
        }
        Ok(())
    }
}
