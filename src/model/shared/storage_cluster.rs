use super::{Backend, queries::storage_cluster as q};
use crate::{
    authorization::Principal,
    cluster::NodeState,
    content::codec::Key,
    model::{Error, session::Run, storage_cluster::*},
};
use anyhow::{Context, Result, ensure};
use sqlx::types::Json;
use uuid::Uuid;

type Row = (Uuid, String, i64, Json<Policy>, Json<Manifest>);
type DeviceRow = (Uuid, Uuid, Option<Uuid>, i64, Json<Announcement>);

fn member_move(row: super::queries::storage_move::Row) -> MemberMove {
    MemberMove {
        id: row.0,
        cluster: row.1,
        group: row.2,
        source: row.3,
        target: row.4,
        state: row.5,
        bytes_per_second: row.6,
    }
}

fn cluster(row: Row) -> Result<Cluster> {
    let (id, name, revision, policy, manifest) = row;
    manifest.validate()?;
    ensure!(
        manifest.cluster_id == id && manifest.revision == revision,
        "cluster manifest revision differs from its record"
    );
    let state = if manifest.members.is_empty() {
        "waiting_members"
    } else if manifest.groups.is_empty() {
        "waiting_layout"
    } else {
        "waiting_probe"
    }
    .into();
    Ok(Cluster {
        id,
        name,
        revision,
        policy: policy.0,
        manifest: manifest.0,
        state,
    })
}

fn device(row: DeviceRow) -> Device {
    Device {
        media_id: row.0,
        node_id: row.1,
        cluster_id: row.2,
        observed_at: row.3,
        announcement: row.4.0,
    }
}

async fn current(tx: &mut q::Tx<'_>, id: Uuid, revision: i64) -> Result<Cluster> {
    let row = q::cluster(id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    let row = cluster(row)?;
    ensure!(
        row.revision == revision && revision > 0,
        Error::OperationAborted
    );
    Ok(row)
}

pub(super) async fn save(tx: &mut q::Tx<'_>, mut value: Cluster) -> Result<Cluster> {
    let previous = q::cluster(value.id)
        .fetch_one(&mut **tx)
        .await?
        .4
        .digest()?;
    value.revision = value
        .revision
        .checked_add(1)
        .context("cluster revision exhausted")?;
    value.manifest.revision = value.revision;
    value.manifest.record_id = Uuid::now_v7();
    value.manifest.previous = Some(previous);
    value.manifest.validate()?;
    let hash = value.manifest.digest()?;
    q::history(&value, &hash).execute(&mut **tx).await?;
    ensure!(
        q::save(&value).execute(&mut **tx).await?.rows_affected() == 1,
        Error::OperationAborted
    );
    cluster((
        value.id,
        value.name,
        value.revision,
        Json(value.policy),
        Json(value.manifest),
    ))
}

fn validate_ids(ids: &[Uuid]) -> Result<()> {
    let unique: std::collections::HashSet<_> = ids.iter().collect();
    ensure!(
        !ids.is_empty()
            && ids.len() <= MEMBER_LIMIT
            && unique.len() == ids.len()
            && !ids.iter().any(Uuid::is_nil),
        s3s::s3_error!(InvalidArgument, "select distinct storage media")
    );
    Ok(())
}

async fn begin_move(
    backend: &Backend,
    tx: &mut q::Tx<'_>,
    mut value: Cluster,
    input: MoveInput,
    drain: bool,
) -> Result<MemberMove> {
    use super::queries::{storage_capacity, storage_move as moves};
    let id = value.id;
    let targets = q::targets(backend, tx, Some(id), None, true).await?;
    storage_capacity::allocation(id)
        .fetch_one(&mut **tx)
        .await?;
    let reserved = storage_capacity::totals(id)
        .fetch_all(&mut **tx)
        .await?
        .into_iter()
        .collect();
    let now = chrono::Utc::now().timestamp_micros();
    let status =
        super::storage_capacity::observations(tx, &value, &targets, &reserved, now).await?;
    let source = value
        .manifest
        .members
        .iter()
        .find(|m| m.media_id == input.source)
        .ok_or(Error::NodeInactive)?;
    let backup = source.topology.purpose == "backup";
    let stripe = u64::from(value.policy.layout.stripe_bytes);
    let target = if let Some(id) = input.target {
        id
    } else {
        ensure!(
            drain
                || source.state == "paused"
                || !status.get(&source.media_id).is_some_and(|v| v.online),
            s3s::s3_error!(
                InvalidRequest,
                "confirm a failed or paused member before assigning a spare"
            )
        );
        let mut spares: Vec<_> = value
            .manifest
            .members
            .iter()
            .filter(|m| {
                m.media_id != source.media_id
                    && (if backup {
                        m.state == "available"
                            && m.topology.purpose == "backup"
                            && m.topology.storage_group == source.topology.storage_group
                    } else {
                        m.topology.purpose == "primary"
                            && (m.state == "spare" || drain && m.state == "available")
                    })
                    && status
                        .get(&m.media_id)
                        .is_some_and(|v| v.online && v.free >= stripe)
            })
            .collect();
        spares.sort_by_key(|m| (std::cmp::Reverse(status[&m.media_id].free), m.media_id));
        let mut selected = None;
        for m in spares {
            if !moves::occupied(input.source, m.media_id)
                .fetch_one(&mut **tx)
                .await?
                && !super::queries::node_handoff::gate(m.node_id)
                    .fetch_one(&mut **tx)
                    .await?
                && (backup
                    || value
                        .manifest
                        .clone()
                        .begin_replacement(input.source, m.media_id)
                        .is_ok())
            {
                selected = Some(m.media_id);
                break;
            }
        }
        selected.context("no eligible spare is available")?
    };
    ensure!(
        status
            .get(&target)
            .is_some_and(|v| v.online && v.free >= stripe),
        s3s::s3_error!(SlowDown, "replacement needs available space on its target")
    );
    let rate = input.bytes_per_second.unwrap_or(16 * 1024 * 1024);
    ensure!(
        (1..=1_099_511_627_776).contains(&rate),
        s3s::s3_error!(InvalidArgument, "invalid transfer rate")
    );
    ensure!(
        !moves::occupied(input.source, target)
            .fetch_one(&mut **tx)
            .await?,
        Error::OperationAborted
    );
    let dest = value
        .manifest
        .members
        .iter()
        .find(|m| m.media_id == target)
        .ok_or(Error::NodeInactive)?;
    ensure!(
        !super::queries::node_handoff::gate(dest.node_id)
            .fetch_one(&mut **tx)
            .await?,
        "replacement target is preparing to go offline"
    );
    let group = if backup {
        ensure!(
            target != source.media_id
                && dest.topology.purpose == "backup"
                && dest.topology.storage_group == source.topology.storage_group
                && dest.state == "available",
            "backups must move within their assigned storage group"
        );
        Uuid::nil()
    } else {
        value.manifest.begin_replacement(input.source, target)?
    };
    let job = MemberMove {
        id: Uuid::now_v7(),
        cluster: id,
        group,
        source: input.source,
        target,
        state: "moving".into(),
        bytes_per_second: rate,
    };
    moves::add(&job, now).execute(&mut **tx).await?;
    save(tx, value).await?;
    Ok(job)
}

async fn backup_step(
    backend: &Backend,
    tx: &mut q::Tx<'_>,
    job: &MemberMove,
    cluster: &Cluster,
) -> Result<MoveStep> {
    use super::queries::{storage_move as m, storage_repair};
    let undo = matches!(job.state.as_str(), "reverting" | "undo_cleanup");
    let cleaning = matches!(job.state.as_str(), "cleaning" | "undo_cleanup");
    if undo {
        if !cleaning {
            if let Some((slot, r)) = m::backup_list(job.id, true)
                .fetch_optional(&mut **tx)
                .await?
            {
                let mut reversed = job.clone();
                std::mem::swap(&mut reversed.source, &mut reversed.target);
                return Ok(MoveStep::Backup {
                    job: reversed,
                    slot,
                    receipt: r.0,
                    remove: false,
                });
            }
            m::state(job.id, "undo_cleanup").execute(&mut **tx).await?;
        } else if let Some((slot, r)) = m::backup_remaining(job.id)
            .fetch_optional(&mut **tx)
            .await?
        {
            let mut reversed = job.clone();
            std::mem::swap(&mut reversed.source, &mut reversed.target);
            return Ok(MoveStep::Backup {
                job: reversed,
                slot,
                receipt: r.0,
                remove: true,
            });
        } else {
            m::state(job.id, "cancelled").execute(&mut **tx).await?;
        }
        return Ok(MoveStep::Advanced);
    }
    if !cleaning {
        if let Some((ns, through, after)) = m::lagging(job.source, job.target)
            .fetch_optional(&mut **tx)
            .await?
        {
            return Ok(MoveStep::Evidence {
                job: job.id,
                media: job.target,
                namespace: ns,
                after: after.try_into()?,
                through: through.try_into()?,
            });
        }
    }
    if let Some((slot, r)) = m::backup_list(job.id, cleaning)
        .fetch_optional(&mut **tx)
        .await?
    {
        return Ok(MoveStep::Backup {
            job: job.clone(),
            slot,
            receipt: r.0,
            remove: cleaning,
        });
    }
    if cleaning {
        if !m::remains(job.source).fetch_one(&mut **tx).await? {
            m::clear_evidence(job.source).execute(&mut **tx).await?;
            m::state(job.id, "completed").execute(&mut **tx).await?;
        }
    } else if let Some((slot, r)) = m::backup_ref(job.source).fetch_optional(&mut **tx).await? {
        r.validate()?;
        let allocation = super::storage_capacity::lock(backend, tx, cluster.id).await?;
        let amount = r.header.file_bytes() + 65536;
        ensure!(
            allocation
                .status
                .get(&job.target)
                .is_some_and(|s| s.online && s.free >= amount),
            s3s::s3_error!(SlowDown, "backup replacement target is full or unavailable")
        );
        storage_repair::reserve(
            r.header.operation,
            job.target,
            cluster.id,
            amount.try_into()?,
        )
        .execute(&mut **tx)
        .await?;
        m::backup_stage(job.id, slot, &r).execute(&mut **tx).await?;
        return Ok(MoveStep::Backup {
            job: job.clone(),
            slot,
            receipt: r.0,
            remove: false,
        });
    } else {
        m::state(job.id, "cleaning").execute(&mut **tx).await?;
    }
    Ok(MoveStep::Advanced)
}

pub(super) async fn draining_source(
    tx: &mut q::Tx<'_>,
    node: Uuid,
) -> Result<Option<(Cluster, Uuid)>> {
    let Some(media) = q::node_media(node).fetch_optional(&mut **tx).await? else {
        return Ok(None);
    };
    let d = q::device(media).fetch_one(&mut **tx).await?;
    let Some(id) = d.2 else { return Ok(None) };
    Ok(Some((
        cluster(q::cluster(id).fetch_one(&mut **tx).await?)?,
        media,
    )))
}

#[async_trait::async_trait]
impl StorageClusters for Backend {
    async fn takeover_targets(&self, principal: &Principal, media: &[Uuid]) -> Result<Vec<Target>> {
        validate_ids(media)?;
        let mut tx = q::begin(self).await?;
        q::admin(&mut tx, principal).await?;
        let mut targets = Vec::new();
        for id in media {
            let assigned = q::device(*id).fetch_one(&mut *tx).await?.2;
            let mut rows = q::targets(self, &mut tx, assigned, Some(*id), false).await?;
            ensure!(rows.len() == 1, "storage media is missing");
            targets.push(rows.pop().unwrap());
        }
        tx.commit().await?;
        Ok(targets)
    }

    async fn takeover(
        &self,
        principal: &Principal,
        key: &Key,
        catalog: Uuid,
        anchor: &Manifest,
        input: Takeover,
    ) -> Result<Cluster> {
        use crate::model::storage_recovery::CatalogEntry;
        validate_ids(&input.media_ids)?;
        crate::config::storage::label(&input.name, "cluster name")?;
        anchor.validate()?;
        let mut tx = q::begin(self).await?;
        q::admin(&mut tx, principal).await?;
        let (namespace, frozen) = super::queries::storage_recovery::catalog(catalog)
            .fetch_one(&mut *tx)
            .await?;
        ensure!(
            namespace == anchor.cluster_id && frozen.0 == *anchor,
            "takeover history changed"
        );
        let deployment = q::deployment().fetch_one(&mut *tx).await?;
        if let Some(existing) = q::cluster(anchor.cluster_id)
            .fetch_optional(&mut *tx)
            .await?
        {
            let mut existing = cluster(existing)?;
            ensure!(
                existing.name == input.name
                    && existing.manifest.deployment_id == deployment
                    && existing.manifest.publication_key
                        == crate::model::storage_recovery::public_key(key, anchor.cluster_id)?,
                "storage belongs to another active cluster"
            );
            ensure!(
                anchor.revision <= existing.revision
                    && q::historical_hash(existing.id, anchor.revision)
                        .fetch_optional(&mut *tx)
                        .await?
                        .as_deref()
                        == Some(anchor.digest()?.as_str()),
                "storage presents a conflicting layout"
            );
            let mut changed = false;
            for media in &input.media_ids {
                let observed = device(q::device_for_update(*media).fetch_one(&mut *tx).await?);
                if observed.cluster_id == Some(existing.id) {
                    continue;
                }
                ensure!(
                    observed.cluster_id.is_none()
                        && q::node_active(&mut tx, observed.node_id).await?,
                    Error::NodeInactive
                );
                let head = observed
                    .announcement
                    .manifest
                    .as_ref()
                    .context("storage has no committed layout")?;
                ensure!(
                    head.cluster_id == existing.id
                        && head.revision <= existing.revision
                        && q::historical_hash(existing.id, head.revision)
                            .fetch_optional(&mut *tx)
                            .await?
                            .as_deref()
                            == Some(head.digest()?.as_str()),
                    "storage layout is outside the recovered chain"
                );
                let member = existing
                    .manifest
                    .members
                    .iter_mut()
                    .find(|m| m.media_id == *media)
                    .context("storage has no recovered slot")?;
                ensure!(
                    member.topology.storage_group == observed.announcement.topology.storage_group
                        && member.topology.purpose == observed.announcement.topology.purpose,
                    "storage failure domain or purpose changed"
                );
                member.node_id = observed.node_id;
                member.topology = observed.announcement.topology;
                ensure!(
                    q::assign(*media, existing.id)
                        .execute(&mut *tx)
                        .await?
                        .rows_affected()
                        == 1,
                    Error::OperationAborted
                );
                changed = true;
            }
            if changed {
                existing = save(&mut tx, existing).await?
            }
            tx.commit().await?;
            return Ok(existing);
        }
        ensure!(
            q::clusters().fetch_all(&mut *tx).await?.len() < 256,
            "storage cluster limit reached"
        );
        let mut manifest = anchor.clone();
        manifest.deployment_id = deployment;
        manifest.publication_key =
            crate::model::storage_recovery::public_key(key, anchor.cluster_id)?;
        manifest.takeover_epoch = anchor
            .takeover_epoch
            .checked_add(1)
            .context("takeover epoch exhausted")?;
        manifest.revision = anchor
            .revision
            .checked_add(1)
            .context("cluster revision exhausted")?;
        manifest.record_id = Uuid::now_v7();
        manifest.previous = Some(anchor.digest()?);
        let mut observed = Vec::new();
        for media in &input.media_ids {
            let targets = q::targets(self, &mut tx, None, Some(*media), true).await?;
            let target = targets
                .first()
                .filter(|_| targets.len() == 1)
                .context("takeover storage is missing or already assigned")?;
            ensure!(
                target.run.is_some() && target.expires > chrono::Utc::now().timestamp_micros(),
                "takeover storage is offline"
            );
            let announcement = &target.device.announcement;
            let head = announcement
                .manifest
                .as_ref()
                .context("storage has no committed layout")?;
            ensure!(
                head.cluster_id == anchor.cluster_id && head.revision <= anchor.revision,
                "takeover storage has another layout"
            );
            observed.push((head.revision, head.digest()?));
            let member = manifest
                .members
                .iter_mut()
                .find(|m| m.media_id == *media)
                .context("storage media is outside the recovered layout")?;
            ensure!(
                member.topology.purpose == announcement.topology.purpose
                    && member.topology.storage_group == announcement.topology.storage_group,
                "restore the original storage group and purpose before takeover"
            );
            member.node_id = target.device.node_id;
            member.topology = announcement.topology.clone();
        }
        manifest.follows(anchor)?;
        let policy = Policy {
            layout: manifest
                .groups
                .first()
                .map(|g| g.layout.clone())
                .unwrap_or_default(),
            ..Policy::default()
        };
        let value = Cluster {
            id: anchor.cluster_id,
            name: input.name,
            revision: manifest.revision,
            policy,
            manifest,
            state: "waiting_probe".into(),
        };
        ensure!(
            q::recovered_count(catalog).fetch_one(&mut *tx).await? == anchor.revision,
            "takeover history contains gaps or competing revisions"
        );
        q::insert(&value).execute(&mut *tx).await?;
        let mut previous: Option<Manifest> = None;
        loop {
            let rows = q::recovered_history(catalog, previous.as_ref().map_or(0, |m| m.revision))
                .fetch_all(&mut *tx)
                .await?;
            if rows.is_empty() {
                break;
            }
            for (digest, entry) in rows {
                ensure!(
                    digest == blake3::hash(&serde_json::to_vec(&entry.0)?).as_bytes(),
                    "takeover history is damaged"
                );
                let CatalogEntry::Manifest(record) = entry.0 else {
                    anyhow::bail!("invalid takeover history")
                };
                record.validate()?;
                if let Some(old) = &previous {
                    record.follows(old)?;
                } else {
                    ensure!(
                        record.revision == 1 && record.previous.is_none(),
                        "takeover history has no origin"
                    );
                }
                ensure!(
                    record.cluster_id == value.id,
                    "takeover history changed cluster"
                );
                let hash = record.digest()?;
                for (_, expected) in observed
                    .iter()
                    .filter(|(revision, _)| *revision == record.revision)
                {
                    ensure!(*expected == hash, "storage has a competing layout history");
                }
                let historical = Cluster {
                    manifest: record.clone(),
                    revision: record.revision,
                    ..value.clone()
                };
                q::history(&historical, &hash).execute(&mut *tx).await?;
                previous = Some(record);
            }
        }
        ensure!(
            previous.as_ref() == Some(anchor),
            "takeover history does not reach the selected head"
        );
        q::history(&value, &value.manifest.digest()?)
            .execute(&mut *tx)
            .await?;
        for media in input.media_ids {
            ensure!(
                q::assign(media, value.id)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected()
                    == 1,
                Error::OperationAborted
            );
        }
        tx.commit().await?;
        Ok(value)
    }

    async fn placement_snapshot(&self, id: Uuid) -> Result<(Cluster, Vec<Target>)> {
        let mut tx = q::begin(self).await?;
        let value = cluster(
            q::cluster(id)
                .fetch_optional(&mut *tx)
                .await?
                .context("storage cluster is missing")?,
        )?;
        let targets = q::targets(self, &mut tx, Some(id), None, false).await?;
        tx.commit().await?;
        Ok((value, targets))
    }

    async fn start_move(
        &self,
        principal: &Principal,
        id: Uuid,
        input: MoveInput,
    ) -> Result<MemberMove> {
        let mut tx = q::begin(self).await?;
        q::admin(&mut tx, principal).await?;
        let value = current(&mut tx, id, input.revision).await?;
        let job = begin_move(self, &mut tx, value, input, false).await?;
        tx.commit().await?;
        Ok(job)
    }

    async fn move_state(
        &self,
        principal: &Principal,
        id: Uuid,
        job: Uuid,
        revision: i64,
        action: &str,
    ) -> Result<MemberMove> {
        use super::queries::storage_move as moves;
        let mut tx = q::begin(self).await?;
        q::admin(&mut tx, principal).await?;
        let mut value = current(&mut tx, id, revision).await?;
        super::storage_capacity::lock(self, &mut tx, id).await?;
        let mut job = member_move(moves::job(job).fetch_one(&mut *tx).await?);
        ensure!(job.cluster == id, Error::OperationAborted);
        let next = match (job.state.as_str(), action) {
            ("moving", "pause") => "paused",
            ("paused", "resume") => "moving",
            ("moving" | "paused", "cancel") => {
                if job.group.is_nil() {
                    "reverting"
                } else if moves::started(job.source, job.target)
                    .fetch_one(&mut *tx)
                    .await?
                {
                    "reverting"
                } else {
                    moves::uncapture(job.source).execute(&mut *tx).await?;
                    value
                        .manifest
                        .finish_replacement(job.group, job.source, job.target, false)?;
                    "cancelled"
                }
            }
            ("paused", "pause")
            | ("moving", "resume")
            | ("cancelled" | "reverting" | "undo_cleanup", "cancel") => return Ok(job),
            _ => return Err(Error::OperationAborted.into()),
        };
        moves::state(job.id, next).execute(&mut *tx).await?;
        save(&mut tx, value).await?;
        job.state = next.to_owned();
        tx.commit().await?;
        Ok(job)
    }

    async fn move_rate(&self, cluster: Uuid, source: Uuid) -> Result<Option<u64>> {
        let mut tx = q::begin(self).await?;
        let Some(job) = super::queries::storage_move::assigned(cluster, source)
            .fetch_optional(&mut *tx)
            .await?
        else {
            return Ok(None);
        };
        ensure!(job.5 != "paused", Error::OperationAborted);
        Ok(Some(job.6.try_into()?))
    }

    async fn move_live(&self, id: Uuid, source: Uuid, target: Uuid) -> Result<bool> {
        let mut tx = q::begin(self).await?;
        let job = member_move(
            super::queries::storage_move::job(id)
                .fetch_one(&mut *tx)
                .await?,
        );
        Ok(match job.state.as_str() {
            "moving" => job.source == source && job.target == target,
            "reverting" => job.source == target && job.target == source,
            _ => false,
        })
    }

    async fn moved_backup(
        &self,
        key: &Key,
        job: Uuid,
        slot: Uuid,
        r: &crate::model::storage_payload::Receipt,
        completion: Option<&crate::model::storage_transfer::Completion>,
    ) -> Result<()> {
        use super::queries::{storage_backup as b, storage_capacity as cap, storage_move as m};
        r.validate()?;
        let mut tx = q::begin(self).await?;
        let job = member_move(m::job(job).fetch_one(&mut *tx).await?);
        ensure!(
            job.group.is_nil() && job.cluster == r.header.cluster,
            Error::OperationAborted
        );
        let file = b::file(slot, r.header.sequence, r.header.shard)
            .fetch_one(&mut *tx)
            .await?;
        ensure!(&file.1.0 == r, Error::OperationAborted);
        let undo = matches!(job.state.as_str(), "reverting" | "undo_cleanup");
        let (source, target) = if undo {
            (job.target, job.source)
        } else {
            (job.source, job.target)
        };
        let selected = if job.state == "undo_cleanup" {
            m::backup_remaining(job.id).fetch_optional(&mut *tx).await?
        } else {
            m::backup_list(job.id, completion.is_none() || undo)
                .fetch_optional(&mut *tx)
                .await?
        }
        .context("backup move checkpoint changed")?;
        ensure!(
            selected.0 == slot && selected.1.0 == *r,
            Error::OperationAborted
        );
        if let Some(proof) = completion {
            ensure!(
                matches!(job.state.as_str(), "moving" | "reverting")
                    && file.0 == source
                    && proof.authorization.grant.media == target
                    && matches!(&proof.authorization.grant.action,crate::model::storage_transfer::Action::BackupCheck{receipt,..} if receipt==r)
                    && proof.receipt.receipt == *r,
                Error::OperationAborted
            );
            let alloc = super::storage_capacity::lock(self, &mut tx, job.cluster).await?;
            let dest = alloc
                .targets
                .iter()
                .find(|t| t.device.media_id == target)
                .ok_or(Error::NodeInactive)?;
            proof.verify(key, &alloc.cluster.manifest, dest)?;
            ensure!(
                m::backup_switch(
                    slot,
                    r,
                    source,
                    target,
                    chrono::Utc::now().timestamp_micros()
                )
                .execute(&mut *tx)
                .await?
                .rows_affected()
                    == 1,
                Error::OperationAborted
            );
            if undo {
                m::backup_restored(job.id, slot, r.header.sequence, r.header.shard)
                    .execute(&mut *tx)
                    .await?;
            } else {
                m::backup_confirm(job.id, slot, r.header.sequence, r.header.shard)
                    .execute(&mut *tx)
                    .await?;
            }
            cap::stamp(r.header.operation, target, proof.receipt.accounting, true)
                .execute(&mut *tx)
                .await?;
        } else {
            ensure!(
                matches!(job.state.as_str(), "cleaning" | "undo_cleanup") && file.0 == target,
                Error::OperationAborted
            );
            if undo {
                m::backup_discarded(job.id, slot, r.header.sequence, r.header.shard)
                    .execute(&mut *tx)
                    .await?;
            } else {
                m::backup_removed(job.id, slot, r.header.sequence, r.header.shard)
                    .execute(&mut *tx)
                    .await?;
            }
            m::backup_release(r.header.operation, source)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn move_evidence(
        &self,
        id: Uuid,
        namespace: Uuid,
        proof: &crate::model::storage_recovery::Proof,
    ) -> Result<()> {
        use super::queries::{storage_journal as j, storage_move as moves};
        let mut tx = q::begin(self).await?;
        let job = member_move(moves::job(id).fetch_one(&mut *tx).await?);
        ensure!(
            matches!(job.state.as_str(), "moving" | "reverting")
                && proof.publication.namespace == namespace
                && proof.publication.cluster == job.cluster,
            Error::OperationAborted
        );
        let media = if job.state == "reverting" {
            job.source
        } else {
            job.target
        };
        let rev = proof.publication.revision;
        let (old, manifest) = j::history(namespace, rev - 1, rev)
            .fetch_one(&mut *tx)
            .await?;
        ensure!(old.0 == *proof, Error::OperationAborted);
        proof.verify(&manifest)?;
        j::ensure_evidence(namespace, media)
            .execute(&mut *tx)
            .await?;
        if j::evidence(namespace, media).fetch_one(&mut *tx).await? < rev as i64 {
            ensure!(
                j::receipt(namespace, media, rev)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected()
                    == 1,
                Error::OperationAborted
            );
        }
        tx.commit().await?;
        Ok(())
    }

    async fn advance_move(&self) -> Result<Option<MoveStep>> {
        use super::queries::{storage_move as moves, storage_payload as physical, storage_repair};
        let mut tx = q::begin(self).await?;
        let now = chrono::Utc::now().timestamp_micros();
        let Some(id) = moves::due(now).fetch_optional(&mut *tx).await? else {
            for (node, cid, source) in moves::draining().fetch_all(&mut *tx).await? {
                let value = cluster(q::cluster(cid).fetch_one(&mut *tx).await?)?;
                if moves::occupied(source, source).fetch_one(&mut *tx).await? {
                    continue;
                }
                let slots = moves::status(source).fetch_one(&mut *tx).await?;
                if !value
                    .manifest
                    .groups
                    .iter()
                    .any(|g| g.members.contains(&source))
                    && slots.0 == 0
                    && slots.3 == 0
                {
                    continue;
                }
                if !super::queries::node_handoff::gate(node)
                    .fetch_one(&mut *tx)
                    .await?
                {
                    continue;
                }
                let input = MoveInput {
                    revision: value.revision,
                    source,
                    target: None,
                    bytes_per_second: None,
                };
                // A blocked source must not prevent another drain from claiming an eligible slot.
                use sqlx::Acquire;
                let mut savepoint = tx.begin().await?;
                if begin_move(self, &mut savepoint, value, input, true)
                    .await
                    .is_ok()
                {
                    savepoint.commit().await?;
                    tx.commit().await?;
                    return Ok(Some(MoveStep::Advanced));
                }
                savepoint.rollback().await?;
            }
            return Ok(None);
        };
        let job = member_move(moves::job(id).fetch_one(&mut *tx).await?);
        let undo = matches!(job.state.as_str(), "reverting" | "undo_cleanup");
        let (source, target) = if undo {
            (job.target, job.source)
        } else {
            (job.source, job.target)
        };
        if !matches!(
            job.state.as_str(),
            "moving" | "reverting" | "cleaning" | "undo_cleanup"
        ) {
            return Ok(None);
        }
        let mut step = MoveStep::Advanced;
        moves::checked(job.id, now + 2_000_000)
            .execute(&mut *tx)
            .await?;
        let mut value = cluster(q::cluster(job.cluster).fetch_one(&mut *tx).await?)?;
        if job.group.is_nil() {
            step = backup_step(self, &mut tx, &job, &value).await?;
        } else if matches!(job.state.as_str(), "cleaning" | "undo_cleanup") {
            // A returning disk can acknowledge its last reservation even with no new writes.
            super::storage_capacity::lock(self, &mut tx, job.cluster).await?;
            if let Some(intent) = moves::cleanup(source, now).fetch_optional(&mut *tx).await? {
                physical::claim_collection(intent.operation, now, now + 5_000_000)
                    .execute(&mut *tx)
                    .await?;
                step = MoveStep::Collect {
                    namespace: intent.namespace,
                    operation: intent.operation,
                    area: intent.area,
                };
            } else if let Some(intent) =
                moves::retired(source, now).fetch_optional(&mut *tx).await?
            {
                if physical::claim_collection(intent.operation, now, now + 5_000_000)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected()
                    == 1
                {
                    step = MoveStep::Collect {
                        namespace: intent.namespace,
                        operation: intent.operation,
                        area: intent.area,
                    };
                }
            } else if !moves::remains(source).fetch_one(&mut *tx).await? {
                moves::clear_evidence(source).execute(&mut *tx).await?;
                moves::state(id, if undo { "cancelled" } else { "completed" })
                    .execute(&mut *tx)
                    .await?;
            }
        } else {
            let rows = if moves::transferring(source).fetch_one(&mut *tx).await? {
                Vec::new()
            } else {
                moves::waiting(source).fetch_all(&mut *tx).await?
            };
            for row in &rows {
                let mapping = super::storage_payload::mapping(row.clone())?;
                let h = &mapping.header;
                let origin = match mapping.receipt {
                    Some(receipt) => crate::model::storage_repair::Origin::Durable(receipt),
                    None => {
                        let Some(row) = physical::receipt_peer(h).fetch_optional(&mut *tx).await?
                        else {
                            continue;
                        };
                        let peer = super::storage_payload::mapping(row)?
                            .receipt
                            .context("peer lacks a durable receipt")?;
                        crate::model::storage_repair::Origin::Planned {
                            header: h.clone(),
                            peer,
                        }
                    }
                };
                origin.validate()?;
                storage_repair::create(&origin).execute(&mut *tx).await?;
                storage_repair::restart(h.fragment)
                    .execute(&mut *tx)
                    .await?;
                physical::enqueue_fill(h.fragment, h.namespace)
                    .execute(&mut *tx)
                    .await?;
                storage_repair::restart_task(h.fragment)
                    .execute(&mut *tx)
                    .await?;
            }
            if let Some((namespace, through, after)) = moves::lagging(source, target)
                .fetch_optional(&mut *tx)
                .await?
            {
                step = MoveStep::Evidence {
                    job: id,
                    media: target,
                    namespace,
                    after: after.try_into()?,
                    through: through.try_into()?,
                };
            } else if rows.is_empty()
                && !moves::live(source).fetch_one(&mut *tx).await?
                && !moves::prepared(job.cluster, source)
                    .fetch_one(&mut *tx)
                    .await?
                && (!undo
                    || !moves::writing(job.source, job.target)
                        .fetch_one(&mut *tx)
                        .await?)
            {
                if undo {
                    moves::uncapture(job.source).execute(&mut *tx).await?;
                }
                value
                    .manifest
                    .finish_replacement(job.group, job.source, job.target, !undo)?;
                save(&mut tx, value).await?;
                moves::state(id, if undo { "undo_cleanup" } else { "cleaning" })
                    .execute(&mut *tx)
                    .await?;
            }
        }
        tx.commit().await?;
        Ok(Some(step))
    }

    async fn space(&self, principal: &Principal, id: Uuid) -> Result<Space> {
        let mut tx = q::begin(self).await?;
        q::admin(&mut tx, principal).await?;
        let value = cluster(q::cluster(id).fetch_one(&mut *tx).await?)?;
        let targets = q::targets(self, &mut tx, Some(id), None, false).await?;
        let totals = super::queries::storage_capacity::totals(id)
            .fetch_all(&mut *tx)
            .await?
            .into_iter()
            .collect();
        let status = super::storage_capacity::observations(
            &mut tx,
            &value,
            &targets,
            &totals,
            chrono::Utc::now().timestamp_micros(),
        )
        .await?;
        let used = targets
            .iter()
            .filter(|t| t.device.announcement.topology.purpose == "primary")
            .fold(0u64, |n, t| {
                n.saturating_add(t.device.announcement.capacity.used_bytes)
            });
        let space = placement::space(&value.manifest, &value.policy.layout, &status, used);
        tx.commit().await?;
        Ok(space)
    }

    async fn backup_points(
        &self,
        principal: &Principal,
        id: Uuid,
        after: Option<Uuid>,
    ) -> Result<BackupPoints> {
        use super::queries::{storage_backup as b, storage_journal as j};
        use std::collections::BTreeMap;
        let mut tx = q::begin(self).await?;
        q::admin(&mut tx, principal).await?;
        let cluster = cluster(q::cluster(id).fetch_one(&mut *tx).await?)?;
        let mut groups = BTreeMap::<String, Vec<Uuid>>::new();
        for m in cluster
            .manifest
            .members
            .iter()
            .filter(|m| m.topology.purpose == "backup")
        {
            groups
                .entry(m.topology.storage_group.clone())
                .or_default()
                .push(m.media_id);
        }
        let mut out = Vec::new();
        let namespaces = b::namespaces(id, after.unwrap_or(Uuid::nil()))
            .fetch_all(&mut *tx)
            .await?;
        let next = (namespaces.len() > 8).then(|| namespaces[7].0);
        for (storage, name) in namespaces.into_iter().take(8) {
            let latest = j::latest(storage).fetch_optional(&mut *tx).await?;
            let revision = latest
                .as_ref()
                .map_or(0, |(p, _)| p.publication.revision as i64);
            for (group, members) in &groups {
                let (missing_bodies, missing_metadata) =
                    b::gaps(storage, group).fetch_one(&mut *tx).await?;
                let mut evidence = 0;
                for media in members {
                    evidence = evidence.max(
                        j::evidence(storage, *media)
                            .fetch_optional(&mut *tx)
                            .await?
                            .unwrap_or(0),
                    );
                }
                let recoverable_through = latest.as_ref().and_then(|(p, settled)| {
                    (*settled
                        && evidence == revision
                        && missing_bodies == 0
                        && missing_metadata == 0)
                        .then_some(p.publication.through)
                });
                out.push(BackupPoint {
                    storage,
                    name: name.clone(),
                    group: group.clone(),
                    missing_bodies,
                    missing_metadata,
                    published_revision: revision,
                    evidence_revision: evidence,
                    recoverable_through,
                });
            }
        }
        tx.commit().await?;
        Ok(BackupPoints {
            entries: out,
            after: next,
        })
    }

    async fn verify_backups(&self, principal: &Principal, id: Uuid, revision: i64) -> Result<()> {
        let mut tx = q::begin(self).await?;
        q::admin(&mut tx, principal).await?;
        current(&mut tx, id, revision).await?;
        super::queries::storage_backup::verify(id, chrono::Utc::now().timestamp_micros())
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn overview(&self) -> Result<Overview> {
        let mut tx = q::begin(self).await?;
        let clusters = q::clusters().fetch_all(&mut *tx).await?;
        let devices = q::devices().fetch_all(&mut *tx).await?;
        ensure!(
            clusters.len() <= 256 && devices.len() <= 1024,
            "storage catalog exceeds the management page limit"
        );
        let result = Overview {
            defaults: Policy::default(),
            runs: super::queries::storage_runs::list()
                .fetch_all(&mut *tx)
                .await?,
            member_moves: super::queries::storage_move::list()
                .fetch_all(&mut *tx)
                .await?
                .into_iter()
                .map(member_move)
                .collect(),
            backups: super::queries::storage_backup::stats()
                .fetch_all(&mut *tx)
                .await?
                .into_iter()
                .map(
                    |(
                        cluster,
                        storage_group,
                        pending,
                        verified,
                        pending_bytes,
                        oldest_pending,
                        last_verified,
                    )| BackupGroup {
                        cluster,
                        storage_group,
                        pending,
                        verified,
                        pending_bytes,
                        oldest_pending,
                        last_verified,
                    },
                )
                .collect(),
            clusters: clusters.into_iter().map(cluster).collect::<Result<_>>()?,
            devices: devices.into_iter().map(device).collect(),
        };
        tx.commit().await?;
        Ok(result)
    }

    async fn create(&self, principal: &Principal, key: &Key, input: Create) -> Result<Cluster> {
        crate::config::storage::label(&input.name, "cluster name")?;
        input.policy.validate()?;
        let mut tx = q::begin(self).await?;
        q::admin(&mut tx, principal).await?;
        ensure!(
            q::clusters().fetch_all(&mut *tx).await?.len() < 256,
            "storage cluster limit reached"
        );
        let deployment = q::deployment().fetch_one(&mut *tx).await?;
        let id = Uuid::now_v7();
        let value = Cluster {
            id,
            name: input.name,
            revision: 1,
            policy: input.policy,
            state: "waiting_members".into(),
            manifest: Manifest {
                record_id: Uuid::now_v7(),
                format_version: FORMAT,
                cluster_id: id,
                deployment_id: deployment,
                publication_key: crate::model::storage_recovery::public_key(key, id)?,
                takeover_epoch: 1,
                revision: 1,
                previous: None,
                members: Vec::new(),
                groups: Vec::new(),
            },
        };
        q::insert(&value).execute(&mut *tx).await?;
        q::history(&value, &value.manifest.digest()?)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(value)
    }

    async fn update(&self, principal: &Principal, id: Uuid, input: Update) -> Result<Cluster> {
        crate::config::storage::label(&input.name, "cluster name")?;
        input.policy.validate()?;
        let mut tx = q::begin(self).await?;
        q::admin(&mut tx, principal).await?;
        let mut value = current(&mut tx, id, input.revision).await?;
        if input.policy.backup_copies > value.policy.backup_copies {
            super::queries::storage_backup::schedule(id)
                .execute(&mut *tx)
                .await?;
        }
        value.name = input.name;
        value.policy = input.policy;
        let value = save(&mut tx, value).await?;
        tx.commit().await?;
        Ok(value)
    }

    async fn add_members(
        &self,
        principal: &Principal,
        id: Uuid,
        input: Members,
    ) -> Result<Cluster> {
        validate_ids(&input.media_ids)?;
        let mut tx = q::begin(self).await?;
        q::admin(&mut tx, principal).await?;
        let mut value = current(&mut tx, id, input.revision).await?;
        ensure!(
            value.manifest.members.len() + input.media_ids.len() <= MEMBER_LIMIT,
            "cluster member limit reached"
        );
        for media in input.media_ids {
            let observed = device(
                q::device(media)
                    .fetch_optional(&mut *tx)
                    .await?
                    .ok_or_else(|| s3s::s3_error!(NoSuchKey))?,
            );
            ensure!(
                observed.cluster_id.is_none(),
                s3s::s3_error!(InvalidRequest, "storage already belongs to a cluster")
            );
            ensure!(
                observed.announcement.manifest.is_none()
                    && observed.announcement.pending_manifest.is_none(),
                s3s::s3_error!(InvalidRequest, "existing layout requires takeover review")
            );
            ensure!(
                q::node_active(&mut tx, observed.node_id).await?,
                Error::NodeInactive
            );
            q::assign(media, id).execute(&mut *tx).await?;
            value.manifest.members.push(Member {
                media_id: media,
                node_id: observed.node_id,
                topology: observed.announcement.topology,
                state: "available".into(),
            });
        }
        value.manifest.members.sort_by_key(|member| member.media_id);
        let value = save(&mut tx, value).await?;
        tx.commit().await?;
        Ok(value)
    }

    async fn enable_groups(
        &self,
        principal: &Principal,
        id: Uuid,
        input: Members,
    ) -> Result<Cluster> {
        validate_ids(&input.media_ids)?;
        let mut tx = q::begin(self).await?;
        q::admin(&mut tx, principal).await?;
        let mut value = cluster(q::cluster(id).fetch_one(&mut *tx).await?)?;
        ensure!(input.revision > 0, Error::OperationAborted);
        ensure!(
            value.policy.layout.data_shards == 1
                && value.policy.layout.parity_shards == 0
                && value.policy.layout.extra_copies == 1,
            s3s::s3_error!(NotImplemented, "only the two-copy layout is available")
        );
        let width = value.policy.layout.width();
        ensure!(
            input.media_ids.len().is_multiple_of(width),
            s3s::s3_error!(InvalidArgument, "select complete layout groups")
        );
        if input.media_ids.chunks(width).all(|members| {
            value
                .manifest
                .groups
                .iter()
                .any(|g| g.layout == value.policy.layout && g.members == members)
        }) {
            if value.revision != input.revision {
                let previous =
                    super::queries::storage_payload::manifest(id, input.revision.try_into()?)
                        .fetch_one(&mut *tx)
                        .await?;
                ensure!(
                    value.revision.checked_sub(1) == Some(input.revision)
                        && !input
                            .media_ids
                            .iter()
                            .any(|m| previous.groups.iter().any(|g| g.members.contains(m))),
                    Error::OperationAborted
                );
            }
            tx.commit().await?;
            return Ok(value);
        }
        ensure!(value.revision == input.revision, Error::OperationAborted);
        for media in &input.media_ids {
            let member = value
                .manifest
                .members
                .iter()
                .find(|m| m.media_id == *media)
                .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
            ensure!(
                !super::queries::storage_move::occupied(*media, *media)
                    .fetch_one(&mut *tx)
                    .await?
                    && member.state == "available"
                    && !super::queries::node_handoff::gate(member.node_id)
                        .fetch_one(&mut *tx)
                        .await?,
                Error::NodeInactive
            );
            let observed = device(q::device(*media).fetch_one(&mut *tx).await?);
            ensure!(
                q::node_active(&mut tx, member.node_id).await?
                    && observed.announcement.capacity.writable()
                        >= u64::from(value.policy.layout.stripe_bytes),
                s3s::s3_error!(
                    InvalidRequest,
                    "every selected storage needs room for a stripe"
                )
            );
        }
        for members in input.media_ids.chunks(width) {
            value.manifest.groups.push(Group {
                replacing: None,
                id: Uuid::now_v7(),
                layout: value.policy.layout.clone(),
                members: members.to_vec(),
            });
        }
        let value = save(&mut tx, value).await?;
        tx.commit().await?;
        Ok(value)
    }

    async fn member_state(
        &self,
        principal: &Principal,
        id: Uuid,
        media: Uuid,
        input: MemberState,
    ) -> Result<Cluster> {
        ensure!(
            matches!(input.state.as_str(), "available" | "paused" | "spare"),
            s3s::s3_error!(InvalidArgument, "unknown storage member state")
        );
        let mut tx = q::begin(self).await?;
        q::admin(&mut tx, principal).await?;
        let mut value = current(&mut tx, id, input.revision).await?;
        if input.state == "spare" {
            ensure!(
                !value
                    .manifest
                    .groups
                    .iter()
                    .any(|group| group.members.contains(&media)),
                "assigned storage cannot become a spare before evacuation"
            );
        }
        let member = value
            .manifest
            .members
            .iter_mut()
            .find(|m| m.media_id == media)
            .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
        ensure!(
            input.state != "spare" || member.topology.purpose == "primary",
            "only primary storage can be a spare"
        );
        member.state = input.state;
        let value = save(&mut tx, value).await?;
        tx.commit().await?;
        Ok(value)
    }

    async fn announce(
        &self,
        node: &NodeState,
        key: &Key,
        run: Run,
        input: Announcement,
    ) -> Result<Registration> {
        ensure!(node.role == "storage", Error::NodeAuthentication);
        input.validate()?;
        if let Some(stamp) = input.capacity.accounting {
            ensure!(
                stamp.boot == run.boot,
                "storage capacity belongs to another boot"
            );
        }
        let mut tx = q::begin(self).await?;
        q::authenticate(&mut tx, node, key, run).await?;
        if let Some(bound) = q::node_media(node.node_id).fetch_optional(&mut *tx).await? {
            ensure!(
                bound == input.media.media_id,
                s3s::s3_error!(
                    InvalidRequest,
                    "node is bound to another media; check the mount"
                )
            );
        }
        let old = q::device_for_update(input.media.media_id)
            .fetch_optional(&mut *tx)
            .await?
            .map(device);
        if let Some(old) = &old {
            if let (Some(before), Some(after)) = (
                old.announcement.capacity.accounting,
                input.capacity.accounting,
            ) {
                ensure!(
                    before.boot != after.boot || before.revision <= after.revision,
                    "storage capacity report moved backwards"
                );
            }
            ensure!(
                old.node_id == node.node_id || !q::node_active(&mut tx, old.node_id).await?,
                Error::NodeConflict
            );
            ensure!(
                old.node_id == node.node_id || old.cluster_id.is_none(),
                s3s::s3_error!(
                    InvalidRequest,
                    "member identity change requires takeover review"
                )
            );
        } else {
            ensure!(
                q::devices().fetch_all(&mut *tx).await?.len() < 1024,
                "storage device limit reached"
            );
        }
        let mut reply = Registration {
            manifest: None,
            history: Vec::new(),
        };
        if let Some(id) = old.as_ref().and_then(|old| old.cluster_id) {
            let mut value = cluster(q::cluster(id).fetch_one(&mut *tx).await?)?;
            let member = value
                .manifest
                .members
                .iter_mut()
                .find(|m| m.media_id == input.media.media_id)
                .context("registered member is missing")?;
            let changed = member.topology != input.topology;
            if changed {
                ensure!(
                    member.topology.purpose == input.topology.purpose
                        && member.topology.storage_group == input.topology.storage_group,
                    s3s::s3_error!(
                        InvalidRequest,
                        "drain the member before changing its purpose or storage group"
                    )
                );
                member.topology = input.topology.clone();
                value = save(&mut tx, value).await?;
            }
            for observed in input.manifest.iter().chain(input.pending_manifest.iter()) {
                ensure!(
                    observed.cluster_id == id
                        && observed.takeover_epoch <= value.manifest.takeover_epoch
                        && observed.revision <= value.revision,
                    Error::OperationAborted
                );
                let known = q::historical_hash(id, observed.revision)
                    .fetch_optional(&mut *tx)
                    .await?;
                ensure!(
                    known.as_deref() == Some(observed.digest()?.as_str()),
                    Error::OperationAborted
                );
            }
            ensure!(
                input.history_after <= value.revision,
                Error::OperationAborted
            );
            let mut bytes = 0;
            for record in q::histories(id, input.history_after)
                .fetch_all(&mut *tx)
                .await?
            {
                let size = serde_json::to_vec(&record.0)?.len();
                if bytes + size > 256 * 1024 && !reply.history.is_empty() {
                    break;
                }
                bytes += size;
                reply.history.push(record.0);
            }
            reply.manifest = Some(value.manifest);
        }
        q::announce(node.node_id, &input).execute(&mut *tx).await?;
        q::authenticate(&mut tx, node, key, run).await?;
        tx.commit().await?;
        Ok(reply)
    }
}
