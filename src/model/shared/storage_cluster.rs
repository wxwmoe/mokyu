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

async fn save(tx: &mut q::Tx<'_>, mut value: Cluster) -> Result<Cluster> {
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
        ensure!(
            q::cluster(anchor.cluster_id)
                .fetch_optional(&mut *tx)
                .await?
                .is_none(),
            "storage cluster is already registered"
        );
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
        let mut value = current(&mut tx, id, input.revision).await?;
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
        for media in &input.media_ids {
            let member = value
                .manifest
                .members
                .iter()
                .find(|m| m.media_id == *media)
                .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
            ensure!(member.state == "available", Error::NodeInactive);
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
