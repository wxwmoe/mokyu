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

    async fn create(&self, principal: &Principal, input: Create) -> Result<Cluster> {
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
                format_version: FORMAT,
                cluster_id: id,
                deployment_id: deployment,
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
        let old = q::device(input.media.media_id)
            .fetch_optional(&mut *tx)
            .await?
            .map(device);
        if let Some(old) = &old {
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
                        && observed.deployment_id == value.manifest.deployment_id
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
