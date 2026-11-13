use crate::{authorization::Principal, cluster::NodeState, content::codec::Key};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

pub(crate) const FORMAT: u32 = 1;
pub(crate) const MEMBER_LIMIT: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = StorageClusterLayout)]
pub(crate) struct Layout {
    pub data_shards: u16,
    pub parity_shards: u16,
    pub extra_copies: u16,
    pub stripe_bytes: u32,
}

impl Default for Layout {
    fn default() -> Self {
        Self {
            data_shards: 1,
            parity_shards: 0,
            extra_copies: 1,
            stripe_bytes: 1024 * 1024,
        }
    }
}

impl Layout {
    pub fn width(&self) -> usize {
        (usize::from(self.data_shards) + usize::from(self.parity_shards))
            * (usize::from(self.extra_copies) + 1)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.data_shards > 0 && self.width() <= MEMBER_LIMIT,
            "layout requires 1..256 primary storage members"
        );
        ensure!(
            self.stripe_bytes.is_power_of_two()
                && (65536..=16 * 1024 * 1024).contains(&self.stripe_bytes),
            "stripe_bytes must be a power of two from 64 KiB to 16 MiB"
        );
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(as = StorageClusterAcknowledgement)]
pub(crate) enum Acknowledgement {
    All,
    DataAndParity,
    Data,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(default, deny_unknown_fields)]
#[schema(as = StorageClusterPolicy)]
pub(crate) struct Policy {
    pub layout: Layout,
    pub read_parallelism: u16,
    pub acknowledgement: Acknowledgement,
    pub degraded_acknowledgement: Option<Acknowledgement>,
    pub data_regions: String,
    pub parity_regions: String,
    pub replica_regions: String,
    pub node_priority: String,
    pub placement_order: String,
    pub backup_copies: u16,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            layout: Layout::default(),
            read_parallelism: 4,
            acknowledgement: Acknowledgement::All,
            degraded_acknowledgement: None,
            data_regions: "same_region".into(),
            parity_regions: "spread".into(),
            replica_regions: "different_region".into(),
            node_priority: "none".into(),
            placement_order: "region_first".into(),
            backup_copies: 0,
        }
    }
}

impl Policy {
    pub fn validate(&self) -> Result<()> {
        self.layout.validate()?;
        ensure!(
            (1..=256).contains(&self.read_parallelism) && self.backup_copies <= 256,
            "invalid read_parallelism or backup_copies"
        );
        for (field, value, allowed) in [
            (
                "data_regions",
                &self.data_regions,
                &["same_region", "any"][..],
            ),
            (
                "parity_regions",
                &self.parity_regions,
                &["spread", "data_region", "any"][..],
            ),
            (
                "replica_regions",
                &self.replica_regions,
                &["different_region", "same_region", "original", "any"][..],
            ),
            (
                "node_priority",
                &self.node_priority,
                &["none", "idle", "free", "fill"][..],
            ),
            (
                "placement_order",
                &self.placement_order,
                &["region_first", "node_first"][..],
            ),
        ] {
            ensure!(allowed.contains(&value.as_str()), "invalid {field}");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = StorageClusterMedia)]
pub(crate) struct Media {
    pub format_version: u32,
    pub media_id: Uuid,
}

impl Media {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.format_version == FORMAT && !self.media_id.is_nil(),
            "invalid or unsupported storage media"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = StorageClusterTopology)]
pub(crate) struct Topology {
    pub url: String,
    pub region: Option<String>,
    pub storage_group: String,
    pub purpose: String,
}

impl Topology {
    pub fn validate(&self) -> Result<()> {
        crate::config::storage::url(&self.url)?;
        crate::config::storage::label(&self.storage_group, "storage_group")?;
        if let Some(region) = &self.region {
            crate::config::storage::label(region, "region")?;
        }
        ensure!(
            matches!(self.purpose.as_str(), "primary" | "backup"),
            "invalid storage purpose"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = StorageClusterCapacity)]
pub(crate) struct Capacity {
    #[serde(default)]
    pub accounting_ready: bool,
    pub limit: Option<u64>,
    pub used_bytes: u64,
    pub reserved_bytes: u64,
    pub available_bytes: u64,
    pub min_free_space: u64,
}

impl Capacity {
    pub fn writable(&self) -> u64 {
        if !self.accounting_ready {
            return 0;
        }
        self.limit
            .map_or(u64::MAX, |limit| {
                limit
                    .saturating_sub(self.used_bytes)
                    .saturating_sub(self.reserved_bytes)
            })
            .min(
                self.available_bytes
                    .saturating_sub(self.min_free_space)
                    .saturating_sub(self.reserved_bytes),
            )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = StorageClusterMember)]
pub(crate) struct Member {
    pub media_id: Uuid,
    pub node_id: Uuid,
    pub topology: Topology,
    pub state: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = StorageClusterGroup)]
pub(crate) struct Group {
    pub id: Uuid,
    pub layout: Layout,
    pub members: Vec<Uuid>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = StorageClusterManifest)]
pub(crate) struct Manifest {
    pub format_version: u32,
    pub record_id: Uuid,
    pub cluster_id: Uuid,
    pub deployment_id: Uuid,
    pub publication_key: [u8; 32],
    pub takeover_epoch: u64,
    pub revision: i64,
    pub previous: Option<String>,
    pub members: Vec<Member>,
    pub groups: Vec<Group>,
}

impl Manifest {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.format_version == FORMAT
                && !self.record_id.is_nil()
                && !self.cluster_id.is_nil()
                && !self.deployment_id.is_nil()
                && self.publication_key != [0; 32]
                && self.takeover_epoch > 0
                && self.revision > 0
                && self.members.len() <= MEMBER_LIMIT,
            "invalid cluster manifest"
        );
        if let Some(hash) = &self.previous {
            ensure!(
                hash.len() == 64
                    && hash
                        .bytes()
                        .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
                "invalid manifest predecessor"
            );
        }
        let mut identities = std::collections::HashSet::new();
        let mut nodes = std::collections::HashSet::new();
        for member in &self.members {
            ensure!(
                !member.media_id.is_nil()
                    && !member.node_id.is_nil()
                    && identities.insert(member.media_id)
                    && nodes.insert(member.node_id),
                "duplicate or missing storage identity"
            );
            member.topology.validate()?;
            ensure!(
                matches!(member.state.as_str(), "available" | "paused" | "spare"),
                "invalid member state"
            );
        }
        let mut assigned = std::collections::HashSet::new();
        let mut groups = std::collections::HashSet::new();
        for group in &self.groups {
            group.layout.validate()?;
            ensure!(
                !group.id.is_nil()
                    && groups.insert(group.id)
                    && group.members.len() == group.layout.width(),
                "invalid layout group"
            );
            let mut domains = std::collections::HashSet::new();
            for id in &group.members {
                let member = self
                    .members
                    .iter()
                    .find(|member| member.media_id == *id)
                    .ok_or_else(|| anyhow::anyhow!("layout member is missing"))?;
                ensure!(
                    assigned.insert(*id) && member.topology.purpose == "primary",
                    "layout member is already assigned or not primary"
                );
                ensure!(
                    domains.insert(&member.topology.storage_group)
                        || group.layout.parity_shards == 0 && group.layout.extra_copies == 0,
                    "layout requires independent storage groups"
                );
            }
        }
        ensure!(
            serde_json::to_vec(self)?.len() <= 128 * 1024,
            "cluster manifest exceeds 128 KiB"
        );
        Ok(())
    }

    pub fn digest(&self) -> Result<String> {
        self.validate()?;
        Ok(blake3::hash(&serde_json::to_vec(self)?)
            .to_hex()
            .to_string())
    }
}

#[derive(Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = StorageClusterAnnouncement)]
pub(crate) struct Announcement {
    pub media: Media,
    pub topology: Topology,
    pub capacity: Capacity,
    pub manifest: Option<Manifest>,
    pub pending_manifest: Option<Manifest>,
    pub history_after: i64,
}

impl Announcement {
    pub fn validate(&self) -> Result<()> {
        ensure!(self.history_after >= 0, "invalid history cursor");
        self.media.validate()?;
        self.topology.validate()?;
        for manifest in self.manifest.iter().chain(self.pending_manifest.iter()) {
            manifest.validate()?;
            ensure!(
                manifest
                    .members
                    .iter()
                    .any(|member| member.media_id == self.media.media_id),
                "manifest does not contain this media"
            );
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Registration {
    pub manifest: Option<Manifest>,
    pub history: Vec<Manifest>,
}

#[derive(Clone, Serialize, Deserialize, ToSchema)]
#[schema(as = StorageClusterDevice)]
pub(crate) struct Device {
    pub node_id: Uuid,
    pub media_id: Uuid,
    pub cluster_id: Option<Uuid>,
    pub observed_at: i64,
    pub announcement: Announcement,
}

#[derive(Clone, Serialize, Deserialize, ToSchema)]
#[schema(as = StorageClusterCluster)]
pub(crate) struct Cluster {
    pub id: Uuid,
    pub name: String,
    pub revision: i64,
    pub policy: Policy,
    pub manifest: Manifest,
    pub state: String,
}

#[derive(Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = StorageClusterCreate)]
pub(crate) struct Create {
    pub name: String,
    #[serde(default)]
    pub policy: Policy,
}

#[derive(Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = StorageClusterUpdate)]
pub(crate) struct Update {
    pub revision: i64,
    pub name: String,
    pub policy: Policy,
}

#[derive(Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = StorageClusterMembers)]
pub(crate) struct Members {
    pub revision: i64,
    pub media_ids: Vec<Uuid>,
}

#[derive(Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = StorageClusterMemberState)]
pub(crate) struct MemberState {
    pub revision: i64,
    pub state: String,
}

#[derive(Serialize, Deserialize, ToSchema)]
#[schema(as = StorageClusterOverview)]
pub(crate) struct Overview {
    pub defaults: Policy,
    pub clusters: Vec<Cluster>,
    pub devices: Vec<Device>,
}

#[async_trait::async_trait]
pub(crate) trait StorageClusters: Send + Sync {
    async fn overview(&self) -> Result<Overview>;
    async fn create(&self, principal: &Principal, key: &Key, input: Create) -> Result<Cluster>;
    async fn update(&self, principal: &Principal, id: Uuid, input: Update) -> Result<Cluster>;
    async fn add_members(&self, principal: &Principal, id: Uuid, input: Members)
    -> Result<Cluster>;
    async fn enable_groups(
        &self,
        principal: &Principal,
        id: Uuid,
        input: Members,
    ) -> Result<Cluster>;
    async fn member_state(
        &self,
        principal: &Principal,
        id: Uuid,
        media: Uuid,
        input: MemberState,
    ) -> Result<Cluster>;
    async fn announce(
        &self,
        node: &NodeState,
        key: &Key,
        run: super::session::Run,
        input: Announcement,
    ) -> Result<Registration>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn region_names_do_not_create_failure_domains() {
        let members: Vec<_> = ["a", "b"]
            .into_iter()
            .map(|region| Member {
                media_id: Uuid::new_v4(),
                node_id: Uuid::new_v4(),
                state: "available".into(),
                topology: Topology {
                    url: "https://storage.example".into(),
                    region: Some(region.into()),
                    storage_group: "host".into(),
                    purpose: "primary".into(),
                },
            })
            .collect();
        let mut manifest = Manifest {
            record_id: Uuid::now_v7(),
            format_version: FORMAT,
            cluster_id: Uuid::new_v4(),
            deployment_id: Uuid::new_v4(),
            publication_key: [1; 32],
            takeover_epoch: 1,
            revision: 1,
            previous: None,
            groups: vec![Group {
                id: Uuid::new_v4(),
                layout: Layout::default(),
                members: members.iter().map(|m| m.media_id).collect(),
            }],
            members,
        };
        assert!(manifest.validate().is_err());
        manifest.members[1].topology.storage_group = "host-2".into();
        assert!(manifest.validate().is_ok());
        manifest.members[1].topology.storage_group = "host".into();
        manifest.groups[0].layout.data_shards = 2;
        manifest.groups[0].layout.extra_copies = 0;
        assert!(manifest.validate().is_ok());
    }
}
