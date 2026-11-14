use super::{
    storage_cluster::{Member, placement::Status},
    storage_payload::Receipt,
    storage_transfer::Completion,
};
use crate::content::codec::Key;
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub(crate) struct File {
    pub media: Uuid,
    pub receipt: Receipt,
    pub verified: bool,
    pub recheck: bool,
}

pub(crate) fn choose<'a>(
    members: &'a [Member],
    status: &HashMap<Uuid, Status>,
    group: Option<&str>,
    occupied: &HashSet<String>,
    bytes: u64,
) -> Option<&'a Member> {
    let group = group.or_else(|| {
        members
            .iter()
            .filter(|m| {
                m.topology.purpose == "backup" && !occupied.contains(&m.topology.storage_group)
            })
            .map(|m| m.topology.storage_group.as_str())
            .min()
    })?;
    members
        .iter()
        .filter(|member| {
            member.topology.purpose == "backup"
                && member.state == "available"
                && member.topology.storage_group == group
                && status
                    .get(&member.media_id)
                    .is_some_and(|s| s.online && s.free >= bytes)
        })
        .max_by_key(|member| {
            (
                status[&member.media_id].free,
                std::cmp::Reverse(member.media_id),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::storage_cluster::Topology;

    #[test]
    fn copies_use_distinct_groups_and_keep_their_assigned_group() {
        let nodes: Vec<_> = [("a", 4), ("a", 9), ("b", 7)]
            .into_iter()
            .enumerate()
            .map(|(index, (group, free))| {
                let id = Uuid::from_u128(1 + index as u128);
                (
                    Member {
                        node_id: id,
                        media_id: id,
                        state: "available".into(),
                        topology: Topology {
                            url: "https://test".into(),
                            region: None,
                            storage_group: group.into(),
                            purpose: "backup".into(),
                        },
                    },
                    free,
                )
            })
            .collect();
        let status = nodes
            .iter()
            .map(|(m, free)| {
                (
                    m.media_id,
                    Status {
                        online: true,
                        free: *free,
                        requests: None,
                    },
                )
            })
            .collect();
        let members: Vec<_> = nodes.into_iter().map(|(m, _)| m).collect();
        let taken = ["a".to_owned()].into_iter().collect();
        assert_eq!(
            choose(&members, &status, None, &HashSet::new(), 3)
                .unwrap()
                .media_id,
            members[1].media_id
        );
        assert_eq!(
            choose(&members, &status, None, &taken, 3).unwrap().media_id,
            members[2].media_id
        );
        assert!(choose(&members, &status, Some("b"), &taken, 8).is_none());
    }
}

#[async_trait::async_trait]
pub(crate) trait Backups: Send + Sync {
    async fn backfill_backups(&self) -> Result<bool>;
    async fn backup_file(&self, id: Uuid, sequence: u32, shard: u16) -> Result<File>;
    async fn backup_link(&self, id: Uuid) -> Result<super::storage_recovery::Link>;
    async fn reset_evidence(&self, id: Uuid, media: Uuid, sequence: u32, shard: u16) -> Result<()>;
    async fn backup_evidence(
        &self,
        id: Uuid,
        media: Uuid,
    ) -> Result<
        Option<(
            super::storage_recovery::Proof,
            super::storage_cluster::Manifest,
        )>,
    >;
    async fn backup_published(
        &self,
        id: Uuid,
        media: Uuid,
        sequence: u32,
        shard: u16,
        revision: u64,
        link: &super::storage_recovery::Link,
    ) -> Result<()>;
    async fn backup_accept(&self, key: &Key, id: Uuid, proof: &Completion) -> Result<()>;
}
