use super::{Acknowledgement, Cluster, Member, Policy};
use anyhow::{Result, ensure};
use std::{cmp::Ordering, collections::HashMap};
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct Status {
    pub online: bool,
    pub draining: bool,
    pub free: u64,
    // Includes dispatched requests absent from the current storage sample, once each.
    pub requests: Option<(u64, u32)>,
}

pub(crate) struct Table {
    policy: Policy,
    groups: Vec<(Uuid, Vec<Member>)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Selection {
    pub group: Uuid,
    pub members: Vec<Uuid>,
    pub acknowledgement: Acknowledgement,
}

struct Candidate {
    selection: Selection,
    degraded: bool,
    region: (bool, bool),
    load: Option<(u64, u32)>,
    free: u64,
    sticky: bool,
}

pub(crate) fn space(
    manifest: &super::Manifest,
    layout: &super::Layout,
    status: &HashMap<Uuid, Status>,
    primary_used_bytes: u64,
) -> super::Space {
    let mut space = super::Space {
        groups: 0,
        writable_groups: 0,
        logical_bytes: 0,
        primary_used_bytes,
    };
    for group in &manifest.groups {
        if &group.layout != layout {
            continue;
        }
        space.groups += 1;
        if group.replacing.is_some() {
            continue;
        }
        let free = group
            .members
            .iter()
            .map(|id| {
                status
                    .get(id)
                    .filter(|s| {
                        s.online
                            && !s.draining
                            && manifest
                                .members
                                .iter()
                                .any(|m| m.media_id == *id && m.state == "available")
                    })
                    .map_or(0, |s| s.free)
            })
            .min()
            .unwrap_or(0);
        if free >= u64::from(layout.stripe_bytes) {
            space.writable_groups += 1;
            space.logical_bytes = space
                .logical_bytes
                .saturating_add(free.saturating_mul(u64::from(layout.data_shards)));
        }
    }
    space
}

impl Table {
    pub fn new(cluster: &Cluster) -> Result<Self> {
        cluster.manifest.validate()?;
        cluster.policy.validate()?;
        let layout = &cluster.policy.layout;
        ensure!(layout.supported(), "unsupported native write layout");
        let mut groups = Vec::new();
        for group in &cluster.manifest.groups {
            if group.layout != *layout || group.replacing.is_some() {
                continue;
            }
            let mut members: Vec<_> = group
                .members
                .iter()
                .map(|id| {
                    cluster
                        .manifest
                        .members
                        .iter()
                        .find(|member| member.media_id == *id)
                        .expect("validated group member")
                        .clone()
                })
                .collect();
            members.sort_by_key(|member| member.media_id);
            groups.push((group.id, members));
        }
        groups.sort_by_key(|(id, _)| *id);
        ensure!(
            !groups.is_empty(),
            s3s::s3_error!(
                ServiceUnavailable,
                "storage cluster has no enabled layout group"
            )
        );
        Ok(Self {
            policy: cluster.policy.clone(),
            groups,
        })
    }

    pub fn select(
        &self,
        payload: Uuid,
        region: Option<&str>,
        bytes: u64,
        status: &HashMap<Uuid, Status>,
        previous: Option<Uuid>,
    ) -> Result<Selection> {
        let start = payload.as_u128() as usize % self.groups.len();
        let simple = self.policy.placement_order == "simple";
        let mut best: Option<Candidate> = None;
        for step in 0..self.groups.len() {
            let (group, members) = &self.groups[(start + step) % self.groups.len()];
            if members
                .iter()
                .any(|m| status.get(&m.media_id).is_some_and(|s| s.draining))
            {
                continue;
            }
            // Simple mode keeps cached roles and does not rank telemetry or regions.
            let layout = &self.policy.layout;
            let k = usize::from(layout.data_shards);
            let width = k + usize::from(layout.parity_shards);
            let mut order: Vec<_> = members.iter().collect();
            let is_ready = |m: &Member| {
                m.state == "available"
                    && status
                        .get(&m.media_id)
                        .is_some_and(|s| s.online && s.free >= bytes)
            };
            if !simple {
                order = ordered_members(order, &self.policy, region);
            }
            for reverse in 0..if !simple && width == 1 && members.len() == 2 {
                2
            } else {
                1
            } {
                if reverse == 1 {
                    order.swap(0, 1);
                }
                let ordered = &order;
                let available: Vec<_> = ordered.iter().map(|m| is_ready(m)).collect();
                if !available[..k].iter().all(|a| *a) {
                    continue;
                }
                let degraded = available.iter().any(|a| !a);
                let acknowledgement = if degraded {
                    self.policy
                        .degraded_acknowledgement
                        .unwrap_or(self.policy.acknowledgement)
                } else {
                    self.policy.acknowledgement
                };
                if (acknowledgement == Acknowledgement::All && degraded)
                    || (acknowledgement == Acknowledgement::DataAndParity
                        && available[..width].iter().any(|v| !v))
                {
                    continue;
                }
                let selection = Selection {
                    group: *group,
                    members: ordered.iter().map(|m| m.media_id).collect(),
                    acknowledgement,
                };
                if simple {
                    return Ok(selection);
                }
                let samples = ordered.iter().map(|member| status.get(&member.media_id));
                let live: Vec<_> = samples.into_iter().flatten().filter(|s| s.online).collect();
                let load = live.iter().try_fold((0, 1), |worst, s| {
                    let value = s.requests.filter(|(_, slots)| *slots > 0)?;
                    Some(if ratio(value, worst) == Ordering::Greater {
                        value
                    } else {
                        worst
                    })
                });
                let primary = ordered[0].topology.region.as_deref();
                let mirror = ordered
                    .get(width)
                    .and_then(|m| m.topology.region.as_deref());
                let candidate = Candidate {
                    selection,
                    degraded,
                    region: (
                        self.policy.data_regions == "same_region"
                            && region.is_some()
                            && primary != region,
                        layout.extra_copies > 0
                            && match self.policy.replica_regions.as_str() {
                                "different_region" => {
                                    primary.is_none() || mirror.is_none() || primary == mirror
                                }
                                "same_region" => primary.is_none() || primary != mirror,
                                _ => false,
                            },
                    ),
                    load,
                    free: live.iter().map(|s| s.free).min().unwrap_or(0),
                    sticky: previous == Some(*group),
                };
                if best
                    .as_ref()
                    .is_none_or(|old| self.compare(&candidate, old).is_lt())
                {
                    best = Some(candidate);
                }
            }
        }
        best.map(|value| value.selection).ok_or_else(|| {
            s3s::s3_error!(
                SlowDown,
                "no storage group can satisfy the write acknowledgement and capacity"
            )
            .into()
        })
    }

    fn compare(&self, a: &Candidate, b: &Candidate) -> Ordering {
        let nodes = match self.policy.node_priority.as_str() {
            "idle" => match (a.load, b.load) {
                (Some(a), Some(b)) => ratio(a, b),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            },
            "free" => b.free.cmp(&a.free),
            "fill" => b.sticky.cmp(&a.sticky).then_with(|| a.free.cmp(&b.free)),
            _ => Ordering::Equal,
        };
        a.degraded.cmp(&b.degraded).then_with(|| {
            if self.policy.placement_order == "node_first" {
                nodes.then_with(|| a.region.cmp(&b.region))
            } else {
                a.region.cmp(&b.region).then(nodes)
            }
        })
    }
}

fn ordered_members<'a>(
    members: Vec<&'a Member>,
    policy: &Policy,
    writer: Option<&str>,
) -> Vec<&'a Member> {
    type Regions<'a> = std::collections::HashSet<&'a str>;
    let layout = &policy.layout;
    let k = usize::from(layout.data_shards);
    let m = usize::from(layout.parity_shards);
    let width = k + m;
    let mut left = members;
    let mut sets: Vec<Vec<&Member>> = Vec::with_capacity(usize::from(layout.extra_copies) + 1);
    let mut occupied = Regions::new();
    for index in 0..=layout.extra_copies {
        let own_rules = index == 0 || policy.replica_regions == "original";
        let take = if own_rules { k } else { width };
        if (own_rules && policy.data_regions == "same_region")
            || (!own_rules && policy.replica_regions != "any")
        {
            let mut counts = HashMap::new();
            for member in &left {
                if let Some(r) = member.topology.region.as_deref() {
                    *counts.entry(r).or_insert(0usize) += 1;
                }
            }
            let primary = sets.first().and_then(|s| s[0].topology.region.as_deref());
            left.sort_by_key(|member| {
                let r = member.topology.region.as_deref();
                let count = r.and_then(|r| counts.get(r)).copied().unwrap_or(0);
                let miss = if own_rules {
                    writer.is_some() && r != writer
                } else if policy.replica_regions == "same_region" {
                    r != primary
                } else {
                    r.is_none_or(|r| occupied.contains(r))
                };
                (
                    count < take,
                    count >= take && miss,
                    std::cmp::Reverse(count),
                    miss,
                    r,
                )
            });
        }
        let set: Vec<_> = left.drain(..take).collect();
        occupied.extend(
            set.iter()
                .filter_map(|member| member.topology.region.as_deref()),
        );
        sets.push(set);
    }
    // Reserve whole additional sets before assigning the original parity.
    for set in &mut sets {
        if set.len() == width {
            continue;
        }
        let home = set[0].topology.region.as_deref();
        let main: Regions = set
            .iter()
            .filter_map(|m| m.topology.region.as_deref())
            .collect();
        let mut parity = HashMap::new();
        while set.len() < width {
            let at = left
                .iter()
                .enumerate()
                .min_by_key(|(_, member)| {
                    let r = member.topology.region.as_deref();
                    match policy.parity_regions.as_str() {
                        "spread" => (
                            r.is_none(),
                            r.is_some_and(|r| main.contains(r)),
                            parity.get(&r).copied().unwrap_or(0),
                        ),
                        "data_region" => (r.is_none(), r != home, 0),
                        _ => (false, false, 0),
                    }
                })
                .unwrap()
                .0;
            let member = left.remove(at);
            *parity
                .entry(member.topology.region.as_deref())
                .or_insert(0usize) += 1;
            set.push(member);
        }
    }
    sets.into_iter().flatten().collect()
}

fn ratio(a: (u64, u32), b: (u64, u32)) -> Ordering {
    (u128::from(a.0) * u128::from(b.1)).cmp(&(u128::from(b.0) * u128::from(a.1)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::storage_cluster::{FORMAT, Group, Layout, Manifest, Topology};

    fn fixture() -> (Cluster, HashMap<Uuid, Status>) {
        let members: Vec<_> = (1..=4)
            .map(|n| Member {
                media_id: Uuid::from_u128(n),
                node_id: Uuid::from_u128(n + 10),
                state: "available".into(),
                topology: Topology {
                    url: "https://storage.test".into(),
                    region: Some(if n % 2 == 1 { "a" } else { "b" }.into()),
                    storage_group: format!("host-{n}"),
                    purpose: "primary".into(),
                },
            })
            .collect();
        let groups = members
            .chunks(2)
            .enumerate()
            .map(|(i, pair)| Group {
                replacing: None,
                id: Uuid::from_u128(i as u128 + 30),
                layout: Layout::default(),
                members: pair.iter().map(|m| m.media_id).collect(),
            })
            .collect();
        let status = members
            .iter()
            .map(|m| {
                (
                    m.media_id,
                    Status {
                        online: true,
                        draining: false,
                        free: 1000,
                        requests: Some((0, 2)),
                    },
                )
            })
            .collect();
        (
            Cluster {
                id: Uuid::from_u128(40),
                name: "test".into(),
                revision: 1,
                policy: Policy::default(),
                state: "ready".into(),
                manifest: Manifest {
                    format_version: FORMAT,
                    record_id: Uuid::from_u128(41),
                    cluster_id: Uuid::from_u128(40),
                    deployment_id: Uuid::from_u128(42),
                    publication_key: [1; 32],
                    takeover_epoch: 1,
                    revision: 1,
                    previous: None,
                    members,
                    groups,
                },
            },
            status,
        )
    }

    #[test]
    fn capacity_only_grows_when_the_bottleneck_grows() {
        let (mut cluster, mut status) = fixture();
        let member = cluster.manifest.groups[0].members[0];
        let other = cluster.manifest.groups[0].members[1];
        let stripe = u64::from(cluster.policy.layout.stripe_bytes);
        cluster.manifest.groups.truncate(1);
        for s in status.values_mut() {
            s.free = 5 * stripe;
        }
        let free = |s: &HashMap<_, _>| {
            space(&cluster.manifest, &cluster.policy.layout, s, 0).logical_bytes
        };
        assert_eq!(free(&status), 5 * stripe);
        status.get_mut(&member).unwrap().free *= 2;
        assert_eq!(free(&status), 5 * stripe);
        status.get_mut(&other).unwrap().free += stripe;
        assert_eq!(free(&status), 6 * stripe);
        status.get_mut(&other).unwrap().online = false;
        assert_eq!(free(&status), 0);
    }

    #[test]
    fn extra_sets_reserve_a_complete_region_before_primary_parity() {
        let (c, _) = fixture();
        let mut members = Vec::new();
        for (i, r) in ["b", "b", "a", "b", "c", "b", "a", "b"]
            .into_iter()
            .enumerate()
        {
            let mut member = c.manifest.members[0].clone();
            member.media_id = Uuid::from_u128(i as u128 + 1);
            member.topology.storage_group = format!("disk-{i}");
            member.topology.region = Some(r.into());
            members.push(member);
        }
        let mut p = c.policy;
        p.layout = Layout {
            data_shards: 2,
            parity_shards: 2,
            extra_copies: 1,
            ..Layout::default()
        };
        for region in [Some("a"), Some("unknown")] {
            let arranged = ordered_members(members.iter().collect(), &p, region);
            let first = &arranged[..2];
            assert_eq!(first[0].topology.region, first[1].topology.region);
            if region == Some("a") {
                assert_eq!(first[0].topology.region.as_deref(), Some("a"));
                let parity: std::collections::HashSet<_> = arranged[2..4]
                    .iter()
                    .map(|m| m.topology.region.as_deref())
                    .collect();
                assert_eq!(parity, [Some("b"), Some("c")].into_iter().collect());
                assert!(
                    arranged[4..]
                        .iter()
                        .all(|m| m.topology.region.as_deref() == Some("b"))
                );
            }
            assert_eq!(
                arranged
                    .iter()
                    .map(|m| &m.topology.storage_group)
                    .collect::<std::collections::HashSet<_>>()
                    .len(),
                8
            );
        }
        members[0].topology.region = Some("c".into());
        members[1].topology.region = Some("c".into());
        let arranged = ordered_members(members.iter().collect(), &p, Some("a"));
        assert_eq!(
            arranged
                .iter()
                .map(|m| m.media_id)
                .collect::<std::collections::HashSet<_>>()
                .len(),
            8
        );
    }

    #[test]
    fn roles_regions_and_degraded_acknowledgement_are_fixed_by_selection() -> Result<()> {
        let (mut cluster, mut status) = fixture();
        let object = Uuid::from_u128(100);
        let selected = Table::new(&cluster)?.select(object, Some("b"), 100, &status, None)?;
        assert_eq!(selected.members, [Uuid::from_u128(2), Uuid::from_u128(1)]);
        for n in 2..=4 {
            status.get_mut(&Uuid::from_u128(n)).unwrap().online = false;
        }
        assert!(
            Table::new(&cluster)?
                .select(object, None, 100, &status, None)
                .is_err()
        );
        cluster.policy.degraded_acknowledgement = Some(Acknowledgement::Data);
        let degraded = Table::new(&cluster)?.select(object, None, 100, &status, None)?;
        assert_eq!(degraded.acknowledgement, Acknowledgement::Data);
        assert_eq!(degraded.members[0], Uuid::from_u128(1));
        assert_eq!(selected.acknowledgement, Acknowledgement::All);
        status.get_mut(&Uuid::from_u128(1)).unwrap().free = 99;
        assert!(
            Table::new(&cluster)?
                .select(object, None, 100, &status, None)
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn preparing_member_blocks_new_objects_even_if_degraded_writes_are_allowed() -> Result<()> {
        let (mut cluster, mut status) = fixture();
        cluster.manifest.groups.truncate(1);
        cluster.policy.degraded_acknowledgement = Some(Acknowledgement::Data);
        let member = cluster.manifest.groups[0].members[1];
        status.get_mut(&member).unwrap().draining = true;
        let table = Table::new(&cluster)?;
        assert!(
            table
                .select(Uuid::now_v7(), None, 100, &status, None)
                .is_err()
        );
        assert_eq!(
            space(&cluster.manifest, &cluster.policy.layout, &status, 0).writable_groups,
            0
        );
        status.get_mut(&member).unwrap().draining = false;
        assert!(
            table
                .select(Uuid::now_v7(), None, 100, &status, None)
                .is_ok()
        );
        Ok(())
    }

    #[test]
    fn simple_skips_rankings_but_checks_admission_and_idle_uses_per_slot_load() -> Result<()> {
        let (mut cluster, mut status) = fixture();
        let object = Uuid::from_u128(100);
        cluster.policy.placement_order = "simple".into();
        status.get_mut(&Uuid::from_u128(1)).unwrap().requests = None;
        let simple = Table::new(&cluster)?.select(object, Some("b"), 100, &status, None)?;
        assert_eq!(simple.members, [Uuid::from_u128(1), Uuid::from_u128(2)]);
        cluster.policy.placement_order = "node_first".into();
        let idle = Table::new(&cluster)?.select(object, Some("b"), 100, &status, None)?;
        assert_eq!(idle.members, [Uuid::from_u128(4), Uuid::from_u128(3)]);
        status.get_mut(&Uuid::from_u128(1)).unwrap().requests = Some((2, 8));
        for n in 3..=4 {
            status.get_mut(&Uuid::from_u128(n)).unwrap().requests = Some((1, 2));
        }
        let normalized = Table::new(&cluster)?.select(object, None, 100, &status, None)?;
        assert_eq!(normalized.group, cluster.manifest.groups[0].id);
        Ok(())
    }

    #[test]
    fn capacity_fill_and_region_order_choose_different_eligible_groups() -> Result<()> {
        let (mut cluster, mut status) = fixture();
        let object = Uuid::from_u128(100);
        let first = cluster.manifest.groups[0].id;
        let second = cluster.manifest.groups[1].id;
        for member in &mut cluster.manifest.members[2..] {
            member.topology.region = Some("c".into());
            status.get_mut(&member.media_id).unwrap().free = 2000;
        }
        cluster.policy.node_priority = "free".into();
        cluster.policy.placement_order = "region_first".into();
        assert_eq!(
            Table::new(&cluster)?
                .select(object, Some("a"), 100, &status, None)?
                .group,
            first
        );
        cluster.policy.placement_order = "node_first".into();
        assert_eq!(
            Table::new(&cluster)?
                .select(object, Some("a"), 100, &status, None)?
                .group,
            second
        );
        cluster.policy.node_priority = "fill".into();
        assert_eq!(
            Table::new(&cluster)?
                .select(object, None, 100, &status, None)?
                .group,
            first
        );
        assert_eq!(
            Table::new(&cluster)?
                .select(object, None, 100, &status, Some(second))?
                .group,
            second
        );
        status.get_mut(&Uuid::from_u128(3)).unwrap().free = 99;
        assert_eq!(
            Table::new(&cluster)?
                .select(object, None, 100, &status, Some(second))?
                .group,
            first
        );
        cluster.policy.placement_order = "simple".into();
        status.get_mut(&Uuid::from_u128(3)).unwrap().free = 2000;
        status.get_mut(&Uuid::from_u128(2)).unwrap().free = 99;
        assert_eq!(
            Table::new(&cluster)?
                .select(object, None, 100, &status, Some(first))?
                .group,
            second
        );
        Ok(())
    }
}
