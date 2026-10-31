use crate::authorization::Principal;
use anyhow::{Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(
    Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize, ToSchema,
)]
#[serde(rename_all = "snake_case")]
#[schema(as=TierUnit)]
#[repr(i16)]
pub(crate) enum Unit {
    Chunk,
    Pack,
    Raw,
    Member,
}

impl Unit {
    pub(crate) fn movable(value: i16) -> Result<Self> {
        match value {
            0 => Ok(Self::Chunk),
            1 => Ok(Self::Pack),
            2 => Ok(Self::Raw),
            _ => anyhow::bail!("invalid tier unit"),
        }
    }
    pub(crate) fn source(self) -> Result<super::source::SourceType> {
        use super::source::SourceType;
        match self {
            Self::Chunk => Ok(SourceType::Chunk),
            Self::Pack => Ok(SourceType::Pack),
            Self::Raw => Ok(SourceType::Raw),
            Self::Member => anyhow::bail!("pack members cannot migrate separately"),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Work {
    Copy {
        source: super::PhysicalSource,
        target: super::PhysicalSource,
        rate: u64,
        recode: bool,
        progress: Progress,
    },
    Remove {
        source: super::PhysicalSource,
    },
}

impl Work {
    pub(crate) fn source(&self) -> &super::PhysicalSource {
        match self {
            Self::Copy { source, .. } | Self::Remove { source } => source,
        }
    }
    pub(crate) fn target(&self) -> Option<&super::PhysicalSource> {
        match self {
            Self::Copy { target, .. } => Some(target),
            Self::Remove { .. } => None,
        }
    }
    pub(crate) fn memory(&self) -> Result<usize> {
        if matches!(self, Self::Remove { .. }) {
            return Ok(0);
        }
        Ok(match self.source() {
            super::PhysicalSource::Chunk(c) => usize::try_from(c.raw_size)?,
            super::PhysicalSource::Pack(p) => p.payload_size()?,
            super::PhysicalSource::Raw(_) => 8 * 1024 * 1024,
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Outcome {
    Retry,
    Discard,
    Checkpoint {
        progress: Progress,
    },
    Verified {
        target: super::PhysicalSource,
        digest: [u8; 32],
    },
    Removed,
}

impl std::fmt::Debug for Outcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Retry => f.write_str("Retry"),
            Self::Discard => f.write_str("Discard"),
            Self::Verified { target, .. } => {
                f.debug_tuple("Verified").field(&target.address()).finish()
            }
            Self::Removed => f.write_str("Removed"),
            Self::Checkpoint { progress } => {
                f.debug_tuple("Checkpoint").field(&progress.parts).finish()
            }
        }
    }
}
#[cfg(feature = "fault-injection")]
impl Outcome {
    pub(crate) async fn boundary(&self) {
        let name = match self {
            Self::Checkpoint { progress } if progress.parts > 0 => "tier-part-written",
            Self::Verified { .. } => "tier-copy-verified",
            Self::Removed => "tier-file-removed",
            _ => return,
        };
        crate::faults::point(name).await;
    }
}
#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct Progress {
    pub upload: Option<String>,
    pub parts: u16,
}

pub(crate) struct Step {
    pub work: Option<Work>,
    pub switched: bool,
}

#[derive(sqlx::FromRow)]
pub(crate) struct Movement {
    pub bucket_id: Uuid,
    pub unit: i16,
    pub physical_id: Uuid,
    pub policy_revision: i64,
    pub source_generation: i64,
    pub source_material: sqlx::types::Json<super::PhysicalSource>,
    pub source_stamp: Vec<u8>,
    pub target_storage: Uuid,
    pub target_address: Uuid,
    pub target_material: Option<sqlx::types::Json<super::PhysicalSource>>,
    pub checkpoint: Option<sqlx::types::Json<Progress>>,
    pub receipt: Option<Vec<u8>>,
    pub phase: String,
    pub retire_after: i64,
}

#[derive(sqlx::FromRow)]
pub(crate) struct Head {
    pub generation: i64,
    pub storage_id: Uuid,
    pub address: Uuid,
    pub activated_at: i64,
    pub pressure_since: i64,
    pub origin_reads: i64,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub(crate) enum Source {
    Chunk(Uuid),
    Pack { pack: Uuid, member: Uuid },
    Raw(Uuid),
}

#[derive(Clone, Copy)]
pub(crate) enum CachedSource {
    Chunk(Uuid),
    Raw(Uuid),
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CachedDemand {
    pub reads: i64,
    pub bytes: Vec<i64>,
}
pub(crate) type CachedKey = (Vec<Uuid>, chrono::DateTime<chrono::Utc>);

#[derive(Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Demand {
    pub reads: i64,
    pub bytes: i64,
    pub origins: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = TieringPolicy)]
pub(crate) struct Policy {
    pub cold_storage: Uuid,
    pub cool_after_seconds: u32,
    pub observation_seconds: u32,
    pub promote_reads: u32,
    pub residency_seconds: u32,
    pub parallelism: u16,
    pub bytes_per_second: u64,
}

impl Policy {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.cold_storage.is_nil()
                && self.cool_after_seconds > 0
                && self.observation_seconds > 0
                && self.observation_seconds <= self.cool_after_seconds
                && self.promote_reads >= 2
                && self.residency_seconds > 0
                && (1..=32).contains(&self.parallelism)
                && (1..=1u64 << 40).contains(&self.bytes_per_second),
            s3s::s3_error!(InvalidArgument, "invalid tiering policy")
        );
        Ok(())
    }
}

#[derive(Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = TieringUpdate)]
pub(crate) struct Update {
    pub revision: i64,
    pub policy: Policy,
}

#[derive(Clone, Copy, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(as=TieringAction)]
pub(crate) enum Transition {
    Pause,
    Resume,
    Drain,
    Disable,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as=TieringTransition)]
pub(crate) struct Change {
    pub revision: i64,
    pub action: Transition,
}

#[derive(Serialize, ToSchema)]
#[schema(as = BucketTiering)]
pub(crate) struct View {
    pub bucket: Uuid,
    pub hot_storage: Uuid,
    pub revision: i64,
    pub state: String,
    pub policy: Option<Policy>,
}

#[derive(Serialize, ToSchema)]
#[schema(as=TieringUnit)]
pub(crate) struct Candidate {
    pub unit: Unit,
    pub id: Uuid,
    pub bytes: i64,
    pub last_read: Option<i64>,
    pub reason: String,
}

#[derive(sqlx::FromRow)]
pub(crate) struct Reference {
    pub physical: Uuid,
    pub owner: Uuid,
    pub visible: bool,
    pub ready: bool,
    pub stored_bytes: i64,
    pub created: i64,
    pub observed: i64,
    pub last_read: Option<i64>,
    pub state: Option<String>,
    pub policy: Option<sqlx::types::Json<Policy>>,
}

pub(crate) fn candidate(
    unit: Unit,
    id: Uuid,
    rows: &[&Reference],
    clock: (i64, i64, i64),
    stale: i64,
    limited: bool,
) -> Candidate {
    let (now, since, flushed) = clock;
    let mut reason = "eligible";
    let mut cold = None;
    let mut busy = false;
    let mut incompatible = false;
    let mut last_read = None;
    let mut age = 0;
    let mut observed = since;
    let mut duration = 0;
    let mut dwell = 0;
    for row in rows {
        last_read = last_read.max(row.last_read);
        age = age.max(row.created);
        observed = observed.max(row.observed);
        busy |= !row.ready || !row.visible;
        let Some(policy) = &row.policy else {
            incompatible = true;
            continue;
        };
        incompatible |= row.state.as_deref() != Some("active")
            || cold.is_some_and(|id| id != policy.cold_storage);
        cold = Some(policy.cold_storage);
        duration = duration.max(i64::from(policy.cool_after_seconds));
        dwell = dwell.max(i64::from(policy.residency_seconds));
    }
    if limited || rows.is_empty() {
        reason = "references_unconfirmed";
    } else if busy {
        reason = "source_busy";
    } else if incompatible {
        reason = "shared_policy";
    } else {
        if since <= 0 || now - flushed > stale || flushed > now || now - observed < duration {
            reason = "observation_gap";
        } else if now - age < dwell.max(duration) {
            reason = "minimum_residency";
        } else if last_read.is_some_and(|at| now - at < duration) {
            reason = "recent_read";
        }
    }
    Candidate {
        unit,
        id,
        bytes: rows.first().map_or(0, |r| r.stored_bytes),
        last_read,
        reason: reason.into(),
    }
}

#[async_trait]
pub(crate) trait TieringStore: Send + Sync {
    async fn active(&self) -> Result<bool>;
    async fn schedule(&self, stale: i64) -> Result<usize>;
    async fn step(
        &self,
        task: super::task::Execution,
        stale: i64,
        busy: Option<Uuid>,
    ) -> Result<Step>;
    async fn finish(&self, task: super::task::Execution, outcome: &Outcome) -> Result<()>;
    async fn candidates(
        &self,
        principal: &Principal,
        bucket: Uuid,
        unit: Unit,
        after: Uuid,
        limit: u16,
        stale: i64,
    ) -> Result<Vec<Candidate>>;
    async fn tiering(&self, principal: &Principal, bucket: Uuid) -> Result<View>;
    async fn transition(&self, principal: &Principal, bucket: Uuid, input: &Change)
    -> Result<View>;
    async fn locations(&self, principal: &Principal, bucket: Uuid, cold: Uuid)
    -> Result<Locations>;
    async fn update(
        &self,
        principal: &Principal,
        bucket: Uuid,
        input: &Update,
        checked: &Locations,
        placement: &super::redundancy::Placement,
        preview: bool,
    ) -> Result<View>;
}

#[derive(PartialEq, Eq)]
pub(crate) struct Locations {
    pub hot: (Uuid, i64, super::redundancy::Location),
    pub cold: (Uuid, i64, super::redundancy::Location),
}

#[cfg(test)]
mod tests {
    use super::*;
    fn shared(cold: Uuid) -> Reference {
        Reference {
            physical: Uuid::from_u128(9),
            owner: Uuid::from_u128(3),
            visible: true,
            ready: true,
            stored_bytes: 80,
            created: 10,
            observed: 10,
            last_read: None,
            state: Some("active".into()),
            policy: Some(sqlx::types::Json(Policy {
                cold_storage: cold,
                cool_after_seconds: 20,
                observation_seconds: 5,
                promote_reads: 2,
                residency_seconds: 12,
                parallelism: 1,
                bytes_per_second: 100,
            })),
        }
    }
    #[test]
    fn every_owner_and_observation_window_must_agree() {
        let cold = Uuid::from_u128(5);
        let first = shared(cold);
        let mut second = shared(cold);
        let reason = |a: &Reference, b: &Reference, clock, limited| {
            candidate(Unit::Pack, a.physical, &[a, b], clock, 5, limited).reason
        };
        assert_eq!(reason(&first, &second, (60, 10, 59), false), "eligible");
        for clock in [(60, 52, 59), (60, 10, 40), (60, 10, 61)] {
            assert_eq!(reason(&first, &second, clock, false), "observation_gap");
        }
        second.last_read = Some(55);
        assert_eq!(reason(&first, &second, (60, 10, 59), false), "recent_read");
        second.policy.as_mut().unwrap().0.cold_storage = Uuid::from_u128(6);
        assert_eq!(
            reason(&second, &first, (60, 10, 59), false),
            "shared_policy"
        );
        second.policy = None;
        assert_eq!(
            reason(&first, &second, (60, 10, 59), false),
            "shared_policy"
        );
        second.visible = false;
        assert_eq!(reason(&first, &second, (60, 10, 59), false), "source_busy");
        assert_eq!(
            reason(&first, &second, (60, 10, 59), true),
            "references_unconfirmed"
        );
    }
}
