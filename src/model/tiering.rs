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
