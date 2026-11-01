use super::queries::tier as q;
use crate::model::{
    metrics::AccessBatch,
    tiering::{Demand, Progress, Unit},
};
use anyhow::Result;
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

#[derive(sqlx::FromRow)]
pub(super) struct PlacementRow {
    pub(super) id: Uuid,
    pub(super) storage_id: Option<Uuid>,
    pub(super) current_bytes: i64,
    pub(super) phase: Option<String>,
    pub(super) source_storage: Option<Uuid>,
    pub(super) source_bytes: Option<i64>,
    pub(super) target_storage: Option<Uuid>,
    pub(super) target_bytes: Option<i64>,
    pub(super) checkpoint: Option<sqlx::types::Json<Progress>>,
}

pub(super) async fn usage(
    db: &mut q::Tx<'_>,
    bucket: Uuid,
    hot: Uuid,
    unit: Unit,
    after: Uuid,
    limit: u16,
) -> Result<Vec<crate::model::tiering::Usage>> {
    use crate::model::tiering::Usage;
    anyhow::ensure!((1..=256).contains(&limit), s3s::s3_error!(InvalidArgument));
    let rows = q::usage_rows(db, bucket, unit, after, limit).await?;
    rows.into_iter()
        .map(|row| {
            let current = row.storage_id.unwrap_or(hot);
            let (mut hot_bytes, mut cold_bytes) = if current == hot {
                (row.current_bytes, 0)
            } else {
                (0, row.current_bytes)
            };
            let mut duplicate_bytes = 0;
            let mut retiring_bytes = 0;
            let mut unconfirmed = false;
            if let Some(phase) = row.phase.as_deref() {
                let source = row.source_bytes.unwrap_or_default();
                let (storage, bytes) = if phase == "retiring" {
                    (row.source_storage, source)
                } else {
                    unconfirmed = row.target_bytes.is_none();
                    let confirmed = if unit == Unit::Raw {
                        let size = u64::try_from(source)?;
                        let parts = row.checkpoint.as_ref().map_or(0, |p| u64::from(p.parts));
                        i64::try_from(size.min(parts * Progress::raw_part_size(size)))?
                    } else {
                        0
                    };
                    (row.target_storage, row.target_bytes.unwrap_or(confirmed))
                };
                if storage == Some(hot) {
                    hot_bytes += bytes
                } else {
                    cold_bytes += bytes
                }
                duplicate_bytes = bytes;
                if matches!(phase, "retiring" | "discarding") {
                    retiring_bytes = bytes;
                }
            }
            Ok(Usage {
                id: row.id,
                unit,
                phase: row.phase,
                hot_bytes,
                cold_bytes,
                duplicate_bytes,
                retiring_bytes,
                unconfirmed,
            })
        })
        .collect()
}

type DemandKey = (Unit, Uuid, chrono::DateTime<chrono::Utc>);

async fn cached(
    db: &mut q::Connection,
    pending: &AccessBatch,
) -> Result<BTreeMap<DemandKey, Demand>> {
    let ids: BTreeSet<_> = pending
        .cached
        .keys()
        .flat_map(|(ids, _)| ids.iter().copied())
        .collect();
    let ids: Vec<_> = ids.into_iter().collect();
    let mut locations = BTreeMap::new();
    for page in ids.chunks(128) {
        for (id, encoding, pack) in q::cached_locations(db, page).await? {
            locations.insert(id, (encoding, pack));
        }
    }
    let mut result = BTreeMap::<DemandKey, Demand>::new();
    for ((ids, at), group) in &pending.cached {
        let mut visited = BTreeSet::new();
        for (id, bytes) in ids.iter().zip(&group.bytes) {
            let Some(&(encoding, pack)) = locations.get(id) else {
                continue;
            };
            let units = match (encoding, pack) {
                (Some(encoding), _) => [Some((Unit::Chunk, encoding)), None],
                (_, Some(pack)) => [Some((Unit::Pack, pack)), Some((Unit::Member, *id))],
                _ => [Some((Unit::Member, *id)), None],
            };
            for (unit, physical) in units.into_iter().flatten() {
                let d = result.entry((unit, physical, *at)).or_default();
                d.bytes += bytes;
                if visited.insert((unit, physical)) {
                    d.reads += group.reads;
                }
            }
        }
    }
    Ok(result)
}

pub(super) async fn demand(db: &mut q::Connection, pending: &AccessBatch) -> Result<()> {
    if pending.demand.is_empty() && pending.cached.is_empty() {
        return Ok(());
    }
    let mut requested = BTreeMap::<Unit, BTreeSet<Uuid>>::new();
    for &(unit, id, _) in pending.demand.keys() {
        requested.entry(unit).or_default().insert(id);
    }
    let mut known = BTreeSet::new();
    for (unit, ids) in requested {
        let ids: Vec<_> = ids.into_iter().collect();
        for page in ids.chunks(128) {
            known.extend(
                q::known_demand(db, unit, page)
                    .await?
                    .into_iter()
                    .map(|id| (unit, id)),
            );
        }
    }
    let mut resolved = cached(db, pending).await?;
    for (&(unit, id, at), d) in &pending.demand {
        if known.contains(&(unit, id)) {
            let entry = resolved.entry((unit, id, at)).or_default();
            entry.reads += d.reads;
            entry.bytes += d.bytes;
            entry.origins += d.origins;
        }
    }
    if resolved.is_empty() {
        return Ok(());
    }
    let now = sqlx::query_scalar::<_, i64>(q::CLOCK)
        .fetch_one(&mut *db)
        .await?;
    let mut totals = BTreeMap::<(Unit, Uuid), Demand>::new();
    let mut windows = Vec::new();
    for ((unit, id, at), d) in resolved {
        let count = totals.entry((unit, id)).or_default();
        count.reads += d.reads;
        count.bytes += d.bytes;
        count.origins += d.origins;
        windows.push((unit, id, at.timestamp(), d.reads, d.bytes, d.origins));
    }
    windows.sort_unstable();
    let totals: Vec<_> = totals
        .into_iter()
        .map(|((u, id), v)| (u, id, now, v.reads, v.bytes, v.origins))
        .collect();
    let pressure: Vec<_> = totals
        .iter()
        .filter(|r| r.5 > 0 && r.0 != Unit::Member)
        .collect();
    q::write_demand(db, now, &pressure, &totals, &windows).await?;
    Ok(())
}
