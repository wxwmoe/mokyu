use super::tier_queries as q;
use crate::model::{
    metrics::AccessBatch,
    tiering::{Demand, Reference, Unit},
};
use anyhow::Result;
use sqlx::QueryBuilder;
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

fn held(unit: Unit) -> &'static str {
    match unit {
        Unit::Chunk => {
            "EXISTS(SELECT 1 FROM pack_inputs pi WHERE pi.chunk_id=u.chunk_id) OR EXISTS(SELECT 1 FROM transfer_sources f JOIN transfer_runs tr ON tr.task_id=f.task_id WHERE f.source_encoding=u.encoding_id AND tr.phase NOT IN ('complete','cancelled'))"
        }
        Unit::Pack => {
            "EXISTS(SELECT 1 FROM pack_members pm JOIN pack_inputs pi ON pi.chunk_id=pm.chunk_id WHERE pm.pack_id=u.id) OR EXISTS(SELECT 1 FROM transfer_sources f JOIN transfer_runs tr ON tr.task_id=f.task_id WHERE f.source_pack=u.id AND tr.phase NOT IN ('complete','cancelled'))"
        }
        Unit::Raw => {
            "EXISTS(SELECT 1 FROM transfer_sources f JOIN transfer_runs tr ON tr.task_id=f.task_id WHERE f.source_raw=u.id AND tr.phase NOT IN ('complete','cancelled'))"
        }
        Unit::Member => unreachable!(),
    }
}

pub(super) async fn pinned(db: &mut q::Tx<'_>, unit: Unit, id: Uuid) -> Result<bool> {
    let (table, column) = q::physical(unit);
    let mut query = QueryBuilder::<q::Db>::new(format!(
        "SELECT EXISTS(SELECT 1 FROM {table} u WHERE ({}) AND u.{column}=",
        held(unit)
    ));
    Ok(query
        .push_bind(id)
        .push(")")
        .build_query_scalar()
        .fetch_one(&mut **db)
        .await?)
}

pub(super) async fn references(
    db: &mut q::Tx<'_>,
    bucket: Uuid,
    unit: Unit,
    after: Uuid,
    limit: u16,
    exact: bool,
) -> Result<(Vec<Uuid>, Vec<Reference>, (i64, i64, i64))> {
    let (table, column) = q::physical(unit);
    let (refs, size, extra) = match unit {
        Unit::Chunk => (
            "JOIN chunks c ON c.current_encoding_id=u.encoding_id JOIN extents e ON e.chunk_id=c.id",
            "u.standalone_size",
            " AND (u.owner_task IS NULL OR EXISTS(SELECT 1 FROM tasks t WHERE t.id=u.owner_task AND t.state='completed')) AND u.owner_stream IS NULL AND NOT u.recovery_pending AND NOT u.sync_pending",
        ),
        Unit::Pack => (
            "JOIN pack_members m ON m.pack_id=u.id JOIN chunks c ON c.id=m.chunk_id AND c.pack_id=u.id JOIN extents e ON e.chunk_id=c.id",
            "u.stored_size",
            " AND (u.owner_task IS NULL OR EXISTS(SELECT 1 FROM tasks t WHERE t.id=u.owner_task AND t.state='completed')) AND NOT EXISTS(SELECT 1 FROM pack_members pm JOIN chunks pc ON pc.id=pm.chunk_id WHERE pm.pack_id=u.id AND pc.pack_id=u.id AND pc.state<>'ready')",
        ),
        Unit::Raw => (
            "JOIN raw_ranges e ON e.source_id=u.id",
            "u.size",
            " AND NOT u.pending_upload AND u.processing_task IS NULL",
        ),
        Unit::Member => {
            return Err(
                s3s::s3_error!(InvalidArgument, "pack members move with their pack").into(),
            );
        }
    };
    let from = format!(" FROM {table} u {refs} JOIN streams s ON s.id=e.stream_id");
    let mut page = QueryBuilder::<q::Db>::new(format!(
        "SELECT DISTINCT u.{column}{from} JOIN objects o ON o.stream_id=s.id WHERE o.bucket_id="
    ));
    page.push_bind(bucket)
        .push(format!(" AND u.{column}{}", if exact { "=" } else { ">" }))
        .push_bind(after)
        .push(format!(" ORDER BY u.{column} LIMIT "))
        .push_bind(i64::from(limit));
    let ids = page
        .build_query_scalar::<Uuid>()
        .fetch_all(&mut **db)
        .await?;
    if ids.is_empty() {
        return Ok((ids, Vec::new(), (0, 0, 0)));
    }
    let heat = if unit == Unit::Pack {
        format!(
            "NULLIF({}(COALESCE(d.last_read,0),COALESCE((SELECT MAX(v.last_read) FROM pack_members m JOIN chunks c ON c.id=m.chunk_id JOIN physical_demand v ON v.unit=3 AND v.physical_id={} WHERE m.pack_id=u.id),0)),0)",
            q::MAXIMUM,
            q::MEMBER_ID
        )
    } else if unit == Unit::Chunk {
        format!(
            "NULLIF({}(COALESCE(d.last_read,0),COALESCE((SELECT v.last_read FROM physical_demand v WHERE v.unit=3 AND v.physical_id=c.{}),0)),0)",
            q::MAXIMUM,
            q::physical(Unit::Member).1
        )
    } else {
        "d.last_read".into()
    };
    let pins = held(unit);
    let mut query=QueryBuilder::<q::Db>::new(format!("SELECT DISTINCT u.{column} AS physical,s.bucket_id AS owner,
            (o.stream_id IS NOT NULL AND s.state='ready') AS visible,(u.state='ready' AND u.quarantine_code IS NULL{extra} AND NOT ({pins})) AS ready,
            CAST(COALESCE({size},0) AS {}) AS stored_bytes,{} AS created,{} AS observed,{heat} AS last_read,t.state,t.policy
            {from} LEFT JOIN objects o ON o.stream_id=s.id LEFT JOIN bucket_tiering t ON t.bucket_id=s.bucket_id
            LEFT JOIN physical_demand d ON d.unit={} AND d.physical_id=u.{column} WHERE u.{column} IN (",q::BIG_INT,q::CREATED,q::OBSERVED,unit as i16));
    let mut list = query.separated(",");
    for id in &ids {
        list.push_bind(*id);
    }
    query.push(") LIMIT 1025");
    let rows = query
        .build_query_as::<Reference>()
        .fetch_all(&mut **db)
        .await?;
    let clock = sqlx::query_as::<_, (i64, i64, i64)>(q::COVERAGE)
        .fetch_one(&mut **db)
        .await?;
    Ok((ids, rows, clock))
}

pub(super) async fn identity(
    db: &mut q::Tx<'_>,
    unit: Unit,
    id: Uuid,
    home: Uuid,
) -> Result<Option<crate::model::Id>> {
    let (table, column) = q::physical(unit);
    let key = if unit == Unit::Chunk {
        "encoding_id"
    } else {
        "id"
    };
    let mut query =
        QueryBuilder::<q::Db>::new(format!("SELECT {key} FROM {table} WHERE {column}="));
    Ok(query
        .push_bind(id)
        .push(" AND storage_id=")
        .push_bind(home)
        .build_query_scalar()
        .fetch_optional(&mut **db)
        .await?)
}

pub(super) async fn gc_identity(
    db: &mut q::Connection,
    unit: Unit,
    id: crate::model::Id,
) -> Result<Option<Uuid>> {
    let (table, column) = q::physical(unit);
    let key = if unit == Unit::Chunk {
        "encoding_id"
    } else {
        "id"
    };
    let mut query =
        QueryBuilder::<q::Db>::new(format!("SELECT {column} FROM {table} WHERE {key}="));
    Ok(query
        .push_bind(id)
        .build_query_scalar()
        .fetch_optional(&mut *db)
        .await?)
}

pub(super) async fn forget(db: &mut q::Connection, unit: Unit, id: Uuid) -> Result<()> {
    let mut query = QueryBuilder::<q::Db>::new("DELETE FROM tier_locations WHERE unit=");
    query
        .push_bind(unit as i16)
        .push(" AND physical_id=")
        .push_bind(id)
        .build()
        .execute(&mut *db)
        .await?;
    Ok(())
}

pub(super) async fn rewrite_source(
    db: &mut q::Connection,
    unit: Unit,
    id: crate::model::Id,
    address: Uuid,
    origin: Option<Uuid>,
    home: Option<Uuid>,
) -> Result<bool> {
    let (table, stable) = q::physical(unit);
    let key = if unit == Unit::Chunk {
        "encoding_id"
    } else {
        "id"
    };
    let mut query = QueryBuilder::<q::Db>::new(format!(
        "SELECT EXISTS(SELECT 1 FROM {table} u LEFT JOIN tier_locations h ON h.physical_id=u.{stable} AND h.unit="
    ));
    Ok(query
        .push_bind(unit as i16)
        .push(format!(" WHERE u.{key}="))
        .push_bind(id)
        .push(" AND u.storage_id=")
        .push_bind(home)
        .push(" AND COALESCE(h.storage_id,u.storage_id)=")
        .push_bind(origin)
        .push(format!(" AND COALESCE(h.address,u.{stable})="))
        .push_bind(address)
        .push(")")
        .build_query_scalar()
        .fetch_one(&mut *db)
        .await?)
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
    let chunk = q::physical(Unit::Member).1;
    let encoding = q::physical(Unit::Chunk).1;
    let pack = q::physical(Unit::Pack).1;
    let mut locations = BTreeMap::new();
    for page in ids.chunks(128) {
        let mut query=QueryBuilder::<q::Db>::new(format!("SELECT c.{chunk},e.{encoding},p.{pack} FROM chunks c
            LEFT JOIN chunk_encodings e ON e.encoding_id=c.current_encoding_id AND e.state='ready' AND e.quarantine_code IS NULL
            LEFT JOIN packs p ON p.id=c.pack_id AND p.state='ready' AND p.quarantine_code IS NULL
            WHERE c.{chunk} IN ("));
        let mut list = query.separated(",");
        for id in page {
            list.push_bind(*id);
        }
        query.push(") AND EXISTS(SELECT 1 FROM bucket_tiering t WHERE t.hot_storage=c.storage_id)");
        for (id, encoding, pack) in query
            .build_query_as::<(Uuid, Option<Uuid>, Option<Uuid>)>()
            .fetch_all(&mut *db)
            .await?
        {
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
        let (table, column) = q::physical(unit);
        let ids: Vec<_> = ids.into_iter().collect();
        for page in ids.chunks(128) {
            let mut query = QueryBuilder::<q::Db>::new(format!(
                "SELECT p.{column} FROM {table} p WHERE p.{column} IN ("
            ));
            let mut list = query.separated(",");
            for id in page {
                list.push_bind(*id);
            }
            query.push(
                ") AND EXISTS(SELECT 1 FROM bucket_tiering t WHERE t.hot_storage=p.storage_id)",
            );
            known.extend(
                query
                    .build_query_scalar::<Uuid>()
                    .fetch_all(&mut *db)
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
    for page in pressure.chunks(64) {
        let mut query = QueryBuilder::<q::Db>::new(
            "UPDATE tier_locations SET origin_reads=(CASE WHEN pressure_since+pressure_seconds>",
        );
        query
            .push_bind(now)
            .push(" THEN origin_reads ELSE 0 END)+CASE ");
        for &&(u, id, _, _, _, n) in page {
            query
                .push(" WHEN unit=")
                .push_bind(u as i16)
                .push(" AND physical_id=")
                .push_bind(id)
                .push(" THEN ")
                .push_bind(n);
        }
        query
            .push(" ELSE 0 END, pressure_since=CASE WHEN pressure_since+pressure_seconds>")
            .push_bind(now)
            .push(" THEN pressure_since ELSE ")
            .push_bind(now)
            .push(" END WHERE storage_id<>home_storage AND (unit,physical_id) IN (");
        for (i, &&(u, id, ..)) in page.iter().enumerate() {
            if i != 0 {
                query.push(",");
            }
            query
                .push("(")
                .push_bind(u as i16)
                .push(",")
                .push_bind(id)
                .push(")");
        }
        query.push(")").build().execute(&mut *db).await?;
    }
    for (rows, header, conflict) in [
        (
            &totals,
            "INSERT INTO physical_demand(unit,physical_id,last_read,read_count,served_bytes,origin_reads) ",
            q::TOTALS,
        ),
        (
            &windows,
            "INSERT INTO physical_demand_windows(unit,physical_id,hour_start,read_count,served_bytes,origin_reads) ",
            q::WINDOWS,
        ),
    ] {
        for page in rows.chunks(128) {
            let mut query = QueryBuilder::<q::Db>::new(header);
            query.push_values(page, |mut row, &(unit, id, at, reads, bytes, origins)| {
                row.push_bind(unit as i16)
                    .push_bind(id)
                    .push_bind(at)
                    .push_bind(reads)
                    .push_bind(bytes)
                    .push_bind(origins);
            });
            query.push(conflict).build().execute(&mut *db).await?;
        }
    }
    Ok(())
}
