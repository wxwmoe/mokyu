use super::super::tier_access::PlacementRow;
use super::tier as q;
use crate::model::tiering::{Reference, Unit};
use anyhow::Result;
use sqlx::QueryBuilder;
use uuid::Uuid;

fn held(unit: Unit) -> &'static str {
    match unit {
        Unit::Chunk => {
            "EXISTS(SELECT 1 FROM node_read_sources ns JOIN node_read_windows w ON w.id=ns.window_id JOIN node_reads r ON r.read_id=w.read_id WHERE ns.source_type='chunk' AND (ns.location_id=u.location_id OR ns.location_id=(SELECT tl.address FROM tier_locations tl WHERE tl.unit=0 AND tl.physical_id=u.location_id)) AND r.expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)) OR EXISTS(SELECT 1 FROM pack_inputs pi WHERE pi.chunk_id=u.chunk_id) OR EXISTS(SELECT 1 FROM transfer_sources f JOIN transfer_runs tr ON tr.task_id=f.task_id WHERE f.source_encoding=u.encoding_id AND tr.phase NOT IN ('complete','cancelled'))"
        }
        Unit::Pack => {
            "EXISTS(SELECT 1 FROM node_read_sources ns JOIN node_read_windows w ON w.id=ns.window_id JOIN node_reads r ON r.read_id=w.read_id WHERE ns.source_type='pack' AND (ns.location_id=u.location_id OR ns.location_id=(SELECT tl.address FROM tier_locations tl WHERE tl.unit=1 AND tl.physical_id=u.location_id)) AND r.expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)) OR EXISTS(SELECT 1 FROM pack_members pm JOIN pack_inputs pi ON pi.chunk_id=pm.chunk_id WHERE pm.pack_id=u.id) OR EXISTS(SELECT 1 FROM transfer_sources f JOIN transfer_runs tr ON tr.task_id=f.task_id WHERE f.source_pack=u.id AND tr.phase NOT IN ('complete','cancelled'))"
        }
        Unit::Raw => {
            "EXISTS(SELECT 1 FROM node_read_sources ns JOIN node_read_windows w ON w.id=ns.window_id JOIN node_reads r ON r.read_id=w.read_id WHERE ns.source_type='raw' AND (ns.location_id=u.location_id OR ns.location_id=(SELECT tl.address FROM tier_locations tl WHERE tl.unit=2 AND tl.physical_id=u.location_id)) AND r.expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)) OR EXISTS(SELECT 1 FROM transfer_sources f JOIN transfer_runs tr ON tr.task_id=f.task_id WHERE f.source_raw=u.id AND tr.phase NOT IN ('complete','cancelled'))"
        }
        Unit::Member => unreachable!(),
    }
}

pub(in super::super) async fn pinned(db: &mut q::Tx<'_>, unit: Unit, id: Uuid) -> Result<bool> {
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

pub(in super::super) async fn references(
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
            "JOIN chunks c ON c.current_encoding_id=u.encoding_id AND c.pack_id IS NULL JOIN extents e ON e.chunk_id=c.id",
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

pub(in super::super) async fn usage_rows(
    db: &mut q::Tx<'_>,
    bucket: Uuid,
    unit: Unit,
    after: Uuid,
    limit: u16,
) -> Result<Vec<PlacementRow>> {
    let (table, column) = q::physical(unit);
    let (refs, size) = match unit {
        Unit::Chunk => (
            "JOIN chunks c ON c.current_encoding_id=u.encoding_id AND c.pack_id IS NULL JOIN extents e ON e.chunk_id=c.id",
            "u.standalone_size",
        ),
        Unit::Pack => (
            "JOIN pack_members p ON p.pack_id=u.id JOIN chunks c ON c.id=p.chunk_id AND c.pack_id=u.id JOIN extents e ON e.chunk_id=c.id",
            "u.stored_size",
        ),
        Unit::Raw => ("JOIN raw_ranges e ON e.source_id=u.id", "u.size"),
        Unit::Member => {
            return Err(
                s3s::s3_error!(InvalidArgument, "pack members move with their pack").into(),
            );
        }
    };
    let mut sql = QueryBuilder::<q::Db>::new(format!(
        "WITH chosen AS (SELECT u.{column} AS id FROM {table} u {refs} JOIN streams s ON s.id=e.stream_id JOIN objects o ON o.stream_id=s.id WHERE o.bucket_id="
    ));
    sql.push_bind(bucket).push(format!(" AND u.{column}>")).push_bind(after)
        .push(" UNION SELECT physical_id FROM tier_moves WHERE bucket_id=").push_bind(bucket)
        .push(" AND unit=").push_bind(unit as i16).push(" AND physical_id>").push_bind(after)
        .push(" ORDER BY id LIMIT ").push_bind(i64::from(limit))
        .push(format!(") SELECT i.id,h.storage_id,CAST(COALESCE(h.stored_bytes,{size},m.source_bytes,0) AS {}) AS current_bytes,m.phase,m.source_storage,m.source_bytes,m.target_storage,m.target_bytes,m.checkpoint FROM chosen i LEFT JOIN {table} u ON u.{column}=i.id LEFT JOIN tier_locations h ON h.physical_id=i.id AND h.unit=",q::BIG_INT)).push_bind(unit as i16)
        .push(" LEFT JOIN tier_moves m ON m.physical_id=i.id AND m.unit=").push_bind(unit as i16).push(" ORDER BY i.id");
    Ok(sql
        .build_query_as::<PlacementRow>()
        .fetch_all(&mut **db)
        .await?)
}

pub(in super::super) async fn identity(
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

pub(in super::super) async fn gc_identity(
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

pub(in super::super) async fn forget(db: &mut q::Connection, unit: Unit, id: Uuid) -> Result<()> {
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

pub(in super::super) async fn rewrite_source(
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
pub(in super::super) async fn cached_locations(
    db: &mut q::Connection,
    page: &[Uuid],
) -> Result<Vec<(Uuid, Option<Uuid>, Option<Uuid>)>> {
    let chunk = q::physical(Unit::Member).1;
    let encoding = q::physical(Unit::Chunk).1;
    let pack = q::physical(Unit::Pack).1;
    let mut query=QueryBuilder::<q::Db>::new(format!("SELECT c.{chunk},e.{encoding},p.{pack} FROM chunks c
        LEFT JOIN chunk_encodings e ON e.encoding_id=c.current_encoding_id AND e.state='ready' AND e.quarantine_code IS NULL
        LEFT JOIN packs p ON p.id=c.pack_id AND p.state='ready' AND p.quarantine_code IS NULL
        WHERE c.{chunk} IN ("));
    let mut list = query.separated(",");
    for id in page {
        list.push_bind(*id);
    }
    query.push(") AND EXISTS(SELECT 1 FROM bucket_tiering t WHERE t.hot_storage=c.storage_id)");
    Ok(query.build_query_as().fetch_all(&mut *db).await?)
}

pub(in super::super) async fn known_demand(
    db: &mut q::Connection,
    unit: Unit,
    page: &[Uuid],
) -> Result<Vec<Uuid>> {
    let (table, column) = q::physical(unit);
    let mut query = QueryBuilder::<q::Db>::new(format!(
        "SELECT p.{column} FROM {table} p WHERE p.{column} IN ("
    ));
    let mut list = query.separated(",");
    for id in page {
        list.push_bind(*id);
    }
    query.push(") AND EXISTS(SELECT 1 FROM bucket_tiering t WHERE t.hot_storage=p.storage_id)");
    Ok(query.build_query_scalar().fetch_all(&mut *db).await?)
}

type DemandRow = (Unit, Uuid, i64, i64, i64, i64);

pub(in super::super) async fn write_demand(
    db: &mut q::Connection,
    now: i64,
    pressure: &[&DemandRow],
    totals: &[DemandRow],
    windows: &[DemandRow],
) -> Result<()> {
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
            totals,
            "INSERT INTO physical_demand(unit,physical_id,last_read,read_count,served_bytes,origin_reads) ",
            q::TOTALS,
        ),
        (
            windows,
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
