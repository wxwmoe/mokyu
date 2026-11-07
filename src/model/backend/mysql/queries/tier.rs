use super::super::Backend;
pub(in super::super) const CHECKPOINT: &str =
    "UPDATE tier_moves SET checkpoint=? WHERE id=? AND phase='copying'";
pub(in super::super) const PARK: &str = "UPDATE tasks SET state='paused',error='tier_wait',execution_token=NULL,updated_at=UTC_TIMESTAMP(6) WHERE id=? AND state='running'";
pub(in super::super) const WAKE: &str = "UPDATE tasks SET state='queued',error=NULL,updated_at=UTC_TIMESTAMP(6) WHERE id IN (SELECT m.id FROM tier_moves m WHERE m.bucket_id=? AND m.retire_after<=?) AND state='paused' AND error='tier_wait'";
pub(in super::super) async fn protected(
    db: &mut Connection,
    address: uuid::Uuid,
) -> anyhow::Result<bool> {
    Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM journal_protected_addresses h WHERE h.address=?) OR EXISTS(SELECT 1 FROM node_read_sources ns JOIN node_read_windows w ON w.id=ns.window_id JOIN node_reads nr ON nr.read_id=w.read_id WHERE ns.location_id=? AND nr.expires_at>UTC_TIMESTAMP(6))").bind(address).bind(address).fetch_one(db).await?)
}
pub(in super::super) async fn changed(
    db: &mut Connection,
    home: uuid::Uuid,
    dest: uuid::Uuid,
) -> anyhow::Result<()> {
    let _ = (db, home, dest);
    Ok(())
}
pub(in super::super) const S3_PAIR: &str =
    "SELECT COUNT(*) FROM storages WHERE id IN (?,?) AND type='s3' AND enabled";
pub(in super::super) const DUE: &str = "SELECT t.bucket_id,t.hot_storage,t.cold_storage,t.revision,t.state,t.policy,s.unit,s.after_id FROM bucket_tiering t JOIN storages hot ON hot.id=t.hot_storage AND hot.type='s3' AND hot.enabled JOIN storages cold ON cold.id=t.cold_storage AND cold.type='s3' AND cold.enabled LEFT JOIN tier_scans s ON s.bucket_id=t.bucket_id AND s.revision=t.revision WHERE (t.state<>'paused' OR EXISTS(SELECT 1 FROM tier_moves m WHERE m.bucket_id=t.bucket_id)) AND COALESCE(s.next_scan,0)<=? ORDER BY COALESCE(s.next_scan,0),t.bucket_id LIMIT 1";
pub(in super::super) const CURSOR: &str = "INSERT INTO tier_scans(bucket_id,revision,unit,after_id,next_scan) VALUES(?,?,?,?,?) ON DUPLICATE KEY UPDATE revision=VALUES(revision),unit=VALUES(unit),after_id=VALUES(after_id),next_scan=VALUES(next_scan)";
pub(in super::super) const HEAD: &str = "SELECT generation,storage_id,address,activated_at,pressure_since,origin_reads FROM tier_locations WHERE unit=? AND physical_id=? FOR UPDATE";
pub(in super::super) const MOVEMENT: &str = "SELECT m.* FROM tier_moves m WHERE id=? FOR UPDATE";
pub(in super::super) const HAS_MOVEMENT: &str =
    "SELECT id FROM tier_moves WHERE unit=? AND physical_id=?";
pub(in super::super) const RUNNING: &str =
    "SELECT COUNT(*) FROM tier_moves WHERE bucket_id=? AND phase IN ('copying','verified')";
pub(in super::super) const BACKLOG: &str = "SELECT COUNT(*) FROM tier_moves WHERE bucket_id=?";
pub(in super::super) const PREPARE: &str = "INSERT INTO tier_moves(id,bucket_id,unit,physical_id,policy_revision,source_generation,source_storage,source_address,source_material,source_stamp,source_key,source_bytes,target_storage,target_address,target_material,target_key,phase,queued_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'copying',?)";
pub(in super::super) const TARGET: &str = "SELECT s.type,s.block_key_id,k.key_algorithm,p.compression_enabled,p.compression_strategy,p.revision FROM storages s JOIN storage_profiles p ON p.storage_id=s.id LEFT JOIN managed_keys k ON k.id=s.block_key_id AND k.revoked_at IS NULL WHERE s.id=? AND s.enabled";
pub(in super::super) const PROOF: &str = "UPDATE tier_moves SET target_material=?,target_key=?,target_bytes=?,receipt=?,phase='verified' WHERE id=? AND phase='copying'";
pub(in super::super) const INITIAL: &str = "INSERT INTO tier_locations(unit,physical_id,home_storage,storage_id,address,generation,material,block_key_id,stored_bytes,activated_at,pressure_seconds) VALUES(?,?,?,?,?,1,?,?,?,?,?)";
pub(in super::super) const SWITCH: &str = "UPDATE tier_locations SET storage_id=?,address=?,generation=generation+1,material=?,block_key_id=?,stored_bytes=?,activated_at=?,pressure_seconds=?,pressure_since=0,origin_reads=0 WHERE unit=? AND physical_id=? AND generation=? AND address=?";
pub(in super::super) const RETIRE: &str = "UPDATE tier_moves SET phase=?,retire_after=? WHERE id=?";
pub(in super::super) const ERASE: &str =
    "DELETE FROM tier_moves WHERE id=? AND phase IN ('retiring','discarding')";
pub(in super::super) const MAXIMUM: &str = "GREATEST";
pub(in super::super) const BIG_INT: &str = "SIGNED";
pub(in super::super) const COVERAGE: &str = "SELECT CAST(UNIX_TIMESTAMP() AS SIGNED), COALESCE(CAST(UNIX_TIMESTAMP(coverage_since) AS SIGNED),0), COALESCE(CAST(UNIX_TIMESTAMP(flushed_at) AS SIGNED),0) FROM access_state WHERE id=1";
pub(in super::super) const CREATED: &str = "CAST(UNIX_TIMESTAMP(u.created_at) AS SIGNED)";
pub(in super::super) const OBSERVED: &str =
    "COALESCE(CAST(UNIX_TIMESTAMP(t.observed_since) AS SIGNED),0)";
pub(in super::super) const MEMBER_ID: &str = "c.uuid";
pub(in super::super) const EXPIRE_TOTALS: &str = "DELETE FROM physical_demand WHERE last_read<CAST(UNIX_TIMESTAMP() AS SIGNED)-GREATEST(?,COALESCE((SELECT MAX(CAST(JSON_UNQUOTE(JSON_EXTRACT(policy,'$.cool_after_seconds')) AS SIGNED)) FROM bucket_tiering),0)) ORDER BY last_read LIMIT ?";
pub(in super::super) async fn active(backend: &Backend) -> anyhow::Result<bool> {
    Ok(
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM bucket_tiering)")
            .fetch_one(&backend.pool)
            .await?,
    )
}
pub(in super::super) fn siblings(
    bucket: uuid::Uuid,
) -> sqlx::query::QueryAs<'static, Db, (Vec<u8>, String), <Db as sqlx::Database>::Arguments> {
    sqlx::query_as("SELECT t.cold_domain,t.cold_space FROM redundancy_links l JOIN bucket_tiering t ON t.bucket_id=l.target_bucket WHERE l.source_bucket=?").bind(bucket)
}
pub(in super::super) fn policy(
    bucket: uuid::Uuid,
) -> sqlx::query::QueryAs<
    'static,
    Db,
    (
        sqlx::types::Json<crate::model::tiering::Policy>,
        i64,
        String,
    ),
    <Db as sqlx::Database>::Arguments,
> {
    sqlx::query_as("SELECT policy,revision,state FROM bucket_tiering WHERE bucket_id=?")
        .bind(bucket)
}
pub(in super::super) fn family(
    bucket: uuid::Uuid,
) -> sqlx::query::QueryAs<'static, Db, (Vec<u8>, String), <Db as sqlx::Database>::Arguments> {
    sqlx::query_as("WITH family AS (SELECT source_bucket FROM redundancy_links WHERE target_bucket=? UNION SELECT source_bucket FROM redundancy_links WHERE source_bucket=?), related AS (SELECT source_bucket AS id FROM family UNION SELECT l.target_bucket FROM redundancy_links l JOIN family f ON f.source_bucket=l.source_bucket WHERE l.target_bucket IS NOT NULL) SELECT l.source_domain AS domain,l.source_space AS space FROM redundancy_links l JOIN family f ON f.source_bucket=l.source_bucket UNION SELECT l.target_domain,l.target_space FROM redundancy_links l JOIN family f ON f.source_bucket=l.source_bucket UNION SELECT t.cold_domain,t.cold_space FROM bucket_tiering t JOIN related r ON r.id=t.bucket_id WHERE t.bucket_id<>?").bind(bucket).bind(bucket).bind(bucket)
}
pub(in super::super) fn cold(
    bucket: uuid::Uuid,
) -> sqlx::query::QueryAs<'static, Db, (Vec<u8>, String), <Db as sqlx::Database>::Arguments> {
    sqlx::query_as("SELECT cold_domain,cold_space FROM bucket_tiering WHERE bucket_id=?")
        .bind(bucket)
}
pub(in super::super) const INSERT_POLICY: &str = "INSERT INTO bucket_tiering(bucket_id,hot_storage,cold_storage,cold_domain,cold_space,policy,revision,state) VALUES(?,?,?,?,?,?,?,'active')";
pub(in super::super) const UPDATE_POLICY: &str = "UPDATE bucket_tiering SET cold_storage=?,cold_domain=?,cold_space=?,policy=?,revision=revision+1,observed_since=CURRENT_TIMESTAMP(6) WHERE bucket_id=? AND revision=?";
pub(in super::super) const EXPIRE: &str = "DELETE FROM physical_demand_windows WHERE hour_start<CAST(UNIX_TIMESTAMP() AS SIGNED)-? ORDER BY hour_start LIMIT ?";
use crate::model::tiering::Unit;
use anyhow::Result;
use sqlx::{MySql, MySqlConnection};
pub(in super::super) type Db = MySql;
pub(in super::super) type Connection = MySqlConnection;
pub(in super::super) type Tx<'a> = sqlx::Transaction<'a, Db>;
pub(in super::super) const CLOCK: &str = "SELECT CAST(UNIX_TIMESTAMP() AS SIGNED)";
pub(in super::super) const TOTALS: &str = " ON DUPLICATE KEY UPDATE last_read=GREATEST(last_read,VALUES(last_read)),read_count=read_count+VALUES(read_count),served_bytes=served_bytes+VALUES(served_bytes),origin_reads=origin_reads+VALUES(origin_reads)";
pub(in super::super) const WINDOWS: &str = " ON DUPLICATE KEY UPDATE read_count=read_count+VALUES(read_count),served_bytes=served_bytes+VALUES(served_bytes),origin_reads=origin_reads+VALUES(origin_reads)";
pub(in super::super) fn physical(unit: Unit) -> (&'static str, &'static str) {
    match unit {
        Unit::Chunk => ("chunk_encodings", "location_id"),
        Unit::Pack => ("packs", "location_id"),
        Unit::Raw => ("raw_sources", "location_id"),
        Unit::Member => ("chunks", "uuid"),
    }
}
pub(in super::super) async fn begin(backend: &Backend) -> Result<Tx<'_>> {
    Ok(backend.pool.begin().await?)
}

pub(in super::super) const SCAN_REVISION: &str =
    "SELECT revision FROM tier_scans WHERE bucket_id=?";
pub(in super::super) const DELETE_POLICY: &str =
    "DELETE FROM bucket_tiering WHERE bucket_id=? AND revision=?";
pub(in super::super) const SET_STATE: &str =
    "UPDATE bucket_tiering SET state=?,revision=revision+1 WHERE bucket_id=? AND revision=?";

pub(in super::super) async fn cold_dependencies(
    db: &mut Tx<'_>,
    bucket: uuid::Uuid,
    home: uuid::Uuid,
    cold: uuid::Uuid,
) -> Result<bool> {
    let mut moves =
        sqlx::QueryBuilder::<Db>::new("SELECT EXISTS(SELECT 1 FROM tier_moves WHERE bucket_id=");
    if moves
        .push_bind(bucket)
        .push(")")
        .build_query_scalar::<bool>()
        .fetch_one(&mut **db)
        .await?
    {
        return Ok(true);
    }
    for (unit, refs) in [
        (
            Unit::Chunk,
            "JOIN chunks c ON c.current_encoding_id=u.encoding_id JOIN extents e ON e.chunk_id=c.id",
        ),
        (
            Unit::Pack,
            "JOIN pack_members m ON m.pack_id=u.id JOIN chunks c ON c.id=m.chunk_id AND c.pack_id=u.id JOIN extents e ON e.chunk_id=c.id",
        ),
        (Unit::Raw, "JOIN raw_ranges e ON e.source_id=u.id"),
    ] {
        let (table, column) = physical(unit);
        let mut query = sqlx::QueryBuilder::<Db>::new(format!(
            "SELECT EXISTS(SELECT 1 FROM tier_locations h JOIN {table} u ON u.{column}=h.physical_id {refs} JOIN objects o ON o.stream_id=e.stream_id WHERE h.unit="
        ));
        if query
            .push_bind(unit as i16)
            .push(" AND h.home_storage=")
            .push_bind(home)
            .push(" AND h.storage_id=")
            .push_bind(cold)
            .push(" AND o.bucket_id=")
            .push_bind(bucket)
            .push(")")
            .build_query_scalar::<bool>()
            .fetch_one(&mut **db)
            .await?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(in super::super) async fn stored_at(
    db: &mut Tx<'_>,
    unit: Unit,
    id: uuid::Uuid,
) -> Result<Option<i64>> {
    let (table, column) = physical(unit);
    let mut query = sqlx::QueryBuilder::<Db>::new(format!(
        "SELECT CAST(UNIX_TIMESTAMP(u.stored_at) AS SIGNED) FROM {table} u WHERE u.{column}="
    ));
    Ok(query
        .push_bind(id)
        .build_query_scalar()
        .fetch_one(&mut **db)
        .await?)
}

pub(in super::super) use super::tier_reads::{
    cached_locations, forget, gc_identity, identity, known_demand, pinned, references,
    rewrite_source, usage_rows, write_demand,
};
