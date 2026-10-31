use super::Backend;
pub(super) const CHECKPOINT: &str =
    "UPDATE tier_moves SET checkpoint=? WHERE id=? AND phase='copying'";
pub(super) const PARK: &str = "UPDATE tasks SET state='paused',error='tier_wait',execution_token=NULL,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND state='running'";
pub(super) const WAKE: &str = "UPDATE tasks SET state='queued',error=NULL,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id IN (SELECT m.id FROM tier_moves m WHERE m.bucket_id=? AND m.retire_after<=?) AND state='paused' AND error='tier_wait'";
pub(super) async fn protected(db: &mut Connection, address: uuid::Uuid) -> anyhow::Result<bool> {
    let _ = (db, address);
    Ok(false)
}
pub(super) async fn changed(
    db: &mut Connection,
    home: uuid::Uuid,
    dest: uuid::Uuid,
) -> anyhow::Result<()> {
    let _ = (db, home, dest);
    Ok(())
}
pub(super) const DUE: &str = "SELECT t.bucket_id,t.hot_storage,t.cold_storage,t.revision,t.state,t.policy,s.unit,s.after_id FROM bucket_tiering t LEFT JOIN tier_scans s ON s.bucket_id=t.bucket_id AND s.revision=t.revision WHERE (t.state<>'paused' OR EXISTS(SELECT 1 FROM tier_moves m WHERE m.bucket_id=t.bucket_id)) AND COALESCE(s.next_scan,0)<=? ORDER BY COALESCE(s.next_scan,0),t.bucket_id LIMIT 1";
pub(super) const CURSOR: &str = "INSERT INTO tier_scans(bucket_id,revision,unit,after_id,next_scan) VALUES(?,?,?,?,?) ON CONFLICT(bucket_id) DO UPDATE SET revision=excluded.revision,unit=excluded.unit,after_id=excluded.after_id,next_scan=excluded.next_scan";
pub(super) const HEAD: &str = "SELECT generation,storage_id,address,activated_at,pressure_since,origin_reads FROM tier_locations WHERE unit=? AND physical_id=?";
pub(super) const MOVEMENT: &str = "SELECT m.* FROM tier_moves m WHERE id=?";
pub(super) const HAS_MOVEMENT: &str = "SELECT id FROM tier_moves WHERE unit=? AND physical_id=?";
pub(super) const RUNNING: &str =
    "SELECT COUNT(*) FROM tier_moves WHERE bucket_id=? AND phase IN ('copying','verified')";
pub(super) const BACKLOG: &str = "SELECT COUNT(*) FROM tier_moves WHERE bucket_id=?";
pub(super) const PREPARE: &str = "INSERT INTO tier_moves(id,bucket_id,unit,physical_id,policy_revision,source_generation,source_storage,source_address,source_material,source_stamp,source_key,source_bytes,target_storage,target_address,target_material,target_key,phase,queued_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'copying',?)";
pub(super) const TARGET: &str = "SELECT s.type,s.block_key_id,k.key_algorithm,p.compression_enabled,p.compression_strategy,p.revision FROM storages s JOIN storage_profiles p ON p.storage_id=s.id LEFT JOIN managed_keys k ON k.id=s.block_key_id AND k.revoked_at IS NULL WHERE s.id=? AND s.enabled";
pub(super) const PROOF: &str = "UPDATE tier_moves SET target_material=?,target_key=?,target_bytes=?,receipt=?,phase='verified' WHERE id=? AND phase='copying'";
pub(super) const INITIAL: &str = "INSERT INTO tier_locations(unit,physical_id,home_storage,storage_id,address,generation,material,block_key_id,stored_bytes,activated_at,pressure_seconds) VALUES(?,?,?,?,?,1,?,?,?,?,?)";
pub(super) const SWITCH: &str = "UPDATE tier_locations SET storage_id=?,address=?,generation=generation+1,material=?,block_key_id=?,stored_bytes=?,activated_at=?,pressure_seconds=?,pressure_since=0,origin_reads=0 WHERE unit=? AND physical_id=? AND generation=? AND address=?";
pub(super) const RETIRE: &str = "UPDATE tier_moves SET phase=?,retire_after=? WHERE id=?";
pub(super) const ERASE: &str =
    "DELETE FROM tier_moves WHERE id=? AND phase IN ('retiring','discarding')";
pub(super) const MAXIMUM: &str = "MAX";
pub(super) const BIG_INT: &str = "BIGINT";
pub(super) const COVERAGE: &str = "SELECT unixepoch(), COALESCE(CAST(coverage_since/1000000 AS INTEGER),0), COALESCE(CAST(flushed_at/1000000 AS INTEGER),0) FROM access_state WHERE id=1";
pub(super) const CREATED: &str = "CAST(u.created_at/1000000 AS INTEGER)";
pub(super) const OBSERVED: &str = "COALESCE(unixepoch(t.observed_since),0)";
pub(super) const MEMBER_ID: &str = "c.uuid";
pub(super) const EXPIRE_TOTALS: &str = "DELETE FROM physical_demand WHERE rowid IN (SELECT rowid FROM physical_demand WHERE last_read<unixepoch()-MAX(?,COALESCE((SELECT MAX(CAST(json_extract(policy,'$.cool_after_seconds') AS INTEGER)) FROM bucket_tiering),0)) ORDER BY last_read LIMIT ?)";
pub(super) async fn active(backend: &Backend) -> anyhow::Result<bool> {
    Ok(
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM bucket_tiering)")
            .fetch_one(&backend.read)
            .await?,
    )
}
pub(super) fn siblings(
    bucket: uuid::Uuid,
) -> sqlx::query::QueryAs<'static, Db, (Vec<u8>, String), <Db as sqlx::Database>::Arguments> {
    sqlx::query_as("SELECT t.cold_domain,t.cold_space FROM redundancy_links l JOIN bucket_tiering t ON t.bucket_id=l.target_bucket WHERE l.source_bucket=?").bind(bucket)
}
pub(super) fn policy(
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
pub(super) fn family(
    bucket: uuid::Uuid,
) -> sqlx::query::QueryAs<'static, Db, (Vec<u8>, String), <Db as sqlx::Database>::Arguments> {
    sqlx::query_as("WITH family AS (SELECT source_bucket FROM redundancy_links WHERE target_bucket=? UNION SELECT source_bucket FROM redundancy_links WHERE source_bucket=?), related AS (SELECT source_bucket AS id FROM family UNION SELECT l.target_bucket FROM redundancy_links l JOIN family f ON f.source_bucket=l.source_bucket WHERE l.target_bucket IS NOT NULL) SELECT l.source_domain AS domain,l.source_space AS space FROM redundancy_links l JOIN family f ON f.source_bucket=l.source_bucket UNION SELECT l.target_domain,l.target_space FROM redundancy_links l JOIN family f ON f.source_bucket=l.source_bucket UNION SELECT t.cold_domain,t.cold_space FROM bucket_tiering t JOIN related r ON r.id=t.bucket_id WHERE t.bucket_id<>?").bind(bucket).bind(bucket).bind(bucket)
}
pub(super) fn cold(
    bucket: uuid::Uuid,
) -> sqlx::query::QueryAs<'static, Db, (Vec<u8>, String), <Db as sqlx::Database>::Arguments> {
    sqlx::query_as("SELECT cold_domain,cold_space FROM bucket_tiering WHERE bucket_id=?")
        .bind(bucket)
}
pub(super) const INSERT_POLICY: &str = "INSERT INTO bucket_tiering(bucket_id,hot_storage,cold_storage,cold_domain,cold_space,policy,revision,state) VALUES(?,?,?,?,?,?,?,'active')";
pub(super) const UPDATE_POLICY: &str = "UPDATE bucket_tiering SET cold_storage=?,cold_domain=?,cold_space=?,policy=?,revision=revision+1,observed_since=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE bucket_id=? AND revision=?";
pub(super) const EXPIRE: &str = "DELETE FROM physical_demand_windows WHERE rowid IN (SELECT rowid FROM physical_demand_windows WHERE hour_start<unixepoch()-? ORDER BY hour_start LIMIT ?)";
use crate::model::tiering::Unit;
use anyhow::Result;
use sqlx::{Sqlite, SqliteConnection};
pub(super) type Db = Sqlite;
pub(super) type Connection = SqliteConnection;
pub(super) type Tx<'a> = sqlx::Transaction<'a, Db>;
pub(super) const CLOCK: &str = "SELECT unixepoch()";
pub(super) const TOTALS: &str = " ON CONFLICT(unit,physical_id) DO UPDATE SET last_read=MAX(physical_demand.last_read,excluded.last_read),read_count=physical_demand.read_count+excluded.read_count,served_bytes=physical_demand.served_bytes+excluded.served_bytes,origin_reads=physical_demand.origin_reads+excluded.origin_reads";
pub(super) const WINDOWS: &str = " ON CONFLICT(unit,physical_id,hour_start) DO UPDATE SET read_count=physical_demand_windows.read_count+excluded.read_count,served_bytes=physical_demand_windows.served_bytes+excluded.served_bytes,origin_reads=physical_demand_windows.origin_reads+excluded.origin_reads";
pub(super) fn physical(unit: Unit) -> (&'static str, &'static str) {
    match unit {
        Unit::Chunk => ("chunk_encodings", "location_id"),
        Unit::Pack => ("packs", "location_id"),
        Unit::Raw => ("raw_sources", "location_id"),
        Unit::Member => ("chunks", "uuid"),
    }
}
pub(super) async fn begin(backend: &Backend) -> Result<Tx<'_>> {
    Ok(backend
        .write
        .begin_with(super::queries::BEGIN_WRITE)
        .await?)
}
