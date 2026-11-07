use super::super::Backend;
pub(in super::super) const CHECKPOINT: &str =
    "UPDATE tier_moves SET checkpoint=$1 WHERE id=$2 AND phase='copying'";
pub(in super::super) const PARK: &str = "UPDATE tasks SET state='paused',error='tier_wait',lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=clock_timestamp() WHERE id=$1 AND state='running'";
pub(in super::super) const WAKE: &str = "UPDATE tasks SET state='queued',error=NULL,updated_at=clock_timestamp() WHERE id IN (SELECT m.id FROM tier_moves m WHERE m.bucket_id=$1 AND m.retire_after<=$2) AND state='paused' AND error='tier_wait'";
pub(in super::super) async fn protected(
    db: &mut Connection,
    address: uuid::Uuid,
) -> anyhow::Result<bool> {
    Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM journal_protected_addresses h WHERE h.address=$1) OR EXISTS(SELECT 1 FROM node_read_sources n JOIN node_read_windows w ON w.id=n.window_id JOIN stream_leases l ON l.id=w.read_id WHERE n.location_id=$1 AND l.expires_at>clock_timestamp()) OR EXISTS(SELECT 1 FROM node_work w WHERE w.result_hash IS NULL AND w.expires_at>clock_timestamp() AND jsonb_path_exists(w.plan,'$.** ? (@ == $v)',jsonb_build_object('v',$1::uuid::text)))").bind(address).fetch_one(&mut *db).await?)
}
pub(in super::super) async fn changed(
    db: &mut Connection,
    home: uuid::Uuid,
    dest: uuid::Uuid,
) -> anyhow::Result<()> {
    for storage in [home, dest] {
        sqlx::query("SELECT mokyu_notify_change('storage',$1)")
            .bind(storage)
            .execute(&mut *db)
            .await?;
    }
    Ok(())
}
pub(in super::super) const S3_PAIR: &str =
    "SELECT COUNT(*) FROM storages WHERE id IN ($1,$2) AND type='s3' AND enabled";
pub(in super::super) const DUE: &str = "SELECT t.bucket_id,t.hot_storage,t.cold_storage,t.revision,t.state,t.policy,s.unit,s.after_id FROM bucket_tiering t JOIN storages hot ON hot.id=t.hot_storage AND hot.type='s3' AND hot.enabled JOIN storages cold ON cold.id=t.cold_storage AND cold.type='s3' AND cold.enabled LEFT JOIN tier_scans s ON s.bucket_id=t.bucket_id AND s.revision=t.revision WHERE (t.state<>'paused' OR EXISTS(SELECT 1 FROM tier_moves m WHERE m.bucket_id=t.bucket_id)) AND COALESCE(s.next_scan,0)<=$1 ORDER BY COALESCE(s.next_scan,0),t.bucket_id LIMIT 1";
pub(in super::super) const CURSOR: &str = "INSERT INTO tier_scans(bucket_id,revision,unit,after_id,next_scan) VALUES($1,$2,$3,$4,$5) ON CONFLICT(bucket_id) DO UPDATE SET revision=excluded.revision,unit=excluded.unit,after_id=excluded.after_id,next_scan=excluded.next_scan";
pub(in super::super) const HEAD: &str = "SELECT generation,storage_id,address,activated_at,pressure_since,origin_reads FROM tier_locations WHERE unit=$1 AND physical_id=$2 FOR UPDATE";
pub(in super::super) const MOVEMENT: &str = "SELECT m.* FROM tier_moves m WHERE id=$1 FOR UPDATE";
pub(in super::super) const HAS_MOVEMENT: &str =
    "SELECT id FROM tier_moves WHERE unit=$1 AND physical_id=$2";
pub(in super::super) const RUNNING: &str =
    "SELECT COUNT(*) FROM tier_moves WHERE bucket_id=$1 AND phase IN ('copying','verified')";
pub(in super::super) const BACKLOG: &str = "SELECT COUNT(*) FROM tier_moves WHERE bucket_id=$1";
pub(in super::super) const PREPARE: &str = "INSERT INTO tier_moves(id,bucket_id,unit,physical_id,policy_revision,source_generation,source_storage,source_address,source_material,source_stamp,source_key,source_bytes,target_storage,target_address,target_material,target_key,phase,queued_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,'copying',$17)";
pub(in super::super) const TARGET: &str = "SELECT s.type,s.block_key_id,k.key_algorithm,p.compression_enabled,p.compression_strategy,p.revision FROM storages s JOIN storage_profiles p ON p.storage_id=s.id LEFT JOIN managed_keys k ON k.id=s.block_key_id AND k.revoked_at IS NULL WHERE s.id=$1 AND s.enabled";
pub(in super::super) const PROOF: &str = "UPDATE tier_moves SET target_material=$1,target_key=$2,target_bytes=$3,receipt=$4,phase='verified' WHERE id=$5 AND phase='copying'";
pub(in super::super) const INITIAL: &str = "INSERT INTO tier_locations(unit,physical_id,home_storage,storage_id,address,generation,material,block_key_id,stored_bytes,activated_at,pressure_seconds) VALUES($1,$2,$3,$4,$5,1,$6,$7,$8,$9,$10)";
pub(in super::super) const SWITCH: &str = "UPDATE tier_locations SET storage_id=$1,address=$2,generation=generation+1,material=$3,block_key_id=$4,stored_bytes=$5,activated_at=$6,pressure_seconds=$7,pressure_since=0,origin_reads=0 WHERE unit=$8 AND physical_id=$9 AND generation=$10 AND address=$11";
pub(in super::super) const RETIRE: &str =
    "UPDATE tier_moves SET phase=$1,retire_after=$2 WHERE id=$3";
pub(in super::super) const ERASE: &str =
    "DELETE FROM tier_moves WHERE id=$1 AND phase IN ('retiring','discarding')";
pub(in super::super) const MAXIMUM: &str = "GREATEST";
pub(in super::super) const BIG_INT: &str = "BIGINT";
pub(in super::super) const COVERAGE: &str = "SELECT floor(extract(epoch from clock_timestamp()))::bigint, COALESCE(floor(extract(epoch from access_coverage_since))::bigint,0), COALESCE(floor(extract(epoch from access_flushed_at))::bigint,0) FROM mokyu_meta WHERE singleton";
pub(in super::super) const CREATED: &str = "floor(extract(epoch from u.created_at))::bigint";
pub(in super::super) const OBSERVED: &str =
    "COALESCE(floor(extract(epoch from t.observed_since))::bigint,0)";
pub(in super::super) const MEMBER_ID: &str = "m.chunk_id";
pub(in super::super) const EXPIRE_TOTALS: &str = "DELETE FROM physical_demand WHERE (unit,physical_id) IN (SELECT unit,physical_id FROM physical_demand WHERE last_read<extract(epoch from now())-GREATEST($1::double precision,COALESCE((SELECT MAX((policy->>'cool_after_seconds')::bigint) FROM bucket_tiering),0)) ORDER BY last_read LIMIT $2)";
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
    sqlx::query_as("SELECT t.cold_domain,t.cold_space FROM redundancy_links l JOIN bucket_tiering t ON t.bucket_id=l.target_bucket WHERE l.source_bucket=$1").bind(bucket)
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
    sqlx::query_as("SELECT policy,revision,state FROM bucket_tiering WHERE bucket_id=$1")
        .bind(bucket)
}
pub(in super::super) fn family(
    bucket: uuid::Uuid,
) -> sqlx::query::QueryAs<'static, Db, (Vec<u8>, String), <Db as sqlx::Database>::Arguments> {
    sqlx::query_as("WITH family AS (SELECT source_bucket FROM redundancy_links WHERE target_bucket=$1 UNION SELECT source_bucket FROM redundancy_links WHERE source_bucket=$2), related AS (SELECT source_bucket AS id FROM family UNION SELECT l.target_bucket FROM redundancy_links l JOIN family f ON f.source_bucket=l.source_bucket WHERE l.target_bucket IS NOT NULL) SELECT l.source_domain AS domain,l.source_space AS space FROM redundancy_links l JOIN family f ON f.source_bucket=l.source_bucket UNION SELECT l.target_domain,l.target_space FROM redundancy_links l JOIN family f ON f.source_bucket=l.source_bucket UNION SELECT t.cold_domain,t.cold_space FROM bucket_tiering t JOIN related r ON r.id=t.bucket_id WHERE t.bucket_id<>$3").bind(bucket).bind(bucket).bind(bucket)
}
pub(in super::super) fn cold(
    bucket: uuid::Uuid,
) -> sqlx::query::QueryAs<'static, Db, (Vec<u8>, String), <Db as sqlx::Database>::Arguments> {
    sqlx::query_as("SELECT cold_domain,cold_space FROM bucket_tiering WHERE bucket_id=$1")
        .bind(bucket)
}
pub(in super::super) const INSERT_POLICY: &str = "INSERT INTO bucket_tiering(bucket_id,hot_storage,cold_storage,cold_domain,cold_space,policy,revision,state) VALUES($1,$2,$3,$4,$5,$6,$7,'active')";
pub(in super::super) const UPDATE_POLICY: &str = "UPDATE bucket_tiering SET cold_storage=$1,cold_domain=$2,cold_space=$3,policy=$4,revision=revision+1,observed_since=now() WHERE bucket_id=$5 AND revision=$6";
pub(in super::super) const EXPIRE: &str = "DELETE FROM physical_demand_windows WHERE (unit,physical_id,hour_start) IN (SELECT unit,physical_id,hour_start FROM physical_demand_windows WHERE hour_start<extract(epoch from now())-$1::double precision ORDER BY hour_start LIMIT $2)";
use crate::model::tiering::Unit;
use anyhow::Result;
use sqlx::{PgConnection, Postgres};
pub(in super::super) type Db = Postgres;
pub(in super::super) type Connection = PgConnection;
pub(in super::super) type Tx<'a> = sqlx::Transaction<'a, Db>;
pub(in super::super) const CLOCK: &str =
    "SELECT floor(extract(epoch from clock_timestamp()))::bigint";
pub(in super::super) const TOTALS: &str = " ON CONFLICT(unit,physical_id) DO UPDATE SET last_read=GREATEST(physical_demand.last_read,excluded.last_read),read_count=physical_demand.read_count+excluded.read_count,served_bytes=physical_demand.served_bytes+excluded.served_bytes,origin_reads=physical_demand.origin_reads+excluded.origin_reads";
pub(in super::super) const WINDOWS: &str = " ON CONFLICT(unit,physical_id,hour_start) DO UPDATE SET read_count=physical_demand_windows.read_count+excluded.read_count,served_bytes=physical_demand_windows.served_bytes+excluded.served_bytes,origin_reads=physical_demand_windows.origin_reads+excluded.origin_reads";
pub(in super::super) fn physical(unit: Unit) -> (&'static str, &'static str) {
    match unit {
        Unit::Chunk => ("chunk_encodings", "encoding_id"),
        Unit::Pack => ("packs", "id"),
        Unit::Raw => ("raw_sources", "id"),
        Unit::Member => ("chunks", "id"),
    }
}
pub(in super::super) async fn begin(backend: &Backend) -> Result<Tx<'_>> {
    Ok(backend.pool.begin().await?)
}

pub(in super::super) const SCAN_REVISION: &str =
    "SELECT revision FROM tier_scans WHERE bucket_id=$1";
pub(in super::super) const DELETE_POLICY: &str =
    "DELETE FROM bucket_tiering WHERE bucket_id=$1 AND revision=$2";
pub(in super::super) const SET_STATE: &str =
    "UPDATE bucket_tiering SET state=$1,revision=revision+1 WHERE bucket_id=$2 AND revision=$3";

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
        "SELECT floor(extract(epoch from u.stored_at))::bigint FROM {table} u WHERE u.{column}="
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
