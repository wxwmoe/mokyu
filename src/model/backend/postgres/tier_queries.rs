use super::Backend;
pub(super) const MAXIMUM: &str = "GREATEST";
pub(super) const BIG_INT: &str = "BIGINT";
pub(super) const COVERAGE: &str = "SELECT floor(extract(epoch from clock_timestamp()))::bigint, COALESCE(floor(extract(epoch from access_coverage_since))::bigint,0), COALESCE(floor(extract(epoch from access_flushed_at))::bigint,0) FROM mokyu_meta WHERE singleton";
pub(super) const CREATED: &str = "floor(extract(epoch from u.created_at))::bigint";
pub(super) const OBSERVED: &str = "COALESCE(floor(extract(epoch from t.observed_since))::bigint,0)";
pub(super) const MEMBER_ID: &str = "m.chunk_id";
pub(super) const EXPIRE_TOTALS: &str = "DELETE FROM physical_demand WHERE (unit,physical_id) IN (SELECT unit,physical_id FROM physical_demand WHERE last_read<extract(epoch from now())-GREATEST($1::double precision,COALESCE((SELECT MAX((policy->>'cool_after_seconds')::bigint) FROM bucket_tiering),0)) ORDER BY last_read LIMIT $2)";
pub(super) async fn active(backend: &Backend) -> anyhow::Result<bool> {
    Ok(
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM bucket_tiering)")
            .fetch_one(&backend.pool)
            .await?,
    )
}
pub(super) fn siblings(
    bucket: uuid::Uuid,
) -> sqlx::query::QueryAs<'static, Db, (Vec<u8>, String), <Db as sqlx::Database>::Arguments> {
    sqlx::query_as("SELECT t.cold_domain,t.cold_space FROM redundancy_links l JOIN bucket_tiering t ON t.bucket_id=l.target_bucket WHERE l.source_bucket=$1").bind(bucket)
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
    sqlx::query_as("SELECT policy,revision,state FROM bucket_tiering WHERE bucket_id=$1")
        .bind(bucket)
}
pub(super) fn family(
    bucket: uuid::Uuid,
) -> sqlx::query::QueryAs<'static, Db, (Vec<u8>, String), <Db as sqlx::Database>::Arguments> {
    sqlx::query_as("WITH family AS (SELECT source_bucket FROM redundancy_links WHERE target_bucket=$1 UNION SELECT source_bucket FROM redundancy_links WHERE source_bucket=$2), related AS (SELECT source_bucket AS id FROM family UNION SELECT l.target_bucket FROM redundancy_links l JOIN family f ON f.source_bucket=l.source_bucket WHERE l.target_bucket IS NOT NULL) SELECT l.source_domain AS domain,l.source_space AS space FROM redundancy_links l JOIN family f ON f.source_bucket=l.source_bucket UNION SELECT l.target_domain,l.target_space FROM redundancy_links l JOIN family f ON f.source_bucket=l.source_bucket UNION SELECT t.cold_domain,t.cold_space FROM bucket_tiering t JOIN related r ON r.id=t.bucket_id WHERE t.bucket_id<>$3").bind(bucket).bind(bucket).bind(bucket)
}
pub(super) fn cold(
    bucket: uuid::Uuid,
) -> sqlx::query::QueryAs<'static, Db, (Vec<u8>, String), <Db as sqlx::Database>::Arguments> {
    sqlx::query_as("SELECT cold_domain,cold_space FROM bucket_tiering WHERE bucket_id=$1")
        .bind(bucket)
}
pub(super) const INSERT_POLICY: &str = "INSERT INTO bucket_tiering(bucket_id,hot_storage,cold_storage,cold_domain,cold_space,policy,revision,state) VALUES($1,$2,$3,$4,$5,$6,1,'active')";
pub(super) const UPDATE_POLICY: &str = "UPDATE bucket_tiering SET cold_storage=$1,cold_domain=$2,cold_space=$3,policy=$4,revision=revision+1,observed_since=now() WHERE bucket_id=$5 AND revision=$6";
pub(super) const EXPIRE: &str = "DELETE FROM physical_demand_windows WHERE (unit,physical_id,hour_start) IN (SELECT unit,physical_id,hour_start FROM physical_demand_windows WHERE hour_start<extract(epoch from now())-$1::double precision ORDER BY hour_start LIMIT $2)";
use crate::model::tiering::Unit;
use anyhow::Result;
use sqlx::{PgConnection, Postgres};
pub(super) type Db = Postgres;
pub(super) type Connection = PgConnection;
pub(super) type Tx<'a> = sqlx::Transaction<'a, Db>;
pub(super) const CLOCK: &str = "SELECT floor(extract(epoch from clock_timestamp()))::bigint";
pub(super) const TOTALS: &str = " ON CONFLICT(unit,physical_id) DO UPDATE SET last_read=GREATEST(physical_demand.last_read,excluded.last_read),read_count=physical_demand.read_count+excluded.read_count,served_bytes=physical_demand.served_bytes+excluded.served_bytes,origin_reads=physical_demand.origin_reads+excluded.origin_reads";
pub(super) const WINDOWS: &str = " ON CONFLICT(unit,physical_id,hour_start) DO UPDATE SET read_count=physical_demand_windows.read_count+excluded.read_count,served_bytes=physical_demand_windows.served_bytes+excluded.served_bytes,origin_reads=physical_demand_windows.origin_reads+excluded.origin_reads";
pub(super) fn physical(unit: Unit) -> (&'static str, &'static str) {
    match unit {
        Unit::Chunk => ("chunk_encodings", "encoding_id"),
        Unit::Pack => ("packs", "id"),
        Unit::Raw => ("raw_sources", "id"),
        Unit::Member => ("chunks", "id"),
    }
}
pub(super) async fn begin(backend: &Backend) -> Result<Tx<'_>> {
    Ok(backend.pool.begin().await?)
}
