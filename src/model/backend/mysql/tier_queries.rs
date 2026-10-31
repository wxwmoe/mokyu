use super::Backend;
pub(super) const MAXIMUM: &str = "GREATEST";
pub(super) const BIG_INT: &str = "SIGNED";
pub(super) const COVERAGE: &str = "SELECT CAST(UNIX_TIMESTAMP() AS SIGNED), COALESCE(CAST(UNIX_TIMESTAMP(coverage_since) AS SIGNED),0), COALESCE(CAST(UNIX_TIMESTAMP(flushed_at) AS SIGNED),0) FROM access_state WHERE id=1";
pub(super) const CREATED: &str = "CAST(UNIX_TIMESTAMP(u.created_at) AS SIGNED)";
pub(super) const OBSERVED: &str = "COALESCE(CAST(UNIX_TIMESTAMP(t.observed_since) AS SIGNED),0)";
pub(super) const MEMBER_ID: &str = "c.uuid";
pub(super) const EXPIRE_TOTALS: &str = "DELETE FROM physical_demand WHERE last_read<CAST(UNIX_TIMESTAMP() AS SIGNED)-GREATEST(?,COALESCE((SELECT MAX(CAST(JSON_UNQUOTE(JSON_EXTRACT(policy,'$.cool_after_seconds')) AS SIGNED)) FROM bucket_tiering),0)) ORDER BY last_read LIMIT ?";
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
pub(super) const INSERT_POLICY: &str = "INSERT INTO bucket_tiering(bucket_id,hot_storage,cold_storage,cold_domain,cold_space,policy,revision,state) VALUES(?,?,?,?,?,?,1,'active')";
pub(super) const UPDATE_POLICY: &str = "UPDATE bucket_tiering SET cold_storage=?,cold_domain=?,cold_space=?,policy=?,revision=revision+1,observed_since=CURRENT_TIMESTAMP(6) WHERE bucket_id=? AND revision=?";
pub(super) const EXPIRE: &str = "DELETE FROM physical_demand_windows WHERE hour_start<CAST(UNIX_TIMESTAMP() AS SIGNED)-? ORDER BY hour_start LIMIT ?";
use crate::model::tiering::Unit;
use anyhow::Result;
use sqlx::{MySql, MySqlConnection};
pub(super) type Db = MySql;
pub(super) type Connection = MySqlConnection;
pub(super) type Tx<'a> = sqlx::Transaction<'a, Db>;
pub(super) const CLOCK: &str = "SELECT CAST(UNIX_TIMESTAMP() AS SIGNED)";
pub(super) const TOTALS: &str = " ON DUPLICATE KEY UPDATE last_read=GREATEST(last_read,VALUES(last_read)),read_count=read_count+VALUES(read_count),served_bytes=served_bytes+VALUES(served_bytes),origin_reads=origin_reads+VALUES(origin_reads)";
pub(super) const WINDOWS: &str = " ON DUPLICATE KEY UPDATE read_count=read_count+VALUES(read_count),served_bytes=served_bytes+VALUES(served_bytes),origin_reads=origin_reads+VALUES(origin_reads)";
pub(super) fn physical(unit: Unit) -> (&'static str, &'static str) {
    match unit {
        Unit::Chunk => ("chunk_encodings", "location_id"),
        Unit::Pack => ("packs", "location_id"),
        Unit::Raw => ("raw_sources", "location_id"),
        Unit::Member => ("chunks", "uuid"),
    }
}
pub(super) async fn begin(backend: &Backend) -> Result<Tx<'_>> {
    Ok(backend.pool.begin().await?)
}
