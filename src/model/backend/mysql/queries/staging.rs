use super::{Rows, Scalar, Statement};
use uuid::Uuid;
pub(in super::super) fn size<'a>(storage: Uuid, key: &'a str) -> Scalar<'a, i64> {
    sqlx::query_scalar("SELECT size FROM staging_objects WHERE storage_id=? AND object_key=?")
        .bind(storage)
        .bind(key)
}
pub(in super::super) fn used(storage: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar(
        "SELECT CAST(COALESCE(SUM(size),0) AS SIGNED) FROM staging_objects WHERE storage_id=?",
    )
    .bind(storage)
}
pub(in super::super) fn reserve<'a>(storage: Uuid, key: &'a str, bytes: i64) -> Statement<'a> {
    sqlx::query("INSERT INTO staging_objects(storage_id,object_key,size) VALUES(?,?,?)")
        .bind(storage)
        .bind(key)
        .bind(bytes)
}
pub(in super::super) fn release<'a>(storage: Uuid, key: &'a str) -> Statement<'a> {
    sqlx::query("DELETE FROM staging_objects WHERE storage_id=? AND object_key=?")
        .bind(storage)
        .bind(key)
}
pub(in super::super) fn old(storage: Option<Uuid>) -> Rows<'static, (Uuid, String)> {
    sqlx::query_as("SELECT s.storage_id,s.object_key FROM staging_objects s WHERE (? IS NULL OR s.storage_id=?) AND s.created_at<UTC_TIMESTAMP(6)-INTERVAL 5 MINUTE AND NOT EXISTS(SELECT 1 FROM fragments f WHERE f.staging_storage_id=s.storage_id AND f.staging_object_key=s.object_key) AND NOT EXISTS(SELECT 1 FROM node_write_tails t JOIN node_writes w ON w.node_id=t.node_id AND w.operation_id=t.operation_id WHERE t.storage_id=s.storage_id AND t.object_key=s.object_key AND w.phase IN ('preparing','active') AND w.expires_at>UTC_TIMESTAMP(6)) ORDER BY s.created_at,s.object_key LIMIT 128").bind(storage).bind(storage)
}
