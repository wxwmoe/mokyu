use super::{Rows, Scalar, Statement};
use uuid::Uuid;
pub(in super::super) fn size<'a>(storage: Uuid, key: &'a str) -> Scalar<'a, i64> {
    sqlx::query_scalar("SELECT size FROM staging_objects WHERE storage_id=? AND object_key=?")
        .bind(storage)
        .bind(key)
}
pub(in super::super) fn used(storage: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT COALESCE(SUM(size),0) FROM staging_objects WHERE storage_id=?")
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
    sqlx::query_as("SELECT s.storage_id,s.object_key FROM staging_objects s WHERE (? IS NULL OR s.storage_id=?) AND s.created_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-300000000 AND NOT EXISTS(SELECT 1 FROM fragments f WHERE f.staging_storage_id=s.storage_id AND f.staging_object_key=s.object_key) ORDER BY s.created_at LIMIT 128").bind(storage).bind(storage)
}
