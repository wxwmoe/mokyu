use crate::model::{
    Id,
    replication::{
        Scope, Snapshot,
        changes::{Change, Confirmation},
    },
};
use sqlx::{MySql, types::Json};
use uuid::Uuid;

type Statement<'q> = sqlx::query::Query<'q, MySql, <MySql as sqlx::Database>::Arguments>;
type Rows<'q, T> = sqlx::query::QueryAs<'q, MySql, T, <MySql as sqlx::Database>::Arguments>;
type Scalar<'q, T> = sqlx::query::QueryScalar<'q, MySql, T, <MySql as sqlx::Database>::Arguments>;

pub(super) fn gate(bucket: Uuid) -> Scalar<'static, i32> {
    sqlx::query_scalar("SELECT subscribers FROM bucket_change_gates WHERE bucket_id=? FOR UPDATE")
        .bind(bucket)
}

pub(super) fn stream_gate(stream: Id) -> Scalar<'static, i32> {
    sqlx::query_scalar("SELECT g.subscribers FROM bucket_change_gates g JOIN streams s ON s.bucket_id=g.bucket_id WHERE s.id=? LOCK IN SHARE MODE").bind(stream)
}

pub(super) fn capture(stream: Id) -> Statement<'static> {
    sqlx::query("INSERT INTO transfer_changes(task_id,object_key,source_version,snapshot) SELECT t.task_id,CAST(s.object_key AS CHAR CHARACTER SET utf8mb4),s.id,JSON_OBJECT('metadata',JSON_EXTRACT(s.metadata,'$'),'checksums',JSON_EXTRACT(s.checksums,'$'),'public',JSON_EXTRACT(IF(s.public_read,'true','false'),'$'),'size',s.size,'etag',s.etag,'payload',s.payload_type) FROM transfer_subscriptions t JOIN streams s ON s.bucket_id=t.bucket_id WHERE s.id=? AND s.state='ready' AND t.active AND EXISTS(SELECT 1 FROM objects WHERE bucket_id=s.bucket_id AND object_key=s.object_key AND stream_id=s.id) AND (JSON_UNQUOTE(JSON_EXTRACT(t.scope,'$.selection'))='all' OR JSON_UNQUOTE(JSON_EXTRACT(t.scope,'$.selection'))='prefix' AND LEFT(s.object_key,OCTET_LENGTH(JSON_UNQUOTE(JSON_EXTRACT(t.scope,'$.prefix'))))=CAST(JSON_UNQUOTE(JSON_EXTRACT(t.scope,'$.prefix')) AS BINARY) OR JSON_UNQUOTE(JSON_EXTRACT(t.scope,'$.selection'))='objects' AND JSON_CONTAINS(JSON_EXTRACT(t.scope,'$.keys'),JSON_QUOTE(CAST(s.object_key AS CHAR CHARACTER SET utf8mb4)))) ON DUPLICATE KEY UPDATE object_key=IF(BINARY transfer_changes.object_key=BINARY VALUES(object_key),transfer_changes.object_key,NULL),source_version=VALUES(source_version),snapshot=VALUES(snapshot),revision=transfer_changes.revision+1").bind(stream)
}

pub(super) fn count(bucket: Uuid, delta: i32) -> Statement<'static> {
    sqlx::query("UPDATE bucket_change_gates SET subscribers=subscribers+? WHERE bucket_id=?")
        .bind(delta)
        .bind(bucket)
}

pub(super) fn subscribe(task: Uuid, bucket: Uuid, scope: &Scope) -> Statement<'_> {
    sqlx::query("INSERT INTO transfer_subscriptions(task_id,bucket_id,scope) VALUES(?,?,?)")
        .bind(task)
        .bind(bucket)
        .bind(Json(scope))
}

pub(super) fn seal(task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE transfer_subscriptions SET active=false WHERE task_id=? AND active")
        .bind(task)
}

pub(super) fn subscribed(task: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM transfer_subscriptions WHERE task_id=?)")
        .bind(task)
}

pub(super) fn forget(task: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM transfer_subscriptions WHERE task_id=? AND NOT active").bind(task)
}

pub(super) fn discard(task: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM transfer_changes WHERE task_id=? LIMIT 128").bind(task)
}

pub(super) fn pending(task: Uuid) -> Rows<'static, Change> {
    sqlx::query_as("SELECT object_key,source_version,snapshot,revision FROM transfer_changes WHERE task_id=? ORDER BY key_hash LIMIT 1 FOR UPDATE").bind(task)
}

pub(super) fn consume(task: Uuid, key: &str, revision: i64) -> Statement<'_> {
    sqlx::query("DELETE FROM transfer_changes WHERE task_id=? AND key_hash=UNHEX(SHA2(CAST(? AS BINARY),256)) AND BINARY object_key=BINARY ? AND revision=?").bind(task).bind(key).bind(key).bind(revision)
}

pub(super) fn upper<'q>(
    bucket: Uuid,
    prefix: &'q str,
    limit: Option<&'q str>,
) -> Scalar<'q, String> {
    sqlx::query_scalar("SELECT CAST(object_key AS CHAR CHARACTER SET utf8mb4) FROM objects WHERE bucket_id=? AND object_key>=? AND (? IS NULL OR object_key<?) AND stream_id IS NOT NULL ORDER BY object_key DESC LIMIT 1").bind(bucket).bind(prefix.as_bytes()).bind(limit.map(str::as_bytes)).bind(limit.map(str::as_bytes))
}

pub(super) fn frozen(stream: Id) -> Rows<'static, super::replication_queries::Source> {
    sqlx::query_as("SELECT id,CAST(object_key AS CHAR CHARACTER SET utf8mb4) AS object_key,metadata,checksums,public_read,size,etag,payload_type FROM streams WHERE id=? AND state IN ('ready','retired') LOCK IN SHARE MODE").bind(stream)
}

pub(super) fn reopen<'q>(
    item: Id,
    source: Option<Id>,
    target: Option<Id>,
    snapshot: &'q Snapshot,
    state: &'q str,
    error: Option<&'q str>,
) -> Statement<'q> {
    sqlx::query("UPDATE transfer_items SET source_version=?,source_pin=CASE WHEN ?='planning' THEN ? ELSE NULL END,target_version=?,snapshot=?,state=?,error=?,position_bytes=0,reuse_target=false,candidate=NULL,output_version=NULL WHERE id=?")
        .bind(source).bind(state).bind(source).bind(target).bind(Json(snapshot)).bind(state).bind(error).bind(item)
}

pub(super) fn confirm(item: Id, value: &Confirmation) -> Statement<'_> {
    sqlx::query("UPDATE transfer_items SET confirmation=? WHERE id=?")
        .bind(Json(value))
        .bind(item)
}

pub(super) fn reset_external(item: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM transfer_external_items WHERE item_id=?").bind(item)
}

pub(super) fn reset_parts(item: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM transfer_external_parts WHERE item_id=?").bind(item)
}
