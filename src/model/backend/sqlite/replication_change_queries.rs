use crate::model::{
    Id,
    replication::{
        Scope, Snapshot,
        changes::{Change, Confirmation},
    },
};
use sqlx::{Sqlite, types::Json};
use uuid::Uuid;

type Statement<'q> = sqlx::query::Query<'q, Sqlite, <Sqlite as sqlx::Database>::Arguments>;
type Rows<'q, T> = sqlx::query::QueryAs<'q, Sqlite, T, <Sqlite as sqlx::Database>::Arguments>;
type Scalar<'q, T> = sqlx::query::QueryScalar<'q, Sqlite, T, <Sqlite as sqlx::Database>::Arguments>;

pub(super) fn gate(bucket: Uuid) -> Scalar<'static, i32> {
    sqlx::query_scalar("SELECT subscribers FROM bucket_change_gates WHERE bucket_id=$1")
        .bind(bucket)
}

pub(super) fn count(bucket: Uuid, delta: i32) -> Statement<'static> {
    sqlx::query("UPDATE bucket_change_gates SET subscribers=subscribers+$2 WHERE bucket_id=$1")
        .bind(bucket)
        .bind(delta)
}

pub(super) fn subscribe(task: Uuid, bucket: Uuid, scope: &Scope) -> Statement<'_> {
    sqlx::query("INSERT INTO transfer_subscriptions(task_id,bucket_id,scope) VALUES($1,$2,$3)")
        .bind(task)
        .bind(bucket)
        .bind(Json(scope))
}

pub(super) fn seal(task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE transfer_subscriptions SET active=false WHERE task_id=$1 AND active")
        .bind(task)
}

pub(super) fn subscribed(task: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM transfer_subscriptions WHERE task_id=$1)")
        .bind(task)
}

pub(super) fn forget(task: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM transfer_subscriptions WHERE task_id=$1 AND NOT active").bind(task)
}

pub(super) fn discard(task: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM transfer_changes WHERE task_id=$1 AND object_key IN (SELECT object_key FROM transfer_changes WHERE task_id=$1 ORDER BY object_key LIMIT 128)").bind(task)
}

pub(super) fn pending(task: Uuid) -> Rows<'static, Change> {
    sqlx::query_as("SELECT object_key,source_version,snapshot,revision FROM transfer_changes WHERE task_id=$1 ORDER BY object_key LIMIT 1").bind(task)
}

pub(super) fn consume(task: Uuid, key: &str, revision: i64) -> Statement<'_> {
    sqlx::query("DELETE FROM transfer_changes WHERE task_id=$1 AND object_key=$2 AND revision=$3")
        .bind(task)
        .bind(key)
        .bind(revision)
}

pub(super) fn upper<'q>(
    bucket: Uuid,
    prefix: &'q str,
    limit: Option<&'q str>,
) -> Scalar<'q, String> {
    sqlx::query_scalar("SELECT CAST(object_key AS TEXT) FROM objects WHERE bucket_id=$1 AND object_key>=$2 AND ($3 IS NULL OR object_key<$3) AND stream_id IS NOT NULL ORDER BY object_key DESC LIMIT 1").bind(bucket).bind(prefix.as_bytes()).bind(limit.map(str::as_bytes))
}

pub(super) fn frozen(stream: Id) -> Rows<'static, super::replication_queries::Source> {
    sqlx::query_as("SELECT id,CAST(object_key AS TEXT) AS object_key,metadata,checksums,public_read,size,etag,payload_type FROM streams WHERE id=$1 AND state IN ('ready','retired')").bind(stream)
}

pub(super) fn reopen<'q>(
    item: Id,
    source: Option<Id>,
    target: Option<Id>,
    snapshot: &'q Snapshot,
    state: &'q str,
    error: Option<&'q str>,
) -> Statement<'q> {
    sqlx::query("UPDATE transfer_items SET source_version=$2,source_pin=CASE WHEN $5='planning' THEN $2 ELSE NULL END,target_version=$3,snapshot=$4,state=$5,error=$6,position_bytes=0,reuse_target=false,candidate=NULL,output_version=NULL WHERE id=$1")
        .bind(item).bind(source).bind(target).bind(Json(snapshot)).bind(state).bind(error)
}

pub(super) fn confirm(item: Id, value: &Confirmation) -> Statement<'_> {
    sqlx::query("UPDATE transfer_items SET confirmation=$2 WHERE id=$1")
        .bind(item)
        .bind(Json(value))
}

pub(super) fn reset_external(item: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM transfer_external_items WHERE item_id=$1").bind(item)
}

pub(super) fn reset_parts(item: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM transfer_external_parts WHERE item_id=$1").bind(item)
}
