use crate::model::{
    Id,
    replication::external::{Part, State},
};
use sqlx::{MySql, types::Json};
use uuid::Uuid;

type Statement<'q> = sqlx::query::Query<'q, MySql, <MySql as sqlx::Database>::Arguments>;
type Rows<'q, T> = sqlx::query::QueryAs<'q, MySql, T, <MySql as sqlx::Database>::Arguments>;
type Scalar<'q, T> = sqlx::query::QueryScalar<'q, MySql, T, <MySql as sqlx::Database>::Arguments>;

pub(super) fn lookup(task: Uuid, key: &str) -> Scalar<'_, Id> {
    sqlx::query_scalar("SELECT id FROM transfer_items WHERE task_id=? AND source_key_hash=UNHEX(SHA2(CAST(? AS BINARY),256)) AND BINARY source_key=BINARY ?").bind(task).bind(key).bind(key)
}

pub(super) fn pending(task: Uuid) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM transfer_items WHERE task_id=? AND state IN ('planning','copying') ORDER BY id LIMIT 1").bind(task)
}

pub(super) fn key(task: Uuid, item: Id) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT source_key FROM transfer_items WHERE task_id=? AND id=?")
        .bind(task)
        .bind(item)
}

pub(super) fn state(item: Id) -> Scalar<'static, Json<State>> {
    sqlx::query_scalar("SELECT state FROM transfer_external_items WHERE item_id=?").bind(item)
}

pub(super) fn insert(item: Id, state: &State) -> Statement<'_> {
    sqlx::query("INSERT INTO transfer_external_items(item_id,state) VALUES(?,?)")
        .bind(item)
        .bind(Json(state))
}

pub(super) fn save(item: Id, state: &State) -> Statement<'_> {
    sqlx::query("UPDATE transfer_external_items SET state=? WHERE item_id=?")
        .bind(Json(state))
        .bind(item)
}

pub(super) fn part(item: Id, part: &Part) -> Statement<'_> {
    sqlx::query("INSERT INTO transfer_external_parts(item_id,part_number,size_bytes,etag,digest) VALUES(?,?,?,?,?)").bind(item).bind(part.number).bind(part.size).bind(&part.etag).bind(&part.digest)
}

pub(super) fn parts(item: Id, first: i32, last: i32) -> Rows<'static, Part> {
    sqlx::query_as("SELECT part_number AS number,size_bytes AS size,etag,digest FROM transfer_external_parts WHERE item_id=? AND part_number>=? AND part_number<=? ORDER BY part_number").bind(item).bind(first).bind(last)
}

pub(super) fn forget(task: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM transfer_connections WHERE task_id=?").bind(task)
}
