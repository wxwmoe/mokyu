use crate::model::{
    Id,
    replication::external::{Part, State},
};
use sqlx::{Sqlite, types::Json};
use uuid::Uuid;

type Statement<'q> = sqlx::query::Query<'q, Sqlite, <Sqlite as sqlx::Database>::Arguments>;
type Rows<'q, T> = sqlx::query::QueryAs<'q, Sqlite, T, <Sqlite as sqlx::Database>::Arguments>;
type Scalar<'q, T> = sqlx::query::QueryScalar<'q, Sqlite, T, <Sqlite as sqlx::Database>::Arguments>;

pub(super) fn lookup(task: Uuid, key: &str) -> Scalar<'_, Id> {
    sqlx::query_scalar("SELECT id FROM transfer_items WHERE task_id=$1 AND source_key=$2")
        .bind(task)
        .bind(key)
}

pub(super) fn pending(task: Uuid) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM transfer_items WHERE task_id=$1 AND state IN ('planning','copying') ORDER BY id LIMIT 1").bind(task)
}

pub(super) fn key(task: Uuid, item: Id) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT source_key FROM transfer_items WHERE task_id=$1 AND id=$2")
        .bind(task)
        .bind(item)
}

pub(super) fn state(item: Id) -> Scalar<'static, Json<State>> {
    sqlx::query_scalar("SELECT state FROM transfer_external_items WHERE item_id=$1").bind(item)
}

pub(super) fn insert(item: Id, state: &State) -> Statement<'_> {
    sqlx::query("INSERT INTO transfer_external_items(item_id,state) VALUES($1,$2)")
        .bind(item)
        .bind(Json(state))
}

pub(super) fn save(item: Id, state: &State) -> Statement<'_> {
    sqlx::query("UPDATE transfer_external_items SET state=$2 WHERE item_id=$1")
        .bind(item)
        .bind(Json(state))
}

pub(super) fn part(item: Id, part: &Part) -> Statement<'_> {
    sqlx::query("INSERT INTO transfer_external_parts(item_id,part_number,size_bytes,etag,digest) VALUES($1,$2,$3,$4,$5)").bind(item).bind(part.number).bind(part.size).bind(&part.etag).bind(&part.digest)
}

pub(super) fn parts(item: Id, first: i32, last: i32) -> Rows<'static, Part> {
    sqlx::query_as("SELECT part_number AS number,size_bytes AS size,etag,digest FROM transfer_external_parts WHERE item_id=$1 AND part_number>=$2 AND part_number<=$3 ORDER BY part_number").bind(item).bind(first).bind(last)
}

pub(super) fn forget(task: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM transfer_connections WHERE task_id=$1").bind(task)
}
