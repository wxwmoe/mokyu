use crate::model::{Id, replication::Snapshot};
use sqlx::{Sqlite, types::Json};
use uuid::Uuid;

type Statement<'q> = sqlx::query::Query<'q, Sqlite, <Sqlite as sqlx::Database>::Arguments>;
type Rows<'q, T> = sqlx::query::QueryAs<'q, Sqlite, T, <Sqlite as sqlx::Database>::Arguments>;

#[derive(sqlx::FromRow)]
pub(in super::super) struct Part {
    pub part_stream: Option<Id>,
    pub node_id: Option<Uuid>,
    pub operation_id: Option<Uuid>,
}

pub(in super::super) fn current(item: Id) -> Rows<'static, Part> {
    sqlx::query_as("SELECT part_stream,node_id,operation_id FROM transfer_imports WHERE item_id=?")
        .bind(item)
}

pub(in super::super) fn insert(item: Id) -> Statement<'static> {
    sqlx::query("INSERT INTO transfer_imports(item_id) VALUES(?)").bind(item)
}

pub(in super::super) fn target(item: Id, target: Option<Id>, snapshot: &Snapshot) -> Statement<'_> {
    sqlx::query("UPDATE transfer_items SET target_version=?,source_pin=?,snapshot=? WHERE id=?")
        .bind(target)
        .bind(target)
        .bind(Json(snapshot))
        .bind(item)
}

pub(in super::super) fn part(
    item: Id,
    stream: Option<Id>,
    node: Option<Uuid>,
    operation: Option<Uuid>,
) -> Statement<'static> {
    sqlx::query(
        "UPDATE transfer_imports SET part_stream=?,node_id=?,operation_id=? WHERE item_id=?",
    )
    .bind(stream)
    .bind(node)
    .bind(operation)
    .bind(item)
}

pub(in super::super) fn forget(item: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM transfer_imports WHERE item_id=?").bind(item)
}

pub(in super::super) fn stream(stream: Id) -> Rows<'static, crate::app::StoredStream> {
    sqlx::query_as("SELECT * FROM streams WHERE id=? AND state='writing'").bind(stream)
}

pub(in super::super) fn owner(
    task: Uuid,
    stream: Id,
    operation: Uuid,
) -> super::Scalar<'static, Id> {
    sqlx::query_scalar("SELECT t.id FROM transfer_imports i JOIN transfer_items t ON t.id=i.item_id WHERE t.task_id=? AND i.part_stream=? AND i.operation_id=?").bind(task).bind(stream).bind(operation)
}
