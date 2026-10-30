use crate::model::{Id, replication::Snapshot};
use sqlx::{Postgres, types::Json};
use uuid::Uuid;

type Statement<'q> = sqlx::query::Query<'q, Postgres, <Postgres as sqlx::Database>::Arguments>;
type Rows<'q, T> = sqlx::query::QueryAs<'q, Postgres, T, <Postgres as sqlx::Database>::Arguments>;
type Scalar<'q, T> =
    sqlx::query::QueryScalar<'q, Postgres, T, <Postgres as sqlx::Database>::Arguments>;

#[derive(sqlx::FromRow)]
pub(super) struct Part {
    pub part_stream: Option<Id>,
    pub node_id: Option<Uuid>,
    pub operation_id: Option<Uuid>,
}

pub(super) fn current(item: Id) -> Rows<'static, Part> {
    sqlx::query_as(
        "SELECT part_stream,node_id,operation_id FROM transfer_imports WHERE item_id=$1 FOR UPDATE",
    )
    .bind(item)
}

pub(super) fn insert(item: Id) -> Statement<'static> {
    sqlx::query("INSERT INTO transfer_imports(item_id) VALUES($1)").bind(item)
}

pub(super) fn target(item: Id, target: Option<Id>, snapshot: &Snapshot) -> Statement<'_> {
    sqlx::query("UPDATE transfer_items SET target_version=$2,source_pin=$2,snapshot=$3 WHERE id=$1")
        .bind(item)
        .bind(target)
        .bind(Json(snapshot))
}

pub(super) fn part(
    item: Id,
    stream: Option<Id>,
    node: Option<Uuid>,
    operation: Option<Uuid>,
) -> Statement<'static> {
    sqlx::query(
        "UPDATE transfer_imports SET part_stream=$2,node_id=$3,operation_id=$4 WHERE item_id=$1",
    )
    .bind(item)
    .bind(stream)
    .bind(node)
    .bind(operation)
}

pub(super) fn forget(item: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM transfer_imports WHERE item_id=$1").bind(item)
}

pub(super) fn stream(stream: Id) -> Rows<'static, crate::app::StoredStream> {
    sqlx::query_as("SELECT * FROM streams WHERE id=$1 AND state='writing' FOR UPDATE").bind(stream)
}

pub(super) fn abort_write(node: Uuid, operation: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE node_writes SET state='aborted',completed_at=clock_timestamp() WHERE node_id=$1 AND operation_id=$2 AND state='active'")
        .bind(node).bind(operation)
}

pub(super) fn owner(task: Uuid, stream: Id, operation: Uuid) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT i.item_id FROM transfer_imports i JOIN transfer_items t ON t.id=i.item_id WHERE t.task_id=$1 AND i.part_stream=$2 AND i.operation_id=$3 FOR UPDATE OF i,t")
        .bind(task).bind(stream).bind(operation)
}
