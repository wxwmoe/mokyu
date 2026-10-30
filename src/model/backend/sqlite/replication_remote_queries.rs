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

pub(super) fn entry(task: Uuid, item: Id) -> Rows<'static, super::replication_queries::Entry> {
    sqlx::query_as("SELECT id,source_key,target_key,source_version,source_pin,target_version,candidate,output_version,snapshot,state,position_bytes,reuse_target,error FROM transfer_items WHERE task_id=?1 AND id=?2").bind(task).bind(item)
}

pub(super) fn stream(id: Id) -> Rows<'static, crate::app::StoredStream> {
    sqlx::query_as("SELECT * FROM streams WHERE id=?1 AND state='ready'").bind(id)
}

pub(super) fn range(stream: Id, offset: i64) -> Rows<'static, crate::model::raw::RawRange> {
    sqlx::query_as("SELECT r.id AS raw_id,r.location_id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine,e.offset_bytes,e.length,e.source_offset FROM raw_ranges e JOIN raw_sources r ON r.id=e.source_id WHERE e.stream_id=$1 AND e.offset_bytes<=$2 AND e.offset_bytes+e.length>$2 ORDER BY e.offset_bytes DESC LIMIT 1").bind(stream).bind(offset)
}

pub(super) fn proof(unit: Id, offset: i64) -> Rows<'static, (i64, i32, Vec<u8>)> {
    sqlx::query_as("SELECT source_offset,length,hash FROM transfer_spans WHERE unit_id=$1 AND source_offset<=$2 AND source_offset+length>$2 ORDER BY source_offset DESC LIMIT 1").bind(unit).bind(offset)
}

pub(super) fn pending_raw(task: Uuid, stream: Id) -> Rows<'static, super::transfer_queries::Unit> {
    sqlx::query_as("SELECT u.id,u.source_type,u.source_id,u.input,u.output,u.candidate,u.state,u.scanned_bytes,u.written_bytes,u.checksum,u.upload_id FROM transfer_sources u WHERE u.task_id=$1 AND u.source_type='raw' AND u.state<>'ready' AND EXISTS(SELECT 1 FROM raw_ranges e WHERE e.stream_id=$2 AND e.source_id=u.source_id) ORDER BY u.id LIMIT 1").bind(task).bind(stream)
}
