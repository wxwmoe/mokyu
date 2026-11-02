use super::super::node_writes::{Completion, Tail};
use super::{Rows, Scalar, Statement};
use crate::model::{Id, StreamTail, node_complete as complete};
use sqlx::types::Json;
use uuid::Uuid;
pub(in super::super) fn settings() -> Scalar<'static, Json<crate::config::UploadSettings>> {
    sqlx::query_scalar(
        "SELECT value FROM runtime_settings WHERE section='uploads' LOCK IN SHARE MODE",
    )
}
pub(in super::super) fn tail(node: Uuid, op: Uuid) -> Rows<'static, Tail> {
    sqlx::query_as("SELECT * FROM node_write_tails WHERE node_id=? AND operation_id=?")
        .bind(node)
        .bind(op)
}
pub(in super::super) async fn fragment(
    tx: &mut super::node_registry::Tx<'_>,
    tail: &StreamTail,
) -> anyhow::Result<Id> {
    let done = super::insert_fragment(tail).execute(&mut **tx).await?;
    Ok(Id::try_from(done.last_insert_id())?)
}
pub(in super::super) fn add_tail<'a>(
    node: Uuid,
    op: Uuid,
    hash: &'a [u8],
    fragment: Id,
    tail: &'a StreamTail,
) -> Statement<'a> {
    sqlx::query("INSERT INTO node_write_tails(node_id,operation_id,request_hash,fragment_id,location_id,storage_id,object_key,offset_bytes,size) VALUES(?,?,?,?,?,?,?,?,?)").bind(node).bind(op).bind(hash).bind(fragment).bind(tail.fragment).bind(tail.staging_storage).bind(&tail.staging_key).bind(tail.offset).bind(tail.size)
}
pub(in super::super) fn finish_tail(node: Uuid, op: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE node_write_tails SET committed=TRUE WHERE node_id=? AND operation_id=?")
        .bind(node)
        .bind(op)
}
pub(in super::super) fn record(id: Uuid) -> Rows<'static, Completion> {
    sqlx::query_as(
        "SELECT manifest_hash,conditions,summary FROM node_completions WHERE upload_id=?",
    )
    .bind(id)
}
pub(in super::super) fn save<'a>(
    upload: Uuid,
    hash: &'a str,
    conditions: &'a complete::Conditions,
    summary: &'a complete::Summary,
) -> Statement<'a> {
    sqlx::query(
        "INSERT INTO node_completions(upload_id,manifest_hash,conditions,summary) VALUES(?,?,?,?)",
    )
    .bind(upload)
    .bind(hash)
    .bind(Json(conditions))
    .bind(Json(summary))
}
pub(in super::super) fn checksums(upload: Uuid) -> Scalar<'static, serde_json::Value> {
    sqlx::query_scalar(
        "SELECT checksums FROM node_completion_parts WHERE upload_id=? ORDER BY ordinal",
    )
    .bind(upload)
}
pub(in super::super) fn sources(
    upload: Uuid,
    first: i32,
    count: i64,
) -> Rows<'static, crate::app::StoredStream> {
    sqlx::query_as("SELECT s.* FROM node_completion_parts p JOIN streams s ON s.id=p.stream_id WHERE p.upload_id=? AND p.ordinal>=? AND s.state IN ('ready','retired') ORDER BY p.ordinal LIMIT ?").bind(upload).bind(first).bind(count)
}
pub(in super::super) fn selected(
    upload: Uuid,
    numbers: &[i32],
) -> Rows<'static, complete::Selected> {
    sqlx::query_as("SELECT p.part_number AS number,s.size,s.etag,s.checksums FROM JSON_TABLE(?, '$[*]' COLUMNS(ordinal FOR ORDINALITY,number INT PATH '$')) n JOIN parts p ON p.part_number=n.number JOIN streams s ON s.id=p.stream_id WHERE p.upload_id=? AND s.state='ready' ORDER BY n.ordinal-1").bind(Json(numbers.to_vec())).bind(upload)
}
pub(in super::super) fn freeze(upload: Uuid, numbers: &[i32]) -> Statement<'static> {
    sqlx::query("INSERT INTO node_completion_parts(upload_id,ordinal,stream_id,part_number,size,etag,checksums) SELECT p.upload_id,n.ordinal-1,s.id,p.part_number,s.size,s.etag,s.checksums FROM JSON_TABLE(?, '$[*]' COLUMNS(ordinal FOR ORDINALITY,number INT PATH '$')) n JOIN parts p ON p.part_number=n.number JOIN streams s ON s.id=p.stream_id WHERE p.upload_id=? AND s.state='ready'").bind(Json(numbers.to_vec())).bind(upload)
}

pub(in super::super) fn attach(
    stream: Id,
    offset: i64,
    size: i32,
    fragment: Id,
) -> Statement<'static> {
    sqlx::query("INSERT INTO extents(stream_id,offset_bytes,length,fragment_id) VALUES(?,?,?,?)")
        .bind(stream)
        .bind(offset)
        .bind(size)
        .bind(fragment)
}
pub(in super::super) fn shared(
    node: &crate::cluster::NodeState,
    run: crate::model::session::Run,
    ids: &[Uuid],
) -> Rows<'static, (Uuid, Uuid, i64)> {
    sqlx::query_as("SELECT DISTINCT p.token,p.storage_id,CAST(TIMESTAMPDIFF(MICROSECOND,UTC_TIMESTAMP(6),p.expires_at) DIV 1000 AS SIGNED) FROM storage_permits p JOIN node_read_permits n ON n.token=p.token JOIN node_reads r ON r.read_id=n.read_id WHERE r.node_id=? AND r.boot_id=? AND r.fence=? AND r.credential_generation=? AND p.direction='read' AND r.expires_at>UTC_TIMESTAMP(6) AND p.expires_at>UTC_TIMESTAMP(6) AND p.token IN (SELECT UNHEX(j.id) FROM JSON_TABLE(?, '$[*]' COLUMNS(id CHAR(32) PATH '$')) j)").bind(node.node_id).bind(run.boot).bind(run.fence).bind(node.credential_generation).bind(Json(ids.iter().map(|i|i.simple().to_string()).collect::<Vec<_>>()))
}
pub(in super::super) fn result(upload: Uuid) -> Scalar<'static, Option<serde_json::Value>> {
    sqlx::query_scalar("SELECT result FROM uploads WHERE id=?").bind(upload)
}
pub(in super::super) fn release_completion(op: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE uploads SET completion_token=NULL,completion_expires_at=NULL WHERE state='completing' AND completion_token=? AND output_stream IS NULL").bind(op)
}
