use super::super::node_writes::{Raw, Write};
use super::{Rows, Scalar, Statement};
use crate::{
    cluster::NodeState,
    model::{Id, node_write as write, session::Run},
    storage::profile::StorageProfile,
};
use sqlx::types::Json;
use uuid::Uuid;

pub(in super::super) fn owned(node: Uuid, op: Uuid) -> Rows<'static, Write> {
    sqlx::query_as("SELECT w.*,s.owner_task_id,CASE WHEN w.phase='active' AND s.state='writing' AND (EXISTS(SELECT 1 FROM objects o WHERE o.bucket_id=w.bucket_id AND o.object_key=s.object_key AND o.write_token=s.id) OR EXISTS(SELECT 1 FROM parts p JOIN uploads u ON u.id=p.upload_id WHERE p.write_token=s.id AND u.state='active') OR EXISTS(SELECT 1 FROM tasks t WHERE t.id=s.owner_task_id AND t.execution_token=s.owner_execution_token AND t.state='running')) THEN CAST(GREATEST(0,TIMESTAMPDIFF(MICROSECOND,UTC_TIMESTAMP(6),w.expires_at) DIV 1000) AS SIGNED) ELSE 0 END AS remaining FROM node_writes w LEFT JOIN streams s ON s.id=w.stream_id WHERE w.node_id=? AND w.operation_id=? FOR UPDATE").bind(node).bind(op)
}
pub(in super::super) fn superseded_object(lease: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM node_writes w JOIN streams s ON s.id=w.stream_id
         JOIN objects o ON o.bucket_id=s.bucket_id AND o.object_key=s.object_key
         WHERE w.lease_id=? AND w.expires_at>UTC_TIMESTAMP(6) AND s.state='writing'
           AND o.write_token<>s.id)",
    )
    .bind(lease)
}

pub(in super::super) fn insert<'a>(
    node: &'a NodeState,
    input: &'a write::Begin,
    hash: &'a [u8],
    stream: Id,
    bucket: Uuid,
    storage: Uuid,
    block: Option<Uuid>,
    profile: &'a StorageProfile,
    copy: Option<&'a write::CopySource>,
) -> Statement<'a> {
    sqlx::query("INSERT INTO node_writes(node_id,operation_id,lease_id,boot_id,fence,credential_generation,request_hash,stream_id,bucket_id,storage_id,block_key_id,profile,intent,copy_source,expires_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,UTC_TIMESTAMP(6)+INTERVAL 90 SECOND)")
        .bind(node.node_id).bind(input.operation).bind(Uuid::new_v4()).bind(input.run.boot).bind(input.run.fence).bind(node.credential_generation).bind(hash).bind(stream).bind(bucket).bind(storage).bind(block).bind(Json(profile)).bind(Json(&input.intent)).bind(copy.map(Json))
}
pub(in super::super) fn count(node: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT count(*) FROM node_writes WHERE node_id=? AND phase='active' AND expires_at>UTC_TIMESTAMP(6)").bind(node)
}
pub(in super::super) fn phase<'a>(
    node: Uuid,
    op: Uuid,
    phase: &'a str,
    hash: Option<&'a [u8]>,
    result: Option<&'a write::Published>,
) -> Statement<'a> {
    sqlx::query("UPDATE node_writes SET phase=?,publish_hash=?,published=?,updated_at=UTC_TIMESTAMP(6) WHERE node_id=? AND operation_id=?").bind(phase).bind(hash).bind(result.map(Json)).bind(node).bind(op)
}
// Find due keys before locking the primary rows used by publishers.
pub(in super::super) fn expired() -> Rows<'static, (Uuid, Uuid, Option<Id>)> {
    sqlx::query_as(
        "SELECT w.node_id,w.operation_id,w.stream_id
         FROM (SELECT node_id,operation_id FROM node_writes
               WHERE phase='active' AND expires_at<=UTC_TIMESTAMP(6) ORDER BY expires_at LIMIT 64) due
         STRAIGHT_JOIN node_writes w FORCE INDEX(PRIMARY)
           ON w.node_id=due.node_id AND w.operation_id=due.operation_id
         WHERE w.phase='active' AND w.expires_at<=UTC_TIMESTAMP(6)
         ORDER BY w.node_id,w.operation_id FOR UPDATE SKIP LOCKED",
    )
}
pub(in super::super) fn prune() -> Statement<'static> {
    sqlx::query(
        "DELETE old FROM (
             SELECT w.node_id,w.operation_id
             FROM (SELECT node_id,operation_id FROM node_writes
                   WHERE phase IN ('aborted','expired','published') AND updated_at<UTC_TIMESTAMP(6)-INTERVAL 1 DAY
                   ORDER BY phase,updated_at LIMIT 128) due
             STRAIGHT_JOIN node_writes w FORCE INDEX(PRIMARY)
               ON w.node_id=due.node_id AND w.operation_id=due.operation_id
             WHERE w.phase IN ('aborted','expired','published') AND w.updated_at<UTC_TIMESTAMP(6)-INTERVAL 1 DAY
             ORDER BY w.node_id,w.operation_id LIMIT 128 FOR UPDATE SKIP LOCKED
         ) picked
         STRAIGHT_JOIN node_writes old FORCE INDEX(PRIMARY)
           ON old.node_id=picked.node_id AND old.operation_id=picked.operation_id",
    )
}
pub(in super::super) fn touch(node: Uuid, run: Run, id: Uuid, renew: bool) -> Statement<'static> {
    sqlx::query(if renew {"UPDATE node_writes SET expires_at=UTC_TIMESTAMP(6)+INTERVAL 90 SECOND WHERE lease_id=? AND node_id=? AND boot_id=? AND fence=? AND phase='active' AND expires_at>UTC_TIMESTAMP(6)"} else {"UPDATE node_writes SET expires_at=UTC_TIMESTAMP(6) WHERE lease_id=? AND node_id=? AND boot_id=? AND fence=? AND phase='active'"}).bind(id).bind(node).bind(run.boot).bind(run.fence)
}
pub(in super::super) fn raw(node: Uuid, op: Uuid) -> Rows<'static, Raw> {
    sqlx::query_as("SELECT * FROM node_write_raw WHERE node_id=? AND operation_id=?")
        .bind(node)
        .bind(op)
}
pub(in super::super) fn add_raw<'a>(
    node: Uuid,
    op: Uuid,
    hash: &'a [u8],
    source: Id,
    location: Uuid,
    input: &'a crate::model::node_raw::Input,
    committed: bool,
) -> Statement<'a> {
    sqlx::query("INSERT INTO node_write_raw(node_id,operation_id,request_hash,source_id,location_id,size,digest,content_hash,committed) VALUES(?,?,?,?,?,?,?,?,?)").bind(node).bind(op).bind(hash).bind(source).bind(location).bind(input.size).bind(input.digest.as_slice()).bind(input.hash.as_ref().map(|h|h.as_slice())).bind(committed)
}
pub(in super::super) fn commit_raw(node: Uuid, op: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE node_write_raw SET committed=TRUE WHERE node_id=? AND operation_id=?")
        .bind(node)
        .bind(op)
}
pub(in super::super) fn advance(node: Uuid, op: Uuid, end: i64) -> Statement<'static> {
    sqlx::query("UPDATE node_writes SET next_offset=?,next_batch=next_batch+1 WHERE node_id=? AND operation_id=?").bind(end).bind(node).bind(op)
}
pub(in super::super) async fn raw_source(
    tx: &mut super::node_registry::Tx<'_>,
    location: Uuid,
    stream: Id,
    storage: Uuid,
    revision: i64,
    medium: &'static str,
) -> anyhow::Result<Id> {
    let done = super::raw_insert(location, stream, storage, revision, medium, "durable")
        .execute(&mut **tx)
        .await?;
    Ok(Id::try_from(done.last_insert_id())?)
}
pub(in super::super) fn pool_load(storage: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT count(*) FROM storage_permits WHERE storage_id=? AND direction='upload' AND expires_at>UTC_TIMESTAMP(6)").bind(storage)
}
pub(in super::super) fn permits(
    node: Uuid,
    op: Uuid,
    storage: Uuid,
    batch: i64,
) -> Rows<'static, (Uuid, i64)> {
    sqlx::query_as("SELECT p.token,CAST(GREATEST(0,TIMESTAMPDIFF(MICROSECOND,UTC_TIMESTAMP(6),p.expires_at) DIV 1000) AS SIGNED) FROM storage_permits p JOIN node_write_permits n ON n.token=p.token WHERE n.node_id=? AND n.operation_id=? AND n.batch_id=? AND p.storage_id=? AND p.direction='upload' AND p.expires_at>UTC_TIMESTAMP(6) ORDER BY p.token").bind(node).bind(op).bind(batch).bind(storage)
}
pub(in super::super) fn permit(token: Uuid, storage: Uuid, node: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO storage_permits(token,storage_id,direction,holder,expires_at) VALUES(?,?,'upload',?,UTC_TIMESTAMP(6)+INTERVAL 60 SECOND)").bind(token).bind(storage).bind(node)
}
pub(in super::super) fn assign(
    token: Uuid,
    node: Uuid,
    op: Uuid,
    batch: i64,
) -> Statement<'static> {
    sqlx::query(
        "INSERT INTO node_write_permits(token,node_id,operation_id,batch_id) VALUES(?,?,?,?)",
    )
    .bind(token)
    .bind(node)
    .bind(op)
    .bind(batch)
}
pub(in super::super) fn release(node: Uuid, op: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM storage_permits WHERE token IN (SELECT token FROM node_write_permits WHERE node_id=? AND operation_id=?)").bind(node).bind(op)
}
pub(in super::super) fn touch_permit(
    node: Uuid,
    run: Run,
    id: Uuid,
    renew: bool,
) -> Statement<'static> {
    sqlx::query(if renew {"UPDATE storage_permits SET expires_at=UTC_TIMESTAMP(6)+INTERVAL 60 SECOND WHERE token=? AND direction='upload' AND expires_at>UTC_TIMESTAMP(6) AND EXISTS(SELECT 1 FROM node_write_permits n JOIN node_writes w ON w.node_id=n.node_id AND w.operation_id=n.operation_id WHERE n.token=storage_permits.token AND w.node_id=? AND w.boot_id=? AND w.fence=? AND w.phase='active' AND w.expires_at>UTC_TIMESTAMP(6))"} else {"DELETE FROM storage_permits WHERE token=? AND direction='upload' AND EXISTS(SELECT 1 FROM node_write_permits n JOIN node_writes w ON w.node_id=n.node_id AND w.operation_id=n.operation_id WHERE n.token=storage_permits.token AND w.node_id=? AND w.boot_id=? AND w.fence=?)"}).bind(id).bind(node).bind(run.boot).bind(run.fence)
}

pub(in super::super) fn replica(publication: Uuid, node: Uuid, op: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE replica_publications SET intent=json_set(intent,'$.node',json_array(?,?)) WHERE id=? AND phase='preparing'").bind(node.to_string()).bind(op.to_string()).bind(publication)
}

pub(in super::super) fn release_batch(node: Uuid, op: Uuid, batch: i64) -> Statement<'static> {
    sqlx::query("DELETE FROM storage_permits WHERE token IN (SELECT token FROM node_write_permits WHERE node_id=? AND operation_id=? AND batch_id=?)").bind(node).bind(op).bind(batch)
}
pub(in super::super) fn batch(
    node: Uuid,
    op: Uuid,
    batch: i64,
) -> Rows<'static, super::super::node_writes::Batch> {
    sqlx::query_as(
        "SELECT request_hash,end_bytes,ready,commit_hash,COALESCE(JSON_EXTRACT(plan,'$.items'),plan) AS plan,JSON_EXTRACT(plan,'$.packing') AS packing FROM node_write_batches WHERE node_id=? AND operation_id=? AND batch_id=?",
    )
    .bind(node)
    .bind(op)
    .bind(batch)
}
pub(in super::super) fn pending(node: Uuid, op: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT count(*) FROM node_write_batches WHERE node_id=? AND operation_id=? AND commit_hash IS NULL").bind(node).bind(op)
}
pub(in super::super) fn add_batch<'a>(
    node: Uuid,
    op: Uuid,
    batch: i64,
    hash: &'a [u8],
    end: i64,
    plan: super::super::node_writes::ChunkPlan<'a>,
    ready: bool,
    receipt: Option<&'a [u8]>,
) -> Statement<'a> {
    sqlx::query("INSERT INTO node_write_batches(node_id,operation_id,batch_id,request_hash,end_bytes,plan,ready,commit_hash) VALUES(?,?,?,?,?,?,?,?)").bind(node).bind(op).bind(batch).bind(hash).bind(end).bind(Json(plan)).bind(ready).bind(receipt)
}
pub(in super::super) fn activate<'a>(
    node: Uuid,
    op: Uuid,
    batch: i64,
    plan: &'a [crate::model::node_chunk::Item],
) -> Statement<'a> {
    sqlx::query("UPDATE node_write_batches SET ready=TRUE,plan=? WHERE node_id=? AND operation_id=? AND batch_id=? AND NOT ready").bind(Json(plan)).bind(node).bind(op).bind(batch)
}
pub(in super::super) fn commit_batch<'a>(
    node: Uuid,
    op: Uuid,
    batch: i64,
    hash: &'a [u8],
) -> Statement<'a> {
    sqlx::query("UPDATE node_write_batches SET commit_hash=? WHERE node_id=? AND operation_id=? AND batch_id=? AND ready AND commit_hash IS NULL").bind(hash).bind(node).bind(op).bind(batch)
}
