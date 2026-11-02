use super::super::node_work::Offer;
use super::{Rows, Scalar, Statement};
use crate::model::{
    node_work::{Exchange, Plan},
    session::Run,
};
use sqlx::types::Json;
use uuid::Uuid;

pub(in super::super) fn offer(node: Uuid, operation: Uuid) -> Rows<'static, Offer> {
    sqlx::query_as("SELECT boot_id,fence,request_hash,task_id,token,plan,materials_encrypted,result_hash,max(0,(expires_at-CAST(unixepoch('subsec')*1000000 AS INTEGER))/1000) AS remaining FROM node_work WHERE node_id=? AND operation_id=?").bind(node).bind(operation)
}

pub(in super::super) fn insert<'a>(
    node: Uuid,
    input: &Exchange,
    hash: &'a [u8],
    task: Option<Uuid>,
    token: Uuid,
    plan: Option<&'a Plan>,
    secret: Option<&'a [u8]>,
) -> Statement<'a> {
    sqlx::query("INSERT INTO node_work(node_id,operation_id,boot_id,fence,request_hash,task_id,token,plan,materials_encrypted,expires_at) VALUES(?,?,?,?,?,?,?,?,?,CAST(unixepoch('subsec')*1000000 AS INTEGER)+90000000)").bind(node).bind(input.operation).bind(input.run.boot).bind(input.run.fence).bind(hash).bind(task).bind(token).bind(plan.map(Json)).bind(secret)
}

pub(in super::super) fn next_task(
    class: u32,
    node: Uuid,
    worker: bool,
) -> Rows<'static, (Uuid, String, serde_json::Value)> {
    sqlx::query_as("SELECT t.id,t.task_type,t.detail FROM tasks t WHERE t.state='queued' AND (t.error IS NULL OR t.error<>'backend_io_failed' OR t.updated_at<=CAST(unixepoch('subsec')*1000000 AS INTEGER)-5000000) AND ((t.source_node=? AND t.task_type='upload' AND ?=2 AND (json_extract(t.detail,'$.operation')<>'cdc' OR NOT EXISTS(SELECT 1 FROM raw_sources r WHERE r.processing_task=t.id AND (r.owner_stream IS NOT NULL OR EXISTS(SELECT 1 FROM raw_ranges e JOIN streams s ON s.id=e.stream_id WHERE e.source_id=r.id AND s.state='writing')))) AND NOT EXISTS(SELECT 1 FROM raw_uploads u WHERE u.source_id=CAST(json_extract(t.detail,'$.source_id') AS INTEGER) AND u.next_attempt_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)) AND EXISTS(SELECT 1 FROM mokyu_meta WHERE id=1 AND NOT maintenance)) OR (t.source_node IS NULL AND ? AND ((?=1 AND t.task_type IN ('pack','unpack')) OR (?=0 AND t.task_type='integrity' AND (json_extract(t.detail,'$.operation')='external_test' OR json_type(t.detail,'$.source_operation')='object')) OR (?=2 AND (t.task_type='storage_migration' AND json_extract(t.detail,'$.operation')='tiering' OR t.task_type='upload' AND json_extract(t.detail,'$.operation')='deduplicate' OR t.task_type='upload' AND json_extract(t.detail,'$.operation')='cdc' AND EXISTS(SELECT 1 FROM raw_sources r WHERE r.processing_task=t.id AND r.medium='shared_s3' AND NOT r.pending_upload AND r.owner_stream IS NULL AND NOT EXISTS(SELECT 1 FROM raw_ranges e JOIN streams s ON s.id=e.stream_id WHERE e.source_id=r.id AND s.state='writing'))))) AND EXISTS(SELECT 1 FROM mokyu_meta WHERE id=1 AND remote_work AND NOT maintenance))) AND NOT EXISTS(SELECT 1 FROM maintenance_controls WHERE policy=t.task_type AND paused) ORDER BY t.updated_at,t.id LIMIT 1").bind(node).bind(class as i64).bind(worker).bind(class as i64).bind(class as i64).bind(class as i64)
}

pub(in super::super) fn settle(node: Uuid, operation: Uuid, hash: &[u8]) -> Statement<'_> {
    sqlx::query("UPDATE node_work SET result_hash=?,materials_encrypted=NULL WHERE node_id=? AND operation_id=? AND result_hash IS NULL").bind(hash).bind(node).bind(operation)
}

pub(in super::super) fn expire_node(node: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE node_work SET expires_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE node_id=? AND result_hash IS NULL AND expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)").bind(node)
}

pub(in super::super) fn requeue() -> Statement<'static> {
    sqlx::query(
        "UPDATE tasks SET state='queued',execution_token=NULL,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE state='running' AND EXISTS(SELECT 1 FROM node_work w WHERE w.task_id=tasks.id AND w.token=tasks.execution_token AND w.result_hash IS NULL AND w.expires_at<=CAST(unixepoch('subsec')*1000000 AS INTEGER))",
    )
}

pub(in super::super) fn prune() -> Statement<'static> {
    sqlx::query(
        "DELETE FROM node_work WHERE expires_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-900000000 AND (task_id IS NULL OR NOT EXISTS(SELECT 1 FROM tasks t WHERE t.id=node_work.task_id AND t.execution_token=node_work.token))",
    )
}

pub(in super::super) fn touch(
    node: Uuid,
    run: Run,
    token: Uuid,
    renew: bool,
) -> Statement<'static> {
    sqlx::query(if renew {
        "UPDATE node_work SET expires_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)+90000000 WHERE node_id=? AND boot_id=? AND fence=? AND token=? AND task_id IS NOT NULL AND result_hash IS NULL AND expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)"
    } else {
        "UPDATE node_work SET expires_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE node_id=? AND boot_id=? AND fence=? AND token=? AND task_id IS NOT NULL AND result_hash IS NULL AND expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)"
    }).bind(node).bind(run.boot).bind(run.fence).bind(token)
}
pub(in super::super) fn storage(id: Uuid) -> Rows<'static, crate::model::storage::StorageRow> {
    sqlx::query_as("SELECT id,name,type,path,endpoint_url,remote_bucket_name,region,prefix,access_key_encrypted,secret_key_encrypted,identity_marker,block_key_id,enabled FROM storages WHERE id=? AND enabled AND type='s3'").bind(id)
}
pub(in super::super) fn block_key(
    id: Uuid,
) -> Rows<'static, crate::model::key::ManagedKeyMaterial> {
    sqlx::query_as("SELECT id,name,purpose,key_algorithm,secret_encrypted,fingerprint FROM managed_keys WHERE id=? AND secret_encrypted IS NOT NULL").bind(id)
}

pub(in super::super) fn hash_candidates(
    limit: i64,
) -> Rows<'static, (crate::model::Id, Uuid, i64)> {
    sqlx::query_as("SELECT w.stream_id,r.storage_id,r.size FROM raw_processing w JOIN streams s ON s.id=w.stream_id JOIN raw_sources r ON r.id=w.source_id JOIN storages st ON st.id=r.storage_id WHERE EXISTS(SELECT 1 FROM mokyu_meta WHERE id=1 AND remote_work AND NOT maintenance) AND NOT EXISTS(SELECT 1 FROM maintenance_controls WHERE policy='upload' AND paused) AND st.enabled AND st.type='s3' AND r.medium='shared_s3' AND r.state='ready' AND s.state<>'writing' AND NOT EXISTS(SELECT 1 FROM raw_uploads u WHERE u.source_id=r.id) AND w.next_attempt_at<=CAST(unixepoch('subsec')*1000000 AS INTEGER) AND (w.token IS NULL OR w.lease_expires_at IS NULL OR w.lease_expires_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)) AND NOT EXISTS(SELECT 1 FROM tasks t WHERE t.id=w.token AND t.state IN ('queued','running','paused','failed')) ORDER BY w.next_attempt_at,w.stream_id LIMIT ?").bind(limit)
}
pub(in super::super) fn rehash(stream: crate::model::Id, task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE raw_processing SET token=NULL,lease_expires_at=NULL,next_attempt_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)+5000000,last_error='backend_io_failed',attempts=attempts+1 WHERE stream_id=? AND token=?").bind(stream).bind(task)
}

pub(in super::super) fn return_hashes() -> Statement<'static> {
    sqlx::query(
        "UPDATE raw_processing SET token=NULL,lease_expires_at=NULL WHERE EXISTS(SELECT 1 FROM tasks t WHERE t.id=raw_processing.token AND t.task_type='upload' AND json_extract(t.detail,'$.operation')='deduplicate' AND t.state='queued') AND NOT EXISTS(SELECT 1 FROM mokyu_meta WHERE remote_work)",
    )
}
pub(in super::super) fn retire_hashes() -> Statement<'static> {
    sqlx::query(
        "UPDATE tasks AS t SET state='completed',execution_token=NULL WHERE t.task_type='upload' AND json_extract(t.detail,'$.operation')='deduplicate' AND t.state='queued' AND NOT EXISTS(SELECT 1 FROM mokyu_meta WHERE remote_work)",
    )
}

pub(in super::super) fn progress(
    task: crate::model::task::Execution,
    detail: &serde_json::Value,
) -> Statement<'_> {
    sqlx::query("UPDATE tasks SET detail=? WHERE id=? AND execution_token=? AND state='running'")
        .bind(Json(detail))
        .bind(task.id)
        .bind(task.token)
}

pub(in super::super) fn rebind_candidate(
    stream: crate::model::Id,
    task: crate::model::task::Execution,
) -> Statement<'static> {
    sqlx::query("UPDATE streams SET owner_execution_token=? WHERE id=? AND state='writing' AND owner_task_id=?").bind(task.token).bind(stream).bind(task.id)
}
pub(in super::super) fn allow_candidate(id: crate::model::Id, task: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM streams WHERE id=? AND state='writing' AND owner_task_id=?)",
    )
    .bind(id)
    .bind(task)
}

pub(in super::super) fn part_owner(id: crate::model::Id) -> Rows<'static, (Uuid, i32)> {
    sqlx::query_as("SELECT upload_id,part_number FROM parts WHERE stream_id=?").bind(id)
}
pub(in super::super) fn discard_write(node: Uuid, operation: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE node_writes SET phase='aborted' WHERE node_id=? AND operation_id=? AND phase='active'").bind(node).bind(operation)
}

pub(in super::super) async fn begin_media(
    backend: &super::super::Backend,
    output: bool,
) -> anyhow::Result<super::node_registry::Tx<'_>> {
    if !output || backend.database_info.synchronous.as_deref() == Some("full") {
        return super::node_registry::begin(backend).await;
    }
    super::node_registry::epoch(backend)?;
    let mut connection = backend.write.acquire().await?;
    connection.close_on_drop();
    super::full_synchronous().execute(&mut *connection).await?;
    let mut tx = sqlx::Transaction::begin(
        connection,
        Some(sqlx::SqlSafeStr::into_sql_str(super::BEGIN_WRITE)),
    )
    .await?;
    backend.fence_control(&mut tx).await?;
    Ok(tx)
}

pub(in super::super) fn lane_count(storage: Uuid, direction: &str) -> Scalar<'_, i64> {
    sqlx::query_scalar("SELECT count(*) FROM storage_permits WHERE storage_id=? AND direction=? AND expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)").bind(storage).bind(direction)
}
pub(in super::super) fn lane(storage: Uuid, direction: &str, work: Uuid) -> Statement<'_> {
    sqlx::query("INSERT INTO storage_permits(token,storage_id,direction,holder,expires_at) VALUES(?,?,?,?,CAST(unixepoch('subsec')*1000000 AS INTEGER)+90000000)").bind(Uuid::new_v4()).bind(storage).bind(direction).bind(work)
}
pub(in super::super) fn align_lanes(node: Uuid, work: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE storage_permits SET expires_at=(SELECT expires_at FROM node_work WHERE node_id=? AND token=? AND task_id IS NOT NULL LIMIT 1) WHERE holder=?").bind(node).bind(work).bind(work)
}
pub(in super::super) fn drop_lanes(work: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM storage_permits WHERE holder=?").bind(work)
}
pub(in super::super) fn drop_node_lanes(node: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM storage_permits WHERE holder IN (SELECT token FROM node_work WHERE node_id=? AND task_id IS NOT NULL AND result_hash IS NULL)").bind(node)
}
