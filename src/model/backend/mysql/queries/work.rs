use super::super::node_work::Offer;
use super::{Rows, Scalar, Statement};
use crate::model::{
    node_work::{Exchange, Plan},
    session::Run,
};
use sqlx::types::Json;
use uuid::Uuid;

pub(in super::super) fn offer(node: Uuid, operation: Uuid) -> Rows<'static, Offer> {
    sqlx::query_as("SELECT boot_id,fence,request_hash,task_id,token,plan,materials_encrypted,result_hash,CAST(GREATEST(0,TIMESTAMPDIFF(MICROSECOND,UTC_TIMESTAMP(6),expires_at) DIV 1000) AS SIGNED) AS remaining FROM node_work WHERE node_id=? AND operation_id=? FOR UPDATE").bind(node).bind(operation)
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
    sqlx::query("INSERT INTO node_work(node_id,operation_id,boot_id,fence,request_hash,task_id,token,plan,materials_encrypted,expires_at) VALUES(?,?,?,?,?,?,?,?,?,UTC_TIMESTAMP(6)+INTERVAL 90 SECOND)").bind(node).bind(input.operation).bind(input.run.boot).bind(input.run.fence).bind(hash).bind(task).bind(token).bind(plan.map(Json)).bind(secret)
}

pub(in super::super) fn next_task(
    class: u32,
    node: Uuid,
    worker: bool,
) -> Rows<'static, (Uuid, String, serde_json::Value)> {
    sqlx::query_as("SELECT t.id,t.task_type,t.detail FROM tasks t WHERE (t.task_type='sweep' OR (NOT EXISTS(SELECT 1 FROM tasks sw WHERE sw.task_type='sweep' AND COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(sw.detail,'$.dry_run')) AS BINARY)='false',FALSE) AND sw.state IN ('queued','running')) AND NOT EXISTS(SELECT 1 FROM node_work w WHERE w.result_hash IS NULL AND w.expires_at>UTC_TIMESTAMP(6) AND CAST(JSON_UNQUOTE(JSON_EXTRACT(w.plan,'$.action')) AS BINARY)='sweep_delete'))) AND (t.state='queued' OR t.state='paused' AND EXISTS(SELECT 1 FROM redundancy_links l WHERE l.id=t.id AND l.state='idle' AND (l.next_reconcile_at<=UTC_TIMESTAMP(6) OR EXISTS(SELECT 1 FROM transfer_changes c WHERE c.task_id=l.id))) OR t.state='failed' AND t.updated_at<TIMESTAMPADD(SECOND,-30,UTC_TIMESTAMP(6)) AND EXISTS(SELECT 1 FROM redundancy_links l WHERE l.id=t.id AND l.state<>'paused')) AND (t.error IS NULL OR t.error NOT IN ('backend_io_failed','source_pending') OR t.updated_at<=UTC_TIMESTAMP(6)-INTERVAL 5 SECOND) AND ((t.source_node=? AND t.task_type='upload' AND ?=2 AND (CAST(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.operation')) AS BINARY)<>'cdc' OR NOT EXISTS(SELECT 1 FROM raw_sources r WHERE r.processing_task=t.id AND (r.owner_stream IS NOT NULL OR EXISTS(SELECT 1 FROM raw_ranges e JOIN streams s ON s.id=e.stream_id WHERE e.source_id=r.id AND s.state='writing')))) AND NOT EXISTS(SELECT 1 FROM raw_uploads u WHERE u.source_id=CAST(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.source_id')) AS SIGNED) AND u.next_attempt_at>UTC_TIMESTAMP(6)) AND EXISTS(SELECT 1 FROM mokyu_meta WHERE id=1 AND NOT maintenance)) OR (t.source_node IS NULL AND ? AND ((?=0 AND (t.task_type='sweep' OR (t.task_type IN ('gc','cleanup') AND JSON_EXTRACT(t.detail,'$.storage_id') IS NOT NULL AND (t.task_type<>'gc' OR NOT (SELECT gc_paused FROM mokyu_meta WHERE id=1))))) OR (?=1 AND t.task_type IN ('pack','unpack','rewrite')) OR (?=0 AND t.task_type='integrity' AND (CAST(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.operation')) AS BINARY)='external_test' AND (NULLIF(CAST(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.source_node')) AS BINARY),'null') IS NULL OR CAST(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.source_node')) AS BINARY)=?) OR JSON_EXTRACT(t.detail,'$.source_operation') IS NOT NULL OR (CAST(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.mode')) AS BINARY) IN ('head','full') AND CAST(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.phase')) AS BINARY) IN ('raw','chunks')))) OR (?=2 AND (t.task_type='storage_migration' AND ((CAST(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.operation')) AS BINARY) IN ('tiering','replication','mirroring') AND (NOT (CAST(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.operation')) AS BINARY) <=> 'replication') OR COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.external')) AS BINARY)='true',FALSE) OR (CAST(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.phase')) AS BINARY) <=> 'verify'))) OR JSON_EXTRACT(t.detail,'$.job') IS NOT NULL) OR t.task_type='upload' AND CAST(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.operation')) AS BINARY)='deduplicate' OR t.task_type='upload' AND CAST(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.operation')) AS BINARY)='cdc' AND EXISTS(SELECT 1 FROM raw_sources r WHERE r.processing_task=t.id AND r.medium='shared_s3' AND NOT r.pending_upload AND r.owner_stream IS NULL AND NOT EXISTS(SELECT 1 FROM raw_ranges e JOIN streams s ON s.id=e.stream_id WHERE e.source_id=r.id AND s.state='writing'))))) AND EXISTS(SELECT 1 FROM mokyu_meta WHERE id=1 AND remote_work AND (NOT maintenance OR t.task_type='sweep')))) AND NOT EXISTS(SELECT 1 FROM maintenance_controls WHERE policy=t.task_type AND paused) ORDER BY t.updated_at,t.id LIMIT 1 FOR UPDATE SKIP LOCKED").bind(node).bind(class as i64).bind(worker).bind(class as i64).bind(class as i64).bind(class as i64).bind(node.to_string()).bind(class as i64)
}

pub(in super::super) fn settle(node: Uuid, operation: Uuid, hash: &[u8]) -> Statement<'_> {
    sqlx::query("UPDATE node_work SET result_hash=?,materials_encrypted=NULL WHERE node_id=? AND operation_id=? AND result_hash IS NULL").bind(hash).bind(node).bind(operation)
}

pub(in super::super) fn expire_node(node: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE node_work SET expires_at=UTC_TIMESTAMP(6) WHERE node_id=? AND result_hash IS NULL AND expires_at>UTC_TIMESTAMP(6)").bind(node)
}

pub(in super::super) async fn requeue(
    tx: &mut super::node_registry::Tx<'_>,
) -> anyhow::Result<u64> {
    // A plain read of running tasks avoids locking their state index during completion.
    let due: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM tasks t
         STRAIGHT_JOIN node_work w FORCE INDEX(node_work_task) ON w.task_id=t.id AND w.token=t.execution_token
         WHERE t.state='running' AND w.result_hash IS NULL AND w.expires_at<=UTC_TIMESTAMP(6))",
    )
    .fetch_one(&mut **tx)
    .await?;
    if !due {
        return Ok(0);
    }
    Ok(sqlx::query(
        "UPDATE node_work w FORCE INDEX(node_work_task)
         STRAIGHT_JOIN tasks t FORCE INDEX(PRIMARY) ON t.id=w.task_id AND t.execution_token=w.token
         SET t.state='queued',t.execution_token=NULL,t.updated_at=UTC_TIMESTAMP(6)
         WHERE w.task_id IS NOT NULL AND w.result_hash IS NULL AND w.expires_at<=UTC_TIMESTAMP(6) AND t.state='running'",
    )
    .execute(&mut **tx)
    .await?
    .rows_affected())
}

pub(in super::super) fn prune() -> Statement<'static> {
    sqlx::query(
        "DELETE FROM node_work WHERE expires_at<UTC_TIMESTAMP(6)-INTERVAL 15 MINUTE AND (task_id IS NULL OR NOT EXISTS(SELECT 1 FROM tasks t WHERE t.id=node_work.task_id AND t.execution_token=node_work.token))",
    )
}

pub(in super::super) fn touch(
    node: Uuid,
    run: Run,
    token: Uuid,
    renew: bool,
) -> Statement<'static> {
    sqlx::query(if renew {
        "UPDATE node_work SET expires_at=UTC_TIMESTAMP(6)+INTERVAL 90 SECOND WHERE node_id=? AND boot_id=? AND fence=? AND token=? AND task_id IS NOT NULL AND result_hash IS NULL AND expires_at>UTC_TIMESTAMP(6)"
    } else {
        "UPDATE node_work SET expires_at=UTC_TIMESTAMP(6) WHERE node_id=? AND boot_id=? AND fence=? AND token=? AND task_id IS NOT NULL AND result_hash IS NULL AND expires_at>UTC_TIMESTAMP(6)"
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
    sqlx::query_as("SELECT w.stream_id,r.storage_id,r.size FROM raw_processing w JOIN streams s ON s.id=w.stream_id JOIN raw_sources r ON r.id=w.source_id JOIN storages st ON st.id=r.storage_id WHERE EXISTS(SELECT 1 FROM mokyu_meta WHERE id=1 AND remote_work AND NOT maintenance) AND NOT EXISTS(SELECT 1 FROM maintenance_controls WHERE policy='upload' AND paused) AND st.enabled AND st.type='s3' AND r.medium='shared_s3' AND r.state='ready' AND s.state<>'writing' AND NOT EXISTS(SELECT 1 FROM raw_uploads u WHERE u.source_id=r.id) AND w.next_attempt_at<=UTC_TIMESTAMP(6) AND (w.token IS NULL OR w.lease_expires_at IS NULL OR w.lease_expires_at<UTC_TIMESTAMP(6)) AND NOT EXISTS(SELECT 1 FROM tasks t WHERE t.id=w.token AND t.state IN ('queued','running','paused','failed')) ORDER BY w.next_attempt_at,w.stream_id LIMIT ? FOR UPDATE SKIP LOCKED").bind(limit)
}
pub(in super::super) fn rehash(stream: crate::model::Id, task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE raw_processing SET token=NULL,lease_expires_at=NULL,next_attempt_at=UTC_TIMESTAMP(6)+INTERVAL 5 SECOND,last_error='backend_io_failed',attempts=attempts+1 WHERE stream_id=? AND token=?").bind(stream).bind(task)
}

pub(in super::super) fn return_hashes() -> Statement<'static> {
    sqlx::query(
        "UPDATE raw_processing SET token=NULL,lease_expires_at=NULL WHERE EXISTS(SELECT 1 FROM tasks t WHERE t.id=raw_processing.token AND t.task_type='upload' AND CAST(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.operation')) AS BINARY)='deduplicate' AND t.state='queued') AND NOT EXISTS(SELECT 1 FROM mokyu_meta WHERE remote_work)",
    )
}
pub(in super::super) fn retire_hashes() -> Statement<'static> {
    sqlx::query(
        "UPDATE tasks AS t SET state='completed',execution_token=NULL WHERE t.task_type='upload' AND CAST(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.operation')) AS BINARY)='deduplicate' AND t.state='queued' AND NOT EXISTS(SELECT 1 FROM mokyu_meta WHERE remote_work)",
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
    _output: bool,
) -> anyhow::Result<super::node_registry::Tx<'_>> {
    super::node_registry::begin(backend).await
}

pub(in super::super) fn lane_count(storage: Uuid, direction: &str) -> Scalar<'_, i64> {
    sqlx::query_scalar("SELECT count(*) FROM storage_permits WHERE storage_id=? AND direction=? AND expires_at>UTC_TIMESTAMP(6)").bind(storage).bind(direction)
}
pub(in super::super) fn lane(storage: Uuid, direction: &str, work: Uuid) -> Statement<'_> {
    sqlx::query("INSERT INTO storage_permits(token,storage_id,direction,holder,expires_at) VALUES(?,?,?,?,UTC_TIMESTAMP(6)+INTERVAL 90 SECOND)").bind(Uuid::new_v4()).bind(storage).bind(direction).bind(work)
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

pub(in super::super) fn source_pending(stream: crate::model::Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM raw_ranges e JOIN raw_sources r ON r.id=e.source_id WHERE e.stream_id=? AND r.pending_upload AND r.quarantine_code IS NULL) OR EXISTS(SELECT 1 FROM extents e JOIN pending_uploads p ON p.chunk_id=e.chunk_id WHERE e.stream_id=?)").bind(stream).bind(stream)
}

pub(in super::super) fn writable() -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT NOT maintenance FROM mokyu_meta WHERE id=1 LOCK IN SHARE MODE")
}

pub(in super::super) fn active_location(storage: Uuid, location: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM storage_permits p JOIN node_work w ON w.token=p.holder WHERE p.storage_id=? AND p.direction='control' AND w.result_hash IS NULL AND w.expires_at>UTC_TIMESTAMP(6) AND JSON_SEARCH(w.plan,'one',?) IS NOT NULL)").bind(storage).bind(location.to_string())
}

pub(in super::super) fn retry_collect(task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET error='backend_io_failed' WHERE id=? AND state='queued' AND execution_token IS NULL").bind(task)
}
