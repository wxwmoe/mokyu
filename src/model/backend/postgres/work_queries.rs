use super::*;
use crate::model::{node_work as work, session::Run};

#[derive(sqlx::FromRow)]
pub(crate) struct Batch {
    pub credential_generation: i64,
    pub boot_id: Uuid,
    pub fence: Uuid,
    pub request_hash: Vec<u8>,
    pub task_id: Option<Uuid>,
    pub token: Uuid,
    pub maintenance_token: Uuid,
    pub plan: Option<sqlx::types::Json<work::Plan>>,
    pub storage_encrypted: Option<Vec<u8>>,
    pub valid_for_ms: i64,
    pub result_hash: Option<Vec<u8>>,
}

pub(crate) fn batch(node: Uuid, operation: Uuid) -> Rows<'static, Batch> {
    sqlx::query_as("SELECT credential_generation,boot_id,fence,request_hash,task_id,token,maintenance_token,plan,storage_encrypted,GREATEST(0,(extract(epoch FROM(expires_at-clock_timestamp()))*1000)::bigint) AS valid_for_ms,result_hash FROM node_work WHERE node_id=$1 AND operation_id=$2 FOR UPDATE")
        .bind(node).bind(operation)
}

pub(crate) fn insert<'a>(
    node: &'a NodeState,
    input: &'a work::Exchange,
    hash: &'a [u8],
    task: Option<Uuid>,
    token: Uuid,
    maintenance: Uuid,
    plan: Option<&'a work::Plan>,
    storage: Option<&'a [u8]>,
) -> Statement<'a> {
    sqlx::query("INSERT INTO node_work(node_id,operation_id,credential_generation,boot_id,fence,request_hash,task_id,token,maintenance_token,plan,storage_encrypted,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,clock_timestamp()+interval '60 seconds')")
        .bind(node.node_id).bind(input.operation).bind(node.credential_generation).bind(input.run.boot).bind(input.run.fence).bind(hash).bind(task).bind(token).bind(maintenance).bind(plan.map(sqlx::types::Json)).bind(storage)
}

pub(crate) fn cleanup(node: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM node_work WHERE node_id=$1 AND operation_id IN (SELECT operation_id FROM node_work WHERE node_id=$1 AND expires_at<clock_timestamp()-interval '24 hours' ORDER BY expires_at LIMIT 128)").bind(node)
}

pub(crate) fn active(node: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT count(*) FROM node_work WHERE node_id=$1 AND plan IS NOT NULL AND result_hash IS NULL AND expires_at>clock_timestamp()").bind(node)
}

pub(crate) fn task(
    id: Uuid,
    token: Uuid,
    node: Uuid,
) -> Rows<'static, (String, Value, Option<Uuid>)> {
    sqlx::query_as("SELECT task_type,detail,parent_task_id FROM tasks WHERE id=$1 AND lease_token=$2 AND lease_node_id=$3 AND state='running' AND lease_expires_at>clock_timestamp() FOR UPDATE").bind(id).bind(token).bind(node)
}

pub(crate) fn task_lease(id: Uuid, token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET lease_expires_at=clock_timestamp()+interval '60 seconds' WHERE id=$1 AND lease_token=$2 AND state='running'").bind(id).bind(token)
}

pub(crate) fn metadata_progress(id: Uuid, token: Uuid, count: i64) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET processed=processed+$3 WHERE id=$1 AND lease_token=$2 AND state='running'").bind(id).bind(token).bind(count)
}

pub(crate) fn result<'a>(node: Uuid, operation: Uuid, hash: &'a [u8]) -> Statement<'a> {
    sqlx::query("UPDATE node_work SET result_hash=$3,expires_at=clock_timestamp() WHERE node_id=$1 AND operation_id=$2 AND result_hash IS NULL").bind(node).bind(operation).bind(hash)
}

pub(crate) fn finish(
    task: Uuid,
    token: Uuid,
    count: i64,
    complete: bool,
    failed: bool,
) -> Statement<'static> {
    sqlx::query("WITH changed AS (UPDATE tasks SET processed=processed+$3,state=CASE WHEN $5 THEN 'failed' WHEN $4 THEN 'completed' ELSE 'queued' END,error=CASE WHEN $5 THEN 'backend_io_failed' ELSE NULL END,lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=clock_timestamp() WHERE id=$1 AND lease_token=$2 AND state='running' RETURNING parent_task_id) UPDATE tasks SET state='failed',error='backend_io_failed',updated_at=clock_timestamp() WHERE $5 AND id=(SELECT parent_task_id FROM changed) AND state IN ('queued','running')").bind(task).bind(token).bind(count).bind(complete).bind(failed)
}

pub(crate) fn physical_active(location: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM node_work w,jsonb_array_elements(w.plan->'items') i WHERE w.result_hash IS NULL AND w.expires_at>clock_timestamp() AND i->>'location'=$1::uuid::text)").bind(location)
}

pub(crate) fn live_permits(token: Uuid, maintenance: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT count(*)=2 FROM storage_permits WHERE token IN ($1,$2) AND expires_at>clock_timestamp()").bind(token).bind(maintenance)
}

pub(crate) fn renew<'a>(
    node: &'a NodeState,
    run: Run,
    ids: &'a [Uuid],
    release: bool,
) -> Rows<'a, (Uuid, i64)> {
    sqlx::query_as("WITH owned AS (UPDATE node_work w SET expires_at=clock_timestamp()+CASE WHEN $6 THEN interval '0 seconds' ELSE interval '60 seconds' END WHERE node_id=$1 AND credential_generation=$2 AND boot_id=$3 AND fence=$4 AND token=ANY($5) AND expires_at>clock_timestamp() AND result_hash IS NULL AND EXISTS(SELECT 1 FROM tasks t WHERE t.id=w.task_id AND t.state='running' AND t.lease_token=w.token AND t.lease_node_id=$1 AND t.lease_expires_at>clock_timestamp()) AND (SELECT count(*) FROM storage_permits p WHERE p.token IN (w.token,w.maintenance_token) AND p.expires_at>clock_timestamp())=2 RETURNING token,maintenance_token,task_id,expires_at), permits AS (UPDATE storage_permits p SET expires_at=o.expires_at FROM owned o WHERE p.token IN (o.token,o.maintenance_token)), tasks AS (UPDATE tasks t SET lease_expires_at=o.expires_at FROM owned o WHERE t.id=o.task_id AND t.lease_token=o.token) SELECT token,CASE WHEN $6 THEN 0 ELSE 60000 END::bigint FROM owned")
        .bind(node.node_id).bind(node.credential_generation).bind(run.boot).bind(run.fence).bind(ids).bind(release)
}

pub(crate) fn pending(node: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks t CROSS JOIN mokyu_meta m WHERE (t.task_type='gc' AND NOT m.maintenance AND NOT m.gc_paused OR t.task_type='cleanup' AND t.detail->>'storage_id' IS NOT NULL AND NOT EXISTS(SELECT 1 FROM maintenance_controls c WHERE c.policy='cleanup' AND c.paused)) AND (t.state='queued' OR (t.state='running' AND t.lease_expires_at<clock_timestamp())) AND t.detail->>'role' IS DISTINCT FROM 'parent' AND (t.execution_scope<>'node' OR t.target_node_id IS NULL OR t.target_node_id=$1))").bind(node)
}

pub(crate) fn upload(source: Id) -> Scalar<'static, Option<String>> {
    sqlx::query_scalar("SELECT upload_id FROM raw_sources WHERE id=$1").bind(source)
}

pub(crate) fn staging(storage: Uuid, maximum: i64) -> Rows<'static, (String,)> {
    sqlx::query_as("SELECT s.object_key FROM staging_objects s WHERE s.storage_id=$1 AND s.created_at<clock_timestamp()-interval '5 minutes' AND NOT EXISTS(SELECT 1 FROM pending_uploads p WHERE p.staging_storage_id=s.storage_id AND p.staging_object_key=s.object_key) AND NOT EXISTS(SELECT 1 FROM fragments f WHERE f.staging_storage_id=s.storage_id AND f.staging_object_key=s.object_key) AND NOT EXISTS(SELECT 1 FROM uploads u WHERE u.staging_storage_id=s.storage_id AND u.staging_object_key=s.object_key) ORDER BY s.created_at,s.object_key LIMIT $2 FOR UPDATE OF s SKIP LOCKED").bind(storage).bind(maximum)
}

pub(crate) fn remove_staging(storage: Uuid, key: &str) -> Statement<'_> {
    sqlx::query("DELETE FROM staging_objects s WHERE s.storage_id=$1 AND s.object_key=$2 AND NOT EXISTS(SELECT 1 FROM pending_uploads p WHERE p.staging_storage_id=s.storage_id AND p.staging_object_key=s.object_key) AND NOT EXISTS(SELECT 1 FROM fragments f WHERE f.staging_storage_id=s.storage_id AND f.staging_object_key=s.object_key) AND NOT EXISTS(SELECT 1 FROM uploads u WHERE u.staging_storage_id=s.storage_id AND u.staging_object_key=s.object_key)").bind(storage).bind(key)
}
