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

pub(in super::super) fn next_task(class: u32) -> Rows<'static, (Uuid, serde_json::Value)> {
    sqlx::query_as("SELECT t.id,t.detail FROM tasks t WHERE t.state='queued' AND ((?=0 AND t.task_type='integrity' AND JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.operation'))='external_test') OR (?=2 AND t.task_type='storage_migration' AND JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.operation'))='tiering')) AND EXISTS(SELECT 1 FROM mokyu_meta WHERE id=1 AND remote_work AND NOT maintenance) AND NOT EXISTS(SELECT 1 FROM maintenance_controls WHERE policy=t.task_type AND paused) ORDER BY t.updated_at,t.id LIMIT 1 FOR UPDATE SKIP LOCKED").bind(class as i64).bind(class as i64)
}

pub(in super::super) fn settle(node: Uuid, operation: Uuid, hash: &[u8]) -> Statement<'_> {
    sqlx::query("UPDATE node_work SET result_hash=?,materials_encrypted=NULL WHERE node_id=? AND operation_id=? AND result_hash IS NULL").bind(hash).bind(node).bind(operation)
}

pub(in super::super) fn expire_node(node: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE node_work SET expires_at=UTC_TIMESTAMP(6) WHERE node_id=? AND result_hash IS NULL AND expires_at>UTC_TIMESTAMP(6)").bind(node)
}

pub(in super::super) fn requeue() -> Statement<'static> {
    sqlx::query(
        "UPDATE tasks SET state='queued',execution_token=NULL,updated_at=UTC_TIMESTAMP(6) WHERE state='running' AND EXISTS(SELECT 1 FROM node_work w WHERE w.task_id=tasks.id AND w.token=tasks.execution_token AND w.result_hash IS NULL AND w.expires_at<=UTC_TIMESTAMP(6))",
    )
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
