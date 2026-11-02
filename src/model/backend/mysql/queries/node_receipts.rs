use super::{Rows, Statement};
use crate::cluster::NodeState;
use serde_json::Value;
use uuid::Uuid;
pub(in super::super) fn stored(
    node: Uuid,
    op: Uuid,
) -> Rows<'static, (String, i64, Vec<u8>, Value)> {
    sqlx::query_as("SELECT action,credential_generation,request_hash,result FROM node_receipts WHERE node_id=? AND operation_id=?").bind(node).bind(op)
}
pub(in super::super) fn save<'a>(
    node: &'a NodeState,
    op: Uuid,
    action: &'a str,
    hash: &'a [u8],
    result: Value,
) -> Statement<'a> {
    sqlx::query("INSERT INTO node_receipts(node_id,operation_id,action,credential_generation,request_hash,result) VALUES(?,?,?,?,?,?)").bind(node.node_id).bind(op).bind(action).bind(node.credential_generation).bind(hash).bind(result)
}
pub(in super::super) fn prune() -> Statement<'static> {
    sqlx::query(
        "DELETE FROM node_receipts WHERE created_at<UTC_TIMESTAMP(6)-INTERVAL 1 DAY LIMIT 128",
    )
}
