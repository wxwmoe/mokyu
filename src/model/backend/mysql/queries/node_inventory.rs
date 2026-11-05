use super::super::node_inventory::Observation;
use super::*;
use crate::model::metrics::{ClusterRuntimePoint, NodeSample, WorkSummary};

pub(in super::super) fn pool(b: &Backend) -> &sqlx::MySqlPool {
    &b.pool
}
pub(in super::super) fn page(after: Option<Uuid>, limit: i64) -> Rows<'static, Observation> {
    sqlx::query_as("SELECT id,name,role,enabled,CAST(UNIX_TIMESTAMP(last_seen_at)*1000000 AS SIGNED) AS last_seen,CAST(UNIX_TIMESTAMP(created_at)*1000000 AS SIGNED) AS created,CAST(UNIX_TIMESTAMP(reported_at)*1000000 AS SIGNED) AS received,report,run_state FROM nodes WHERE (? IS NULL OR id>?) ORDER BY id LIMIT ?").bind(after).bind(after).bind(limit)
}
pub(in super::super) fn node(id: Uuid) -> Rows<'static, Observation> {
    sqlx::query_as("SELECT id,name,role,enabled,CAST(UNIX_TIMESTAMP(last_seen_at)*1000000 AS SIGNED) AS last_seen,CAST(UNIX_TIMESTAMP(created_at)*1000000 AS SIGNED) AS created,CAST(UNIX_TIMESTAMP(reported_at)*1000000 AS SIGNED) AS received,report,run_state FROM nodes WHERE id=?").bind(id)
}
pub(in super::super) fn status() -> Rows<'static, (String, i64, bool, i64)> {
    sqlx::query_as(
        "SELECT deployment_mode,deployment_mode_revision,remote_work,(SELECT CAST(count(*) AS SIGNED) FROM storages WHERE type='local') FROM mokyu_meta WHERE id=1",
    )
}
pub(in super::super) fn primary<'a>(id: Uuid, report: &'a Value) -> Statement<'a> {
    sqlx::query("UPDATE nodes SET report=?,reported_at=UTC_TIMESTAMP(6),last_seen_at=UTC_TIMESTAMP(6) WHERE id=? AND role='control' AND enabled").bind(sqlx::types::Json(report)).bind(id)
}
pub(in super::super) fn tasks(id: Uuid) -> Rows<'static, WorkSummary> {
    sqlx::query_as("SELECT id,task_type,state,CAST(processed AS CHAR) AS processed,updated_at FROM tasks WHERE source_node=? AND state='running' UNION ALL SELECT t.id,t.task_type,t.state,CAST(processed AS CHAR),t.updated_at FROM tasks t JOIN node_work w ON w.task_id=t.id AND w.token=t.execution_token WHERE w.node_id=? AND (t.source_node IS NULL OR t.source_node<>?) AND t.state='running' ORDER BY updated_at DESC,id LIMIT 21").bind(id).bind(id).bind(id)
}
pub(in super::super) fn last_sample(id: Uuid) -> Scalar<'static, Option<i64>> {
    sqlx::query_scalar("SELECT CAST(UNIX_TIMESTAMP(MAX(received_at))*1000000 AS SIGNED) FROM node_runtime_samples WHERE node_id=?").bind(id)
}
pub(in super::super) fn sample<'a>(id: Uuid, sample: &'a NodeSample) -> Statement<'a> {
    sqlx::query(
        "INSERT INTO node_runtime_samples(node_id,received_at,sample) VALUES(?,UTC_TIMESTAMP(6),?)",
    )
    .bind(id)
    .bind(sqlx::types::Json(sample))
}
pub(in super::super) fn last_aggregate() -> Scalar<'static, Option<i64>> {
    sqlx::query_scalar(
        "SELECT CAST(UNIX_TIMESTAMP(MAX(captured_at))*1000000 AS SIGNED) FROM cluster_runtime_samples",
    )
}
pub(in super::super) fn aggregate(point: &ClusterRuntimePoint) -> Statement<'_> {
    sqlx::query("INSERT INTO cluster_runtime_samples(captured_at,sample) VALUES(TIMESTAMPADD(MICROSECOND,?,TIMESTAMP\'1970-01-01 00:00:00\'),?)").bind(point.at.timestamp_micros()).bind(sqlx::types::Json(point))
}
pub(in super::super) fn node_samples(
    id: Uuid,
    since: i64,
    limit: i64,
) -> Rows<'static, (Uuid, i64, sqlx::types::Json<NodeSample>)> {
    sqlx::query_as("SELECT node_id,CAST(UNIX_TIMESTAMP(received_at)*1000000 AS SIGNED),sample FROM node_runtime_samples WHERE node_id=? AND received_at>=TIMESTAMPADD(MICROSECOND,?,TIMESTAMP\'1970-01-01 00:00:00\') ORDER BY received_at DESC LIMIT ?").bind(id).bind(since).bind(limit)
}
pub(in super::super) fn history(
    since: i64,
    limit: i64,
) -> Scalar<'static, sqlx::types::Json<ClusterRuntimePoint>> {
    sqlx::query_scalar("SELECT sample FROM cluster_runtime_samples WHERE captured_at>=TIMESTAMPADD(MICROSECOND,?,TIMESTAMP\'1970-01-01 00:00:00\') ORDER BY captured_at DESC LIMIT ?").bind(since).bind(limit)
}
pub(in super::super) fn enabled() -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT CAST(count(*) AS SIGNED) FROM nodes WHERE enabled")
}
pub(in super::super) fn window(
    since: i64,
) -> Rows<'static, (Uuid, i64, sqlx::types::Json<NodeSample>)> {
    sqlx::query_as(
        "SELECT s.node_id, CAST(UNIX_TIMESTAMP(s.received_at)*1000000 AS SIGNED), s.sample
         FROM nodes n JOIN node_runtime_samples s ON s.node_id=n.id
         WHERE n.enabled AND s.received_at>=TIMESTAMPADD(MICROSECOND,?,TIMESTAMP'1970-01-01 00:00:00')
           AND s.received_at>=COALESCE((
               SELECT r.received_at FROM node_runtime_samples r WHERE r.node_id=n.id
               ORDER BY r.received_at DESC LIMIT 1 OFFSET 1
           ),s.received_at)
         ORDER BY s.node_id,s.received_at DESC",
    )
    .bind(since)
}
pub(in super::super) fn prune_nodes(before: i64) -> Statement<'static> {
    sqlx::query("DELETE FROM node_runtime_samples WHERE received_at<TIMESTAMPADD(MICROSECOND,?,TIMESTAMP\'1970-01-01 00:00:00\') ORDER BY received_at LIMIT 512").bind(before)
}
pub(in super::super) fn prune_totals(before: i64) -> Statement<'static> {
    sqlx::query("DELETE FROM cluster_runtime_samples WHERE captured_at<TIMESTAMPADD(MICROSECOND,?,TIMESTAMP\'1970-01-01 00:00:00\') ORDER BY captured_at LIMIT 512").bind(before)
}
