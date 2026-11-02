use super::super::node_inventory::Observation;
use super::*;
use crate::model::metrics::{ClusterRuntimePoint, NodeSample, WorkSummary};

pub(in super::super) fn pool(b: &Backend) -> &sqlx::SqlitePool {
    &b.read
}
pub(in super::super) fn page(after: Option<Uuid>, limit: i64) -> Rows<'static, Observation> {
    sqlx::query_as("SELECT id,name,role,enabled,last_seen_at AS last_seen,created_at AS created,reported_at AS received,report,run_state FROM nodes WHERE (? IS NULL OR id>?) ORDER BY id LIMIT ?").bind(after).bind(after).bind(limit)
}
pub(in super::super) fn node(id: Uuid) -> Rows<'static, Observation> {
    sqlx::query_as("SELECT id,name,role,enabled,last_seen_at AS last_seen,created_at AS created,reported_at AS received,report,run_state FROM nodes WHERE id=?").bind(id)
}
pub(in super::super) fn status() -> Rows<'static, (String, i64, bool, i64)> {
    sqlx::query_as(
        "SELECT deployment_mode,deployment_mode_revision,remote_work,(SELECT count(*) FROM storages WHERE type='local') FROM mokyu_meta WHERE id=1",
    )
}
pub(in super::super) fn primary<'a>(id: Uuid, report: &'a Value) -> Statement<'a> {
    sqlx::query("UPDATE nodes SET report=?,reported_at=CAST(unixepoch('subsec')*1000000 AS INTEGER),last_seen_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND role='control' AND enabled").bind(sqlx::types::Json(report)).bind(id)
}
pub(in super::super) fn tasks(id: Uuid) -> Rows<'static, WorkSummary> {
    sqlx::query_as("SELECT id,task_type,state,CAST(processed AS TEXT) AS processed,updated_at FROM tasks WHERE source_node=? AND state='running' UNION ALL SELECT t.id,t.task_type,t.state,CAST(processed AS TEXT),t.updated_at FROM tasks t JOIN node_work w ON w.task_id=t.id AND w.token=t.execution_token WHERE w.node_id=? AND (t.source_node IS NULL OR t.source_node<>?) AND t.state='running' ORDER BY updated_at DESC,id LIMIT 21").bind(id).bind(id).bind(id)
}
pub(in super::super) fn last_sample(id: Uuid) -> Scalar<'static, Option<i64>> {
    sqlx::query_scalar("SELECT MAX(received_at) FROM node_runtime_samples WHERE node_id=?").bind(id)
}
pub(in super::super) fn sample<'a>(id: Uuid, sample: &'a NodeSample) -> Statement<'a> {
    sqlx::query("INSERT INTO node_runtime_samples(node_id,received_at,sample) VALUES(?,CAST(unixepoch('subsec')*1000000 AS INTEGER),?)").bind(id).bind(sqlx::types::Json(sample))
}
pub(in super::super) fn last_aggregate() -> Scalar<'static, Option<i64>> {
    sqlx::query_scalar("SELECT MAX(captured_at) FROM cluster_runtime_samples")
}
pub(in super::super) fn aggregate(point: &ClusterRuntimePoint) -> Statement<'_> {
    sqlx::query("INSERT INTO cluster_runtime_samples(captured_at,sample) VALUES(?,?)")
        .bind(point.at.timestamp_micros())
        .bind(sqlx::types::Json(point))
}
pub(in super::super) fn node_samples(
    id: Uuid,
    since: i64,
    limit: i64,
) -> Rows<'static, (Uuid, i64, sqlx::types::Json<NodeSample>)> {
    sqlx::query_as("SELECT node_id,received_at,sample FROM node_runtime_samples WHERE node_id=? AND received_at>=? ORDER BY received_at DESC LIMIT ?").bind(id).bind(since).bind(limit)
}
pub(in super::super) fn history(
    since: i64,
    limit: i64,
) -> Scalar<'static, sqlx::types::Json<ClusterRuntimePoint>> {
    sqlx::query_scalar("SELECT sample FROM cluster_runtime_samples WHERE captured_at>=? ORDER BY captured_at DESC LIMIT ?").bind(since).bind(limit)
}
pub(in super::super) fn enabled() -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT count(*) FROM nodes WHERE enabled")
}
pub(in super::super) fn window(
    since: i64,
) -> Rows<'static, (Uuid, i64, sqlx::types::Json<NodeSample>)> {
    sqlx::query_as("SELECT node_id,received_at,sample FROM (SELECT s.node_id,s.received_at,s.sample,ROW_NUMBER() OVER(PARTITION BY s.node_id ORDER BY s.received_at DESC) AS rn FROM node_runtime_samples s JOIN nodes n ON n.id=s.node_id WHERE n.enabled AND s.received_at>=?) samples WHERE rn<=2 ORDER BY node_id,received_at DESC").bind(since)
}
pub(in super::super) fn prune_nodes(before: i64) -> Statement<'static> {
    sqlx::query("DELETE FROM node_runtime_samples WHERE rowid IN (SELECT rowid FROM node_runtime_samples WHERE received_at<? ORDER BY received_at LIMIT 512)").bind(before)
}
pub(in super::super) fn prune_totals(before: i64) -> Statement<'static> {
    sqlx::query("DELETE FROM cluster_runtime_samples WHERE rowid IN (SELECT rowid FROM cluster_runtime_samples WHERE captured_at<? ORDER BY captured_at LIMIT 512)").bind(before)
}
