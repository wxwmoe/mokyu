use super::{Rows, Scalar, Statement};
use crate::model::node::HandoffRow;
use uuid::Uuid;

pub(in super::super) fn role(id: Uuid) -> Rows<'static, (String, bool)> {
    sqlx::query_as("SELECT role,enabled FROM nodes WHERE id=$1 FOR UPDATE").bind(id)
}
pub(in super::super) fn row(id: Uuid) -> Rows<'static, HandoffRow> {
    sqlx::query_as("SELECT operation,revision,state FROM node_handoffs WHERE node_id=$1").bind(id)
}
pub(in super::super) fn gate(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM node_handoffs WHERE node_id=$1 AND state<>'serving')",
    )
    .bind(id)
}

pub(in super::super) async fn counts(
    tx: &mut super::storage_cluster::Tx<'_>,
    id: Uuid,
) -> anyhow::Result<crate::model::node::NodeDrain> {
    let (_, chunks, raw, parts, writes, reads) = super::retirement(id).fetch_one(&mut **tx).await?;
    let (tails, tasks) = blockers(id).fetch_one(&mut **tx).await?;
    Ok(crate::model::node::NodeDrain {
        chunks,
        raw,
        parts,
        writes,
        reads,
        tails,
        tasks,
    })
}
pub(in super::super) fn details(id: Uuid) -> Rows<'static, (i64, i64)> {
    sqlx::query_as("SELECT CAST((SELECT COALESCE(sum(staged_size),0) FROM pending_uploads WHERE node_id=$1 AND staging_storage_id IS NULL)+(SELECT COALESCE(sum(r.size),0) FROM raw_uploads a JOIN raw_sources r ON r.id=a.source_id WHERE a.node_id=$1 AND r.pending_upload AND r.medium IN ('local','memory'))+(SELECT COALESCE(sum(s.size),0) FROM parts p JOIN streams s ON s.id=p.stream_id JOIN uploads u ON u.id=p.upload_id WHERE p.remote_node_id=$1 AND p.remote_etag IS NULL AND u.state IN ('active','completing'))+(SELECT COALESCE(sum(f.size),0) FROM multipart_tails t JOIN uploads u ON u.id=t.upload_id JOIN fragments f ON f.id=t.fragment_id WHERE t.node_id=$1 AND u.state IN ('active','completing') AND EXISTS(SELECT 1 FROM extents e JOIN parts p ON p.stream_id=e.stream_id WHERE p.upload_id=t.upload_id AND e.fragment_id=t.fragment_id)) AS BIGINT),CAST((SELECT count(*) FROM multipart_tails t JOIN uploads u ON u.id=t.upload_id WHERE t.node_id=$1 AND u.state IN ('active','completing') AND EXISTS(SELECT 1 FROM extents e JOIN parts p ON p.stream_id=e.stream_id WHERE p.upload_id=t.upload_id AND e.fragment_id=t.fragment_id) AND t.source_error IS NOT NULL) AS BIGINT)").bind(id)
}
pub(in super::super) fn insert(id: Uuid, operation: Uuid, state: &str) -> Statement<'_> {
    sqlx::query("INSERT INTO node_handoffs(node_id,operation,revision,state) VALUES($1,$2,1,$3)")
        .bind(id)
        .bind(operation)
        .bind(state)
}
pub(in super::super) fn update<'a>(
    id: Uuid,
    revision: i64,
    operation: Uuid,
    state: &'a str,
) -> Statement<'a> {
    sqlx::query("UPDATE node_handoffs SET operation=$1,revision=revision+1,state=$2 WHERE node_id=$3 AND revision=$4").bind(operation).bind(state).bind(id).bind(revision)
}
pub(in super::super) fn advance(id: Uuid, revision: i64, state: &str) -> Statement<'_> {
    sqlx::query(
        "UPDATE node_handoffs SET state=$1,revision=revision+1 WHERE node_id=$2 AND revision=$3",
    )
    .bind(state)
    .bind(id)
    .bind(revision)
}
pub(in super::super) fn tails(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE multipart_tails SET requested=TRUE WHERE node_id=$1 AND NOT requested")
        .bind(id)
}
pub(in super::super) fn blockers(id: Uuid) -> Rows<'static, (i64, i64)> {
    sqlx::query_as("SELECT (SELECT count(*) FROM multipart_tails t JOIN uploads u ON u.id=t.upload_id WHERE t.node_id=$1 AND u.state IN ('active','completing') AND EXISTS(SELECT 1 FROM extents e JOIN parts p ON p.stream_id=e.stream_id WHERE p.upload_id=t.upload_id AND e.fragment_id=t.fragment_id)),(SELECT count(*) FROM tasks t WHERE (t.execution_scope='node' AND t.target_node_id=$1 AND t.state IN ('queued','running','paused','failed')) OR (t.state='running' AND EXISTS(SELECT 1 FROM node_work w WHERE w.node_id=$1 AND w.task_id=t.id AND w.result_hash IS NULL AND w.expires_at>clock_timestamp())))").bind(id)
}
