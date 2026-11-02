use super::{Rows, Scalar, Statement};
use crate::model::Id;
use uuid::Uuid;
pub(in super::super) fn owner(id: Id) -> Scalar<'static, Option<Uuid>> {
    sqlx::query_scalar("SELECT node_id FROM raw_uploads WHERE source_id=? FOR UPDATE").bind(id)
}
pub(in super::super) fn name(id: Id, node: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE raw_uploads SET node_id=? WHERE source_id=? AND node_id IS NULL")
        .bind(node)
        .bind(id)
}
pub(in super::super) fn target(task: Uuid, node: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET source_node=? WHERE id=?")
        .bind(node)
        .bind(task)
}
pub(in super::super) fn exists(source: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM raw_uploads WHERE source_id=?)").bind(source)
}
pub(in super::super) fn snapshot(
    id: Uuid,
    node: Uuid,
) -> Rows<
    'static,
    (
        Option<Uuid>,
        i64,
        bool,
        bool,
        bool,
        Option<String>,
        Option<i64>,
    ),
> {
    sqlx::query_as("SELECT COALESCE(r.storage_id,w.storage_id),COALESCE(r.size,t.size,0),COALESCE(a.node_id=? OR (w.node_id=? AND w.phase='active' AND w.expires_at>UTC_TIMESTAMP(6)),FALSE),EXISTS(SELECT 1 FROM node_read_sources f JOIN node_read_windows v ON v.id=f.window_id JOIN node_reads q ON q.read_id=v.read_id WHERE f.location_id=? AND q.node_id=? AND q.expires_at>UTC_TIMESTAMP(6)),COALESCE(r.state='ready' AND r.medium='shared_s3' AND NOT r.pending_upload AND r.quarantine_code IS NULL AND p.cache_enabled,FALSE),p.cache_soft_limit,p.cache_entries FROM (SELECT ? AS id) i LEFT JOIN raw_sources r ON r.location_id=i.id LEFT JOIN raw_uploads a ON a.source_id=r.id LEFT JOIN node_write_raw t ON t.location_id=i.id AND t.node_id=? LEFT JOIN node_writes w ON w.node_id=t.node_id AND w.operation_id=t.operation_id LEFT JOIN storage_profiles p ON p.storage_id=COALESCE(r.storage_id,w.storage_id)").bind(node).bind(node).bind(id).bind(node).bind(id).bind(node)
}

pub(in super::super) fn pending(node: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks t WHERE t.source_node=? AND t.task_type='upload' AND t.state='queued' AND NOT EXISTS(SELECT 1 FROM raw_uploads u WHERE u.source_id=CAST(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.source_id')) AS SIGNED) AND u.next_attempt_at>UTC_TIMESTAMP(6)) AND NOT EXISTS(SELECT 1 FROM maintenance_controls WHERE policy='upload' AND paused))").bind(node)
}
