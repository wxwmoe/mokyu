use super::*;
pub(in super::super) fn task_progress(
    task: Execution,
    cursor: &str,
    processed: i64,
    detail: Value,
) -> Statement<'_> {
    sqlx::query("UPDATE tasks SET progress_cursor=?,processed=processed+?,detail=?,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND execution_token=? AND state='running'").bind(cursor).bind(processed).bind(detail).bind(task.id).bind(task.token)
}
pub(in super::super) fn physical_indexed(location: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM chunk_encodings u WHERE u.location_id=? AND NOT EXISTS(SELECT 1 FROM tier_locations h WHERE h.unit=0 AND h.physical_id=u.location_id)) OR EXISTS(SELECT 1 FROM packs u WHERE u.location_id=? AND NOT EXISTS(SELECT 1 FROM tier_locations h WHERE h.unit=1 AND h.physical_id=u.location_id)) OR EXISTS(SELECT 1 FROM tier_locations WHERE address=?) OR EXISTS(SELECT 1 FROM tier_moves WHERE source_address=?) OR EXISTS(SELECT 1 FROM tier_moves WHERE target_address=?)").bind(location).bind(location).bind(location).bind(location).bind(location)
}
pub(in super::super) fn sweep_drained(task: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM node_reads WHERE expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)) AND NOT EXISTS(SELECT 1 FROM node_writes WHERE phase='preparing' OR phase='active' AND expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)) AND NOT EXISTS(SELECT 1 FROM node_native_work WHERE result_hash IS NULL AND expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)) AND NOT EXISTS(SELECT 1 FROM node_work WHERE task_id<>? AND plan IS NOT NULL AND result_hash IS NULL AND expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)) AND NOT EXISTS(SELECT 1 FROM storage_permits p WHERE p.expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER) AND NOT EXISTS(SELECT 1 FROM node_work w WHERE w.token=p.holder AND w.task_id=?))").bind(task).bind(task)
}
