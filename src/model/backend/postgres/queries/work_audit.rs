pub(in super::super) use super::missing_raw_pages as missing_pages;
use super::*;
use crate::model::{integrity::Cursor, task::Execution};
pub(in super::super) fn remember(task: Uuid, pack: Id, code: Option<&str>) -> Statement<'_> {
    sqlx::query("INSERT INTO integrity_packs(task_id,pack_id,error_code) VALUES($1,$2,$3) ON CONFLICT DO NOTHING").bind(task).bind(pack).bind(code)
}
pub(in super::super) use super::raw_targets;

pub(in super::super) fn finish(
    task: crate::model::task::Execution,
    error: Option<&str>,
) -> Statement<'_> {
    super::work::finish(task.id, task.token, 0, true, error)
}
pub(in super::super) fn mapped(stream: Id, offset: i64, location: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM raw_ranges WHERE stream_id=$1 AND offset_bytes=$2 AND source_id=$3)").bind(stream).bind(offset).bind(location)
}
pub(in super::super) fn previous(task: Uuid, pack: Id) -> Scalar<'static, Option<String>> {
    sqlx::query_scalar("SELECT error_code FROM integrity_packs WHERE task_id=$1 AND pack_id=$2")
        .bind(task)
        .bind(pack)
}
