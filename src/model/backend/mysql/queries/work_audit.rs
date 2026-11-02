use super::*;
pub(in super::super) fn missing_pages(raw: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT page_count IS NULL FROM raw_sources WHERE id=?").bind(raw)
}
pub(in super::super) fn remember(task: Uuid, pack: Id, code: Option<&str>) -> Statement<'_> {
    sqlx::query("INSERT IGNORE INTO integrity_packs(task_id,pack_id,error_code) VALUES(?,?,?)")
        .bind(task)
        .bind(pack)
        .bind(code)
}
pub(in super::super) async fn raw_targets(
    db: &mut sqlx::MySqlConnection,
    detail: &Detail,
    cursor: &Cursor,
    limit: usize,
) -> Result<Vec<(Uuid, String, Id, i64, Id)>> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT o.bucket_id,CONVERT(o.object_key USING utf8mb4),o.stream_id,e.offset_bytes,e.source_id FROM objects o JOIN streams s ON s.id=o.stream_id JOIN raw_ranges e ON e.stream_id=s.id WHERE s.payload_type='raw'",
    );
    super::integrity_scope(&mut query, detail);
    if let (Some(bucket), Some(key)) = (cursor.bucket, &cursor.key) {
        query
            .push(" AND (o.bucket_id,o.object_key,e.offset_bytes)>(")
            .push_bind(bucket)
            .push(",")
            .push_bind(key.as_bytes().to_vec())
            .push(",")
            .push_bind(cursor.after_extent)
            .push(")");
    }
    query
        .push(" ORDER BY o.bucket_id,o.object_key,e.offset_bytes LIMIT ")
        .push_bind(limit as i64);
    Ok(query.build_query_as().fetch_all(db).await?)
}

pub(in super::super) fn finish(
    task: crate::model::task::Execution,
    error: Option<&str>,
) -> Statement<'_> {
    super::finish_task(
        task,
        if error.is_some() {
            "failed"
        } else {
            "completed"
        },
        error,
    )
}
pub(in super::super) fn mapped(stream: Id, offset: i64, location: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM raw_ranges e JOIN raw_sources r ON r.id=e.source_id WHERE e.stream_id=? AND e.offset_bytes=? AND r.location_id=?)").bind(stream).bind(offset).bind(location)
}
pub(in super::super) fn previous(task: Uuid, pack: Id) -> Scalar<'static, Option<String>> {
    sqlx::query_scalar("SELECT error_code FROM integrity_packs WHERE task_id=? AND pack_id=?")
        .bind(task)
        .bind(pack)
}
