use super::*;

pub(in super::super) async fn flush_pack_access(
    connection: &mut sqlx::MySqlConnection,
    pending: &AccessBatch,
) -> Result<()> {
    if pending.packs.is_empty() {
        return Ok(());
    }
    let data = sqlx::types::Json(
        pending
            .packs
            .iter()
            .map(|((id, at), c)| {
                (
                    *id,
                    at.timestamp_micros(),
                    c.downloads,
                    c.bytes,
                    c.partial,
                    c.useful,
                    c.partial_bytes,
                )
            })
            .collect::<Vec<_>>(),
    );
    sqlx::query("INSERT INTO pack_access_windows(pack_id,window_start,downloads,downloaded_bytes,partial_downloads,useful_bytes,partial_bytes) SELECT p.id,TIMESTAMPADD(MICROSECOND,j.at,'1970-01-01'),j.n,j.bytes_count,j.partial_count,j.useful,j.partial_bytes FROM JSON_TABLE(?,'$[*]' COLUMNS(id BIGINT PATH '$[0]',at BIGINT PATH '$[1]',n BIGINT PATH '$[2]',bytes_count BIGINT PATH '$[3]',partial_count BIGINT PATH '$[4]',useful BIGINT PATH '$[5]',partial_bytes BIGINT PATH '$[6]')) j JOIN packs p ON p.id=j.id ORDER BY p.id,j.at ON DUPLICATE KEY UPDATE downloads=pack_access_windows.downloads+j.n,downloaded_bytes=pack_access_windows.downloaded_bytes+j.bytes_count,partial_downloads=pack_access_windows.partial_downloads+j.partial_count,useful_bytes=pack_access_windows.useful_bytes+j.useful,partial_bytes=pack_access_windows.partial_bytes+j.partial_bytes,updated_at=CURRENT_TIMESTAMP(6)").bind(data).execute(connection).await?;
    Ok(())
}

pub(in super::super) async fn flush_member_access(
    connection: &mut sqlx::MySqlConnection,
    pending: &AccessBatch,
) -> Result<()> {
    if pending.members.is_empty() {
        return Ok(());
    }
    let data = sqlx::types::Json(
        pending
            .members
            .iter()
            .map(|((pack, chunk, at), n)| (*pack, *chunk, at.timestamp_micros(), *n))
            .collect::<Vec<_>>(),
    );
    sqlx::query("INSERT INTO pack_member_access_windows(pack_id,chunk_id,window_start,downloads) SELECT p.id,c.id,TIMESTAMPADD(MICROSECOND,j.at,'1970-01-01'),j.n FROM JSON_TABLE(?,'$[*]' COLUMNS(pack BIGINT PATH '$[0]',chunk BIGINT PATH '$[1]',at BIGINT PATH '$[2]',n BIGINT PATH '$[3]')) j JOIN packs p ON p.id=j.pack JOIN chunks c ON c.id=j.chunk ORDER BY p.id,j.at,c.id ON DUPLICATE KEY UPDATE downloads=pack_member_access_windows.downloads+j.n").bind(data).execute(connection).await?;
    Ok(())
}

pub(in super::super) fn access_coverage(dropped: bool, stale: f64) -> Statement<'static> {
    sqlx::query("UPDATE access_state SET coverage_since=IF(? OR coverage_since IS NULL OR flushed_at IS NULL OR flushed_at<TIMESTAMPADD(MICROSECOND,-?,CURRENT_TIMESTAMP(6)),CURRENT_TIMESTAMP(6),coverage_since),flushed_at=CURRENT_TIMESTAMP(6) WHERE id=1").bind(dropped).bind(stale*1_000_000.)
}

pub(in super::super) async fn expire_access(
    connection: &mut sqlx::MySqlConnection,
    retention: f64,
    batch: i64,
) -> Result<()> {
    for sql in [
        super::super::queries::tier::EXPIRE,
        super::super::queries::tier::EXPIRE_TOTALS,
    ] {
        sqlx::query(sql)
            .bind(retention as i64)
            .bind(batch)
            .execute(&mut *connection)
            .await?;
    }
    for table in [
        "chunk_access_windows",
        "pack_access_windows",
        "pack_member_access_windows",
    ] {
        sqlx::query(sqlx::AssertSqlSafe(format!("DELETE FROM {table} WHERE window_start<TIMESTAMPADD(MICROSECOND,-?,CURRENT_TIMESTAMP(6)) ORDER BY window_start LIMIT ?"))).bind(retention*1_000_000.).bind(batch).execute(&mut *connection).await?;
    }
    Ok(())
}

pub(in super::super) fn reset_access_coverage() -> Statement<'static> {
    sqlx::query(
        "UPDATE access_state SET coverage_since=CURRENT_TIMESTAMP(6),flushed_at=CURRENT_TIMESTAMP(6) WHERE id=1",
    )
}

pub(in super::super) fn abandon_startup_streams() -> Statement<'static> {
    sqlx::query(
        "UPDATE streams SET state='abandoned',owner_task_id=NULL,owner_execution_token=NULL,touched_at=CURRENT_TIMESTAMP(6) WHERE state='writing' AND NOT EXISTS(SELECT 1 FROM tasks t WHERE t.id=streams.owner_task_id AND streams.state='writing' AND t.state IN ('queued','running','paused') AND COALESCE(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.node_migration.candidate')),JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.node_conversion.candidate')))=CAST(streams.id AS BINARY)) AND NOT EXISTS(SELECT 1 FROM transfer_sources t WHERE t.candidate=streams.id) AND NOT EXISTS(SELECT 1 FROM transfer_imports i JOIN transfer_items t ON t.id=i.item_id WHERE i.part_stream=streams.id OR t.candidate=streams.id) AND NOT EXISTS(SELECT 1 FROM replica_publications rp WHERE rp.source_pin=streams.id AND rp.phase='preparing') AND NOT EXISTS(SELECT 1 FROM replica_targets rt WHERE rt.candidate=streams.id AND rt.phase IN ('preparing','prepared')) AND NOT EXISTS(SELECT 1 FROM raw_processing h JOIN node_work nw ON nw.task_id=h.token WHERE h.stream_id=streams.id AND nw.result_hash IS NULL AND nw.expires_at>UTC_TIMESTAMP(6)) AND NOT EXISTS(SELECT 1 FROM native_part_reads n WHERE n.stream_id=streams.id AND n.expires_at>UTC_TIMESTAMP(6)) AND NOT EXISTS(SELECT 1 FROM node_writes w WHERE w.stream_id=streams.id AND (w.phase='preparing' OR w.phase='active' AND w.expires_at>UTC_TIMESTAMP(6)))",
    )
}

pub(in super::super) fn fail_startup_chunks() -> Statement<'static> {
    sqlx::query(
        "UPDATE chunks SET state='failed',unreferenced_at=COALESCE(unreferenced_at,CURRENT_TIMESTAMP(6)) WHERE state IN ('preparing','uploading') AND NOT EXISTS(SELECT 1 FROM transfer_sources t WHERE t.candidate=chunks.owner_stream) AND NOT EXISTS(SELECT 1 FROM node_writes w WHERE w.stream_id=chunks.owner_stream AND (w.phase='preparing' OR w.phase='active' AND w.expires_at>UTC_TIMESTAMP(6)))",
    )
}

pub(in super::super) fn retire_startup_encodings() -> Statement<'static> {
    sqlx::query(
        "UPDATE chunk_encodings SET owner_stream=NULL,state='retired',unreferenced_at=COALESCE(unreferenced_at,CURRENT_TIMESTAMP(6)) WHERE state='uploading' AND NOT (boot_id IS NOT NULL AND quarantined_at IS NOT NULL AND encoding_id IN (SELECT current_encoding_id FROM chunks WHERE state='ready')) AND NOT EXISTS(SELECT 1 FROM node_writes w WHERE w.stream_id=chunk_encodings.owner_stream AND (w.phase='preparing' OR w.phase='active' AND w.expires_at>UTC_TIMESTAMP(6)))",
    )
}

pub(in super::super) fn retire_startup_packs() -> Statement<'static> {
    sqlx::query(
        "UPDATE packs SET state='retired',unreferenced_at=COALESCE(unreferenced_at,CURRENT_TIMESTAMP(6)) WHERE state='preparing'",
    )
}

pub(in super::super) fn clear_pack_inputs() -> Statement<'static> {
    sqlx::query("DELETE FROM pack_inputs")
}

pub(in super::super) fn reset_completing_uploads() -> Statement<'static> {
    sqlx::query(
        "UPDATE uploads SET state='active' WHERE state='completing' AND output_stream IS NULL AND NOT EXISTS(SELECT 1 FROM node_completions c WHERE c.upload_id=uploads.id)",
    )
}

pub(in super::super) fn reset_startup_tasks(maintenance: bool) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state=IF(? AND task_type IN ('purge','pack','unpack','upload','cache_flush','storage_migration','rewrite'),'paused','queued'),execution_token=NULL,updated_at=CURRENT_TIMESTAMP(6) WHERE state='running' OR (? AND state='queued' AND task_type IN ('purge','pack','unpack','upload','cache_flush','storage_migration','rewrite'))").bind(maintenance).bind(maintenance)
}

fn stale_stream_filter(query: &mut sqlx::QueryBuilder<MySql>, retention: Option<f64>) {
    query.push(" AND NOT EXISTS(SELECT 1 FROM raw_processing h JOIN node_work nw ON nw.task_id=h.token WHERE h.stream_id=s.id AND nw.result_hash IS NULL AND nw.expires_at>UTC_TIMESTAMP(6)) AND NOT EXISTS(SELECT 1 FROM native_part_reads n WHERE n.stream_id=s.id AND n.expires_at>UTC_TIMESTAMP(6)) AND NOT EXISTS(SELECT 1 FROM node_writes w WHERE w.stream_id=s.id AND (w.phase='preparing' OR w.phase='active' AND w.expires_at>UTC_TIMESTAMP(6))) AND NOT EXISTS(SELECT 1 FROM node_reads nr WHERE nr.stream_id=s.id AND nr.expires_at>UTC_TIMESTAMP(6))");
    query.push(" AND ((s.state IN ('retired','abandoned') AND (").push_bind(retention).push(" IS NULL OR s.touched_at<TIMESTAMPADD(MICROSECOND,-").push_bind(retention.map(|v|v*1_000_000.)).push(",CURRENT_TIMESTAMP(6)))) OR s.state='writing') AND NOT EXISTS(SELECT 1 FROM objects WHERE stream_id=s.id) AND NOT EXISTS(SELECT 1 FROM parts WHERE stream_id=s.id) AND NOT EXISTS(SELECT 1 FROM journal_entries j WHERE j.stream_id=s.id) AND NOT EXISTS(SELECT 1 FROM transfer_changes p WHERE p.source_version=s.id) AND NOT EXISTS(SELECT 1 FROM replica_publications rp WHERE rp.source_pin=s.id AND rp.phase='preparing') AND NOT EXISTS(SELECT 1 FROM replica_targets rt WHERE rt.candidate=s.id AND rt.phase IN ('preparing','prepared')) AND NOT EXISTS(SELECT 1 FROM transfer_items p WHERE p.source_pin=s.id) AND NOT EXISTS(SELECT 1 FROM transfer_items p WHERE p.candidate=s.id) AND NOT EXISTS(SELECT 1 FROM transfer_imports i WHERE i.part_stream=s.id) AND NOT EXISTS(SELECT 1 FROM transfer_sources p WHERE p.candidate=s.id) AND NOT EXISTS(SELECT 1 FROM uploads WHERE output_stream=s.id AND state IN ('active','completing')) AND NOT EXISTS(SELECT 1 FROM tasks t WHERE t.id=s.owner_task_id AND s.state='writing' AND t.state IN ('queued','running','paused') AND COALESCE(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.node_migration.candidate')),JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.node_conversion.candidate')))=CAST(s.id AS BINARY)) AND NOT EXISTS(SELECT 1 FROM tasks t WHERE t.id=s.owner_task_id AND t.state='running' AND t.execution_token=s.owner_execution_token)");
}

pub(in super::super) fn stale_streams(
    active: &[Id],
    batch: i64,
    storage: Option<Uuid>,
    retention: Option<f64>,
) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new("SELECT s.id FROM streams s WHERE TRUE");
    if !active.is_empty() {
        query.push(" AND s.id NOT IN ");
        chunk_ids(&mut query, active);
    }
    if let Some(storage) = storage {
        query
            .push(" AND EXISTS(SELECT 1 FROM buckets b WHERE b.id=s.bucket_id AND b.storage_id=")
            .push_bind(storage)
            .push(")");
    }
    stale_stream_filter(&mut query, retention);
    query
        .push(" ORDER BY s.touched_at,s.id LIMIT ")
        .push_bind(batch);
    query
}

pub(in super::super) fn lock_stale_stream(
    id: Id,
    storage: Option<Uuid>,
    retention: Option<f64>,
) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new("SELECT s.publication_id FROM streams s WHERE s.id=");
    query.push_bind(id);
    if let Some(storage) = storage {
        query
            .push(" AND EXISTS(SELECT 1 FROM buckets b WHERE b.id=s.bucket_id AND b.storage_id=")
            .push_bind(storage)
            .push(")");
    }
    stale_stream_filter(&mut query, retention);
    query.push(" FOR UPDATE SKIP LOCKED");
    query
}

pub(in super::super) fn abandon_cleanup_stream(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET state='abandoned',owner_task_id=NULL,owner_execution_token=NULL WHERE id=? AND NOT EXISTS(SELECT 1 FROM replica_publications rp WHERE rp.source_pin=streams.id AND rp.phase='preparing') AND NOT EXISTS(SELECT 1 FROM replica_targets rt WHERE rt.candidate=streams.id AND rt.phase IN ('preparing','prepared')) AND NOT EXISTS(SELECT 1 FROM raw_processing h JOIN node_work nw ON nw.task_id=h.token WHERE h.stream_id=streams.id AND nw.result_hash IS NULL AND nw.expires_at>UTC_TIMESTAMP(6)) AND NOT EXISTS(SELECT 1 FROM native_part_reads n WHERE n.stream_id=streams.id AND n.expires_at>UTC_TIMESTAMP(6)) AND NOT EXISTS(SELECT 1 FROM node_writes w WHERE w.stream_id=streams.id AND (w.phase='preparing' OR w.phase='active' AND w.expires_at>UTC_TIMESTAMP(6)))").bind(id)
}

pub(in super::super) fn cleanup_stream_bucket(id: Id) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT bucket_id FROM streams WHERE id=?").bind(id)
}

pub(in super::super) fn lock_cleanup_stream(id: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM streams s WHERE id=? AND state='abandoned' AND NOT EXISTS(SELECT 1 FROM raw_processing h JOIN node_work nw ON nw.task_id=h.token WHERE h.stream_id=s.id AND nw.result_hash IS NULL AND nw.expires_at>UTC_TIMESTAMP(6)) AND NOT EXISTS(SELECT 1 FROM native_part_reads n WHERE n.stream_id=s.id AND n.expires_at>UTC_TIMESTAMP(6)) AND NOT EXISTS(SELECT 1 FROM node_writes w WHERE w.stream_id=s.id AND (w.phase='preparing' OR w.phase='active' AND w.expires_at>UTC_TIMESTAMP(6))) AND NOT EXISTS(SELECT 1 FROM node_reads nr WHERE nr.stream_id=s.id AND nr.expires_at>UTC_TIMESTAMP(6)) AND NOT EXISTS(SELECT 1 FROM objects WHERE stream_id=s.id) AND NOT EXISTS(SELECT 1 FROM parts WHERE stream_id=s.id) AND NOT EXISTS(SELECT 1 FROM uploads WHERE output_stream=s.id AND state IN ('active','completing')) FOR UPDATE SKIP LOCKED").bind(id)
}

pub(in super::super) async fn detach_stream_chunks(
    connection: &mut sqlx::MySqlConnection,
    id: Id,
    batch: i64,
) -> Result<u64> {
    let ids: Vec<Id> = sqlx::query_scalar(
        "SELECT id FROM chunks WHERE owner_stream=? ORDER BY id LIMIT ? FOR UPDATE",
    )
    .bind(id)
    .bind(batch)
    .fetch_all(&mut *connection)
    .await?;
    if ids.is_empty() {
        return Ok(0);
    }
    let mut query = sqlx::QueryBuilder::new(
        "UPDATE chunks c SET owner_stream=NULL,state=IF(state IN ('preparing','uploading'),'failed',state),unreferenced_at=IF(NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id),COALESCE(unreferenced_at,CURRENT_TIMESTAMP(6)),unreferenced_at) WHERE id IN ",
    );
    chunk_ids(&mut query, &ids);
    Ok(query.build().execute(connection).await?.rows_affected())
}

pub(in super::super) async fn remove_stream_extents(
    connection: &mut sqlx::MySqlConnection,
    id: Id,
    batch: i64,
) -> Result<usize> {
    let rows:Vec<(i64,Option<Id>)>=sqlx::query_as("SELECT offset_bytes,chunk_id FROM extents WHERE stream_id=? ORDER BY offset_bytes LIMIT ? FOR UPDATE").bind(id).bind(batch).fetch_all(&mut *connection).await?;
    let Some((last, _)) = rows.last() else {
        return Ok(0);
    };
    sqlx::query("DELETE FROM extents WHERE stream_id=? AND offset_bytes<=?")
        .bind(id)
        .bind(last)
        .execute(&mut *connection)
        .await?;
    let chunks: Vec<Id> = rows.iter().filter_map(|r| r.1).collect();
    mark_unreferenced_chunks(connection, &chunks).await?;
    Ok(rows.len())
}

pub(in super::super) fn cleanup_stream_empty(id: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM raw_ranges WHERE stream_id=?) AND NOT EXISTS(SELECT 1 FROM extents WHERE stream_id=?) AND NOT EXISTS(SELECT 1 FROM chunks WHERE owner_stream=?) AND NOT EXISTS(SELECT 1 FROM fragments WHERE owner_stream=?) AND NOT EXISTS(SELECT 1 FROM chunk_encodings WHERE owner_stream=?)").bind(id).bind(id).bind(id).bind(id).bind(id)
}

pub(in super::super) fn detach_upload_output(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE uploads SET output_stream=NULL WHERE output_stream=? AND state IN ('completed','aborted')").bind(id)
}

pub(in super::super) fn remove_empty_stream(id: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM streams WHERE id=?").bind(id)
}

pub(in super::super) fn stale_fragments(
    batch: i64,
    active: &[Id],
    storage: Option<Uuid>,
) -> sqlx::QueryBuilder<MySql> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT f.id FROM fragments f WHERE NOT EXISTS(SELECT 1 FROM extents WHERE fragment_id=f.id)",
    );
    if let Some(storage) = storage {
        query.push(" AND EXISTS(SELECT 1 FROM streams s JOIN buckets b ON b.id=s.bucket_id WHERE s.id=f.owner_stream AND b.storage_id=").push_bind(storage).push(")");
    }
    query.push(" AND (f.sealed OR f.owner_stream IS NULL OR (");
    if !active.is_empty() {
        query.push("f.owner_stream NOT IN ");
        chunk_ids(&mut query, active);
        query.push(" AND ");
    }
    query.push("NOT EXISTS(SELECT 1 FROM streams s WHERE s.id=f.owner_stream AND s.state='writing'))) ORDER BY f.created_at,f.id LIMIT ").push_bind(batch);
    query
}

pub(in super::super) fn lock_cleanup_fragment(
    id: Id,
) -> Rows<
    'static,
    (
        Uuid,
        bool,
        Option<Id>,
        Option<Uuid>,
        Option<Uuid>,
        Option<String>,
    ),
> {
    sqlx::query_as("SELECT location_id,sealed,owner_stream,file_id,staging_storage_id,staging_object_key FROM fragments f WHERE id=? AND NOT EXISTS(SELECT 1 FROM journal_protected_addresses h WHERE h.unit='fragment' AND h.address=f.location_id) AND NOT EXISTS(SELECT 1 FROM extents WHERE fragment_id=f.id) AND NOT EXISTS(SELECT 1 FROM node_read_sources ns JOIN node_read_windows w ON w.id=ns.window_id JOIN node_reads nr ON nr.read_id=w.read_id WHERE ns.location_id=f.location_id AND nr.expires_at>UTC_TIMESTAMP(6)) FOR UPDATE SKIP LOCKED").bind(id)
}

pub(in super::super) fn remove_fragment_row(id: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM fragments WHERE id=?").bind(id)
}

pub(in super::super) fn lock_tombstones(
    storage: Option<Uuid>,
    batch: i64,
) -> Rows<'static, (Uuid, Vec<u8>)> {
    // The outer scan must not lock the stream index before a writer's object row.
    sqlx::query_as("SELECT scan.bucket_id,scan.object_key FROM objects scan WHERE scan.stream_id IS NULL AND (? IS NULL OR EXISTS(SELECT 1 FROM buckets b WHERE b.id=scan.bucket_id AND b.storage_id=?)) AND scan.write_token=(SELECT o.write_token FROM objects o FORCE INDEX (PRIMARY) WHERE o.bucket_id=scan.bucket_id AND o.object_key=scan.object_key AND o.stream_id IS NULL AND NOT EXISTS(SELECT 1 FROM streams s WHERE s.bucket_id=o.bucket_id AND s.object_key=o.object_key AND s.state='writing') FOR UPDATE SKIP LOCKED) ORDER BY scan.bucket_id,scan.object_key LIMIT ?").bind(storage).bind(storage).bind(batch)
}

pub(in super::super) fn delete_tombstones(rows: Vec<(Uuid, Vec<u8>)>) -> Statement<'static> {
    let keys = rows
        .into_iter()
        .map(|(bucket, key)| (bucket.simple().to_string(), hex::encode(key)))
        .collect::<Vec<_>>();
    sqlx::query("DELETE o FROM JSON_TABLE(?, '$[*]' COLUMNS(bucket_hex CHAR(32) PATH '$[0]',key_hex VARCHAR(2048) PATH '$[1]')) k STRAIGHT_JOIN objects o FORCE INDEX (PRIMARY) ON o.bucket_id=UNHEX(k.bucket_hex) AND o.object_key=UNHEX(k.key_hex)").bind(sqlx::types::Json(keys))
}

pub(in super::super) async fn remove_tombstones(
    connection: &mut sqlx::MySqlConnection,
    storage: Option<Uuid>,
    batch: i64,
) -> Result<u64> {
    let rows = lock_tombstones(storage, batch)
        .fetch_all(&mut *connection)
        .await?;
    if rows.is_empty() {
        return Ok(0);
    }
    Ok(delete_tombstones(rows)
        .execute(connection)
        .await?
        .rows_affected())
}

pub(in super::super) fn lock_reclaim_chunk(
    id: Id,
    storage: Uuid,
    grace: f64,
) -> Rows<'static, (Option<Id>,)> {
    sqlx::query_as("SELECT owner_stream FROM chunks c WHERE id=? AND storage_id=? AND state IN ('ready','failed','deleting') AND unreferenced_at<TIMESTAMPADD(MICROSECOND,-?,CURRENT_TIMESTAMP(6)) AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id) AND NOT EXISTS(SELECT 1 FROM pack_inputs WHERE chunk_id=c.id) AND NOT EXISTS(SELECT 1 FROM transfer_members p WHERE p.source_chunk=c.id) AND NOT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=c.id) AND NOT EXISTS(SELECT 1 FROM chunk_encodings WHERE chunk_id=c.id AND owner_stream IS NOT NULL) FOR UPDATE SKIP LOCKED").bind(id).bind(storage).bind(grace*1_000_000.)
}

pub(in super::super) fn claim_reclaim_chunk(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunks SET state='deleting',pack_id=NULL WHERE id=?").bind(id)
}

pub(in super::super) fn reclaim_locations(id: Id) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT location_id FROM chunk_encodings WHERE chunk_id=? AND state IN ('ready','uploading','retired','deleting')").bind(id)
}

pub(in super::super) fn retire_chunk_encodings(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings e JOIN chunks c ON c.id=e.chunk_id SET e.state='retired',e.unreferenced_at=COALESCE(e.unreferenced_at,c.unreferenced_at,CURRENT_TIMESTAMP(6)) WHERE c.id=? AND e.state IN ('ready','uploading')").bind(id)
}

pub(in super::super) fn mark_orphan_chunks() -> Statement<'static> {
    sqlx::query(
        "UPDATE chunks c SET unreferenced_at=CURRENT_TIMESTAMP(6) WHERE c.state='ready' AND c.owner_stream IS NULL AND c.unreferenced_at IS NULL AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id)",
    )
}

fn physical_table(packed: bool) -> (&'static str, &'static str) {
    if packed {
        ("packs", "id")
    } else {
        ("chunk_encodings", "encoding_id")
    }
}

pub(in super::super) async fn cleanup_history(
    connection: &mut sqlx::MySqlConnection,
    section: &str,
    age: f64,
    batch: i64,
    active: &[Uuid],
) -> Result<u64> {
    let (table, id, time, guard) = match section {
        "key_replacements" => ("key_replacements", "id", "expires_at", "TRUE"),
        "maintenance_previews" => ("maintenance_previews", "id", "expires_at", "TRUE"),
        "media_operations" => ("media_operations", "id", "created_at", "TRUE"),
        "audit_events" => ("audit_events", "id", "created_at", "TRUE"),
        "chunks" => (
            "chunks",
            "id",
            "deleted_at",
            "h.state='deleted' AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=h.id) AND NOT EXISTS(SELECT 1 FROM chunk_encodings WHERE chunk_id=h.id AND (state<>'deleted' OR recovery_pending OR owner_stream IS NOT NULL)) AND NOT EXISTS(SELECT 1 FROM pack_members WHERE chunk_id=h.id) AND NOT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=h.id) AND NOT EXISTS(SELECT 1 FROM pack_inputs WHERE chunk_id=h.id) AND NOT EXISTS(SELECT 1 FROM cache_pins WHERE chunk_id=h.id)",
        ),
        "packs" => (
            "packs",
            "id",
            "deleted_at",
            "h.state='deleted' AND NOT EXISTS(SELECT 1 FROM chunks WHERE pack_id=h.id) AND NOT EXISTS(SELECT 1 FROM pending_uploads WHERE source_pack_id=h.id)",
        ),
        "chunk_encodings" => (
            "chunk_encodings",
            "encoding_id",
            "deleted_at",
            "h.state='deleted' AND NOT h.recovery_pending AND h.owner_stream IS NULL",
        ),
        "uploads" => (
            "uploads",
            "id",
            "touched_at",
            "h.state IN ('completed','aborted') AND NOT EXISTS(SELECT 1 FROM parts WHERE upload_id=h.id) AND NOT EXISTS(SELECT 1 FROM quota_reservations WHERE upload_id=h.id)",
        ),
        "integrity_issues" => (
            "integrity_issues",
            "id",
            "(SELECT t.updated_at FROM tasks t WHERE t.id=h.task_id AND t.state='completed')",
            "TRUE",
        ),
        "tasks" => (
            "tasks",
            "id",
            "updated_at",
            "h.state='completed' AND NOT EXISTS(SELECT 1 FROM integrity_issues WHERE task_id=h.id) AND NOT EXISTS(SELECT 1 FROM external_tests e WHERE e.id=h.id AND e.cleanup_required) AND NOT EXISTS(SELECT 1 FROM replica_targets rt WHERE rt.task_id=h.id)",
        ),
        "sessions" => (
            "sessions",
            "id",
            "expires_at",
            "NOT EXISTS(SELECT 1 FROM session_refresh_tokens r WHERE r.session_id=h.id AND r.revoked_at IS NULL AND r.expires_at>CURRENT_TIMESTAMP(6))",
        ),
        "refresh_tokens" => (
            "session_refresh_tokens",
            "id",
            "revoked_at",
            "NOT EXISTS(SELECT 1 FROM session_refresh_tokens live WHERE live.session_id=h.session_id AND live.family_id=h.family_id AND live.revoked_at IS NULL AND live.expires_at>CURRENT_TIMESTAMP(6))",
        ),
        _ => anyhow::bail!("unknown cleanup section"),
    };
    // Select the locked batch first: DELETE with a derived subquery still waits on skipped rows.
    let duration = match section {
        "chunks" | "packs" | "chunk_encodings" => Some(
            "(SELECT (CAST(LEFT(sp.deleted_chunk_retention,LENGTH(sp.deleted_chunk_retention)-1) AS UNSIGNED)*CASE RIGHT(sp.deleted_chunk_retention,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=h.storage_id)",
        ),
        "uploads" => Some(
            "(SELECT (CAST(LEFT(sp.upload_retention,LENGTH(sp.upload_retention)-1) AS UNSIGNED)*CASE RIGHT(sp.upload_retention,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM buckets b JOIN storage_profiles sp ON sp.storage_id=b.storage_id WHERE b.id=h.bucket_id)",
        ),
        _ => None,
    };
    let mut query = sqlx::QueryBuilder::new(format!(
        "SELECT h.{id} FROM {table} h WHERE {guard} AND {time}<"
    ));
    if let Some(duration) = duration {
        query.push(format!(
            "TIMESTAMPADD(SECOND,-({duration}),UTC_TIMESTAMP(6))"
        ));
    } else {
        query
            .push("TIMESTAMPADD(MICROSECOND,-")
            .push_bind(age * 1_000_000.)
            .push(",CURRENT_TIMESTAMP(6))");
    }
    if section == "uploads" && !active.is_empty() {
        query.push(" AND h.id NOT IN ");
        uuid_ids(&mut query, active);
    }
    query
        .push(format!(" ORDER BY {time},h.{id} LIMIT "))
        .push_bind(batch)
        .push(" FOR UPDATE SKIP LOCKED");
    let rows = query.build().fetch_all(&mut *connection).await?;
    if rows.is_empty() {
        return Ok(0);
    }
    let mut deletion = sqlx::QueryBuilder::new(format!("DELETE FROM {table} WHERE {id} IN ("));
    let mut ids = deletion.separated(",");
    for row in rows {
        if matches!(
            table,
            "audit_events" | "chunks" | "packs" | "chunk_encodings" | "integrity_issues"
        ) {
            ids.push_bind(row.try_get::<i64, _>(id)?);
        } else {
            ids.push_bind(row.try_get::<Uuid, _>(id)?);
        }
    }
    ids.push_unseparated(")");
    Ok(deletion.build().execute(connection).await?.rows_affected())
}

#[async_trait::async_trait]
impl MaintenanceStore for Backend {
    async fn preview(
        &self,
        principal: &Principal,
        action: &str,
        parameters: Value,
        impact: Value,
        confirmation: String,
    ) -> Result<MaintenancePreview> {
        transactions::create_maintenance_preview(
            &self.pool,
            principal,
            action,
            parameters,
            impact,
            confirmation,
        )
        .await
    }
    async fn execution(
        &self,
        principal: &Principal,
        id: Uuid,
        action: &str,
    ) -> Result<Option<MaintenanceExecution>> {
        transactions::maintenance_execution(&self.pool, principal, id, action).await
    }
    async fn destructive_sweep(&self) -> Result<bool> {
        Ok(destructive_sweep().fetch_one(&self.pool).await?)
    }
    async fn mode(&self, enabled: bool) -> Result<()> {
        transactions::maintenance_mode(&self.pool, enabled).await
    }
    async fn pack_creation(&self, paused: bool) -> Result<()> {
        transactions::change_pack_creation(&self.pool, paused).await
    }
    async fn policy_paused(&self, policy: &str) -> Result<bool> {
        Ok(maintenance_policy_paused(policy)
            .fetch_one(&self.pool)
            .await?)
    }
    async fn controls(&self) -> Result<Vec<Value>> {
        maintenance_controls(&mut *self.pool.acquire().await?).await
    }
    async fn preparing_packs(&self) -> Result<String> {
        Ok(preparing_pack_count().fetch_one(&self.pool).await?)
    }
    async fn change_policy(&self, policy: &str, paused: bool) -> Result<()> {
        transactions::change_maintenance_policy(&self.pool, policy, paused).await
    }
    async fn start_policy(&self, policy: &str, node: Option<Uuid>) -> Result<(Uuid, bool)> {
        ensure!(node.is_none(), "MySQL does not support cluster nodes");
        transactions::start_lifecycle_policy(&self.pool, policy).await
    }
    async fn finish_policy(
        &self,
        id: Execution,
        count: i64,
        again: bool,
        report: Value,
    ) -> Result<()> {
        transactions::finish_lifecycle_policy(&self.pool, id, count, again, report).await
    }
    async fn schedule(&self) -> Result<Vec<(String, bool, Option<f64>)>> {
        Ok(maintenance_schedule().fetch_all(&self.pool).await?)
    }
    async fn reuse_due(&self) -> Result<bool> {
        Ok(due_pack_reuse().fetch_one(&self.pool).await?)
    }
    async fn scheduled(&self, policy: &str) -> Result<()> {
        scheduled_maintenance(policy).execute(&self.pool).await?;
        Ok(())
    }
    async fn chunk_candidates(
        &self,
        storage: Uuid,
        grace: f64,
        batch: i64,
    ) -> Result<Vec<(Id, i32)>> {
        collect_chunks(&mut *self.pool.acquire().await?, storage, grace, batch).await
    }
    async fn cleanup_history(
        &self,
        section: &str,
        age: f64,
        batch: i64,
        active: &[Uuid],
        timeout_ms: u128,
    ) -> Result<u64> {
        transactions::cleanup_history(&self.pool, section, age, batch, active, timeout_ms).await
    }
    async fn recover(&self, maintenance: bool) -> Result<()> {
        transactions::recover_runtime(&self.pool, maintenance).await
    }
    async fn fragment_exists(&self, id: Uuid) -> Result<bool> {
        Ok(
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM fragments WHERE location_id=?) OR EXISTS(SELECT 1 FROM multipart_files WHERE id=?)")
                .bind(id).bind(id)
                .fetch_one(&self.pool)
                .await?,
        )
    }
    async fn recovery_fragments(&self, after: Id) -> Result<Vec<RecoveryFragment>> {
        Ok(sqlx::query_as("SELECT f.id,f.location_id AS fragment_location,f.size,f.hash,f.staging_storage_id,f.staging_object_key,f.file_id,f.file_offset,m.storage_id AS file_storage,CASE WHEN m.source_id IS NOT NULL THEN m.id END AS raw_source FROM fragments f LEFT JOIN multipart_files m ON m.id=f.file_id WHERE f.id>? AND EXISTS(SELECT 1 FROM extents WHERE fragment_id=f.id) ORDER BY f.id LIMIT 128").bind(after).fetch_all(&self.pool).await?)
    }
    async fn fragment_uploads(&self, id: Id) -> Result<Vec<Uuid>> {
        Ok(sqlx::query_scalar("SELECT DISTINCT p.upload_id FROM parts p JOIN extents e ON e.stream_id=p.stream_id WHERE e.fragment_id=?").bind(id).fetch_all(&self.pool).await?)
    }
    async fn expired_uploads(&self, batch: i64, storage: Option<Uuid>) -> Result<Vec<Uuid>> {
        collect_uploads(&mut *self.pool.acquire().await?, batch, storage).await
    }

    async fn stale_streams(
        &self,
        active: &[Id],
        batch: i64,
        storage: Option<Uuid>,
        retention: Option<f64>,
    ) -> Result<Vec<Id>> {
        Ok(stale_streams(active, batch, storage, retention)
            .build_query_scalar()
            .fetch_all(&self.pool)
            .await?)
    }
    async fn claim_stream(
        &self,
        id: Id,
        storage: Option<Uuid>,
        retention: Option<f64>,
    ) -> Result<bool> {
        transactions::claim_stale_stream(&self.pool, id, storage, retention).await
    }
    async fn cleanup_stream(&self, id: Id, batch: i64) -> Result<usize> {
        transactions::cleanup_stream(&self.pool, id, batch).await
    }
    async fn stale_fragments(
        &self,
        batch: i64,
        active: &[Id],
        storage: Option<Uuid>,
    ) -> Result<Vec<Id>> {
        Ok(stale_fragments(batch, active, storage)
            .build_query_scalar()
            .fetch_all(&self.pool)
            .await?)
    }
    async fn claim_fragment(&self, id: Id) -> Result<Option<FragmentCleanup>> {
        transactions::claim_fragment(&self.pool, id).await
    }
    async fn cleanup_tombstones(&self, storage: Option<Uuid>, batch: i64) -> Result<u64> {
        transactions::cleanup_tombstones(&self.pool, storage, batch).await
    }
    async fn gc_paused(&self) -> Result<bool> {
        Ok(
            sqlx::query_scalar("SELECT gc_paused OR maintenance FROM mokyu_meta WHERE id=1")
                .fetch_one(&self.pool)
                .await?,
        )
    }
    async fn storages(&self) -> Result<Vec<Uuid>> {
        Ok(
            sqlx::query_scalar("SELECT storage_id FROM storage_profiles ORDER BY storage_id")
                .fetch_all(&self.pool)
                .await?,
        )
    }
    async fn retire_chunk(
        &self,
        id: Id,
        storage: Uuid,
        grace: f64,
        active: &[Id],
    ) -> Result<Option<Vec<Uuid>>> {
        transactions::retire_chunk(&self.pool, id, storage, grace, active).await
    }
    async fn delete_chunk(&self, id: Id, storage: Uuid) -> Result<()> {
        finish_collect_chunk(&mut *self.pool.acquire().await?, id, storage).await
    }
    async fn retire_packs(&self, storage: Uuid) -> Result<()> {
        collect_empty_packs(&mut *self.pool.acquire().await?, storage).await
    }
    async fn physical_candidates(
        &self,
        packed: bool,
        grace: f64,
        batch: i64,
        minimum: f64,
        storage: Uuid,
    ) -> Result<Vec<(Id, Uuid, bool)>> {
        collect_physical(
            &mut *self.pool.acquire().await?,
            packed,
            grace,
            batch,
            minimum,
            storage,
        )
        .await
    }
    async fn physical_age(&self, packed: bool, id: Id, stored: DateTime<Utc>) -> Result<()> {
        collect_age(&mut *self.pool.acquire().await?, packed, id, stored).await
    }
    async fn claim_physical(&self, packed: bool, id: Id, grace: f64, minimum: f64) -> Result<bool> {
        claim_collect_physical(&mut *self.pool.acquire().await?, packed, id, grace, minimum).await
    }
    async fn delete_physical(&self, packed: bool, id: Id, retention: f64) -> Result<()> {
        let mut tx = super::super::queries::tier::begin(self).await?;
        let value = finish_collect_physical(&mut tx, packed, id, retention).await?;
        tx.commit().await?;
        Ok(value)
    }
    async fn gc_status(&self) -> Result<(Vec<(String, i64)>, bool, bool)> {
        let counts = sqlx::query_as("SELECT state,COUNT(*) FROM chunks GROUP BY state")
            .fetch_all(&self.pool)
            .await?;
        let (paused, maintenance) =
            sqlx::query_as("SELECT gc_paused,maintenance FROM mokyu_meta WHERE id=1")
                .fetch_one(&self.pool)
                .await?;
        Ok((counts, paused, maintenance))
    }
    async fn mark_orphans(&self) -> Result<usize> {
        transactions::mark_orphan_chunks(&self.pool).await
    }
}

pub(in super::super) fn destructive_sweep() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM tasks WHERE task_type='sweep' AND CAST(JSON_UNQUOTE(JSON_EXTRACT(detail,'$.dry_run')) AS BINARY)='false' AND state IN ('queued','running')) OR EXISTS(SELECT 1 FROM node_work WHERE result_hash IS NULL AND expires_at>UTC_TIMESTAMP(6) AND CAST(JSON_UNQUOTE(JSON_EXTRACT(plan,'$.action')) AS BINARY)='sweep_delete')",
    )
}

pub(in super::super) fn set_maintenance_mode(enabled: bool) -> Statement<'static> {
    sqlx::query("UPDATE mokyu_meta SET maintenance=? WHERE id=1").bind(enabled)
}

pub(in super::super) fn pause_maintenance_tasks() -> Statement<'static> {
    sqlx::query(
        "UPDATE tasks SET state='paused',updated_at=CURRENT_TIMESTAMP(6) WHERE task_type IN ('purge','pack','unpack','storage_migration','rewrite') AND state IN ('queued','running')",
    )
}

pub(in super::super) fn unpacking_all() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM tasks WHERE task_type='unpack' AND CAST(JSON_UNQUOTE(JSON_EXTRACT(detail,'$.all')) AS BINARY)='true' AND state<>'completed')",
    )
}

pub(in super::super) fn pause_pack_creation(paused: bool) -> Statement<'static> {
    sqlx::query("UPDATE mokyu_meta SET pack_creation_paused=? WHERE id=1").bind(paused)
}

pub(in super::super) fn maintenance_policy_paused(policy: &str) -> Scalar<'_, bool> {
    sqlx::query_scalar("SELECT IF(c.policy='gc',m.gc_paused,c.paused) FROM maintenance_controls c CROSS JOIN mokyu_meta m WHERE c.policy=?").bind(policy)
}

pub(in super::super) fn pause_gc(paused: bool) -> Statement<'static> {
    sqlx::query("UPDATE mokyu_meta SET gc_paused=? WHERE id=1").bind(paused)
}

pub(in super::super) fn pause_maintenance_policy(policy: &str, paused: bool) -> Statement<'_> {
    sqlx::query(
        "UPDATE maintenance_controls SET paused=?,updated_at=CURRENT_TIMESTAMP(6) WHERE policy=?",
    )
    .bind(paused)
    .bind(policy)
}

pub(in super::super) fn active_lifecycle_task(policy: &str) -> Scalar<'_, Uuid> {
    sqlx::query_scalar("SELECT id FROM tasks WHERE policy=? AND task_type=? AND state IN ('queued','running','paused') ORDER BY created_at,id LIMIT 1").bind(policy).bind(policy)
}

pub(in super::super) fn scheduled_maintenance(policy: &str) -> Statement<'_> {
    sqlx::query(
        "UPDATE maintenance_controls SET last_scheduled_at=CURRENT_TIMESTAMP(6) WHERE policy=?",
    )
    .bind(policy)
}

pub(in super::super) fn maintenance_schedule() -> Rows<'static, (String, bool, Option<f64>)> {
    sqlx::query_as(
        "SELECT c.policy,IF(c.policy='gc',m.gc_paused,c.paused),TIMESTAMPDIFF(MICROSECOND,c.last_scheduled_at,CURRENT_TIMESTAMP(6))/1000000e0 FROM maintenance_controls c CROSS JOIN mokyu_meta m",
    )
}

pub(in super::super) fn due_pack_reuse() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pack_changes WHERE reason='reuse' AND next_check_at<=CURRENT_TIMESTAMP(6))",
    )
}

pub(in super::super) fn preparing_pack_count() -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT CAST(COUNT(*) AS CHAR) FROM packs WHERE state='preparing'")
}

pub(in super::super) async fn maintenance_controls(
    connection: &mut sqlx::MySqlConnection,
) -> Result<Vec<Value>> {
    let rows = sqlx::query("SELECT c.*,IF(c.policy='gc',m.gc_paused,c.paused) AS policy_paused,(SELECT id FROM tasks t WHERE t.policy=c.policy ORDER BY created_at DESC,id DESC LIMIT 1) AS latest_id,(SELECT id FROM tasks t WHERE t.policy=c.policy AND state IN ('queued','running','paused') ORDER BY created_at,id LIMIT 1) AS active_id FROM maintenance_controls c CROSS JOIN mokyu_meta m ORDER BY c.policy").fetch_all(&mut *connection).await?;
    let mut ids = Vec::new();
    for row in &rows {
        for column in ["latest_id", "active_id"] {
            if let Some(id) = row.try_get::<Option<Uuid>, _>(column)? {
                ids.push(id);
            }
        }
    }
    ids.sort_unstable();
    ids.dedup();
    let mut tasks = std::collections::HashMap::new();
    if !ids.is_empty() {
        let mut query = sqlx::QueryBuilder::<MySql>::new("SELECT * FROM tasks WHERE id IN ");
        uuid_ids(&mut query, &ids);
        for row in query.build().fetch_all(connection).await? {
            tasks.insert(row.try_get::<Uuid, _>("id")?, task_value(row, false)?);
        }
    }
    rows.into_iter().map(|row| {
        let latest = row.try_get::<Option<Uuid>,_>("latest_id")?;
        let active = row.try_get::<Option<Uuid>,_>("active_id")?;
        Ok(serde_json::json!({"policy":row.try_get::<String,_>("policy")?,"paused":row.try_get::<bool,_>("policy_paused")?,"last_scheduled_at":row.try_get::<DateTime<Utc>,_>("last_scheduled_at")?,"updated_at":row.try_get::<DateTime<Utc>,_>("updated_at")?,"latest":latest.and_then(|id|tasks.get(&id)),"active":active.and_then(|id|tasks.get(&id))}))
    }).collect()
}

pub(in super::super) fn remove_maintenance_preview(
    actor: Uuid,
    token: Option<Uuid>,
    action: &str,
) -> Statement<'_> {
    sqlx::query("DELETE FROM maintenance_previews WHERE actor_id=? AND token_id<=>? AND action=? AND task_id IS NULL").bind(actor).bind(token).bind(action)
}

pub(in super::super) fn create_maintenance_preview(
    id: Uuid,
    actor: Uuid,
    token: Option<Uuid>,
    action: &str,
    parameters: Value,
    fingerprint: String,
) -> Statement<'_> {
    sqlx::query("INSERT INTO maintenance_previews(id,actor_id,token_id,action,parameters,fingerprint) VALUES(?,?,?,?,?,?)").bind(id).bind(actor).bind(token).bind(action).bind(parameters).bind(fingerprint)
}

pub(in super::super) fn maintenance_preview_expiry(id: Uuid) -> Scalar<'static, DateTime<Utc>> {
    sqlx::query_scalar("SELECT expires_at FROM maintenance_previews WHERE id=?").bind(id)
}

pub(in super::super) fn lock_maintenance_preview(
    id: Uuid,
    actor: Uuid,
    token: Option<Uuid>,
    action: &str,
) -> Rows<'_, (Value, String, Option<Uuid>)> {
    sqlx::query_as("SELECT parameters,fingerprint,task_id FROM maintenance_previews WHERE id=? AND actor_id=? AND token_id<=>? AND action=? AND expires_at>CURRENT_TIMESTAMP(6) FOR UPDATE").bind(id).bind(actor).bind(token).bind(action)
}

pub(in super::super) fn maintenance_preview_task(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE id=?)").bind(id)
}

pub(in super::super) fn record_maintenance_preview(id: Uuid, task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE maintenance_previews SET task_id=? WHERE id=?")
        .bind(task)
        .bind(id)
}

pub(in super::super) fn lock_integrity_start() -> Statement<'static> {
    sqlx::query("SELECT id FROM mokyu_meta WHERE id=1 FOR UPDATE")
}

pub(in super::super) fn integrity_busy() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM tasks WHERE task_type='integrity' AND JSON_EXTRACT(detail,'$.source_operation') IS NULL AND state IN ('queued','running'))",
    )
}

pub(in super::super) fn integrity_storages(storage: Option<Uuid>) -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM storages WHERE enabled AND (? IS NULL OR id=?) ORDER BY created_at,id",
    )
    .bind(storage)
    .bind(storage)
}

pub(in super::super) async fn collect_uploads(
    db: &mut sqlx::MySqlConnection,
    batch: i64,
    storage: Option<Uuid>,
) -> Result<Vec<Uuid>> {
    Ok(sqlx::query_scalar("SELECT u.id FROM uploads u JOIN buckets b ON b.id=u.bucket_id JOIN storage_profiles p ON p.storage_id=b.storage_id WHERE (? IS NULL OR b.storage_id=?) AND u.state='active' AND u.touched_at<TIMESTAMPADD(SECOND,-(CAST(LEFT(p.multipart_idle_timeout,LENGTH(p.multipart_idle_timeout)-1) AS UNSIGNED)*CASE RIGHT(p.multipart_idle_timeout,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END),CURRENT_TIMESTAMP(6)) ORDER BY u.touched_at,u.id LIMIT ?").bind(storage).bind(storage).bind(batch).fetch_all(&mut *db).await?)
}

pub(in super::super) async fn collect_chunks(
    db: &mut sqlx::MySqlConnection,
    storage: Uuid,
    grace: f64,
    batch: i64,
) -> Result<Vec<(Id, i32)>> {
    Ok(sqlx::query_as("SELECT id,raw_size FROM chunks c WHERE storage_id=? AND state IN ('ready','failed','deleting') AND unreferenced_at<TIMESTAMPADD(MICROSECOND,-?,CURRENT_TIMESTAMP(6)) AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id) AND NOT EXISTS(SELECT 1 FROM pack_inputs WHERE chunk_id=c.id) AND NOT EXISTS(SELECT 1 FROM transfer_members p WHERE p.source_chunk=c.id) AND NOT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=c.id) ORDER BY unreferenced_at,id LIMIT ?").bind(storage).bind(grace*1_000_000.).bind(batch).fetch_all(&mut *db).await?)
}

pub(in super::super) async fn finish_collect_chunk(
    db: &mut sqlx::MySqlConnection,
    id: Id,
    storage: Uuid,
) -> Result<()> {
    sqlx::query("UPDATE chunks SET state='deleted',deleted_at=CURRENT_TIMESTAMP(6) WHERE id=? AND storage_id=? AND state='deleting'").bind(id).bind(storage).execute(&mut *db).await?;
    Ok(())
}

pub(in super::super) async fn collect_empty_packs(
    db: &mut sqlx::MySqlConnection,
    storage: Uuid,
) -> Result<()> {
    sqlx::query("UPDATE packs p SET state='retired',unreferenced_at=COALESCE(unreferenced_at,(SELECT MAX(c.unreferenced_at) FROM pack_members m JOIN chunks c ON c.id=m.chunk_id WHERE m.pack_id=p.id),CURRENT_TIMESTAMP(6)) WHERE state='ready' AND NOT EXISTS(SELECT 1 FROM chunks WHERE pack_id=p.id AND state='ready') AND p.storage_id=?").bind(storage).execute(&mut *db).await?;
    Ok(())
}

pub(in super::super) async fn collect_physical(
    db: &mut sqlx::MySqlConnection,
    packed: bool,
    grace: f64,
    batch: i64,
    minimum: f64,
    storage: Uuid,
) -> Result<Vec<(Id, Uuid, bool)>> {
    let tier_unit = i16::from(packed);
    let state_guard = if packed {
        "state IN ('retired','deleting')"
    } else {
        "(state IN ('retired','deleting') OR (state='deleted' AND recovery_pending)) AND owner_stream IS NULL"
    };
    let (table, id) = physical_table(packed);
    let source = if packed {
        "source_pack"
    } else {
        "source_encoding"
    };
    let unknown = if packed {
        "stored_at IS NULL"
    } else {
        "stored_at IS NULL OR recovery_pending"
    };
    Ok(sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT {table}.{id},COALESCE(tl.address,{table}.location_id),(tl.address IS NULL AND ({unknown})) FROM {table} LEFT JOIN tier_locations tl ON tl.unit={tier_unit} AND tl.physical_id={table}.location_id WHERE {state_guard} AND NOT EXISTS(SELECT 1 FROM journal_protected_addresses h WHERE h.address=COALESCE(tl.address,{table}.location_id)) AND NOT EXISTS(SELECT 1 FROM transfer_sources p WHERE p.{source}={table}.{id}) AND unreferenced_at<TIMESTAMPADD(MICROSECOND,-?,CURRENT_TIMESTAMP(6)) AND (?=0 OR stored_at IS NULL OR stored_at<=TIMESTAMPADD(MICROSECOND,-?,CURRENT_TIMESTAMP(6))) AND COALESCE(tl.storage_id,{table}.storage_id)=? AND NOT EXISTS(SELECT 1 FROM node_read_sources ns JOIN node_read_windows w ON w.id=ns.window_id JOIN node_reads nr ON nr.read_id=w.read_id WHERE ns.location_id=COALESCE((SELECT a.address FROM tier_locations a WHERE a.unit={tier_unit} AND a.physical_id={table}.location_id),{table}.location_id) AND nr.expires_at>UTC_TIMESTAMP(6)) AND NOT EXISTS(SELECT 1 FROM tier_moves mv WHERE mv.unit={tier_unit} AND mv.physical_id={table}.location_id) AND NOT EXISTS(SELECT 1 FROM tier_locations th WHERE th.unit={tier_unit} AND th.physical_id={table}.location_id AND th.activated_at>CAST(UNIX_TIMESTAMP() AS SIGNED)-?) ORDER BY unreferenced_at,{id} LIMIT ?"))).bind(grace*1_000_000.).bind(minimum).bind(minimum*1_000_000.).bind(storage).bind(minimum as i64).bind(batch).fetch_all(&mut *db).await?)
}

pub(in super::super) async fn collect_age(
    db: &mut sqlx::MySqlConnection,
    packed: bool,
    id: Id,
    stored: DateTime<Utc>,
) -> Result<()> {
    if !packed {
        sqlx::query("UPDATE chunk_encodings SET stored_at=CASE WHEN recovery_pending THEN ? ELSE COALESCE(stored_at,?) END WHERE encoding_id=?").bind(stored).bind(stored).bind(id).execute(&mut *db).await?;
        return Ok(());
    }
    let (table, column) = physical_table(packed);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {table} SET stored_at=COALESCE(stored_at,?) WHERE {column}=?"
    )))
    .bind(stored)
    .bind(id)
    .execute(&mut *db)
    .await?;
    Ok(())
}

pub(in super::super) async fn claim_collect_physical(
    db: &mut sqlx::MySqlConnection,
    packed: bool,
    id: Id,
    grace: f64,
    minimum: f64,
) -> Result<bool> {
    let tier_unit = i16::from(packed);
    let state_guard = if packed {
        "state IN ('retired','deleting')"
    } else {
        "(state IN ('retired','deleting') OR (state='deleted' AND recovery_pending)) AND owner_stream IS NULL"
    };
    let (table, column) = physical_table(packed);
    let source = if packed {
        "source_pack"
    } else {
        "source_encoding"
    };
    Ok(sqlx::query(sqlx::AssertSqlSafe(format!("UPDATE {table} SET state='deleting' WHERE {column}=? AND {state_guard} AND NOT EXISTS(SELECT 1 FROM journal_protected_addresses h WHERE h.address=COALESCE((SELECT th.address FROM tier_locations th WHERE th.unit={tier_unit} AND th.physical_id={table}.location_id),{table}.location_id)) AND NOT EXISTS(SELECT 1 FROM transfer_sources p WHERE p.{source}={table}.{column}) AND unreferenced_at<TIMESTAMPADD(MICROSECOND,-?,CURRENT_TIMESTAMP(6)) AND (?=0 OR stored_at IS NULL OR stored_at<=TIMESTAMPADD(MICROSECOND,-?,CURRENT_TIMESTAMP(6))) AND NOT EXISTS(SELECT 1 FROM node_read_sources ns JOIN node_read_windows w ON w.id=ns.window_id JOIN node_reads nr ON nr.read_id=w.read_id WHERE ns.location_id=COALESCE((SELECT a.address FROM tier_locations a WHERE a.unit={tier_unit} AND a.physical_id={table}.location_id),{table}.location_id) AND nr.expires_at>UTC_TIMESTAMP(6)) AND NOT EXISTS(SELECT 1 FROM tier_moves mv WHERE mv.unit={tier_unit} AND mv.physical_id={table}.location_id) AND NOT EXISTS(SELECT 1 FROM tier_locations th WHERE th.unit={tier_unit} AND th.physical_id={table}.location_id AND th.activated_at>CAST(UNIX_TIMESTAMP() AS SIGNED)-?)"))).bind(id).bind(grace*1_000_000.).bind(minimum).bind(minimum*1_000_000.).bind(minimum as i64).execute(&mut *db).await?.rows_affected()!=0)
}

pub(in super::super) async fn finish_collect_physical(
    db: &mut sqlx::MySqlConnection,
    packed: bool,
    id: Id,
    retention: f64,
) -> Result<()> {
    let unit = if packed {
        crate::model::tiering::Unit::Pack
    } else {
        crate::model::tiering::Unit::Chunk
    };
    let address = super::super::queries::tier::gc_identity(&mut *db, unit, id).await?;
    if !packed {
        let changed=sqlx::query("UPDATE chunk_encodings SET recovery_pending=recovery_pending AND (deleted_at IS NULL OR deleted_at>TIMESTAMPADD(MICROSECOND,-?,CURRENT_TIMESTAMP(6))),state='deleted',unreferenced_at=CURRENT_TIMESTAMP(6),deleted_at=COALESCE(deleted_at,CURRENT_TIMESTAMP(6)) WHERE encoding_id=? AND state='deleting'").bind(retention*1_000_000.).bind(id).execute(&mut *db).await?.rows_affected();
        if changed == 1
            && let Some(address) = address
        {
            super::super::queries::tier::forget(&mut *db, unit, address).await?;
        }

        return Ok(());
    }
    let (table, column) = physical_table(packed);
    let changed=sqlx::query(sqlx::AssertSqlSafe(format!("UPDATE {table} SET state='deleted',deleted_at=CURRENT_TIMESTAMP(6) WHERE {column}=? AND state='deleting'"))).bind(id).execute(&mut *db).await?.rows_affected();
    if changed == 1
        && let Some(address) = address
    {
        super::super::queries::tier::forget(&mut *db, unit, address).await?;
    }

    Ok(())
}
