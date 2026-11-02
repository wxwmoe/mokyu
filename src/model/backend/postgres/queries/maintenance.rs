use super::*;

pub(in super::super) fn advance_task<'q>(
    id: Uuid,
    cursor: &'q str,
    processed: i64,
) -> Statement<'q> {
    sqlx::query("UPDATE tasks SET cursor=$2,processed=processed+$3,updated_at=now() WHERE id=$1")
        .bind(id)
        .bind(cursor)
        .bind(processed)
}

pub(in super::super) fn purge_upload_history<'q>(
    bucket: Uuid,
    active: &'q [Uuid],
    batch: i64,
) -> Statement<'q> {
    sqlx::query("DELETE FROM uploads WHERE id IN (SELECT id FROM uploads WHERE bucket_id=$1 AND state IN ('completed','aborted') AND NOT (id=ANY($2)) LIMIT $3)").bind(bucket).bind(active).bind(batch)
}

pub(in super::super) fn purge_waiting(bucket: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM streams WHERE bucket_id=$1) OR EXISTS(SELECT 1 FROM uploads WHERE bucket_id=$1)").bind(bucket)
}

pub(in super::super) fn purge_object_rows(bucket: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM objects WHERE bucket_id=$1").bind(bucket)
}

pub(in super::super) fn purge_bucket_row(bucket: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM buckets WHERE id=$1 AND state='purging'").bind(bucket)
}

pub(in super::super) fn complete_task(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='completed',updated_at=now() WHERE id=$1").bind(id)
}

pub(in super::super) fn complete_running_task(id: Execution) -> Statement<'static> {
    sqlx::query(
        "UPDATE tasks SET state='completed',updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$2 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp())",
    )
    .bind(id.id)
        .bind(id.token)
}

pub(in super::super) fn physical_indexed(location: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM chunk_encodings u WHERE u.encoding_id=$1 AND NOT EXISTS(SELECT 1 FROM tier_locations h WHERE h.unit=0 AND h.physical_id=u.encoding_id)) OR EXISTS(SELECT 1 FROM packs u WHERE u.id=$1 AND NOT EXISTS(SELECT 1 FROM tier_locations h WHERE h.unit=1 AND h.physical_id=u.id)) OR EXISTS(SELECT 1 FROM tier_locations WHERE address=$1) OR EXISTS(SELECT 1 FROM tier_moves WHERE source_address=$1) OR EXISTS(SELECT 1 FROM tier_moves WHERE target_address=$1)").bind(location)
}

pub(in super::super) fn task_progress<'q>(
    id: Execution,
    cursor: &'q str,
    processed: i64,
    detail: Value,
) -> Statement<'q> {
    sqlx::query(
        "UPDATE tasks SET cursor=$2,processed=processed+$3,detail=$4,updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$5 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp())",
    )
    .bind(id.id)
    .bind(cursor)
    .bind(processed)
    .bind(detail)
        .bind(id.token)
}

pub(in super::super) fn gc_candidates(
    storage: Uuid,
    grace: f64,
    batch: i64,
) -> Rows<'static, (Id, i32)> {
    sqlx::query_as("SELECT id,raw_size FROM chunks WHERE storage_id=$1 AND state IN ('ready','failed','deleting') AND unreferenced_at<now()-$2*interval '1 second' AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=chunks.id) AND NOT EXISTS(SELECT 1 FROM pack_inputs WHERE chunk_id=chunks.id) AND NOT EXISTS(SELECT 1 FROM transfer_members p WHERE p.source_chunk=chunks.id) AND NOT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=chunks.id) ORDER BY unreferenced_at,id LIMIT $3").bind(storage).bind(grace).bind(batch)
}

pub(in super::super) fn reset_access_coverage() -> Statement<'static> {
    sqlx::query("UPDATE mokyu_meta SET access_coverage_since=now(),access_flushed_at=now()")
}

pub(in super::super) fn abandon_startup_streams() -> Statement<'static> {
    sqlx::query(
        "UPDATE streams SET state='abandoned',owner_task_id=NULL,owner_lease_token=NULL,touched_at=now() WHERE state='writing' AND NOT EXISTS(SELECT 1 FROM transfer_sources t WHERE t.candidate=streams.id) AND NOT EXISTS(SELECT 1 FROM transfer_imports i JOIN transfer_items t ON t.id=i.item_id WHERE i.part_stream=streams.id OR t.candidate=streams.id) AND NOT EXISTS(SELECT 1 FROM replica_publications rp WHERE rp.source_pin=streams.id AND rp.phase='preparing') AND NOT EXISTS(SELECT 1 FROM replica_targets rt WHERE rt.candidate=streams.id AND rt.phase IN ('preparing','prepared'))",
    )
}

pub(in super::super) fn fail_startup_chunks() -> Statement<'static> {
    sqlx::query(
        "UPDATE chunks SET state='failed',unreferenced_at=COALESCE(unreferenced_at,now()) WHERE state IN ('preparing','uploading') AND NOT EXISTS(SELECT 1 FROM transfer_sources t WHERE t.candidate=chunks.owner_stream)",
    )
}

pub(in super::super) fn retire_startup_encodings() -> Statement<'static> {
    sqlx::query(
        "UPDATE chunk_encodings SET owner_stream=NULL,state='retired',unreferenced_at=COALESCE(unreferenced_at,now()) WHERE state='uploading' AND NOT (boot_id IS NOT NULL AND quarantined_at IS NOT NULL AND encoding_id IN (SELECT current_encoding_id FROM chunks WHERE state='ready'))",
    )
}

pub(in super::super) fn retire_startup_packs() -> Statement<'static> {
    sqlx::query(
        "UPDATE packs SET state='retired',unreferenced_at=COALESCE(unreferenced_at,now()) WHERE state='preparing'",
    )
}

pub(in super::super) fn clear_pack_inputs() -> Statement<'static> {
    sqlx::query("DELETE FROM pack_inputs")
}

pub(in super::super) fn reset_completing_uploads() -> Statement<'static> {
    sqlx::query(
        "UPDATE uploads SET state='active' WHERE state='completing' AND output_stream IS NULL",
    )
}

pub(in super::super) fn reset_startup_tasks(maintenance: bool) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state=CASE WHEN task_type IN ('purge','pack','unpack','upload','cache_flush','storage_migration','rewrite') AND $1 THEN 'paused' ELSE 'queued' END,lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE state='running' OR (task_type IN ('purge','pack','unpack','upload','cache_flush','storage_migration','rewrite') AND state='queued' AND $1)").bind(maintenance)
}

pub(in super::super) fn fragment_indexed(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM fragments WHERE id=$1) OR EXISTS(SELECT 1 FROM multipart_files WHERE id=$1)").bind(id)
}

pub(in super::super) fn recovery_fragments(after: Id) -> Rows<'static, RecoveryFragment> {
    sqlx::query_as("SELECT f.id,f.id AS fragment_location,f.size,f.hash,f.staging_storage_id,f.staging_object_key,f.file_id,f.file_offset,m.storage_id AS file_storage,CASE WHEN m.source_id IS NOT NULL THEN m.id END AS raw_source FROM fragments f LEFT JOIN multipart_files m ON m.id=f.file_id WHERE f.id>$1 AND EXISTS(SELECT 1 FROM extents WHERE fragment_id=f.id) ORDER BY f.id LIMIT 128").bind(after)
}

pub(in super::super) fn fragment_uploads(id: Id) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT DISTINCT p.upload_id FROM parts p JOIN extents e ON e.stream_id=p.stream_id WHERE e.fragment_id=$1").bind(id)
}

pub(in super::super) fn expire_stream_leases() -> Statement<'static> {
    sqlx::query("DELETE FROM stream_leases WHERE expires_at<=now()")
}

pub(in super::super) fn expired_uploads(
    batch: i64,
    storage: Option<Uuid>,
) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT u.id FROM uploads u JOIN buckets b ON b.id=u.bucket_id JOIN storage_profiles p ON p.storage_id=b.storage_id WHERE ($2::uuid IS NULL OR b.storage_id=$2) AND u.state='active' AND u.touched_at<now()-(left(p.multipart_idle_timeout,-1)::bigint*CASE right(p.multipart_idle_timeout,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END)*interval '1 second' ORDER BY u.touched_at,u.id LIMIT $1").bind(batch).bind(storage)
}

fn stale_stream_filter(query: &mut QueryBuilder<Postgres>, retention: Option<f64>, single: bool) {
    query
        .push(" AND ((s.state IN ('retired','abandoned') AND (")
        .push_bind(retention)
        .push("::double precision IS NULL OR s.touched_at<now()-")
        .push_bind(retention)
        .push("*interval '1 second')) OR s.state='writing')");
    if !single {
        query.push(" AND NOT EXISTS(SELECT 1 FROM stream_leases l WHERE l.stream_id=s.id AND l.expires_at>now())");
    }
    query.push(" AND NOT EXISTS(SELECT 1 FROM objects WHERE stream_id=s.id) AND NOT EXISTS(SELECT 1 FROM parts WHERE stream_id=s.id) AND NOT EXISTS(SELECT 1 FROM transfer_changes p WHERE p.source_version=s.id) AND NOT EXISTS(SELECT 1 FROM replica_publications rp WHERE rp.source_pin=s.id AND rp.phase='preparing') AND NOT EXISTS(SELECT 1 FROM replica_targets rt WHERE rt.candidate=s.id AND rt.phase IN ('preparing','prepared')) AND NOT EXISTS(SELECT 1 FROM transfer_items p WHERE p.source_pin=s.id) AND NOT EXISTS(SELECT 1 FROM transfer_items p WHERE p.candidate=s.id) AND NOT EXISTS(SELECT 1 FROM transfer_imports i WHERE i.part_stream=s.id) AND NOT EXISTS(SELECT 1 FROM transfer_sources p WHERE p.candidate=s.id) AND NOT EXISTS(SELECT 1 FROM tasks t WHERE t.id=s.owner_task_id AND t.state='running' AND t.lease_token=s.owner_lease_token AND ((t.lease_node_id IS NULL AND t.lease_expires_at IS NULL) OR t.lease_expires_at>now()))");
}

pub(in super::super) fn stale_streams(
    active: &[Id],
    batch: i64,
    storage: Option<Uuid>,
    retention: Option<f64>,
    single: bool,
) -> QueryBuilder<Postgres> {
    let mut query = QueryBuilder::new(
        "SELECT s.id FROM streams s JOIN buckets b ON b.id=s.bucket_id WHERE NOT(s.id=ANY(",
    );
    query.push_bind(active).push("))");
    if let Some(storage) = storage {
        query.push(" AND b.storage_id=").push_bind(storage);
    }
    stale_stream_filter(&mut query, retention, single);
    query
        .push(" ORDER BY s.touched_at,s.id LIMIT ")
        .push_bind(batch);
    query
}

pub(in super::super) fn claim_stale_stream(
    id: Id,
    retention: Option<f64>,
    storage: Option<Uuid>,
    single: bool,
) -> QueryBuilder<Postgres> {
    let mut query = QueryBuilder::new(
        "UPDATE streams s SET state='abandoned',owner_task_id=NULL,owner_lease_token=NULL WHERE id=",
    );
    query.push_bind(id);
    if let Some(storage) = storage {
        query
            .push(" AND EXISTS(SELECT 1 FROM buckets b WHERE b.id=s.bucket_id AND b.storage_id=")
            .push_bind(storage)
            .push(")");
    }
    stale_stream_filter(&mut query, retention, single);
    query.push(" RETURNING s.publication_id");
    query
}

pub(in super::super) fn detach_stream_chunks(id: Id, batch: i64) -> Statement<'static> {
    sqlx::query("UPDATE chunks c SET owner_stream=NULL,state=CASE WHEN state IN ('preparing','uploading') THEN 'failed' ELSE state END,unreferenced_at=CASE WHEN NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id) THEN COALESCE(unreferenced_at,now()) ELSE unreferenced_at END WHERE id IN (SELECT id FROM chunks WHERE owner_stream=$1 ORDER BY id LIMIT $2)").bind(id).bind(batch)
}

pub(in super::super) fn remove_stream_extents(id: Id, batch: i64) -> Scalar<'static, Option<Id>> {
    sqlx::query_scalar("DELETE FROM extents WHERE stream_id=$1 AND offset_bytes IN (SELECT offset_bytes FROM extents WHERE stream_id=$1 ORDER BY offset_bytes LIMIT $2) RETURNING chunk_id").bind(id).bind(batch)
}

pub(in super::super) fn remove_empty_stream(id: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM streams s WHERE id=$1 AND NOT EXISTS(SELECT 1 FROM raw_ranges WHERE stream_id=s.id) AND NOT EXISTS(SELECT 1 FROM extents WHERE stream_id=s.id) AND NOT EXISTS(SELECT 1 FROM chunks WHERE owner_stream=s.id) AND NOT EXISTS(SELECT 1 FROM fragments WHERE owner_stream=s.id) AND NOT EXISTS(SELECT 1 FROM chunk_encodings WHERE owner_stream=s.id)").bind(id)
}

pub(in super::super) fn stale_fragments<'q>(
    batch: i64,
    active: &'q [Id],
    storage: Option<Uuid>,
) -> Scalar<'q, Id> {
    sqlx::query_scalar("SELECT f.id FROM fragments f WHERE ($3::uuid IS NULL OR f.staging_storage_id=$3 OR EXISTS(SELECT 1 FROM streams s JOIN buckets b ON b.id=s.bucket_id WHERE s.id=f.owner_stream AND b.storage_id=$3)) AND (f.sealed OR f.owner_stream IS NULL OR (NOT (f.owner_stream=ANY($2)) AND NOT EXISTS(SELECT 1 FROM streams s WHERE s.id=f.owner_stream AND s.state='writing'))) AND NOT EXISTS(SELECT 1 FROM extents WHERE fragment_id=f.id) ORDER BY f.created_at,f.id LIMIT $1").bind(batch).bind(active).bind(storage)
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
        Option<String>,
        Option<Uuid>,
    ),
> {
    sqlx::query_as("SELECT id,sealed,owner_stream,staging_storage_id,staging_object_key,file_id FROM fragments f WHERE id=$1 AND NOT EXISTS(SELECT 1 FROM node_read_sources n JOIN node_read_windows w ON w.id=n.window_id JOIN stream_leases l ON l.id=w.read_id WHERE n.kind='fragment' AND n.location_id=f.id AND l.expires_at>clock_timestamp()) AND NOT EXISTS(SELECT 1 FROM extents WHERE fragment_id=f.id) FOR UPDATE SKIP LOCKED").bind(id)
}

pub(in super::super) fn remove_fragment_row(id: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM fragments WHERE id=$1").bind(id)
}

pub(in super::super) fn remove_tombstones(batch: i64, storage: Option<Uuid>) -> Statement<'static> {
    sqlx::query("DELETE FROM objects WHERE (bucket_id,object_key) IN (SELECT o.bucket_id,o.object_key FROM objects o JOIN buckets b ON b.id=o.bucket_id WHERE ($2::uuid IS NULL OR b.storage_id=$2) AND o.stream_id IS NULL AND NOT EXISTS(SELECT 1 FROM streams s WHERE s.bucket_id=o.bucket_id AND s.object_key=o.object_key AND s.state='writing') ORDER BY o.bucket_id,o.object_key LIMIT $1 FOR UPDATE OF o SKIP LOCKED)").bind(batch).bind(storage)
}

pub(in super::super) fn gc_paused() -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT gc_paused OR maintenance FROM mokyu_meta")
}

pub(in super::super) fn profile_storages() -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT storage_id FROM storage_profiles ORDER BY storage_id")
}

pub(in super::super) fn lock_reclaim_chunk(
    id: Id,
    storage: Uuid,
    grace: f64,
) -> Rows<'static, (Option<Id>,)> {
    sqlx::query_as("SELECT owner_stream FROM chunks WHERE id=$1 AND storage_id=$2 AND state IN ('ready','failed','deleting') AND unreferenced_at<now()-$3*interval '1 second' AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=chunks.id) AND NOT EXISTS(SELECT 1 FROM pack_inputs WHERE chunk_id=chunks.id) AND NOT EXISTS(SELECT 1 FROM transfer_members p WHERE p.source_chunk=chunks.id) AND NOT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=chunks.id) AND NOT EXISTS(SELECT 1 FROM chunk_encodings WHERE chunk_id=chunks.id AND owner_stream IS NOT NULL) FOR UPDATE").bind(id).bind(storage).bind(grace)
}

pub(in super::super) fn claim_reclaim_chunk(id: Id, storage: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE chunks SET state='deleting',pack_id=NULL WHERE id=$1 AND storage_id=$2")
        .bind(id)
        .bind(storage)
}

pub(in super::super) fn reclaim_locations(id: Id) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT encoding_id FROM chunk_encodings WHERE chunk_id=$1 AND state IN ('ready','uploading','retired','deleting')").bind(id)
}

pub(in super::super) fn retire_chunk_encodings(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings l SET state='retired',unreferenced_at=COALESCE(l.unreferenced_at,c.unreferenced_at,now()) FROM chunks c WHERE l.chunk_id=c.id AND c.id=$1 AND l.state IN ('ready','uploading')").bind(id)
}

pub(in super::super) fn finish_reclaim_chunk(id: Id, storage: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE chunks SET state='deleted',deleted_at=now() WHERE id=$1 AND storage_id=$2 AND state='deleting'").bind(id).bind(storage)
}

pub(in super::super) fn retire_empty_packs(storage: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE packs p SET state='retired',unreferenced_at=COALESCE(unreferenced_at,(SELECT max(c.unreferenced_at) FROM pack_members m JOIN chunks c ON c.id=m.chunk_id WHERE m.pack_id=p.id),now()) WHERE state='ready' AND NOT EXISTS(SELECT 1 FROM chunks WHERE pack_id=p.id AND state='ready') AND p.storage_id=$1").bind(storage)
}

pub(in super::super) fn gc_counts() -> Rows<'static, (String, i64)> {
    sqlx::query_as("SELECT state,count(*) FROM chunks GROUP BY state")
}

pub(in super::super) fn gc_state() -> Rows<'static, (bool, bool)> {
    sqlx::query_as("SELECT gc_paused,maintenance FROM mokyu_meta")
}

pub(in super::super) fn mark_orphan_chunks() -> Statement<'static> {
    sqlx::query(
        "UPDATE chunks c SET unreferenced_at=clock_timestamp() WHERE c.state='ready' AND c.owner_stream IS NULL AND c.unreferenced_at IS NULL AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id)",
    )
}

pub(in super::super) fn cleanup_statement_timeout(timeout: String) -> Statement<'static> {
    sqlx::query("SELECT set_config('statement_timeout',$1,true)").bind(timeout)
}

pub(in super::super) fn cleanup_history<'q>(
    section: &str,
    age: f64,
    batch: i64,
    active: &'q [Uuid],
) -> Result<Statement<'q>> {
    let sql = match section {
        "key_replacements" => {
            "DELETE FROM key_replacements WHERE id IN (SELECT id FROM key_replacements WHERE expires_at<now()-$1*interval '1 second' ORDER BY expires_at LIMIT $2 FOR UPDATE SKIP LOCKED)"
        }
        "maintenance_previews" => {
            "DELETE FROM maintenance_previews WHERE id IN (SELECT id FROM maintenance_previews WHERE expires_at<now()-$1*interval '1 second' ORDER BY expires_at LIMIT $2 FOR UPDATE SKIP LOCKED)"
        }
        "media_operations" => {
            "DELETE FROM media_operations WHERE id IN (SELECT id FROM media_operations WHERE created_at<now()-$1*interval '1 second' ORDER BY created_at,id LIMIT $2 FOR UPDATE SKIP LOCKED)"
        }
        "audit_events" => {
            "DELETE FROM audit_events WHERE id IN (SELECT id FROM audit_events WHERE created_at<now()-$1*interval '1 second' ORDER BY created_at,id LIMIT $2 FOR UPDATE SKIP LOCKED)"
        }
        "chunks" => {
            "DELETE FROM chunks WHERE id IN (SELECT id FROM chunks c WHERE state='deleted' AND deleted_at<now()-((SELECT (left(sp.deleted_chunk_retention,-1)::bigint*CASE right(sp.deleted_chunk_retention,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=c.storage_id)+$1::double precision*0)*interval '1 second' AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id) AND NOT EXISTS(SELECT 1 FROM chunk_encodings WHERE chunk_id=c.id AND (state<>'deleted' OR recovery_pending OR owner_stream IS NOT NULL)) AND NOT EXISTS(SELECT 1 FROM pack_members m JOIN packs p ON p.id=m.pack_id WHERE m.chunk_id=c.id AND p.state<>'deleted') ORDER BY deleted_at,id LIMIT $2 FOR UPDATE SKIP LOCKED)"
        }
        "packs" => {
            "DELETE FROM packs WHERE id IN (SELECT id FROM packs p WHERE state='deleted' AND deleted_at<now()-((SELECT (left(sp.deleted_chunk_retention,-1)::bigint*CASE right(sp.deleted_chunk_retention,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=p.storage_id)+$1::double precision*0)*interval '1 second' AND NOT EXISTS(SELECT 1 FROM chunks WHERE pack_id=p.id) AND NOT EXISTS(SELECT 1 FROM pending_uploads WHERE source_pack_id=p.id) ORDER BY deleted_at,id LIMIT $2 FOR UPDATE SKIP LOCKED)"
        }
        "chunk_encodings" => {
            "DELETE FROM chunk_encodings WHERE encoding_id IN (SELECT encoding_id FROM chunk_encodings WHERE state='deleted' AND NOT recovery_pending AND owner_stream IS NULL AND deleted_at<now()-((SELECT (left(sp.deleted_chunk_retention,-1)::bigint*CASE right(sp.deleted_chunk_retention,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=chunk_encodings.storage_id)+$1::double precision*0)*interval '1 second' ORDER BY deleted_at,encoding_id LIMIT $2 FOR UPDATE SKIP LOCKED)"
        }
        "uploads" => {
            "DELETE FROM uploads WHERE id IN (SELECT id FROM uploads u WHERE state IN ('completed','aborted') AND touched_at<now()-((SELECT (left(sp.upload_retention,-1)::bigint*CASE right(sp.upload_retention,1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=(SELECT storage_id FROM buckets WHERE id=u.bucket_id))+$1::double precision*0)*interval '1 second' AND raw_source_id IS NULL AND NOT (id=ANY($3)) AND NOT EXISTS(SELECT 1 FROM parts WHERE upload_id=u.id) ORDER BY touched_at,id LIMIT $2 FOR UPDATE SKIP LOCKED)"
        }
        "integrity_issues" => {
            "DELETE FROM integrity_issues WHERE id IN (SELECT i.id FROM integrity_issues i JOIN tasks t ON t.id=i.task_id WHERE t.state='completed' AND t.updated_at<now()-$1*interval '1 second' ORDER BY i.id LIMIT $2 FOR UPDATE OF i SKIP LOCKED)"
        }
        "tasks" => {
            "DELETE FROM tasks WHERE id IN (SELECT id FROM tasks t WHERE state='completed' AND updated_at<now()-$1*interval '1 second' AND NOT EXISTS(SELECT 1 FROM integrity_issues WHERE task_id=t.id) AND NOT EXISTS(SELECT 1 FROM external_tests e WHERE e.id=t.id AND e.cleanup_required) AND NOT EXISTS(SELECT 1 FROM replica_targets rt WHERE rt.task_id=t.id) ORDER BY updated_at,id LIMIT $2 FOR UPDATE SKIP LOCKED)"
        }
        "sessions" => {
            "DELETE FROM sessions WHERE token_hash IN (SELECT token_hash FROM sessions s WHERE expires_at<now()-$1*interval '1 second' AND NOT EXISTS(SELECT 1 FROM session_refresh_tokens r WHERE r.session_id=s.id AND r.revoked_at IS NULL AND r.expires_at>now()) ORDER BY expires_at,token_hash LIMIT $2 FOR UPDATE SKIP LOCKED)"
        }
        "refresh_tokens" => {
            "DELETE FROM session_refresh_tokens WHERE id IN (SELECT id FROM session_refresh_tokens WHERE revoked_at<now()-$1*interval '1 second' AND NOT EXISTS(SELECT 1 FROM session_refresh_tokens live WHERE live.session_id=session_refresh_tokens.session_id AND live.family_id=session_refresh_tokens.family_id AND live.revoked_at IS NULL AND live.expires_at>now()) ORDER BY expires_at,id LIMIT $2 FOR UPDATE SKIP LOCKED)"
        }
        _ => anyhow::bail!("unknown cleanup section"),
    };
    let query = sqlx::query(sql).bind(age).bind(batch);
    Ok(if section == "uploads" {
        query.bind(active)
    } else {
        query
    })
}

pub(in super::super) async fn history_limits(db: &mut sqlx::PgConnection) -> Result<()> {
    sqlx::raw_sql("SET LOCAL lock_timeout='100ms'; SET LOCAL work_mem='4MB'")
        .execute(db)
        .await?;
    Ok(())
}

pub(in super::super) async fn cleanup_limits(db: &mut sqlx::PgConnection) -> Result<()> {
    sqlx::raw_sql("SET LOCAL lock_timeout='100ms'; SET LOCAL statement_timeout='1s'")
        .execute(db)
        .await?;
    Ok(())
}

fn physical_table(packed: bool) -> (&'static str, &'static str, &'static str) {
    if packed {
        (
            "packs",
            "id",
            "NOT EXISTS(SELECT 1 FROM stream_leases l JOIN extents e ON e.stream_id=l.stream_id JOIN pack_members m ON m.chunk_id=e.chunk_id WHERE m.pack_id=r.id AND l.expires_at>now()) AND NOT EXISTS(SELECT 1 FROM transfer_sources p WHERE p.source_pack=r.id)",
        )
    } else {
        (
            "chunk_encodings",
            "encoding_id",
            "NOT EXISTS(SELECT 1 FROM stream_leases l JOIN extents e ON e.stream_id=l.stream_id JOIN chunk_encodings ce ON ce.chunk_id=e.chunk_id WHERE ce.encoding_id=r.encoding_id AND l.expires_at>now()) AND NOT EXISTS(SELECT 1 FROM transfer_sources p WHERE p.source_encoding=r.encoding_id)",
        )
    }
}

fn node_read_source_guard(kind: &str, location: &str) -> String {
    format!(
        "NOT EXISTS(SELECT 1 FROM node_read_sources n JOIN node_read_windows w ON w.id=n.window_id JOIN stream_leases l ON l.id=w.read_id WHERE n.kind='{kind}' AND n.location_id={location} AND l.expires_at>clock_timestamp())"
    )
}

pub(in super::super) fn physical_gc_candidates(
    packed: bool,
    grace: f64,
    batch: i64,
    minimum: f64,
    storage: Uuid,
    single: bool,
) -> Rows<'static, (Id, Uuid, bool)> {
    let (table, id_column, lease_guard) = physical_table(packed);
    let tier_unit = i16::from(packed);
    let state_guard = if packed {
        "r.state IN ('retired','deleting')"
    } else {
        "(r.state IN ('retired','deleting') OR (r.state='deleted' AND r.recovery_pending)) AND r.owner_stream IS NULL"
    };
    let unknown = if packed {
        "r.stored_at IS NULL"
    } else {
        "r.stored_at IS NULL OR r.recovery_pending"
    };
    let lease_guard = format!(
        "({}) AND ({})",
        if single {
            if packed {
                "NOT EXISTS(SELECT 1 FROM transfer_sources p WHERE p.source_pack=r.id)"
            } else {
                "NOT EXISTS(SELECT 1 FROM transfer_sources p WHERE p.source_encoding=r.encoding_id)"
            }
        } else {
            lease_guard
        },
        node_read_source_guard(
            if packed { "pack" } else { "chunk" },
            &format!("COALESCE(tl.address,r.{id_column})")
        )
    );
    sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT r.{id_column},COALESCE(tl.address,r.{id_column}),(tl.address IS NULL AND ({unknown})) FROM {table} r LEFT JOIN tier_locations tl ON tl.unit={tier_unit} AND tl.physical_id=r.{id_column} WHERE NOT EXISTS(SELECT 1 FROM tier_moves mv WHERE mv.unit={tier_unit} AND mv.physical_id=r.{id_column}) AND NOT EXISTS(SELECT 1 FROM tier_locations th WHERE th.unit={tier_unit} AND th.physical_id=r.{id_column} AND th.activated_at>floor(extract(epoch from clock_timestamp()))::bigint-$3::bigint) AND {state_guard} AND r.unreferenced_at<now()-$1*interval '1 second' AND ($3::double precision=0 OR r.stored_at IS NULL OR r.stored_at<=now()-$3*interval '1 second') AND COALESCE(tl.storage_id,r.storage_id)=$4 AND {lease_guard} AND NOT EXISTS(SELECT 1 FROM tasks t WHERE t.task_type='integrity' AND t.state='running' AND t.detail->>'operation' IN ('recheck','verify') AND t.detail->>'source_location'=r.{id_column}::text AND ((t.lease_node_id IS NULL AND t.lease_expires_at IS NULL) OR t.lease_expires_at>clock_timestamp())) ORDER BY r.unreferenced_at,r.{id_column} LIMIT $2"))).bind(grace).bind(batch).bind(minimum).bind(storage)
}

pub(in super::super) fn physical_gc_age(
    packed: bool,
    id: Id,
    stored: DateTime<Utc>,
) -> Statement<'static> {
    if !packed {
        return sqlx::query("UPDATE chunk_encodings SET stored_at=CASE WHEN recovery_pending THEN $2 ELSE COALESCE(stored_at,$2) END WHERE encoding_id=$1").bind(id).bind(stored);
    }
    let (table, id_column, _lease_guard) = physical_table(packed);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {table} SET stored_at=COALESCE(stored_at,$2) WHERE {id_column}=$1"
    )))
    .bind(id)
    .bind(stored)
}

pub(in super::super) fn claim_physical_gc(
    packed: bool,
    id: Id,
    grace: f64,
    minimum: f64,
) -> Statement<'static> {
    let (table, id_column, lease_guard) = physical_table(packed);
    let tier_unit = i16::from(packed);
    let lease_guard = format!(
        "({lease_guard}) AND ({})",
        node_read_source_guard(
            if packed { "pack" } else { "chunk" },
            &format!(
                "COALESCE((SELECT th.address FROM tier_locations th WHERE th.unit={tier_unit} AND th.physical_id=r.{id_column}),r.{id_column})"
            )
        )
    );
    let state_guard = if packed {
        "state IN ('retired','deleting')"
    } else {
        "(state IN ('retired','deleting') OR (state='deleted' AND recovery_pending)) AND owner_stream IS NULL"
    };
    sqlx::query(sqlx::AssertSqlSafe(format!("UPDATE {table} r SET state='deleting' WHERE NOT EXISTS(SELECT 1 FROM tier_moves mv WHERE mv.unit={tier_unit} AND mv.physical_id=r.{id_column}) AND NOT EXISTS(SELECT 1 FROM tier_locations th WHERE th.unit={tier_unit} AND th.physical_id=r.{id_column} AND th.activated_at>floor(extract(epoch from clock_timestamp()))::bigint-$3::bigint) AND {lease_guard} AND NOT EXISTS(SELECT 1 FROM tasks t WHERE t.task_type='integrity' AND t.state='running' AND t.detail->>'operation' IN ('recheck','verify') AND t.detail->>'source_location'=r.{id_column}::text AND ((t.lease_node_id IS NULL AND t.lease_expires_at IS NULL) OR t.lease_expires_at>clock_timestamp())) AND {id_column}=$1 AND {state_guard} AND unreferenced_at<now()-$2*interval '1 second' AND ($3::double precision=0 OR stored_at IS NULL OR stored_at<=now()-$3*interval '1 second')"))).bind(id).bind(grace).bind(minimum)
}

pub(in super::super) fn finish_physical_gc(
    packed: bool,
    id: Id,
    retention: f64,
) -> Statement<'static> {
    if !packed {
        return sqlx::query("UPDATE chunk_encodings SET recovery_pending=recovery_pending AND (deleted_at IS NULL OR deleted_at>clock_timestamp()-$2*interval '1 second'),state='deleted',unreferenced_at=clock_timestamp(),deleted_at=COALESCE(deleted_at,clock_timestamp()) WHERE encoding_id=$1 AND state='deleting'").bind(id).bind(retention);
    }
    let (table, id_column, _lease_guard) = physical_table(packed);
    sqlx::query(sqlx::AssertSqlSafe(format!("UPDATE {table} SET state='deleted',deleted_at=now() WHERE {id_column}=$1 AND state='deleting'"))).bind(id)
}

#[async_trait::async_trait]
impl MaintenanceStore for Backend {
    async fn destructive_sweep(&self) -> Result<bool> {
        Ok(work::sweep_pending().fetch_one(&self.pool).await?)
    }
    async fn mode(&self, enabled: bool) -> Result<()> {
        transactions::maintenance_mode(&self.pool, enabled).await
    }

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

    async fn pack_creation(&self, paused: bool) -> Result<()> {
        transactions::change_pack_creation(&self.pool, paused).await
    }
    async fn policy_paused(&self, policy: &str) -> Result<bool> {
        Ok(maintenance_policy_paused(policy)
            .fetch_one(&self.pool)
            .await?)
    }
    async fn controls(&self) -> Result<Vec<Value>> {
        Ok(maintenance_controls().fetch_all(&self.pool).await?)
    }
    async fn preparing_packs(&self) -> Result<String> {
        Ok(preparing_pack_count().fetch_one(&self.pool).await?)
    }
    async fn change_policy(&self, policy: &str, paused: bool) -> Result<()> {
        transactions::change_maintenance_policy(&self.pool, policy, paused).await
    }
    async fn start_policy(&self, policy: &str, node: Option<Uuid>) -> Result<(Uuid, bool)> {
        transactions::start_lifecycle_policy(&self.pool, self.single, policy, node).await
    }
    async fn finish_policy(
        &self,
        id: Execution,
        count: i64,
        again: bool,
        report: Value,
    ) -> Result<()> {
        if self.single {
            return transactions::finish_single_lifecycle(&self.pool, id, count, again, report)
                .await;
        }
        finish_lifecycle_task(id, count, again, report)
            .execute(&self.pool)
            .await?;
        Ok(())
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
        Ok(gc_candidates(storage, grace, batch)
            .fetch_all(&self.pool)
            .await?)
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
        Ok(fragment_indexed(id).fetch_one(&self.pool).await?)
    }
    async fn recovery_fragments(&self, after: Id) -> Result<Vec<RecoveryFragment>> {
        Ok(recovery_fragments(after).fetch_all(&self.pool).await?)
    }
    async fn fragment_uploads(&self, id: Id) -> Result<Vec<Uuid>> {
        Ok(fragment_uploads(id).fetch_all(&self.pool).await?)
    }
    async fn expired_uploads(&self, batch: i64, storage: Option<Uuid>) -> Result<Vec<Uuid>> {
        Ok(expired_uploads(batch, storage)
            .fetch_all(&self.pool)
            .await?)
    }

    async fn stale_streams(
        &self,
        active: &[Id],
        batch: i64,
        storage: Option<Uuid>,
        retention: Option<f64>,
    ) -> Result<Vec<Id>> {
        Ok(stale_streams(
            active,
            batch,
            storage,
            retention,
            self.single && self.control_epoch.is_none(),
        )
        .build_query_scalar::<Id>()
        .fetch_all(&self.pool)
        .await?)
    }
    async fn claim_stream(
        &self,
        id: Id,
        storage: Option<Uuid>,
        retention: Option<f64>,
    ) -> Result<bool> {
        transactions::claim_stale_stream(
            &self.pool,
            id,
            storage,
            retention,
            self.single && self.control_epoch.is_none(),
        )
        .await
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
        Ok(gc_paused().fetch_one(&self.pool).await?)
    }
    async fn storages(&self) -> Result<Vec<Uuid>> {
        Ok(profile_storages().fetch_all(&self.pool).await?)
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
        finish_reclaim_chunk(id, storage)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    async fn retire_packs(&self, storage: Uuid) -> Result<()> {
        retire_empty_packs(storage).execute(&self.pool).await?;
        Ok(())
    }
    async fn physical_candidates(
        &self,
        packed: bool,
        grace: f64,
        batch: i64,
        minimum: f64,
        storage: Uuid,
    ) -> Result<Vec<(Id, Uuid, bool)>> {
        Ok(physical_gc_candidates(
            packed,
            grace,
            batch,
            minimum,
            storage,
            self.single && self.control_epoch.is_none(),
        )
        .fetch_all(&self.pool)
        .await?)
    }
    async fn physical_age(&self, packed: bool, id: Id, stored: DateTime<Utc>) -> Result<()> {
        physical_gc_age(packed, id, stored)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    async fn claim_physical(&self, packed: bool, id: Id, grace: f64, minimum: f64) -> Result<bool> {
        Ok(claim_physical_gc(packed, id, grace, minimum)
            .execute(&self.pool)
            .await?
            .rows_affected()
            != 0)
    }
    async fn delete_physical(&self, packed: bool, id: Id, retention: f64) -> Result<()> {
        let mut tx = super::super::queries::tier::begin(self).await?;
        let unit = if packed {
            crate::model::tiering::Unit::Pack
        } else {
            crate::model::tiering::Unit::Chunk
        };
        let address = super::super::queries::tier::gc_identity(&mut *tx, unit, id).await?;
        let changed = finish_physical_gc(packed, id, retention)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        if changed == 1
            && let Some(address) = address
        {
            super::super::queries::tier::forget(&mut *tx, unit, address).await?;
        }
        tx.commit().await?;
        Ok(())
    }
    async fn gc_status(&self) -> Result<(Vec<(String, i64)>, bool, bool)> {
        let counts = gc_counts().fetch_all(&self.pool).await?;
        let (paused, maintenance) = gc_state().fetch_one(&self.pool).await?;
        Ok((counts, paused, maintenance))
    }
    async fn mark_orphans(&self) -> Result<usize> {
        transactions::mark_orphan_chunks(&self.pool).await
    }
}

pub(in super::super) fn remove_maintenance_preview<'q>(
    actor: Option<Uuid>,
    token: Option<Uuid>,
    action: &'q str,
) -> Statement<'q> {
    sqlx::query("DELETE FROM maintenance_previews WHERE actor_id=$1 AND token_id IS NOT DISTINCT FROM $2 AND action=$3 AND task_id IS NULL").bind(actor).bind(token).bind(action)
}

pub(in super::super) fn create_maintenance_preview<'q>(
    actor: Option<Uuid>,
    token: Option<Uuid>,
    action: &'q str,
    parameters: Value,
    fingerprint: String,
) -> Rows<'q, (Uuid, DateTime<Utc>)> {
    sqlx::query_as("INSERT INTO maintenance_previews(actor_id,token_id,action,parameters,fingerprint) VALUES($1,$2,$3,$4,$5) RETURNING id,expires_at").bind(actor).bind(token).bind(action).bind(parameters).bind(fingerprint)
}

pub(in super::super) fn lock_maintenance_preview<'q>(
    id: Uuid,
    actor: Option<Uuid>,
    token: Option<Uuid>,
    action: &'q str,
) -> Rows<'q, (Value, String, Option<Uuid>)> {
    sqlx::query_as("SELECT parameters,fingerprint,task_id FROM maintenance_previews WHERE id=$1 AND actor_id=$2 AND token_id IS NOT DISTINCT FROM $3 AND action=$4 AND expires_at>now() FOR UPDATE").bind(id).bind(actor).bind(token).bind(action)
}

pub(in super::super) fn maintenance_preview_task(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE id=$1)").bind(id)
}

pub(in super::super) fn record_maintenance_preview(id: Uuid, task: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE maintenance_previews SET task_id=$2 WHERE id=$1")
        .bind(id)
        .bind(task)
}

pub(super) const JOB_SQL: &str = "SELECT to_jsonb(t)||jsonb_build_object('bucket_name',b.name,'blocked',CASE WHEN c.paused OR (coalesce(c.policy,'')='gc' AND m.gc_paused) THEN 'PolicyPaused' WHEN m.maintenance AND t.task_type IN ('pack','unpack','purge','gc','upload','storage_migration') THEN 'MaintenanceMode' WHEN m.pack_creation_paused AND t.task_type='pack' AND t.detail->>'policy' IN ('pack','repack') THEN 'PackCreationStopped' END) FROM tasks t LEFT JOIN buckets b ON b.id=t.bucket_id CROSS JOIN mokyu_meta m LEFT JOIN maintenance_controls c ON c.policy=CASE WHEN t.task_type='pack' THEN t.detail->>'policy' ELSE t.task_type END";
