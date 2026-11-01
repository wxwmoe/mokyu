use super::*;

pub(in super::super) async fn inventory_physical(
    connection: &mut sqlx::SqliteConnection,
) -> Result<Value> {
    let rows:Vec<(String,String,i64,i64)>=sqlx::query_as("SELECT source_type,state,COUNT(*),CAST(COALESCE(SUM(stored_size),0) AS INTEGER) FROM (SELECT 'chunk' AS source_type,state,standalone_size AS stored_size FROM chunk_encodings UNION ALL SELECT 'pack',state,stored_size FROM packs UNION ALL SELECT 'raw',state,size FROM raw_sources) sources GROUP BY source_type,state ORDER BY source_type,state").fetch_all(connection).await?;
    Ok(Value::Array(rows.into_iter().map(|(source_type,state,objects,stored_bytes)|serde_json::json!({"source_type":source_type,"state":state,"objects":objects,"stored_bytes":stored_bytes})).collect()))
}

pub(in super::super) fn inventory_live() -> Scalar<'static, Value> {
    sqlx::query_scalar(
        "WITH refs AS (SELECT e.chunk_id,SUM(e.length) AS bytes FROM objects o JOIN extents e ON e.stream_id=o.stream_id WHERE e.chunk_id IS NOT NULL GROUP BY e.chunk_id), raw_live AS (SELECT id,size FROM raw_sources r WHERE EXISTS(SELECT 1 FROM raw_ranges e JOIN objects o ON o.stream_id=e.stream_id WHERE e.source_id=r.id)), physical AS (SELECT l.standalone_size AS bytes,l.standalone_size-IIF(l.encryption_algorithm='none',0,16) AS payload FROM refs r JOIN chunks c ON c.id=r.chunk_id JOIN chunk_encodings l ON l.chunk_id=c.id AND l.state='ready' UNION ALL SELECT p.stored_size,p.stored_size-IIF(p.encryption_algorithm='none',0,16) FROM packs p WHERE state='ready' AND EXISTS(SELECT 1 FROM refs r JOIN chunks c ON c.id=r.chunk_id WHERE c.pack_id=p.id) UNION ALL SELECT size,size FROM raw_live) SELECT JSON_OBJECT('raw_objects',(SELECT COUNT(*) FROM raw_live),'chunks',COUNT(*),'reference_bytes',CAST((COALESCE(SUM(r.bytes),0)+(SELECT COALESCE(SUM(e.length),0) FROM raw_ranges e JOIN objects o ON o.stream_id=e.stream_id)) AS INTEGER),'raw_bytes',CAST((COALESCE(SUM(c.raw_size),0)+(SELECT COALESCE(SUM(size),0) FROM raw_live)) AS INTEGER),'pending_raw_bytes',CAST(COALESCE(SUM(IIF(c.pack_id IS NULL AND NOT EXISTS(SELECT 1 FROM chunk_encodings WHERE chunk_id=c.id AND state='ready'),c.raw_size,0)),0) AS INTEGER),'stored_bytes',(SELECT CAST(COALESCE(SUM(bytes),0) AS INTEGER) FROM physical),'payload_bytes',(SELECT CAST(COALESCE(SUM(payload),0) AS INTEGER) FROM physical)) FROM refs r JOIN chunks c ON c.id=r.chunk_id",
    )
}

pub(in super::super) fn inventory_unreferenced() -> Scalar<'static, Value> {
    sqlx::query_scalar(
        "SELECT JSON_OBJECT('chunks',COUNT(*),'eligible_chunks',CAST(COALESCE(SUM(unreferenced_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-((SELECT (CAST(substr(sp.unreferenced_grace,1,length(sp.unreferenced_grace)-1) AS INTEGER)*CASE substr(sp.unreferenced_grace,-1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=c.storage_id))*1000000 AND owner_stream IS NULL AND NOT EXISTS(SELECT 1 FROM pack_inputs WHERE chunk_id=c.id) AND NOT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=c.id)),0) AS INTEGER)) FROM chunks c WHERE state IN ('ready','failed','deleting') AND unreferenced_at IS NOT NULL AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id)",
    )
}

pub(in super::super) fn inventory_reclaimable() -> Scalar<'static, Value> {
    sqlx::query_scalar(
        "SELECT JSON_OBJECT('bytes',CAST(COALESCE(SUM(bytes),0) AS INTEGER),'eligible',CAST(COALESCE(SUM(IIF(since<CAST(unixepoch('subsec')*1000000 AS INTEGER)-((SELECT (CAST(substr(sp.unreferenced_grace,1,length(sp.unreferenced_grace)-1) AS INTEGER)*CASE substr(sp.unreferenced_grace,-1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=p.storage_id))*1000000,bytes,0)),0) AS INTEGER)) FROM (SELECT l.standalone_size AS bytes,COALESCE(l.unreferenced_at,c.unreferenced_at) AS since,l.storage_id FROM chunk_encodings l JOIN chunks c ON c.id=l.chunk_id WHERE l.state IN ('retired','deleting') OR (l.state='ready' AND c.unreferenced_at IS NOT NULL AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id)) UNION ALL SELECT stored_size,unreferenced_at,storage_id FROM packs WHERE state IN ('retired','deleting') UNION ALL SELECT size,unreferenced_at,storage_id FROM raw_sources r WHERE r.owner_stream IS NULL AND NOT EXISTS(SELECT 1 FROM raw_ranges WHERE source_id=r.id) AND NOT EXISTS(SELECT 1 FROM multipart_files WHERE source_id=r.id)) p",
    )
}

pub(in super::super) fn inventory_tasks() -> Scalar<'static, Value> {
    sqlx::query_scalar(
        "SELECT COALESCE(json_group_object(state,n),JSON_OBJECT()) FROM (SELECT state,COUNT(*) n FROM tasks GROUP BY state) t",
    )
}

pub(in super::super) fn inventory_uploads() -> Scalar<'static, Value> {
    sqlx::query_scalar(
        "SELECT COALESCE(json_group_object(state,n),JSON_OBJECT()) FROM (SELECT state,COUNT(*) n FROM uploads WHERE state IN ('active','completing') GROUP BY state) u",
    )
}

pub(in super::super) async fn inventory_cleanup(
    connection: &mut sqlx::SqliteConnection,
    tasks: f64,
) -> Result<Value> {
    let rows:Vec<(String,i64,Option<i64>)>=sqlx::query_as("WITH cutoffs AS (SELECT CAST(unixepoch('subsec')*1000000 AS INTEGER)-? AS tasks) SELECT 'chunks',COUNT(*),MIN(deleted_at) FROM chunks c WHERE state='deleted' AND deleted_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-((SELECT (CAST(substr(sp.deleted_chunk_retention,1,length(sp.deleted_chunk_retention)-1) AS INTEGER)*CASE substr(sp.deleted_chunk_retention,-1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=c.storage_id))*1000000 AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id) AND NOT EXISTS(SELECT 1 FROM chunk_encodings WHERE chunk_id=c.id AND (state<>'deleted' OR recovery_pending OR owner_stream IS NOT NULL)) AND NOT EXISTS(SELECT 1 FROM pack_members WHERE chunk_id=c.id) AND NOT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=c.id) AND NOT EXISTS(SELECT 1 FROM pack_inputs WHERE chunk_id=c.id) AND NOT EXISTS(SELECT 1 FROM cache_pins WHERE chunk_id=c.id)
        UNION ALL SELECT 'uploads',COUNT(*),MIN(touched_at) FROM uploads u WHERE state IN ('completed','aborted') AND touched_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-((SELECT (CAST(substr(sp.upload_retention,1,length(sp.upload_retention)-1) AS INTEGER)*CASE substr(sp.upload_retention,-1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=(SELECT storage_id FROM buckets WHERE id=u.bucket_id)))*1000000 AND NOT EXISTS(SELECT 1 FROM parts WHERE upload_id=u.id) AND NOT EXISTS(SELECT 1 FROM quota_reservations WHERE upload_id=u.id)
        UNION ALL SELECT 'tasks',COUNT(*),MIN(updated_at) FROM tasks t WHERE state='completed' AND updated_at<(SELECT tasks FROM cutoffs) AND NOT EXISTS(SELECT 1 FROM integrity_issues WHERE task_id=t.id) AND NOT EXISTS(SELECT 1 FROM external_tests e WHERE e.id=t.id AND e.cleanup_required)
        UNION ALL SELECT 'integrity_issues',COUNT(*),MIN(t.updated_at) FROM integrity_issues i JOIN tasks t ON t.id=i.task_id WHERE t.state='completed' AND t.updated_at<(SELECT tasks FROM cutoffs)
        UNION ALL SELECT 'sessions',COUNT(*),MIN(expires_at) FROM sessions s WHERE expires_at<CAST(unixepoch('subsec')*1000000 AS INTEGER) AND NOT EXISTS(SELECT 1 FROM session_refresh_tokens r WHERE r.session_id=s.id AND r.revoked_at IS NULL AND r.expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER))
        UNION ALL SELECT 'packs',COUNT(*),MIN(deleted_at) FROM packs p WHERE state='deleted' AND deleted_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-((SELECT (CAST(substr(sp.deleted_chunk_retention,1,length(sp.deleted_chunk_retention)-1) AS INTEGER)*CASE substr(sp.deleted_chunk_retention,-1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=p.storage_id))*1000000 AND NOT EXISTS(SELECT 1 FROM chunks WHERE pack_id=p.id) AND NOT EXISTS(SELECT 1 FROM pending_uploads WHERE source_pack_id=p.id)
        UNION ALL SELECT 'chunk_encodings',COUNT(*),MIN(deleted_at) FROM chunk_encodings ce WHERE state='deleted' AND deleted_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-((SELECT (CAST(substr(sp.deleted_chunk_retention,1,length(sp.deleted_chunk_retention)-1) AS INTEGER)*CASE substr(sp.deleted_chunk_retention,-1) WHEN 'm' THEN 60 WHEN 'h' THEN 3600 WHEN 'd' THEN 86400 ELSE 1 END) FROM storage_profiles sp WHERE sp.storage_id=ce.storage_id))*1000000").bind((tasks*1_000_000.) as i64).fetch_all(connection).await?;
    let mut result = serde_json::Map::new();
    for (name, eligible, oldest) in rows {
        result.insert(
            name,
            serde_json::json!({"eligible":eligible,"oldest_at":oldest.map(datetime).transpose()?}),
        );
    }
    Ok(Value::Object(result))
}

pub(in super::super) fn inventory_database() -> Scalar<'static, Value> {
    sqlx::query_scalar(
        "SELECT json_object('table',s.tbl_name,'total_bytes',SUM(d.pgsize),'index_bytes',SUM(IIF(s.type='index',d.pgsize,0)),'live_rows_estimate',NULL,'dead_rows_estimate',NULL,'last_autovacuum',NULL,'last_autoanalyze',NULL) FROM dbstat('main',TRUE) d JOIN sqlite_schema s ON s.name=d.name WHERE s.tbl_name IN ('chunks','extents','raw_sources','raw_ranges','streams','objects','uploads','parts','fragments','multipart_files','tasks','sessions','session_refresh_tokens','integrity_issues','chunk_encodings','packs','pack_members','pack_maintenance','pack_changes','pack_inputs','chunk_access_stats','chunk_access_windows','pack_access_windows','pack_member_access_windows','pending_uploads','cache_pins') GROUP BY s.tbl_name ORDER BY s.tbl_name",
    )
}

pub(in super::super) fn storage_inventory_buckets(storage: Uuid) -> Rows<'static, (i64, i64, i64)> {
    sqlx::query_as("SELECT COUNT(DISTINCT b.id),COUNT(o.stream_id),CAST(COALESCE(SUM(s.size),0) AS INTEGER) FROM buckets b LEFT JOIN objects o ON o.bucket_id=b.id AND o.stream_id IS NOT NULL LEFT JOIN streams s ON s.id=o.stream_id WHERE b.storage_id=?").bind(storage)
}

pub(in super::super) fn storage_inventory_chunks(storage: Uuid) -> Rows<'static, (i64, i64)> {
    sqlx::query_as("SELECT COUNT(*),CAST(COALESCE(SUM(standalone_size),0) AS INTEGER) FROM chunks WHERE storage_id=? AND state IN ('ready','deleting')").bind(storage)
}

pub(in super::super) fn storage_inventory_physical(storage: Uuid) -> Rows<'static, (i64, i64)> {
    sqlx::query_as("SELECT COUNT(*),CAST(COALESCE(SUM(bytes),0) AS INTEGER) FROM (SELECT standalone_size AS bytes FROM chunk_encodings WHERE storage_id=? AND state IN ('ready','retired','deleting') UNION ALL SELECT stored_size FROM packs WHERE storage_id=? AND state IN ('ready','retired','deleting') UNION ALL SELECT size FROM raw_sources WHERE storage_id=? AND state<>'writing') p").bind(storage).bind(storage).bind(storage)
}

#[async_trait::async_trait]
impl crate::model::metrics::MetricsStore for Backend {
    async fn inventory(
        &self,
        metrics: &crate::config::MetricsSettings,
        maintenance: &crate::config::Maintenance,
    ) -> Result<Value> {
        transactions::collect_inventory(self, metrics, maintenance).await
    }
    async fn storage_inventory(&self, storage: Uuid) -> Result<Value> {
        transactions::collect_storage(&self.read, storage).await
    }
    async fn usage(
        &self,
        principal: &Principal,
        scope: &crate::model::metrics::ScopeQuery,
        limit: usize,
    ) -> Result<crate::model::metrics::UsageSnapshot> {
        transactions::read_usage(self, principal, scope, limit).await
    }
    async fn runtime(
        &self,
        principal: &Principal,
    ) -> Result<crate::model::metrics::RuntimeHistory> {
        transactions::read_runtime(&self.read, principal).await
    }
    async fn sample_runtime(&self, point: RuntimePoint, retention: f64) -> Result<()> {
        transactions::sample_runtime(&self.write, point, retention).await
    }
    async fn flush_access(&self, pending: AccessBatch, stale: f64) -> Result<()> {
        transactions::flush_access(&self.write, pending, stale).await
    }
    async fn expire_access(&self, retention: f64, batch: i64) -> Result<()> {
        if let Err(error) = transactions::expire_access(&self.write, retention, batch).await {
            tracing::warn!(%error,"access statistics cleanup failed");
        }
        Ok(())
    }
}

pub(in super::super) fn usage_touch_due(key: &str) -> Scalar<'_, bool> {
    sqlx::query_scalar("SELECT requested_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-3600000000 FROM usage_summary WHERE id=?").bind(key)
}

pub(in super::super) async fn evict_usage_scopes(
    connection: &mut sqlx::SqliteConnection,
    keep: i64,
) -> Result<()> {
    sqlx::query("DELETE FROM usage_summary WHERE id IN (SELECT id FROM usage_summary WHERE id<>'global' ORDER BY requested_at DESC,id LIMIT -1 OFFSET ?)").bind(keep).execute(connection).await?;
    Ok(())
}

pub(in super::super) fn create_usage_scope(key: &str) -> Statement<'_> {
    sqlx::query("INSERT INTO usage_summary(id) VALUES(?)").bind(key)
}

pub(in super::super) async fn usage_bucket_members(
    connection: &mut sqlx::SqliteConnection,
    key: &str,
    ids: &[Uuid],
) -> Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    let mut query = sqlx::QueryBuilder::new("INSERT INTO usage_buckets(scope_id,bucket_id) ");
    query.push_values(ids, |mut row, id| {
        row.push_bind(key).push_bind(id);
    });
    query.build().execute(connection).await?;
    Ok(())
}

pub(in super::super) fn touch_usage_scope(key: &str) -> Statement<'_> {
    sqlx::query("UPDATE usage_summary SET requested_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND requested_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-3600000000").bind(key)
}

pub(in super::super) fn usage_saved(
    key: &str,
) -> Rows<'_, (Option<i64>, Option<sqlx::types::Json<StorageUsage>>)> {
    sqlx::query_as("SELECT as_of,data FROM usage_summary WHERE id=?").bind(key)
}

pub(in super::super) fn usage_history(
    key: &str,
) -> Rows<'_, (i64, sqlx::types::Json<StorageUsage>)> {
    sqlx::query_as("SELECT snapshot_at,data FROM (SELECT snapshot_at,data FROM usage_history WHERE scope_id=? ORDER BY snapshot_at DESC LIMIT 2048) h ORDER BY snapshot_at").bind(key)
}

pub(in super::super) fn expire_usage_scopes(retention: f64) -> Statement<'static> {
    sqlx::query("DELETE FROM usage_summary WHERE id<>'global' AND requested_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-?").bind((retention*1_000_000.) as i64)
}

pub(in super::super) fn append_usage_history(interval: f64) -> Statement<'static> {
    sqlx::query("INSERT INTO usage_history(scope_id,snapshot_at,data) SELECT s.id,s.as_of,s.data FROM usage_summary s WHERE s.as_of IS NOT NULL AND NOT EXISTS(SELECT 1 FROM usage_history h WHERE h.scope_id=s.id AND h.snapshot_at>s.as_of-?) ON CONFLICT(scope_id,snapshot_at) DO NOTHING").bind((interval*1_000_000.) as i64)
}

pub(in super::super) fn expire_usage_history(retention: f64) -> Statement<'static> {
    sqlx::query("DELETE FROM usage_history WHERE snapshot_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-?").bind((retention*1_000_000.) as i64)
}

pub(in super::super) async fn save_usage(
    connection: &mut sqlx::SqliteConnection,
    at: DateTime<Utc>,
    rows: Vec<(String, sqlx::types::Json<StorageUsage>)>,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut query = sqlx::QueryBuilder::new("UPDATE usage_summary SET as_of=");
    query.push_bind(at.timestamp_micros()).push(",data=CASE id");
    let mut ids = Vec::new();
    for (id, data) in rows {
        query
            .push(" WHEN ")
            .push_bind(id.clone())
            .push(" THEN ")
            .push_bind(data);
        ids.push(id);
    }
    query.push(" ELSE data END WHERE id IN (");
    let mut values = query.separated(",");
    for id in ids {
        values.push_bind(id);
    }
    values.push_unseparated(")");
    query
        .push(" AND (as_of IS NULL OR as_of<")
        .push_bind(at.timestamp_micros())
        .push(")");
    query.build().execute(connection).await?;
    Ok(())
}

// Merge intervals before allocating each covered segment across projects and their buckets.
pub(in super::super) const USAGE_SUMMARY: &str = r#"WITH chunk_ranges AS (
    SELECT DISTINCT o.bucket_id,b.project_id,e.chunk_id,e.source_offset AS lo,e.source_offset+e.length AS hi
    FROM objects o JOIN buckets b ON b.id=o.bucket_id JOIN extents e ON e.stream_id=o.stream_id WHERE e.chunk_id IS NOT NULL
), ordered_ranges AS (
    SELECT r.*,MAX(hi) OVER(PARTITION BY bucket_id,chunk_id ORDER BY lo,hi ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS previous_hi FROM chunk_ranges r
), grouped_ranges AS (
    SELECT r.*,SUM(IIF(previous_hi IS NULL OR lo>previous_hi,1,0)) OVER(PARTITION BY bucket_id,chunk_id ORDER BY lo,hi ROWS UNBOUNDED PRECEDING) AS group_id FROM ordered_ranges r
), ranges AS (
    SELECT bucket_id,project_id,chunk_id,MIN(lo) AS lo,MAX(hi) AS hi FROM grouped_ranges GROUP BY bucket_id,project_id,chunk_id,group_id
), boundaries AS (
    SELECT chunk_id,lo AS point FROM ranges UNION SELECT chunk_id,hi FROM ranges
), segments AS (
    SELECT chunk_id,point AS lo,LEAD(point) OVER(PARTITION BY chunk_id ORDER BY point) AS hi FROM boundaries
), coverage AS (
    SELECT s.chunk_id,s.lo,s.hi,r.bucket_id,r.project_id FROM segments s JOIN ranges r ON r.chunk_id=s.chunk_id AND r.lo<=s.lo AND r.hi>=s.hi WHERE s.hi IS NOT NULL
), project_counts AS (
    SELECT chunk_id,lo,COUNT(DISTINCT project_id) AS n FROM coverage GROUP BY chunk_id,lo
), bucket_counts AS (
    SELECT chunk_id,lo,project_id,COUNT(*) AS n FROM coverage GROUP BY chunk_id,lo,project_id
), allocated AS (
    SELECT c.bucket_id,c.chunk_id,SUM(CAST(c.hi-c.lo AS REAL)/p.n/b.n) AS raw_bytes FROM coverage c JOIN project_counts p USING(chunk_id,lo) JOIN bucket_counts b USING(chunk_id,lo,project_id) GROUP BY c.bucket_id,c.chunk_id
), chunk_unique AS (
    SELECT chunk_id,SUM(hi-lo) AS bytes FROM (SELECT DISTINCT chunk_id,lo,hi FROM coverage) u GROUP BY chunk_id
), sources AS (
    SELECT c.id AS chunk_id,CASE WHEN e.encoding_id IS NOT NULL THEN 'chunk' WHEN p.id IS NOT NULL THEN 'pack' ELSE 'pending' END AS source_type,
        COALESCE(e.encoding_id,p.id,c.id) AS source_id,COALESCE(e.standalone_size,p.stored_size) AS stored_size,u.bytes
    FROM chunk_unique u JOIN chunks c ON c.id=u.chunk_id LEFT JOIN chunk_encodings e ON e.chunk_id=c.id AND e.state='ready' LEFT JOIN packs p ON p.id=c.pack_id AND p.state='ready'
), source_totals AS (
    SELECT source_type,source_id,SUM(bytes) AS raw_bytes,MAX(stored_size) AS stored_size FROM sources GROUP BY source_type,source_id
), attribution AS (
    SELECT a.bucket_id,a.chunk_id,a.raw_bytes,s.source_type,s.source_id,a.raw_bytes/t.raw_bytes*t.stored_size AS encoded_bytes FROM allocated a JOIN sources s USING(chunk_id) JOIN source_totals t USING(source_type,source_id)
), scope_buckets AS (
    SELECT 'global' AS scope_id,id AS bucket_id FROM buckets UNION ALL SELECT scope_id,bucket_id FROM usage_buckets
), scope_segments AS (
    SELECT DISTINCT b.scope_id,c.chunk_id,c.lo,c.hi FROM scope_buckets b JOIN coverage c ON c.bucket_id=b.bucket_id
), scope_unique AS (
    SELECT u.scope_id,COUNT(DISTINCT u.chunk_id) AS chunks,COUNT(DISTINCT IIF(s.source_type='pack',s.source_id,NULL)) AS packs,SUM(u.hi-u.lo) AS bytes,
        SUM(IIF(s.source_type='pack',u.hi-u.lo,0)) AS packed_bytes,SUM(IIF(s.source_type='pending',u.hi-u.lo,0)) AS pending_bytes
    FROM scope_segments u JOIN sources s USING(chunk_id) GROUP BY u.scope_id
), scope_allocated AS (
    SELECT b.scope_id,SUM(a.raw_bytes) AS raw_bytes,COALESCE(SUM(a.encoded_bytes),0) AS encoded_bytes,MIN(a.encoded_bytes IS NOT NULL) AS complete
    FROM scope_buckets b JOIN attribution a ON a.bucket_id=b.bucket_id GROUP BY b.scope_id
), logical_totals AS (
    SELECT 'global' AS id,COUNT(b.id) AS buckets,COALESCE(SUM(q.used_bytes),0) AS bytes,COALESCE(SUM(q.object_count),0) AS objects FROM buckets b LEFT JOIN quota_scopes q ON q.scope='bucket' AND q.scope_id=b.id
    UNION ALL SELECT m.scope_id,COUNT(b.id),COALESCE(SUM(q.used_bytes),0),COALESCE(SUM(q.object_count),0) FROM usage_buckets m JOIN buckets b ON b.id=m.bucket_id LEFT JOIN quota_scopes q ON q.scope='bucket' AND q.scope_id=b.id GROUP BY m.scope_id
), scope_logical AS (
    SELECT s.id,COALESCE(l.buckets,0) AS buckets,COALESCE(l.bytes,0) AS bytes,COALESCE(l.objects,0) AS objects FROM usage_summary s LEFT JOIN logical_totals l ON l.id=s.id
), raw_references AS (
    SELECT DISTINCT o.bucket_id,b.project_id,r.id AS source_id,r.size
    FROM objects o JOIN buckets b ON b.id=o.bucket_id JOIN raw_ranges e ON e.stream_id=o.stream_id JOIN raw_sources r ON r.id=e.source_id
), raw_project_counts AS (
    SELECT source_id,COUNT(DISTINCT project_id) AS n FROM raw_references GROUP BY source_id
), raw_allocated AS (
    SELECT r.*,CAST(r.size AS REAL)/p.n/COUNT(*) OVER(PARTITION BY r.source_id,r.project_id) AS attributed
    FROM raw_references r JOIN raw_project_counts p USING(source_id)
), raw_scopes AS (
    SELECT id,SUM(size) AS bytes,SUM(attributed) AS attributed FROM (
        SELECT s.scope_id AS id,r.source_id,MAX(r.size) AS size,SUM(r.attributed) AS attributed
        FROM scope_buckets s JOIN raw_allocated r ON r.bucket_id=s.bucket_id GROUP BY s.scope_id,r.source_id
    ) scoped_raw GROUP BY id
), raw_inventory AS (
    SELECT r.*,
        EXISTS(SELECT 1 FROM raw_ranges e JOIN objects o ON o.stream_id=e.stream_id WHERE e.source_id=r.id) AS selected,
        r.owner_stream IS NULL AND NOT EXISTS(SELECT 1 FROM raw_ranges e WHERE e.source_id=r.id) AS garbage
    FROM raw_sources r
), raw_physical AS (
    SELECT COALESCE(SUM(CASE WHEN state<>'writing' THEN size ELSE 0 END),0) AS indexed_bytes,
        COALESCE(SUM(CASE WHEN selected THEN size ELSE 0 END),0) AS selected_bytes,
        COALESCE(SUM(CASE WHEN NOT selected AND NOT garbage AND state<>'writing' THEN size ELSE 0 END),0) AS retained_bytes,
        COALESCE(SUM(CASE WHEN NOT selected AND garbage THEN size ELSE 0 END),0) AS gc_bytes,
        COALESCE(SUM(CASE WHEN state='writing' THEN size ELSE 0 END),0) AS unconfirmed_bytes,
        COUNT(*) AS raw_objects FROM raw_inventory
), physical AS (
    SELECT 'chunk' AS source_type,e.encoding_id AS source_id,e.state,e.standalone_size AS stored_size,
        e.state IN ('retired','deleting') OR (c.unreferenced_at IS NOT NULL AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id)) AS garbage
    FROM chunk_encodings e JOIN chunks c ON c.id=e.chunk_id WHERE e.state IN ('ready','retired','deleting','uploading')
    UNION ALL SELECT 'pack',id,state,stored_size,state IN ('retired','deleting') FROM packs WHERE state<>'deleted'
), bridge AS (
    SELECT JSON_OBJECT(
        'indexed_bytes',CAST((COALESCE(SUM(IIF(p.state IN ('ready','retired','deleting'),p.stored_size,0)),0))+(SELECT indexed_bytes FROM raw_physical) AS TEXT),
        'selected_bytes',CAST((COALESCE(SUM(IIF(t.source_id IS NOT NULL,p.stored_size,0)),0))+(SELECT selected_bytes FROM raw_physical) AS TEXT),
        'retained_bytes',CAST((COALESCE(SUM(IIF(t.source_id IS NULL AND p.state IN ('ready','retired','deleting') AND NOT p.garbage,p.stored_size,0)),0))+(SELECT retained_bytes FROM raw_physical) AS TEXT),
        'gc_bytes',CAST((COALESCE(SUM(IIF(t.source_id IS NULL AND p.garbage,p.stored_size,0)),0))+(SELECT gc_bytes FROM raw_physical) AS TEXT),
        'unconfirmed_bytes',CAST((COALESCE(SUM(IIF(p.state IN ('preparing','uploading'),p.stored_size,0)),0))+(SELECT unconfirmed_bytes FROM raw_physical) AS TEXT),
        'chunk_objects',CAST(COALESCE(SUM(p.source_type='chunk' AND p.state IN ('ready','retired','deleting')),0) AS TEXT),
        'raw_objects',CAST((SELECT raw_objects FROM raw_physical) AS TEXT),
        'pack_objects',CAST(COALESCE(SUM(p.source_type='pack' AND p.state IN ('ready','retired','deleting')),0) AS TEXT)
    ) AS data FROM physical p LEFT JOIN source_totals t ON t.source_type=p.source_type AND t.source_id=p.source_id AND t.source_type<>'pending'
)
SELECT l.id,JSON_OBJECT(
    'buckets',CAST(l.buckets AS TEXT),'objects',CAST(l.objects AS TEXT),'logical_bytes',CAST(l.bytes AS TEXT),
    'unique_bytes',CAST((COALESCE(u.bytes,0)+COALESCE(r.bytes,0)) AS TEXT),'attributed_raw_bytes',CAST(ROUND((COALESCE(a.raw_bytes,0)+COALESCE(r.attributed,0)),6) AS TEXT),
    'encoded_bytes',IIF(COALESCE(a.complete,TRUE),CAST(ROUND((COALESCE(a.encoded_bytes,0)+COALESCE(r.attributed,0)),6) AS TEXT),NULL),
    'encoded_known_bytes',CAST(ROUND((COALESCE(a.encoded_bytes,0)+COALESCE(r.attributed,0)),6) AS TEXT),
    'local_savings',CAST(l.bytes-(COALESCE(u.bytes,0)+COALESCE(r.bytes,0)) AS TEXT),'shared_savings',CAST(ROUND((COALESCE(u.bytes,0)+COALESCE(r.bytes,0))-(COALESCE(a.raw_bytes,0)+COALESCE(r.attributed,0)),6) AS TEXT),
    'encoding_savings',IIF(COALESCE(a.complete,TRUE),CAST(ROUND((COALESCE(a.raw_bytes,0)+COALESCE(r.attributed,0))-(COALESCE(a.encoded_bytes,0)+COALESCE(r.attributed,0)),6) AS TEXT),NULL),
    'chunks',CAST(COALESCE(u.chunks,0) AS TEXT),'packs',CAST(COALESCE(u.packs,0) AS TEXT),
    'packed_bytes',CAST(COALESCE(u.packed_bytes,0) AS TEXT),'pending_bytes',CAST(COALESCE(u.pending_bytes,0) AS TEXT),
    'physical',IIF(l.id='global',json((SELECT data FROM bridge)),NULL)
) AS data FROM scope_logical l LEFT JOIN scope_unique u ON u.scope_id=l.id LEFT JOIN scope_allocated a ON a.scope_id=l.id LEFT JOIN raw_scopes r ON r.id=l.id
"#;

pub(in super::super) fn collect_usage() -> Rows<'static, (String, sqlx::types::Json<StorageUsage>)>
{
    sqlx::query_as(USAGE_SUMMARY)
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for WorkSummary {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            task_type: row.try_get("task_type")?,
            state: row.try_get("state")?,
            processed: row.try_get("processed")?,
            updated_at: date(row, "updated_at")?,
        })
    }
}

pub(in super::super) fn runtime_history() -> Scalar<'static, sqlx::types::Json<RuntimePoint>> {
    sqlx::query_scalar(
        "SELECT data FROM (SELECT sampled_at,data FROM runtime_samples ORDER BY sampled_at DESC LIMIT 2048) h ORDER BY sampled_at",
    )
}

pub(in super::super) fn runtime_work() -> Rows<'static, WorkSummary> {
    sqlx::query_as(
        "SELECT id,policy AS task_type,state,CAST(processed AS TEXT) AS processed,updated_at FROM tasks WHERE state IN ('queued','running','paused') ORDER BY CASE state WHEN 'running' THEN 0 WHEN 'queued' THEN 1 ELSE 2 END,updated_at DESC,id LIMIT 5",
    )
}

pub(in super::super) fn runtime_pending() -> Rows<'static, (String, String, Option<i64>, String)> {
    sqlx::query_as(
        "SELECT CAST(COUNT(*) AS TEXT),CAST(COALESCE(SUM(staged_size),0) AS TEXT),MIN(created_at),CAST(COALESCE(SUM(last_error IS NOT NULL),0) AS TEXT) FROM pending_uploads",
    )
}

pub(in super::super) fn runtime_sample(point: RuntimePoint) -> Statement<'static> {
    sqlx::query("INSERT INTO runtime_samples(sampled_at,data) VALUES(?,?)")
        .bind(point.at.timestamp_micros())
        .bind(sqlx::types::Json(point))
}

pub(in super::super) fn runtime_expire(retention: f64) -> Statement<'static> {
    sqlx::query("DELETE FROM runtime_samples WHERE sampled_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-?").bind((retention*1_000_000.) as i64)
}

pub(in super::super) async fn flush_chunk_access(
    connection: &mut sqlx::SqliteConnection,
    pending: &AccessBatch,
) -> Result<()> {
    if pending.chunks.is_empty() {
        return Ok(());
    }
    let data = sqlx::types::Json(
        pending
            .chunks
            .iter()
            .map(|((id, at), c)| {
                (
                    *id,
                    at.timestamp_micros(),
                    c.reads,
                    c.ranges,
                    c.bytes,
                    c.origins,
                )
            })
            .collect::<Vec<_>>(),
    );
    sqlx::query("INSERT INTO chunk_access_stats(chunk_id,read_count,range_read_count,read_bytes,last_read_at) SELECT c.id,SUM(json_extract(j.value,'$[2]')),SUM(json_extract(j.value,'$[3]')),SUM(json_extract(j.value,'$[4]')),CAST(unixepoch('subsec')*1000000 AS INTEGER) FROM json_each(?) j JOIN chunks c ON c.id=json_extract(j.value,'$[0]') GROUP BY c.id ORDER BY c.id ON CONFLICT(chunk_id) DO UPDATE SET read_count=chunk_access_stats.read_count+excluded.read_count,range_read_count=chunk_access_stats.range_read_count+excluded.range_read_count,read_bytes=chunk_access_stats.read_bytes+excluded.read_bytes,last_read_at=excluded.last_read_at").bind(&data).execute(&mut *connection).await?;
    sqlx::query("INSERT INTO chunk_access_windows(chunk_id,window_start,read_count,range_read_count,read_bytes,range_origin_reads) SELECT c.id,json_extract(j.value,'$[1]'),json_extract(j.value,'$[2]'),json_extract(j.value,'$[3]'),json_extract(j.value,'$[4]'),json_extract(j.value,'$[5]') FROM json_each(?) j JOIN chunks c ON c.id=json_extract(j.value,'$[0]') ORDER BY c.id,json_extract(j.value,'$[1]') ON CONFLICT(chunk_id,window_start) DO UPDATE SET read_count=chunk_access_windows.read_count+excluded.read_count,range_read_count=chunk_access_windows.range_read_count+excluded.range_read_count,read_bytes=chunk_access_windows.read_bytes+excluded.read_bytes,range_origin_reads=chunk_access_windows.range_origin_reads+excluded.range_origin_reads").bind(data).execute(connection).await?;
    Ok(())
}

pub(in super::super) async fn flush_pack_access(
    connection: &mut sqlx::SqliteConnection,
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
    sqlx::query("INSERT INTO pack_access_windows(pack_id,window_start,downloads,downloaded_bytes,partial_downloads,useful_bytes,partial_bytes) SELECT p.id,json_extract(j.value,'$[1]'),json_extract(j.value,'$[2]'),json_extract(j.value,'$[3]'),json_extract(j.value,'$[4]'),json_extract(j.value,'$[5]'),json_extract(j.value,'$[6]') FROM json_each(?) j JOIN packs p ON p.id=json_extract(j.value,'$[0]') ORDER BY p.id,json_extract(j.value,'$[1]') ON CONFLICT(pack_id,window_start) DO UPDATE SET downloads=pack_access_windows.downloads+excluded.downloads,downloaded_bytes=pack_access_windows.downloaded_bytes+excluded.downloaded_bytes,partial_downloads=pack_access_windows.partial_downloads+excluded.partial_downloads,useful_bytes=pack_access_windows.useful_bytes+excluded.useful_bytes,partial_bytes=pack_access_windows.partial_bytes+excluded.partial_bytes,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)").bind(data).execute(connection).await?;
    Ok(())
}

pub(in super::super) async fn flush_member_access(
    connection: &mut sqlx::SqliteConnection,
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
    sqlx::query("INSERT INTO pack_member_access_windows(pack_id,chunk_id,window_start,downloads) SELECT p.id,c.id,json_extract(j.value,'$[2]'),json_extract(j.value,'$[3]') FROM json_each(?) j JOIN packs p ON p.id=json_extract(j.value,'$[0]') JOIN chunks c ON c.id=json_extract(j.value,'$[1]') ORDER BY p.id,json_extract(j.value,'$[2]'),c.id ON CONFLICT(pack_id,window_start,chunk_id) DO UPDATE SET downloads=pack_member_access_windows.downloads+excluded.downloads").bind(data).execute(connection).await?;
    Ok(())
}

pub(in super::super) fn access_coverage(dropped: bool, stale: f64) -> Statement<'static> {
    sqlx::query("UPDATE access_state SET coverage_since=IIF(? OR coverage_since IS NULL OR flushed_at IS NULL OR flushed_at<CAST(unixepoch('subsec')*1000000 AS INTEGER)-?,CAST(unixepoch('subsec')*1000000 AS INTEGER),coverage_since),flushed_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=1").bind(dropped).bind((stale*1_000_000.) as i64)
}

pub(in super::super) async fn expire_access(
    connection: &mut sqlx::SqliteConnection,
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
    for (table, columns) in [
        ("chunk_access_windows", "chunk_id,window_start"),
        ("pack_access_windows", "pack_id,window_start"),
        (
            "pack_member_access_windows",
            "pack_id,window_start,chunk_id",
        ),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(format!("DELETE FROM {table} WHERE ({columns}) IN (SELECT {columns} FROM {table} WHERE window_start<CAST(unixepoch('subsec')*1000000 AS INTEGER)-? ORDER BY window_start,{columns} LIMIT ?)"))).bind((retention*1_000_000.) as i64).bind(batch).execute(&mut *connection).await?;
    }
    Ok(())
}

pub(super) fn source_scope(
    query: &mut sqlx::QueryBuilder<Sqlite>,
    stream: Option<Id>,
    chunks: &[Id],
) {
    query.push("c.id IN (");
    if let Some(stream) = stream {
        query
            .push("SELECT chunk_id FROM extents WHERE stream_id=")
            .push_bind(stream);
    } else if chunks.is_empty() {
        query.push("NULL");
    } else {
        let mut values = query.separated(",");
        for id in chunks {
            values.push_bind(*id);
        }
    }
    query.push(")");
}
